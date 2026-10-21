from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from .broker import Broker
from .config import Settings
from .errors import (
    AmbiguousContract,
    BrokerError,
    BrokerTimeout,
    GuardError,
    UncertainOperation,
    ValidationError,
)
from .models import (
    Contract,
    Health,
    OrderSpec,
    OrderState,
    OrderType,
    Preview,
    Quote,
    QuoteClassification,
    Side,
    TimeInForce,
)
from .persistence import Store


def _finite_positive(value: Decimal, label: str) -> None:
    if not value.is_finite() or value <= 0:
        raise ValidationError(f"{label} must be finite and positive")


def _state_from_row(row: dict[str, Any]) -> OrderState:
    result = json.loads(row["result_json"]) if row.get("result_json") else {}
    return OrderState(
        intent_id=row["intent_id"],
        order_id=row.get("order_id"),
        perm_id=row.get("perm_id"),
        order_ref=row["order_ref"],
        broker_status=row.get("broker_status") or result.get("broker_status", ""),
        status=row["state"],
        filled=Decimal(str(result.get("filled", "0"))),
        remaining=(
            Decimal(str(result["remaining"])) if result.get("remaining") is not None else None
        ),
        average_fill_price=(
            Decimal(str(result["average_fill_price"]))
            if result.get("average_fill_price") is not None
            else None
        ),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        uncertain=row["state"] == "uncertain",
        message=result.get("message"),
    )


class TradingService:
    def __init__(self, settings: Settings, broker: Broker, store: Store):
        self.settings = settings
        self.broker = broker
        self.store = store
        self.broker.set_event_handler(self._handle_broker_event)

    def _handle_broker_event(self, event_type: str, value: Any) -> None:
        """Runs on the adapter dispatcher, never on its socket reader thread."""
        if event_type == "execution":
            self.store.add_execution(value)
            return
        if event_type == "order_state" and value.order_id is not None:
            row = self.store.get_by_order_id(value.order_id)
            if row is not None:
                self._record_result(row["intent_id"], value)

    def connect(self) -> Health:
        last_error: Exception | None = None
        for attempt in range(self.settings.reconnect_attempts + 1):
            try:
                health = self.broker.connect()
                self.settings.assert_account_environment(list(health.managed_accounts))
                if not health.handshake_ready:
                    raise BrokerError("socket connected but IBKR handshake is not ready")
                return self.broker.synchronize()
            except (BrokerError, ConnectionError, TimeoutError) as exc:
                last_error = exc
                if attempt >= self.settings.reconnect_attempts:
                    raise
                time.sleep(min(self.settings.reconnect_base_delay_seconds * (2**attempt), 30.0))
        raise BrokerError("connection failed") from last_error

    def resolve_contract(
        self,
        symbol: str,
        *,
        security_type: str = "STK",
        currency: str = "USD",
        con_id: int | None = None,
        primary_exchange: str | None = None,
    ) -> Contract:
        if security_type != "STK":
            raise ValidationError("only US-listed stocks and ETFs (STK) are supported")
        if currency != "USD":
            raise ValidationError("only USD contracts are supported")
        matches = self.broker.search_contracts(
            symbol.upper(), security_type=security_type, currency=currency
        )
        filtered = [item for item in matches if con_id is None or item.con_id == con_id]
        filtered = [
            item
            for item in filtered
            if primary_exchange is None or item.primary_exchange == primary_exchange
        ]
        if not filtered:
            raise ValidationError("no supported contract matched the supplied identity")
        if len(filtered) != 1:
            choices = [
                {
                    "con_id": item.con_id,
                    "symbol": item.symbol,
                    "primary_exchange": item.primary_exchange,
                    "currency": item.currency,
                }
                for item in filtered
            ]
            raise AmbiguousContract(
                f"contract is ambiguous; select con_id or primary_exchange from {choices}"
            )
        contract = filtered[0]
        if contract.security_type != "STK" or contract.currency != "USD":
            raise ValidationError("resolved contract is outside the supported product scope")
        return contract

    def validate_order(
        self,
        spec: OrderSpec,
        *,
        quote: Quote | None = None,
        pending_sell_credit: Decimal = Decimal("0"),
    ) -> Decimal:
        if spec.contract.con_id <= 0:
            raise ValidationError("a qualified positive conId is required")
        if spec.contract.security_type != "STK" or spec.contract.currency != "USD":
            raise ValidationError("only USD stocks and ETFs are supported")
        _finite_positive(spec.quantity, "quantity")
        if spec.quantity != spec.quantity.to_integral_value():
            raise ValidationError("fractional shares are not supported")
        if spec.quantity > self.settings.max_order_quantity:
            raise ValidationError("quantity exceeds MBT_MAX_ORDER_QUANTITY")
        if spec.order_type is OrderType.LMT:
            if spec.limit_price is None:
                raise ValidationError("LMT orders require a limit price")
            _finite_positive(spec.limit_price, "limit price")
            reference = spec.limit_price
            applicable_tick = spec.contract.min_tick
            for low_edge, increment in sorted(spec.contract.price_increments):
                if spec.limit_price >= low_edge:
                    applicable_tick = increment
            if applicable_tick is not None:
                _finite_positive(applicable_tick, "contract minimum tick")
                if spec.limit_price % applicable_tick != 0:
                    raise ValidationError(
                        f"limit price is not aligned to minimum tick {applicable_tick}"
                    )
        elif spec.order_type is OrderType.MKT:
            if spec.limit_price is not None:
                raise ValidationError("MKT orders must not have a limit price")
            if quote is None:
                raise ValidationError("MKT orders require a fresh price reference")
            age = (datetime.now(UTC) - quote.received_at).total_seconds()
            if age < 0 or age > self.settings.max_quote_age_seconds:
                raise ValidationError("quote is stale for market-order notional validation")
            if quote.classification is QuoteClassification.UNAVAILABLE:
                raise ValidationError("quote is unavailable for market-order validation")
            if (
                quote.classification
                in {QuoteClassification.DELAYED, QuoteClassification.DELAYED_FROZEN}
                and not self.settings.allow_delayed_market_data
            ):
                raise ValidationError("delayed quote is not allowed for market-order validation")
            reference = quote.price_reference(spec.side)
            if reference is None:
                raise ValidationError("quote has no usable price reference")
            _finite_positive(reference, "price reference")
        else:
            raise ValidationError("only LMT and MKT orders are supported")
        if spec.time_in_force not in {TimeInForce.DAY, TimeInForce.GTC}:
            raise ValidationError("only DAY and GTC are supported")
        notional = spec.quantity * reference
        if notional > self.settings.max_order_notional:
            raise ValidationError("estimated notional exceeds MBT_MAX_ORDER_NOTIONAL")
        if spec.side is Side.SELL:
            positions = self.broker.positions()
            held = sum(
                (
                    item.quantity
                    for item in positions
                    if item.account == self.settings.account
                    and item.contract.con_id == spec.contract.con_id
                ),
                Decimal("0"),
            )
            pending = self.store.pending_sell_quantity(spec.contract.con_id)
            available = held - pending + pending_sell_credit
            if spec.quantity > available:
                raise ValidationError(
                    f"SELL would exceed available holdings ({available} after pending sells)"
                )
        return notional

    def preview(self, spec: OrderSpec) -> Preview:
        quote = self.broker.quote(spec.contract) if spec.order_type is OrderType.MKT else None
        self.validate_order(spec, quote=quote)
        return self.broker.preview(spec, order_ref=f"mbt-preview:{uuid.uuid4()}")

    def _assert_write(self, *, live_invocation: bool, submission: bool) -> None:
        health = self.broker.health()
        if not (
            health.socket_connected
            and health.handshake_ready
            and health.broker_server_connected
            and health.synchronized
        ):
            raise GuardError("broker session is not fully connected and synchronized")
        if self.settings.operation_mode == "read_only":
            raise GuardError("application operation mode is read_only")
        if submission and self.settings.disable_new_orders:
            raise GuardError(
                "new orders are disabled; inspection and eligible cancellation remain available"
            )
        if self.settings.environment == "paper":
            if self.settings.operation_mode != "paper_write":
                raise GuardError("paper mutations require paper_write mode")
            if live_invocation:
                raise GuardError("--live cannot be used with the paper environment")
            return
        if self.settings.operation_mode != "live_write":
            raise GuardError("live mutations require live_write mode")
        if not self.settings.enable_live_execution:
            raise GuardError("MBT_ENABLE_LIVE_EXECUTION is not enabled")
        if self.settings.account not in self.settings.live_account_allowlist:
            raise GuardError("configured live account is not in MBT_LIVE_ACCOUNT_ALLOWLIST")
        if not live_invocation:
            raise GuardError("live mutation requires the explicit --live invocation flag")

    @staticmethod
    def _payload(spec: OrderSpec) -> dict[str, Any]:
        return {
            "con_id": spec.contract.con_id,
            "symbol": spec.contract.symbol,
            "security_type": spec.contract.security_type,
            "currency": spec.contract.currency,
            "exchange": spec.contract.exchange,
            "primary_exchange": spec.contract.primary_exchange,
            "side": spec.side.value,
            "quantity": spec.quantity,
            "order_type": spec.order_type.value,
            "time_in_force": spec.time_in_force.value,
            "limit_price": spec.limit_price,
            "outside_regular_hours": spec.outside_regular_hours,
        }

    def _record_result(self, intent_id: str, state: OrderState) -> OrderState:
        normalized = replace(state, intent_id=intent_id)
        result = asdict(normalized)
        self.store.update_intent(
            intent_id,
            state=normalized.status,
            broker_status=normalized.broker_status,
            order_id=normalized.order_id,
            perm_id=normalized.perm_id,
            result=result,
        )
        self.store.add_event(
            intent_id,
            "order_status",
            result,
            broker_status=normalized.broker_status,
            dedupe_key=(
                f"status:{normalized.broker_status}:{normalized.filled}:{normalized.remaining}:"
                f"{normalized.perm_id}"
            ),
        )
        return normalized

    def _perform_mutation(
        self,
        *,
        key: str,
        operation: str,
        payload: dict[str, Any],
        action: Callable[[dict[str, Any]], OrderState],
        contract_id: int | None = None,
    ) -> OrderState:
        row, created = self.store.begin_intent(
            idempotency_key=key,
            operation=operation,
            account=self.settings.account,
            environment=self.settings.environment,
            client_id=self.settings.client_id,
            payload=payload,
        )
        if not created:
            return _state_from_row(row)
        try:
            if contract_id is not None:
                unresolved = [
                    item
                    for item in self.store.unresolved_for_contract(contract_id)
                    if item != row["intent_id"]
                ]
                if unresolved:
                    raise GuardError(
                        f"contract has unresolved mutation intent(s) {unresolved}; "
                        "reconcile or resolve first"
                    )
            state = action(row)
        except (BrokerTimeout, ConnectionError, TimeoutError) as exc:
            self.store.update_intent(
                row["intent_id"],
                state="uncertain",
                error={"type": type(exc).__name__, "message": str(exc)},
            )
            raise UncertainOperation(
                "broker outcome is uncertain; automatic resubmission is blocked pending "
                "reconciliation"
            ) from exc
        except Exception as exc:
            self.store.update_intent(
                row["intent_id"],
                state="rejected",
                error={"type": type(exc).__name__, "message": str(exc)},
            )
            raise
        return self._record_result(row["intent_id"], state)

    def submit(
        self, spec: OrderSpec, *, idempotency_key: str, live_invocation: bool = False
    ) -> OrderState:
        self._assert_write(live_invocation=live_invocation, submission=True)
        quote = self.broker.quote(spec.contract) if spec.order_type is OrderType.MKT else None
        estimated_notional = self.validate_order(spec, quote=quote)
        payload = self._payload(spec) | {"estimated_notional": estimated_notional}
        return self._perform_mutation(
            key=idempotency_key,
            operation="submit",
            payload=payload,
            action=lambda row: self.broker.submit(spec, order_ref=row["order_ref"]),
            contract_id=spec.contract.con_id,
        )

    def modify(
        self,
        order_id: int,
        spec: OrderSpec,
        *,
        idempotency_key: str,
        live_invocation: bool = False,
    ) -> OrderState:
        self._assert_write(live_invocation=live_invocation, submission=False)
        existing_row = self.store.get_by_order_id(order_id)
        if existing_row is None:
            raise GuardError("order is not app-owned")
        existing = _state_from_row(existing_row)
        if existing.status not in {"pending_submission", "working", "partially_filled"}:
            raise ValidationError(
                f"order in state {existing.status!r} is not eligible for modification"
            )
        if spec.quantity < existing.filled:
            raise ValidationError("new total quantity cannot be below already-filled quantity")
        original = json.loads(existing_row["payload_json"])
        immutable = {
            "con_id": spec.contract.con_id,
            "side": spec.side.value,
            "order_type": spec.order_type.value,
            "time_in_force": spec.time_in_force.value,
            "outside_regular_hours": spec.outside_regular_hours,
        }
        changed = [name for name, value in immutable.items() if original.get(name) != value]
        if changed:
            raise ValidationError(
                "modify permits only limit price and total quantity; changed immutable fields: "
                + ", ".join(changed)
            )
        quote = self.broker.quote(spec.contract) if spec.order_type is OrderType.MKT else None
        self.validate_order(
            spec,
            quote=quote,
            pending_sell_credit=existing.remaining or Decimal("0"),
        )
        payload = self._payload(spec) | {"target_order_id": order_id}
        return self._perform_mutation(
            key=idempotency_key,
            operation="modify",
            payload=payload,
            action=lambda _: self.broker.modify(existing, spec),
            contract_id=spec.contract.con_id,
        )

    def cancel(
        self,
        order_id: int,
        *,
        idempotency_key: str,
        live_invocation: bool = False,
    ) -> OrderState:
        self._assert_write(live_invocation=live_invocation, submission=False)
        existing_row = self.store.get_by_order_id(order_id)
        if existing_row is None:
            raise GuardError("order is not app-owned")
        existing = _state_from_row(existing_row)
        if existing.status not in {"pending_submission", "working", "partially_filled"}:
            raise ValidationError(
                f"order in state {existing.status!r} is not eligible for cancellation"
            )
        payload = {"target_order_id": order_id, "target_intent_id": existing.intent_id}
        return self._perform_mutation(
            key=idempotency_key,
            operation="cancel",
            payload=payload,
            action=lambda _: self.broker.cancel(existing),
        )

    def reconcile(self) -> list[dict[str, Any]]:
        self.broker.synchronize()
        open_by_ref = {item.order_ref: item for item in self.broker.open_orders()}
        completed_by_ref = {item.order_ref: item for item in self.broker.completed_orders()}
        for execution in self.broker.executions():
            self.store.add_execution(execution)
        results: list[dict[str, Any]] = []
        for row in self.store.list_intents(unresolved_only=True):
            state = open_by_ref.get(row["order_ref"]) or completed_by_ref.get(row["order_ref"])
            if state is not None:
                self._record_result(row["intent_id"], state)
                results.append({"intent_id": row["intent_id"], "outcome": state.status})
            else:
                results.append(
                    {
                        "intent_id": row["intent_id"],
                        "outcome": "unresolved",
                        "message": "not found in available TWS order/execution scope",
                    }
                )
        return results
