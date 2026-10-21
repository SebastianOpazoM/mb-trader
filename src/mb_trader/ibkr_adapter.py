from __future__ import annotations

import logging
import queue
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .config import Settings
from .errors import BrokerError, BrokerTimeout
from .models import (
    AccountValue,
    Contract,
    Execution,
    Health,
    OrderSpec,
    OrderState,
    Position,
    Preview,
    Quote,
    QuoteClassification,
    utc_now,
)

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract as IBContract
    from ibapi.execution import ExecutionFilter
    from ibapi.order import Order as IBOrder
    from ibapi.wrapper import EWrapper

    IBAPI_AVAILABLE = True
except ImportError:
    IBAPI_AVAILABLE = False

    class EWrapper:  # type: ignore[no-redef]
        pass

    class EClient:  # type: ignore[no-redef]
        pass

    IBContract = IBOrder = ExecutionFilter = object  # type: ignore[misc,assignment]


LOG = logging.getLogger(__name__)
_INFO_CODES = {2103, 2104, 2105, 2106, 2107, 2108, 2158}


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "" or str(value).lower() in {"nan", "inf", "-inf"}:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _contract(value: Any, details: Any | None = None) -> Contract:
    market_ids: tuple[int, ...] = ()
    min_tick = None
    if details is not None:
        min_tick = _decimal(getattr(details, "minTick", None))
        raw_ids = getattr(details, "marketRuleIds", "") or ""
        market_ids = tuple(int(item) for item in raw_ids.split(",") if item.strip().isdigit())
    return Contract(
        con_id=int(getattr(value, "conId", 0)),
        symbol=str(getattr(value, "symbol", "")),
        security_type=str(getattr(value, "secType", "")),
        currency=str(getattr(value, "currency", "")),
        exchange=str(getattr(value, "exchange", "")),
        primary_exchange=getattr(value, "primaryExchange", None) or None,
        local_symbol=getattr(value, "localSymbol", None) or None,
        min_tick=min_tick,
        size_increment=_decimal(getattr(details, "sizeIncrement", None)),
        market_rule_ids=market_ids,
    )


def _ib_contract(contract: Contract) -> Any:
    value = IBContract()
    value.conId = contract.con_id
    value.symbol = contract.symbol
    value.secType = contract.security_type
    value.currency = contract.currency
    value.exchange = contract.exchange
    if contract.primary_exchange:
        value.primaryExchange = contract.primary_exchange
    return value


def _ib_order(spec: OrderSpec, account: str, order_ref: str, *, what_if: bool = False) -> Any:
    order = IBOrder()
    order.account = account
    order.action = spec.side.value
    order.orderType = spec.order_type.value
    order.totalQuantity = float(spec.quantity)
    order.tif = spec.time_in_force.value
    order.outsideRth = spec.outside_regular_hours
    order.orderRef = order_ref
    order.transmit = True
    order.whatIf = what_if
    if spec.limit_price is not None:
        order.lmtPrice = float(spec.limit_price)
    return order


def _internal_status(broker_status: str, filled: Decimal, remaining: Decimal | None) -> str:
    normalized = broker_status.lower().replace(" ", "")
    if normalized in {"pendingcancel"}:
        return "cancel_requested"
    if normalized in {"cancelled", "apicancelled"}:
        return "cancelled"
    if normalized in {"inactive"}:
        return "rejected"
    if remaining == 0 or normalized == "filled":
        return "filled"
    if filled > 0:
        return "partially_filled"
    if normalized in {"presubmitted", "submitted"}:
        return "working"
    return "pending_submission"


@dataclass
class _Pending:
    event: threading.Event = field(default_factory=threading.Event)
    values: list[Any] = field(default_factory=list)
    error: BrokerError | None = None


class IBKRAdapter(EWrapper, EClient):
    """One classic official TWS API session with correlated synchronous service calls."""

    def __init__(self, settings: Settings):
        if not IBAPI_AVAILABLE:
            raise BrokerError(
                "official IBKR Python SDK is not installed; install it from the downloaded "
                "TWS API source with `python -m pip install <TWS API>/source/pythonclient`"
            )
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.settings = settings
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._managed = threading.Event()
        self._managed_accounts: list[str] = []
        self._request_lock = threading.Lock()
        self._next_request_id = 1
        self._order_lock = threading.Lock()
        self._next_order_id: int | None = None
        self._pending: dict[tuple[str, int], _Pending] = {}
        self._pending_lock = threading.Lock()
        self._last_contact: datetime | None = None
        self._upstream = False
        self._synchronized = False
        self._order_states: dict[int, OrderState] = {}
        self._order_contracts: dict[int, Contract] = {}
        self._order_specs: dict[int, Any] = {}
        self._open_order_ids: set[int] = set()
        self._completed: list[OrderState] = []
        self._executions: dict[str, Execution] = {}
        self._commissions: dict[str, tuple[Decimal | None, str | None]] = {}
        self._quote_ticks: dict[int, dict[str, Any]] = defaultdict(dict)
        self._market_data_type: dict[int, int] = {}
        self._market_rules: dict[int, tuple[tuple[Decimal, Decimal], ...]] = {}
        self._event_handler: Callable[[str, Any], None] | None = None
        self._event_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._dispatcher = threading.Thread(
            target=self._dispatch_events,
            name="ibkr-event-dispatcher",
            daemon=True,
        )
        self._dispatcher.start()

    def set_event_handler(self, handler: Callable[[str, Any], None]) -> None:
        self._event_handler = handler

    def _emit(self, event_type: str, value: Any) -> None:
        self._event_queue.put((event_type, value))

    def _dispatch_events(self) -> None:
        while True:
            event_type, value = self._event_queue.get()
            try:
                handler = self._event_handler
                if handler is not None:
                    handler(event_type, value)
            except Exception:
                LOG.exception("failed to persist asynchronous IBKR %s event", event_type)
            finally:
                self._event_queue.task_done()

    def _contact(self) -> None:
        self._last_contact = utc_now()

    def _new_request(self, kind: str) -> tuple[int, _Pending]:
        with self._request_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
        pending = _Pending()
        with self._pending_lock:
            self._pending[(kind, request_id)] = pending
        return request_id, pending

    def _get_pending(self, kind: str, request_id: int) -> _Pending | None:
        with self._pending_lock:
            return self._pending.get((kind, request_id))

    def _finish(self, kind: str, request_id: int) -> None:
        pending = self._get_pending(kind, request_id)
        if pending:
            pending.event.set()

    def _wait(self, kind: str, request_id: int, pending: _Pending) -> list[Any]:
        try:
            if not pending.event.wait(self.settings.request_timeout_seconds):
                raise BrokerTimeout(f"IBKR {kind} request {request_id} timed out")
            if pending.error:
                raise pending.error
            return list(pending.values)
        finally:
            with self._pending_lock:
                self._pending.pop((kind, request_id), None)

    def _allocate_order_id(self) -> int:
        with self._order_lock:
            if self._next_order_id is None:
                raise BrokerError("nextValidId has not been received")
            value = self._next_order_id
            self._next_order_id += 1
            return value

    def connect(self) -> Health:
        if self.isConnected():
            return self.health()
        self._ready.clear()
        self._managed.clear()
        self._managed_accounts.clear()
        if not EClient.connect(
            self, self.settings.host, self.settings.port, self.settings.client_id
        ):
            raise BrokerError("failed to open the local TWS/Gateway socket")
        self._thread = threading.Thread(target=self.run, name="ibkr-reader", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + self.settings.request_timeout_seconds
        for event, label in ((self._ready, "nextValidId"), (self._managed, "managedAccounts")):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not event.wait(remaining):
                self.disconnect()
                raise BrokerTimeout(f"connection handshake did not receive {label}")
        self.settings.assert_account_environment(self._managed_accounts)
        return self.health()

    def disconnect(self) -> None:
        self._synchronized = False
        if self.isConnected():
            EClient.disconnect(self)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._event_queue.join()

    def health(self) -> Health:
        return Health(
            socket_connected=bool(self.isConnected()),
            handshake_ready=self._ready.is_set() and self._managed.is_set(),
            broker_server_connected=self._upstream,
            synchronized=self._synchronized,
            last_contact=self._last_contact,
            managed_accounts=tuple(self._managed_accounts),
            message=None if self._upstream else "upstream connectivity is not confirmed",
        )

    def synchronize(self) -> Health:
        self.account_summary()
        self.positions()
        self.open_orders()
        self.executions()
        self._upstream = True
        self._synchronized = True
        return self.health()

    def account_summary(self) -> list[AccountValue]:
        request_id, pending = self._new_request(
            "account",
        )
        tags = ",".join(
            (
                "NetLiquidation",
                "TotalCashValue",
                "AvailableFunds",
                "BuyingPower",
                "EquityWithLoanValue",
                "InitMarginReq",
                "MaintMarginReq",
                "ExcessLiquidity",
            )
        )
        self.reqAccountSummary(request_id, "All", tags)
        try:
            return self._wait("account", request_id, pending)
        finally:
            self.cancelAccountSummary(request_id)

    def positions(self) -> list[Position]:
        request_id, pending = self._new_request("positions")
        self.reqPositions()
        try:
            return self._wait("positions", request_id, pending)
        finally:
            self.cancelPositions()

    def search_contracts(
        self, symbol: str, *, security_type: str = "STK", currency: str = "USD"
    ) -> list[Contract]:
        request_id, pending = self._new_request("contract")
        query = IBContract()
        query.symbol = symbol
        query.secType = security_type
        query.exchange = "SMART"
        query.currency = currency
        self.reqContractDetails(request_id, query)
        contracts = self._wait("contract", request_id, pending)
        qualified: list[Contract] = []
        for contract in contracts:
            increments: list[tuple[Decimal, Decimal]] = []
            for rule_id in contract.market_rule_ids:
                increments.extend(self._get_market_rule(rule_id))
            qualified.append(
                replace(
                    contract,
                    price_increments=tuple(sorted(set(increments))),
                )
            )
        return qualified

    def _get_market_rule(self, rule_id: int) -> tuple[tuple[Decimal, Decimal], ...]:
        cached = self._market_rules.get(rule_id)
        if cached is not None:
            return cached
        pending = _Pending()
        with self._pending_lock:
            self._pending[("market_rule", rule_id)] = pending
        self.reqMarketRule(rule_id)
        values = self._wait("market_rule", rule_id, pending)
        result = tuple(values)
        self._market_rules[rule_id] = result
        return result

    def quote(self, contract: Contract) -> Quote:
        request_id, pending = self._new_request("quote")
        self.reqMktData(
            request_id,
            _ib_contract(contract),
            "",
            True,
            self.settings.allow_regulatory_snapshot,
            [],
        )
        try:
            self._wait("quote", request_id, pending)
        finally:
            self.cancelMktData(request_id)
        ticks = self._quote_ticks.pop(request_id, {})
        data_type = self._market_data_type.pop(request_id, 0)
        classification = {
            1: QuoteClassification.REALTIME,
            2: QuoteClassification.FROZEN,
            3: QuoteClassification.DELAYED,
            4: QuoteClassification.DELAYED_FROZEN,
        }.get(data_type, QuoteClassification.UNAVAILABLE)
        warning = () if ticks else ("no entitled or available snapshot price was returned",)
        return Quote(
            contract=contract,
            bid=_decimal(ticks.get("bid")),
            ask=_decimal(ticks.get("ask")),
            last=_decimal(ticks.get("last")),
            received_at=utc_now(),
            classification=classification,
            source_timestamp=ticks.get("timestamp"),
            warnings=warning,
        )

    def preview(self, spec: OrderSpec, *, order_ref: str) -> Preview:
        order_id = self._allocate_order_id()
        pending = _Pending()
        with self._pending_lock:
            self._pending[("whatif", order_id)] = pending
        self.placeOrder(
            order_id,
            _ib_contract(spec.contract),
            _ib_order(spec, self.settings.account, order_ref, what_if=True),
        )
        values = self._wait("whatif", order_id, pending)
        if not values:
            raise BrokerError("what-if returned no estimate")
        state = values[-1]
        warning_text = str(getattr(state, "warningText", "") or "")
        return Preview(
            accepted=str(getattr(state, "status", "")).lower() != "inactive",
            initial_margin_change=_decimal(getattr(state, "initMarginChange", None)),
            maintenance_margin_change=_decimal(getattr(state, "maintMarginChange", None)),
            equity_with_loan_change=_decimal(getattr(state, "equityWithLoanChange", None)),
            commission=_decimal(getattr(state, "commission", None)),
            commission_currency=getattr(state, "commissionCurrency", None) or None,
            warnings=(warning_text,) if warning_text else (),
            raw_status=getattr(state, "status", None),
        )

    def submit(self, spec: OrderSpec, *, order_ref: str) -> OrderState:
        order_id = self._allocate_order_id()
        pending = _Pending()
        with self._pending_lock:
            self._pending[("order", order_id)] = pending
        self._order_contracts[order_id] = spec.contract
        self.placeOrder(
            order_id,
            _ib_contract(spec.contract),
            _ib_order(spec, self.settings.account, order_ref),
        )
        values = self._wait("order", order_id, pending)
        return values[-1] if values else self._order_states[order_id]

    def modify(self, state: OrderState, spec: OrderSpec) -> OrderState:
        if state.order_id is None:
            raise BrokerError("cannot modify an order without orderId")
        pending = _Pending()
        with self._pending_lock:
            self._pending[("order", state.order_id)] = pending
        self.placeOrder(
            state.order_id,
            _ib_contract(spec.contract),
            _ib_order(spec, self.settings.account, state.order_ref),
        )
        values = self._wait("order", state.order_id, pending)
        return values[-1] if values else self._order_states[state.order_id]

    def cancel(self, state: OrderState) -> OrderState:
        if state.order_id is None:
            raise BrokerError("cannot cancel an order without orderId")
        pending = _Pending()
        with self._pending_lock:
            self._pending[("cancel", state.order_id)] = pending
        self.cancelOrder(state.order_id, "")
        values = self._wait("cancel", state.order_id, pending)
        return values[-1]

    def open_orders(self) -> list[OrderState]:
        request_id, pending = self._new_request("open_orders")
        self._open_order_ids.clear()
        self.reqOpenOrders()
        self._wait("open_orders", request_id, pending)
        return [
            self._order_states[item] for item in self._open_order_ids if item in self._order_states
        ]

    def completed_orders(self) -> list[OrderState]:
        request_id, pending = self._new_request("completed_orders")
        self._completed.clear()
        self.reqCompletedOrders(False)
        self._wait("completed_orders", request_id, pending)
        return list(self._completed)

    def executions(self) -> list[Execution]:
        request_id, pending = self._new_request("executions")
        query = ExecutionFilter()
        query.acctCode = self.settings.account
        query.clientId = self.settings.client_id
        self.reqExecutions(request_id, query)
        self._wait("executions", request_id, pending)
        return list(self._executions.values())

    # Callbacks below only update memory and signal; they never touch SQLite or wait.
    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        self._contact()
        with self._order_lock:
            if self._next_order_id is None or orderId > self._next_order_id:
                self._next_order_id = orderId
        self._ready.set()

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
        self._contact()
        self._managed_accounts = [item for item in accountsList.split(",") if item]
        self._managed.set()

    def accountSummary(  # noqa: N802
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ) -> None:
        self._contact()
        pending = self._get_pending("account", reqId)
        if pending:
            parsed: Decimal | str | None = _decimal(value)
            if parsed is None and value:
                parsed = value
            pending.values.append(AccountValue(account, tag, parsed, currency or None))

    def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
        self._contact()
        self._finish("account", reqId)

    def position(self, account: str, contract: Any, position: Any, avgCost: float) -> None:
        self._contact()
        with self._pending_lock:
            candidates = [p for (kind, _), p in self._pending.items() if kind == "positions"]
        if candidates:
            candidates[-1].values.append(
                Position(
                    account=account,
                    contract=_contract(contract),
                    quantity=_decimal(position) or Decimal("0"),
                    average_cost=_decimal(avgCost),
                )
            )

    def positionEnd(self) -> None:  # noqa: N802
        self._contact()
        with self._pending_lock:
            keys = [key for key in self._pending if key[0] == "positions"]
        for _, request_id in keys:
            self._finish("positions", request_id)

    def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
        self._contact()
        pending = self._get_pending("contract", reqId)
        if pending:
            pending.values.append(_contract(contractDetails.contract, contractDetails))

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        self._contact()
        self._finish("contract", reqId)

    def marketRule(self, marketRuleId: int, priceIncrements: list[Any]) -> None:  # noqa: N802
        self._contact()
        pending = self._get_pending("market_rule", marketRuleId)
        if pending:
            for item in priceIncrements:
                low_edge = _decimal(getattr(item, "lowEdge", None))
                increment = _decimal(getattr(item, "increment", None))
                if low_edge is not None and increment is not None and increment > 0:
                    pending.values.append((low_edge, increment))
            pending.event.set()

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802
        self._contact()
        key = {1: "bid", 2: "ask", 4: "last", 66: "bid", 67: "ask", 68: "last"}.get(tickType)
        if key and price > 0:
            self._quote_ticks[reqId][key] = price

    def tickString(self, reqId: int, tickType: int, value: str) -> None:  # noqa: N802
        self._contact()
        if tickType in {45, 88} and value.isdigit():
            self._quote_ticks[reqId]["timestamp"] = datetime.fromtimestamp(int(value), UTC)

    def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
        self._contact()
        self._market_data_type[reqId] = marketDataType

    def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
        self._contact()
        self._finish("quote", reqId)

    def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
        self._contact()
        self._order_contracts[orderId] = _contract(contract)
        self._order_specs[orderId] = order
        self._open_order_ids.add(orderId)
        whatif = self._get_pending("whatif", orderId)
        if whatif:
            whatif.values.append(orderState)
            whatif.event.set()
        status = str(getattr(orderState, "status", "") or "PendingSubmit")
        existing = self._order_states.get(orderId)
        state = OrderState(
            intent_id="",
            order_id=orderId,
            perm_id=int(getattr(order, "permId", 0)) or None,
            order_ref=str(getattr(order, "orderRef", "") or ""),
            broker_status=status,
            status=_internal_status(status, Decimal("0"), None),
            filled=existing.filled if existing else Decimal("0"),
            remaining=existing.remaining
            if existing
            else _decimal(getattr(order, "totalQuantity", None)),
            average_fill_price=existing.average_fill_price if existing else None,
            updated_at=utc_now(),
        )
        self._order_states[orderId] = state
        self._emit("order_state", state)

    def openOrderEnd(self) -> None:  # noqa: N802
        self._contact()
        with self._pending_lock:
            keys = [key for key in self._pending if key[0] == "open_orders"]
        for _, request_id in keys:
            self._finish("open_orders", request_id)

    def orderStatus(  # noqa: N802
        self,
        orderId: int,
        status: str,
        filled: Any,
        remaining: Any,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        self._contact()
        filled_decimal = _decimal(filled) or Decimal("0")
        remaining_decimal = _decimal(remaining)
        order = self._order_specs.get(orderId)
        previous = self._order_states.get(orderId)
        state = OrderState(
            intent_id="",
            order_id=orderId,
            perm_id=permId or (previous.perm_id if previous else None),
            order_ref=str(
                getattr(order, "orderRef", "") or (previous.order_ref if previous else "")
            ),
            broker_status=status,
            status=_internal_status(status, filled_decimal, remaining_decimal),
            filled=filled_decimal,
            remaining=remaining_decimal,
            average_fill_price=_decimal(avgFillPrice),
            updated_at=utc_now(),
            message=whyHeld or None,
        )
        self._order_states[orderId] = state
        self._emit("order_state", state)
        for kind in ("order", "cancel"):
            pending = self._get_pending(kind, orderId)
            if pending:
                pending.values.append(state)
                if kind == "order" or state.status in {"cancelled", "filled", "rejected"}:
                    pending.event.set()

    def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
        self._contact()
        raw_time = str(getattr(execution, "time", ""))
        occurred = self._execution_time(raw_time)
        item = Execution(
            exec_id=str(execution.execId),
            order_id=int(execution.orderId),
            perm_id=int(getattr(execution, "permId", 0)) or None,
            account=str(execution.acctNumber),
            contract=_contract(contract),
            side=str(execution.side),
            shares=_decimal(execution.shares) or Decimal("0"),
            price=_decimal(execution.price) or Decimal("0"),
            occurred_at=occurred,
        )
        commission = self._commissions.get(item.exec_id)
        if commission:
            item = replace(item, commission=commission[0], commission_currency=commission[1])
        self._executions[item.exec_id] = item
        self._emit("execution", item)

    @staticmethod
    def _execution_time(raw_time: str) -> datetime:
        pieces = raw_time.strip().split()
        if len(pieces) < 2:
            return utc_now()
        try:
            naive = datetime.strptime(" ".join(pieces[:2]), "%Y%m%d %H:%M:%S")
            zone = ZoneInfo(pieces[2]) if len(pieces) >= 3 else UTC
            return naive.replace(tzinfo=zone).astimezone(UTC)
        except (ValueError, ZoneInfoNotFoundError):
            return utc_now()

    def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        self._contact()
        self._finish("executions", reqId)

    def commissionReport(self, commissionReport: Any) -> None:  # noqa: N802
        self._record_commission(commissionReport)

    def commissionAndFeesReport(self, report: Any) -> None:  # noqa: N802
        self._record_commission(report)

    def _record_commission(self, report: Any) -> None:
        self._contact()
        exec_id = str(report.execId)
        self._commissions[exec_id] = (
            _decimal(getattr(report, "commission", None)),
            getattr(report, "currency", None) or None,
        )
        existing = self._executions.get(exec_id)
        if existing:
            self._executions[exec_id] = replace(
                existing,
                commission=self._commissions[exec_id][0],
                commission_currency=self._commissions[exec_id][1],
            )
            self._emit("execution", self._executions[exec_id])

    def completedOrder(self, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
        self._contact()
        status = str(getattr(orderState, "status", ""))
        filled = _decimal(getattr(order, "filledQuantity", None)) or Decimal("0")
        total = _decimal(getattr(order, "totalQuantity", None))
        remaining = None if total is None else max(total - filled, Decimal("0"))
        self._completed.append(
            OrderState(
                intent_id="",
                order_id=int(getattr(order, "orderId", 0)) or None,
                perm_id=int(getattr(order, "permId", 0)) or None,
                order_ref=str(getattr(order, "orderRef", "") or ""),
                broker_status=status,
                status=_internal_status(status, filled, remaining),
                filled=filled,
                remaining=remaining,
                average_fill_price=None,
                updated_at=utc_now(),
            )
        )

    def completedOrdersEnd(self) -> None:  # noqa: N802
        self._contact()
        with self._pending_lock:
            keys = [key for key in self._pending if key[0] == "completed_orders"]
        for _, request_id in keys:
            self._finish("completed_orders", request_id)

    def error(self, reqId: int, *args: Any) -> None:
        self._contact()
        numeric = [item for item in args if isinstance(item, int)]
        # API 10.40 adds errorTime before errorCode; older releases do not.
        code = numeric[1] if len(numeric) >= 2 else (numeric[0] if numeric else 0)
        strings = [item for item in args if isinstance(item, str)]
        message = strings[0] if strings else "unknown IBKR error"
        if code in _INFO_CODES:
            if code in {2104, 2106, 2158}:
                self._upstream = True
            LOG.info("IBKR info %s: %s", code, message)
            return
        if code == 202:
            # Cancellation confirmation is followed by authoritative orderStatus.
            LOG.info("IBKR cancel acknowledgement for order %s: %s", reqId, message)
            return
        if code == 1100:
            self._upstream = False
            self._synchronized = False
        elif code == 1101:
            self._upstream = True
            self._synchronized = False
        elif code == 1102:
            self._upstream = True
        error = BrokerError(f"IBKR error {code}: {message}")
        with self._pending_lock:
            matches = [
                pending for (kind, ident), pending in self._pending.items() if ident == reqId
            ]
        for pending in matches:
            pending.error = error
            pending.event.set()
        if not matches and code not in {1100, 1101, 1102}:
            LOG.warning("unmatched IBKR error reqId=%s code=%s: %s", reqId, code, message)

    def connectionClosed(self) -> None:  # noqa: N802
        self._ready.clear()
        self._managed.clear()
        self._upstream = False
        self._synchronized = False
        error = BrokerError("IBKR socket connection closed")
        with self._pending_lock:
            pending_values = list(self._pending.values())
        for pending in pending_values:
            pending.error = error
            pending.event.set()
