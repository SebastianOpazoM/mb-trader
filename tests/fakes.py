from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from mb_trader.models import (
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
)

CONTRACT = Contract(
    con_id=265598,
    symbol="AAPL",
    security_type="STK",
    currency="USD",
    exchange="SMART",
    primary_exchange="NASDAQ",
    min_tick=Decimal("0.01"),
)


class FakeBroker:
    def __init__(self, *, account: str = "DU123", position: Decimal = Decimal("10")):
        self.account = account
        self.position_quantity = position
        self.contracts = [CONTRACT]
        self.submit_count = 0
        self.submit_error: Exception | None = None
        self.cancel_result: OrderState | None = None
        self.states: list[OrderState] = []
        self.execution_items: list[Execution] = []
        self.sync_count = 0
        self.ready = True
        self._lock = threading.Lock()

    def set_event_handler(self, handler: Callable[[str, object], None]) -> None:
        self.event_handler = handler

    def connect(self) -> Health:
        return self.health()

    def disconnect(self) -> None:
        pass

    def health(self) -> Health:
        return Health(
            self.ready,
            self.ready,
            self.ready,
            self.ready,
            datetime.now(UTC),
            (self.account,),
        )

    def synchronize(self) -> Health:
        self.sync_count += 1
        return self.health()

    def account_summary(self) -> list[AccountValue]:
        return [AccountValue(self.account, "NetLiquidation", Decimal("10000"), "USD")]

    def positions(self) -> list[Position]:
        return [Position(self.account, CONTRACT, self.position_quantity, Decimal("100"))]

    def search_contracts(
        self, symbol: str, *, security_type: str = "STK", currency: str = "USD"
    ) -> list[Contract]:
        return [item for item in self.contracts if item.symbol == symbol]

    def quote(self, contract: Contract) -> Quote:
        return Quote(
            contract,
            Decimal("99.90"),
            Decimal("100.10"),
            Decimal("100"),
            datetime.now(UTC),
            QuoteClassification.REALTIME,
        )

    def preview(self, spec: OrderSpec, *, order_ref: str) -> Preview:
        return Preview(True, commission=Decimal("1"), commission_currency="USD")

    def submit(self, spec: OrderSpec, *, order_ref: str) -> OrderState:
        with self._lock:
            self.submit_count += 1
        if self.submit_error:
            raise self.submit_error
        state = OrderState(
            "",
            100 + self.submit_count,
            900 + self.submit_count,
            order_ref,
            "Submitted",
            "working",
            Decimal("0"),
            spec.quantity,
            None,
            datetime.now(UTC),
        )
        self.states.append(state)
        return state

    def modify(self, state: OrderState, spec: OrderSpec) -> OrderState:
        changed = replace(
            state,
            broker_status="Submitted",
            status="partially_filled" if state.filled else "working",
            remaining=spec.quantity - state.filled,
            updated_at=datetime.now(UTC),
        )
        self.states.append(changed)
        return changed

    def cancel(self, state: OrderState) -> OrderState:
        if self.cancel_result:
            return replace(self.cancel_result, order_ref=state.order_ref)
        result = replace(
            state,
            broker_status="Cancelled",
            status="cancelled",
            updated_at=datetime.now(UTC),
        )
        self.states.append(result)
        return result

    def open_orders(self) -> list[OrderState]:
        return [item for item in self.states if item.status in {"working", "partially_filled"}]

    def completed_orders(self) -> list[OrderState]:
        return [item for item in self.states if item.status in {"filled", "cancelled", "rejected"}]

    def executions(self) -> list[Execution]:
        return self.execution_items
