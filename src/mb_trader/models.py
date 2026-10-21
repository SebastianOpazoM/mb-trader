from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum


def utc_now() -> datetime:
    return datetime.now(UTC)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LMT = "LMT"
    MKT = "MKT"


class TimeInForce(str, Enum):
    DAY = "DAY"
    GTC = "GTC"


class QuoteClassification(str, Enum):
    REALTIME = "realtime"
    DELAYED = "delayed"
    FROZEN = "frozen"
    DELAYED_FROZEN = "delayed_frozen"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class Contract:
    con_id: int
    symbol: str
    security_type: str
    currency: str
    exchange: str
    primary_exchange: str | None = None
    local_symbol: str | None = None
    min_tick: Decimal | None = None
    size_increment: Decimal | None = None
    market_rule_ids: tuple[int, ...] = ()
    price_increments: tuple[tuple[Decimal, Decimal], ...] = ()


@dataclass(frozen=True, slots=True)
class Quote:
    contract: Contract
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    received_at: datetime
    classification: QuoteClassification
    source_timestamp: datetime | None = None
    warnings: tuple[str, ...] = ()

    def price_reference(self, side: Side) -> Decimal | None:
        return (self.ask if side is Side.BUY else self.bid) or self.last


@dataclass(frozen=True, slots=True)
class Position:
    account: str
    contract: Contract
    quantity: Decimal
    average_cost: Decimal | None
    market_price: Decimal | None = None
    market_value: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    received_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class AccountValue:
    account: str
    tag: str
    value: Decimal | str | None
    currency: str | None
    received_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class OrderSpec:
    contract: Contract
    side: Side
    quantity: Decimal
    order_type: OrderType
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Decimal | None = None
    outside_regular_hours: bool = False


@dataclass(frozen=True, slots=True)
class Preview:
    accepted: bool
    initial_margin_change: Decimal | None = None
    maintenance_margin_change: Decimal | None = None
    equity_with_loan_change: Decimal | None = None
    commission: Decimal | None = None
    commission_currency: str | None = None
    warnings: tuple[str, ...] = ()
    raw_status: str | None = None


@dataclass(frozen=True, slots=True)
class OrderState:
    intent_id: str
    order_id: int | None
    perm_id: int | None
    order_ref: str
    broker_status: str
    status: str
    filled: Decimal
    remaining: Decimal | None
    average_fill_price: Decimal | None
    updated_at: datetime
    uncertain: bool = False
    message: str | None = None


@dataclass(frozen=True, slots=True)
class Execution:
    exec_id: str
    order_id: int
    perm_id: int | None
    account: str
    contract: Contract
    side: str
    shares: Decimal
    price: Decimal
    occurred_at: datetime
    commission: Decimal | None = None
    commission_currency: str | None = None


@dataclass(frozen=True, slots=True)
class Health:
    socket_connected: bool
    handshake_ready: bool
    broker_server_connected: bool
    synchronized: bool
    last_contact: datetime | None
    managed_accounts: tuple[str, ...]
    message: str | None = None
