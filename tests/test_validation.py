from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from mb_trader.errors import AmbiguousContract, GuardError, ValidationError
from mb_trader.models import OrderSpec, OrderType, Quote, QuoteClassification, Side
from mb_trader.persistence import Store
from mb_trader.service import TradingService

from .fakes import CONTRACT, FakeBroker


def test_ambiguous_contract_is_never_selected_implicitly(settings) -> None:
    broker = FakeBroker()
    broker.contracts.append(replace(CONTRACT, con_id=2, primary_exchange="NYSE"))
    service = TradingService(settings, broker, Store(settings.database_path))
    with pytest.raises(AmbiguousContract):
        service.resolve_contract("AAPL")


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (
            OrderSpec(CONTRACT, Side.BUY, Decimal("0"), OrderType.LMT, limit_price=Decimal("1")),
            "quantity",
        ),
        (
            OrderSpec(CONTRACT, Side.BUY, Decimal("1.5"), OrderType.LMT, limit_price=Decimal("1")),
            "fractional",
        ),
        (OrderSpec(CONTRACT, Side.BUY, Decimal("1"), OrderType.LMT), "limit price"),
        (
            OrderSpec(
                CONTRACT, Side.BUY, Decimal("1"), OrderType.LMT, limit_price=Decimal("1.001")
            ),
            "minimum tick",
        ),
    ],
)
def test_invalid_orders_fail_closed(settings, spec, message) -> None:
    service = TradingService(settings, FakeBroker(), Store(settings.database_path))
    with pytest.raises(ValidationError, match=message):
        service.validate_order(spec)


def test_market_order_requires_fresh_acceptable_quote(settings) -> None:
    service = TradingService(settings, FakeBroker(), Store(settings.database_path))
    spec = OrderSpec(CONTRACT, Side.BUY, Decimal("1"), OrderType.MKT)
    stale = Quote(
        CONTRACT,
        Decimal("99"),
        Decimal("100"),
        None,
        datetime.now(UTC) - timedelta(minutes=1),
        QuoteClassification.REALTIME,
    )
    with pytest.raises(ValidationError, match="stale"):
        service.validate_order(spec, quote=stale)


def test_sell_accounts_for_pending_app_sell(settings) -> None:
    broker = FakeBroker(position=Decimal("10"))
    store = Store(settings.database_path)
    service = TradingService(settings, broker, store)
    first = OrderSpec(CONTRACT, Side.SELL, Decimal("7"), OrderType.LMT, limit_price=Decimal("100"))
    service.submit(first, idempotency_key="sell-1")
    second = replace(first, quantity=Decimal("4"))
    with pytest.raises(ValidationError, match="available holdings"):
        service.validate_order(second)


def test_read_only_and_disable_new_order_guards(readonly_settings, settings) -> None:
    spec = OrderSpec(CONTRACT, Side.BUY, Decimal("1"), OrderType.LMT, limit_price=Decimal("100"))
    with pytest.raises(GuardError, match="read_only"):
        TradingService(
            readonly_settings, FakeBroker(), Store(readonly_settings.database_path)
        ).submit(spec, idempotency_key="x")
    disabled = replace(settings, disable_new_orders=True)
    with pytest.raises(GuardError, match="disabled"):
        TradingService(disabled, FakeBroker(), Store(disabled.database_path)).submit(
            spec, idempotency_key="x"
        )


def test_live_requires_all_three_opt_ins(settings) -> None:
    live = replace(
        settings,
        environment="live",
        operation_mode="live_write",
        enable_live_execution=True,
        live_account_allowlist=frozenset({settings.account}),
    )
    spec = OrderSpec(CONTRACT, Side.BUY, Decimal("1"), OrderType.LMT, limit_price=Decimal("100"))
    service = TradingService(live, FakeBroker(), Store(live.database_path))
    with pytest.raises(GuardError, match="--live"):
        service.submit(spec, idempotency_key="live-1")


def test_mutation_requires_fully_synchronized_broker(settings) -> None:
    broker = FakeBroker()
    broker.ready = False
    spec = OrderSpec(
        CONTRACT,
        Side.BUY,
        Decimal("1"),
        OrderType.LMT,
        limit_price=Decimal("100"),
    )
    with pytest.raises(GuardError, match="synchronized"):
        TradingService(settings, broker, Store(settings.database_path)).submit(
            spec, idempotency_key="not-ready"
        )


def test_tiered_market_rule_tick_is_enforced(settings) -> None:
    contract = replace(
        CONTRACT,
        min_tick=Decimal("0.01"),
        price_increments=((Decimal("0"), Decimal("0.01")), (Decimal("100"), Decimal("0.05"))),
    )
    spec = OrderSpec(
        contract,
        Side.BUY,
        Decimal("1"),
        OrderType.LMT,
        limit_price=Decimal("100.01"),
    )
    with pytest.raises(ValidationError, match="0.05"):
        TradingService(settings, FakeBroker(), Store(settings.database_path)).validate_order(spec)
