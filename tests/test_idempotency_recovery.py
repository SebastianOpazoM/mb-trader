from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from mb_trader.errors import IdempotencyConflict, UncertainOperation, ValidationError
from mb_trader.models import Execution, OrderSpec, OrderState, OrderType, Side
from mb_trader.persistence import Store
from mb_trader.service import TradingService

from .fakes import CONTRACT, FakeBroker


def _spec(quantity: str = "2") -> OrderSpec:
    return OrderSpec(
        CONTRACT, Side.BUY, Decimal(quantity), OrderType.LMT, limit_price=Decimal("100")
    )


def test_concurrent_same_idempotency_key_sends_once(settings) -> None:
    broker = FakeBroker()
    service = TradingService(settings, broker, Store(settings.database_path))
    with ThreadPoolExecutor(max_workers=8) as pool:
        states = list(pool.map(lambda _: service.submit(_spec(), idempotency_key="same"), range(8)))
    assert broker.submit_count == 1
    assert len({item.intent_id for item in states}) == 1


def test_same_key_different_payload_conflicts(settings) -> None:
    service = TradingService(settings, FakeBroker(), Store(settings.database_path))
    service.submit(_spec("1"), idempotency_key="key")
    with pytest.raises(IdempotencyConflict):
        service.submit(_spec("2"), idempotency_key="key")


def test_duplicate_execution_is_deduplicated(settings) -> None:
    store = Store(settings.database_path)
    execution = Execution(
        "exec-1",
        1,
        2,
        settings.account,
        CONTRACT,
        "BOT",
        Decimal("1"),
        Decimal("100"),
        datetime.now(UTC),
    )
    assert store.add_execution(execution)
    assert not store.add_execution(execution)


def test_timeout_after_send_is_uncertain_and_not_resent_after_restart(settings) -> None:
    broker = FakeBroker()
    broker.submit_error = TimeoutError("lost acknowledgement")
    store = Store(settings.database_path)
    service = TradingService(settings, broker, store)
    with pytest.raises(UncertainOperation):
        service.submit(_spec(), idempotency_key="uncertain")
    restarted_broker = FakeBroker()
    restarted = TradingService(settings, restarted_broker, Store(settings.database_path))
    state = restarted.submit(_spec(), idempotency_key="uncertain")
    assert state.uncertain
    assert restarted_broker.submit_count == 0


def test_modify_cannot_reduce_below_partial_fill(settings) -> None:
    broker = FakeBroker()
    store = Store(settings.database_path)
    service = TradingService(settings, broker, store)
    original = service.submit(_spec("5"), idempotency_key="original")
    row = store.get_intent(original.intent_id)
    partial = replace(
        original, status="partially_filled", filled=Decimal("3"), remaining=Decimal("2")
    )
    service._record_result(row["intent_id"], partial)
    with pytest.raises(ValidationError, match="already-filled"):
        service.modify(original.order_id, _spec("2"), idempotency_key="modify")


def test_cancel_fill_race_records_fill_not_cancelled(settings) -> None:
    broker = FakeBroker()
    service = TradingService(settings, broker, Store(settings.database_path))
    original = service.submit(_spec("2"), idempotency_key="original")
    broker.cancel_result = OrderState(
        "",
        original.order_id,
        original.perm_id,
        original.order_ref,
        "Filled",
        "filled",
        Decimal("2"),
        Decimal("0"),
        Decimal("100"),
        datetime.now(UTC),
    )
    result = service.cancel(original.order_id, idempotency_key="cancel")
    assert result.status == "filled"


def test_reconcile_resynchronizes_before_resolving(settings) -> None:
    broker = FakeBroker()
    broker.submit_error = TimeoutError("lost")
    service = TradingService(settings, broker, Store(settings.database_path))
    with pytest.raises(UncertainOperation):
        service.submit(_spec(), idempotency_key="uncertain")
    result = service.reconcile()
    assert broker.sync_count == 1
    assert result[0]["outcome"] == "unresolved"
