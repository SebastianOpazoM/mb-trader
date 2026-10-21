from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from mb_trader.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        host="127.0.0.1",
        port=4002,
        client_id=17,
        account="DU123",
        environment="paper",
        operation_mode="paper_write",
        request_timeout_seconds=1,
        reconnect_attempts=2,
        reconnect_base_delay_seconds=0.01,
        database_path=tmp_path / "test.db",
        disable_new_orders=False,
        max_order_quantity=Decimal("100"),
        max_order_notional=Decimal("10000"),
        max_quote_age_seconds=15,
        allow_delayed_market_data=False,
        allow_regulatory_snapshot=False,
        enable_live_execution=False,
        live_account_allowlist=frozenset(),
    )


@pytest.fixture
def readonly_settings(settings: Settings) -> Settings:
    return replace(settings, operation_mode="read_only")
