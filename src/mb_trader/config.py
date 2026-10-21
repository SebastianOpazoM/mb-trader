from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .errors import ConfigurationError


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    if raw.lower() in {"1", "true", "yes", "on"}:
        return True
    if raw.lower() in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc


def _decimal(name: str, default: str) -> Decimal:
    try:
        value = Decimal(os.getenv(name, default))
    except InvalidOperation as exc:
        raise ConfigurationError(f"{name} must be a decimal") from exc
    if not value.is_finite() or value <= 0:
        raise ConfigurationError(f"{name} must be finite and positive")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    host: str
    port: int
    client_id: int
    account: str
    environment: str
    operation_mode: str
    request_timeout_seconds: float
    reconnect_attempts: int
    reconnect_base_delay_seconds: float
    database_path: Path
    disable_new_orders: bool
    max_order_quantity: Decimal
    max_order_notional: Decimal
    max_quote_age_seconds: float
    allow_delayed_market_data: bool
    allow_regulatory_snapshot: bool
    enable_live_execution: bool
    live_account_allowlist: frozenset[str]

    @classmethod
    def from_env(cls, env_file: Path | None = Path(".env")) -> Settings:
        if env_file is not None:
            _load_dotenv(env_file)
        account = os.getenv("MBT_ACCOUNT", "").strip()
        environment = os.getenv("MBT_ENVIRONMENT", "paper").lower()
        mode = os.getenv("MBT_OPERATION_MODE", "read_only").lower()
        client_id = _int("MBT_CLIENT_ID", 17)
        port = _int("MBT_PORT", 4002)
        if not account:
            raise ConfigurationError("MBT_ACCOUNT is required")
        if environment not in {"paper", "live"}:
            raise ConfigurationError("MBT_ENVIRONMENT must be paper or live")
        if mode not in {"read_only", "paper_write", "live_write"}:
            raise ConfigurationError(
                "MBT_OPERATION_MODE must be read_only, paper_write, or live_write"
            )
        if client_id <= 0:
            raise ConfigurationError("MBT_CLIENT_ID must be a stable nonzero integer")
        if not 1 <= port <= 65535:
            raise ConfigurationError("MBT_PORT must be between 1 and 65535")
        timeout = float(os.getenv("MBT_REQUEST_TIMEOUT_SECONDS", "15"))
        quote_age = float(os.getenv("MBT_MAX_QUOTE_AGE_SECONDS", "15"))
        reconnect_delay = float(os.getenv("MBT_RECONNECT_BASE_DELAY_SECONDS", "1"))
        if min(timeout, quote_age, reconnect_delay) <= 0:
            raise ConfigurationError("timeouts and reconnect delay must be positive")
        allowlist = frozenset(
            item.strip()
            for item in os.getenv("MBT_LIVE_ACCOUNT_ALLOWLIST", "").split(",")
            if item.strip()
        )
        return cls(
            host=os.getenv("MBT_HOST", "127.0.0.1"),
            port=port,
            client_id=client_id,
            account=account,
            environment=environment,
            operation_mode=mode,
            request_timeout_seconds=timeout,
            reconnect_attempts=_int("MBT_RECONNECT_ATTEMPTS", 5),
            reconnect_base_delay_seconds=reconnect_delay,
            database_path=Path(os.getenv("MBT_DATABASE_PATH", "./var/mb_trader.db")),
            disable_new_orders=_bool("MBT_DISABLE_NEW_ORDERS", True),
            max_order_quantity=_decimal("MBT_MAX_ORDER_QUANTITY", "100"),
            max_order_notional=_decimal("MBT_MAX_ORDER_NOTIONAL", "10000"),
            max_quote_age_seconds=quote_age,
            allow_delayed_market_data=_bool("MBT_ALLOW_DELAYED_MARKET_DATA", False),
            allow_regulatory_snapshot=_bool("MBT_ALLOW_REGULATORY_SNAPSHOT", False),
            enable_live_execution=_bool("MBT_ENABLE_LIVE_EXECUTION", False),
            live_account_allowlist=allowlist,
        )

    def assert_account_environment(self, managed_accounts: list[str]) -> None:
        if self.account not in managed_accounts:
            raise ConfigurationError(
                "configured account is not managed by this session; returned "
                f"{len(managed_accounts)} account(s)"
            )
        expected_mode = "paper_write" if self.environment == "paper" else "live_write"
        if self.operation_mode not in {"read_only", expected_mode}:
            raise ConfigurationError(
                f"operation mode {self.operation_mode!r} conflicts with "
                f"{self.environment!r} environment"
            )
