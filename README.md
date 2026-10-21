# mb-trader

`mb-trader` is a local Python service and JSON CLI for inspecting and operating one explicitly
configured Interactive Brokers account through TWS or IB Gateway. It implements guarded account
reads, stock/ETF contract qualification, optional quote snapshots, offline validation, IBKR
what-if preview, paper order submit/modify/cancel, execution reconciliation, and durable audit
state. It contains no investment logic.

The safe defaults are paper environment, application read-only mode, regular trading hours, and
new orders disabled. Nothing trades on startup, lookup, preview, tests, or examples.

## Status

As of 2026-10-21:

- Implemented and offline-tested: typed configuration/domain models, validation and limits,
  account/environment/write/live gates, transactional idempotency, SQLite intents/events,
  ambiguous-contract rejection, no-short checks including pending app sells, lifecycle mapping,
  execution deduplication, uncertain-outcome handling, reconciliation, JSON CLI, and one-process
  locking.
- Implemented but not broker-verified here: the official classic `EClient`/`EWrapper` adapter for
  account summary, positions, contract details, snapshots, what-if, submit, modify, cancel, open
  and completed orders, executions, commissions, connectivity codes, and synchronization.
- Not installed in this development environment: the proprietary official IBKR Python SDK. The
  repository deliberately does not replace it with PyPI's unofficial `ibapi` distribution.
- Still manual: TWS/IB Gateway installation, login/2FA, API configuration, account selection,
  market-data entitlements, and the explicitly opted-in paper smoke workflow.
- Live mutation is guarded in code but intentionally unverified and not enabled.

## Install

Python 3.11+ is required (developed with Python 3.11.9). Create a virtual environment and install
this project:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Install the **official** SDK separately, following IBKR's documented process:

1. Download the Windows TWS API installer from
   <https://download2.interactivebrokers.com/installers/tws-api/index.html> and accept its
   non-commercial license.
2. Run `TWS API Install {version}.msi`.
3. Install its local Python source into this same virtual environment (adjust the path to the
   actual installation):

```powershell
python -m pip install "C:\TWS API\source\pythonclient"
python -m pip show ibapi
python -c "from ibapi.client import EClient; from ibapi.wrapper import EWrapper; print('official SDK imports OK')"
```

Record the exact `pip show ibapi` version together with the exact TWS/Gateway build used for the
broker verification. The SDK was absent here, so no exact installed SDK or workstation build can
truthfully be recorded yet. The adapter targets current 10.40+ signatures and accommodates the
new error callback timestamp while retaining older error callback handling.

Copy the example configuration and edit the account ID; never put a brokerage password here:

```powershell
Copy-Item .env.example .env
```

Then follow [the operator runbook](docs/RUNBOOK.md).

## CLI

Every command writes one JSON document to stdout. Diagnostics go to stderr and do not print the
configured account ID. Use `--pretty` before the subcommand for formatted JSON.

```powershell
mb-trader --pretty health
mb-trader --pretty accounts
mb-trader --pretty positions
mb-trader --pretty lookup AAPL
mb-trader --pretty quote AAPL --con-id 265598

# Offline validation: may request a quote only for a market order; never sends an order.
mb-trader --pretty validate AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 100.00

# IBKR credit/margin what-if. It is not a fill simulation and never falls through to submission.
mb-trader --pretty preview AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 100.00

# Requires MBT_OPERATION_MODE=paper_write, MBT_DISABLE_NEW_ORDERS=false, and explicit --write.
mb-trader --pretty submit AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 100.00 --idempotency-key demo-submit-001 --write
mb-trader --pretty modify 123 AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 99.50 --idempotency-key demo-modify-001 --write
mb-trader --pretty cancel 123 --idempotency-key demo-cancel-001 --write
mb-trader --pretty orders --unresolved
mb-trader --pretty reconcile
```

A market-order notional check uses a fresh acceptable ask (buy), bid (sell), or last price. It is
an estimate and is not an execution-price cap. Delayed data is rejected unless explicitly allowed.
Regulatory snapshots, which may be separately billed, default to disabled.

## Python use

The CLI is a thin caller of the same service:

```python
from decimal import Decimal
from mb_trader.config import Settings
from mb_trader.ibkr_adapter import IBKRAdapter
from mb_trader.models import OrderSpec, OrderType, Side
from mb_trader.persistence import Store
from mb_trader.service import TradingService

settings = Settings.from_env()
broker = IBKRAdapter(settings)
service = TradingService(settings, broker, Store(settings.database_path))
service.connect()
try:
    contract = service.resolve_contract("AAPL", con_id=265598)
    spec = OrderSpec(contract, Side.BUY, Decimal("1"), OrderType.LMT,
                     limit_price=Decimal("100.00"))
    preview = service.preview(spec)
    # This explicit call is the only mutation here. Configuration guards still apply.
    result = service.submit(spec, idempotency_key="strategy-run-20261021-001")
finally:
    broker.disconnect()
```

## Development

```powershell
python -m pytest
python -m ruff check .
python -m compileall -q src tests
```

Offline tests never import a working broker session or place orders. See
[the architecture note](docs/ARCHITECTURE.md) for boundaries and SDK method mapping.
