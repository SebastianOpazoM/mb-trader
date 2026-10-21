# Operator runbook

## TWS or IB Gateway

Prefer IB Gateway for ongoing local operation and TWS for development. Install a current Stable
or Latest build and a matching current official TWS API. Log in directly in that application,
including 2FA. This program neither accepts credentials nor uses an API key.

Usual defaults are:

| Application | Paper | Live |
| --- | ---: | ---: |
| IB Gateway | 4002 | 4001 |
| TWS | 7497 | 7496 |

Ports are configurable and are not proof of paper/live status. In TWS, open Global Configuration
→ API → Settings, enable socket clients, verify the exact socket port, and keep access restricted
to trusted local connections. Gateway exposes equivalent API settings. Use the broker's Read-Only
API option for read workflows; disable it only when intentionally testing paper mutations.
Broker read-only and `MBT_OPERATION_MODE=read_only` are separate controls.

The configured account must appear in `managedAccounts`; otherwise startup fails. The application
does not infer environment from an account prefix. Set `MBT_ENVIRONMENT` and the account explicitly,
then confirm the actual session yourself. Paper access requires an eligible, initialized paper
account and appropriate trading permissions.

## First connection

Start TWS/Gateway, finish login and any prompts, then:

```powershell
mb-trader --pretty health
mb-trader --pretty accounts
mb-trader --pretty positions
mb-trader --pretty lookup AAPL
```

Ready means more than an open socket: the API handshake returned `nextValidId`, managed-account
discovery succeeded, upstream requests completed, and account/position/order/execution state was
synchronized. Error 502 usually means the application is stopped, socket clients are disabled,
the port is wrong, a trusted-IP/firewall rule blocks it, or another connection setting differs.

Market data is optional for account reads. A quote can be real-time, delayed, frozen,
delayed-frozen, or unavailable. Missing entitlements must not be described as live data. Leave
`MBT_ALLOW_REGULATORY_SNAPSHOT=false` unless you explicitly accept possible snapshot charges.

## Paper mutation smoke test

First use `read_only` and preview. A what-if is a broker credit/margin estimate, not a fill
simulator or guarantee:

```powershell
mb-trader --pretty preview AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 1.00
```

For a separately requested mutation test, verify paper login and broker trading permissions, set:

```dotenv
MBT_ENVIRONMENT=paper
MBT_OPERATION_MODE=paper_write
MBT_DISABLE_NEW_ORDERS=false
```

Then invoke the explicit paper workflow with a user-chosen contract and price:

```powershell
mb-trader --pretty smoke AAPL --con-id 265598 --side BUY --quantity 1 --order-type LMT --limit-price 1.00 --idempotency-key paper-smoke-001 --write
```

Even a far-away limit order can fill. The workflow does not promise zero execution and does not
require a fill to verify submit/cancel plumbing. Confirm final state in both JSON/SQLite and the
broker UI. Reusing the same key and payload returns its existing result; a different payload is an
error.

## Restart and uncertainty

Only one process may own a configured database/session; a sibling `.lock` file enforces this for
the CLI. On each start, the service waits for handshake and performs account, position, order, and
execution synchronization before mutation. Startup connection attempts use bounded exponential
backoff. Codes 1100/1101 clear upstream/synchronization readiness; 1102 retains data but later
mutation should still be preceded by state checks. Informational farm codes are logged, not treated
as order rejection.

After timeout, socket loss, or a crash near a mutation:

```powershell
mb-trader --pretty orders --unresolved
mb-trader --pretty reconcile
```

Never invent a new idempotency key to retry an uncertain order. Reconciliation examines available
open orders, completed orders, and executions. TWS retrieval scope is limited; if it cannot prove
the outcome, the intent remains unresolved and the affected contract stays blocked. Inspect the
broker UI/activity and resolve operationally before trading that contract again.

The disable-new-orders switch does not liquidate, cancel, or prevent inspection. Cancellation of
eligible app-owned orders remains available in a write mode. No global cancel, external-order
control, automatic liquidation, fund transfer, FX, derivative, or short sale is exposed.

## Reauthentication and shutdown

Expect broker-driven daily restarts and periodic manual reauthentication. Configure the supported
TWS/Gateway restart options, but do not disable 2FA or automate around authentication. This tool
cannot promise indefinite unattended login. Stop it normally so subscriptions are cancelled and
the socket is closed; after an abnormal exit, restart and reconcile before mutation.

## Live guard (not approved for use by this milestone)

Changing only a port cannot enable live writes. Code requires all of: `MBT_ENVIRONMENT=live`,
`MBT_OPERATION_MODE=live_write`, `MBT_ENABLE_LIVE_EXECUTION=true`, an exact account match in
`MBT_LIVE_ACCOUNT_ALLOWLIST`, and `--live --write` on the mutation. No live mutation was tested or
authorized in this delivery.
