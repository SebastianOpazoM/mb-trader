# Architecture and IBKR method map

## Choice of API layer

The service uses one official classic `EClient`/`EWrapper` connection. IBKR introduced
`TWSSyncWrapper` in TWS API 10.40, and its direct-return calls are useful for simple scripts.
This application needs durable order intent before send, late and out-of-order status/execution
handling, commission correlation, upstream connectivity state, cancellation races, and reconnect
resynchronization. The classic event stream exposes those lifecycle signals directly. A small
correlation layer provides synchronous service calls without creating a second broker session.

The reader thread only updates protected in-memory state and signals waiters. It never waits on a
callback and never writes SQLite. Service methods perform validation and persistence on their
caller thread.

## Components

- `models.py`: immutable typed domain values with `Decimal` quantities and money.
- `broker.py`: protocol consumed by the reusable service and offline fakes.
- `ibkr_adapter.py`: the sole broker-session owner and official SDK boundary.
- `service.py`: qualification, validation, guards, idempotent mutations, and reconciliation.
- `persistence.py`: short SQLite transactions, canonical payload hashes, audit events, and
  execution deduplication.
- `cli.py`: JSON serialization and explicit operator opt-ins only.

## SDK mapping

| Service operation | Official SDK request | Principal callbacks |
| --- | --- | --- |
| Connect/readiness | `connect`, reader `run` | `nextValidId`, `managedAccounts`, `error`, `connectionClosed` |
| Account summary | `reqAccountSummary` | `accountSummary`, `accountSummaryEnd` |
| Positions | `reqPositions` | `position`, `positionEnd` |
| Contract lookup | `reqContractDetails` | `contractDetails`, `contractDetailsEnd` |
| Quote snapshot | `reqMktData(snapshot=True)` | `marketDataType`, `tickPrice`, `tickString`, `tickSnapshotEnd` |
| What-if | `placeOrder` with `whatIf=True` | `openOrder` / `OrderState` |
| Submit/modify | `placeOrder` with new/existing `orderId` | `openOrder`, `orderStatus`, `error` |
| Cancel | `cancelOrder` | `orderStatus`, `error` |
| App/open orders | `reqOpenOrders` | `openOrder`, `openOrderEnd` |
| Completed orders | `reqCompletedOrders(False)` | `completedOrder`, `completedOrdersEnd` |
| Executions | `reqExecutions` with account/client filter | `execDetails`, `execDetailsEnd` |
| Commissions | streaming with executions | `commissionAndFeesReport` or legacy `commissionReport` |

Order IDs originate only from `nextValidId` and are allocated under one lock. Client ID 0 is
rejected by configuration; manual-order binding and global cancel are not exposed. `orderRef` is
only correlation metadata. Transactional `(account, environment, idempotency_key)` uniqueness and
the canonical payload hash enforce application idempotency.

## Recovery model

An intent is saved as `pending_send` before every submit/modify/cancel. A timeout or connection
loss after that point becomes `uncertain`; it is never automatically resent. Trading in the same
contract is blocked until reconciliation or operator resolution. Reconciliation first refreshes
account, position, open-order, and execution state, then matches the saved `orderRef`, `orderId`,
and `permId` within the history TWS exposes. Absence is not proof that an order never existed, so
unmatched state remains unresolved.

IBKR statuses are retained alongside normalized states. A cancel send is `cancel_requested`, not
`cancelled`; a late fill wins when callbacks report it. Executions deduplicate globally by
`execId`. TWS order/execution queries are a recovery aid, not permanent brokerage history.

## Documentation checked

Official IBKR pages were read on 2026-10-21: requirements (Python 3.11 minimum), Windows SDK
installation, connection handshake/`nextValidId`, synchronous wrapper introduction, API settings,
what-if, order ID/placement/modification, connectivity codes, market data, and API limitations.
Exact behavior must still be checked against the locally installed SDK and TWS/Gateway build.
