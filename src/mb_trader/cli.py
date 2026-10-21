from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .config import Settings
from .errors import ConfigurationError, TraderError
from .ibkr_adapter import IBKRAdapter
from .jsonutil import dumps
from .locking import ProcessLock
from .models import Contract, OrderSpec, OrderType, Side, TimeInForce
from .persistence import Store
from .service import TradingService


def _decimal(raw: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("must be a decimal") from exc
    if not value.is_finite():
        raise argparse.ArgumentTypeError("must be finite")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mb-trader")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="connect, synchronize, and report readiness")
    sub.add_parser("accounts", help="list configured/managed accounts and summary values")
    sub.add_parser("positions", help="list positions for the selected account")
    orders = sub.add_parser("orders", help="list durable app-owned order intents")
    orders.add_argument("--unresolved", action="store_true")

    lookup = sub.add_parser("lookup", help="resolve a US stock/ETF contract")
    _add_contract_identity(lookup)
    quote = sub.add_parser("quote", help="request an optional market-data snapshot")
    _add_contract_identity(quote)
    validate = sub.add_parser("validate", help="run local validation only; never sends an order")
    _add_order(validate)
    preview = sub.add_parser("preview", help="run broker what-if; never falls through to submit")
    _add_order(preview)
    submit = sub.add_parser("submit", help="submit an explicitly opted-in order")
    _add_order(submit)
    _add_mutation(submit)
    modify = sub.add_parser("modify", help="modify price/total quantity of an app-owned order")
    modify.add_argument("order_id", type=int)
    _add_order(modify)
    _add_mutation(modify)
    cancel = sub.add_parser("cancel", help="request cancellation of an app-owned order")
    cancel.add_argument("order_id", type=int)
    _add_mutation(cancel)
    reconcile = sub.add_parser("reconcile", help="resynchronize uncertain/pending durable intents")
    reconcile.add_argument("--write", action="store_true", help="acknowledge broker session access")
    smoke = sub.add_parser("smoke", help="paper integration flow; mutations require --write")
    _add_order(smoke)
    smoke.add_argument("--write", action="store_true")
    smoke.add_argument("--idempotency-key")
    smoke.add_argument("--live", action="store_true", help=argparse.SUPPRESS)
    return parser


def _add_contract_identity(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("symbol")
    parser.add_argument("--con-id", type=int)
    parser.add_argument("--primary-exchange")
    parser.add_argument("--security-type", default="STK", choices=["STK"])
    parser.add_argument("--currency", default="USD", choices=["USD"])


def _add_order(parser: argparse.ArgumentParser) -> None:
    _add_contract_identity(parser)
    parser.add_argument("--side", required=True, choices=[item.value for item in Side])
    parser.add_argument("--quantity", required=True, type=_decimal)
    parser.add_argument("--order-type", required=True, choices=[item.value for item in OrderType])
    parser.add_argument("--limit-price", type=_decimal)
    parser.add_argument(
        "--tif", default=TimeInForce.DAY.value, choices=[item.value for item in TimeInForce]
    )
    parser.add_argument("--outside-rth", action="store_true")


def _add_mutation(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--write", action="store_true", help="required explicit mutation opt-in")
    parser.add_argument("--live", action="store_true", help="additional live-account opt-in")


@contextmanager
def _connected(settings: Settings, store: Store) -> Iterator[TradingService]:
    lock = ProcessLock(settings.database_path.with_suffix(settings.database_path.suffix + ".lock"))
    with lock:
        broker = IBKRAdapter(settings)
        service = TradingService(settings, broker, store)
        service.connect()
        try:
            yield service
        finally:
            broker.disconnect()


def _resolve(service: TradingService, args: argparse.Namespace) -> Contract:
    return service.resolve_contract(
        args.symbol,
        security_type=args.security_type,
        currency=args.currency,
        con_id=args.con_id,
        primary_exchange=args.primary_exchange,
    )


def _spec(service: TradingService, args: argparse.Namespace) -> OrderSpec:
    return OrderSpec(
        contract=_resolve(service, args),
        side=Side(args.side),
        quantity=args.quantity,
        order_type=OrderType(args.order_type),
        time_in_force=TimeInForce(args.tif),
        limit_price=args.limit_price,
        outside_regular_hours=args.outside_rth,
    )


def _require_write(args: argparse.Namespace) -> None:
    if not args.write:
        raise ConfigurationError("mutation requires the explicit --write flag")


def _run(args: argparse.Namespace, settings: Settings, store: Store) -> Any:
    if args.command == "orders":
        return store.list_intents(unresolved_only=args.unresolved)
    with _connected(settings, store) as service:
        if args.command == "health":
            return service.broker.health()
        if args.command == "accounts":
            return {
                "selected_account": settings.account,
                "managed_accounts": service.broker.health().managed_accounts,
                "summary": service.broker.account_summary(),
            }
        if args.command == "positions":
            return [item for item in service.broker.positions() if item.account == settings.account]
        if args.command == "lookup":
            return _resolve(service, args)
        if args.command == "quote":
            return service.broker.quote(_resolve(service, args))
        if args.command == "validate":
            spec = _spec(service, args)
            quote = (
                service.broker.quote(spec.contract) if spec.order_type is OrderType.MKT else None
            )
            return {
                "valid": True,
                "estimated_notional": service.validate_order(spec, quote=quote),
                "broker_contacted_for_order": False,
            }
        if args.command == "preview":
            return service.preview(_spec(service, args))
        if args.command == "submit":
            _require_write(args)
            return service.submit(
                _spec(service, args),
                idempotency_key=args.idempotency_key,
                live_invocation=args.live,
            )
        if args.command == "modify":
            _require_write(args)
            return service.modify(
                args.order_id,
                _spec(service, args),
                idempotency_key=args.idempotency_key,
                live_invocation=args.live,
            )
        if args.command == "cancel":
            _require_write(args)
            return service.cancel(
                args.order_id,
                idempotency_key=args.idempotency_key,
                live_invocation=args.live,
            )
        if args.command == "reconcile":
            return service.reconcile()
        if args.command == "smoke":
            if settings.environment != "paper":
                raise ConfigurationError("smoke workflow is restricted to the paper environment")
            spec = _spec(service, args)
            contract = spec.contract
            quote = service.broker.quote(contract)
            preview = service.preview(spec)
            result: dict[str, Any] = {
                "health": service.broker.health(),
                "contract": contract,
                "quote": quote,
                "preview": preview,
                "mutation": "not requested",
            }
            if args.write:
                if not args.idempotency_key:
                    raise ConfigurationError("smoke --write requires --idempotency-key")
                state = service.submit(spec, idempotency_key=args.idempotency_key)
                result["submit"] = state
                result["cancel"] = service.cancel(
                    state.order_id or 0,
                    idempotency_key=f"{args.idempotency_key}:cancel",
                )
                result["mutation"] = "submit and cancel requested; a fill was not required"
            return result
    raise AssertionError("unhandled command")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        settings = Settings.from_env(args.env_file)
        store = Store(settings.database_path)
        print(dumps({"ok": True, "result": _run(args, settings, store)}, pretty=args.pretty))
        return 0
    except TraderError as exc:
        print(dumps({"ok": False, **exc.as_dict()}, pretty=args.pretty))
        return 2
    except KeyboardInterrupt:
        print(dumps({"ok": False, "error": "interrupted", "message": "operation interrupted"}))
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
