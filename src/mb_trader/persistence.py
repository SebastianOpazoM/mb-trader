from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .errors import IdempotencyConflict
from .jsonutil import json_default
from .models import Execution


def _now() -> str:
    return datetime.now(UTC).isoformat()


def canonical_payload(payload: dict[str, Any]) -> tuple[str, str]:
    encoded = json.dumps(payload, default=json_default, sort_keys=True, separators=(",", ":"))
    return encoded, hashlib.sha256(encoded.encode()).hexdigest()


class Store:
    """SQLite audit store. Each operation gets its own short transaction."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._schema_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    @contextmanager
    def _transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    account TEXT NOT NULL,
                    environment TEXT NOT NULL,
                    client_id INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    order_id INTEGER,
                    perm_id INTEGER,
                    state TEXT NOT NULL,
                    broker_status TEXT,
                    result_json TEXT,
                    error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(account, environment, idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS lifecycle_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
                    event_type TEXT NOT NULL,
                    broker_status TEXT,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    dedupe_key TEXT,
                    UNIQUE(intent_id, dedupe_key)
                );
                CREATE TABLE IF NOT EXISTS executions (
                    exec_id TEXT PRIMARY KEY,
                    intent_id TEXT,
                    order_id INTEGER NOT NULL,
                    perm_id INTEGER,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_intents_order_id ON intents(order_id);
                CREATE INDEX IF NOT EXISTS idx_intents_perm_id ON intents(perm_id);
                """
            )

    def begin_intent(
        self,
        *,
        idempotency_key: str,
        operation: str,
        account: str,
        environment: str,
        client_id: int,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        if not idempotency_key.strip():
            raise IdempotencyConflict("idempotency key must not be empty")
        payload_json, payload_hash = canonical_payload(payload)
        now = _now()
        intent_id = str(uuid.uuid4())
        order_ref = f"mbt:{intent_id}"
        with self._transaction(immediate=True) as connection:
            existing = connection.execute(
                """SELECT * FROM intents
                   WHERE account=? AND environment=? AND idempotency_key=?""",
                (account, environment, idempotency_key),
            ).fetchone()
            if existing:
                if existing["payload_hash"] != payload_hash:
                    raise IdempotencyConflict(
                        "idempotency key was already used with a different payload"
                    )
                return dict(existing), False
            connection.execute(
                """INSERT INTO intents
                   (intent_id,idempotency_key,operation,account,environment,client_id,
                    payload_json,payload_hash,order_ref,state,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,'pending_send',?,?)""",
                (
                    intent_id,
                    idempotency_key,
                    operation,
                    account,
                    environment,
                    client_id,
                    payload_json,
                    payload_hash,
                    order_ref,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            return dict(row), True

    def update_intent(
        self,
        intent_id: str,
        *,
        state: str,
        broker_status: str | None = None,
        order_id: int | None = None,
        perm_id: int | None = None,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        result_json = None if result is None else json.dumps(result, default=json_default)
        error_json = None if error is None else json.dumps(error, default=json_default)
        with self._transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE intents SET state=?, broker_status=COALESCE(?,broker_status),
                   order_id=COALESCE(?,order_id), perm_id=COALESCE(?,perm_id),
                   result_json=COALESCE(?,result_json), error_json=COALESCE(?,error_json),
                   updated_at=? WHERE intent_id=?""",
                (
                    state,
                    broker_status,
                    order_id,
                    perm_id,
                    result_json,
                    error_json,
                    _now(),
                    intent_id,
                ),
            )

    def add_event(
        self,
        intent_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        broker_status: str | None = None,
        dedupe_key: str | None = None,
    ) -> bool:
        with self._transaction(immediate=True) as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO lifecycle_events
                   (intent_id,event_type,broker_status,payload_json,occurred_at,dedupe_key)
                   VALUES (?,?,?,?,?,?)""",
                (
                    intent_id,
                    event_type,
                    broker_status,
                    json.dumps(payload, default=json_default, sort_keys=True),
                    _now(),
                    dedupe_key,
                ),
            )
            return cursor.rowcount == 1

    def add_execution(self, execution: Execution, intent_id: str | None = None) -> bool:
        payload = json.dumps(asdict(execution), default=json_default, sort_keys=True)
        with self._transaction(immediate=True) as connection:
            if intent_id is None:
                row = connection.execute(
                    """SELECT intent_id FROM intents
                       WHERE order_id=? OR (? IS NOT NULL AND perm_id=?)
                       ORDER BY created_at DESC LIMIT 1""",
                    (execution.order_id, execution.perm_id, execution.perm_id),
                ).fetchone()
                intent_id = row["intent_id"] if row else None
            cursor = connection.execute(
                """INSERT OR IGNORE INTO executions
                   (exec_id,intent_id,order_id,perm_id,payload_json,occurred_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    execution.exec_id,
                    intent_id,
                    execution.order_id,
                    execution.perm_id,
                    payload,
                    execution.occurred_at.isoformat(),
                ),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                connection.execute(
                    """UPDATE executions SET intent_id=COALESCE(?,intent_id),
                       order_id=?,perm_id=COALESCE(?,perm_id),payload_json=?,occurred_at=?
                       WHERE exec_id=?""",
                    (
                        intent_id,
                        execution.order_id,
                        execution.perm_id,
                        payload,
                        execution.occurred_at.isoformat(),
                        execution.exec_id,
                    ),
                )
            return inserted

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_by_order_id(self, order_id: int) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM intents WHERE order_id=? ORDER BY created_at DESC LIMIT 1",
                (order_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_intents(self, *, unresolved_only: bool = False) -> list[dict[str, Any]]:
        query = "SELECT * FROM intents"
        parameters: tuple[str, ...] = ()
        if unresolved_only:
            query += " WHERE state IN ('pending_send','uncertain','cancel_requested')"
        query += " ORDER BY created_at DESC"
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters).fetchall()]

    def pending_sell_quantity(self, con_id: int) -> Decimal:
        terminal = {"filled", "cancelled", "rejected"}
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT intent_id,operation,payload_json,result_json,state,order_id
                   FROM intents ORDER BY created_at, rowid"""
            ).fetchall()
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            target = payload.get("target_order_id")
            key = str(row["order_id"] or target or row["intent_id"])
            previous = latest.get(key, {})
            result = json.loads(row["result_json"]) if row["result_json"] else {}
            latest[key] = {
                "side": payload.get("side", previous.get("side")),
                "con_id": payload.get("con_id", previous.get("con_id")),
                "quantity": payload.get("quantity", previous.get("quantity", "0")),
                "remaining": result.get("remaining", previous.get("remaining")),
                "state": row["state"],
            }
        return sum(
            (
                Decimal(
                    str(item["remaining"] if item["remaining"] is not None else item["quantity"])
                )
                for item in latest.values()
                if item["side"] == "SELL"
                and int(item["con_id"] or -1) == con_id
                and item["state"] not in terminal
            ),
            Decimal("0"),
        )

    def unresolved_for_contract(self, con_id: int) -> list[str]:
        matches: list[str] = []
        for row in self.list_intents(unresolved_only=True):
            payload = json.loads(row["payload_json"])
            if int(payload.get("con_id", -1)) == con_id:
                matches.append(row["intent_id"])
        return matches
