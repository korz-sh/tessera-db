# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""HTTP surface of the hot owner: handler, authentication and routing."""

import hmac
import json
import logging
import sqlite3
import time
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from ..backends.base import LeaseLost
from .utils import _DB_NAME

logger = logging.getLogger(__name__)


# @implements durable-write-contract/ambiguous-delivery-is-never-replayed-automatically
class _HttpError(Exception):
    """Request-level rejection with a fixed HTTP status and error name."""

    def __init__(self, status: int, error: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.error = error


def _json_safe(value: Any) -> Any:
    """Map a SQLite value onto a JSON-safe one (mirrors examples/handler.py).

    bytes -> utf-8 text with unpaired bytes replaced by U+FFFD; None, int,
    float and str pass through unchanged.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _sql_params(body: dict) -> tuple[str, list]:
    """Validate the shared ``{"sql", "params"}`` body fields (handler.py shape)."""
    sql = body.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise _HttpError(
            400, "BadRequest", "body must include a non-empty string field 'sql'"
        )
    params = body.get("params", [])
    if not isinstance(params, list):
        raise _HttpError(
            400, "BadRequest", "field 'params' must be a JSON list (positional '?' binds)"
        )
    return sql, params


def _make_handler_class(owner: "Owner") -> type["_OwnerHTTPHandler"]:
    """Build a per-Owner handler class carrying a callback to *owner*."""
    return type("OwnerHTTPHandler", (_OwnerHTTPHandler,), {"owner": owner})


class _OwnerHTTPHandler(BaseHTTPRequestHandler):
    """HTTP surface of the hot owner (one thread per request).

    All handlers serialize on ``owner._lock`` — HTTP writes, reads and the
    publish loop share the single warm connection and the publish state, so
    the lock is the only synchronization point needed.
    """

    owner: "Owner"  # injected per-Owner by _make_handler_class()

    # HTTP/1.1 = keep-alive: the client reuses the TCP connection across
    # writes. With the HTTP/1.0 default each write pays a new handshake, and
    # that was 98% of the cost (measured: 1285 ops/s over HTTP vs 58k for the
    # same work without HTTP).
    protocol_version = "HTTP/1.1"
    # No Nagle: bodies are small and the response goes out in two writes
    # (headers + body); waiting for the first ACK adds ~40 ms.
    disable_nagle_algorithm = True

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Silence the default per-request stderr logging."""

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _authenticate(self) -> None:
        """Requires ``Authorization: Bearer <session_token>``.

        The owner is network-reachable (port 8080 on the VPC): without this,
        the only thing protecting it is the security group, and any
        compromised workload in the same VPC could write to the tenant.

        The comparison is constant-time (``hmac.compare_digest``): a ``==``
        exits on the first differing byte and leaks the token byte by byte
        to anyone able to measure latency.

        Auth is NOT optional (A4): the transitional ``TESSERA_REQUIRE_AUTH``
        flag from the staged rollout was removed. An owner without a token
        is one that never went through ``start()`` and cannot be serving.
        """
        expected = self.owner.session_token
        if not expected:
            # No token means nothing to compare (an owner that never went
            # through start()); failing closed here would break boot.
            return

        header = self.headers.get("Authorization", "")
        scheme, _, presented = header.partition(" ")
        if scheme.lower() != "bearer" or not presented:
            raise _HttpError(
                401, "Unauthorized", "expected 'Authorization: Bearer <token>'"
            )
        if not hmac.compare_digest(presented.strip(), expected):
            raise _HttpError(401, "Unauthorized", "invalid session token")

        # Fencing on the HTTP path: a client holding a stale lease view may
        # be talking to an owner that is no longer the holder. The epoch is
        # the same guarantee the manifest CAS already enforces.
        raw_epoch = self.headers.get("X-Tessera-Epoch", "").strip()
        if raw_epoch:
            try:
                claimed = int(raw_epoch)
            except ValueError:
                raise _HttpError(
                    400, "BadRequest", "X-Tessera-Epoch must be an integer"
                ) from None
            if claimed != self.owner.epoch:
                raise _HttpError(
                    409,
                    "LeaseLost",
                    f"epoch mismatch: client sent {claimed}, owner holds "
                    f"{self.owner.epoch}; re-read the lease",
                )

    # ------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------

    def _send_json(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> dict:
        length_raw = self.headers.get("Content-Length")
        try:
            length = int(length_raw or 0)
        except ValueError:
            raise _HttpError(400, "BadRequest", "invalid Content-Length") from None
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise _HttpError(400, "BadRequest", "body is not valid JSON") from None
        if not isinstance(body, dict):
            raise _HttpError(400, "BadRequest", "body must be a JSON object")
        return body

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/write":
                self._authenticate()
                self._handle_write()
            elif path == "/flush":
                self._authenticate()
                self._handle_flush()
            elif path == "/read":
                self._authenticate()
                self._handle_read()
            else:
                raise _HttpError(
                    404, "NotFound",
                    f"path {path!r} not found; expected /write, /flush or /read",
                )
        except _HttpError as exc:
            self._send_json(exc.status, {"error": exc.error, "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001 - last-resort mapping
            self._send_json(
                500, {"error": "InternalError", "detail": f"{type(exc).__name__}: {exc}"}
            )

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/health":
                self._handle_health()
            else:
                raise _HttpError(
                    404, "NotFound", f"path {path!r} not found; expected /health"
                )
        except _HttpError as exc:
            self._send_json(exc.status, {"error": exc.error, "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001 - last-resort mapping
            self._send_json(
                500, {"error": "InternalError", "detail": f"{type(exc).__name__}: {exc}"}
            )

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def _handle_write(self) -> None:
        """POST /write — one committed transaction on the warm connection.

        ``per_commit`` mode: commit and durable publication share the owner
        lock; the receipt's ``version`` equals ``published`` (the manifest
        floor already covers this transaction). ``windowed`` mode: the
        receipt acknowledges the local commit only — ``version > published``
        until the background publish lands — unless the body carries
        ``sync: true``, which restores the synchronous publish for this
        write.
        """
        owner = self.owner
        body = self._read_json_body()
        sql, params = _sql_params(body)
        sync = bool(body.get("sync"))
        with owner._lock:
            # Fencing first: a lost lease is 409 even while the server is up.
            try:
                if owner._handle is not None:
                    owner._handle.ensure_alive()
            except LeaseLost:
                owner._fenced = True
            if owner._fenced:
                owner._serving = False
                raise _HttpError(
                    409,
                    "LeaseLost",
                    "fencing loss: this owner no longer holds the tenant lease",
                )
            if not owner._serving or owner._stop_event.is_set():
                raise _HttpError(
                    503, "Unavailable", "owner is not serving or is shutting down"
                )
            if owner._conn is None:
                raise _HttpError(503, "Unavailable", "owner connection is closed")
            wal_path = Path(f"{owner._dest / _DB_NAME}-wal")
            before_size = wal_path.stat().st_size if wal_path.exists() else 0
            before_changes = owner._conn.total_changes
            before_schema = owner._conn.execute("PRAGMA schema_version").fetchone()[0]
            try:
                cur = owner._conn.execute(sql, params)
                owner._conn.commit()
            except sqlite3.Error as exc:
                owner._conn.rollback()
                # Client-supplied SQL: SQLite errors are the caller's fault
                # (400), exactly like examples/handler.py.
                raise _HttpError(400, type(exc).__name__, str(exc)) from None
            result = {"lastrowid": cur.lastrowid, "rowcount": cur.rowcount}
            was_dirty = owner._dirty
            owner._dirty = True  # failures after local commit must retire dirty state
            owner._last_write = time.monotonic()
            try:
                made_changes = (
                    owner._conn.total_changes != before_changes
                    or owner._conn.execute("PRAGMA schema_version").fetchone()[0] != before_schema
                    or (wal_path.stat().st_size if wal_path.exists() else 0) != before_size
                )
                owner._dirty = was_dirty or made_changes
                if made_changes:
                    # A commit that produced durable work advances the
                    # sequence; a no-op commit must not push version ahead
                    # of published (there is nothing to lose).
                    owner._commit_seq += 1
                if sync or owner._publish_mode != "windowed":
                    if not owner._publish_locked():
                        raise LeaseLost("owner publication was fenced")
            except Exception as exc:
                owner._fenced = True
                owner._serving = False
                owner._stop_event.set()
                raise _HttpError(503, "WriteOutcomeUnknown", str(exc)) from None
            version = owner._commit_seq
            published = owner._published_commit_seq
        self._send_json(
            200, {"result": result, "version": version, "published": published}
        )

    def _handle_flush(self) -> None:
        """POST /flush — publish every pending commit before responding.

        Windowed mode: the caller blocks until the durability window
        closes (``version <= published``). Per-commit mode: a no-op —
        nothing can be pending — but the fencing/admission checks still
        run so the call doubles as a liveness probe.
        """
        owner = self.owner
        with owner._lock:
            try:
                if owner._handle is not None:
                    owner._handle.ensure_alive()
            except LeaseLost:
                owner._fenced = True
            if owner._fenced:
                owner._serving = False
                raise _HttpError(
                    409,
                    "LeaseLost",
                    "fencing loss: this owner no longer holds the tenant lease",
                )
            if not owner._serving or owner._stop_event.is_set():
                raise _HttpError(
                    503, "Unavailable", "owner is not serving or is shutting down"
                )
            try:
                if not owner._publish_locked():
                    raise LeaseLost("owner publication was fenced")
            except Exception as exc:
                owner._fenced = True
                owner._serving = False
                owner._stop_event.set()
                raise _HttpError(503, "WriteOutcomeUnknown", str(exc)) from None
            version = owner._commit_seq
            published = owner._published_commit_seq
        self._send_json(200, {"version": version, "published": published})

    def _handle_read(self) -> None:
        """POST /read — rows from the warm local database (published state
        plus any not-yet-published dirty writes). ``version`` is the
        committed sequence — it may run ahead of what object storage
        holds in windowed mode; ``published`` is the durable floor the
        caller can rely on."""
        owner = self.owner
        sql, params = _sql_params(self._read_json_body())
        with owner._lock:
            if not owner._serving or owner._stop_event.is_set():
                raise _HttpError(
                    503, "Unavailable", "owner is not serving or is shutting down"
                )
            if owner._conn is None:
                raise _HttpError(503, "Unavailable", "owner connection is closed")
            try:
                cur = owner._conn.execute(sql, params)
                columns = (
                    [d[0] for d in cur.description] if cur.description is not None else []
                )
                rows = [[_json_safe(v) for v in row] for row in cur.fetchall()]
            except sqlite3.Error as exc:
                raise _HttpError(400, type(exc).__name__, str(exc)) from None
            version = owner._commit_seq
            published = owner._published_commit_seq
        self._send_json(
            200,
            {
                "rows": rows,
                "columns": columns,
                "version": version,
                "published": published,
            },
        )

    def _handle_health(self) -> None:
        """GET /health — liveness/fencing snapshot (no serving gate: health
        must answer even on a fenced or stopping owner)."""
        owner = self.owner
        # Snapshot WITHOUT owner._lock: a publish inside its transient
        # error budget holds the lock for up to publish_error_budget_s,
        # and health is exactly the probe that must keep answering
        # through that outage. Each field is an atomic read — a slightly
        # inconsistent snapshot is fine for liveness.
        tenant = owner._tenant
        version = owner._commit_seq
        published = owner._published_commit_seq
        dirty = owner._dirty
        idle_for_s = time.monotonic() - owner._last_write
        epoch = owner._handle.lease.epoch if owner._handle is not None else 0
        self._send_json(
            200,
            {
                "tenant": tenant,
                "epoch": epoch,
                "version": version,
                "published": published,
                "writes_pending": dirty,
                "idle_for_s": round(idle_for_s, 3),
            },
        )
