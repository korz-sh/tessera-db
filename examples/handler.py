"""AWS Lambda handlers for the tessera-db reference deployment.

One module, two entrypoints (stdlib + tessera + boto3 only; the python3.12
Lambda runtime bundles boto3):

``handler(event, context)``
    API Gateway HTTP API (payload v2) proxy for ``POST /{tenant}/{op}`` with
    ``op`` in ``create | write | read``. JSON bodies, JSON error bodies of
    the shape ``{"error": <ExceptionName>, "detail": <str>}`` mapped to HTTP
    status codes (see ``_ERROR_STATUS``).

``compactor(event, context)``
    EventBridge-scheduled housekeeping: garbage-collect every tenant
    (:meth:`tessera.store.Store.gc`) and fold delta chains that crossed the
    same thresholds the Store's inline compaction trigger uses — this is
    what catches tenants that stopped writing before their chain triggered
    the inline path.

Serverless notes:

- Backend and :class:`~tessera.store.Store` are lazily-built module-level
  singletons reused across warm invocations; the local SQLite cache lives
  under ``/tmp`` (the only writable path in Lambda — SQLite's WAL needs
  real files).
- Lease hygiene: API Gateway spreads requests over ephemeral sandboxes,
  each with its own Store. A retained lease (TTL ``LEASE_TTL_MS`` = 60 s)
  would stay pinned by whichever sandbox served the last write and starve
  every other sandbox with ``TenantBusy`` until the TTL lapses, so every
  lease-taking operation (write, gc, compact) is followed by
  ``store.close()`` — it releases the handles idempotently WITHOUT touching
  the warm caches (tenant dirs and the manifest etag cache all survive, so
  warm invocations still hydrate for free). Releasing preserves the fencing
  epoch, so cross-sandbox interleaving stays conflict-free.
- Write forwarding (M8): a write that lands on a Lambda while a hot owner
  holds the tenant's live lease is FORWARDED to that owner's ``POST /write``
  instead of acquiring + hydrating here. Forwarding contract:

    - The lease item (DynamoDB, PK ``tenant_id``) carries the owner's
      ``endpoint`` while it is hot (acquire sets it, release removes it).
      ``_lookup_owner_endpoint`` returns it only when unexpired.
    - ``_forward_to_owner`` POSTs the identical ``{"sql", "params"}`` body
      the owner serves and forwards the owner's (status, JSON payload)
      verbatim — the owner is authoritative once reachable, because it may
      have already committed; the Lambda must never re-execute the write
      locally after a forwarded attempt (double-apply risk). The response
      gains an ``X-Tessera-Owner: 1`` marker header.
    - Only connection failure before sending yields ``None`` for fenced
      fallback. Once sending begins, transport failure returns HTTP 502
      ``WriteOutcomeUnknown`` with ``retryable: false``; reconcile the
      outcome before retrying. Owner errors are authoritative, including
      authentication rejection. Invalid successful receipts also report
      uncertainty; non-JSON error bodies become ``OwnerError`` payloads.
    - Reads are never forwarded: the Lambda serves them itself (cheap,
      no lease involved).

- Logging is one structured line per request (tenant + op + status + ms);
  SQL text and parameters are never logged.
"""

from __future__ import annotations

import base64
import hmac
import http.client
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tessera.backends.base import (
    BackendError,
    CASFailed,
    LeaseLost,
    NotFound,
    TenantBusy,
)
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.store import (
    Conflict,
    StaleRead,
    Store,
    TenantExists,
    TenantNotFound,
)

__all__ = ["compactor", "handler"]

# Object layout of the store (tessera/store.py): the manifest is the only
# mutable object and lives at "<tenant>/manifest.json".
_MANIFEST_SUFFIX = "manifest.json"

# Scheduled-compaction trigger: replicate the Store's inline thresholds
# (Store.__init__: inline_compact_deltas=200, inline_compact_bytes=16 MiB)
# so the periodic pass folds exactly the chains the inline trigger would
# have folded had the tenant kept writing.
_COMPACT_DELTAS = 200
_COMPACT_BYTES = 16 * 1024 * 1024

# Tenant identifiers become S3 key prefixes ("<tenant>/manifest.json"), so
# they must be a single safe path segment (no "/", no ".." tricks).
_TENANT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

# (exception class, HTTP status). Specific classes first; BackendError — the
# base of every tessera error — last, as the catch-all for library errors.
# Anything outside the taxonomy falls through to the 500 in handler().
_ERROR_STATUS: tuple[tuple[type[Exception], int], ...] = (
    (Conflict, 409),
    (LeaseLost, 409),
    (TenantNotFound, 404),
    (TenantExists, 409),
    (StaleRead, 409),
    (TenantBusy, 503),
    (CASFailed, 409),
    (BackendError, 500),
)

# Client-supplied SQL runs verbatim inside the write/read transaction, so
# SQLite errors (syntax, missing table, constraint violation, ...) are the
# caller's fault and surface as 400, not 500. Genuine server-side faults
# still hit the generic 500.
_SQL_STATUS: tuple[type[Exception], int] = (sqlite3.Error, 400)

_OPS = ("create", "write", "read")

_init_lock = threading.Lock()
_backend: S3DynamoBackend | None = None
_store: Store | None = None
_lease_table: Any | None = None

# ---------------------------------------------------------------------------
# Per-request AWS-call timing breakdown (Fase 0 of the latency plan).
#
# _TimingBackend wraps the S3DynamoBackend the Store uses and accumulates
# wall-clock ms per segment into a thread-local, reset at the top of every
# request and dumped into the tessera.request log line. Segments:
#
#   lease_ms     acquire/renew/release_lease (DynamoDB)
#   manifest_ms  get/head/put on */manifest.json (put if_match = the CAS)
#   s3_get_ms    every other get/head/list (hydrate: snapshots, deltas, chunks)
#   s3_put_ms    every other put (delta/chunk/snapshot uploads)
#   aws_calls    total backend calls observed
#
# Overhead is one perf_counter pair + dict update per backend call — noise
# next to a network round-trip.
# ---------------------------------------------------------------------------

_timings = threading.local()


def _timings_reset() -> None:
    _timings.data = {}


def _timings_snapshot() -> dict:
    data = getattr(_timings, "data", None) or {}
    return {k: (round(v, 1) if isinstance(v, float) else v) for k, v in data.items()}


def _record(segment: str, ms: float) -> None:
    data = getattr(_timings, "data", None)
    if data is None:
        data = _timings.data = {}
    data[segment] = data.get(segment, 0.0) + ms
    data["aws_calls"] = data.get("aws_calls", 0) + 1


class _TimingBackend:
    """Transparent timing proxy over an :class:`S3DynamoBackend`.

    Not a Backend subclass on purpose: it forwards *everything* via
    ``__getattr__`` so it stays correct if the Backend contract grows, and
    only the timed methods are overridden explicitly.
    """

    _LEASE_OPS = ("acquire_lease", "renew_lease", "release_lease")

    def __init__(self, inner: S3DynamoBackend) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    @staticmethod
    def _segment(op: str, key: str | None) -> str:
        if op in _TimingBackend._LEASE_OPS:
            return "lease_ms"
        if key is not None and key.endswith(_MANIFEST_SUFFIX):
            return "manifest_ms"
        return "s3_put_ms" if op == "put" else "s3_get_ms"

    def _timed(self, op: str, key: str | None, fn, *args, **kwargs):
        started = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            _record(self._segment(op, key), (time.perf_counter() - started) * 1000.0)

    # --- timed S3 object ops ---
    def get(self, key: str):
        return self._timed("get", key, self._inner.get, key)

    def head(self, key: str):
        return self._timed("head", key, self._inner.head, key)

    def put(self, key: str, data: bytes, **kwargs):
        return self._timed("put", key, self._inner.put, key, data, **kwargs)

    def delete(self, key: str):
        return self._timed("delete", key, self._inner.delete, key)

    def list(self, prefix: str):
        return self._timed("list", None, self._inner.list, prefix)

    def mtime(self, key: str):
        return self._timed("mtime", None, self._inner.mtime, key)

    # --- timed DynamoDB lease ops ---
    def acquire_lease(
        self,
        tenant: str,
        ttl_ms: int,
        endpoint: str = "",
        session_token: str = "",
        min_epoch: int = 0,
    ):
        # The kwargs go BY NAME: this proxy wraps the Backend contract, and
        # passing them positionally would tie it to the signature order.
        return self._timed(
            "acquire_lease",
            None,
            self._inner.acquire_lease,
            tenant,
            ttl_ms,
            endpoint=endpoint,
            session_token=session_token,
            min_epoch=min_epoch,
        )

    def renew_lease(self, lease, ttl_ms: int):
        return self._timed("renew_lease", None, self._inner.renew_lease, lease, ttl_ms)

    def release_lease(self, lease):
        return self._timed("release_lease", None, self._inner.release_lease, lease)

# Owner-write forwarding: default contact budget for a reachable owner.
# Configurable via OWNER_FORWARD_TIMEOUT_MS (milliseconds) for tests and
# tuning; read per call so warm containers pick up redeployed env cleanly.
_FORWARD_TIMEOUT_DEFAULT_MS = 2000

# Sticky-lease mode (Fase 2 of docs/SERVERLESS-LATENCY-PLAN.md). When
# STICKY_LEASE=1 the write path does NOT call store.close() after each
# request: the Store retains the lease handle (with its background renewal)
# across warm invocations, so a warm same-sandbox write skips the DynamoDB
# acquire + release entirely and — because the retained epoch fences all
# other writers — the pre-write manifest GET as well (Store fast path).
# Trade-offs, all bounded:
#   - Another sandbox writing the same tenant gets TenantBusy until the
#     retention window lapses (Store retain_ms, STICKY_RETAIN_MS below) or
#     this sandbox dies and the lease TTL expires. The 503 carries
#     Retry-After so clients back off instead of hammering.
#   - A sandbox frozen by Lambda between invocations stops renewing; its
#     lease simply expires and the next writer acquires a fresh epoch. The
#     frozen sandbox's next write finds its handle "lost" -> LeaseLost 409,
#     store.close() resets it and the retry acquires cleanly (fencing keeps
#     this correct by construction).
# Recommended pairing: LEASE_TTL_MS=10000 (not the default 60000) so a
# crashed sandbox blocks the tenant for at most 10 s.


def _sticky() -> bool:
    return os.environ.get("STICKY_LEASE", "0").strip().lower() in ("1", "true", "yes")


# Owner-endpoint probe cache (A2.6): the GetItem probe runs on EVERY write
# but its answer almost never changes. Cache it briefly per sandbox;
# fail-open forwarding already tolerates a stale endpoint (unreachable
# owner -> fall back to the local path).
_OWNER_PROBE_TTL_S = 1.0
_owner_probe: dict[str, tuple[float, str | None]] = {}

# ---------------------------------------------------------------------------
# Auto-promotion (docs/N-WRITERS-PLAN.md): when N writers contend for one
# tenant, TenantBusy is the signal. The first loser that wins the promotion
# marker launches an EPHEMERAL hot owner on Fargate (ecs:RunTask); once the
# owner's endpoint lands in the lease, every Lambda forwards to it (M8) and
# contention disappears. The owner self-terminates after OWNER_IDLE_S without
# writes. Everything here is fail-open: any failure leaves the system exactly
# as it is today (sticky lease + client retries).
#
# Env (all required to enable, see template.yaml):
#   AUTO_PROMOTE=1, OWNER_CLUSTER, OWNER_TASKDEF_ARN, OWNER_SUBNETS
#   (comma-separated), OWNER_SECURITY_GROUP, OWNER_IDLE_S (default 60).
# ---------------------------------------------------------------------------

# The promotion marker uses the LEASE TABLE with a reserved tenant-id prefix
# ("~" sorts after every valid tenant char and _TENANT_RE rejects it, so no
# real tenant can collide). Conditional put = one launcher per window.
_PROMOTE_MARKER_PREFIX = "~promote#"
_PROMOTE_MARKER_TTL_S = 120

_ecs_client: Any | None = None

# Yield check cache: (checked_monotonic, marker_live) per tenant. The sticky
# lease and a starting owner COMPETE for the lease (the Lambdas renew it
# continuously under load, so the owner can never acquire). While a
# promotion marker is live, sticky Lambdas release the lease after each
# write ("polite yield") so the owner's backoff wins within a few seconds.
_YIELD_CHECK_TTL_S = 3.0
_yield_check: dict[str, tuple[float, bool]] = {}


def _promotion_in_progress(tenant: str) -> bool:
    """True while *tenant*'s promotion marker is live (cached ~3 s)."""
    cached = _yield_check.get(tenant)
    now = time.monotonic()
    if cached is not None and now - cached[0] < _YIELD_CHECK_TTL_S:
        return cached[1]
    live = False
    try:
        item = (
            _get_lease_table()
            .get_item(Key={"tenant_id": f"{_PROMOTE_MARKER_PREFIX}{tenant}"})
            .get("Item")
        )
        live = bool(item) and float(item.get("expires_at", 0)) > time.time()
    except Exception:  # noqa: BLE001 — fail-open: no marker info, no yield
        live = False
    _yield_check[tenant] = (now, live)
    return live


def _auto_promote_enabled() -> bool:
    return (
        os.environ.get("AUTO_PROMOTE", "0").strip() == "1"
        and bool(os.environ.get("OWNER_CLUSTER"))
        and bool(os.environ.get("OWNER_TASKDEF_ARN"))
        and bool(os.environ.get("OWNER_SUBNETS"))
    )


def _get_ecs():
    global _ecs_client
    with _init_lock:
        if _ecs_client is None:
            _ecs_client = boto3.client(
                "ecs", region_name=os.environ.get("REGION", "us-east-1")
            )
        return _ecs_client


def _try_promote(tenant: str) -> None:
    """Best-effort: launch an ephemeral owner for *tenant* (at most one per
    marker window). Every failure is swallowed and logged — promotion is an
    optimization, never a requirement (fail-open by design)."""
    if not _auto_promote_enabled():
        return
    now = int(time.time())
    try:
        # 1. Promotion marker: conditional put, first loser wins. expires_at
        # reuses the lease table's TTL attribute so stale markers vanish.
        _get_lease_table().put_item(
            Item={
                "tenant_id": f"{_PROMOTE_MARKER_PREFIX}{tenant}",
                "expires_at": now + _PROMOTE_MARKER_TTL_S,
            },
            ConditionExpression="attribute_not_exists(tenant_id) OR expires_at < :now",
            ExpressionAttributeValues={":now": now},
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code != "ConditionalCheckFailedException":
            print(json.dumps({"msg": "tessera.promote.marker_error", "tenant": tenant, "error": code}))
        return  # someone else is already launching (or marker infra failed)
    try:
        # 2. Launch the ephemeral owner task. Capacity is configurable:
        # OWNER_CAPACITY_PROVIDER=FARGATE_SPOT (default) is ~70% cheaper and
        # a Spot interruption is already handled by design — the lease
        # expires, the fencing epoch invalidates the dead owner and writes
        # fall back to the normal acquire path. Set FARGATE for on-demand,
        # or empty to inherit the cluster's default capacity provider.
        capacity = os.environ.get("OWNER_CAPACITY_PROVIDER", "FARGATE_SPOT")
        kwargs: dict[str, Any] = (
            {"capacityProviderStrategy": [{"capacityProvider": capacity, "weight": 1}]}
            if capacity
            else {}
        )
        _get_ecs().run_task(
            cluster=os.environ["OWNER_CLUSTER"],
            taskDefinition=os.environ["OWNER_TASKDEF_ARN"],
            count=1,
            **kwargs,
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [
                        s.strip()
                        for s in os.environ["OWNER_SUBNETS"].split(",")
                        if s.strip()
                    ],
                    "securityGroups": (
                        [os.environ["OWNER_SECURITY_GROUP"]]
                        if os.environ.get("OWNER_SECURITY_GROUP")
                        else []
                    ),
                    "assignPublicIp": "ENABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": "owner",
                        "environment": [
                            {"name": "TESSERA_TENANT", "value": tenant},
                            {"name": "TESSERA_BUCKET", "value": os.environ["BUCKET"]},
                            {"name": "TESSERA_TABLE", "value": os.environ["TABLE"]},
                            {"name": "TESSERA_REGION", "value": os.environ.get("REGION", "us-east-1")},
                            {
                                "name": "TESSERA_IDLE_S",
                                "value": os.environ.get("OWNER_IDLE_S", "60"),
                            },
                            {
                                "name": "TESSERA_ADVERTISE_MODE",
                                "value": os.environ.get("OWNER_ADVERTISE_MODE", "public"),
                            },
                            {
                                # Launched INTO contention: keep retrying the
                                # acquire while sticky Lambdas yield (up to
                                # the marker TTL).
                                "name": "TESSERA_ACQUIRE_WAIT_S",
                                "value": str(_PROMOTE_MARKER_TTL_S),
                            },
                        ],
                    }
                ]
            },
        )
        print(json.dumps({"msg": "tessera.promote.launched", "tenant": tenant}))
    except Exception as exc:  # noqa: BLE001 — fail-open by contract
        print(
            json.dumps(
                {
                    "msg": "tessera.promote.failed",
                    "tenant": tenant,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        )


class _HttpError(Exception):
    """Request-level rejection with a fixed status and error name."""

    def __init__(self, status: int, error: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.error = error


def _get_pair() -> tuple[S3DynamoBackend, Store]:
    """Lazily build the backend + store singletons (warm-container reuse)."""
    global _backend, _store
    with _init_lock:
        if _store is None:
            backend = S3DynamoBackend(
                bucket=os.environ["BUCKET"],
                table_name=os.environ["TABLE"],
                region=os.environ.get("REGION", "us-east-1"),
                # S3 Express One Zone for the write-once deltas (optional).
                # Cuts the hot-path PUT from ~28 ms to single-digit ms;
                # everything durable long-term stays on standard S3.
                express_bucket=os.environ.get("EXPRESS_BUCKET", "").strip()
                or None,
            )
            _backend = _TimingBackend(backend)
            _store = Store(
                _backend,
                ttl_ms=int(os.environ.get("LEASE_TTL_MS", "60000")),
                # Sticky mode: how long a lease handle is reused across warm
                # invocations before being dropped (Store default is 2 s,
                # tuned here for request inter-arrival gaps).
                retain_ms=int(os.environ.get("STICKY_RETAIN_MS", "5000")),
                # /tmp is the only writable directory in Lambda; the Store
                # needs real files for SQLite's WAL. Env unset -> KeyError,
                # surfaced as 500 by handler() (graceful, no import crash).
                root="/tmp/tessera",
            )
        return _backend, _store


def _get_lease_table():
    """Lazily build a DynamoDB Table handle for the lease table (warm reuse).

    Kept separate from ``_get_pair`` so write forwarding can probe the lease
    WITHOUT building the (expensive) backend/Store pair — a forwarded write
    never touches S3 or hydrates a local database.
    """
    global _lease_table
    with _init_lock:
        if _lease_table is None:
            dynamodb = boto3.resource(
                "dynamodb", region_name=os.environ.get("REGION", "us-east-1")
            )
            _lease_table = dynamodb.Table(os.environ["TABLE"])
        return _lease_table


def _forward_timeout_s() -> float:
    """Owner-contact timeout in seconds (env OWNER_FORWARD_TIMEOUT_MS, ms)."""
    raw = os.environ.get("OWNER_FORWARD_TIMEOUT_MS")
    try:
        return max(int(raw), 0) / 1000.0 if raw else _FORWARD_TIMEOUT_DEFAULT_MS / 1000.0
    except (TypeError, ValueError):
        return _FORWARD_TIMEOUT_DEFAULT_MS / 1000.0


def _lookup_owner_endpoint(tenant: str) -> tuple[str, str, int] | None:
    """Owner del tenant como ``(endpoint, session_token, epoch)``, o None.

    Reads the lease item directly from the lease table (GetItem,
    ConsistentRead): acquire with an endpoint SETs it, release REMOVEs it,
    and expired rows keep a past ``expires_at`` until TTL sweeps them — so
    liveness is checked here, not trusted. Fail-open: any lookup error
    (missing table, throttling, ...) yields None and the caller falls back
    to the normal write path. The (endpoint | None) answer is cached for
    ``_OWNER_PROBE_TTL_S`` per sandbox (A2.6): a stale positive is harmless
    (forwarding fails open) and a stale negative lasts at most 1 s.
    """
    cached = _owner_probe.get(tenant)
    now = time.monotonic()
    if cached is not None and now - cached[0] < _OWNER_PROBE_TTL_S:
        return cached[1]

    try:
        item = (
            _get_lease_table()
            .get_item(Key={"tenant_id": tenant}, ConsistentRead=True)
            .get("Item")
        )
    except ClientError:
        return None
    found: tuple[str, str, int] | None = None
    if item:
        try:
            live = float(item.get("expires_at", 0)) > time.time()
        except (TypeError, ValueError):
            live = False
        if live and item.get("endpoint"):
            # The token and the epoch come out of the SAME GetItem already
            # made to discover the endpoint: authenticated forwarding does
            # not cost a single extra AWS call.
            try:
                epoch = int(item.get("epoch", 0))
            except (TypeError, ValueError):
                epoch = 0
            found = (
                str(item["endpoint"]),
                str(item.get("session_token", "")),
                epoch,
            )
    # Cache the TUPLE, not just the endpoint: storing the endpoint without
    # its token would guarantee a 401 after every owner handover.
    _owner_probe[tenant] = (now, found)
    return found


# ---------------------------------------------------------------------------
# Connection pool to the owner (one per endpoint and per sandbox)
#
# `urlopen` opens a NEW TCP connection per request. Measured against a real
# owner: 0.590 ms/write with a fresh connection vs 0.295 ms reusing it —
# the handshake was HALF the forwarding cost, and that on loopback. Over a
# VPC network the handshake weighs more, not less.
#
# One dict per sandbox is enough: a Lambda sandbox serves one request at a
# time, so there is no internal concurrency to protect beyond the lock that
# guards the dict. A broken connection is dropped without resending the
# write: once the send has started, the outcome may be unknown.
# ---------------------------------------------------------------------------

_owner_conns: dict[str, Any] = {}
_owner_conns_lock = threading.Lock()


def _owner_connection(endpoint: str, timeout_s: float):
    """Persistent HTTP connection to the *endpoint* owner (cached)."""
    parsed = urllib.parse.urlsplit(endpoint)
    key = f"{parsed.hostname}:{parsed.port or 80}"
    with _owner_conns_lock:
        conn = _owner_conns.get(key)
        if conn is None:
            conn = http.client.HTTPConnection(
                parsed.hostname, parsed.port or 80, timeout=timeout_s
            )
            _owner_conns[key] = conn
        return key, conn


def _drop_owner_connection(key: str) -> None:
    """Drop a broken connection so the next call opens a fresh one."""
    with _owner_conns_lock:
        conn = _owner_conns.pop(key, None)
    if conn is not None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - closing is best-effort
            pass


def _forward_to_owner(
    endpoint: str,
    body: dict,
    timeout_s: float | None = None,
    session_token: str = "",
    epoch: int = 0,
) -> tuple[int, dict] | None:
    """POST ``body`` to ``{endpoint}/write``; forward (status, payload).

    A reachable owner's status is forwarded faithfully (200/400/409/500...)
    with its JSON payload; a non-JSON error body is mapped onto a generic
    ``{"error": "OwnerError", "detail": ...}`` payload so the response stays
    a JSON object. Only a failure before sending returns None for fenced
    fallback. Once sending begins, failures return WriteOutcomeUnknown;
    they must never trigger replay or local execution.
    """
    if timeout_s is None:
        timeout_s = _forward_timeout_s()
    # Local validation/serialization errors are not ambiguous transport errors.
    headers = {"Content-Type": "application/json"}
    if session_token:
        headers["Authorization"] = f"Bearer {session_token}"
    if epoch:
        headers["X-Tessera-Epoch"] = str(epoch)
    payload_bytes = json.dumps(body).encode("utf-8")
    key, conn = _owner_connection(endpoint, timeout_s)
    try:
        if conn.sock is None:
            conn.connect()  # No HTTP request has been sent if this fails.
    except (OSError, http.client.HTTPException):
        _drop_owner_connection(key)
        return None

    def unknown(detail: str) -> tuple[int, dict]:
        _drop_owner_connection(key)
        return 502, {"error": "WriteOutcomeUnknown", "detail": detail, "retryable": False}

    try:
        conn.request("POST", "/write", payload_bytes, headers)
        resp = conn.getresponse()
        code, raw = int(resp.status), resp.read()
    except Exception:  # Sending may have partially or fully reached the owner.
        return unknown("owner write may have executed; reconcile before retrying")
    try:
        payload = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, TypeError, ValueError):
        payload = {
            "error": "OwnerError",
            "detail": f"owner returned non-JSON body ({len(raw)} bytes)",
        }
    if not isinstance(payload, dict):
        payload = {"error": "OwnerError", "detail": "owner returned non-object JSON"}
    if 200 <= code < 300 and (
        type(payload.get("version")) is not int or payload["version"] < 1
    ):
        return unknown("owner returned success without a valid durable version")
    return code, payload


# ---------------------------------------------------------------------------
# Response / logging helpers
# ---------------------------------------------------------------------------


def _response(status: int, body: dict, headers: dict | None = None) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", **(headers or {})},
        "body": json.dumps(body),
    }


def _error_response(status: int, error: str, detail: str) -> dict:
    return _response(status, {"error": error, "detail": detail})


def _log(tenant: str, op: str, status: int, ms: float) -> None:
    """One CloudWatch line per request: tenant + op + status + ms + per-segment
    AWS timing breakdown (no SQL)."""
    print(
        json.dumps(
            {
                "msg": "tessera.request",
                "tenant": tenant,
                "op": op,
                "status": status,
                "ms": round(ms),
                **_timings_snapshot(),
            }
        )
    )


def _map_error(exc: Exception) -> tuple[int, str]:
    """Map a tessera exception to (status, error-name)."""
    for cls, status in _ERROR_STATUS:
        if isinstance(exc, cls):
            return status, cls.__name__
    if isinstance(exc, _SQL_STATUS[0]):
        return _SQL_STATUS[1], type(exc).__name__
    return 500, "InternalError"


# ---------------------------------------------------------------------------
# Request parsing
# ---------------------------------------------------------------------------


def _parse_body(event: dict) -> dict:
    raw = event.get("body")
    if raw in (None, ""):
        return {}
    if event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise _HttpError(400, "BadRequest", f"body is not valid base64: {exc}") from None
    try:
        body = json.loads(raw)
    except (TypeError, ValueError):
        raise _HttpError(400, "BadRequest", "body is not valid JSON") from None
    if not isinstance(body, dict):
        raise _HttpError(400, "BadRequest", "body must be a JSON object")
    return body


def _sql_params(body: dict) -> tuple[str, list]:
    """Validate the shared ``{"sql", "params"}`` body fields."""
    sql = body.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise _HttpError(400, "BadRequest", "body must include a non-empty string field 'sql'")
    params = body.get("params", [])
    if not isinstance(params, list):
        raise _HttpError(400, "BadRequest", "field 'params' must be a JSON list (positional '?' binds)")
    return sql, params


def _min_version(headers: dict) -> int | None:
    raw = headers.get("x-tessera-version")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        version = int(str(raw).strip())
    except ValueError:
        raise _HttpError(
            400, "BadRequest", f"header X-Tessera-Version must be an integer, got {raw!r}"
        ) from None
    if version < 1:
        raise _HttpError(400, "BadRequest", "header X-Tessera-Version must be >= 1")
    return version


# ---------------------------------------------------------------------------
# API operations (op names mirror Store methods)
# ---------------------------------------------------------------------------


def _op_create(tenant: str, body: dict) -> dict:
    """POST /{tenant}/create — body ``{"schema": str}`` (optional)."""
    schema = body.get("schema")
    if schema is not None and (not isinstance(schema, str) or not schema.strip()):
        raise _HttpError(400, "BadRequest", "field 'schema' must be a non-empty string when present")
    _backend, store = _get_pair()
    manifest = store.create(tenant, schema)
    return _response(
        201,
        {"tenant": tenant, "version": manifest.version},
        {"X-Tessera-Version": str(manifest.version)},
    )


def _op_write(tenant: str, body: dict) -> dict:
    """POST /{tenant}/write — body ``{"sql", "params"}``, one committed txn.

    Hot-owner fast path first: when a live lease carries an ``endpoint``,
    the write is forwarded to that owner and its (status, payload) response
    is returned verbatim (marker header ``X-Tessera-Owner: 1``) — no lease
    acquisition, no hydration. Only proven pre-send failures fall back to
    the normal store path; ambiguous outcomes remain explicit errors.
    """
    sql, params = _sql_params(body)

    owner_info = _lookup_owner_endpoint(tenant)
    if owner_info:
        endpoint, session_token, epoch = owner_info
        forwarded = _forward_to_owner(
            endpoint, body, session_token=session_token, epoch=epoch
        )
        if forwarded is not None:
            status, payload = forwarded
            if status == 401:
                # Refresh discovery for future calls; this rejection is final.
                _owner_probe.pop(tenant, None)  # invalidate the cached view
                print(
                    json.dumps(
                        {
                            "msg": "tessera.forward.unauthorized",
                            "tenant": tenant,
                            "endpoint": endpoint,
                            "reason": "owner rejected our credential; "
                            "invalidating cached discovery",
                        }
                    )
                )
        if forwarded is not None:
            status, payload = forwarded
            headers = {"X-Tessera-Owner": "1"}
            version = payload.get("version")
            if version is not None:
                headers["X-Tessera-Version"] = str(version)
            return _response(status, payload, headers)
        print(
            json.dumps(
                {
                    "msg": "tessera.forward.fallback",
                    "tenant": tenant,
                    "endpoint": endpoint,
                    "reason": "owner unreachable; executing write locally",
                }
            )
        )

    _backend, store = _get_pair()

    def _txn(conn):
        cur = conn.execute(sql, params)
        conn.commit()
        return {"lastrowid": cur.lastrowid, "rowcount": cur.rowcount}

    if _sticky():
        # Sticky mode: retain the lease across warm invocations. A "lost"
        # retained handle (sandbox was frozen past the TTL, or the lease was
        # stolen) surfaces as LeaseLost; reset the store's handle and retry
        # ONCE with a fresh acquire — fn never ran, so the retry is safe.
        try:
            result, version = store.write(tenant, _txn)
        except LeaseLost:
            store.close()
            result, version = store.write(tenant, _txn)
        # Polite yield (docs/N-WRITERS-PLAN.md): while a promotion marker is
        # live an ephemeral owner is trying to acquire this tenant's lease —
        # and a retained, continuously-renewed sticky lease would starve it
        # forever. Releasing after the write gives the owner's backoff a
        # window; once its endpoint lands in the lease, writes forward and
        # this path is not reached at all.
        if _auto_promote_enabled() and _promotion_in_progress(tenant):
            store.close()
    else:
        try:
            result, version = store.write(tenant, _txn)
        finally:
            store.close()  # lease hygiene: never pin the lease across sandboxes
    return _response(
        200,
        {"result": result, "version": version},
        {"X-Tessera-Version": str(version)},
    )


def _json_safe(value):
    """Map a SQLite value onto a JSON-safe one (bytes -> utf-8 text, with
    unpaired bytes replaced by U+FFFD; None/int/float/str pass through)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _manifest_version(backend: S3DynamoBackend, tenant: str) -> int:
    """Version of the tenant's current manifest; TenantNotFound -> 404."""
    try:
        data, _etag = backend.get(f"{tenant}/{_MANIFEST_SUFFIX}")
    except NotFound:
        raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
    return int(json.loads(data.decode("utf-8"))["version"])


def _op_read(tenant: str, body: dict, headers: dict) -> dict:
    """POST /{tenant}/read — body ``{"sql", "params"}``; optional
    ``X-Tessera-Version`` request header sets ``min_version``."""
    sql, params = _sql_params(body)
    _backend, store = _get_pair()
    requested = _min_version(headers)

    # Store.read does not return the manifest version it hydrated, so the
    # response reports a guaranteed FLOOR: the version probed here (S3 is
    # strongly consistent, so the hydrated state is at least this) and the
    # requested min_version (which Store.read enforces before the query —
    # StaleRead 409 when it does not become visible in read_wait_ms).
    probed = _manifest_version(_backend, tenant)
    floor = max(requested or 0, probed)

    def _query(conn):
        cur = conn.execute(sql, params)
        columns = [d[0] for d in cur.description] if cur.description is not None else []
        rows = [[_json_safe(value) for value in row] for row in cur.fetchall()]
        return columns, rows

    columns, rows = store.read(tenant, _query, min_version=floor or None)
    return _response(
        200,
        {"rows": rows, "columns": columns, "version": floor},
        {"X-Tessera-Version": str(floor)},
    )


# ---------------------------------------------------------------------------
# Routing (API Gateway HTTP API payload v2)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Reference API authentication
#
# The reference stack exposes a public URL with no authorizer: anyone with
# the URL could write to any tenant. A static per-stack token is enough —
# this stack is spun up to measure and then decommissioned, so the goal is
# no publicly writable URL, not building an IdP.
#
# The secret is read ONCE per cold sandbox and cached: reading it per
# request would add an AWS call to the hot path of a 70 ms write.
# ---------------------------------------------------------------------------

_api_key_cache: str | None = None
_api_key_lock = threading.Lock()


def _expected_api_key() -> str:
    """Expected key, cached per sandbox. Empty = auth disabled."""
    global _api_key_cache
    if _api_key_cache is not None:
        return _api_key_cache
    with _api_key_lock:
        if _api_key_cache is not None:
            return _api_key_cache
        value = os.environ.get("API_KEY", "").strip()
        secret_arn = os.environ.get("API_KEY_SECRET_ARN", "").strip()
        if not value and secret_arn:
            try:
                client = boto3.client(
                    "secretsmanager",
                    region_name=os.environ.get("REGION", "us-east-1"),
                )
                value = str(
                    client.get_secret_value(SecretId=secret_arn)["SecretString"]
                ).strip()
            except Exception as exc:  # noqa: BLE001
                # Fail CLOSED: if the key was configured but could not be
                # read, serving without auth would be worse than a 500.
                print(
                    json.dumps(
                        {
                            "msg": "tessera.auth.secret_unavailable",
                            "error": type(exc).__name__,
                        }
                    )
                )
                raise _HttpError(
                    500, "InternalError", "API key configured but unavailable"
                ) from exc
        _api_key_cache = value
        return value


def _authenticate_request(headers: dict) -> None:
    """Validate ``X-Tessera-Key`` against the stack key.

    With no key configured the API stays open (compatibility with existing
    stacks and with local mode); with a key, a request without it or with a
    wrong one gets 401. The comparison is constant-time.
    """
    expected = _expected_api_key()
    if not expected:
        return
    presented = str(headers.get("x-tessera-key") or "")
    if not presented or not hmac.compare_digest(presented, expected):
        raise _HttpError(
            401, "Unauthorized", "missing or invalid X-Tessera-Key header"
        )


def _handle(event: Any) -> tuple[dict, str, str]:
    """Route one HttpApi payload-v2 event; returns (response, tenant, op)."""
    request_context = event.get("requestContext") if isinstance(event, dict) else None
    http = request_context.get("http") if isinstance(request_context, dict) else None
    if not isinstance(http, dict):
        # Direct invokes / non-HttpApi events are server-side misuse: 500,
        # BEFORE any env access (import stays crash-free without env vars).
        raise _HttpError(
            500,
            "InternalError",
            "unrecognized event: missing requestContext.http (expected API Gateway HTTP API payload v2)",
        )
    method = str(http.get("method") or "")
    path = str(http.get("path") or "")
    if method != "POST":
        return (
            _response(
                405,
                {"error": "MethodNotAllowed", "detail": f"method {method!r} not allowed; use POST"},
                {"Allow": "POST"},
            ),
            "-",
            "-",
        )

    segments = [part for part in path.split("/") if part]
    # Only the last two segments matter, so a stage prefix (named stages
    # appear in the raw path) is tolerated.
    if len(segments) < 2:
        raise _HttpError(404, "NotFound", f"path {path!r} does not match /{{tenant}}/{{op}}")
    tenant, op = segments[-2], segments[-1]
    if not _TENANT_RE.match(tenant):
        raise _HttpError(
            400,
            "BadRequest",
            f"invalid tenant {tenant!r}: must match {_TENANT_RE.pattern} (single path segment)",
        )
    if op not in _OPS:
        raise _HttpError(404, "NotFound", f"unknown op {op!r}; expected create|write|read")

    body = _parse_body(event)
    headers = {str(k).lower(): v for k, v in (event.get("headers") or {}).items()}
    _authenticate_request(headers)
    if op == "create":
        response = _op_create(tenant, body)
    elif op == "write":
        response = _op_write(tenant, body)
    else:
        response = _op_read(tenant, body, headers)
    return response, tenant, op


# @spec examples/handler
def handler(event: Any, context: Any) -> dict:
    """API Gateway HTTP API entrypoint (payload v2): POST /{tenant}/{op}."""
    started = time.perf_counter()
    _timings_reset()
    tenant = op = "-"

    def _path_tenant_op() -> tuple[str, str]:
        """Best-effort (tenant, op) from the raw path when _handle raised
        before returning them (e.g. TenantBusy inside _op_write)."""
        try:
            path = str(event["requestContext"]["http"]["path"])
            segments = [part for part in path.split("/") if part]
            if len(segments) >= 2 and _TENANT_RE.match(segments[-2]) and segments[-1] in _OPS:
                return segments[-2], segments[-1]
        except Exception:  # noqa: BLE001 — logging metadata only
            pass
        return "-", "-"

    try:
        response, tenant, op = _handle(event)
    except _HttpError as exc:
        response = _error_response(exc.status, exc.error, str(exc))
    except Exception as exc:  # noqa: BLE001 — last-resort mapping (bad event, missing env, ...)
        status, name = _map_error(exc)
        response = _error_response(status, name, str(exc))
        tenant, op = _path_tenant_op()
        if isinstance(exc, TenantBusy):
            # Sticky-lease contention (A2.3): another sandbox retains the
            # tenant's lease. It frees within the retention window (or the
            # lease TTL if that sandbox died), so tell clients when to retry
            # instead of letting them hammer.
            retry_s = max(1, round(int(os.environ.get("LEASE_TTL_MS", "60000")) / 2000))
            response["headers"]["Retry-After"] = str(retry_s)
            # Auto-promotion (docs/N-WRITERS-PLAN.md): contention on a write
            # is the promotion signal. Best-effort and fail-open; reads never
            # contend (no lease), so op == "write" is implied but cheap to
            # assert via the op var.
            if op == "write":
                _try_promote(tenant)
    _log(tenant, op, response["statusCode"], (time.perf_counter() - started) * 1000.0)
    return response


# ---------------------------------------------------------------------------
# Scheduled compactor / GC (EventBridge)
# ---------------------------------------------------------------------------


def _discover_tenants(backend: S3DynamoBackend) -> list[str]:
    """Tenant ids = distinct first path segment of keys ending in
    ``manifest.json`` (the manifest is the one object every tenant owns).

    Scaling caveat: ``list("")`` walks the ENTIRE bucket every run, which is
    fine for a reference deployment but O(objects) at scale — a production
    fleet would keep a tenant registry or paginate per known prefix instead.
    """
    tenants = set()
    for key in backend.list(""):
        if "/" in key and key.endswith(_MANIFEST_SUFFIX):
            tenants.add(key.split("/", 1)[0])
    return sorted(tenants)


def _delta_stats(backend: S3DynamoBackend, tenant: str) -> tuple[int, int]:
    """(delta count, total delta bytes) of the tenant's current manifest."""
    data, _etag = backend.get(f"{tenant}/{_MANIFEST_SUFFIX}")
    manifest = json.loads(data.decode("utf-8"))
    deltas = manifest.get("deltas") or []
    return len(deltas), sum(int(entry.get("size", 0)) for entry in deltas)


# @spec examples/compactor
def compactor(event: Any, context: Any) -> dict:
    """EventBridge-scheduled GC + opportunistic compaction over all tenants.

    Never raises and never lets one tenant's failure kill the run: every
    per-tenant step is individually guarded and the summary (logged) carries
    the per-tenant errors.
    """
    started = time.perf_counter()
    summary: dict = {"tenants": 0, "gc_ok": 0, "compacted": [], "errors": {}}
    try:
        backend, store = _get_pair()
        tenants = _discover_tenants(backend)
        summary["tenants"] = len(tenants)
        for tenant in tenants:
            problems: list[str] = []
            try:
                report = store.gc(tenant)
                summary["gc_ok"] += 1
                if report["deltas_deleted"] or report["snapshots_deleted"]:
                    print(json.dumps({"msg": "tessera.gc", "tenant": tenant, **report}))
            except Exception as exc:  # noqa: BLE001 — isolation by design
                problems.append(f"gc: {type(exc).__name__}: {exc}")
            try:
                count, total_bytes = _delta_stats(backend, tenant)
                if count > _COMPACT_DELTAS or total_bytes > _COMPACT_BYTES:
                    store.compact(tenant)
                    summary["compacted"].append(tenant)
            except Exception as exc:  # noqa: BLE001 — isolation by design
                problems.append(f"compact: {type(exc).__name__}: {exc}")
            finally:
                store.close()  # release this tenant's lease NOW (see module docstring)
            if problems:
                summary["errors"][tenant] = "; ".join(problems)
    except Exception as exc:  # noqa: BLE001 — a scheduled run must not raise
        summary["fatal"] = f"{type(exc).__name__}: {exc}"
    summary["ms"] = round((time.perf_counter() - started) * 1000.0)
    print(json.dumps({"msg": "tessera.compactor", **summary}))
    return summary
