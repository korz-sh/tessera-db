#!/usr/bin/env python3
"""Cross-database benchmark harness for tessera-db.

Engines (uniform adapter interface):

- ``sqlite``             embedded stdlib sqlite3 (WAL, synchronous=NORMAL).
- ``postgres``           psycopg2 (optional dep; skipped with a note if absent).
- ``mysql``              pymysql (optional dep; skipped with a note if absent).
- ``tessera-hot``        tessera Owner over LocalBackend: HTTP ``/write`` per
                         op; per_commit publish by default (each write waits
                         the publish round-trip), windowed via
                         ``--tessera-publish-mode`` (acks at local commit).
- ``tessera-store``      tessera Store over LocalBackend: full durability path
                         per operation (delta PUT + manifest CAS per txn).
- ``tessera-serverless`` real AWS stack (API Gateway -> Lambda -> S3 +
                         DynamoDB) when ``--api-url`` / ``TESSERA_API_URL`` is
                         set; otherwise skipped with a note. Production-
                         faithful number: per-op cost is ~100x local, so the
                         measured op count is capped at min(N, 300) and the
                         measured n is reported.

Two benchmark types (``--type``):

``micro`` (default — single sequential client, exactly as before):

Workloads (per engine; 20 warmup ops first, never measured):

- ``insert_row``    N single-row INSERTs, commit each (durability per row).
- ``insert_batch``  N rows in batches of ``--batch`` (one transaction per
                    batch; per-row rate reported).
- ``point_select``  N x SELECT payload WHERE id = <seeded id>.
- ``update_row``    N x UPDATE payload WHERE id = ?, commit each.

Report: ops/s, p50/p95 per-op latency (ms) and measured n per workload x
engine. For ``insert_batch`` p50/p95 are per BATCH while ops/s is per ROW.

One tessera-only extra metric: ``cold_first_query_vfs`` — a fresh Store with
an empty cache root reading a 2000-row format-2 tenant through the VFS
(single shot, ms to first result).

``api`` (concurrent, duration-based; ``--vusers`` threads, ``--duration``
seconds per scenario, each vuser with its OWN connection):

- ``api_write_single``  ALL vusers INSERT into ONE tenant/table: sqlite
                        exercises WAL's single writer (busy_timeout=5000,
                        SQLITE_BUSY counted honestly as busy_errors);
                        tessera-serverless funnels everything through ONE
                        lease (503 TenantBusy bursts resolved by the shared
                        retry policy, counted as retries); tessera-hot
                        funnels through ONE owner (lock serializes; p95
                        shows publish-window queueing).
- ``api_write_multi``   vuser i writes tenant i (tenants pre-provisioned).
                        SQL engines use ONE table with a ``tenant_id`` INT
                        column + index (shared-schema pattern — see the
                        asymmetry comment in the source); tessera is
                        DB-per-tenant: vuser i hits tenant i's own endpoint
                        (serverless) / its own Owner (hot).
- ``api_mixed_multi``   same multi-tenant setup, 80% point SELECT (last 10
                        rows of the vuser's tenant) / 20% INSERT. Reads
                        never take leases (tessera) and never block on
                        writers (pg/mysql MVCC; sqlite WAL readers).

Engines for the API suite: sqlite, postgres, mysql, tessera-hot,
tessera-serverless (tessera-store excluded — see the applicability matrix
in the source; its Store path is a single-client durability measurement).
Report per scenario x engine: TOTAL ops/s, aggregate p50/p95 ms and error
counters {busy_errors, http_503, retries, other}.

Fairness: same machine; ``micro`` is one sequential client, ``api`` is
``--vusers`` threads with one connection each. Local tessera modes run
over LocalBackend (local filesystem), NOT S3 — they measure tessera's
coordination overhead (leases, manifest CAS, delta publishing), not
object-storage latency. The serverless mode is the only one exercising real
S3 + DynamoDB + Lambda.

Usage:
    python examples/benchmark.py [--type micro|api|all] [--rows N]
        [--batch B] [--vusers N] [--duration S] [--only e1,e2]
        [--api-url URL] [--pg-dsn DSN] [--mysql-dsn DSN] [--json-out PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

# Make `import tessera` work when invoked as a plain script from the repo
# (script dir is examples/, so the repo root is not on sys.path by default).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tessera.backends.local import LocalBackend  # noqa: E402
from tessera.owner import Owner  # noqa: E402
from tessera.store import Store  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WARMUP = 20


class _BenchBackendFactory:
    """Builds the object backend behind the local tessera engines.

    ``fs`` (default): :class:`LocalBackend` on a per-engine tmp dir —
    near-zero IO latency, so results measure pure coordination overhead.
    ``endpoint``: :class:`S3DynamoBackend` against an S3+DynamoDB-compatible
    endpoint (``--tessera-endpoint``; ministack, DynamoDB-Local+MinIO) —
    real HTTP round-trips and conditional-write semantics on every
    lease/delta/manifest operation; the bench provisions one bucket and one
    lease table per run. ``aws``: same backend against REAL AWS using the
    ambient credential chain and PRE-PROVISIONED ``--tessera-bucket`` /
    ``--tessera-table`` (the bench creates no AWS resources).
    """

    def __init__(self) -> None:
        self.mode = "fs"
        self.endpoint_url: str | None = None
        self.bucket: str | None = None
        self.table_name: str | None = None
        self._shared = None  # (bucket, table, s3_client, dynamodb)

    @property
    def label(self) -> str:
        return {"fs": "LocalBackend (filesystem)",
                "endpoint": f"S3DynamoBackend via {self.endpoint_url}",
                "aws": "S3DynamoBackend via real AWS"}.get(self.mode, self.mode)

    def _provision(self):
        if self._shared is not None:
            return self._shared
        if self.mode == "aws":
            import boto3
            from botocore.config import Config
            # Tight timeouts: the owner lease TTL is 60 s, so a single
            # stalled call under boto3 defaults (60 s connect + 60 s read
            # x retries) can block a renewal past the skew-adjusted expiry
            # and fence a healthy owner. Cap every call well under the TTL.
            cfg = Config(
                connect_timeout=5, read_timeout=10,
                retries={"max_attempts": 3, "mode": "standard"},
            )
            s3 = boto3.client("s3", config=cfg)
            dynamodb = boto3.resource("dynamodb", config=cfg)
            self._shared = (self.bucket, self.table_name, s3, dynamodb)
            return self._shared
        import boto3
        from uuid import uuid4
        run = uuid4().hex[:8]
        bucket = f"tessera-bench-{run}"
        table = f"tessera-bench-leases-{run}"
        kwargs = dict(
            endpoint_url=self.endpoint_url,
            region_name="us-east-1",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        s3 = boto3.client("s3", **kwargs)
        dynamodb = boto3.resource("dynamodb", **kwargs)
        s3.create_bucket(Bucket=bucket)
        dynamodb.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.Table(table).wait_until_exists()
        self._shared = (bucket, table, s3, dynamodb)
        return self._shared

    def make(self, tmp_dir: str):
        """One Backend for one engine/owner; tmp_dir still carries the
        tenant's local cache/objects dir (used only by LocalBackend)."""
        if self.mode == "fs":
            return LocalBackend(os.path.join(tmp_dir, "objects"))
        from tessera.backends.s3dynamo import S3DynamoBackend
        bucket, table, s3, dynamodb = self._provision()
        return S3DynamoBackend(
            bucket=bucket,
            table_name=table,
            region="us-east-1",
            s3_client=s3,
            dynamodb=dynamodb,
        )


_BACKEND_FACTORY = _BenchBackendFactory()


class _CountingBackend:
    """Backend proxy that counts calls by object-key category.

    Wraps whatever ``_BACKEND_FACTORY.make`` returns so a run can report
    MEASURED backend ops per commit — the amortization evidence for
    windowed publish (3 PUTs per publish, not per commit).
    """

    _PUT_KINDS = (
        ("/manifest.json", "manifest"),  # CAS — one per publish
        ("/history/", "history"),
        ("/wal/", "delta"),
        ("/chunks/", "chunk"),
        ("/snap/", "snapshot"),
    )

    def __init__(self, inner) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.puts: dict[str, int] = {k: 0 for _, k in self._PUT_KINDS}
        self.puts["other"] = 0
        self.gets = 0
        self.heads = 0
        self.deletes = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    @staticmethod
    def _kind(key: str) -> str:
        for marker, kind in _CountingBackend._PUT_KINDS:
            if marker in key:
                return kind
        return "other"

    def put(self, key, data, **kwargs):
        with self._lock:
            self.puts[self._kind(key)] += 1
        return self._inner.put(key, data, **kwargs)

    def get(self, key, **kwargs):
        with self._lock:
            self.gets += 1
        return self._inner.get(key, **kwargs)

    def head(self, key):
        with self._lock:
            self.heads += 1
        return self._inner.head(key)

    def delete(self, key):
        with self._lock:
            self.deletes += 1
        return self._inner.delete(key)

    def gc_delete_batch(self, keys, lease):
        with self._lock:
            self.deletes += len(keys)
        return self._inner.gc_delete_batch(keys, lease)

    def totals(self) -> dict:
        with self._lock:
            puts = dict(self.puts)
            return {
                "puts_total": sum(puts.values()),
                "puts_by_key": puts,
                "publishes": puts["manifest"],  # one manifest CAS per publish
                "gets": self.gets,
                "heads": self.heads,
                "deletes": self.deletes,
            }

# Serverless per-op cost is ~100x local (real Lambda + S3 + DynamoDB over
# HTTP); capping keeps the runtime bounded while staying production-faithful.
SERVERLESS_MAX_OPS = 300
# API Gateway request payloads are small: cap rows per multi-row INSERT.
SERVERLESS_MAX_BATCH_ROWS = 10

ENGINE_ORDER = (
    "sqlite",
    "postgres",
    "mysql",
    "tessera-hot",
    "tessera-store",
    "tessera-serverless",
)

WORKLOADS = (
    ("insert_row", "single-row INSERT, commit each (durability per row)"),
    ("insert_batch", "batches of --batch rows, one txn per batch "
                     "(p50/p95 per batch, ops/s per row)"),
    ("point_select", "SELECT payload WHERE id = ? (seeded id)"),
    ("update_row", "UPDATE payload WHERE id = ?, commit each"),
)

INSERT_SQL = "INSERT INTO bench_users (k, payload) VALUES (?, ?)"
SELECT_SQL = "SELECT payload FROM bench_users WHERE id = ?"
UPDATE_SQL = "UPDATE bench_users SET payload = ? WHERE id = ?"

# Schema for the sqlite adapter and Store.create (executescript).
TESSERA_SCHEMA = (
    "CREATE TABLE bench_users ("
    "id INTEGER PRIMARY KEY, k INTEGER NOT NULL, payload TEXT NOT NULL);"
    "CREATE INDEX idx_bench_users_k ON bench_users (k);"
)

POSTGRES_DDL = (
    "DROP TABLE IF EXISTS bench_users",
    "CREATE TABLE bench_users ("
    "id BIGSERIAL PRIMARY KEY, k INTEGER NOT NULL, payload TEXT NOT NULL)",
    "CREATE INDEX idx_bench_users_k ON bench_users (k)",
)

MYSQL_DDL = (
    "DROP TABLE IF EXISTS bench_users",
    "CREATE TABLE bench_users ("
    "id BIGINT AUTO_INCREMENT PRIMARY KEY, k INTEGER NOT NULL, "
    "payload TEXT NOT NULL)",
    "CREATE INDEX idx_bench_users_k ON bench_users (k)",
)

# ---------------------------------------------------------------------------
# API-suite constants (concurrent, duration-based)
# ---------------------------------------------------------------------------
#
# Engine x scenario applicability matrix:
#
#   engine              api_write_single  api_write_multi  api_mixed_multi
#   ------------------  ----------------  ---------------  ---------------
#   sqlite                   yes               yes              yes
#   postgres                 yes               yes              yes
#   mysql                    yes               yes              yes
#   tessera-hot              yes (1 owner)     yes (1 owner/    yes (same
#                                              per vuser)       owners)
#   tessera-store            NO                NO               NO
#   tessera-serverless       yes (1 tenant)    yes (vusers      yes (same
#                                              tenants)         tenants)
#
# tessera-store is EXCLUDED from the API suite: Store.write/read acquire a
# lease + hydrate + run one transaction + publish a delta with a manifest
# CAS per call — a single-client durability path. Driving it with concurrent
# vusers would measure Python lock queueing inside one process, not tessera
# (the concurrent tessera surfaces are the hot Owner's HTTP server and the
# serverless stack). Non-applicable engines are reported as note rows,
# never silently dropped.

API_SCENARIOS = (
    ("api_write_single",
     "ALL vusers INSERT into ONE tenant/table (k=vuser id, payload ~256 B)"),
    ("api_write_multi",
     "vuser i INSERTs into tenant i (tenants pre-provisioned)"),
    ("api_mixed_multi",
     "vuser i -> tenant i: 80% point SELECT (last 10 rows) / 20% INSERT"),
)

API_ENGINE_ORDER = (
    "sqlite",
    "postgres",
    "mysql",
    "tessera-hot",
    "tessera-serverless",
)

# 80% point SELECT / 20% INSERT in api_mixed_multi.
API_MIXED_READ_SHARE = 0.8

# Unique-per-run tenant suffix. In ``aws`` mode the bucket/table outlive the
# process, so fixed names (bench-hot, bench-store, bench-cold-vfs) collide
# with leftovers from a previous run (TenantExists) and silently kill the
# whole benchmark. Stamped names make every run collision-free; leftover
# prefixes are cleaned manually like serverless/API tenants.
_RUN_STAMP = int(time.time())

# Timeouts for the concurrent suite (serverless ops can stall for the full
# retry policy, so the grace is generous; local engines finish in ms).
API_SETUP_TIMEOUT_S = 300.0
API_JOIN_GRACE_S = 150.0

# --- shared-schema pattern for the plain SQL engines -----------------------
#
# ASYMMETRY (deliberate, documented): postgres/mysql/sqlite vusers share ONE
# physical table with a tenant_id INT column + index (the classic shared-
# schema multi-tenant pattern), while tessera is DB-per-tenant — every
# vuser's rows live in that tenant's OWN sqlite database behind its own
# lease/endpoint, so tessera statements carry no tenant_id at all. Both
# shapes serve the same logical workload; the difference in isolation model
# is exactly what the benchmark is measuring.
API_TENANT_ID_SINGLE = 0  # tenant id used by api_write_single (one tenant)

API_INSERT_SHARED_SQL = (
    "INSERT INTO bench_api (tenant_id, k, payload) VALUES (?, ?, ?)"
)
API_RECENT_SHARED_SQL = (
    "SELECT payload FROM bench_api WHERE tenant_id = ? "
    "ORDER BY id DESC LIMIT 10"
)

SQLITE_API_DDL = (
    "CREATE TABLE IF NOT EXISTS bench_api ("
    "id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, "
    "k INTEGER NOT NULL, payload TEXT NOT NULL);"
    "CREATE INDEX IF NOT EXISTS idx_bench_api_tenant "
    "ON bench_api (tenant_id);"
)

POSTGRES_API_DDL = (
    "DROP TABLE IF EXISTS bench_api",
    "CREATE TABLE bench_api ("
    "id BIGSERIAL PRIMARY KEY, tenant_id INTEGER NOT NULL, "
    "k INTEGER NOT NULL, payload TEXT NOT NULL)",
    "CREATE INDEX idx_bench_api_tenant ON bench_api (tenant_id)",
)

MYSQL_API_DDL = (
    "DROP TABLE IF EXISTS bench_api",
    "CREATE TABLE bench_api ("
    "id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant_id INTEGER NOT NULL, "
    "k INTEGER NOT NULL, payload TEXT NOT NULL)",
    "CREATE INDEX idx_bench_api_tenant ON bench_api (tenant_id)",
)

# tessera engines: each tenant's OWN database (bench_users, DB-per-tenant);
# no tenant_id column — the tenant IS the database.
API_TESSERA_RECENT_SQL = (
    "SELECT payload FROM bench_users ORDER BY id DESC LIMIT 10"
)

# sqlite API workers each open the SAME database file (sqlite3 connections
# are not thread-safe shared): WAL keeps readers lock-free, busy_timeout
# makes a blocked writer WAIT instead of failing, and any surviving
# SQLITE_BUSY is counted honestly as busy_errors.
SQLITE_API_BUSY_TIMEOUT_MS = 5000

# Pre-seeded rows per tenant so api_mixed_multi's SELECTs return data.
# Serverless seeds fewer rows: every row costs a real Lambda round-trip.
API_SEED_ROWS_PER_TENANT = 20
API_SERVERLESS_SEED_ROWS_PER_TENANT = 3


# @spec examples/payload
def payload(i: int) -> str:
    """Deterministic ~256 B payload for row *i*."""
    return f"{'x' * (200 + (i % 56))}#{i}"


# @spec examples/skip-engine
class SkipEngine(Exception):
    """Raised by an engine builder when the engine cannot run here."""


def _chunks(seq, size):
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


def _percentile(sorted_values, q):
    """Percentile *q* (0..1) of an already-sorted list."""
    if not sorted_values:
        return 0.0
    idx = max(0, min(len(sorted_values) - 1, round(q * (len(sorted_values) - 1))))
    return sorted_values[idx]


def _stats(lats_ms, ops, n_reported):
    """Result dict from per-op latencies (ms), op count, reported n."""
    lats = sorted(lats_ms)
    elapsed = sum(lats) / 1000.0
    return {
        "p50": _percentile(lats, 0.50),
        "p95": _percentile(lats, 0.95),
        "ops_s": (ops / elapsed) if elapsed > 0 else 0.0,
        "n": n_reported,
    }


def _http_post_json(url, body, timeout=30.0, token=None):
    """POST JSON; returns (status, parsed-body). HTTP error statuses are
    returned, not raised (they are part of the contract). When *token* is
    set it is sent as ``Authorization: Bearer <token>`` — the hot owner
    requires it since AUTH-PLAN; the serverless API authenticates through
    its own gateway surface and gets none."""
    data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url, data=data, method="POST", headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, raw = int(resp.status), resp.read()
    except urllib.error.HTTPError as exc:  # reachable server, error status
        status, raw = int(exc.code), exc.read()
    try:
        out = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, ValueError):
        out = {"error": "BadBody", "detail": f"non-JSON body ({len(raw)} bytes)"}
    return status, out


def _check_http(status, body, op):
    if status != 200:
        raise RuntimeError(f"{op} failed (HTTP {status}): {body}")


def _http_post_quiet(url, body, timeout_s, token=None):
    """POST JSON without raising on transport failure; returns
    (status, parsed-body). Unreachable host / timeout -> (0, {}) so callers
    can turn that into a SkipEngine instead of a traceback. HTTP error
    statuses are still returned (part of the contract, e.g. 409 = exists)."""
    try:
        return _http_post_json(url, body, timeout=timeout_s, token=token)
    except (urllib.error.URLError, TimeoutError):
        return 0, {}


# Client-side retry policy: tenant leases are single-writer; a 503 with
# error "TenantBusy" means another sandbox holds the tenant's lease —
# bounded retry resolves it (once the stale retention lapses or the sandbox
# reuses the lease). Other statuses pass through unchanged.
TENANTBUSY_MAX_RETRIES = 4

# Module-level retry counter, bumped by _post_with_tenantbusy_retry for
# every automatic TenantBusy retry. The API suite snapshots this per
# scenario (the collector's "retries" error counter) so 503 bursts are
# visible in the results, not only in console output.
API_RETRY_STATS = {"retries": 0}
_API_RETRY_STATS_LOCK = threading.Lock()


def _post_with_tenantbusy_retry(post, url, body, timeout_s, log=True):
    """POST *body* via *post* (a (url, body, timeout) -> (status, dict)
    callable), retrying HTTP 503/TenantBusy up to TENANTBUSY_MAX_RETRIES
    extra times with full-jitter backoff (base 0.5 s doubling). Every retry
    attempt increments API_RETRY_STATS["retries"]. Set *log* = False to
    suppress the per-retry console line (the concurrent API suite reports
    the counter instead of interleaved prints)."""
    status, out = post(url, body, timeout_s)
    for attempt in range(TENANTBUSY_MAX_RETRIES):
        if not (status == 503 and out.get("error") == "TenantBusy"):
            return status, out
        delay = random.uniform(0, 0.5 * 2 ** attempt)  # 0.5, 1, 2, 4 ± jitter
        with _API_RETRY_STATS_LOCK:
            API_RETRY_STATS["retries"] += 1
        if log:
            print(
                f"  tessera-serverless: 503 TenantBusy (lease held elsewhere) — "
                f"retrying in {delay:.2f}s "
                f"({attempt + 1}/{TENANTBUSY_MAX_RETRIES})"
            )
        time.sleep(delay)
        status, out = post(url, body, timeout_s)
    return status, out


# ---------------------------------------------------------------------------
# Adapters (uniform interface)
# ---------------------------------------------------------------------------


# @spec examples/adapter
class Adapter:
    """Uniform engine interface.

    - exec_write(sql, params): one committed write (commit per call).
    - exec_many_in_txn([(sql, params), ...]): many statements, ONE transaction.
    - query(sql, params) -> rows.
    """

    max_batch_rows: int | None = None  # rows per multi-row INSERT (None = inf)
    max_ops: int | None = None         # measured-op cap (None = args.rows)

    def exec_write(self, sql, params):
        raise NotImplementedError

    def exec_many_in_txn(self, stmts):
        raise NotImplementedError

    def query(self, sql, params):
        raise NotImplementedError

    def teardown(self):
        pass


# @spec examples/sqlite-adapter
class SqliteAdapter(Adapter):
    """Embedded stdlib sqlite3: autocommit connection, explicit BEGIN/COMMIT
    per write (durability per row), WAL + synchronous=NORMAL."""

    def __init__(self, tmp_dir):
        self._tmp = tmp_dir
        self._conn = sqlite3.connect(
            os.path.join(tmp_dir, "bench.sqlite"), isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(TESSERA_SCHEMA)

    def exec_write(self, sql, params):
        conn = self._conn
        conn.execute("BEGIN")
        try:
            conn.execute(sql, params)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def exec_many_in_txn(self, stmts):
        conn = self._conn
        conn.execute("BEGIN")
        try:
            for sql, params in stmts:
                conn.execute(sql, params)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def query(self, sql, params):
        return self._conn.execute(sql, params).fetchall()

    def teardown(self):
        try:
            self._conn.close()
        finally:
            shutil.rmtree(self._tmp, ignore_errors=True)


class _DbApiAdapter(Adapter):
    """Common shape for psycopg2/pymysql connections.

    The connection runs in autocommit mode (a write commits per call);
    exec_many_in_txn temporarily disables autocommit so the batch is exactly
    ONE committed transaction. SQL uses sqlite-style '?' placeholders,
    rewritten to '%s' for these drivers (our statements are fixed constants).
    """

    def __init__(self, conn):
        self._conn = conn
        self._cur = conn.cursor()

    @staticmethod
    def _sql(sql):
        return sql.replace("?", "%s")

    def exec_write(self, sql, params):
        self._cur.execute(self._sql(sql), list(params))  # autocommit

    def exec_many_in_txn(self, stmts):
        self._conn.autocommit = False
        try:
            for sql, params in stmts:
                self._cur.execute(self._sql(sql), list(params))
            self._conn.commit()
        finally:
            self._conn.autocommit = True

    def query(self, sql, params):
        self._cur.execute(self._sql(sql), list(params))
        return self._cur.fetchall()


# @spec examples/postgres-adapter
class PostgresAdapter(_DbApiAdapter):
    def teardown(self):
        try:
            self._cur.execute("DROP TABLE IF EXISTS bench_users")
            self._conn.commit()
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass
        self._conn.close()


# @spec examples/mysql-adapter
class MysqlAdapter(_DbApiAdapter):
    def teardown(self):
        try:
            self._cur.execute("DROP TABLE IF EXISTS bench_users")
            self._conn.commit()
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass
        self._conn.close()


# @spec examples/tessera-hot-adapter
class TesseraHotAdapter(Adapter):
    """Owner over LocalBackend: POST /write per op, POST /read per query.

    NOTE: in the default per_commit publish mode each /write includes the
    publish round-trip (delta PUT + history PUT + manifest CAS) before it
    ACKs — that is the dominant term in the measured write latency. With
    --tessera-publish-mode=windowed /write ACKs at local commit and every
    commit in the publish window folds into ONE publish (bounded
    durability window); publish_stats() reports the measured backend
    ops-per-commit for the run.
    """

    def __init__(self, owner, base_url, tmp_dir, counter=None):
        self._owner = owner
        self._base = base_url
        self._token = owner.session_token
        self._tmp = tmp_dir
        self._counter = counter  # _CountingBackend or None
        self.writes = 0  # successful /write commits (seed + measured)

    def exec_write(self, sql, params):
        status, body = _http_post_json(
            f"{self._base}/write", {"sql": sql, "params": list(params)},
            token=self._token,
        )
        _check_http(status, body, "POST /write")
        self.writes += 1

    def publish_stats(self):
        """Measured backend ops vs commits — call AFTER teardown so the
        final stop()-publish is included."""
        if self._counter is None:
            return None
        totals = self._counter.totals()
        totals["commits"] = self.writes
        totals["puts_per_commit"] = (
            round(totals["puts_total"] / self.writes, 4)
            if self.writes else None
        )
        totals["publishes_per_commit"] = (
            round(totals["publishes"] / self.writes, 4)
            if self.writes else None
        )
        return totals

    def exec_many_in_txn(self, stmts):
        # /write executes exactly one statement per request (one committed
        # transaction each): the hot owner batches at PUBLISH level, not at
        # the transaction level.
        for sql, params in stmts:
            self.exec_write(sql, params)

    def query(self, sql, params):
        status, body = _http_post_json(
            f"{self._base}/read", {"sql": sql, "params": list(params)},
            token=self._token,
        )
        _check_http(status, body, "POST /read")
        return body.get("rows", [])

    def teardown(self):
        try:
            self._owner.stop()
            self._owner.join(timeout=15)
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass
        shutil.rmtree(self._tmp, ignore_errors=True)


# @spec examples/tessera-store-adapter
class TesseraStoreAdapter(Adapter):
    """Store over LocalBackend: the FULL durability path per operation.

    Every write is one committed transaction whose WAL frames are uploaded as
    a write-once delta and published with a manifest CAS — here against the
    local filesystem (LocalBackend), so this measures tessera's coordination
    overhead (lease + CAS + delta), not S3 latency.
    """

    def __init__(self, store, tenant, tmp_dir):
        self._store = store
        self._tenant = tenant
        self._tmp = tmp_dir

    def exec_write(self, sql, params):
        self._store.write(
            self._tenant, lambda c: (c.execute(sql, params), c.commit())
        )

    def exec_many_in_txn(self, stmts):
        def _txn(conn):
            for sql, params in stmts:
                conn.execute(sql, params)
            conn.commit()

        self._store.write(self._tenant, _txn)

    def query(self, sql, params):
        return self._store.read(
            self._tenant, lambda c: c.execute(sql, params).fetchall()
        )

    def teardown(self):
        try:
            self._store.close()
        finally:
            shutil.rmtree(self._tmp, ignore_errors=True)


# @spec examples/tessera-serverless-adapter
class TesseraServerlessAdapter(Adapter):
    """Real AWS stack (API Gateway -> Lambda -> S3 + DynamoDB) via urllib."""

    max_ops = SERVERLESS_MAX_OPS
    max_batch_rows = SERVERLESS_MAX_BATCH_ROWS

    def __init__(self, api_url, tenant):
        self._url = api_url.rstrip("/")
        self._tenant = tenant

    def _post(self, op, body):
        status, out = _post_with_tenantbusy_retry(
            _http_post_json, f"{self._url}/{self._tenant}/{op}", body, 120.0
        )
        _check_http(status, out, f"POST /{self._tenant}/{op}")
        return out

    def exec_write(self, sql, params):
        self._post("write", {"sql": sql, "params": list(params)})

    def exec_many_in_txn(self, stmts):
        # The HTTP surface takes one statement per request; each /write is its
        # own committed transaction (documented divergence from SQL engines).
        for sql, params in stmts:
            self.exec_write(sql, params)

    def query(self, sql, params):
        body = self._post("read", {"sql": sql, "params": list(params)})
        return body.get("rows", [])

    def teardown(self):
        print(
            f"  tessera-serverless: tenant {self._tenant!r} left in place "
            "(remote stack; no teardown)"
        )


# ---------------------------------------------------------------------------
# Engine builders (return an Adapter or raise SkipEngine)
# ---------------------------------------------------------------------------


# @spec examples/build-sqlite
def build_sqlite(args):
    tmp = tempfile.mkdtemp(prefix="bench-sqlite-")
    return SqliteAdapter(tmp)


# @spec examples/build-postgres
def build_postgres(args):
    try:
        import psycopg2
    except ImportError:
        raise SkipEngine(
            "postgres: skipped (psycopg2 not installed; "
            "pip install 'tessera-db[bench]')"
        ) from None
    try:
        conn = psycopg2.connect(args.pg_dsn)
    except Exception as exc:  # noqa: BLE001 - unreachable server etc.
        raise SkipEngine(
            f"postgres: skipped (cannot connect via {args.pg_dsn!r}: {exc})"
        ) from None
    conn.autocommit = True
    cur = conn.cursor()
    for ddl in POSTGRES_DDL:
        cur.execute(ddl)
    return PostgresAdapter(conn)


def _parse_kv_dsn(dsn):
    """Parse 'host=... port=... user=... password=...' into a dict."""
    out = {}
    for token in dsn.split():
        if "=" in token:
            key, _, value = token.partition("=")
            out[key.strip()] = value.strip()
    return out


# @spec examples/build-mysql
def build_mysql(args):
    try:
        import pymysql  # noqa: F401 - presence check + connect below
    except ImportError:
        raise SkipEngine(
            "mysql: skipped (pymysql not installed; "
            "pip install 'tessera-db[bench]')"
        ) from None
    import pymysql

    cfg = _parse_kv_dsn(args.mysql_dsn)
    try:
        conn = pymysql.connect(
            host=cfg.get("host", "127.0.0.1"),
            port=int(cfg.get("port", "3306")),
            user=cfg.get("user", "root"),
            password=cfg.get("password", ""),
            autocommit=True,
            charset="utf8mb4",
        )
    except Exception as exc:  # noqa: BLE001 - unreachable server etc.
        raise SkipEngine(
            f"mysql: skipped (cannot connect via {args.mysql_dsn!r}: {exc})"
        ) from None
    cur = conn.cursor()
    cur.execute(
        "CREATE DATABASE IF NOT EXISTS tessera_bench CHARACTER SET utf8mb4"
    )
    conn.select_db("tessera_bench")
    for ddl in MYSQL_DDL:
        cur.execute(ddl)
    return MysqlAdapter(conn)


def _seed_store_rows(store, tenant, n, batch, start_k=1):
    """Seed *n* rows into *tenant* via the Store write path (per batch)."""
    rows = [(k, payload(k)) for k in range(start_k, start_k + n)]

    def _txn_factory(chunk):
        def _fn(conn):
            conn.executemany(INSERT_SQL, chunk)
            conn.commit()

        return _fn

    for chunk in _chunks(rows, batch):
        store.write(tenant, _txn_factory(chunk))


# @spec examples/build-tessera-hot
def build_tessera_hot(args):
    """LocalBackend -> Store.create + migrate_to_v2 -> Owner (HTTP).

    Seeding happens through the Owner's own HTTP write path (via the uniform
    _seed_adapter), not through the Store, so setup exercises the same
    surface the measured workloads do.
    """
    tmp = tempfile.mkdtemp(prefix="bench-hot-")
    backend = _CountingBackend(_BACKEND_FACTORY.make(tmp))
    cache_root = os.path.join(tmp, "cache")
    store = Store(backend, root=cache_root, ttl_ms=60_000)
    tenant = f"bench-hot-{_RUN_STAMP}"
    store.create(tenant, TESSERA_SCHEMA)
    store.migrate_to_v2(tenant)
    store.close()  # release the lease so the Owner can acquire it

    owner = Owner(
        tenant,
        backend,
        ttl_ms=60_000,
        publish_mode=args.tessera_publish_mode,
        publish_every_s=args.tessera_publish_every_s,
        idle_s=600.0,
        root=cache_root,
    )
    owner.serve_in_background()
    deadline = time.monotonic() + 20.0
    while not owner.serving:
        if time.monotonic() > deadline:
            owner.stop()
            raise RuntimeError("tessera-hot: owner did not start serving")
        time.sleep(0.05)
    base = f"http://127.0.0.1:{owner.http_port}"
    status, body = _http_post_json(
        f"{base}/read", {"sql": "SELECT 1", "params": []},
        token=owner.session_token,
    )
    _check_http(status, body, "POST /read (warm-up probe)")
    if args.tessera_publish_mode == "windowed":
        print(
            "  NOTE tessera-hot: windowed publish — /write ACKs at local "
            "commit; commits inside "
            f"{args.tessera_publish_every_s}s fold into ONE publish "
            "(bounded durability window; backend ops-per-commit in JSON meta)."
        )
    else:
        print(
            "  NOTE tessera-hot: per_commit publish — each /write waits for "
            "the delta+history+manifest CAS round-trip (write latency ≈ "
            "publish latency). --tessera-publish-mode=windowed amortizes "
            "that cost across commits inside the publish window."
        )
    return TesseraHotAdapter(owner, base, tmp, backend)


# @spec examples/build-tessera-store
def build_tessera_store(args):
    tmp = tempfile.mkdtemp(prefix="bench-store-")
    backend = _BACKEND_FACTORY.make(tmp)
    store = Store(backend, root=os.path.join(tmp, "cache"), ttl_ms=60_000)
    tenant = f"bench-store-{_RUN_STAMP}"
    store.create(tenant, TESSERA_SCHEMA)
    return TesseraStoreAdapter(store, tenant, tmp)


# @spec examples/build-tessera-serverless
def build_tessera_serverless(args):
    api_url = args.api_url or (os.environ.get("TESSERA_API_URL") or "").strip()
    if not api_url:
        raise SkipEngine(
            "tessera-serverless: skipped (no --api-url and TESSERA_API_URL "
            "is unset)"
        )
    tenant = f"bench-srv-{int(time.time())}"
    status, body = _post_with_tenantbusy_retry(
        _http_post_quiet,
        f"{api_url.rstrip('/')}/{tenant}/create", {"schema": TESSERA_SCHEMA},
        120.0,
    )
    if status not in (200, 201, 409):  # 409 = already exists: tolerated
        raise SkipEngine(
            f"tessera-serverless: skipped (create returned HTTP {status}: "
            f"{body})"
        )
    print(
        f"  tessera-serverless: tenant {tenant!r} ready (status {status}); "
        f"capping measured ops at min(N, {SERVERLESS_MAX_OPS}) — per-op cost "
        "is ~100x local (real Lambda + S3 + DynamoDB over HTTP); the "
        "measured n is reported per workload."
    )
    return TesseraServerlessAdapter(api_url, tenant)


ENGINE_BUILDERS = (
    ("sqlite", build_sqlite),
    ("postgres", build_postgres),
    ("mysql", build_mysql),
    ("tessera-hot", build_tessera_hot),
    ("tessera-store", build_tessera_store),
    ("tessera-serverless", build_tessera_serverless),
)


# ---------------------------------------------------------------------------
# Workload measurement
# ---------------------------------------------------------------------------


def _effective_n(adapter, n):
    cap = adapter.max_ops
    return min(n, cap) if cap else n


def _effective_batch(adapter, batch):
    return min(batch, adapter.max_batch_rows) if adapter.max_batch_rows else batch


def _multirow_insert(m):
    """Single multi-row INSERT statement covering *m* (k, payload) pairs."""
    return "INSERT INTO bench_users (k, payload) VALUES " + ",".join(
        ["(?, ?)"] * m
    )


def _seed_adapter(adapter, args):
    """Seed rows through the adapter's OWN write path (setup, untimed) and
    return the seeded row ids (workload targets are chosen from these)."""
    n = _effective_n(adapter, args.rows)
    batch = _effective_batch(adapter, args.batch)
    for chunk in _chunks(range(args.rows), batch):
        sql = _multirow_insert(len(chunk))
        params = []
        for k in chunk:
            params.extend((k, payload(k)))
        adapter.exec_write(sql, params)
    rows = adapter.query("SELECT id FROM bench_users ORDER BY id", [])
    return [row[0] for row in rows]


# @spec examples/measure-workload
def measure_workload(adapter, workload, n, batch, ids):
    """Run one workload against *adapter*; returns the stats dict."""
    n = _effective_n(adapter, n)
    effective_batch = _effective_batch(adapter, batch)
    lats = []
    seq = 0  # global row counter for freshly generated rows

    if workload == "insert_row":
        k_base = 1_000_000_000
        for _ in range(WARMUP):
            adapter.exec_write(INSERT_SQL, (k_base + seq, payload(k_base + seq)))
            seq += 1
        for _ in range(n):
            k = k_base + WARMUP + seq
            t0 = time.perf_counter()
            adapter.exec_write(INSERT_SQL, (k, payload(k)))
            lats.append((time.perf_counter() - t0) * 1000.0)
        return _stats(lats, n, n)

    if workload == "insert_batch":
        k_base = 2_000_000_000
        for warm_rows in _chunks(range(WARMUP), effective_batch):
            stmts = []
            for _ in warm_rows:
                stmts.append((INSERT_SQL, (k_base + seq, payload(k_base + seq))))
                seq += 1
            adapter.exec_many_in_txn(stmts)
        done = 0
        for chunk in _chunks(range(n), effective_batch):
            stmts = []
            for _ in chunk:
                k = k_base + 10_000_000 + seq
                stmts.append((INSERT_SQL, (k, payload(k))))
                seq += 1
            t0 = time.perf_counter()
            adapter.exec_many_in_txn(stmts)
            lats.append((time.perf_counter() - t0) * 1000.0)
            done += len(chunk)
        return _stats(lats, done, done)  # per-batch lats; ops/s = rows/s

    if workload == "point_select":
        if not ids:
            raise RuntimeError("point_select: no seeded rows available")
        for i in range(WARMUP):
            adapter.query(SELECT_SQL, (ids[i % len(ids)],))
        for i in range(n):
            t0 = time.perf_counter()
            rows = adapter.query(SELECT_SQL, (ids[i % len(ids)],))
            lats.append((time.perf_counter() - t0) * 1000.0)
            if i == 0:
                first = rows[0][0] if rows else None
                if not isinstance(first, str) or not first.startswith("x"):
                    raise RuntimeError(
                        f"point_select sanity check failed: {rows[:1]!r}"
                    )
        return _stats(lats, n, n)

    if workload == "update_row":
        k_base = 3_000_000_000
        for i in range(WARMUP):
            adapter.exec_write(
                UPDATE_SQL, (payload(k_base + i), ids[i % len(ids)])
            )
        for i in range(n):
            t0 = time.perf_counter()
            adapter.exec_write(
                UPDATE_SQL, (payload(k_base + WARMUP + i), ids[i % len(ids)])
            )
            lats.append((time.perf_counter() - t0) * 1000.0)
        return _stats(lats, n, n)

    raise ValueError(f"unknown workload: {workload}")


# ---------------------------------------------------------------------------
# API suite: concurrent, realistic API traffic (duration-based)
# ---------------------------------------------------------------------------


class _HttpStatusError(RuntimeError):
    """HTTP-level failure with the status attached. API-suite workers use it
    to tell a surfaced HTTP 503 apart from other failures."""

    def __init__(self, status, body):
        self.status = status
        super().__init__(f"HTTP {status}: {body}")


class _Countdown:
    """Tiny count-down latch for the vuser startup rendezvous (all workers
    report ready, then the main thread releases the start gate)."""

    def __init__(self, n):
        self._cv = threading.Condition()
        self._left = n

    def count_down(self):
        with self._cv:
            self._left -= 1
            if self._left <= 0:
                self._cv.notify_all()

    def wait(self, timeout):
        with self._cv:
            self._cv.wait_for(lambda: self._left <= 0, timeout)
            return self._left <= 0


class _ApiCollector:
    """Thread-safe collector for one scenario x engine run.

    Workers append one record per finished op under a lock; percentiles are
    computed post-hoc from the collected latency list. Successful ops drive
    p50/p95 and n; ALL finished ops (successful or not) count toward the
    TOTAL ops/s. Error counters: busy_errors (sqlite SQLITE_BUSY),
    http_503 (a 503 that survived the retry policy), retries (automatic
    TenantBusy retries performed by the shared helper), other."""

    def __init__(self):
        self._lock = threading.Lock()
        self._lats = []
        self._ops = 0
        self._errors = {
            "busy_errors": 0,
            "http_503": 0,
            "retries": 0,
            "other": 0,
        }

    def record(self, lat_ms, error_key=None):
        with self._lock:
            self._ops += 1
            if error_key is None:
                self._lats.append(lat_ms)
            else:
                self._errors[error_key] += 1

    def take_retries(self):
        """Snapshot-and-reset the module-level TenantBusy retry counter into
        this scenario's ``retries`` bucket (scenarios run sequentially)."""
        with _API_RETRY_STATS_LOCK:
            self._errors["retries"] = API_RETRY_STATS["retries"]
            API_RETRY_STATS["retries"] = 0

    def result(self, elapsed_s, note=""):
        with self._lock:
            lats = sorted(self._lats)
            ops = self._ops
            errors = dict(self._errors)
        return {
            "ops_s": (ops / elapsed_s) if elapsed_s > 0 else 0.0,
            "p50": _percentile(lats, 0.50),
            "p95": _percentile(lats, 0.95),
            "n": len(lats),
            "errors": errors,
            "note": note,
        }


def _api_worker_loop(worker_id, client, collector, start_gate, duration,
                     read_share):
    """One vuser: run ops against its OWN client until the scenario
    deadline. Each op is one committed write (or one point SELECT when
    *read_share* > 0). Error classification is honest: sqlite SQLITE_BUSY
    ('database is locked') -> busy_errors, HTTP 503 surviving the retry
    policy -> http_503, everything else -> other."""
    start_gate.wait()
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        is_read = read_share > 0.0 and random.random() < read_share
        t0 = time.perf_counter()
        error_key = None
        try:
            if is_read:
                client.read_recent(worker_id)
            else:
                client.write(worker_id)
        except sqlite3.OperationalError as exc:
            error_key = (
                "busy_errors" if "locked" in str(exc).lower() else "other"
            )
        except _HttpStatusError as exc:
            error_key = "http_503" if exc.status == 503 else "other"
        except Exception:  # noqa: BLE001 - any failure is an honest error
            error_key = "other"
        collector.record((time.perf_counter() - t0) * 1000.0, error_key)


def _api_tid_single(_worker_id):
    """api_write_single: every vuser writes the ONE shared tenant."""
    return API_TENANT_ID_SINGLE


def _api_tid_multi(worker_id):
    """api_write_multi / api_mixed_multi: vuser i owns tenant i."""
    return worker_id


class _SqliteApiWorker:
    """One vuser's private sqlite connection to the SHARED bench database
    file (sqlite3 connections must not be shared across threads, so each
    vuser opens the same file itself).

    WAL + synchronous=NORMAL + busy_timeout=5000: readers never block behind
    the single writer; writers queue on the write lock for up to 5 s and any
    surviving SQLITE_BUSY surfaces as an honest busy_errors count."""

    def __init__(self, db_path, tenant_of):
        self._conn = sqlite3.connect(
            db_path,
            timeout=SQLITE_API_BUSY_TIMEOUT_MS / 1000.0,
            isolation_level=None,  # autocommit: each INSERT commits
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query — PRAGMA with a module-level int constant, no untrusted input
        self._conn.execute(f"PRAGMA busy_timeout={SQLITE_API_BUSY_TIMEOUT_MS}")
        self._tenant_of = tenant_of
        self._seq = 0

    def write(self, worker_id):
        self._seq += 1
        self._conn.execute(
            API_INSERT_SHARED_SQL,
            (self._tenant_of(worker_id), worker_id,
             payload(worker_id * 1_000_003 + self._seq)),
        )

    def read_recent(self, worker_id):
        self._conn.execute(
            API_RECENT_SHARED_SQL, (self._tenant_of(worker_id),)
        ).fetchall()

    def close(self):
        self._conn.close()


class _SharedTableApiWorker:
    """One vuser's private autocommit connection (psycopg2 or pymysql) to
    the shared bench_api table. sqlite-style '?' placeholders are rewritten
    to '%s' for these drivers (statements are fixed constants).

    NOTE the deliberate asymmetry vs the tessera workers below: these
    engines use the SHARED-SCHEMA multi-tenant pattern (ONE table, indexed
    tenant_id column, all vusers in one database) while tessera is
    DB-per-tenant — each vuser's tenant is its own sqlite database with its
    own lease/endpoint, so tessera statements carry no tenant_id. The
    isolation-model difference is what the benchmark measures."""

    def __init__(self, conn, tenant_of):
        self._conn = conn
        self._cur = conn.cursor()
        self._tenant_of = tenant_of
        self._seq = 0

    @staticmethod
    def _sql(sql):
        return sql.replace("?", "%s")

    def write(self, worker_id):
        self._seq += 1
        self._cur.execute(
            self._sql(API_INSERT_SHARED_SQL),
            [self._tenant_of(worker_id), worker_id,
             payload(worker_id * 1_000_003 + self._seq)],
        )  # autocommit

    def read_recent(self, worker_id):
        self._cur.execute(
            self._sql(API_RECENT_SHARED_SQL), [self._tenant_of(worker_id)]
        )
        self._cur.fetchall()

    def close(self):
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass


class _TesseraHttpApiWorker:
    """One vuser's HTTP client for the tessera API-suite engines (hot Owner
    or serverless). The endpoint is pre-resolved per worker at construction:
    api_write_single points EVERY worker at the ONE owner/tenant, the multi
    scenarios at worker i's OWN tenant (DB-per-tenant; see the asymmetry
    NOTE in _SharedTableApiWorker).

    tessera-serverless writes go through _post_with_tenantbusy_retry (the
    existing full-jitter retry policy): a contended lease shows up as
    counted retries, and a 503 that survives the bounded retries surfaces
    as http_503. Latencies include the retry sleeps — honest queueing."""

    def __init__(self, write_url, read_url, tenant_label, serverless=False,
                 token=None):
        self._write_url = write_url
        self._read_url = read_url
        self._tenant = tenant_label
        self._serverless = serverless
        self._token = token
        self._seq = 0

    def _post(self, url, body):
        if self._serverless:
            status, out = _post_with_tenantbusy_retry(
                _http_post_json, url, body, 120.0, log=False
            )
        else:
            try:
                status, out = _http_post_json(
                    url, body, timeout=30.0, token=self._token
                )
            except (urllib.error.URLError, TimeoutError) as exc:
                raise _HttpStatusError(
                    0, f"transport: {type(exc).__name__}: {exc}"
                ) from None
        if status != 200:
            raise _HttpStatusError(status, out)
        return out

    def write(self, worker_id):
        self._seq += 1
        self._post(
            self._write_url,
            {"sql": INSERT_SQL,  # per-tenant database: no tenant_id column
             "params": [worker_id,
                        payload(worker_id * 1_000_003 + self._seq)]},
        )

    def read_recent(self, worker_id):
        self._post(self._read_url,
                   {"sql": API_TESSERA_RECENT_SQL, "params": []})

    def close(self):
        pass  # stateless urllib; nothing to close per worker


def _start_hot_owner(tmp_dir, tenant, publish_mode="per_commit",
                     publish_every_s=1.0):
    """Store.create + migrate_to_v2 + Owner(serve_in_background) for ONE
    tenant over its OWN LocalBackend; returns (owner, base_url)."""
    backend = _BACKEND_FACTORY.make(tmp_dir)
    cache_root = os.path.join(tmp_dir, "cache")
    store = Store(backend, root=cache_root, ttl_ms=60_000)
    store.create(tenant, TESSERA_SCHEMA)
    store.migrate_to_v2(tenant)
    store.close()  # release the lease so the Owner can acquire it
    owner = Owner(
        tenant,
        backend,
        ttl_ms=60_000,
        publish_mode=publish_mode,
        publish_every_s=publish_every_s,
        idle_s=600.0,
        root=cache_root,
    )
    owner.serve_in_background()
    deadline = time.monotonic() + 20.0
    while not owner.serving:
        if time.monotonic() > deadline:
            owner.stop()
            raise RuntimeError(
                f"tessera-hot: owner for {tenant!r} did not start serving"
            )
        time.sleep(0.05)
    base = f"http://127.0.0.1:{owner.http_port}"
    status, body = _http_post_json(
        f"{base}/read", {"sql": "SELECT 1", "params": []},
        token=owner.session_token,
    )
    _check_http(status, body, "POST /read (warm-up probe)")
    return owner, base


def _stop_hot_owners(owners):
    """Best-effort teardown of every Owner (stop() publishes pending
    deltas, like the micro suite's teardown)."""
    for owner in owners:
        try:
            owner.stop()
            owner.join(timeout=15)
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass


def _api_worker(worker_id, make_client, collector, ready, start_gate, duration,
                read_share, setup_errors):
    """Vuser thread body: build the client (own connection), do one untimed
    warmup op, wait for the start gate, loop until the deadline."""
    client = None
    try:
        try:
            client = make_client(worker_id)
            client.write(worker_id)  # untimed warmup op
        except Exception as exc:  # noqa: BLE001 - worker setup failure
            setup_errors.append(f"worker {worker_id}: setup failed: {exc}")
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
                client = None
            return
    finally:
        ready.count_down()
    start_gate.wait()
    try:
        _api_worker_loop(
            worker_id, client, collector, start_gate, duration, read_share
        )
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - teardown is best-effort
            pass


def _run_api_scenario(vusers, duration, make_client, read_share=0.0):
    """Run ONE API scenario: *vusers* threads, each with its OWN client
    (own connection), looping ops until the per-scenario deadline.
    *read_share* is the fraction of reads (0.8 for api_mixed_multi, else 0).
    Returns the collector result dict (percentiles computed post-hoc)."""
    collector = _ApiCollector()
    setup_errors = []
    ready = _Countdown(vusers)
    start_gate = threading.Event()

    threads = []
    for i in range(vusers):
        thread = threading.Thread(
            target=_api_worker,
            args=(i, make_client, collector, ready, start_gate, duration,
                  read_share, setup_errors),
            daemon=True,
        )
        threads.append(thread)
        thread.start()
    if not ready.wait(API_SETUP_TIMEOUT_S):
        setup_errors.append("setup timeout: some workers never became ready")
    started = time.perf_counter()
    start_gate.set()
    for thread in threads:
        thread.join(timeout=duration + API_JOIN_GRACE_S)
    elapsed = time.perf_counter() - started
    return collector.result(elapsed, note="; ".join(setup_errors))


def _with_note(result, text):
    """Attach an engine note to a scenario result (keeping any setup-error
    note that the runner already added)."""
    existing = result.get("note", "")
    result["note"] = f"{existing}; {text}" if existing else text
    return result


def _api_engine_sqlite(args, vusers, duration):
    """API suite for sqlite: ONE shared database FILE; every vuser opens its
    OWN connection. WAL keeps the many reads lock-free; the single-writer
    rule serializes the vusers' INSERTs via busy_timeout=5000, and a
    surviving SQLITE_BUSY lands in busy_errors (counted honestly)."""
    tmp = tempfile.mkdtemp(prefix="bench-api-sqlite-")
    db_path = os.path.join(tmp, "bench-api.sqlite")
    setup = sqlite3.connect(db_path, isolation_level=None)
    setup.execute("PRAGMA journal_mode=WAL")
    setup.execute("PRAGMA synchronous=NORMAL")
    setup.executescript(SQLITE_API_DDL)
    # Seed reads-per-tenant rows (setup, untimed) so api_mixed_multi's
    # point SELECTs return data.
    for tid in range(vusers):
        for j in range(API_SEED_ROWS_PER_TENANT):
            k = 1_000_000 + tid * 100_000 + j
            setup.execute(API_INSERT_SHARED_SQL,
                          (tid, tid, payload(k)))
    setup.close()

    def _make_single(_worker_id):
        return _SqliteApiWorker(db_path, _api_tid_single)

    def _make_multi(worker_id):
        return _SqliteApiWorker(db_path, _api_tid_multi)

    results = {}
    try:
        results["api_write_single"] = _with_note(
            _run_api_scenario(vusers, duration, _make_single),
            "one shared WAL db file, per-vuser connections "
            "(busy_timeout=5000); single writer: surviving SQLITE_BUSY is "
            "counted in busy_errors",
        )
        results["api_write_multi"] = _with_note(
            _run_api_scenario(vusers, duration, _make_multi),
            "shared-schema pattern: ONE table + indexed tenant_id column "
            "(SQL engines share one schema; tessera is DB-per-tenant)",
        )
        results["api_mixed_multi"] = _with_note(
            _run_api_scenario(
                vusers, duration, _make_multi, API_MIXED_READ_SHARE
            ),
            "reads take no writer locks (WAL read snapshots); same "
            "shared-schema table",
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return results, []


def _api_engine_postgres(args, vusers, duration):
    """API suite for postgres: one DDL pass, then ONE AUTOCOMMIT CONNECTION
    PER VUSER (psycopg2 connections are not thread-safe shared) against the
    shared bench_api table (auto PK -> near-linear concurrent INSERTs)."""
    try:
        import psycopg2
    except ImportError:
        raise SkipEngine(
            "postgres: skipped (psycopg2 not installed; "
            "pip install 'tessera-db[bench]')"
        ) from None
    try:
        admin = psycopg2.connect(args.pg_dsn)
    except Exception as exc:  # noqa: BLE001 - unreachable server etc.
        raise SkipEngine(
            f"postgres: skipped (cannot connect via {args.pg_dsn!r}: {exc})"
        ) from None
    admin.autocommit = True
    cur = admin.cursor()
    for ddl in POSTGRES_API_DDL:
        cur.execute(ddl)
    insert_pg = API_INSERT_SHARED_SQL.replace("?", "%s")
    for tid in range(vusers):  # seed reads for api_mixed_multi (untimed)
        for j in range(API_SEED_ROWS_PER_TENANT):
            k = 1_000_000 + tid * 100_000 + j
            cur.execute(insert_pg, (tid, tid, payload(k)))
    cur.close()
    admin.close()

    def _connect():
        conn = psycopg2.connect(args.pg_dsn)
        conn.autocommit = True
        return conn

    def _make_single(_worker_id):
        return _SharedTableApiWorker(_connect(), _api_tid_single)

    def _make_multi(worker_id):
        return _SharedTableApiWorker(_connect(), _api_tid_multi)

    try:
        results = {
            "api_write_single": _with_note(
                _run_api_scenario(vusers, duration, _make_single),
                "shared table bench_api, auto PK (BIGSERIAL): near-linear "
                "concurrent INSERTs expected",
            ),
            "api_write_multi": _with_note(
                _run_api_scenario(vusers, duration, _make_multi),
                "shared-schema pattern: ONE table, tenant_id column + index "
                "(SQL engines share one schema; tessera is DB-per-tenant — "
                "deliberate asymmetry)",
            ),
            "api_mixed_multi": _with_note(
                _run_api_scenario(
                    vusers, duration, _make_multi, API_MIXED_READ_SHARE
                ),
                "reads never lock writers (MVCC); shared-schema table",
            ),
        }
    finally:
        try:  # teardown is best-effort
            conn = _connect()
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS bench_api")
            cur.close()
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return results, []


def _api_engine_mysql(args, vusers, duration):
    """API suite for mysql: one DDL pass in the tessera_bench database, then
    ONE AUTOCOMMIT CONNECTION PER VUSER against the shared bench_api table
    (AUTO_INCREMENT PK -> near-linear concurrent INSERTs)."""
    try:
        import pymysql  # noqa: F401 - presence check + connect below
    except ImportError:
        raise SkipEngine(
            "mysql: skipped (pymysql not installed; "
            "pip install 'tessera-db[bench]')"
        ) from None
    import pymysql

    cfg = _parse_kv_dsn(args.mysql_dsn)

    def _connect():
        conn = pymysql.connect(
            host=cfg.get("host", "127.0.0.1"),
            port=int(cfg.get("port", "3306")),
            user=cfg.get("user", "root"),
            password=cfg.get("password", ""),
            autocommit=True,
            charset="utf8mb4",
        )
        conn.select_db("tessera_bench")
        return conn

    try:
        admin = _connect()
    except Exception as exc:  # noqa: BLE001 - unreachable server etc.
        raise SkipEngine(
            f"mysql: skipped (cannot connect via {args.mysql_dsn!r}: {exc})"
        ) from None
    admin.cursor().execute(
        "CREATE DATABASE IF NOT EXISTS tessera_bench CHARACTER SET utf8mb4"
    )
    admin.select_db("tessera_bench")
    cur = admin.cursor()
    for ddl in MYSQL_API_DDL:
        cur.execute(ddl)
    insert_my = API_INSERT_SHARED_SQL.replace("?", "%s")
    for tid in range(vusers):  # seed reads for api_mixed_multi (untimed)
        for j in range(API_SEED_ROWS_PER_TENANT):
            k = 1_000_000 + tid * 100_000 + j
            cur.execute(insert_my, (tid, tid, payload(k)))
    cur.close()
    admin.close()

    def _make_single(_worker_id):
        return _SharedTableApiWorker(_connect(), _api_tid_single)

    def _make_multi(worker_id):
        return _SharedTableApiWorker(_connect(), _api_tid_multi)

    try:
        results = {
            "api_write_single": _with_note(
                _run_api_scenario(vusers, duration, _make_single),
                "shared table bench_api, auto PK (AUTO_INCREMENT): "
                "near-linear concurrent INSERTs expected",
            ),
            "api_write_multi": _with_note(
                _run_api_scenario(vusers, duration, _make_multi),
                "shared-schema pattern: ONE table, tenant_id column + index "
                "(SQL engines share one schema; tessera is DB-per-tenant — "
                "deliberate asymmetry)",
            ),
            "api_mixed_multi": _with_note(
                _run_api_scenario(
                    vusers, duration, _make_multi, API_MIXED_READ_SHARE
                ),
                "reads never lock writers (InnoDB MVCC); shared-schema table",
            ),
        }
    finally:
        try:  # teardown is best-effort
            conn = _connect()
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS bench_api")
            cur.close()
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return results, []


def _api_engine_tessera_hot(args, vusers, duration):
    """API suite for tessera-hot.

    api_write_single: ONE owner — every vuser hits its HTTP; the owner's
    internal lock serializes writes (single lease = single writer) and
    each /write waits its own publish round-trip (per_commit mode).

    api_write_multi / api_mixed_multi: ONE OWNER PER TENANT (--vusers
    Owners, each with its own LocalBackend root); worker i -> owner i.
    Reads never touch the lease/publish path. All owners are stopped with
    publish=True in teardown."""
    results = {}

    # ---- api_write_single: one shared owner/tenant
    tmp_single = tempfile.mkdtemp(prefix="bench-api-hot1-")
    owner = None
    try:
        owner, base = _start_hot_owner(
            tmp_single, "bench-api-hot-single",
            publish_mode=args.tessera_publish_mode,
            publish_every_s=args.tessera_publish_every_s,
        )

        def _make_single(_worker_id):
            return _TesseraHttpApiWorker(
                f"{base}/write", f"{base}/read", "bench-api-hot-single",
                token=owner.session_token,
            )

        results["api_write_single"] = _with_note(
            _run_api_scenario(vusers, duration, _make_single),
            f"ONE owner (single lease): writes serialize on the owner's "
            f"lock (publish_mode={args.tessera_publish_mode})",
        )
    finally:
        if owner is not None:
            _stop_hot_owners([owner])
        shutil.rmtree(tmp_single, ignore_errors=True)

    # ---- multi: one owner per tenant (shared across the 2 scenarios)
    tmp_dirs, owners, bases = [], [], []
    try:
        for i in range(vusers):
            tmp = tempfile.mkdtemp(prefix=f"bench-api-hot-{i}-")
            owner, base = _start_hot_owner(
                tmp, f"bench-api-hot-t{i}",
                publish_mode=args.tessera_publish_mode,
                publish_every_s=args.tessera_publish_every_s,
            )
            tmp_dirs.append(tmp)
            owners.append(owner)
            bases.append(base)
        print(f"  tessera-hot: {vusers} owners serving (one per tenant)")
        for i, owner in enumerate(owners):  # seed reads (setup, untimed)
            for j in range(API_SEED_ROWS_PER_TENANT):
                k = 1_000_000 + i * 100_000 + j
                status, body = _http_post_json(
                    f"{bases[i]}/write",
                    {"sql": INSERT_SQL, "params": [k, payload(k)]},
                    token=owner.session_token,
                )
                _check_http(status, body, "POST /write (api seed)")

        def _make_multi(worker_id):
            return _TesseraHttpApiWorker(
                f"{bases[worker_id]}/write", f"{bases[worker_id]}/read",
                f"tenant-{worker_id}",
                token=owners[worker_id].session_token,
            )

        results["api_write_multi"] = _with_note(
            _run_api_scenario(vusers, duration, _make_multi),
            f"{vusers} Owners (one per tenant, DB-per-tenant): worker i -> "
            "owner i",
        )
        results["api_mixed_multi"] = _with_note(
            _run_api_scenario(
                vusers, duration, _make_multi, API_MIXED_READ_SHARE
            ),
            "reads take no leases (owner serves from the warm local "
            "database); 80% point SELECT / 20% INSERT",
        )
    finally:
        _stop_hot_owners(owners)
        for tmp in tmp_dirs:
            shutil.rmtree(tmp, ignore_errors=True)
    return results, []


def _api_engine_tessera_serverless(args, vusers, duration):
    """API suite for tessera-serverless (real AWS stack).

    api_write_single: ONE tenant/lease — concurrent vusers trigger 503
    TenantBusy bursts, absorbed by the existing full-jitter retry policy
    (counted per scenario as retries; a 503 that survives the bounded
    retries surfaces as http_503).

    api_write_multi / api_mixed_multi: --vusers tenants are PRE-CREATED via
    POST /create (409 tolerated) and SHARED across both scenarios; worker i
    -> tenant i. Tenants stay in place (remote stack; no teardown)."""
    api_url = args.api_url or (os.environ.get("TESSERA_API_URL") or "").strip()
    if not api_url:
        raise SkipEngine(
            "tessera-serverless: skipped (no --api-url and TESSERA_API_URL "
            "is unset)"
        )
    base = api_url.rstrip("/")
    stamp = int(time.time())

    def _create(tenant):
        status, body = _post_with_tenantbusy_retry(
            _http_post_quiet, f"{base}/{tenant}/create",
            {"schema": TESSERA_SCHEMA}, 120.0,
        )
        if status not in (200, 201, 409):  # 409 = already exists: tolerated
            raise SkipEngine(
                f"tessera-serverless: skipped (create for {tenant!r} "
                f"returned HTTP {status}: {body})"
            )

    def _make_client(tenant):
        return _TesseraHttpApiWorker(
            f"{base}/{tenant}/write", f"{base}/{tenant}/read", tenant,
            serverless=True,
        )

    # ---- api_write_single: one shared tenant/lease
    single_tenant = f"bench-api-{stamp}-single"
    _create(single_tenant)
    print(f"  tessera-serverless: single tenant {single_tenant!r} ready")
    results = {
        "api_write_single": _with_note(
            _run_api_scenario(
                vusers, duration, lambda _wid: _make_client(single_tenant)
            ),
            "single tenant lease: concurrent vusers hit 503 TenantBusy "
            "bursts resolved by the retry policy (retries counter; "
            "surfaced 503s after retries exhaust -> http_503)",
        ),
    }

    # ---- multi tenants, SHARED by api_write_multi + api_mixed_multi
    tenants = [f"bench-api-{stamp}-t{i}" for i in range(vusers)]
    for tenant in tenants:
        _create(tenant)  # 409 tolerated
        for j in range(API_SERVERLESS_SEED_ROWS_PER_TENANT):
            _make_client(tenant).write(1_000_000 + j)  # seed reads (untimed)
    print(
        f"  tessera-serverless: {vusers} tenants ready "
        f"({API_SERVERLESS_SEED_ROWS_PER_TENANT} seed rows each)"
    )

    def _make_multi(worker_id):
        return _make_client(tenants[worker_id])

    results["api_write_multi"] = _with_note(
        _run_api_scenario(vusers, duration, _make_multi),
        f"{vusers} pre-created tenants (409 tolerated), worker i -> tenant "
        "i; tenants are SHARED with api_mixed_multi and left in place "
        "(remote stack; no teardown)",
    )
    results["api_mixed_multi"] = _with_note(
        _run_api_scenario(
            vusers, duration, _make_multi, API_MIXED_READ_SHARE
        ),
        "reads take no leases (read path is lease-free); 80% point SELECT "
        "/ 20% INSERT",
    )
    return results, [
        f"tessera-serverless: API-suite tenants 'bench-api-{stamp}-*' left "
        "in place (remote stack; no teardown)"
    ]


API_ENGINE_RUNNERS = (
    ("sqlite", _api_engine_sqlite),
    ("postgres", _api_engine_postgres),
    ("mysql", _api_engine_mysql),
    ("tessera-hot", _api_engine_tessera_hot),
    ("tessera-serverless", _api_engine_tessera_serverless),
)


# @spec examples/run-api-suite
def run_api_suite(selected, args):
    """Run the concurrent API suite for the selected engines.

    Returns ({scenario: {engine: result-dict}}, [notes]). Non-selected and
    non-applicable engines simply don't appear; they are reported as note
    rows by the printer."""
    api_results = {scenario: {} for scenario, _ in API_SCENARIOS}
    notes = []
    for engine, runner in API_ENGINE_RUNNERS:
        if engine not in selected:
            continue
        print(f"\n[api:{engine}] setting up ({args.vusers} vusers, "
              f"{args.duration}s/scenario)...")
        try:
            engine_results, engine_notes = runner(
                args, args.vusers, args.duration
            )
        except SkipEngine as exc:
            notes.append(str(exc))
            continue
        except Exception as exc:  # noqa: BLE001 - setup failure -> note
            notes.append(
                f"{engine}: skipped (api setup failed: "
                f"{type(exc).__name__}: {exc})"
            )
            continue
        for scenario, stats in engine_results.items():
            api_results[scenario][engine] = stats
        notes.extend(engine_notes)
    if "tessera-store" in selected:
        notes.append(
            "tessera-store: excluded from the API suite (single-client "
            "Store durability path; see the engine x scenario matrix "
            "comment in the source)"
        )
    return api_results, notes


# @spec examples/print-api-report
def print_api_report(selected, vusers, duration, api_results, notes):
    """Print the API-suite section: one table per scenario x engine with
    TOTAL ops/s, aggregate p50/p95 and the error counters."""
    print()
    print("=" * 78)
    print(f"=== API (concurrent, {vusers} vusers, {duration}s/scenario) ===")
    print("FAIRNESS: same machine, concurrent vuser threads, EACH with its")
    print("own connection (pg/mysql/sqlite connections are not thread-safe")
    print("when shared; sqlite vusers open the same WAL database file with")
    print("busy_timeout=5000). Percentiles are aggregate across vusers.")
    print("ops/s is TOTAL finished ops (including errors); p50/p95/n cover")
    print("successful ops only. SQL engines share ONE schema (tenant_id")
    print("column + index); tessera is DB-per-tenant — the asymmetry is")
    print("deliberate (see notes). tessera-store is excluded.")
    print("=" * 78)

    if notes:
        print()
        print("Notes:")
        for note in notes:
            print(f"  - {note}")

    header = (
        f"{'engine':<20}{'ops/s':>12}{'p50 ms':>10}{'p95 ms':>10}{'n':>8}"
        "  errors (busy/503/retry/other)"
    )
    for scenario, description in API_SCENARIOS:
        print()
        print(f"== {scenario}: {description}")
        print(header)
        for engine in selected:
            stats = api_results.get(scenario, {}).get(engine)
            if engine not in API_ENGINE_ORDER:
                print(f"{engine:<20}{'(excluded)':>60}")
                continue
            if stats is None:
                print(f"{engine:<20}{'(skipped)':>60}")
                continue
            err = stats["errors"]
            err_txt = (
                f"busy={err['busy_errors']} 503={err['http_503']} "
                f"retry={err['retries']} other={err['other']}"
            )
            print(
                f"{engine:<20}{stats['ops_s']:>12.1f}{stats['p50']:>10.2f}"
                f"{stats['p95']:>10.2f}{stats['n']:>8d}  {err_txt}"
            )
        for engine in selected:
            stats = api_results.get(scenario, {}).get(engine)
            if stats and stats.get("note"):
                print(f"  note {engine}: {stats['note']}")


# ---------------------------------------------------------------------------
# tessera-only metric: cold first query through the VFS
# ---------------------------------------------------------------------------


# @spec examples/cold-first-query-vfs
def cold_first_query_vfs(tmp_dir, n_rows=2000):
    """Time (ms, single shot) of the FIRST read of a 2000-row format-2 tenant
    through a FRESH Store with an EMPTY cache root: the manifest HEAD, VFS
    session registration, chunk downloads and the query itself."""
    backend = _BACKEND_FACTORY.make(tmp_dir)
    seed_cache = os.path.join(tmp_dir, "cache-a")
    store = Store(backend, root=seed_cache)
    tenant = f"bench-cold-vfs-{_RUN_STAMP}"
    store.create(tenant, TESSERA_SCHEMA)
    _seed_store_rows(store, tenant, n_rows, 200)
    store.migrate_to_v2(tenant)
    store.close()

    fresh = Store(backend, root=os.path.join(tmp_dir, "cache-b"))  # empty root
    try:
        started = time.perf_counter()
        fresh.read(
            tenant,
            lambda c: c.execute(
                "SELECT COUNT(*), SUM(LENGTH(payload)) FROM bench_users"
            ).fetchone(),
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    finally:
        fresh.close()
    return elapsed_ms


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


# @spec examples/print-report
def print_report(order, results, skipped, cold_ms):
    print()
    print("=" * 78)
    print("FAIRNESS: same machine, single sequential client; 20 warmup ops "
          "per workload")
    print(f"(never measured). Local tessera backend: {_BACKEND_FACTORY.label}.")
    if _BACKEND_FACTORY.mode == "fs":
        print("LocalBackend = the LOCAL filesystem, NOT S3: results measure")
        print("tessera's coordination overhead (lease + manifest CAS + delta")
        print("publishing), not object-storage latency.")
    else:
        print("The endpoint carries real HTTP + conditional writes, but it")
        print("is NOT production S3/DynamoDB latency — treat numbers as")
        print("'emulated S3' unless the endpoint IS AWS.")
    print("tessera-serverless is the production-faithful path: real AWS S3")
    print("+ DynamoDB + Lambda over HTTP.")
    print("=" * 78)

    if skipped:
        print()
        print("Notes:")
        for note in skipped:
            print(f"  - {note}")

    header = f"{'engine':<20}{'ops/s':>12}{'p50 ms':>10}{'p95 ms':>10}{'n':>8}"
    for workload, description in WORKLOADS:
        print()
        print(f"== {workload}: {description}")
        print(header)
        for engine in order:
            stats = results.get(engine, {}).get(workload)
            if stats is None:
                print(f"{engine:<20}{'(skipped)':>60}")
                continue
            print(
                f"{engine:<20}{stats['ops_s']:>12.1f}{stats['p50']:>10.2f}"
                f"{stats['p95']:>10.2f}{stats['n']:>8d}"
            )

    if cold_ms is not None:
        print()
        print(
            f"== tessera-only: cold_first_query_vfs = {cold_ms:.1f} ms "
            "(single shot; 2000-row format-2 tenant, migrate_to_v2, fresh "
            "Store with an EMPTY cache root: manifest HEAD + VFS session "
            "registration + chunk downloads + first query)"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


# @spec examples/benchmark-parse-args
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Cross-database benchmark for tessera-db "
        "(sqlite, postgres, mysql, tessera x3)."
    )
    parser.add_argument("--rows", type=int, default=1000,
                        help="measured rows/ops per workload (default 1000)")
    parser.add_argument("--batch", type=int, default=100,
                        help="rows per batch transaction (default 100)")
    parser.add_argument(
        "--type", choices=("micro", "api", "all"), default="micro",
        help="benchmark type: micro = sequential single-client workloads "
        "(default, as before); api = concurrent duration-based scenarios "
        "(api_write_single, api_write_multi, api_mixed_multi); "
        "all = micro first, then api",
    )
    parser.add_argument(
        "--vusers", type=int, default=8,
        help="API suite: concurrent virtual users per scenario "
        "(default 8); each vuser gets its own connection",
    )
    parser.add_argument(
        "--duration", type=int, default=30,
        help="API suite: seconds per scenario (default 30)",
    )
    parser.add_argument(
        "--only",
        default="",
        help="comma-separated engines to run; valid: "
        + ",".join(ENGINE_ORDER),
    )
    parser.add_argument(
        "--api-url",
        default=None,
        help="base URL of the tessera serverless stack "
        "(default: env TESSERA_API_URL; unset -> tessera-serverless skipped)",
    )
    parser.add_argument(
        "--pg-dsn",
        default="postgresql://postgres:postgres@localhost:5432/postgres",
        help="psycopg2 DSN for the postgres engine",
    )
    parser.add_argument(
        "--mysql-dsn",
        default="host=127.0.0.1 port=3306 user=root password=bench",
        help="key=value DSN for the mysql adapter",
    )
    parser.add_argument(
        "--json-out", default=None,
        help="write results JSON to this path",
    )
    parser.add_argument(
        "--tessera-backend", choices=["fs", "endpoint", "aws"], default="fs",
        help="object backend behind the local tessera engines: fs=LocalBackend "
        "(default), endpoint=S3DynamoBackend vs --tessera-endpoint "
        "(ministack/MinIO+DynamoDB-Local), aws=real AWS with --tessera-bucket/"
        "--tessera-table",
    )
    parser.add_argument(
        "--tessera-endpoint", default=None,
        help="S3+DynamoDB endpoint URL for --tessera-backend=endpoint "
        "(e.g. http://localhost:4566)",
    )
    parser.add_argument("--tessera-bucket", default=None,
                        help="pre-provisioned bucket for --tessera-backend=aws")
    parser.add_argument("--tessera-table", default=None,
                        help="pre-provisioned lease table for --tessera-backend=aws")
    parser.add_argument(
        "--tessera-publish-mode", choices=["per_commit", "windowed"],
        default="per_commit",
        help="owner publish mode for tessera-hot engines: per_commit "
        "(default; each /write waits for the publish round-trip) or "
        "windowed (/write ACKs at local commit; commits inside "
        "--tessera-publish-every-s fold into ONE publish)",
    )
    parser.add_argument(
        "--tessera-publish-every-s", type=float, default=1.0,
        help="publish cadence for --tessera-publish-mode=windowed "
        "(default 1.0s; bounds the durability window)",
    )
    return parser.parse_args(argv)


# @spec examples/benchmark-main
def main(argv=None):
    args = parse_args(argv)
    if args.rows < 1 or args.batch < 1:
        print("error: --rows and --batch must be >= 1", file=sys.stderr)
        return 2
    if args.vusers < 1 or args.duration < 1:
        print("error: --vusers and --duration must be >= 1", file=sys.stderr)
        return 2

    if args.only.strip():
        selected = [e.strip() for e in args.only.split(",") if e.strip()]
        unknown = [e for e in selected if e not in ENGINE_ORDER]
        if unknown:
            print(
                f"error: unknown engine(s) {unknown!r}; valid: "
                f"{', '.join(ENGINE_ORDER)}",
                file=sys.stderr,
            )
            return 2
    else:
        selected = list(ENGINE_ORDER)

    if args.tessera_backend == "endpoint" and not args.tessera_endpoint:
        print("error: --tessera-backend=endpoint needs --tessera-endpoint",
              file=sys.stderr)
        return 2
    if args.tessera_backend == "aws" and not (
        args.tessera_bucket and args.tessera_table
    ):
        print("error: --tessera-backend=aws needs --tessera-bucket and "
              "--tessera-table (pre-provisioned; the bench creates no AWS "
              "resources)", file=sys.stderr)
        return 2
    _BACKEND_FACTORY.mode = args.tessera_backend
    _BACKEND_FACTORY.endpoint_url = args.tessera_endpoint
    _BACKEND_FACTORY.bucket = args.tessera_bucket
    _BACKEND_FACTORY.table_name = args.tessera_table

    print("tessera-db cross-database benchmark")
    print(f"  rows={args.rows} batch={args.batch} warmup={WARMUP}")
    print(f"  engines: {', '.join(selected)}")
    print(f"  tessera backend: {_BACKEND_FACTORY.label}")
    if args.type in ("api", "all"):
        print(f"  api: vusers={args.vusers} duration={args.duration}s/scenario")

    results: dict[str, dict] = {}
    skipped_notes = []
    api_results: dict[str, dict] = {}
    publish_stats: dict[str, dict] = {}
    cold_ms = None

    if args.type in ("micro", "all"):
        for engine, builder in ENGINE_BUILDERS:
            if engine not in selected:
                continue
            print(f"\n[{engine}] setting up...")
            try:
                adapter = builder(args)
            except SkipEngine as exc:
                skipped_notes.append(str(exc))
                continue
            except Exception as exc:  # noqa: BLE001 - setup failure -> skip
                skipped_notes.append(
                    f"{engine}: skipped (setup failed: "
                    f"{type(exc).__name__}: {exc})"
                )
                continue
            try:
                ids = _seed_adapter(adapter, args)
                engine_results = {}
                for workload, _desc in WORKLOADS:
                    engine_results[workload] = measure_workload(
                        adapter, workload, args.rows, args.batch, ids
                    )
                results[engine] = engine_results
            finally:
                try:
                    adapter.teardown()
                except Exception:  # noqa: BLE001 - teardown is best-effort
                    pass
                stats_fn = getattr(adapter, "publish_stats", None)
                if stats_fn is not None:
                    stats = stats_fn()
                    if stats:
                        publish_stats[engine] = stats
                        print(
                            f"  backend ops: {stats['puts_total']} PUTs / "
                            f"{stats['commits']} commits "
                            f"({stats['puts_per_commit']} PUTs/commit, "
                            f"{stats['publishes']} publishes)"
                        )

        if any(e.startswith("tessera") for e in selected):
            print("\n[cold_first_query_vfs] measuring (tessera-only metric)...")
            cold_tmp = tempfile.mkdtemp(prefix="bench-cold-")
            try:
                cold_ms = cold_first_query_vfs(cold_tmp)
            except Exception as exc:  # noqa: BLE001 - never lose the run
                print(f"  cold_first_query_vfs FAILED: {exc!r}")
                skipped_notes.append(
                    f"cold_first_query_vfs: failed ({exc!r})"
                )
            finally:
                shutil.rmtree(cold_tmp, ignore_errors=True)

        print_report(selected, results, skipped_notes, cold_ms)

    if args.type in ("api", "all"):
        api_results, api_notes = run_api_suite(selected, args)
        print_api_report(
            selected, args.vusers, args.duration, api_results, api_notes
        )

    if args.json_out:
        out = {
            "meta": {
                "type": args.type,
                "rows": args.rows,
                "batch": args.batch,
                "warmup": WARMUP,
                "vusers": args.vusers,
                "api_duration_s": args.duration,
                "serverless_max_ops": SERVERLESS_MAX_OPS,
                "tessera_backend": _BACKEND_FACTORY.mode,
                "tessera_backend_label": _BACKEND_FACTORY.label,
                "tessera_publish_mode": args.tessera_publish_mode,
                "tessera_publish_every_s": args.tessera_publish_every_s,
                "tessera_backend_ops": publish_stats,
                "generated_unix_s": round(time.time(), 3),
            },
            "tessera_only": {
                "cold_first_query_vfs_ms": (
                    round(cold_ms, 3) if cold_ms is not None else None
                )
            },
            "api": api_results,  # {scenario: {engine: {ops_s, p50, p95,
            #                      n, errors, note}}} (api suite)
        }
        out.update(results)  # {engine: {workload: {p50, p95, ops_s, n}}}
        path = Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"\nJSON results written to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
