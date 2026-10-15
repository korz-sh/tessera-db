#!/usr/bin/env python3
"""Concurrency smoke test for the tessera-db reference HTTP API (stdlib only).

Runs three phases against a deployed stack (see examples/README.md):

  0. create  --tenants tenants, each with a small ``users`` schema; a 409
              TenantExists from a previous run is tolerated.
  1. write   --writes INSERTs distributed round-robin over the tenants.
              Tenants are sharded across --concurrency worker threads so
              each worker owns a disjoint subset: every tenant has at most
              one writer at a time (the single-writer-per-tenant invariant),
              which keeps the natural Conflict count at zero.
  2. read    --reads SELECTs over uniformly random tenants; 10% carry the
              ``X-Tessera-Version`` of the last acked version of that
              tenant (read-your-writes spot checks).

Robustness: retries happen ONLY on connection errors (DNS/socket/reset).
HTTP 4xx/5xx are results, never retried.

Exit code is 1 when any ``Conflict`` (409 on write) or any read-your-writes
violation (a min_version read answered 409 StaleRead after its write had
been acked) is observed, else 0.

Example:
    python3 smoke_test.py --url https://abc123.execute-api.us-east-1.amazonaws.com
"""

from __future__ import annotations

import argparse
import json
import math
import random
import socket
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request

SCHEMA = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
WRITE_SQL = "INSERT INTO users (name) VALUES (?)"
READ_SQL = "SELECT COUNT(*) FROM users"

# Fraction of reads that carry X-Tessera-Version (read-your-writes checks).
MIN_VERSION_FRACTION = 0.10

# Attempts for CONNECTION errors only; HTTP statuses are never retried.
CONN_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# HTTP plumbing
# ---------------------------------------------------------------------------


# @implements operator-readiness/reproducible-local-integration
def _post(url: str, payload: dict, timeout: float, headers: dict | None = None):
    """POST JSON; return ``(status, headers, body_bytes)``.

    ``status == -1`` means the connection failed after retries. HTTPError
    (4xx/5xx) is a RESULT: returned as-is, never retried. It subclasses
    URLError, so it is caught FIRST.
    """
    data = json.dumps(payload).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(1, CONN_ATTEMPTS + 1):
        request = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read()
        except (urllib.error.URLError, ConnectionError, TimeoutError, socket.timeout) as exc:
            last_error = exc  # connection-level: retry with a short backoff
            time.sleep(0.2 * attempt)
    return -1, {}, str(last_error).encode("utf-8")


def _json_body(raw: bytes) -> dict:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _percentile(values: list[float], pct: float) -> float:
    """Linear-interpolated percentile (NaN when empty)."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low, high = math.floor(rank), math.ceil(rank)
    if low == high:
        return float(ordered[low])
    return float(ordered[low] + (ordered[high] - ordered[low]) * (rank - low))


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


def _new_stats() -> dict:
    return {
        "latencies": [],
        "status": {},
        "conflicts": 0,
        "busy": 0,
        "conn_errors": 0,
        "errors": 0,
        "violations": 0,
        "min_version_reads": 0,
    }


def _record_status(stats: dict, status: int) -> None:
    key = str(status)
    stats["status"][key] = stats["status"].get(key, 0) + 1


def _write_worker(
    worker: int,
    subset: list[str],
    count: int,
    url: str,
    timeout: float,
    versions: dict[str, int],
    stats: dict,
) -> None:
    """Owns a disjoint tenant subset: one writer per tenant by construction.

    ``versions`` is only touched for this worker's own tenants, so no lock
    is needed; the read phase treats it as read-only.
    """
    for i in range(count):
        tenant = subset[i % len(subset)]
        payload = {"sql": WRITE_SQL, "params": [f"user-{worker}-{i}"]}
        started = time.perf_counter()
        status, _headers, body = _post(f"{url}/{tenant}/write", payload, timeout)
        stats["latencies"].append((time.perf_counter() - started) * 1000.0)
        _record_status(stats, status)
        if status == -1:
            stats["conn_errors"] += 1
            continue
        if 200 <= status < 300:
            version = _json_body(body).get("version")
            if isinstance(version, int) and version > 0:
                versions[tenant] = version
        else:
            stats["errors"] += 1
            if status == 409:
                error = _json_body(body).get("error")
                # 409 on write = Conflict in practice; LeaseLost/CASFailed
                # (also 409) are counted separately via the status histogram.
                if error == "Conflict" or error is None:
                    stats["conflicts"] += 1
            elif status == 503:
                stats["busy"] += 1  # TenantBusy after the lease backoff


def _read_worker(
    worker: int,
    count: int,
    tenants: list[str],
    url: str,
    timeout: float,
    versions: dict[str, int],
    stats: dict,
) -> None:
    rng = random.Random(1_000 + worker)
    for _ in range(count):
        tenant = rng.choice(tenants)
        headers = None
        check = False
        if rng.random() < MIN_VERSION_FRACTION:
            version = versions.get(tenant)
            if isinstance(version, int) and version > 0:
                headers = {"X-Tessera-Version": str(version)}
                check = True
                stats["min_version_reads"] += 1
        started = time.perf_counter()
        status, _headers, body = _post(
            f"{url}/{tenant}/read", {"sql": READ_SQL}, timeout, headers
        )
        stats["latencies"].append((time.perf_counter() - started) * 1000.0)
        _record_status(stats, status)
        if status == -1:
            stats["conn_errors"] += 1
            continue
        if not (200 <= status < 300):
            stats["errors"] += 1
            if status == 409 and check:
                # Read-your-writes violation: the write carrying version
                # ``version`` was already acked, and the only 409 the read
                # path can produce is StaleRead.
                stats["violations"] += 1


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------


def _shard(total: int, shards: int) -> list[int]:
    base, extra = divmod(max(total, 0), shards)
    return [base + (1 if i < extra else 0) for i in range(shards)]


def _ok_count(status_counts: dict) -> int:
    """Number of 2xx responses recorded in a ``by status`` histogram."""
    return sum(
        n
        for code, n in status_counts.items()
        if code.lstrip("-").isdigit() and 200 <= int(code) < 300
    )


def _merge(parts: list[dict]) -> dict:
    merged = _new_stats()
    for part in parts:
        merged["latencies"].extend(part["latencies"])
        for key in ("conflicts", "busy", "conn_errors", "errors", "violations", "min_version_reads"):
            merged[key] += part[key]
        for code, n in part["status"].items():
            merged["status"][code] = merged["status"].get(code, 0) + n
    return merged


def _phase_create(url: str, args: argparse.Namespace) -> tuple[list[str], int, int, int]:
    created = existed = failed = 0
    tenants: list[str] = []
    for i in range(args.tenants):
        tenant = f"smoke-{i}"
        status, _headers, body = _post(
            f"{url}/{tenant}/create", {"schema": SCHEMA}, args.timeout
        )
        if 200 <= status < 300:
            created += 1
        elif status == 409:
            existed += 1  # TenantExists from a previous run: fine
        else:
            failed += 1
            print(
                f"WARNING: create {tenant} failed: HTTP {status} {_json_body(body)}",
                file=sys.stderr,
            )
        tenants.append(tenant)
    return tenants, created, existed, failed


def _run_phase(target, plans: list[tuple]) -> list[dict]:
    threads = []
    for worker, plan in enumerate(plans):
        thread = threading.Thread(
            target=target, args=plan, name=f"{target.__name__}-{worker}"
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    return [plan[-1] for plan in plans]


def _report(
    url: str,
    args: argparse.Namespace,
    created: int,
    existed: int,
    failed: int,
    write: dict,
    reads: dict,
    elapsed_s: float,
) -> int:
    conflicts = write["conflicts"]
    violations = reads["violations"]
    ok = conflicts == 0 and violations == 0
    bar = "=" * 64
    print()
    print(bar)
    print("tessera-db smoke test")
    print(bar)
    print(f"url:          {url}")
    print(
        f"tenants:      {args.tenants}  "
        f"(created {created}, existed {existed}, failed {failed})"
    )
    print(
        f"writes:       {args.writes}  ok={_ok_count(write['status'])}"
        f"  errors={write['errors']}  conn_errors={write['conn_errors']}"
    )
    print(f"  by status:  {write['status'] or '-'}")
    print(
        f"reads:        {args.reads}  ok={_ok_count(reads['status'])}"
        f"  errors={reads['errors']}  conn_errors={reads['conn_errors']}"
        f"  min_version={reads['min_version_reads']}"
    )
    print(f"  by status:  {reads['status'] or '-'}")
    print(f"conflicts:    {conflicts}   tenant_busy(503): {write['busy']}")
    print(f"violations:   {violations}  (409 StaleRead on min_version read)")
    print(
        f"write ms:     p50={_percentile(write['latencies'], 50):.1f}"
        f"  p95={_percentile(write['latencies'], 95):.1f}"
        f"  mean={statistics.fmean(write['latencies']) if write['latencies'] else float('nan'):.1f}"
    )
    print(
        f"read  ms:     p50={_percentile(reads['latencies'], 50):.1f}"
        f"  p95={_percentile(reads['latencies'], 95):.1f}"
        f"  mean={statistics.fmean(reads['latencies']) if reads['latencies'] else float('nan'):.1f}"
    )
    print(f"wall time:    {elapsed_s:.1f}s")
    print(bar)
    print(f"RESULT: {'PASS' if ok else 'FAIL'}  (conflicts must be 0, violations must be 0)")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Concurrency smoke test for the tessera-db reference API."
    )
    parser.add_argument(
        "--url", required=True, help="API base URL (the ApiUrl stack output)"
    )
    parser.add_argument("--tenants", type=int, default=20)
    parser.add_argument("--writes", type=int, default=1000)
    parser.add_argument("--reads", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=30.0, help="per-request timeout (s)")
    args = parser.parse_args(argv)

    url = args.url.rstrip("/")
    if not url.startswith(("http://", "https://")):
        parser.error("--url must start with http:// or https://")
    concurrency = max(1, args.concurrency)
    started = time.perf_counter()

    # Phase 0: create tenants (409 TenantExists tolerated).
    tenants, created, existed, failed = _phase_create(url, args)
    print(f"[phase 0] tenants: created={created} existed={existed} failed={failed}")

    # Phase 1: writes, tenants sharded across workers (single writer each).
    # Worker count is capped at the tenant count so every worker owns a
    # non-empty subset and the requested write total is fully executed.
    versions: dict[str, int] = {}
    write_workers = max(1, min(concurrency, len(tenants)))
    counts = _shard(args.writes, write_workers)
    stats = [_new_stats() for _ in range(write_workers)]
    plans = [
        (worker, tenants[worker::write_workers], counts[worker], url, args.timeout, versions, stats[worker])
        for worker in range(write_workers)
        if counts[worker] > 0
    ]
    _run_phase(_write_worker, plans)
    write = _merge(stats)
    print(
        f"[phase 1] writes done: ok={_ok_count(write['status'])}"
        f" errors={write['errors']} conn_errors={write['conn_errors']}"
    )

    # Phase 2: reads over random tenants, 10% with min_version spot checks.
    read_stats = [_new_stats() for _ in range(concurrency)]
    read_counts = _shard(args.reads, concurrency)
    read_plans = [
        (worker, read_counts[worker], tenants, url, args.timeout, versions, read_stats[worker])
        for worker in range(concurrency)
        if read_counts[worker] > 0
    ]
    _run_phase(_read_worker, read_plans)
    reads = _merge(read_stats)
    print(
        f"[phase 2] reads done: ok={_ok_count(reads['status'])}"
        f" errors={reads['errors']} conn_errors={reads['conn_errors']}"
        f" min_version={reads['min_version_reads']}"
    )

    return _report(
        url, args, created, existed, failed, write, reads, time.perf_counter() - started
    )


if __name__ == "__main__":
    sys.exit(main())
