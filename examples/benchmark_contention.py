"""Contention + auto-promotion benchmark.

N threads write to the SAME tenant against the real serverless stack and
the script observes the full story: pure contention (503), cold start of
the ephemeral owner on Fargate, transition to forwarding (header
``X-Tessera-Owner: 1``), and steady state. Reports ops/s per phase,
p50/p95 latencies, and a 10 s-bucket timeline.

Requires the stack deployed with ``EnableAutoPromote=true`` (see
template.yaml: ECR image + subnets + security group).

Usage:
    TESSERA_API_URL=https://xxxx.execute-api.us-east-2.amazonaws.com/ \\
    python examples/benchmark_contention.py --tenant cont-bench \\
        --vusers 6 --duration 150

Honest reading: from a remote client (e.g. South America -> us-east-2)
the measured throughput saturates the client RTT, not the system — 6
workers at ~550 ms RTT yield ~11 ops/s as the harness THEORETICAL
MAXIMUM. The number that measures the improvement is the RATIO between
the contention phase and the forwarded phase, plus the forwarded p50
latency (which includes the full RTT).
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request


def _post(api: str, tenant: str, op: str, body: dict, timeout: float = 15.0):
    request = urllib.request.Request(
        f"{api}/{tenant}/{op}",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return urllib.request.urlopen(request, timeout=timeout)


# @spec examples/benchmark-contention-main
# @spec examples/main
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--tenant", default="cont-bench")
    parser.add_argument("--vusers", type=int, default=6)
    parser.add_argument("--duration", type=int, default=150, help="segundos")
    parser.add_argument("--api-url", default=os.environ.get("TESSERA_API_URL", ""))
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()
    if not args.api_url:
        print("error: falta --api-url o TESSERA_API_URL", file=sys.stderr)
        return 2
    api = args.api_url.rstrip("/")

    # Tenant idempotente (409 TenantExists es OK).
    try:
        _post(api, args.tenant, "create", {"schema": "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT);"})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise

    stats: collections.Counter = collections.Counter()
    lock = threading.Lock()
    lat_direct: list[float] = []
    lat_fwd: list[float] = []
    timeline: list[tuple[int, str]] = []
    t_start = time.time()
    stop = t_start + args.duration

    def worker(wid: int) -> None:
        i = 0
        while time.time() < stop:
            i += 1
            body = {"sql": f"INSERT OR REPLACE INTO kv VALUES ('w{wid}-{i}','x')"}
            t0 = time.time()
            try:
                with _post(api, args.tenant, "write", body) as resp:
                    ms = (time.time() - t0) * 1000.0
                    fwd = resp.headers.get("X-Tessera-Owner") == "1"
                    with lock:
                        stats["ok_fwd" if fwd else "ok_direct"] += 1
                        (lat_fwd if fwd else lat_direct).append(ms)
                        timeline.append((int(t0 - t_start), "F" if fwd else "D"))
            except urllib.error.HTTPError as exc:
                with lock:
                    stats[f"http_{exc.code}"] += 1
                    timeline.append((int(t0 - t_start), str(exc.code)))
            except Exception as exc:  # noqa: BLE001 - conteo, no crash
                with lock:
                    stats[f"err_{type(exc).__name__}"] += 1

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(args.vusers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    duration = time.time() - t_start

    total_ok = stats["ok_fwd"] + stats["ok_direct"]
    print(f"duration: {duration:.1f} s · vusers: {args.vusers} · tenant: {args.tenant}")
    print("results:", dict(stats))
    print(f"ops/s ok (global): {total_ok / duration:.1f}")

    report: dict = {
        "vusers": args.vusers,
        "duration_s": round(duration, 1),
        "counts": dict(stats),
    }
    for name, lats in (("direct", lat_direct), ("forwarded", lat_fwd)):
        if lats:
            lats.sort()
            p50 = lats[len(lats) // 2]
            p95 = lats[int(len(lats) * 0.95)]
            print(f"{name}: n={len(lats)} p50={p50:.0f}ms p95={p95:.0f}ms")
            report[name] = {"n": len(lats), "p50_ms": round(p50), "p95_ms": round(p95)}

    # Pure forwarded phase: ops/s from the first forward (measures steady state).
    first_fwd = next((t for t, k in timeline if k == "F"), None)
    if first_fwd is not None:
        fwd_ops = sum(1 for t, k in timeline if k == "F")
        fwd_window = duration - first_fwd
        if fwd_window > 5:
            print(
                f"forwarded steady state: {fwd_ops / fwd_window:.1f} ops/s "
                f"(from t={first_fwd}s; promotion cold start: ~{first_fwd}s)"
            )
            report["steady_state_ops_s"] = round(fwd_ops / fwd_window, 1)
            report["promotion_cold_start_s"] = first_fwd

    buckets: dict[int, collections.Counter] = {}
    for t, kind in timeline:
        buckets.setdefault(t // 10 * 10, collections.Counter())[kind] += 1
    print("timeline (D=direct, F=forwarded, HTTP codes):")
    for bucket in sorted(buckets):
        print(f"  t={bucket:3}-{bucket + 10:3}s: {dict(buckets[bucket])}")
    report["timeline"] = {str(b): dict(c) for b, c in sorted(buckets.items())}

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"JSON: {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
