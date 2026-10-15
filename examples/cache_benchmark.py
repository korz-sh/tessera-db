"""Bounded, synthetic cache comparison. No AWS clients or credentials used."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import platform
import statistics
import tempfile
import threading
import time

from tessera import LocalBackend, Store, __version__


# @spec examples/meter
class Meter:
    def __init__(self, inner):
        self.inner = inner
        self.lock = threading.Lock()
        self.calls = Counter()
        self.uploaded = self.downloaded = 0

    def __getattr__(self, name):
        value = getattr(self.inner, name)
        if not callable(value) or name.startswith("_"):
            return value
        def measured(*args, **kwargs):
            result = value(*args, **kwargs)
            with self.lock:
                self.calls[name] += 1
                if name == "put":
                    self.uploaded += len(args[1])
                if name == "get":
                    self.downloaded += len(result[0])
            return result
        return measured


# @spec examples/scenario
def scenario(root, format, vfs, shared, workers, iterations):
    backend = LocalBackend(root / "objects")
    with closing(Store(backend, root=root / "setup", use_vfs=False)) as setup:
        for worker in range(workers):
            tenant = f"t{worker}"
            setup.create(tenant, "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);")
            def seed(conn):
                conn.executemany("INSERT INTO items VALUES (?, ?)",
                                 [(i, "x" * 1024) for i in range(100)])
                conn.commit()
            setup.write(tenant, seed)
            if format == 2:
                setup.migrate_to_v2(tenant)
    results = []
    stores = [Store(Meter(backend), root=root / ("shared" if shared else f"worker-{i}"),
                    use_vfs=vfs, inline_compact=False) for i in range(workers)]
    try:
        for phase, operations in [("cold_read", 1), ("warm_read", iterations),
                                  ("write", iterations)]:
            for store in stores:
                store._backend.calls.clear()
                store._backend.uploaded = store._backend.downloaded = 0
            barrier = threading.Barrier(workers)
            def worker(i):
                store, tenant = stores[i], f"t{i}"
                elapsed = []
                barrier.wait(timeout=10)
                for n in range(operations):
                    start = time.perf_counter()
                    if phase == "write":
                        def change(conn):
                            conn.execute("UPDATE items SET value=? WHERE id=?", (str(n), n % 100))
                            conn.commit()
                        store.write(tenant, change)
                    else:
                        assert store.read(tenant, lambda c: c.execute("SELECT COUNT(*) FROM items").fetchone()[0]) == 100
                    elapsed.append((time.perf_counter() - start) * 1000)
                return elapsed
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=workers) as pool:
                samples = sorted(x for group in pool.map(worker, range(workers)) for x in group)
            seconds = time.perf_counter() - start
            calls = sum((store._backend.calls for store in stores), Counter())
            results.append(dict(format=format, vfs=vfs, shared=shared, workers=workers,
                                phase=phase, operations=len(samples), seconds=seconds,
                                ops_s=len(samples) / seconds, p50_ms=statistics.median(samples),
                                p95_ms=samples[max(0, int(len(samples) * .95 + .999) - 1)],
                                calls=dict(calls), uploaded_bytes=sum(s._backend.uploaded for s in stores),
                                downloaded_bytes=sum(s._backend.downloaded for s in stores)))
    finally:
        for store in stores:
            store.close()
    return results


# @spec examples/cache-benchmark-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (1 <= args.iterations <= 100 and 1 <= args.repeats <= 5 and 1 <= args.workers <= 8):
        parser.error("caps: iterations 1..100, repeats 1..5, workers 1..8")
    results = []
    with tempfile.TemporaryDirectory(prefix="tessera-cache-bench-") as directory:
        for repeat in range(args.repeats):
            for format, vfs in [(1, False), (2, False), (2, True)]:
                for shared in [False, True]:
                    root = Path(directory) / f"{repeat}-{format}-{vfs}-{shared}"
                    rows = scenario(root, format, vfs, shared, args.workers, args.iterations)
                    for row in rows:
                        row["repeat"] = repeat
                    results.extend(rows)
    report = dict(version=__version__, python=platform.python_version(), system=platform.system(),
                  backend="LocalBackend", note="Logical backend calls; not AWS billing or network latency.",
                  iterations=args.iterations, repeats=args.repeats, workers=args.workers, results=results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Recorded {len(results)} phase measurements in {args.output}")


if __name__ == "__main__":
    main()
