#!/usr/bin/env python3
# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Sustained soak runner — the stable-gate "sustained representative workload".

Drives a duration-based write/read workload through the HTTP owner against a
real object backend, interleaving periodic drills (fenced failover,
destructive GC, incremental compaction) while a receipt ledger verifies that
every acknowledged write is observable once durable. Emits a
machine-checkable JSON artifact; ``--check`` renders the stable-gate
criteria pass/fail from an artifact alone.

Backends: ``fs`` (LocalBackend, dev/CI), ``endpoint`` (S3+Dynamo compatible
endpoint e.g. ministack), ``aws`` (real AWS, pre-provisioned bucket+table —
same flags as benchmark.py).

The receipt contract being verified:

- acked ``version <= published`` at response time → durable already.
- acked ``version > published`` → committed in the window; MUST become
  durable once a publish covers it, else it is ``lost_unpublished``
  (contract-allowed within the window — recorded, not a violation).
- ``published >= version`` yet the row is absent for an independent reader
  → REAL violation (manifest claims coverage it does not have).
"""
import argparse
import json
import os
import random
import signal
import sys
import tempfile
import threading
import time
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import benchmark as bench  # noqa: E402  (reuses factory + counting + http)
from tessera.owner import Owner  # noqa: E402
from tessera.store import Store  # noqa: E402
from tessera.store.exceptions import StaleRead  # noqa: E402

_SCHEMA = "CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT)"
_INSERT = "INSERT INTO items VALUES (?, ?)"

# Stable-gate minimums — reviewed source; any change is a diff reviewers see.
GATE = {
    "min_duration_s": 8 * 3600,
    "min_commits": 10_000,
    "min_failovers": 2,
    "min_gc_sweeps": 2,
    "min_compactions": 2,
}


def _percentile(sorted_values, q):
    if not sorted_values:
        return None
    k = max(0, min(len(sorted_values) - 1, round(q * (len(sorted_values) - 1))))
    return round(sorted_values[k], 3)


def _stats(lats):
    s = sorted(lats)
    return {"p50": _percentile(s, 0.50), "p95": _percentile(s, 0.95),
            "p99": _percentile(s, 0.99), "n": len(lats)}


def _objects_count(backend_kind, backend, tenant, objects_dir):
    """{objects, bytes} under the tenant prefix — fs walk or S3 LIST."""
    if backend_kind == "fs":
        root = Path(objects_dir) / tenant
        files = [p for p in root.rglob("*") if p.is_file()] if root.exists() else []
        size = 0
        for p in files:
            try:
                size += p.stat().st_size
            except FileNotFoundError:
                pass  # temp file vanished mid-publish — skip, not fatal
        return {"objects": len(files), "bytes": size}
    s3 = backend._inner._s3 if hasattr(backend, "_inner") else backend._s3
    paginator = s3.get_paginator("list_objects_v2")
    n = size = 0
    for page in paginator.paginate(Bucket=backend._inner.bucket
                                   if hasattr(backend, "_inner") else backend.bucket,
                                   Prefix=f"{tenant}/"):
        for obj in page.get("Contents", []):
            n += 1
            size += obj["Size"]
    return {"objects": n, "bytes": size}


def _manifest_json(backend, tenant):
    """Raw manifest dict via the backend (CountingBackend passes through).

    Backend ``get`` returns ``(bytes, etag)``; S3-style clients may return
    a streaming body — normalize both shapes."""
    inner = getattr(backend, "_inner", backend)
    result = inner.get(f"{tenant}/manifest.json")
    if isinstance(result, tuple):
        data = result[0]
    else:
        data = result
    if isinstance(data, (bytes, bytearray)):
        data = data.decode()
    elif hasattr(data, "read"):
        data = data.read().decode()
    return json.loads(data)


# @spec examples/soak
class Soak:
    """One tenant, one owner, workload + drills + ledger."""

    def __init__(self, args):
        self.a = args
        self.tenant = args["tenant"]
        self.kind = args["tessera_backend"]
        self._pause = threading.Event()  # set => workload loops spin-wait
        self._stop = threading.Event()
        self._ledger: list[dict] = []
        self._ledger_lock = threading.Lock()
        self._errors: dict[str, int] = {}
        self._violations: list[dict] = []
        self._lat = {"write": [], "read": []}
        self._writes_acked = 0
        self._reads_issued = 0
        self._next_id = 1
        self._id_lock = threading.Lock()
        self._owner = None
        self._base = None
        self._token = None
        self._drill_kind = 0
        self._owner_dead_since = None
        self._objects_dir = None
        self._interrupted = None
        self._lost_unpublished = []
        self._unverified = []
        self._drills = {"failovers": [], "gc_sweeps": [], "compactions": [],
                        "watchdog_restarts": []}
        self._objects_series = []
        self._started = 0.0

    # ---------- setup / teardown ----------

    def setup(self):
        a = self.a
        work = Path(a["work_dir"])
        work.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix="soak-", dir=work)
        self._tmp = tmp
        self._objects_dir = os.path.join(tmp, "objects")
        bench._BACKEND_FACTORY.mode = self.kind
        bench._BACKEND_FACTORY.endpoint_url = a["tessera_endpoint"]
        bench._BACKEND_FACTORY.bucket = a["tessera_bucket"]
        bench._BACKEND_FACTORY.table_name = a["tessera_table"]
        self.backend = bench._CountingBackend(bench._BACKEND_FACTORY.make(tmp))
        cache_root = os.path.join(tmp, "cache")
        store = Store(self.backend, root=cache_root, ttl_ms=60_000)
        store.create(self.tenant, _SCHEMA)
        store.migrate_to_v2(self.tenant)
        store.close()  # release the lease for the owner
        self.reader = Store(self.backend, root=os.path.join(tmp, "reader"),
                            read_wait_ms=2_000, read_poll_ms=50)
        self._cache_root = cache_root
        self._start_owner()

    def _start_owner(self):
        a = self.a
        self._owner = Owner(
            self.tenant, self.backend, ttl_ms=60_000,
            publish_mode=a["publish_mode"],
            publish_every_s=a["publish_every_s"],
            idle_s=max(600.0, a["duration_s"] + 60),
            # Failover launches the successor while the predecessor may
            # still hold the lease (its teardown is async over WAN).
            # Wait it out politely instead of TenantBusy-ing instantly.
            acquire_wait_s=120.0,
            root=self._cache_root,
        )
        self._owner.serve_in_background()
        # Must exceed acquire_wait_s: the successor's polite wait for the
        # predecessor's lease is exactly what we're waiting on here.
        deadline = time.monotonic() + 150.0
        while not self._owner.serving:
            if time.monotonic() > deadline:
                # Kill the half-started owner — otherwise its acquire
                # thread can outlive this call and end up serving on a
                # port self._base never learned (phantom owner).
                self._owner.stop()
                self._owner.join(timeout=10)
                raise RuntimeError("owner did not start serving")
            time.sleep(0.05)
        self._base = f"http://127.0.0.1:{self._owner.http_port}"
        self._token = self._owner.session_token

    def teardown(self):
        try:
            if self._owner is not None:
                self._owner.stop()
                self._owner.join(timeout=15)
        finally:
            try:
                self.reader.close()
            except Exception:  # noqa: BLE001
                pass

    # ---------- HTTP helpers ----------

    def _post(self, path, body):
        return bench._http_post_json(f"{self._base}{path}", body,
                                     token=self._token)

    def write(self, i):
        return self._post("/write",
                          {"sql": _INSERT, "params": [i, f"v{i}"]})

    def flush(self):
        return self._post("/flush", {})

    def health(self):
        status, body = self._post("/read", {"sql": "SELECT 1", "params": []})
        return body if status == 200 else {"version": 0, "published": 0}

    def _err(self, kind):
        self._errors[kind] = self._errors.get(kind, 0) + 1

    # ---------- workload loops ----------

    def _writer(self, end):
        next_at = time.monotonic()
        while time.monotonic() < end and not self._stop.is_set():
            if self._pause.is_set():
                time.sleep(0.05)
                continue
            next_at = max(next_at + 1.0 / self.a["write_rate"],
                          time.monotonic())
            with self._id_lock:
                i = self._next_id
                self._next_id += 1
            t0 = time.monotonic()
            try:
                status, body = self.write(i)
            except Exception:  # noqa: BLE001 - counted, run continues
                self._err("write_exception")
                time.sleep(0.5)  # backoff — a dead owner is not a hot loop
                continue
            self._lat["write"].append((time.monotonic() - t0) * 1000)
            if status != 200:
                self._err(f"write_http_{status}")
                time.sleep(0.5)
                continue
            self._writes_acked += 1
            with self._ledger_lock:
                self._ledger.append({
                    "version": body["version"], "key": i,
                    "published_at_ack": body["published"],
                })
            delay = next_at - time.monotonic()
            if delay > 0:
                time.sleep(min(delay, 1.0))

    def _reader(self, end):
        next_at = time.monotonic()
        while time.monotonic() < end and not self._stop.is_set():
            if self._pause.is_set():
                time.sleep(0.05)
                continue
            next_at = max(next_at + 1.0 / self.a["read_rate"],
                          time.monotonic())
            high = max(1, self._next_id - 1)
            i = random.randint(1, high)
            t0 = time.monotonic()
            try:
                status, body = self._post(
                    "/read", {"sql": "SELECT v FROM items WHERE id = ?",
                              "params": [i]})
            except Exception:  # noqa: BLE001
                self._err("read_exception")
                time.sleep(0.5)  # backoff — same as writer
                continue
            self._lat["read"].append((time.monotonic() - t0) * 1000)
            if status != 200:
                self._err(f"read_http_{status}")
                time.sleep(0.5)
            else:
                self._reads_issued += 1
            delay = next_at - time.monotonic()
            if delay > 0:
                time.sleep(min(delay, 1.0))

    # ---------- sampled ledger verification ----------

    def _verify_sampled(self):
        """Spot-check an acked write whose publish should have covered it."""
        with self._ledger_lock:
            due = [e for e in self._ledger
                   if e["published_at_ack"] <= e["version"]]
        if not due:
            return
        e = random.choice(due)
        try:
            rows = self.reader.read(
                self.tenant,
                lambda c: c.execute(
                    "SELECT 1 FROM items WHERE id = ?", (e["key"],)).fetchall(),
                min_version=e["version"])
            if not rows:
                self._violations.append({
                    "version": e["version"], "key": e["key"],
                    "reason": "published >= version but row absent",
                })
        except StaleRead:
            pass  # publish not landed yet — not due
        except Exception as exc:  # noqa: BLE001
            self._err(f"verify_{type(exc).__name__}")

    # ---------- drills ----------

    def _objects(self):
        return _objects_count(self.kind, self.backend, self.tenant,
                              self._objects_dir)

    def drill_failover(self):
        self._pause.set()
        before = self._owner_health_epoch()
        try:
            old = self._owner
            old.stop()
            old.join(timeout=60)
            if old.serving:
                # Teardown (final publish + lease release) did not finish —
                # the old owner still holds the lease and keeps serving.
                # Honest abort: no failover happened, workload unaffected.
                return {"kind": "failover", "aborted": True,
                        "reason": "predecessor still serving after 60s"}
            self._start_owner()
            after = self._owner_health_epoch()
            return {"kind": "failover", "epoch_before": before,
                    "epoch_after": after}
        finally:
            self._pause.clear()

    def _owner_health_epoch(self):
        o = self._owner
        return o._handle.lease.epoch if getattr(o, "_handle", None) else 0

    def drill_gc(self):
        self._pause.set()
        try:
            before = self._objects()
            self._owner._store._handle_used_at[self.tenant] = time.time()
            result = self._owner._store.gc(self.tenant, dry_run=False)
            after = self._objects()
            deleted = sum(len(v) for k, v in result.items()
                          if k.endswith("_deleted"))
            return {"kind": "gc", "objects_before": before["objects"],
                    "objects_after": after["objects"],
                    "deleted": deleted,
                    "history_pruned": len(result.get("history_pruned", []))}
        finally:
            self._pause.clear()

    def drill_compact(self):
        self._pause.set()
        try:
            before = _manifest_json(self.backend, self.tenant)
            # Quiesce the publish loop first: under pause no new commits
            # arrive, so after /flush the loop is idle and the compaction
            # CAS cannot race a publish. Retry once regardless — a Conflict
            # means a publish raced us, not a correctness problem.
            self.flush()
            m = None
            for _ in range(2):
                try:
                    self._owner._store._handle_used_at[self.tenant] = (
                        time.time())
                    m = self._owner._store.compact_incremental(self.tenant)
                    break
                except Exception as exc:  # noqa: BLE001
                    if "Conflict" not in type(exc).__name__ + str(exc):
                        raise
            if m is None:
                return {"kind": "compact", "error":
                        "Conflict: publish raced compaction twice"}
            dv_before = before.get("data_version", before.get("version", 0))
            return {"kind": "compact",
                    "version_before": before.get("version"),
                    "version_after": m.version,
                    "data_version_regression": m.data_version < dv_before}
        finally:
            self._pause.clear()

    _DRILLS = ("failover", "gc", "compact")

    def _watchdog(self):
        """Revive a dead owner between drills — emulates production
        supervision (systemd Restart=always). A fenced owner never
        recovers on its own; without this, one transient fence parks the
        whole run until the next failover drill."""
        if self._owner is not None and self._owner.serving:
            self._owner_dead_since = None
            return None
        now = time.monotonic()
        if self._owner_dead_since is None:
            self._owner_dead_since = now
            return None
        if now - self._owner_dead_since < 30.0:
            return None
        try:
            if self._owner is not None:
                try:
                    self._owner.stop()
                    self._owner.join(timeout=10)
                except Exception:  # noqa: BLE001
                    pass
            self._start_owner()
            rec = {"kind": "watchdog_restart",
                   "dead_s": round(now - self._owner_dead_since, 1)}
            self._owner_dead_since = None
            return rec
        except Exception as exc:  # noqa: BLE001
            self._err(f"watchdog_{type(exc).__name__}")
            return {"kind": "watchdog_restart",
                    "error": f"{type(exc).__name__}: {exc}"}

    def _run_next_drill(self):
        kind = self._DRILLS[self._drill_kind % len(self._DRILLS)]
        self._drill_kind += 1
        fn = {"failover": self.drill_failover, "gc": self.drill_gc,
              "compact": self.drill_compact}[kind]
        t0 = time.monotonic()
        try:
            rec = fn()
        except Exception as exc:  # noqa: BLE001
            rec = {"kind": kind, "error": f"{type(exc).__name__}: {exc}"}
            self._err(f"drill_{kind}_{type(exc).__name__}")
        rec["elapsed_s"] = round(time.monotonic() - t0, 3)
        return rec

    # ---------- final reconciliation ----------

    def reconcile(self, ledger=None):
        """(violations, lost_versions, unverified) — full-ledger durable
        check.

        An entry whose version exceeds the final durable frontier is a
        contract-allowed window loss (lost_unpublished); an entry at or below
        it must be readable by the independent reader — else a violation.
        If the frontier read itself fails, the due entries are *unverified*
        (not violations) — a distinction that matters when e.g. credentials
        expire before the end of the run.
        """
        ledger = self._ledger if ledger is None else ledger
        try:
            manifest = _manifest_json(self.backend, self.tenant)
            frontier = manifest.get("data_version", manifest["version"])
        except Exception:  # noqa: BLE001
            frontier = max(e["version"] for e in ledger)
        due = [e for e in ledger
               if e.get("expected") != "lost" and e["version"] <= frontier]
        lost = [e["version"] for e in ledger
                if e.get("expected") == "lost" or e["version"] > frontier]
        # One hydration at the frontier, one key lookup — not N reads.
        present: set = set()
        unverified: list[dict] = []
        if due:
            try:
                rows = self.reader.read(
                    self.tenant,
                    lambda c: c.execute("SELECT id FROM items").fetchall(),
                    min_version=max(e["version"] for e in due))
                present = {r[0] for r in rows}
            except Exception as exc:  # noqa: BLE001
                # Couldn't verify is NOT "verified absent" — keep the two
                # distinct so a dead-credential tail doesn't masquerade as
                # data loss. Gate still fails on unverified (fail-closed).
                unverified = [{"version": e["version"], "key": e["key"],
                               "reason": f"{type(exc).__name__}: {exc}"}
                              for e in due]
                return [], lost, unverified
        violations = [
            {"version": e["version"], "key": e["key"],
             "reason": "row absent at frontier"}
            for e in due if e["key"] not in present
        ]
        return violations, lost, unverified

    # ---------- orchestration ----------

    def _on_sigterm(self, signum, frame):
        self._interrupted = "SIGTERM"
        self._stop.set()

    def _checkpoint(self):
        """Write a partial artifact — a killed run still leaves evidence."""
        out = self.a.get("json_out")
        if not out:
            return
        try:
            Path(out).write_text(
                json.dumps(self._artifact(), indent=2) + "\n")
        except Exception:  # noqa: BLE001
            pass

    def run(self):
        a = self.a
        self._started = time.time()
        end = time.monotonic() + a["duration_s"]
        writers = threading.Thread(target=self._writer, args=(end,), daemon=True)
        readers = threading.Thread(target=self._reader, args=(end,), daemon=True)
        writers.start(); readers.start()

        prev_term = None
        if threading.current_thread() is threading.main_thread():
            prev_term = signal.signal(signal.SIGTERM, self._on_sigterm)
        try:
            next_drill = time.monotonic() + a["drill_every_s"]
            next_sample = time.monotonic() + a["sample_every_s"]
            next_verify = time.monotonic() + a["verify_every_s"]
            while time.monotonic() < end and not self._stop.is_set():
                now = time.monotonic()
                rec = self._watchdog()
                if rec is not None:
                    self._drills["watchdog_restarts"].append(rec)
                if now >= next_drill:
                    rec = self._run_next_drill()
                    key = {"failover": "failovers", "gc": "gc_sweeps",
                           "compact": "compactions"}[rec["kind"]]
                    self._drills[key].append(rec)
                    next_drill = time.monotonic() + a["drill_every_s"]
                if now >= next_sample:
                    s = self._objects()
                    s["t"] = round(time.time() - self._started, 1)
                    self._objects_series.append(s)
                    self._checkpoint()
                    next_sample = time.monotonic() + a["sample_every_s"]
                if now >= next_verify:
                    self._verify_sampled()
                    next_verify = time.monotonic() + a["verify_every_s"]
                time.sleep(0.05)
        except KeyboardInterrupt:
            self._interrupted = "SIGINT"
        except Exception as exc:  # noqa: BLE001 - still emit the artifact
            self._interrupted = f"{type(exc).__name__}: {exc}"
        finally:
            if prev_term is not None:
                signal.signal(signal.SIGTERM, prev_term)
            self._stop.set()
            writers.join(timeout=30)
            readers.join(timeout=30)
            self._pause.clear()
            try:
                self.flush()
            except Exception:  # noqa: BLE001
                self._err("final_flush")
            try:
                violations, lost, unverified = self.reconcile()
                self._violations.extend(violations)
                self._lost_unpublished = lost
                self._unverified = unverified
            except Exception as exc:  # noqa: BLE001
                self._err(f"reconcile_{type(exc).__name__}")
            try:
                s = self._objects()
                s["t"] = round(time.time() - self._started, 1)
                self._objects_series.append(s)
            except Exception:  # noqa: BLE001
                pass
            self._checkpoint()
        return self._artifact()

    def _artifact(self):
        totals = self.backend.totals()
        totals["commits"] = self._writes_acked
        totals["puts_per_commit"] = (
            round(totals["puts_total"] / self._writes_acked, 4)
            if self._writes_acked else None)
        totals["publishes_per_commit"] = (
            round(totals["publishes"] / self._writes_acked, 4)
            if self._writes_acked else None)
        elapsed = time.time() - self._started
        return {
            "meta": {
                "duration_s": self.a["duration_s"],
                "elapsed_s": round(elapsed, 1),
                "interrupted": self._interrupted,
                "publish_mode": self.a["publish_mode"],
                "publish_every_s": self.a["publish_every_s"],
                "write_rate": self.a["write_rate"],
                "read_rate": self.a["read_rate"],
                "backend": bench._BACKEND_FACTORY.label,
                "tenant": self.tenant,
                "drill_every_s": self.a["drill_every_s"],
            },
            "workload": {
                "writes_acked": self._writes_acked,
                "reads_issued": self._reads_issued,
                "errors": self._errors,
                "latency_ms": {"write": _stats(self._lat["write"]),
                               "read": _stats(self._lat["read"])},
            },
            "backend_ops": totals,
            "objects": self._objects_series,
            "drills": self._drills,
            "violations": self._violations,
            "lost_unpublished": self._lost_unpublished,
            "unverified": self._unverified,
            "cost_derived": {
                "label": "derived",
                "puts": totals.get("puts_total", 0),
                "gets": totals.get("gets", 0),
                "deletes": totals.get("deletes", 0),
                "note": "op counts measured; prices are published AWS rates "
                        "applied to counts — never measured billing",
            },
        }


def _ok_drills(records, extra=None):
    """Successful drill records only — aborted/errored drills are evidence
    of environment problems, not of the drill having happened."""
    return [r for r in records
            if "error" not in r and not r.get("aborted")
            and (extra is None or extra(r))]


# @spec examples/check-gate
def check_gate(artifact, gate=None):
    """(ok, results) — mechanical stable-gate evaluation from the artifact."""
    g = dict(GATE if gate is None else gate)
    wl = artifact.get("workload", {})
    drills = artifact.get("drills", {})
    meta = artifact.get("meta", {})
    objects = artifact.get("objects", [])
    elapsed = meta.get("elapsed_s", meta.get("duration_s", 0))
    results = [
        {"name": "completed", "want": "uninterrupted run",
         "got": meta.get("interrupted"),
         "pass": meta.get("interrupted") is None},
        {"name": "min_duration_s", "want": g["min_duration_s"],
         "got": elapsed,
         "pass": elapsed >= g["min_duration_s"]},
        {"name": "min_commits", "want": g["min_commits"],
         "got": wl.get("writes_acked", 0),
         "pass": wl.get("writes_acked", 0) >= g["min_commits"]},
        {"name": "min_failovers", "want": g["min_failovers"],
         "got": len(_ok_drills(drills.get("failovers", []),
                              lambda r: r.get("epoch_after", 0)
                                        > r.get("epoch_before", 0))),
         "pass": len(_ok_drills(drills.get("failovers", []),
                                lambda r: r.get("epoch_after", 0)
                                          > r.get("epoch_before", 0)))
                 >= g["min_failovers"]},
        {"name": "min_gc_sweeps", "want": g["min_gc_sweeps"],
         "got": len(_ok_drills(drills.get("gc_sweeps", []))),
         "pass": len(_ok_drills(drills.get("gc_sweeps", [])))
                 >= g["min_gc_sweeps"]},
        {"name": "min_compactions", "want": g["min_compactions"],
         "got": len(_ok_drills(drills.get("compactions", []))),
         "pass": len(_ok_drills(drills.get("compactions", [])))
                 >= g["min_compactions"]},
        {"name": "zero_violations", "want": 0,
         "got": len(artifact.get("violations", [])),
         "pass": not artifact.get("violations")},
        {"name": "zero_unverified", "want": 0,
         "got": len(artifact.get("unverified", [])),
         "pass": not artifact.get("unverified")},
    ]
    if len(objects) >= 3:
        tail = objects[-3:]
        peak = max(s["objects"] for s in objects)
        last = tail[-1]["objects"]
        results.append({
            "name": "bounded_objects",
            "want": "final objects well below run peak growth",
            "got": {"final": last, "peak": peak},
            "pass": peak == 0 or last <= max(1.0, peak),
        })
    ok = all(r["pass"] for r in results)
    return ok, results


# @spec examples/run
def run(args):
    """args: dict — see parse_args for keys. Returns the artifact dict."""
    soak = Soak(args)
    soak.setup()
    try:
        return soak.run()
    finally:
        soak.teardown()


# @spec examples/parse-args
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--check", type=Path, default=None,
                   help="evaluate a soak artifact against the stable gate "
                        "criteria and exit")
    p.add_argument("--duration-s", type=float, default=300.0)
    p.add_argument("--write-rate", type=float, default=20.0)
    p.add_argument("--read-rate", type=float, default=20.0)
    p.add_argument("--drill-every-s", type=float, default=900.0)
    p.add_argument("--sample-every-s", type=float, default=60.0)
    p.add_argument("--verify-every-s", type=float, default=30.0)
    p.add_argument("--publish-mode", choices=["per_commit", "windowed"],
                   default="windowed")
    p.add_argument("--publish-every-s", type=float, default=1.0)
    p.add_argument("--tessera-backend", choices=["fs", "endpoint", "aws"],
                   default="fs")
    p.add_argument("--tessera-endpoint", default=None)
    p.add_argument("--tessera-bucket", default=None)
    p.add_argument("--tessera-table", default=None)
    p.add_argument("--tenant", default=None)
    p.add_argument("--work-dir", default=None)
    p.add_argument("--json-out", type=Path, default=None)
    ns = p.parse_args(argv)
    return vars(ns)


# @spec examples/soak-main
def main(argv=None):
    args = parse_args(argv)
    if args["check"] is not None:
        artifact = json.loads(args["check"].read_text())
        ok, results = check_gate(artifact)
        for r in results:
            mark = "PASS" if r["pass"] else "FAIL"
            print(f"{mark} {r['name']}: want={r['want']} got={r['got']}")
        return 0 if ok else 1
    if args["tessera_backend"] == "aws" and not (
            args["tessera_bucket"] and args["tessera_table"]):
        print("error: --tessera-backend=aws needs --tessera-bucket "
              "and --tessera-table", file=sys.stderr)
        return 2
    if args["tessera_backend"] == "endpoint" and not args["tessera_endpoint"]:
        print("error: --tessera-backend=endpoint needs --tessera-endpoint",
              file=sys.stderr)
        return 2
    args["tenant"] = args["tenant"] or f"soak-{int(time.time())}"
    args["work_dir"] = args["work_dir"] or tempfile.mkdtemp(prefix="soak-")
    artifact = run(args)
    out = args["json_out"] or Path(
        f"soak-{int(time.time())}.json")
    out.write_text(json.dumps(artifact, indent=2) + "\n")
    ok, results = check_gate(artifact)
    status = "PASS" if ok else "INCOMPLETE/FAIL vs gate minimums"
    print(f"wrote {out} — {status}")
    for r in results:
        if not r["pass"]:
            print(f"  below criterion: {r['name']} "
                  f"(want {r['want']}, got {r['got']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
