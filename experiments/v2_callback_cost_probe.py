"""Bounded isolated AWS probe of v2 CallbackStore commit shapes."""
import collections
import json
import math
import statistics
import tempfile
import threading
import time
import uuid
from pathlib import Path

from experiments.aws_turso_rc_drill import cleanup, save, TAG_KEY
from experiments.consumer_findings_modes_benchmark import staging_session, percentile, remote_artifacts
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.turso import CallbackStore
from tessera.experimental.turso.fencing import FencedBackend


REGION = "us-east-2"
ACCOUNT = "REDACTED"
REPORT = Path("/tmp/tessera-v2-orm-shape-cost-2026-09-24.json")
STATE = Path("/tmp/tessera-v2-orm-shape-cost-2026-09-24-state.json")


# @spec experiments/counter
class Counter:
    def __init__(self):
        self.lock = threading.Lock()
        self.calls = collections.Counter()

    def hook(self, model=None, **_):
        if model is not None:
            with self.lock:
                self.calls[f"{model.service_model.service_name}.{model.name}"] += 1

    def snapshot(self):
        with self.lock:
            return dict(self.calls)


# @spec experiments/difference
def difference(after, before):
    return {k: after.get(k, 0) - before.get(k, 0)
            for k in sorted(set(after) | set(before))
            if after.get(k, 0) != before.get(k, 0)}


# @spec experiments/time-summary
def time_summary(values):
    return {"count": len(values), "p50_ms": round(statistics.median(values), 1),
            "p95_ms": round(percentile(values, .95), 1),
            "max_ms": round(max(values), 1)}


# @spec experiments/v2-callback-cost-probe-main
def main():
    session = staging_session("staging", REGION)
    account = session.client("sts").get_caller_identity()["Account"]
    if account != ACCOUNT:
        raise RuntimeError(f"wrong AWS account: {account}")
    nonce = uuid.uuid4().hex[:12]
    state = {"account": account, "region": REGION,
             "bucket": f"tessera-v2-rc-{account}-{nonce}",
             "table": f"tessera-v2-rc-{nonce}",
             "bucket_created": False, "table_created": False,
             "tag": {TAG_KEY: "orm-shaped-callback-cost"}}
    save(STATE, state)
    report = {"state": state, "shapes": [], "source": "CallbackStore native, deployed consumer bridge",
              "schema": "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
              "scope": "isolated staging account; no production or consumer data"}
    counter = Counter()
    try:
        s3, ddb = session.client("s3"), session.client("dynamodb")
        resource = session.resource("dynamodb")
        for client in (s3, ddb, resource.meta.client):
            client.meta.events.register("before-call", counter.hook)
        s3.create_bucket(Bucket=state["bucket"], CreateBucketConfiguration={"LocationConstraint": REGION})
        state["bucket_created"] = True
        save(STATE, state)
        s3.put_bucket_tagging(Bucket=state["bucket"], Tagging={
            "TagSet": [{"Key": TAG_KEY, "Value": "orm-shaped-callback-cost"}]})
        ddb.create_table(TableName=state["table"],
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": TAG_KEY, "Value": "orm-shaped-callback-cost"}])
        state["table_created"] = True
        save(STATE, state)
        ddb.get_waiter("table_exists").wait(TableName=state["table"],
            WaiterConfig={"Delay": 2, "MaxAttempts": 30})
        raw = S3DynamoBackend(state["bucket"], state["table"], REGION,
                              s3_client=s3, dynamodb=resource, backoff_max_attempts=1)
        backend = FencedBackend(raw, ddb)
        with tempfile.TemporaryDirectory(prefix="tessera-v2-cost-probe-") as tmp:
            for label, transactions, rows_per_tx in (
                ("one_row_per_commit", 60, 1),
                ("ten_rows_per_commit", 30, 10),
                ("hundred_rows_per_commit", 12, 100),
            ):
                group = "cost-" + label.replace("_", "-")
                cache = Path(tmp) / group
                store = CallbackStore(backend, group_for_tenant=lambda _: group,
                                      cache_root=cache)
                store.create("tenant", "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
                before = counter.snapshot()
                writes = []
                for tx in range(transactions):
                    def insert(conn):
                        conn.executemany("INSERT INTO items(name) VALUES (?)",
                                         [(f"item-{tx}-{i}",) for i in range(rows_per_tx)])
                        conn.commit()
                    started = time.perf_counter()
                    store.write("tenant", insert)
                    writes.append((time.perf_counter() - started) * 1000)
                after_writes = counter.snapshot()
                reads = []
                for _ in range(12):
                    started = time.perf_counter()
                    count = store.read("tenant", lambda c: c.execute("SELECT COUNT(*) FROM items").fetchone()[0])
                    reads.append((time.perf_counter() - started) * 1000)
                after_reads = counter.snapshot()
                if count != transactions * rows_per_tx:
                    raise AssertionError(f"wrong recovered count for {label}: {count}")
                manifest, _ = store.backend.get(f"turso-native/{group}/manifest.json")
                manifest = json.loads(manifest)
                retained = remote_artifacts(s3, state["bucket"], "native", group)
                report["shapes"].append({"label": label, "transactions": transactions,
                    "rows_per_transaction": rows_per_tx, "rows_recovered": count,
                    "published_version": manifest["version"],
                    "writes": time_summary(writes), "reads": time_summary(reads),
                    "write_calls": difference(after_writes, before),
                    "read_calls": difference(after_reads, after_writes),
                    "retained_s3": retained})
                save(REPORT, report)
                store.close()
    finally:
        report["cleanup_errors"] = cleanup(session, state)
        report["cleanup_complete"] = not report["cleanup_errors"]
        save(REPORT, report)
        if report["cleanup_complete"]:
            STATE.unlink(missing_ok=True)
    if report["cleanup_errors"]:
        raise RuntimeError(report["cleanup_errors"])
    print(json.dumps(report))


if __name__ == "__main__":
    main()
