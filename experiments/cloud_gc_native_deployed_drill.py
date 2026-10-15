"""Isolated AWS drill of the native S3/DynamoDB mark, index, and manual sweep.

This measures the storage engine, not the HTTPS API, ORM, or monthly bill.
Run from an extracted, reviewed public source snapshot. Every run creates and
destroys its own bucket and table; never point this at deployed resources.
"""

from __future__ import annotations

import argparse
import json
import math
import select
import signal
import statistics
import subprocess
import sys
import time
import uuid
from collections import Counter
from hashlib import sha256
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from tessera.backends.base import LeaseLost
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.turso.fencing import FencedBackend
from tessera.experimental.turso.lifecycle import RetentionPolicy, RetentionSession
from tessera.experimental.turso.owner import BatchOwner, Request
from tessera.experimental.turso.snapshot import digest

ROOT = Path(__file__).resolve().parents[1]
GROUP = "gc-drill"
POLICY = RetentionPolicy(artifact_grace_seconds=0)


# @spec experiments/source-identity
def source_identity():
    path = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
    manifest = json.loads(path.read_bytes())
    if manifest.get("format") != "tessera-public-source-snapshot-1":
        raise ValueError("run from a reviewed public source snapshot")
    for name, expected in manifest["files"].items():
        candidate = (ROOT / name).resolve()
        if not candidate.is_relative_to(ROOT.resolve()) or not candidate.is_file():
            raise ValueError("public source file missing or outside snapshot: " + name)
        if sha256(candidate.read_bytes()).hexdigest() != expected:
            raise ValueError("public source checksum differs: " + name)
    return {"manifest_sha256": sha256(path.read_bytes()).hexdigest(),
            "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest()}


# @spec experiments/quantiles
def quantiles(samples):
    ordered = sorted(samples)
    if not ordered:
        return None
    return {"p50": round(ordered[math.ceil(.50 * len(ordered)) - 1], 3),
            "p95": round(ordered[math.ceil(.95 * len(ordered)) - 1], 3),
            "p99": round(ordered[math.ceil(.99 * len(ordered)) - 1], 3),
            "mean": round(statistics.mean(ordered), 3), "count": len(ordered)}


# @spec experiments/cloud-gc-native-deployed-drill-meter
class Meter:
    def __init__(self, max_requests, max_seconds):
        self.calls = Counter()
        self.phase = "setup"
        self.started = time.monotonic()
        self.max_requests = max_requests
        self.max_seconds = max_seconds
        self.enforce = True

    def before_call(self, model, **_kwargs):
        if self.enforce and (sum(self.calls.values()) >= self.max_requests
                             or time.monotonic() - self.started >= self.max_seconds):
            raise RuntimeError("drill request or elapsed budget exhausted")
        self.calls[(self.phase, model.service_model.service_name, model.name)] += 1

    def phase_calls(self, phase):
        return {f"{service}.{operation}": count
                for (name, service, operation), count in sorted(self.calls.items())
                if name == phase}

    def check(self):
        if time.monotonic() - self.started >= self.max_seconds:
            raise RuntimeError("drill elapsed budget exhausted")


def _missing(exc, codes):
    return isinstance(exc, ClientError) and exc.response["Error"]["Code"] in codes


# @spec experiments/takeover-probe
def takeover_probe(backend, table):
    """Displace a live process after one mark page and fence its next cursor CAS."""
    old_session = RetentionSession(backend, GROUP, policy=POLICY)
    old = old_session.__enter__()
    try:
        started = old.incremental_mark_step(max_metadata=1)
        first_page = old.incremental_mark_step(max_metadata=1)
        if started["phase"] != "history" or not started["started"]:
            raise RuntimeError("takeover mark did not start")
        if first_page["phase"] != "history" or first_page["marked"] != 1:
            raise RuntimeError("takeover first page did not advance")
        before, _ = backend.get_gc_mark_state(GROUP)
        previous = old_session._lease
        table.update_item(Key={"tenant_id": previous.tenant},
                          UpdateExpression="SET expires_at=:expired",
                          ExpressionAttributeValues={":expired": 0})
        with RetentionSession(backend, GROUP, policy=POLICY) as successor:
            current = successor.owner._lease
            if current.epoch <= previous.epoch:
                raise RuntimeError("takeover did not advance owner epoch")
            try:
                old.incremental_mark_step(max_metadata=1)
            except LeaseLost:
                pass
            else:
                raise RuntimeError("displaced owner advanced the mark")
            after_rejection, _ = backend.get_gc_mark_state(GROUP)
            if after_rejection != before:
                raise RuntimeError("displaced owner changed durable mark state")
            resumed = successor.incremental_mark_step(max_metadata=1)
            if resumed["phase"] != "history" or resumed["marked"] != 1:
                raise RuntimeError("new owner did not resume the mark")
            after_resume, _ = backend.get_gc_mark_state(GROUP)
            if after_resume == before:
                raise RuntimeError("new owner did not persist the resumed cursor")
        return {"stale_owner_rejected": True, "new_owner_resumed": True,
                "old_epoch": previous.epoch, "new_epoch": current.epoch,
                "cycle_id_unchanged": resumed["cycle_id"] == started["cycle_id"]}
    finally:
        # @spec experiments/cloud-gc-native-deployed-drill-crash-worker
        # The displaced owner must not release the new owner's lease.
        old_session.__exit__(LeaseLost, LeaseLost("owner displaced"), None)


# @spec experiments/crash-worker
def crash_worker(profile, expected_account_id, region, bucket, table):
    """Checkpoint one mark page, then wait while the parent kills this process."""
    session = boto3.Session(profile_name=profile, region_name=region)
    if session.client("sts").get_caller_identity()["Account"] != expected_account_id:
        raise RuntimeError("crash worker AWS account differs")
    s3 = session.client("s3")
    resource = session.resource("dynamodb")
    meter = Meter(100, 30)
    meter.phase = "crashed_mark"
    s3.meta.events.register("before-call.s3.*", meter.before_call)
    resource.meta.client.meta.events.register("before-call.dynamodb.*", meter.before_call)
    raw = S3DynamoBackend(bucket, table, region=region, s3_client=s3,
                          dynamodb=resource, backoff_max_attempts=1)
    backend = FencedBackend(raw, session.client("dynamodb"))
    backend.client.meta.events.register("before-call.dynamodb.*", meter.before_call)
    with RetentionSession(backend, GROUP, policy=POLICY, lease_ttl_ms=10_000) as lifecycle:
        started = lifecycle.incremental_mark_step(max_metadata=1)
        page = lifecycle.incremental_mark_step(max_metadata=1)
        if (started["phase"] != "history" or not started["started"]
                or page["phase"] != "history" or page["marked"] != 1):
            raise RuntimeError("crash worker did not persist its first mark page")
        state_raw, _ = backend.get_gc_mark_state(GROUP)
        print(json.dumps({"phase": "checkpointed", "cycle_id": started["cycle_id"],
                          "state_sha256": digest(state_raw),
                          "owner_epoch": lifecycle.owner._lease.epoch,
                          "lease_expires_at": lifecycle.owner._lease.expires_at,
                          "sdk_calls": meter.phase_calls("crashed_mark")}), flush=True)
        while True:
            time.sleep(60)


# @spec experiments/process-kill-probe
def process_kill_probe(backend, *, profile, expected_account_id, region, bucket, table):
    """SIGKILL a separate marking process and resume after its lease expires."""
    command = [sys.executable, str(Path(__file__).resolve()), "--crash-worker",
               "--profile", profile, "--expected-account-id", expected_account_id,
               "--region", region, "--bucket", bucket, "--table", table]
    child = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, bufsize=1)
    checkpoint = None
    try:
        readable, _, _ = select.select([child.stdout], [], [], 45)
        if readable:
            line = child.stdout.readline()
            if line:
                checkpoint = json.loads(line)
        if checkpoint is None or checkpoint.get("phase") != "checkpointed":
            raise RuntimeError("crash worker did not report a durable mark page")
        state_before, _ = backend.get_gc_mark_state(GROUP)
        if digest(state_before) != checkpoint["state_sha256"]:
            raise RuntimeError("crash worker mark state differs from checkpoint")
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    if child.returncode != -signal.SIGKILL:
        raise RuntimeError("mark worker did not die from SIGKILL")
    state_after, _ = backend.get_gc_mark_state(GROUP)
    if state_after != state_before:
        raise RuntimeError("mark state changed after worker death")
    wait = max(0, checkpoint["lease_expires_at"] - time.time() + 0.75)
    if wait > 12:
        raise RuntimeError("crash worker lease expiry is outside expected window")
    time.sleep(wait)
    with RetentionSession(backend, GROUP, policy=POLICY) as lifecycle:
        new_epoch = lifecycle.owner._lease.epoch
        if new_epoch <= checkpoint["owner_epoch"]:
            raise RuntimeError("recovery owner did not advance the epoch")
        resumed = lifecycle.incremental_mark_step(max_metadata=1)
        if (resumed["cycle_id"] != checkpoint["cycle_id"]
                or resumed["phase"] != "history" or resumed["marked"] != 1):
            raise RuntimeError("recovery owner did not resume the same mark cycle")
    return {"child_exit_signal": "SIGKILL", "old_epoch": checkpoint["owner_epoch"],
            "new_epoch": new_epoch, "same_cycle_resumed": True,
            "lease_wait_s": round(wait, 3), "sdk_calls": checkpoint["sdk_calls"]}


# @spec experiments/destroy
def destroy(s3, ddb, bucket, table):
    errors = []
    try:
        for _ in range(1000):
            try:
                page = s3.list_objects_v2(Bucket=bucket, MaxKeys=1000)
            except ClientError as exc:
                if _missing(exc, {"NoSuchBucket", "404"}):
                    break
                raise
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if not objects:
                break
            deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            if deleted.get("Errors"):
                raise RuntimeError("S3 batch deletion reported errors")
        else:
            raise RuntimeError("cleanup object-page cap exceeded")
        try:
            s3.delete_bucket(Bucket=bucket)
        except ClientError as exc:
            if not _missing(exc, {"NoSuchBucket", "404"}):
                raise
        try:
            s3.head_bucket(Bucket=bucket)
            errors.append("bucket still exists")
        except ClientError as exc:
            if not _missing(exc, {"NoSuchBucket", "404"}):
                raise
    except Exception as exc:  # noqa: BLE001 - teardown must continue to DynamoDB
        errors.append("bucket: " + type(exc).__name__ + ": " + str(exc)[:160])
    try:
        try:
            ddb.delete_table(TableName=table)
            ddb.get_waiter("table_not_exists").wait(
                TableName=table, WaiterConfig={"Delay": 2, "MaxAttempts": 90})
        except ClientError as exc:
            if not _missing(exc, {"ResourceNotFoundException"}):
                raise
        try:
            ddb.describe_table(TableName=table)
            errors.append("table still exists")
        except ClientError as exc:
            if not _missing(exc, {"ResourceNotFoundException"}):
                raise
    except Exception as exc:  # noqa: BLE001 - teardown must report every failure
        errors.append("table: " + type(exc).__name__ + ": " + str(exc)[:160])
    return errors


# @spec experiments/cloud-gc-native-deployed-drill-run
def run(session, *, expected_account_id, region, commits, output,
        max_requests=100_000, max_seconds=1800, identity=None,
        incremental_index_chunks=None, mark_metadata_per_step=32,
        takeover=False, process_kill=False, profile=None):
    if not expected_account_id.isdecimal() or len(expected_account_id) != 12:
        raise ValueError("expected account ID must have 12 digits")
    if type(commits) is not int or not 1 <= commits <= 10_000:
        raise ValueError("commits must be 1..10000")
    if type(takeover) is not bool or (takeover and commits < 2):
        raise ValueError("takeover requires at least two commits")
    if (type(process_kill) is not bool or (process_kill and commits < 2)
            or (process_kill and takeover)
            or (process_kill and (not isinstance(profile, str) or not profile))):
        raise ValueError("process kill requires a profile, two commits and no forced takeover")
    if not 100 <= max_requests <= 1_000_000 or not 60 <= max_seconds <= 7200:
        raise ValueError("invalid drill budget")
    if incremental_index_chunks is not None and (type(incremental_index_chunks) is not int
                                                 or not 1 <= incremental_index_chunks <= 100):
        raise ValueError("incremental index chunks must be 1..100")
    if type(mark_metadata_per_step) is not int or not 1 <= mark_metadata_per_step <= 100:
        raise ValueError("mark metadata per step must be 1..100")
    identity = source_identity() if identity is None else identity
    actual = session.client("sts").get_caller_identity()["Account"]
    if actual != expected_account_id:
        raise RuntimeError("AWS account differs from --expected-account-id")
    suffix = uuid.uuid4().hex[:16]
    bucket = "tessera-gc-drill-" + suffix
    table = "tessera-gc-drill-" + suffix
    s3 = session.client("s3")
    ddb = session.client("dynamodb")
    resource = session.resource("dynamodb")
    meter = Meter(max_requests, max_seconds)
    s3.meta.events.register("before-call.s3.*", meter.before_call)
    ddb.meta.events.register("before-call.dynamodb.*", meter.before_call)
    # boto3 resource operations use their own client; meter that client too.
    resource.meta.client.meta.events.register("before-call.dynamodb.*", meter.before_call)
    report = {"status": "failed", "scope": "native S3/DynamoDB engine only",
              "region": region, "account_id": actual, "bucket": bucket,
              "table": table, "commits": commits, "source": identity,
              "incremental_index_chunks": incremental_index_chunks,
              "mark_metadata_per_step": mark_metadata_per_step,
              "takeover_enabled": takeover,
              "process_kill_enabled": process_kill,
              "policy": {"artifact_grace_seconds": 0},
              "sdk_calls_definition": "observed botocore before-call attempts; not billed units",
              "latency_unit": "milliseconds"}
    failure = None
    try:
        if region != session.region_name:
            raise ValueError("session region differs from requested region")
        create = {"Bucket": bucket}
        if region != "us-east-1":
            create["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**create)
        resource.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST").wait_until_exists()
        raw = S3DynamoBackend(bucket, table, region=region, s3_client=s3,
                              dynamodb=resource, backoff_max_attempts=1)
        backend = FencedBackend(raw, ddb)
        meter.phase = "publish"
        publish_ms = []
        publish_started = time.monotonic()
        with BatchOwner(backend, GROUP, native=True,
                        schema=["CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)"]) as owner:
            for number in range(1, commits + 1):
                meter.check()
                start = time.monotonic()
                owner.commit([Request(str(number), [
                    ("INSERT INTO items VALUES(?, ?)", [number, str(number)])])])
                publish_ms.append((time.monotonic() - start) * 1000)
        publish_wall = time.monotonic() - publish_started
        report["publish_ms"] = quantiles(publish_ms)
        report["publish_wall_s"] = round(publish_wall, 3)
        report["publish_commits_per_s"] = round(commits / publish_wall, 3)
        report["publish_sdk_calls"] = meter.phase_calls("publish")
        # A valid, deliberately orphaned image proves that sweep executes a DELETE.
        orphan = f"turso-native/{GROUP}/images/{uuid.uuid4().hex}.db"
        s3.put_object(Bucket=bucket, Key=orphan, Body=b"orphan-drill")
        # LastModified has second precision. The cutoff keeps its entire second.
        time.sleep(1.1)
        if takeover:
            meter.phase = "takeover"
            report["takeover"] = takeover_probe(backend, resource.Table(table))
            report["takeover_sdk_calls"] = meter.phase_calls("takeover")
            if not report["takeover"]["cycle_id_unchanged"]:
                raise RuntimeError("takeover did not resume the same mark cycle")
        if process_kill:
            meter.check()
            meter.phase = "process_kill"
            report["process_kill"] = process_kill_probe(
                backend, profile=profile,
                expected_account_id=expected_account_id, region=region,
                bucket=bucket, table=table)
            report["process_kill_parent_sdk_calls"] = meter.phase_calls("process_kill")
            child_calls = sum(report["process_kill"]["sdk_calls"].values())
            meter.max_requests -= child_calls
            if meter.max_requests <= sum(meter.calls.values()):
                raise RuntimeError("drill request budget exhausted by crash probe")
            meter.check()
        meter.phase = "mark"
        mark_ms = []
        for _ in range(math.ceil((commits + 1) / mark_metadata_per_step) + 8):
            meter.check()
            start = time.monotonic()
            with RetentionSession(backend, GROUP, policy=POLICY) as lifecycle:
                mark = lifecycle.incremental_mark_step(max_metadata=mark_metadata_per_step)
            mark_ms.append((time.monotonic() - start) * 1000)
            if mark["phase"] == "marked_unverified":
                break
            if mark["phase"] in {"budget", "deadline"}:
                raise RuntimeError("mark could not advance within its page budget")
        else:
            raise RuntimeError("mark step cap exceeded")
        report["mark_ms"] = quantiles(mark_ms)
        report["mark_sdk_calls"] = meter.phase_calls("mark")
        meter.phase = "index"
        index_ms = []
        for _ in range(math.ceil((commits + 1) / mark_metadata_per_step) + 20):
            meter.check()
            start = time.monotonic()
            with RetentionSession(backend, GROUP, policy=POLICY) as lifecycle:
                if incremental_index_chunks is None:
                    reduced = lifecycle.exact_index_step()
                else:
                    reduced = lifecycle.incremental_exact_index_step(
                        max_chunks=incremental_index_chunks)
            index_ms.append((time.monotonic() - start) * 1000)
            if reduced["phase"] == "indexed_unverified":
                break
            if reduced["phase"] in {"budget", "deadline"}:
                raise RuntimeError("exact index could not advance within its page budget")
        else:
            raise RuntimeError("exact index step cap exceeded")
        report["index_ms"] = round(sum(index_ms), 3)
        report["index_step_ms"] = quantiles(index_ms)
        report["index_result"] = reduced
        report["index_sdk_calls"] = meter.phase_calls("index")
        state_raw, _ = backend.get_gc_mark_state(GROUP)
        state = json.loads(state_raw)
        index_raw, _ = backend.get(state["index_root"]["key"])
        index = json.loads(index_raw)
        if (digest(index_raw) != state["index_root"]["sha256"]
                or len(index["keys"]) != state["index_root"]["key_count"]):
            raise RuntimeError("exact index mismatch")
        report["index_bytes"] = len(index_raw)
        report["index_keys"] = len(index["keys"])
        report["mark_chunks"] = state["chunk_count"]
        report["mark_bytes"] = state["mark_bytes"]
        report["history_count"] = state["history_count"]
        meter.phase = "sweep"
        sweep_ms = []
        deleted = 0
        for _ in range(math.ceil((commits + 3) / 100) + 20):
            meter.check()
            start = time.monotonic()
            with RetentionSession(backend, GROUP, policy=POLICY) as lifecycle:
                swept = lifecycle.experimental_image_sweep_step()
            sweep_ms.append((time.monotonic() - start) * 1000)
            deleted += swept.get("deleted", 0)
            if swept["phase"] == "complete":
                break
            if swept["phase"] in {"budget", "deadline"}:
                raise RuntimeError("sweep could not advance within its page budget")
        else:
            raise RuntimeError("sweep step cap exceeded")
        report["sweep_ms"] = quantiles(sweep_ms)
        report["sweep_deleted_images"] = deleted
        report["sweep_sdk_calls"] = meter.phase_calls("sweep")
        if deleted != 1:
            raise RuntimeError("orphan image was not deleted exactly once")
        try:
            s3.head_object(Bucket=bucket, Key=orphan)
            raise RuntimeError("orphan image remains")
        except ClientError as exc:
            if not _missing(exc, {"404", "NoSuchKey", "NotFound"}):
                raise
        meter.phase = "recovery"
        start = time.monotonic()
        with BatchOwner(backend, GROUP, native=True) as owner:
            if owner.read("SELECT count(*) FROM items") != [(commits,)]:
                raise RuntimeError("cold head recovery failed")
            expected_edges = ([(1, "1")] if commits == 1
                              else [(1, "1"), (commits, str(commits))])
            if owner.read("SELECT id,value FROM items WHERE id IN (?,?) ORDER BY id",
                          (1, commits)) != expected_edges:
                raise RuntimeError("cold head edge rows differ")
            if owner.lookup("1") is None or owner.lookup(str(commits)) is None:
                raise RuntimeError("acknowledged receipts missing after cold recovery")
        report["cold_head_ms"] = round((time.monotonic() - start) * 1000, 3)
        report["recovered_edge_rows_and_receipts"] = True
        with RetentionSession(backend, GROUP, policy=POLICY) as lifecycle:
            if lifecycle.inspect(0, "SELECT count(*) FROM items") != [(0,)]:
                raise RuntimeError("historical version recovery failed")
        report["recovery_sdk_calls"] = meter.phase_calls("recovery")
        report["recovered_rows"] = commits
        meter.phase = "inventory"
        objects = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            objects.extend(page.get("Contents", []))
        report["remaining_s3_objects"] = len(objects)
        report["remaining_s3_bytes"] = sum(item["Size"] for item in objects)
        report["inventory_sdk_calls"] = meter.phase_calls("inventory")
        report["status"] = "passed"
    except BaseException as exc:  # noqa: BLE001 - destroy resources even on interruption
        failure = exc
        report["error"] = type(exc).__name__ + ": " + str(exc)[:300]
    finally:
        meter.enforce = False
        meter.phase = "cleanup"
        cleanup_errors = destroy(s3, ddb, bucket, table)
        report["cleanup_sdk_calls"] = meter.phase_calls("cleanup")
        report["resources_destroyed"] = not cleanup_errors
        report["cleanup_errors"] = cleanup_errors
        child_calls = sum(report.get("process_kill", {}).get("sdk_calls", {}).values())
        report["total_sdk_calls"] = sum(meter.calls.values()) + child_calls
        report["elapsed_s"] = round(time.monotonic() - meter.started, 3)
        if cleanup_errors:
            report["status"] = "failed"
        Path(output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if failure is not None:
        raise failure
    if cleanup_errors:
        raise RuntimeError("isolated drill cleanup failed: " + "; ".join(cleanup_errors))
    return report


# @spec experiments/cloud-gc-native-deployed-drill-main
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--crash-worker":
        worker_parser = argparse.ArgumentParser(description="Internal crash worker")
        worker_parser.add_argument("--profile", required=True)
        worker_parser.add_argument("--expected-account-id", required=True)
        worker_parser.add_argument("--region", required=True)
        worker_parser.add_argument("--bucket", required=True)
        worker_parser.add_argument("--table", required=True)
        worker_args = worker_parser.parse_args(sys.argv[2:])
        crash_worker(worker_args.profile, worker_args.expected_account_id,
                     worker_args.region, worker_args.bucket, worker_args.table)
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--commits", type=int, default=30)
    parser.add_argument("--max-requests", type=int, default=100_000)
    parser.add_argument("--max-seconds", type=int, default=1800)
    parser.add_argument("--incremental-index-chunks", type=int,
                        help="verify this many mark chunks per fenced index run")
    parser.add_argument("--mark-metadata-per-step", type=int, default=32,
                         help="history/pin metadata entries per mark page (default: 32)")
    parser.add_argument("--takeover", action="store_true",
                        help="force lease expiry after one mark page and verify stale-owner fencing")
    parser.add_argument("--process-kill", action="store_true",
                        help="SIGKILL a separate marking worker and recover its durable cursor")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = run(boto3.Session(profile_name=args.profile, region_name=args.region),
                 expected_account_id=args.expected_account_id, region=args.region,
                 commits=args.commits, output=args.output,
                 max_requests=args.max_requests, max_seconds=args.max_seconds,
                  incremental_index_chunks=args.incremental_index_chunks,
                  mark_metadata_per_step=args.mark_metadata_per_step,
                   takeover=args.takeover, process_kill=args.process_kill,
                   profile=args.profile)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
