"""Measure consumer-shaped findings persistence across v1 and v2 publication modes."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time
import uuid

import boto3
from botocore.exceptions import UnauthorizedSSOTokenError

from experiments.aws_turso_rc_drill import cleanup, save, TAG_KEY
from tessera import LocalBackend
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.turso import BulkOperation, Operation, Runtime, RuntimeLimits
from tessera.experimental.turso.fencing import FencedBackend
from tessera.store import Store


INSERT_ISSUE = (
    "INSERT INTO scan_issue (uuid, scan_id, tool, rule_name, rule_set, severity, "
    "file_path, line_number, message, issue_key, new_issue) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
MODES = ("v1", "snapshot", "incremental", "native")
V2_PREFIX = {"snapshot": "turso-experiment", "incremental": "turso-blocks",
             "native": "turso-native"}


# @spec experiments/staging-session
def staging_session(profile, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    try:
        session.client("sts").get_caller_identity()
        return session
    except UnauthorizedSSOTokenError:
        # AWS CLI v2 can refresh SSO tokens that older embedded botocore cannot.
        # Keep the exported short-lived credentials in this process only.
        raw = subprocess.check_output([
            "aws", "configure", "export-credentials", "--profile", profile,
            "--format", "process",
        ])
        credentials = json.loads(raw)
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )


# @spec experiments/schema-from-consumer
def schema_from_consumer(ddl_dir):
    paths = [ddl_dir / name for name in ("repository.sql", "attempt_manifest.sql")]
    texts = [path.read_text() for path in paths]
    ddl = "\n".join(texts)
    statements = [part.strip() for part in re.sub(r"(?m)^--.*$", "", ddl).split(";")
                  if part.strip()]
    if len(statements) < 12 or not any("CREATE TABLE scan_issue" in s for s in statements):
        raise ValueError("unexpected consumer repository schema")
    return ddl, statements, {
        path.name: hashlib.sha256(text.encode()).hexdigest()
        for path, text in zip(paths, texts)
    }


# @spec experiments/params
def params(scan, issue):
    identity = f"scan-{scan}-issue-{issue}"
    return [identity, scan + 1, "semgrep", f"rule-{issue % 20}", None,
            "HIGH" if issue % 5 == 0 else "INFO", f"src/file-{issue % 50}.py",
            issue + 1, "representative finding text", identity, 1]


# @spec experiments/percentile
def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


# @spec experiments/remote-artifacts
def remote_artifacts(s3, bucket, mode, group):
    """Count retained physical bytes before the isolated bucket is destroyed."""
    prefix = f"{V2_PREFIX[mode]}/{group}/"
    counts = {"objects": 0, "bytes": 0, "image_objects": 0,
              "image_bytes": 0, "history_objects": 0, "history_bytes": 0}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            size = item["Size"]
            counts["objects"] += 1
            counts["bytes"] += size
            if "/images/" in item["Key"]:
                counts["image_objects"] += 1
                counts["image_bytes"] += size
            elif "/history/" in item["Key"]:
                counts["history_objects"] += 1
                counts["history_bytes"] += size
    return counts


# @spec experiments/local-artifacts
def local_artifacts(backend, mode, group):
    prefix = backend.root / V2_PREFIX[mode] / group
    counts = {"objects": 0, "bytes": 0, "image_objects": 0,
              "image_bytes": 0, "history_objects": 0, "history_bytes": 0}
    for path in prefix.rglob("*"):
        if not path.is_file():
            continue
        size = path.stat().st_size
        counts["objects"] += 1
        counts["bytes"] += size
        if "images" in path.parts:
            counts["image_objects"] += 1
            counts["image_bytes"] += size
        elif "history" in path.parts:
            counts["history_objects"] += 1
            counts["history_bytes"] += size
    return counts


# @spec experiments/measure
def measure(mode, backend, root, group, ddl, statements, scans, findings, chunk,
            submit_concurrency=1, atomic=False):
    latencies = []
    scan_latencies = []
    receipts = []
    operation_bytes = []
    checkpoint_versions = []
    if mode == "v1":
        store = Store(backend, root=root / "cache")
        try:
            store.create(group, ddl)
            for scan in range(scans):
                started = time.monotonic()
                def write(conn):
                    for issue in range(findings):
                        conn.execute(INSERT_ISSUE, params(scan, issue))
                    conn.commit()
                store.write(group, write)
                elapsed = (time.monotonic() - started) * 1000
                latencies.append(elapsed)
                scan_latencies.append(elapsed)
        finally:
            store.close()
        reopened = Store(backend, root=root / "recovery-cache")
        try:
            rows = reopened.read(group, lambda conn: conn.execute(
                "SELECT COUNT(*) FROM scan_issue").fetchone()[0])
        finally:
            reopened.close()
    else:
        options = {"incremental": mode == "incremental", "native": mode == "native"}
        runtime = Runtime(backend, group, schema=statements, **options,
                          limits=RuntimeLimits(
                              max_batch=1 if atomic else 16,
                              max_pending=1 if atomic else 32,
                              max_pending_bytes=8 * 1048576 + 4096 if atomic else 1048576,
                              batch_window_ms=0 if atomic else 20 if submit_concurrency > 1 else 0))
        try:
            checkpoint_key = (runtime._owner.storage.manifest()[0]["checkpoint"]["key"]
                              if mode == "native" else None)
            for scan in range(scans):
                scan_started = time.monotonic()
                operation_type = BulkOperation if atomic else Operation
                offsets = (0,) if atomic else range(0, findings, chunk)
                operations = [operation_type(f"scan-{scan}-issues-{offset}", [
                        (INSERT_ISSUE, params(scan, issue))
                        for issue in range(offset, findings if atomic else min(offset + chunk, findings))
                    ]) for offset in offsets]
                operation_bytes.extend(len(operation.payload) for operation in operations)

                def submit(operation):
                    started = time.monotonic()
                    receipt = runtime.submit(operation, timeout=180 if atomic else 60)
                    return receipt, (time.monotonic() - started) * 1000

                if submit_concurrency == 1:
                    outcomes = [submit(operation) for operation in operations]
                else:
                    with ThreadPoolExecutor(max_workers=submit_concurrency) as pool:
                        outcomes = list(pool.map(submit, operations))
                receipts.extend(receipt for receipt, _ in outcomes)
                latencies.extend(latency for _, latency in outcomes)
                scan_latencies.append((time.monotonic() - scan_started) * 1000)
                if mode == "native":
                    manifest, _ = runtime._owner.storage.manifest()
                    next_key = manifest["checkpoint"]["key"]
                    if next_key != checkpoint_key:
                        checkpoint_versions.append(manifest["version"])
                        checkpoint_key = next_key
            queue_metrics = runtime.metrics()
        finally:
            runtime.close()
        with Runtime(backend, group, **options) as reopened:
            rows = reopened.read("SELECT COUNT(*) FROM scan_issue")[0][0]
            with reopened._reader.open() as (conn, _):
                durable = {row[0] for row in conn.execute("SELECT id FROM _tessera_receipts")}
            expected = {f"scan-{scan}-issues-{offset}" for scan in range(scans)
                        for offset in ((0,) if atomic else range(0, findings, chunk))}
            if durable != expected or len(receipts) != len(expected):
                raise AssertionError("published receipt set differs")
    if rows != scans * findings:
        raise AssertionError(f"{mode} recovered {rows} issues")
    result = {
        "mode": mode, "scans": scans, "issues_per_scan": findings,
        "atomic_scan": atomic or mode == "v1",
        "operations": len(latencies), "rows_recovered": rows,
        "receipts_reconciled": len(receipts),
        "scan_completion_ms": {"median": statistics.median(scan_latencies),
                               "max": max(scan_latencies)},
        "ack_latency_ms": {"median": statistics.median(latencies),
                           "p95": percentile(latencies, 0.95)},
    }
    if mode != "v1":
        result["publication_batches"] = queue_metrics["batches"]
        result["mean_batch_width"] = queue_metrics["mean_batch_width"]
        result["encoded_operation_bytes"] = {"max": max(operation_bytes),
                                             "total": sum(operation_bytes)}
        result["published_versions"] = sorted({receipt.version for receipt in receipts})
        if mode == "native":
            result["checkpoint_versions"] = checkpoint_versions
    return result


# @spec experiments/run-local
def run_local(ddl, statements, scans, findings, chunk, repetitions, submit_concurrency,
              atomic=False, modes=MODES):
    results = []
    with tempfile.TemporaryDirectory(prefix="tessera-consumer-findings-") as directory:
        root = Path(directory)
        for repeat in range(repetitions):
            for mode in modes if repeat % 2 == 0 else tuple(reversed(modes)):
                path = root / f"{repeat}-{mode}"
                backend = LocalBackend(path / "objects")
                group = f"benchmark-{repeat}-{mode}"
                result = measure(mode, backend, path, group, ddl, statements,
                                 scans, findings, chunk, submit_concurrency, atomic)
                if mode != "v1":
                    result["retained_local"] = local_artifacts(backend, mode, group)
                results.append(result)
    return results


# @spec experiments/run-aws
def run_aws(args, ddl, statements):
    session = staging_session(args.profile, args.region)
    account = session.client("sts").get_caller_identity()["Account"]
    if account != "REDACTED":
        raise RuntimeError(f"unexpected staging account: {account}")
    nonce = uuid.uuid4().hex[:12]
    state = {"account": account, "region": args.region,
             "bucket": f"tessera-v2-rc-{account}-{nonce}",
             "table": f"tessera-v2-rc-{nonce}",
             "bucket_created": False, "table_created": False,
             "tag": {TAG_KEY: "consumer-findings-modes"}}
    save(args.state, state)
    report = {**state, "runs": [], "source_commit": args.source_commit}
    try:
        s3, ddb = session.client("s3"), session.client("dynamodb")
        s3.create_bucket(Bucket=state["bucket"], CreateBucketConfiguration={
            "LocationConstraint": args.region})
        state["bucket_created"] = report["bucket_created"] = True
        save(args.state, state)
        s3.put_bucket_tagging(Bucket=state["bucket"], Tagging={
            "TagSet": [{"Key": TAG_KEY, "Value": "consumer-findings-modes"}]})
        ddb.create_table(TableName=state["table"],
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": TAG_KEY, "Value": "consumer-findings-modes"}])
        state["table_created"] = report["table_created"] = True
        save(args.state, state)
        ddb.get_waiter("table_exists").wait(TableName=state["table"],
            WaiterConfig={"Delay": 2, "MaxAttempts": 30})
        with tempfile.TemporaryDirectory(prefix="tessera-consumer-aws-") as directory:
            root = Path(directory)
            for repeat in range(args.repetitions):
                for mode in args.modes if repeat % 2 == 0 else tuple(reversed(args.modes)):
                    raw = S3DynamoBackend(state["bucket"], state["table"], args.region,
                        s3_client=s3, dynamodb=session.resource("dynamodb"),
                        backoff_max_attempts=1)
                    backend = raw if mode == "v1" else FencedBackend(raw, ddb)
                    group = f"benchmark-{repeat}-{mode}"
                    result = measure(mode, backend, root / group,
                        group, ddl, statements, args.scans, args.findings, args.chunk,
                         args.submit_concurrency, args.atomic)
                    if mode != "v1":
                        result["retained_s3"] = remote_artifacts(
                            s3, state["bucket"], mode, group)
                    report["runs"].append(result)
    finally:
        report["cleanup_errors"] = cleanup(session, state)
        report["cleanup_complete"] = not report["cleanup_errors"]
        save(args.output, report)
        if report["cleanup_complete"]:
            args.state.unlink(missing_ok=True)
    if report["cleanup_errors"]:
        raise RuntimeError("cleanup incomplete: " + "; ".join(report["cleanup_errors"]))
    return report


# @spec experiments/consumer-findings-modes-benchmark-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer-ddl-dir", type=Path)
    parser.add_argument("--scans", type=int, default=1)
    parser.add_argument("--findings", type=int, default=500)
    parser.add_argument("--chunk", type=int, default=40)
    parser.add_argument("--submit-concurrency", type=int, default=1)
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--atomic", action="store_true",
                        help="publish each scan as one bounded BulkOperation")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--aws-staging", action="store_true")
    parser.add_argument("--profile", default="staging")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    if args.cleanup:
        if args.state is None:
            parser.error("--cleanup requires --state")
        state = json.loads(args.state.read_text())
        session = staging_session(args.profile, state["region"])
        if (session.client("sts").get_caller_identity()["Account"] != "REDACTED"
                or state["account"] != "REDACTED"
                or not state["bucket"].startswith("tessera-v2-rc-REDACTED-")
                or not state["table"].startswith("tessera-v2-rc-")):
            raise RuntimeError("cleanup target is outside this staging experiment")
        errors = cleanup(session, state)
        if errors:
            raise RuntimeError("cleanup incomplete: " + "; ".join(errors))
        args.state.unlink()
        return
    if args.consumer_ddl_dir is None:
        parser.error("--consumer-ddl-dir is required for a benchmark run")
    if not (1 <= args.scans <= 12 and 1 <= args.findings <= 5000
            and 1 <= args.chunk <= 64 and 1 <= args.repetitions <= 3
            and 1 <= args.submit_concurrency <= 16 and len(set(args.modes)) == len(args.modes)):
        parser.error("workload exceeds bounded limits")
    if args.aws_staging and (args.output is None or args.state is None):
        parser.error("AWS staging requires --output and --state")
    if args.atomic and args.submit_concurrency != 1:
        parser.error("--atomic requires --submit-concurrency 1")
    ddl, statements, hashes = schema_from_consumer(args.consumer_ddl_dir)
    checkout_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True).strip()
    if args.source_commit is not None and args.source_commit != checkout_commit:
        parser.error("--source-commit does not match the checked-out revision")
    args.source_commit = checkout_commit
    source_diff = subprocess.check_output([
        "git", "diff", "--binary", "--", "tessera/experimental/turso",
        "experiments/consumer_findings_modes_benchmark.py",
    ])
    report = {"workload": {"scans": args.scans, "findings_per_scan": args.findings,
                            "v2_chunk_size": args.chunk, "repetitions": args.repetitions,
                             "submit_concurrency": args.submit_concurrency,
                             "modes": args.modes,
                             "atomic_scan": args.atomic,
                            "consumer_schema_sha256": hashes,
                             "ack_boundary": "published transaction"}}
    report["source"] = {"commit": checkout_commit,
                        "working_diff_sha256": hashlib.sha256(source_diff).hexdigest(),
                        "working_tree_modified": bool(source_diff)}
    if args.aws_staging:
        report.update(run_aws(args, ddl, statements))
    else:
        report["backend"] = "LocalBackend"
        report["runs"] = run_local(ddl, statements, args.scans, args.findings,
                                   args.chunk, args.repetitions, args.submit_concurrency,
                                   args.atomic, args.modes)
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
