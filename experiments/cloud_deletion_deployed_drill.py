"""Deploy a disposable BYOC stack and verify scheduled deletion over HTTPS."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # Use this source snapshot's client for the drill.
from tessera.experimental.cloud_client import CloudSqlClient, _open

MODULE = ROOT / "deploy/aws/terraform"


def _terraform(arguments, *, env):
    return subprocess.check_output(["terraform", f"-chdir={MODULE}", *arguments],
                                   cwd=ROOT, env=env, text=True, stderr=subprocess.STDOUT)


def _request(base, method, path, token, body=None):
    raw = None if body is None else json.dumps(body).encode()
    request = Request(base + path, data=raw, method=method,
                      headers={"Authorization": "Bearer " + token,
                               "Content-Type": "application/json"})
    try:
        with _open(request, timeout=40) as response:
            return response.status, json.loads(response.read())
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _bulk_probe(base, database_id, token):
    """One consumer-shaped 5,000-row scan, replay, and atomic rollback over HTTPS."""
    rows = []
    message = "representative finding text " + "x" * 160
    for number in range(5000):
        key = f"scan-1-issue-{number}"
        rows.append((key, 1, "semgrep", f"rule-{number % 20}", None,
                     "HIGH" if number % 5 == 0 else "INFO",
                     f"src/file-{number % 50}.py", number + 1,
                     message, key, 1))
    placeholder = "(" + ",".join("?" for _ in range(11)) + ")"
    statements = []
    for offset in range(0, len(rows), 64):
        chunk = rows[offset:offset + 64]
        statements.append([
            "INSERT INTO findings VALUES " + ",".join([placeholder] * len(chunk)),
            [value for row in chunk for value in row],
        ])
    body = {"operation_id": "bulk-scan-1", "statements": statements}
    payload_bytes = len(json.dumps(body, separators=(",", ":")).encode())
    assert 1_048_576 < payload_bytes < 2_097_152
    path = f"/v1/databases/{database_id}/transactions"
    started = time.monotonic()
    status, receipt = _request(base, "POST", path, token, body)
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    assert status == 200 and receipt["version"] == 2, (status, receipt)
    assert _request(base, "POST", path, token, body) == (200, receipt)
    bad = {"operation_id": "bulk-scan-bad", "statements": [
        ["INSERT INTO findings VALUES " + placeholder,
         ["new-key", 2, "semgrep", "rule", None, "INFO", "src/new.py", 1,
          "new", "new-key", 1]],
        ["INSERT INTO findings VALUES " + placeholder, list(rows[0])[:-1] + [2]],
    ]}
    status, failed = _request(base, "POST", path, token, bad)
    assert status == 422 and failed["error"] == "sql_failed", (status, failed)
    status, lookup = _request(base, "GET",
                              f"/v1/databases/{database_id}/operations/bulk-scan-bad", token)
    assert status == 200 and lookup["state"] == "absent"
    status, count = _request(base, "POST", f"/v1/databases/{database_id}/query",
                             token, {"sql": "SELECT count(*) FROM findings"})
    assert status == 200 and count["rows"] == [[5000]], (status, count)
    return {"rows": 5000, "statements": len(statements), "payload_bytes": payload_bytes,
            "write_ms": elapsed_ms, "published_version": receipt["version"],
            "idempotent_replay": True, "duplicate_rollback": True}


def _cost_samples(logs, log_group, started_ms, ended_ms, expected):
    """Collect redacted SDK-call summaries after CloudWatch delivery catches up."""
    deadline = time.monotonic() + 45
    samples = []
    while True:
        events = (event for page in logs.get_paginator("filter_log_events").paginate(
            logGroupName=log_group, startTime=started_ms, endTime=ended_ms,
            filterPattern='"TesseraDB AWS call sample:"')
            for event in page.get("events", []))
        samples = []
        for event in events:
            marker = "TesseraDB AWS call sample: "
            if marker in event["message"]:
                sample = json.loads(event["message"].split(marker, 1)[1])
                if (sample.get("kind") == "tessera_aws_call_sample_v1"
                        and sample.get("operation") not in
                        {"retention", "scheduled_deletion"}):
                    samples.append(sample)
        if len(samples) >= expected or time.monotonic() >= deadline:
            break
        time.sleep(3)
    calls = Counter()
    attempts = Counter()
    for sample in samples:
        calls.update(sample["logical_sdk_calls"])
        attempts.update(sample.get("sdk_http_attempts", {}))
    return {"minimum_expected_http_requests": expected,
            "observed_samples": len(samples),
            "complete": len(samples) >= expected,
            "logical_sdk_calls": dict(sorted(calls.items())),
            "sdk_http_attempts": dict(sorted(attempts.items())),
            "sdk_http_attempts_observed": all(
                "sdk_http_attempts" in sample for sample in samples),
            "logical_not_billed_units": True, "samples": samples}


def _cold_recovery_samples(profile, region, bucket, table, database_id, scans,
                           *, samples):
    """Read the last benchmark receipt and rows from new external processes."""
    from examples.cloud_matched_scan_benchmark import summary

    durations = []
    calls = Counter()
    attempts = Counter()
    for _ in range(samples):
        started = time.monotonic()
        raw = subprocess.check_output([
            sys.executable, "-m", "experiments.cloud_api_recovery_probe",
            "--profile", profile, "--region", region, "--bucket", bucket,
            "--table", table, "--database-id", database_id,
            "--operation-id", f"matched-scan-{scans}",
            "--sql", "SELECT COUNT(*) FROM scan_issue WHERE scan_id=?",
            "--params-json", json.dumps([scans]),
            "--expected-rows-json", "[[500]]",
        ], cwd=ROOT, text=True, timeout=120)
        durations.append(round((time.monotonic() - started) * 1000, 3))
        result = json.loads(raw)
        if result["published_version"] != scans or result["rows"] != [[500]]:
            raise AssertionError("cold benchmark recovery version or rows differ")
        calls.update(result["logical_sdk_calls"])
        attempts.update(result["sdk_http_attempts"])
    return {"scope": "fresh external process; no server failure injected",
            "process_wall_ms": summary(durations),
            "logical_sdk_calls": dict(sorted(calls.items())),
            "sdk_http_attempts": dict(sorted(attempts.items())),
            "sdk_attempts_not_billed_units": True}


# @spec experiments/cloud-deletion-deployed-drill-drill
# @spec experiments/drill
def drill(profile, region, output, lambda_zip, *, usage_only=False,
          restore_drill=False, bulk_drill=False, benchmark_scans=0,
          benchmark_ddl_dir=None, benchmark_rows_per_statement=40,
          recovery_samples=5,
          cost_snapshot=False, rate_drill=False):
    session = boto3.Session(profile_name=profile, region_name=region)
    secret_client = session.client("secretsmanager")
    logs = session.client("logs") if benchmark_scans else None
    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    suffix = uuid4().hex[:10]
    name = "tessera-del-" + suffix
    state = Path(output).resolve().with_suffix(".tfstate")
    admin = secrets.token_urlsafe(48)
    report = {"region": region, "status": "failed", "stack_name": name,
              "lambda_zip_sha256": sha256(Path(lambda_zip).read_bytes()).hexdigest()}
    started_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if (ROOT / ".git").exists():
        report["source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    else:
        manifest = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
        if not manifest.is_file():
            raise RuntimeError("public source manifest required outside Git checkout")
        report["public_manifest_sha256"] = sha256(manifest.read_bytes()).hexdigest()
    env = {**os.environ, "AWS_PROFILE": profile, "AWS_REGION": region,
           "AWS_DEFAULT_REGION": region}
    secret_arn = None
    variables = []
    error = None
    try:
        secret_arn = secret_client.create_secret(Name=name + "-admin",
                                                  SecretString=admin)["ARN"]
        variables = ["-var=name=" + name, "-var=region=" + region,
                     "-var=lambda_zip_path=" + str(Path(lambda_zip).resolve()),
                     "-var=admin_secret_arn=" + secret_arn,
                     "-var=force_destroy_bucket=true"]
        if benchmark_scans:
            variables.append("-var=enable_aws_call_metrics=true")
        _terraform(["apply", "-auto-approve", "-input=false", "-no-color",
                    "-state=" + str(state), *variables], env=env)
        base = _terraform(["output", "-raw", "-state=" + str(state), "api_url"],
                          env=env).strip()
        bucket = _terraform(["output", "-raw", "-state=" + str(state), "data_bucket"],
                            env=env).strip()
        table = _terraform(["output", "-raw", "-state=" + str(state),
                            "coordination_table"], env=env).strip()
        indexes = dynamodb.describe_table(TableName=table)["Table"].get(
            "GlobalSecondaryIndexes", [])
        assert any(index["IndexName"] == "pending-deletions"
                   and index["IndexStatus"] == "ACTIVE" for index in indexes), indexes
        report["pending_deletions_index_active"] = True

        tenant_body = {"name": "deployed-drill", "request_id": uuid4().hex}
        if bulk_drill:
            tenant_body["limits"] = {"max_databases": 32,
                                      "max_frontier_bytes": 1_073_741_824,
                                      "max_operation_bytes": 2_097_152,
                                      "max_tokens": 64}
        status, tenant = _request(base, "POST", "/v1/tenants", admin, tenant_body)
        assert status == 201, (status, tenant)
        status, management = _request(base, "POST",
                                      f"/v1/tenants/{tenant['id']}/tokens", admin)
        assert status == 201, (status, management)
        manager = management["token"]

        create_body = {"name": "scheduled-delete", "schema": [
            "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"],
            "request_id": uuid4().hex}
        if bulk_drill:
            create_body["schema"].append(
                "CREATE TABLE findings(id TEXT PRIMARY KEY, scan_id INTEGER, source TEXT, "
                "rule TEXT, nullable TEXT, severity TEXT, path TEXT, line INTEGER, "
                "message TEXT, issue_key TEXT, count INTEGER)")
            create_body["limits"] = {"max_frontier_bytes": 67_108_864,
                                     "max_operation_bytes": 2_097_152, "max_tokens": 64}
        status, created = _request(base, "POST", "/v1/databases", manager, create_body)
        assert status == 201, (status, created)
        database_id = created["id"]
        status, issued = _request(base, "POST", f"/v1/databases/{database_id}/tokens",
                                  manager, {"scope": "write"})
        assert status == 201, (status, issued)
        sql_client = CloudSqlClient(base, database_id, issued["token"])
        receipt = sql_client.write("drill-write", [["INSERT INTO kv VALUES(?, ?)",
                                                     ["k", "v"]]])
        assert receipt["version"] == 1, receipt
        assert sql_client.lookup("drill-write")["state"] == "published"
        assert sql_client.read("SELECT v FROM kv WHERE k=?", ["k"],
                               min_version=receipt["version"]) == [["v"]]
        report["sql_client_https"] = True
        if rate_drill:
            from tessera.experimental.cloud.tenant_rate import (
                DynamoDBTenantRateMeter, TenantRateLimited)

            meter = DynamoDBTenantRateMeter(dynamodb, table)
            contention_tenant = uuid4().hex
            contention_limits = {"max_requests_per_minute": 2,
                                 "max_body_bytes_per_minute": 1_024}
            contention_now = int(time.time() // 60) * 60 + 1

            def contend(_):
                try:
                    meter.admit(contention_tenant, 100, contention_limits,
                                now=contention_now)
                    return True
                except TenantRateLimited:
                    return False

            with ThreadPoolExecutor(max_workers=5) as pool:
                contention_results = list(pool.map(contend, range(5)))
            assert contention_results.count(True) == 2, contention_results
            # Keep all four tenant requests inside one fixed UTC minute.
            seconds_into_minute = time.time() % 60
            if seconds_into_minute > 5:
                time.sleep(61 - seconds_into_minute)
            rate_minute = int(time.time() // 60)
            status, limited = _request(base, "POST", "/v1/tenants", admin, {
                "name": "rate-drill", "request_id": uuid4().hex,
                "rate_limits": {"max_requests_per_minute": 3,
                                "max_body_bytes_per_minute": 1_048_576}})
            assert status == 201, (status, limited)
            status, manager_token = _request(
                base, "POST", f"/v1/tenants/{limited['id']}/tokens", admin)
            assert status == 201, (status, manager_token)
            status, limited_db = _request(base, "POST", "/v1/databases",
                                          manager_token["token"], {
                                              "name": "limited", "request_id": uuid4().hex,
                                              "schema": ["CREATE TABLE kv(v INTEGER)"]})
            assert status == 201, (status, limited_db)
            status, limited_token = _request(
                base, "POST", f"/v1/databases/{limited_db['id']}/tokens",
                manager_token["token"], {"scope": "write"})
            assert status == 201, (status, limited_token)
            status, rows = _request(
                base, "POST", f"/v1/databases/{limited_db['id']}/query",
                limited_token["token"], {"sql": "SELECT count(*) FROM kv"})
            assert status == 200 and rows["rows"] == [[0]], (status, rows)
            status, rejected = _request(
                base, "POST", f"/v1/databases/{limited_db['id']}/transactions",
                limited_token["token"], {"operation_id": "rejected-by-budget",
                                          "statements": [["INSERT INTO kv VALUES(1)", []]]})
            assert status == 429 and rejected["error"] == "rate_limited", (status, rejected)
            assert int(time.time() // 60) == rate_minute, "rate probe crossed minute"
            status, increased = _request(
                base, "PUT", f"/v1/tenants/{limited['id']}/rate-limits", admin,
                {"rate_limits": {"max_requests_per_minute": 4,
                                 "max_body_bytes_per_minute": 1_048_576}})
            assert status == 200 and increased["rate_limits"]["max_requests_per_minute"] == 4
            status, accepted = _request(
                base, "POST", f"/v1/databases/{limited_db['id']}/transactions",
                limited_token["token"], {"operation_id": "rejected-by-budget",
                                          "statements": [["INSERT INTO kv VALUES(1)", []]]})
            assert status == 200 and accepted["version"] == 1, (status, accepted)
            counter = dynamodb.get_item(
                TableName=table,
                Key={"tenant_id": {"S": "_tessera_cloud/rate/" + limited["id"] + "/"
                                    + str(rate_minute)}},
                ConsistentRead=True).get("Item")
            assert counter is not None and counter["requests"]["N"] == "4", counter
            report["tenant_rate_budget"] = {
                "initial_admitted_requests": 3, "fourth_request_rejected": True,
                "final_counter_requests": 4,
                "same_minute_limit_raise_admitted": True,
                "concurrent_admissions": contention_results.count(True),
                "concurrent_rejections": contention_results.count(False),
                "retry_after_seconds": rejected["retry_after_seconds"],
                "counter_expires_at": int(counter["quota_expires_at"]["N"]),
                "ttl_status": dynamodb.describe_time_to_live(
                    TableName=table)["TimeToLiveDescription"]["TimeToLiveStatus"]}
        if bulk_drill:
            report["bulk_scan"] = _bulk_probe(base, database_id, issued["token"])
        if benchmark_scans:
            from examples.cloud_matched_scan_benchmark import Tessera, benchmark, schema

            benchmark_started_ms = int(time.time() * 1000) - 1000
            benchmark_schema, hashes = schema(benchmark_ddl_dir)
            benchmark_client = Tessera(benchmark_schema, base, manager)
            try:
                matched = benchmark(benchmark_client, benchmark_scans, 500,
                                    benchmark_rows_per_statement)
                cold = _cold_recovery_samples(
                    profile, region, bucket, table, benchmark_client.database_id,
                    benchmark_scans, samples=recovery_samples)
            finally:
                benchmark_client.close()
            if not benchmark_client.deleted:
                raise AssertionError("matched benchmark database delete was not requested")
            benchmark_ended_ms = int(time.time() * 1000) + 1000
            report["matched_benchmark"] = {"backend": "tessera", "schema_sha256": hashes,
                                           "client_measurement": "external sequential round trips",
                                           "database_delete_requested": True,
                                           "cold_process_recovery": cold, **matched}
            report["matched_benchmark"]["aws_call_telemetry"] = _cost_samples(
                logs, f"/aws/lambda/{name}-sql", benchmark_started_ms,
                benchmark_ended_ms, 3 * benchmark_scans + 6)
        if restore_drill:
            status, newer = _request(
                base, "POST", f"/v1/databases/{database_id}/transactions",
                issued["token"], {"operation_id": "drill-newer", "statements": [
                    ["INSERT INTO kv VALUES(?, ?)", ["later", "v2"]]]})
            assert status == 200 and newer["version"] == 2, (status, newer)
            status, history = _request(base, "GET", f"/v1/databases/{database_id}/history",
                                       manager)
            assert status == 200 and history["current_version"] == 2, (status, history)
            assert history["retained_versions"] == [0, 1]
            restore_body = {"source_version": 1, "name": "restored-copy",
                            "request_id": "restore-drill-stable"}
            restore_path = f"/v1/databases/{database_id}/restore"
            status, copied = _request(base, "POST", restore_path, manager, restore_body)
            assert status == 201 and copied["id"] != database_id, (status, copied)
            assert _request(base, "POST", restore_path, manager, restore_body) == (201, copied)
            status, copy_token = _request(base, "POST", f"/v1/databases/{copied['id']}/tokens",
                                          manager, {"scope": "read"})
            assert status == 201, (status, copy_token)
            status, rows = _request(base, "POST", f"/v1/databases/{copied['id']}/query",
                                    copy_token["token"], {"sql": "SELECT k FROM kv ORDER BY k"})
            assert status == 200 and rows["rows"] == [["k"]], (status, rows)
            status, copied_receipt = _request(
                base, "GET", f"/v1/databases/{copied['id']}/operations/drill-write",
                copy_token["token"])
            assert status == 200 and copied_receipt["state"] == "absent"
            report["restore_copy"] = {"source_version": 1, "new_database": True,
                                      "old_receipts_reset": True, "retry_same_id": True,
                                      "history_versions": history["retained_versions"]}
        prefix = f"turso-native/cloud-{database_id}/"
        for number in range(120):
            s3.put_object(Bucket=bucket, Key=prefix + f"orphan/{number:04d}", Body=b"orphan")
        path = f"/v1/databases/{database_id}/usage"
        cursor = None
        usage_pages = []
        while True:
            query = {"limit": 50}
            if cursor is None:
                query["logical"] = "true"
            else:
                query["cursor"] = cursor
            status, page = _request(base, "GET", path + "?" + urlencode(query), manager)
            assert status == 200, (status, page)
            usage_pages.append(page)
            cursor = page["next_cursor"]
            if cursor is None:
                break
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
        assert sum(page["s3_objects_page"] for page in usage_pages) == len(listed)
        assert sum(page["s3_retained_bytes_page"] for page in usage_pages) == sum(
            obj["Size"] for obj in listed)
        assert usage_pages[0]["published"]["logical_database_bytes"] > 0
        report["usage_inventory"] = {
            "pages": len(usage_pages), "objects": len(listed),
            "retained_s3_bytes": sum(obj["Size"] for obj in listed),
            "logical_database_bytes": usage_pages[0]["published"]["logical_database_bytes"],
            "matches_s3_listing": True,
        }
        if cost_snapshot:
            from cloud_stack_cost_snapshot import collect

            ended_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            if ended_at <= started_at:
                raise RuntimeError("cost snapshot needs at least one completed UTC minute")
            lambda_function = _terraform(
                ["output", "-raw", "-state=" + str(state), "lambda_function"],
                env=env).strip()
            report["stack_cost_snapshot"] = collect(
                session, bucket=bucket, table=table, api_url=base,
                lambda_function=lambda_function, start=started_at, end=ended_at)
        if not usage_only:
            status, deletion = _request(base, "DELETE", f"/v1/databases/{database_id}", manager)
            assert status == 202 and deletion["state"] == "deleting", (status, deletion)
            queue_key = {"tenant_id": {"S": "_tessera_cloud/deletion/" + database_id}}
            queued = dynamodb.get_item(TableName=table, Key=queue_key,
                                       ConsistentRead=True).get("Item")
            assert queued is not None, "DELETE did not persist queue intent"
            assert queued["deletion_shard"]["S"] == str(int(database_id[:2], 16) % 4)
            assert queued["deletion_due"]["S"].endswith("#" + database_id)
            report["deletion_queue_intent_recorded"] = True
            status, _ = _request(base, "POST", f"/v1/databases/{database_id}/query",
                                 issued["token"], {"sql": "SELECT 1"})
            assert status == 401
            started = time.monotonic()
            states = []
            while time.monotonic() - started < 900:
                status, current = _request(base, "GET", f"/v1/databases/{database_id}", manager)
                assert status == 200, (status, current)
                states.append(current["state"])
                if current["state"] == "deleted":
                    break
                time.sleep(15)
            assert states[-1] == "deleted", states[-1]
            deletion_elapsed_s = round(time.monotonic() - started, 3)
            pointer = dynamodb.get_item(TableName=table,
                                        Key={"tenant_id": {"S": "turso-pointer/" + prefix
                                                            + "manifest.json"}},
                                        ConsistentRead=True)["Item"]
            assert pointer["deleted"]["BOOL"] is True
            assert not s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("Contents")
            queue_deadline = time.monotonic() + 120
            while dynamodb.get_item(TableName=table, Key=queue_key,
                                    ConsistentRead=True).get("Item"):
                assert time.monotonic() < queue_deadline, "completed queue item was not drained"
                time.sleep(5)
            report.update({"grace_seconds": 60, "orphan_objects": 120, "polls": len(states),
                           "deletion_elapsed_s": deletion_elapsed_s,
                           "pointer_retired": True, "group_empty": True,
                           "deletion_queue_drained": True})
        status, _ = _request(base, "DELETE",
                             f"/v1/tenants/{tenant['id']}/tokens/{management['token_id']}",
                             admin)
        assert status == 200
        status, _ = _request(base, "GET", "/v1/databases", manager)
        assert status == 401
        report["tenant_management_token_revoked"] = True
        report["status"] = "passed"
    except BaseException as exc:  # noqa: BLE001 - teardown follows interruption
        error = exc
        report["failure_type"] = type(exc).__name__
    finally:
        cleanup_errors = []
        if secret_arn is not None:
            try:
                _terraform(["destroy", "-auto-approve", "-input=false", "-no-color",
                            "-state=" + str(state), *variables], env=env)
                report["terraform_state_empty"] = not _terraform(
                    ["state", "list", "-state=" + str(state)], env=env).strip()
            except Exception as exc:  # noqa: BLE001 - report cleanup failure
                cleanup_errors.append("terraform:" + type(exc).__name__)
            try:
                secret_client.delete_secret(SecretId=secret_arn,
                                            ForceDeleteWithoutRecovery=True)
                report["secret_delete_requested"] = True
            except ClientError as exc:
                cleanup_errors.append("secret:" + exc.response["Error"]["Code"])
        report["cleanup_errors"] = cleanup_errors
        Path(output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        if report.get("terraform_state_empty"):
            state.unlink(missing_ok=True)
            Path(str(state) + ".backup").unlink(missing_ok=True)
    if error is not None:
        raise error
    if cleanup_errors or not report.get("terraform_state_empty"):
        raise RuntimeError("disposable deployment teardown was not verified")
    return report


# @spec experiments/cloud-deletion-deployed-drill-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--lambda-zip", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--usage-only", action="store_true")
    parser.add_argument("--restore-drill", action="store_true")
    parser.add_argument("--bulk-drill", action="store_true")
    parser.add_argument("--benchmark-scans", type=int, default=0)
    parser.add_argument("--benchmark-ddl-dir", type=Path)
    parser.add_argument("--benchmark-rows-per-statement", type=int, default=40)
    parser.add_argument("--recovery-samples", type=int, default=5)
    parser.add_argument("--cost-snapshot", action="store_true")
    parser.add_argument("--rate-drill", action="store_true")
    args = parser.parse_args()
    if args.restore_drill and args.bulk_drill:
        parser.error("--restore-drill and --bulk-drill are separate disposable runs")
    if (not 0 <= args.benchmark_scans <= 100
            or not 8 <= args.benchmark_rows_per_statement <= 500
            or not 1 <= args.recovery_samples <= 20):
        parser.error("matched benchmark workload exceeds bounded limits")
    print(json.dumps(drill(args.profile, args.region, args.output, args.lambda_zip,
                           usage_only=args.usage_only or args.restore_drill or args.bulk_drill
                           or bool(args.benchmark_scans) or args.rate_drill,
                           restore_drill=args.restore_drill,
                           bulk_drill=args.bulk_drill, benchmark_scans=args.benchmark_scans,
                           benchmark_ddl_dir=args.benchmark_ddl_dir,
                           benchmark_rows_per_statement=args.benchmark_rows_per_statement,
                           recovery_samples=args.recovery_samples,
                           cost_snapshot=args.cost_snapshot, rate_drill=args.rate_drill),
                     indent=2))


if __name__ == "__main__":
    main()
