"""Create, exercise and destroy an isolated v2 BYOC stack in AWS staging.

The report excludes tokens, account ID, bucket and table names. Resource names
are unique per run and every created object is deleted in the finally block.
"""

from __future__ import annotations

import argparse
import json
import math
import secrets
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from experiments.cloud_migrate_legacy_tenant import migrate_pages
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.service import (
    CatalogConflict,
    DatabaseCatalog,
    dispatch_request,
)
from tessera.experimental.cloud.tenants import TenantQuotaExceeded
from tessera.experimental.turso import Operation, Runtime, RuntimeLimits
from tessera.experimental.turso.api import DatabaseQuotaExceeded
from tessera.experimental.turso.fencing import FencedBackend
from tessera.experimental.turso.native import NativeLogStore

ROOT = Path(__file__).resolve().parents[1]


def _missing(error):
    return error.response["Error"]["Code"] in (
        "NoSuchBucket", "404", "ResourceNotFoundException"
    )


def _empty_bucket(s3, bucket):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for start in range(0, len(objects), 1000):
            s3.delete_objects(Bucket=bucket,
                              Delete={"Objects": objects[start:start + 1000], "Quiet": True})


def _percentiles(samples):
    ordered = sorted(samples)
    return {"p50": round(ordered[math.ceil(0.50 * len(ordered)) - 1], 3),
            "p95": round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3),
            "p99": round(ordered[math.ceil(0.99 * len(ordered)) - 1], 3)}


def _tier_benchmark(service, backend, iterations):
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]
    cold = service.catalog.create("cold-tier", schema)["id"]
    warm = service.catalog.create("warm-tier", schema)["id"]
    cold_samples = []
    warm_samples = []
    for number in range(iterations):
        started = time.monotonic()
        service.write(cold, f"cold-{number}", [["INSERT INTO kv VALUES(?, ?)",
                                                  [str(number), uuid4().hex]]])
        cold_samples.append((time.monotonic() - started) * 1000)
    warm_started = time.monotonic()
    with Runtime(backend, "cloud-" + warm, native=True,
                 limits=RuntimeLimits(max_batch=1, max_pending=1,
                                      batch_window_ms=0)) as runtime:
        warm_open_ms = (time.monotonic() - warm_started) * 1000
        for number in range(iterations):
            operation = Operation(f"warm-{number}", [["INSERT INTO kv VALUES(?, ?)",
                                                       [str(number), uuid4().hex]]])
            started = time.monotonic()
            runtime.submit(operation, timeout=60)
            warm_samples.append((time.monotonic() - started) * 1000)
    assert service.read(cold, "SELECT COUNT(*) FROM kv") == [(iterations,)]
    assert service.read(warm, "SELECT COUNT(*) FROM kv") == [(iterations,)]
    return {"iterations_per_tier": iterations, "measurement_location": "external client",
            "cold_ms": _percentiles(cold_samples),
            "warm_ms": _percentiles(warm_samples),
            "warm_owner_open_ms": round(warm_open_ms, 3),
            "cold_samples_ms": [round(value, 3) for value in cold_samples],
            "warm_samples_ms": [round(value, 3) for value in warm_samples]}


class _FailOneCatalogCreate:
    """Inject one crash after tenant reservation and before catalog creation."""

    def __init__(self, backend):
        self.backend = backend
        self.failed = False

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def put(self, key, body, *, if_match=None, if_none_match=False):
        if (not self.failed and if_none_match
                and key.startswith("_tessera_cloud/catalog/")):
            self.failed = True
            raise RuntimeError("injected crash after tenant reservation")
        return self.backend.put(key, body, if_match=if_match,
                                if_none_match=if_none_match)


class _FailOneOwnerCAS(_FailOneCatalogCreate):
    """Inject one stop after migration reservation and before owner CAS."""

    def put(self, key, body, *, if_match=None, if_none_match=False):
        if (not self.failed and if_match is not None
                and key.startswith("_tessera_cloud/catalog/")):
            self.failed = True
            raise RuntimeError("injected stop after migration reservation")
        return self.backend.put(key, body, if_match=if_match,
                                if_none_match=if_none_match)


def _tenant_drill(service, raw, fenced):
    limits = {"max_databases": 1, "max_frontier_bytes": 67_108_864,
              "max_operation_bytes": 65_536, "max_tokens": 64}
    tenant_id = service.tenants.create(
        "isolated-tenant-drill", request_id="tenant-drill", limits=limits)["id"]
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]

    def create(index):
        try:
            return service.catalog.create(
                f"race-{index}", schema, request_id=f"race-{index}",
                tenant_id=tenant_id)
        except TenantQuotaExceeded:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, (0, 1)))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    winner_index = 0 if results[0] is not None else 1
    assert service.tenants.get(tenant_id)["reserved_databases"] == 1
    assert create(winner_index)["id"] == winners[0]["id"]
    tenant_token = service.tenants.issue_token(tenant_id)
    assert dispatch_request(service, "GET", f"/v1/databases/{winners[0]['id']}",
                            tenant_token["token"], lambda: None)[0] == 200

    crash_tenant = service.tenants.create(
        "isolated-crash-drill", request_id="crash-drill", limits=limits)["id"]
    interrupted = _FailOneCatalogCreate(raw)
    interrupted_catalog = DatabaseCatalog(
        interrupted, data_backend=fenced, tenant_catalog=service.tenants)
    try:
        interrupted_catalog.create(
            "crash-retry", schema, request_id="crash-retry", tenant_id=crash_tenant)
    except RuntimeError as exc:
        assert str(exc) == "injected crash after tenant reservation"
    else:
        raise AssertionError("catalog fault injection did not fire")
    assert service.tenants.get(crash_tenant)["reserved_databases"] == 1
    recovered = service.catalog.create(
        "crash-retry", schema, request_id="crash-retry", tenant_id=crash_tenant)
    assert recovered["state"] == "active"
    assert service.catalog.create(
        "crash-retry", schema, request_id="crash-retry",
        tenant_id=crash_tenant)["id"] == recovered["id"]
    assert dispatch_request(service, "GET", f"/v1/databases/{recovered['id']}",
                            tenant_token["token"], lambda: None) == (
                                404, {"error": "not_found"})
    service.tenants.revoke_token(tenant_id, tenant_token["token_id"])
    assert service.tenants.authorize(tenant_token["token"]) is None
    return {"concurrent_final_slot": True, "same_request_replay": True,
            "crash_after_reservation_recovered": True,
            "cross_tenant_denied": True, "management_token_revoked": True}


def _legacy_migration_drill(service, raw, fenced, admin_token):
    """Exercise real S3 CAS migration, then return one ID for cold recovery."""
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]
    body = {"name": "legacy-migration-drill", "request_id": "legacy-migration-drill",
            "schema": schema}
    legacy = service.catalog.create(body["name"], schema,
                                    request_id=body["request_id"])
    legacy_id = legacy["id"]
    receipt = service.write(legacy_id, "staging-op", [
        ["INSERT INTO kv VALUES(?, ?)", ["probe", "durable"]]])
    strict = DatabaseService(fenced, admin_token=admin_token, catalog_backend=raw,
                             allow_legacy_root_create=False)
    try:
        dispatch_request(strict, "POST", "/v1/databases", admin_token,
                         lambda: dict(body, request_id="new-root-request"))
    except CatalogConflict:
        pass
    else:
        raise AssertionError("strict root creation was admitted")
    assert dispatch_request(strict, "POST", "/v1/databases", admin_token,
                            lambda: body)[1]["id"] == legacy_id
    limits = {"max_databases": 256, "max_frontier_bytes": 1_073_741_824,
              "max_operation_bytes": 1_048_576, "max_tokens": 64}
    tenant_id = strict.tenants.create("migration-default", request_id="migration-default",
                                      limits=limits)["id"]
    dry = list(migrate_pages(strict.catalog, tenant_id))
    candidates = sum(page["counts"].get("would_migrate", 0) for page in dry)
    assert dry[-1]["complete"] and candidates >= 1
    interrupted = _FailOneOwnerCAS(raw)
    interrupted_catalog = DatabaseCatalog(interrupted, data_backend=fenced,
                                          tenant_catalog=strict.tenants)
    try:
        list(migrate_pages(interrupted_catalog, tenant_id, apply=True))
    except RuntimeError as exc:
        assert str(exc) == "injected stop after migration reservation"
    else:
        raise AssertionError("migration fault injection did not fire")
    assert strict.tenants.get(tenant_id)["reserved_databases"] == 1
    applied = list(migrate_pages(strict.catalog, tenant_id, apply=True))
    assert applied[-1]["complete"]
    assert sum(page["counts"].get("migrated", 0) for page in applied) == candidates
    verified = list(migrate_pages(strict.catalog, tenant_id))
    assert verified[-1]["complete"]
    assert sum(page["counts"].get("would_migrate", 0) for page in verified) == 0
    assert not any(page["counts"].get(status, 0)
                   for page in verified
                   for status in ("invalid_fingerprint", "invalid_state",
                                  "unexpected_catalog_key"))
    assert strict.tenants.get(tenant_id)["reserved_databases"] == candidates
    assert strict.catalog.get(legacy_id)["tenant_id"] == tenant_id
    assert dispatch_request(strict, "POST", "/v1/databases", admin_token,
                            lambda: body)[1]["id"] == legacy_id
    manager = strict.tenants.issue_token(tenant_id)
    assert dispatch_request(strict, "GET", f"/v1/databases/{legacy_id}",
                            manager["token"], lambda: None)[0] == 200
    strict.tenants.revoke_token(tenant_id, manager["token_id"])
    assert strict.lookup(legacy_id, "staging-op")["version"] == receipt["version"]
    assert strict.read(legacy_id, "SELECT v FROM kv WHERE k=?", ["probe"]
                       ) == [("durable",)]
    return {"legacy_live_candidates": candidates,
            "reservation_crash_recovered": True,
            "root_new_create_denied": True,
            "root_replay_preserved": True,
            "tenant_management_after_migration": True,
            "no_unowned_live_catalog_records": True}, legacy_id, receipt["version"]


# @spec experiments/smoke
def smoke(profile, region, output, *, benchmark_writes=0, deletion_drill=False,
          quota_drill=False, tenant_drill=False, legacy_migration_drill=False):
    session = boto3.Session(profile_name=profile, region_name=region)
    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    suffix = uuid4().hex[:12]
    bucket = "tessera-byoc-smoke-" + suffix
    table = "tessera-byoc-smoke-" + suffix
    state_file = Path(output).with_suffix(".state.json")
    state_file.write_text(json.dumps({"region": region, "suffix": suffix,
                                      "bucket": bucket, "table": table}) + "\n")
    bucket_created = table_created = False
    if (ROOT / ".git").exists():
        source = {"source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()}
    else:
        manifest = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
        if not manifest.is_file():
            raise RuntimeError("public source manifest required outside Git checkout")
        source = {"public_manifest_sha256": sha256(manifest.read_bytes()).hexdigest()}
    report = {"region": region, **source, "status": "failed"}
    started = time.monotonic()
    error = None
    try:
        kwargs = {"Bucket": bucket}
        if region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**kwargs)
        bucket_created = True
        dynamodb.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table_created = True
        dynamodb.get_waiter("table_exists").wait(TableName=table)

        raw = S3DynamoBackend(bucket, table, region,
                              s3_client=s3, dynamodb=session.resource("dynamodb"),
                              backoff_max_attempts=1)
        fenced = FencedBackend(raw, dynamodb)
        admin_token = secrets.token_urlsafe(36)
        service = DatabaseService(fenced, admin_token=admin_token,
                                  catalog_backend=raw)
        created = service.catalog.create(
            "staging-smoke", ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"])
        database_id = created["id"]
        token = service.catalog.issue_token(database_id)
        assert service.catalog.authorize(database_id, token.secret, write=True)
        transaction = [["INSERT INTO kv VALUES(?, ?)", ["probe", "durable"]]]
        write_started = time.monotonic()
        receipt = service.write(database_id, "staging-op", transaction)
        report["write_ms"] = round((time.monotonic() - write_started) * 1000, 3)
        assert receipt == service.write(database_id, "staging-op", transaction)
        assert service.read(database_id, "SELECT v FROM kv WHERE k=?", ["probe"],
                            min_version=receipt["version"]) == [("durable",)]
        cold = json.loads(subprocess.check_output([
            sys.executable, "-m", "experiments.cloud_api_recovery_probe",
            "--profile", profile, "--region", region, "--bucket", bucket,
            "--table", table, "--database-id", database_id,
        ], text=True, timeout=120))
        assert cold["published_version"] == receipt["version"]
        service.catalog.revoke_token(database_id, token.token_id)
        assert not service.catalog.authorize(database_id, token.secret)
        if benchmark_writes:
            report["tier_benchmark"] = _tier_benchmark(service, fenced, benchmark_writes)
        if tenant_drill:
            report["tenant_drill"] = _tenant_drill(service, raw, fenced)
        if quota_drill:
            limits = {"max_frontier_bytes": 67_108_864,
                      "max_operation_bytes": 1_024, "max_tokens": 1}
            quota_id = service.catalog.create(
                "quota-drill", ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"],
                limits=limits)["id"]
            first_write = [["INSERT INTO kv VALUES(?, ?)", ["first", "x" * 50_000]]]
            try:
                service.write(quota_id, "quota-first", first_write)
            except DatabaseQuotaExceeded:
                pass
            else:
                raise AssertionError("operation quota did not reject the write")
            assert service.lookup(quota_id, "quota-first")["state"] == "absent"
            raised = dict(limits, max_operation_bytes=65_536)
            service.catalog.update_limits(quota_id, raised)
            quota_receipt = service.write(quota_id, "quota-first", first_write)
            storage = NativeLogStore(fenced, "cloud-" + quota_id)
            before, _ = storage.manifest()
            frontier_bytes = before["checkpoint"]["bytes"] + before["log_bytes"]
            lowered = dict(raised, max_frontier_bytes=max(65_536, frontier_bytes))
            service.catalog.update_limits(quota_id, lowered)
            try:
                service.write(quota_id, "quota-second", [
                    ["INSERT INTO kv VALUES(?, ?)", ["second", "y" * 50_000]]])
            except DatabaseQuotaExceeded:
                pass
            else:
                raise AssertionError("frontier quota did not reject the write")
            assert storage.manifest()[0] == before
            assert service.lookup(quota_id, "quota-second")["state"] == "absent"
            assert service.write(quota_id, "quota-first", first_write) == quota_receipt
            cold_quota = DatabaseService(fenced, admin_token=admin_token,
                                         catalog_backend=raw)
            assert cold_quota.read(quota_id, "SELECT length(v) FROM kv") == [(50_000,)]
            report["quota_drill"] = {
                "rejected_operation_before_publication": True,
                "rejected_frontier_before_publication": True,
                "published_replay_survives_lowered_limit": True,
                "cold_recovery": True,
                "frontier_bytes_before_rejection": frontier_bytes,
            }
        if deletion_drill:
            prefix = f"turso-native/cloud-{database_id}/"
            for number in range(120):
                raw.put(prefix + f"orphan/{number:04d}", b"orphan")
            requested = service.catalog.request_deletion(database_id)
            assert service.catalog.purge_step(database_id)["purged_objects"] == 0
            assert not service.catalog.authorize(database_id, token.secret)
            while time.time() < requested["delete_after"]:
                time.sleep(min(1, requested["delete_after"] - time.time()))
            sweep_started = time.monotonic()
            sweep_count = 0
            sweep_results = []
            for _ in range(20):
                sweep_results.append(service.catalog.sweep_deletions())
                sweep_count += 1
                if service.catalog.deletion_status(database_id)["state"] == "deleted":
                    break
            report["deletion_sweeps"] = sweep_results
            assert service.catalog.deletion_status(database_id)["state"] == "deleted"
            assert fenced.pointer_retired(prefix + "manifest.json")
            assert raw.list_page(prefix) == []
            report["deletion_drill"] = {
                "grace_seconds": 60, "orphan_objects": 120,
                "sweep_invocations": sweep_count,
                "purge_ms": round((time.monotonic() - sweep_started) * 1000, 3),
                "pointer_retired": True, "group_empty": True,
            }
        if legacy_migration_drill:
            migration, legacy_id, legacy_version = _legacy_migration_drill(
                service, raw, fenced, admin_token)
            recovered = json.loads(subprocess.check_output([
                sys.executable, "-m", "experiments.cloud_api_recovery_probe",
                "--profile", profile, "--region", region, "--bucket", bucket,
                "--table", table, "--database-id", legacy_id,
            ], text=True, timeout=120))
            assert recovered["published_version"] == legacy_version
            migration["fresh_process_recovery_after_migration"] = True
            report["legacy_migration_drill"] = migration
        report.update({"status": "passed", "published_version": receipt["version"],
                       "receipt_fingerprint": receipt["fingerprint"],
                       "cold_process_recovery": True, "token_revocation": True,
                       "database_count": len(service.catalog.list())})
    except BaseException as exc:  # noqa: BLE001 - cleanup also follows interrupts
        error = exc
        report["failure_type"] = type(exc).__name__
    finally:
        cleanup_errors = []
        if bucket_created:
            try:
                _empty_bucket(s3, bucket)
                s3.delete_bucket(Bucket=bucket)
            except BaseException as exc:  # noqa: BLE001 - verify teardown independently
                cleanup_errors.append("bucket:" + type(exc).__name__)
        if table_created:
            try:
                dynamodb.delete_table(TableName=table)
                dynamodb.get_waiter("table_not_exists").wait(TableName=table)
            except BaseException as exc:  # noqa: BLE001 - verify teardown independently
                cleanup_errors.append("table:" + type(exc).__name__)
        report["cleanup_errors"] = cleanup_errors
        report["elapsed_s"] = round(time.monotonic() - started, 3)
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as exc:
            report["bucket_absent"] = _missing(exc)
        except Exception as exc:  # noqa: BLE001 - record verification failure
            report["bucket_absent"] = False
            report["bucket_verification_error"] = type(exc).__name__
        else:
            report["bucket_absent"] = False
        try:
            dynamodb.describe_table(TableName=table)
        except ClientError as exc:
            report["table_absent"] = _missing(exc)
        except Exception as exc:  # noqa: BLE001 - record verification failure
            report["table_absent"] = False
            report["table_verification_error"] = type(exc).__name__
        else:
            report["table_absent"] = False
        Path(output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        if report["bucket_absent"] and report["table_absent"]:
            state_file.unlink(missing_ok=True)
    if error is not None:
        raise error
    if cleanup_errors or not report["bucket_absent"] or not report["table_absent"]:
        raise RuntimeError("isolated staging cleanup was not verified")
    return report


# @spec experiments/cloud-api-staging-smoke-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--output", required=True)
    parser.add_argument("--benchmark-writes", type=int, default=0)
    parser.add_argument("--deletion-drill", action="store_true")
    parser.add_argument("--quota-drill", action="store_true")
    parser.add_argument("--tenant-drill", action="store_true")
    parser.add_argument("--legacy-migration-drill", action="store_true")
    args = parser.parse_args()
    if not 0 <= args.benchmark_writes <= 100:
        parser.error("--benchmark-writes must be 0..100")
    print(json.dumps(smoke(args.profile, args.region, args.output,
                           benchmark_writes=args.benchmark_writes,
                           deletion_drill=args.deletion_drill,
                           quota_drill=args.quota_drill,
                           tenant_drill=args.tenant_drill,
                           legacy_migration_drill=args.legacy_migration_drill), indent=2))


if __name__ == "__main__":
    main()
