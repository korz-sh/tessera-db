"""Deploy a disposable stack and observe scheduled retention on live AWS."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tessera.experimental.cloud_client import _open

MODULE = ROOT / "deploy/aws/terraform"


def _terraform(arguments, env):
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


def _absent(call, missing_codes):
    try:
        call()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in missing_codes:
            return True
        raise
    return False


# @spec experiments/cloud-retention-deployed-drill-drill
def drill(profile, region, output, lambda_zip, *, native_gc=False,
          expected_account_id=None):
    if native_gc and (not isinstance(expected_account_id, str)
                      or len(expected_account_id) != 12
                      or not expected_account_id.isdecimal()):
        raise ValueError("native GC drill requires a 12-digit expected account ID")
    session = boto3.Session(profile_name=profile, region_name=region)
    if (native_gc and session.client("sts").get_caller_identity()["Account"]
            != expected_account_id):
        raise RuntimeError("AWS account differs from --expected-account-id")
    secret_client = session.client("secretsmanager")
    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    events = session.client("events")
    name = "tessera-ret-" + uuid4().hex[:10]
    state = Path(output).resolve().with_suffix(".tfstate")
    admin = secrets.token_urlsafe(48)
    report = {"region": region, "status": "failed", "stack_name": name,
              "native_gc": native_gc,
              "lambda_zip_sha256": sha256(Path(lambda_zip).read_bytes()).hexdigest()}
    if native_gc:
        from experiments.cloud_gc_native_deployed_drill import source_identity

        report["source"] = source_identity()
        report["source"]["retention_probe_sha256"] = sha256(
            Path(__file__).read_bytes()).hexdigest()
    elif (ROOT / ".git").exists():
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
                     "-var=force_destroy_bucket=true", "-var=gc_grace_seconds=60"]
        if native_gc:
            variables.extend(["-var=enable_native_gc=true",
                              "-var=retention_metadata_limit=2",
                              "-var=retention_keep_last=8",
                              "-var=retention_seconds=604800"])
        else:
            variables.extend(["-var=retention_keep_last=1",
                              "-var=retention_seconds=0"])
        _terraform(["apply", "-auto-approve", "-input=false", "-no-color",
                    "-state=" + str(state), *variables], env)
        base = _terraform(["output", "-raw", "-state=" + str(state), "api_url"], env).strip()
        bucket = _terraform(["output", "-raw", "-state=" + str(state), "data_bucket"], env).strip()
        table = _terraform(["output", "-raw", "-state=" + str(state),
                            "coordination_table"], env).strip()
        rule = events.describe_rule(Name=name + "-retention-sweep")
        targets = events.list_targets_by_rule(Rule=rule["Name"])["Targets"]
        assert rule["State"] == "ENABLED" and len(targets) == 1
        assert json.loads(targets[0]["Input"]) == {"tessera_maintenance": "retention"}

        status, tenant = _request(base, "POST", "/v1/tenants", admin,
                                  {"name": "retention-drill", "request_id": uuid4().hex})
        assert status == 201, (status, tenant)
        status, management = _request(base, "POST",
                                      f"/v1/tenants/{tenant['id']}/tokens", admin)
        assert status == 201, (status, management)
        manager = management["token"]
        status, created = _request(base, "POST", "/v1/databases", manager,
                                   {"name": "retention-drill", "request_id": uuid4().hex,
                                    "schema": [
                                        "CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)"]})
        assert status == 201, (status, created)
        database_id = created["id"]
        status, token = _request(base, "POST", f"/v1/databases/{database_id}/tokens",
                                 manager, {"scope": "write"})
        assert status == 201, (status, token)
        for number in range(1, 4):
            status, receipt = _request(
                base, "POST", f"/v1/databases/{database_id}/transactions", token["token"],
                {"operation_id": f"retention-{number}", "statements": [
                    ["INSERT INTO kv VALUES(?, ?)", [number, str(number)]]]})
            assert status == 200 and receipt["version"] == number, (status, receipt)
        prefix = f"turso-native/cloud-{database_id}/"
        orphan = prefix + "images/" + "f" * 32 + ".db"
        s3.put_object(Bucket=bucket, Key=orphan, Body=b"unpublished")
        report["initial_history"] = len(s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix + "history/").get("Contents", []))
        assert report["initial_history"] == 3
        started = time.monotonic()
        if native_gc:
            # The module refuses grace below 60 seconds. Wait past the full
            # S3 LastModified second before asking the Lambda to sweep.
            time.sleep(62)
            lambda_client = session.client("lambda")
            report["maintenance_invocations"] = []
            for _ in range(30):
                if time.monotonic() - started > 420:
                    raise RuntimeError("native GC maintenance drill exceeded time cap")
                invoked = time.monotonic()
                response = lambda_client.invoke(
                    FunctionName=name + "-sql", InvocationType="RequestResponse",
                    Payload=b'{"tessera_maintenance":"retention"}')
                payload = json.loads(response["Payload"].read())
                if response.get("FunctionError"):
                    raise RuntimeError("retention Lambda failed: " + str(payload)[:300])
                report["maintenance_invocations"].append({
                    "elapsed_ms": round((time.monotonic() - invoked) * 1000, 3),
                    "result": payload})
                item = dynamodb.get_item(
                    TableName=table,
                    Key={"tenant_id": {"S": "turso-gc-mark/cloud-" + database_id}},
                    ConsistentRead=True).get("Item")
                state_body = item.get("body", {}).get("B") if item else None
                cycle = json.loads(state_body) if state_body else None
                orphan_exists = bool(s3.list_objects_v2(
                    Bucket=bucket, Prefix=orphan, MaxKeys=1).get("Contents"))
                if cycle and cycle["phase"] == "complete" and not orphan_exists:
                    break
            else:
                raise RuntimeError("native GC cycle did not complete within 30 invocations")
            history = len(s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix + "history/").get("Contents", []))
            assert history == 3, "native GC deleted retained history"
            report["native_cycle"] = {"phase": cycle["phase"],
                                      "history_count": cycle["history_count"],
                                      "mark_chunks": cycle["chunk_count"],
                                      "index_keys": cycle["index_root"]["key_count"]}
            assert report["native_cycle"]["index_keys"] >= 4
        else:
            while time.monotonic() - started < 650:
                history = len(s3.list_objects_v2(
                    Bucket=bucket, Prefix=prefix + "history/").get("Contents", []))
                orphan_exists = bool(s3.list_objects_v2(
                    Bucket=bucket, Prefix=orphan, MaxKeys=1).get("Contents"))
                if history <= 1 and not orphan_exists:
                    break
                time.sleep(20)
            assert history <= 1, "scheduled sweep did not prune expired history"
        assert not s3.list_objects_v2(Bucket=bucket, Prefix=orphan,
                                      MaxKeys=1).get("Contents"), "scheduled sweep did not remove orphan"
        report["scheduled_elapsed_s"] = round(time.monotonic() - started, 3)
        report["remaining_history"] = history
        status, rows = _request(base, "POST", f"/v1/databases/{database_id}/query",
                                token["token"], {"sql": "SELECT count(*) FROM kv"})
        assert status == 200 and rows["rows"] == [[3]], (status, rows)
        item = dynamodb.get_item(TableName=table,
                                 Key={"tenant_id": {"S": "_tessera_cloud/retention/" + database_id}},
                                 ConsistentRead=True).get("Item")
        if not native_gc:
            assert item is not None
        status, _ = _request(base, "DELETE",
                             f"/v1/tenants/{tenant['id']}/tokens/{management['token_id']}",
                             admin)
        assert status == 200
        report.update({"status": "passed", "scheduled_orphan_removed": True,
                       "rows_after_gc": rows["rows"],
                       "retention_cursor_persisted": item is not None,
                       "tenant_management_token_revoked": True})
    except BaseException as exc:  # noqa: BLE001 - teardown follows interruption
        error = exc
        report["failure_type"] = type(exc).__name__
        report["failure_message"] = str(exc)[:500]
    finally:
        cleanup_errors = []
        if secret_arn is not None:
            try:
                _terraform(["destroy", "-auto-approve", "-input=false", "-no-color",
                            "-state=" + str(state), *variables], env)
                report["terraform_state_empty"] = not _terraform(
                    ["state", "list", "-state=" + str(state)], env).strip()
                for _ in range(6):
                    absent = {
                        "bucket": not any(item["Name"].startswith(name + "-data-")
                                          for item in s3.list_buckets()["Buckets"]),
                        "table": _absent(
                            lambda: dynamodb.describe_table(TableName=name + "-coordination"),
                            {"ResourceNotFoundException"}),
                        "lambda": _absent(
                            lambda: session.client("lambda").get_function(FunctionName=name + "-sql"),
                            {"ResourceNotFoundException"}),
                        "retention_rule": _absent(
                            lambda: events.describe_rule(Name=name + "-retention-sweep"),
                            {"ResourceNotFoundException"}),
                    }
                    if all(absent.values()):
                        break
                    time.sleep(10)
                report["aws_resources_absent"] = absent
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
    if (cleanup_errors or not report.get("terraform_state_empty")
            or not all(report.get("aws_resources_absent", {}).values())):
        raise RuntimeError("disposable deployment teardown was not verified")
    return report


# @spec experiments/cloud-retention-deployed-drill-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--lambda-zip", required=True)
    parser.add_argument("--native-gc", action="store_true",
                        help="exercise the opt-in scheduler with a two-object listing cap")
    parser.add_argument("--expected-account-id",
                        help="required with --native-gc; checked before creating resources")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(json.dumps(drill(args.profile, args.region, args.output, args.lambda_zip,
                           native_gc=args.native_gc,
                           expected_account_id=args.expected_account_id), indent=2))


if __name__ == "__main__":
    main()
