"""Exercise a disposable API Gateway/Lambda BYOC stack without printing tokens."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from uuid import uuid4

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tessera.experimental.cloud_client import _open


def _request(base, method, path, token, body=None):
    raw = None if body is None else json.dumps(body).encode()
    request = Request(base + path, data=raw, method=method,
                      headers={"Authorization": "Bearer " + token,
                               "Content-Type": "application/json"})
    started = time.monotonic()
    try:
        with _open(request, timeout=60) as response:
            status, data = response.status, response.read()
    except HTTPError as exc:
        status, data = exc.code, exc.read()
    return status, json.loads(data), round((time.monotonic() - started) * 1000, 3)


# @spec experiments/cloud-https-staging-probe-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--admin-secret-arn", required=True)
    parser.add_argument("--lambda-zip", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    admin = boto3.Session(profile_name=args.profile, region_name=args.region).client(
        "secretsmanager").get_secret_value(SecretId=args.admin_secret_arn)["SecretString"]
    base = args.api_url.rstrip("/")
    report = {"region": args.region, "status": "failed",
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                       text=True).strip(),
              "lambda_zip_sha256": sha256(Path(args.lambda_zip).read_bytes()).hexdigest()}
    try:
        run_id = uuid4().hex
        status, tenant, tenant_ms = _request(base, "POST", "/v1/tenants", admin,
                                             {"name": "https-probe", "request_id": run_id})
        assert status == 201, (status, tenant)
        status, management, management_ms = _request(
            base, "POST", f"/v1/tenants/{tenant['id']}/tokens", admin)
        assert status == 201, (status, management)
        manager = management["token"]
        status, denied, _ = _request(base, "POST", "/v1/databases", admin,
                                     {"name": "root-denied", "request_id": run_id,
                                      "schema": []})
        assert status == 409 and denied["error"] == "catalog_conflict", (status, denied)
        status, created, create_ms = _request(base, "POST", "/v1/databases", manager,
                                              {"name": "https-probe", "request_id": run_id,
                                               "schema": [
                                                   "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]})
        assert status == 201, (status, created)
        database_id = created["id"]
        status, issued, issue_ms = _request(base, "POST",
                                             f"/v1/databases/{database_id}/tokens", manager,
                                             {"scope": "write"})
        assert status == 201, (status, issued)
        token = issued["token"]
        transaction = {"operation_id": "https-op-1", "statements": [
            ["INSERT INTO kv VALUES(?, ?)", ["probe", "durable"]]]}
        status, receipt, write_ms = _request(base, "POST",
                                              f"/v1/databases/{database_id}/transactions",
                                              token, transaction)
        assert status == 200 and receipt["version"] == 1, (status, receipt)
        status, replay, replay_ms = _request(base, "POST",
                                              f"/v1/databases/{database_id}/transactions",
                                              token, transaction)
        assert status == 200 and replay == receipt, (status, replay)
        status, rows, read_ms = _request(base, "POST",
                                          f"/v1/databases/{database_id}/query", token,
                                          {"sql": "SELECT v FROM kv WHERE k=?", "params": ["probe"],
                                           "min_version": receipt["version"]})
        assert status == 200 and rows["rows"] == [["durable"]], (status, rows)
        status, lookup, lookup_ms = _request(base, "GET",
                                              f"/v1/databases/{database_id}/operations/https-op-1",
                                              token)
        assert status == 200 and lookup["state"] == "published", (status, lookup)
        status, _, _ = _request(base, "DELETE",
                                f"/v1/databases/{database_id}/tokens/{issued['token_id']}",
                                manager)
        assert status == 200
        status, _, _ = _request(base, "POST", f"/v1/databases/{database_id}/query",
                                token, {"sql": "SELECT 1"})
        assert status == 401
        status, _, _ = _request(base, "DELETE",
                                f"/v1/tenants/{tenant['id']}/tokens/{management['token_id']}",
                                admin)
        assert status == 200
        report.update({"status": "passed", "version": receipt["version"],
                       "tenant_create_ms": tenant_ms,
                       "tenant_token_ms": management_ms,
                       "strict_root_admission": True,
                       "create_ms": create_ms, "issue_token_ms": issue_ms,
                       "write_ms": write_ms, "idempotent_replay_ms": replay_ms,
                       "read_ms": read_ms, "lookup_ms": lookup_ms,
                       "token_revocation": True})
    finally:
        Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
