"""Run the same bounded scan workload against Tessera Cloud or Turso Cloud.

Use disposable databases. Set TESSERA_API_URL and TESSERA_TENANT_TOKEN for
--backend tessera, or TURSO_DATABASE_URL and TURSO_AUTH_TOKEN for --backend turso.
The optional --ddl-dir loads repository.sql and attempt_manifest.sql from a
local consumer checkout; no consumer DDL is distributed with this script.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tessera.experimental.cloud_client import _open

INSERT = (
    "INSERT INTO scan_issue (uuid,scan_id,tool,rule_name,rule_set,severity,"
    "file_path,line_number,message,issue_key,new_issue) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
)
SYNTHETIC_SCHEMA = [
    ("CREATE TABLE scan_issue (id INTEGER PRIMARY KEY, uuid TEXT NOT NULL UNIQUE, "
     "scan_id INTEGER NOT NULL, tool TEXT NOT NULL, rule_name TEXT NOT NULL, "
     "rule_set TEXT, severity TEXT NOT NULL, file_path TEXT NOT NULL, "
     "line_number INTEGER, message TEXT, issue_key TEXT NOT NULL, "
     "new_issue INTEGER NOT NULL DEFAULT 0)"),
    "CREATE INDEX idx_scan_issue_scan ON scan_issue (scan_id,severity)",
    "CREATE INDEX idx_scan_issue_key ON scan_issue (scan_id,issue_key)",
]


# @spec examples/schema
def schema(ddl_dir):
    if ddl_dir is None:
        statements = SYNTHETIC_SCHEMA
        return statements, {"synthetic_schema": hashlib.sha256(
            json.dumps(statements, separators=(",", ":")).encode()).hexdigest()}
    paths = [ddl_dir / name for name in ("repository.sql", "attempt_manifest.sql")]
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    statements = []
    for path in paths:
        sql = re.sub(r"(?m)^--.*$", "", path.read_text())
        statements.extend(part.strip() for part in sql.split(";") if part.strip())
    return statements, hashes


# @spec examples/issue
def issue(scan, number):
    key = f"scan-{scan}-issue-{number}"
    return (key, scan, "semgrep", f"rule-{number % 20}", None,
            "HIGH" if number % 5 == 0 else "INFO", f"src/file-{number % 50}.py",
            number + 1, "representative finding text", key, 1)


# @spec examples/statements-for-scan
def statements_for_scan(scan, findings, rows_per_statement):
    rows = [issue(scan, number) for number in range(findings)]
    placeholder = "(" + ",".join("?" for _ in range(11)) + ")"
    statements = []
    for offset in range(0, findings, rows_per_statement):
        chunk = rows[offset:offset + rows_per_statement]
        sql = INSERT.replace("VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                             "VALUES " + ",".join([placeholder] * len(chunk)))
        statements.append([sql, [value for row in chunk for value in row]])
    return statements


# @spec examples/percentile
def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


# @spec examples/summary
def summary(values):
    return {"count": len(values), "p50": statistics.median(values),
            "p95": percentile(values, .95), "p99": percentile(values, .99),
            "samples": values}


# @spec examples/tessera
class Tessera:
    def __init__(self, statements, api_url, tenant_token):
        self.base = api_url.rstrip("/")
        self.manager = tenant_token
        self.database_id = None
        self.token = None
        self.deleted = False
        status, body = self.request("POST", "/v1/databases", self.manager, {
            "name": "matched-scan-benchmark", "schema": statements,
            "request_id": uuid4().hex,
            "limits": {"max_frontier_bytes": 67_108_864,
                       "max_operation_bytes": 1_048_576, "max_tokens": 64}})
        if status != 201:
            raise RuntimeError(f"create database failed: {status} {body}")
        self.database_id = body["id"]
        status, body = self.request("POST", f"/v1/databases/{self.database_id}/tokens",
                                    self.manager, {"scope": "write"})
        if status != 201:
            self.close()
            raise RuntimeError(f"issue token failed: {status} {body}")
        self.token = body["token"]

    def request(self, method, path, token, body=None):
        raw = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request = Request(self.base + path, data=raw, method=method, headers={
            "Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with _open(request, timeout=60) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def write(self, scan, statements):
        body = {"operation_id": f"matched-scan-{scan}", "statements": statements}
        status, receipt = self.request(
            "POST", f"/v1/databases/{self.database_id}/transactions", self.token, body)
        if status != 200:
            raise RuntimeError(f"scan write failed: {status} {receipt}")
        return receipt["version"], len(json.dumps(body, separators=(",", ":")).encode())

    def count(self, scan, severity=None, min_version=None):
        sql = "SELECT COUNT(*) FROM scan_issue WHERE scan_id=?"
        params = [scan]
        if severity is not None:
            sql += " AND severity=?"
            params.append(severity)
        status, body = self.request("POST", f"/v1/databases/{self.database_id}/query",
                                    self.token, {"sql": sql, "params": params,
                                                 "min_version": min_version})
        if status != 200:
            raise RuntimeError(f"scan read failed: {status} {body}")
        return body["rows"][0][0]

    def rollback(self, scans):
        body = {"operation_id": "matched-rollback", "statements": [
            [INSERT, list(issue(scans + 1, 0))], [INSERT, list(issue(1, 0))]]}
        status, response = self.request("POST",
                                        f"/v1/databases/{self.database_id}/transactions",
                                        self.token, body)
        if status != 422 or response.get("error") != "sql_failed":
            raise AssertionError(f"forced rollback did not fail: {status} {response}")
        return self.count(scans + 1) == 0

    def close(self):
        if self.database_id is not None:
            status, _ = self.request("DELETE", f"/v1/databases/{self.database_id}",
                                     self.manager)
            self.deleted = status == 202


# @spec examples/turso
class Turso:
    def __init__(self, statements, url, token):
        import libsql

        self.conn = libsql.connect(database=url, auth_token=token)
        for statement in statements:
            self.conn.execute(statement)
        self.conn.commit()

    def write(self, scan, statements):
        try:
            for sql, params in statements:
                self.conn.execute(sql, params)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        body = {"operation_id": f"matched-scan-{scan}", "statements": statements}
        return None, len(json.dumps(body, separators=(",", ":")).encode())

    def count(self, scan, severity=None, min_version=None):
        sql = "SELECT COUNT(*) FROM scan_issue WHERE scan_id=?"
        params = [scan]
        if severity is not None:
            sql += " AND severity=?"
            params.append(severity)
        return self.conn.execute(sql, params).fetchone()[0]

    def rollback(self, scans):
        rejected = False
        try:
            self.conn.execute(INSERT, issue(scans + 1, 0))
            self.conn.execute(INSERT, issue(1, 0))
            self.conn.commit()
        except ValueError:
            rejected = True
            self.conn.rollback()
        return rejected and self.count(scans + 1) == 0

    def close(self):
        self.conn.close()


# @spec examples/benchmark
def benchmark(client, scans, findings, rows_per_statement):
    writes, counts, filters, payloads = [], [], [], []
    started = time.perf_counter()
    for scan in range(1, scans + 1):
        statements = statements_for_scan(scan, findings, rows_per_statement)
        before = time.perf_counter()
        version, payload_bytes = client.write(scan, statements)
        writes.append((time.perf_counter() - before) * 1000)
        payloads.append(payload_bytes)
        before = time.perf_counter()
        count = client.count(scan, min_version=version)
        counts.append((time.perf_counter() - before) * 1000)
        if count != findings:
            raise AssertionError(f"scan {scan}: expected {findings}, got {count}")
        before = time.perf_counter()
        high = client.count(scan, "HIGH", min_version=version)
        filters.append((time.perf_counter() - before) * 1000)
        if high != math.ceil(findings / 5):
            raise AssertionError(f"scan {scan}: bad severity count {high}")
    elapsed = time.perf_counter() - started
    if not client.rollback(scans):
        raise AssertionError("forced transaction rollback did not remove the valid row")
    if client.count(1) != findings:
        raise AssertionError("first scan changed after forced rollback")
    return {"scans": scans, "findings_per_scan": findings,
            "statements_per_scan": math.ceil(findings / rows_per_statement),
            "workload_json_bytes": payloads, "wire_bytes_measured": False,
            "write_ms": summary(writes),
            "count_read_ms": summary(counts), "filtered_read_ms": summary(filters),
            "full_loop_seconds": elapsed, "full_loop_scans_per_second": scans / elapsed,
            "write_only_findings_per_second": scans * findings / (sum(writes) / 1000),
            "forced_rollback_verified": True}


# @spec examples/cloud-matched-scan-benchmark-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("tessera", "turso"), required=True)
    parser.add_argument("--ddl-dir", type=Path)
    parser.add_argument("--scans", type=int, default=5)
    parser.add_argument("--findings", type=int, default=500)
    parser.add_argument("--rows-per-statement", type=int, default=40)
    parser.add_argument("--client-label", default="unspecified")
    parser.add_argument("--service-region", default="unspecified")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (not 1 <= args.scans <= 100 or not 1 <= args.findings <= 5000
            or not 1 <= args.rows_per_statement <= 500
            or math.ceil(args.findings / args.rows_per_statement) > 64):
        parser.error("workload exceeds bounded limits")
    ddl, hashes = schema(args.ddl_dir)
    if args.backend == "tessera":
        client = Tessera(ddl, os.environ["TESSERA_API_URL"],
                         os.environ["TESSERA_TENANT_TOKEN"])
    else:
        client = Turso(ddl, os.environ["TURSO_DATABASE_URL"],
                       os.environ["TURSO_AUTH_TOKEN"])
    report = {"backend": args.backend, "schema_sha256": hashes,
              "started_utc": datetime.now(timezone.utc).isoformat(),
              "client_label": args.client_label, "service_region": args.service_region,
              "client_measurement": "external sequential round trips"}
    try:
        report.update(benchmark(client, args.scans, args.findings,
                                args.rows_per_statement))
        report["status"] = "passed"
    except BaseException as exc:
        report["status"] = "failed"
        report["failure_type"] = type(exc).__name__
        raise
    finally:
        client.close()
        if isinstance(client, Tessera):
            report["database_delete_requested"] = client.deleted
        if args.output:
            args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
