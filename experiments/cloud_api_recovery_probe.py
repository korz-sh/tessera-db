"""Fresh-process S3/DynamoDB recovery check for the isolated BYOC harness."""

from __future__ import annotations

import argparse
import json

import boto3

from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.cost_telemetry import (
    begin_request, end_request, register_session,
)
from tessera.experimental.turso.fencing import FencedBackend


# @spec experiments/recover
def recover(session, bucket, table, database_id, operation_id, sql, params,
            expected_rows, *, region):
    """Read an acknowledged receipt and SQL rows using fresh AWS clients."""
    if (not isinstance(params, list) or not isinstance(expected_rows, list)
            or not isinstance(operation_id, str) or not operation_id):
        raise ValueError("invalid recovery expectation")
    register_session(session)
    counts, token = begin_request()
    try:
        raw = S3DynamoBackend(bucket, table, region,
                              s3_client=session.client("s3"),
                              dynamodb=session.resource("dynamodb"),
                              backoff_max_attempts=1)
        service = DatabaseService(FencedBackend(raw, session.client("dynamodb")),
                                  admin_token="recovery-probe-" + "x" * 32,
                                  catalog_backend=raw)
        receipt = service.lookup(database_id, operation_id)
        rows = json.loads(json.dumps(service.read(database_id, sql, params)))
        if receipt["state"] != "published" or rows != expected_rows:
            raise AssertionError("cold recovery lost receipt or expected SQL rows")
    finally:
        end_request(token)
    return {"operation_id": operation_id, "published_version": receipt["version"],
            "rows": rows, "logical_sdk_calls": dict(sorted(counts.logical.items())),
            "sdk_http_attempts": dict(sorted(counts.http_attempts.items())),
            "sdk_attempts_not_billed_units": True}


# @spec experiments/cloud-api-recovery-probe-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--database-id", required=True)
    parser.add_argument("--operation-id", default="staging-op")
    parser.add_argument("--sql", default="SELECT v FROM kv WHERE k=?")
    parser.add_argument("--params-json", default='["probe"]')
    parser.add_argument("--expected-rows-json", default='[["durable"]]')
    args = parser.parse_args()
    params = json.loads(args.params_json)
    expected_rows = json.loads(args.expected_rows_json)
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    print(json.dumps(recover(session, args.bucket, args.table, args.database_id,
                             args.operation_id, args.sql, params, expected_rows,
                             region=args.region)))


if __name__ == "__main__":
    main()
