"""Remove only an isolated BYOC smoke bucket/table after an interrupted run."""

from __future__ import annotations

import argparse
import json
import re

import boto3
from botocore.exceptions import ClientError

from .cloud_api_staging_smoke import _empty_bucket, _missing


# @spec experiments/cloud-cleanup-staging-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--suffix", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{12}", args.suffix):
        parser.error("suffix must be the twelve hex characters printed in the smoke name")
    name = "tessera-byoc-smoke-" + args.suffix
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    try:
        _empty_bucket(s3, name)
        s3.delete_bucket(Bucket=name)
    except ClientError as exc:
        if not _missing(exc):
            raise
    try:
        dynamodb.delete_table(TableName=name)
        dynamodb.get_waiter("table_not_exists").wait(TableName=name)
    except ClientError as exc:
        if not _missing(exc):
            raise
    try:
        s3.head_bucket(Bucket=name)
    except ClientError as exc:
        bucket_absent = _missing(exc)
    else:
        bucket_absent = False
    try:
        dynamodb.describe_table(TableName=name)
    except ClientError as exc:
        table_absent = _missing(exc)
    else:
        table_absent = False
    print(json.dumps({"bucket_absent": bucket_absent, "table_absent": table_absent}))
    if not bucket_absent or not table_absent:
        raise RuntimeError("cleanup did not verify both resources absent")


if __name__ == "__main__":
    main()
