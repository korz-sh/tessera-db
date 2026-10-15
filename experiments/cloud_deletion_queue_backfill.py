"""Backfill legacy deleting catalog records into the durable deletion queue.

The default mode only counts candidates. Each JSON output line is a completed
page checkpoint; pass its ``next_cursor`` to resume after interruption.
"""

from __future__ import annotations

import argparse
import json
import re

import boto3

from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud.service import DatabaseCatalog, _DELETION_QUEUE_PREFIX
from tessera.experimental.turso.fencing import FencedBackend

PREFIX = "_tessera_cloud/catalog/"
KEY = re.compile(r"^_tessera_cloud/catalog/[0-9a-f]{32}\.json$")


# @spec experiments/backfill
def backfill(catalog, *, apply=False, verify=False, start_after=None, page_size=1000,
             max_pages=None):
    """Yield a checkpoint only after every record on its S3 page succeeds."""
    if start_after is not None and KEY.fullmatch(start_after) is None:
        raise ValueError("start_after must be an exact catalog JSON key")
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be 1..1000")
    if max_pages is not None and max_pages < 1:
        raise ValueError("max_pages must be positive")
    cursor = start_after
    pages = 0
    while max_pages is None or pages < max_pages:
        page = catalog.backend.list_page(PREFIX, page_size, start_after=cursor)
        if not page:
            yield {"page": pages + 1, "scanned": 0, "deleting": 0,
                   "ensured": 0, "missing_queue": 0, "invalid_queue": 0,
                   "next_cursor": None, "complete": True}
            return
        deleting = ensured = missing_queue = invalid_queue = 0
        for key, _ in page:
            if KEY.fullmatch(key) is None:
                continue
            database_id = key[len(PREFIX):-5]
            status = catalog.deletion_status(database_id)
            if status["state"] == "deleting":
                deleting += 1
                if apply:
                    catalog._enqueue_deletion(database_id, status["delete_after"])
                    ensured += 1
                if verify:
                    item = catalog.data_backend.client.get_item(
                        TableName=catalog.data_backend.table_name,
                        Key={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id}},
                        ConsistentRead=True).get("Item")
                    if item is None:
                        missing_queue += 1
                    elif not catalog._valid_deletion_queue_item(database_id, item):
                        invalid_queue += 1
        pages += 1
        cursor = page[-1][0]
        yield {"page": pages, "scanned": len(page), "deleting": deleting,
               "ensured": ensured, "missing_queue": missing_queue,
               "invalid_queue": invalid_queue,
               "next_cursor": cursor,
               "complete": False}


# @spec experiments/cloud-deletion-queue-backfill-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--apply", action="store_true",
                        help="write queue items; otherwise only count candidates")
    parser.add_argument("--verify", action="store_true",
                        help="strongly read queue items and count missing ones")
    parser.add_argument("--start-after")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int)
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    client = session.client("dynamodb")
    if args.apply or args.verify:
        table = client.describe_table(TableName=args.table)["Table"]
        indexes = {item["IndexName"]: item["IndexStatus"]
                   for item in table.get("GlobalSecondaryIndexes", [])}
        if indexes.get("pending-deletions") != "ACTIVE":
            raise RuntimeError("pending-deletions GSI must be ACTIVE before backfill")
    raw = S3DynamoBackend(args.bucket, args.table, args.region,
                          s3_client=session.client("s3"),
                          dynamodb=session.resource("dynamodb"))
    catalog = DatabaseCatalog(raw, data_backend=FencedBackend(raw, client))
    for checkpoint in backfill(catalog, apply=args.apply, verify=args.verify,
                               start_after=args.start_after,
                               page_size=args.page_size, max_pages=args.max_pages):
        print(json.dumps(checkpoint, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
