"""Audit tenant reservations and catalog ownership in bounded pages.

Default mode is read-only. ``--apply`` is accepted only for reservation pages
and releases a slot only after a deleted catalog record, strongly read
permanent pointer-retirement marker, unversioned bucket and empty group agree.
Run catalog auditing separately to
detect live records missing reservations. The stack must be quiet for a full
catalog audit; no page sequence is a point-in-time snapshot.
"""

from __future__ import annotations

import argparse
import json
import re

import boto3

from tessera.backends.base import NotFound
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud.service import DatabaseCatalog
from tessera.experimental.cloud.tenants import TenantCatalog
from tessera.experimental.turso.fencing import FencedBackend

CATALOG_PREFIX = "_tessera_cloud/catalog/"
CATALOG_KEY = re.compile(r"_tessera_cloud/catalog/[0-9a-f]{32}\.json\Z")


# @spec experiments/reservation-pages
def reservation_pages(catalog, tenant_id, *, apply=False, start_after=None,
                      page_size=100, max_pages=None):
    """Classify reserved IDs; repair only strongly verified deleted entries."""
    tenants = catalog.tenants
    tenants._key(tenant_id)
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise ValueError("page_size must be 1..100")
    if max_pages is not None and (type(max_pages) is not int or max_pages < 1):
        raise ValueError("max_pages must be positive")
    cursor = start_after
    page_number = 0
    while max_pages is None or page_number < max_pages:
        ids, next_cursor = tenants.reserved_page(tenant_id, cursor=cursor,
                                                  limit=page_size)
        reservation_record, _ = tenants._load(tenant_id)
        items = []
        for database_id in ids:
            fingerprint = reservation_record["reservations"].get(database_id)
            if fingerprint is None:
                # A concurrent release invalidates the page snapshot. Retry
                # after traffic is quiet rather than claim an audited item.
                raise RuntimeError("tenant reservations changed during audit page")
            try:
                record, _ = catalog._load(database_id)
            except NotFound:
                status = "missing_catalog"
            else:
                if record.get("tenant_id") != tenant_id:
                    status = "owner_mismatch"
                elif record.get("create_fingerprint") != fingerprint:
                    status = "fingerprint_mismatch"
                elif record["state"] != "deleted":
                    status = record["state"]
                else:
                    status = catalog._tenant_slot_release_status(database_id, record)
                    if status == "release_ready" and apply:
                        catalog._release_tenant_slot_after_purge(
                            database_id, expected_tenant_id=tenant_id,
                            expected_fingerprint=fingerprint)
                        status = "released"
            items.append({"database_id": database_id, "status": status})
        page_number += 1
        counts = {}
        for item in items:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        yield {"mode": "reservations", "tenant_id": tenant_id,
               "page": page_number, "inspected": len(items), "counts": counts,
               "items": items, "next_cursor": next_cursor,
               "complete": next_cursor is None}
        if next_cursor is None:
            return
        cursor = next_cursor


# @spec experiments/catalog-pages
def catalog_pages(catalog, tenant_id, *, start_after=None, page_size=100,
                  max_pages=None):
    """Find tenant-owned live records absent from the reservation map."""
    tenants = catalog.tenants
    tenants._key(tenant_id)
    if start_after is not None and (not isinstance(start_after, str)
                                    or not start_after.startswith(CATALOG_PREFIX)
                                    or len(start_after) > 1024):
        raise ValueError("start_after must belong to the catalog prefix")
    if type(page_size) is not int or not 1 <= page_size <= 1000:
        raise ValueError("page_size must be 1..1000")
    if max_pages is not None and (type(max_pages) is not int or max_pages < 1):
        raise ValueError("max_pages must be positive")
    if not hasattr(catalog.backend, "list_page"):
        raise RuntimeError("catalog audit requires a paged S3 backend")
    cursor = start_after
    page_number = 0
    while max_pages is None or page_number < max_pages:
        page = catalog.backend.list_page(CATALOG_PREFIX, page_size,
                                         start_after=cursor)
        reservation_record, tenant_etag = tenants._load(tenant_id)
        reservations = reservation_record["reservations"]
        issues = []
        owned = 0
        for key, _ in page:
            if CATALOG_KEY.fullmatch(key) is None:
                issues.append({"key": key, "status": "unexpected_catalog_key"})
                continue
            database_id = key[len(CATALOG_PREFIX):-5]
            record, _ = catalog._load(database_id)
            if record.get("tenant_id") != tenant_id:
                continue
            owned += 1
            reserved_fingerprint = reservations.get(database_id)
            if record["state"] in ("creating", "active", "deleting"):
                if reserved_fingerprint is None:
                    status = "live_without_reservation"
                elif reserved_fingerprint != record.get("create_fingerprint"):
                    status = "fingerprint_mismatch"
                else:
                    continue
            elif record["state"] == "deleted" and reserved_fingerprint is not None:
                status = "deleted_still_reserved"
            else:
                continue
            issues.append({"database_id": database_id, "status": status})
        page_number += 1
        cursor = page[-1][0] if page else None
        yield {"mode": "catalog", "tenant_id": tenant_id,
               "page": page_number, "scanned": len(page), "owned": owned,
               "issues": issues, "tenant_etag": tenant_etag,
               # S3 may return a short nonempty page before more keys.
               "next_cursor": cursor, "complete": not page}
        if not page:
            return


# @spec experiments/cloud-tenant-reconcile-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--mode", choices=("reservations", "catalog"), required=True)
    parser.add_argument("--apply", action="store_true",
                        help="release only verified deleted reservations")
    parser.add_argument("--start-after")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int)
    args = parser.parse_args()
    if args.apply and args.mode != "reservations":
        parser.error("--apply is valid only in reservations mode")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    raw = S3DynamoBackend(args.bucket, args.table, args.region,
                          s3_client=session.client("s3"),
                          dynamodb=session.resource("dynamodb"))
    catalog = DatabaseCatalog(raw,
                              data_backend=FencedBackend(raw, session.client("dynamodb")),
                              tenant_catalog=TenantCatalog(raw))
    if args.mode == "reservations":
        iterator = reservation_pages(catalog, args.tenant_id, apply=args.apply,
                                     start_after=args.start_after,
                                     page_size=args.page_size, max_pages=args.max_pages)
    else:
        iterator = catalog_pages(catalog, args.tenant_id,
                                 start_after=args.start_after,
                                 page_size=args.page_size, max_pages=args.max_pages)
    for checkpoint in iterator:
        print(json.dumps(checkpoint, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
