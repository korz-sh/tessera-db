"""Checkpointed assignment of legacy root databases to one tenant.

Run only after root creation and restore of unowned sources are disabled and
management/deletion traffic is quiesced. Dry-run is the default. Each applied
database reserves its immutable fingerprint before a CAS adds tenant_id.
Interrupted pages can be replayed with the same tenant and cursor.
"""

from __future__ import annotations

import argparse
import json
import re

import boto3

from tessera.backends.base import CASFailed
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud.service import DatabaseCatalog, _document, _limits
from tessera.experimental.cloud.tenants import TenantCatalog

PREFIX = "_tessera_cloud/catalog/"
CATALOG_KEY = re.compile(r"_tessera_cloud/catalog/[0-9a-f]{32}\.json\Z")
FINGERPRINT = re.compile(r"[0-9a-f]{64}\Z")
LIVE_STATES = frozenset(("creating", "active", "deleting"))


# @spec experiments/migrate-pages
def migrate_pages(catalog, tenant_id, *, apply=False, start_after=None,
                  page_size=100, max_pages=None):
    """Yield durable page checkpoints; hold reservations on ambiguous races."""
    tenants = catalog.tenants
    tenants._key(tenant_id)
    if start_after is not None and (not isinstance(start_after, str)
                                    or not start_after.startswith(PREFIX)
                                    or len(start_after) > 1024):
        raise ValueError("start_after must belong to the catalog prefix")
    if type(page_size) is not int or not 1 <= page_size <= 1000:
        raise ValueError("page_size must be 1..1000")
    if max_pages is not None and (type(max_pages) is not int or max_pages < 1):
        raise ValueError("max_pages must be positive")
    if not hasattr(catalog.backend, "list_page"):
        raise RuntimeError("migration requires a paged S3 backend")
    tenant, _ = tenants._load(tenant_id)
    if tenant["state"] != "active":
        raise RuntimeError("target tenant must be active")
    if apply and tenant["tokens"]:
        raise RuntimeError("revoke target tenant management tokens before migration")
    cursor = start_after
    page_number = 0
    while max_pages is None or page_number < max_pages:
        page = catalog.backend.list_page(PREFIX, page_size, start_after=cursor)
        items = []
        for key, _ in page:
            if CATALOG_KEY.fullmatch(key) is None:
                if apply:
                    raise RuntimeError("unexpected catalog key; migration stopped")
                items.append({"key": key, "status": "unexpected_catalog_key"})
                continue
            database_id = key[len(PREFIX):-5]
            record, etag = catalog._load(database_id)
            fingerprint = record.get("create_fingerprint")
            if record.get("tenant_id") is None and record["state"] == "deleted":
                status = "deleted_legacy"
            elif not isinstance(fingerprint, str) or not FINGERPRINT.fullmatch(fingerprint):
                status = "invalid_fingerprint"
            elif record.get("tenant_id") is not None:
                if record["tenant_id"] == tenant_id and record["state"] in LIVE_STATES:
                    tenants.require_reservation(tenant_id, database_id, fingerprint)
                    status = "already_migrated"
                else:
                    status = "other_owner_or_deleted"
            elif record["state"] not in LIVE_STATES:
                status = "invalid_state"
            else:
                tenants.validate_database_limits(tenant_id,
                                                 _limits(record.get("limits")))
                if apply:
                    tenants.reserve_database(tenant_id, database_id, fingerprint)
                    for _ in range(8):
                        if (record.get("tenant_id") not in (None, tenant_id)
                                or record.get("create_fingerprint") != fingerprint
                                or record["state"] not in LIVE_STATES):
                            raise RuntimeError(
                                "catalog changed after reservation; slot remains held")
                        if record.get("tenant_id") == tenant_id:
                            break
                        updated = dict(record, tenant_id=tenant_id)
                        try:
                            catalog.backend.put(key, _document(updated), if_match=etag)
                            break
                        except CASFailed:
                            record, etag = catalog._load(database_id)
                    else:
                        raise RuntimeError("catalog CAS remained busy; slot remains held")
                    tenants.require_reservation(tenant_id, database_id, fingerprint)
                    status = "migrated"
                else:
                    status = "would_migrate"
            if apply and status in ("invalid_fingerprint", "invalid_state"):
                raise RuntimeError(f"{status}; migration stopped")
            items.append({"database_id": database_id, "status": status})
        page_number += 1
        cursor = page[-1][0] if page else None
        counts = {}
        for item in items:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        tenant, tenant_etag = tenants._load(tenant_id)
        yield {"page": page_number, "scanned": len(page), "counts": counts,
               "items": items, "tenant_id": tenant_id,
               "reserved_databases": len(tenant["reservations"]),
               "tenant_etag": tenant_etag,
               "next_cursor": cursor, "complete": not page}
        if not page:
            return


# @spec experiments/cloud-migrate-legacy-tenant-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--management-quiesced", action="store_true",
                        help="affirm that root create/restore and management/deletion traffic are stopped")
    parser.add_argument("--start-after")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int)
    args = parser.parse_args()
    if args.apply and not args.management_quiesced:
        parser.error("--apply requires --management-quiesced")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    raw = S3DynamoBackend(args.bucket, args.table, args.region,
                          s3_client=session.client("s3"),
                          dynamodb=session.resource("dynamodb"))
    catalog = DatabaseCatalog(raw, tenant_catalog=TenantCatalog(raw))
    for checkpoint in migrate_pages(catalog, args.tenant_id, apply=args.apply,
                                    start_after=args.start_after,
                                    page_size=args.page_size, max_pages=args.max_pages):
        print(json.dumps(checkpoint, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
