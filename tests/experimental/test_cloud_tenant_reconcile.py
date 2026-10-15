"""Quota reconciliation may release only verified, physically deleted IDs."""

import json
from hashlib import sha256

import pytest

from experiments.cloud_tenant_reconcile import catalog_pages, reservation_pages
from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService


class PagedLocalBackend(LocalBackend):
    def list_page(self, prefix, limit=1000, *, start_after=None):
        keys = [key for key in self.list(prefix) if start_after is None or key > start_after]
        return [(key, len(self.get(key)[0])) for key in keys[:limit]]


def _deleted(catalog, database_id):
    record, etag = catalog._load(database_id)
    record["state"] = "deleted"
    catalog.backend.put(catalog._key(database_id),
                        json.dumps(record, sort_keys=True,
                                   separators=(",", ":")).encode(), if_match=etag)


def test_reservation_audit_only_releases_verified_deleted_entries(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    retired = set()
    backend.pointer_retired = lambda key: key in retired
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("app")["id"]
    active = service.catalog.create("active", [], request_id="active",
                                    tenant_id=tenant_id)["id"]
    deleted = service.catalog.create("deleted", [], request_id="deleted",
                                     tenant_id=tenant_id)["id"]
    _deleted(service.catalog, deleted)
    missing = "f" * 32
    service.tenants.reserve_database(tenant_id, missing, sha256(b"missing").hexdigest())

    pages = list(reservation_pages(service.catalog, tenant_id, page_size=1))
    assert len(pages) == 3 and pages[-1]["complete"]
    statuses = {item["database_id"]: item["status"] for page in pages
                for item in page["items"]}
    assert statuses == {active: "active", deleted: "deleted_unretired",
                        missing: "missing_catalog"}
    assert service.tenants.get(tenant_id)["reserved_databases"] == 3

    retired.add(f"turso-native/cloud-{deleted}/manifest.json")
    nonempty = list(reservation_pages(service.catalog, tenant_id))
    assert {item["database_id"]: item["status"] for item in nonempty[0]["items"]}[
        deleted] == "deleted_nonempty"
    for key in backend.list(f"turso-native/cloud-{deleted}/"):
        backend.delete(key)
    ready = list(reservation_pages(service.catalog, tenant_id))
    assert {item["database_id"]: item["status"] for item in ready[0]["items"]}[
        deleted] == "release_ready"
    applied = list(reservation_pages(service.catalog, tenant_id, apply=True))
    assert {item["database_id"]: item["status"] for item in applied[0]["items"]}[
        deleted] == "released"
    assert service.tenants.get(tenant_id)["reserved_databases"] == 2
    assert active in service.tenants._load(tenant_id)[0]["reservations"]
    assert missing in service.tenants._load(tenant_id)[0]["reservations"]
    assert all("token" not in json.dumps(page).lower() for page in applied)


def test_catalog_audit_finds_live_database_without_slot_and_resumes(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("app")["id"]
    first = service.catalog.create("first", [], request_id="first",
                                   tenant_id=tenant_id)["id"]
    second = service.catalog.create("second", [], request_id="second",
                                    tenant_id=tenant_id)["id"]
    fingerprint = service.catalog._load(first)[0]["create_fingerprint"]
    service.tenants._release_database_after_purge(tenant_id, first, fingerprint)
    backend.put("_tessera_cloud/catalog/unexpected", b"unexpected")

    first_page = next(catalog_pages(service.catalog, tenant_id, page_size=1,
                                    max_pages=1))
    assert not first_page["complete"] and first_page["next_cursor"]
    rest = list(catalog_pages(service.catalog, tenant_id,
                              start_after=first_page["next_cursor"], page_size=1))
    pages = [first_page, *rest]
    assert pages[-1]["complete"] and pages[-1]["scanned"] == 0
    issues = [issue for page in pages for issue in page["issues"]]
    assert {issue["status"] for issue in issues} == {
        "live_without_reservation", "unexpected_catalog_key"}
    assert any(issue.get("database_id") == first for issue in issues)
    assert not any(issue.get("database_id") == second for issue in issues)
    with pytest.raises(ValueError, match="catalog prefix"):
        list(catalog_pages(service.catalog, tenant_id,
                           start_after="foreign/key.json"))


def test_catalog_audit_continues_after_short_nonempty_s3_page(tmp_path, monkeypatch):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("app")["id"]
    for index in range(3):
        service.catalog.create(f"db-{index}", [], request_id=f"db-{index}",
                               tenant_id=tenant_id)
    original = backend.list_page
    monkeypatch.setattr(backend, "list_page", lambda prefix, limit, **options:
                        original(prefix, 1, **options))
    pages = list(catalog_pages(service.catalog, tenant_id, page_size=100))
    assert len(pages) == 4
    assert [page["owned"] for page in pages] == [1, 1, 1, 0]
    assert [page["complete"] for page in pages] == [False, False, False, True]
