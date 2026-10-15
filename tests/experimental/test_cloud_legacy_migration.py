"""Legacy root IDs remain replayable while fresh BYOC admission uses tenants."""

import json

import pytest

from experiments.cloud_migrate_legacy_tenant import migrate_pages
from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.service import dispatch_request, error_response


class PagedLocalBackend(LocalBackend):
    def list_page(self, prefix, limit=1000, *, start_after=None):
        keys = [key for key in self.list(prefix)
                if start_after is None or key > start_after]
        return [(key, None) for key in keys[:limit]]


def _call(service, method, path, token, body=None):
    try:
        return dispatch_request(service, method, path, token, lambda: body)
    except Exception as exc:  # noqa: BLE001 - exercise the transport error map
        return error_response(exc)


def test_strict_root_replays_migrated_legacy_id_without_new_admission(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    admin = "a" * 48
    old = DatabaseService(backend, admin_token=admin)
    body = {"name": "legacy", "request_id": "stable",
            "schema": ["CREATE TABLE kv(v INTEGER)"]}
    created = _call(old, "POST", "/v1/databases", admin, body)
    assert created[0] == 201
    database_id = created[1]["id"]
    strict = DatabaseService(backend, admin_token=admin,
                             allow_legacy_root_create=False)
    assert _call(strict, "POST", "/v1/databases", admin, body) == created
    assert _call(strict, "POST", "/v1/databases", admin,
                 dict(body, request_id="new"))[0] == 409
    assert _call(strict, "POST", "/v1/databases", admin,
                 dict(body, name="changed"))[0] == 409
    tenant_id = strict.tenants.create("default", request_id="legacy-default")["id"]
    dry = list(migrate_pages(strict.catalog, tenant_id))
    assert dry[0]["counts"] == {"would_migrate": 1}
    assert dry[-1]["complete"] is True
    assert strict.tenants.get(tenant_id)["reserved_databases"] == 0
    applied = list(migrate_pages(strict.catalog, tenant_id, apply=True))
    assert applied[0]["counts"] == {"migrated": 1}
    assert strict.tenants.get(tenant_id)["reserved_databases"] == 1
    assert strict.catalog.get(database_id)["tenant_id"] == tenant_id
    assert _call(strict, "POST", "/v1/databases", admin, body)[1]["id"] == database_id
    assert _call(old, "POST", "/v1/databases", admin, body)[1]["id"] == database_id
    assert next(migrate_pages(strict.catalog, tenant_id, apply=True))["counts"] == {
        "already_migrated": 1}
    tenant_token = strict.tenants.issue_token(tenant_id)["token"]
    assert _call(strict, "GET", f"/v1/databases/{database_id}", tenant_token)[0] == 200
    assert _call(strict, "GET", "/v1/databases", tenant_token)[1]["databases"][0][
        "id"] == database_id
    restored = _call(strict, "POST", f"/v1/databases/{database_id}/restore", admin,
                     {"source_version": 0, "name": "copy", "request_id": "restore"})
    assert restored[0] == 201
    assert restored[1]["tenant_id"] == tenant_id
    assert strict.tenants.get(tenant_id)["reserved_databases"] == 2


def test_legacy_migration_resume_after_reserved_slot_and_failed_catalog_cas(
        tmp_path, monkeypatch):
    backend = PagedLocalBackend(tmp_path)
    old = DatabaseService(backend, admin_token="a" * 48)
    created = old.catalog.create("legacy", [], request_id="stable")
    tenant_id = old.tenants.create("default", request_id="legacy-default")["id"]
    original_put = backend.put
    injected = False

    def fail_once(key, body, **conditions):
        nonlocal injected
        if (not injected and key == old.catalog._key(created["id"])
                and conditions.get("if_match") is not None):
            injected = True
            raise RuntimeError("injected stop before owner CAS")
        return original_put(key, body, **conditions)

    monkeypatch.setattr(backend, "put", fail_once)
    with pytest.raises(RuntimeError, match="injected stop"):
        list(migrate_pages(old.catalog, tenant_id, apply=True))
    assert old.tenants.get(tenant_id)["reserved_databases"] == 1
    assert "tenant_id" not in old.catalog.get(created["id"])
    assert next(migrate_pages(old.catalog, tenant_id, apply=True))["counts"] == {
        "migrated": 1}
    assert old.tenants.get(tenant_id)["reserved_databases"] == 1


def test_strict_root_restore_cannot_create_from_unmigrated_source(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    old = DatabaseService(backend, admin_token="a" * 48)
    source = old.catalog.create("legacy", [], request_id="source")["id"]
    strict = DatabaseService(backend, admin_token="a" * 48,
                             allow_legacy_root_create=False)
    denied = _call(strict, "POST", f"/v1/databases/{source}/restore", "a" * 48,
                   {"source_version": 0, "name": "copy", "request_id": "copy"})
    assert denied[0] == 409
    assert denied[1]["error"] == "catalog_conflict"


def test_migration_pages_resume_from_checkpoint(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    for number in range(3):
        service.catalog.create(f"legacy-{number}", [], request_id=str(number))
    tenant_id = service.tenants.create("default")["id"]
    first = next(migrate_pages(service.catalog, tenant_id, page_size=1,
                               max_pages=1, apply=True))
    assert first["next_cursor"] and not first["complete"]
    rest = list(migrate_pages(service.catalog, tenant_id, start_after=first["next_cursor"],
                              page_size=1, apply=True))
    assert rest[-1]["complete"]
    assert service.tenants.get(tenant_id)["reserved_databases"] == 3
    assert all(item["tenant_id"] == tenant_id for item in service.catalog.list())


def test_apply_requires_target_tenant_without_management_tokens(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("default")["id"]
    service.tenants.issue_token(tenant_id)
    with pytest.raises(RuntimeError, match="revoke target tenant"):
        list(migrate_pages(service.catalog, tenant_id, apply=True))


def test_invalid_legacy_state_is_reported_and_stops_apply(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    database_id = service.catalog.create("legacy", [], request_id="stable")["id"]
    tenant_id = service.tenants.create("default")["id"]
    record, etag = service.catalog._load(database_id)
    record["state"] = "corrupt"
    backend.put(service.catalog._key(database_id), json.dumps(record).encode(),
                if_match=etag)
    assert next(migrate_pages(service.catalog, tenant_id))["counts"] == {
        "invalid_state": 1}
    with pytest.raises(RuntimeError, match="invalid_state"):
        list(migrate_pages(service.catalog, tenant_id, apply=True))
    assert service.tenants.get(tenant_id)["reserved_databases"] == 0


def test_deleted_legacy_record_does_not_consume_tenant_slot(tmp_path):
    backend = PagedLocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    database_id = service.catalog.create("legacy", [], request_id="stable")["id"]
    tenant_id = service.tenants.create("default")["id"]
    record, etag = service.catalog._load(database_id)
    record["state"] = "deleted"
    record.pop("create_fingerprint")
    backend.put(service.catalog._key(database_id), json.dumps(record).encode(),
                if_match=etag)
    assert next(migrate_pages(service.catalog, tenant_id, apply=True))["counts"] == {
        "deleted_legacy": 1}
    assert service.tenants.get(tenant_id)["reserved_databases"] == 0
