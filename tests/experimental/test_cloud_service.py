"""The first network surface must preserve tenant auth and v2 receipts."""

import json
from http.client import HTTPConnection
from threading import Thread
from unittest.mock import patch
from urllib.parse import urlencode

import pytest

from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService, serve
from tessera.experimental.cloud.service import (
    CatalogConflict,
    CatalogQuotaExceeded,
    DatabaseCatalog,
    DatabaseUnavailable,
    RestoreOutcomeUnknown,
    parse_json_body,
    SqlExecutionFailed,
    dispatch_request,
    error_response,
)

ADMIN = "a" * 48


def test_admin_secret_must_be_header_safe(tmp_path):
    with pytest.raises(ValueError, match="whitespace"):
        DatabaseService(LocalBackend(tmp_path / "objects"), admin_token=ADMIN + "\n")
    with pytest.raises(TypeError, match="fenced AWS backend"):
        DatabaseService(LocalBackend(tmp_path / "objects"), admin_token=ADMIN,
                        native_gc_enabled=True)
    with pytest.raises(ValueError, match="requires native GC"):
        DatabaseService(LocalBackend(tmp_path / "objects"), admin_token=ADMIN,
                        retention_metadata_limit=2)


def _request(port, method, path, *, token=None, body=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=15)
    headers = {}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    if body is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(body)
    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    result = response.status, json.loads(response.read())
    connection.close()
    return result


def test_http_catalog_auth_receipts_and_cold_recovery(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    server = serve(service, port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        assert _request(port, "GET", "/v1/databases")[0] == 401
        status, created = _request(port, "POST", "/v1/databases", token=ADMIN,
                                   body={"name": "first", "schema": [
                                       "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"],
                                         "request_id": "first-create"})
        assert status == 201
        assert _request(port, "POST", "/v1/databases", token=ADMIN,
                        body={"name": "first", "schema": [
                            "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"],
                            "request_id": "first-create"}) == (201, created)
        db = created["id"]
        status, second = _request(port, "POST", "/v1/databases", token=ADMIN,
                                  body={"name": "second", "schema": [
                                      "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"]})
        assert status == 201
        assert len(_request(port, "GET", "/v1/databases", token=ADMIN)[1]["databases"]) == 2
        first_page = _request(port, "GET", "/v1/databases?limit=1", token=ADMIN)
        assert first_page[0] == 200 and len(first_page[1]["databases"]) == 1
        assert first_page[1]["next_cursor"]
        second_page = _request(port, "GET", "/v1/databases?" + urlencode({
            "limit": 1, "cursor": first_page[1]["next_cursor"]}), token=ADMIN)
        assert second_page[0] == 200 and len(second_page[1]["databases"]) == 1
        assert second_page[1]["next_cursor"] is None
        assert {item["id"] for item in first_page[1]["databases"]
                + second_page[1]["databases"]} == {db, second["id"]}
        assert _request(port, "GET", "/v1/databases?limit=0", token=ADMIN)[0] == 400
        assert _request(port, "GET", "/v1/databases?cursor=other", token=ADMIN)[0] == 400

        status, issued = _request(port, "POST", f"/v1/databases/{db}/tokens",
                                  token=ADMIN, body={"scope": "write"})
        assert status == 201
        token = issued["token"]
        assert token not in backend.get(service.catalog._key(db))[0].decode()
        transaction = {"operation_id": "op-1", "statements": [
            ["INSERT INTO kv VALUES (?, ?)", ["hello", "world"]]]}
        assert _request(port, "POST", f"/v1/databases/{db}/transactions",
                        body=transaction)[0] == 401
        assert _request(port, "POST", f"/v1/databases/{second['id']}/transactions",
                        token=token, body=transaction)[0] == 401
        status, receipt = _request(port, "POST", f"/v1/databases/{db}/transactions",
                                   token=token, body=transaction)
        assert status == 200 and receipt["version"] == 1
        assert _request(port, "POST", f"/v1/databases/{db}/transactions",
                        token=token, body=transaction) == (status, receipt)
        assert _request(port, "POST", f"/v1/databases/{db}/transactions",
                        token=token, body={"operation_id": "op-1", "statements": [
                            ["INSERT INTO kv VALUES (?, ?)", ["other", "value"]]]})[0] == 422
        assert _request(port, "POST", f"/v1/databases/{db}/query", token=token,
                        body={"sql": "SELECT v FROM kv WHERE k=?", "params": ["hello"]}) == (
                            200, {"rows": [["world"]]})
        assert _request(port, "GET", f"/v1/databases/{db}/operations/op-1",
                        token=token)[1]["state"] == "published"

        read_token = _request(port, "POST", f"/v1/databases/{db}/tokens", token=ADMIN,
                              body={"scope": "read"})[1]["token"]
        assert _request(port, "POST", f"/v1/databases/{db}/transactions",
                        token=read_token, body=transaction)[0] == 401
        assert _request(port, "POST", f"/v1/databases/{db}/query", token=read_token,
                        body={"sql": "SELECT v FROM kv"})[0] == 200
        assert _request(port, "POST", f"/v1/databases/{db}/query", token=read_token,
                        body={"sql": "SELECT hex(randomblob(600000))"})[0] == 413
        assert _request(port, "POST", f"/v1/databases/{db}/query", token=read_token,
                        body={"sql": "SELECT randomblob(2)"})[0] == 400
        assert _request(port, "DELETE", f"/v1/databases/{db}/tokens/{issued['token_id']}",
                        token=ADMIN)[0] == 200
        assert _request(port, "POST", f"/v1/databases/{db}/query", token=token,
                        body={"sql": "SELECT v FROM kv"})[0] == 401
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    recovered = DatabaseService(backend, admin_token=ADMIN)
    assert recovered.read(db, "SELECT v FROM kv WHERE k=?", ["hello"]) == [("world",)]
    assert recovered.lookup(db, "op-1")["version"] == 1


def test_bounded_warm_read_cache_reuses_frontier_and_rechecks_publication(
        tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN, read_cache_bytes=16_777_216)
    db = service.catalog.create("cached", ["CREATE TABLE kv(v INTEGER)"])["id"]
    first = service.write(db, "first", [["INSERT INTO kv VALUES(1)", []]])
    native_gets = []
    get = backend.get

    def counted_get(key):
        if key.startswith("turso-native/"):
            native_gets.append(key)
        return get(key)

    monkeypatch.setattr(backend, "get", counted_get)
    assert service.read(db, "SELECT count(*) FROM kv", min_version=first["version"]) == [(1,)]
    cold_gets = len(native_gets)
    assert service.read(db, "SELECT count(*) FROM kv", min_version=first["version"]) == [(1,)]
    warm_gets = len(native_gets) - cold_gets
    assert cold_gets > warm_gets >= 1

    second = service.write(db, "second", [["INSERT INTO kv VALUES(2)", []]])
    assert service.read(db, "SELECT count(*) FROM kv", min_version=second["version"]) == [(2,)]
    cache = service._cached_reader.cache_root
    assert len([entry for entry in cache.iterdir() if entry.is_dir()]) == 1

    other = service.catalog.create("other", ["CREATE TABLE kv(v INTEGER)"])["id"]
    assert service.read(other, "SELECT count(*) FROM kv") == [(0,)]
    assert not cache.exists()
    other_cache = service._cached_reader.cache_root
    service.discard_read_cache(other)
    assert not other_cache.exists()
    service.close()


def test_warm_read_cache_skips_frontiers_over_budget(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN, read_cache_bytes=1)
    db = service.catalog.create("large", ["CREATE TABLE kv(v INTEGER)"])["id"]
    try:
        assert service.read(db, "SELECT count(*) FROM kv") == [(0,)]
        assert service.read(db, "SELECT count(*) FROM kv") == [(0,)]
        assert list(service._cached_reader.cache_root.iterdir()) == []
    finally:
        service.close()


def test_database_limits_reject_unpublished_writes_and_preserve_replays(tmp_path):
    from tessera.experimental.turso.api import DatabaseQuotaExceeded
    from tessera.experimental.turso.native import NativeLogStore

    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    limits = {"max_frontier_bytes": 67_108_864,
              "max_operation_bytes": 1_024, "max_tokens": 1}
    body = {"name": "quota", "schema": ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"],
            "request_id": "quota-create", "limits": limits}
    status, created = dispatch_request(service, "POST", "/v1/databases", ADMIN,
                                       lambda: body)
    assert status == 201 and created["limits"] == limits
    assert dispatch_request(service, "POST", "/v1/databases", ADMIN,
                            lambda: body) == (201, created)
    db = created["id"]
    assert dispatch_request(service, "GET", f"/v1/databases/{db}/limits", None,
                            dict)[0] == 401
    assert dispatch_request(service, "GET", f"/v1/databases/{db}/limits", ADMIN,
                            dict) == (200, {"limits": limits})
    token = service.catalog.issue_token(db)
    with pytest.raises(CatalogQuotaExceeded) as token_error:
        dispatch_request(service, "POST", f"/v1/databases/{db}/tokens", ADMIN,
                         lambda: {"scope": "read"})
    assert error_response(token_error.value)[0] == 413
    large = [["INSERT INTO kv VALUES(?, ?)", ["first", "x" * 50_000]]]
    with pytest.raises(DatabaseQuotaExceeded) as payload_error:
        dispatch_request(service, "POST", f"/v1/databases/{db}/transactions",
                         token.secret, lambda: {"operation_id": "quota-op",
                                                "statements": large})
    assert error_response(payload_error.value)[0] == 413
    with pytest.raises(DatabaseQuotaExceeded, match="operation payload quota exceeded"):
        service.write(db, "quota-op", large)
    assert service.lookup(db, "quota-op")["state"] == "absent"

    raised = dict(limits, max_operation_bytes=65_536)
    assert dispatch_request(service, "PUT", f"/v1/databases/{db}/limits", ADMIN,
                            lambda: {"limits": raised}) == (200, {"limits": raised})
    first = service.write(db, "quota-op", large)
    assert first["version"] == 1
    storage = NativeLogStore(backend, "cloud-" + db)
    manifest, _ = storage.manifest()
    frontier_bytes = manifest["checkpoint"]["bytes"] + manifest["log_bytes"]
    lowered = dict(raised, max_frontier_bytes=max(65_536, frontier_bytes))
    service.catalog.update_limits(db, lowered)
    images_before = set(backend.list(storage.prefix + "images/"))
    second = [["INSERT INTO kv VALUES(?, ?)", ["second", "y"]]]
    with pytest.raises(DatabaseQuotaExceeded) as frontier_error:
        dispatch_request(service, "POST", f"/v1/databases/{db}/transactions",
                         token.secret, lambda: {"operation_id": "second",
                                                "statements": second})
    assert error_response(frontier_error.value)[0] == 413
    with pytest.raises(DatabaseQuotaExceeded, match="published frontier byte quota exceeded"):
        service.write(db, "second", second)
    assert service.lookup(db, "second")["state"] == "absent"
    assert set(backend.list(storage.prefix + "images/")) == images_before
    assert service.write(db, "quota-op", large) == first
    recovered = DatabaseService(backend, admin_token=ADMIN)
    assert recovered.catalog.get(db)["limits"] == lowered
    assert recovered.read(db, "SELECT length(v) FROM kv WHERE k=?", ["first"]) == [(50_000,)]


def test_managed_bulk_transaction_keeps_consumer_sized_scan_atomic(tmp_path):
    from tessera.experimental.turso.api import DatabaseQuotaExceeded

    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    schema = [("CREATE TABLE findings(id TEXT PRIMARY KEY, scan_id INTEGER, source TEXT, "
               "rule TEXT, nullable TEXT, severity TEXT, path TEXT, line INTEGER, "
               "message TEXT, issue_key TEXT, count INTEGER)")]
    limits = {"max_frontier_bytes": 67_108_864,
              "max_operation_bytes": 2_097_152, "max_tokens": 64}
    db = service.catalog.create("bulk", schema, limits=limits)["id"]
    rows = []
    message = "representative finding text " + "x" * 160
    for number in range(5000):
        key = f"scan-1-issue-{number}"
        rows.append((key, 1, "semgrep", f"rule-{number % 20}", None,
                     "HIGH" if number % 5 == 0 else "INFO",
                     f"src/file-{number % 50}.py", number + 1,
                     message, key, 1))
    placeholder = "(" + ",".join("?" for _ in range(11)) + ")"
    statements = []
    for offset in range(0, len(rows), 64):
        chunk = rows[offset:offset + 64]
        sql = "INSERT INTO findings VALUES " + ",".join([placeholder] * len(chunk))
        statements.append([sql, [value for row in chunk for value in row]])
    assert len(statements) == 79
    raw = json.dumps({"operation_id": "scan-1", "statements": statements},
                     separators=(",", ":")).encode()
    assert 1_048_576 < len(raw) < 2_097_152
    assert parse_json_body(raw)["operation_id"] == "scan-1"
    receipt = service.write(db, "scan-1", statements)
    assert receipt["version"] == 1
    assert service.read(db, "SELECT count(*) FROM findings") == [(5000,)]
    bad = [["INSERT INTO findings VALUES " + placeholder,
            ["new-key", 2, "semgrep", "rule", None, "INFO", "src/new.py", 1,
             "new", "new-key", 1]],
           ["INSERT INTO findings VALUES " + placeholder, list(rows[0])[:-1] + [2]]]
    with pytest.raises(SqlExecutionFailed):
        service.write(db, "bad-scan", bad)
    assert service.lookup(db, "bad-scan")["state"] == "absent"
    assert service.read(db, "SELECT count(*) FROM findings") == [(5000,)]
    with pytest.raises(SqlExecutionFailed):
        service.write(db, "huge-result", [["SELECT hex(randomblob(600000))", []]])
    assert service.lookup(db, "huge-result")["state"] == "absent"
    lowered = dict(limits, max_operation_bytes=65_536)
    service.catalog.update_limits(db, lowered)
    assert service.write(db, "scan-1", statements) == receipt
    with pytest.raises(DatabaseQuotaExceeded):
        service.write(db, "scan-2", statements)
    assert service.lookup(db, "scan-2")["state"] == "absent"


def test_restore_copy_keeps_source_receipts_and_reconciles_activation(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    source = service.catalog.create(
        "source", ["CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)"],
        request_id="restore-source")["id"]
    first = [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]]
    second = [["INSERT INTO kv VALUES(?, ?)", [2, "two"]]]
    service.write(source, "source-one", first)
    service.write(source, "source-two", second)
    path = f"/v1/databases/{source}/history"
    assert dispatch_request(service, "GET", path, None, dict)[0] == 401
    status, page = dispatch_request(service, "GET", path + "?limit=1", ADMIN, dict)
    assert status == 200 and page["current_version"] == 2
    assert page["retained_versions"] == [0] and page["next_cursor"]
    status, next_page = dispatch_request(
        service, "GET", path + "?" + urlencode({"cursor": page["next_cursor"]}),
        ADMIN, dict)
    assert status == 200 and next_page["retained_versions"] == [1]
    assert next_page["next_cursor"] is None
    assert dispatch_request(service, "POST", f"/v1/databases/{source}/restore",
                            None, dict)[0] == 401

    body = {"source_version": 1, "name": "recovered", "request_id": "restore-once"}
    status, restored = dispatch_request(
        service, "POST", f"/v1/databases/{source}/restore", ADMIN, lambda: body)
    assert status == 201
    target = restored["id"]
    assert target != source
    assert restored["restored_from"] == {"database_id": source, "version": 1}
    assert service.read(source, "SELECT k FROM kv ORDER BY k") == [(1,), (2,)]
    assert service.read(target, "SELECT k FROM kv ORDER BY k") == [(1,)]
    assert service.lookup(source, "source-two")["state"] == "published"
    assert service.lookup(target, "source-one")["state"] == "absent"
    assert service.lookup(target, "source-two")["state"] == "absent"

    # Model a lost activation reply after target publication: retry must adopt
    # the verified target rather than publishing a second copy.
    record, etag = service.catalog._load(target)
    record["state"] = "creating"
    backend.put(service.catalog._key(target),
                json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
                if_match=etag)
    assert dispatch_request(service, "POST", f"/v1/databases/{source}/restore",
                            ADMIN, lambda: body) == (201, restored)
    assert service.write(target, "source-one", [["INSERT INTO kv VALUES(?, ?)",
                                                  [3, "new"]]])["version"] == 1
    assert service.catalog.restore_copy(source, 1, "recovered",
                                        request_id="restore-once") == restored
    with pytest.raises(CatalogConflict):
        service.catalog.restore_copy(source, 2, "recovered", request_id="restore-once")
    fresh = DatabaseService(backend, admin_token=ADMIN)
    assert fresh.read(target, "SELECT k FROM kv ORDER BY k") == [(1,), (3,)]
    assert fresh.read(source, "SELECT k FROM kv ORDER BY k") == [(1,), (2,)]


def test_restore_copy_unknown_publication_retries_same_target(tmp_path, monkeypatch):
    from tessera.experimental.turso.lifecycle import LifecycleManager
    from tessera.experimental.turso.snapshot import UnknownOutcome

    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    source = service.catalog.create("source", ["CREATE TABLE kv(k INTEGER)"])["id"]
    service.write(source, "one", [["INSERT INTO kv VALUES(?)", [1]]])
    original = LifecycleManager.clone

    def lose_reply(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise UnknownOutcome("lost target publication reply")

    monkeypatch.setattr(LifecycleManager, "clone", lose_reply)
    with pytest.raises(RestoreOutcomeUnknown) as unknown:
        service.catalog.restore_copy(source, 1, "copy", request_id="stable-restore")
    status, body = error_response(unknown.value)
    assert status == 202 and body["database_id"] == unknown.value.database_id
    monkeypatch.setattr(LifecycleManager, "clone", original)
    restored = service.catalog.restore_copy(source, 1, "copy", request_id="stable-restore")
    assert restored["id"] == unknown.value.database_id
    assert service.read(restored["id"], "SELECT k FROM kv") == [(1,)]
    assert service.lookup(restored["id"], "one")["state"] == "absent"


def test_catalog_and_sql_use_fenced_aws_backend_under_moto():
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3 = session.client("s3")
        dynamodb = session.resource("dynamodb")
        s3.create_bucket(Bucket="tessera-cloud-test")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        raw = S3DynamoBackend("tessera-cloud-test", "leases", s3_client=s3,
                              dynamodb=dynamodb, backoff_max_attempts=1)
        backend = FencedBackend(raw, session.client("dynamodb"))
        service = DatabaseService(backend, admin_token=ADMIN, catalog_backend=raw)
        db = service.catalog.create("remote", ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"])
        token = service.catalog.issue_token(db["id"])
        assert service.catalog.authorize(db["id"], token.secret, write=True)
        first = service.write(db["id"], "aws-op", [["INSERT INTO kv VALUES(?, ?)",
                                                     ["k", "v"]]])
        assert first["version"] == 1
        assert service.catalog.history_page(db["id"])["retained_versions"] == [0]
        copied = service.catalog.restore_copy(db["id"], 1, "aws-copy",
                                               request_id="aws-restore-copy")
        assert copied["restored_from"] == {"database_id": db["id"], "version": 1}
        assert service.lookup(copied["id"], "aws-op")["state"] == "absent"
        fresh = DatabaseService(backend, admin_token=ADMIN, catalog_backend=raw)
        assert fresh.lookup(db["id"], "aws-op")["state"] == "published"
        assert fresh.read(db["id"], "SELECT v FROM kv WHERE k=?", ["k"]) == [("v",)]
        assert fresh.read(copied["id"], "SELECT v FROM kv WHERE k=?", ["k"]) == [("v",)]

        prefix = f"turso-native/cloud-{db['id']}/"
        for number in range(3):
            raw.put(prefix + f"orphan/{number}", b"retained")
        raw.put(prefix[:-1] + "-neighbor/keep", b"outside")
        assert dispatch_request(service, "GET", f"/v1/databases/{db['id']}/usage",
                                None, dict)[0] == 401
        first = service.usage(db["id"], limit=2, include_logical=True)
        assert first["published"]["version"] == 1
        assert first["published"]["logical_database_bytes"] > 0
        assert first["published"]["artifact_bytes"] > 0
        assert first["next_cursor"] is not None
        pages = [first]
        while pages[-1]["next_cursor"]:
            path = f"/v1/databases/{db['id']}/usage?" + urlencode({
                "limit": 2, "cursor": pages[-1]["next_cursor"]})
            status, page = dispatch_request(service, "GET", path, ADMIN, dict)
            assert status == 200
            assert page["published"] is None
            pages.append(page)
        keys = raw.list(prefix)
        assert sum(page["s3_objects_page"] for page in pages) == len(keys)
        assert sum(page["s3_retained_bytes_page"] for page in pages) == sum(
            len(raw.get(key)[0]) for key in keys)
        with pytest.raises(ValueError, match="exact database group"):
            service.usage(db["id"], cursor="turso-native/cloud-other/manifest.json")
        with pytest.raises(ValueError, match="first page"):
            service.usage(db["id"], cursor=first["next_cursor"], include_logical=True)

        from experiments.cloud_api_staging_smoke import _tier_benchmark

        comparison = _tier_benchmark(service, backend, 2)
        assert comparison["iterations_per_tier"] == 2
        assert len(comparison["cold_samples_ms"]) == 2
        assert len(comparison["warm_samples_ms"]) == 2


def test_create_request_id_replays_and_recovers_a_published_creating_record(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]
    first = service.catalog.create("replay", schema, request_id="stable-create-id")
    assert service.catalog.create("replay", schema, request_id="stable-create-id") == first

    record, etag = service.catalog._load(first["id"])
    record["state"] = "creating"
    backend.put(service.catalog._key(first["id"]),
                json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
                if_match=etag)
    assert service.catalog.create("replay", schema, request_id="stable-create-id") == first
    assert service.catalog.list() == [first]

    try:
        service.catalog.create("different", schema, request_id="stable-create-id")
    except CatalogConflict:
        pass
    else:
        raise AssertionError("reusing a request ID with different content must fail")


def test_create_request_id_recovers_before_first_manifest(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]
    with (patch("tessera.experimental.cloud.service.BatchOwner",
                side_effect=RuntimeError("simulated crash")),
          pytest.raises(RuntimeError, match="simulated crash")):
        service.catalog.create("replay", schema, request_id="crashed-create")
    assert service.catalog.list() == []
    created = service.catalog.create("replay", schema, request_id="crashed-create")
    assert created["state"] == "active"
    assert service.catalog.create("replay", schema, request_id="crashed-create") == created


def test_deletion_request_revokes_tokens_and_is_idempotent(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    service = DatabaseService(backend, admin_token=ADMIN)
    schema = ["CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]
    created = service.catalog.create("delete-me", schema, request_id="delete-create")
    database_id = created["id"]
    token = service.catalog.issue_token(database_id)
    assert service.catalog.authorize(database_id, token.secret, write=True)
    assert dispatch_request(service, "DELETE", f"/v1/databases/{database_id}", ADMIN,
                            dict)[0] == 503
    assert service.catalog.get(database_id)["state"] == "active"
    monkeypatch.setattr("tessera.experimental.cloud.service.time.time", lambda: 1000.0)

    first = service.catalog.request_deletion(database_id)
    assert first == {"id": database_id, "state": "deleting", "delete_after": 1060.0}
    assert service.catalog.request_deletion(database_id) == first
    assert not service.catalog.authorize(database_id, token.secret, write=True)
    assert service.catalog.list() == []
    with pytest.raises(DatabaseUnavailable):
        service.catalog.get(database_id)
    with pytest.raises(DatabaseUnavailable):
        service.catalog.issue_token(database_id)
    with pytest.raises(DatabaseUnavailable):
        service.catalog.create("delete-me", schema, request_id="delete-create")
    record, _ = service.catalog._load(database_id)
    assert record["tokens"] == {}


def test_bounded_purge_resumes_after_delete_failure_and_isolates_group():
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend
    from tessera.experimental.turso.lifecycle import RetentionSession

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3 = session.client("s3")
        s3.create_bucket(Bucket="tessera-purge-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        raw = S3DynamoBackend("tessera-purge-test", "leases", s3_client=s3,
                              dynamodb=dynamodb, backoff_max_attempts=1)
        fenced = FencedBackend(raw, session.client("dynamodb"))
        service = DatabaseService(fenced, admin_token=ADMIN, catalog_backend=raw)
        db = service.catalog.create("delete", ["CREATE TABLE kv(k TEXT PRIMARY KEY)"])["id"]
        side_keys = ["_tessera_cloud/retention/" + db,
                     "_tessera_cloud/native-gc-cleanup/" + db]
        for key in side_keys:
            service.catalog._save_retention_cursor(key, "", False, "saved")
            assert service.catalog._retention_cursor(key) == ("saved", True)
        prefix = f"turso-native/cloud-{db}/"
        with RetentionSession(fenced, "cloud-" + db) as lifecycle:
            assert lifecycle.incremental_mark_step()["started"]
        assert fenced.get_gc_mark_state("cloud-" + db)[0] is not None
        neighbor = f"turso-native/cloud-{db}x/keep"
        raw.put(neighbor, b"neighbor")
        for index in range(7):
            raw.put(prefix + f"orphan/{index}", b"garbage")
        pointer = prefix + "manifest.json"
        assert not fenced.pointer_retired(pointer)
        assert dispatch_request(service, "DELETE", f"/v1/databases/{db}", None,
                                dict)[0] == 401
        status, value = dispatch_request(service, "DELETE", f"/v1/databases/{db}", ADMIN,
                                         dict)
        assert status == 202 and value["state"] == "deleting"
        assert dispatch_request(service, "GET", f"/v1/databases/{db}", ADMIN,
                                dict)[1]["state"] == "deleting"
        assert service.catalog.purge_step(db)["purged_objects"] == 0
        record, etag = service.catalog._load(db)
        record["delete_after"] = 0
        raw.put(service.catalog._key(db), json.dumps(record, sort_keys=True,
                separators=(",", ":")).encode(), if_match=etag)

        original_delete_batch = raw.delete_batch

        def interrupted_delete(keys):
            original_delete_batch(keys)
            raise OSError("simulated lost delete reply")

        with (patch.object(raw, "delete_batch", side_effect=interrupted_delete),
              pytest.raises(OSError, match="simulated")):
            service.catalog.purge_step(db, max_objects=2)
        assert fenced.pointer_retired(pointer)
        assert fenced.get_gc_mark_state("cloud-" + db) == (None, None)
        assert service.catalog.deletion_status(db)["state"] == "deleting"
        assert raw.head(neighbor) is not None
        for _ in range(10):
            result = service.catalog.purge_step(db, max_objects=2)
            if result["state"] == "deleted":
                break
        assert result["state"] == "deleted"
        assert raw.list_page(prefix) == []
        assert raw.head(neighbor) is not None
        assert service.catalog.purge_step(db)["state"] == "deleted"
        assert service.catalog.deletion_status(db)["state"] == "deleted"
        assert all(service.catalog._retention_cursor(key) == ("", False)
                   for key in side_keys)
        assert fenced.pointer_retired(pointer)
        assert service.usage(db)["state"] == "deleted"
        assert service.usage(db)["s3_objects_page"] == 0

        queued = service.catalog.create("queued", ["CREATE TABLE kv(k TEXT PRIMARY KEY)"])["id"]
        service.catalog.request_deletion(queued)
        record, etag = service.catalog._load(queued)
        record["delete_after"] = 0
        raw.put(service.catalog._key(queued), json.dumps(record, sort_keys=True,
                separators=(",", ":")).encode(), if_match=etag)
        for _ in range(12):
            sweep = service.catalog.sweep_deletions(page_size=50)
            assert sweep["scanned"] >= 1 or sweep["wrapped"]
            if service.catalog.deletion_status(queued)["state"] == "deleted":
                break
        assert service.catalog.deletion_status(queued)["state"] == "deleted"
        assert fenced.pointer_retired(f"turso-native/cloud-{queued}/manifest.json")

        versioned = service.catalog.create("versioned", ["CREATE TABLE kv(k TEXT)"])["id"]
        service.catalog.request_deletion(versioned)
        record, etag = service.catalog._load(versioned)
        record["delete_after"] = 0
        raw.put(service.catalog._key(versioned), json.dumps(record, sort_keys=True,
                separators=(",", ":")).encode(), if_match=etag)
        s3.put_bucket_versioning(Bucket="tessera-purge-test",
                                 VersioningConfiguration={"Status": "Enabled"})
        with pytest.raises(RuntimeError, match="unversioned"):
            service.catalog.purge_step(versioned)
        assert not fenced.pointer_retired(f"turso-native/cloud-{versioned}/manifest.json")


def test_pending_deletion_recovers_crash_before_catalog_change(monkeypatch):
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3 = session.client("s3")
        s3.create_bucket(Bucket="tessera-deletion-queue-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"},
                                  {"AttributeName": "deletion_shard", "AttributeType": "S"},
                                  {"AttributeName": "deletion_due", "AttributeType": "S"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "pending-deletions",
                "KeySchema": [{"AttributeName": "deletion_shard", "KeyType": "HASH"},
                              {"AttributeName": "deletion_due", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-deletion-queue-test", "leases", s3_client=s3,
                              dynamodb=dynamodb, backoff_max_attempts=1)
        service = DatabaseService(FencedBackend(raw, session.client("dynamodb")),
                                  admin_token=ADMIN, catalog_backend=raw)
        with patch.object(raw, "list_page", side_effect=AssertionError("idle S3 LIST")):
            assert service.catalog.sweep_pending_deletions()["attempted"] == 0
        db = service.catalog.create("queued", ["CREATE TABLE kv(k TEXT PRIMARY KEY)"])["id"]
        clock = [1000.0]
        monkeypatch.setattr("tessera.experimental.cloud.service.time.time", lambda: clock[0])
        original_put = raw.put

        def interrupted_put(key, *args, **kwargs):
            if key == service.catalog._key(db) and kwargs.get("if_match"):
                raise OSError("crash before catalog CAS")
            return original_put(key, *args, **kwargs)

        with (patch.object(raw, "put", side_effect=interrupted_put),
              pytest.raises(OSError, match="crash before catalog")):
            service.catalog.request_deletion(db)
        assert service.catalog.get(db)["state"] == "active"
        assert service.catalog.sweep_pending_deletions()["attempted"] == 1
        status = service.catalog.deletion_status(db)
        assert status["state"] == "deleting" and status["delete_after"] == 1060.0
        assert service.catalog.sweep_pending_deletions()["attempted"] == 0
        clock[0] = 1061.0
        with patch.object(service.catalog, "purge_step", side_effect=OSError("lost reply")):
            assert service.catalog.sweep_pending_deletions()["failed"] == 1
        assert session.client("dynamodb").get_item(
            TableName="leases", Key={"tenant_id": {"S": "_tessera_cloud/deletion/" + db}}
        )["Item"]["deletion_due"]["S"].startswith("0000001121000")
        clock[0] = 1122.0
        for _ in range(8):
            service.catalog.sweep_pending_deletions()
            if service.catalog.deletion_status(db)["state"] == "deleted":
                break
            clock[0] += 61
        assert service.catalog.deletion_status(db)["state"] == "deleted"
        assert service.catalog.sweep_pending_deletions()["attempted"] == 0
        item = session.client("dynamodb").get_item(
            TableName="leases", Key={"tenant_id": {"S": "_tessera_cloud/deletion/" + db}})
        assert "Item" not in item


def test_pending_deletion_rotates_first_shard_under_backlog(tmp_path, monkeypatch):
    queried = []

    class Client:
        def query(self, **arguments):
            queried.append(arguments["ExpressionAttributeValues"][":shard"]["S"])
            return {"Items": []}

    class QueueBackend:
        client = Client()
        table_name = "coordination"

    catalog = DatabaseCatalog(LocalBackend(tmp_path / "catalog"),
                              data_backend=QueueBackend())
    clock = [0.0]
    monkeypatch.setattr("tessera.experimental.cloud.service.time.time", lambda: clock[0])
    for minute in range(4):
        clock[0] = minute * 60.0
        assert catalog.sweep_pending_deletions()["attempted"] == 0
    assert [queried[index:index + 4] for index in range(0, 16, 4)] == [
        ["0", "1", "2", "3"], ["1", "2", "3", "0"],
        ["2", "3", "0", "1"], ["3", "0", "1", "2"]]


def test_pending_deletion_reports_visible_lag_without_extra_queries(tmp_path, monkeypatch):
    deleted = []

    def item(prefix, due_ms):
        database_id = prefix + "0" * 30
        return {"tenant_id": {"S": "_tessera_cloud/deletion/" + database_id},
                "deletion_due": {"S": f"{due_ms:013d}#{database_id}"}}

    class Client:
        def query(self, **arguments):
            shard = arguments["ExpressionAttributeValues"][":shard"]["S"]
            if shard == "0":
                return {"Items": [item("00", 100_000), item("04", 150_000)],
                        "LastEvaluatedKey": {"tenant_id": {"S": "more"}}}
            if shard == "1":
                return {"Items": [item("01", 120_000)]}
            return {"Items": []}

        def delete_item(self, **arguments):
            deleted.append(arguments)

    class QueueBackend:
        client = Client()
        table_name = "coordination"

    catalog = DatabaseCatalog(LocalBackend(tmp_path / "catalog"),
                              data_backend=QueueBackend())
    monkeypatch.setattr("tessera.experimental.cloud.service.time.time", lambda: 200.0)
    monkeypatch.setattr(catalog, "deletion_status",
                        lambda database_id: {"id": database_id, "state": "deleted"})
    result = catalog.sweep_pending_deletions()
    assert result == {"attempted": 3, "completed": 3, "failed": 0,
                      "queried_shards": 4, "sampled_due": 3,
                      "truncated_shards": [0], "oldest_visible_due_age_s": 100.0}
    assert len(deleted) == 3


def test_pending_deletion_malformed_due_does_not_break_lag_sample(tmp_path, monkeypatch):
    class Client:
        def query(self, **arguments):
            if arguments["ExpressionAttributeValues"][":shard"]["S"] == "0":
                return {"Items": [{"tenant_id": {"S": "_tessera_cloud/deletion/" + "0" * 32},
                                   "deletion_due": {"S": "0000000000000-bad"}}]}
            return {"Items": []}

        def delete_item(self, **_arguments):
            return {}

    class QueueBackend:
        client = Client()
        table_name = "coordination"

    catalog = DatabaseCatalog(LocalBackend(tmp_path / "catalog"),
                              data_backend=QueueBackend())
    monkeypatch.setattr("tessera.experimental.cloud.service.time.time", lambda: 200.0)
    monkeypatch.setattr(catalog, "deletion_status",
                        lambda database_id: {"id": database_id, "state": "deleted"})
    result = catalog.sweep_pending_deletions()
    assert result["completed"] == 1
    assert result["oldest_visible_due_age_s"] is None


def test_catalog_sweeps_do_not_wrap_after_a_short_nonempty_s3_page(monkeypatch):
    keys = [f"_tessera_cloud/catalog/{number:032x}.json" for number in range(2)]

    class Backend:
        def list_page(self, _prefix, _limit, *, start_after=None):
            following = [key for key in keys if start_after is None or key > start_after]
            return [(following[0], 1)] if following else []

    class Client:
        def __init__(self):
            self.items = {}

        def get_item(self, *, Key, **_kwargs):
            item = self.items.get(Key["tenant_id"]["S"])
            return {"Item": item} if item else {}

        def put_item(self, *, Item, **_kwargs):
            self.items[Item["tenant_id"]["S"]] = Item

    class DataBackend:
        client = Client()
        table_name = "coordination"

    catalog = DatabaseCatalog(Backend(), data_backend=DataBackend())
    monkeypatch.setattr(catalog, "deletion_status",
                        lambda database_id: {"id": database_id, "state": "active"})
    monkeypatch.setattr(catalog, "_load", lambda _database_id: ({"state": "active"}, "etag"))
    monkeypatch.setattr(catalog, "retention_step",
                        lambda _database_id, **_kwargs: {
                            "phase": "history_overflow", "deleted": 0, "bytes": 0})
    deletion = [catalog.sweep_deletions() for _ in range(3)]
    retention = [catalog.sweep_retention() for _ in range(3)]
    assert [(item["scanned"], item["wrapped"]) for item in deletion] == [
        (1, False), (1, False), (0, True)]
    assert [(item["scanned"], item["wrapped"]) for item in retention] == [
        (1, False), (1, False), (0, True)]
    assert [item["history_overflow"] for item in retention] == [1, 1, 0]


def test_history_and_usage_continue_after_short_s3_pages():
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        session.client("s3").create_bucket(Bucket="tessera-short-pages-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-short-pages-test", "leases",
                              s3_client=session.client("s3"), dynamodb=dynamodb)
        service = DatabaseService(FencedBackend(raw, session.client("dynamodb")),
                                  admin_token=ADMIN, catalog_backend=raw)
        database_id = service.catalog.create(
            "paged", ["CREATE TABLE kv(k INTEGER PRIMARY KEY)"])["id"]
        for number in (1, 2):
            service.write(database_id, f"op-{number}",
                          [["INSERT INTO kv VALUES(?)", [number]]])
        raw.put(service.catalog._key("0" * 32), b'{"state":"deleted"}',
                if_none_match=True)
        original = raw.list_page

        def short_page(prefix, limit=1000, *, start_after=None):
            return original(prefix, min(limit, 1), start_after=start_after)

        with patch.object(raw, "list_page", side_effect=short_page):
            catalog = []
            cursor = None
            for _ in range(5):
                page = service.catalog.list_page(cursor=cursor)
                catalog.extend(page["databases"])
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            assert [item["id"] for item in catalog] == [database_id]
            assert cursor is None

            history = []
            cursor = None
            for _ in range(5):
                page = service.catalog.history_page(database_id, cursor=cursor)
                history.extend(page["retained_versions"])
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            assert history == [0, 1] and cursor is None

            count = 0
            cursor = None
            for _ in range(30):
                page = service.usage(database_id, cursor=cursor)
                count += page["s3_objects_page"]
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            assert count == len(raw.list(f"turso-native/cloud-{database_id}/"))
            assert cursor is None


def test_scheduled_retention_resumes_after_cursor_write_failure(monkeypatch):
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend
    from tessera.experimental.turso.lifecycle import RetentionPolicy

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3 = session.client("s3")
        s3.create_bucket(Bucket="tessera-retention-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        raw = S3DynamoBackend("tessera-retention-test", "leases", s3_client=s3,
                              dynamodb=dynamodb, backoff_max_attempts=1)
        fenced = FencedBackend(raw, session.client("dynamodb"))
        service = DatabaseService(fenced, admin_token=ADMIN, catalog_backend=raw)
        db = service.catalog.create("retain", ["CREATE TABLE kv(k TEXT PRIMARY KEY)"])["id"]
        neighbor = service.catalog.create("neighbor", ["CREATE TABLE kv(k TEXT)"])["id"]
        orphan = f"turso-native/cloud-{db}/images/" + "f" * 32 + ".db"
        raw.put(orphan, b"unpublished")
        policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                                 artifact_grace_seconds=0)
        service.catalog.retention_policy = policy
        monkeypatch.setattr("tessera.experimental.cloud.service.BatchOwner",
                            lambda *_args, **_kwargs: pytest.fail("GC restored SQLite"))
        original = service.catalog._save_retention_cursor
        interrupted = False

        def fail_once(key, previous, existed, cursor):
            nonlocal interrupted
            if key.endswith(db) and not interrupted:
                interrupted = True
                raise OSError("cursor write interrupted")
            return original(key, previous, existed, cursor)

        monkeypatch.setattr(service.catalog, "_save_retention_cursor", fail_once)
        first = service.catalog.sweep_retention(page_size=5)
        assert first["scanned"] == 2 and first["failed"] == 1
        assert raw.head(orphan) is None
        assert service.catalog._retention_cursor("_tessera_cloud/retention/" + db) == ("", False)
        monkeypatch.setattr(service.catalog, "_save_retention_cursor", original)
        second = service.catalog.sweep_retention(page_size=5)
        assert second["scanned"] == 0 and second["wrapped"]
        third = service.catalog.sweep_retention(page_size=5)
        assert third["scanned"] == 2 and third["failed"] == 0
        assert service.catalog._retention_cursor("_tessera_cloud/retention/" + db)[1]
        assert service.read(db, "SELECT count(*) FROM kv") == [(0,)]
        assert service.read(neighbor, "SELECT count(*) FROM kv") == [(0,)]
        assert service.catalog.sweep_retention(page_size=5)["wrapped"]
        page_one = service.catalog.sweep_retention(page_size=1)
        first_cursor = service.catalog._retention_cursor(
            "_tessera_cloud/retention-sweep")[0]
        restarted = DatabaseService(fenced, admin_token=ADMIN, catalog_backend=raw)
        page_two = restarted.catalog.sweep_retention(page_size=1)
        second_cursor = restarted.catalog._retention_cursor(
            "_tessera_cloud/retention-sweep")[0]
        assert page_one["scanned"] == page_two["scanned"] == 1
        assert first_cursor != second_cursor
        s3.put_bucket_versioning(Bucket="tessera-retention-test",
                                 VersioningConfiguration={"Status": "Enabled"})
        with pytest.raises(RuntimeError, match="unversioned"):
            restarted.catalog.retention_step(db)
        assert restarted.read(db, "SELECT count(*) FROM kv") == [(0,)]


def test_opt_in_native_gc_scheduler_completes_after_metadata_overflow(monkeypatch):
    import time

    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend
    from tessera.experimental.turso.gc_mark import _read_state
    from tessera.experimental.turso.lifecycle import (
        LifecycleManager,
        RetentionPolicy,
        RetentionSession,
    )

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        session.client("s3").create_bucket(Bucket="tessera-native-scheduler-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-native-scheduler-test", "leases",
                              s3_client=session.client("s3"), dynamodb=dynamodb,
                              backoff_max_attempts=1)
        fenced = FencedBackend(raw, session.client("dynamodb"))
        policy = RetentionPolicy(keep_last=4, retain_seconds=604800,
                                 artifact_grace_seconds=0)
        service = DatabaseService(fenced, admin_token=ADMIN, catalog_backend=raw,
                                  retention_policy=policy, native_gc_enabled=True,
                                  retention_metadata_limit=2)
        database_id = service.catalog.create(
            "large", ["CREATE TABLE kv(k INTEGER PRIMARY KEY)"])["id"]
        for number in range(1, 5):
            service.write(database_id, f"op-{number}",
                          [["INSERT INTO kv VALUES(?)", [number]]])
        group = "cloud-" + database_id
        orphan = f"turso-native/{group}/images/" + "f" * 32 + ".db"
        raw.put(orphan, b"unpublished")
        time.sleep(1.1)
        original_cycle = LifecycleManager.native_gc_cycle_step

        def one_step_per_visit(self, **kwargs):
            return original_cycle(self, max_steps=1, **kwargs)

        monkeypatch.setattr(LifecycleManager, "native_gc_cycle_step",
                            one_step_per_visit)
        first = service.catalog.retention_step(database_id)
        assert first["phase"] == "native_gc" and first["stage"] == "history"
        initial_cycle = _read_state(fenced, group)[0]["cycle_id"]
        service.write(database_id, "op-5", [["INSERT INTO kv VALUES(?)", [5]]])
        phases = [first["phase"]]
        for _ in range(20):
            result = service.catalog.retention_step(database_id)
            phases.append(result["phase"])
            if result["phase"] == "native_gc" and result["stage"] == "complete":
                break
        else:
            raise AssertionError("scheduled native GC did not finish")
        assert len(phases) > 5 and set(phases) == {"native_gc"}
        assert raw.head(orphan) is None
        final_state = _read_state(fenced, group)[0]
        assert final_state["phase"] == "complete"
        assert final_state["cycle_id"] != initial_cycle
        restarted = DatabaseService(fenced, admin_token=ADMIN, catalog_backend=raw,
                                    retention_policy=policy, native_gc_enabled=True,
                                    retention_metadata_limit=2)
        assert restarted.read(database_id, "SELECT count(*) FROM kv") == [(5,)]
        old_cycle = final_state["cycle_id"]
        old_mark_prefix = f"turso-native/{group}/gc-mark/{old_cycle}/"
        old_index_prefix = f"turso-native/{group}/gc-index/{old_cycle}/"
        assert raw.list(old_mark_prefix) and raw.list(old_index_prefix)
        with RetentionSession(fenced, group, policy=policy) as lifecycle:
            assert lifecycle.incremental_mark_step(restart=True)["restarted"]
        original_save = restarted.catalog._save_retention_cursor
        interrupted_cleanup = False

        def fail_cleanup_cursor_once(key, previous, existed, cursor):
            nonlocal interrupted_cleanup
            if key.endswith(database_id) and "native-gc-cleanup" in key \
                    and not interrupted_cleanup:
                interrupted_cleanup = True
                raise OSError("cleanup cursor write interrupted")
            return original_save(key, previous, existed, cursor)

        monkeypatch.setattr(restarted.catalog, "_save_retention_cursor",
                            fail_cleanup_cursor_once)
        for _ in range(40):
            try:
                result = restarted.catalog.retention_step(database_id)
            except OSError as exc:
                assert str(exc) == "cleanup cursor write interrupted"
                continue
            if result.get("cleanup_phase") == "complete":
                break
        else:
            raise AssertionError("scheduled inactive-cycle cleanup did not finish")
        assert interrupted_cleanup
        assert raw.list(old_mark_prefix) == []
        assert raw.list(old_index_prefix) == []
        active = _read_state(fenced, group)[0]["cycle_id"]
        assert active != old_cycle
        assert raw.list(f"turso-native/{group}/gc-mark/{active}/")
        assert restarted.catalog.retention_step(database_id)["cleanup_phase"] == "waiting"
        assert restarted.read(database_id, "SELECT count(*) FROM kv") == [(5,)]
