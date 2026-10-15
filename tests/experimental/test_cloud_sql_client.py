"""A client must never turn an uncertain write into a new SQL operation."""

from io import BytesIO
from threading import Thread
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError

import pytest

from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService, serve
from tessera.experimental.cloud_client import (
    CloudApiError,
    CloudSqlClient,
    CloudWriteOutcomeUnknown,
)


ADMIN = "a" * 48


@pytest.fixture
def cloud(tmp_path):
    service = DatabaseService(LocalBackend(tmp_path / "objects"), admin_token=ADMIN)
    database_id = service.catalog.create(
        "client", ["CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)"])["id"]
    token = service.catalog.issue_token(database_id).secret
    server = serve(service, port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = CloudSqlClient(
        f"http://127.0.0.1:{server.server_address[1]}", database_id, token,
        allow_insecure_localhost=True)
    try:
        yield service, client
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_client_write_read_lookup_and_same_id_replay(cloud):
    _, client = cloud
    statements = [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]]
    receipt = client.write("insert-one", statements)
    assert receipt["version"] == 1
    assert client.write("insert-one", statements) == receipt
    assert client.lookup("insert-one")["state"] == "published"
    assert client.read("SELECT v FROM kv WHERE k=?", [1],
                       min_version=receipt["version"]) == [["one"]]
    with pytest.raises(CloudApiError) as exc:
        client.write("insert-one", [["INSERT INTO kv VALUES(?, ?)", [2, "two"]]])
    assert exc.value.status == 422
    assert client.read("SELECT count(*) FROM kv") == [[1]]


def test_lost_write_response_is_unknown_and_reconciles_by_same_id(cloud, monkeypatch):
    import tessera.experimental.cloud_client as client_module

    _, client = cloud
    original = client_module._open
    calls = []

    def lose_response(request, timeout):
        calls.append(request)
        with original(request, timeout=timeout) as response:
            assert response.status == 200
            response.read()
        raise TimeoutError("response lost after server commit")

    statements = [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]]
    monkeypatch.setattr(client_module, "_open", lose_response)
    with pytest.raises(CloudWriteOutcomeUnknown) as exc:
        client.write("stable-write", statements)
    assert exc.value.operation_id == "stable-write"
    assert len(calls) == 1
    monkeypatch.setattr(client_module, "_open", original)
    assert client.lookup("stable-write")["state"] == "published"
    assert client.write("stable-write", statements)["version"] == 1
    assert client.read("SELECT count(*) FROM kv") == [[1]]


def test_server_failure_after_commit_is_unknown(cloud, monkeypatch):
    service, client = cloud
    original = service.write

    def commit_then_fail(*args):
        original(*args)
        raise RuntimeError("server reply failed after commit")

    monkeypatch.setattr(service, "write", commit_then_fail)
    with pytest.raises(CloudWriteOutcomeUnknown) as exc:
        client.write("uncertain-503", [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]])
    assert exc.value.operation_id == "uncertain-503"
    monkeypatch.setattr(service, "write", original)
    assert client.lookup("uncertain-503")["state"] == "published"
    assert client.read("SELECT count(*) FROM kv") == [[1]]


def test_mismatched_ack_fingerprint_is_unknown_but_receipt_is_recoverable(cloud, monkeypatch):
    service, client = cloud
    original = service.write

    def commit_then_misreport(*args):
        receipt = original(*args)
        return {**receipt, "fingerprint": "0" * 64}

    monkeypatch.setattr(service, "write", commit_then_misreport)
    statements = [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]]
    with pytest.raises(CloudWriteOutcomeUnknown):
        client.write("mismatched-ack", statements)
    receipt = client.lookup("mismatched-ack")
    assert receipt["state"] == "published"
    assert receipt["fingerprint"] == client.fingerprint(statements)
    assert client.read("SELECT count(*) FROM kv") == [[1]]


@pytest.mark.parametrize("status", [408, 429])
def test_timeout_and_throttle_replies_leave_write_outcome_unknown(cloud, monkeypatch, status):
    import tessera.experimental.cloud_client as client_module

    _, client = cloud

    def reply_without_commit(request, timeout):
        raise HTTPError(request.full_url, status, "uncertain", {},
                        BytesIO(b'{"error":"uncertain"}'))

    monkeypatch.setattr(client_module, "_open", reply_without_commit)
    with pytest.raises(CloudWriteOutcomeUnknown) as exc:
        client.write("same-id", [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]])
    assert exc.value.operation_id == "same-id"


def test_client_requires_secure_origin_and_stable_operation_id():
    with pytest.raises(ValueError, match="HTTPS"):
        CloudSqlClient("http://example.com", "0" * 32, "token")
    with pytest.raises(ValueError, match="operation_id"):
        CloudSqlClient("https://example.com", "0" * 32, "token").write("", [])


def test_client_does_not_follow_redirect_with_bearer_token():
    received = []

    class Destination(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.end_headers()

        do_POST = do_GET

        def log_message(self, *_args):
            pass

    destination = ThreadingHTTPServer(("127.0.0.1", 0), Destination)
    destination_thread = Thread(target=destination.serve_forever, daemon=True)
    destination_thread.start()

    class Redirect(BaseHTTPRequestHandler):
        def do_POST(self):
            body = b'{"error":"redirect"}'
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{destination.server_port}/steal")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
    source_thread = Thread(target=source.serve_forever, daemon=True)
    source_thread.start()
    try:
        client = CloudSqlClient(
            f"http://127.0.0.1:{source.server_port}", "0" * 32,
            "secret-token", allow_insecure_localhost=True)
        with pytest.raises(CloudWriteOutcomeUnknown):
            client.write("redirected-write", [["SELECT 1", []]])
        assert received == []
    finally:
        source.shutdown()
        source.server_close()
        source_thread.join(timeout=5)
        destination.shutdown()
        destination.server_close()
        destination_thread.join(timeout=5)
