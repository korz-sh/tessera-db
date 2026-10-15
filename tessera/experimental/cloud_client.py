"""Small HTTP SQL client for the experimental AWS BYOC service.

Writes require caller-owned operation IDs. A lost response is never retried
with a new identity: inspect the receipt or resend the identical request.
"""

from __future__ import annotations

import json
from hashlib import sha256
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


_opener = build_opener(_NoRedirect())


def _open(request, timeout):
    return _opener.open(request, timeout=timeout)


# @spec experimental/cloud-api-error
class CloudApiError(RuntimeError):
    def __init__(self, status, code):
        self.status = status
        self.code = code
        super().__init__(f"TesseraDB HTTP {status}: {code}")


# @spec experimental/cloud-transport-error
class CloudTransportError(RuntimeError):
    """No valid HTTP response was received for a read or receipt lookup."""


# @spec experimental/cloud-protocol-error
class CloudProtocolError(RuntimeError):
    """The service returned a malformed response."""


# @spec experimental/cloud-write-outcome-unknown
class CloudWriteOutcomeUnknown(RuntimeError):
    """The write may have committed; reconcile this exact operation ID."""

    def __init__(self, operation_id):
        self.operation_id = operation_id
        super().__init__(
            f"write outcome unknown for {operation_id!r}; look up its receipt "
            "or retry the identical operation ID and payload")


# @spec experimental/cloud-sql-client
class CloudSqlClient:
    """Connect one scoped token to one TesseraDB database over HTTPS."""

    def __init__(self, api_url, database_id, token, *, timeout=30,
                 allow_insecure_localhost=False):
        parsed = urlsplit(api_url)
        if (parsed.scheme != "https" and not (
                allow_insecure_localhost and parsed.scheme == "http"
                and parsed.hostname in {"localhost", "127.0.0.1", "::1"})):
            raise ValueError("api_url must use HTTPS (or explicitly allowed local HTTP)")
        if (not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in ("", "/")
                or parsed.query or parsed.fragment):
            raise ValueError("api_url must be an origin without credentials or a path")
        if (not isinstance(database_id, str) or len(database_id) != 32
                or any(character not in "0123456789abcdef" for character in database_id)):
            raise ValueError("database_id must be 32 lowercase hex characters")
        if (not isinstance(token, str) or not token
                or any(character.isspace() for character in token)):
            raise ValueError("token must be a nonempty header-safe string")
        if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
                or not 0 < timeout <= 300):
            raise ValueError("timeout must be between 0 and 300 seconds")
        self.api_url = api_url.rstrip("/")
        self.database_id = database_id
        self._token = token
        self.timeout = timeout

    @staticmethod
    def _operation_id(value):
        if not isinstance(value, str) or not 1 <= len(value) <= 128:
            raise ValueError("operation_id must be 1..128 characters")
        return value

    @staticmethod
    def fingerprint(statements):
        """Return the service's canonical transaction fingerprint for reconciliation."""
        if not isinstance(statements, (list, tuple)):
            raise ValueError("statements must be a list or tuple")
        try:
            payload = json.dumps(list(statements), sort_keys=True,
                                 separators=(",", ":"), allow_nan=False).encode()
        except (TypeError, ValueError) as exc:
            raise ValueError("statements must be JSON serializable") from exc
        return sha256(payload).hexdigest()

    def _request(self, method, suffix, *, body=None, write_id=None):
        try:
            raw = None if body is None else json.dumps(
                body, separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("request body must be JSON serializable") from exc
        request = Request(
            self.api_url + f"/v1/databases/{self.database_id}" + suffix,
            data=raw, method=method,
            headers={"Authorization": "Bearer " + self._token,
                     "Content-Type": "application/json"})
        try:
            with _open(request, timeout=self.timeout) as response:
                status, payload = response.status, response.read(1_048_577)
        except HTTPError as exc:
            try:
                status, payload = exc.code, exc.read(1_048_577)
            except (URLError, TimeoutError, OSError, HTTPException) as read_error:
                if write_id is not None:
                    raise CloudWriteOutcomeUnknown(write_id) from read_error
                raise CloudTransportError("TesseraDB transport failed") from read_error
        except (URLError, TimeoutError, OSError, HTTPException) as exc:
            if write_id is not None:
                raise CloudWriteOutcomeUnknown(write_id) from exc
            raise CloudTransportError("TesseraDB transport failed") from exc
        if len(payload) > 1_048_576:
            if write_id is not None:
                raise CloudWriteOutcomeUnknown(write_id)
            raise CloudProtocolError("response exceeds 1 MiB")
        try:
            result = json.loads(payload)
            if not isinstance(result, dict):
                raise ValueError("JSON object required")
        except (UnicodeError, ValueError) as exc:
            if write_id is not None:
                raise CloudWriteOutcomeUnknown(write_id) from exc
            raise CloudProtocolError("invalid JSON response") from exc
        if write_id is not None and (status in (202, 408, 429) or 300 <= status < 400
                                    or status >= 500):
            raise CloudWriteOutcomeUnknown(write_id)
        if status != 200:
            raise CloudApiError(status, result.get("error", "unexpected_response"))
        return result

    def write(self, operation_id, statements):
        """Execute one atomic transaction and return its durable receipt.

        On CloudWriteOutcomeUnknown, call lookup(operation_id), then retry only
        this same operation ID and the same statements if reconciliation needs it.
        """
        operation_id = self._operation_id(operation_id)
        expected_fingerprint = self.fingerprint(statements)
        result = self._request(
            "POST", "/transactions",
            body={"operation_id": operation_id, "statements": statements},
            write_id=operation_id)
        if (result.get("operation_id") != operation_id
                or type(result.get("version")) is not int
                or result["version"] < 0
                or result.get("fingerprint") != expected_fingerprint
                or not isinstance(result.get("rows"), list)):
            raise CloudWriteOutcomeUnknown(operation_id)
        return result

    def read(self, sql, params=(), *, min_version=None):
        body = {"sql": sql, "params": params}
        if min_version is not None:
            if type(min_version) is not int or min_version < 0:
                raise ValueError("min_version must be a nonnegative integer")
            body["min_version"] = min_version
        result = self._request("POST", "/query", body=body)
        if not isinstance(result.get("rows"), list):
            raise CloudProtocolError("query response has no rows list")
        return result["rows"]

    def lookup(self, operation_id):
        operation_id = self._operation_id(operation_id)
        result = self._request(
            "GET", "/operations/" + quote(operation_id, safe=""))
        if (result.get("operation_id") != operation_id
                or result.get("state") not in {"absent", "published"}):
            raise CloudProtocolError("invalid receipt response")
        if (result["state"] == "published"
                and (type(result.get("version")) is not int
                     or result["version"] < 0)):
            raise CloudProtocolError("published receipt has no version")
        return result
