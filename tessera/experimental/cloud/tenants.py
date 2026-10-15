# SPDX-License-Identifier: Apache-2.0
"""CAS-backed tenant management and fail-closed database slot admission.

This is the control-plane primitive; HTTP routing and legacy catalog migration
are separate steps. SQL database tokens never use this catalog.
"""

from __future__ import annotations

import hmac
import json
import re
import secrets
from hashlib import sha256
from uuid import uuid4

from tessera.backends.base import CASFailed, NotFound

_PREFIX = "_tessera_cloud/tenants/"
_ID = re.compile(r"[0-9a-f]{32}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN = re.compile(r"tessera_tenant_([0-9a-f]{32})\.([A-Za-z0-9_-]{32,128})\Z")
DEFAULT_TENANT_LIMITS = {
    "max_databases": 32,
    "max_frontier_bytes": 1_073_741_824,
    "max_operation_bytes": 1_048_576,
    "max_tokens": 64,
}
DEFAULT_RATE_LIMITS = {
    "max_requests_per_minute": 300,
    "max_body_bytes_per_minute": 67_108_864,
}


# @spec cloud/tenant-conflict
class TenantConflict(RuntimeError):
    """The requested identity or catalog mutation conflicts with stored state."""


# @spec cloud/tenant-quota-exceeded
class TenantQuotaExceeded(RuntimeError):
    """An aggregate tenant limit rejected admission before database creation."""


def _document(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _limits(value):
    if value is None:
        return dict(DEFAULT_TENANT_LIMITS)
    if not isinstance(value, dict) or set(value) != set(DEFAULT_TENANT_LIMITS):
        raise ValueError("tenant limits require all four fields")
    bounds = {"max_databases": (1, 256),
              "max_frontier_bytes": (65_536, 1_073_741_824),
              "max_operation_bytes": (1_024, 8_388_608),
              "max_tokens": (1, 64)}
    for name, (minimum, maximum) in bounds.items():
        if type(value[name]) is not int or not minimum <= value[name] <= maximum:
            raise ValueError(f"{name} must be an integer from {minimum} to {maximum}")
    return dict(value)


def _rate_limits(value):
    if value is None:
        return dict(DEFAULT_RATE_LIMITS)
    if not isinstance(value, dict) or set(value) != set(DEFAULT_RATE_LIMITS):
        raise ValueError("rate limits require both fields")
    bounds = {"max_requests_per_minute": (1, 600),
              "max_body_bytes_per_minute": (1_024, 1_073_741_824)}
    for name, (minimum, maximum) in bounds.items():
        if type(value[name]) is not int or not minimum <= value[name] <= maximum:
            raise ValueError(f"{name} must be an integer from {minimum} to {maximum}")
    return dict(value)


# @spec cloud/tenant-catalog
class TenantCatalog:
    """One bounded mutable S3 object per tenant; CAS serializes reservations."""

    def __init__(self, backend, *, rate_meter=None):
        self.backend = backend
        self.rate_meter = rate_meter

    @staticmethod
    def _key(tenant_id):
        if not isinstance(tenant_id, str) or not _ID.fullmatch(tenant_id):
            raise ValueError("invalid tenant ID")
        return _PREFIX + tenant_id + ".json"

    def _load(self, tenant_id):
        body, etag = self.backend.get(self._key(tenant_id))
        record = json.loads(body)
        if not isinstance(record, dict) or record.get("id") != tenant_id:
            raise ValueError("invalid tenant catalog record")
        return record, etag

    def get(self, tenant_id):
        record, _ = self._load(tenant_id)
        return {"id": record["id"], "name": record["name"],
                "state": record["state"], "limits": _limits(record["limits"]),
                "rate_limits": _rate_limits(record.get("rate_limits")),
                "reserved_databases": len(record["reservations"]),
                "token_ids": sorted(record["tokens"])}

    def list_page(self, *, cursor=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        if cursor is not None and (not isinstance(cursor, str)
                                   or not cursor.startswith(_PREFIX)
                                   or len(cursor) > 1024):
            raise ValueError("invalid tenant cursor")
        paged = hasattr(self.backend, "list_page")
        if paged:
            keys = [key for key, _ in self.backend.list_page(
                _PREFIX, limit, start_after=cursor)]
        else:
            keys = [key for key in self.backend.list(_PREFIX)
                    if cursor is None or key > cursor][:limit + 1]
        selected = keys[:limit]
        records = []
        for key in selected:
            suffix = key[len(_PREFIX):]
            if suffix.endswith(".json") and _ID.fullmatch(suffix[:-5]):
                records.append(self.get(suffix[:-5]))
        return {"tenants": records,
                "next_cursor": (selected[-1] if selected and
                                (paged or len(keys) > limit) else None)}

    def create(self, name, *, request_id=None, limits=None, rate_limits=None):
        if not isinstance(name, str) or not 1 <= len(name) <= 128:
            raise ValueError("tenant name must be 1..128 characters")
        if request_id is not None and (not isinstance(request_id, str)
                                       or not 1 <= len(request_id) <= 128):
            raise ValueError("request_id must be 1..128 characters")
        parsed = _limits(limits)
        parsed_rate = _rate_limits(rate_limits)
        identity = {"name": name, "limits": parsed}
        # Preserve stable request IDs created before the rate-limit field existed.
        if parsed_rate != DEFAULT_RATE_LIMITS:
            identity["rate_limits"] = parsed_rate
        fingerprint = sha256(_document(identity)).hexdigest()
        tenant_id = (sha256(("tessera-tenant-v1:" + request_id).encode()).hexdigest()[:32]
                     if request_id is not None else uuid4().hex)
        record = {"id": tenant_id, "name": name, "state": "active",
                  "limits": parsed, "rate_limits": parsed_rate,
                  "fingerprint": fingerprint,
                  "tokens": {}, "reservations": {}}
        try:
            self.backend.put(self._key(tenant_id), _document(record), if_none_match=True)
        except CASFailed:
            if request_id is None:
                raise TenantConflict("generated tenant ID collision") from None
            existing, _ = self._load(tenant_id)
            if existing.get("fingerprint") != fingerprint:
                raise TenantConflict("request_id was used with different tenant content")
        return self.get(tenant_id)

    def admit_request(self, tenant_id, body_bytes, *, authorized_record=None):
        if self.rate_meter is None:
            return
        if authorized_record is None:
            record, _ = self._load(tenant_id)
        else:
            record = authorized_record
            if record["id"] != tenant_id:
                raise TenantConflict("tenant authorization record mismatch")
        if record["state"] != "active":
            raise TenantConflict("tenant is not active")
        self.rate_meter.admit(tenant_id, body_bytes,
                              _rate_limits(record.get("rate_limits")))

    def update_rate_limits(self, tenant_id, rate_limits):
        if rate_limits is None:
            raise ValueError("rate_limits object required")
        parsed = _rate_limits(rate_limits)

        def replace(record):
            if record["state"] != "active":
                raise TenantConflict("tenant is not active")
            record["rate_limits"] = parsed
            return dict(parsed)

        return self._change(tenant_id, replace)

    def _change(self, tenant_id, edit):
        for _ in range(16):
            record, etag = self._load(tenant_id)
            result = edit(record)
            try:
                self.backend.put(self._key(tenant_id), _document(record), if_match=etag)
            except CASFailed:
                continue
            return result
        raise TenantConflict("tenant catalog is busy; retry the same request")

    def issue_token(self, tenant_id):
        token_id = uuid4().hex[:16]
        secret = secrets.token_urlsafe(32)
        token = f"tessera_tenant_{tenant_id}.{secret}"
        digest = sha256(token.encode()).hexdigest()

        def add(record):
            if record["state"] != "active":
                raise TenantConflict("tenant is not active")
            if len(record["tokens"]) >= 8:
                raise TenantQuotaExceeded("tenant management token limit exceeded")
            record["tokens"][token_id] = digest

        self._change(tenant_id, add)
        return {"token_id": token_id, "token": token}

    def revoke_token(self, tenant_id, token_id):
        if not isinstance(token_id, str) or not re.fullmatch(r"[0-9a-f]{16}", token_id):
            raise ValueError("invalid tenant token ID")

        def remove(record):
            record["tokens"].pop(token_id, None)

        self._change(tenant_id, remove)

    def authorized_record(self, token):
        if not isinstance(token, str) or (match := _TOKEN.fullmatch(token)) is None:
            return None
        try:
            record, _ = self._load(match.group(1))
        except NotFound:
            return None
        if record["state"] != "active":
            return None
        digest = sha256(token.encode()).hexdigest()
        if any(hmac.compare_digest(digest, candidate)
               for candidate in record["tokens"].values()):
            return record
        return None

    def authorize(self, token):
        record = self.authorized_record(token)
        return None if record is None else record["id"]

    def validate_database_limits(self, tenant_id, database_limits):
        record, _ = self._load(tenant_id)
        if record["state"] != "active":
            raise TenantConflict("tenant is not active")
        if not isinstance(database_limits, dict):
            raise TypeError("database limits must be an object")
        for name in ("max_frontier_bytes", "max_operation_bytes", "max_tokens"):
            if name not in database_limits or type(database_limits[name]) is not int:
                raise ValueError(f"missing or invalid {name}")
            if database_limits[name] > record["limits"][name]:
                raise TenantQuotaExceeded(f"{name} exceeds tenant ceiling")

    def reserve_database(self, tenant_id, database_id, fingerprint):
        if not isinstance(database_id, str) or not _ID.fullmatch(database_id):
            raise ValueError("invalid database ID")
        if not isinstance(fingerprint, str) or not _DIGEST.fullmatch(fingerprint):
            raise ValueError("invalid creation fingerprint")

        def reserve(record):
            if record["state"] != "active":
                raise TenantConflict("tenant is not active")
            reservations = record["reservations"]
            existing = reservations.get(database_id)
            if existing is not None:
                if existing != fingerprint:
                    raise TenantConflict("database ID was reserved with different content")
                return False
            if len(reservations) >= record["limits"]["max_databases"]:
                raise TenantQuotaExceeded("tenant database limit exceeded")
            reservations[database_id] = fingerprint
            return True

        return self._change(tenant_id, reserve)

    def require_reservation(self, tenant_id, database_id, fingerprint):
        record, _ = self._load(tenant_id)
        if record["reservations"].get(database_id) != fingerprint:
            raise TenantConflict("database is missing its tenant reservation")

    def reserved_page(self, tenant_id, *, cursor=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        if cursor is not None and (not isinstance(cursor, str) or not _ID.fullmatch(cursor)):
            raise ValueError("invalid tenant database cursor")
        record, _ = self._load(tenant_id)
        ids = sorted(database_id for database_id in record["reservations"]
                     if cursor is None or database_id > cursor)
        selected = ids[:limit]
        return selected, selected[-1] if len(ids) > limit else None

    def _release_database_after_purge(self, tenant_id, database_id, fingerprint):
        """Caller must have verified deleted catalog state and retired pointer."""
        def release(record):
            current = record["reservations"].get(database_id)
            if current is None:
                return False
            if current != fingerprint:
                raise TenantConflict("database reservation fingerprint changed")
            del record["reservations"][database_id]
            return True

        return self._change(tenant_id, release)
