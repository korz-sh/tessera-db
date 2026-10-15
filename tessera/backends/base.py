# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Object-storage backend contract for tessera-db.

A backend stores one SQLite DB's objects per tenant over an object store.
Per tenant there is exactly ONE mutable object (the manifest), always written
with CAS (if-match); every other object is write-once. Backends also provide
crash-safe per-tenant leases used to fence concurrent writers.

The S3 + DynamoDB backend will implement this same ABC.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

__all__ = [
    "Backend",
    "BackendError",
    "CASFailed",
    "Lease",
    "LeaseLost",
    "NotFound",
    "TenantBusy",
]


# @spec backends/backend-error
class BackendError(Exception):
    """Base class for all object-storage backend errors."""


# @spec backends/not-found
class NotFound(BackendError):
    """Raised by :meth:`Backend.get` when the key does not exist."""


# @spec backends/casfailed
class CASFailed(BackendError):
    """Raised by :meth:`Backend.put` when a conditional write fails.

    Covers both a mismatching ``if_match`` etag and ``if_none_match`` on an
    existing key. The future S3 backend maps HTTP 412 to this.
    """


# @spec backends/tenant-busy
class TenantBusy(BackendError):
    """Raised by :meth:`Backend.acquire_lease` when another owner holds the lease."""


# @spec backends/lease-lost
class LeaseLost(BackendError):
    """Raised by :meth:`Backend.renew_lease`/:meth:`Backend.release_lease` when
    the caller no longer owns the lease (released, stolen, or process died)."""


@dataclass
# @spec backends/lease
class Lease:
    """A granted per-tenant lease.

    tenant:     tenant identifier the lease fences.
    owner:      opaque token unique to one acquire (uuid4 hex locally).
    epoch:      monotonically increasing counter per tenant; increments on
                every acquire, so stale holders are detectable by epoch.
    expires_at: unix seconds after which the lease is considered expired.
    endpoint:   optional endpoint URL recorded with the lease ("" when unset).
    session_token: session secret the owner publishes next to the endpoint
                ("" when no owner is serving). The lease is already an
                IAM-authenticated channel: whoever can read it is authorized
                to operate the tenant, so it doubles as the distribution
                channel for the owner's credential with zero provisioning.
    """

    tenant: str
    owner: str
    epoch: int
    expires_at: float
    endpoint: str = ""
    session_token: str = ""


# @spec backends/backend
class Backend(ABC):
    """Abstract object store + lease manager.

    Object model: keys are strings using "/" separators; values are immutable
    byte blobs addressed by content etag. The manifest is the only mutable
    object and must be written with :meth:`put` CAS conditions.
    """

    @abstractmethod
    def get(self, key: str) -> tuple[bytes, str]:
        """Return ``(data, etag)`` for *key*.

        Raises :class:`NotFound` if the key does not exist.
        """

    @abstractmethod
    def head(self, key: str) -> str | None:
        """Return the etag of *key*, or ``None`` if it does not exist."""

    @abstractmethod
    def put(
        self,
        key: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> str:
        """Store *data* at *key* and return the new etag.

        Conditions (evaluated against the current object):
        - ``if_match`` given: fail with :class:`CASFailed` unless the current
          etag equals it (a missing object never matches).
        - ``if_none_match=True``: fail with :class:`CASFailed` if the key
          already exists (create-only).
        - ``if_match=None, if_none_match=False``: unconditional overwrite.

        The condition check and the write are atomic with respect to other
        CAS puts on the same key: exactly one concurrent CAS put wins.
        """

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete *key*. Idempotent: deleting a missing key is a no-op."""

    supports_fenced_gc = False

    def gc_delete(self, key: str, lease: Lease) -> None:
        """Delete *key* under the GC fencing contract; fail by default.

        An implementation must guarantee a delete can never remove an
        object a successor manifest could reference. Two mechanisms
        satisfy it:

        - **Atomic exclusion** (LocalBackend): deletion is serialized
          with lease release/acquisition — no successor can interleave.
        - **Skew-bounded deletion on a complete mark** (S3DynamoBackend):
          the mark phase runs under the lease over the full closure of
          committed manifests, and every delete is issued at least the
          skew margin before the lease could be taken over, so it lands
          before the earliest possible successor publication. A TTL/expiry
          check is sufficient BECAUSE the mark is complete; a plain TTL
          check on an incomplete mark is not.
        """
        raise BackendError("backend cannot fence GC deletion; use dry_run=True")

    def gc_delete_batch(self, keys: list[str], lease: Lease) -> None:
        """Delete a batch under the same fencing contract as gc_delete.

        The default loops per key; backends with a bulk-delete API
        override it (a batch must still satisfy the fencing contract —
        one ensure_alive per batch is the caller's responsibility).
        """
        for key in keys:
            self.gc_delete(key, lease)

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """Return all keys starting with *prefix*, sorted lexicographically."""

    def mtime(self, key: str) -> float | None:
        """Return the object's last-modified time as unix seconds, or ``None``.

        Contract: ``None`` means unknown or missing. Conservative callers
        (age-based garbage collection) must skip deletion of objects with an
        unknown mtime rather than guess an age.
        """
        return None

    @abstractmethod
    def acquire_lease(
        self,
        tenant: str,
        ttl_ms: int,
        endpoint: str = "",
        session_token: str = "",
        min_epoch: int = 0,
    ) -> Lease:
        """Acquire the exclusive lease for *tenant*, held for ``ttl_ms``.

        ``endpoint`` is an optional endpoint URL recorded with the lease
        state (``""`` records none).

        ``min_epoch`` is a FLOOR for the issued epoch, not a value: the
        result is always greater. It exists because the lease state can
        DISAPPEAR (DynamoDB's TTL sweeper removing an idle tenant's item),
        and without a floor the numbering would return to 1 — fencing
        assumes the epoch never goes down. The caller passes the manifest's
        ``last_epoch``, the highest epoch that was actually committed.
        Implementations that cannot lose their state must still honor it.

        Raises :class:`TenantBusy` if another owner currently holds it. The returned :class:`Lease` must be passed
        back to :meth:`renew_lease`/:meth:`release_lease` by the same backend
        handle.
        """

    @abstractmethod
    def renew_lease(self, lease: Lease, ttl_ms: int) -> Lease:
        """Extend *lease* by ``ttl_ms`` and return the updated :class:`Lease`.

        The epoch is preserved. Raises :class:`LeaseLost` if the caller no
        longer owns the lease.
        """

    @abstractmethod
    def release_lease(self, lease: Lease) -> None:
        """Release *lease* early. Idempotent for the same owner.

        Raises :class:`LeaseLost` if the caller no longer owns the lease.
        """
