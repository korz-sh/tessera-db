# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Store exceptions - shared across all store submodules."""

from ..backends.base import BackendError


# @spec store/tenant-exists
class TenantExists(BackendError):
    """Raised when creating a tenant that already has a manifest."""


# @spec store/tenant-not-found
class TenantNotFound(BackendError):
    """Raised when the tenant has no manifest (never created or deleted)."""


# @spec store/conflict
class Conflict(BackendError):
    """Raised when a write loses a race: a stale lease epoch, a delta-key
    collision, or a failed manifest CAS. The local copy is discarded and the
    caller's transaction function is never retried."""


# @spec store/stale-read
class StaleRead(BackendError):
    """Raised when a read with ``min_version`` did not observe that version
    within ``read_wait_ms``."""


# @spec store/version-unavailable
class VersionUnavailable(BackendError):
    """The requested version is no longer recoverable: GC collected it.

    Deliberately distinct from ``TenantNotFound``: the tenant exists, but
    that point in time does not. The message says which is the oldest
    available version, because at 3 AM what you need to know is how far
    back you CAN go.
    """
