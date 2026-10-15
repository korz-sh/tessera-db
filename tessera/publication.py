"""Shared predecessor preservation and conditional publication protocol."""

from collections.abc import Callable

from .backends.base import Backend, BackendError, CASFailed
from .manifest import Manifest

__all__ = [
    "HistoryCollisionError",
    "archive_predecessor",
    "cas_manifest",
    "publish_manifest",
]


# @spec tessera/history-collision-error
class HistoryCollisionError(BackendError):
    """Different bytes already sit under a history archive version.

    Real fencing, not a transient: someone else published that version —
    the owner retries nothing and fences immediately.
    """


# @spec tessera/archive-predecessor
def archive_predecessor(backend: Backend, tenant: str, previous: Manifest) -> None:
    """Preserve the predecessor manifest as ``history/<v>.json`` before CAS.

    Create-only and content-checked: an existing archive with the same
    bytes reconciles (an earlier attempt that died mid-publish left it),
    different bytes are a real collision. Independent of every other
    object upload — callers may run it concurrently with the delta PUT.
    """
    history_key = f"{tenant}/history/{previous.version}.json"
    payload = previous.to_json().encode("utf-8")
    try:
        backend.put(history_key, payload, if_none_match=True)
    except CASFailed:
        existing, _ = backend.get(history_key)
        if existing != payload:
            raise HistoryCollisionError(
                f"history collision for {history_key!r}"
            ) from None


# @spec tessera/cas-manifest
def cas_manifest(
    backend: Backend, tenant: str, new: Manifest, etag: str,
    *, before_cas: Callable[[], None] | None = None,
) -> str:
    """CAS the new manifest over *etag*; return the committed etag."""
    if before_cas is not None:
        before_cas()
    return backend.put(
        f"{tenant}/manifest.json", new.to_json().encode("utf-8"), if_match=etag
    )


# @spec tessera/publish-manifest
def publish_manifest(
    backend: Backend, tenant: str, previous: Manifest, new: Manifest, etag: str,
    *, before_cas: Callable[[], None] | None = None,
) -> str:
    """Preserve the predecessor before CAS; return the committed object's etag.

    Callers own lease fencing and immutable data uploads. A failed CAS may
    leave the current version archived; no uncommitted version is archived.
    Archive mtimes are approximate, not authoritative commit timestamps.
    """
    archive_predecessor(backend, tenant, previous)
    return cas_manifest(backend, tenant, new, etag, before_cas=before_cas)
