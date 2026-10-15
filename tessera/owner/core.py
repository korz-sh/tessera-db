# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Owner core - construction and introspection; behavior lives in the
mixins this class composes (lifecycle, publish)."""

import sqlite3
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

from ..backends.base import Backend
from ..lease import LeaseHandle
from ..store import Store
from .lifecycle import LifecycleMixin
from .publish import PublishMixin


# @spec owner/owner
class Owner(LifecycleMixin, PublishMixin):
    """Hot owner of ONE tenant: indefinite lease, warm HTTP, batched publish.

    Args:
        tenant: tenant to own (must already exist — the manifest is fetched
            at boot; a missing manifest raises :class:`TenantNotFound`).
        backend: object store + lease manager
            (:class:`~tessera.backends.base.Backend`).
        http_host: bind address of the HTTP server.
        http_port: bind port; ``0`` picks a free port (exposed as
            :attr:`http_port` after :meth:`start` binds).
        ttl_ms: lease TTL in milliseconds (>= 3000 recommended; the lease
            counts as expired 1 s early for clock-skew safety).
        publish_every_s: cadence of the batched delta publish while dirty.
        publish_mode: ``"per_commit"`` (default) publishes synchronously on
            every /write before acknowledging; ``"windowed"`` acknowledges
            after the local commit and folds all commits inside
            ``publish_every_s`` into one publish — cheaper backend ops per
            write, at the cost of a bounded durability window (acked
            commits are lost if the owner's local WAL is discarded before
            the next publish). ``sync: true`` on /write or ``POST /flush``
            forces an immediate publish in windowed mode.
        idle_s: shut down cleanly after this many seconds without writes.
        compact_every_s: cadence of the background incremental-compaction
            check (format-2 tenants only).
        root: local cache root; the tenant database hydrates to
            ``<root>/<tenant>/db.sqlite`` and a :class:`~tessera.store.Store`
            for compaction shares the same root.
    """

    def __init__(
        self,
        tenant: str,
        backend: Backend,
        *,
        http_host: str = "127.0.0.1",
        http_port: int = 0,
        advertise_host: str | None = None,
        ttl_ms: int = 30000,
        publish_every_s: float = 1.0,
        publish_mode: str = "per_commit",
        idle_s: float = 300.0,
        compact_every_s: float = 60.0,
        acquire_wait_s: float = 0.0,
        publish_error_budget_s: float = 30.0,
        root: str | Path = "/tmp/tessera-owner",
    ) -> None:
        self._tenant = tenant
        self._backend = backend
        self._http_host = http_host
        # Host published in the lease ``endpoint``. Binding to 0.0.0.0 (a
        # container behind an awsvpc ENI) makes the BIND host useless for
        # dispatchers — they need the routable address of this task.
        self._advertise_host = advertise_host or http_host
        self._http_port_requested = int(http_port)
        self._ttl_ms = int(ttl_ms)
        self._publish_every_s = float(publish_every_s)
        if publish_mode not in ("per_commit", "windowed"):
            raise ValueError(
                f"publish_mode must be 'per_commit' or 'windowed', "
                f"got {publish_mode!r}"
            )
        self._publish_mode = publish_mode
        self._idle_s = float(idle_s)
        self._compact_every_s = float(compact_every_s)
        self._acquire_wait_s = float(acquire_wait_s)
        # How long a publish retries transient backend errors before
        # fencing (issue #149). Sized like the lease TTL: an outage longer
        # than this kills the renewal thread anyway, so a bigger budget
        # could never be spent.
        self._publish_error_budget_s = float(publish_error_budget_s)
        self._root = Path(root)

        # Publish/serve state — every field below is guarded by _lock.
        self._lock = threading.Lock()
        self._serving = False
        self._fenced = False
        self._dirty = False
        self._last_write = time.monotonic()
        self._last_published_version = 0
        # Committed-write sequence vs the floor guaranteed durable in
        # object storage; both seed from the boot manifest version so
        # ``version`` in the /write response stays in the same range the
        # manifest exposes. version > published => durability window open.
        self._commit_seq = 0
        self._published_commit_seq = 0
        self._seq = 0  # next delta seq is _seq + 1 (per (tenant, epoch))
        self._epoch = 0
        self._session_token = ""
        # Published manifest head, cached after every successful CAS:
        # lets the publish path skip the manifest GET entirely. A stale
        # entry only loses the CAS (the conditional write still validates)
        # and falls back to refetch; compaction invalidates it.
        self._cached_manifest = None
        self._cached_etag = ""

        # Lifecycle.
        self._stop_event = threading.Event()
        self._loop_done = threading.Event()
        self._stop_publish = True

        # Resources owned by start().
        self._handle: LeaseHandle | None = None
        self._store: Store | None = None
        self._conn: sqlite3.Connection | None = None
        self._dest: Path | None = None
        self._server: ThreadingHTTPServer | None = None
        self._http_port_value = int(http_port)
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"<Owner tenant={self._tenant!r} port={self._http_port_value} "
            f"serving={self._serving} fenced={self._fenced}>"
        )

    @property
    def tenant(self) -> str:
        """The owned tenant."""
        return self._tenant

    @property
    def http_port(self) -> int:
        """Bound HTTP port (real port once the server is bound)."""
        return self._http_port_value

    @property
    def epoch(self) -> int:
        """Fencing epoch of the lease this owner holds (0 before start)."""
        return self._epoch

    @property
    def session_token(self) -> str:
        """Credential this owner requires in ``Authorization: Bearer``.

        Generated at boot and published in the lease item, where clients
        that forward writes read it from. Empty before ``start()``.
        """
        return self._session_token

    @property
    def last_published_version(self) -> int:
        """Manifest version of the last successful publish (locked read)."""
        with self._lock:
            return self._last_published_version

    @property
    def serving(self) -> bool:
        """True while the owner accepts writes (locked read)."""
        with self._lock:
            return self._serving
