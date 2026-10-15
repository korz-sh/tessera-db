# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Hot-owner process for tessera-db (M8).

The *hot owner* is a long-lived process that holds ONE tenant's lease for its
whole life and serves that tenant's writes over HTTP from a warm local SQLite
file — the opposite trade-off of :class:`~tessera.store.Store`, which
acquires a lease and hydrates per request:

- **Indefinite lease**: :meth:`Owner.start` acquires the tenant lease once
  (full-jitter backoff while the previous holder exits) and the handle's
  renewal thread keeps it alive for as long as the process runs. The lease
  ``endpoint`` records the owner's HTTP address, so a dispatcher can discover
  where the tenant's writes live.
- **Warm HTTP**: one :class:`sqlite3.Connection` is opened on the
  hydrated local database at boot and kept open; ``POST /write`` and
  ``POST /read`` hit the warm file with zero acquire/hydration work per
  request (warm restarts rehydrate with zero downloads via the
  ``tessera.local`` markers).
- **Durable publication**: in the default ``per_commit`` mode each write
  uploads its committed WAL frames and CAS-publishes a new manifest
  before HTTP acknowledgement. ``windowed`` mode acknowledges after the
  local commit and folds every commit inside ``publish_every_s`` into ONE
  publish — fewer backend ops per write, at the cost of a bounded
  durability window (acks ahead of ``published`` die with the local WAL).
  The /write receipt exposes the contract: ``version`` is the committed
  sequence, ``published`` the floor guaranteed durable in object storage;
  ``sync: true`` or ``POST /flush`` closes the window on demand.
  After each successful publish the WAL is checkpointed
  back into ``db.sqlite`` (single connection, lock held), so the next publish
  captures only NEW frames and the local file keeps matching the committed
  manifest.
- **Idle shutdown**: after ``idle_s`` without writes (and nothing
  pending) the owner stops serving, releases the lease and exits cleanly
  (``start()`` returns ``True``).
- **Fencing**: a handle that stops being ``"alive"``, or a lost manifest CAS
  (``if_match``), stops serving immediately (writes -> 409, stopped owner ->
  503) and exits with a fencing-loss result (``start()`` returns ``False``).
  The local database is never allowed to drift ahead of the committed
  manifest: teardown discards the local directory whenever unpublished local
  commits would otherwise be checkpointed into ``db.sqlite`` (the same
  invariant Store's CAS-failure path enforces with ``rmtree``).

HTTP surface (JSON; response shapes mirror ``examples/handler.py``):

- ``POST /write`` ``{"sql", "params", "sync"?}`` -> ``{"result": {"lastrowid",
  "rowcount"}, "version": committed_seq, "published": durable_floor}``
- ``POST /flush`` ``{}`` -> ``{"version": committed_seq, "published":
  durable_floor}`` (publishes pending commits first)
- ``POST /read`` ``{"sql", "params"}`` -> ``{"rows", "columns", "version",
  "published"}``
- ``GET /health`` -> ``{"tenant", "epoch", "version", "published",
  "writes_pending", "idle_for_s"}``

Exit codes: ``start()`` returns ``True`` for a clean exit (idle timeout or a
``stop()`` request — process exit code 0) and ``False`` for fencing loss
(process exit code 1); an embedder running the owner as a binary maps the
boolean directly.
"""

from ..delta import read_wal_frames
from .core import Owner

__all__ = ["Owner"]
