# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""High-level lease handle with background renewal for tessera-db.

:class:`LeaseHandle` wraps one acquired backend lease and keeps it alive:

- A daemon renewal thread re-renews the lease on a ``ttl/3`` cadence, capped
  so a sleep never lands past the skew-adjusted expiry.
- Skew tolerance: the lease counts as expired 1 s *before* its ``expires_at``
  so modest clock skew between writers cannot produce two holders that both
  believe they are exclusive. :meth:`LeaseHandle.ensure_alive` is the fencing
  gate M2's store calls before every manifest CAS.
- :meth:`LeaseHandle.release` is idempotent and safe to call from a context
  manager exit.

The ``epoch`` carried by the underlying :class:`Lease` is the monotonic
fencing token; this module only manages liveness and fencing around it.
"""

import logging
import threading
import time
from typing import Literal

from .backends.base import Backend, Lease, LeaseLost

logger = logging.getLogger(__name__)

__all__ = ["Lease", "LeaseHandle", "LeaseLost"]

# A lease is considered expired this many seconds before its expires_at, so
# small clock skew between concurrent writers can never yield two holders
# that both pass the fencing check.
_SKEW_S = 1.0

# Floor for the renewal thread's wait: if the skew deadline is already in the
# past we degrade to a fast poll instead of a hot loop.
_MIN_WAIT_S = 0.05


# @spec tessera/lease-handle
class LeaseHandle:
    """Owns one backend lease plus the daemon thread that renews it.

    Create via :meth:`acquire`. State machine::

        alive --release()--> released
        alive --renewal failure / skew expiry--> lost

    ``released`` and ``lost`` are terminal; neither transitions back to
    ``alive``. All state/lease mutations happen under one lock, shared with
    the renewal thread, so ``renew()``/``release()`` never race it.
    """

    def __init__(
        self,
        backend: Backend,
        lease: Lease,
        ttl_ms: int,
        owner: str | None = None,
    ) -> None:
        self._backend = backend
        self._ttl_ms = int(ttl_ms)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._state: str = "alive"
        if owner is not None:
            # Optional sugar: record the caller's owner label on the handle
            # instead of the backend-assigned token. Renewals talk to the
            # backend with this stored lease, so owner-keyed backends may
            # refuse them — the parameter exists for record-keeping only.
            lease = Lease(
                tenant=lease.tenant,
                owner=owner,
                epoch=lease.epoch,
                expires_at=lease.expires_at,
                endpoint=lease.endpoint,
            )
        self._lease = lease
        self._thread = threading.Thread(
            target=self._renewal_loop,
            name=f"tessera-lease-{lease.tenant}",
            daemon=True,
        )
        self._thread.start()

    @classmethod
    def acquire(
        cls,
        backend: Backend,
        tenant: str,
        ttl_ms: int,
        owner: str | None = None,
        endpoint: str = "",
        session_token: str = "",
        min_epoch: int = 0,
    ) -> "LeaseHandle":
        """Acquire *tenant*'s lease for ``ttl_ms`` and start renewing it.

        ``endpoint`` and ``session_token`` are forwarded to the backend and
        recorded in the lease — the lease is the channel through which a
        client discovers both WHERE to talk to the owner and WHICH
        credential to use. Raises :class:`TenantBusy` (via the backend) if
        another owner holds the lease.

        ``min_epoch`` is a FLOOR for the issued epoch: it covers the case
        where the lease item disappeared (DynamoDB TTL sweeper) and the
        backend would restart numbering from zero.
        """
        lease = backend.acquire_lease(
            tenant,
            ttl_ms,
            endpoint=endpoint,
            session_token=session_token,
            min_epoch=min_epoch,
        )
        return cls(backend, lease, ttl_ms, owner=owner)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def state(self) -> Literal["alive", "lost", "released"]:
        """Current state: ``"alive"``, ``"lost"`` or ``"released"``."""
        with self._lock:
            return self._state

    @property
    def lease(self) -> Lease:
        """The current :class:`Lease` record (refreshed by renewals)."""
        with self._lock:
            return self._lease

    def __repr__(self) -> str:
        lease = self.lease
        return (
            f"<LeaseHandle tenant={lease.tenant!r} epoch={lease.epoch} "
            f"state={self.state!r}>"
        )

    # ------------------------------------------------------------------
    # Fencing / renewal
    # ------------------------------------------------------------------

    def ensure_alive(self) -> None:
        """Raise :class:`LeaseLost` unless the lease is provably still ours.

        Fails when the handle is not ``"alive"``, or when the skew-adjusted
        clock says the lease has expired (``now >= expires_at - 1 s``). In the
        latter case the state moves to ``"lost"`` first: a handle that cannot
        prove exclusivity must never fence a manifest write.
        """
        with self._lock:
            if self._state != "alive":
                raise LeaseLost(
                    f"lease for tenant {self._lease.tenant!r} is "
                    f"{self._state!r}, not alive"
                )
            if time.time() >= self._lease.expires_at - _SKEW_S:
                self._state = "lost"
                raise LeaseLost(
                    f"lease for tenant {self._lease.tenant!r} expired at or "
                    f"before {self._lease.expires_at - _SKEW_S:.3f} (skew margin)"
                )

    def renew(self) -> "LeaseHandle":
        """Renew the lease immediately in the background of the expiry clock.

        Success refreshes the stored :class:`Lease`. A :class:`LeaseLost`
        means the backend evaluated the condition and the lease is provably
        gone — the handle is marked ``"lost"``. Any *other* backend
        exception is transient (the condition was never evaluated): it is
        logged, retried on the next tick, and does NOT mark the handle;
        ``ensure_alive()`` remains the single fencing gate and stops
        serving exactly at the skew-adjusted expiry. Renewing a handle that
        is already ``"lost"``/``"released"`` raises :class:`LeaseLost`.
        """
        with self._lock:
            if self._state != "alive":
                raise LeaseLost(
                    f"lease for tenant {self._lease.tenant!r} is "
                    f"{self._state!r}, not alive"
                )
            lease = self._lease
        try:
            renewed = self._backend.renew_lease(lease, self._ttl_ms)
        except LeaseLost:
            # The condition was evaluated and failed: the lease expired or
            # another owner holds it. That is a real fencing loss.
            with self._lock:
                if self._state == "alive":  # don't clobber a concurrent release()
                    self._state = "lost"
            return self
        except Exception:
            # Transport error, throttle or 5xx: the condition was never
            # evaluated, so this proves nothing about ownership. A single
            # blip must not kill a healthy owner — stay alive and let the
            # next renewal retry; ``ensure_alive`` still fences the moment
            # the local clock crosses ``expires_at - _SKEW_S``.
            logger.warning(
                "lease renewal for %r failed transiently; will retry",
                lease.tenant, exc_info=True,
            )
            return self
        with self._lock:
            if self._state == "alive":  # same epoch, extended expires_at
                self._lease = renewed
        return self

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def release(self) -> None:
        """Stop the renewal thread and release the backend lease. Idempotent.

        A :class:`LeaseLost` from the backend (lease already lost) is
        swallowed: releasing a lost lease is a no-op, and the handle still
        ends in ``"released"``. Other backend errors propagate.
        """
        with self._lock:
            if self._state == "released":
                return
            self._state = "released"  # stops the renewal loop at its next check
        self._stop.set()
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        with self._lock:
            lease = self._lease
        try:
            self._backend.release_lease(lease)
        except LeaseLost:
            pass

    def __enter__(self) -> "LeaseHandle":
        if self.state != "alive":
            raise LeaseLost(
                f"lease for tenant {self._lease.tenant!r} is "
                f"{self.state!r}, not alive"
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            self.release()
        except Exception:
            # Best-effort teardown: never let lease cleanup mask the
            # with-block outcome (or raise from __exit__ at all).
            pass
        return False

    # ------------------------------------------------------------------
    # Background renewal thread
    # ------------------------------------------------------------------

    def _renewal_loop(self) -> None:
        while True:
            with self._lock:
                if self._state != "alive":
                    return
                lease = self._lease
            # Full cadence is ttl/3; never sleep past the skew-adjusted
            # expiry (floored so a past deadline polls instead of spinning).
            wait_s = min(
                self._ttl_ms / 3000.0,
                max(_MIN_WAIT_S, lease.expires_at - _SKEW_S - time.time()),
            )
            if self._stop.wait(wait_s):
                return
            with self._lock:
                if self._state != "alive":
                    return
                lease = self._lease
            try:
                renewed = self._backend.renew_lease(lease, self._ttl_ms)
            except LeaseLost:
                with self._lock:
                    if self._state == "alive":
                        self._state = "lost"
                return
            except Exception:
                # Transient backend failure: nothing learned about
                # ownership. Retry next tick — the stale ``expires_at``
                # shrinks ``wait_s`` on its own, so retries accelerate as
                # the skew deadline approaches, and ``ensure_alive`` fences
                # precisely when ownership can no longer be proven.
                logger.warning(
                    "lease renewal for %r failed transiently; will retry",
                    lease.tenant, exc_info=True,
                )
                continue
            with self._lock:
                if self._state != "alive":
                    return
                self._lease = renewed
