"""Cooperative serialization of mutable Store caches on local POSIX disks."""

from contextlib import contextmanager
from functools import wraps
from pathlib import Path
import fcntl
import logging
import os
import shutil
import threading
from weakref import WeakValueDictionary

from .local import _OWNER_MARKER


class _RootLock:
    def __init__(self):
        self.mutex = threading.RLock()
        self.depth = 0


_registry_mutex = threading.Lock()
_registry = WeakValueDictionary()


@contextmanager
# @spec tessera/cache-lock
def cache_lock(root: Path):
    root = root.resolve()
    with _registry_mutex:
        state = _registry.get(root)
        if state is None:
            state = _RootLock()
            _registry[root] = state
    # flock alone does not provide reentrancy across Store instances in the
    # same thread. Hold a shared RLock first, then one fd per outer operation.
    with state.mutex:
        if state.depth:
            state.depth += 1
            try:
                yield
            finally:
                state.depth -= 1
            return
        root.mkdir(parents=True, exist_ok=True)
        # Never unlink this file: waiters must keep locking the same inode.
        with (root / ".tessera-cache.lock").open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            lock.seek(0)
            if lock.read():
                # An interrupted operation may have committed SQLite pages
                # without publishing its manifest. These are disposable
                # caches, never authoritative data. Keep immutable chunks —
                # and owner-managed directories (``_OWNER_MARKER``): the hot
                # Owner's dir holds its open connection and unpublished WAL,
                # so it is NOT disposable and the owner alone decides its
                # lifecycle (hydrate at start, discard at teardown).
                for child in root.iterdir():
                    if child.name != "chunks" and child.is_dir():
                        if (child / _OWNER_MARKER).exists():
                            continue
                        if child.is_symlink():
                            child.unlink()
                        else:
                            shutil.rmtree(child)
            lock.seek(0)
            lock.truncate()
            lock.write(b"active\n")
            lock.flush()
            os.fsync(lock.fileno())
            state.depth = 1
            try:
                yield
            except BaseException:
                # Leave the marker for the next holder, including on a
                # normal exception whose local cleanup may have failed.
                raise
            else:
                try:
                    lock.seek(0)
                    lock.truncate()
                    lock.flush()
                    os.fsync(lock.fileno())
                except OSError:
                    # Cleanup must not erase an already published receipt.
                    logging.getLogger(__name__).exception("cache clean marker failed")
            finally:
                state.depth = 0
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


# @spec tessera/cache-access
def cache_access(fn):
    """Hold the cache lock across hydration, callbacks and cleanup."""
    @wraps(fn)
    def guarded(self, *args, **kwargs):
        with cache_lock(self._root):
            return fn(self, *args, **kwargs)
    return guarded
