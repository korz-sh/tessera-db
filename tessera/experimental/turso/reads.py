"""Pinned, verified and locally cached published-frontier reads."""

from contextlib import closing, contextmanager
import json
from pathlib import Path
import shutil
import tempfile
import threading
import time
import uuid

from .api import Receipt, check_sql
from .snapshot import InvalidSnapshot, canonical, connect, digest


# @spec turso/published-cache-corrupt
class PublishedCacheCorrupt(InvalidSnapshot):
    pass


# @spec turso/remote-frontier-corrupt
class RemoteFrontierCorrupt(InvalidSnapshot):
    pass


# @spec turso/published-version-unavailable
class PublishedVersionUnavailable(TimeoutError):
    pass


_CACHE_MANIFEST = ".published-manifest.json"


def _receipt(conn, operation_id):
    row = conn.execute(
        "SELECT digest,version,result FROM _tessera_receipts WHERE id=?",
        (operation_id,),
    ).fetchone()
    return None if row is None else Receipt(operation_id, row[0], row[1], json.loads(row[2]))


# @spec turso/published-reader
class PublishedReader:
    def __init__(self, storage, *, cache_root=None, poll_ms=10,
                 max_cache_bytes=None):
        if type(poll_ms) is not int or not 1 <= poll_ms <= 1000:
            raise ValueError("read poll interval must be 1..1000 ms")
        if max_cache_bytes is not None and (type(max_cache_bytes) is not int
                                            or max_cache_bytes < 0):
            raise ValueError("read cache budget must be nonnegative or None")
        self.storage = storage
        self.max_cache_bytes = max_cache_bytes
        self._temporary = None
        if cache_root is None:
            self._temporary = tempfile.TemporaryDirectory(prefix="turso-read-cache-")
            cache_root = self._temporary.name
        self.cache_root = Path(cache_root)
        self.poll = poll_ms / 1000
        self._lock = threading.RLock()
        self._metrics = {
            "cold_builds": 0,
            "incremental_builds": 0,
            "incremental_fallbacks": 0,
            "warm_opens": 0,
            "local_corruptions": 0,
            "remote_corruptions": 0,
        }

    def _increment(self, name):
        with self._lock:
            self._metrics[name] += 1

    def _manifest(self):
        try:
            return self.storage.manifest()
        except InvalidSnapshot as exc:
            self._increment("remote_corruptions")
            raise RemoteFrontierCorrupt("remote published manifest is corrupt") from exc

    def _frontier(self, *, min_version=None, receipt=None, timeout=None):
        if min_version is not None and (type(min_version) is not int or min_version < 0):
            raise ValueError("min_version must be a nonnegative integer or None")
        if receipt is not None and not isinstance(receipt, Receipt):
            raise TypeError("receipt must be a Receipt or None")
        if receipt is not None:
            if min_version is not None and min_version != receipt.version:
                raise ValueError("min_version and receipt version must agree")
            min_version = receipt.version
        target = 0 if min_version is None else min_version
        if (timeout is not None
                and (not isinstance(timeout, (int, float))
                     or isinstance(timeout, bool) or timeout < 0)):
            raise ValueError("timeout must be nonnegative or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            manifest, etag = self._manifest()
            if manifest["version"] >= target:
                return manifest, etag
            if deadline is not None and time.monotonic() >= deadline:
                raise PublishedVersionUnavailable(
                    f"published version {target} is not visible before the deadline"
                )
            delay = self.poll if deadline is None else min(self.poll, deadline - time.monotonic())
            if delay > 0:
                time.sleep(delay)

    def _entry(self, manifest):
        return self.cache_root / digest(canonical(manifest))

    @staticmethod
    def _copy_entry(entry, destination):
        for source in entry.iterdir():
            if source.is_file() and source.name != _CACHE_MANIFEST:
                shutil.copy2(source, destination / source.name)

    def _validate(self, root, manifest, error_type):
        path = root / "db.sqlite"
        try:
            with closing(connect(path)) as conn:
                self.storage.validate(conn, manifest)
        except Exception as exc:
            raise error_type("published frontier failed integrity validation") from exc

    def _build(self, manifest, entry):
        self.cache_root.mkdir(parents=True, exist_ok=True)
        old_entries = ([old for old in self.cache_root.iterdir()
                        if old.is_dir() and old != entry]
                       if self.max_cache_bytes is not None else [])
        build = self.cache_root / (".building-" + uuid.uuid4().hex)
        build.mkdir()
        try:
            extended = False
            if len(old_entries) == 1 and hasattr(self.storage, "extend_restored"):
                old = old_entries[0]
                try:
                    previous = json.loads((old / _CACHE_MANIFEST).read_bytes())
                    extended = self.storage.extend_restored(
                        previous, old / "db.sqlite", manifest, build / "db.sqlite")
                    if extended:
                        self._validate(build, manifest, RemoteFrontierCorrupt)
                except (InvalidSnapshot, FileNotFoundError, ValueError, TypeError):
                    # The cached base may be corrupt or the remote suffix may
                    # be gone. Rebuild and validate the full published frontier.
                    self._increment("incremental_fallbacks")
                    shutil.rmtree(build)
                    build.mkdir()
                    extended = False
            try:
                if not extended:
                    self.storage.restore_manifest(manifest, build / "db.sqlite")
                    self._validate(build, manifest, RemoteFrontierCorrupt)
            except InvalidSnapshot as exc:
                self._increment("remote_corruptions")
                if isinstance(exc, RemoteFrontierCorrupt):
                    raise
                raise RemoteFrontierCorrupt(
                    "remote published artifacts are corrupt"
                ) from exc
            if self.max_cache_bytes is not None:
                (build / _CACHE_MANIFEST).write_bytes(canonical(manifest))
                for old in old_entries:
                    shutil.rmtree(old)
            try:
                build.rename(entry)
            except FileExistsError:
                shutil.rmtree(build)
            self._increment("cold_builds")
            if extended:
                self._increment("incremental_builds")
        except BaseException:
            shutil.rmtree(build, ignore_errors=True)
            raise

    @contextmanager
    def open(self, *, min_version=None, receipt=None, timeout=None):
        # A GC can retire artifacts from an old frontier after a concurrent
        # writer has published a newer pointer. Re-read the pointer and start
        # over only in that case; a missing artifact in the *current* frontier
        # remains a corruption error. Never replay a caller's SQL callback.
        for attempt in range(3):
            manifest, etag = self._frontier(
                min_version=min_version, receipt=receipt, timeout=timeout
            )
            entry = self._entry(manifest)
            with tempfile.TemporaryDirectory(prefix="turso-pinned-read-") as root:
                isolated = Path(root)
                try:
                    with self._lock:
                        self.cache_root.mkdir(parents=True, exist_ok=True)
                        if not entry.exists():
                            self._build(manifest, entry)
                        try:
                            self._copy_entry(entry, isolated)
                            self._validate(isolated, manifest, PublishedCacheCorrupt)
                            self._increment("warm_opens")
                        except PublishedCacheCorrupt:
                            self._increment("local_corruptions")
                            shutil.rmtree(entry, ignore_errors=True)
                            self._build(manifest, entry)
                            self._copy_entry(entry, isolated)
                            self._validate(isolated, manifest, PublishedCacheCorrupt)
                        if self.max_cache_bytes is not None:
                            cached_bytes = sum(path.stat().st_size for path in entry.iterdir()
                                               if path.is_file())
                            if cached_bytes > self.max_cache_bytes:
                                shutil.rmtree(entry, ignore_errors=True)
                except RemoteFrontierCorrupt:
                    if attempt < 2 and self._manifest()[1] != etag:
                        continue
                    raise
                with closing(connect(isolated / "db.sqlite")) as conn:
                    self.storage.validate(conn, manifest)
                    if receipt is not None and _receipt(conn, receipt.operation_id) != receipt:
                        raise PublishedVersionUnavailable(
                            "receipt is not present at the selected published frontier"
                        )
                    yield conn, manifest
                    return

    def read(self, sql, params=(), **frontier):
        check_sql(sql, read=True)
        with self.open(**frontier) as (conn, _):
            return conn.execute(sql, params).fetchall()

    def lookup(self, operation_id):
        with self.open() as (conn, _):
            return _receipt(conn, operation_id)

    def metrics(self):
        with self._lock:
            return dict(self._metrics)

    def close(self):
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None
