"""Single-host shared journal spike. Not safe for multi-host/NFS use or AZ loss."""

import base64
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import threading

ZERO = "0" * 64
MAX_PACK = 8 * 1024 * 1024


# @spec journal/conflict
class Conflict(ValueError):
    pass


# @spec journal/corrupt
class Corrupt(ValueError):
    pass


# @spec journal/encode
def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
# @spec journal/mutation
class Mutation:
    group: str
    operation_id: str
    epoch: int
    expected_version: int
    payload: bytes

    def __post_init__(self):
        if (not isinstance(self.group, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.group)
                or not isinstance(self.operation_id, str) or not 1 <= len(self.operation_id) <= 128
                or type(self.epoch) is not int or self.epoch < 1
                or type(self.expected_version) is not int or self.expected_version < 0
                or not isinstance(self.payload, bytes) or len(self.payload) > 65536):
            raise ValueError("invalid journal mutation")


# @spec journal/journal
class Journal:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = self._broken = False
        self.sync = os.fsync
        self.hook = lambda stage: None  # fault injection only
        self._fd = self._guard = None
        self.epochs, self.versions, self.receipts = {}, {}, {}
        self.end, self.hash = 0, ZERO
        try:
            self._guard = os.open(self.root / "writer.lock", os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self._guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.root / "journal.bin"
            exists = path.exists()
            if not exists and (self.root / "head.json").exists():
                raise Corrupt("committed journal file is missing")
            self._fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            if not exists:
                self.sync(self._fd)
                self._head(0, ZERO)
            self._recover()
        except BaseException:
            self.close()
            raise

    def _head(self, end, digest):
        with (self.root / "head.tmp").open("wb") as stream:
            stream.write(encode(dict(format="shared-journal-1", end=end, hash=digest)))
            stream.flush()
            self.sync(stream.fileno())
        os.replace(self.root / "head.tmp", self.root / "head.json")
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            self.sync(directory)
        finally:
            os.close(directory)

    def _recover(self):
        try:
            head = json.loads((self.root / "head.json").read_bytes())
            if (head["format"] != "shared-journal-1" or type(head["end"]) is not int
                    or head["end"] < 0 or head["end"] > os.fstat(self._fd).st_size):
                raise ValueError("invalid committed boundary")
            with os.fdopen(os.dup(self._fd), "rb") as stream:
                while self.end < head["end"]:
                    raw = stream.read(36)
                    if len(raw) != 36:
                        raise ValueError("short header")
                    size, expected = struct.unpack(">I32s", raw)
                    if size > MAX_PACK or self.end + 36 + size > head["end"]:
                        raise ValueError("invalid frame length")
                    body = stream.read(size)
                    digest = hashlib.sha256(body).digest()
                    if digest != expected:
                        raise ValueError("frame hash mismatch")
                    pack = json.loads(body)
                    if pack["previous"] != self.hash:
                        raise ValueError("broken hash chain")
                    self._apply(pack["entries"], digest.hex())
                    self.end += 36 + size
                    self.hash = digest.hex()
            if self.hash != head["hash"]:
                raise ValueError("committed hash mismatch")
            os.ftruncate(self._fd, self.end)
            self.sync(self._fd)
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise Corrupt("cannot recover committed journal") from exc

    def _assert_current_head(self):
        if self._closed or self._broken:
            raise RuntimeError("journal closed or recovery required")
        try:
            head = json.loads((self.root / "head.json").read_bytes())
            if (head["format"] != "shared-journal-1"
                    or type(head["end"]) is not int or head["end"] < 0
                    or not isinstance(head["hash"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", head["hash"])):
                raise ValueError("invalid committed head")
        except (OSError, KeyError, TypeError, ValueError) as exc:
            self._broken = True
            raise Corrupt("cannot verify committed head") from exc
        if head["end"] != self.end or head["hash"] != self.hash:
            self._broken = True
            raise Conflict("stale journal handle")

    def _apply(self, entries, pack_hash):
        for entry in entries:
            group = entry["group"]
            if entry["kind"] == "claim":
                self.epochs[group] = entry["epoch"]
            else:
                self.versions[group] = entry["version"]
                self.receipts[(group, entry["id"])] = dict(version=entry["version"], pack=pack_hash,
                                                          sha256=entry["sha256"], payload=entry["payload"])

    def _persist(self, entries):
        if self._closed or self._broken:
            raise RuntimeError("journal closed or recovery required")
        body = encode(dict(previous=self.hash, entries=entries))
        if len(body) > MAX_PACK:
            raise ValueError("pack too large")
        digest = hashlib.sha256(body).digest()
        frame = struct.pack(">I32s", len(body), digest) + body
        try:
            os.lseek(self._fd, self.end, os.SEEK_SET)
            remaining = memoryview(frame)
            while remaining:
                written = os.write(self._fd, remaining)
                if written <= 0:
                    raise OSError("short journal write")
                remaining = remaining[written:]
            self.sync(self._fd)
            self.hook("log_synced")
            self._head(self.end + len(frame), digest.hex())
            self.hook("head_synced")
        except BaseException:
            self._broken = True
            raise
        self.end += len(frame)
        self.hash = digest.hex()
        self._apply(entries, self.hash)

    def claim(self, group, epoch):
        self.claim_many([(group, epoch)])

    def claim_many(self, claims):
        claims = list(claims)
        if not 1 <= len(claims) <= 64:
            raise ValueError("claim pack requires 1..64 groups")
        for group, epoch in claims:
            Mutation(group, "validation", epoch, 0, b"")
        if len({group for group, _ in claims}) != len(claims):
            raise ValueError("claim pack contains duplicate groups")
        with self._lock:
            self._assert_current_head()
            if any(epoch <= self.epochs.get(group, 0) for group, epoch in claims):
                raise Conflict("every epoch must increase")
            self._persist([dict(kind="claim", group=group, epoch=epoch)
                           for group, epoch in claims])

    def append(self, requests):
        requests = list(requests)
        if not 1 <= len(requests) <= 64 or not all(isinstance(r, Mutation) for r in requests):
            raise ValueError("pack requires 1..64 mutations")
        with self._lock:
            self._assert_current_head()
            entries, versions, seen = [], dict(self.versions), {}
            for r in requests:
                key = (r.group, r.operation_id)
                digest = hashlib.sha256(r.payload).hexdigest()
                old = self.receipts.get(key) or seen.get(key)
                if old:
                    if old["sha256"] != digest:
                        raise Conflict("operation ID changed payload")
                    continue
                if self.epochs.get(r.group) != r.epoch or versions.get(r.group, 0) != r.expected_version:
                    raise Conflict("stale owner or group version")
                versions[r.group] = r.expected_version + 1
                entry = dict(kind="mutation", group=r.group, id=r.operation_id, epoch=r.epoch,
                             version=versions[r.group], sha256=digest,
                             payload=base64.b64encode(r.payload).decode())
                seen[key] = entry
                entries.append(entry)
            if entries:
                self._persist(entries)
            return [dict(self.receipts[(r.group, r.operation_id)]) for r in requests]

    def lookup(self, group, operation_id):
        with self._lock:
            result = self.receipts.get((group, operation_id))
            return None if result is None else dict(result)

    def scan(self, group):
        """Return a stable, version-ordered copy for local recovery."""
        if not isinstance(group, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", group):
            raise ValueError("invalid group")
        with self._lock:
            if self._closed or self._broken:
                raise RuntimeError("journal closed or recovery required")
            found = []
            for (record_group, operation_id), receipt in self.receipts.items():
                if record_group == group:
                    found.append(dict(receipt, operation_id=operation_id))
            return sorted(found, key=lambda item: item["version"])

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                for fd in (self._fd, self._guard):
                    if fd is not None:
                        os.close(fd)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
