"""Local object-store archive experiment; journal ACK does not depend on it."""

import hashlib
import json
import os
from pathlib import Path

from tessera import LocalBackend
from tessera.backends.base import NotFound
from .local import Corrupt, Journal, encode


# @spec journal/archive
# @implements tenant-recovery/portable-non-destructive-export
class Archive:
    def __init__(self, backend):
        if not isinstance(backend, LocalBackend):
            raise TypeError("archive spike is LocalBackend only")
        self.backend = backend
        self.key = "shared-journal-spike/archive.json"
        try:
            raw, self.etag = backend.get(self.key)
            self.manifest = json.loads(raw)
        except NotFound:
            self.manifest, self.etag = dict(segments=[], end=0, hash="0" * 64), None

    def checkpoint(self, journal):
        with journal._lock:
            end, digest = journal.end, journal.hash
            if end == self.manifest["end"]:
                if digest != self.manifest["hash"]:
                    raise Corrupt("archive belongs to another journal")
                return False
            start = self.manifest["end"]
            if end < start:
                raise Corrupt("journal is behind archive")
            if start:
                # The exported prefix must still be the prefix we archived.
                with (journal.root / "journal.bin").open("rb") as stream:
                    header = stream.read(start)
                expected = b"".join(self.backend.get(s["key"])[0] for s in self.manifest["segments"])
                if header != expected:
                    raise Corrupt("archive prefix changed")
            data = os.pread(journal._fd, end - start, start)
            if len(data) != end - start:
                raise Corrupt("short export")
        sha = hashlib.sha256(data).hexdigest()
        key = f"shared-journal-spike/segments/{start:020d}-{end:020d}-{sha}.bin"
        try:
            self.backend.put(key, data, if_none_match=True)
        except Exception:
            # A lost immutable upload reply can be reconciled by exact bytes.
            if self.backend.get(key)[0] != data:
                raise
        manifest = dict(segments=self.manifest["segments"] + [dict(key=key, bytes=len(data), sha256=sha)],
                        end=end, hash=digest)
        raw = encode(manifest)
        try:
            etag = self.backend.put(self.key, raw, if_match=self.etag, if_none_match=self.etag is None)
        except Exception:
            actual, etag = self.backend.get(self.key)
            if actual != raw:
                raise
        self.manifest, self.etag = manifest, etag
        return True

    def restore(self, destination):
        destination = Path(destination)
        destination.mkdir(exist_ok=False)
        raw, _ = self.backend.get(self.key)
        manifest = json.loads(raw)
        with (destination / "journal.bin").open("xb") as output:
            for segment in manifest["segments"]:
                data, _ = self.backend.get(segment["key"])
                if len(data) != segment["bytes"] or hashlib.sha256(data).hexdigest() != segment["sha256"]:
                    raise Corrupt("archive segment mismatch")
                output.write(data)
            output.flush()
            os.fsync(output.fileno())
        (destination / "head.json").write_bytes(encode(dict(format="shared-journal-1", end=manifest["end"], hash=manifest["hash"])))
        # Fresh replay validates the complete framed hash chain. Destination
        # is a test recovery artifact, not an atomically installed live service.
        with Journal(destination):
            pass
