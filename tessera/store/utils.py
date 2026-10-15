# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Store utilities - shared helper functions."""

import os
from pathlib import Path

# Name (under the cache root) of the GLOBAL content-addressed chunk cache
# backing the VFS read path. Reserved: see Store._lru_evict.
VFS_CHUNKS_DIRNAME = "chunks"

# Warm VFS sessions (one per (tenant, manifest.version)) kept per tenant
# before the oldest is released.
VFS_SESSIONS_PER_TENANT = 2


def _manifest_key(tenant: str) -> str:
    """Key of the one mutable manifest object of *tenant*."""
    return f"{tenant}/manifest.json"


def _snapshot_key(tenant: str, version: int) -> str:
    """Key of the write-once snapshot backing manifest *version*."""
    return f"{tenant}/snap/{version}.sqlite"


def _delta_key(tenant: str, epoch: int, seq: int) -> str:
    """Key of the write-once delta *seq* produced under lease *epoch*."""
    return f"{tenant}/wal/{epoch:08d}-{seq:08d}.delta"


def _snapshot_version(key: str) -> int | None:
    """Version number encoded in a snapshot key ``<tenant>/snap/<n>.sqlite``.

    Returns ``None`` for keys that do not fit the pattern; unparseable keys
    are never candidates for garbage collection.
    """
    stem = key.rsplit("/", 1)[-1]
    if not stem.endswith(".sqlite"):
        return None
    try:
        return int(stem[: -len(".sqlite")])
    except ValueError:
        return None


def _history_version(key: str) -> int | None:
    """Version number encoded in a history key ``<tenant>/history/<n>.json``.

    Returns ``None`` for keys that do not fit the pattern; unparseable keys
    are never candidates for history pruning.
    """
    stem = key.rsplit("/", 1)[-1]
    if not stem.endswith(".json"):
        return None
    try:
        return int(stem[: -len(".json")])
    except ValueError:
        return None


def _canonicalize_vacuum_header(path: Path) -> None:
    """Pin the free-running SQLite header counters of a VACUUM INTO output.

    ``VACUUM INTO`` is byte-deterministic EXCEPT for the schema cookie (and
    the related change counters), which tick on every vacuum. Re-chunking an
    unchanged database would then hash to a brand-new chunk set, breaking
    the dedupe invariant ("re-chunking an unmodified database uploads
    nothing new"). Pinning the change counter (offset 24), schema cookie
    (40) and version-valid-for (92) to 1 makes the compaction output depend
    only on logical content. All three fields are free-running metadata; any
    value is a legal, fully valid SQLite header.
    """
    with open(path, "r+b") as f:
        header = bytearray(f.read(100))
        if len(header) < 100 or bytes(header[:16]) != b"SQLite format 3\x00":
            return
        for offset in (24, 40, 92):
            header[offset : offset + 4] = (1).to_bytes(4, "big")
        f.seek(0)
        f.write(header)
        f.flush()
        os.fsync(f.fileno())
