# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""TSD1 delta codec for tessera-db.

A *delta* is the serialized form of the SQLite WAL frames produced between two
checkpoints. Applying a delta to a snapshot reproduces the database exactly,
page for page.

TSD1 wire format (all integers big-endian unsigned):

    magic       4 bytes   b"TSD1"
    page_size   u32       SQLite page size in bytes
    page_count  u32       final database size in pages
    n_frames    u32       number of (pgno, page) records
    records     n_frames x (pgno: u32, page: page_size bytes)
    trailer     32 bytes  sha256 of every byte that precedes it
"""

import hashlib
import os
import struct
from dataclasses import dataclass
from pathlib import Path

__all__ = ["Delta", "MAGIC", "serialize", "parse", "read_wal_frames", "apply_delta"]

MAGIC = b"TSD1"
_DIGEST_SIZE = 32

# TSD1 header: magic + page_size + page_count + n_frames.
_HEADER = struct.Struct(">4sIII")
_PGNO = struct.Struct(">I")

# SQLite WAL layout (all big-endian).
_WAL_MAGIC_LITTLE = 0x377F0682
_WAL_MAGIC_BIG = 0x377F0683
_WAL_HEADER = struct.Struct(">8I")  # magic, version, page_size, ckpt_seq, salt1, salt2, ck1, ck2
_WAL_FRAME_HEADER = struct.Struct(">6I")  # pgno, db_size_after_commit, salt1, salt2, ck1, ck2


@dataclass
# @spec tessera/delta
class Delta:
    """Committed WAL frames between two checkpoints.

    page_size:  SQLite page size in bytes.
    page_count: final database size in pages (db size after the last commit).
    frames:     (pgno, page_data) pairs with 1-based pgno, in commit order.
    """

    page_size: int
    page_count: int
    frames: list[tuple[int, bytes]]


def _is_valid_page_size(page_size: int) -> bool:
    return 512 <= page_size <= 65536 and page_size & (page_size - 1) == 0


# @spec tessera/serialize
def serialize(delta: Delta) -> bytes:
    """Serialize *delta* to TSD1 bytes (body + sha256 trailer)."""
    if not _is_valid_page_size(delta.page_size):
        raise ValueError(f"invalid page size: {delta.page_size!r}")
    if not 0 <= delta.page_count <= 0xFFFFFFFF:
        raise ValueError(f"page_count out of u32 range: {delta.page_count!r}")
    n_frames = len(delta.frames)
    if n_frames > 0xFFFFFFFF:
        raise ValueError(f"too many frames for u32: {n_frames}")
    parts = [_HEADER.pack(MAGIC, delta.page_size, delta.page_count, n_frames)]
    for pgno, page in delta.frames:
        if not 1 <= pgno <= 0xFFFFFFFF:
            raise ValueError(f"invalid page number: {pgno!r}")
        if len(page) != delta.page_size:
            raise ValueError(
                f"frame for page {pgno} has {len(page)} bytes, expected {delta.page_size}"
            )
        parts.append(_PGNO.pack(pgno))
        parts.append(page)
    body = b"".join(parts)
    return body + hashlib.sha256(body).digest()


# @spec tessera/parse
def parse(data: bytes) -> Delta:
    """Parse TSD1 bytes back into a :class:`Delta`.

    Raises ValueError if the sha256 trailer does not match the body or the
    body is structurally malformed.
    """
    data = bytes(data)
    if len(data) < _HEADER.size + _DIGEST_SIZE:
        raise ValueError(f"delta too short: {len(data)} bytes")
    body = data[:-_DIGEST_SIZE]
    if hashlib.sha256(body).digest() != data[-_DIGEST_SIZE:]:
        raise ValueError("delta checksum mismatch (sha256 trailer)")
    magic, page_size, page_count, n_frames = _HEADER.unpack_from(body)
    if magic != MAGIC:
        raise ValueError(f"bad magic: {magic!r}")
    if not _is_valid_page_size(page_size):
        raise ValueError(f"invalid page size: {page_size}")
    record_size = _PGNO.size + page_size
    frames: list[tuple[int, bytes]] = []
    offset = _HEADER.size
    for i in range(n_frames):
        end = offset + record_size
        if end > len(body):
            raise ValueError(f"truncated frame {i} of {n_frames}")
        (pgno,) = _PGNO.unpack_from(body, offset)
        if pgno < 1:
            raise ValueError(f"invalid page number: {pgno}")
        frames.append((pgno, body[offset + _PGNO.size : end]))
        offset = end
    if offset != len(body):
        raise ValueError("trailing bytes after final frame")
    return Delta(page_size=page_size, page_count=page_count, frames=frames)


# @spec tessera/read-wal-frames
def read_wal_frames(wal_path: str | Path) -> Delta:
    """Extract the frames of all committed transactions from a SQLite WAL file.

    * Frames accumulated without a commit frame when EOF is reached belong to
      an aborted/uncommitted transaction and are discarded.
    * Parsing stops at the first frame whose salts differ from the WAL header
      salts (leftovers of an older WAL generation after wraparound).
    * ``page_count`` is the db size recorded by the last commit frame.

    Raises ValueError when the WAL header is invalid or the file contains no
    committed transaction.
    """
    raw = Path(wal_path).read_bytes()
    if len(raw) < _WAL_HEADER.size:
        raise ValueError(f"WAL file too small: {len(raw)} bytes")
    (_magic, _version, raw_page_size, _ckpt_seq, salt1, salt2, _ck1, _ck2) = _WAL_HEADER.unpack_from(raw)
    if _magic not in (_WAL_MAGIC_LITTLE, _WAL_MAGIC_BIG):
        raise ValueError(f"invalid WAL magic: {_magic:#010x}")
    # SQLite encodes a 65536-byte page size as 1 in the WAL header.
    page_size = 65536 if raw_page_size == 1 else raw_page_size
    if not _is_valid_page_size(page_size):
        raise ValueError(f"invalid WAL page size: {raw_page_size}")

    frame_size = _WAL_FRAME_HEADER.size + page_size
    committed: list[tuple[int, bytes]] = []
    pending: list[tuple[int, bytes]] = []
    page_count = 0
    offset = _WAL_HEADER.size
    while offset + frame_size <= len(raw):
        pgno, db_size, fsalt1, fsalt2, _fck1, _fck2 = _WAL_FRAME_HEADER.unpack_from(raw, offset)
        if (fsalt1, fsalt2) != (salt1, salt2):
            break  # frame from an older WAL generation: end of valid content
        pending.append((pgno, raw[offset + _WAL_FRAME_HEADER.size : offset + frame_size]))
        if db_size != 0:  # commit frame: the accumulated run is a committed txn
            committed.extend(pending)
            pending.clear()
            page_count = db_size
        offset += frame_size

    if not committed:
        raise ValueError("WAL contains no committed transactions")
    return Delta(page_size=page_size, page_count=page_count, frames=committed)


# @spec tessera/apply-delta
def apply_delta(db_path: str | Path, delta: Delta) -> None:
    """Apply *delta* to the database file at *db_path*. Idempotent.

    The delta's sha256 trailer is re-verified before the database file is
    touched. Each page is then written at offset ``(pgno - 1) * page_size``
    (extending the file as needed), the file is truncated to
    ``page_count * page_size``, flushed and fsynced — so applying the same
    delta twice yields a byte-identical file.
    """
    # 1. Integrity check BEFORE touching the DB file.
    data = serialize(delta)
    body = data[:-_DIGEST_SIZE]
    if hashlib.sha256(body).digest() != data[-_DIGEST_SIZE:]:
        raise ValueError("delta integrity check failed (sha256 trailer mismatch)")

    # 2. Write pages at fixed offsets, then truncate to the final size.
    fd = os.open(os.fspath(db_path), os.O_RDWR | os.O_CREAT, 0o644)
    with os.fdopen(fd, "r+b") as f:
        for pgno, page in delta.frames:
            f.seek((pgno - 1) * delta.page_size)
            f.write(page)
        f.truncate(delta.page_count * delta.page_size)
        f.flush()
        os.fsync(f.fileno())
