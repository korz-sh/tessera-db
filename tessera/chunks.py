# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Content-addressed chunks for tessera-db.

A chunk is a fixed-size slice of the database file: ``chunk_pages`` pages of
``page_size`` bytes each (default ``CHUNK_PAGES = 1024`` pages = 4 MiB at a
4096-byte page). Chunks are the compacted base of a tenant database in
manifest format 2; delta objects are applied on top of them.

Invariants:

- A chunk's address is the sha256 hex of its *uncompressed* content, zero
  padding included. The last chunk of a database is padded with zero bytes to
  exactly ``chunk_pages * page_size``, so every chunk of a given database has
  an identical size and the hash is deterministic.
- The padded tail is a lie: the manifest's ``page_count`` is the truth.
  :func:`assemble` concatenates the chunks and truncates to
  ``page_count * page_size``, so the padding never reaches the assembled db.
- Chunk objects are stored under ``{tenant}/chunks/{hh}/{hash}`` (two-level
  fan-out on the first two hash hex digits) and are write-once:
  :func:`put_if_absent` dedupes via CAS create-only writes.
- :func:`get_chunk` verifies the hash on read, so bit-rot or a corrupt object
  surfaces as :class:`~tessera.backends.base.BackendError` instead of a
  silently corrupt database.

Codec helpers (:func:`encode_chunk`/:func:`decode_chunk`) allow chunks to be
stored compressed (``"zstd"``) while hashes are always computed over the
uncompressed content.
"""

import hashlib
import os
from pathlib import Path

from tessera.backends.base import Backend, BackendError, CASFailed

__all__ = [
    "CHUNK_PAGES",
    "UnsupportedCodec",
    "assemble",
    "chunk_key",
    "decode_chunk",
    "encode_chunk",
    "get_chunk",
    "page_size_of",
    "put_if_absent",
    "split",
]

CHUNK_PAGES = 1024


# @spec tessera/unsupported-codec
class UnsupportedCodec(BackendError):
    """Raised for an unknown chunk codec or a missing optional dependency."""


# @spec tessera/page-size-of
def page_size_of(db_path: str | os.PathLike) -> int:
    """Return the SQLite page size of *db_path* from its header.

    Bytes 16-17 of the SQLite header hold the page size as a big-endian
    unsigned 16-bit integer; the value ``1`` means 65536. Raises
    :class:`ValueError` if the header is unreadable or the page size is not a
    power of two between 512 and 65536.
    """
    with open(db_path, "rb") as f:
        header = f.read(18)
    if len(header) < 18:
        raise ValueError(f"not a sqlite database (file too small for header): {db_path}")
    value = int.from_bytes(header[16:18], "big")
    if value == 1:
        return 65536
    if value < 512 or value > 65536 or value & (value - 1) != 0:
        raise ValueError(f"invalid sqlite page size {value} in {db_path}")
    return value


# @spec tessera/split
def split(db_path: str | os.PathLike, chunk_pages: int = CHUNK_PAGES) -> list[tuple[str, bytes]]:
    """Split the database at *db_path* into content-addressed chunks.

    Consecutive chunks of ``chunk_pages`` pages each are read from the file;
    the last chunk is padded with zero bytes to exactly
    ``chunk_pages * page_size`` so every chunk has the same size. Each chunk
    is hashed with sha256 over the (padded, uncompressed) content.

    Returns ``[(hash_hex, padded_bytes), ...]`` in file order. An empty file
    yields an empty list. Raises :class:`ValueError` if the file size is not
    a multiple of the page size.
    """
    if chunk_pages <= 0:
        raise ValueError(f"chunk_pages must be positive, got {chunk_pages}")
    size = os.path.getsize(db_path)
    if size == 0:
        return []
    page_size = page_size_of(db_path)
    if size % page_size != 0:
        raise ValueError(
            f"database size {size} is not a multiple of page size {page_size}: {db_path}"
        )
    chunk_len = chunk_pages * page_size
    chunks: list[tuple[str, bytes]] = []
    with open(db_path, "rb") as f:
        while True:
            data = f.read(chunk_len)
            if not data:
                break
            if len(data) < chunk_len:
                data += b"\x00" * (chunk_len - len(data))
            chunks.append((hashlib.sha256(data).hexdigest(), data))
    return chunks


# @spec tessera/assemble
def assemble(
    chunk_blobs: list[bytes],
    page_count: int,
    page_size: int,
    dest_path: str | os.PathLike,
) -> Path:
    """Reassemble a database from chunk blobs and return the destination path.

    Writes the concatenation of *chunk_blobs* to *dest_path* (create or
    truncate), then truncates to ``page_count * page_size`` and fsyncs.
    ``page_count`` is the truth: any zero padding on the last chunk is cut
    off here.

    Raises :class:`ValueError` if the blobs are empty or of unequal length,
    or if together they cover fewer bytes than ``page_count * page_size``.
    """
    if not chunk_blobs:
        raise ValueError("chunk_blobs must not be empty")
    if page_count <= 0:
        raise ValueError(f"page_count must be positive, got {page_count}")
    if page_size <= 0:
        raise ValueError(f"page_size must be positive, got {page_size}")
    chunk_len = len(chunk_blobs[0])
    if chunk_len == 0:
        raise ValueError("chunk blobs must be non-empty")
    for i, blob in enumerate(chunk_blobs):
        if len(blob) != chunk_len:
            raise ValueError(
                f"chunk blob {i} has length {len(blob)}, expected {chunk_len} "
                "(all chunks must have the same length)"
            )
    covered = chunk_len * len(chunk_blobs)
    needed = page_count * page_size
    if covered < needed:
        raise ValueError(
            f"{len(chunk_blobs)} chunk(s) of {chunk_len} bytes cover {covered} bytes, "
            f"but page_count {page_count} x page_size {page_size} needs {needed}"
        )
    fd = os.open(dest_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        for blob in chunk_blobs:
            view = memoryview(blob)
            while view:
                written = os.write(fd, view)
                view = view[written:]
        os.ftruncate(fd, needed)
        os.fsync(fd)
    finally:
        os.close(fd)
    return Path(dest_path)


# @spec tessera/chunk-key
def chunk_key(tenant: str, hash_hex: str) -> str:
    """Return the object key for a chunk: ``{tenant}/chunks/{hh}/{hash}``."""
    return f"{tenant}/chunks/{hash_hex[:2]}/{hash_hex}"


# @spec tessera/put-if-absent
def put_if_absent(backend: Backend, tenant: str, data: bytes) -> tuple[str, bool]:
    """Upload *data* as a chunk unless it already exists. Returns ``(key, existed)``.

    Uses a create-only CAS put; a :class:`~tessera.backends.base.CASFailed`
    means another writer already stored the identical content (chunks are
    content-addressed, so same hash = same bytes), which is dedupe, not error.
    """
    hash_hex = hashlib.sha256(data).hexdigest()
    key = chunk_key(tenant, hash_hex)
    try:
        backend.put(key, data, if_none_match=True)
    except CASFailed:
        return key, True
    return key, False


# @spec tessera/get-chunk
def get_chunk(backend: Backend, tenant: str, hash_hex: str) -> bytes:
    """Fetch the chunk *hash_hex* for *tenant* and verify its content hash.

    Raises :class:`~tessera.backends.base.BackendError` on a hash mismatch
    (corrupt or swapped object) and propagates
    :class:`~tessera.backends.base.NotFound` if the key is missing.
    """
    key = chunk_key(tenant, hash_hex)
    data, _etag = backend.get(key)
    if hashlib.sha256(data).hexdigest() != hash_hex:
        raise BackendError(f"chunk hash mismatch: {key}")
    return data


def _zstandard():
    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - exercised only without zstandard
        raise UnsupportedCodec(
            "chunk codec 'zstd' requires the optional 'zstandard' package "
            "(pip install zstandard)"
        ) from exc
    return zstandard


# @spec tessera/encode-chunk
def encode_chunk(data: bytes, codec: str) -> bytes:
    """Encode chunk bytes for storage. ``"none"`` is the identity codec.

    ``"zstd"`` compresses with the optional ``zstandard`` package (level 3,
    standard frame). Any other codec raises :class:`UnsupportedCodec`.
    """
    if codec == "none":
        return data
    if codec == "zstd":
        zstandard = _zstandard()
        return zstandard.ZstdCompressor(level=3).compress(data)
    raise UnsupportedCodec(f"unknown chunk codec: {codec!r}")


# @spec tessera/decode-chunk
def decode_chunk(data: bytes, codec: str) -> bytes:
    """Decode stored chunk bytes. ``"none"`` is the identity codec.

    ``"zstd"`` decompresses a standard zstd frame via the optional
    ``zstandard`` package. Any other codec raises :class:`UnsupportedCodec`.
    """
    if codec == "none":
        return data
    if codec == "zstd":
        zstandard = _zstandard()
        return zstandard.ZstdDecompressor().decompress(data)
    raise UnsupportedCodec(f"unknown chunk codec: {codec!r}")
