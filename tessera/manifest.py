# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Tenant manifest for tessera-db.

The manifest is the ONE mutable object of a tenant: it records which snapshot
and delta objects make up the current database state, a monotonic version
counter used for compare-and-swap (CAS) commits, and the lease epoch of the
last committer. Every other object in storage is write-once.

The manifest is always written with CAS (if-match on the previous version), so
the version counter doubles as the optimistic-concurrency token.

Format 2 (additive to format 1) replaces the base snapshot with ``chunks[]``:
a compacted, content-addressed base (the last chunk is zero-padded to the
chunk size; ``page_count`` is the truth about the database size) with
``deltas[]`` applied on top. Format 1 manifests keep the legacy
snapshot + deltas shape and must not carry any format-2 field.
"""

import json
from dataclasses import dataclass, field

__all__ = [
    "MANIFEST_FORMAT",
    "ChunkEntry",
    "DeltaEntry",
    "Manifest",
    "ManifestError",
    "UnsupportedFormat",
]

MANIFEST_FORMAT = 1


# @spec tessera/manifest-error
# @implements bounded-write-cost/commit-frontier-is-distinct-from-object-generation
class ManifestError(Exception):
    """Base class for manifest errors (malformed JSON or invalid content)."""


# @spec tessera/unsupported-format
class UnsupportedFormat(ManifestError):
    """Raised when a manifest declares a format this code does not understand."""


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestError(
            f"manifest field {name!r} must be an int, got {type(value).__name__}"
        )
    return value


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ManifestError(
            f"manifest field {name!r} must be a string, got {type(value).__name__}"
        )
    return value


@dataclass
# @spec tessera/delta-entry
class DeltaEntry:
    """A committed delta object referenced by a manifest.

    key:   object key of the delta (e.g. ``"delta/000007.tsd1"``).
    etag:  content etag of the delta object (sha256 hex for the local backend).
    size:  delta object size in bytes.
    epoch: lease epoch of the writer that uploaded the delta (0 = unknown).
    """

    key: str
    etag: str
    size: int
    epoch: int = 0


@dataclass
# @spec tessera/chunk-entry
class ChunkEntry:
    """A chunk of the compacted base referenced by a format-2 manifest.

    key:  object key of the chunk (e.g. ``"t1/chunks/ab/ab12...ef"``).
    etag: content etag of the chunk object (sha256 hex for the local backend).
    size: stored size of the chunk object in bytes (equals the uncompressed
          size when ``chunk_codec`` is ``"none"``).
    hash: sha256 hex of the uncompressed chunk content; the key's last
          segment must equal it (see ``tessera.chunks.chunk_key``).
    """

    key: str
    etag: str
    size: int
    hash: str


@dataclass
# @spec tessera/manifest
class Manifest:
    """Current committed state of one tenant's database.

    format:     manifest wire format: 1 (snapshot + deltas) or 2 (chunks +
                deltas). :data:`MANIFEST_FORMAT` is the format new manifests
                are written with and is still 1 until compaction lands.
    version:    monotonic CAS counter; bumped on every committed change.
                This is the OBJECT's generation — anything that CASes a new
                manifest (including compaction, which adds no commits) moves
                it, so it must never be read as a commit frontier.
    data_version: the COMMIT frontier this manifest actually covers: the
                highest committed write sequence whose effects are in the
                data. ``0`` means "same as ``version``" — the invariant
                every pre-windowed producer holds by construction — and is
                normalized to ``version`` on construction, so the attribute
                is always the concrete frontier. Windowed publishes stamp
                it with the commit sequence folded in; compaction carries
                it forward verbatim. ``min_version`` reads compare against
                this field, never against ``version``.
    snapshot:   object key of the base snapshot (e.g. ``"snap/0.sqlite"``).
                Format 2 only: must be empty — chunks replace the snapshot.
    deltas:     delta objects applied on top of the base, in commit order.
    last_epoch: lease epoch of the last writer that committed this manifest.

    Format-2 fields (all defaulted so v1 manifests keep working):

    chunks:            chunk objects forming the compacted base, in order.
                       The last chunk is zero-padded; ``page_count`` is the
                       truth about the database size.
    chunk_pages:       chunk size in pages (e.g. 1024 = 4 MiB at page 4096).
    page_count:        total pages of the assembled database.
    compacted_version: manifest version whose compaction produced ``chunks``.
    chunk_codec:       codec chunks are stored with: ``"none"`` or ``"zstd"``.
                       Hashes are always over uncompressed content.

    Note: ``snapshot`` carries a default only because ``format``/``version``
    precede it in the field order and dataclasses require it; a format-1
    manifest with a blank snapshot fails :meth:`validate`.
    """

    format: int = 1
    version: int = 1
    snapshot: str = ""
    deltas: list[DeltaEntry] = field(default_factory=list)
    last_epoch: int = 0
    chunks: list[ChunkEntry] = field(default_factory=list)
    chunk_pages: int = 0
    page_count: int = 0
    compacted_version: int = 0
    chunk_codec: str = "none"
    # Last in field order so positional construction is unaffected; the
    # wire field is likewise additive-optional (absent == ``version``).
    data_version: int = 0

    def __post_init__(self) -> None:
        if self.data_version == 0:
            self.data_version = self.version

    def to_json(self) -> str:
        """Serialize to a JSON string. Exact inverse of :meth:`from_json`.

        Format-1 manifests serialize exactly the legacy five-field shape;
        format-2 manifests add ``chunks``, ``chunk_pages``, ``page_count``,
        ``compacted_version`` and ``chunk_codec``.
        """
        obj: dict = {
            "format": self.format,
            "version": self.version,
            "snapshot": self.snapshot,
            # `vars()` instead of `asdict()`: DeltaEntry and ChunkEntry are
            # FLAT dataclasses (str/int only, nothing nested), so asdict's
            # recursive copy is pure overhead — and the manifest is
            # serialized on EVERY write, inside the CAS. Measured on a
            # 200-delta manifest: 0.276 ms -> 0.005 ms (54x).
            # `json.dumps` does not mutate what it receives, so exposing the
            # __dict__ without copying is safe here.
            "deltas": [vars(entry) for entry in self.deltas],
            "last_epoch": self.last_epoch,
        }
        if self.data_version != self.version:
            # Commit frontier diverged from the object generation (windowed
            # publish fold or a compaction CAS above unpublished commits).
            # Legacy manifests never emit this; readers default it to
            # ``version``, which is exactly the frontier they covered.
            obj["data_version"] = self.data_version
        if self.format == 2:
            obj["chunks"] = [vars(entry) for entry in self.chunks]
            obj["chunk_pages"] = self.chunk_pages
            obj["page_count"] = self.page_count
            obj["compacted_version"] = self.compacted_version
            obj["chunk_codec"] = self.chunk_codec
        return json.dumps(obj)

    @classmethod
    def from_json(cls, data: str) -> "Manifest":
        """Parse and validate a JSON manifest.

        Raises :class:`ManifestError` for malformed JSON, missing required
        fields, wrong field types or invalid content, and
        :class:`UnsupportedFormat` when ``format`` is not 1 or 2.

        Format 1 additionally tolerates absent optional fields (deltas may
        omit ``epoch``; format-2 fields default). Format 2 requires
        ``chunks``, ``chunk_pages``, ``page_count`` and ``compacted_version``.
        """
        try:
            obj = json.loads(data)
        except (TypeError, ValueError) as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
        if not isinstance(obj, dict):
            raise ManifestError("manifest JSON must be an object")

        if "format" not in obj:
            raise ManifestError("manifest JSON missing required field: 'format'")
        fmt = _require_int(obj["format"], "format")
        if fmt not in (1, 2):
            raise UnsupportedFormat(
                f"unsupported manifest format: {fmt} (expected 1 or 2)"
            )

        for name in ("version", "snapshot", "deltas", "last_epoch"):
            if name not in obj:
                raise ManifestError(f"manifest JSON missing required field: {name!r}")
        version = _require_int(obj["version"], "version")
        snapshot = _require_str(obj["snapshot"], "snapshot")
        last_epoch = _require_int(obj["last_epoch"], "last_epoch")

        raw_deltas = obj["deltas"]
        if not isinstance(raw_deltas, list):
            raise ManifestError("manifest field 'deltas' must be a list")
        deltas: list[DeltaEntry] = []
        for i, raw in enumerate(raw_deltas):
            if not isinstance(raw, dict):
                raise ManifestError(f"deltas[{i}] must be an object")
            for name in ("key", "etag", "size"):
                if name not in raw:
                    raise ManifestError(f"deltas[{i}] missing required field: {name!r}")
            deltas.append(
                DeltaEntry(
                    key=_require_str(raw["key"], f"deltas[{i}].key"),
                    etag=_require_str(raw["etag"], f"deltas[{i}].etag"),
                    size=_require_int(raw["size"], f"deltas[{i}].size"),
                    epoch=_require_int(raw.get("epoch", 0), f"deltas[{i}].epoch"),
                )
            )

        chunks: list[ChunkEntry] = []
        chunk_pages = 0
        page_count = 0
        compacted_version = 0
        chunk_codec = "none"
        # Parse v2 fields whenever present (even on format 1) so that a v1
        # manifest carrying v2 fields fails validate() instead of silently
        # dropping them.
        if "chunks" in obj:
            raw_chunks = obj["chunks"]
            if not isinstance(raw_chunks, list):
                raise ManifestError("manifest field 'chunks' must be a list")
            for i, raw in enumerate(raw_chunks):
                if not isinstance(raw, dict):
                    raise ManifestError(f"chunks[{i}] must be an object")
                for name in ("key", "etag", "size", "hash"):
                    if name not in raw:
                        raise ManifestError(f"chunks[{i}] missing required field: {name!r}")
                chunks.append(
                    ChunkEntry(
                        key=_require_str(raw["key"], f"chunks[{i}].key"),
                        etag=_require_str(raw["etag"], f"chunks[{i}].etag"),
                        size=_require_int(raw["size"], f"chunks[{i}].size"),
                        hash=_require_str(raw["hash"], f"chunks[{i}].hash"),
                    )
                )
        if "chunk_pages" in obj:
            chunk_pages = _require_int(obj["chunk_pages"], "chunk_pages")
        if "page_count" in obj:
            page_count = _require_int(obj["page_count"], "page_count")
        if "compacted_version" in obj:
            compacted_version = _require_int(obj["compacted_version"], "compacted_version")
        if "chunk_codec" in obj:
            chunk_codec = _require_str(obj["chunk_codec"], "chunk_codec")

        # Commit frontier: absent means this manifest covers commits up to
        # its own version (the pre-windowed invariant). Present means a
        # windowed publish fold or compaction CAS already separated the two
        # axes — validate() bounds it below ``version``.
        data_version = version
        if "data_version" in obj:
            data_version = _require_int(obj["data_version"], "data_version")

        if fmt == 2:
            if not (chunks or chunk_pages or page_count or compacted_version):
                # Same relabeled-v1 rule as Manifest.validate(): a manifest
                # declaring format 2 with no v2 content is unsupported.
                raise UnsupportedFormat(
                    "manifest declares format 2 but carries no format-2 fields "
                    "(chunks/chunk_pages/page_count/compacted_version all absent or zero); "
                    "it looks like a v1 manifest relabeled as format 2"
                )
            # Format 2 requires the full v2 payload; a manifest with some v2
            # content but missing pieces fails here with a missing-field error.
            for name in ("chunks", "chunk_pages", "page_count", "compacted_version"):
                if name not in obj:
                    raise ManifestError(f"manifest JSON missing required field: {name!r}")

        manifest = cls(
            format=fmt,
            version=version,
            snapshot=snapshot,
            deltas=deltas,
            last_epoch=last_epoch,
            chunks=chunks,
            chunk_pages=chunk_pages,
            page_count=page_count,
            compacted_version=compacted_version,
            chunk_codec=chunk_codec,
            data_version=data_version,
        )
        manifest.validate()
        return manifest

    def validate(self) -> None:
        """Raise :class:`ManifestError`/:class:`UnsupportedFormat` if invalid.

        Common checks: ``format`` in {1, 2}, ``version >= 1``, every delta
        entry valid, ``last_epoch >= 0``, and ``1 <= data_version <=
        version`` (a manifest must never claim to cover commits beyond its
        own CAS generation).

        Format 1: non-blank ``snapshot`` and NO format-2 fields (a v1
        manifest carrying ``chunks``/``chunk_pages``/``page_count``/
        ``compacted_version``/non-default ``chunk_codec`` is invalid).

        Format 2: empty ``snapshot`` (chunks replace the snapshot), non-empty
        valid ``chunks``, ``chunk_pages > 0``, ``page_count > 0``,
        ``compacted_version >= 1`` and ``chunk_codec`` in ``{"none", "zstd"}``.
        A manifest declaring format 2 with no format-2 content at all is
        rejected as :class:`UnsupportedFormat` (it looks like a v1 manifest
        relabeled as format 2).
        """
        if isinstance(self.format, bool) or not isinstance(self.format, int):
            raise ManifestError(
                f"manifest field 'format' must be an int, got {type(self.format).__name__}"
            )
        if self.format not in (1, 2):
            raise UnsupportedFormat(
                f"unsupported manifest format: {self.format} (expected 1 or 2)"
            )
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ManifestError(
                f"manifest field 'version' must be an int, got {type(self.version).__name__}"
            )
        if self.version < 1:
            raise ManifestError(f"manifest version must be >= 1, got {self.version}")
        if (
            isinstance(self.data_version, bool)
            or not isinstance(self.data_version, int)
        ):
            raise ManifestError(
                "manifest field 'data_version' must be an int, got "
                f"{type(self.data_version).__name__}"
            )
        if not 1 <= self.data_version <= self.version:
            # A frontier ABOVE the object generation claims to cover commits
            # this manifest does not contain — the failure mode that breaks
            # min_version reads, so it is rejected at the boundary.
            raise ManifestError(
                f"manifest data_version must satisfy 1 <= data_version <= "
                f"version, got {self.data_version} (version {self.version})"
            )
        if isinstance(self.last_epoch, bool) or not isinstance(self.last_epoch, int):
            raise ManifestError(
                f"manifest field 'last_epoch' must be an int, got {type(self.last_epoch).__name__}"
            )
        if self.last_epoch < 0:
            raise ManifestError(f"manifest last_epoch must be >= 0, got {self.last_epoch}")
        for i, entry in enumerate(self.deltas):
            if not isinstance(entry, DeltaEntry):
                raise ManifestError(f"deltas[{i}] must be a DeltaEntry")
            if not entry.key:
                raise ManifestError(f"deltas[{i}].key must not be empty")
            if not entry.etag:
                raise ManifestError(f"deltas[{i}].etag must not be empty")
            if isinstance(entry.size, bool) or not isinstance(entry.size, int) or entry.size < 0:
                raise ManifestError(
                    f"deltas[{i}].size must be a non-negative int, got {entry.size!r}"
                )
            if isinstance(entry.epoch, bool) or not isinstance(entry.epoch, int) or entry.epoch < 0:
                raise ManifestError(
                    f"deltas[{i}].epoch must be a non-negative int, got {entry.epoch!r}"
                )

        if self.format == 1:
            if not isinstance(self.snapshot, str) or not self.snapshot.strip():
                raise ManifestError("manifest snapshot must be a non-blank string")
            carries_v2_fields = (
                bool(self.chunks)
                or self.chunk_pages != 0
                or self.page_count != 0
                or self.compacted_version != 0
                or self.chunk_codec != "none"
            )
            if carries_v2_fields:
                raise ManifestError(
                    "format 1 manifest must not carry format-2 fields "
                    "(chunks/chunk_pages/page_count/compacted_version/chunk_codec)"
                )
            return

        # Format 2.
        if (
            not self.chunks
            and self.chunk_pages == 0
            and self.page_count == 0
            and self.compacted_version == 0
        ):
            raise UnsupportedFormat(
                "manifest declares format 2 but carries no format-2 fields "
                "(chunks/chunk_pages/page_count/compacted_version all absent or zero); "
                "it looks like a v1 manifest relabeled as format 2"
            )
        if not isinstance(self.snapshot, str) or self.snapshot != "":
            raise ManifestError(
                "format 2 manifest snapshot must be empty (chunks replace the snapshot)"
            )
        if not self.chunks:
            raise ManifestError("format 2 manifest must reference at least one chunk")
        if isinstance(self.chunk_pages, bool) or not isinstance(self.chunk_pages, int):
            raise ManifestError(
                f"manifest field 'chunk_pages' must be an int, got {type(self.chunk_pages).__name__}"
            )
        if self.chunk_pages <= 0:
            raise ManifestError(f"manifest chunk_pages must be > 0, got {self.chunk_pages}")
        if isinstance(self.page_count, bool) or not isinstance(self.page_count, int):
            raise ManifestError(
                f"manifest field 'page_count' must be an int, got {type(self.page_count).__name__}"
            )
        if self.page_count <= 0:
            raise ManifestError(f"manifest page_count must be > 0, got {self.page_count}")
        if isinstance(self.compacted_version, bool) or not isinstance(self.compacted_version, int):
            raise ManifestError(
                "manifest field 'compacted_version' must be an int, "
                f"got {type(self.compacted_version).__name__}"
            )
        if self.compacted_version < 1:
            raise ManifestError(
                f"manifest compacted_version must be >= 1, got {self.compacted_version}"
            )
        if self.chunk_codec not in ("none", "zstd"):
            raise ManifestError(
                f"manifest chunk_codec must be 'none' or 'zstd', got {self.chunk_codec!r}"
            )
        for i, entry in enumerate(self.chunks):
            if not isinstance(entry, ChunkEntry):
                raise ManifestError(f"chunks[{i}] must be a ChunkEntry")
            if not entry.key:
                raise ManifestError(f"chunks[{i}].key must not be empty")
            if not entry.etag:
                raise ManifestError(f"chunks[{i}].etag must not be empty")
            if not entry.hash:
                raise ManifestError(f"chunks[{i}].hash must not be empty")
            if isinstance(entry.size, bool) or not isinstance(entry.size, int) or entry.size <= 0:
                raise ManifestError(
                    f"chunks[{i}].size must be a positive int, got {entry.size!r}"
                )
