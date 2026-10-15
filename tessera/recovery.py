"""Private full-image recovery helpers; callers own publication and leases."""

from contextlib import closing, contextmanager
from pathlib import Path
import logging
import sqlite3
import tempfile
import uuid

from .backends.base import BackendError
from .chunks import UnsupportedCodec, encode_chunk, page_size_of, put_if_absent, split
from .local import ensure_local
from .manifest import ChunkEntry, Manifest

logger = logging.getLogger(__name__)


@contextmanager
# @spec tessera/materialize
def materialize(backend, tenant, manifest, root):
    """Yield a verified standalone image in an exclusively created directory."""
    if manifest.format == 2 and manifest.chunk_codec != "none":
        raise UnsupportedCodec("full-image recovery currently requires chunk codec 'none'")
    Path(root).mkdir(parents=True, exist_ok=True)
    temporary = tempfile.TemporaryDirectory(prefix="tessera-recovery-", dir=root,
                                            ignore_cleanup_errors=True)
    try:
        tmp = temporary.name
        source = ensure_local(backend, tenant, manifest, dest=Path(tmp) / "source")
        image = Path(tmp) / "image.sqlite"
        with closing(sqlite3.connect(source)) as conn:
            with closing(sqlite3.connect(image)) as output:
                conn.backup(output)
                if output.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise BackendError("recovery image failed SQLite integrity check")
        yield image
    finally:
        try:
            temporary.cleanup()
        except Exception:
            logger.exception("recovery temporary cleanup failed")


# @spec tessera/image-manifest
def image_manifest(backend, tenant, image, template, version, epoch):
    """Upload independently owned image objects, preserving the target format."""
    if template.format == 2 and template.chunk_codec != "none":
        raise UnsupportedCodec("full-image recovery currently requires chunk codec 'none'")
    if template.format == 1:
        # A private key allows retries after orphan uploads. The numeric final
        # component preserves the existing snapshot GC version parser.
        key = f"{tenant}/snap/{uuid.uuid4().hex}/{version}.sqlite"
        backend.put(key, image.read_bytes(), if_none_match=True)
        return Manifest(version=version, snapshot=key, deltas=[], last_epoch=epoch)
    chunks = []
    for _digest, blob in split(image, chunk_pages=template.chunk_pages):
        encoded = encode_chunk(blob, template.chunk_codec)
        key, _ = put_if_absent(backend, tenant, encoded)
        etag = backend.head(key)
        if etag is None:
            raise BackendError(f"recovery chunk missing after upload: {key}")
        chunks.append(ChunkEntry(key=key, hash=key.rsplit("/", 1)[-1],
                                 etag=etag, size=len(encoded)))
    return Manifest(format=2, version=version, snapshot="", deltas=[],
                    last_epoch=epoch, chunks=chunks, chunk_pages=template.chunk_pages,
                    page_count=image.stat().st_size // page_size_of(image),
                    compacted_version=version, chunk_codec=template.chunk_codec)
