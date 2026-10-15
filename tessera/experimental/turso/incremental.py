"""Bounded block deltas of sealed images; no dependency on Turso log internals."""

import json
import re
import struct

from .snapshot import SnapshotStore, InvalidSnapshot, canonical, digest

BLOCK = 4096
MAX_IMAGE = 64 * 1024 * 1024
MAX_DEPTH = 8
MAX_HEADER = 256 * 1024
MAGIC = b"TSDELTA1"


# @spec turso/incremental-store
class IncrementalStore(SnapshotStore):
    format = "tessera-turso-blocks-1"
    needs_previous = True

    def __init__(self, backend, group, *, boundary=None):
        super().__init__(backend, group, boundary=boundary)
        self.prefix = f"turso-blocks/{group}/"
        self.key = self.prefix + "manifest.json"
        self._sealed = None

    @staticmethod
    def _pack(header, payload):
        raw = canonical(header)
        return MAGIC + struct.pack(">I", len(raw)) + raw + payload

    @staticmethod
    def _unpack(encoded):
        if len(encoded) < 12 or encoded[:8] != MAGIC:
            raise InvalidSnapshot("invalid block envelope")
        size = struct.unpack(">I", encoded[8:12])[0]
        if size > MAX_HEADER or size > len(encoded) - 12:
            raise InvalidSnapshot("invalid block header length")
        try:
            h = json.loads(encoded[12:12 + size])
            if (type(h["size"]) is not int or not 0 <= h["size"] <= MAX_IMAGE
                    or type(h["depth"]) is not int or not 0 <= h["depth"] <= MAX_DEPTH
                    or not re.fullmatch(r"[0-9a-f]{64}", h["hash"])):
                raise ValueError()
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidSnapshot("invalid block metadata") from exc
        return h, encoded[12 + size:]

    def encode_image(self, data, previous):
        if len(data) > MAX_IMAGE:
            raise ValueError("incremental experiment limits images to 64 MiB")
        header = dict(size=len(data), hash=digest(data), depth=0, blocks=None, parent=None)
        full = self._pack(header, data)
        def remember(encoded, depth):
            self._sealed = (digest(encoded), data, depth)
            return encoded
        if previous is None:
            return remember(full, 0)
        if self._sealed is not None and self._sealed[0] == previous["sha256"]:
            _, old, depth = self._sealed
        else:
            prior = self.load_image(previous)
            depth = self._unpack(prior)[0]["depth"]
            old = self.decode_image(prior)
        if depth == MAX_DEPTH:
            return remember(full, 0)
        blocks = [i for i in range(0, len(data), BLOCK) if data[i:i + BLOCK] != old[i:i + BLOCK]]
        header.update(depth=depth + 1, blocks=blocks,
                      parent={k: previous[k] for k in ("image", "bytes", "sha256")})
        delta = self._pack(header, b"".join(data[i:i + BLOCK] for i in blocks))
        return remember(delta, depth + 1) if len(delta) < len(full) else remember(full, 0)

    def decode_image(self, encoded):
        return self._decode(encoded, MAX_DEPTH)

    def artifact_keys(self, manifest):
        keys = set()
        reference = {k: manifest[k] for k in ("image", "bytes", "sha256")}
        for _ in range(MAX_DEPTH + 1):
            keys.add(reference["image"])
            encoded = self.load_image(reference)
            header, _ = self._unpack(encoded)
            if header["parent"] is None:
                return keys
            reference = header["parent"]
        raise InvalidSnapshot("incremental chain exceeds depth limit")

    def _decode(self, encoded, budget):
        h, payload = self._unpack(encoded)
        try:
            if h["depth"] > budget:
                raise ValueError("chain exceeds depth limit")
            if h["depth"] == 0:
                if h["parent"] is not None or h["blocks"] is not None:
                    raise ValueError("invalid full image")
                data = payload
            else:
                p, blocks = h["parent"], h["blocks"]
                if (not re.fullmatch(re.escape(self.prefix) + r"images/[0-9a-f]{32}\.db", p["image"])
                        or type(p["bytes"]) is not int or not 0 <= p["bytes"] <= MAX_IMAGE + MAX_HEADER + 12
                        or not re.fullmatch(r"[0-9a-f]{64}", p["sha256"])
                        or not isinstance(blocks, list)
                        or any(type(i) is not int or i < 0 or i >= h["size"] or i % BLOCK for i in blocks)
                        or blocks != sorted(set(blocks))):
                    raise ValueError("invalid delta references")
                parent = self.load_image(p)
                if self._unpack(parent)[0]["depth"] != h["depth"] - 1:
                    raise ValueError("noncontiguous depth")
                data = bytearray(self._decode(parent, budget - 1)[:h["size"]])
                data.extend(b"\0" * (h["size"] - len(data)))
                offset = 0
                for i in blocks:
                    length = min(BLOCK, h["size"] - i)
                    if offset + length > len(payload):
                        raise ValueError("truncated delta")
                    data[i:i + length] = payload[offset:offset + length]
                    offset += length
                if offset != len(payload):
                    raise ValueError("trailing delta bytes")
            if len(data) != h["size"] or digest(data) != h["hash"]:
                raise ValueError("reconstructed image mismatch")
            return bytes(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidSnapshot("invalid incremental image") from exc
