"""SQL/schema compatibility evidence for a pinned Turso engine."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from .snapshot import ENGINE, InvalidSnapshot, connect


# @spec turso/unsupported-migration
class UnsupportedMigration(ValueError):
    """The migration is outside the reviewed additive DDL subset."""


# @spec turso/schema-mismatch
class SchemaMismatch(ValueError):
    """A schema inventory is missing a required object or has the wrong shape."""


_IDENTIFIER = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)'
_ADDITIVE = tuple(
    re.compile(pattern, re.IGNORECASE | re.DOTALL)
    for pattern in (
        rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+{_IDENTIFIER}\s+ON\s+{_IDENTIFIER}\s*\(.+\)",
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{_IDENTIFIER}\s*\(.+\)",
        rf"ALTER\s+TABLE\s+{_IDENTIFIER}\s+ADD\s+COLUMN\s+{_IDENTIFIER}\s+.+",
    )
)
_DESTRUCTIVE = re.compile(
    r"\b(?:DROP|RENAME|VACUUM|ATTACH|DETACH|PRAGMA|TRIGGER)\b", re.IGNORECASE
)


# @spec turso/validate-additive-migration
def validate_additive_migration(statements):
    """Validate the whole migration before one statement can execute."""
    checked = []
    for statement in statements:
        if not isinstance(statement, str):
            raise UnsupportedMigration("migration statements must be strings")
        sql = statement.strip().removesuffix(";").strip()
        if (not sql or ";" in sql or _DESTRUCTIVE.search(sql)
                or not any(pattern.fullmatch(sql) for pattern in _ADDITIVE)):
            raise UnsupportedMigration(f"unsupported migration statement: {sql[:80]}")
        checked.append(sql)
    if not checked:
        raise UnsupportedMigration("migration must contain at least one statement")
    return tuple(checked)


# @spec turso/apply-additive-migration
def apply_additive_migration(owner, statements):
    """Atomically apply reviewed DDL and publish it as one new frontier."""
    statements = validate_additive_migration(statements)
    with owner._lock:
        if owner._closed or owner._broken:
            raise RuntimeError("owner closed or recovery required")
        owner._require_live_heartbeat()
        try:
            # Turso deliberately rejects schema changes in BEGIN CONCURRENT.
            # The owner lock + remote lease already provide the exclusive
            # migration boundary, so use an ordinary exclusive-capable txn.
            owner._conn.execute("BEGIN")
            for statement in statements:
                owner._conn.execute(statement).fetchall()
            owner._conn.execute("COMMIT")
            with owner._lease_guard:
                owner._require_live_heartbeat()
                owner._lease = owner.storage.backend.renew_lease(
                    owner._lease, owner._lease_ttl_ms
                )
                owner.storage.lease = owner._lease
                owner._manifest, owner._etag = owner.storage.publish(
                    owner._conn,
                    version=owner._manifest["version"] + 1,
                    epoch=owner._lease.epoch,
                    etag=owner._etag,
                    source=owner._path,
                )
        except Exception:
            try:
                owner._conn.execute("ROLLBACK")
            except Exception:
                pass
            try:
                owner._reload()
            except Exception as exc:
                owner._broken = True
                raise InvalidSnapshot("migration recovery failed") from exc
            raise
        return owner._manifest


@dataclass(frozen=True)
# @spec turso/schema-requirement
class SchemaRequirement:
    tables: dict[str, tuple[str, ...]]
    indexes: dict[str, tuple[str, ...]]


# @spec turso/schema-inventory
def schema_inventory(conn):
    """Return complete table/column/index/FK reflection without ORM inference."""
    tables = {}
    indexes = {}
    foreign_keys = {}
    names = [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()]
    for table in names:
        quoted = table.replace('"', '""')
        tables[table] = tuple(row[1] for row in conn.execute(
            f'PRAGMA table_info("{quoted}")'
        ).fetchall())
        foreign_keys[table] = tuple(
            (row[2], row[3], row[4], row[6])
            for row in conn.execute(f'PRAGMA foreign_key_list("{quoted}")').fetchall()
        )
        for row in conn.execute(f'PRAGMA index_list("{quoted}")').fetchall():
            index = row[1]
            if index.startswith("sqlite_autoindex_"):
                continue
            index_quoted = index.replace('"', '""')
            indexes[index] = tuple(
                part[2] for part in conn.execute(
                    f'PRAGMA index_info("{index_quoted}")'
                ).fetchall()
            )
    return {"tables": tables, "indexes": indexes, "foreign_keys": foreign_keys}


# @spec turso/require-schema
def require_schema(conn, requirement):
    if not isinstance(requirement, SchemaRequirement):
        raise TypeError("SchemaRequirement required")
    inventory = schema_inventory(conn)
    for table, columns in requirement.tables.items():
        actual = inventory["tables"].get(table)
        if actual is None or any(column not in actual for column in columns):
            raise SchemaMismatch(f"required table/columns missing: {table}")
    for index, columns in requirement.indexes.items():
        if inventory["indexes"].get(index) != columns:
            raise SchemaMismatch(f"required index missing or incomplete: {index}")
    return inventory


@dataclass(frozen=True)
# @spec turso/upgrade-evidence
class UpgradeEvidence:
    source_engine: str
    candidate_engine: str
    version: int
    receipt_count: int
    sqlite_sha256: str


# @spec turso/rehearse-engine-upgrade
def rehearse_engine_upgrade(storage, destination, *, candidate_engine=ENGINE,
                             candidate_connect=connect):
    """Reopen one exact frontier with a candidate connector and export it.

    The candidate must identify itself explicitly.  This keeps an engine
    mismatch fail-closed instead of treating SQLite file readability as proof
    that Tessera's MVCC/log semantics are compatible.
    """
    if not isinstance(candidate_engine, str) or not candidate_engine:
        raise ValueError("candidate_engine must be explicit")
    advertised = (ENGINE if candidate_connect is connect
                  else getattr(candidate_connect, "engine_version", None))
    if advertised != candidate_engine:
        raise ValueError("candidate connector does not advertise the requested engine")
    manifest, _ = storage.manifest()
    if manifest["engine"] != ENGINE:
        raise InvalidSnapshot("source engine identity is not supported")
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    with tempfile.TemporaryDirectory(prefix="turso-upgrade-", dir=destination.parent) as root:
        path = Path(root) / "candidate.db"
        storage.restore_manifest(manifest, path)
        with closing(candidate_connect(path)) as conn:
            if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise InvalidSnapshot("candidate engine failed integrity check")
            receipts = conn.execute(
                "SELECT id,digest,version,result FROM _tessera_receipts ORDER BY id"
            ).fetchall()
            # A stock SQLite reopen is the mandatory rollback/exit boundary.
            conn.execute("PRAGMA journal_mode=wal").fetchall()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        with closing(sqlite3.connect(path)) as stock:
            if stock.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise InvalidSnapshot("candidate export is not stock SQLite")
            if stock.execute("SELECT count(*) FROM _tessera_receipts").fetchone()[0] != len(receipts):
                raise InvalidSnapshot("candidate changed the receipt frontier")
        os.link(path, destination)
    return UpgradeEvidence(
        source_engine=manifest["engine"], candidate_engine=candidate_engine,
        version=manifest["version"], receipt_count=len(receipts),
        sqlite_sha256=hashlib.sha256(destination.read_bytes()).hexdigest(),
    )
