"""Opt-in Store-shaped bridge for existing SQLite callback consumers."""

from __future__ import annotations

import math
import sqlite3
import turso

from tessera.backends.base import NotFound
from tessera.store import TenantExists, TenantNotFound

from .native import NativeLogStore
from .owner import BatchOwner
from .reads import PublishedReader


class _SQLiteCallbackConnection:
    """Translate sqlite3 row factories used by legacy callbacks."""

    def __init__(self, connection):
        object.__setattr__(self, "_connection", connection)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        if name == "row_factory" and value is sqlite3.Row:
            value = turso.Row
        setattr(self._connection, name, value)

    def create_function(self, name, num_params, func, *, deterministic=False):
        # SQLAlchemy's pysqlite dialect installs REGEXP/FLOOR during connect.
        # Turso already implements both expressions, but its Python DBAPI has
        # no UDF registration. Reject application UDFs rather than silently
        # claiming that they were registered.
        sqlalchemy_regexp = (
            name == "regexp" and num_params == 2
            and getattr(func, "__module__", None) == "sqlalchemy.dialects.sqlite.pysqlite"
            and getattr(func, "__qualname__", None)
            == "SQLiteDialect_pysqlite.on_connect.<locals>.regexp"
        )
        sqlalchemy_floor = name == "floor" and num_params == 1 and func is math.floor
        if not (sqlalchemy_regexp or sqlalchemy_floor):
            raise NotImplementedError("Turso does not support Python SQL functions")


def _schema_statements(script):
    if script is None:
        return ()
    pending = ""
    statements = []
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip().removesuffix(";").strip()
            if statement:
                statements.append(statement)
            pending = ""
    if pending.strip():
        raise ValueError("incomplete SQLite schema")
    return tuple(statements)


# @spec turso/callback-store
class CallbackStore:
    """Run Store-style callbacks through v2 native, one publication per write.

    Reads use a verified published frontier. Writes create a short-lived owner,
    so unrelated request handlers never hold a lease while idle. Callback
    receipts are unique invocation markers; the application remains responsible
    for making a retry after a process crash safe at its business boundary.
    """

    def __init__(self, backend, *, group_for_tenant, cache_root=None):
        if not callable(group_for_tenant):
            raise TypeError("group_for_tenant must be callable")
        self.backend = backend
        self.group_for_tenant = group_for_tenant
        self.cache_root = cache_root
        self._closed = False

    def _group(self, tenant):
        if self._closed:
            raise RuntimeError("store is closed")
        return self.group_for_tenant(tenant)

    def exists(self, tenant):
        group = self._group(tenant)
        try:
            NativeLogStore(self.backend, group).manifest()
        except NotFound:
            return False
        return True

    def create(self, tenant, schema=None):
        group = self._group(tenant)
        with BatchOwner(self.backend, group, schema=_schema_statements(schema),
                        native=True, cache_root=self.cache_root) as owner:
            if not owner.created:
                raise TenantExists(f"tenant {tenant!r} already exists")
            return owner.storage.manifest()[0]

    def read(self, tenant, fn, *, min_version=None):
        group = self._group(tenant)
        storage = NativeLogStore(self.backend, group)
        reader = PublishedReader(storage, cache_root=self.cache_root)
        try:
            try:
                with reader.open(min_version=min_version) as (conn, _):
                    return fn(_SQLiteCallbackConnection(conn))
            except NotFound as exc:
                raise TenantNotFound(f"tenant {tenant!r} does not exist") from exc
        finally:
            reader.close()

    def write(self, tenant, fn):
        group = self._group(tenant)
        if not self.exists(tenant):
            raise TenantNotFound(f"tenant {tenant!r} does not exist")
        with BatchOwner(self.backend, group, native=True,
                        cache_root=self.cache_root) as owner:
            def callback(conn):
                original_factory = conn.row_factory
                try:
                    return fn(_SQLiteCallbackConnection(conn))
                finally:
                    conn.row_factory = original_factory

            return owner.apply_callback(callback)

    def gc(self, tenant, *, policy=None, dry_run=True):
        """Plan or run retention GC for one v2 group under a short-lived owner."""
        group = self._group(tenant)
        if not self.exists(tenant):
            raise TenantNotFound(f"tenant {tenant!r} does not exist")
        with BatchOwner(self.backend, group, native=True,
                        cache_root=self.cache_root) as owner:
            return owner.lifecycle(policy=policy).gc(dry_run=dry_run)

    def close(self):
        self._closed = True
