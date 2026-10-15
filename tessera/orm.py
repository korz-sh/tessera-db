# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Optional SQLAlchemy integration (ORM and Core) on top of tessera.

Inside the functions you pass to :meth:`Store.write` and :meth:`Store.read`,
tessera hands you a connection shaped like :mod:`sqlite3`. SQLAlchemy knows
how to adopt an existing connection (the engine's ``creator`` argument), so
the full ORM works **without the core knowing anything about SQLAlchemy**:
the lease, fencing, write-once delta and manifest CAS stay exactly the same.

This module is sugar over that pattern, so you write models instead of SQL
by hand. It is an **optional** dependency::

    pip install "tessera-db[orm]"

Complete example::

    from sqlalchemy import Column, Integer, String, select
    from sqlalchemy.orm import declarative_base
    from tessera.orm import TesseraORM

    Base = declarative_base()

    class User(Base):
        __tablename__ = "users"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    db = TesseraORM(store, Base)
    db.create_tenant("acme")                      # DDL derived from models

    def add_ada(session):
        session.add(User(name="ada"))

    version = db.write("acme", add_ada)           # lease + delta + CAS

    def names(session):
        return [u.name for u in session.scalars(select(User).order_by(User.id))]

    db.read("acme", names, min_version=version)   # read-your-writes -> ["ada"]

**Why callbacks and not** ``with db.write(...) as session``: the connection
only exists *inside* the callback tessera invokes under the lease. A context
manager would have to create the session before the connection exists, which
forces threads or fragile coroutines. The callback is the honest way to
express "this runs under the lease, and if it fails nothing is published".

It also accepts the experimental v2 ``CallbackStore`` bridge. Turso
implements ``REGEXP`` and ``FLOOR`` in SQL, but its DBAPI does not register
custom Python functions.

What does **not** change versus using tessera by hand:

- One writer per tenant remains the guarantee; :meth:`TesseraORM.write`
  takes the lease.
- A ``Conflict`` propagates to the caller: transactions are never retried
  on their own.
- If the callback raises, ``rollback`` runs and no delta is published.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

try:
    from sqlalchemy import create_engine
    from sqlalchemy.dialects.sqlite import dialect as _sqlite_dialect
    from sqlalchemy.orm import Session
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.schema import CreateTable
except ImportError as exc:  # pragma: no cover - guard, no logic
    raise ImportError(
        "tessera.orm requires SQLAlchemy. Install the extra:\n"
        '    pip install "tessera-db[orm]"'
    ) from exc

if TYPE_CHECKING:
    from tessera.store import Store

T = TypeVar("T")


# @spec tessera/session-for
def session_for(conn: Any) -> Session:
    """Return a :class:`~sqlalchemy.orm.Session` that uses *conn*.

    *conn* is the connection tessera passes to the :meth:`Store.write` /
    :meth:`Store.read` callbacks: a real ``sqlite3.Connection`` on the
    hydration path, or the sqlite3-shaped adapter over apsw on the VFS path.
    Both work.

    ``creator`` + ``StaticPool`` make SQLAlchemy **adopt that connection**
    instead of opening its own: without them the engine would open another
    file and the writes would never enter the tenant's delta.

    Use it directly if you want to drive the session by hand; for the common
    case :class:`TesseraORM` is enough.
    """
    engine = create_engine("sqlite://", creator=lambda: conn, poolclass=StaticPool)
    return Session(engine)


# @spec tessera/schema-from-metadata
def schema_from_metadata(metadata: Any) -> str:
    """DDL (``CREATE TABLE``) for all models, ready for ``Store.create``.

    Accepts a declarative Base or a ``MetaData``. Tables come out in
    dependency order (``sorted_tables``), so FKs do not break.
    """
    meta = getattr(metadata, "metadata", metadata)
    dialect = _sqlite_dialect()
    statements = [
        str(CreateTable(table).compile(dialect=dialect)).strip()
        for table in meta.sorted_tables
    ]
    if not statements:
        raise ValueError(
            "the metadata has no tables: did you import the models before "
            "building TesseraORM?"
        )
    return ";\n".join(statements) + ";"


# @spec tessera/tessera-orm
class TesseraORM:
    """ORM facade over a :class:`~tessera.store.Store`.

    Args:
        store: the :class:`Store` already configured with its backend.
        base: declarative Base (or ``MetaData``) of your models, used to
            generate the DDL in :meth:`create_tenant`.
    """

    def __init__(self, store: "Store", base: Any) -> None:
        self._store = store
        self._base = base
        self.last_version: int | None = None

    def create_tenant(self, tenant: str) -> int:
        """Create *tenant* with the schema derived from the models.

        Returns the initial version. Propagates ``TenantExists`` if it
        already existed.
        """
        manifest = self._store.create(tenant, schema_from_metadata(self._base))
        version = manifest["version"] if isinstance(manifest, dict) else manifest.version
        self.last_version = version
        return version

    def write(self, tenant: str, fn: Callable[[Session], Any]) -> int:
        """Run *fn* with a write ``Session`` and publish the delta.

        *fn* receives the session and does its thing (``add``, ``merge``,
        ``delete``, queries). The ``commit`` is done by tessera on return, so
        there is **no need** to call it inside *fn* (calling it anyway is
        harmless).

        Returns the published version (also stored on :attr:`last_version`).
        If *fn* raises, ``rollback`` runs, the exception propagates and
        nothing is published.
        """

        def run(conn: Any) -> None:
            session = session_for(conn)
            try:
                fn(session)
                session.commit()
                conn.commit()
            except BaseException:
                session.rollback()
                raise
            finally:
                session.close()

        _, version = self._store.write(tenant, run)
        self.last_version = version
        return version

    def read(
        self,
        tenant: str,
        fn: Callable[[Session], T],
        min_version: int | None = None,
    ) -> T:
        """Run *fn* with a read ``Session`` and return its result.

        With *min_version* it waits to see that version before reading
        (read-your-writes); if it does not show up in time ``StaleRead``
        propagates.
        """

        def run(conn: Any) -> T:
            session = session_for(conn)
            try:
                return fn(session)
            finally:
                session.close()

        if min_version is None:
            return self._store.read(tenant, run)
        return self._store.read(tenant, run, min_version=min_version)
