# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""apsw adapters: run sqlite3-shaped fns on VFS connections."""

from ..backends.base import BackendError

try:  # optional dependency: the VFS paths need apsw
    import apsw

    from ..vfs import TesseraVFS
except ImportError:  # pragma: no cover - exercised only without apsw installed
    apsw = None  # type: ignore[assignment]
    TesseraVFS = None  # type: ignore[assignment,misc]


class _ApswCursorAdapter:
    """sqlite3-Cursor-shaped wrapper over one apsw cursor.

    Covers exactly the surface read functions use: ``execute`` (returns
    ``self``, sqlite3-style), ``fetchone``, ``fetchall``, ``description``
    and ``lastrowid``. The iteration protocol is intentionally not provided.

    ``description`` is captured eagerly at :meth:`execute` time because apsw's
    ``cursor.getdescription()`` raises once execution has completed (zero-row
    statements complete immediately); with no description available it reads
    as ``None``, mirroring sqlite3's behaviour for non-result statements.
    """

    def __init__(self, raw_conn: "apsw.Connection", raw_cursor: "apsw.Cursor") -> None:
        self._raw_conn = raw_conn
        self._raw_cursor = raw_cursor
        self._description: tuple | None = None

    def execute(self, sql: str, params=()) -> "_ApswCursorAdapter":
        self._raw_cursor.execute(sql, params)
        try:
            self._description = self._raw_cursor.getdescription()
        except apsw.ExecutionCompleteError:
            self._description = None  # zero-row statement: no pending execution
        return self

    @property
    def description(self) -> tuple | None:
        """Column descriptions of the last executed statement (may be None)."""
        return self._description

    @property
    def lastrowid(self) -> int:
        """``conn.last_insert_rowid()`` — always 0 on a read-only connection."""
        return self._raw_conn.last_insert_rowid()

    def fetchone(self):
        """Next row, or ``None`` when exhausted (sqlite3 semantics)."""
        return self._raw_cursor.fetchone()

    def fetchall(self):
        """All remaining rows (``[]`` when already exhausted)."""
        return self._raw_cursor.fetchall()


class _ApswConnAdapter:
    """sqlite3.Connection-shaped wrapper over a READ-ONLY apsw.Connection.

    Lets existing read functions (written against :mod:`sqlite3`) run
    unchanged on the TesseraVFS read path:

    - ``execute(sql, params=())`` returns a :class:`_ApswCursorAdapter`
      (``fetchone``/``fetchall``/``description``/``lastrowid``).
    - ``cursor()`` returns a wrapper whose ``execute`` behaves the same.
    - ``commit()`` is a no-op: the connection is read-only, there is never a
      local transaction to commit.
    - ``close()`` closes the underlying apsw connection (idempotent).
    - Any other attribute delegates to the underlying apsw.Connection, so
      pragmas via ``execute`` and properties like ``in_transaction`` work.

    Writes (INSERT/UPDATE/DELETE/DDL) raise :class:`apsw.ReadOnlyError
    <apsw.Error>` naturally: the VFS serves a read-only logical file.
    """

    def __init__(self, conn: "apsw.Connection") -> None:
        self._raw = conn
        self._closed = False

    def execute(self, sql: str, params=()) -> _ApswCursorAdapter:
        return _ApswCursorAdapter(self._raw, self._raw.cursor()).execute(sql, params)

    def cursor(self) -> _ApswCursorAdapter:
        return _ApswCursorAdapter(self._raw, self._raw.cursor())

    def commit(self) -> None:
        pass  # read-only connection: nothing to commit

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._raw.close()

    def __getattr__(self, name: str):
        # Delegate everything else (in_transaction, blobopen, ...) to the
        # underlying apsw.Connection. Guarded lookup avoids infinite
        # recursion if __getattr__ fires before __init__ completed.
        raw = self.__dict__.get("_raw")
        if raw is None:
            raise AttributeError(name)
        return getattr(raw, name)


class _ApswWriteConnAdapter(_ApswConnAdapter):
    """sqlite3.Connection-shaped wrapper over a WRITABLE apsw.Connection.

    Extends :class:`_ApswConnAdapter` (execute/cursor/close/``__getattr__``
    delegation are inherited) for the VFS WRITE path (M7-A2): ``commit()``
    is REAL and ``execute`` allows DML — writes land in the session's local
    ``-wal`` sidecar via the writable VFS session.

    Transaction semantics on ``write()`` fns (apsw vs sqlite3): apsw has NO
    implicit transactions. A fn using explicit ``BEGIN``/``COMMIT`` gets one
    committed transaction; a fn doing plain DML (no explicit BEGIN) then
    ``commit()`` auto-commits statement-by-statement (each INSERT is its own
    committed transaction, and ``commit()`` is a harmless no-op). A delta may
    therefore carry SEVERAL committed transactions — TSD1 and
    :func:`tessera.delta.read_wal_frames` handle that (all committed frames
    are collected in order, ``page_count`` from the last commit frame).

    Note: with the sqlite3 plain path a plain-DML fn without ``commit()``
    would roll back; over apsw it auto-commits per statement. Callers that
    care about atomicity must use explicit BEGIN/COMMIT on either path.
    """

    def commit(self) -> None:
        """Real commit: issues COMMIT when a transaction is open.

        apsw.Connection has no ``commit()`` method (committing is the COMMIT
        statement); a bare COMMIT with no open transaction would raise, so
        this is a no-op unless ``in_transaction`` — matching :mod:`sqlite3`'s
        no-op commit outside a transaction.
        """
        if self._raw.in_transaction:
            self._raw.execute("COMMIT")

    def rollback(self) -> None:
        """sqlite3-shaped rollback (no-op outside a transaction)."""
        if self._raw.in_transaction:
            self._raw.execute("ROLLBACK")


__all__ = [
    "apsw",
    "BackendError",
    "TesseraVFS",
    "_ApswConnAdapter",
    "_ApswCursorAdapter",
    "_ApswWriteConnAdapter",
]
