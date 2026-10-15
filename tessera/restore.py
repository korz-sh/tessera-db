# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Recovery CLI: ``python -m tessera.restore``.

Exists because at 3 AM, with a tenant on fire, nobody wants to write a
script. Three things and nothing else:

    # how far back can I go?
    python -m tessera.restore --tenant acme --list

    # what was there at 14:32? (touches nothing)
    python -m tessera.restore --tenant acme --at "2026-09-01 14:32" --dry-run

    # restore
    python -m tessera.restore --tenant acme --at "2026-09-01 14:32"

The backend comes from environment variables (the same ones the rest uses):
``BUCKET`` + ``TABLE`` + ``REGION`` for S3/DynamoDB, or ``TESSERA_LOCAL_ROOT``
for the local backend. ``--dry-run`` is the default for an ambiguous ``--at``:
the tool shows what it would pick and what it contains BEFORE changing
anything.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
from typing import Any

from tessera.store import Store, VersionUnavailable


def _build_store() -> Store:
    """Store from the environment; clear message if config is missing."""
    local_root = os.environ.get("TESSERA_LOCAL_ROOT", "").strip()
    if local_root:
        from tessera.backends.local import LocalBackend

        backend: Any = LocalBackend(local_root)
    else:
        bucket = os.environ.get("BUCKET", "").strip()
        table = os.environ.get("TABLE", "").strip()
        if not bucket or not table:
            raise SystemExit(
                "missing configuration: set BUCKET and TABLE (S3+DynamoDB "
                "backend) or TESSERA_LOCAL_ROOT (local backend)."
            )
        from tessera.backends.s3dynamo import S3DynamoBackend

        backend = S3DynamoBackend(
            bucket=bucket,
            table_name=table,
            region=os.environ.get("REGION", "us-east-1"),
            express_bucket=os.environ.get("EXPRESS_BUCKET", "").strip() or None,
        )
    root = os.environ.get("TESSERA_CACHE_ROOT", "/tmp/tessera-restore")
    return Store(backend, root=root)


def _parse_when(raw: str) -> float:
    """'2026-09-01 14:32' | '2026-09-01T14:32:00' | epoch -> unix timestamp."""
    raw = raw.strip()
    try:
        return float(raw)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(raw, fmt).timestamp()
        except ValueError:
            continue
    try:
        return _dt.datetime.fromisoformat(raw).timestamp()
    except ValueError:
        raise SystemExit(
            f"could not parse {raw!r} as a date. Use "
            f"'2026-09-01 14:32', ISO-8601, or a unix timestamp."
        ) from None


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "?"
    return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _cmd_list(store: Store, tenant: str) -> int:
    entries = store.history(tenant)
    if not entries:
        print(f"tenant {tenant!r} has no recovery points")
        return 1
    print(f"recovery points for {tenant!r} (newest to oldest):\n")
    print(f"  {'version':>8}  {'replaced':<20}  state")
    for entry in entries:
        marker = "  <- CURRENT" if entry["current"] else ""
        print(
            f"  {entry['version']:>8}  {_fmt_ts(entry['timestamp']):<20}"
            f"  {'current' if entry['current'] else 'archived'}{marker}"
        )
    oldest = min(e["version"] for e in entries)
    print(
        f"\nyou can go back to version {oldest}. Anything older was "
        f"collected by GC (see history_retention_s)."
    )
    return 0


def _summarize(conn) -> dict:
    """Row count per table: what is in that point, without dumping it."""
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    return {
        name: conn.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        for name in tables
    }


# @spec tessera/main
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tessera.restore",
        description="Point-in-time recovery for a tenant.",
    )
    parser.add_argument("--tenant", required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", help="list recovery points")
    group.add_argument("--version", type=int, help="restore to this version")
    group.add_argument("--at", help="restore to the state at this instant")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be restored, without changing anything",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="do not ask for confirmation (for scripts)",
    )
    args = parser.parse_args(argv)

    store = _build_store()
    try:
        if args.list or (args.version is None and args.at is None):
            return _cmd_list(store, args.tenant)

        when = _parse_when(args.at) if args.at else None
        try:
            resolved = store._resolve_version(args.tenant, args.version, when)
            summary = store.read_at(
                args.tenant, _summarize, version=args.version, at=when
            )
        except VersionUnavailable as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

        print(f"tenant  : {args.tenant}")
        print(f"version : {resolved}")
        print("contents at that point:")
        for name, count in sorted(summary.items()):
            print(f"  {name}: {count} rows")

        if args.dry_run:
            print("\n--dry-run: nothing was changed.")
            return 0

        current = store.read(args.tenant, _summarize)
        print("\nCURRENT contents (will be replaced):")
        for name, count in sorted(current.items()):
            print(f"  {name}: {count} rows")

        if not args.yes:
            answer = input("\nrestore? [type 'yes' to confirm] ").strip()
            if answer.lower() not in {"yes"}:
                print("cancelled.")
                return 1

        manifest = store.restore(args.tenant, version=args.version, at=when)
        print(
            f"\ndone: {args.tenant} restored to the state of version "
            f"{resolved}, published as version {manifest.version}."
        )
        print(
            "the previous state is still archived: this restore is also "
            "reversible."
        )
        return 0
    finally:
        store.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
