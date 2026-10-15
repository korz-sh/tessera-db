# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""tessera — one SQLite database per tenant on object storage.

Public surface of the package. What is exported here is covered by SemVer:
breaking one of these signatures requires a MAJOR bump, and tests verify it
(``tests/test_api_compat.py``).

Everything else — unlisted submodules, any name with a leading underscore,
``tessera.chunks``, ``tessera.delta``, ``tessera.vfs``, ``tessera.lease`` — is
**internal**: it may change in a PATCH without notice. If you need something
from there for a real use case, open an issue and we will promote it
explicitly instead of depending on an implementation detail.

Typical usage::

    from tessera import Store, S3DynamoBackend

    store = Store(S3DynamoBackend(bucket="...", table_name="...", region="..."))
    store.create("acme", "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")

Symbols are resolved **lazily** (PEP 562): ``import tessera`` does not pull in
``boto3`` or ``apsw``. A consumer that only uses the local backend does not
pay the cost of importing the AWS stack, and the package import stays in
microseconds. The real cost only shows up when you ask for the symbol that
needs it.

``tessera.orm`` (SQLAlchemy integration) is public but **optional**: it is not
exported from here because its dependency lives in an extra
(``pip install "tessera-db[orm]"``). Import it explicitly:
``from tessera.orm import TesseraORM``.
"""

from __future__ import annotations

# ``TYPE_CHECKING`` without importing ``typing``: importing typing cost ~7 ms
# (measured with -X importtime) and was 90% of the cost of ``import tessera``.
# Type checkers evaluate this constant statically, so the import block at the
# bottom is still visible to them.
TYPE_CHECKING = False

__version__ = "2.0.0"

# Symbol -> submodule map. The SINGLE source of truth for the public surface:
# __all__ derives from it, so the two cannot diverge.
_EXPORTS: dict[str, str] = {
    # Core
    "Store": "tessera.store",
    "Manifest": "tessera.manifest",
    # Backends
    "Backend": "tessera.backends.base",
    "LocalBackend": "tessera.backends.local",
    "S3DynamoBackend": "tessera.backends.s3dynamo",
    "Lease": "tessera.backends.base",
    # Hot owner (tier 2)
    "Owner": "tessera.owner",
    # Exceptions are contract: consumers catch them by name, so moving them
    # or changing their base is breaking (test_api_compat covers this).
    "BackendError": "tessera.backends.base",
    "NotFound": "tessera.backends.base",
    "CASFailed": "tessera.backends.base",
    "TenantBusy": "tessera.backends.base",
    "LeaseLost": "tessera.backends.base",
    "TenantExists": "tessera.store",
    "TenantNotFound": "tessera.store",
    "Conflict": "tessera.store",
    "StaleRead": "tessera.store",
    "VersionUnavailable": "tessera.store",
}

__all__ = ["__version__", *sorted(_EXPORTS)]


def __getattr__(name: str):
    """Resolves public symbols lazily (PEP 562)."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(
            f"module 'tessera' has no attribute {name!r}. "
            f"The public surface is: {', '.join(sorted(_EXPORTS))}. "
            f"Everything else is internal — import it from its submodule at "
            f"your own risk (it may change without a MAJOR bump)."
        )
    import importlib

    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value  # cache: the second access does not re-import
    return value


def __dir__() -> list[str]:
    """Autocomplete in REPL/IDE despite the lazy imports."""
    return sorted(__all__)


if TYPE_CHECKING:
    # Only for type checkers and IDEs: never runs at runtime, so it does not
    # break the lazy loading above.
    from tessera.backends.base import (
        Backend,
        BackendError,
        CASFailed,
        Lease,
        LeaseLost,
        NotFound,
        TenantBusy,
    )
    from tessera.backends.local import LocalBackend
    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.manifest import Manifest
    from tessera.owner import Owner
    from tessera.store import (
        Conflict,
        StaleRead,
        Store,
        TenantExists,
        TenantNotFound,
        VersionUnavailable,
    )
