"""Public API contract: what may NOT change without a MAJOR bump.

This file is a safety net, not a behaviour test. It freezes the signatures
and types that a consumer already wrote code against for v1.0.0. If a change
turns something here red, the question is not "how do I fix the test?" but
"do I really want to ship a MAJOR?".

Golden rule: a consumer pinning ``tessera-db>=1.1,<2`` must be able to
upgrade within that band without reading the changelog and without anything
breaking.

What IS breaking (and therefore MAJOR):

- removing a parameter, renaming it, or changing the positional order;
- removing the default from a parameter that had one (it makes an argument
  the consumer does not pass mandatory);
- removing a public symbol or changing its base type;
- breaking the on-disk format or relaxing a v1.0 guarantee.

What is NOT breaking: adding keyword parameters WITH defaults at the end,
adding new functions, modules, or exceptions.
"""

from __future__ import annotations

import inspect
import tomllib
from pathlib import Path

import pytest

import tessera
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
from tessera.owner import Owner
from tessera.store import (
    Conflict,
    StaleRead,
    Store,
    TenantExists,
    TenantNotFound,
)

# Signatures frozen at v1.0.0. The positional list is EXACT (order included);
# keyword-with-default parameters are listed separately because adding a new
# one is MINOR and must not break this test.
V1_POSITIONAL = {
    "Store.__init__": ["self", "backend"],
    "Store.create": ["self", "tenant", "schema"],
    "Store.write": ["self", "tenant", "fn"],
    "Store.read": ["self", "tenant", "fn"],
    "Store.compact": ["self", "tenant"],
    "Store.delete": ["self", "tenant"],
    "S3DynamoBackend.__init__": ["self", "bucket", "table_name", "region"],
    "LocalBackend.__init__": ["self", "root_dir"],
    "Owner.__init__": ["self", "tenant", "backend"],
}

# Parameters that had a default at v1.0.0 and must keep it: removing it
# forces the consumer to pass something it does not pass today.
V1_MUST_HAVE_DEFAULTS = {
    "Store.__init__": [
        "ttl_ms", "retain_ms", "read_wait_ms", "read_poll_ms", "lru_bytes",
        "root", "inline_compact", "gc_delta_age_s", "gc_keep_snapshots",
        "use_vfs",
    ],
    "Store.create": ["schema"],
    "Store.read": ["min_version"],
    "Store.write": ["lease"],
    "S3DynamoBackend.__init__": ["region", "s3_client", "dynamodb"],
    "Owner.__init__": [
        "http_host", "http_port", "ttl_ms", "publish_every_s", "idle_s",
        "compact_every_s", "root",
    ],
}

_TARGETS = {
    "Store": Store,
    "S3DynamoBackend": S3DynamoBackend,
    "LocalBackend": LocalBackend,
    "Owner": Owner,
}


def _resolve(dotted: str):
    cls_name, meth_name = dotted.split(".")
    return getattr(_TARGETS[cls_name], meth_name)


@pytest.mark.parametrize("dotted,expected", sorted(V1_POSITIONAL.items()))
def test_positional_parameters_are_frozen(dotted: str, expected: list[str]):
    """The v1.0.0 positional prefix does not change: neither order nor names.

    A consumer may have called ``S3DynamoBackend(bucket, table, region)``
    positionally. Reordering or renaming breaks that code silently (or
    worse: it does not break and passes the bucket as the table).
    """
    params = list(inspect.signature(_resolve(dotted)).parameters)
    actual = params[: len(expected)]
    assert actual == expected, (
        f"BREAKING CHANGE in {dotted}: the positional parameters went from "
        f"{expected} to {actual}. This requires a MAJOR. To add parameters, "
        f"put them at the end and give them a default."
    )


@pytest.mark.parametrize("dotted,names", sorted(V1_MUST_HAVE_DEFAULTS.items()))
def test_optional_parameters_keep_their_defaults(dotted: str, names: list[str]):
    """What was optional at v1.0.0 stays optional.

    Removing the default from a parameter is breaking even if the signature
    "grows": the consumer that never passed it stops working.
    """
    params = inspect.signature(_resolve(dotted)).parameters
    for name in names:
        assert name in params, (
            f"BREAKING CHANGE in {dotted}: parameter {name!r} disappeared. "
            f"Requires MAJOR."
        )
        assert params[name].default is not inspect.Parameter.empty, (
            f"BREAKING CHANGE in {dotted}: parameter {name!r} lost its "
            f"default and became mandatory. Requires MAJOR."
        )


def test_exception_hierarchy_is_stable():
    """Exceptions are part of the contract: the consumer catches them.

    Changing an exception's base breaks ``except BackendError:`` in foreign
    code with no import error to reveal it.
    """
    for exc in (
        NotFound,
        CASFailed,
        TenantBusy,
        LeaseLost,
        TenantExists,
        TenantNotFound,
        Conflict,
        StaleRead,
    ):
        assert issubclass(exc, BackendError), (
            f"BREAKING CHANGE: {exc.__name__} stopped inheriting from "
            f"BackendError; a consumer's `except BackendError` stops "
            f"catching it."
        )
        assert issubclass(exc, Exception)


def test_backend_contract_methods_exist():
    """The ``Backend`` contract is what a backend author implements."""
    for method in (
        "get",
        "head",
        "put",
        "delete",
        "list",
        "mtime",
        "acquire_lease",
        "renew_lease",
        "release_lease",
    ):
        assert hasattr(Backend, method), (
            f"BREAKING CHANGE: the Backend contract lost {method!r}; "
            f"any external backend stops complying."
        )


def test_lease_fields_are_stable():
    """``Lease`` travels to the consumer: its fields are contract."""
    fields = set(Lease.__dataclass_fields__)
    required = {"tenant", "owner", "epoch", "expires_at"}
    assert required <= fields, (
        f"BREAKING CHANGE: Lease lost fields {required - fields}."
    )


def test_put_supports_both_cas_conditions():
    """CAS is the central guarantee: ``if_match`` and ``if_none_match`` stay."""
    params = inspect.signature(Backend.put).parameters
    assert "if_match" in params and "if_none_match" in params, (
        "BREAKING CHANGE: Backend.put lost a CAS condition. The manifest CAS "
        "and the write-once deltas both depend on them."
    )


def test_version_is_consistent_across_sources():
    """``pyproject.toml`` and ``tessera.__version__`` cannot diverge.

    If they diverge, an installed package reports a version it does not
    have: impossible to diagnose in production.
    """
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    declared = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert declared == tessera.__version__, (
        f"pyproject.toml says {declared!r} and tessera.__version__ says "
        f"{tessera.__version__!r}. They must be bumped together."
    )


# ---------------------------------------------------------------------------
# Package public surface: what `tessera` exports and what importing it costs
# ---------------------------------------------------------------------------


def test_public_surface_is_importable_from_the_package_root():
    """``from tessera import X`` works for every declared surface symbol.

    It is the ergonomics promised to consumers: not having to know the
    submodule structure to use the basics.
    """
    import tessera as pkg

    for name in pkg.__all__:
        assert hasattr(pkg, name), (
            f"BREAKING CHANGE: {name!r} is in __all__ but cannot be "
            f"resolved. The public surface must always import."
        )


def test_public_surface_contents_are_frozen():
    """Removing a symbol from the surface is MAJOR.

    Adding one is MINOR (which is why the comparison is <=, not ==).
    """
    import tessera as pkg

    v1_surface = {
        "Store",
        "Manifest",
        "Backend",
        "LocalBackend",
        "S3DynamoBackend",
        "Lease",
        "Owner",
        "BackendError",
        "NotFound",
        "CASFailed",
        "TenantBusy",
        "LeaseLost",
        "TenantExists",
        "TenantNotFound",
        "Conflict",
        "StaleRead",
        "__version__",
    }
    missing = v1_surface - set(pkg.__all__)
    assert not missing, (
        f"BREAKING CHANGE: missing from the public surface: {missing}. "
        f"Requires MAJOR."
    )


def test_importing_the_package_does_not_pull_heavy_dependencies():
    """``import tessera`` does not pull in boto3 or apsw.

    A consumer that only uses the local backend (tests, dev) should not pay
    the cost of importing the AWS stack. Verified in a clean subprocess
    because the test interpreter already has everything loaded.
    """
    import subprocess
    import sys

    code = (
        "import sys, tessera; "
        "print('boto3' in sys.modules, 'apsw' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "False False", (
        f"`import tessera` loaded heavy dependencies ({out}). The public "
        f"surface must resolve lazily (PEP 562)."
    )


def test_unknown_attribute_error_lists_the_public_surface():
    """An unknown symbol's error shows what the public surface is."""
    import tessera as pkg

    with pytest.raises(AttributeError, match="The public surface is"):
        pkg.EstoNoExiste
