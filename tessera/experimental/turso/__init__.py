"""Native Turso runtime for opt-in Tessera alpha evaluation.

Install with the experimental-turso extra. Symbols load lazily so merely
importing this namespace does not import the optional engine.
"""

from __future__ import annotations

_EXPORTS = {
    "CallbackStore": "tessera.experimental.turso.callback_store",
    "BulkOperation": "tessera.experimental.turso.api",
    "Operation": "tessera.experimental.turso.api",
    "OperationAPI": "tessera.experimental.turso.api",
    "OperationError": "tessera.experimental.turso.api",
    "OperationFailed": "tessera.experimental.turso.api",
    "OperationLimitExceeded": "tessera.experimental.turso.api",
    "OperationOutcomeUnknown": "tessera.experimental.turso.api",
    "OperationRejected": "tessera.experimental.turso.api",
    "AdmissionLimitExceeded": "tessera.experimental.turso.api",
    "OperationState": "tessera.experimental.turso.api",
    "OperationStatus": "tessera.experimental.turso.api",
    "Operator": "tessera.experimental.turso.operations",
    "OperationTimeout": "tessera.experimental.turso.api",
    "PublishedCacheCorrupt": "tessera.experimental.turso.reads",
    "PublishedReader": "tessera.experimental.turso.reads",
    "PublishedVersionUnavailable": "tessera.experimental.turso.reads",
    "BatchOwner": "tessera.experimental.turso.owner",
    "FencedBackend": "tessera.experimental.turso.fencing",
    "IdentityConflict": "tessera.experimental.turso.owner",
    "AuthorizationDenied": "tessera.experimental.turso.identity",
    "GroupBoundary": "tessera.experimental.turso.identity",
    "InvalidIdentity": "tessera.experimental.turso.identity",
    "IncrementalStore": "tessera.experimental.turso.incremental",
    "InvalidSnapshot": "tessera.experimental.turso.snapshot",
    "LifecycleManager": "tessera.experimental.turso.lifecycle",
    "RetentionPolicy": "tessera.experimental.turso.lifecycle",
    "SchemaMismatch": "tessera.experimental.turso.compatibility",
    "SchemaRequirement": "tessera.experimental.turso.compatibility",
    "UnsupportedMigration": "tessera.experimental.turso.compatibility",
    "UpgradeEvidence": "tessera.experimental.turso.compatibility",
    "apply_additive_migration": "tessera.experimental.turso.compatibility",
    "rehearse_engine_upgrade": "tessera.experimental.turso.compatibility",
    "require_schema": "tessera.experimental.turso.compatibility",
    "schema_inventory": "tessera.experimental.turso.compatibility",
    "NativeJournalOwner": "tessera.experimental.turso.journal",
    "NativeLogStore": "tessera.experimental.turso.native",
    "NativeRecoveryError": "tessera.experimental.turso.journal",
    "QueueFull": "tessera.experimental.turso.queue",
    "QueuedOwner": "tessera.experimental.turso.queue",
    "Receipt": "tessera.experimental.turso.api",
    "RestorePlan": "tessera.experimental.turso.operations",
    "Request": "tessera.experimental.turso.api",
    "SnapshotStore": "tessera.experimental.turso.snapshot",
    "Runtime": "tessera.experimental.turso.runtime",
    "RuntimeLimits": "tessera.experimental.turso.runtime",
    "RemoteFrontierCorrupt": "tessera.experimental.turso.reads",
    "UnknownOutcome": "tessera.experimental.turso.snapshot",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module 'tessera.experimental.turso' has no attribute {name!r}")
    import importlib

    try:
        value = getattr(importlib.import_module(module_name), name)
    except ModuleNotFoundError as exc:
        if exc.name == "turso":
            raise ModuleNotFoundError(
                "Turso experiments require: pip install "
                "'tessera-db[experimental-turso]'"
            ) from exc
        raise
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
