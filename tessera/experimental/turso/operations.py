"""Safe, bounded operational diagnostics for the candidate Turso runtime."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import time

@dataclass(frozen=True)
# @spec turso/restore-plan
class RestorePlan:
    group: str
    source_version: int
    current_version: int
    publish_version: int
    source_artifacts: tuple[str, ...]
    confirmation: str


# @spec turso/operator
class Operator:
    """Read-mostly operator facade with explicit recovery preflights."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.owner = runtime._owner
        self.storage = self.owner.storage
        self.lifecycle = self.owner.lifecycle()
        self.group = self.owner.boundary.group

    def _manifest(self):
        return self.storage.manifest()[0]

    def history(self):
        return [
            {
                "group": self.group,
                "version": version,
                "epoch": manifest["epoch"],
                "artifacts": len(self.storage.artifact_keys(manifest)),
            }
            for version in self.lifecycle.versions()
            for manifest in (self.lifecycle.manifest_at(version),)
        ]

    def inspect_group(self):
        manifest = self._manifest()
        keys = sorted({
            key for version in self.lifecycle.versions()
            for key in self.storage.artifact_keys(self.lifecycle.manifest_at(version))
        })
        retained_bytes = sum(len(self.storage.backend.get(key)[0]) for key in keys)
        report = {
            "group": self.group,
            "boundary": self.owner.boundary.mode,
            "version": manifest["version"],
            "epoch": manifest["epoch"],
            "retained_versions": self.lifecycle.versions(),
            "retained_bytes": retained_bytes,
            "checkpoint_lag_versions": 0,
            "gc_lag_versions": len(self.lifecycle.versions()) - 1,
            "gc": self.lifecycle.gc(dry_run=True),
        }
        return report

    def lookup_operation(self, group, operation_id):
        if group != self.group:
            raise ValueError("exact group does not match this runtime")
        status = self.runtime.lookup(operation_id)
        result = {
            "group": group,
            "operation_id": operation_id,
            "state": status.state.value,
            "fingerprint": status.fingerprint,
        }
        if status.receipt is not None:
            result["version"] = status.receipt.version
        return result

    def doctor(self):
        manifest = self._manifest()
        lease = self.owner._lease
        heartbeat = self.owner._heartbeat_error
        return {
            "healthy": heartbeat is None,
            "group": self.group,
            "frontier": {"version": manifest["version"], "epoch": manifest["epoch"]},
            "owner": {
                "identity": hashlib.sha256(lease.owner.encode()).hexdigest()[:12],
                "epoch": lease.epoch,
                "lease_deadline": lease.expires_at,
                "lease_remaining_seconds": max(0.0, lease.expires_at - time.time()),
                "renewal_healthy": heartbeat is None,
            },
            "runtime": self.runtime.metrics(),
            "cache": self.runtime.read_metrics(),
            "recovery": self.inspect_group(),
        }

    def export(self, version, destination):
        return self.lifecycle.export(version, Path(destination))

    def restore(self, version, *, dry_run=True, plan=None):
        if dry_run:
            current = self._manifest()["version"]
            source = self.lifecycle.manifest_at(version)
            raw = {
                "group": self.group,
                "source_version": version,
                "current_version": current,
                "publish_version": current + 1,
                "source_artifacts": tuple(sorted(self.storage.artifact_keys(source))),
            }
            confirmation = hashlib.sha256(
                json.dumps(raw, sort_keys=True).encode()
            ).hexdigest()[:16]
            return RestorePlan(**raw, confirmation=confirmation)
        if not isinstance(plan, RestorePlan):
            raise ValueError("restore execution requires its exact preflight plan")
        expected = self.restore(version, dry_run=True)
        if plan != expected:
            raise ValueError("restore preflight is stale or does not match the exact target")
        return self.lifecycle.restore(version)

    @staticmethod
    def as_json(value):
        if isinstance(value, RestorePlan):
            value = asdict(value)
        return json.dumps(value, sort_keys=True, indent=2)
