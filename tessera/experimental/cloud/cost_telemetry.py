"""Opt-in, request-scoped AWS SDK call and HTTP attempt counts for cost studies.

Counts are not DynamoDB capacity units or billable AWS requests. The collector
records only service and operation names, never request arguments.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from threading import Lock

_LOCK = Lock()

@dataclass
# @spec cloud/call-counts
# @implements bounded-write-cost/cost-claims-carry-measured-evidence
class CallCounts:
    logical: Counter = field(default_factory=Counter)
    http_attempts: Counter = field(default_factory=Counter)


_active: CallCounts | None = None


def _record_call(model, **_kwargs):
    with _LOCK:
        if _active is not None:
            _active.logical[f"{model.service_model.service_name}.{model.name}"] += 1


def _record_result(model, parsed, **_kwargs):
    """Count responses plus botocore retries reported by the SDK."""
    metadata = parsed.get("ResponseMetadata", {}) if isinstance(parsed, dict) else {}
    retries = metadata.get("RetryAttempts", 0)
    if type(retries) is not int or retries < 0:
        return
    with _LOCK:
        if _active is not None:
            _active.http_attempts[
                f"{model.service_model.service_name}.{model.name}"] += retries + 1


# @spec cloud/register-session
def register_session(session):
    """Attach one observer to a boto3 session before its clients are created."""
    session.events.register("before-call.*.*", _record_call,
                            unique_id="tessera-cloud-cost-telemetry")
    session.events.register("after-call.*.*", _record_result,
                            unique_id="tessera-cloud-cost-telemetry-attempts")


# @spec cloud/begin-request
def begin_request():
    global _active
    counts = CallCounts()
    with _LOCK:
        if _active is not None:
            raise RuntimeError("AWS call collection already active")
        _active = counts
    return counts, counts


# @spec cloud/end-request
def end_request(token):
    global _active
    with _LOCK:
        if _active is not token:
            raise RuntimeError("AWS call collection scope changed")
        _active = None
