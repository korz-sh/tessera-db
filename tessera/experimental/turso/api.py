"""Candidate v2 operation contract for the experimental Turso runtime."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _statement_kind(sql):
    """Return the top-level statement keyword, including WITH-prefixed SQL."""
    words = []
    token = []
    depth = 0
    quote = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            if token and depth == 0:
                words.append("".join(token).upper())
            token = []
            depth += 1
        elif char == ")":
            token = []
            depth = max(0, depth - 1)
        elif depth == 0 and (char.isalnum() or char == "_"):
            token.append(char)
        elif token:
            if depth == 0:
                words.append("".join(token).upper())
            token = []
        index += 1
    if token and depth == 0:
        words.append("".join(token).upper())
    if not words:
        return None
    if words[0] != "WITH":
        return words[0]
    return next(
        (word for word in words[1:] if word in ("SELECT", "INSERT", "UPDATE", "DELETE")),
        None,
    )


# @spec turso/check-sql
def check_sql(sql, *, read=False):
    allowed = ("SELECT",) if read else ("SELECT", "INSERT", "UPDATE", "DELETE")
    if (not isinstance(sql, str) or _statement_kind(sql) not in allowed
            or ";" in sql or "--" in sql or "/*" in sql
            or "_tessera_" in sql.lower()):
        raise ValueError("managed SQL subset required; reserved identifiers and semicolons prohibited")


def _operation_payload(operation_id, statements, *, max_statements, max_bytes):
    if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
        raise ValueError("operation ID must be a nonempty string of at most 128 characters")
    statements = list(statements)
    if not 1 <= len(statements) <= max_statements:
        raise ValueError(f"an operation needs 1..{max_statements} statements")
    for sql, params in statements:
        check_sql(sql)
        if not isinstance(params, (list, tuple)):
            raise ValueError("positional JSON parameters required")
    payload = _canonical(statements)
    if len(payload) > max_bytes:
        raise ValueError(f"operation exceeds {max_bytes // 1048576} MiB" if max_bytes >= 1048576
                         else "operation exceeds 64 KiB")
    return payload


@dataclass(frozen=True, init=False)
# @spec turso/operation
class Operation:
    """Canonical write payload identified for idempotent submission."""

    operation_id: str
    payload: bytes

    def __init__(self, operation_id, statements):
        payload = _operation_payload(operation_id, statements, max_statements=64,
                                     max_bytes=65536)
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "payload", payload)

    @property
    def fingerprint(self):
        return hashlib.sha256(self.payload).hexdigest()


# @spec turso/bulk-operation
class BulkOperation(Operation):
    """One bounded atomic transaction for a large scan or import.

    Queue admission must reserve the entire encoded payload. A caller should
    configure RuntimeLimits.max_pending_bytes to fit the intended operation.
    """

    def __init__(self, operation_id, statements):
        payload = _operation_payload(operation_id, statements,
                                     max_statements=8192, max_bytes=8 * 1048576)
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "payload", payload)


# Alpha compatibility: both names are the exact same accepted type.
Request = Operation


@dataclass(frozen=True)
# @spec turso/api-receipt
class Receipt:
    """Durable proof that an operation is present at a published frontier."""

    operation_id: str
    fingerprint: str
    version: int
    rows: list


# @spec turso/operation-state
class OperationState(str, Enum):
    ABSENT = "absent"
    REJECTED = "rejected"
    ADMITTED = "admitted"
    EXECUTING = "executing"
    COMMITTED_LOCAL = "committed_local"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
# @spec turso/operation-status
class OperationStatus:
    operation_id: str
    state: OperationState
    fingerprint: str | None = None
    receipt: Receipt | None = None

    def __post_init__(self):
        if not isinstance(self.operation_id, str) or not self.operation_id:
            raise ValueError("operation ID must be nonempty")
        if not isinstance(self.state, OperationState):
            raise TypeError("state must be an OperationState")
        if self.state is OperationState.PUBLISHED and self.receipt is None:
            raise ValueError("published status requires a receipt")
        if self.receipt is not None:
            if self.state is not OperationState.PUBLISHED:
                raise ValueError("only published status may contain a receipt")
            if (self.receipt.operation_id != self.operation_id
                    or self.receipt.fingerprint != self.fingerprint):
                raise ValueError("status and receipt identity must match")

    @property
    def outcome_known(self) -> bool:
        return self.state in {
            OperationState.ABSENT,
            OperationState.REJECTED,
            OperationState.PUBLISHED,
            OperationState.FAILED,
        }


# @spec turso/operation-error
class OperationError(Exception):
    admitted = False
    outcome_known = False


# @spec turso/operation-rejected
class OperationRejected(OperationError):
    outcome_known = True


# @spec turso/operation-failed
class OperationFailed(OperationError):
    admitted = True
    outcome_known = True


# @spec turso/operation-limit-exceeded
class OperationLimitExceeded(OperationFailed):
    """Admitted operation exceeded a runtime bound and was rolled back."""


# @spec turso/database-quota-exceeded
class DatabaseQuotaExceeded(OperationLimitExceeded):
    """A known quota rejection before any new frontier was published."""


# @spec turso/admission-limit-exceeded
class AdmissionLimitExceeded(OperationRejected):
    """Operation was rejected before admission by a capacity bound."""


# @spec turso/operation-timeout
class OperationTimeout(OperationError, TimeoutError):
    admitted = True


# @spec turso/operation-outcome-unknown
class OperationOutcomeUnknown(OperationError, RuntimeError):
    admitted = True


@runtime_checkable
# @spec turso/operation-api
class OperationAPI(Protocol):
    def submit(self, operation: Operation, *, timeout: float | None = None) -> Receipt: ...

    def read(self, sql: str, params: tuple | list = ()) -> list: ...

    def lookup(self, operation_id: str) -> OperationStatus: ...

    def close(self) -> None: ...
