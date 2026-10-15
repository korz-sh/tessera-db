"""Versioned identity and authorization boundary for the Turso runtime.

The group, not a row-level tenant label, is the security and publication
boundary. Authentication remains the embedding service's responsibility; this
module consumes an already-authenticated principal and fails before owner work.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable, Iterable


IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,64}")


# @spec turso/invalid-identity
class InvalidIdentity(ValueError):
    pass


# @spec turso/authorization-denied
class AuthorizationDenied(PermissionError):
    pass


# @spec turso/validate-identifier
def validate_identifier(value: object, *, name: str) -> str:
    if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None:
        raise InvalidIdentity(
            f"{name} must be 1..64 letters, digits, underscores or hyphens"
        )
    return value


@dataclass(frozen=True)
# @spec turso/group-boundary
class GroupBoundary:
    """Canonical membership of one independently authorized transaction group."""

    group: str
    tenants: tuple[str, ...]
    mode: str
    version: int = 1

    def __post_init__(self) -> None:
        validate_identifier(self.group, name="group")
        if self.version != 1:
            raise InvalidIdentity("unsupported group boundary version")
        if not isinstance(self.tenants, tuple):
            raise InvalidIdentity("canonical boundary tenants must be a tuple")
        for tenant in self.tenants:
            validate_identifier(tenant, name="tenant")
        if self.mode == "isolated":
            if len(self.tenants) != 1:
                raise InvalidIdentity("isolated boundary needs exactly one tenant")
        elif self.mode == "shared":
            if len(self.tenants) < 2 or tuple(sorted(set(self.tenants))) != self.tenants:
                raise InvalidIdentity("shared boundary tenants must be unique and sorted")
        else:
            raise InvalidIdentity("invalid group boundary mode")

    @classmethod
    def isolated(cls, group: str, tenant: str | None = None) -> "GroupBoundary":
        group = validate_identifier(group, name="group")
        tenant = group if tenant is None else validate_identifier(tenant, name="tenant")
        return cls(group=group, tenants=(tenant,), mode="isolated")

    @classmethod
    def shared(cls, group: str, tenants: Iterable[str]) -> "GroupBoundary":
        group = validate_identifier(group, name="group")
        if isinstance(tenants, (str, bytes)):
            raise InvalidIdentity("shared tenants must be a collection of IDs")
        try:
            values = tuple(validate_identifier(value, name="tenant") for value in tenants)
        except TypeError as exc:
            raise InvalidIdentity("shared tenants must be an iterable") from exc
        if len(values) < 2 or len(set(values)) != len(values):
            raise InvalidIdentity("a shared group needs at least two unique tenants")
        return cls(group=group, tenants=tuple(sorted(values)), mode="shared")

    @classmethod
    def from_dict(cls, value: object) -> "GroupBoundary":
        try:
            if not isinstance(value, dict) or value.get("version") != 1:
                raise InvalidIdentity("unsupported group boundary version")
            mode = value["mode"]
            group = value["group"]
            tenants = value["tenants"]
            expected = {"version", "mode", "group", "tenants"}
            if set(value) != expected:
                raise InvalidIdentity("invalid group boundary fields")
            if mode == "isolated":
                if not isinstance(tenants, list) or len(tenants) != 1:
                    raise InvalidIdentity("isolated boundary needs exactly one tenant")
                result = cls.isolated(group, tenants[0])
            elif mode == "shared":
                if not isinstance(tenants, list):
                    raise InvalidIdentity("shared boundary tenants must be a list")
                result = cls.shared(group, tenants)
            else:
                raise InvalidIdentity("invalid group boundary mode")
            if result.to_dict() != value:
                raise InvalidIdentity("group boundary is not canonical")
            return result
        except (KeyError, TypeError) as exc:
            raise InvalidIdentity("invalid group boundary") from exc

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "mode": self.mode,
            "group": self.group,
            "tenants": list(self.tenants),
        }


# @spec turso/require-group-access
def require_group_access(
    boundary: GroupBoundary,
    *,
    principal: object | None,
    authorize: Callable[[object, str], bool] | None,
) -> None:
    """Authorize an authenticated principal before any owner/backend work.

    Omitting both arguments is trusted embedded mode. Service adapters must
    provide both; a partial configuration fails closed.
    """

    if (principal is None) != (authorize is None):
        raise InvalidIdentity("principal and authorize must be supplied together")
    if authorize is None:
        return
    if not callable(authorize):
        raise InvalidIdentity("authorize must be callable")
    try:
        allowed = authorize(principal, boundary.group)
    except AuthorizationDenied:
        raise
    except Exception as exc:
        raise AuthorizationDenied("group authorization failed") from exc
    if allowed is not True:
        raise AuthorizationDenied("principal is not authorized for group")
