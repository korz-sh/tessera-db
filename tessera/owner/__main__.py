# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Container entrypoint (``python -m tessera.owner``)."""

import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Env vars the entrypoint REQUIRES — without any of them the process aborts
# with exit code 2 BEFORE touching AWS.
_ENV_REQUIRED = ("TESSERA_TENANT", "TESSERA_BUCKET", "TESSERA_TABLE")


def _main() -> int:
    """Env-driven entrypoint for container deployments (Fargate, Docker...).

    Builds one :class:`~tessera.backends.s3dynamo.S3DynamoBackend` and one
    :class:`Owner` purely from environment variables, then blocks on
    :meth:`Owner.start` and maps the boolean to the process exit code:

    - ``TESSERA_TENANT`` / ``TESSERA_BUCKET`` / ``TESSERA_TABLE`` (required):
      tenant to own, store bucket and lease table — the same naming
      conventions as ``examples/template.yaml``
      (``tessera-prod-store-ACCOUNT`` / ``tessera-prod-leases``).
    - ``TESSERA_REGION`` (default ``us-east-1``).
    - ``TESSERA_TTL_MS`` (default ``30000``), ``TESSERA_PUBLISH_EVERY_S``
      (default ``1.0``), ``TESSERA_PUBLISH_MODE`` (``per_commit`` default
      or ``windowed`` — see :class:`~tessera.owner.Owner`), ``TESSERA_IDLE_S``
      (default ``300``), ``TESSERA_COMPACT_EVERY_S`` (default ``60``).
    - ``TESSERA_HOST`` (default ``0.0.0.0``) and ``TESSERA_PORT`` (default
      ``8080``): the HTTP bind — ``0.0.0.0`` so the task's awsvpc ENI is
      reachable by the dispatcher.

    Exit codes: ``0`` clean exit (idle timeout or ``stop()``), ``1`` fencing
    loss or boot failure, ``2`` missing/invalid configuration.
    """
    from ..backends.s3dynamo import S3DynamoBackend
    from .core import Owner

    logging.basicConfig(level=logging.INFO)

    def _required(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            print(
                f"error: missing required environment variable {name} "
                f"(required: {', '.join(_ENV_REQUIRED)})",
                file=sys.stderr,
            )
            raise SystemExit(2)
        return value

    def _number(
        name: str, cast: type[int] | type[float], default: int | float
    ) -> int | float:
        raw = os.environ.get(name, "").strip()
        if not raw:
            return default
        try:
            return cast(raw)
        except ValueError:
            print(
                f"error: {name}={raw!r} is not a valid number ({cast.__name__})",
                file=sys.stderr,
            )
            raise SystemExit(2) from None

    tenant = _required("TESSERA_TENANT")
    bucket = _required("TESSERA_BUCKET")
    table = _required("TESSERA_TABLE")
    region = os.environ.get("TESSERA_REGION", "").strip() or "us-east-1"

    # Routable address to publish in the lease endpoint. Priority:
    # TESSERA_ADVERTISE_HOST (explicit) > TESSERA_ADVERTISE_MODE=public
    # (task's public IP via checkip.amazonaws.com — needed when dispatchers
    # live OUTSIDE the VPC, e.g. non-VPC Lambdas) > the task's private IP
    # from ECS container metadata (dispatchers in the same VPC) > the bind
    # host (fine outside containers).
    advertise = os.environ.get("TESSERA_ADVERTISE_HOST", "").strip() or None
    if advertise is None and (
        os.environ.get("TESSERA_ADVERTISE_MODE", "").strip().lower() == "public"
    ):
        try:
            import urllib.request

            with urllib.request.urlopen(
                "https://checkip.amazonaws.com", timeout=3
            ) as resp:
                advertise = resp.read().decode("ascii").strip() or None
        except Exception:  # noqa: BLE001 - best-effort
            logger.warning(
                "owner %s: could not resolve public IP; falling back to private",
                tenant,
            )
    if advertise is None and os.environ.get("ECS_CONTAINER_METADATA_URI_V4"):
        try:
            import urllib.request

            with urllib.request.urlopen(
                os.environ["ECS_CONTAINER_METADATA_URI_V4"] + "/task", timeout=1
            ) as resp:
                meta = json.load(resp)
            for container in meta.get("Containers", ()):
                for network in container.get("Networks", ()):
                    addresses = network.get("IPv4Addresses") or ()
                    if addresses:
                        advertise = str(addresses[0])
                        break
                if advertise:
                    break
        except Exception:  # noqa: BLE001 - metadata is best-effort
            logger.warning(
                "owner %s: could not resolve the task IP from ECS "
                "metadata; publishing the bind host",
                tenant,
            )

    try:
        owner = Owner(
            tenant,
            S3DynamoBackend(
                bucket,
                table,
                region,
                express_bucket=os.environ.get("TESSERA_EXPRESS_BUCKET", "").strip()
                or None,
            ),
            http_host=os.environ.get("TESSERA_HOST", "").strip() or "0.0.0.0",
            http_port=_number("TESSERA_PORT", int, 8080),
            advertise_host=advertise,
            ttl_ms=_number("TESSERA_TTL_MS", int, 30000),
            publish_every_s=_number("TESSERA_PUBLISH_EVERY_S", float, 1.0),
            publish_mode=os.environ.get(
                "TESSERA_PUBLISH_MODE", "per_commit"
            ).strip().lower() or "per_commit",
            idle_s=_number("TESSERA_IDLE_S", float, 300.0),
            compact_every_s=_number("TESSERA_COMPACT_EVERY_S", float, 60.0),
            acquire_wait_s=_number("TESSERA_ACQUIRE_WAIT_S", float, 0.0),
        )
        result = owner.start()
    except Exception:  # noqa: BLE001 - boot failure: clean log + exit 1
        logger.exception("owner %s: failed to start", tenant)
        return 1
    return 0 if result else 1


if __name__ == "__main__":  # pragma: no cover - exercised in containers
    raise SystemExit(_main())
