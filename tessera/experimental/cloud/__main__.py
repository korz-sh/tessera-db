"""Run the experimental single-process TesseraDB SQL service."""

from __future__ import annotations

import argparse
import os

from tessera.backends.local import LocalBackend

from .service import DatabaseService, serve


# @spec cloud/main
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("local", "aws"), default="local")
    parser.add_argument("--objects-root", default="./tessera-objects")
    parser.add_argument("--bucket")
    parser.add_argument("--table")
    parser.add_argument("--region")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--behind-tls-proxy", action="store_true",
                        help="required to bind beyond loopback; proxy must enforce TLS")
    parser.add_argument("--allow-legacy-root-create", action="store_true",
                        help="compatibility mode for pre-tenant deployments")
    args = parser.parse_args(argv)

    if args.host not in ("127.0.0.1", "::1", "localhost") and not args.behind_tls_proxy:
        parser.error("non-loopback bind requires --behind-tls-proxy")

    admin_token = os.environ.get("TESSERA_ADMIN_TOKEN", "")
    if len(admin_token) < 32:
        parser.error("TESSERA_ADMIN_TOKEN must have at least 32 characters")
    if args.backend == "local":
        backend = LocalBackend(args.objects_root)
        catalog_backend = backend
    else:
        if not all((args.bucket, args.table, args.region)):
            parser.error("AWS backend requires --bucket, --table and --region")
        import boto3

        from tessera.backends.s3dynamo import S3DynamoBackend
        from tessera.experimental.turso.fencing import FencedBackend

        session = boto3.Session(region_name=args.region)
        dynamodb = session.client("dynamodb")
        catalog_backend = S3DynamoBackend(
            args.bucket, args.table, args.region,
            s3_client=session.client("s3"), dynamodb=session.resource("dynamodb"),
        )
        backend = FencedBackend(catalog_backend, dynamodb)

    server = serve(DatabaseService(backend, admin_token=admin_token,
                                   catalog_backend=catalog_backend,
                                   allow_legacy_root_create=args.allow_legacy_root_create),
                   args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
