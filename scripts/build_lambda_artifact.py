"""Build a Linux x86_64 Python 3.12 Lambda ZIP from a clean TesseraDB tree."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from hashlib import sha256
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ROOT = Path(__file__).resolve().parents[1]
GENERATED_DIRS = {"__pycache__", ".pytest_cache", ".ruff_cache", ".venv",
                  "build", "dist"}
GENERATED_FILES = {"uv.lock"}


# @implements operator-readiness/explicit-aws-dependency-installation
def _generated(path):
    """Ignore tool output that never enters the built wheel or Lambda ZIP."""
    if str(path.relative_to(ROOT)) in GENERATED_FILES:
        return True
    return any(part in GENERATED_DIRS or part.endswith(".egg-info")
               for part in path.relative_to(ROOT).parts)


def _run(argv, *, cwd=ROOT, env=None):
    subprocess.run(argv, cwd=cwd, env=env, check=True)


# @spec scripts/build
def build(output):
    if (ROOT / ".git").exists():
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT).strip():
            raise RuntimeError("commit or stash changes before building a release artifact")
    else:
        manifest_path = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
        if not manifest_path.is_file():
            raise RuntimeError("source snapshot manifest is required outside a Git checkout")
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("format") != "tessera-public-source-snapshot-1":
            raise RuntimeError("unsupported source snapshot manifest")
        expected = manifest.get("files")
        if not isinstance(expected, dict) or not expected:
            raise RuntimeError("source snapshot file hashes are missing")
        for name, digest in expected.items():
            if not isinstance(name, str) or name.startswith("/") or ".." in Path(name).parts:
                raise RuntimeError("source snapshot contains an invalid path")
            path = ROOT / name
            if (path.is_symlink() or not path.is_file()
                    or sha256(path.read_bytes()).hexdigest() != digest):
                raise RuntimeError(f"source snapshot file changed: {name}")
        actual = {str(path.relative_to(ROOT)) for path in ROOT.rglob("*")
                  if path.is_file() and not _generated(path)}
        if actual != set(expected) | {"PUBLIC_SOURCE_MANIFEST.json"}:
            raise RuntimeError("source snapshot contains missing or unexpected files")
    output = Path(output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("UV_CACHE_DIR", "/tmp/tessera-lambda-uv-cache")
    with tempfile.TemporaryDirectory(prefix="tessera-lambda-build-") as temporary:
        root = Path(temporary)
        wheel_dir = root / "wheel"
        package_dir = root / "package"
        _run(["uv", "build", "--wheel", "--out-dir", str(wheel_dir)], env=env)
        wheels = list(wheel_dir.glob("tessera_db-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("expected exactly one tessera-db wheel")
        requirements = ROOT / "requirements-lambda.txt"
        if not requirements.is_file():
            raise RuntimeError("hashed Lambda dependency lock is missing")
        _run(["uv", "pip", "install", "--target", str(package_dir),
              "--python-version", "3.12",
              "--python-platform", "x86_64-manylinux_2_28", "--no-build",
              "--require-hashes", "-r", str(requirements)], env=env)
        # The first install pins every third-party distribution by version and
        # hash. The locally built project wheel is installed without resolving
        # again; its dependencies are already present from the lock above.
        _run(["uv", "pip", "install", "--target", str(package_dir),
              "--no-deps", str(wheels[0])], env=env)
        # uv records a temporary wheel path and install-cache timestamp. They
        # are irrelevant in Lambda and would change the ZIP on every build.
        # Remove both files and their RECORD rows together.
        project_infos = list(package_dir.glob("tessera_db-*.dist-info"))
        if len(project_infos) != 1:
            raise RuntimeError("expected one TesseraDB distribution")
        project_info = project_infos[0]
        record = project_info / "RECORD"
        if not record.is_file():
            raise RuntimeError("expected local wheel provenance metadata")
        lines = record.read_text().splitlines()
        markers = tuple(project_info.name + "/" + name + ","
                        for name in ("direct_url.json", "uv_cache.json"))
        for name, marker in zip(("direct_url.json", "uv_cache.json"), markers):
            if not (project_info / name).is_file() or sum(
                    line.startswith(marker) for line in lines) != 1:
                raise RuntimeError(f"expected one {name} RECORD row")
            (project_info / name).unlink()
        record.write_text("\n".join(line for line in lines
                                    if not line.startswith(markers)) + "\n")
        # Installed CLI entry points embed the builder's interpreter path and
        # have no role in the Lambda handler. Omit them and their RECORD rows.
        for metadata_record in package_dir.glob("*.dist-info/RECORD"):
            rows = metadata_record.read_text().splitlines()
            if any(row.startswith("bin/") for row in rows):
                metadata_record.write_text("\n".join(
                    row for row in rows if not row.startswith("bin/")) + "\n")
        upstream_license = ROOT / "licenses/pyturso-0.7.2-LICENSE.md"
        if (not upstream_license.is_file() or sha256(upstream_license.read_bytes()).hexdigest()
                != "b646f9ee8bcaf87e8de75153b9df7a2861c7ac445c87e741768b3c2bccf47bc5"):
            raise RuntimeError("pinned pyturso 0.7.2 license text is missing or changed")
        pyturso_metadata = list(package_dir.glob("pyturso-0.7.2.dist-info"))
        if len(pyturso_metadata) != 1:
            raise RuntimeError("expected the pinned pyturso 0.7.2 distribution")
        notice = pyturso_metadata[0] / "licenses/LICENSE.md"
        notice.parent.mkdir(parents=True, exist_ok=True)
        notice.write_bytes(upstream_license.read_bytes())
        check_env = env.copy()
        check_env["PYTHONPATH"] = str(package_dir)
        check_env["PYTHONDONTWRITEBYTECODE"] = "1"
        _run([sys.executable, "-c",
              ("import pathlib,tessera,turso; "
               "from tessera.experimental.cloud.lambda_handler import lambda_handler; "
               "assert pathlib.Path(tessera.__file__).is_relative_to(pathlib.Path(__import__('os').environ['PYTHONPATH'])); "
               "assert pathlib.Path(turso.__file__).is_relative_to(pathlib.Path(__import__('os').environ['PYTHONPATH']))")],
             cwd=root, env=check_env)
        with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=6,
                     strict_timestamps=False) as archive:
            for path in sorted(package_dir.rglob("*")):
                if path.is_file():
                    if path.is_symlink():
                        raise RuntimeError(f"Lambda package contains symlink: {path}")
                    relative = path.relative_to(package_dir)
                    if (relative.parts[0] == "bin" or "__pycache__" in relative.parts
                            or path.suffix == ".pyc"):
                        continue
                    info = ZipInfo(str(relative), date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type = ZIP_DEFLATED
                    info.create_system = 3
                    info.external_attr = 0o644 << 16
                    archive.writestr(info, path.read_bytes(), compress_type=ZIP_DEFLATED,
                                     compresslevel=6)
        with ZipFile(output) as archive:
            for name in ("tessera/experimental/cloud/lambda_handler.py",
                         "tessera/experimental/cloud/service.py"):
                if name not in archive.namelist():
                    raise RuntimeError(f"Lambda ZIP missing {name}")
    print(f"{output} ({output.stat().st_size} bytes)")


# @spec scripts/build-lambda-artifact-main

# @spec scripts/main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    build(args.output)


if __name__ == "__main__":
    main()
