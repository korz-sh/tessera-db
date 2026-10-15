"""Inventory dependency license metadata and notices inside a Lambda ZIP."""

from __future__ import annotations

import argparse
import json
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile


# @spec scripts/audit
def audit(path):
    with ZipFile(path) as archive:
        names = set(archive.namelist())
        records = []
        for metadata_path in sorted(name for name in names
                                    if name.endswith(".dist-info/METADATA")):
            metadata = Parser().parsestr(archive.read(metadata_path).decode())
            root = metadata_path.rsplit("/", 1)[0] + "/"
            notices = sorted(name for name in names
                             if name.startswith(root) and (
                                 "/licenses/" in name or name.rsplit("/", 1)[-1].lower()
                                 in {"license", "license.txt", "license.md", "notice"}))
            records.append({
                "name": metadata.get("Name"), "version": metadata.get("Version"),
                "license_expression": metadata.get("License-Expression"),
                "license_field": metadata.get("License"),
                "license_classifiers": [value for value in metadata.get_all("Classifier", [])
                                        if value.startswith("License ::")],
                "notice_files": notices,
                "needs_manual_review": not notices,
            })
    return {"artifact": Path(path).name, "packages": records,
            "packages_without_notices": [item["name"] for item in records
                                         if item["needs_manual_review"]]}


# @spec scripts/main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("zip")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = audit(args.zip)
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"packages": len(result["packages"]),
                      "packages_without_notices": result["packages_without_notices"]}))


if __name__ == "__main__":
    main()
