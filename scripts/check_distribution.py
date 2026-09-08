"""Check release archives for required and accidental files."""

from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path


def _members(path: Path) -> set[str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return set(archive.namelist())
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            return {member.name for member in archive.getmembers()}
    raise ValueError(f"Unsupported distribution: {path}")


def main() -> None:
    distributions = [Path(value) for value in sys.argv[1:]]
    if not distributions:
        raise SystemExit("Pass wheel and sdist paths.")
    for path in distributions:
        members = _members(path)
        if not any(name.endswith("rulebricks_spark/py.typed") for name in members):
            raise SystemExit(f"{path.name} does not contain rulebricks_spark/py.typed.")
        unexpected = sorted(
            name
            for name in members
            if "__pycache__" in name or name.endswith((".pyc", ".pyo")) or "/.env" in name
        )
        if unexpected:
            raise SystemExit(f"{path.name} contains unexpected files: {unexpected}")
    print(f"Validated {len(distributions)} distribution archives.")


if __name__ == "__main__":
    main()
