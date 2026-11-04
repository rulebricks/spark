"""Check archive contents and ensure release versions match the Git tag."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from email import message_from_bytes
from pathlib import Path

from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename
from packaging.version import Version


def validate_distribution(path: Path, expected_version: Version | None = None) -> Version:
    if path.suffix == ".whl":
        name, filename_version, _, _ = parse_wheel_filename(path.name)
        with zipfile.ZipFile(path) as archive:
            members = set(archive.namelist())
            metadata_paths = [
                p for p in members if p.endswith(".dist-info/METADATA") and p.count("/") == 1
            ]
            if len(metadata_paths) != 1:
                raise ValueError(f"{path.name} must contain exactly one package metadata file.")
            metadata = message_from_bytes(archive.read(metadata_paths[0]))
    elif path.name.endswith(".tar.gz"):
        name, filename_version = parse_sdist_filename(path.name)
        with tarfile.open(path, "r:gz") as archive:
            members = set(archive.getnames())
            metadata_paths = [p for p in members if p.endswith("/PKG-INFO") and p.count("/") == 1]
            if len(metadata_paths) != 1:
                raise ValueError(f"{path.name} must contain exactly one package metadata file.")
            member = archive.getmember(metadata_paths[0])
            if not member.isfile():
                raise ValueError(f"{path.name} package metadata must be a regular file.")
            stream = archive.extractfile(member)
            assert stream is not None
            with stream:
                metadata = message_from_bytes(stream.read())
    else:
        raise ValueError(f"Unsupported distribution: {path}")

    if name != "rulebricks-spark" or canonicalize_name(metadata.get("Name", "")) != name:
        raise ValueError(f"{path.name} has an unexpected package name.")
    version = Version(metadata.get("Version", ""))
    if version != filename_version:
        raise ValueError(
            f"{path.name} filename version {filename_version} does not match metadata {version}."
        )
    if expected_version is not None and version != expected_version:
        raise ValueError(
            f"{path.name} contains version {version}; expected {expected_version} from the release tag."
        )
    if not any(name.endswith("rulebricks_spark/py.typed") for name in members):
        raise ValueError(f"{path.name} does not contain rulebricks_spark/py.typed.")
    unexpected = sorted(
        name
        for name in members
        if "__pycache__" in name or name.endswith((".pyc", ".pyo")) or "/.env" in name
    )
    if unexpected:
        raise ValueError(f"{path.name} contains unexpected files: {unexpected}")
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-version", help="Release tag, such as v0.2.2 or 0.2.2")
    parser.add_argument("distributions", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        expected = Version(args.expected_version) if args.expected_version is not None else None
        versions = {validate_distribution(path, expected) for path in args.distributions}
        if len(versions) != 1:
            raise ValueError("Distribution archives contain different versions.")
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    print(
        f"Validated {len(args.distributions)} distribution archives for version {versions.pop()}."
    )


if __name__ == "__main__":
    main()
