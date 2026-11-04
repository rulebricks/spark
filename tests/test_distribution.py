"""Release checks must reject stale or inconsistently labeled artifacts."""

import io
import tarfile
import zipfile

import pytest
from packaging.version import Version

from scripts.check_distribution import main, validate_distribution


def distribution(tmp_path, kind, version="0.2.2", metadata_version=None, typed=True):
    metadata = (
        f"Metadata-Version: 2.4\nName: rulebricks-spark\nVersion: {metadata_version or version}\n"
    ).encode()
    files = {"rulebricks_spark/_version.py": b"# runtime version uses installed metadata\n"}
    if typed:
        files["rulebricks_spark/py.typed"] = b""
    if kind == "wheel":
        path = tmp_path / f"rulebricks_spark-{version}-py3-none-any.whl"
        with zipfile.ZipFile(path, "w") as archive:
            for name, content in files.items():
                archive.writestr(name, content)
            archive.writestr(f"rulebricks_spark-{version}.dist-info/METADATA", metadata)
    else:
        prefix = f"rulebricks_spark-{version}"
        path = tmp_path / f"{prefix}.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name, content in {**files, "PKG-INFO": metadata}.items():
                entry = tarfile.TarInfo(f"{prefix}/{name}")
                entry.size = len(content)
                archive.addfile(entry, io.BytesIO(content))
    return path


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
@pytest.mark.parametrize("tag", ["v0.2.2", "0.2.2"])
def test_release_archive_matches_tag(tmp_path, kind, tag):
    assert validate_distribution(distribution(tmp_path, kind), Version(tag)) == Version("0.2.2")


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_old_artifact_is_rejected_before_upload(tmp_path, kind):
    path = distribution(tmp_path, kind, version="0.2.1")
    with pytest.raises(ValueError, match="expected 0.2.2 from the release tag"):
        validate_distribution(path, Version("v0.2.2"))


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_renaming_an_old_archive_does_not_bypass_version_check(tmp_path, kind):
    path = distribution(tmp_path, kind, metadata_version="0.2.1")
    with pytest.raises(ValueError, match="filename version 0.2.2 does not match metadata 0.2.1"):
        validate_distribution(path, Version("0.2.2"))


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_release_archive_still_requires_typing_marker(tmp_path, kind):
    with pytest.raises(ValueError, match="py.typed"):
        validate_distribution(distribution(tmp_path, kind, typed=False))


def test_wheel_and_sdist_must_share_one_version(tmp_path, monkeypatch):
    wheel = distribution(tmp_path, "wheel")
    sdist = distribution(tmp_path, "sdist", version="0.2.1")
    monkeypatch.setattr("sys.argv", ["check_distribution.py", str(wheel), str(sdist)])
    with pytest.raises(SystemExit, match="different versions"):
        main()
