import errno
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.app.storage.atomic import rename_noreplace, rename_noreplace_bound


def test_atomic_directory_publication_has_exactly_one_winner(tmp_path: Path) -> None:
    sources = [tmp_path / f"candidate-{index}" for index in range(8)]
    for index, source in enumerate(sources):
        source.mkdir()
        (source / "owner").write_text(str(index))
    destination = tmp_path / "published"

    def publish(source: Path) -> bool:
        try:
            rename_noreplace(source, destination)
            return True
        except FileExistsError:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(publish, sources))
    assert outcomes.count(True) == 1
    winner = outcomes.index(True)
    assert (destination / "owner").read_text() == str(winner)
    assert all(source.exists() == (index != winner) for index, source in enumerate(sources))


@pytest.mark.parametrize("kind", ["empty_directory", "file", "symlink"])
def test_atomic_publication_preserves_existing_destination(tmp_path: Path, kind: str) -> None:
    source = tmp_path / "source"
    source.mkdir()
    destination = tmp_path / "destination"
    if kind == "empty_directory":
        destination.mkdir()
    elif kind == "file":
        destination.write_bytes(b"foreign")
    else:
        destination.symlink_to(tmp_path / "missing")
    identity = destination.lstat()
    with pytest.raises(FileExistsError):
        rename_noreplace(source, destination)
    assert source.is_dir()
    assert destination.lstat().st_ino == identity.st_ino


def test_atomic_publication_unsupported_platform_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    destination = tmp_path / "destination"
    monkeypatch.setattr("backend.app.storage.atomic.sys.platform", "unsupported")
    with pytest.raises(OSError) as failure:
        rename_noreplace(source, destination)
    assert failure.value.errno == errno.ENOTSUP
    assert source.exists() and not destination.exists()


@pytest.mark.parametrize("name", ["", ".", "..", "../outside", "a/b", "a\0b"])
def test_atomic_publication_rejects_non_leaf_names(tmp_path, name):
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(ValueError):
            rename_noreplace_bound(descriptor, name, descriptor, "destination")
    finally:
        os.close(descriptor)


@pytest.mark.skipif(sys.platform == "darwin", reason="Unsupported-platform migration contract")
@pytest.mark.parametrize("existing", [False, True])
def test_unsupported_secure_migration_refuses_before_any_write(tmp_path, existing):
    from backend.app.market.continuity import RepairQueueError
    from backend.app.market.store import MarketStore

    target = tmp_path / "market" / "control.duckdb"
    if existing:
        target.parent.mkdir()
        MarketStore(target).initialize_schema()
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    directories = {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_dir()}
    with pytest.raises(RepairQueueError, match="unavailable on this platform"):
        MarketStore(target).initialize_all_writer_schema(staging_directory=tmp_path / "staging")
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == before
    assert {
        path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_dir()
    } == directories
