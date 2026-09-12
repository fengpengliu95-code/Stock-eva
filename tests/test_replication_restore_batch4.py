import hashlib
import os
from pathlib import Path

import duckdb
import pytest

import backend.app.storage.replication as replication
from backend.app.storage.replication import (
    DestinationArchiveWriter,
    RestoreService,
    build_source_checkpoint,
    canonical_json_bytes,
    domain_sha256,
    initialize_destination,
)


def _dataset_source(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    parquet = source / "bars.parquet"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """COPY (
                SELECT DATE '2026-09-09' AS trade_date,
                       'sh.600000' AS symbol, 'stock' AS security_type,
                       'sh' AS exchange, 'main' AS board,
                       10.0::DOUBLE AS open, 11.0::DOUBLE AS high,
                       9.0::DOUBLE AS low, 10.5::DOUBLE AS close,
                       10.0::DOUBLE AS preclose, 100.0::DOUBLE AS volume,
                       1000.0::DOUBLE AS amount, 0.1::DOUBLE AS turnover_rate,
                       0.05::DOUBLE AS pct_change, 1.0::DOUBLE AS adjust_factor,
                       'none' AS price_adjustment, true AS is_trading,
                       false AS is_suspended, false AS is_st,
                       'baostock' AS source, 'row-1' AS source_record_id,
                       TIMESTAMPTZ '2026-09-09 08:00:00+00' AS ingested_at,
                       'accepted' AS quality_status, '{}'::JSON AS quality_issues
            ) TO ? (FORMAT PARQUET)""",
            [str(parquet)],
        )
    finally:
        connection.close()
    object_sha = hashlib.sha256(parquet.read_bytes()).hexdigest()
    manifest = canonical_json_bytes(
        {
            "dataset": "stock-eva-market",
            "schema_version": 2,
            "generation": "generation-1",
            "files": [
                {
                    "path": "bars.parquet",
                    "sha256": object_sha,
                    "row_count": 1,
                    "source": "baostock",
                    "trade_date": "2026-09-09",
                }
            ],
        }
    )
    sentinel = canonical_json_bytes({"dataset": "stock-eva-market", "schema_version": 2})
    (source / ".stock-eva-dataset.json").write_bytes(sentinel)
    (source / "manifest.json").write_bytes(manifest)
    inventory = [
        {
            "relative_path": "bars.parquet",
            "object_sha256": object_sha,
            "size_bytes": parquet.stat().st_size,
            "row_count": 1,
            "trade_date": "2026-09-09",
            "source": "baostock",
        }
    ]
    checkpoint = build_source_checkpoint(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
        publication_binding_sha256="c" * 64,
        pointer_row_sha256="d" * 64,
        pointer_generation="generation-1",
        source_run_id="run-1",
        source_trade_date="2026-09-09",
        source_published_at="2026-09-09T00:00:00Z",
        pointer_db_device=1,
        pointer_db_inode=2,
        pointer_db_schema_digest="e" * 64,
        manifest_canonical_sha256=hashlib.sha256(manifest).hexdigest(),
        source_manifest_bytes_sha256=hashlib.sha256(manifest).hexdigest(),
        source_object_set_sha256=domain_sha256(
            "stock-eva/r2f4.3/object-set/v1", inventory
        ),
        object_inventory=inventory,
    )
    return source, checkpoint, sentinel


def _archive(tmp_path: Path):
    source, checkpoint, sentinel = _dataset_source(tmp_path)
    archive = tmp_path / "archive"
    archive.mkdir()
    descriptor = initialize_destination(
        archive,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id="host-a"
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.record is not None
    return archive, descriptor, result.record


def test_restore_plan_is_zero_write_and_sanitized(tmp_path: Path) -> None:
    archive, _descriptor, record = _archive(tmp_path)
    destination = tmp_path / "restore"
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))

    result = RestoreService(archive).plan(destination)

    assert result.status == "dry_run"
    assert result.reason_code == "NONE"
    assert result.mode == "plan"
    assert result.execution_allowed is False
    assert result.effects.writes is False
    assert result.object_count == record.object_count
    assert not destination.exists()
    assert sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")) == before


def test_verified_restore_publishes_once_after_all_semantic_checks(tmp_path: Path) -> None:
    archive, _descriptor, record = _archive(tmp_path)
    destination = tmp_path / "restore"

    result = RestoreService(archive).execute(destination)

    assert result.status == "ready"
    assert result.reason_code == "NONE"
    assert result.effects.writes is True
    assert result.effects.restore_writes is True
    assert result.rename_allowed is True
    assert destination.joinpath(".stock-eva-dataset.json").is_file()
    assert destination.joinpath("manifest.json").is_file()
    assert destination.joinpath("bars.parquet").is_file()
    assert result.checkpoint_id == record.checkpoint_id


def test_corrupt_archive_leaves_no_final_restore_root(tmp_path: Path) -> None:
    archive, descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    assert descriptor.root_path.joinpath("_replication", "history").is_dir()

    # Corrupt the immutable object after publication; the archive reader must
    # fail before any final root is made visible.
    generation = next(
        path
        for path in (archive / "_replication" / "history").iterdir()
        if not path.name.startswith(".")
    )
    (generation / "bars.parquet").write_bytes(b"corrupt")

    result = RestoreService(archive).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"VERIFY_FAILED", "SOURCE_UNAVAILABLE"}
    assert not destination.exists()


def test_restore_query_failure_is_before_visibility(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"

    result = RestoreService(
        archive,
        representative_query=lambda _root: (_ for _ in ()).throw(RuntimeError("query failed")),
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "VERIFY_FAILED"
    assert not destination.exists()


def test_restore_final_exists_race_never_overwrites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    original = replication._destination_rename_noreplace

    def race(parent_fd: int, source: str, target: str) -> None:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent_fd)
        os.write(fd, b"attacker")
        os.close(fd)
        original(parent_fd, source, target)

    monkeypatch.setattr(replication, "_destination_rename_noreplace", race)
    result = RestoreService(archive).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"DESTINATION_CONFLICT", "PATH_CHANGED"}
    assert destination.read_text() == "attacker"


@pytest.mark.parametrize("unsafe", [Path("relative"), Path("~")])
def test_restore_rejects_non_absolute_destination_without_writes(
    tmp_path: Path, unsafe: Path
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    result = RestoreService(archive).execute(unsafe)
    assert result.status == "unavailable"
    assert result.reason_code == "PATH_INVALID"
