import json
import os
import sqlite3
from pathlib import Path

import pytest

from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout
from backend.app.storage.replication import (
    JournalRecord,
    ReplicationSidecarStore,
    ReplicationStatusService,
    SourceInstanceStore,
    build_journal_record,
    build_source_checkpoint,
)


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "market_data_dir": tmp_path / "market",
        "user_data_dir": tmp_path / "user",
        "local_control_dir": tmp_path / "control",
        "local_staging_dir": tmp_path / "staging",
        "local_lock_dir": tmp_path / "locks",
        "local_temp_dir": tmp_path / "tmp",
        "local_market_dataset_root": tmp_path / "dataset",
    }
    values.update(overrides)
    return Settings(**values)


def test_replication_defaults_off_and_layout_is_local(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    layout = StorageLayout(settings)

    assert settings.replication_enabled is False
    assert settings.replication_drain_enabled is False
    assert settings.replication_direction == "local_to_nas"
    assert layout.replication_database == tmp_path / "control" / "replication.sqlite3"
    assert layout.replication_journal_root == tmp_path / "control" / "replication-journal"
    assert layout.replication_lock == tmp_path / "locks" / "replication.lock"
    assert (
        layout.replication_source_instance
        == tmp_path / "dataset" / "_replication" / "source-instance.json"
    )


def test_replication_disabled_is_zero_work(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = ReplicationStatusService(settings).read()

    assert result.status == "disabled"
    assert result.reason_code == "DISABLED"
    assert result.provider_requests == 0
    assert result.effects.writes is False
    assert result.paths_exposed is False
    assert not (tmp_path / "control").exists()
    assert not (tmp_path / "dataset").exists()


def test_replication_status_reads_valid_sidecar_without_writing(tmp_path: Path) -> None:
    settings = _settings(tmp_path, replication_enabled=True)
    layout = StorageLayout(settings)
    source = SourceInstanceStore(layout.replication_source_instance).create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_domain="stock-eva/r2f4.3/local-canonical",
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )
    ReplicationSidecarStore(layout.replication_database).initialize(
        source_instance_id=source.source_instance_id,
        source_instance_sha256=source.source_instance_sha256,
    )
    before = (layout.replication_database.stat().st_ino, layout.replication_database.read_bytes())

    result = ReplicationStatusService(settings).read()

    assert result.status == "ready"
    assert result.reason_code == "NONE"
    assert result.source_ready is True
    assert result.provider_requests == 0
    assert (
        layout.replication_database.stat().st_ino,
        layout.replication_database.read_bytes(),
    ) == before


def test_replication_status_missing_sidecar_is_unavailable_without_initialization(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, replication_enabled=True)
    layout = StorageLayout(settings)
    source = SourceInstanceStore(layout.replication_source_instance).create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_domain="stock-eva/r2f4.3/local-canonical",
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )
    del source
    result = ReplicationStatusService(settings).read()

    assert result.status == "unavailable"
    assert result.reason_code == "REPLICATION_STATE_UNAVAILABLE"
    assert layout.replication_database.exists() is False


def test_replication_sidecar_normative_ddl_identity_and_immutable_event_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)

    with sqlite3.connect(path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
        assert {
            "replication_sidecar_meta",
            "replication_intents",
            "replication_destination_cache",
            "replication_attempt_events",
            "replication_heads",
        } <= tables
        assert connection.execute(
            "SELECT schema_identity, schema_version FROM replication_sidecar_meta"
        ).fetchone() == ("stock-eva/r2f4.3/replication-sidecar/v1", 1)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM replication_sidecar_meta")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE replication_sidecar_meta SET schema_version=1 WHERE sidecar_id=1"
            )


def test_source_instance_record_is_immutable_and_fsynced(tmp_path: Path) -> None:
    path = tmp_path / "dataset" / "_replication" / "source-instance.json"
    store = SourceInstanceStore(path)
    record = store.create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_domain="stock-eva/r2f4.3/local-canonical",
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )
    first_bytes = path.read_bytes()
    first_stat = path.stat()

    assert len(record.source_instance_sha256) == 64
    assert json.loads(first_bytes)["source_instance_sha256"] == record.source_instance_sha256
    assert store.read() == record
    assert (
        store.create(
            canonical_root_path=tmp_path / "dataset",
            canonical_schema_digest="c" * 64,
            source_instance_domain="stock-eva/r2f4.3/local-canonical",
            source_instance_nonce="d" * 64,
            created_at="2026-09-09T00:00:00Z",
        )
        == record
    )
    assert path.read_bytes() == first_bytes
    assert path.stat().st_ino == first_stat.st_ino
    assert path.is_symlink() is False
    assert os.stat(path).st_mode & 0o077 == 0


def test_journal_install_is_no_replace_and_reuses_identical_target(tmp_path: Path) -> None:
    checkpoint_id = "e" * 64
    record = build_journal_record(
        checkpoint_id=checkpoint_id,
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
        publication_binding_sha256="c" * 64,
        source_published_at="2026-09-09T00:00:00Z",
        checkpoint_projection={"checkpoint_id": checkpoint_id, "row_count": 1},
        created_at="2026-09-09T00:00:01Z",
    )
    root = tmp_path / "journals"
    first = record.install(root)
    first_bytes = first.read_bytes()
    first_stat = first.stat()
    assert JournalRecord.read(first) == record
    assert record.install(root) == first
    assert first.read_bytes() == first_bytes
    assert first.stat().st_ino == first_stat.st_ino

    conflicting = record.model_copy(
        update={"checkpoint_projection": {"checkpoint_id": checkpoint_id, "row_count": 2}}
    )
    with pytest.raises(Exception, match="durability"):
        conflicting.install(root)
    assert first.read_bytes() == first_bytes


def test_source_checkpoint_hash_excludes_sequence_and_binds_complete_inventory() -> None:
    checkpoint = build_source_checkpoint(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
        publication_binding_sha256="c" * 64,
        pointer_row_sha256="d" * 64,
        pointer_generation="generation-1",
        source_run_id="run-1",
        source_trade_date="2026-09-09",
        source_published_at="2026-09-09T08:00:00Z",
        pointer_db_device=1,
        pointer_db_inode=2,
        pointer_db_schema_digest="e" * 64,
        manifest_canonical_sha256="f" * 64,
        source_manifest_bytes_sha256="0" * 64,
        source_object_set_sha256="1" * 64,
        object_inventory=[
            {
                "relative_path": "bars.parquet",
                "sha256": "2" * 64,
                "byte_count": 10,
                "row_count": 1,
            }
        ],
    )

    checkpoint.verify_hashes()
    assert checkpoint.checkpoint_id != checkpoint.checkpoint_payload_sha256
    assert checkpoint.object_inventory[0].relative_path == "bars.parquet"
