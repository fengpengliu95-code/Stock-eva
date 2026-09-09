import json
import os
import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout
from backend.app.storage.replication import (
    JournalRecord,
    ReplicationDurabilityError,
    ReplicationSidecarStore,
    ReplicationStateUnavailable,
    ReplicationStatusResponse,
    ReplicationStatusService,
    SourceInstanceStore,
    build_journal_record,
    build_source_checkpoint,
    domain_sha256,
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


def _checkpoint_for_journal(
    *,
    source_instance_id: str = "a" * 64,
    source_instance_sha256: str = "b" * 64,
    publication_binding_sha256: str = "c" * 64,
    created_at: str = "2026-09-09T00:00:01Z",
):
    entry = {
        "relative_path": "bars.parquet",
        "object_sha256": "2" * 64,
        "size_bytes": 10,
        "row_count": 1,
        "trade_date": "2026-09-09",
        "source": "bao_stock",
    }
    object_set = domain_sha256("stock-eva/r2f4.3/object-set/v1", [entry])
    checkpoint = build_source_checkpoint(
        source_instance_id=source_instance_id,
        source_instance_sha256=source_instance_sha256,
        publication_binding_sha256=publication_binding_sha256,
        pointer_row_sha256="d" * 64,
        pointer_generation="generation-1",
        source_run_id="run-1",
        source_trade_date="2026-09-09",
        source_published_at="2026-09-09T00:00:00Z",
        pointer_db_device=1,
        pointer_db_inode=2,
        pointer_db_schema_digest="e" * 64,
        manifest_canonical_sha256="f" * 64,
        source_manifest_bytes_sha256="0" * 64,
        source_object_set_sha256=object_set,
        object_inventory=[entry],
    )
    return build_journal_record(
        checkpoint_id=checkpoint.checkpoint_id,
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
        publication_binding_sha256=checkpoint.publication_binding_sha256,
        source_published_at=checkpoint.source_published_at,
        checkpoint_projection=checkpoint,
        created_at=created_at,
    )


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


def test_source_instance_identity_golden_preimage_binds_nonce_and_only_dataset_identity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "dataset" / "_replication" / "source-instance.json"
    record = SourceInstanceStore(path).create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_domain="stock-eva/r2f4.3/local-canonical",
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )
    dataset_identity = domain_sha256(
        "stock-eva/r2f4.3/dataset-identity/v1",
        {
            "canonical_root_path": str(tmp_path / "dataset"),
            "canonical_schema_digest": "c" * 64,
            "source_instance_domain": "stock-eva/r2f4.3/local-canonical",
            "source_instance_nonce": "d" * 64,
        },
    )
    assert record.dataset_identity == dataset_identity
    assert record.source_instance_id == domain_sha256(
        "stock-eva/r2f4.3/source-instance/v3", {"dataset_identity": dataset_identity}
    )


def test_replication_paths_without_local_dataset_root_are_typed_unavailable(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, local_market_dataset_root=None, replication_enabled=True)
    layout = StorageLayout(settings)

    assert layout.replication_source_instance is None
    result = ReplicationStatusService(settings).read()
    assert result.status == "unavailable"
    assert result.reason_code == "SOURCE_NOT_CONFIGURED"


def test_source_instance_read_binds_current_canonical_root(tmp_path: Path) -> None:
    path = tmp_path / "dataset" / "_replication" / "source-instance.json"
    store = SourceInstanceStore(path)
    store.create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_domain="stock-eva/r2f4.3/local-canonical",
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )

    with pytest.raises(ReplicationDurabilityError, match="canonical root"):
        store.read(canonical_root_path=tmp_path / "other-dataset")


def test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch() -> None:
    entry = {
        "relative_path": "bars.parquet",
        "object_sha256": "2" * 64,
        "size_bytes": 10,
        "row_count": 1,
        "trade_date": "2026-09-09",
        "source": "bao_stock",
    }
    object_set = domain_sha256("stock-eva/r2f4.3/object-set/v1", [entry])
    common = {
        "source_instance_id": "a" * 64,
        "source_instance_sha256": "b" * 64,
        "publication_binding_sha256": "c" * 64,
        "pointer_row_sha256": "d" * 64,
        "pointer_generation": "generation-1",
        "source_run_id": "run-1",
        "source_trade_date": "2026-09-09",
        "source_published_at": "2026-09-09T08:00:00Z",
        "pointer_db_device": 1,
        "pointer_db_inode": 2,
        "pointer_db_schema_digest": "e" * 64,
        "manifest_canonical_sha256": "f" * 64,
        "source_manifest_bytes_sha256": "0" * 64,
        "source_object_set_sha256": object_set,
        "object_inventory": [entry],
    }
    build_source_checkpoint(**common)
    with pytest.raises(ReplicationDurabilityError, match="object set"):
        build_source_checkpoint(**{**common, "source_object_set_sha256": "1" * 64})


def test_journal_projection_is_exact_source_checkpoint_projection() -> None:
    with pytest.raises(
        (ValidationError, ReplicationDurabilityError), match="projection|checkpoint"
    ):
        build_journal_record(
            checkpoint_id="e" * 64,
            source_instance_id="a" * 64,
            source_instance_sha256="b" * 64,
            publication_binding_sha256="c" * 64,
            source_published_at="2026-09-09T00:00:00Z",
            checkpoint_projection={"checkpoint_id": "e" * 64, "path": "/secret"},
            created_at="2026-09-09T00:00:01Z",
        )


def test_journal_install_revalidates_model_copy_projection_before_write(tmp_path: Path) -> None:
    record = _checkpoint_for_journal()
    tampered = record.model_copy(
        update={
            "checkpoint_projection": {
                **record.checkpoint_projection.model_dump(mode="json"),
                "secret": "must-not-persist",
            }
        }
    )
    with pytest.raises(ReplicationDurabilityError, match="projection"):
        tampered.install(tmp_path / "journals")
    assert not (tmp_path / "journals").exists()


def test_sidecar_global_reachability_rejects_orphan_event(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute(
            """INSERT INTO replication_attempt_events
            (event_id, intent_id, event_sequence, prev_event_sha256, event_type,
             from_state, to_state, attempt, reason_code, state_version, occurred_at,
             event_sha256)
            VALUES (?, ?, 0, ?, 'intent_created', NULL, 'pending', 0, 'NONE', 0,
                    '2026-09-09T00:00:00Z', ?)""",
            ("c" * 64, "d" * 64, "0" * 64, "e" * 64),
        )
    with pytest.raises(Exception, match="reachability|sequence|hash"):
        ReplicationSidecarStore(path).read_status()


@pytest.mark.parametrize("tamper", ["user_version", "index"])
def test_sidecar_reader_rejects_schema_identity_tamper(tmp_path: Path, tamper: str) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    with sqlite3.connect(path) as connection:
        if tamper == "user_version":
            connection.execute("PRAGMA user_version = 99")
        else:
            connection.execute("DROP INDEX replication_heads_due_idx")
    with pytest.raises(ReplicationStateUnavailable, match="DDL|version"):
        ReplicationSidecarStore(path).read_status()


def test_public_status_rejects_non_allowlisted_reason_and_exposes_no_path() -> None:
    with pytest.raises(ValidationError):
        ReplicationStatusResponse(
            status="unavailable",
            reason_code="/private/secret",
            enabled=True,
            source_ready=False,
            destination_configured=False,
            outbox_schema_version=None,
            pending_count=0,
            copying_count=0,
            verifying_count=0,
            retry_wait_count=0,
            dead_letter_count=0,
            last_replicated_source_manifest_sha256=None,
            last_replicated_at=None,
            local_ready=False,
            destination_health="unknown",
            destination_health_observed_at=None,
            queue_lag_seconds=None,
            lag_seconds=None,
            effects={
                "writes": False,
                "canonical_writes": False,
                "destination_writes": False,
                "outbox_writes": False,
                "restore_writes": False,
            },
        )


def test_public_status_rejects_uppercase_digest_and_non_utc_timestamp() -> None:
    kwargs = {
        "status": "ready",
        "reason_code": "NONE",
        "enabled": True,
        "source_ready": True,
        "destination_configured": False,
        "outbox_schema_version": 1,
        "pending_count": 0,
        "copying_count": 0,
        "verifying_count": 0,
        "retry_wait_count": 0,
        "dead_letter_count": 0,
        "last_replicated_source_manifest_sha256": "A" * 64,
        "last_replicated_at": None,
        "local_ready": False,
        "destination_health": "unknown",
        "destination_health_observed_at": None,
        "queue_lag_seconds": None,
        "lag_seconds": None,
        "effects": {
            "writes": False,
            "canonical_writes": False,
            "destination_writes": False,
            "outbox_writes": False,
            "restore_writes": False,
        },
    }
    with pytest.raises(ValidationError):
        ReplicationStatusResponse(**kwargs)


def test_journal_conflicting_target_fails_without_overwrite(tmp_path: Path) -> None:
    record = _checkpoint_for_journal(
        source_instance_id="b" * 64,
        source_instance_sha256="c" * 64,
        publication_binding_sha256="d" * 64,
    )
    root = tmp_path / "journals"
    target = record.install(root)
    before = target.read_bytes()
    target.write_bytes(before[:-1] + b" ")
    with pytest.raises(ReplicationDurabilityError, match="durability"):
        record.install(root)
    assert target.read_bytes() == before[:-1] + b" "


def test_journal_install_is_no_replace_and_reuses_identical_target(tmp_path: Path) -> None:
    record = _checkpoint_for_journal()
    root = tmp_path / "journals"
    first = record.install(root)
    first_bytes = first.read_bytes()
    first_stat = first.stat()
    assert JournalRecord.read(first) == record
    assert record.install(root) == first
    assert first.read_bytes() == first_bytes
    assert first.stat().st_ino == first_stat.st_ino

    conflicting = build_journal_record(
        checkpoint_id=record.checkpoint_id,
        source_instance_id=record.source_instance_id,
        source_instance_sha256=record.source_instance_sha256,
        publication_binding_sha256=record.publication_binding_sha256,
        source_published_at=record.source_published_at,
        checkpoint_projection=record.checkpoint_projection,
        created_at="2026-09-09T00:00:02Z",
    )
    with pytest.raises(Exception, match="durability"):
        conflicting.install(root)
    assert first.read_bytes() == first_bytes


def test_source_checkpoint_hash_excludes_sequence_and_binds_complete_inventory() -> None:
    entry = {
        "relative_path": "bars.parquet",
        "object_sha256": "2" * 64,
        "size_bytes": 10,
        "row_count": 1,
        "trade_date": "2026-09-09",
        "source": "bao_stock",
    }
    object_set = domain_sha256("stock-eva/r2f4.3/object-set/v1", [entry])
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
        source_object_set_sha256=object_set,
        object_inventory=[entry],
    )

    checkpoint.verify_hashes()
    preimage = checkpoint.model_dump(mode="json")
    preimage.pop("checkpoint_id")
    preimage.pop("checkpoint_payload_sha256")
    preimage.pop("checkpoint_schema")
    assert (
        domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1", preimage) == checkpoint.checkpoint_id
    )
    assert checkpoint.checkpoint_id != checkpoint.checkpoint_payload_sha256
    assert checkpoint.object_inventory[0].relative_path == "bars.parquet"
