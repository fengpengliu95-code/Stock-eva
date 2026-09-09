import json
import os
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

import backend.app.storage.replication as replication
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


def test_status_uses_no_filesystem_clone_and_preserves_all_sidecar_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path, replication_enabled=True)
    layout = StorageLayout(settings)
    source = SourceInstanceStore(layout.replication_source_instance).create(
        canonical_root_path=tmp_path / "dataset",
        canonical_schema_digest="c" * 64,
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )
    ReplicationSidecarStore(layout.replication_database).initialize(
        source_instance_id=source.source_instance_id,
        source_instance_sha256=source.source_instance_sha256,
    )
    before = {
        path: (path.stat().st_ino, path.read_bytes())
        for path in layout.replication_database.parent.glob("replication.sqlite3*")
    }

    def fail_if_clone(*args: object, **kwargs: object) -> None:
        raise AssertionError("status must not create a filesystem SQLite clone")

    snapshot_checks: list[object] = []
    real_snapshot_check = replication._assert_sqlite_snapshot_unchanged

    def observe_snapshot(snapshot: object) -> None:
        snapshot_checks.append(snapshot)
        real_snapshot_check(snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(replication, "_open_verified_sqlite_clone", fail_if_clone, raising=False)
    monkeypatch.setattr(replication, "_assert_sqlite_snapshot_unchanged", observe_snapshot)
    assert ReplicationStatusService(settings).read().reason_code == "NONE"
    # The status service performs two independent sidecar reads (precedence
    # check, then source-identity-bound check), each with open/end proofs.
    assert len(snapshot_checks) == 4
    after = {
        path: (path.stat().st_ino, path.read_bytes())
        for path in layout.replication_database.parent.glob("replication.sqlite3*")
    }
    assert after == before


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


def test_journal_read_verifies_nested_checkpoint_object_set(tmp_path: Path) -> None:
    record = _checkpoint_for_journal()
    checkpoint_values = record.checkpoint_projection.model_dump(mode="json")
    checkpoint_values["object_inventory"][0]["object_sha256"] = "3" * 64
    checkpoint_preimage = dict(checkpoint_values)
    checkpoint_preimage.pop("checkpoint_schema")
    checkpoint_preimage.pop("checkpoint_id")
    checkpoint_preimage.pop("checkpoint_payload_sha256")
    checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1", checkpoint_preimage)
    checkpoint_values["checkpoint_id"] = checkpoint_id
    checkpoint_values["checkpoint_payload_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/source-checkpoint-payload/v1",
        checkpoint_preimage | {"checkpoint_id": checkpoint_id},
    )
    outer = record.model_dump(mode="json")
    outer["checkpoint_projection"] = checkpoint_values
    outer["checkpoint_id"] = checkpoint_id
    outer["checkpoint_payload_sha256"] = checkpoint_values["checkpoint_payload_sha256"]
    outer["journal_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-journal/v1",
        {
            key: outer[key]
            for key in (
                "journal_schema_version",
                "checkpoint_id",
                "source_instance_id",
                "source_instance_sha256",
                "publication_binding_sha256",
                "checkpoint_payload_sha256",
                "source_published_at",
                "created_at",
            )
        },
    )
    path = tmp_path / "journals" / f"{checkpoint_id}.json"
    path.parent.mkdir()
    path.write_bytes(replication.canonical_json_bytes(outer))
    path.chmod(0o600)
    with pytest.raises(ReplicationDurabilityError, match="object set"):
        JournalRecord.read(path)


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


def test_sidecar_reader_rejects_intent_source_identity_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    identity_fields = {
        "direction": "local_to_nas",
        "destination_id": "1" * 32,
        "source_instance_id": "c" * 64,
        "source_sequence": 1,
        "checkpoint_id": "d" * 64,
        "plan_sha256": "e" * 64,
    }
    intent_id = domain_sha256("stock-eva/r2f4.3/replication-intent/v1", identity_fields)
    row = {
        "intent_id": intent_id,
        "schema_version": 1,
        "operation_day": "2026-09-09",
        **identity_fields,
        "pointer_row_sha256": "f" * 64,
        "pointer_generation": "generation-1",
        "source_run_id": "run-1",
        "source_trade_date": "2026-09-09",
        "source_published_at": "2026-09-09T00:00:00Z",
        "pointer_db_device": 1,
        "pointer_db_inode": 2,
        "pointer_db_schema_digest": "0" * 64,
        "manifest_canonical_sha256": "1" * 64,
        "source_object_set_sha256": "2" * 64,
        "publication_binding_sha256": "3" * 64,
        "source_instance_sha256": "4" * 64,
        "source_manifest_bytes_sha256": "5" * 64,
        "object_count": 0,
        "row_count": 0,
        "byte_count": 0,
        "created_at": "2026-09-09T00:00:00Z",
    }
    row["intent_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-intent-row/v1",
        {key: value for key, value in row.items() if key != "intent_sha256"},
    )
    columns = ",".join(row)
    placeholders = ",".join("?" * len(row))
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"INSERT INTO replication_intents ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )
    with pytest.raises(ReplicationStateUnavailable, match="source identity"):
        store.read_status()


def test_sidecar_reader_rejects_stale_lease_on_nonleased_head(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    occurred_at = "2026-09-09T00:00:00Z"
    identity_fields = {
        "direction": "local_to_nas",
        "destination_id": "1" * 32,
        "source_instance_id": "a" * 64,
        "source_sequence": 1,
        "checkpoint_id": "d" * 64,
        "plan_sha256": "e" * 64,
    }
    intent_id = domain_sha256("stock-eva/r2f4.3/replication-intent/v1", identity_fields)
    intent = {
        "intent_id": intent_id,
        "schema_version": 1,
        "operation_day": "2026-09-09",
        **identity_fields,
        "pointer_row_sha256": "f" * 64,
        "pointer_generation": "generation-1",
        "source_run_id": "run-1",
        "source_trade_date": "2026-09-09",
        "source_published_at": occurred_at,
        "pointer_db_device": 1,
        "pointer_db_inode": 2,
        "pointer_db_schema_digest": "0" * 64,
        "manifest_canonical_sha256": "1" * 64,
        "source_object_set_sha256": "2" * 64,
        "publication_binding_sha256": "3" * 64,
        "source_instance_sha256": "b" * 64,
        "source_manifest_bytes_sha256": "4" * 64,
        "object_count": 0,
        "row_count": 0,
        "byte_count": 0,
        "created_at": occurred_at,
    }
    intent["intent_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-intent-row/v1",
        {key: value for key, value in intent.items() if key != "intent_sha256"},
    )
    event_fields = {
        "event_id": domain_sha256(
            "stock-eva/r2f4.3/replication-event-id/v1",
            {
                "intent_id": intent_id,
                "event_sequence": 0,
                "attempt": 0,
                "state_version": 0,
                "occurred_at": occurred_at,
            },
        ),
        "intent_id": intent_id,
        "event_sequence": 0,
        "prev_event_sha256": "0" * 64,
        "event_type": "intent_created",
        "from_state": None,
        "to_state": "pending",
        "attempt": 0,
        "reason_code": "NONE",
        "state_version": 0,
        "occurred_at": occurred_at,
        "destination_replication_generation": None,
        "destination_record_sha256": None,
        "destination_head_sha256": None,
    }
    event = {
        **event_fields,
        "event_sha256": domain_sha256("stock-eva/r2f4.3/replication-event/v1", event_fields),
    }
    with sqlite3.connect(path) as connection:
        columns = ",".join(intent)
        connection.execute(
            f"INSERT INTO replication_intents ({columns}) VALUES ({','.join('?' * len(intent))})",
            tuple(intent.values()),
        )
        columns = ",".join(event)
        connection.execute(
            f"INSERT INTO replication_attempt_events ({columns}) VALUES "
            f"({','.join('?' * len(event))})",
            tuple(event.values()),
        )
        connection.execute(
            """INSERT INTO replication_heads
               (intent_id, current_state, state_version, last_event_sequence,
                lease_owner, lease_until, next_attempt_at, last_reason_code, updated_at)
               VALUES (?, 'pending', 0, 0, 'stale-owner', ?, ?, 'NONE', ?)""",
            (intent_id, "2026-09-09T00:10:00Z", "2026-09-09T00:00:00Z", occurred_at),
        )
    with pytest.raises(ReplicationStateUnavailable, match="invalid|stale"):
        store.read_status()


def test_status_rejects_existing_wal_shm_without_writes(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    wal_path = path.with_name(path.name + "-wal")
    shm_path = path.with_name(path.name + "-shm")
    wal_path.write_bytes(b"unexpected wal")
    shm_path.write_bytes(b"unexpected shm")
    wal_path.chmod(0o600)
    shm_path.chmod(0o600)
    before = {
        artifact: (artifact.stat().st_ino, artifact.stat().st_mtime_ns, artifact.read_bytes())
        for artifact in (path, wal_path, shm_path)
    }
    with pytest.raises(ReplicationStateUnavailable, match="WAL|SHM|sidecar"):
        store.read_status()
    after = {
        artifact: (artifact.stat().st_ino, artifact.stat().st_mtime_ns, artifact.read_bytes())
        for artifact in (path, wal_path, shm_path)
    }
    assert after == before


def test_sidecar_sqlite_engine_uses_memory_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    real_connect = sqlite3.connect
    databases: list[object] = []

    def memory_only_connect(database: object, *args: object, **kwargs: object):
        databases.append(database)
        if database != ":memory:":
            raise AssertionError("sidecar must never reopen a pathname")
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(replication.sqlite3, "connect", memory_only_connect)
    assert store.read_status().reason_code == "NONE"
    assert databases and all(database == ":memory:" for database in databases)


def test_sqlite_deserialize_failure_closes_private_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = sqlite3.connect(":memory:")
    seed.execute("CREATE TABLE probe (value TEXT)")
    payload = seed.serialize()
    seed.close()
    connections: list[object] = []

    class FailingDeserializeConnection:
        def deserialize(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("injected deserialize failure")

        def close(self) -> None:
            connections.append("closed")

    def connect_memory_only(database: object, *_args: object, **_kwargs: object):
        assert database == ":memory:"
        connection = FailingDeserializeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(replication.sqlite3, "connect", connect_memory_only)
    with pytest.raises(ReplicationStateUnavailable, match="deserialize"):
        replication._deserialize_sqlite_bytes(payload)
    assert len(connections) == 2 and connections[1] == "closed"


def test_status_same_stat_byte_mutation_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    real_validate = replication._validate_sqlite_schema

    def mutate_after_open(connection: sqlite3.Connection) -> None:
        real_validate(connection)
        stat_before = path.stat()
        payload = bytearray(path.read_bytes())
        payload[-1] ^= 1
        path.write_bytes(payload)
        os.utime(path, ns=(stat_before.st_atime_ns, stat_before.st_mtime_ns))

    monkeypatch.setattr(replication, "_validate_sqlite_schema", mutate_after_open)
    with pytest.raises(ReplicationStateUnavailable, match="changed|fingerprint"):
        store.read_status()


def test_sidecar_writer_cleanup_failure_is_typed_and_closes_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "journal" / "record.json"
    real_unlink = replication.os.unlink
    cleanup_calls: list[tuple[int, ...]] = []
    real_close_descriptors = replication._close_descriptors

    def fail_temp_cleanup(name: object, *args: object, **kwargs: object):
        if kwargs.get("dir_fd") is not None:
            raise OSError("injected cleanup failure")
        return real_unlink(name, *args, **kwargs)

    def observe_descriptor_close(descriptors: list[int]) -> None:
        cleanup_calls.append(tuple(descriptors))
        real_close_descriptors(descriptors)

    monkeypatch.setattr(replication.os, "unlink", fail_temp_cleanup)
    monkeypatch.setattr(replication, "_close_descriptors", observe_descriptor_close)
    with pytest.raises(ReplicationDurabilityError, match="cleanup"):
        replication._install_no_replace(path, b"record")
    assert cleanup_calls


def test_memory_writer_cleanup_failure_is_typed_and_closes_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    real_unlink = replication.os.unlink
    closed_sessions: list[object] = []
    real_close_writer_session = replication._close_writer_session

    def fail_temp_cleanup(name: object, *args: object, **kwargs: object):
        if kwargs.get("dir_fd") is not None:
            raise OSError("injected cleanup failure")
        return real_unlink(name, *args, **kwargs)

    def observe_writer_close(session: object) -> None:
        closed_sessions.append(session)
        real_close_writer_session(session)  # type: ignore[arg-type]

    monkeypatch.setattr(replication.os, "unlink", fail_temp_cleanup)
    monkeypatch.setattr(replication, "_close_writer_session", observe_writer_close)
    with pytest.raises(ReplicationDurabilityError, match="cleanup"):
        store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert closed_sessions
    assert path.exists()
    assert any(
        entry.name.startswith(f".{path.name}.") and entry.name.endswith(".tmp")
        for entry in path.parent.iterdir()
    )


def test_writer_identity_check_rejects_replacement_between_open_and_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    path.parent.mkdir()
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    real_connect = sqlite3.connect

    def racing_connect(database: object, *args: object, **kwargs: object):
        os.replace(path, path.with_name("replaced.sqlite3"))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(replication.sqlite3, "connect", racing_connect)
    with pytest.raises(ReplicationDurabilityError, match="identity|changed|writable"):
        ReplicationSidecarStore._connect_writer(path)
    assert not path.exists()
    assert (path.with_name("replaced.sqlite3")).stat().st_size > 100


def test_sidecar_writer_lock_serializes_descriptor_sessions(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    first = replication._open_writer_session(path)
    acquired: list[replication._WriterSession] = []

    def open_second() -> None:
        acquired.append(replication._open_writer_session(path))

    thread = threading.Thread(target=open_second)
    thread.start()
    time.sleep(0.05)
    assert thread.is_alive()
    replication._close_writer_session(first)
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(acquired) == 1
    replication._close_writer_session(acquired[0])


def test_sidecar_cas_helper_requires_writer_lock_token(tmp_path: Path) -> None:
    session = replication._WriterSession(
        path=tmp_path / "control" / "replication.sqlite3",
        parent_fd=-1,
        descriptors=[],
        target_fd=None,
        payload=None,
        stat=None,
        fingerprint=None,
    )
    with pytest.raises(ReplicationDurabilityError, match="lock token"):
        replication._install_memory_snapshot(session, b"not-a-database")


def test_repeated_initialize_reads_held_descriptor_from_offset_zero(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert store.read_status().reason_code == "NONE"


def test_existing_empty_sidecar_concurrent_initializers_have_one_winner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    path.parent.mkdir(parents=True)
    path.touch(mode=0o600)
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def initialize(source_instance_id: str) -> None:
        barrier.wait()
        try:
            ReplicationSidecarStore(path).initialize(
                source_instance_id=source_instance_id,
                source_instance_sha256="b" * 64,
            )
        except ReplicationDurabilityError:
            outcomes.append("error")
        else:
            outcomes.append("success")

    threads = [
        threading.Thread(target=initialize, args=("a" * 64,)),
        threading.Thread(target=initialize, args=("c" * 64,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
    assert outcomes.count("success") == 1
    assert outcomes.count("error") == 1


def test_connect_writer_closes_memory_connection_on_durability_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication.sqlite3"
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    closed: list[bool] = []

    class FakeConnection:
        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(replication, "_deserialize_sqlite_bytes", lambda _payload: FakeConnection())
    monkeypatch.setattr(
        replication,
        "_assert_writer_session_stable",
        lambda _session: (_ for _ in ()).throw(
            ReplicationDurabilityError("injected durability failure")
        ),
    )
    with pytest.raises(ReplicationDurabilityError, match="durability"):
        ReplicationSidecarStore._connect_writer(path)
    assert closed == [True]


def test_writer_dirs_reject_symlink_ancestor_without_creating_through_it(tmp_path: Path) -> None:
    real_control = tmp_path / "real-control"
    real_control.mkdir()
    linked_control = tmp_path / "linked-control"
    linked_control.symlink_to(real_control, target_is_directory=True)
    settings = _settings(tmp_path, local_control_dir=linked_control)
    with pytest.raises((OSError, ValueError, RuntimeError)):
        StorageLayout(settings).ensure_replication_writer_dirs()
    assert not (real_control / "replication-journal").exists()


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
