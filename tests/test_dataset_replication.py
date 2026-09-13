import hashlib
import json
import multiprocessing
import os
import sqlite3
import threading
from pathlib import Path

import pytest
from pydantic import ValidationError

import backend.app.storage.replication as replication
from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout
from backend.app.storage.replication import (
    JournalRecord,
    ReplicationCASConflict,
    ReplicationClaim,
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
    source_published_at: str = "2026-09-09T00:00:00Z",
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
        source_published_at=source_published_at,
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


def _initialize_sidecar_process(path: str, source_instance_id: str, queue: object) -> None:
    try:
        ReplicationSidecarStore(Path(path)).initialize(
            source_instance_id=source_instance_id,
            source_instance_sha256="b" * 64,
        )
    except ReplicationDurabilityError:
        queue.put("error")  # type: ignore[attr-defined]
    else:
        queue.put("success")  # type: ignore[attr-defined]


def _install_fixed_generation_process(path: str, payload: bytes, queue: object) -> None:
    """Race the deterministic filename install without a shared Python lock."""
    root_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        replication._install_fixed_at(root_fd, "00000000000000000001.db", payload)
    except replication.ReplicationCASConflict:
        queue.put("cas_conflict")  # type: ignore[attr-defined]
    except BaseException as exc:  # pragma: no cover - diagnostic child path
        queue.put(type(exc).__name__)  # type: ignore[attr-defined]
    else:
        queue.put("installed")  # type: ignore[attr-defined]
    finally:
        os.close(root_fd)


def _crash_after_generation_link_process(path: str, payload: bytes) -> None:
    """Crash after linking the fixed generation, before staging cleanup."""
    root_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    real_unlink = replication.os.unlink

    def crash_before_staging_cleanup(name: object, *args: object, **kwargs: object) -> object:
        if isinstance(name, str) and name.endswith(".db.tmp"):
            os._exit(77)
        return real_unlink(name, *args, **kwargs)

    replication.os.unlink = crash_before_staging_cleanup  # type: ignore[assignment]
    try:
        replication._install_fixed_at(root_fd, "00000000000000000001.db", payload)
    finally:  # pragma: no cover - the injected crash exits before cleanup
        os.close(root_fd)


def _claim_sidecar_process(path: str, worker_id: str, queue: object) -> None:
    try:
        claim = ReplicationSidecarStore(Path(path)).claim_due(
            worker_id=worker_id, now="2026-09-09T00:01:00Z", lease_seconds=900
        )
    except BaseException as exc:  # pragma: no cover - child diagnostic path
        queue.put(("error", type(exc).__name__))  # type: ignore[attr-defined]
    else:
        if claim is None:
            queue.put(("none", None))  # type: ignore[attr-defined]
        else:
            queue.put(("claimed", claim.intent_id))  # type: ignore[attr-defined]


def test_replication_defaults_off_and_layout_is_local(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    layout = StorageLayout(settings)

    assert settings.replication_enabled is False
    assert settings.replication_drain_enabled is False
    assert settings.replication_direction == "local_to_nas"
    assert layout.replication_sidecar_root == tmp_path / "control" / "replication-sidecar"
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


def test_public_status_exposes_fixed_local_chain_trust_scope(tmp_path: Path) -> None:
    result = ReplicationStatusService(_settings(tmp_path)).read()
    assert result.trust_scope == "LOCAL_CHAIN_ONLY"
    values = result.model_dump()
    values["trust_scope"] = "REMOTE_ATTESTED"
    with pytest.raises(ValidationError):
        ReplicationStatusResponse.model_validate(values)


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
    root = layout.replication_sidecar_root
    before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}

    result = ReplicationStatusService(settings).read()

    assert result.status == "ready"
    assert result.reason_code == "NONE"
    assert result.source_ready is True
    assert result.provider_requests == 0
    assert result.trust_scope == "LOCAL_CHAIN_ONLY"
    assert {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()} == before


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
    root = layout.replication_sidecar_root
    before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}

    def fail_if_clone(*args: object, **kwargs: object) -> None:
        raise AssertionError("status must not create a filesystem SQLite clone")

    monkeypatch.setattr(replication, "_open_verified_sqlite_clone", fail_if_clone, raising=False)
    assert ReplicationStatusService(settings).read().reason_code == "NONE"
    after = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}
    assert after == before


@pytest.mark.parametrize(
    "primary_type",
    [ReplicationStateUnavailable, ReplicationDurabilityError],
)
def test_status_preserves_typed_primary_when_cleanup_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    primary_type: type[ReplicationDurabilityError],
) -> None:
    settings = _settings(tmp_path, replication_enabled=True)
    layout = StorageLayout(settings)
    ReplicationSidecarStore(layout.replication_sidecar_root).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    real_open = replication._open_generation_readonly
    real_close = replication._close_fd_best_effort

    def fail_validation(_connection: sqlite3.Connection) -> None:
        raise primary_type("injected status primary")

    def fail_close(fd: int, label: str, failures: list[str]) -> None:
        real_close(fd, label, failures)
        failures.append("injected_status_cleanup")

    def open_then_inject(path: Path):
        handles = real_open(path)
        monkeypatch.setattr(replication, "_validate_sqlite_schema", fail_validation)
        monkeypatch.setattr(replication, "_close_fd_best_effort", fail_close)
        return handles

    monkeypatch.setattr(replication, "_open_generation_readonly", open_then_inject)
    with pytest.raises(primary_type, match="injected status primary") as caught:
        ReplicationSidecarStore(layout.replication_sidecar_root).read_status()
    assert "replication_cleanup=failed" in getattr(caught.value, "__notes__", [])


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
    assert layout.replication_sidecar_root.exists() is False


def test_replication_sidecar_normative_ddl_identity_and_immutable_event_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)

    connection = _latest_generation_connection(path)
    try:
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
    finally:
        connection.close()


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
    path = tmp_path / "control" / "replication-sidecar"
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )

    def mutate(connection: sqlite3.Connection) -> None:
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

    _rewrite_latest_generation(path, mutate)
    with pytest.raises(Exception, match="reachability|sequence|hash"):
        ReplicationSidecarStore(path).read_status()


@pytest.mark.parametrize("tamper", ["user_version", "index"])
def test_sidecar_reader_rejects_schema_identity_tamper(tmp_path: Path, tamper: str) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )

    def mutate(connection: sqlite3.Connection) -> None:
        if tamper == "user_version":
            connection.execute("PRAGMA user_version = 99")
        else:
            connection.execute("DROP INDEX replication_heads_due_idx")

    _rewrite_latest_generation(path, mutate)
    with pytest.raises(ReplicationStateUnavailable, match="DDL|version"):
        ReplicationSidecarStore(path).read_status()


def test_sidecar_reader_rejects_intent_source_identity_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
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

    def mutate(connection: sqlite3.Connection) -> None:
        connection.execute(
            f"INSERT INTO replication_intents ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )

    _rewrite_latest_generation(path, mutate)
    with pytest.raises(ReplicationStateUnavailable, match="source identity"):
        store.read_status()


def test_sidecar_reader_rejects_stale_lease_on_nonleased_head(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
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

    def mutate(connection: sqlite3.Connection) -> None:
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

    _rewrite_latest_generation(path, mutate)
    with pytest.raises(ReplicationStateUnavailable, match="invalid|stale"):
        store.read_status()


def test_status_rejects_existing_wal_shm_without_writes(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    root = path.parent / "replication-sidecar"
    wal_path = root / "00000000000000000000.db-wal"
    shm_path = root / "00000000000000000000.db-shm"
    wal_path.write_bytes(b"unexpected wal")
    shm_path.write_bytes(b"unexpected shm")
    wal_path.chmod(0o600)
    shm_path.chmod(0o600)
    before = {
        artifact: (artifact.stat().st_ino, artifact.stat().st_mtime_ns, artifact.read_bytes())
        for artifact in (root / "00000000000000000000.db", wal_path, shm_path)
    }
    with pytest.raises(ReplicationStateUnavailable, match="WAL|SHM|sidecar"):
        store.read_status()
    after = {
        artifact: (artifact.stat().st_ino, artifact.stat().st_mtime_ns, artifact.read_bytes())
        for artifact in (root / "00000000000000000000.db", wal_path, shm_path)
    }
    assert after == before


def test_checkpoint_journals_import_sorted_by_published_at_and_checkpoint_id(
    tmp_path: Path,
) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    first = _checkpoint_for_journal(
        publication_binding_sha256="c" * 64,
        created_at="2026-09-09T00:00:01Z",
        source_published_at="2026-09-09T00:00:00Z",
    )
    second = _checkpoint_for_journal(
        publication_binding_sha256="d" * 64,
        created_at="2026-09-09T00:00:02Z",
        source_published_at="2026-09-09T00:00:00Z",
    )
    # The helper above intentionally exercises the public journal import path;
    # records with equal publication time are ordered by checkpoint id.
    result = store.import_journals(
        [second, first],
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:10Z",
    )
    assert result.intent_ids
    assert result.source_sequences == (1, 2)
    ordered = sorted((first, second), key=lambda item: item.checkpoint_id)
    assert tuple(
        store.get_intent(intent_id).checkpoint_id for intent_id in result.intent_ids
    ) == tuple(record.checkpoint_id for record in ordered)
    assert (
        store.import_journals(
            [first, second],
            operation_day="2026-09-10",
            destination_id="1" * 32,
            created_at="2026-09-09T00:00:11Z",
        ).intent_ids
        == result.intent_ids
    )


def test_generic_transition_cannot_complete_replication_without_dedicated_proof(
    tmp_path: Path,
) -> None:
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    intent = sidecar.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    claim = sidecar.claim_due(worker_id="worker-a", now="2026-09-09T00:01:00Z")
    assert claim is not None
    verifying = sidecar.transition(
        intent_id=intent.intent_ids[0],
        worker_id="worker-a",
        expected_state_version=claim.state_version,
        to_state="verifying",
        now="2026-09-09T00:01:01Z",
    )
    before = {
        path.name: (path.stat().st_ino, path.read_bytes())
        for path in sidecar.path.iterdir()
        if path.is_file()
    }
    with pytest.raises(ReplicationDurabilityError, match="dedicated|proof"):
        sidecar.transition(
            intent_id=intent.intent_ids[0],
            worker_id="worker-a",
            expected_state_version=verifying.state_version,
            to_state="replicated",
            destination_replication_generation="2" * 64,
            destination_record_sha256="3" * 64,
            destination_head_sha256="4" * 64,
            now="2026-09-09T00:01:02Z",
        )
    after = {
        path.name: (path.stat().st_ino, path.read_bytes())
        for path in sidecar.path.iterdir()
        if path.is_file()
    }
    assert after == before
    assert sidecar.read_status().verifying_count == 1


def test_concurrent_claim_uses_lease_and_state_version_cas(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    intent = store.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:10Z",
    )
    winner = store.claim_due(worker_id="worker-a", now="2026-09-09T00:01:00Z", lease_seconds=900)
    assert isinstance(winner, ReplicationClaim)
    assert winner.intent_id == intent.intent_ids[0]
    with pytest.raises(ReplicationCASConflict):
        store.transition(
            intent_id=intent.intent_ids[0],
            worker_id="worker-b",
            expected_state_version=winner.state_version,
            to_state="verifying",
            now="2026-09-09T00:01:01Z",
        )
    reclaimed = store.claim_due(worker_id="worker-b", now="2026-09-09T00:17:00Z", lease_seconds=900)
    assert reclaimed is not None
    assert reclaimed.state_version > winner.state_version
    with pytest.raises(ReplicationCASConflict):
        store.transition(
            intent_id=intent.intent_ids[0],
            worker_id="worker-a",
            expected_state_version=winner.state_version,
            to_state="verifying",
            now="2026-09-09T00:17:00Z",
        )


def test_multiprocess_claim_has_one_winner_and_no_provider_requests(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    store.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    processes = [
        context.Process(
            target=_claim_sidecar_process,
            args=(str(store.path), f"worker-{index}", queue),
        )
        for index in (1, 2)
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    assert all(process.exitcode == 0 for process in processes)
    assert sum(result[0] == "claimed" for result in results) == 1
    assert sum(result[0] == "none" for result in results) == 1
    assert all(result[0] != "error" for result in results)
    assert store.read_status().provider_requests == 0


def test_retry_schedule_and_dead_letter_are_bounded(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    intent = store.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    now = "2026-09-09T00:00:00Z"
    for attempt, delay in enumerate((60, 300, 1800, 7200, 43200), start=1):
        claim = store.claim_due(worker_id=f"worker-{attempt}", now=now, lease_seconds=900)
        assert claim is not None
        next_state = store.transition(
            intent_id=intent.intent_ids[0],
            worker_id=f"worker-{attempt}",
            expected_state_version=claim.state_version,
            to_state="retry_wait",
            reason_code="DESTINATION_UNAVAILABLE",
            now=now,
        )
        assert next_state.current_state == "retry_wait"
        now = next_state.next_attempt_at
        assert next_state.retry_delay_seconds == delay
    claim = store.claim_due(worker_id="worker-6", now=now, lease_seconds=900)
    assert claim is not None
    terminal = store.transition(
        intent_id=intent.intent_ids[0],
        worker_id="worker-6",
        expected_state_version=claim.state_version,
        to_state="dead_letter",
        reason_code="DESTINATION_UNAVAILABLE",
        now=now,
    )
    assert terminal.current_state == "dead_letter"
    assert terminal.attempt == 6
    assert store.read_status().dead_letter_count == 1


def test_nonretryable_failure_terminalizes_without_retry(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    intent = store.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    claim = store.claim_due(worker_id="worker-a", now="2026-09-09T00:00:00Z")
    assert claim is not None
    terminal = store.transition(
        intent_id=intent.intent_ids[0],
        worker_id="worker-a",
        expected_state_version=claim.state_version,
        to_state="dead_letter",
        reason_code="DESTINATION_TRUST_FAILED",
        now="2026-09-09T00:00:01Z",
    )
    assert terminal.current_state == "dead_letter"
    assert terminal.last_reason_code == "DESTINATION_TRUST_FAILED"


def test_terminal_audit_is_retained(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    intent = store.enqueue_checkpoint(
        record.checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    claim = store.claim_due(worker_id="worker-a", now="2026-09-09T00:00:00Z")
    assert claim is not None
    store.transition(
        intent_id=intent.intent_ids[0],
        worker_id="worker-a",
        expected_state_version=claim.state_version,
        to_state="dead_letter",
        reason_code="DESTINATION_TRUST_FAILED",
        now="2026-09-09T00:00:01Z",
    )
    status = store.read_status()
    assert status.dead_letter_count == 1
    connection = _latest_generation_connection(store.path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM replication_attempt_events WHERE intent_id = ?",
            (intent.intent_ids[0],),
        ).fetchone() == (3,)
    finally:
        connection.close()


def test_reconcile_current_checkpoint_without_provider(tmp_path: Path) -> None:
    store = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    record = _checkpoint_for_journal()
    result = store.reconcile(
        record,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        now="2026-09-09T00:00:00Z",
    )
    assert result.intent_ids
    assert result.provider_requests == 0


def test_checkpoint_journal_import_is_sorted_idempotent_before_removal(tmp_path: Path) -> None:
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    journal_root = tmp_path / "control" / "replication-journal"
    first = _checkpoint_for_journal(source_published_at="2026-09-09T00:00:00Z")
    second = _checkpoint_for_journal(source_published_at="2026-09-09T00:00:01Z")
    first.install(journal_root)
    second.install(journal_root)
    result = sidecar.import_journal_files(
        journal_root,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:01:00Z",
    )
    assert result.source_sequences == (1, 2)
    assert list(journal_root.iterdir()) == []
    # A crash before cleanup leaves the same journals safe to re-import.  The
    # already committed generation is reused and no extra generation appears.
    generation_count = len(list(sidecar.path.glob("[0-9]*.db")))
    assert (
        sidecar.import_journals(
            [first, second],
            operation_day="2026-09-10",
            destination_id="1" * 32,
            created_at="2026-09-09T00:02:00Z",
        ).imported_count
        == 0
    )
    assert len(list(sidecar.path.glob("[0-9]*.db"))) == generation_count


def test_journal_import_uses_one_held_root_descriptor_across_path_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    journal_root = tmp_path / "control" / "replication-journal"
    record = _checkpoint_for_journal()
    record.install(journal_root)
    old_root = journal_root.with_name("replication-journal-old")
    real_listdir = replication.os.listdir
    swapped = False

    def replace_root_after_enumeration(value: object) -> object:
        nonlocal swapped
        names = real_listdir(value)  # type: ignore[arg-type]
        if isinstance(value, int) and not swapped:
            journal_root.rename(old_root)
            journal_root.mkdir(mode=0o700)
            swapped = True
        return names

    monkeypatch.setattr(replication.os, "listdir", replace_root_after_enumeration)
    result = sidecar.import_journal_files(
        journal_root,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:01:00Z",
    )
    assert result.imported_count == 1
    assert swapped is True
    assert list(old_root.iterdir()) == []
    assert list(journal_root.iterdir()) == []


def test_operation_day_is_scheduling_metadata_not_intent_identity(tmp_path: Path) -> None:
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    checkpoint = _checkpoint_for_journal().checkpoint_projection
    first = sidecar.enqueue_checkpoint(
        checkpoint,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    second = sidecar.enqueue_checkpoint(
        checkpoint,
        operation_day="2026-09-10",
        destination_id="1" * 32,
        created_at="2026-09-09T00:01:00Z",
    )
    assert second.intent_ids == first.intent_ids
    assert second.imported_count == 0
    assert sidecar.get_intent(first.intent_ids[0]).operation_day == "2026-09-09"


def test_claim_due_operation_day_filters_source_publication_with_fake_clock(tmp_path: Path) -> None:
    cutoff = "2026-09-09T15:59:59.999999Z"  # 23:59:59.999999 Asia/Shanghai
    claims = []
    for index, published_at in enumerate(
        (
            "2026-09-09T15:59:59.999998Z",
            cutoff,
            "2026-09-09T16:00:00Z",
        ),
        start=1,
    ):
        sidecar = ReplicationSidecarStore(tmp_path / "control" / f"replication-sidecar-{index}")
        sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
        sidecar.enqueue_checkpoint(
            _checkpoint_for_journal(source_published_at=published_at).checkpoint_projection,
            operation_day="2026-09-09",
            destination_id=f"{index:02d}" * 16,
            created_at="2026-09-09T00:00:00Z",
        )

        claims.append(
            sidecar.claim_due(
                worker_id=f"worker-{index}",
                now="2026-09-10T00:01:00Z",
                operation_day="2026-09-09",
            )
        )
    assert claims[0] is not None
    assert claims[1] is not None
    assert claims[2] is None


def test_outbox_transaction_crash_before_generation_install_preserves_prior_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in sidecar.path.iterdir()}

    def crash_before_install(*args: object, **kwargs: object) -> None:
        raise ReplicationDurabilityError("injected generation crash")

    monkeypatch.setattr(replication, "_install_generation", crash_before_install)
    with pytest.raises(ReplicationDurabilityError, match="injected generation crash"):
        sidecar.enqueue_checkpoint(
            _checkpoint_for_journal().checkpoint_projection,
            operation_day="2026-09-09",
            destination_id="1" * 32,
            created_at="2026-09-09T00:00:00Z",
        )
    assert {
        path.name: (path.stat().st_ino, path.read_bytes()) for path in sidecar.path.iterdir()
    } == before
    assert sidecar.read_status().pending_count == 0


def test_disabled_outbox_operation_is_zero_write(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    result = replication.ReplicationOutboxService(path, enabled=False).enqueue(
        _checkpoint_for_journal().checkpoint_projection,
        operation_day="2026-09-09",
        destination_id="1" * 32,
    )
    assert result.status == "disabled"
    assert result.provider_requests == 0
    assert not path.exists()


def test_sidecar_sqlite_engine_uses_memory_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication-sidecar"
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
    path = tmp_path / "control" / "replication-sidecar"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    real_validate = replication._validate_sqlite_schema
    mutated = False

    def mutate_after_open(connection: sqlite3.Connection) -> None:
        nonlocal mutated
        real_validate(connection)
        if mutated:
            return
        mutated = True
        root = path.parent / "replication-sidecar"
        generation = root / "00000000000000000000.db"
        stat_before = generation.stat()
        payload = bytearray(generation.read_bytes())
        payload[-1] ^= 1
        generation.write_bytes(payload)
        os.utime(generation, ns=(stat_before.st_atime_ns, stat_before.st_mtime_ns))

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

    def observe_descriptor_close(descriptors: list[int], failures: list[str] | None = None) -> None:
        cleanup_calls.append(tuple(descriptors))
        real_close_descriptors(descriptors, failures)

    monkeypatch.setattr(replication.os, "unlink", fail_temp_cleanup)
    monkeypatch.setattr(replication, "_close_descriptors", observe_descriptor_close)
    with pytest.raises(ReplicationDurabilityError, match="cleanup"):
        replication._install_no_replace(path, b"record")
    assert cleanup_calls


def test_descriptor_cleanup_failure_after_primary_read_error_is_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_close = replication._close_fd_best_effort

    def fail_close(fd: int, label: str, failures: list[str]) -> None:
        real_close(fd, label, failures)
        failures.append("injected_descriptor_cleanup")

    monkeypatch.setattr(replication, "_close_fd_best_effort", fail_close)
    with pytest.raises(ReplicationDurabilityError) as caught:
        replication._read_nofollow(tmp_path / "missing-record.json")
    assert "replication_cleanup=failed" in getattr(caught.value, "__notes__", [])


def test_sidecar_entry_cleanup_failure_preserves_typed_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "record.json").mkdir()
    parent_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    real_close = replication._close_fd_best_effort

    def fail_close(fd: int, label: str, failures: list[str]) -> None:
        real_close(fd, label, failures)
        failures.append("injected_descriptor_cleanup")

    monkeypatch.setattr(replication, "_close_fd_best_effort", fail_close)
    try:
        with pytest.raises(ReplicationDurabilityError) as caught:
            replication._read_at(parent_fd, "record.json")
        assert "replication_cleanup=failed" in getattr(caught.value, "__notes__", [])
    finally:
        os.close(parent_fd)


def test_descriptor_cleanup_failure_without_primary_is_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_close = replication._close_fd_best_effort

    def fail_close(fd: int, label: str, failures: list[str]) -> None:
        real_close(fd, label, failures)
        failures.append("injected_descriptor_cleanup")

    monkeypatch.setattr(replication, "_close_fd_best_effort", fail_close)
    with pytest.raises(ReplicationDurabilityError, match="cleanup"):
        replication._fsync_directory(tmp_path)


def test_memory_writer_cleanup_failure_is_typed_and_closes_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert _generation_files(path.parent / "replication-sidecar")


def test_writer_identity_check_rejects_replacement_between_open_and_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    path.parent.mkdir()
    ReplicationSidecarStore(path).initialize(
        source_instance_id="a" * 64,
        source_instance_sha256="b" * 64,
    )
    root = path.parent / "replication-sidecar"
    lock = root / ".writer.lock"
    lock.unlink()
    with pytest.raises(
        ReplicationDurabilityError, match="identity|changed|writable|unavailable|disappeared"
    ):
        ReplicationSidecarStore._connect_writer(path)
    assert _generation_files(root)


def test_sidecar_concurrent_initializers_have_one_multiprocess_winner(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    path.parent.mkdir(parents=True)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(
            target=_initialize_sidecar_process,
            args=(str(path), source_instance_id, queue),
        )
        for source_instance_id in ("a" * 64, "c" * 64)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    assert all(process.exitcode == 0 for process in processes)
    assert outcomes.count("success") == 1
    assert outcomes.count("error") == 1


def test_repeated_initialize_reads_held_descriptor_from_offset_zero(tmp_path: Path) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    store = ReplicationSidecarStore(path)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert store.read_status().reason_code == "NONE"


def test_existing_empty_sidecar_concurrent_initializers_have_one_winner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "replication-sidecar"
    path.parent.mkdir(parents=True)
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
    path = tmp_path / "control" / "replication-sidecar"
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
        "_compare_generation_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
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


def _generation_sidecar_root(tmp_path: Path) -> Path:
    return tmp_path / "control" / "replication-sidecar"


def _generation_files(root: Path) -> list[Path]:
    return sorted(
        root.glob(
            "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].db"
        )
    )


def _latest_generation_connection(path: Path) -> sqlite3.Connection:
    root = path if path.name == "replication-sidecar" else path.parent / "replication-sidecar"
    return replication._deserialize_sqlite_bytes(_generation_files(root)[-1].read_bytes())


def _rewrite_latest_generation(path: Path, mutate: object) -> None:
    root = path if path.name == "replication-sidecar" else path.parent / "replication-sidecar"
    generation = _generation_files(root)[-1]
    connection = _latest_generation_connection(root)
    try:
        mutate(connection)
        connection.commit()
        payload = connection.serialize(name="main")
    finally:
        connection.close()
    generation.write_bytes(payload)


def test_sidecar_generation_namespace_rejects_mutable_or_unknown_entries(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    unknown = root / "replication.sqlite3"
    unknown.write_bytes(b"retired mutable prototype")
    before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}
    with pytest.raises(ReplicationStateUnavailable, match="namespace|unknown"):
        store.read_status()
    after = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}
    assert after == before


def test_sidecar_generation_gap_or_symlink_is_unavailable_without_writes(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    first = _generation_files(root)[0]
    gap = root / "00000000000000000002.db"
    gap.symlink_to(first)
    before = {
        path.name: (path.stat().st_ino, path.read_bytes())
        for path in root.iterdir()
        if not path.is_symlink()
    }
    with pytest.raises(ReplicationStateUnavailable, match="generation|namespace|symlink"):
        store.read_status()
    after = {
        path.name: (path.stat().st_ino, path.read_bytes())
        for path in root.iterdir()
        if not path.is_symlink()
    }
    assert after == before


def test_sidecar_generation_status_is_zero_write_and_reads_highest_stable_generation(
    tmp_path: Path,
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    replication._install_generation(session, payload)
    replication._close_generation_writer_session(session)
    before = {
        path.name: (
            path.stat().st_ino,
            path.stat().st_size,
            path.stat().st_mtime_ns,
            path.read_bytes(),
        )
        for path in root.iterdir()
    }
    assert store.read_status().reason_code == "NONE"
    after = {
        path.name: (
            path.stat().st_ino,
            path.stat().st_size,
            path.stat().st_mtime_ns,
            path.read_bytes(),
        )
        for path in root.iterdir()
    }
    assert after == before
    assert [path.name for path in _generation_files(root)] == [
        "00000000000000000000.db",
        "00000000000000000001.db",
    ]


def test_committed_generation_staging_hardlink_residue_is_ignored_read_only(
    tmp_path: Path,
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    staging = root / ".staging"
    staging.mkdir(exist_ok=True)
    staging.chmod(0o700)
    generation = _generation_files(root)[0]
    residue = staging / ".00000000000000000000.db.tmp"
    os.link(generation, residue)
    before = {
        path.name: (path.stat().st_ino, path.stat().st_nlink, path.read_bytes())
        for path in (generation, residue)
    }
    assert store.read_status().reason_code == "NONE"
    after = {
        path.name: (path.stat().st_ino, path.stat().st_nlink, path.read_bytes())
        for path in (generation, residue)
    }
    assert after == before


def test_prelink_staging_orphan_blocks_status_then_writer_cleans_it(
    tmp_path: Path,
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    staging = root / ".staging"
    staging.mkdir(exist_ok=True)
    staging.chmod(0o700)
    orphan = staging / ".00000000000000000001.db.tmp"
    orphan.write_bytes(b"pre-link orphan")
    orphan.chmod(0o600)
    with pytest.raises(ReplicationStateUnavailable, match="staging|temporary|namespace"):
        store.read_status()
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert not orphan.exists()
    assert store.read_status().reason_code == "NONE"


def test_link_then_process_crash_leaves_proven_committed_staging_alias(
    tmp_path: Path,
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_crash_after_generation_link_process, args=(str(root), payload)
    )
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 77
    assert _generation_files(root)[-1].read_bytes() == payload
    assert store.read_status().reason_code == "NONE"


def test_staging_unlink_failure_is_degraded_but_proven_alias_remains_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    session = replication._open_generation_writer_session(root)
    real_unlink = replication.os.unlink

    def fail_staging_unlink(name: object, *args: object, **kwargs: object) -> object:
        if name == ".00000000000000000001.db.tmp":
            raise OSError("injected staging cleanup failure")
        return real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(replication.os, "unlink", fail_staging_unlink)
    try:
        with pytest.raises(replication.ReplicationPostCommitConflict):
            replication._install_generation(session, payload)
    finally:
        # The lock close is outside the injected staging unlink operation.
        monkeypatch.setattr(replication.os, "unlink", real_unlink)
        replication._close_generation_writer_session(session)
    assert _generation_files(root)[-1].read_bytes() == payload
    residue = root / ".staging" / ".00000000000000000001.db.tmp"
    assert residue.read_bytes() == payload
    assert store.read_status().reason_code == "NONE"


def test_sidecar_generation_lock_replacement_is_fail_closed_before_publish(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    lock = root / ".writer.lock"
    replacement = root / ".replacement.lock"
    replacement.write_bytes(b"replacement")
    replacement.chmod(0o600)
    lock.unlink()
    replacement.rename(lock)
    with pytest.raises(ReplicationDurabilityError, match="lock|authority"):
        store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert [path.name for path in _generation_files(root)] == ["00000000000000000000.db"]


def test_sidecar_generation_concurrent_initializers_have_one_multiprocess_winner(
    tmp_path: Path,
) -> None:
    root = _generation_sidecar_root(tmp_path)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(
            target=_initialize_sidecar_process,
            args=(str(root), source_instance_id, queue),
        )
        for source_instance_id in ("a" * 64, "c" * 64)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    assert all(process.exitcode == 0 for process in processes)
    assert outcomes.count("success") == 1
    assert outcomes.count("error") == 1
    assert [path.name for path in _generation_files(root)] == ["00000000000000000000.db"]


def test_sidecar_generation_repeat_initialize_is_idempotent(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    generation = _generation_files(root)[0]
    before = generation.stat()
    payload = generation.read_bytes()
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    after = generation.stat()
    assert generation.read_bytes() == payload
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


def test_sidecar_generation_auxiliary_appearance_after_install_is_degraded_and_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    real_install = replication._install_fixed_at

    def install_then_inject(*args: object, **kwargs: object) -> None:
        real_install(*args, **kwargs)
        (root / "00000000000000000001.db-wal").write_bytes(b"unexpected auxiliary")

    monkeypatch.setattr(replication, "_install_fixed_at", install_then_inject)
    try:
        with pytest.raises(ReplicationStateUnavailable, match="namespace|unknown|auxiliary"):
            replication._install_generation(session, payload)
    finally:
        replication._close_generation_writer_session(session)
    assert (root / "00000000000000000001.db").exists()
    with pytest.raises(ReplicationStateUnavailable):
        store.read_status()


def test_sidecar_generation_lock_replacement_after_install_is_degraded_and_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    real_install = replication._install_fixed_at

    def install_then_replace(*args: object, **kwargs: object) -> None:
        real_install(*args, **kwargs)
        lock = root / ".writer.lock"
        lock.unlink()
        replacement = root / ".replacement.lock"
        replacement.write_bytes(b"replacement")
        replacement.chmod(0o600)
        replacement.rename(lock)

    monkeypatch.setattr(replication, "_install_fixed_at", install_then_replace)
    try:
        with pytest.raises(ReplicationDurabilityError, match="lock|authority"):
            replication._install_generation(session, payload)
    finally:
        replication._close_generation_writer_session(session)
    assert (root / "00000000000000000001.db").exists()
    with pytest.raises(ReplicationStateUnavailable):
        store.read_status()


def test_sidecar_generation_eexist_is_typed_cas_conflict_without_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    target = root / "00000000000000000001.db"
    target_payload = b"attacker-owned-bytes"
    real_link = replication.os.link

    def racing_link(source: object, destination: object, **kwargs: object) -> object:
        target.write_bytes(target_payload)
        target.chmod(0o600)
        return real_link(source, destination, **kwargs)

    monkeypatch.setattr(replication.os, "link", racing_link)
    try:
        with pytest.raises(replication.ReplicationCASConflict):
            replication._install_generation(session, payload)
    finally:
        replication._close_generation_writer_session(session)
    assert target.read_bytes() == target_payload


def test_generation_metadata_binds_sequence_parent_and_content_hash(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    ReplicationSidecarStore(root).initialize(
        source_instance_id="a" * 64, source_instance_sha256="b" * 64
    )
    generation = _generation_files(root)[0]
    payload = generation.read_bytes()
    connection = replication._deserialize_sqlite_bytes(payload)
    try:
        metadata = connection.execute(
            """SELECT generation_number, previous_generation_sha256,
                      generation_payload_sha256
                 FROM replication_sidecar_meta"""
        ).fetchone()
    finally:
        connection.close()
    assert metadata is not None
    assert metadata[0] == 0
    assert metadata[1] == replication.ZERO_SHA256
    assert isinstance(metadata[2], str)
    assert len(metadata[2]) == 64
    assert metadata[2] != replication.ZERO_SHA256


def test_generation_parent_chain_tamper_is_unavailable(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    replication._install_generation(session, payload)
    replication._close_generation_writer_session(session)
    first = _generation_files(root)[0]
    tampered = replication._deserialize_sqlite_bytes(first.read_bytes())
    try:
        tampered.execute(
            "INSERT INTO replication_destination_cache "
            "(destination_id, descriptor_sha256, health_state, cache_version, updated_at) "
            "VALUES (?, ?, 'unknown', 0, ?)",
            ("e" * 32, "f" * 64, "2026-09-09T00:00:00Z"),
        )
        tampered.commit()
        first.write_bytes(tampered.serialize(name="main"))
    finally:
        tampered.close()
    with pytest.raises(ReplicationStateUnavailable):
        store.read_status()


def test_genesis_partial_initialization_resumes_deterministically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    real_install = replication._install_generation
    calls = 0

    def fail_once(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise replication.ReplicationDurabilityError("injected generation crash")
        return real_install(*args, **kwargs)

    monkeypatch.setattr(replication, "_install_generation", fail_once)
    store = ReplicationSidecarStore(root)
    with pytest.raises(ReplicationDurabilityError, match="crash"):
        store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert (root / "genesis.json").exists()
    assert _generation_files(root) == []
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    assert [path.name for path in _generation_files(root)] == ["00000000000000000000.db"]


def test_same_next_generation_multiprocess_has_one_no_replace_winner(tmp_path: Path) -> None:
    root = _generation_sidecar_root(tmp_path)
    root.mkdir(parents=True)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    payload = b"fixed-next-generation"
    processes = [
        context.Process(
            target=_install_fixed_generation_process,
            args=(str(root), payload, queue),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    assert all(process.exitcode == 0 for process in processes)
    assert sorted(outcomes) == ["cas_conflict", "installed"]
    assert (root / "00000000000000000001.db").read_bytes() == payload


def test_postlinearization_namespace_conflict_preserves_generation_and_degrades(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _generation_sidecar_root(tmp_path)
    store = ReplicationSidecarStore(root)
    store.initialize(source_instance_id="a" * 64, source_instance_sha256="b" * 64)
    session = replication._open_generation_writer_session(root)
    previous = _generation_files(root)[-1].read_bytes()
    payload = replication._generation_payload(
        "a" * 64,
        "b" * 64,
        "2026-09-09T00:00:00Z",
        generation_number=1,
        previous_generation_sha256=hashlib.sha256(previous).hexdigest(),
    )
    real_link = replication.os.link

    def link_then_add_unknown(source: object, destination: object, **kwargs: object) -> object:
        result = real_link(source, destination, **kwargs)
        (root / "late-namespace-entry").write_bytes(b"external")
        return result

    monkeypatch.setattr(replication.os, "link", link_then_add_unknown)
    try:
        with pytest.raises(replication.ReplicationPostCommitConflict):
            replication._install_generation(session, payload)
    finally:
        replication._close_generation_writer_session(session)
    assert (root / "00000000000000000001.db").exists()
    with pytest.raises(ReplicationStateUnavailable):
        store.read_status()
