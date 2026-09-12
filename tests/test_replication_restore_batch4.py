import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pytest

import backend.app.storage.replication as replication
from backend.app.storage.replication import (
    DestinationArchiveWriter,
    RestoreAuditStore,
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


def test_execute_requires_explicit_audit_root_before_copy(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"

    result = RestoreService(archive).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "OUTBOX_DURABILITY_UNAVAILABLE"
    assert not destination.exists()


def test_verified_restore_publishes_once_after_all_semantic_checks(tmp_path: Path) -> None:
    archive, _descriptor, record = _archive(tmp_path)
    destination = tmp_path / "restore"

    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

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

    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"VERIFY_FAILED", "SOURCE_UNAVAILABLE"}
    assert not destination.exists()


def test_restore_query_failure_is_before_visibility(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"

    result = RestoreService(
        archive,
        audit_store=RestoreAuditStore(tmp_path / "audit"),
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
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"DESTINATION_CONFLICT", "PATH_CHANGED"}
    assert destination.read_text() == "attacker"


def test_staging_extra_after_query_is_rejected_and_audit_is_sanitized(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    audit = RestoreAuditStore(tmp_path / "audit")

    def add_extra(stage: Path) -> None:
        (stage / "unexpected.txt").write_text("not part of the inventory")

    result = RestoreService(
        archive, audit_store=audit, representative_query=add_extra
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "VERIFY_FAILED"
    assert not destination.exists()
    reports = list((tmp_path / "audit").glob("*.json"))
    assert len(reports) == 2
    for report_path in reports:
        report = report_path.read_text()
        assert str(tmp_path) not in report
        assert "unexpected.txt" not in report


def test_restore_audit_is_two_phase_descriptor_bound_and_effects_are_split(
    tmp_path: Path,
) -> None:
    archive, descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    audit = RestoreAuditStore(tmp_path / "audit")

    result = RestoreService(archive, audit_store=audit).execute(destination)

    assert result.status == "ready"
    assert result.effects.destination_writes is False
    assert result.effects.restore_writes is True
    assert result.effects.audit_writes is True
    assert result.audit_status == "terminal"
    started = next((tmp_path / "audit").glob("*.started.json"))
    terminal = next((tmp_path / "audit").glob("*.terminal.json"))
    assert f'"destination_id":"{descriptor.destination_id}"' in started.read_text()
    assert '"destination_ancestry_sha256":"' in started.read_text()
    assert '"attempt_basename_sha256":"' in started.read_text()
    assert '"final_basename_sha256":"' in started.read_text()
    assert '"start_event_sha256"' in terminal.read_text()


def test_restore_staging_basename_swap_does_not_delete_attacker_or_publish(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"

    def swap(stage: Path) -> None:
        moved = stage.with_name(stage.name + ".moved")
        stage.rename(moved)
        stage.symlink_to(moved, target_is_directory=True)

    result = RestoreService(
        archive,
        audit_store=RestoreAuditStore(tmp_path / "audit"),
        representative_query=swap,
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"VERIFY_FAILED", "SOURCE_UNAVAILABLE"}
    assert not destination.exists()


@pytest.mark.parametrize("replacement", ["recreate", "symlink"])
def test_restore_destination_base_replacement_is_rejected_before_publish(
    tmp_path: Path, replacement: str
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    base = tmp_path / "base"
    base.mkdir()
    destination = base / "restore"

    def replace_base(stage: Path) -> None:
        moved = base.with_name("base.moved")
        base.rename(moved)
        if replacement == "recreate":
            base.mkdir()
        else:
            base.symlink_to(moved, target_is_directory=True)

    result = RestoreService(
        archive,
        audit_store=RestoreAuditStore(tmp_path / "audit"),
        representative_query=replace_base,
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"PATH_INVALID", "SOURCE_UNAVAILABLE"}
    assert not destination.exists()
    assert not (base.with_name("base.moved") / "restore").exists()


@pytest.mark.parametrize("replacement", ["recreate", "symlink"])
def test_restore_destination_intermediate_replacement_is_rejected_before_publish(
    tmp_path: Path, replacement: str
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    outer = tmp_path / "outer"
    inner = outer / "inner"
    inner.mkdir(parents=True)
    destination = inner / "restore"

    def replace_intermediate(stage: Path) -> None:
        moved = outer.with_name("outer.moved")
        outer.rename(moved)
        if replacement == "recreate":
            outer.mkdir()
        else:
            outer.symlink_to(moved, target_is_directory=True)

    result = RestoreService(
        archive,
        audit_store=RestoreAuditStore(tmp_path / "audit"),
        representative_query=replace_intermediate,
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code in {"PATH_INVALID", "SOURCE_UNAVAILABLE"}
    assert not destination.exists()
    assert not (outer.with_name("outer.moved") / "inner" / "restore").exists()


@pytest.mark.parametrize("replacement", ["recreate", "symlink"])
def test_restore_ambient_swap_after_rename_is_compensated_by_held_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    base = tmp_path / "base"
    base.mkdir()
    destination = base / "restore"
    original_rename = replication._destination_rename_noreplace

    def rename_with_ambient_swap(parent_fd: int, source: str, target: str) -> None:
        moved = base.with_name("base.moved")
        base.rename(moved)
        if replacement == "recreate":
            base.mkdir()
        else:
            base.symlink_to(moved, target_is_directory=True)
        original_rename(parent_fd, source, target)

    monkeypatch.setattr(replication, "_destination_rename_noreplace", rename_with_ambient_swap)
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.effects.restore_writes is True
    assert result.orphan_cleanup_status == "removed"
    assert not destination.exists()
    assert not (base.with_name("base.moved") / "restore").exists()
    terminal = next((tmp_path / "audit").glob("*.terminal.json"))
    assert '"orphan_cleanup_status":"removed"' in terminal.read_text()


def test_restore_ambient_swap_inode_mismatch_never_deletes_detached_final(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    base = tmp_path / "base"
    base.mkdir()
    destination = base / "restore"
    original_rename = replication._destination_rename_noreplace

    def rename_with_ambient_swap(parent_fd: int, source: str, target: str) -> None:
        moved = base.with_name("base.moved")
        base.rename(moved)
        base.mkdir()
        original_rename(parent_fd, source, target)

    def reject_owned_final(*args: object, **kwargs: object) -> None:
        raise replication.ReplicationStateUnavailable("injected final inode mismatch")

    monkeypatch.setattr(replication, "_destination_rename_noreplace", rename_with_ambient_swap)
    monkeypatch.setattr(replication, "_assert_owned_final_binding", reject_owned_final)
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.effects.restore_writes is True
    assert result.orphan_cleanup_status == "unknown"
    assert not destination.exists()
    assert (base.with_name("base.moved") / "restore").is_dir()


def test_restore_post_rename_failure_keeps_visible_root_and_skips_semantic_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    calls = 0
    original = replication._verify_staged_restore_tree

    def fail_after_rename(*args: object, **kwargs: object) -> None:
        nonlocal calls
        if kwargs.get("semantic", True) is False:
            raise replication.ReplicationStateUnavailable("post-rename identity failure")
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(replication, "_verify_staged_restore_tree", fail_after_rename)
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.effects.destination_writes is False
    assert result.audit_status == "terminal"
    assert destination.is_dir()
    assert calls == 2


def test_restore_final_inode_swap_is_unknown_manual_orphan_without_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    attacker = tmp_path / "restore.attacker"
    original = replication._verify_staged_restore_tree
    swapped = False

    def swap_after_final_readback(*args: object, **kwargs: object) -> None:
        nonlocal swapped
        original(*args, **kwargs)
        if kwargs.get("semantic", True) is False and not swapped:
            swapped = True
            destination.rename(attacker)
            destination.mkdir()

    monkeypatch.setattr(replication, "_verify_staged_restore_tree", swap_after_final_readback)
    audit_root = tmp_path / "audit"
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(audit_root)
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.effects.restore_writes is True
    assert result.orphan_cleanup_status == "unknown"
    assert result.manual_intervention_required is True
    assert result.observed_final_inode_sha256 is not None
    assert destination.is_dir()
    assert attacker.is_dir()
    terminal = next(audit_root.glob("*.terminal.json"))
    payload = terminal.read_text()
    assert '"manual_intervention_required":true' in payload
    assert '"orphan_cleanup_status":"unknown"' in payload
    assert '"destination_ancestry_sha256":"' in payload
    assert '"attempt_basename_sha256":"' in payload
    assert '"final_basename_sha256":"' in payload
    assert '"observed_final_inode_sha256":"' in payload
    assert str(tmp_path) not in payload


def test_restore_final_inode_swap_immediately_after_rename_keeps_attacker_and_staging_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    attacker = tmp_path / "restore.attacker"
    original = replication._destination_rename_noreplace

    def rename_then_replace_final(parent_fd: int, source: str, target: str) -> None:
        original(parent_fd, source, target)
        destination.rename(attacker)
        destination.mkdir()

    monkeypatch.setattr(replication, "_destination_rename_noreplace", rename_then_replace_final)
    audit_root = tmp_path / "audit"
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(audit_root)
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.orphan_cleanup_status == "unknown"
    assert result.manual_intervention_required is True
    assert destination.is_dir()
    assert attacker.is_dir()
    assert len(list(tmp_path.glob(".restore-*.staging"))) == 0
    assert len(list(audit_root.glob("*.terminal.json"))) == 1


@pytest.mark.parametrize("replacement", ["symlink", "file", "missing"])
def test_restore_final_observation_error_is_unknown_manual_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    destination = tmp_path / "restore"
    attacker = tmp_path / "restore.attacker"
    original = replication._destination_rename_noreplace

    def rename_then_replace_final(parent_fd: int, source: str, target: str) -> None:
        original(parent_fd, source, target)
        destination.rename(attacker)
        if replacement == "symlink":
            destination.symlink_to(attacker, target_is_directory=True)
        elif replacement == "file":
            destination.write_text("attacker")

    monkeypatch.setattr(replication, "_destination_rename_noreplace", rename_then_replace_final)
    audit_root = tmp_path / "audit"
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(audit_root)
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.rename_allowed is False
    assert result.effects.restore_writes is True
    assert result.orphan_cleanup_status == "unknown"
    assert result.manual_intervention_required is True
    assert attacker.is_dir()
    if replacement == "symlink":
        assert destination.is_symlink()
    elif replacement == "file":
        assert destination.is_file()
    else:
        assert not destination.exists()
    assert len(list(tmp_path.glob(".restore-*.staging"))) == 0
    terminal = next(audit_root.glob("*.terminal.json"))
    payload = terminal.read_text()
    assert '"manual_intervention_required":true' in payload
    assert '"orphan_cleanup_status":"unknown"' in payload
    assert str(tmp_path) not in payload


def test_restore_corrupt_archive_has_started_and_terminal_audit(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    generation = next(
        path
        for path in (archive / "_replication" / "history").iterdir()
        if not path.name.startswith(".")
    )
    (generation / "bars.parquet").write_bytes(b"corrupt")

    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(tmp_path / "restore")

    assert result.status == "unavailable"
    assert len(list((tmp_path / "audit").glob("*.started.json"))) == 1
    assert len(list((tmp_path / "audit").glob("*.terminal.json"))) == 1


def test_restore_audit_namespace_extras_fail_before_start(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    audit_root = tmp_path / "audit"
    audit_root.mkdir()
    (audit_root / "unexpected").write_text("x")

    result = RestoreService(archive, audit_store=RestoreAuditStore(audit_root)).execute(
        tmp_path / "restore"
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert not (tmp_path / "restore").exists()
    assert not list(audit_root.glob("*.started.json"))
    assert not list(audit_root.glob("*.terminal.json"))


def test_restore_malformed_audit_staging_temp_fails_closed(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    audit = RestoreAuditStore(tmp_path / "audit")
    audit.ensure_ready()
    (tmp_path / "audit" / ".staging" / (".audit-" + "a" * 32 + ".tmp")).write_bytes(b"{}")

    result = RestoreService(archive, audit_store=audit).execute(tmp_path / "restore")

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert not list((tmp_path / "audit").glob("*.started.json"))


def test_restore_terminal_audit_failure_preserves_primary_reason(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)

    class FailingTerminalAudit(RestoreAuditStore):
        def write_terminal_event(self, event: object) -> None:
            raise RuntimeError("audit terminal failed")

    result = RestoreService(
        archive,
        audit_store=FailingTerminalAudit(tmp_path / "audit"),
        representative_query=lambda _root: (_ for _ in ()).throw(RuntimeError("query failed")),
    ).execute(tmp_path / "restore")

    assert result.status == "unavailable"
    assert result.reason_code == "VERIFY_FAILED"


def test_unreadable_descriptor_terminal_failure_preserves_source_primary(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    (archive / replication.DESTINATION_DESCRIPTOR_NAME).write_bytes(b"corrupt")

    class FailingTerminalAudit(RestoreAuditStore):
        def write_terminal_event(self, event: object) -> None:
            raise RuntimeError("injected terminal audit failure")

    audit_root = tmp_path / "audit"
    result = RestoreService(
        archive, audit_store=FailingTerminalAudit(audit_root)
    ).execute(tmp_path / "restore")

    assert result.status == "unavailable"
    assert result.reason_code == "SOURCE_UNAVAILABLE"
    assert result.audit_status == "unavailable"
    assert result.effects.audit_writes is True
    assert result.effects.restore_writes is False
    assert result.effects.destination_writes is False
    assert len(list(audit_root.glob("*.started.json"))) == 1
    assert len(list(audit_root.glob("*.terminal.json"))) == 0
    assert not (tmp_path / "restore").exists()


def test_restore_audit_reconcile_closes_crash_started_record(tmp_path: Path) -> None:
    audit = RestoreAuditStore(tmp_path / "audit")
    started = audit.write_started(destination_id="a" * 32, started_at="2026-09-12T00:00:00Z")

    assert audit.reconcile() == 1
    assert (tmp_path / "audit" / f"{started.audit_id}.terminal.json").exists()


def test_restore_audit_reconcile_reports_unknown_orphan_without_deletion(
    tmp_path: Path,
) -> None:
    audit = RestoreAuditStore(tmp_path / "audit")
    started = audit.write_started(
        destination_id="a" * 32,
        started_at="2026-09-12T00:00:00Z",
        destination_ancestry_sha256="b" * 64,
        attempt_basename_sha256="c" * 64,
        final_basename_sha256="d" * 64,
    )

    assert audit.reconcile() == 1
    terminal = (tmp_path / "audit" / f"{started.audit_id}.terminal.json").read_text()
    assert '"orphan_cleanup_status":"unknown"' in terminal
    assert '"reason_code":"CONTROL_STATE_UNAVAILABLE"' in terminal


def test_restore_unreadable_descriptor_audits_null_destination_identity(
    tmp_path: Path,
) -> None:
    archive, descriptor, _record = _archive(tmp_path)
    (archive / replication.DESTINATION_DESCRIPTOR_NAME).write_bytes(b"corrupt")

    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(tmp_path / "restore")

    assert result.status == "unavailable"
    assert result.reason_code == "SOURCE_UNAVAILABLE"
    started = next((tmp_path / "audit").glob("*.started.json"))
    assert '"destination_id":null' in started.read_text()
    assert descriptor.destination_id not in started.read_text()


def test_restore_malformed_generation_is_typed_and_terminalized(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)

    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(tmp_path / "restore", generation=[])

    assert result.status == "unavailable"
    assert result.reason_code == "SOURCE_UNAVAILABLE"
    assert result.effects.audit_writes is True
    assert result.audit_status == "terminal"
    assert len(list((tmp_path / "audit").glob("*.started.json"))) == 1
    assert len(list((tmp_path / "audit").glob("*.terminal.json"))) == 1


def test_restore_audit_concurrent_writers_serialize_without_overwrite(tmp_path: Path) -> None:
    audit = RestoreAuditStore(tmp_path / "audit")

    def write_one(index: int) -> str:
        event = audit.write_started(
            destination_id=f"{index:032x}", started_at="2026-09-12T00:00:00Z"
        )
        return event.audit_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        audit_ids = list(pool.map(write_one, range(8)))

    assert len(set(audit_ids)) == 8
    assert len(list((tmp_path / "audit").glob("*.started.json"))) == 8
    assert len(list((tmp_path / "audit").glob("*.terminal.json"))) == 0


def test_concurrent_restores_different_targets_allow_unrelated_parent_changes(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    barrier = threading.Barrier(2)

    def run(index: int) -> object:
        return RestoreService(
            archive,
            audit_store=RestoreAuditStore(tmp_path / f"audit-{index}"),
            representative_query=lambda _stage: barrier.wait(timeout=10),
        ).execute(tmp_path / f"restore-{index}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, range(2)))

    assert [result.status for result in results] == ["ready", "ready"]
    assert all((tmp_path / f"restore-{index}").is_dir() for index in range(2))


def test_concurrent_restores_same_target_have_one_atomic_winner(
    tmp_path: Path,
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    barrier = threading.Barrier(2)
    destination = tmp_path / "restore"

    def run(index: int) -> object:
        return RestoreService(
            archive,
            audit_store=RestoreAuditStore(tmp_path / f"audit-same-{index}"),
            representative_query=lambda _stage: barrier.wait(timeout=10),
        ).execute(destination)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, range(2)))

    assert sorted(result.status for result in results) == ["ready", "unavailable"]
    assert destination.is_dir()
    assert all(
        result.reason_code == "NONE"
        or result.reason_code in {"DESTINATION_CONFLICT", "PATH_CHANGED"}
        for result in results
    )


def test_symlinked_canonical_root_is_rejected_without_copy(tmp_path: Path) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    alias = tmp_path / "canonical-alias"
    alias.symlink_to(canonical, target_is_directory=True)
    destination = tmp_path / "restore"

    result = RestoreService(
        archive,
        canonical_roots=(alias,),
        audit_store=RestoreAuditStore(tmp_path / "audit"),
    ).execute(destination)

    assert result.status == "unavailable"
    assert result.reason_code == "SYMLINK_UNSAFE"
    assert not destination.exists()


@pytest.mark.parametrize("unsafe", [Path("relative"), Path("~")])
def test_restore_rejects_non_absolute_destination_without_writes(
    tmp_path: Path, unsafe: Path
) -> None:
    archive, _descriptor, _record = _archive(tmp_path)
    result = RestoreService(
        archive, audit_store=RestoreAuditStore(tmp_path / "audit")
    ).execute(unsafe)
    assert result.status == "unavailable"
    assert result.reason_code == "PATH_INVALID"
