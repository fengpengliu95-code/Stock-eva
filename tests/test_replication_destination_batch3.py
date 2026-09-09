import hashlib
import json
from pathlib import Path

import pytest

import backend.app.storage.replication as replication
from backend.app.storage.replication import (
    DestinationArchiveReader,
    DestinationArchiveWriter,
    DestinationDescriptor,
    ReplicationDurabilityError,
    ReplicationSidecarStore,
    VerifiedDestinationCommitProof,
    build_source_checkpoint,
    canonical_json_bytes,
    domain_sha256,
    initialize_destination,
)


def _source_fixture(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    payload = b"bars-data-01"
    object_sha = hashlib.sha256(payload).hexdigest()
    sentinel = canonical_json_bytes({"dataset": "stock-eva-market", "schema_version": 2})
    (source / "bars.parquet").write_bytes(payload)
    (source / ".stock-eva-dataset.json").write_bytes(sentinel)
    manifest = canonical_json_bytes(
        {"objects": [{"path": "bars.parquet", "sha256": object_sha, "size": len(payload)}]}
    )
    (source / "manifest.json").write_bytes(manifest)
    entry = {
        "relative_path": "bars.parquet",
        "object_sha256": object_sha,
        "size_bytes": len(payload),
        "row_count": 1,
        "trade_date": "2026-09-09",
        "source": "fixture",
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
        source_published_at="2026-09-09T00:00:00Z",
        pointer_db_device=1,
        pointer_db_inode=2,
        pointer_db_schema_digest="e" * 64,
        manifest_canonical_sha256="f" * 64,
        source_manifest_bytes_sha256=hashlib.sha256(manifest).hexdigest(),
        source_object_set_sha256=object_set,
        object_inventory=[entry],
    )
    return source, checkpoint, sentinel


def test_destination_descriptor_rejects_unsafe_root_and_binds_digest(tmp_path: Path) -> None:
    with pytest.raises(ReplicationDurabilityError):
        DestinationDescriptor.from_root(Path("/"), single_writer_host_id="host")
    root = tmp_path / "nas"
    root.mkdir()
    descriptor = DestinationDescriptor.from_root(
        root,
        single_writer_host_id="host-a",
        sentinel_sha256="0" * 64,
        mount_point=str(root),
        fs_type="local",
    )
    assert descriptor.destination_id == descriptor.descriptor_sha256[:32]
    descriptor.verify()
    tampered = descriptor.model_copy(update={"single_writer_host_id": "host-b"})
    with pytest.raises(ReplicationDurabilityError):
        tampered.verify()


def test_destination_copy_publishes_record_and_head_only_after_readback(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    persisted = DestinationArchiveReader(destination).descriptor
    assert persisted.model_dump(mode="json") == descriptor.model_dump(mode="json")
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "NONE"
    reader = DestinationArchiveReader(descriptor)
    head = reader.read_head()
    assert head is not None
    record = reader.read_record(head.replication_generation)
    assert record.record_sha256 == head.record_sha256
    assert record.object_count == 1
    assert reader.read_object(head.replication_generation, "bars.parquet") == b"bars-data-01"


def test_destination_copy_is_idempotent_and_does_not_overwrite_head(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    writer = DestinationArchiveWriter(descriptor, source_root=source)
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    head_bytes = (destination / "_replication" / "head.json").read_bytes()
    second = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:02:00Z")
    assert first.record is not None
    assert second.reason_code == "ALREADY_REPLICATED"
    assert (destination / "_replication" / "head.json").read_bytes() == head_bytes


def test_verified_proof_is_required_for_sidecar_completion(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
    )
    intent = sidecar.enqueue_checkpoint(
        checkpoint,
        operation_day="2026-09-09",
        destination_id=descriptor.destination_id,
        created_at="2026-09-09T00:00:00Z",
    )
    claim = sidecar.claim_due(worker_id="worker-a", now="2026-09-09T00:01:00Z")
    assert claim is not None
    verifying = sidecar.transition(
        intent_id=intent.intent_id,
        worker_id="worker-a",
        expected_state_version=claim.state_version,
        to_state="verifying",
        now="2026-09-09T00:01:01Z",
    )
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=claim.source_sequence, now="2026-09-09T00:01:02Z"
    )
    proof = DestinationArchiveReader(descriptor).verify_commit(
        intent_id=intent.intent_id,
        checkpoint_id=checkpoint.checkpoint_id,
        source_sequence=claim.source_sequence,
        worker_id="worker-a",
        state_version=verifying.state_version,
        destination_generation=result.record.replication_generation,
        now="2026-09-09T00:01:03Z",
    )
    completed = sidecar.complete_replication(proof)
    assert completed.current_state == "replicated"


def test_destination_object_hash_failure_never_advances_head(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    (source / "bars.parquet").write_bytes(b"tampered")
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "COPY_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_destination_older_source_cannot_overwrite_newer_head(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(descriptor, source_root=source)
    writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:01:30Z")
    before = (destination / "_replication" / "head.json").read_bytes()
    result = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:02:00Z")
    assert result.reason_code == "DESTINATION_AHEAD"
    assert (destination / "_replication" / "head.json").read_bytes() == before


def test_destination_lock_symlink_and_unsupported_mount_fail_closed(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    lock = destination / "_replication" / ".writer.lock"
    lock.unlink()
    lock.symlink_to(tmp_path / "attacker")
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "MOUNT_UNSUPPORTED"
    assert not (destination / "_replication" / "head.json").exists()
    with pytest.raises(ReplicationDurabilityError):
        DestinationDescriptor.from_root(destination, fs_type="cifs")


def test_destination_sentinel_tamper_is_rejected_before_any_generation(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    (destination / ".stock-eva-dataset.json").write_bytes(b"tampered")
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_destination_reader_rejects_head_alias_tamper_without_path_reopen(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(descriptor, source_root=source)
    result = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.record is not None
    head_path = destination / "_replication" / "head.json"
    values = json.loads(head_path.read_bytes())
    values["manifest_sha256"] = "1" * 64
    values["head_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-head/v1",
        {key: value for key, value in values.items() if key != "head_sha256"},
    )
    head_path.write_bytes(canonical_json_bytes(values))
    with pytest.raises(ReplicationDurabilityError):
        DestinationArchiveReader(descriptor).verify_commit(
            checkpoint_id=checkpoint.checkpoint_id, source_sequence=1
        )


def test_destination_source_symlink_is_copy_failure_and_not_published(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    original = source / "bars.parquet"
    original.unlink()
    original.symlink_to(tmp_path / "attacker")
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "COPY_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_destination_partial_write_is_not_reported_as_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )

    def partial_write(fd: int, payload: bytes) -> None:
        if payload:
            replication.os.write(fd, payload[:1])
        raise ReplicationDurabilityError("injected partial destination write")

    monkeypatch.setattr(replication, "_write_fully", partial_write)
    result = DestinationArchiveWriter(descriptor, source_root=source).replicate(
        checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z"
    )
    assert result.reason_code == "COPY_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_verified_destination_proof_cannot_be_fabricated() -> None:
    with pytest.raises(TypeError):
        VerifiedDestinationCommitProof(
            intent_id=None,
            checkpoint_id="a" * 64,
            destination_id="b" * 32,
            source_sequence=1,
            destination_generation="c" * 64,
            destination_record_sha256="d" * 64,
            destination_head_sha256="e" * 64,
            worker_id=None,
            state_version=None,
            now="2026-09-09T00:00:00Z",
        )
