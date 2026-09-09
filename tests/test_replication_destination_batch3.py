import errno
import hashlib
import json
import os
from pathlib import Path

import pytest

import backend.app.storage.replication as replication
from backend.app.storage.preflight import MountInfo
from backend.app.storage.replication import (
    DestinationArchiveReader,
    DestinationArchiveWriter,
    DestinationCommitVerifier,
    DestinationDescriptor,
    ReplicationClaimContext,
    ReplicationDurabilityError,
    ReplicationSidecarStore,
    ReplicationStateUnavailable,
    VerifiedDestinationCommitProof,
    build_source_checkpoint,
    canonical_json_bytes,
    domain_sha256,
    initialize_destination,
)


class _FakeMountInspector:
    def __init__(
        self,
        root: Path,
        *,
        options: tuple[str, ...] = ("local", "rw"),
        volume_id: str | None = "vol-a",
    ) -> None:
        self.info = MountInfo(root, "apfs", options, volume_id)

    def find_mount(self, path: Path) -> MountInfo:
        del path
        return self.info


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


def test_destination_writer_requires_explicit_configured_host_identity(tmp_path: Path) -> None:
    source, _checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    with pytest.raises(TypeError):
        DestinationArchiveWriter(descriptor, source_root=source)
    with pytest.raises(ReplicationDurabilityError):
        DestinationArchiveWriter(descriptor, source_root=source, writer_host_id="host-b")


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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
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
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    head_bytes = (destination / "_replication" / "head.json").read_bytes()
    second = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:02:00Z")
    assert first.record is not None
    assert second.reason_code == "ALREADY_REPLICATED"
    assert (destination / "_replication" / "head.json").read_bytes() == head_bytes


def test_destination_head_install_never_uses_unconditional_replace(
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

    def fail_unconditional_replace(*args: object, **kwargs: object) -> None:
        raise AssertionError("destination head must not use unconditional replace")

    monkeypatch.setattr(replication.os, "replace", fail_unconditional_replace)
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    assert writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z").record
    second = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:02:00Z")
    assert second.reason_code == "NONE"


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
    sidecar = ReplicationSidecarStore(
        tmp_path / "control" / "replication-sidecar",
        destination_verifier=DestinationCommitVerifier(descriptor),
    )
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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=claim.source_sequence, now="2026-09-09T00:01:02Z")
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


def test_completion_rejects_unconfigured_or_mutated_proof_before_sidecar_write(
    tmp_path: Path,
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    result = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.record is not None
    proof = DestinationArchiveReader(descriptor).verify_commit(
        checkpoint_id=checkpoint.checkpoint_id,
        source_sequence=1,
        now="2026-09-09T00:01:01Z",
    )
    sidecar = ReplicationSidecarStore(tmp_path / "control" / "replication-sidecar")
    sidecar.initialize(
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
        created_at="2026-09-09T00:00:00Z",
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
        now="2026-09-09T00:01:00Z",
    )
    proof = proof.model_copy(
        update={
            "intent_id": intent.intent_id,
            "worker_id": "worker-a",
            "state_version": verifying.state_version,
        }
    )
    with pytest.raises(ReplicationDurabilityError):
        sidecar.complete_replication(proof)

    class FakeVerifier(DestinationCommitVerifier):
        def verify(
            self, candidate: VerifiedDestinationCommitProof
        ) -> VerifiedDestinationCommitProof:
            return candidate

    with pytest.raises(ReplicationDurabilityError):
        ReplicationSidecarStore(
            tmp_path / "control" / "replication-sidecar",
            destination_verifier=FakeVerifier(descriptor),
        ).complete_replication(proof)
    configured = ReplicationSidecarStore(
        tmp_path / "control" / "replication-sidecar",
        destination_verifier=DestinationCommitVerifier(descriptor),
    )
    mutated = proof.model_copy(update={"destination_head_sha256": "1" * 64})
    with pytest.raises(ReplicationDurabilityError):
        configured.complete_replication(mutated)
    assert configured.read_status().verifying_count == 1


def test_writer_revalidates_persisted_descriptor_and_host_before_mutation(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    descriptor_path = destination / ".stock-eva-replication-destination.json"
    values = descriptor.model_dump(mode="json")
    values["single_writer_host_id"] = "host-b"
    values["descriptor_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/destination-descriptor/v1",
        {key: value for key, value in values.items() if key != "descriptor_sha256"},
    )
    descriptor_path.write_bytes(canonical_json_bytes(values))
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id="host-a"
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_legacy_descriptor_missing_mount_fields_is_not_adopted(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    values = descriptor.model_dump(mode="json")
    values.pop("normalized_options")
    values.pop("volume_id")
    values["descriptor_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/destination-descriptor/v1",
        {key: value for key, value in values.items() if key != "descriptor_sha256"},
    )
    (destination / ".stock-eva-replication-destination.json").write_bytes(
        canonical_json_bytes(values)
    )
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_history_pollution_is_rejected_before_first_no_head_mutation(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    (destination / "_replication" / "history" / "unknown").write_bytes(b"x")
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_history_staging_orphan_is_rejected_before_mutation(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    staging = destination / "_replication" / "history" / ("." + "a" * 64 + ".staging")
    staging.mkdir()
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_orphan_generation_is_recovered_without_recopied_source(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert first.record is not None
    (destination / "_replication" / "head.json").unlink()
    original_read = replication._destination_read_relative

    source_identity = (source.stat().st_dev, source.stat().st_ino)

    def fail_source_read(fd: int, relative_path: str) -> bytes:
        if (
            (os_stat := replication.os.fstat(fd)).st_dev,
            os_stat.st_ino,
        ) == source_identity and relative_path in {
            "bars.parquet",
            "manifest.json",
            ".stock-eva-dataset.json",
        }:
            raise AssertionError("orphan recovery recopied source")
        return original_read(fd, relative_path)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(replication, "_destination_read_relative", fail_source_read)
    try:
        recovered = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:02:00Z")
    finally:
        monkeypatch.undo()
    assert recovered.reason_code == "NONE"
    assert (destination / "_replication" / "head.json").exists()


def test_orphan_child_generation_is_recovered_without_recopied_source(
    tmp_path: Path,
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert first.record is not None
    head_path = destination / "_replication" / "head.json"
    first_head = head_path.read_bytes()
    second = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:02:00Z")
    assert second.record is not None
    head_path.write_bytes(first_head)
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    original_read = replication._destination_read_relative

    def fail_source_read(fd: int, relative_path: str) -> bytes:
        info = replication.os.fstat(fd)
        if (info.st_dev, info.st_ino) == source_identity and relative_path in {
            "bars.parquet",
            "manifest.json",
            ".stock-eva-dataset.json",
        }:
            raise AssertionError("orphan recovery recopied source")
        return original_read(fd, relative_path)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(replication, "_destination_read_relative", fail_source_read)
    try:
        recovered = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:03:00Z")
    finally:
        monkeypatch.undo()
    assert recovered.reason_code == "NONE"
    assert recovered.record is not None
    assert recovered.record.replication_generation == second.record.replication_generation
    assert DestinationArchiveReader(descriptor).read_head().replication_generation == (
        second.record.replication_generation
    )


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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
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
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
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
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
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
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "COPY_FAILED"
    assert not (destination / "_replication" / "head.json").exists()


def test_destination_no_replace_install_unsupported_is_typed_and_zero_head_write(
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

    def unsupported_install(*args: object, **kwargs: object) -> None:
        raise ReplicationDurabilityError("destination generation install unsupported")

    monkeypatch.setattr(replication, "_destination_rename_noreplace", unsupported_install)
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "MOUNT_UNSUPPORTED"
    assert not (destination / "_replication" / "head.json").exists()


def test_verified_destination_proof_cannot_be_fabricated() -> None:
    proof = VerifiedDestinationCommitProof(
        intent_id=None,
        checkpoint_id="a" * 64,
        destination_id="b" * 32,
        source_sequence=1,
        destination_generation="c" * 64,
        destination_record_sha256="d" * 64,
        destination_head_sha256="e" * 64,
        source_instance_id=None,
        source_instance_sha256=None,
        worker_id=None,
        state_version=None,
        now="2026-09-09T00:00:00Z",
    )
    assert proof.destination_id == "b" * 32
    with pytest.raises((TypeError, ValueError)):
        proof.destination_id = "f" * 32


def test_mount_identity_drift_before_staging_is_zero_write(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination, options=("rw", "local", "rw"))
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    assert descriptor.normalized_options == "local,rw"
    inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code in {"DESTINATION_REBOUND", "DESTINATION_MOUNT_UNAVAILABLE"}
    assert result.destination_writes is False
    assert not (destination / "_replication" / "head.json").exists()
    assert not tuple((destination / "_replication" / "history").iterdir())


@pytest.mark.parametrize("drift", ["mountpoint", "filesystem", "volume"])
def test_live_mount_identity_fields_are_independently_fail_closed(
    tmp_path: Path, drift: str
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    if drift == "mountpoint":
        inspector.info = MountInfo(Path("/"), "apfs", ("local", "rw"), "vol-a")
    elif drift == "filesystem":
        inspector.info = MountInfo(destination, "ext4", ("local", "rw"), "vol-a")
    else:
        inspector.info = MountInfo(destination, "apfs", ("local", "rw"), "vol-b")
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code in {"DESTINATION_REBOUND", "DESTINATION_MOUNT_UNAVAILABLE"}
    assert result.destination_writes is False
    assert not (destination / "_replication" / "head.json").exists()


def test_live_mount_probe_requires_root_under_reported_mountpoint(
    tmp_path: Path,
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    inspector.info = MountInfo(
        destination.parent / "different-mount", "apfs", ("local", "rw"), "vol-a"
    )
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code in {"DESTINATION_REBOUND", "DESTINATION_MOUNT_UNAVAILABLE"}
    assert result.destination_writes is False


def test_mount_identity_drift_after_staging_is_degraded_with_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    original_mkdir = replication.os.mkdir

    def drift_after_staging(name: str, *args: object, **kwargs: object) -> None:
        original_mkdir(name, *args, **kwargs)
        if str(name).startswith(".") and str(name).endswith(".staging"):
            inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")

    monkeypatch.setattr(replication.os, "mkdir", drift_after_staging)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code in {"DESTINATION_TRUST_FAILED", "DESTINATION_REBOUND"}
    assert result.destination_writes is True
    assert not (destination / "_replication" / "head.json").exists()


def test_mount_identity_drift_after_head_commit_preserves_head_as_degraded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    original = replication._destination_replace_at

    def install_then_remount(
        parent_fd: int, name: str, payload: bytes, *, expected: bytes | None
    ) -> None:
        original(parent_fd, name, payload, expected=expected)
        if name == "head.json":
            inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")

    monkeypatch.setattr(replication, "_destination_replace_at", install_then_remount)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.destination_writes is True
    assert (destination / "_replication" / "head.json").exists()


def test_reader_root_session_reproves_live_mount_without_writing(tmp_path: Path) -> None:
    _source, _checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    reader = DestinationArchiveReader(descriptor, mount_inspector=inspector)
    before = (destination / ".stock-eva-replication-destination.json").read_bytes()
    inspector.info = MountInfo(destination, "apfs", ("local", "rw"), "vol-b")
    with pytest.raises(ReplicationDurabilityError):
        reader.read_head()
    assert (destination / ".stock-eva-replication-destination.json").read_bytes() == before


def test_claim_context_binds_writer_proof_for_sidecar_completion(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
    )
    sidecar = ReplicationSidecarStore(
        tmp_path / "control" / "replication-sidecar",
        destination_verifier=DestinationCommitVerifier(descriptor),
    )
    sidecar.initialize(
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
        created_at="2026-09-09T00:00:00Z",
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
        worker_id=claim.worker_id,
        expected_state_version=claim.state_version,
        to_state="verifying",
        now="2026-09-09T00:01:01Z",
    )
    context = ReplicationClaimContext(
        intent_id=intent.intent_id,
        worker_id=claim.worker_id,
        state_version=verifying.state_version,
        checkpoint_id=checkpoint.checkpoint_id,
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
    )
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(
        checkpoint,
        source_sequence=claim.source_sequence,
        claim_context=context,
        now="2026-09-09T00:01:02Z",
    )
    assert result.proof is not None
    assert result.proof.intent_id == context.intent_id
    assert result.proof.worker_id == context.worker_id
    assert result.proof.state_version == context.state_version
    assert result.proof.source_instance_id == context.source_instance_id
    assert result.proof.source_instance_sha256 == context.source_instance_sha256
    assert sidecar.complete_replication(result.proof).current_state == "replicated"


def test_claim_context_checkpoint_mismatch_is_zero_write(tmp_path: Path) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    context = ReplicationClaimContext(
        intent_id="1" * 64,
        worker_id="worker-a",
        state_version=1,
        checkpoint_id="2" * 64,
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
    )
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(
        checkpoint,
        source_sequence=1,
        claim_context=context,
        now="2026-09-09T00:01:00Z",
    )
    assert result.reason_code == "DESTINATION_TRUST_FAILED"
    assert result.destination_writes is False
    assert not (destination / "_replication" / "head.json").exists()
    source_mismatch = context.model_copy(
        update={
            "checkpoint_id": checkpoint.checkpoint_id,
            "source_instance_sha256": "3" * 64,
        }
    )
    source_result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(
        checkpoint,
        source_sequence=1,
        claim_context=source_mismatch,
        now="2026-09-09T00:01:00Z",
    )
    assert source_result.reason_code == "DESTINATION_TRUST_FAILED"
    assert source_result.destination_writes is False


def test_cas_conflict_after_generation_install_reports_destination_effect(
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
    original = replication._destination_replace_at

    def reject_head(parent_fd: int, name: str, payload: bytes, *, expected: bytes | None) -> None:
        if name == "head.json":
            raise replication.ReplicationCASConflict("destination head CAS conflict")
        original(parent_fd, name, payload, expected=expected)

    monkeypatch.setattr(replication, "_destination_replace_at", reject_head)
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "CAS_CONFLICT"
    assert result.destination_writes is True


def _claim_for_checkpoint(checkpoint, *, intent: str = "1" * 64) -> ReplicationClaimContext:
    return ReplicationClaimContext(
        intent_id=intent,
        worker_id="worker-a",
        state_version=1,
        checkpoint_id=checkpoint.checkpoint_id,
        source_instance_id=checkpoint.source_instance_id,
        source_instance_sha256=checkpoint.source_instance_sha256,
    )


def test_commit_verifier_reproves_mount_after_reader_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        single_writer_host_id="host-a",
        mount_inspector=inspector,
    )
    claim = _claim_for_checkpoint(checkpoint)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, claim_context=claim, now="2026-09-09T00:01:00Z")
    assert result.proof is not None
    verifier = DestinationCommitVerifier(descriptor, mount_inspector=inspector)
    original = verifier.reader.verify_commit

    def verify_then_remount(*args: object, **kwargs: object) -> VerifiedDestinationCommitProof:
        refreshed = original(*args, **kwargs)
        inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")
        return refreshed

    monkeypatch.setattr(verifier.reader, "verify_commit", verify_then_remount)
    with pytest.raises(replication.ReplicationStateUnavailable):
        verifier.verify(result.proof)


def test_normal_publish_mount_drift_after_verifier_preserves_head_as_degraded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    original = replication.DestinationArchiveReader.verify_commit

    def verify_then_remount(
        reader: DestinationArchiveReader, *args: object, **kwargs: object
    ) -> VerifiedDestinationCommitProof:
        refreshed = original(reader, *args, **kwargs)
        if kwargs.get("destination_generation") is not None:
            inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")
        return refreshed

    monkeypatch.setattr(replication.DestinationArchiveReader, "verify_commit", verify_then_remount)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.destination_writes is True
    assert (destination / "_replication" / "head.json").exists()


def test_generation_install_mount_drift_before_history_fsync_keeps_head_unpublished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    original = replication._destination_rename_noreplace

    def install_then_remount(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")

    monkeypatch.setattr(replication, "_destination_rename_noreplace", install_then_remount)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "DESTINATION_REBOUND"
    assert result.destination_writes is True
    assert not (destination / "_replication" / "head.json").exists()
    generations = tuple(
        p for p in (destination / "_replication" / "history").iterdir() if p.name != ".staging"
    )
    assert len(generations) == 1


def test_history_fsync_mount_drift_keeps_head_unpublished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    history_stat = (destination / "_replication" / "history").stat()
    original = replication._fsync_open_directory
    drifted = False

    def fsync_then_remount(fd: int) -> None:
        nonlocal drifted
        original(fd)
        current = os.fstat(fd)
        if (
            not drifted
            and current.st_dev == history_stat.st_dev
            and current.st_ino == history_stat.st_ino
        ):
            drifted = True
            inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")

    monkeypatch.setattr(replication, "_fsync_open_directory", fsync_then_remount)
    result = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert drifted
    assert result.reason_code == "DESTINATION_REBOUND"
    assert result.destination_writes is True
    assert not (destination / "_replication" / "head.json").exists()


def test_orphan_recovery_mount_drift_after_verifier_preserves_head_as_degraded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    writer = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    )
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert first.record is not None
    head = destination / "_replication" / "head.json"
    head.unlink()
    original = replication.DestinationArchiveReader.verify_commit

    def verify_then_remount(
        reader: DestinationArchiveReader, *args: object, **kwargs: object
    ) -> VerifiedDestinationCommitProof:
        refreshed = original(reader, *args, **kwargs)
        inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")
        return refreshed

    monkeypatch.setattr(replication.DestinationArchiveReader, "verify_commit", verify_then_remount)
    recovered = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:02:00Z")
    assert recovered.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert recovered.destination_writes is True
    assert head.exists()


def test_orphan_child_recovery_mount_drift_after_verifier_preserves_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    inspector = _FakeMountInspector(destination)
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
        mount_inspector=inspector,
    )
    writer = DestinationArchiveWriter(
        descriptor,
        source_root=source,
        writer_host_id=descriptor.single_writer_host_id,
        mount_inspector=inspector,
    )
    first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert first.record is not None
    head = destination / "_replication" / "head.json"
    first_head = head.read_bytes()
    second = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:02:00Z")
    assert second.record is not None
    head.write_bytes(first_head)
    original = replication.DestinationArchiveReader.verify_commit

    def verify_then_remount(
        reader: DestinationArchiveReader, *args: object, **kwargs: object
    ) -> VerifiedDestinationCommitProof:
        refreshed = original(reader, *args, **kwargs)
        if kwargs.get("destination_generation") is not None:
            inspector.info = MountInfo(destination, "apfs", ("local", "ro"), "vol-a")
        return refreshed

    monkeypatch.setattr(replication.DestinationArchiveReader, "verify_commit", verify_then_remount)
    recovered = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:03:00Z")
    assert recovered.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert recovered.destination_writes is True
    assert head.exists()


def test_cleanup_failure_preserves_primary_copy_reason_and_effect(
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
    (source / "bars.parquet").write_bytes(b"changed-source")

    def fail_cleanup(*args: object, **kwargs: object) -> None:
        raise ReplicationDurabilityError("injected staging cleanup failure")

    monkeypatch.setattr(replication, "_destination_remove_tree_at", fail_cleanup)
    result = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    ).replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
    assert result.reason_code == "COPY_FAILED"
    assert result.destination_writes is True


def _head_recovery_case(tmp_path: Path, case: str):
    source, checkpoint, sentinel = _source_fixture(tmp_path)
    destination = tmp_path / "nas"
    destination.mkdir()
    descriptor = initialize_destination(
        destination,
        acknowledgement="CREATE_EMPTY_NAS_ARCHIVE_R2F4_3",
        sentinel_bytes=sentinel,
    )
    writer = DestinationArchiveWriter(
        descriptor, source_root=source, writer_host_id=descriptor.single_writer_host_id
    )
    sequence = 1
    if case == "genesis_orphan":
        first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
        assert first.record is not None
        (destination / "_replication" / "head.json").unlink()
    elif case == "child_orphan":
        first = writer.replicate(checkpoint, source_sequence=1, now="2026-09-09T00:01:00Z")
        assert first.record is not None
        first_head = (destination / "_replication" / "head.json").read_bytes()
        second = writer.replicate(checkpoint, source_sequence=2, now="2026-09-09T00:02:00Z")
        assert second.record is not None
        (destination / "_replication" / "head.json").write_bytes(first_head)
        sequence = 2
    elif case != "normal":
        raise AssertionError(f"unknown head case {case}")
    return writer, checkpoint, destination, sequence


@pytest.mark.parametrize("case", ["normal", "genesis_orphan", "child_orphan"])
def test_head_install_parent_fsync_failure_after_linearization_is_controlled(
    tmp_path: Path, case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, checkpoint, destination, sequence = _head_recovery_case(tmp_path, case)
    replication_stat = (destination / "_replication").stat()
    original = replication._fsync_open_directory
    injected = False

    def fail_replication_fsync(fd: int) -> None:
        nonlocal injected
        info = os.fstat(fd)
        if (
            not injected
            and info.st_dev == replication_stat.st_dev
            and info.st_ino == replication_stat.st_ino
        ):
            injected = True
            raise ReplicationDurabilityError("injected head parent fsync failure")
        original(fd)

    monkeypatch.setattr(replication, "_fsync_open_directory", fail_replication_fsync)
    result = writer.replicate(checkpoint, source_sequence=sequence, now="2026-09-09T00:03:00Z")
    assert injected
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.destination_writes is True
    assert (destination / "_replication" / "head.json").exists()


@pytest.mark.parametrize("case", ["normal", "genesis_orphan", "child_orphan"])
def test_head_install_readback_failure_after_linearization_is_controlled(
    tmp_path: Path, case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, checkpoint, destination, sequence = _head_recovery_case(tmp_path, case)
    original = replication._destination_read_at
    read_count = 0
    failure_at = 4 if case == "normal" else 3

    def fail_head_readback(
        root_fd: int,
        components: tuple[str, ...],
        **kwargs: object,
    ):
        nonlocal read_count
        if components == ("head.json",):
            read_count += 1
            if read_count == failure_at:
                raise ReplicationDurabilityError("injected head readback failure")
        return original(root_fd, components, **kwargs)

    monkeypatch.setattr(replication, "_destination_read_at", fail_head_readback)
    result = writer.replicate(checkpoint, source_sequence=sequence, now="2026-09-09T00:03:00Z")
    assert read_count >= failure_at
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.destination_writes is True
    assert (destination / "_replication" / "head.json").exists()


@pytest.mark.parametrize("case", ["normal", "genesis_orphan", "child_orphan"])
def test_head_install_lock_failure_after_linearization_is_controlled(
    tmp_path: Path, case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, checkpoint, destination, sequence = _head_recovery_case(tmp_path, case)
    head = destination / "_replication" / "head.json"
    baseline = head.read_bytes() if head.exists() else None
    original = replication._destination_assert_lock
    injected = False

    def fail_after_head(*args: object, **kwargs: object) -> None:
        nonlocal injected
        current = head.read_bytes() if head.exists() else None
        if not injected and current != baseline:
            injected = True
            raise ReplicationStateUnavailable("injected post-head lock proof failure")
        original(*args, **kwargs)

    monkeypatch.setattr(replication, "_destination_assert_lock", fail_after_head)
    result = writer.replicate(checkpoint, source_sequence=sequence, now="2026-09-09T00:03:00Z")
    assert injected
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.destination_writes is True
    assert head.exists()


def test_head_install_pre_syscall_failure_keeps_head_unpublished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, checkpoint, destination, sequence = _head_recovery_case(tmp_path, "normal")
    replication_stat = (destination / "_replication").stat()
    original = replication.os.link

    def fail_head_link(*args: object, **kwargs: object):
        parent_fd = kwargs.get("dst_dir_fd")
        if isinstance(parent_fd, int):
            info = os.fstat(parent_fd)
            if info.st_dev == replication_stat.st_dev and info.st_ino == replication_stat.st_ino:
                raise OSError(errno.EIO, "injected pre-linearization head link failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(replication.os, "link", fail_head_link)
    result = writer.replicate(checkpoint, source_sequence=sequence, now="2026-09-09T00:03:00Z")
    assert result.reason_code == "COPY_FAILED"
    assert result.destination_writes is True
    assert not (destination / "_replication" / "head.json").exists()
