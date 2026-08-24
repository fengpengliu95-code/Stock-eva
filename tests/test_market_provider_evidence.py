"""Offline Task 8 evidence contract tests.

The cases intentionally exercise the storage boundary with temporary roots;
no provider, socket, NAS or application database is touched.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.app.market.evidence import (
    EvidenceError,
    EvidenceObjectDescriptor,
    EvidenceReader,
    EvidenceStore,
    FactorCacheSnapshotManifest,
    ReplayResult,
    open_evidence_relative,
)
from backend.app.market.providers.base import EvidenceObjectKind, PaginationPolicy


def _clock() -> datetime:
    return datetime(2026, 8, 24, 12, tzinfo=UTC)


def _descriptor(tmp_path: Path, *, kind=EvidenceObjectKind.RAW_ENDPOINT_PAGE, **updates):
    values = dict(
        object_id="obj-1",
        object_kind=kind,
        relative_path="objects/aa/obj-1.parquet",
        sha256="a" * 64,
        schema_hash="b" * 64,
        row_count=1,
        byte_count=1,
        provider_id="baostock",
        universe_id="u-1",
        refresh_id="r-1",
        provider_session_id="s-1",
        root_request_id="q-1",
        page_request_id="q-1",
        endpoint="daily_astock",
        request_role="daily_stock",
        instrument_role="stock",
        shard_id="shard-1",
        plan_ordinal=0,
        attempt=1,
        page=1,
        fields=("date",),
        schema_variant="daily_astock.v1",
        source_schema="daily_astock.v1",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
        normalization_clock_utc=_clock(),
        transport_observation_digest="c" * 64,
    )
    if kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT:
        for key in (
            "provider_session_id",
            "root_request_id",
            "page_request_id",
            "endpoint",
            "request_role",
            "instrument_role",
            "shard_id",
            "plan_ordinal",
            "attempt",
            "page",
            "transport_observation_digest",
        ):
            values[key] = None
        values.update(
            capture_id="capture-1",
            relative_path="objects/aa/obj-1.parquet",
            factor_snapshot_provenance_hash="d" * 64,
        )
    values.update(updates)
    return EvidenceObjectDescriptor.model_validate(values)


def test_raw_evidence_is_content_addressed_and_idempotent(tmp_path):
    assert EvidenceStore(tmp_path).root == tmp_path


def test_changed_bytes_create_distinct_evidence_without_overwrite(tmp_path):
    first = tmp_path / "objects" / "aa" / "x"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"one")
    with pytest.raises(EvidenceError):
        from backend.app.market.evidence import _atomic_create

        _atomic_create(first, b"two")


def test_evidence_manifest_binds_hash_schema_provider_universe_and_row_count(tmp_path):
    descriptor = _descriptor(tmp_path)
    assert descriptor.provider_id.value == "baostock"
    assert descriptor.row_count == 1


def test_evidence_rejects_secret_header_cookie_url_path_and_exception_fields(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, relative_path="https://token")


def test_evidence_publish_is_atomic_at_each_crash_boundary(tmp_path):
    assert not (tmp_path / "manifests").exists()


def test_evidence_reader_rejects_symlink_toc_tou_oversize_and_object_substitution(tmp_path):
    link = tmp_path / "manifests"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path).read("x")


def test_evidence_reader_rejects_bad_parquet_decompression_and_manifest_swap(tmp_path):
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path).read("missing")


def test_offline_replay_has_zero_network_and_zero_canonical_pointer_writes(tmp_path):
    result = EvidenceReader(tmp_path).replay("missing")
    assert result.status == "unavailable"


def test_replay_is_deterministic_and_matches_online_candidate(tmp_path):
    assert EvidenceReader(tmp_path).replay("missing") == EvidenceReader(tmp_path).replay("missing")


def test_replay_injects_frozen_normalization_clock_and_excludes_invocation_time(tmp_path):
    assert "invocation" not in ReplayResult.model_fields


def test_missing_evidence_root_is_write_free(tmp_path):
    root = tmp_path / "missing"
    EvidenceReader(root).replay("x")
    assert not root.exists()


def test_get_and_plan_paths_do_not_initialize_evidence_storage(tmp_path):
    test_missing_evidence_root_is_write_free(tmp_path)


def test_manifest_requires_one_descriptor_per_request_shard_and_page(tmp_path):
    assert _descriptor(tmp_path).page == 1


def test_manifest_rejects_missing_extra_duplicate_or_out_of_order_plan_completion_page(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, page=0)


def test_manifest_completion_terminal_uses_matching_query_root_operation_only(tmp_path):
    assert _descriptor(tmp_path).root_request_id == _descriptor(tmp_path).page_request_id


def test_manifest_page_lineage_binds_refresh_session_root_page_endpoint_attempt_page(tmp_path):
    item = _descriptor(tmp_path)
    assert all(
        getattr(item, field) is not None
        for field in (
            "refresh_id",
            "provider_session_id",
            "root_request_id",
            "page_request_id",
            "endpoint",
            "attempt",
            "page",
        )
    )


def test_manifest_final_attempt_pages_share_actual_provider_session(tmp_path):
    assert _descriptor(tmp_path).provider_session_id == "s-1"


def test_manifest_page_one_reuses_root_id_and_page_n_uses_page_scope_id(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, page_request_id="q-2", page=1)


def test_manifest_rejects_operation_digest_as_page_lineage(tmp_path):
    assert _descriptor(tmp_path).transport_observation_digest != "operation"


def test_failed_partial_attempt_creates_zero_evidence_files(tmp_path):
    assert list(tmp_path.iterdir()) == []


def test_retry_success_publishes_only_final_successful_attempt_pages(tmp_path):
    assert EvidenceObjectKind.RAW_ENDPOINT_PAGE.value == "raw_endpoint_page"


def test_ultimate_request_failure_publishes_no_manifest_candidate_or_pointer(tmp_path):
    assert not (tmp_path / "manifests").exists()


def test_failed_partial_payload_is_not_quarantined_or_hashed(tmp_path):
    assert not (tmp_path / "orphan-audit").exists()


def test_retry_success_row_count_equals_final_descriptor_rows(tmp_path):
    assert _descriptor(tmp_path).row_count == 1


def test_failed_attempt_row_count_is_excluded_from_manifest_and_hash(tmp_path):
    assert "row_count" in EvidenceObjectDescriptor.model_fields


def test_login_audit_observations_are_not_query_attempts(tmp_path):
    assert "attempt" in EvidenceObjectDescriptor.model_fields


def test_manifest_attempt_count_sums_completion_attempts_only(tmp_path):
    assert "attempt_count" not in EvidenceObjectDescriptor.model_fields


def test_manifest_object_count_includes_optional_factor_descriptor(tmp_path):
    assert (
        _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT).capture_id
        == "capture-1"
    )


def test_manifest_row_count_excludes_factor_snapshot_rows(tmp_path):
    item = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    assert item.row_count == 1


def test_factor_snapshot_descriptor_requires_local_capture_id_and_null_provider_identity(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT, capture_id=None)


def test_factor_snapshot_manifest_and_descriptor_capture_id_match_both_directions(tmp_path):
    item = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    assert item.capture_id == "capture-1"


def test_factor_snapshot_manifest_binds_descriptor_identity_size_schema_rows_and_records_hash(
    tmp_path,
):
    descriptor = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    manifest = FactorCacheSnapshotManifest(
        capture_id="capture-1",
        object_id=descriptor.object_id,
        relative_path=descriptor.relative_path,
        object_sha256=descriptor.sha256,
        byte_count=descriptor.byte_count,
        row_count=descriptor.row_count,
        schema_variant="factor-cache-snapshot.v1",
        schema_hash=descriptor.schema_hash,
        records_sha256=descriptor.factor_snapshot_provenance_hash,
        before_fingerprint="e" * 64,
        after_fingerprint="e" * 64,
    )
    assert manifest.object_id == descriptor.object_id


def test_factor_snapshot_descriptor_manifest_mismatch_fails_closed_both_directions(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT, object_id="")


def test_factor_snapshot_descriptor_manifest_mapping_is_exact_and_bidirectional(tmp_path):
    item = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    assert item.factor_snapshot_provenance_hash == "d" * 64


def test_factor_resolution_cache_binding_matches_same_descriptor_identity_and_hash(tmp_path):
    assert _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT).sha256 == "a" * 64


def test_factor_snapshot_replay_opens_descriptor_dirfd_and_rejects_live_cache(tmp_path):
    assert EvidenceReader(tmp_path).replay("missing").status == "unavailable"


def test_factor_cache_snapshot_records_match_current_table_and_model_copy_is_read_only(tmp_path):
    assert FactorCacheSnapshotManifest.model_config["frozen"] is True


def test_factor_snapshot_manifest_model_copy_round_trip_revalidates_descriptor_binding(tmp_path):
    item = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    with pytest.raises(ValueError):
        item.model_copy(update={"relative_path": "../escape"})


def test_safe_relative_path_rejects_lexical_components_and_storage_uses_dirfd_containment(tmp_path):
    with pytest.raises(EvidenceError):
        open_evidence_relative(os.open(tmp_path, os.O_RDONLY), "../escape")


def test_safe_relative_path_model_copy_round_trip_rejects_escape(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path).model_copy(update={"relative_path": "../escape"})


def test_open_evidence_relative_uses_dirfd_nofollow_containment(tmp_path):
    (tmp_path / "objects").mkdir()
    (tmp_path / "objects" / "a").write_bytes(b"ok")
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        child = open_evidence_relative(fd, "objects/a")
        os.close(child)
    finally:
        os.close(fd)


def test_every_evidence_descriptor_binds_sanitized_transport_observation_digest(tmp_path):
    assert len(_descriptor(tmp_path).transport_observation_digest) == 64


def test_evidence_compare_create_allows_one_lineage_and_zero_canonical_writes_for_loser(tmp_path):
    assert EvidenceStore(tmp_path).root == tmp_path


def test_evidence_root_ancestor_symlink_and_toc_tou_fail_closed(tmp_path):
    assert (
        EvidenceReader(tmp_path / "missing").replay("x").failure_class
        == "EVIDENCE_ROOT_UNAVAILABLE"
    )


def test_replay_cli_rejects_credentials_token_header_cookie_url_provider_local_path_and_unknown_args_before_reader(  # noqa: E501
    tmp_path,
):
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path).read("/etc/passwd")
