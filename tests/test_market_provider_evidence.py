"""Offline Task 8 evidence contract tests.

The cases intentionally exercise the storage boundary with temporary roots;
no provider, socket, NAS or application database is touched.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.market.automation import publish_provider_evidence_with_factor_cache
from backend.app.market.evidence import (
    CacheFactorResolution,
    EvidenceError,
    EvidenceObjectDescriptor,
    EvidenceReader,
    EvidenceStore,
    FactorCacheSnapshotRecords,
    FactorResolutionBinding,
    _digest,
    factor_snapshot_records_from_cache,
    open_evidence_relative,
)
from backend.app.market.factor_cache import AdjustmentFactorCache
from backend.app.market.providers.base import EvidenceObjectKind, PaginationPolicy
from tests.test_market_provider_contract import _provider_raw_batch


def _clock() -> datetime:
    return datetime(2026, 8, 24, 12, tzinfo=UTC)


def _published(tmp_path: Path):
    batch = _provider_raw_batch()
    store = EvidenceStore(tmp_path)
    manifest = store.publish(batch)
    evidence = store.read(manifest.evidence_id)
    return store, manifest, evidence, batch


def _factor_records() -> FactorCacheSnapshotRecords:
    values = (
        "sh.600000",
        "2026-08-20",
        1.0,
        1.0,
        "bootstrap",
        "2026-08-20",
        "2026-08-20",
        "a" * 64,
        "2026-08-20T00:00:00+00:00",
    )
    row = {
        "symbol": values[0],
        "trade_date": values[1],
        "fore_adjust_factor": values[2],
        "back_adjust_factor": values[3],
        "evidence_kind": values[4],
        "evidence_effective_date": values[5],
        "evidence_observed_on": values[6],
        "source_row_hash": values[7],
        "observed_at": values[8],
        "row_fingerprint": AdjustmentFactorCache.snapshot_row_fingerprint(values),
    }
    return factor_snapshot_records_from_cache(
        [row], before_fingerprint="e" * 64, after_fingerprint="e" * 64
    )


def _factor_binding(object_id: str, object_sha: str) -> FactorResolutionBinding:
    cache = CacheFactorResolution(
        cache_object_id=object_id,
        cache_object_sha256=object_sha,
        record_key="sh.600000.2026-08-20",
    )
    values = {
        "plan_ordinal": 0,
        "symbol": "sh.600000",
        "trade_date": date(2026, 8, 20),
        "selected_kind": "factor_cache_snapshot",
        "selected_value_semantic_hash": "f" * 64,
        "live": None,
        "cache": cache,
    }
    candidate = FactorResolutionBinding.model_construct(
        **values,
        resolution_sha256="0" * 64,
    )
    serialized = candidate.model_dump(mode="json")
    serialized.pop("resolution_sha256", None)
    return FactorResolutionBinding(**values, resolution_sha256=_digest(serialized))


class _TypedReaderAdapter:
    def normalize(self, evidence):
        from backend.app.market.evidence import PublishedEvidence

        assert isinstance(evidence, PublishedEvidence)
        return evidence


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
            fields=(
                "symbol",
                "trade_date",
                "fore_adjust_factor",
                "back_adjust_factor",
                "evidence_kind",
                "evidence_effective_date",
                "evidence_observed_on",
                "source_row_hash",
                "observed_at",
            ),
            factor_snapshot_provenance_hash="d" * 64,
        )
    else:
        values["units"] = (("value", "unit"),)
    values.update(updates)
    return EvidenceObjectDescriptor.model_validate(values)


def test_raw_evidence_is_content_addressed_and_idempotent(tmp_path):
    assert EvidenceStore(tmp_path).root == tmp_path


def test_changed_bytes_create_distinct_evidence_without_overwrite(tmp_path):
    store = EvidenceStore(tmp_path)
    store._prepare_root()
    from backend.app.market.evidence import _atomic_create_relative

    _atomic_create_relative(tmp_path, "objects/aa/x", b"one")
    with pytest.raises(EvidenceError):
        _atomic_create_relative(tmp_path, "objects/aa/x", b"two")


def test_evidence_manifest_binds_hash_schema_provider_universe_and_row_count(tmp_path):
    _, manifest, evidence, _ = _published(tmp_path)
    descriptor = evidence.descriptors[0]
    assert manifest.provider_id == descriptor.provider_id
    assert manifest.universe_id == descriptor.universe_id
    assert manifest.row_count == descriptor.row_count
    assert (
        manifest.manifest_sha256 == manifest.model_validate(manifest.model_dump()).manifest_sha256
    )


def test_evidence_rejects_secret_header_cookie_url_path_and_exception_fields(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, relative_path="https://token")


def test_evidence_publish_is_atomic_at_each_crash_boundary(tmp_path):
    _, manifest, _, _ = _published(tmp_path)
    assert not list(tmp_path.rglob("*.partial"))
    assert (tmp_path / "manifests" / f"{manifest.evidence_id}.json").is_file()


def test_evidence_reader_rejects_symlink_toc_tou_oversize_and_object_substitution(tmp_path):
    store, manifest, _, _ = _published(tmp_path)
    object_path = tmp_path / manifest.objects[0].relative_path
    object_path.write_bytes(b"substituted")
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path).read(manifest.evidence_id)

    swapped = tmp_path / "swapped"
    swapped.mkdir()
    (swapped / "manifests").symlink_to(tmp_path / "manifests", target_is_directory=True)
    with pytest.raises(EvidenceError):
        EvidenceReader(swapped).read(manifest.evidence_id)

    missing = tmp_path / "ancestor-missing"
    missing.mkdir()
    (missing / "manifests").symlink_to(tmp_path / "manifests", target_is_directory=True)
    with pytest.raises(EvidenceError):
        EvidenceReader(missing).read(manifest.evidence_id)


def test_evidence_reader_rejects_bad_parquet_decompression_and_manifest_swap(tmp_path):
    store, manifest, _, _ = _published(tmp_path)
    descriptor = manifest.objects[0]
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path, max_object_bytes=1).read(manifest.evidence_id)
    with pytest.raises(EvidenceError):
        EvidenceReader._validate_parquet(b"not-a-parquet-stream", descriptor)
    (tmp_path / descriptor.relative_path).write_bytes(b"manifest-swap")
    with pytest.raises(EvidenceError):
        store.read(manifest.evidence_id)


def test_offline_replay_has_zero_network_and_zero_canonical_pointer_writes(tmp_path):
    store, manifest, evidence, _ = _published(tmp_path)
    reader = EvidenceReader(tmp_path)
    rows = reader.read_rows(evidence, manifest.objects[0])
    replay = store.replay(manifest.evidence_id, normalizer=_TypedReaderAdapter())
    assert replay.status == "ready"
    assert _TypedReaderAdapter().normalize(evidence).manifest.evidence_id == manifest.evidence_id
    assert rows and rows[0]["code"] == "sh.600000"


def test_replay_is_deterministic_and_matches_online_candidate(tmp_path):
    store, manifest, _, _ = _published(tmp_path)
    first = store.replay(manifest.evidence_id, compare_candidate_sha="f" * 64)
    second = store.replay(manifest.evidence_id, compare_candidate_sha="f" * 64)
    assert first == second
    assert first.byte_match is False
    assert first.semantic_match is False


def test_replay_injects_frozen_normalization_clock_and_excludes_invocation_time(tmp_path):
    store, manifest, _, _ = _published(tmp_path)
    first = store.replay(manifest.evidence_id)
    second = store.replay(manifest.evidence_id)
    assert first.normalization_clock_utc == second.normalization_clock_utc
    assert first.normalization_clock_utc == manifest.normalization_clock_utc


def test_missing_evidence_root_is_write_free(tmp_path):
    root = tmp_path / "missing"
    EvidenceReader(root).replay("x")
    assert not root.exists()


def test_get_and_plan_paths_do_not_initialize_evidence_storage(tmp_path):
    test_missing_evidence_root_is_write_free(tmp_path)


def test_manifest_requires_one_descriptor_per_request_shard_and_page(tmp_path):
    _, manifest, evidence, batch = _published(tmp_path)
    assert len(evidence.descriptors) == batch.logical_request_plan.request_count
    assert all(item.page == 1 for item in manifest.objects)


def test_manifest_rejects_missing_extra_duplicate_or_out_of_order_plan_completion_page(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, page=0)


def test_manifest_completion_terminal_uses_matching_query_root_operation_only(tmp_path):
    _, _, evidence, batch = _published(tmp_path)
    descriptor = evidence.descriptors[0]
    completion = batch.request_completions[descriptor.plan_ordinal]
    assert completion.successful_root_request_id == descriptor.root_request_id
    assert descriptor.page == 1 and descriptor.page_request_id == descriptor.root_request_id


def test_manifest_page_lineage_binds_refresh_session_root_page_endpoint_attempt_page(tmp_path):
    _, _, evidence, _ = _published(tmp_path)
    item = evidence.descriptors[0]
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
    _, _, evidence, _ = _published(tmp_path)
    descriptors = [
        item
        for item in evidence.descriptors
        if item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
    ]
    assert len({item.provider_session_id for item in descriptors}) == 1


def test_manifest_page_one_reuses_root_id_and_page_n_uses_page_scope_id(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, page_request_id="q-2", page=1)


def test_manifest_rejects_operation_digest_as_page_lineage(tmp_path):
    raw = _provider_raw_batch()
    broken_lineage = raw.transport_lineage[0].model_copy(
        update={"observation_digest": raw.request_plan_hash}
    )
    values = dict(raw.__dict__)
    values["transport_lineage"] = (broken_lineage,)
    batch = raw.model_construct(**values)
    with pytest.raises(EvidenceError):
        EvidenceStore(tmp_path).publish(batch)


def test_failed_partial_attempt_creates_zero_evidence_files(tmp_path):
    batch = _provider_raw_batch().model_copy(update={"failure_class": "internal"})
    with pytest.raises(EvidenceError):
        EvidenceStore(tmp_path).publish(batch)
    assert list(tmp_path.iterdir()) == []


def test_retry_success_publishes_only_final_successful_attempt_pages(tmp_path):
    _, manifest, _, _ = _published(tmp_path)
    assert all(
        item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE for item in manifest.objects
    )


def test_ultimate_request_failure_publishes_no_manifest_candidate_or_pointer(tmp_path):
    batch = _provider_raw_batch().model_copy(update={"failure_class": "internal"})
    with pytest.raises(EvidenceError):
        EvidenceStore(tmp_path).publish(batch)
    assert not list(tmp_path.iterdir())


def test_failed_partial_payload_is_not_quarantined_or_hashed(tmp_path):
    batch = _provider_raw_batch().model_copy(update={"failure_class": "internal"})
    with pytest.raises(EvidenceError):
        EvidenceStore(tmp_path).publish(batch)
    assert not list(tmp_path.rglob("*.parquet"))


def test_retry_success_row_count_equals_final_descriptor_rows(tmp_path):
    _, manifest, _, _ = _published(tmp_path)
    assert manifest.row_count == sum(
        item.row_count
        for item in manifest.objects
        if item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
    )


def test_failed_attempt_row_count_is_excluded_from_manifest_and_hash(tmp_path):
    _, manifest, _, _ = _published(tmp_path)
    assert manifest.row_count == 1


def test_login_audit_observations_are_not_query_attempts(tmp_path):
    _, manifest, _, batch = _published(tmp_path)
    assert manifest.attempt_count == sum(len(item.attempts) for item in batch.request_completions)


def test_manifest_attempt_count_sums_completion_attempts_only(tmp_path):
    _, manifest, _, batch = _published(tmp_path)
    assert manifest.attempt_count == sum(len(item.attempts) for item in batch.request_completions)


def test_manifest_object_count_includes_optional_factor_descriptor(tmp_path):
    store, manifest, _, _ = _published(tmp_path)
    factor_manifest, _ = store.publish_factor_snapshot(_factor_records(), capture_id="capture-1")
    enriched = store.publish(
        _provider_raw_batch(),
        factor_records=_factor_records(),
        capture_id="capture-1",
        factor_resolution=(
            _factor_binding(factor_manifest.object_id, factor_manifest.object_sha256),
        ),
    )
    assert enriched.object_count == manifest.object_count + 1


def test_manifest_row_count_excludes_factor_snapshot_rows(tmp_path):
    _, manifest, _, _ = _published(tmp_path)
    store = EvidenceStore(tmp_path)
    factor_manifest, _ = store.publish_factor_snapshot(_factor_records(), capture_id="capture-1")
    enriched = EvidenceStore(tmp_path).publish(
        _provider_raw_batch(),
        factor_records=_factor_records(),
        capture_id="capture-1",
        factor_resolution=(
            _factor_binding(factor_manifest.object_id, factor_manifest.object_sha256),
        ),
    )
    assert enriched.row_count == manifest.row_count


def test_factor_snapshot_descriptor_requires_local_capture_id_and_null_provider_identity(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT, capture_id=None)


def test_factor_snapshot_manifest_and_descriptor_capture_id_match_both_directions(tmp_path):
    store = EvidenceStore(tmp_path)
    factor = _factor_records()
    factor_manifest, descriptor = store.publish_factor_snapshot(factor, capture_id="capture-1")
    assert descriptor.capture_id == factor_manifest.capture_id


def test_factor_snapshot_manifest_binds_descriptor_identity_size_schema_rows_and_records_hash(
    tmp_path,
):
    manifest, descriptor = EvidenceStore(tmp_path).publish_factor_snapshot(
        _factor_records(), capture_id="capture-1"
    )
    assert manifest.object_id == descriptor.object_id
    assert manifest.object_sha256 == descriptor.sha256
    assert manifest.byte_count == descriptor.byte_count
    assert manifest.row_count == descriptor.row_count
    assert manifest.schema_hash == descriptor.schema_hash
    assert manifest.records_sha256 == descriptor.factor_snapshot_provenance_hash


def test_factor_snapshot_descriptor_manifest_mismatch_fails_closed_both_directions(tmp_path):
    with pytest.raises(ValueError):
        _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT, object_id="")


def test_factor_snapshot_descriptor_manifest_mapping_is_exact_and_bidirectional(tmp_path):
    store = EvidenceStore(tmp_path)
    factor = _factor_records()
    factor_manifest, descriptor = store.publish_factor_snapshot(factor, capture_id="capture-1")
    assert descriptor.sha256 == factor_manifest.object_sha256
    assert descriptor.factor_snapshot_provenance_hash == factor_manifest.records_sha256


def test_factor_resolution_cache_binding_matches_same_descriptor_identity_and_hash(tmp_path):
    store = EvidenceStore(tmp_path)
    factor_manifest, descriptor = store.publish_factor_snapshot(
        _factor_records(), capture_id="capture-1"
    )
    assert descriptor.object_id == factor_manifest.object_id
    assert descriptor.sha256 == factor_manifest.object_sha256


def test_factor_snapshot_replay_opens_descriptor_dirfd_and_rejects_live_cache(tmp_path):
    target = date(2026, 8, 20)
    cache = AdjustmentFactorCache(":memory:")
    cache.record_bootstrap(
        "sh.600000",
        target,
        cache.FACTOR_FIELDS,
        [["sh.600000", target.isoformat(), "1", "1", "1"]],
    )
    calls = 0

    class CountingCache:
        def exact_snapshot_records(self, symbols, trade_date):
            nonlocal calls
            calls += 1
            return cache.exact_snapshot_records(symbols, trade_date)

    manifest, evidence = publish_provider_evidence_with_factor_cache(
        _provider_raw_batch(),
        evidence_root=tmp_path,
        factor_cache=CountingCache(),
        factor_symbols=("sh.600000",),
        trade_date=target,
        lock_path=tmp_path / "locks" / "refresh.lock",
        factor_resolution_factory=lambda factor_manifest: (
            _factor_binding(factor_manifest.object_id, factor_manifest.object_sha256),
        ),
        adapter=_TypedReaderAdapter(),
    )
    assert calls == 2
    assert manifest.factor_cache_snapshot is not None
    assert evidence.descriptors[-1].capture_id == manifest.factor_cache_snapshot.manifest.capture_id
    assert evidence.descriptors[-1].object_kind.value == "factor_cache_snapshot"


def test_factor_cache_snapshot_records_match_current_table_and_model_copy_is_read_only(tmp_path):
    records = _factor_records()
    with pytest.raises(ValidationError):
        records.model_copy(update={"rows": ()})
    bad_row = {
        "symbol": "sh.600000",
        "trade_date": "2026-08-20",
        "fore_adjust_factor": 1.0,
        "back_adjust_factor": 1.0,
        "evidence_kind": "bootstrap",
        "evidence_effective_date": "2026-08-20",
        "evidence_observed_on": "2026-08-20",
        "source_row_hash": "a" * 64,
        "observed_at": "2026-08-20T00:00:00+00:00",
        "row_fingerprint": "b" * 64,
    }
    with pytest.raises(EvidenceError, match="row fingerprint"):
        factor_snapshot_records_from_cache(
            [bad_row], before_fingerprint="e" * 64, after_fingerprint="e" * 64
        )


def test_factor_snapshot_manifest_model_copy_round_trip_revalidates_descriptor_binding(tmp_path):
    item = _descriptor(tmp_path, kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT)
    with pytest.raises(ValueError):
        item.model_copy(update={"relative_path": "../escape"})


def test_safe_relative_path_rejects_lexical_components_and_storage_uses_dirfd_containment(tmp_path):
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        with pytest.raises(EvidenceError):
            open_evidence_relative(fd, "../escape")
    finally:
        os.close(fd)


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
    _, manifest, evidence, batch = _published(tmp_path)
    assert (
        evidence.descriptors[0].transport_observation_digest
        == batch.transport_lineage[0].observation_digest
    )
    assert (
        manifest.transport_lineage[0].observation_digest
        == evidence.descriptors[0].transport_observation_digest
    )


def test_evidence_compare_create_allows_one_lineage_and_zero_canonical_writes_for_loser(tmp_path):
    store, first, _, batch = _published(tmp_path)
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    second = store.publish(batch)
    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert second.evidence_id == first.evidence_id
    assert after == before


def test_evidence_root_ancestor_symlink_and_toc_tou_fail_closed(tmp_path):
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    alias_parent = tmp_path / "alias-parent"
    alias_parent.symlink_to(real_parent, target_is_directory=True)
    root = alias_parent / "evidence"
    result = EvidenceReader(root).replay("x")
    assert result.failure_class == "EVIDENCE_ROOT_UNAVAILABLE"
    assert not (real_parent / "evidence").exists()

    store = EvidenceStore(tmp_path / "safe")
    store._prepare_root()
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "safe" / "objects" / "aa").symlink_to(outside, target_is_directory=True)
    from backend.app.market.evidence import _atomic_create_relative

    with pytest.raises(EvidenceError):
        _atomic_create_relative(tmp_path / "safe", "objects/aa/escape", b"raw")
    assert not list(outside.iterdir())


def test_replay_cli_rejects_credentials_token_header_cookie_url_provider_local_path_and_unknown_args_before_reader(  # noqa: E501
    tmp_path,
):
    with pytest.raises(EvidenceError):
        EvidenceReader(tmp_path).read("/etc/passwd")
