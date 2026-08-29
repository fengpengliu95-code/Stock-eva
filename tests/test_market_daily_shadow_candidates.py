from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from backend.app.market.daily_shadow_candidates import (
    DailyCandidateMismatch,
    DailyCandidateReader,
    DailyCandidateStore,
    DailyCandidateUnavailable,
    build_daily_candidate,
    build_daily_evidence_inputs,
)
from backend.app.market.daily_shadow_canonical import PublishedDailyCanonicalProjection
from backend.app.market.daily_shadow_models import (
    CanonicalDailyOhlcRow,
    DailyCanonicalLineageState,
    DailyCanonicalReadResult,
    DailyCanonicalSnapshot,
    DailyCanonicalUnavailableReason,
    DailyShadowFetchObservation,
    DailyShadowFetchResult,
    DailyShadowSourceRow,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)
from backend.app.market.providers.tickflow import TickFlowFreeAdapter
from backend.app.market.providers.tickflow_daily_shadow import (
    TickFlowFreeDailyShadowAdapter,
)
from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowCompletion,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowRequestCompletion,
)

TRADE_DATE = date(2026, 8, 10)


def _snapshot() -> DailyCanonicalSnapshot:
    rows = (
        CanonicalDailyOhlcRow(
            trade_date=TRADE_DATE,
            symbol="sh.600000",
            open=Decimal("10.00"),
            high=Decimal("10.40"),
            low=Decimal("9.90"),
            close=Decimal("10.20"),
        ),
        CanonicalDailyOhlcRow(
            trade_date=TRADE_DATE,
            symbol="sz.000001",
            open=Decimal("11.00"),
            high=Decimal("11.40"),
            low=Decimal("10.90"),
            close=Decimal("11.20"),
        ),
    )
    symbols = tuple(row.symbol for row in rows)
    mapping = tuple((symbol, canonical_to_tickflow_daily_symbol(symbol)) for symbol in symbols)
    return DailyCanonicalSnapshot(
        trade_date=TRADE_DATE,
        lineage_state=DailyCanonicalLineageState.LEGACY_UNAVAILABLE,
        manifest_generation="generation-daily-shadow",
        manifest_sha256="1" * 64,
        partition_relative_path=(
            "bars/source=baostock/year=2026/month=08/date=2026-08-10_222222222222.parquet"
        ),
        partition_sha256="2" * 64,
        partition_row_count=4,
        eligible_symbol_count=2,
        excluded_symbol_count=2,
        canonical_universe_sha256="0" * 64,
        canonical_exclusion_sha256="4" * 64,
        symbol_mapping_sha256=domain_sha256("stock-eva/r2f3/daily-symbol-mapping/v1", mapping),
        ohlc_sha256="6" * 64,
        rows=rows,
    )


def _fetch(snapshot: DailyCanonicalSnapshot, *, first_close="10.20", volume=1000):
    plan = TickFlowFreeDailyShadowAdapter().plan(snapshot)
    rows = (
        DailyShadowSourceRow(
            trade_date=TRADE_DATE,
            timestamp=1786320000000,
            provider_symbol="600000.SH",
            symbol="sh.600000",
            open=10.0,
            high=10.4,
            low=9.9,
            close=float(first_close),
            volume=volume,
            amount=10200.0,
        ),
        DailyShadowSourceRow(
            trade_date=TRADE_DATE,
            timestamp=1786320000000,
            provider_symbol="000001.SZ",
            symbol="sz.000001",
            open=11.0,
            high=11.4,
            low=10.9,
            close=11.2,
            volume=2000,
            amount=22400.0,
        ),
    )
    result = DailyShadowFetchResult(
        status="ready",
        trade_date=TRADE_DATE,
        request_plan_sha256=plan.request_plan_sha256,
        request_count=1,
        rows=rows,
        observations=(
            DailyShadowFetchObservation(
                ordinal=0,
                outcome="SUCCESS",
                elapsed_ms=1,
                response_bytes=100,
                expected_rows=2,
                observed_rows=2,
            ),
        ),
    )
    return plan, result


class _ProjectionReader:
    def __init__(self, snapshot, *, fail_on=0):
        self.snapshot = snapshot
        self.calls = 0
        self.fail_on = fail_on

    def verify(self):
        self.calls += 1
        if self.calls == self.fail_on:
            return DailyCanonicalReadResult(
                status="unavailable",
                unavailable_reason=DailyCanonicalUnavailableReason.CANONICAL_CHANGED,
            )
        return DailyCanonicalReadResult(status="ready", snapshot=self.snapshot)


def _projection(snapshot, *, fail_on=0):
    return PublishedDailyCanonicalProjection(_ProjectionReader(snapshot, fail_on=fail_on), snapshot)


def _publish_evidence(tmp_path, snapshot, plan, fetch):
    inputs = build_daily_evidence_inputs(
        plan,
        fetch,
        job_id="daily-job-20260810",
        window_id="tickflow-free-daily-window-v1",
        session_id="daily-session-20260810",
        evidence_id="daily-evidence-20260810",
    )
    bundle = ShadowEvidenceStore(tmp_path).publish(
        plan=inputs.plan,
        completion=inputs.completion,
        attempts=inputs.attempts,
    )
    return inputs, bundle


def test_final_success_source_rows_publish_through_existing_immutable_evidence(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path, snapshot, plan, fetch)

    read = ShadowEvidenceReader(tmp_path).read(bundle.evidence_id)
    assert read == bundle
    assert len(read.rows) == 2
    assert read.page_refs == (f"{inputs.plan.requests[0].request_id}:page-000001",)
    assert read.manifest_sha256 == bundle.manifest_sha256
    assert "raw_response_b64" not in str(read.rows)


def test_failed_partial_attempt_cannot_be_published_or_read(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs = build_daily_evidence_inputs(
        plan,
        fetch,
        job_id="daily-job-20260810",
        window_id="tickflow-free-daily-window-v1",
        session_id="daily-session-20260810",
        evidence_id="daily-evidence-20260810",
    )
    failed = inputs.attempts[0].model_copy(update={"outcome": "failure"})
    with pytest.raises(Exception, match="final attempt unavailable"):
        ShadowEvidenceStore(tmp_path).publish(
            plan=inputs.plan,
            completion=inputs.completion,
            attempts=(failed,),
        )
    assert not (tmp_path / "bundles" / "daily-evidence-20260810").exists()


def test_wrong_timestamp_cannot_become_daily_evidence():
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    wrong_row = fetch.rows[0].model_copy(update={"timestamp": 0})
    wrong_fetch = fetch.model_copy(update={"rows": (wrong_row, fetch.rows[1])})

    with pytest.raises(DailyCandidateUnavailable, match="final success"):
        build_daily_evidence_inputs(
            plan,
            wrong_fetch,
            job_id="daily-job-20260810",
            window_id="tickflow-free-daily-window-v1",
            session_id="daily-session-20260810",
            evidence_id="daily-evidence-20260810",
        )


def test_next_shanghai_trade_date_timestamp_cannot_become_daily_evidence():
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    timestamp = int(
        datetime.combine(
            TRADE_DATE + timedelta(days=1),
            time.min,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).timestamp()
        * 1000
    )
    wrong_row = fetch.rows[0].model_copy(update={"timestamp": timestamp})
    wrong_fetch = fetch.model_copy(update={"rows": (wrong_row, fetch.rows[1])})

    with pytest.raises(DailyCandidateUnavailable, match="final success"):
        build_daily_evidence_inputs(
            plan,
            wrong_fetch,
            job_id="daily-job-20260810",
            window_id="tickflow-free-daily-window-v1",
            session_id="daily-session-20260810",
            evidence_id="daily-evidence-20260810",
        )


def test_task14_fixed_five_plan_cannot_build_daily_evidence():
    snapshot = _snapshot()
    _plan, fetch = _fetch(snapshot)
    with pytest.raises(TypeError, match="Daily shadow plan"):
        build_daily_evidence_inputs(
            TickFlowFreeAdapter().plan(TRADE_DATE),
            fetch,
            job_id="daily-job-20260810",
            window_id="tickflow-free-daily-window-v1",
            session_id="daily-session-20260810",
            evidence_id="daily-evidence-20260810",
        )


def test_candidate_binds_exact_evidence_canonical_hashes_and_semantic_states(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path, snapshot, plan, fetch)

    artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=inputs.plan,
        evidence=bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )

    candidate = artifacts.candidate
    assert candidate.expected_symbol_count == 2
    assert candidate.observed_symbol_count == 2
    assert candidate.evidence_sha256 == bundle.manifest_sha256
    assert candidate.canonical_snapshot_sha256 == snapshot.snapshot_sha256
    assert candidate.canonical_symbol_set_sha256 == plan.canonical_symbol_set_sha256
    assert candidate.units_state == "UNKNOWN"
    assert candidate.suspension_semantics_state == "UNKNOWN"
    assert candidate.factor_evidence_state == "UNQUALIFIED"
    assert candidate.adjustment_request_state == "NONE_REQUESTED"
    assert candidate.quality_verdict == "PASS"
    assert candidate.reconciliation_verdict == "PASS"
    assert artifacts.reconciliation.price_cell_count == 8
    assert artifacts.reconciliation.within_tolerance_count == 8
    assert plan.shards[0].request_sha256 in inputs.plan.requests[0].symbol_or_index_shard


def test_evidence_request_must_bind_exact_daily_shard_membership(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path, snapshot, plan, fetch)
    wrong_request = inputs.plan.requests[0].model_copy(
        update={"symbol_or_index_shard": "daily-shard-00-002"}
    )
    wrong_plan = inputs.plan.model_copy(update={"requests": (wrong_request,)})

    with pytest.raises(DailyCandidateUnavailable, match=r"candidate inputs|evidence .* mismatch"):
        build_daily_candidate(
            plan=plan,
            evidence_plan=wrong_plan,
            evidence=bundle,
            evidence_reader=ShadowEvidenceReader(tmp_path),
            canonical=_projection(snapshot),
            terms_evidence_sha256="7" * 64,
        )


def test_candidate_rejects_manifest_rows_that_do_not_match_hashed_page_rows(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs = build_daily_evidence_inputs(
        plan,
        fetch,
        job_id="daily-job-20260810",
        window_id="tickflow-free-daily-window-v1",
        session_id="daily-session-20260810",
        evidence_id="daily-evidence-20260810",
    )
    original_attempt = inputs.attempts[0]
    reversed_page = {
        "page_identity": original_attempt.pages[0]["page_identity"],
        "rows": list(reversed(original_attempt.pages[0]["rows"])),
    }
    attempt = ShadowAttempt(
        **{
            **original_attempt.model_dump(mode="python"),
            "pages": (reversed_page,),
        }
    )
    original_completion = inputs.completion.requests[0]
    request_completion = ShadowRequestCompletion(
        ordinal=original_completion.ordinal,
        request_id=original_completion.request_id,
        endpoint=original_completion.endpoint,
        endpoint_class=original_completion.endpoint_class,
        final_attempt_id=original_completion.final_attempt_id,
        pages=(reversed_page,),
    )
    completion = ShadowCompletion(
        session_id=inputs.completion.session_id,
        job_id=inputs.completion.job_id,
        provider_id=inputs.completion.provider_id,
        window_id=inputs.completion.window_id,
        evidence_id=inputs.completion.evidence_id,
        request_plan_sha256=inputs.plan.request_plan_sha256,
        requests=(request_completion,),
    )
    bundle = ShadowEvidenceStore(tmp_path).publish(
        plan=inputs.plan,
        completion=completion,
        attempts=(attempt,),
    )

    with pytest.raises(DailyCandidateUnavailable, match="page graph mismatch"):
        build_daily_candidate(
            plan=plan,
            evidence_plan=inputs.plan,
            evidence=bundle,
            evidence_reader=ShadowEvidenceReader(tmp_path),
            canonical=_projection(snapshot),
            terms_evidence_sha256="7" * 64,
        )


def test_terms_evidence_hash_is_strict_lowercase_sha256(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path, snapshot, plan, fetch)

    with pytest.raises(DailyCandidateUnavailable, match="terms evidence"):
        build_daily_candidate(
            plan=plan,
            evidence_plan=inputs.plan,
            evidence=bundle,
            evidence_reader=ShadowEvidenceReader(tmp_path),
            canonical=_projection(snapshot),
            terms_evidence_sha256="Z" * 64,
        )


def test_one_tick_passes_but_any_cell_above_one_tick_fails_whole_session(tmp_path):
    snapshot = _snapshot()
    plan, within = _fetch(snapshot, first_close="10.21")
    inputs, bundle = _publish_evidence(tmp_path / "within", snapshot, plan, within)
    passed = build_daily_candidate(
        plan=plan,
        evidence_plan=inputs.plan,
        evidence=bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "within"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )
    assert passed.reconciliation.max_close_delta == Decimal("0.01")

    plan, outside = _fetch(snapshot, first_close="10.211")
    inputs, bundle = _publish_evidence(tmp_path / "outside", snapshot, plan, outside)
    with pytest.raises(DailyCandidateMismatch) as captured:
        build_daily_candidate(
            plan=plan,
            evidence_plan=inputs.plan,
            evidence=bundle,
            evidence_reader=ShadowEvidenceReader(tmp_path / "outside"),
            canonical=_projection(snapshot),
            terms_evidence_sha256="7" * 64,
        )
    assert captured.value.report.verdict == "MISMATCH"
    assert captured.value.report.within_tolerance_count == 7


def test_activity_values_do_not_enter_ohlc_reconciliation(tmp_path):
    snapshot = _snapshot()
    plan, first = _fetch(snapshot, volume=1000)
    first_inputs, first_bundle = _publish_evidence(tmp_path / "first", snapshot, plan, first)
    first_artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=first_inputs.plan,
        evidence=first_bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "first"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )

    plan, second = _fetch(snapshot, volume=999999)
    second_inputs, second_bundle = _publish_evidence(tmp_path / "second", snapshot, plan, second)
    second_artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=second_inputs.plan,
        evidence=second_bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "second"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )
    assert first_artifacts.reconciliation.max_open_delta == Decimal("0")
    assert second_artifacts.reconciliation.max_open_delta == Decimal("0")
    assert first_artifacts.reconciliation.price_cell_count == 8
    assert second_artifacts.reconciliation.price_cell_count == 8
    assert first_artifacts.candidate.source_rows_sha256 != (
        second_artifacts.candidate.source_rows_sha256
    )


def test_normalized_ohlc_hash_ignores_json_integer_float_spelling(tmp_path):
    snapshot = _snapshot()
    plan, first = _fetch(snapshot)
    first_inputs, first_bundle = _publish_evidence(tmp_path / "first", snapshot, plan, first)
    first_artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=first_inputs.plan,
        evidence=first_bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "first"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )

    integer_row = first.rows[0].model_copy(update={"open": 10})
    second = first.model_copy(update={"rows": (integer_row, first.rows[1])})
    second_inputs, second_bundle = _publish_evidence(tmp_path / "second", snapshot, plan, second)
    second_artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=second_inputs.plan,
        evidence=second_bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "second"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )

    assert first_artifacts.candidate.source_rows_sha256 != (
        second_artifacts.candidate.source_rows_sha256
    )
    assert first_artifacts.candidate.normalized_ohlc_sha256 == (
        second_artifacts.candidate.normalized_ohlc_sha256
    )


def test_canonical_change_before_or_after_comparison_aborts_candidate(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path, snapshot, plan, fetch)
    for fail_on in (1, 2):
        with pytest.raises(Exception, match="canonical snapshot changed"):
            build_daily_candidate(
                plan=plan,
                evidence_plan=inputs.plan,
                evidence=bundle,
                evidence_reader=ShadowEvidenceReader(tmp_path),
                canonical=_projection(snapshot, fail_on=fail_on),
                terms_evidence_sha256="7" * 64,
            )


def test_candidate_bundle_is_atomic_idempotent_and_strictly_readable(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path / "evidence", snapshot, plan, fetch)
    artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=inputs.plan,
        evidence=bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "evidence"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )
    store = DailyCandidateStore(tmp_path / "candidates")
    first = store.publish(artifacts)
    second = store.publish(artifacts)
    assert first == second
    assert (
        DailyCandidateReader(tmp_path / "candidates").read(artifacts.candidate.candidate_id)
        == artifacts
    )

    crash_root = tmp_path / "crash"
    with pytest.raises(RuntimeError, match="interrupted"):
        DailyCandidateStore(crash_root).publish(artifacts, simulate_crash=True)
    assert not (crash_root / "daily-candidates" / artifacts.candidate.candidate_id).exists()


def test_candidate_reader_rejects_symlinked_bundle_ancestor(tmp_path):
    snapshot = _snapshot()
    plan, fetch = _fetch(snapshot)
    inputs, bundle = _publish_evidence(tmp_path / "evidence", snapshot, plan, fetch)
    artifacts = build_daily_candidate(
        plan=plan,
        evidence_plan=inputs.plan,
        evidence=bundle,
        evidence_reader=ShadowEvidenceReader(tmp_path / "evidence"),
        canonical=_projection(snapshot),
        terms_evidence_sha256="7" * 64,
    )
    root = tmp_path / "candidates"
    DailyCandidateStore(root).publish(artifacts)
    bundles = root / "daily-candidates"
    escaped = root / "escaped-bundles"
    bundles.rename(escaped)
    bundles.symlink_to(escaped, target_is_directory=True)

    with pytest.raises(DailyCandidateUnavailable, match="unavailable"):
        DailyCandidateReader(root).read(artifacts.candidate.candidate_id)
