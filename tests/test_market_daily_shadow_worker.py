from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from backend.app.market.daily_shadow_candidates import DailyCandidateStore
from backend.app.market.daily_shadow_canonical import PublishedDailyCanonicalProjection
from backend.app.market.daily_shadow_models import (
    DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
    CanonicalDailyOhlcRow,
    DailyCanonicalLineageState,
    DailyCanonicalReadResult,
    DailyCanonicalSnapshot,
    DailyShadowFetchObservation,
    DailyShadowFetchResult,
    DailyShadowSourceRow,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)
from backend.app.market.daily_shadow_registry import (
    DAILY_SHADOW_TERMS_CONTRACT_VERSION,
    DailyShadowContract,
    DailyShadowRegistry,
)
from backend.app.market.daily_shadow_worker import (
    DailyHalfOpenProbeResult,
    DailyShadowWorker,
)
from backend.app.market.providers.shadow_contracts import TermsEvidence
from backend.app.market.shadow_calendar import ConfirmedSessionSnapshot
from backend.app.market.shadow_evidence import ShadowEvidenceReader, ShadowEvidenceStore

TRADE_DATE = date(2026, 7, 14)
NOW = datetime(2026, 8, 28, tzinfo=UTC)


def _canonical_bytes(value) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _snapshot() -> DailyCanonicalSnapshot:
    rows = (
        CanonicalDailyOhlcRow(
            trade_date=TRADE_DATE,
            symbol="sh.600000",
            open=Decimal("10"),
            high=Decimal("10.4"),
            low=Decimal("9.9"),
            close=Decimal("10.2"),
        ),
        CanonicalDailyOhlcRow(
            trade_date=TRADE_DATE,
            symbol="sz.000001",
            open=Decimal("11"),
            high=Decimal("11.4"),
            low=Decimal("10.9"),
            close=Decimal("11.2"),
        ),
    )
    symbols = tuple(row.symbol for row in rows)
    mapping = tuple((item, canonical_to_tickflow_daily_symbol(item)) for item in symbols)
    return DailyCanonicalSnapshot(
        trade_date=TRADE_DATE,
        lineage_state=DailyCanonicalLineageState.LEGACY_UNAVAILABLE,
        manifest_generation="generation-worker",
        manifest_sha256="1" * 64,
        partition_relative_path="bars/source=baostock/date=2026-07-14.parquet",
        partition_sha256="2" * 64,
        partition_row_count=2,
        eligible_symbol_count=2,
        excluded_symbol_count=0,
        canonical_universe_sha256="0" * 64,
        canonical_exclusion_sha256="3" * 64,
        symbol_mapping_sha256=domain_sha256("stock-eva/r2f3/daily-symbol-mapping/v1", mapping),
        ohlc_sha256="4" * 64,
        rows=rows,
    )


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
                unavailable_reason="CANONICAL_CHANGED",
            )
        return DailyCanonicalReadResult(status="ready", snapshot=self.snapshot)


def _projection(snapshot, *, fail_on=0):
    return PublishedDailyCanonicalProjection(_ProjectionReader(snapshot, fail_on=fail_on), snapshot)


def _calendar(snapshot: DailyCanonicalSnapshot) -> ConfirmedSessionSnapshot:
    dates = tuple(TRADE_DATE + timedelta(days=index) for index in range(20))
    values = {
        "snapshot_id": "daily-calendar-window-20260714",
        "provider_id": "tickflow",
        "window_id": "daily-window-20260714",
        "calendar_generation": "calendar-generation-2026",
        "calendar_sha256": "5" * 64,
        "confirmed_next_sessions": [item.isoformat() for item in dates],
        "universe_id": "daily-canonical-active-universe",
        "universe_sha256": DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        "captured_at": NOW.isoformat(),
    }
    return ConfirmedSessionSnapshot(
        **values,
        snapshot_sha256=hashlib.sha256(_canonical_bytes(values)).hexdigest(),
    )


def _terms() -> TermsEvidence:
    return TermsEvidence.build(
        content_bytes=b"reviewed TickFlow Free Daily Bar terms",
        official_url_allowlist=("https://free-api.tickflow.org",),
        terms_evidence_id="tickflow-free-daily-bar-20260828",
        provider_id="tickflow",
        content_object_relpath="terms/tickflow-free-daily-bar-20260828.txt",
        contract_version=DAILY_SHADOW_TERMS_CONTRACT_VERSION,
        as_of_date="2026-08-28",
        reviewer="stock-eva-owner",
        review_id="r2f3-free-daily-bar-review-20260828",
        approved_intended_use="free-historical-daily-ohlc-shadow-only",
        approved_retention="final-success-source-evidence-only",
        approved_credential_mode="credentialless-free",
        approved_quota_decision="unqualified-max-40-sequential-one-attempt",
    )


def _fetch(plan) -> DailyShadowFetchResult:
    timestamp = int(
        datetime.combine(TRADE_DATE, datetime.min.time(), tzinfo=UTC).timestamp() * 1000
    )
    values = (
        ("sh.600000", 10.0, 10.4, 9.9, 10.2),
        ("sz.000001", 11.0, 11.4, 10.9, 11.2),
    )
    rows = tuple(
        DailyShadowSourceRow(
            trade_date=TRADE_DATE,
            timestamp=timestamp,
            provider_symbol=canonical_to_tickflow_daily_symbol(symbol),
            symbol=symbol,
            open=open_value,
            high=high,
            low=low,
            close=close,
            volume=1000,
            amount=10000.0,
        )
        for symbol, open_value, high, low, close in values
    )
    return DailyShadowFetchResult(
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


def _unavailable_fetch(plan, failure_class="recv_timeout") -> DailyShadowFetchResult:
    return DailyShadowFetchResult(
        status="unavailable",
        trade_date=TRADE_DATE,
        request_plan_sha256=plan.request_plan_sha256,
        request_count=1,
        observations=(
            DailyShadowFetchObservation(
                ordinal=0,
                outcome="FAILURE",
                elapsed_ms=2,
                response_bytes=17,
                expected_rows=2,
                observed_rows=0,
                failure_class=failure_class,
            ),
        ),
        failure_class=failure_class,
        failed_ordinal=0,
    )


def _worker(
    tmp_path,
    *,
    fetcher,
    clock=lambda: NOW,
    canonical_factory=None,
    probe_runner=None,
):
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    registry = DailyShadowRegistry(control / "daily_bar_shadow.sqlite3", clock=clock)
    terms = _terms()
    registry.initialize(DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256), terms)
    snapshot = _snapshot()
    evidence_root = tmp_path / "evidence"
    candidate_root = tmp_path / "candidates"
    worker = DailyShadowWorker(
        registry=registry,
        canonical_factory=canonical_factory or (lambda _trade_date: _projection(snapshot)),
        confirmed_calendar=_calendar(snapshot),
        fetcher=fetcher,
        evidence_store=ShadowEvidenceStore(evidence_root),
        evidence_reader=ShadowEvidenceReader(evidence_root),
        candidate_store=DailyCandidateStore(candidate_root),
        terms_evidence_sha256=terms.manifest_sha256,
        owner="pytest-worker",
        probe_runner=probe_runner,
        clock=clock,
    )
    return worker, registry, snapshot, evidence_root, candidate_root, control


def test_one_offline_session_commits_evidence_candidate_then_terminal_state(tmp_path):
    provider_calls = []

    def fetcher(plan):
        provider_calls.append(plan.request_plan_sha256)
        return _fetch(plan)

    worker, registry, _snapshot_value, evidence_root, candidate_root, control = _worker(
        tmp_path, fetcher=fetcher
    )

    result = worker.run_one(TRADE_DATE)

    assert result.outcome == "SUCCESS"
    assert result.provider_requests == 1
    assert result.expected_symbols == result.observed_symbols == 2
    assert result.window_state == "OBSERVING"
    assert result.consecutive_sessions == 1
    assert len(provider_calls) == 1
    assert registry.read().window.consecutive_sessions == 1
    assert (evidence_root / "bundles" / result.evidence_id).is_dir()
    assert (candidate_root / "daily-candidates" / result.candidate_id).is_dir()
    assert not (control / "provider_registry.sqlite3").exists()
    assert result.canonical_writes is False
    assert result.publication_enabled is False
    assert result.failover_enabled is False


def test_duplicate_completed_date_and_concurrent_worker_make_zero_provider_calls(
    tmp_path,
):
    provider_calls = 0
    concurrent_results = []
    second_provider_calls = 0

    def second_fetcher(_plan):
        nonlocal second_provider_calls
        second_provider_calls += 1
        raise AssertionError("concurrent loser must not call provider")

    worker, registry, snapshot, evidence_root, candidate_root, _control = _worker(
        tmp_path, fetcher=lambda _plan: None
    )
    second = DailyShadowWorker(
        registry=registry,
        canonical_factory=lambda _trade_date: _projection(snapshot),
        confirmed_calendar=worker.confirmed_calendar,
        fetcher=second_fetcher,
        evidence_store=ShadowEvidenceStore(evidence_root),
        evidence_reader=ShadowEvidenceReader(evidence_root),
        candidate_store=DailyCandidateStore(candidate_root),
        terms_evidence_sha256=worker.terms_evidence_sha256,
        owner="pytest-worker-two",
        clock=lambda: NOW,
    )

    def first_fetcher(plan):
        nonlocal provider_calls
        provider_calls += 1
        concurrent_results.append(second.run_one(TRADE_DATE))
        return _fetch(plan)

    worker.fetcher = first_fetcher
    first = worker.run_one(TRADE_DATE)
    duplicate = worker.run_one(TRADE_DATE)

    assert first.outcome == "SUCCESS"
    assert [item.outcome for item in concurrent_results] == ["BUSY"]
    assert duplicate.outcome == "ALREADY_TERMINAL"
    assert provider_calls == 1
    assert second_provider_calls == 0
    assert registry.read().session_report_count == 1
    assert registry.read().window.consecutive_sessions == 1


def test_fetch_exceptions_open_circuit_and_skip_without_a_fourth_request(tmp_path):
    now = [NOW]
    provider_calls = 0

    def failing_fetcher(_plan):
        nonlocal provider_calls
        provider_calls += 1
        raise RuntimeError("sensitive upstream failure body")

    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=failing_fetcher,
        clock=lambda: now[0],
    )

    results = [worker.run_one(TRADE_DATE) for _ in range(3)]
    skipped = worker.run_one(TRADE_DATE)

    assert [item.outcome for item in results] == ["UNAVAILABLE"] * 3
    assert all(item.failure_class == "provider_unavailable" for item in results)
    assert skipped.outcome == "SKIPPED_CIRCUIT_OPEN"
    assert skipped.provider_requests == 0
    assert provider_calls == 3
    assert registry.circuit_action(owner="circuit-read", now=now[0]).state == "OPEN"
    assert "sensitive upstream failure body" not in json.dumps(
        [item.model_dump(mode="json") for item in results]
    )
    assert not evidence_root.exists()
    assert not candidate_root.exists()


def test_half_open_is_fixed_five_probe_only_and_does_not_start_full_session(tmp_path):
    now = [NOW]
    full_provider_calls = 0
    probe_calls = 0

    def failing_fetcher(plan):
        nonlocal full_provider_calls
        full_provider_calls += 1
        return _unavailable_fetch(plan)

    def successful_probe(_trade_date):
        nonlocal probe_calls
        probe_calls += 1
        return DailyHalfOpenProbeResult(outcome="SUCCESS", observed_symbols=5)

    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=failing_fetcher,
        probe_runner=successful_probe,
        clock=lambda: now[0],
    )
    for _ in range(3):
        assert worker.run_one(TRADE_DATE).outcome == "FAILURE"
    assert registry.circuit_action(owner="circuit-read", now=now[0]).state == "OPEN"

    now[0] += timedelta(seconds=901)
    probe = worker.run_one(TRADE_DATE)

    assert probe.outcome == "HALF_OPEN_PROBE_SUCCESS"
    assert probe.provider_requests == 1
    assert probe.expected_symbols == probe.observed_symbols == 5
    assert probe.circuit_state == "CLOSED"
    assert full_provider_calls == 3
    assert probe_calls == 1
    assert registry.read().session_report_count == 3
    assert not evidence_root.exists()
    assert not candidate_root.exists()


def test_first_rate_limited_session_opens_and_next_slot_skips_full_provider(tmp_path):
    now = [NOW]
    provider_calls = 0

    def rate_limited_fetcher(plan):
        nonlocal provider_calls
        provider_calls += 1
        return _unavailable_fetch(plan, failure_class="rate_limited")

    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=rate_limited_fetcher,
        clock=lambda: now[0],
    )

    failed = worker.run_one(TRADE_DATE)
    skipped = worker.run_one(TRADE_DATE)

    assert failed.outcome == "FAILURE"
    assert failed.failure_class == "rate_limited"
    assert failed.circuit_state == "OPEN"
    assert skipped.outcome == "SKIPPED_CIRCUIT_OPEN"
    assert skipped.provider_requests == 0
    assert skipped.circuit_state == "OPEN"
    assert provider_calls == 1
    assert registry.read().circuit_state == "OPEN"
    assert not evidence_root.exists()
    assert not candidate_root.exists()


def test_competing_half_open_worker_skips_and_failed_probe_reopens(tmp_path):
    now = [NOW]
    full_provider_calls = 0
    competing_results = []

    def failing_fetcher(plan):
        nonlocal full_provider_calls
        full_provider_calls += 1
        return _unavailable_fetch(plan)

    worker, registry, snapshot, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=failing_fetcher,
        clock=lambda: now[0],
    )
    competing = DailyShadowWorker(
        registry=registry,
        canonical_factory=lambda _trade_date: _projection(snapshot),
        confirmed_calendar=worker.confirmed_calendar,
        fetcher=lambda _plan: (_ for _ in ()).throw(
            AssertionError("HALF_OPEN competitor must not call provider")
        ),
        evidence_store=ShadowEvidenceStore(evidence_root),
        evidence_reader=ShadowEvidenceReader(evidence_root),
        candidate_store=DailyCandidateStore(candidate_root),
        terms_evidence_sha256=worker.terms_evidence_sha256,
        owner="pytest-competing-probe",
        clock=lambda: now[0],
    )

    def failed_probe(_trade_date):
        competing_results.append(competing.run_one(TRADE_DATE))
        return DailyHalfOpenProbeResult(
            outcome="FAILURE",
            observed_symbols=3,
            failure_class="recv_timeout",
        )

    worker.probe_runner = failed_probe
    for _ in range(3):
        assert worker.run_one(TRADE_DATE).outcome == "FAILURE"
    now[0] += timedelta(seconds=901)

    result = worker.run_one(TRADE_DATE)

    assert result.outcome == "HALF_OPEN_PROBE_FAILURE"
    assert result.failure_class == "recv_timeout"
    assert result.provider_requests == 1
    assert result.observed_symbols == 3
    assert [item.outcome for item in competing_results] == ["SKIPPED_HALF_OPEN"]
    assert competing_results[0].provider_requests == 0
    assert result.circuit_state == "OPEN"
    assert full_provider_calls == 3
    assert not evidence_root.exists()
    assert not candidate_root.exists()


def test_canonical_unavailable_stops_before_plan_lease_and_provider(tmp_path):
    provider_calls = 0

    def forbidden_fetch(_plan):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("canonical-unavailable path must not call provider")

    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=forbidden_fetch,
        canonical_factory=lambda _trade_date: DailyCanonicalReadResult(
            status="unavailable",
            unavailable_reason="CANONICAL_CHANGED",
        ),
    )

    result = worker.run_one(TRADE_DATE)

    assert result.outcome == "UNAVAILABLE"
    assert result.failure_class == "canonical_unavailable"
    assert result.provider_requests == 0
    assert provider_calls == 0
    assert registry.read().epoch_count == 0
    assert not evidence_root.exists()
    assert not candidate_root.exists()


def test_reconciliation_mismatch_keeps_evidence_but_never_candidate_or_count(tmp_path):
    def mismatching_fetcher(plan):
        fetched = _fetch(plan)
        changed = DailyShadowSourceRow.model_validate(
            {**fetched.rows[0].model_dump(mode="python"), "close": 10.22}
        )
        return DailyShadowFetchResult.model_validate(
            {
                **fetched.model_dump(mode="python"),
                "rows": (changed, fetched.rows[1]),
            }
        )

    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path, fetcher=mismatching_fetcher
    )

    result = worker.run_one(TRADE_DATE)

    assert result.outcome == "MISMATCH"
    assert result.failure_class == "reconciliation_mismatch"
    assert result.window_state == "RESET"
    assert registry.read().window.consecutive_sessions == 0
    assert registry.read().session_report_count == 1
    assert len(tuple((evidence_root / "bundles").iterdir())) == 1
    assert not (candidate_root / "daily-candidates").exists()


@pytest.mark.parametrize(
    ("crash_stage", "evidence_count", "candidate_count"),
    (("after_evidence_publish", 1, 0), ("after_candidate_publish", 1, 1)),
)
def test_immutable_publish_crash_leaves_objects_but_never_counts_session(
    tmp_path, crash_stage, evidence_count, candidate_count
):
    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path, fetcher=_fetch
    )
    if crash_stage == "after_evidence_publish":
        publish = worker.evidence_store.publish

        def crashing_publish(**kwargs):
            publish(**kwargs)
            raise RuntimeError("sensitive evidence crash")

        worker.evidence_store.publish = crashing_publish
    else:
        publish = worker.candidate_store.publish

        def crashing_publish(artifacts):
            publish(artifacts)
            raise RuntimeError("sensitive candidate crash")

        worker.candidate_store.publish = crashing_publish

    result = worker.run_one(TRADE_DATE)

    status = registry.read()
    evidence_bundles = evidence_root / "bundles"
    candidate_bundles = candidate_root / "daily-candidates"
    assert result.outcome == "UNAVAILABLE"
    assert result.failure_class == "worker_unavailable"
    assert status.window.consecutive_sessions == 0
    assert status.session_report_count == 1
    assert (
        len(tuple(evidence_bundles.iterdir())) if evidence_bundles.exists() else 0
    ) == evidence_count
    assert (
        len(tuple(candidate_bundles.iterdir())) if candidate_bundles.exists() else 0
    ) == candidate_count
    assert "sensitive" not in json.dumps(result.model_dump(mode="json"))


@pytest.mark.parametrize(
    ("fail_on", "evidence_count", "candidate_count"),
    ((1, 0, 0), (4, 1, 1)),
)
def test_canonical_reverify_failure_never_counts_partial_session(
    tmp_path, fail_on, evidence_count, candidate_count
):
    snapshot = _snapshot()
    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path,
        fetcher=_fetch,
        canonical_factory=lambda _trade_date: _projection(snapshot, fail_on=fail_on),
    )

    result = worker.run_one(TRADE_DATE)

    assert result.outcome == "UNAVAILABLE"
    assert result.failure_class == "candidate_unavailable"
    assert registry.read().window.consecutive_sessions == 0
    assert registry.read().session_report_count == 1
    evidence_bundles = evidence_root / "bundles"
    candidate_bundles = candidate_root / "daily-candidates"
    assert (
        len(tuple(evidence_bundles.iterdir())) if evidence_bundles.exists() else 0
    ) == evidence_count
    assert (
        len(tuple(candidate_bundles.iterdir())) if candidate_bundles.exists() else 0
    ) == candidate_count


@pytest.mark.parametrize(
    "crash_at",
    ("after_evidence", "after_candidate", "after_report", "after_attestation"),
)
def test_db_attach_crash_rolls_back_all_sidecar_terminal_rows(tmp_path, crash_at):
    worker, registry, _snapshot_value, evidence_root, candidate_root, _control = _worker(
        tmp_path, fetcher=_fetch
    )

    result = worker.run_one(TRADE_DATE, simulate_db_crash_at=crash_at)

    status = registry.read()
    assert result.outcome == "UNAVAILABLE"
    assert result.failure_class == "terminal_unavailable"
    assert status.window.consecutive_sessions == 0
    assert status.session_report_count == 0
    assert len(tuple((evidence_root / "bundles").iterdir())) == 1
    assert len(tuple((candidate_root / "daily-candidates").iterdir())) == 1
    retry = worker.run_one(TRADE_DATE)
    assert retry.outcome == "BUSY"
    assert retry.provider_requests == 0
