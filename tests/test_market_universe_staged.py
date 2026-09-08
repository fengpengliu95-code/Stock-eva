import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from backend.app.classification.models import ClassificationSnapshot
from backend.app.classification.store import ClassificationReadSnapshot
from backend.app.config import Settings
from backend.app.market.automation import (
    CanonicalRefreshExecution,
    ConcreteUniversePostSuccessHook,
    LegacyPreflightSnapshot,
    MainBoardInspection,
    MarketAutomationService,
    UniverseHookResult,
    UniverseMaintenanceService,
    _calendar_failure_reason,
    classify_universe_mode,
    observe_legacy_request_universe,
)
from backend.app.market.calendar_generation import (
    CalendarGenerationError,
    CalendarStoreUnavailable,
)
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore
from backend.app.market.universe import (
    SourceRefsV1,
    UniverseAttemptPlanV1,
    UniversePublicationContextV1,
    UniverseSidecarStore,
    canonical_json_bytes,
    classification_snapshot_sha256,
    domain_sha256,
    source_version_digest,
)
from tests.test_market_automation import synthetic_calendar
from tests.test_market_universe import (
    _authority,
    _bundle,
    _calendar_authority,
    _contract,
    _user_capture,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_calendar_failure_reason_preserves_control_conflict_and_unavailable_classes():
    assert _calendar_failure_reason(CalendarStoreUnavailable("locked")) == (
        "CONTROL_STATE_UNAVAILABLE"
    )
    assert _calendar_failure_reason(CalendarGenerationError("source conflict")) == (
        "CALENDAR_CONFLICT"
    )
    assert _calendar_failure_reason(CalendarGenerationError("missing generation")) == (
        "CALENDAR_UNAVAILABLE"
    )
    assert (
        _calendar_failure_reason(
            authority=SimpleNamespace(
                read_result=SimpleNamespace(reason="CALENDAR_CONFLICT_SOURCE_UNAVAILABLE")
            )
        )
        == "CALENDAR_CONFLICT"
    )


def test_settings_expose_closed_universe_maintenance_matrix():
    settings = Settings()
    assert settings.market_universe_mode == "off"
    assert settings.market_universe_maintenance_enabled is False
    assert settings.market_universe_maintenance_interval_seconds == 86400


@pytest.mark.parametrize(
    "environment,mode,reason",
    [
        ("production", "shadow", "BLOCKED_PRODUCTION_MODE_OFF"),
        ("production", "enforce", "BLOCKED_PRODUCTION_MODE_OFF"),
        ("staging", "enforce", "BLOCKED_ENFORCE_NOT_ENABLED"),
    ],
)
def test_universe_mode_matrix_is_fail_closed(environment, mode, reason):
    decision = classify_universe_mode(environment, mode)
    assert decision.reason_code == reason
    assert decision.provider_requests == 0
    assert decision.writes_canonical is False


def test_invalid_profile_is_not_shadow_authority():
    decision = classify_universe_mode("qa-production", "shadow")
    assert decision.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert decision.action == "defer"
    assert decision.provider_requests == 0


def test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison():
    inspection = MainBoardInspection(
        trade_date=date(2026, 8, 20),
        main_board_count=1,
        shanghai_count=1,
        shenzhen_count=0,
        total_expected_count=1,
        metadata_provider_requests=1,
        main_board_symbols=("sh.600000",),
    )
    snapshot = inspection.as_preflight_snapshot(("sh.600000", "sh.000001", "sz.399001"))
    result = observe_legacy_request_universe(snapshot, "accepted", None, None)
    assert result.reason_code == "NO_COMPARISON"
    assert result.control_reason == "CONTROL_STATE_UNAVAILABLE"
    assert not hasattr(result, "observed_symbols")


def _ready_result(run_id: str, target: date) -> RefreshResult:
    stamp = datetime(2026, 8, 20, 10, tzinfo=UTC)
    return RefreshResult(
        run_id=run_id,
        request_key="daily:fixture",
        run_kind="daily",
        requested_date=target,
        source="baostock",
        status="ready",
        requested_count=1,
        succeeded_count=1,
        coverage_ratio=1.0,
        failed_symbols=[],
        quality_issues=[],
        started_at=stamp,
        completed_at=stamp,
    )


class _NoopProvider:
    def trading_dates(self, start_date, end_date):
        return [start_date]


def test_disabled_maintenance_is_zero_hook_and_preserves_legacy_tick(tmp_path: Path):
    calls = []

    class Hook:
        def offer(self, *args):
            calls.append(args)
            return UniverseHookResult(status="DEFER", reason_code=None, provider_requests=0)

    def canonical(**kwargs):
        return _ready_result(kwargs["run_id"], kwargs["trade_date"])

    service = MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        _NoopProvider(),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        canonical_refresh=canonical,
        universe_post_success_hook=Hook(),
        market_universe_maintenance_enabled=False,
        market_universe_mode="shadow",
    )
    service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    assert calls == []
    assert service.last_universe_decision.reason_code == "NONE"


def test_shadow_maintenance_hook_is_last_and_consumes_once(tmp_path: Path):
    events = []

    class Hook:
        def offer(self, trade_date, now, context):
            events.append(("universe", trade_date, context))
            return UniverseHookResult(status="DEFER", reason_code=None, provider_requests=0)

    class Shadow:
        def offer(self, *args, **kwargs):
            events.append(("shadow",))

    def canonical(**kwargs):
        events.append(("canonical",))
        return _ready_result(kwargs["run_id"], kwargs["trade_date"])

    service = MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        _NoopProvider(),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        canonical_refresh=canonical,
        shadow_handoff=Shadow(),
        universe_post_success_hook=Hook(),
        market_universe_maintenance_enabled=True,
        market_universe_mode="shadow",
        environment="staging",
    )
    service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    # An ordinary RefreshResult carries no sealed CandidateStore context and
    # therefore cannot enter the production-wired Universe service.
    assert [item[0] for item in events] == ["canonical", "shadow"]


def test_fresh_sealed_context_orders_canonical_postpublish_shadow_then_maintenance(
    tmp_path: Path,
):
    """AC15: only a sealed canonical execution can reach the real maintenance service."""
    events = []
    target = date(2026, 7, 23)
    provider_factory_calls = []

    class Shadow:
        def offer(self, *args, **kwargs):
            events.append("shadow")

    class PostPublish:
        def run_after_publication(self, result):
            events.append("post_publish")

    class ClassificationReader:
        def read_snapshot(self, as_of, *, known_at, include_securities):
            return ClassificationReadSnapshot(
                ready_generation_id=None,
                generation=None,
                securities=[],
                security_snapshot_date=None,
                index_components=[],
                index_snapshot_date=None,
                sector_memberships=[],
                sector_snapshot_date=None,
            )

    sidecar = UniverseSidecarStore(tmp_path / "universe.sqlite3")

    class RecordingMaintenance(UniverseMaintenanceService):
        def __call__(self, **kwargs):
            events.append("maintenance")
            return super().__call__(**kwargs)

    def provider_factory(**_kwargs):
        provider_factory_calls.append(True)
        raise AssertionError("classification-unavailable lane must not construct provider")

    maintenance = RecordingMaintenance(
        sidecar=sidecar,
        calendar_store=SimpleNamespace(read_authority=lambda: _calendar_authority(target)),
        classification_store=ClassificationReader(),
        user_store=SimpleNamespace(),
        lock_path=tmp_path / "maintenance.lock",
        provider_factory=provider_factory,
        context_validator=lambda value, **_kwargs: value,
        clock=lambda: datetime(2026, 7, 23, 19, tzinfo=UTC),
    )

    # Attach the consumer outside the callback so the automation path exercises
    # the same sealed-context handoff used by the production callback.
    context_by_run: dict[str, UniversePublicationContextV1] = {}

    def canonical_with_context(**kwargs):
        events.append("canonical")
        result = _ready_result(kwargs["run_id"], kwargs["trade_date"])
        context_by_run[kwargs["run_id"]] = _maintenance_context(
            kwargs["trade_date"], kwargs["run_id"]
        )
        return CanonicalRefreshExecution(result=result, builder_outcome="accepted")

    canonical_with_context.consume_universe_publication_context = lambda run_id, trade_date: (
        context_by_run.get(run_id)
    )
    service = MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        _NoopProvider(),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        lock_path=tmp_path / "refresh.lock",
        post_publish=PostPublish(),
        shadow_handoff=Shadow(),
        canonical_refresh=canonical_with_context,
        universe_post_success_hook=ConcreteUniversePostSuccessHook(
            environment="staging",
            mode="shadow",
            enabled=True,
            maintenance_service=maintenance,
        ),
        market_universe_maintenance_enabled=True,
        market_universe_mode="shadow",
        environment="staging",
        universe_context_validator=lambda value, **_kwargs: value,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None and outcome.result.status == "ready"
    assert events == ["canonical", "post_publish", "shadow", "maintenance"]
    assert service.last_universe_result is not None
    assert service.last_universe_result.status == "BLOCKED"
    assert service.last_universe_result.reason_code == "CLASSIFICATION_UNAVAILABLE"
    assert service.last_universe_result.provider_requests == 0
    assert provider_factory_calls == []
    assert sidecar.read_latest_terminal_finished_at(target) is not None


def test_concrete_hook_rejects_non_strict_service_fallback():
    with pytest.raises(TypeError):
        ConcreteUniversePostSuccessHook(
            environment="staging",
            mode="shadow",
            enabled=True,
            maintenance_service=object(),
        )


def test_release_candidate_removes_runner_and_raw_symbol_public_fields():
    import backend.app.market.automation as automation

    assert not hasattr(automation, "UniverseMaintenanceRunner")
    assert "legacy_shadow_input" not in CanonicalRefreshExecution.__annotations__
    assert "observed_symbols" not in LegacyPreflightSnapshot.__annotations__
    assert "inspection" not in LegacyPreflightSnapshot.__annotations__


def test_missing_context_consumer_does_not_call_universe_hook(tmp_path: Path):
    calls = []

    class Hook:
        def offer(self, *args):
            calls.append(args)
            return UniverseHookResult(status="DEFER", reason_code=None, provider_requests=0)

    def canonical(**kwargs):
        return CanonicalRefreshExecution(
            result=_ready_result(kwargs["run_id"], kwargs["trade_date"])
        )

    service = MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        _NoopProvider(),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        canonical_refresh=canonical,
        universe_post_success_hook=Hook(),
        market_universe_maintenance_enabled=True,
        market_universe_mode="shadow",
        environment="staging",
    )
    service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    assert calls == []


def test_ordinary_refresh_result_never_enters_universe_service(tmp_path: Path):
    calls = []

    class Hook:
        def offer(self, *args):
            calls.append(args)
            return UniverseHookResult(status="DEFER", reason_code=None, provider_requests=0)

    def canonical(**kwargs):
        return _ready_result(kwargs["run_id"], kwargs["trade_date"])

    service = MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        _NoopProvider(),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        canonical_refresh=canonical,
        universe_post_success_hook=Hook(),
        market_universe_maintenance_enabled=True,
        market_universe_mode="shadow",
        environment="staging",
    )
    service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    assert calls == []


def _attempt_plan(
    *,
    operation_day: date = date(2026, 8, 20),
    attempt_id: str = "attempt-1",
    created: datetime = datetime(2026, 8, 20, 0, tzinfo=UTC),
):
    values = {
        "attempt_id": attempt_id,
        "hook_kind": "universe_post_success",
        "refresh_id": "refresh-1",
        "canonical_run_id": "refresh-1",
        "source_version_digest": "a" * 64,
        "trade_date": date(2026, 8, 20),
        "operation_day": operation_day,
        "attempt_status": "RUNNING",
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": created,
    }
    dedup_preimage = {
        "trade_date": values["trade_date"].isoformat(),
        "operation_day": values["operation_day"].isoformat(),
        "canonical_run_id": values["canonical_run_id"],
        "source_version_digest": values["source_version_digest"],
    }
    plan_preimage = {
        **dedup_preimage,
        "hook_kind": values["hook_kind"],
        "refresh_id": values["refresh_id"],
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": created.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "attempt_status": "RUNNING",
    }
    values["dedup_key"] = domain_sha256(
        "stock-eva/r2f4.2/universe-attempt-dedup/v1", dedup_preimage
    )
    values["planned_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/universe-attempt-plan/v1", plan_preimage
    )
    return UniverseAttemptPlanV1(**values)


def test_durable_attempt_claim_is_restart_safe_and_allows_first_empty_sidecar(tmp_path: Path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    plan = _attempt_plan()
    assert store.claim_attempt(plan).claimed is True
    assert store.claim_attempt(plan).claimed is False


def test_attempt_dedup_identity_is_four_fields_but_plan_hash_is_complete():
    first = _attempt_plan()
    second = _attempt_plan(attempt_id="attempt-2", created=datetime(2026, 8, 20, 0, 1, tzinfo=UTC))
    assert first.dedup_key == second.dedup_key
    assert first.planned_sha256 != second.planned_sha256


def test_attempt_plan_hash_preimage_uses_canonical_utc_z_timestamp():
    created = datetime(2026, 8, 20, 0, tzinfo=UTC)
    values = {
        "attempt_id": "attempt-z",
        "hook_kind": "universe_post_success",
        "refresh_id": "refresh-z",
        "canonical_run_id": "refresh-z",
        "source_version_digest": "a" * 64,
        "trade_date": date(2026, 8, 20),
        "operation_day": date(2026, 8, 20),
        "attempt_status": "RUNNING",
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": created,
    }
    identity = {
        "trade_date": "2026-08-20",
        "operation_day": "2026-08-20",
        "canonical_run_id": "refresh-z",
        "source_version_digest": "a" * 64,
    }
    planned = {
        **identity,
        "hook_kind": "universe_post_success",
        "refresh_id": "refresh-z",
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": "2026-08-20T00:00:00Z",
        "attempt_status": "RUNNING",
    }
    plan = UniverseAttemptPlanV1(
        **values,
        dedup_key=domain_sha256("stock-eva/r2f4.2/universe-attempt-dedup/v1", identity),
        planned_sha256=domain_sha256("stock-eva/r2f4.2/universe-attempt-plan/v1", planned),
    )
    assert plan.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z") == (
        "2026-08-20T00:00:00Z"
    )


def test_publication_context_evidence_refs_are_hash_only_three_content_refs():
    context = _maintenance_context(date(2026, 9, 1))
    lineage = json.loads(context.publication_lineage_json)
    assert context.evidence_refs == tuple(
        sorted(
            (
                lineage["evidence_sha256"],
                lineage["candidate_manifest_sha256"],
                lineage["gate_report_sha256"],
            )
        )
    )
    assert len(context.evidence_refs) == 3
    assert all(len(value) == 64 for value in context.evidence_refs)
    assert lineage["evidence_id"] not in context.evidence_refs
    assert lineage["candidate_id"] not in context.evidence_refs


def _maintenance_context(target: date, run_id: str = "run-maintenance"):
    lineage = {
        "provider_id": "baostock",
        "universe_id": "all-main-board-plus-required-symbols",
        "evidence_id": "a" * 64,
        "evidence_sha256": "b" * 64,
        "candidate_id": "c" * 64,
        "candidate_manifest_sha256": "d" * 64,
        "gate_report_sha256": "e" * 64,
        "adapter_version": "r2f2.v1",
        "source_schema_version": "daily_astock.v1",
    }
    lineage_json = canonical_json_bytes(lineage).decode()
    evidence_refs = tuple(
        sorted(
            {
                lineage["evidence_sha256"],
                lineage["candidate_manifest_sha256"],
                lineage["gate_report_sha256"],
            }
        )
    )
    values = {
        "run_id": run_id,
        "trade_date": target,
        "manifest_ref": lineage["candidate_manifest_sha256"],
        "evidence_refs": evidence_refs,
        "publication_lineage_json": lineage_json,
        "publication_lineage_sha256": domain_sha256(
            "stock-eva/r2f4.2/publication-lineage/v2", lineage
        ),
        "status": "ready",
        "created_at": datetime(2026, 9, 1, 18, tzinfo=UTC),
    }
    context_sha = domain_sha256(
        "stock-eva/r2f4.2/universe-publication-context/v1",
        {
            **values,
            "trade_date": target.isoformat(),
            "evidence_refs": list(evidence_refs),
            "created_at": values["created_at"].astimezone(UTC).isoformat().replace("+00:00", "Z"),
        },
    )
    values.update(context_id=context_sha[:32], context_sha256=context_sha)
    return UniversePublicationContextV1.model_validate(values)


@pytest.mark.parametrize("generation", [None, "requested_unverified", "future"])
def test_maintenance_unqualified_classification_is_durable_and_never_calls_provider(
    tmp_path: Path, generation
):
    target = date(2026, 9, 1)
    context = _maintenance_context(target)
    classification_calls = []
    provider_calls = []
    generation_value = (
        None
        if generation is None
        else SimpleNamespace(
            generation_id="classification-1",
            sequence=1,
            source="baostock",
            source_version="v1",
            source_snapshot_date=date(2026, 8, 31),
            source_date_semantics=(
                "source_observed" if generation == "future" else "requested_unverified"
            ),
            observed_at=datetime(2026, 9, 1, 20 if generation == "future" else 8, tzinfo=UTC),
        )
    )

    class ClassificationReader:
        def read_snapshot(self, as_of, *, known_at, include_securities):
            classification_calls.append((as_of, known_at, include_securities))
            return ClassificationReadSnapshot(
                ready_generation_id=None,
                generation=generation_value,
                securities=[],
                security_snapshot_date=None,
                index_components=[],
                index_snapshot_date=None,
                sector_memberships=[],
                sector_snapshot_date=None,
            )

    class UserReader:
        def capture_required_symbol_snapshot_existing(self):
            raise AssertionError("unqualified classification must not read UserStore")

    def provider_factory(**_kwargs):
        provider_calls.append(True)
        raise AssertionError("unqualified classification must not construct provider")

    sidecar = UniverseSidecarStore(tmp_path / "universe.sqlite3")
    service = UniverseMaintenanceService(
        sidecar=sidecar,
        calendar_store=SimpleNamespace(read_authority=lambda: _calendar_authority(target)),
        classification_store=ClassificationReader(),
        user_store=UserReader(),
        lock_path=tmp_path / "refresh.lock",
        provider_factory=provider_factory,
        context_validator=lambda value, **_kwargs: value,
        clock=lambda: datetime(2026, 9, 1, 19, tzinfo=UTC),
    )

    result = service(
        trade_date=target.isoformat(),
        now="2026-09-01T19:00:00+00:00",
        context=context,
        sidecar=sidecar,
    )
    expected_reason = {
        None: "CLASSIFICATION_UNAVAILABLE",
        "requested_unverified": "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
        "future": "PIT_CUTOFF_VIOLATION",
    }[generation]
    assert result.status == "BLOCKED"
    assert result.reason_code == expected_reason
    assert result.provider_requests == 0
    assert provider_calls == []
    assert classification_calls[0][0] == target
    assert classification_calls[0][1] == datetime(2026, 9, 1, 19, tzinfo=UTC)
    assert classification_calls[0][2] is True
    assert sidecar.read_latest_terminal_finished_at(target) is not None


def test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop(tmp_path: Path):
    target = date(2026, 9, 1)
    contract_fixture = _contract(trade_date=target)
    mapping, evidence, _snapshot = _bundle(contract_fixture)
    authority = _authority(mapping, evidence)
    provider_calls = []

    class ReviewedAuthority:
        def preflight_source_version_digest(self, *, snapshot):
            refs = SourceRefsV1(
                calendar_generation_id="0" * 32,
                calendar_sha256="0" * 64,
                classification_generation_id="cls-1",
                classification_generation_sequence=1,
                classification_source="fixture",
                classification_source_version="v1",
                classification_source_snapshot_date=date(2026, 8, 31),
                classification_observed_at=datetime(2026, 9, 1, 8, tzinfo=UTC),
                classification_snapshot_sha256=classification_snapshot_sha256(evidence),
                required_symbol_snapshot_id="0" * 32,
                required_symbol_snapshot_sha256="0" * 64,
                instrument_evidence_ids=tuple(item.evidence_id for item in evidence),
                semantic_mapping_sha256=mapping.mapping_sha256,
                source_version_digest="0" * 64,
                provider_id="baostock",
                trade_date=target,
                exact_pit_cutoff=datetime(2026, 9, 1, 19, tzinfo=UTC),
                source_date_semantics="source_observed",
            )
            return source_version_digest(refs, evidence)

        def admit(self, snapshot, *, trade_date, refresh_id):
            return authority

    class Provider:
        max_attempts = 1

        def fetch(self, trade_date):
            provider_calls.append(trade_date)
            return ClassificationSnapshot(
                source="baostock",
                source_version="fixture",
                source_snapshot_date=trade_date,
                source_date_semantics="source_observed",
                observed_at=datetime(2026, 9, 1, 18, tzinfo=UTC),
            )

    def provider_factory(**kwargs):
        assert kwargs == {"max_attempts": 1}
        return Provider()

    sidecar = UniverseSidecarStore(tmp_path / "universe.sqlite3")
    service = UniverseMaintenanceService(
        sidecar=sidecar,
        calendar_store=SimpleNamespace(read_authority=lambda: _calendar_authority(target)),
        classification_store=SimpleNamespace(
            read_snapshot=lambda as_of, **kwargs: ClassificationReadSnapshot(
                ready_generation_id="cls-1",
                generation=SimpleNamespace(
                    generation_id="cls-1",
                    sequence=1,
                    source="fixture",
                    source_version="v1",
                    source_snapshot_date=date(2026, 8, 31),
                    source_date_semantics="source_observed",
                    observed_at=datetime(2026, 9, 1, 8, tzinfo=UTC),
                ),
                securities=[],
                security_snapshot_date=None,
                index_components=[],
                index_snapshot_date=None,
                sector_memberships=[],
                sector_snapshot_date=None,
            )
        ),
        user_store=SimpleNamespace(
            capture_required_symbol_snapshot_existing=lambda: _user_capture(
                contract_fixture.members
            )
        ),
        lock_path=tmp_path / "refresh.lock",
        provider_factory=provider_factory,
        context_validator=lambda value, **_kwargs: value,
        reviewed_authority=ReviewedAuthority(),
        clock=lambda: datetime(2026, 9, 1, 19, tzinfo=UTC),
    )
    context = _maintenance_context(target)
    first = service(
        trade_date=target.isoformat(),
        now="2026-09-01T19:00:00+00:00",
        context=context,
        sidecar=sidecar,
    )
    second = service(
        trade_date=target.isoformat(),
        now="2026-09-01T19:30:00+00:00",
        context=context,
        sidecar=sidecar,
    )
    assert first.status == "PROMOTED", first
    assert first.provider_requests == 1
    assert second.status == "DEFER"
    assert second.reason_code == "NONE"
    assert second.provider_requests == 0
    assert provider_calls == [target]
    head, promoted = sidecar.read_verified_snapshot()
    assert head.sequence == 1
    assert promoted.trade_date == target


def test_failed_acquisition_records_terminal_and_preserves_prior_head_and_canonical_artifacts(
    tmp_path: Path,
):
    """AC16: an acquisition failure is durable and cannot damage canonical state."""
    target = date(2026, 9, 1)
    contract_fixture = _contract(trade_date=target)
    mapping, evidence, _snapshot = _bundle(contract_fixture)
    authority = _authority(mapping, evidence)

    class ReviewedAuthority:
        def preflight_source_version_digest(self, *, snapshot):
            refs = SourceRefsV1(
                calendar_generation_id="0" * 32,
                calendar_sha256="0" * 64,
                classification_generation_id="cls-1",
                classification_generation_sequence=1,
                classification_source="fixture",
                classification_source_version="v1",
                classification_source_snapshot_date=date(2026, 8, 31),
                classification_observed_at=datetime(2026, 9, 1, 8, tzinfo=UTC),
                classification_snapshot_sha256=classification_snapshot_sha256(evidence),
                required_symbol_snapshot_id="0" * 32,
                required_symbol_snapshot_sha256="0" * 64,
                instrument_evidence_ids=tuple(item.evidence_id for item in evidence),
                semantic_mapping_sha256=mapping.mapping_sha256,
                source_version_digest="0" * 64,
                provider_id="baostock",
                trade_date=target,
                exact_pit_cutoff=datetime(2026, 9, 1, 19, tzinfo=UTC),
                source_date_semantics="source_observed",
            )
            return source_version_digest(refs, evidence)

        def admit(self, snapshot, *, trade_date, refresh_id):
            return authority

    class Provider:
        max_attempts = 1

        def fetch(self, trade_date):
            provider_fetches.append(trade_date)
            raise RuntimeError("synthetic acquisition failure")

    provider_factories = []
    provider_fetches = []

    def provider_factory(**kwargs):
        provider_factories.append(kwargs)
        return Provider()

    generation = SimpleNamespace(
        generation_id="cls-1",
        sequence=1,
        source="fixture",
        source_version="v1",
        source_snapshot_date=date(2026, 8, 31),
        source_date_semantics="source_observed",
        observed_at=datetime(2026, 9, 1, 8, tzinfo=UTC),
    )
    classification = ClassificationReadSnapshot(
        ready_generation_id="cls-1",
        generation=generation,
        securities=[],
        security_snapshot_date=None,
        index_components=[],
        index_snapshot_date=None,
        sector_memberships=[],
        sector_snapshot_date=None,
    )
    classification_store = SimpleNamespace(
        read_snapshot=lambda as_of, **kwargs: classification,
    )
    user_store = SimpleNamespace(
        capture_required_symbol_snapshot_existing=lambda: _user_capture(contract_fixture.members),
    )
    calendar_store = SimpleNamespace(read_authority=lambda: _calendar_authority(target))
    reviewed = ReviewedAuthority()
    sidecar = UniverseSidecarStore(tmp_path / "universe.sqlite3")

    # Establish an already-published prior head with the same strict service.
    class GoodProvider:
        max_attempts = 1

        def fetch(self, trade_date):
            return ClassificationSnapshot(
                source="baostock",
                source_version="fixture",
                source_snapshot_date=trade_date,
                source_date_semantics="source_observed",
                observed_at=datetime(2026, 9, 1, 18, tzinfo=UTC),
            )

    good = UniverseMaintenanceService(
        sidecar=sidecar,
        calendar_store=calendar_store,
        classification_store=classification_store,
        user_store=user_store,
        lock_path=tmp_path / "maintenance.lock",
        provider_factory=lambda **kwargs: GoodProvider(),
        context_validator=lambda value, **_kwargs: value,
        reviewed_authority=reviewed,
        clock=lambda: datetime(2026, 9, 1, 19, tzinfo=UTC),
    )
    first = good(
        trade_date=target.isoformat(),
        now="2026-09-01T19:00:00+00:00",
        context=_maintenance_context(target, "run-prior"),
        sidecar=sidecar,
    )
    assert first.status == "PROMOTED"
    prior_head, prior_contract = sidecar.read_verified_snapshot()

    canonical_root = tmp_path / "canonical"
    canonical_root.mkdir()
    artifacts = {
        canonical_root / "canonical.parquet": b"canonical bytes",
        canonical_root / "manifest.json": b"manifest bytes",
        canonical_root / "CURRENT": b"prior pointer\n",
    }
    for path, payload in artifacts.items():
        path.write_bytes(payload)
    before_artifacts = {path: (path.read_bytes(), path.stat().st_ino) for path in artifacts}

    failing = UniverseMaintenanceService(
        sidecar=sidecar,
        calendar_store=calendar_store,
        classification_store=classification_store,
        user_store=user_store,
        lock_path=tmp_path / "maintenance.lock",
        provider_factory=provider_factory,
        context_validator=lambda value, **_kwargs: value,
        reviewed_authority=reviewed,
        clock=lambda: datetime(2026, 9, 2, 19, tzinfo=UTC),
    )
    failed = failing(
        trade_date=target.isoformat(),
        now="2026-09-02T19:00:00+00:00",
        context=_maintenance_context(target, "run-failed"),
        sidecar=sidecar,
    )

    assert failed.status == "BLOCKED"
    assert failed.reason_code == "CLASSIFICATION_UNAVAILABLE"
    assert failed.provider_requests == 1
    assert provider_factories == [{"max_attempts": 1}]
    assert provider_fetches == [target]
    current_head, current_contract = sidecar.read_verified_snapshot()
    assert current_head == prior_head
    assert current_contract == prior_contract
    assert sidecar.read_latest_terminal_finished_at(target) == datetime(2026, 9, 2, 19, tzinfo=UTC)
    (
        _status_head,
        _status_contract,
        _head_source,
        _source_rows,
        attempts,
    ) = sidecar.read_status_snapshot(target)
    assert attempts[-1][1] is not None
    assert attempts[-1][1].terminal_status == "failed"
    assert attempts[-1][1].reason_code == "CLASSIFICATION_UNAVAILABLE"
    assert {path: (path.read_bytes(), path.stat().st_ino) for path in artifacts} == before_artifacts

    # The durable terminal closes the same slot on restart; it must not acquire again.
    second = failing(
        trade_date=target.isoformat(),
        now="2026-09-02T19:30:00+00:00",
        context=_maintenance_context(target, "run-failed"),
        sidecar=sidecar,
    )
    assert second.status == "DEFER"
    assert second.reason_code == "NONE"
    assert second.provider_requests == 0
    assert provider_fetches == [target]
