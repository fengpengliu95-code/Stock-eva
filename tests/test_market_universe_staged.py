from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from backend.app.config import Settings
from backend.app.market.automation import (
    CanonicalRefreshExecution,
    ConcreteUniversePostSuccessHook,
    MainBoardInspection,
    MarketAutomationService,
    UniverseHookResult,
    classify_universe_mode,
    observe_legacy_request_universe,
)
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore
from backend.app.market.universe import (
    UniverseAttemptPlanV1,
    UniverseSidecarStore,
    domain_sha256,
)
from tests.test_market_automation import synthetic_calendar

SHANGHAI = ZoneInfo("Asia/Shanghai")


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


def test_concrete_hook_uses_durable_terminal_interval_and_typed_runner():
    terminal = datetime(2026, 8, 19, 0, tzinfo=UTC)

    class Sidecar:
        def read_latest_terminal_finished_at(self, trade_date):
            return terminal

    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
        return UniverseHookResult(
            status="BLOCKED", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
        )

    hook = ConcreteUniversePostSuccessHook(
        environment="staging",
        mode="shadow",
        enabled=True,
        interval_seconds=60,
        sidecar=Sidecar(),
        maintenance_runner=runner,
    )
    result = hook.offer("2026-08-20", "2026-08-20T00:02:00+00:00", None)
    assert result.status == "BLOCKED"
    assert len(calls) == 1
    assert calls[0]["trade_date"] == "2026-08-20"


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


def _attempt_plan(*, operation_day: date = date(2026, 8, 20), attempt_id: str = "attempt-1"):
    created = datetime(2026, 8, 20, 0, tzinfo=UTC)
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
        "hook_kind": values["hook_kind"],
        "refresh_id": values["refresh_id"],
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": created.isoformat(),
        "attempt_status": "RUNNING",
    }
    values["dedup_key"] = domain_sha256(
        "stock-eva/r2f4.2/universe-attempt-dedup/v1", dedup_preimage
    )
    values["planned_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/universe-attempt-plan/v1", dedup_preimage
    )
    return UniverseAttemptPlanV1(**values)


def test_durable_attempt_claim_is_restart_safe_and_allows_first_empty_sidecar(tmp_path: Path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    plan = _attempt_plan()
    assert store.claim_attempt(plan).claimed is True
    assert store.claim_attempt(plan).claimed is False
