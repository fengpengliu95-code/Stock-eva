import asyncio
import hashlib
import importlib
import json
import os
import sqlite3
import stat
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

import duckdb
import httpx
import pytest
from pydantic import ValidationError

import backend.app.cli as cli
import backend.app.market.store as market_store_module
from backend.app.api.market import get_market_store
from backend.app.config import get_settings
from backend.app.main import app
from backend.app.market.automation import canonical_refresh_callback
from backend.app.market.baostock import BaoStockProvider, ProviderBatch
from backend.app.market.baostock_vendor import (
    emit_terminal_observation,
    transport_observation_sink,
)
from backend.app.market.continuity import ContinuityDecision, RepairExecutionResult
from backend.app.market.factor_cache import AdjustmentFactorCache
from backend.app.market.failures import MarketFailure, MarketFailureError
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.provider_health import (
    CircuitState,
    InMemoryProviderHealthStore,
    ProviderHealthError,
    SQLiteProviderHealthStore,
)
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    provider_session_scope,
    refresh_scope,
    request_scope,
)
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.store import MarketStore
from backend.app.orchestration.after_close import (
    AfterClosePipelineService,
    AfterCloseTaskStore,
    StrategyWorkItem,
)
from backend.app.regime.snapshots import RegimeSnapshotUnavailable
from tests.test_baostock_provider import FIXTURE_PATH, ContinueAfterFailureClient, FakeBaoStock
from tests.test_market_data import fixture_payload
from tests.test_market_provider_contract import _CompleteSdkClient

SHANGHAI = ZoneInfo("Asia/Shanghai")


def use_local_storage_settings(monkeypatch) -> None:
    settings = cli.get_settings().model_copy(update={"nas_market_dataset_root": None})
    monkeypatch.setattr(cli, "get_settings", lambda: settings)


def load_module(name: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"{name} is not implemented")


def assert_public_repair_payload_is_sanitized(value: object) -> None:
    forbidden = (
        "job_id",
        "request_key",
        "run_id",
        "provider_requests",
        "error_message",
        "path",
        "token",
        "url",
    )
    if isinstance(value, dict):
        for key, item in value.items():
            assert isinstance(key, str)
            assert not any(term in key for term in forbidden), key
            assert_public_repair_payload_is_sanitized(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            assert_public_repair_payload_is_sanitized(item)
        return
    if isinstance(value, str):
        assert not any(term in value for term in forbidden), value


def synthetic_calendar():
    module = load_module("backend.app.market.calendar")
    return module.TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-22",
            "sources": [
                {
                    "exchange": "SSE",
                    "title": "synthetic official closure notice",
                    "url": "https://example.test/sse-2026",
                }
            ],
            "closed_dates": ["2026-01-01", "2026-02-16", "2026-02-17"],
        }
    )


def fixture_bars(trade_date: date = date(2026, 7, 23)):
    payload = fixture_payload()
    bars = normalize_baostock_rows(
        fields=payload["daily_fields"],
        rows=payload["daily_rows"],
        factor_fields=payload["factor_fields"],
        factor_rows=payload["factor_rows"],
        ingested_at=datetime(2026, 7, 23, 10, tzinfo=UTC),
    )
    return [
        item.model_copy(
            update={
                "trade_date": trade_date,
                "source_record_id": f"fixture:{item.symbol}:{trade_date}",
            }
        )
        for item in bars
    ]


def test_bundled_2026_calendar_uses_confirmed_exchange_closures() -> None:
    module = load_module("backend.app.market.calendar")
    calendar = module.get_trading_calendar()

    assert calendar.session_status(date(2026, 1, 5)) == "open"
    assert calendar.session_status(date(2026, 2, 16)) == "closed"
    assert calendar.session_status(date(2026, 6, 19)) == "closed"
    assert calendar.session_status(date(2026, 9, 25)) == "closed"
    assert calendar.session_status(date(2026, 10, 7)) == "closed"
    assert calendar.session_status(date(2026, 10, 8)) == "open"
    assert calendar.session_status(date(2027, 1, 4)) == "unknown"


def test_calendar_fails_closed_outside_confirmed_year() -> None:
    calendar = synthetic_calendar()

    assert calendar.session_status(date(2026, 7, 24)) == "open"
    assert calendar.session_status(date(2026, 2, 16)) == "closed"
    assert calendar.session_status(date(2026, 7, 25)) == "closed"
    assert calendar.session_status(date(2027, 1, 4)) == "unknown"


def test_latest_expected_session_waits_until_1810_and_respects_holiday() -> None:
    calendar = synthetic_calendar()

    assert calendar.latest_expected_session(datetime(2026, 7, 24, 18, 9, tzinfo=SHANGHAI)) == date(
        2026, 7, 23
    )
    assert calendar.latest_expected_session(datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI)) == date(
        2026, 7, 24
    )
    assert calendar.latest_expected_session(datetime(2026, 2, 17, 20, 0, tzinfo=SHANGHAI)) == date(
        2026, 2, 13
    )


def test_market_phase_is_deterministic_for_open_and_closed_sessions() -> None:
    calendar = synthetic_calendar()

    assert calendar.market_phase(datetime(2026, 7, 24, 8, tzinfo=SHANGHAI)) == "pre_market"
    assert calendar.market_phase(datetime(2026, 7, 24, 10, tzinfo=SHANGHAI)) == "market_open"
    assert (
        calendar.market_phase(datetime(2026, 7, 24, 16, tzinfo=SHANGHAI)) == "after_close_waiting"
    )
    assert calendar.market_phase(datetime(2026, 7, 25, 10, tzinfo=SHANGHAI)) == "closed"
    assert (
        calendar.market_phase(datetime(2027, 1, 4, 10, tzinfo=SHANGHAI)) == "calendar_unavailable"
    )


def test_schedule_policy_runs_catch_up_and_uses_finite_retry_slots() -> None:
    module = load_module("backend.app.market.automation")
    calendar = synthetic_calendar()
    policy = module.SchedulePolicy(calendar)

    first = policy.decide(
        datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI),
        published_as_of=date(2026, 7, 23),
        state=None,
    )
    assert first.action == "run"
    assert first.target_session == date(2026, 7, 24)

    failed = module.SchedulerState(
        target_session=date(2026, 7, 24),
        refresh_state="retry_wait",
        attempt_count=1,
        last_attempt_at=datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI),
        next_retry_at=datetime(2026, 7, 24, 18, 40, tzinfo=SHANGHAI),
    )
    waiting = policy.decide(
        datetime(2026, 7, 24, 18, 30, tzinfo=SHANGHAI),
        published_as_of=date(2026, 7, 23),
        state=failed,
    )
    catch_up = policy.decide(
        datetime(2026, 7, 24, 19, 5, tzinfo=SHANGHAI),
        published_as_of=date(2026, 7, 23),
        state=failed,
    )
    assert waiting.action == "wait"
    assert waiting.next_run_at == datetime(2026, 7, 24, 18, 40, tzinfo=SHANGHAI)
    assert catch_up.action == "run"

    assert policy.next_retry_after(
        date(2026, 7, 24),
        datetime(2026, 7, 24, 21, 0, tzinfo=SHANGHAI),
    ) == datetime(2026, 7, 25, 7, 15, tzinfo=SHANGHAI)
    assert (
        policy.next_retry_after(
            date(2026, 7, 24),
            datetime(2026, 7, 25, 7, 15, tzinfo=SHANGHAI),
        )
        is None
    )


class CompleteProvider:
    def __init__(self, bars) -> None:
        self.bars = bars
        self.fetch_calls = 0
        self.calendar_calls = 0

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        self.calendar_calls += 1
        return [start_date]

    def fetch(self, trade_date: date, symbols=None) -> ProviderBatch:
        self.fetch_calls += 1
        return ProviderBatch(
            bars=self.bars,
            expected_symbols=[item.symbol for item in self.bars],
            failed_symbols=[],
        )


def test_automatic_due_refresh_uses_canonical_callback_and_never_legacy_fetch(tmp_path: Path):
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    calls = []

    def canonical(**kwargs):
        calls.append(kwargs)
        target = kwargs["trade_date"]
        return RefreshResult(
            run_id=kwargs["run_id"],
            request_key=kwargs["request_key"],
            run_kind="daily",
            requested_date=target,
            source="baostock",
            status="ready",
            requested_count=1,
            succeeded_count=1,
            coverage_ratio=1,
            started_at=datetime(2026, 7, 23, 10, tzinfo=UTC),
            completed_at=datetime(2026, 7, 23, 10, 1, tzinfo=UTC),
        )

    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        canonical_refresh=canonical,
    )
    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    assert outcome.result is not None and outcome.result.status == "ready"
    assert len(calls) == 1
    assert provider.fetch_calls == 0


def test_real_service_due_route_uses_one_raw_fetch_and_one_normalize(tmp_path: Path):
    target = date(2026, 7, 23)
    client = _CompleteSdkClient(response_date=target.isoformat())
    factor_cache = AdjustmentFactorCache(":memory:")
    factor_cache.record_bootstrap(
        "sh.600000",
        target,
        factor_cache.FACTOR_FIELDS,
        [["sh.600000", target.isoformat(), "1", "1", "1"]],
    )
    provider = BaoStockProvider(
        client=client,
        max_attempts=1,
        min_request_interval_seconds=0,
        factor_cache=factor_cache,
    )
    legacy_fetch_calls = 0

    def legacy_fetch(*args, **kwargs):
        nonlocal legacy_fetch_calls
        legacy_fetch_calls += 1
        raise AssertionError("automatic route must not call legacy fetch")

    provider.fetch = legacy_fetch
    adapter = BaoStockProviderAdapter(
        client=client,
        max_attempts=1,
        min_request_interval_seconds=0,
        factor_cache=factor_cache,
    )
    raw_calls = 0
    normalize_calls = 0
    original_fetch_raw = adapter.fetch_raw
    original_normalize = adapter.normalize

    def counted_fetch_raw(*args, **kwargs):
        nonlocal raw_calls
        raw_calls += 1
        return original_fetch_raw(*args, **kwargs)

    def counted_normalize(*args, **kwargs):
        nonlocal normalize_calls
        normalize_calls += 1
        return original_normalize(*args, **kwargs)

    adapter.fetch_raw = counted_fetch_raw
    adapter.normalize = counted_normalize
    store = MarketStore(tmp_path / "market.duckdb")
    callback = canonical_refresh_callback(
        store,
        adapter,
        evidence_root=tmp_path / "evidence",
        lock_path=tmp_path / "locks" / "refresh.lock",
        factor_cache=factor_cache,
    )
    service = load_module("backend.app.market.automation").MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        lock_path=tmp_path / "locks" / "refresh.lock",
        health_store=InMemoryProviderHealthStore(),
        canonical_refresh=callback,
    )
    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    assert outcome.result is not None and outcome.result.status == "ready"
    assert raw_calls == normalize_calls == 1
    assert legacy_fetch_calls == 0


def test_automation_optional_continuity_dependency_preserves_legacy_behavior_exactly(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI)
    baseline_store = MarketStore(tmp_path / "baseline.duckdb")
    explicit_store = MarketStore(tmp_path / "explicit-none.duckdb")
    save_published(baseline_store, date(2026, 7, 24))
    save_published(explicit_store, date(2026, 7, 24))
    baseline_provider = CompleteProvider(fixture_bars())
    explicit_provider = CompleteProvider(fixture_bars())
    baseline = module.MarketAutomationService(
        baseline_store,
        baseline_provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )
    explicit = module.MarketAutomationService(
        explicit_store,
        explicit_provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        continuity=None,
        repair_enabled=False,
    )

    baseline_outcome = baseline.run_due_once(now)
    explicit_outcome = explicit.run_due_once(now)

    assert explicit_outcome == baseline_outcome
    assert explicit_provider.fetch_calls == baseline_provider.fetch_calls == 0
    assert explicit_provider.calendar_calls == baseline_provider.calendar_calls == 0
    assert explicit_store.repair_queue_snapshot().status == "unavailable"


def test_automation_optional_continuity_hook_is_planning_only_and_never_consumes_lease(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 24))
    provider = CompleteProvider(fixture_bars())

    class PlanningOnlyContinuity:
        def __init__(self) -> None:
            self.calls = []

        def claim_ready_once(self, **kwargs):
            self.calls.append(kwargs)
            assert kwargs["lease_consumer"] is None
            return {"reason_code": "REPAIR_READY"}

    continuity = PlanningOnlyContinuity()
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        continuity=continuity,
        repair_enabled=True,
    )

    outcome = service.run_due_once(datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI))

    assert outcome.decision.action == "none"
    assert len(continuity.calls) == 1
    assert service.last_continuity_decision == {"reason_code": "REPAIR_READY"}
    assert provider.fetch_calls == provider.calendar_calls == 0
    assert store.repair_queue_snapshot().status == "unavailable"


def test_automation_executes_one_repair_result_without_entering_freshness_lane(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 24))
    provider = CompleteProvider(fixture_bars())
    target = date(2026, 7, 24)
    refresh = RefreshResult(
        run_id="repair-2026-07-24-attempt-1",
        request_key="repair:baostock:2026-07-24:all-main-board",
        run_kind="repair",
        requested_date=target,
        source="baostock",
        status="ready",
        requested_count=1,
        succeeded_count=1,
        coverage_ratio=1,
        started_at=datetime(2026, 7, 24, 10, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )
    continuity = object()
    executor = type(
        "Executor",
        (),
        {
            "execute_once": lambda self, **kwargs: RepairExecutionResult(
                status="published",
                decision=ContinuityDecision(
                    action="run",
                    lane="repair",
                    target_session=target,
                    repair_job_id="repair:2026-07-24:all-main-board",
                    reason_code="REPAIR_CLAIMED",
                ),
                refresh_result=refresh,
                reason_code="REPAIR_PUBLISHED",
                provider_requests=1,
            )
        },
    )()
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        continuity=continuity,
        repair_enabled=True,
        repair_executor=executor,
    )

    outcome = service.run_due_once(datetime(2026, 7, 24, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is None
    assert outcome.continuity_result is not None
    assert outcome.continuity_result.refresh_result == refresh
    assert outcome.state.refresh_state == "success"
    assert provider.fetch_calls == provider.calendar_calls == 0


@pytest.mark.parametrize("repair_kind", ["success", "partial", "error", "consumer"])
def test_repair_outcome_is_continuity_only_and_preserves_freshness_retry_state(
    tmp_path: Path,
    repair_kind: str,
) -> None:
    module = load_module("backend.app.market.automation")
    target = date(2026, 7, 24)
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 23))
    before_state = module.SchedulerState(
        target_session=target,
        refresh_state="retry_wait",
        attempt_count=2,
        last_attempt_at=datetime(2026, 7, 24, 18, tzinfo=SHANGHAI),
        last_success_at=datetime(2026, 7, 23, 18, 20, tzinfo=SHANGHAI),
        next_retry_at=datetime(2026, 7, 24, 18, 40, tzinfo=SHANGHAI),
        calendar_status="confirmed",
        error_code="transport_timeout",
    )
    store.save_scheduler_state(before_state)
    provider = CompleteProvider(fixture_bars(target))
    decision = ContinuityDecision(
        action="run",
        lane="repair",
        target_session=date(2026, 7, 23),
        repair_job_id="repair:2026-07-23:all-main-board",
        reason_code="REPAIR_CLAIMED",
    )
    repair_refresh = RefreshResult(
        run_id="repair-2026-07-23-attempt-1",
        request_key="repair:baostock:2026-07-23:all-main-board",
        run_kind="repair",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="ready"
        if repair_kind == "success"
        else ("partial" if repair_kind == "partial" else "error"),
        requested_count=1,
        succeeded_count=1 if repair_kind == "success" else 0,
        coverage_ratio=1 if repair_kind == "success" else 0,
        failed_symbols=[] if repair_kind == "success" else ["sh.600000"],
        quality_issues=[] if repair_kind == "success" else ["provider_error"],
        error_message=None if repair_kind == "success" else "provider request failed",
        failure_stage=None if repair_kind == "success" else "fetch",
        failure_class=None if repair_kind == "success" else "transport_timeout",
        retryable=None if repair_kind == "success" else True,
        started_at=datetime(2026, 7, 24, 10, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )

    class FakeRepairExecutor:
        def execute_once(self, **kwargs):
            if kwargs["freshness"].action == "run":
                return RepairExecutionResult(
                    status="skipped",
                    decision=ContinuityDecision(
                        action="run",
                        lane="freshness",
                        target_session=target,
                        reason_code="FRESHNESS_DUE",
                    ),
                    reason_code="FRESHNESS_DUE",
                    provider_requests=0,
                )
            if repair_kind == "consumer":
                return RepairExecutionResult(
                    status="failed",
                    decision=decision,
                    reason_code="REPAIR_CONSUMER_FAILED",
                    provider_requests=1,
                )
            return RepairExecutionResult(
                status="published" if repair_kind == "success" else "failed",
                decision=decision,
                refresh_result=repair_refresh,
                reason_code=(
                    "REPAIR_PUBLISHED" if repair_kind == "success" else "REPAIR_RESULT_FAILED"
                ),
                provider_requests=1,
            )

    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        continuity=object(),
        repair_enabled=True,
        repair_executor=FakeRepairExecutor(),
    )

    first = service.run_due_once(datetime(2026, 7, 24, 18, 20, tzinfo=SHANGHAI))

    assert store.scheduler_state() == before_state
    assert first.state == before_state
    assert first.continuity_result is not None
    assert first.continuity_result.provider_requests == 1
    assert provider.fetch_calls == provider.calendar_calls == 0

    # The old retry slot remains due and is allowed to run freshness on the next invocation.
    second = service.run_due_once(datetime(2026, 7, 24, 18, 40, tzinfo=SHANGHAI))
    assert second.decision.action == "run"
    assert second.state.refresh_state == "success"
    assert provider.fetch_calls == 1


def _typed_repair_decision(
    *,
    target: date,
    action: str = "run",
    lane: str = "repair",
    reason_code: str = "REPAIR_CLAIMED",
    next_run_at: datetime | None = None,
) -> ContinuityDecision:
    return ContinuityDecision(
        action=action,
        lane=lane,
        target_session=target,
        repair_job_id=(f"repair:{target.isoformat()}:all-main-board" if lane == "repair" else None),
        next_run_at=next_run_at,
        reason_code=reason_code,
    )


def _typed_repair_refresh(
    *,
    target: date,
    status: str = "ready",
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> RefreshResult:
    started_at = started_at or datetime(2026, 8, 20, 10, tzinfo=UTC)
    completed_at = completed_at or datetime(2026, 8, 20, 10, 1, tzinfo=UTC)
    ready = status == "ready"
    return RefreshResult(
        run_id=f"repair-{target.isoformat()}-attempt-1",
        request_key=f"repair:baostock:{target.isoformat()}:all-main-board",
        run_kind="repair",
        requested_date=target,
        source="baostock",
        status=status,
        requested_count=1,
        succeeded_count=1 if ready else 0,
        coverage_ratio=1 if ready else 0,
        failed_symbols=[] if ready else ["sh.600000"],
        quality_issues=[] if ready else ["provider_error"],
        error_message=None if ready else "market refresh failed",
        failure_stage=None if ready else "fetch",
        failure_class=None if ready else "transport_timeout",
        retryable=None if ready else True,
        started_at=started_at,
        completed_at=completed_at,
    )


def test_automation_outcome_is_typed_and_legacy_dump_omits_only_new_null_field() -> None:
    module = load_module("backend.app.market.automation")
    decision = module.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = module.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    legacy = module.AutomationOutcome(decision=decision, state=state)

    assert legacy.model_dump(mode="json") == {
        "decision": decision.model_dump(mode="json"),
        "state": state.model_dump(mode="json"),
        "result": None,
    }
    assert "continuity_result" not in legacy.model_dump_json()
    with pytest.raises(ValidationError):
        module.AutomationOutcome(
            decision=decision,
            state=state,
            continuity_result={"status": "published"},
        )


def test_automation_outcome_repair_is_not_a_freshness_result() -> None:
    automation = load_module("backend.app.market.automation")
    target = date(2026, 8, 19)
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    repair = RepairExecutionResult(
        status="published",
        decision=_typed_repair_decision(target=target),
        refresh_result=_typed_repair_refresh(target=target),
        reason_code="REPAIR_PUBLISHED",
        provider_requests=1,
    )
    outcome = automation.AutomationOutcome(
        decision=decision,
        state=state,
        continuity_result=repair,
    )

    assert outcome.result is None
    assert outcome.continuity_result == repair
    assert outcome.model_dump(mode="json")["continuity_result"]["status"] == "published"


@pytest.mark.parametrize(
    "updates",
    [
        {"provider_requests": True},
        {
            "status": "published",
            "refresh_result": _typed_repair_refresh(target=date(2026, 8, 19), status="partial"),
        },
        {"status": "failed", "refresh_result": _typed_repair_refresh(target=date(2026, 8, 19))},
        {"status": "skipped", "provider_requests": 1},
        {
            "status": "skipped",
            "refresh_result": _typed_repair_refresh(target=date(2026, 8, 19), status="partial"),
        },
        {
            "status": "failed",
            "decision": _typed_repair_decision(
                target=date(2026, 8, 19),
                lane="freshness",
                reason_code="FRESHNESS_WAIT",
                action="wait",
                next_run_at=datetime(2026, 8, 20, 11, tzinfo=UTC),
            ),
        },
        {
            "status": "published",
            "refresh_result": _typed_repair_refresh(target=date(2026, 8, 19)).model_copy(
                update={"request_key": "repair:baostock:2026-08-20:all-main-board"}
            ),
        },
        {
            "status": "published",
            "refresh_result": _typed_repair_refresh(
                target=date(2026, 8, 19), completed_at=datetime(2026, 8, 20, 9, tzinfo=UTC)
            ),
        },
    ],
    ids=[
        "bool-provider-request",
        "published-partial",
        "failed-ready",
        "skipped-provider-request",
        "skipped-refresh-result",
        "failed-freshness-decision",
        "request-key-mismatch",
        "refresh-time-order",
    ],
)
def test_repair_execution_result_rejects_illegal_status_matrix(updates: dict[str, object]) -> None:
    target = date(2026, 8, 19)
    payload: dict[str, object] = {
        "status": "published",
        "decision": _typed_repair_decision(target=target),
        "refresh_result": _typed_repair_refresh(target=target),
        "reason_code": "REPAIR_PUBLISHED",
        "provider_requests": 1,
    }
    payload.update(updates)
    with pytest.raises(ValidationError):
        RepairExecutionResult.model_validate(payload)


def test_repair_execution_result_roundtrip_revalidates_model_copy_contradiction() -> None:
    target = date(2026, 8, 19)
    valid = RepairExecutionResult(
        status="published",
        decision=_typed_repair_decision(target=target),
        refresh_result=_typed_repair_refresh(target=target),
        reason_code="REPAIR_PUBLISHED",
        provider_requests=1,
    )
    bypassed = valid.model_copy(update={"status": "skipped", "provider_requests": 0})
    with pytest.raises(ValidationError):
        RepairExecutionResult.model_validate(bypassed.model_dump(mode="python"))


def test_consumer_failure_with_refresh_evidence_requires_one_provider_request() -> None:
    target = date(2026, 8, 19)
    decision = _typed_repair_decision(
        target=target,
        action="run",
        reason_code="REPAIR_CLAIMED",
    )
    failed_refresh = _typed_repair_refresh(target=target, status="error")
    with pytest.raises(ValidationError):
        RepairExecutionResult(
            status="failed",
            decision=decision,
            refresh_result=failed_refresh,
            reason_code="REPAIR_CONSUMER_FAILED",
            provider_requests=0,
        )
    valid_without_result = RepairExecutionResult(
        status="failed",
        decision=decision,
        reason_code="REPAIR_CONSUMER_FAILED",
        provider_requests=0,
    )
    assert valid_without_result.refresh_result is None
    assert valid_without_result.provider_requests == 0


def test_consumer_failure_model_copy_roundtrip_cannot_zero_evidence_count() -> None:
    target = date(2026, 8, 19)
    valid = RepairExecutionResult(
        status="failed",
        decision=_typed_repair_decision(target=target),
        refresh_result=_typed_repair_refresh(target=target, status="error"),
        reason_code="REPAIR_CONSUMER_FAILED",
        provider_requests=1,
    )
    bypassed = valid.model_copy(update={"provider_requests": 0})
    with pytest.raises(ValidationError):
        RepairExecutionResult.model_validate(bypassed.model_dump(mode="python"))


def test_cli_automation_payload_is_typed_and_preserves_legacy_shape() -> None:
    automation = load_module("backend.app.market.automation")
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    legacy = automation.AutomationOutcome(decision=decision, state=state)
    payload = cli._automation_outcome_payload(legacy)
    assert set(payload) == {"decision", "state", "result"}
    assert payload["result"] is None
    repair = RepairExecutionResult(
        status="published",
        decision=_typed_repair_decision(target=date(2026, 8, 19)),
        refresh_result=_typed_repair_refresh(target=date(2026, 8, 19)),
        reason_code="REPAIR_PUBLISHED",
        provider_requests=1,
    )
    repaired = automation.AutomationOutcome(
        decision=decision,
        state=state,
        continuity_result=repair,
    )
    repair_payload = cli._automation_outcome_payload(repaired)
    assert set(repair_payload) == {"decision", "state", "result", "continuity_result"}
    assert repair_payload["result"] is None
    assert repair_payload["continuity_result"]["status"] == "published"
    assert set(repair_payload["continuity_result"]) == {
        "status",
        "decision",
        "refresh_result",
        "reason_code",
    }
    assert set(repair_payload["continuity_result"]["decision"]) == {
        "action",
        "lane",
        "target_session",
        "next_run_at",
        "reason_code",
    }
    assert set(repair_payload["continuity_result"]["refresh_result"]) == {
        "status",
        "requested_date",
        "run_kind",
        "requested_count",
        "succeeded_count",
        "coverage_ratio",
        "failure_stage",
        "failure_class",
        "retryable",
    }
    serialized = json.dumps(repair_payload)
    assert "path" not in serialized
    assert "token" not in serialized
    assert "url" not in serialized
    assert "provider_requests" not in serialized
    assert "repair_job_id" not in serialized
    assert "run_id" not in serialized
    assert "request_key" not in serialized
    assert "error_message" not in serialized
    assert_public_repair_payload_is_sanitized(repair_payload["continuity_result"])


def test_cli_repair_payload_is_an_explicit_allowlist_and_excludes_raw_failure_text() -> None:
    automation = load_module("backend.app.market.automation")
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    target = date(2026, 8, 19)
    refresh = _typed_repair_refresh(target=target, status="error").model_copy(
        update={
            "error_message": "raw token=secret /private/path https://provider.invalid/private",
            "run_id": "internal/path/token",
        }
    )
    repair = RepairExecutionResult(
        status="failed",
        decision=_typed_repair_decision(target=target),
        refresh_result=refresh,
        reason_code="REPAIR_RESULT_FAILED",
        provider_requests=1,
    )
    payload = cli._automation_outcome_payload(
        automation.AutomationOutcome(
            decision=decision,
            state=state,
            continuity_result=repair,
        )
    )
    serialized = json.dumps(payload)
    repair_refresh = payload["continuity_result"]["refresh_result"]
    assert set(repair_refresh) == {
        "status",
        "requested_date",
        "run_kind",
        "requested_count",
        "succeeded_count",
        "coverage_ratio",
        "failure_stage",
        "failure_class",
        "retryable",
    }
    assert "error_message" not in serialized
    assert "raw token=secret" not in serialized
    assert "/private/path" not in serialized
    assert "provider.invalid" not in serialized
    assert_public_repair_payload_is_sanitized(payload["continuity_result"])


def test_cli_automation_payload_revalidates_outer_model_copy_before_reading_fields() -> None:
    automation = load_module("backend.app.market.automation")
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    target = date(2026, 8, 19)
    refresh = _typed_repair_refresh(target=target, status="error").model_copy(
        update={
            "error_message": "raw token=secret /private/path https://provider.invalid/private",
            "run_id": "run-id-secret",
        }
    )
    base_repair = RepairExecutionResult(
        status="failed",
        decision=_typed_repair_decision(target=target),
        refresh_result=_typed_repair_refresh(target=target, status="error"),
        reason_code="REPAIR_RESULT_FAILED",
        provider_requests=1,
    )
    bypassed_refresh = refresh.model_copy(update={"request_key": "request-key-secret"})
    bypassed_repair = base_repair.model_copy(update={"refresh_result": bypassed_refresh})
    bypassed_repair = bypassed_repair.model_copy(
        update={"status": "skipped", "provider_requests": 0}
    )
    bypassed_outcome = automation.AutomationOutcome(
        decision=decision,
        state=state,
        continuity_result=base_repair,
    ).model_copy(
        update={
            "result": {
                "run_id": "run-id-secret",
                "error_message": "raw token=secret /private/path https://provider.invalid/private",
                "run_kind": "repair",
            },
            "continuity_result": bypassed_repair,
        }
    )

    with pytest.raises(ValidationError) as caught:
        cli._automation_outcome_payload(bypassed_outcome)

    error_text = str(caught.value)
    assert "raw token=secret" not in error_text
    assert "/private/path" not in error_text
    assert "provider.invalid" not in error_text
    assert "run-id-secret" not in error_text
    assert "request-key-secret" not in error_text


def test_cli_automation_payload_roundtrip_keeps_legal_legacy_result_shape() -> None:
    automation = load_module("backend.app.market.automation")
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    result = _typed_repair_refresh(target=date(2026, 8, 19), status="error").model_copy(
        update={"run_kind": "daily", "request_key": None}
    )
    outcome = automation.AutomationOutcome(decision=decision, state=state, result=result)

    payload = cli._automation_outcome_payload(outcome)

    assert set(payload) == {"decision", "state", "result"}
    assert payload["result"] == result.model_dump(mode="json")


def test_auto_refresh_cli_invalid_outcome_is_a_stable_error_boundary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """A bypassed automation result must not escape the CLI as a traceback."""
    automation = load_module("backend.app.market.automation")
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    decision = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    state = automation.SchedulerState(
        target_session=date(2026, 8, 20),
        refresh_state="success",
    )
    invalid_outcome = automation.AutomationOutcome.model_construct(
        decision=decision,
        state=state,
        result={
            "run_id": "private/run-id",
            "error_message": "token=private https://provider.invalid/private",
            "run_kind": "repair",
        },
        continuity_result=None,
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        cli.MarketAutomationService,
        "run_due_once",
        lambda *_args, **_kwargs: invalid_outcome,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "auto-refresh-once", "--execute"],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload == {
        "status": "error",
        "reason_code": "AUTOMATION_RESULT_UNAVAILABLE",
        "writes_market_data": False,
    }
    serialized = captured.out + captured.err
    assert "private/run-id" not in serialized
    assert "token=private" not in serialized
    assert "provider.invalid" not in serialized


class HealthClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def open_health_endpoint(health_store, endpoint: ProviderEndpoint, *, prefix: str) -> None:
    for index in range(health_store.failure_threshold):
        health_store.record_terminal_failure(
            f"{prefix}-{index}",
            endpoint,
            NormalizedTransportError.RECV_TIMEOUT,
        )


class RecordingProbeRunner:
    def __init__(
        self,
        *,
        error: NormalizedTransportError | None = None,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.error = error
        self.entered = entered
        self.release = release
        self.calls: list[tuple[ProviderEndpoint, date, str]] = []

    def run(self, endpoint: ProviderEndpoint, *, trade_date: date, refresh_id: str) -> None:
        self.calls.append((endpoint, trade_date, refresh_id))
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            assert self.release.wait(timeout=2)
        if self.error is not None:
            failure = RuntimeError("sanitized provider probe failure")
            failure.normalized_error = self.error
            raise failure
        with refresh_scope(refresh_id), provider_session_scope("probe-session"):
            with request_scope(endpoint, attempt=1, request_id="probe-request"):
                emit_terminal_observation(
                    started_at=monotonic(),
                    protocol_stage=ProtocolStage.OPERATION,
                    recv_calls=1,
                    response_bytes=32,
                    end_marker_seen=True,
                    provider_code="0",
                    normalized_error=None,
                )


class ScopedObservationProvider(CompleteProvider):
    def __init__(
        self,
        bars,
        *,
        fetch_error: NormalizedTransportError | None = None,
    ) -> None:
        super().__init__(bars)
        self.fetch_error = fetch_error

    @contextmanager
    def refresh_operation(self, refresh_id: str):
        with refresh_scope(refresh_id):
            yield

    @staticmethod
    def _observe(
        endpoint: ProviderEndpoint,
        *,
        request_id: str,
        provider_session_id: str | None = None,
        attempt: int = 1,
        error: NormalizedTransportError | None = None,
        stage: ProtocolStage = ProtocolStage.OPERATION,
    ) -> None:
        with provider_session_scope(provider_session_id or f"session-{request_id}"):
            with request_scope(endpoint, attempt=attempt, request_id=request_id):
                emit_terminal_observation(
                    started_at=monotonic(),
                    protocol_stage=stage,
                    recv_calls=1,
                    response_bytes=32,
                    end_marker_seen=error is None,
                    provider_code="0" if error is None else None,
                    normalized_error=error,
                )

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        self.calendar_calls += 1
        self._observe(ProviderEndpoint.TRADE_DATES, request_id="calendar")
        return [start_date]

    def fetch(self, trade_date: date, symbols=None) -> ProviderBatch:
        self.fetch_calls += 1
        self._observe(ProviderEndpoint.ALL_STOCK, request_id="fetch-connect")
        if self.fetch_error is not None:
            self._observe(
                ProviderEndpoint.ALL_STOCK,
                request_id="fetch-failure",
                error=self.fetch_error,
                stage=ProtocolStage.OPERATION,
            )
            raise MarketFailureError(
                MarketFailure(
                    failure_stage="fetch",
                    failure_class="transport_connect",
                    retryable=True,
                )
            )
        return ProviderBatch(
            bars=self.bars,
            expected_symbols=[item.symbol for item in self.bars],
            failed_symbols=[],
        )


def save_published(store: MarketStore, trade_date: date) -> None:
    bars = fixture_bars(trade_date)
    store.save_refresh(
        bars,
        RefreshResult(
            run_id=f"published-{trade_date}",
            requested_date=trade_date,
            source="baostock",
            status="ready",
            requested_count=len(bars),
            succeeded_count=len(bars),
            coverage_ratio=1,
            started_at=datetime(2026, 7, 23, 10, tzinfo=UTC),
            completed_at=datetime(2026, 7, 23, 10, 1, tzinfo=UTC),
        ),
        publish=True,
    )


def test_publication_gates_keep_last_complete_snapshot_on_partial_attempt(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 23))
    incomplete = [bar for bar in fixture_bars(date(2026, 7, 24)) if bar.symbol != "sz.399001"]
    provider = CompleteProvider(incomplete)

    result = module.run_publication_refresh(
        store,
        provider,
        trade_date=date(2026, 7, 24),
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert "required_index_missing:sz.399001" in result.quality_issues
    assert store.published_refresh().requested_date == date(2026, 7, 23)
    assert store.latest_refresh().requested_date == date(2026, 7, 24)


def test_partial_retry_cannot_mutate_published_bars_for_same_session(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    session = date(2026, 7, 23)
    first = module.run_publication_refresh(
        store,
        CompleteProvider(fixture_bars(session)),
        trade_date=session,
        required_symbols={"sh.600000"},
    )
    assert first.status == "ready"
    published_run_id = store.published_refresh().run_id
    original = {bar["symbol"]: bar["close"] for bar in store.bars_for(session, "baostock")}
    incomplete = [
        bar.model_copy(update={"close": bar.close + 999})
        for bar in fixture_bars(session)
        if bar.symbol != "sz.399001"
    ]

    result = module.run_publication_refresh(
        store,
        CompleteProvider(incomplete),
        trade_date=session,
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert {bar["symbol"]: bar["close"] for bar in store.bars_for(session, "baostock")} == original
    assert store.published_refresh().run_id == published_run_id
    assert store.published_refresh().status == "ready"


def test_active_partial_bar_cannot_produce_ready_publication_or_move_pointer(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    session = date(2026, 7, 23)
    save_published(store, session)
    pointer_before = store.published_refresh()
    poisoned = [
        bar.model_copy(
            update={
                "quality_status": "partial",
                "quality_issues": ["fixture_active_partial"],
            }
        )
        if bar.symbol == "sh.600000"
        else bar
        for bar in fixture_bars(session)
    ]

    result = module.run_publication_refresh(
        store,
        CompleteProvider(poisoned),
        trade_date=session,
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert "invalid_publication_bar_quality:sh.600000:active_bar_not_ready" in (
        result.quality_issues
    )
    assert store.published_refresh().run_id == pointer_before.run_id


def test_complete_publication_requires_user_symbols_and_factor_completeness(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())

    missing_user = module.run_publication_refresh(
        store,
        provider,
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000", "sz.000999"},
    )
    assert missing_user.status == "partial"
    assert "required_symbol_missing:sz.000999" in missing_user.quality_issues
    assert store.published_refresh() is None

    ready = module.run_publication_refresh(
        store,
        provider,
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000"},
    )
    assert ready.status == "ready"
    assert store.published_refresh().run_id == ready.run_id


def test_automation_machine_validates_calendar_before_fetch(tmp_path: Path) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    provider.trading_dates = lambda start, end: []
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.state.calendar_status == "conflict"
    assert outcome.state.refresh_state == "error"
    assert provider.fetch_calls == 0
    assert store.published_refresh() is None


def test_automation_publishes_once_and_does_not_repeat_success(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)

    first = service.run_due_once(now)
    second = service.run_due_once(now)

    assert first.result.status == "ready"
    assert first.state.refresh_state == "success"
    assert second.decision.action == "none"
    assert provider.fetch_calls == 1
    assert provider.calendar_calls == 1


def test_open_provider_skips_multiple_existing_slots_without_provider_or_publication_calls(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    first_slot = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    health_clock = HealthClock(first_slot)
    health_store = InMemoryProviderHealthStore(
        failure_threshold=1,
        cooldown_seconds=7_200,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    open_health_endpoint(health_store, ProviderEndpoint.ALL_STOCK, prefix="open")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    probe_runner = RecordingProbeRunner()
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
        probe_runner=probe_runner,
    )

    first = service.run_due_once(first_slot)
    second_slot = datetime(2026, 7, 23, 18, 40, tzinfo=SHANGHAI)
    health_clock.now = second_slot
    second = service.run_due_once(second_slot)

    assert first.state.error_code == "SKIPPED_CIRCUIT_OPEN"
    assert second.state.error_code == "SKIPPED_CIRCUIT_OPEN"
    assert first.state.attempt_count == second.state.attempt_count == 0
    assert first.state.last_attempt_at == first_slot
    assert first.state.next_retry_at == second_slot
    assert second.state.next_retry_at == datetime(2026, 7, 23, 19, 20, tzinfo=SHANGHAI)
    assert provider.calendar_calls == 0
    assert provider.fetch_calls == 0
    assert probe_runner.calls == []
    assert store.list_refreshes() == []
    assert store.published_refresh() is None


def test_circuit_skip_after_final_existing_slot_stays_delayed_without_new_retry(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    final_slot = datetime(2026, 7, 24, 7, 15, tzinfo=SHANGHAI)
    health_store = InMemoryProviderHealthStore(
        failure_threshold=1,
        cooldown_seconds=86_400,
        clock=HealthClock(final_slot),
    )
    open_health_endpoint(health_store, ProviderEndpoint.ALL_STOCK, prefix="open")
    provider = CompleteProvider(fixture_bars())
    service = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        provider,
        synthetic_calendar(),
        required_symbols=set,
        health_store=health_store,
    )

    skipped = service.run_due_once(final_slot)
    later = service.run_due_once(final_slot + timedelta(minutes=1))

    assert skipped.state.refresh_state == "delayed"
    assert skipped.state.next_retry_at is None
    assert later.decision.action == "none"
    assert provider.calendar_calls == provider.fetch_calls == 0


@pytest.mark.parametrize(
    ("probe_error", "expected_state", "expected_code"),
    [
        (None, CircuitState.CLOSED, "PROVIDER_PROBE_SUCCEEDED"),
        (
            NormalizedTransportError.RECV_TIMEOUT,
            CircuitState.OPEN,
            "PROVIDER_PROBE_FAILED",
        ),
    ],
)
def test_elapsed_cooldown_runs_only_one_endpoint_probe_and_never_refreshes_same_slot(
    tmp_path: Path,
    probe_error: NormalizedTransportError | None,
    expected_state: CircuitState,
    expected_code: str,
) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    health_clock = HealthClock(now)
    health_store = InMemoryProviderHealthStore(
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    open_health_endpoint(health_store, ProviderEndpoint.DAILY_ASTOCK, prefix="open")
    health_clock.now += timedelta(seconds=60)
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    runner = RecordingProbeRunner(error=probe_error)
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
        probe_runner=runner,
    )

    outcome = service.run_due_once(now)

    assert [(endpoint, trade_date) for endpoint, trade_date, _ in runner.calls] == [
        (ProviderEndpoint.DAILY_ASTOCK, date(2026, 7, 23))
    ]
    assert runner.calls[0][2].startswith("probe-")
    assert health_store.endpoint_health(ProviderEndpoint.DAILY_ASTOCK).state == expected_state
    assert outcome.state.error_code == expected_code
    assert outcome.state.attempt_count == 0
    assert outcome.state.next_retry_at == datetime(2026, 7, 23, 18, 40, tzinfo=SHANGHAI)
    assert provider.calendar_calls == 0
    assert provider.fetch_calls == 0
    assert outcome.result is None
    assert store.list_refreshes() == []


def test_successful_probe_waits_until_next_existing_slot_before_full_refresh(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    health_clock = HealthClock(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    health_store = InMemoryProviderHealthStore(
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    open_health_endpoint(health_store, ProviderEndpoint.TRADE_DATES, prefix="open")
    health_clock.now += timedelta(seconds=60)
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    runner = RecordingProbeRunner()
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
        probe_runner=runner,
    )

    probe = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    waiting = service.run_due_once(datetime(2026, 7, 23, 18, 30, tzinfo=SHANGHAI))
    refreshed = service.run_due_once(datetime(2026, 7, 23, 18, 40, tzinfo=SHANGHAI))

    assert probe.state.error_code == "PROVIDER_PROBE_SUCCEEDED"
    assert waiting.decision.action == "wait"
    assert refreshed.result is not None and refreshed.result.status == "ready"
    assert provider.calendar_calls == provider.fetch_calls == 1
    assert len(runner.calls) == 1


def test_expired_half_open_from_crashed_process_is_reprobed_on_next_existing_slot(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    health_clock = HealthClock(now)
    health_path = tmp_path / "provider-health.sqlite3"
    crashed = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
        clock=health_clock,
    )
    crashed.initialize()
    endpoint = ProviderEndpoint.TRADE_DATES
    open_health_endpoint(crashed, endpoint, prefix="open")
    health_clock.now += timedelta(seconds=60)
    assert crashed.acquire_probe(endpoint, owner="crashed-process") is not None
    health_clock.now += timedelta(seconds=11)
    restarted = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
        clock=health_clock,
    )
    restarted.initialize()
    runner = RecordingProbeRunner()
    service = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        CompleteProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=set,
        health_store=restarted,
        probe_runner=runner,
    )

    outcome = service.run_due_once(now)

    assert outcome.state.error_code == "PROVIDER_PROBE_SUCCEEDED"
    assert len(runner.calls) == 1
    assert restarted.endpoint_health(endpoint).state == CircuitState.CLOSED


def test_formal_calendar_and_fetch_share_refresh_id_and_resolve_each_touched_endpoint_once(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    health_path = tmp_path / "control" / "provider-health.sqlite3"
    health_store = SQLiteProviderHealthStore(health_path)
    health_store.initialize()
    provider = ScopedObservationProvider(fixture_bars())
    store = MarketStore(tmp_path / "market.duckdb")
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None and outcome.result.status == "ready"
    request_key = "daily:baostock:2026-07-23:all-main-board"
    assert outcome.result.request_key == request_key
    assert outcome.result.run_id.startswith(
        f"{hashlib.sha256(request_key.encode()).hexdigest()[:12]}-"
    )
    observations = health_store.list_observations()
    assert {item.refresh_id for item in observations} == {outcome.result.run_id}
    assert {item.endpoint for item in observations} == {
        ProviderEndpoint.TRADE_DATES,
        ProviderEndpoint.ALL_STOCK,
    }
    assert health_store.endpoint_health(ProviderEndpoint.TRADE_DATES).consecutive_failures == 0
    assert health_store.endpoint_health(ProviderEndpoint.ALL_STOCK).consecutive_failures == 0
    restarted = SQLiteProviderHealthStore(health_path)
    restarted.initialize()
    assert restarted.list_observations() == observations


def test_socket_complete_cannot_override_later_terminal_endpoint_error(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    health_store = InMemoryProviderHealthStore(failure_threshold=3)
    provider = ScopedObservationProvider(
        fixture_bars(),
        fetch_error=NormalizedTransportError.PAGINATION_STALLED,
    )
    store = MarketStore(tmp_path / "market.duckdb")
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None and outcome.result.status == "error"
    assert health_store.endpoint_health(ProviderEndpoint.ALL_STOCK).consecutive_failures == 1
    assert health_store.endpoint_health(ProviderEndpoint.TRADE_DATES).consecutive_failures == 0
    assert (
        len(
            [
                item
                for item in health_store.list_observations()
                if item.endpoint == ProviderEndpoint.ALL_STOCK
            ]
        )
        == 2
    )


def test_real_provider_exhausted_first_index_call_opens_breaker_despite_later_success(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")

    class FirstIndexFails(ContinueAfterFailureClient):
        def query_history_k_data_plus(self, code, fields, **kwargs):
            self.history_calls[code] = self.history_calls.get(code, 0) + 1
            if code == "sh.000001":
                raise TimeoutError("synthetic exhausted first index")
            return FakeBaoStock.query_history_k_data_plus(self, code, fields, **kwargs)

    client = FirstIndexFails(json.loads(FIXTURE_PATH.read_text()))
    provider = BaoStockProvider(client=client, max_attempts=2, min_request_interval_seconds=0)
    health_store = InMemoryProviderHealthStore(failure_threshold=1)
    service = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        provider,
        synthetic_calendar(),
        required_symbols=set,
        health_store=health_store,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None and outcome.result.status == "partial"
    assert health_store.endpoint_health(ProviderEndpoint.INDEX_HISTORY).state == CircuitState.OPEN
    operations = [
        item
        for item in health_store.list_observations()
        if item.endpoint == ProviderEndpoint.INDEX_HISTORY
        and item.protocol_stage == ProtocolStage.OPERATION
    ]
    assert [(item.attempt, item.normalized_error) for item in operations] == [
        (2, NormalizedTransportError.RECV_TIMEOUT),
        (1, None),
    ]


def test_unclassified_operation_failure_survives_later_same_endpoint_success() -> None:
    module = load_module("backend.app.market.automation")
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=2,
        min_request_interval_seconds=0,
    )
    health = InMemoryProviderHealthStore(failure_threshold=1)
    collector = module._RefreshObservationCollector(health, "runtime-error-refresh")

    with (
        transport_observation_sink(collector.record),
        provider.refresh_operation("runtime-error-refresh"),
    ):
        with pytest.raises(RuntimeError):
            provider._read(
                ProviderEndpoint.INDEX_HISTORY,
                lambda: (_ for _ in ()).throw(
                    RuntimeError("private-token unclassified provider error")
                ),
            )
        provider._read(
            ProviderEndpoint.INDEX_HISTORY,
            lambda: FakeBaoStock(json.loads(FIXTURE_PATH.read_text())).query_history_k_data_plus(
                "sh.000001",
                "date,code",
                start_date="2026-07-23",
                end_date="2026-07-23",
            ),
        )
    collector.resolve_touched_endpoints()

    assert health.endpoint_health(ProviderEndpoint.INDEX_HISTORY).state == CircuitState.OPEN
    operations = [
        item
        for item in health.list_observations()
        if item.endpoint == ProviderEndpoint.INDEX_HISTORY
        and item.protocol_stage == ProtocolStage.OPERATION
    ]
    assert [item.normalized_error for item in operations] == [
        NormalizedTransportError.PROTOCOL_ERROR,
        None,
    ]


@pytest.mark.parametrize(
    "mode",
    [
        "zero",
        "wrong-endpoint",
        "wrong-refresh",
        "multiple-session",
        "attempt-two",
        "terminal-error-then-success",
        "second-session-complete",
        "attempt-two-complete",
    ],
)
def test_probe_without_exact_matching_operation_success_reopens_circuit(
    tmp_path: Path,
    mode: str,
) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    health_clock = HealthClock(now)
    health_store = InMemoryProviderHealthStore(
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    endpoint = ProviderEndpoint.ALL_STOCK
    open_health_endpoint(health_store, endpoint, prefix="open")
    health_clock.now += timedelta(seconds=60)

    class InvalidRunner:
        def run(self, leased_endpoint, *, trade_date, refresh_id) -> None:
            del trade_date
            if mode == "zero":
                return
            observed_endpoint = (
                ProviderEndpoint.TRADE_DATES if mode == "wrong-endpoint" else leased_endpoint
            )
            observed_refresh = "wrong-refresh" if mode == "wrong-refresh" else refresh_id
            sessions = ("one", "two") if mode == "multiple-session" else ("one",)
            if mode in {"second-session-complete", "attempt-two-complete"}:
                with refresh_scope(observed_refresh), provider_session_scope("extra-session"):
                    with request_scope(
                        observed_endpoint,
                        attempt=2 if mode == "attempt-two-complete" else 1,
                    ):
                        emit_terminal_observation(
                            started_at=monotonic(),
                            protocol_stage=ProtocolStage.COMPLETE,
                            recv_calls=1,
                            response_bytes=32,
                            end_marker_seen=True,
                            provider_code="0",
                            normalized_error=None,
                        )
            for session in sessions:
                with refresh_scope(observed_refresh), provider_session_scope(session):
                    with request_scope(
                        observed_endpoint,
                        attempt=2 if mode == "attempt-two" else 1,
                    ):
                        if mode == "terminal-error-then-success":
                            emit_terminal_observation(
                                started_at=monotonic(),
                                protocol_stage=ProtocolStage.RECEIVE,
                                recv_calls=1,
                                response_bytes=0,
                                end_marker_seen=False,
                                provider_code=None,
                                normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                            )
                        emit_terminal_observation(
                            started_at=monotonic(),
                            protocol_stage=ProtocolStage.OPERATION,
                            recv_calls=1,
                            response_bytes=32,
                            end_marker_seen=True,
                            provider_code="0",
                            normalized_error=None,
                        )

    outcome = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        CompleteProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=set,
        health_store=health_store,
        probe_runner=InvalidRunner(),
    ).run_due_once(now)

    assert outcome.state.error_code == "PROVIDER_PROBE_FAILED"
    assert health_store.endpoint_health(endpoint).state == CircuitState.OPEN


def test_recovered_internal_retry_records_one_final_endpoint_success(tmp_path: Path) -> None:
    module = load_module("backend.app.market.automation")

    class RecoveredProvider(ScopedObservationProvider):
        def fetch(self, trade_date: date, symbols=None) -> ProviderBatch:
            del trade_date, symbols
            self.fetch_calls += 1
            self._observe(
                ProviderEndpoint.ALL_STOCK,
                request_id="failed-attempt",
                provider_session_id="failed-session",
                attempt=1,
                error=NormalizedTransportError.RECV_TIMEOUT,
                stage=ProtocolStage.RECEIVE,
            )
            self._observe(
                ProviderEndpoint.ALL_STOCK,
                request_id="successful-attempt",
                provider_session_id="successful-session",
                attempt=2,
            )
            return ProviderBatch(
                bars=self.bars,
                expected_symbols=[item.symbol for item in self.bars],
                failed_symbols=[],
            )

    health_store = InMemoryProviderHealthStore()
    service = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        RecoveredProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None and outcome.result.status == "ready"
    assert health_store.endpoint_health(ProviderEndpoint.ALL_STOCK).consecutive_failures == 0


def test_provider_audit_write_failure_is_sanitized_and_prevents_publication(
    tmp_path: Path,
    caplog,
) -> None:
    module = load_module("backend.app.market.automation")
    secret = "token=health-secret /private/control.sqlite3 SELECT raw_payload"

    class FailingAuditStore(InMemoryProviderHealthStore):
        def record_observation(self, observation) -> None:
            del observation
            raise ProviderHealthError(secret)

    health_store = FailingAuditStore()
    provider = ScopedObservationProvider(fixture_bars())
    store = MarketStore(tmp_path / "market.duckdb")
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=health_store,
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is None
    assert outcome.state.refresh_state == "error"
    assert store.published_refresh() is None
    assert store.list_refreshes() == []
    assert secret not in caplog.text


def test_real_probe_runner_constructs_one_attempt_independent_provider_per_probe() -> None:
    module = load_module("backend.app.market.automation")
    providers = []

    class FakeProbeProvider:
        def __init__(self, max_attempts: int) -> None:
            self.max_attempts = max_attempts
            self.refresh_ids: list[str] = []
            self.calls = []

        @contextmanager
        def refresh_operation(self, refresh_id: str):
            self.refresh_ids.append(refresh_id)
            yield

        def probe_endpoint(self, endpoint, **kwargs) -> None:
            self.calls.append((endpoint, kwargs))

    def factory(*, max_attempts: int):
        provider = FakeProbeProvider(max_attempts)
        providers.append(provider)
        return provider

    runner = module.BaoStockProbeRunner(factory)
    runner.run(
        ProviderEndpoint.ALL_STOCK,
        trade_date=date(2026, 7, 23),
        refresh_id="probe-first",
    )
    runner.run(
        ProviderEndpoint.DAILY_FACTOR,
        trade_date=date(2026, 7, 23),
        refresh_id="probe-second",
    )

    assert len(providers) == 2
    assert [provider.max_attempts for provider in providers] == [1, 1]
    assert [provider.refresh_ids for provider in providers] == [
        ["probe-first"],
        ["probe-second"],
    ]
    assert [provider.calls[0][0] for provider in providers] == [
        ProviderEndpoint.ALL_STOCK,
        ProviderEndpoint.DAILY_FACTOR,
    ]


def test_two_scheduler_services_share_one_global_probe_lease(tmp_path: Path) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    health_clock = HealthClock(now)
    health_path = tmp_path / "control" / "provider-health.sqlite3"
    health_first = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    health_second = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=30,
        clock=health_clock,
    )
    health_first.initialize()
    health_second.initialize()
    open_health_endpoint(health_first, ProviderEndpoint.ALL_STOCK, prefix="open")
    health_clock.now += timedelta(seconds=60)
    entered = threading.Event()
    release = threading.Event()
    runner = RecordingProbeRunner(entered=entered, release=release)
    services = [
        module.MarketAutomationService(
            MarketStore(tmp_path / f"market-{index}.duckdb"),
            CompleteProvider(fixture_bars()),
            synthetic_calendar(),
            required_symbols=lambda: {"sh.600000"},
            health_store=health,
            probe_runner=runner,
        )
        for index, health in enumerate((health_first, health_second))
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(services[0].run_due_once, now)
        assert entered.wait(timeout=2)
        second_future = executor.submit(services[1].run_due_once, now)
        second = second_future.result(timeout=2)
        release.set()
        first = first_future.result(timeout=2)

    assert len(runner.calls) == 1
    assert {first.state.error_code, second.state.error_code} == {
        "PROVIDER_PROBE_SUCCEEDED",
        "SKIPPED_CIRCUIT_OPEN",
    }
    assert all(service.provider.calendar_calls == 0 for service in services)
    assert all(service.provider.fetch_calls == 0 for service in services)


def test_scheduler_restart_reads_persisted_open_health_and_skips_provider(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)
    clock = HealthClock(now)
    health_path = tmp_path / "control" / "provider-health.sqlite3"
    first = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=3_600,
        clock=clock,
    )
    first.initialize()
    open_health_endpoint(first, ProviderEndpoint.ALL_STOCK, prefix="open")
    restarted = SQLiteProviderHealthStore(
        health_path,
        failure_threshold=1,
        cooldown_seconds=3_600,
        clock=clock,
    )
    restarted.initialize()
    provider = CompleteProvider(fixture_bars())
    service = module.MarketAutomationService(
        MarketStore(tmp_path / "market.duckdb"),
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        health_store=restarted,
        probe_runner=RecordingProbeRunner(),
    )

    outcome = service.run_due_once(now)

    assert outcome.state.error_code == "SKIPPED_CIRCUIT_OPEN"
    assert provider.calendar_calls == provider.fetch_calls == 0


def test_automation_keeps_ready_publication_when_typed_regime_capture_fails(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    task_store = AfterCloseTaskStore(tmp_path / "user.sqlite3")
    regime_calls: list[str] = []
    strategy_calls: list[str] = []

    class FailingRegimeRunner:
        def run(self, _result, *, idempotency_key: str) -> str:
            regime_calls.append(idempotency_key)
            raise RegimeSnapshotUnavailable("/private/market unexpected-regime-secret")

    class RecordingStrategyRunner:
        def run(self, item, *, trade_date, idempotency_key: str) -> str:
            del trade_date
            strategy_calls.append(idempotency_key)
            return f"strategy-run:{item.strategy_id}"

    pipeline = AfterClosePipelineService(
        market_store=store,
        task_store=task_store,
        list_strategies=lambda: [
            StrategyWorkItem(
                strategy_id="strategy-1",
                version=1,
                version_id="strategy-version-1",
            )
        ],
        strategy_runner=RecordingStrategyRunner(),
        list_alert_rules=lambda: [],
        alert_runner=object(),
        regime_runner=FailingRegimeRunner(),
    )
    service = module.MarketAutomationService(
        store,
        CompleteProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        post_publish=pipeline,
    )
    now = datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI)

    first = service.run_due_once(now)
    second = service.run_due_once(now)

    assert first.result is not None and first.result.status == "ready"
    assert first.state.refresh_state == "success"
    assert second.decision.action == "none"
    assert store.published_refresh() is not None
    assert store.published_refresh().run_id == first.result.run_id
    assert len(regime_calls) == 2
    assert len(strategy_calls) == 1
    tasks = task_store.list_tasks(date(2026, 7, 23))
    regime_task = next(item for item in tasks if item.kind == "regime")
    strategy_task = next(item for item in tasks if item.kind == "strategy")
    assert regime_task.status == "error"
    assert regime_task.error_code == "regime_snapshot_capture_failed"
    assert regime_task.result_reference is None
    assert regime_task.attempt_count == 2
    assert strategy_task.status == "completed"
    assert strategy_task.attempt_count == 1

    private_bytes = b"".join(
        path.read_bytes()
        for path in sorted(task_store.path.parent.glob(f"{task_store.path.name}*"))
        if path.is_file()
    )
    assert b"unexpected-regime-secret" not in private_bytes
    assert b"/private/market" not in private_bytes
    assert b"result_payload" not in private_bytes
    assert b"content_hash" not in private_bytes
    assert b"dataset_identity_hash" not in private_bytes


def test_partial_attempt_waits_then_catches_up_at_retry_slot(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    bars = [bar for bar in fixture_bars() if bar.symbol != "sz.399001"]
    provider = CompleteProvider(bars)
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )

    first = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))
    waiting = service.run_due_once(datetime(2026, 7, 23, 18, 30, tzinfo=SHANGHAI))
    retried = service.run_due_once(datetime(2026, 7, 23, 18, 45, tzinfo=SHANGHAI))

    assert first.state.refresh_state == "retry_wait"
    assert first.state.next_retry_at == datetime(2026, 7, 23, 18, 40, tzinfo=SHANGHAI)
    assert waiting.decision.action == "wait"
    assert retried.decision.action == "run"
    assert retried.state.attempt_count == 2
    assert retried.state.next_retry_at == datetime(2026, 7, 23, 19, 20, tzinfo=SHANGHAI)
    assert provider.fetch_calls == 2


@pytest.mark.parametrize(
    ("failure_class", "retryable", "expected_state"),
    [
        ("transport_timeout", True, "retry_wait"),
        ("semantic", False, "error"),
    ],
)
def test_automation_retry_policy_uses_structured_retryable_failure(
    tmp_path: Path,
    failure_class: str,
    retryable: bool,
    expected_state: str,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")

    class FailingProvider(CompleteProvider):
        def fetch(self, trade_date: date, symbols=None) -> ProviderBatch:
            raise MarketFailureError(
                MarketFailure(
                    failure_stage="fetch",
                    failure_class=failure_class,
                    retryable=retryable,
                )
            )

    service = module.MarketAutomationService(
        store,
        FailingProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is not None
    assert outcome.result.failure_class == failure_class
    assert outcome.result.retryable is retryable
    assert outcome.state.refresh_state == expected_state
    assert (outcome.state.next_retry_at is not None) is retryable


def test_calendar_failure_is_not_scheduled_when_structured_failure_is_not_retryable(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")

    class FailingCalendarProvider(CompleteProvider):
        def trading_dates(self, start_date: date, end_date: date) -> list[date]:
            raise MarketFailureError(
                MarketFailure(
                    failure_stage="validate",
                    failure_class="calendar",
                    retryable=False,
                )
            )

    service = module.MarketAutomationService(
        store,
        FailingCalendarProvider(fixture_bars()),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
    )

    outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.result is None
    assert outcome.state.refresh_state == "error"
    assert outcome.state.next_retry_at is None
    assert outcome.state.error_code == "calendar"


def test_required_symbols_include_positions_and_all_watchlists() -> None:
    module = load_module("backend.app.market.automation")

    class Item:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

    class Watchlist:
        def __init__(self, identifier: str) -> None:
            self.id = identifier

    class FakeUserStore:
        def list_positions(self):
            return [Item("sh.600000")]

        def list_watchlists(self):
            return [Watchlist("list-a"), Watchlist("list-b")]

        def list_watchlist_items(self, identifier):
            return {
                "list-a": [Item("sz.000001")],
                "list-b": [Item("sh.600000"), Item("sh.600001")],
            }[identifier]

    assert module.collect_required_symbols(FakeUserStore()) == {
        "sh.600000",
        "sh.600001",
        "sz.000001",
    }


def test_automation_loop_checks_immediately_for_startup_catch_up() -> None:
    module = load_module("backend.app.market.automation")
    stop = asyncio.Event()

    class Service:
        calls = 0

        def run_due_once(self, now):
            self.calls += 1
            stop.set()

    service = Service()

    asyncio.run(
        module.run_automation_loop(
            service,
            stop,
            clock=lambda: datetime(2026, 7, 23, 19, tzinfo=SHANGHAI),
            poll_seconds=0.01,
        )
    )

    assert service.calls == 1


def test_automation_loop_survives_one_unexpected_iteration_failure() -> None:
    module = load_module("backend.app.market.automation")
    stop = asyncio.Event()

    class Service:
        calls = 0

        def run_due_once(self, now):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("synthetic iteration failure")
            stop.set()

    service = Service()
    asyncio.run(
        module.run_automation_loop(
            service,
            stop,
            clock=lambda: datetime(2026, 7, 23, 19, tzinfo=SHANGHAI),
            poll_seconds=0.01,
        )
    )

    assert service.calls == 2


def test_refresh_lock_rejects_a_second_process(tmp_path: Path) -> None:
    module = load_module("backend.app.market.automation")
    lock_path = tmp_path / ".refresh.lock"

    with module.RefreshRunLock(lock_path):
        with pytest.raises(module.RefreshAlreadyRunning):
            with module.RefreshRunLock(lock_path):
                pass


def test_market_schema_migration_busy_lock_is_sanitized_and_zero_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "local_temp_dir": tmp_path / "tmp",
            "local_lock_dir": tmp_path / "locks",
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])
    lock_path = settings.local_lock_dir / "market-refresh.lock"

    with cli.RefreshRunLock(lock_path):
        before = {
            item.relative_to(tmp_path): (
                item.stat().st_size,
                item.stat().st_mtime_ns,
                hashlib.sha256(item.read_bytes()).hexdigest(),
            )
            for item in tmp_path.rglob("*")
            if item.is_file()
        }
        assert cli.main() == 1
        after = {
            item.relative_to(tmp_path): (
                item.stat().st_size,
                item.stat().st_mtime_ns,
                hashlib.sha256(item.read_bytes()).hexdigest(),
            )
            for item in tmp_path.rglob("*")
            if item.is_file()
        }

    assert before == after
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "market_control_schema_migration_busy",
        "status": "error",
        "writes_market_control_schema": False,
    }


def test_market_schema_migration_adds_continuity_tables_under_shared_lock(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "local_temp_dir": tmp_path / "tmp",
            "local_lock_dir": tmp_path / "locks",
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "migration": "market_control_schema",
        "status": "ready",
        "writes_market_control_schema": True,
    }
    path = settings.market_data_dir / settings.market_database_name
    with duckdb.connect(str(path), read_only=True) as connection:
        assert {"repair_jobs", "repair_attempts"} <= {
            row[0] for row in connection.execute("SHOW TABLES").fetchall()
        }


def test_market_schema_migration_rolls_back_base_when_continuity_stage_fails(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    path = tmp_path / "market" / "stock_eva.duckdb"
    path.parent.mkdir()
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            CREATE TABLE refresh_runs (
                run_id VARCHAR PRIMARY KEY, requested_date DATE NOT NULL,
                source VARCHAR NOT NULL, status VARCHAR NOT NULL,
                requested_count INTEGER NOT NULL, succeeded_count INTEGER NOT NULL,
                failed_symbols JSON NOT NULL, error_message VARCHAR,
                started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        connection.execute("CREATE TABLE continuity_schema_meta(blocker VARCHAR)")
    path.chmod(0o640)
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": path.parent,
            "market_database_name": path.name,
            "local_temp_dir": tmp_path / "tmp",
            "local_lock_dir": tmp_path / "locks",
        }
    )
    lock_path = settings.local_lock_dir / "market-refresh.lock"
    with cli.RefreshRunLock(lock_path):
        pass
    before = (
        path.stat().st_size,
        path.stat().st_mtime_ns,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "error_code": "market_control_schema_migration_failed",
        "status": "error",
        "writes_market_control_schema": False,
    }
    after = (
        path.stat().st_size,
        path.stat().st_mtime_ns,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    assert after == before
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    with duckdb.connect(str(path), read_only=True) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(refresh_runs)").fetchall()
        }
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    assert "coverage_ratio" not in columns
    assert "repair_jobs" not in tables


def test_market_schema_migration_new_database_failure_leaves_tree_unchanged(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "local_temp_dir": tmp_path / "tmp",
            "local_lock_dir": tmp_path / "locks",
        }
    )
    lock_path = settings.local_lock_dir / "market-refresh.lock"
    with cli.RefreshRunLock(lock_path):
        pass

    def tree_fingerprint() -> dict[Path, tuple[int, int, str]]:
        return {
            item.relative_to(tmp_path): (
                item.stat().st_size,
                item.stat().st_mtime_ns,
                hashlib.sha256(item.read_bytes()).hexdigest(),
            )
            for item in tmp_path.rglob("*")
            if item.is_file()
        }

    before = tree_fingerprint()

    real_child = cli.MarketStore._run_bound_schema_migration_child

    def fail_migration_stage(directory_fd: int, action: str, **kwargs):
        if action == "initialize_and_migrate":
            return False, None, {}
        return real_child(directory_fd, action, **kwargs)

    monkeypatch.setattr(
        cli.MarketStore,
        "_run_bound_schema_migration_child",
        staticmethod(fail_migration_stage),
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "market_control_schema_migration_failed",
        "status": "error",
        "writes_market_control_schema": False,
    }
    assert tree_fingerprint() == before
    assert not settings.market_data_dir.exists()


@pytest.mark.parametrize("link_kind", ["live", "broken"])
def test_writer_schema_migration_rejects_symlink_database_targets(
    tmp_path: Path,
    link_kind: str,
) -> None:
    continuity = load_module("backend.app.market.continuity")
    target_parent = tmp_path / "market"
    target_parent.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()
    target = target_parent / "stock_eva.duckdb"
    external = tmp_path / "external.duckdb"
    if link_kind == "live":
        with duckdb.connect(str(external)) as connection:
            connection.execute("CREATE TABLE external_evidence (value INTEGER)")
        external_before = (
            external.stat().st_size,
            external.stat().st_mtime_ns,
            hashlib.sha256(external.read_bytes()).hexdigest(),
        )
        target.symlink_to(external)
    else:
        target.symlink_to(external)
        external_before = None
    link_before = os.readlink(target)

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert target.is_symlink()
    assert os.readlink(target) == link_before
    if external_before is None:
        assert not external.exists()
    else:
        assert (
            external.stat().st_size,
            external.stat().st_mtime_ns,
            hashlib.sha256(external.read_bytes()).hexdigest(),
        ) == external_before


def test_writer_schema_migration_rejects_nonregular_target(tmp_path: Path) -> None:
    continuity = load_module("backend.app.market.continuity")
    staging = tmp_path / "staging"
    staging.mkdir()
    target = tmp_path / "market.duckdb"
    target.mkdir()

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert target.is_dir()


@pytest.mark.parametrize("symlink_scope", ["target_parent", "staging_parent"])
def test_writer_schema_migration_rejects_symlink_parent_scope(
    tmp_path: Path,
    symlink_scope: str,
) -> None:
    continuity = load_module("backend.app.market.continuity")
    external = tmp_path / "external"
    external.mkdir()
    target_parent = tmp_path / "market"
    staging = tmp_path / "staging"
    if symlink_scope == "target_parent":
        target_parent.symlink_to(external, target_is_directory=True)
        staging.mkdir()
    else:
        target_parent.mkdir()
        staging.symlink_to(external, target_is_directory=True)
    target = target_parent / "stock_eva.duckdb"
    before = tuple(external.iterdir())

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert tuple(external.iterdir()) == before
    assert not os.path.lexists(target)


def test_writer_schema_migration_refuses_staging_allocation_collision(
    tmp_path: Path,
    monkeypatch,
) -> None:
    continuity = load_module("backend.app.market.continuity")
    staging = tmp_path / "staging"
    staging.mkdir()
    marker = staging / "foreign-collision"
    marker.write_bytes(b"foreign staging evidence")
    target = tmp_path / "market" / "stock_eva.duckdb"
    collision = staging / f".{target.name}.collision.migration"
    collision.mkdir()
    collision_marker = collision / "marker"
    collision_marker.write_bytes(b"foreign collision evidence")

    class CollisionId:
        hex = "collision"

    monkeypatch.setattr(market_store_module, "uuid4", lambda: CollisionId())

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert marker.read_bytes() == b"foreign staging evidence"
    assert collision_marker.read_bytes() == b"foreign collision evidence"
    assert not target.parent.exists()


def test_writer_schema_migration_link_failure_cleans_only_owned_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    continuity = load_module("backend.app.market.continuity")
    staging = tmp_path / "staging"
    staging.mkdir()
    marker = staging / "foreign-marker"
    marker.write_bytes(b"foreign staging evidence")
    target = tmp_path / "market" / "stock_eva.duckdb"
    before = {item.name: item.read_bytes() for item in staging.iterdir()}

    def fail_link(*_args, **_kwargs):
        raise OSError("synthetic atomic link failure")

    monkeypatch.setattr(os, "link", fail_link)

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert {item.name: item.read_bytes() for item in staging.iterdir()} == before
    assert not target.parent.exists()


def test_writer_schema_migration_target_collision_preserves_external_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    continuity = load_module("backend.app.market.continuity")
    staging = tmp_path / "staging"
    staging.mkdir()
    target = tmp_path / "market" / "stock_eva.duckdb"
    collision_bytes = b"external collision evidence"
    collision_inode: list[int] = []
    real_link = os.link

    def collide_at_link(source, destination, *args, **kwargs):
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=kwargs.get("dst_dir_fd"),
        )
        try:
            os.write(descriptor, collision_bytes)
            collision_inode.append(os.fstat(descriptor).st_ino)
        finally:
            os.close(descriptor)
        return real_link(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "link", collide_at_link)

    with pytest.raises(continuity.RepairQueueError):
        MarketStore(target).initialize_all_writer_schema(staging_directory=staging)

    assert target.read_bytes() == collision_bytes
    assert target.stat().st_ino == collision_inode[0]
    assert list(staging.iterdir()) == []


def test_writer_schema_migration_secures_modes_and_replaces_existing_with_cow_inode(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    new_path = tmp_path / "new" / "stock_eva.duckdb"
    MarketStore(new_path).initialize_all_writer_schema(staging_directory=staging)
    assert stat.S_IMODE(new_path.stat().st_mode) == 0o600

    existing_path = tmp_path / "existing.duckdb"
    existing = MarketStore(existing_path)
    existing.initialize_schema()
    existing_path.chmod(0o664)
    inode_before = existing_path.stat().st_ino

    existing.initialize_all_writer_schema(staging_directory=staging)

    assert existing_path.stat().st_ino != inode_before
    assert stat.S_IMODE(existing_path.stat().st_mode) == 0o600


def test_automation_defers_without_fetching_when_refresh_lock_is_busy(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    store = MarketStore(tmp_path / "market.duckdb")
    provider = CompleteProvider(fixture_bars())
    lock_path = tmp_path / ".refresh.lock"
    service = module.MarketAutomationService(
        store,
        provider,
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        lock_path=lock_path,
    )

    with module.RefreshRunLock(lock_path):
        outcome = service.run_due_once(datetime(2026, 7, 23, 18, 10, tzinfo=SHANGHAI))

    assert outcome.state.error_code == "refresh_already_running"
    assert outcome.state.refresh_state == "retry_wait"
    assert provider.calendar_calls == 0
    assert provider.fetch_calls == 0


def test_auto_refresh_once_defaults_to_network_free_plan(
    monkeypatch,
    capsys,
) -> None:
    use_local_storage_settings(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "auto-refresh-once"])

    def unexpected_run(*args, **kwargs):
        raise AssertionError("one-shot automation needs explicit --execute")

    monkeypatch.setattr(
        cli.MarketAutomationService,
        "run_due_once",
        unexpected_run,
    )

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["writes_market_data"] is False
    assert payload["execute_requires"] == "--execute"
    assert "target_session" in payload


def test_auto_refresh_dry_run_does_not_create_empty_runtime_tree(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "auto-refresh-once"])

    assert cli.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["writes_market_data"] is False
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_auto_refresh_dry_run_reports_sanitized_provider_health_without_constructing_provider(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    health_store = SQLiteProviderHealthStore(
        settings.local_control_dir / settings.provider_health_database_name,
        failure_threshold=1,
        cooldown_seconds=3_600,
    )
    health_store.initialize()
    open_health_endpoint(health_store, ProviderEndpoint.ALL_STOCK, prefix="open")
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        cli,
        "BaoStockProvider",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("dry-run must not construct a provider")
        ),
    )
    monkeypatch.setattr(sys, "argv", ["stock-eva", "auto-refresh-once"])

    assert cli.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["provider_health"] == {
        "state": "OPEN",
        "blocking_endpoints": ["all_stock"],
        "probe_endpoints": [],
        "cooldown_until": {
            "all_stock": health_store.endpoint_health(
                ProviderEndpoint.ALL_STOCK
            ).cooldown_until.isoformat()
        },
    }
    serialized = json.dumps(payload).lower()
    for forbidden in ("lease_id", "owner", "token", "raw", str(tmp_path).lower()):
        assert forbidden not in serialized


def test_auto_refresh_dry_run_does_not_reap_or_write_expired_probe_lease(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
            "provider_circuit_failure_threshold": 1,
            "provider_circuit_cooldown_seconds": 1,
            "provider_circuit_probe_lease_seconds": 1,
        }
    )
    old_clock = HealthClock(datetime(2020, 1, 1, tzinfo=UTC))
    path = settings.local_control_dir / settings.provider_health_database_name
    health = SQLiteProviderHealthStore(
        path,
        failure_threshold=1,
        cooldown_seconds=1,
        probe_lease_seconds=1,
        clock=old_clock,
    )
    health.initialize()
    open_health_endpoint(health, ProviderEndpoint.ALL_STOCK, prefix="expired")
    old_clock.now += timedelta(seconds=1)
    assert health.acquire_probe(ProviderEndpoint.ALL_STOCK, owner="crashed") is not None
    before = path.read_bytes()
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "auto-refresh-once"])

    assert cli.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_health"]["state"] == "HALF_OPEN"
    assert path.read_bytes() == before
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        state = connection.execute(
            "SELECT state FROM endpoint_circuits WHERE endpoint = 'all_stock'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert state == "HALF_OPEN"


def test_market_status_api_is_read_only_and_reports_capability(
    tmp_path: Path,
) -> None:
    module = load_module("backend.app.market.automation")
    calendar_module = load_module("backend.app.market.calendar")
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 23))
    app.dependency_overrides[get_market_store] = lambda: store
    app.dependency_overrides[calendar_module.get_trading_calendar] = synthetic_calendar
    app.dependency_overrides[module.get_market_clock] = lambda: (
        lambda: datetime(2026, 7, 24, 10, tzinfo=SHANGHAI)
    )

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/status")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["market_phase"] == "market_open"
    assert payload["calendar_status"] == "confirmed"
    assert payload["latest_expected_session"] == "2026-07-23"
    assert payload["published_as_of"] == "2026-07-23"
    assert payload["capability"] == {
        "mode": "end_of_day",
        "realtime": False,
        "automatic_when_running": True,
        "sleep_catch_up": True,
    }


def test_market_status_reports_external_scheduler_state(tmp_path: Path) -> None:
    module = load_module("backend.app.market.automation")
    calendar_module = load_module("backend.app.market.calendar")
    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, date(2026, 7, 23))
    store.save_scheduler_state(
        module.SchedulerState(
            target_session=date(2026, 7, 23),
            refresh_state="success",
            last_success_at=datetime(2026, 7, 23, 18, 20, tzinfo=SHANGHAI),
        )
    )
    settings = get_settings().model_copy(
        update={
            "auto_refresh_enabled": False,
            "scheduled_refresh_enabled": True,
        }
    )
    app.dependency_overrides[get_market_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[calendar_module.get_trading_calendar] = synthetic_calendar
    app.dependency_overrides[module.get_market_clock] = lambda: (
        lambda: datetime(2026, 7, 24, 10, tzinfo=SHANGHAI)
    )

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/status")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["refresh_state"] == "success"


def test_market_summary_rejects_expected_date_query_parameter(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    app.dependency_overrides[get_market_store] = lambda: store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/summary?expected_date=2026-07-23")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


def test_portfolio_valuation_rejects_expected_date_query_parameter(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    app.dependency_overrides[get_market_store] = lambda: store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/portfolio/valuation?expected_date=2026-07-23")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
