import asyncio
import hashlib
import importlib
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

import httpx
import pytest

import backend.app.cli as cli
from backend.app.api.market import get_market_store
from backend.app.config import get_settings
from backend.app.main import app
from backend.app.market.baostock import ProviderBatch
from backend.app.market.baostock_vendor import emit_terminal_observation
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
from backend.app.market.store import MarketStore
from backend.app.orchestration.after_close import (
    AfterClosePipelineService,
    AfterCloseTaskStore,
    StrategyWorkItem,
)
from backend.app.regime.snapshots import RegimeSnapshotUnavailable
from tests.test_market_data import fixture_payload

SHANGHAI = ZoneInfo("Asia/Shanghai")


def use_local_storage_settings(monkeypatch) -> None:
    settings = cli.get_settings().model_copy(update={"nas_market_dataset_root": None})
    monkeypatch.setattr(cli, "get_settings", lambda: settings)


def load_module(name: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"{name} is not implemented")


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
        stage: ProtocolStage = ProtocolStage.COMPLETE,
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
                stage=ProtocolStage.PAGINATION,
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
