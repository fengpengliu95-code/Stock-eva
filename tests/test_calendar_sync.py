import asyncio
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

import backend.app.cli as cli
from backend.app.api.market import get_calendar_sync_store, get_market_store
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.baostock_vendor import emit_terminal_observation
from backend.app.market.calendar import TradingCalendar, get_trading_calendar
from backend.app.market.calendar_sync import (
    CalendarSyncPolicy,
    CalendarSyncService,
    CalendarSyncStore,
    CalendarSyncStoreReadError,
    read_calendar_conflict,
    run_calendar_sync_loop,
)
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
from tests.test_baostock_provider import FIXTURE_PATH, FakeBaoStock, FakeResult

SHANGHAI = ZoneInfo("Asia/Shanghai")


def synthetic_calendar() -> TradingCalendar:
    return TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-22",
            "sources": [
                {
                    "exchange": "SSE",
                    "title": "Synthetic 2026 closure notice",
                    "url": "https://example.test/sse-2026",
                    "notice_no": "SSE-2026",
                }
            ],
            "closed_dates": ["2026-01-01", "2026-02-16"],
        }
    )


class CalendarProvider:
    def __init__(self, sessions: list[date]) -> None:
        self.sessions = sessions
        self.calls: list[tuple[date, date]] = []

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        self.calls.append((start_date, end_date))
        return [item for item in self.sessions if start_date <= item <= end_date]


class ObservedCalendarProvider(CalendarProvider):
    @contextmanager
    def refresh_operation(self, refresh_id: str):
        with refresh_scope(refresh_id):
            yield

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        with provider_session_scope("calendar-session"):
            with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                emit_terminal_observation(
                    started_at=0,
                    protocol_stage=ProtocolStage.OPERATION,
                    recv_calls=1,
                    response_bytes=32,
                    end_marker_seen=True,
                    provider_code="0",
                    normalized_error=None,
                )
        return super().trading_dates(start_date, end_date)


class RecordingHealthStore(InMemoryProviderHealthStore):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.terminal_outcomes = []

    def record_endpoint_outcome(
        self,
        refresh_id,
        endpoint,
        *,
        success,
        normalized_error=None,
        observed_at=None,
    ):
        self.terminal_outcomes.append((refresh_id, endpoint, success, normalized_error))
        return super().record_endpoint_outcome(
            refresh_id,
            endpoint,
            success=success,
            normalized_error=normalized_error,
            observed_at=observed_at,
        )


def _calendar_control_counts(path: Path) -> tuple[int, int]:
    with sqlite3.connect(path) as connection:
        runs = connection.execute("SELECT COUNT(*) FROM calendar_sync_runs").fetchone()[0]
        state = connection.execute("SELECT COUNT(*) FROM calendar_sync_state").fetchone()[0]
    return runs, state


def test_calendar_sync_schedule_has_monthly_full_startup_and_1630_light() -> None:
    policy = CalendarSyncPolicy()

    monthly = policy.decide(
        datetime(2026, 8, 1, 4, 5, tzinfo=SHANGHAI),
        state=None,
    )
    startup = policy.decide(
        datetime(2026, 8, 3, 9, 0, tzinfo=SHANGHAI),
        state=None,
        startup=True,
    )
    daily = policy.decide(
        datetime(2026, 8, 3, 16, 30, tzinfo=SHANGHAI),
        state=None,
    )

    assert monthly.action == "full"
    assert monthly.reason == "monthly_reconciliation"
    assert startup.action == "light"
    assert startup.reason == "startup_check"
    assert daily.action == "light"
    assert daily.reason == "daily_1630_check"


def test_bundled_calendar_supports_cross_year_strategy_history() -> None:
    calendar = get_trading_calendar()

    assert calendar.session_status(date(2025, 1, 1)) == "closed"
    assert calendar.session_status(date(2025, 1, 2)) == "open"
    assert calendar.session_status(date(2025, 10, 8)) == "closed"
    assert calendar.previous_session(date(2026, 1, 5)) == date(2025, 12, 31)
    assert calendar.latest_expected_session(datetime(2026, 1, 5, 18, 9, tzinfo=SHANGHAI)) == date(
        2025, 12, 31
    )

    current = date(2025, 1, 1)
    end = date(2026, 7, 24)
    confirmed_open = 0
    while current <= end:
        confirmed_open += calendar.session_status(current) == "open"
        current += timedelta(days=1)
    assert confirmed_open >= 260


def test_read_calendar_conflict_is_dynamic_select_only_and_fail_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "calendar.sqlite3"
    CalendarSyncStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO calendar_sync_state (singleton, payload_json) VALUES (1, ?)",
            ('{"conflict_detected": false}',),
        )

    assert read_calendar_conflict(path) is False

    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT OR REPLACE INTO calendar_sync_state (singleton, payload_json) VALUES (1, ?)",
            ('{"conflict_detected": true}',),
        )
    assert read_calendar_conflict(path) is True

    with pytest.raises(CalendarSyncStoreReadError):
        read_calendar_conflict(tmp_path / "missing.sqlite3")

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE calendar_sync_state")
    with pytest.raises(CalendarSyncStoreReadError):
        read_calendar_conflict(path)


def test_calendar_sync_schedule_does_not_repeat_completed_slots(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 8, 3)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    now = datetime(2026, 8, 3, 16, 30, tzinfo=SHANGHAI)
    result = service.execute(service.plan(now=now, mode="light"))

    decision = CalendarSyncPolicy().decide(now, state=store.state())

    assert result.status == "ready"
    assert decision.action == "none"
    assert decision.next_sync_at == datetime(2026, 8, 4, 16, 30, tzinfo=SHANGHAI)


def test_successful_sync_versions_observations_and_notice_metadata(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 7, 23), date(2026, 7, 24)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="full",
        start_date=date(2026, 7, 23),
        end_date=date(2026, 7, 24),
    )

    result = service.execute(plan)
    state = store.state()
    run = store.run(result.run_id)
    observations = store.observations(result.run_id)

    assert result.status == "ready"
    assert state.last_good_run_id == result.run_id
    assert state.last_full_sync_at == result.completed_at
    assert state.conflict_detected is False
    assert [item.session_date for item in observations] == [
        date(2026, 7, 23),
        date(2026, 7, 24),
    ]
    assert all(item.provider_is_open for item in observations)
    assert run.authority_checksum
    assert run.sources[0]["url"] == "https://example.test/sse-2026"
    assert run.sources[0]["notice_no"] == "SSE-2026"
    assert run.range_start == date(2026, 7, 23)
    assert run.range_end == date(2026, 7, 24)


def test_cross_year_sync_uses_both_confirmed_authority_versions(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = get_trading_calendar()
    service = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2025, 12, 31), date(2026, 1, 5)]),
    )
    plan = service.plan(
        now=datetime(2026, 1, 5, 16, 30, tzinfo=SHANGHAI),
        mode="full",
        start_date=date(2025, 12, 31),
        end_date=date(2026, 1, 5),
    )

    result = service.execute(plan)

    assert result.status == "ready"
    assert plan.authority_years == [2025, 2026]
    assert {
        item.session_date: item.official_status for item in store.observations(result.run_id)
    } == {
        date(2025, 12, 31): "open",
        date(2026, 1, 1): "closed",
        date(2026, 1, 2): "closed",
        date(2026, 1, 3): "closed",
        date(2026, 1, 4): "closed",
        date(2026, 1, 5): "open",
    }


def test_conflict_is_quarantined_without_replacing_last_known_good(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = synthetic_calendar()
    good = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 23), date(2026, 7, 24)]),
    )
    range_kwargs = {
        "now": datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        "mode": "full",
        "start_date": date(2026, 7, 23),
        "end_date": date(2026, 7, 24),
    }
    good_result = good.execute(good.plan(**range_kwargs))

    conflicting = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 23)]),
    )
    conflict_result = conflicting.execute(conflicting.plan(**range_kwargs))
    state = store.state()

    assert conflict_result.status == "quarantined"
    assert conflict_result.conflicts == [
        {
            "date": "2026-07-24",
            "official": "open",
            "provider": "closed",
        }
    ]
    assert state.last_good_run_id == good_result.run_id
    assert state.conflict_detected is True
    assert state.conflict_run_id == conflict_result.run_id
    assert store.run(conflict_result.run_id).status == "quarantined"


def test_unknown_year_is_observed_but_never_promoted_to_authority(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        CalendarProvider([date(2027, 1, 4)]),
    )
    plan = service.plan(
        now=datetime(2027, 1, 4, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2027, 1, 4),
        end_date=date(2027, 1, 4),
    )

    result = service.execute(plan)
    observation = store.observations(result.run_id)[0]

    assert result.status == "observed_only"
    assert observation.official_status == "unknown"
    assert store.state().last_good_run_id is None
    assert synthetic_calendar().session_status(date(2027, 1, 4)) == "unknown"


def test_provider_failure_is_audited_once_and_preserves_last_known_good(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = synthetic_calendar()
    good = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 24)]),
    )
    good_result = good.execute(
        good.plan(
            now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 24),
            end_date=date(2026, 7, 24),
        )
    )

    class FailingProvider:
        def trading_dates(self, start_date, end_date):
            raise OSError("synthetic transport failure")

    failing = CalendarSyncService(store, calendar, FailingProvider())
    failed = failing.execute(
        failing.plan(
            now=datetime(2026, 7, 25, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 25),
            end_date=date(2026, 7, 25),
        )
    )

    assert failed.status == "error"
    assert store.run(failed.run_id).status == "error"
    assert store.state().last_good_run_id == good_result.run_id
    assert (
        CalendarSyncPolicy()
        .decide(
            datetime(2026, 7, 25, 16, 31, tzinfo=SHANGHAI),
            state=store.state(),
        )
        .action
        == "none"
    )


def test_calendar_execute_is_skipped_while_provider_circuit_is_not_closed(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = ObservedCalendarProvider([date(2026, 7, 24)])
    health = InMemoryProviderHealthStore(failure_threshold=1)
    health.record_terminal_failure(
        "open-calendar",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.RECV_TIMEOUT,
    )
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "skipped_circuit_open"
    assert provider.calls == []
    assert store.state().last_attempt_at is None


def test_calendar_terminal_observation_updates_breaker_before_calendar_state_save(
    tmp_path: Path,
) -> None:
    health = InMemoryProviderHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = ObservedCalendarProvider([date(2026, 7, 24)])
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    assert service.execute(plan).status == "ready"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).consecutive_failures == 0
    assert len(health.list_observations()) == 1

    class FailingObservedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            with provider_session_scope("failed-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=2):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=1,
                        response_bytes=0,
                        end_marker_seen=False,
                        provider_code=None,
                        normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                    )
            raise OSError("private-token raw calendar failure")

    failed_store = CalendarSyncStore(tmp_path / "failed-calendar.sqlite3")
    failed_service = CalendarSyncService(
        failed_store,
        synthetic_calendar(),
        FailingObservedProvider([]),
        health_store=health,
    )
    failed = failed_service.execute(
        failed_service.plan(
            now=datetime(2026, 7, 25, 16, 30, tzinfo=SHANGHAI),
            mode="light",
        )
    )
    assert failed.status == "error"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert failed_store.run(failed.run_id).status == "error"


def test_calendar_complete_success_evidence_records_terminal_success(tmp_path: Path) -> None:
    health = RecordingHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        ObservedCalendarProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "ready"
    assert health.terminal_outcomes == [(result.run_id, ProviderEndpoint.TRADE_DATES, True, None)]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.CLOSED


@pytest.mark.parametrize("malformation", ["invalid-date", "missing-schema"])
def test_real_provider_calendar_high_level_failure_trips_circuit_without_state_write(
    tmp_path: Path,
    malformation: str,
) -> None:
    class MalformedCalendarClient(FakeBaoStock):
        def query_trade_dates(self, **_kwargs):
            if malformation == "missing-schema":
                return FakeResult(
                    ["is_trading_day"],
                    [["1"]],
                )
            return FakeResult(
                ["calendar_date", "is_trading_day"],
                [["private-token-not-a-date", "1"]],
            )

    health = RecordingHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = BaoStockProvider(
        client=MalformedCalendarClient(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )
    before_bytes = store.path.read_bytes()
    before_state = store.state()
    before_counts = _calendar_control_counts(store.path)

    result = service.execute(plan)

    assert result.status == "error"
    observations = health.list_observations()
    assert len(observations) == 1
    assert observations[0].protocol_stage == ProtocolStage.OPERATION
    assert observations[0].normalized_error is None
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert store.path.read_bytes() == before_bytes
    assert store.state() == before_state
    assert _calendar_control_counts(store.path) == before_counts
    with pytest.raises(KeyError):
        store.run(result.run_id)
    assert "private-token" not in result.model_dump_json()


@pytest.mark.parametrize(
    "invalid_evidence",
    ["second-session", "attempt-two", "multiple-operation"],
)
def test_calendar_rejects_inconsistent_success_evidence_without_state_write(
    tmp_path: Path,
    invalid_evidence: str,
) -> None:
    class InvalidEvidenceProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date: date, end_date: date) -> list[date]:
            stage = (
                ProtocolStage.OPERATION
                if invalid_evidence == "multiple-operation"
                else ProtocolStage.COMPLETE
            )
            session = (
                "extra-session" if invalid_evidence == "second-session" else "calendar-session"
            )
            with provider_session_scope(session):
                with request_scope(
                    ProviderEndpoint.TRADE_DATES,
                    attempt=2 if invalid_evidence == "attempt-two" else 1,
                ):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=stage,
                        recv_calls=1,
                        response_bytes=32,
                        end_marker_seen=True,
                        provider_code="0",
                        normalized_error=None,
                    )
            return super().trading_dates(start_date, end_date)

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    before_bytes = store.path.read_bytes()
    before_state = store.state()
    before_counts = _calendar_control_counts(store.path)
    health = RecordingHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        InvalidEvidenceProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "error"
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert store.path.read_bytes() == before_bytes
    assert store.state() == before_state
    assert _calendar_control_counts(store.path) == before_counts


def test_calendar_health_audit_failure_prevents_calendar_state_write(tmp_path: Path) -> None:
    class FailingAudit(InMemoryProviderHealthStore):
        def record_observation(self, observation) -> None:
            del observation
            raise ProviderHealthError("token=/private/provider-health.sqlite3")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        ObservedCalendarProvider([date(2026, 7, 24)]),
        health_store=FailingAudit(),
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError):
        service.execute(plan)

    assert store.state().last_attempt_at is None


def test_calendar_unclassified_failure_resolves_breaker_without_state_write(
    tmp_path: Path,
) -> None:
    class UnclassifiedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            del start_date, end_date
            with provider_session_scope("runtime-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=1,
                        response_bytes=32,
                        end_marker_seen=True,
                        provider_code="0",
                        normalized_error=None,
                    )
            raise RuntimeError("private-token /private/calendar provider failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = RecordingHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        UnclassifiedProvider([]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "error"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert store.state().last_attempt_at is None
    with pytest.raises(KeyError):
        store.run(result.run_id)
    serialized = result.model_dump_json()
    assert "private-token" not in serialized
    assert "/private/calendar" not in serialized


def test_calendar_unclassified_zero_observation_failure_does_not_kill_loop(
    tmp_path: Path,
) -> None:
    class ZeroObservationFailure(CalendarProvider):
        def trading_dates(self, start_date, end_date):
            self.calls.append((start_date, end_date))
            raise AssertionError("private-token zero-observation failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = InMemoryProviderHealthStore(failure_threshold=1)
    provider = ZeroObservationFailure([])
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        clock=lambda: datetime(2026, 7, 24, 8, tzinfo=UTC),
        health_store=health,
    )
    stop = asyncio.Event()
    clock_calls = 0

    def loop_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        if clock_calls >= 2:
            stop.set()
        return datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI)

    asyncio.run(
        run_calendar_sync_loop(
            service,
            stop,
            clock=loop_clock,
            poll_seconds=0.001,
        )
    )

    assert len(provider.calls) == 1
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert store.state().last_attempt_at is None


def test_calendar_unclassified_failure_propagates_health_outcome_write_error(
    tmp_path: Path,
) -> None:
    class FailingOutcome(InMemoryProviderHealthStore):
        def record_terminal_failure(self, *args, **kwargs):
            del args, kwargs
            raise ProviderHealthError("token=/private/provider-health.sqlite3")

    class UnclassifiedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            del start_date, end_date
            with provider_session_scope("runtime-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=0,
                        response_bytes=0,
                        end_marker_seen=False,
                        provider_code=None,
                        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                    )
            raise RuntimeError("private-token provider failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        UnclassifiedProvider([]),
        health_store=FailingOutcome(failure_threshold=1),
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError):
        service.execute(plan)

    assert store.state().last_attempt_at is None


def test_calendar_success_without_operation_observation_fails_closed(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = InMemoryProviderHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        CalendarProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError, match="audit is incomplete"):
        service.execute(plan)

    assert store.state().last_attempt_at is None
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN


def test_calendar_loop_checks_startup_then_daily_slot(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 7, 24)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    stop = asyncio.Event()
    moments = iter(
        [
            datetime(2026, 7, 24, 9, 0, tzinfo=SHANGHAI),
            datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        ]
    )

    async def exercise() -> None:
        calls = 0

        def clock() -> datetime:
            nonlocal calls
            calls += 1
            if calls >= 2:
                stop.set()
            return next(moments)

        await run_calendar_sync_loop(
            service,
            stop,
            clock=clock,
            poll_seconds=0.001,
        )

    asyncio.run(exercise())

    assert len(provider.calls) == 2
    assert store.state().last_startup_slot_date == date(2026, 7, 24)
    assert store.state().last_light_slot_date == date(2026, 7, 24)


def test_calendar_sync_cli_defaults_to_network_free_plan(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync"])
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)

    def unexpected_provider(*args, **kwargs):
        raise AssertionError("dry-run calendar sync must not construct provider")

    monkeypatch.setattr(cli, "BaoStockProvider", unexpected_provider)

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["writes_calendar_state"] is False
    assert payload["network_requests"] == 0
    assert payload["execute_requires"] == "--execute"
    assert not (settings.local_control_dir / settings.calendar_sync_database_name).exists()
    assert not (settings.local_control_dir / settings.provider_health_database_name).exists()
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_calendar_sync_cli_execute_uses_persistent_health_gate_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
        provider_circuit_failure_threshold=1,
    )
    health = SQLiteProviderHealthStore(
        settings.local_control_dir / settings.provider_health_database_name,
        failure_threshold=1,
        cooldown_seconds=settings.provider_circuit_cooldown_seconds,
        probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
    )
    health.initialize()
    health.record_terminal_failure(
        "open-cli-calendar",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.RECV_TIMEOUT,
    )
    calls = 0

    class NoCallProvider:
        def __init__(self, **_kwargs) -> None:
            pass

        def trading_dates(self, *_args) -> list[date]:
            nonlocal calls
            calls += 1
            raise AssertionError("open circuit must skip calendar provider")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockProvider", NoCallProvider)
    monkeypatch.setattr(
        cli,
        "get_market_clock",
        lambda: lambda: datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "calendar-sync",
            "--mode",
            "light",
            "--start",
            "2026-07-24",
            "--end",
            "2026-07-24",
            "--execute",
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "skipped_circuit_open"
    assert payload["writes_calendar_state"] is False
    assert calls == 0


def test_market_status_exposes_calendar_maintenance_without_client_input(
    tmp_path: Path,
) -> None:
    market_store = MarketStore(tmp_path / "market.duckdb")
    sync_store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        sync_store,
        synthetic_calendar(),
        CalendarProvider([date(2026, 7, 23)]),
        clock=lambda: datetime(2026, 7, 24, 8, tzinfo=UTC),
    )
    result = service.execute(
        service.plan(
            now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 24),
            end_date=date(2026, 7, 24),
        )
    )
    app.dependency_overrides[get_market_store] = lambda: market_store
    app.dependency_overrides[get_calendar_sync_store] = lambda: sync_store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.get("/api/v1/market/status")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["calendar_status"] == "conflict"
    assert (
        datetime.fromisoformat(payload["calendar_last_sync_at"].replace("Z", "+00:00"))
        == result.completed_at
    )
    assert payload["calendar_conflict_detected"] is True
    assert (
        datetime.fromisoformat(payload["calendar_conflict_at"].replace("Z", "+00:00"))
        == result.completed_at
    )
    assert payload["calendar_next_sync_at"] is not None
