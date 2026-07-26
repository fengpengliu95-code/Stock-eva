import asyncio
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

import backend.app.cli as cli
from backend.app.api.market import get_calendar_sync_store, get_market_store
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.calendar import TradingCalendar, get_trading_calendar
from backend.app.market.calendar_sync import (
    CalendarSyncPolicy,
    CalendarSyncService,
    CalendarSyncStore,
    run_calendar_sync_loop,
)
from backend.app.market.store import MarketStore

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
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync"])
    settings = Settings(
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
