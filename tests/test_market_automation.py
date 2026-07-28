import asyncio
import importlib
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

import backend.app.cli as cli
from backend.app.api.market import get_market_store
from backend.app.config import get_settings
from backend.app.main import app
from backend.app.market.baostock import ProviderBatch
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.store import MarketStore
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
