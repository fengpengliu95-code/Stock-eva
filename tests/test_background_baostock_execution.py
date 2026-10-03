import asyncio
import multiprocessing
import time
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.blocking import run_blocking_process
from backend.app.market.automation import run_automation_loop
from backend.app.market.baostock import BaoStockError, BaoStockProvider
from backend.app.market.calendar_sync import run_calendar_sync_loop


class _Result:
    error_code = "0"
    error_msg = "success"
    fields = ["calendar_date", "is_trading_day"]

    def __init__(self) -> None:
        self._rows = iter([["2026-07-23", "1"]])

    def next(self) -> bool:
        try:
            self._current = next(self._rows)
        except StopIteration:
            return False
        return True

    def get_row_data(self):
        return self._current


class _SyntheticBaoStockTransport:
    def login(self):
        return type("Login", (), {"error_code": "0", "error_msg": "success"})()

    def logout(self):
        return None

    def query_trade_dates(self, **_kwargs):
        return _Result()


class _StalledBaoStockTransport(_SyntheticBaoStockTransport):
    def query_trade_dates(self, **_kwargs):
        time.sleep(10)
        raise AssertionError("wall-clock deadline did not interrupt the transport")


class _BackgroundRefresh:
    def __init__(self, marker: Path) -> None:
        self.marker = marker
        self.provider = BaoStockProvider(
            client=_SyntheticBaoStockTransport(),
            max_attempts=1,
            min_request_interval_seconds=0,
            socket_timeout_seconds=0.2,
        )

    def run_due_once(self, _now: datetime) -> None:
        assert self.provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23)) == [
            date(2026, 7, 23)
        ]
        self.marker.write_text("complete")


class _CalendarPlan:
    pass


class _BackgroundCalendar:
    def __init__(self, marker: Path) -> None:
        self.marker = marker
        self.provider = BaoStockProvider(
            client=_SyntheticBaoStockTransport(),
            max_attempts=1,
            min_request_interval_seconds=0,
            socket_timeout_seconds=0.2,
        )

    def initialize_for_execution(self) -> None:
        return None

    def plan(self, **_kwargs):
        return _CalendarPlan()

    def execute(self, _plan):
        assert self.provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23)) == [
            date(2026, 7, 23)
        ]
        self.marker.write_text("complete")
        return object()


def _block_forever() -> None:
    while True:
        time.sleep(1)


def test_lifespan_background_refresh_runs_real_wrapper_without_blocking_health(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "refresh-complete"

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_automation_loop(
                _BackgroundRefresh(marker),
                stop,
                clock=lambda: datetime(2026, 7, 23, tzinfo=UTC),
                poll_seconds=60,
                blocking_runner=run_blocking_process,
            )
        )
        try:
            yield
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    with TestClient(app) as client:
        started = time.monotonic()
        assert client.get("/health").json() == {"status": "ok"}
        assert time.monotonic() - started < 0.5
        deadline = time.monotonic() + 2
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.read_text() == "complete"


def test_calendar_only_background_worker_runs_real_wrapper(tmp_path: Path) -> None:
    async def exercise() -> None:
        marker = tmp_path / "calendar-complete"
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_calendar_sync_loop(
                _BackgroundCalendar(marker),
                stop,
                clock=lambda: datetime(2026, 7, 23, tzinfo=UTC),
                poll_seconds=60,
                blocking_runner=run_blocking_process,
            )
        )
        deadline = time.monotonic() + 2
        while not marker.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert marker.read_text() == "complete"

    asyncio.run(exercise())


def test_cancelling_blocking_child_is_bounded_and_reaps_process() -> None:
    async def exercise() -> None:
        before = {child.pid for child in multiprocessing.active_children()}
        task = asyncio.create_task(run_blocking_process(_block_forever, cancel_grace_seconds=0.05))
        await asyncio.sleep(0.05)
        started = time.monotonic()
        task.cancel()
        result = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert time.monotonic() - started < 0.5
        assert {child.pid for child in multiprocessing.active_children()} == before

    asyncio.run(exercise())


def test_background_baostock_deadline_is_bounded_and_reaps_process() -> None:
    async def exercise() -> None:
        provider = BaoStockProvider(
            client=_StalledBaoStockTransport(),
            max_attempts=1,
            min_request_interval_seconds=0,
            socket_timeout_seconds=0.05,
        )
        before = {child.pid for child in multiprocessing.active_children()}
        started = time.monotonic()
        try:
            await run_blocking_process(
                provider.trading_dates,
                date(2026, 7, 23),
                date(2026, 7, 23),
            )
        except BaoStockError as error:
            assert "deadline exceeded" in str(error)
        else:
            raise AssertionError("the stalled synthetic transport must time out")
        assert time.monotonic() - started < 1
        assert {child.pid for child in multiprocessing.active_children()} == before

    asyncio.run(exercise())
