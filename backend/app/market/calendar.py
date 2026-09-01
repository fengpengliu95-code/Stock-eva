"""Fail-closed Shanghai/Shenzhen trading calendar."""

import json
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel

SHANGHAI = ZoneInfo("Asia/Shanghai")
DATA_AVAILABLE_AT = time(18, 10)
MARKET_OPEN_AT = time(9, 30)
MARKET_CLOSE_AT = time(15, 0)

SessionStatus = Literal["open", "closed", "unknown"]
MarketPhase = Literal[
    "pre_market",
    "market_open",
    "after_close_waiting",
    "closed",
    "calendar_unavailable",
]


class CalendarSource(BaseModel):
    exchange: Literal["SSE", "SZSE"]
    title: str
    url: str
    notice_no: str | None = None


class CalendarConfig(BaseModel):
    year: int
    status: Literal["confirmed"]
    published_on: date
    sources: list[CalendarSource]
    closed_dates: list[date]


class TradingCalendar:
    def __init__(self, configs: list[CalendarConfig]) -> None:
        self.configs = {item.year: item for item in configs}

    def snapshot(self) -> "TradingCalendar":
        """Return this concrete calendar for operation-boundary pinning."""
        return self

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "TradingCalendar":
        return cls([CalendarConfig.model_validate(payload)])

    @classmethod
    def from_path(cls, path: Path) -> "TradingCalendar":
        payload = json.loads(path.read_text())
        raw_configs = payload if isinstance(payload, list) else [payload]
        return cls([CalendarConfig.model_validate(item) for item in raw_configs])

    def session_status(self, value: date) -> SessionStatus:
        config = self.configs.get(value.year)
        if config is None:
            return "unknown"
        if value.weekday() >= 5 or value in config.closed_dates:
            return "closed"
        return "open"

    def confirmed_open_sessions(self, start: date, end: date) -> tuple[date, ...] | None:
        """Return inclusive confirmed opens, or no result if any day is unknown."""
        current = start
        opened: list[date] = []
        while current <= end:
            status = self.session_status(current)
            if status == "unknown":
                return None
            if status == "open":
                opened.append(current)
            current += timedelta(days=1)
        return tuple(opened)

    def previous_session(self, value: date) -> date | None:
        current = value - timedelta(days=1)
        for _ in range(370):
            status = self.session_status(current)
            if status == "open":
                return current
            if status == "unknown":
                return None
            current -= timedelta(days=1)
        return None

    def latest_expected_session(self, now: datetime) -> date | None:
        local = now.astimezone(SHANGHAI)
        today_status = self.session_status(local.date())
        if today_status == "unknown":
            return None
        if today_status == "open" and local.time() >= DATA_AVAILABLE_AT:
            return local.date()
        if today_status == "open":
            return self.previous_session(local.date())
        current = local.date()
        for _ in range(370):
            current -= timedelta(days=1)
            status = self.session_status(current)
            if status == "open":
                return current
            if status == "unknown":
                return None
        return None

    def market_phase(self, now: datetime) -> MarketPhase:
        local = now.astimezone(SHANGHAI)
        status = self.session_status(local.date())
        if status == "unknown":
            return "calendar_unavailable"
        if status == "closed":
            return "closed"
        if local.time() < MARKET_OPEN_AT:
            return "pre_market"
        if local.time() < MARKET_CLOSE_AT:
            return "market_open"
        return "after_close_waiting"

    @property
    def status(self) -> Literal["confirmed", "unavailable"]:
        return "confirmed" if self.configs else "unavailable"

    def sources_for(self, year: int) -> list[CalendarSource]:
        config = self.configs.get(year)
        return [] if config is None else config.sources


CALENDAR_PATH = Path(__file__).with_name("calendars") / "cn_a_share_2026.json"
CALENDAR_DIRECTORY = CALENDAR_PATH.parent


@lru_cache
def _load_bundled_snapshot() -> TradingCalendar:
    # The generation module is intentionally imported lazily: it depends on
    # TradingCalendar for its immutable snapshot type.
    from .calendar_generation import (
        CalendarConfigSnapshot,
        CalendarSourceMetadata,
        ImmutableCalendarSnapshot,
        bundled_sha256,
    )

    payloads: list[dict[str, object]] = []
    configs: list[CalendarConfig] = []
    for path in sorted(CALENDAR_DIRECTORY.glob("cn_a_share_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_configs = payload if isinstance(payload, list) else [payload]
        payloads.extend(raw_configs)
        configs.extend(CalendarConfig.model_validate(item) for item in raw_configs)
    snapshots = tuple(
        CalendarConfigSnapshot(
            year=config.year,
            status="confirmed",
            published_on=config.published_on,
            sources=tuple(
                CalendarSourceMetadata(
                    exchange=source.exchange,
                    title=source.title,
                    url=source.url,
                    notice_no=source.notice_no,
                )
                for source in config.sources
            ),
            closed_dates=tuple(config.closed_dates),
        )
        for config in sorted(configs, key=lambda item: item.year)
    )
    return ImmutableCalendarSnapshot(
        snapshots,
        bundled_sha256=bundled_sha256(tuple(payloads)),
    )


class LiveTradingCalendar(TradingCalendar):
    """A long-lived facade that reads runtime authority at every boundary."""

    def __init__(self, settings) -> None:
        from backend.app.config import CalendarRuntimeSettings, Settings

        if isinstance(settings, Settings):
            settings = CalendarRuntimeSettings.from_settings(settings)
        if not isinstance(settings, CalendarRuntimeSettings):
            raise TypeError("settings must be CalendarRuntimeSettings or Settings")
        self.settings = settings

    @property
    def configs(self):
        # Compatibility for existing readers that inspect a frozen concrete
        # map. New operations should use snapshot() explicitly.
        return self.snapshot().configs

    def snapshot(self) -> TradingCalendar:
        if not self.settings.calendar_runtime_enabled:
            return _load_bundled_snapshot()
        from .calendar_generation import EMPTY_CALENDAR, CalendarGenerationStore

        result = CalendarGenerationStore(
            self.settings.local_control_dir / self.settings.calendar_generation_database_name
        ).read()
        if result.status != "ready" or result.calendar is None:
            return EMPTY_CALENDAR
        return result.calendar

    def session_status(self, value: date) -> SessionStatus:
        return self.snapshot().session_status(value)

    def confirmed_open_sessions(self, start: date, end: date) -> tuple[date, ...] | None:
        return self.snapshot().confirmed_open_sessions(start, end)

    def previous_session(self, value: date) -> date | None:
        return self.snapshot().previous_session(value)

    def latest_expected_session(self, now: datetime) -> date | None:
        return self.snapshot().latest_expected_session(now)

    def market_phase(self, now: datetime) -> MarketPhase:
        return self.snapshot().market_phase(now)

    @property
    def status(self) -> Literal["confirmed", "unavailable"]:
        return self.snapshot().status

    def sources_for(self, year: int) -> list[CalendarSource]:
        return self.snapshot().sources_for(year)


def build_live_trading_calendar(settings) -> TradingCalendar:
    """Project already-resolved settings into a long-lived live facade."""
    return LiveTradingCalendar(settings)


def get_trading_calendar() -> TradingCalendar:
    """Return the default env-only facade without exposing dependency inputs."""
    from backend.app.config import get_calendar_runtime_settings

    return build_live_trading_calendar(get_calendar_runtime_settings())
