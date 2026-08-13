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
def get_trading_calendar() -> TradingCalendar:
    configs: list[CalendarConfig] = []
    for path in sorted(CALENDAR_DIRECTORY.glob("cn_a_share_*.json")):
        payload = json.loads(path.read_text())
        raw_configs = payload if isinstance(payload, list) else [payload]
        configs.extend(CalendarConfig.model_validate(item) for item in raw_configs)
    return TradingCalendar(configs)
