"""Offline, shadow-specific reader for reviewed confirmed calendar snapshots.

This intentionally has its own protocol and model.  The continuity calendar reader is
not a qualification authority and is never imported here.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ShadowCalendarUnavailable(RuntimeError):
    """Calendar evidence is missing, unknown, malformed, or changed."""


class ConfirmedSessionSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str
    provider_id: str
    window_id: str
    calendar_generation: str
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    confirmed_next_sessions: tuple[date, ...]
    universe_id: str
    universe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    captured_at: str
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class ConfirmedCalendarReader:
    """Read exact CalendarConfig JSON without creating or mutating anything."""

    def __init__(
        self,
        calendar_root: Path | str,
        *,
        provider_id: str,
        window_id: str,
        calendar_generation: str,
        universe_id: str,
        universe_sha256: str,
        official_metadata: str = "official-calendar-config",
    ) -> None:
        self.calendar_root = Path(calendar_root)
        self.provider_id = provider_id
        self.window_id = window_id
        self.calendar_generation = calendar_generation
        self.universe_id = universe_id
        self.universe_sha256 = universe_sha256
        self.official_metadata = official_metadata
        if not self.calendar_root.is_absolute() or len(universe_sha256) != 64:
            raise ValueError("shadow calendar descriptor is invalid")

    def _read_file(self, path: Path) -> bytes:
        try:
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise ShadowCalendarUnavailable("confirmed calendar unavailable")
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                before = os.fstat(fd)
                raw = os.read(fd, 1_048_577)
                after = os.fstat(fd)
            finally:
                os.close(fd)
            if len(raw) > 1_048_576 or (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise ShadowCalendarUnavailable("confirmed calendar unavailable")
            return raw
        except (OSError, ValueError) as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc

    def _configs(self, years: set[int]) -> tuple[dict[str, Any], ...]:
        if self.calendar_root.is_file():
            candidates = [self.calendar_root]
        elif self.calendar_root.is_dir():
            candidates = sorted(self.calendar_root.glob("cn_a_share_*.json"))
        else:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        found: list[dict[str, Any]] = []
        for path in candidates:
            try:
                payload = json.loads(self._read_file(path).decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
            values = payload if isinstance(payload, list) else [payload]
            # Reviewed dumps may carry one explicit authority envelope.  Preserve the
            # envelope in the generation hash, while validating the same CalendarConfig
            # objects as the canonical reader.
            if isinstance(payload, dict) and "configs" in payload:
                envelope = payload
                values = envelope["configs"]
                if not isinstance(values, list):
                    raise ShadowCalendarUnavailable("confirmed calendar unavailable")
            for item in values:
                if not isinstance(item, dict) or item.get("year") not in years:
                    continue
                if item.get("status") != "confirmed" or set(item) != {
                    "year",
                    "status",
                    "published_on",
                    "sources",
                    "closed_dates",
                }:
                    raise ShadowCalendarUnavailable("confirmed calendar unavailable")
                if not isinstance(item["sources"], list) or not isinstance(
                    item["closed_dates"], list
                ):
                    raise ShadowCalendarUnavailable("confirmed calendar unavailable")
                found.append(item)
        if {int(item["year"]) for item in found} != years:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        return tuple(sorted(found, key=lambda item: int(item["year"])))

    def read(
        self,
        start: date,
        end: date,
        *,
        captured_at: str = "offline",
        snapshot_id: str | None = None,
    ) -> ConfirmedSessionSnapshot:
        if end < start:
            raise ValueError("calendar interval is invalid")
        configs = self._configs(set(range(start.year, end.year + 1)))
        closed = {date.fromisoformat(item) for config in configs for item in config["closed_dates"]}
        sessions: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5 and current not in closed:
                sessions.append(current)
            current += timedelta(days=1)
        calendar_payload = {
            "metadata": self.official_metadata,
            "generation": self.calendar_generation,
            "configs": configs,
        }
        calendar_sha = _sha(_canonical(calendar_payload))
        sid = snapshot_id or f"snapshot-{self.provider_id}-{start.isoformat()}-{end.isoformat()}"
        snapshot_payload = {
            "snapshot_id": sid,
            "provider_id": self.provider_id,
            "window_id": self.window_id,
            "calendar_generation": self.calendar_generation,
            "calendar_sha256": calendar_sha,
            "confirmed_next_sessions": [item.isoformat() for item in sessions],
            "universe_id": self.universe_id,
            "universe_sha256": self.universe_sha256,
            "captured_at": captured_at,
        }
        return ConfirmedSessionSnapshot(
            **snapshot_payload,
            snapshot_sha256=_sha(_canonical(snapshot_payload)),
        )
