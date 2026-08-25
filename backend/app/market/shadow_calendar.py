"""Offline, shadow-specific reader for reviewed confirmed calendar snapshots.

This intentionally has its own protocol and model.  The continuity calendar reader is
not a qualification authority and is never imported here.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ShadowCalendarUnavailable(RuntimeError):
    """Calendar evidence is missing, unknown, malformed, or changed."""


_TRUSTED_AUTHORITY_VERSION = "r2f3-calendar-authority-v1"
_TRUSTED_SOURCES = (
    ("SSE", "Shanghai Stock Exchange", "https://www.sse.com.cn/", None),
    ("SZSE", "Shenzhen Stock Exchange", "https://www.szse.cn/", None),
)


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

    @model_validator(mode="after")
    def validate_snapshot_hash(self) -> ConfirmedSessionSnapshot:
        values = self.model_dump(mode="json")
        values.pop("snapshot_sha256")
        if self.snapshot_sha256 != _sha(_canonical(values)):
            raise ValueError("confirmed session snapshot hash mismatch")
        return self


class ShadowCalendarSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange: Literal["SSE", "SZSE"]
    title: str
    url: str
    notice_no: str | None = None


class ShadowCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    year: int
    status: str
    published_on: date
    sources: tuple[ShadowCalendarSource, ...] = Field(min_length=1)
    closed_dates: tuple[date, ...]

    @model_validator(mode="after")
    def validate_trusted_sources(self) -> ShadowCalendarConfig:
        actual = tuple(
            (item.exchange, item.title, item.url, item.notice_no)
            for item in sorted(self.sources, key=lambda value: value.exchange)
        )
        if actual != _TRUSTED_SOURCES:
            raise ValueError("calendar sources are not trusted")
        return self


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
        universe_id: str,
        universe_sha256: str,
    ) -> None:
        self.calendar_root = Path(calendar_root)
        self.provider_id = provider_id
        self.window_id = window_id
        self.universe_id = universe_id
        self.universe_sha256 = universe_sha256
        if (
            not self.calendar_root.is_absolute()
            or provider_id not in {"tickflow", "tushare"}
            or not re.fullmatch(r"[0-9a-f]{64}", universe_sha256)
            or not self.calendar_root.is_dir()
        ):
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        try:
            root_fd = self._open_root()
        except OSError as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
        try:
            authority_fd = os.open(
                "calendar-manifest.json",
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=root_fd,
            )
            try:
                authority = self._read_fd(authority_fd)
            finally:
                os.close(authority_fd)
        except (OSError, ShadowCalendarUnavailable) as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
        finally:
            os.close(root_fd)
        try:
            payload = json.loads(authority.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"payload_sha256"}
            or not isinstance(payload.get("payload_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", payload["payload_sha256"])
        ):
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        self.payload_sha256 = payload["payload_sha256"]
        self.calendar_generation = f"{_TRUSTED_AUTHORITY_VERSION}-{self.payload_sha256[:16]}"

    def _open_root(self) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        current = os.open(os.sep, flags)
        try:
            for component in self.calendar_root.parts[1:]:
                next_fd = os.open(component, flags, dir_fd=current)
                os.close(current)
                current = next_fd
            return current
        except Exception:
            os.close(current)
            raise

    @staticmethod
    def _read_fd(fd: int, *, limit: int = 1_048_576) -> bytes:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        raw = os.pread(fd, limit + 1, 0)
        after = os.fstat(fd)

        def fingerprint(info):
            return (
                info.st_dev,
                info.st_ino,
                info.st_size,
                info.st_mtime_ns,
                getattr(info, "st_ctime_ns", 0),
            )

        if (
            len(raw) > limit
            or len(raw) != before.st_size
            or fingerprint(before) != fingerprint(after)
        ):
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        return raw

    def _read_relative(self, root_fd: int, name: str) -> bytes:
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=root_fd,
            )
            try:
                return self._read_fd(fd)
            finally:
                os.close(fd)
        except (OSError, ShadowCalendarUnavailable) as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc

    def _configs(self, years: set[int]) -> tuple[dict[str, Any], ...]:
        candidates = sorted(self.calendar_root.glob("cn_a_share_*.json"))
        if not candidates:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        try:
            root_fd = self._open_root()
        except OSError as exc:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
        raw_files: list[tuple[str, bytes]] = []
        try:
            for path in candidates:
                raw_files.append((path.name, self._read_relative(root_fd, path.name)))
        finally:
            os.close(root_fd)
        payload_bytes = b"".join(name.encode("utf-8") + b"\0" + raw for name, raw in raw_files)
        if _sha(payload_bytes) != self.payload_sha256:
            raise ShadowCalendarUnavailable("confirmed calendar unavailable")
        found: list[dict[str, Any]] = []
        for _name, raw in raw_files:
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
            values = payload if isinstance(payload, list) else [payload]
            if isinstance(payload, dict) and "configs" in payload:
                raise ShadowCalendarUnavailable("confirmed calendar unavailable")
            for item in values:
                if not isinstance(item, dict) or item.get("year") not in years:
                    continue
                try:
                    validated = ShadowCalendarConfig.model_validate(item)
                except (TypeError, ValueError) as exc:
                    raise ShadowCalendarUnavailable("confirmed calendar unavailable") from exc
                if validated.status != "confirmed":
                    raise ShadowCalendarUnavailable("confirmed calendar unavailable")
                found.append(validated.model_dump(mode="json"))
        if len(found) != len(years) or {int(item["year"]) for item in found} != years:
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
            "authority_version": _TRUSTED_AUTHORITY_VERSION,
            "payload_sha256": self.payload_sha256,
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


__all__ = [
    "ConfirmedCalendarReader",
    "ConfirmedSessionSnapshot",
    "ShadowCalendarConfig",
    "ShadowCalendarSource",
    "ShadowCalendarUnavailable",
]
