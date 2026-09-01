"""Read-only public status for the promoted calendar runtime.

This module is intentionally limited to the calendar control plane.  It does not
resolve general application settings, inspect a market store, create runtime
directories, or construct a provider client.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.config import CalendarRuntimeSettings

from .calendar import SHANGHAI
from .calendar_generation import CalendarGenerationStore, CalendarSourceBundleV1

CalendarRuntimeStatus = Literal["disabled", "ready", "unavailable"]
CalendarNextYearStatus = Literal["confirmed", "not_due", "pending", "action_required"]
CalendarRuntimeOutcome = Literal[
    "STAGED",
    "ALREADY_STAGED",
    "SOURCE_PENDING",
    "SOURCE_INVALID",
    "SOURCE_CONFLICT",
    "OFFICIAL_UNAVAILABLE",
    "OFFICIAL_HASH_MISMATCH",
    "MACHINE_UNAVAILABLE",
    "MACHINE_CONFLICT",
    "SKIPPED_CIRCUIT_OPEN",
    "ALREADY_ATTEMPTED",
    "PARENT_CHANGED",
    "HISTORY_CHANGE",
    "CALENDAR_AUTHORITY_CHANGED",
    "CONTROL_STATE_UNAVAILABLE",
    "NOT_DUE",
    "PROMOTED",
]


class CalendarGenerationStatusV1(BaseModel):
    """The exact, bounded public generation-runtime contract."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        allow_inf_nan=False,
    )

    schema_version: Literal[1] = 1
    runtime_enabled: bool
    status: CalendarRuntimeStatus
    generation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    covered_years: tuple[int, ...] = ()
    covered_through: str | None = Field(default=None, pattern=r"^[0-9]{4}-12-31$")
    current_year_ready: bool
    next_year: int = Field(ge=1900, le=9998)
    next_year_status: CalendarNextYearStatus
    last_outcome: CalendarRuntimeOutcome | None = None
    conflict_detected: bool
    provider_requests: Literal[0] = 0
    canonical_writes: Literal[False] = False

    @model_validator(mode="after")
    def _validate_projection(self) -> CalendarGenerationStatusV1:
        if tuple(sorted(set(self.covered_years))) != self.covered_years:
            raise ValueError("covered years must be sorted and unique")
        if any(type(year) is not int or year < 1900 or year > 9998 for year in self.covered_years):
            raise ValueError("covered year is invalid")
        if self.covered_years:
            expected_through = f"{self.covered_years[-1]:04d}-12-31"
            if self.covered_through != expected_through:
                raise ValueError("covered-through does not match coverage")
        elif self.covered_through is not None:
            raise ValueError("empty coverage cannot have covered-through")
        if self.status == "disabled" and self.runtime_enabled:
            raise ValueError("disabled status requires disabled runtime")
        if self.status == "disabled" and (
            self.generation_sha256 is not None
            or self.last_outcome is not None
            or self.conflict_detected
        ):
            raise ValueError("disabled runtime cannot expose runtime control state")
        if self.status != "disabled" and not self.runtime_enabled:
            raise ValueError("enabled status requires enabled runtime")
        if self.status == "unavailable" and (
            self.generation_sha256 is not None
            or self.covered_years
            or self.covered_through is not None
            or self.current_year_ready
            or self.last_outcome is not None
            or self.conflict_detected
        ):
            raise ValueError("unavailable runtime must be empty")
        if self.current_year_ready and not self.covered_years:
            raise ValueError("current year cannot be ready without coverage")
        return self


def _aware_now(value: datetime | None) -> datetime:
    value = datetime.now(UTC) if value is None else value
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("calendar runtime clock must be timezone-aware")
    return value.astimezone(UTC)


def _empty_status(
    *, enabled: bool, next_year: int, next_year_status: CalendarNextYearStatus = "not_due"
) -> CalendarGenerationStatusV1:
    return CalendarGenerationStatusV1(
        runtime_enabled=enabled,
        status="unavailable" if enabled else "disabled",
        generation_sha256=None,
        covered_years=(),
        covered_through=None,
        current_year_ready=False,
        next_year=next_year,
        next_year_status=next_year_status,
        last_outcome=None,
        conflict_detected=False,
    )


def _last_outcome(control: object) -> CalendarRuntimeOutcome | None:
    attempts = tuple(getattr(control, "attempts", ()))
    candidates = tuple(getattr(control, "candidates", ()))
    latest_attempt = max(
        attempts,
        key=lambda item: (item.started_at, item.target_year, item.slot_date),
        default=None,
    )
    latest_candidate = max(candidates, key=lambda item: item.staging_sequence, default=None)
    if latest_attempt is None:
        if latest_candidate is None:
            return None
        return "SOURCE_CONFLICT" if latest_candidate.admission == "quarantined" else "STAGED"
    if (
        latest_candidate is not None
        and latest_candidate.source_sha256 != latest_attempt.source_sha256
        and latest_candidate.staged_at >= latest_attempt.started_at
    ):
        return "SOURCE_CONFLICT" if latest_candidate.admission == "quarantined" else "STAGED"
    outcome = getattr(latest_attempt, "outcome", None)
    if outcome == "RUNNING":
        return "ALREADY_ATTEMPTED"
    return outcome if outcome in CalendarRuntimeOutcome.__args__ else None


def _latest_candidate_conflict(control: object) -> bool:
    """Report conflict only for each year's latest candidate."""
    latest_by_year: dict[int, object] = {}
    for candidate in getattr(control, "candidates", ()):
        source = CalendarSourceBundleV1.model_validate_json(candidate.canonical_source_bytes)
        prior = latest_by_year.get(source.year)
        if prior is None or candidate.staging_sequence > prior.staging_sequence:
            latest_by_year[source.year] = candidate
    return any(candidate.admission == "quarantined" for candidate in latest_by_year.values())


def _next_year_policy(
    control: object,
    *,
    covered_years: tuple[int, ...],
    local: datetime,
) -> CalendarNextYearStatus:
    """Derive policy from the same verified control snapshot used for coverage."""
    current_year = local.year
    if current_year not in covered_years:
        return "not_due"
    next_year = current_year + 1
    active = {
        source.year: source.source_sha256 for source in getattr(control, "promoted_sources", ())
    }
    latest: tuple[object, CalendarSourceBundleV1] | None = None
    for candidate in getattr(control, "candidates", ()):
        source = CalendarSourceBundleV1.model_validate_json(candidate.canonical_source_bytes)
        if source.year == next_year and (
            latest is None or candidate.staging_sequence > latest[0].staging_sequence
        ):
            latest = (candidate, source)
    if latest is not None and latest[0].admission == "quarantined":
        if local.date() < date(current_year, 10, 1):
            return "not_due"
        if local.date() < date(current_year, 12, 15):
            return "pending"
        return "action_required"
    if next_year in covered_years:
        # A listed bundled year is already confirmed.  A candidate is only
        # relevant when it is a revision of an active promoted year.
        if latest is None or active.get(next_year) == latest[1].source_sha256:
            return "confirmed"
    if local.date() < date(current_year, 10, 1):
        return "not_due"
    if local.date() < date(current_year, 12, 15):
        return "pending"
    return "action_required"


def build_calendar_generation_status(
    settings: CalendarRuntimeSettings,
    *,
    now: datetime | None = None,
) -> CalendarGenerationStatusV1:
    """Build a strict status from one verified existing-only control snapshot."""
    if not isinstance(settings, CalendarRuntimeSettings):
        raise TypeError("calendar runtime settings are required")
    effective = _aware_now(now)
    local = effective.astimezone(SHANGHAI)
    if not settings.calendar_runtime_enabled:
        from .calendar import _load_bundled_snapshot

        bundled = _load_bundled_snapshot()
        covered_years = tuple(sorted(int(year) for year in bundled.configs))
        return CalendarGenerationStatusV1(
            runtime_enabled=False,
            status="disabled",
            generation_sha256=None,
            covered_years=covered_years,
            covered_through=(f"{covered_years[-1]:04d}-12-31" if covered_years else None),
            current_year_ready=local.year in covered_years,
            next_year=local.year + 1,
            next_year_status=_next_year_policy(
                (),
                covered_years=covered_years,
                local=local,
            ),
            last_outcome=None,
            conflict_detected=False,
        )

    store = CalendarGenerationStore(
        settings.local_control_dir / settings.calendar_generation_database_name
    )
    control = store.read_control()
    read_result = getattr(control, "read_result", None)
    if (
        read_result is None
        or getattr(read_result, "status", None) != "ready"
        or getattr(read_result, "calendar", None) is None
    ):
        return _empty_status(enabled=True, next_year=local.year + 1)

    calendar = read_result.calendar
    covered_years = tuple(sorted(int(year) for year in calendar.configs))
    current_ready = local.year in covered_years
    generation = read_result.generation
    conflict = _latest_candidate_conflict(control)
    policy = _next_year_policy(control, covered_years=covered_years, local=local)
    status = CalendarGenerationStatusV1(
        runtime_enabled=True,
        status="ready",
        generation_sha256=(generation.generation_sha256 if generation is not None else None),
        covered_years=covered_years,
        covered_through=(f"{covered_years[-1]:04d}-12-31" if covered_years else None),
        current_year_ready=current_ready,
        next_year=local.year + 1,
        next_year_status=policy,
        last_outcome=_last_outcome(control),
        conflict_detected=conflict,
    )
    # Public status must be revalidated, including values derived from a control reader.
    return CalendarGenerationStatusV1.model_validate(status.model_dump(mode="python"))


__all__ = [
    "CalendarGenerationStatusV1",
    "build_calendar_generation_status",
]
