"""Strict, read-only projection of the persisted exact-session universe sidecar.

This module deliberately has no provider, UserStore, calendar or writer dependencies.  The
sidecar is the only authority for the public status projection; a missing or unverifiable sidecar
is an independent control error and is never silently treated as an empty universe.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any

from backend.app.market.universe import (
    INDEX_SYMBOLS,
    UniverseAttemptPlanV1,
    UniverseAttemptResultV1,
    UniverseContractV1,
    UniverseSidecarStore,
)

_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_PUBLIC_REASONS = frozenset(
    {
        "CONTROL_STATE_UNAVAILABLE",
        "PIT_VISIBILITY_INVALID",
        "CALENDAR_UNAVAILABLE",
        "CALENDAR_CONFLICT",
        "CLASSIFICATION_UNAVAILABLE",
        "USER_STORE_UNAVAILABLE",
        "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
        "PIT_CUTOFF_VIOLATION",
        "USER_SNAPSHOT_CHANGED",
        "REQUIRED_INDEX_NOT_TRADING",
        "REQUIRED_SYMBOL_INVALID",
        "UNIVERSE_UNKNOWN_NONZERO",
        "UNIVERSE_COUNT_MISMATCH",
        "UNIVERSE_MISSING_SYMBOL",
        "UNIVERSE_EXTRA_SYMBOL",
        "UNIVERSE_DUPLICATE_SYMBOL",
        "UNIVERSE_SESSION_DRIFT",
        "UNIVERSE_STATE_MISMATCH",
        "UNIVERSE_SOURCE_VERSION_CHANGED",
        "DATE_MISMATCH",
        "UNIVERSE_STORAGE_UNAVAILABLE",
        "UNIVERSE_SCHEMA_MISMATCH",
        "UNIVERSE_HEAD_CAS_CONFLICT",
        "BLOCKED_ENFORCE_NOT_ENABLED",
        "BLOCKED_PRODUCTION_MODE_OFF",
        "LEGACY_SHADOW_DRIFT",
        "ATTEMPT_INDETERMINATE",
        "UNIVERSE_IDENTITY_CONFLICT",
        "NONE",
    }
)


class UniverseStatusControlError(RuntimeError):
    """The sidecar cannot be proven safe for a public read-only status."""


class UniverseStatusDateError(ValueError):
    """The requested date is invalid or not yet visible at the trusted clock."""


def parse_universe_trade_date(value: str) -> date:
    """Parse only the exact public ``YYYY-MM-DD`` lexical contract.

    This helper performs no filesystem or environment access, allowing the API and CLI to
    reject malformed input before constructing a sidecar reader.
    """
    if not isinstance(value, str) or _DATE_RE.fullmatch(value) is None:
        raise UniverseStatusDateError("PIT_VISIBILITY_INVALID")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise UniverseStatusDateError("PIT_VISIBILITY_INVALID") from exc


def _safe_reason(value: str | None, *, fallback: str = "CONTROL_STATE_UNAVAILABLE") -> str:
    return value if value in _PUBLIC_REASONS else fallback


def _base(trade_date: date) -> dict[str, Any]:
    return {
        "trade_date": trade_date.isoformat(),
        "universe_id": "all-main-board-plus-required-symbols",
        "schema_version": 1,
        "scope": "all-main-board-plus-required-symbols",
        "provider_requests": 0,
        "writes": False,
    }


def _empty(base: dict[str, Any], *, status: str, reason: str) -> dict[str, Any]:
    return {
        **base,
        "status": status,
        "verified_head_trade_date": None,
        "contract_id": None,
        "contract_sha256": None,
        "source_state_id": None,
        "source_state_sha256": None,
        "source_version_digest": None,
        "classification_generation_id": None,
        "calendar_generation_id": None,
        "calendar_sha256": None,
        "counts": None,
        "layers": None,
        "required_indexes": None,
        "required_user_symbol_count": None,
        "publication_eligible": False,
        "reason_code": reason,
    }


def _head_fields(base: dict[str, Any], contract: UniverseContractV1) -> dict[str, Any]:
    refs = contract.source_refs
    return {
        **base,
        "verified_head_trade_date": contract.trade_date.isoformat(),
        "contract_id": contract.contract_id,
        "contract_sha256": contract.contract_sha256,
        "source_state_id": contract.source_state_id,
        "source_state_sha256": contract.source_state_sha256,
        "source_version_digest": refs.source_version_digest,
        "classification_generation_id": refs.classification_generation_id,
        "calendar_generation_id": refs.calendar_generation_id,
        "calendar_sha256": refs.calendar_sha256,
        "counts": contract.counts.model_dump(mode="json"),
        "layers": {
            "classification_evidence_count": contract.classification_evidence_count,
            "classification_evidence_sha256": contract.classification_evidence_sha256,
            "effective_main_board_count": contract.effective_main_board_count,
            "effective_main_board_sha256": contract.effective_main_board_sha256,
            "required_additions_count": contract.required_additions_count,
            "required_additions_sha256": contract.required_additions_sha256,
        },
        "required_indexes": list(INDEX_SYMBOLS),
        "required_user_symbol_count": sum(
            "required_user" in member.scope_roles for member in contract.members
        ),
    }


def _status_with_head(
    base: dict[str, Any], contract: UniverseContractV1, *, status: str, reason: str
) -> dict[str, Any]:
    return {
        **_head_fields(base, contract),
        "status": status,
        "publication_eligible": status == "ready" and contract.publication_eligible,
        "reason_code": reason,
    }


def _attempt_sort_key(
    item: tuple[UniverseAttemptPlanV1, UniverseAttemptResultV1 | None],
) -> tuple[datetime, str]:
    return item[0].created_at, item[0].attempt_id


def _matching_attempt(
    attempts: tuple[tuple[UniverseAttemptPlanV1, UniverseAttemptResultV1 | None], ...],
    source_version_digest: str,
) -> tuple[UniverseAttemptPlanV1, UniverseAttemptResultV1 | None] | None:
    matching = [item for item in attempts if item[0].source_version_digest == source_version_digest]
    return max(matching, key=_attempt_sort_key) if matching else None


def _terminal_status(
    base: dict[str, Any],
    attempt: tuple[UniverseAttemptPlanV1, UniverseAttemptResultV1 | None],
) -> dict[str, Any] | None:
    _, result = attempt
    if result is None:
        return _empty(base, status="blocked", reason="ATTEMPT_INDETERMINATE")
    if result.terminal_status == "succeeded":
        return None
    if result.terminal_status == "ATTEMPT_INDETERMINATE":
        reason = "ATTEMPT_INDETERMINATE"
    else:
        reason = _safe_reason(result.reason_code)
    return _empty(base, status="blocked", reason=reason)


def build_universe_status(
    trade_date: date,
    sidecar: UniverseSidecarStore,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build one deterministic status from strict sidecar rows only.

    The sidecar reader runs before the future-date check.  This ordering is intentional: a bad
    control store must remain a 503/exit-3 condition even when the requested date is also future.
    """
    base = _base(trade_date)
    try:
        head, contract, head_source, source_rows, attempts = sidecar.read_status_snapshot(
            trade_date
        )
    except Exception as exc:
        raise UniverseStatusControlError from exc

    clock = (now or datetime.now(UTC)).astimezone(UTC)
    if trade_date > clock.date():
        raise UniverseStatusDateError("PIT_VISIBILITY_INVALID")

    if head is None or contract is None or head_source is None:
        if not attempts:
            return _empty(base, status="unavailable", reason="CONTROL_STATE_UNAVAILABLE")
        latest = max(attempts, key=_attempt_sort_key)
        if latest[1] is not None and latest[1].terminal_status == "succeeded":
            raise UniverseStatusControlError
        terminal = _terminal_status(base, latest)
        if terminal is not None:
            return terminal
        raise UniverseStatusControlError

    if contract.trade_date != trade_date:
        return _status_with_head(base, contract, status="stale", reason="DATE_MISMATCH")

    latest_source = (
        max(source_rows, key=lambda row: (row.verified_at, row.source_state_id))
        if source_rows
        else None
    )
    if (
        latest_source is None
        or latest_source.verified_at <= head_source.verified_at
        or latest_source.source_version_digest == contract.source_refs.source_version_digest
    ):
        return _status_with_head(base, contract, status="ready", reason="NONE")

    matched = _matching_attempt(attempts, latest_source.source_version_digest)
    if matched is None:
        return _status_with_head(
            base, contract, status="stale", reason="UNIVERSE_SOURCE_VERSION_CHANGED"
        )
    terminal = _terminal_status(base, matched)
    if terminal is not None:
        return terminal
    # A successful source acquisition must atomically promote the corresponding head.  Seeing a
    # success without that exact head is a control/schema violation, never a stale success.
    raise UniverseStatusControlError
