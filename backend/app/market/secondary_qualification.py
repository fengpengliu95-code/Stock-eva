"""Offline, fail-closed whole-session qualification harness for secondary providers."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, computed_field

_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_SYMBOL_PATTERN = r"^(sh|sz)\.[0-9]{6}$"
_VECTOR_DOMAIN = "stock-eva/r2f5.1/canonical-qualification-version-vector/v1"
_CANDIDATE_DOMAIN = "stock-eva/r2f5.1/session-candidate/v1"


def _digest(domain: str, value: object) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude={"version_vector_sha256"})
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(domain.encode("ascii") + b"\n" + payload).hexdigest()


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CanonicalQualificationVersionVectorV1(_Frozen):
    adapter_sha256: str = Field(pattern=_SHA256_PATTERN)
    endpoint_sha256: str = Field(pattern=_SHA256_PATTERN)
    schema_sha256: str = Field(pattern=_SHA256_PATTERN)
    normalizer_sha256: str = Field(pattern=_SHA256_PATTERN)
    reconciliation_sha256: str = Field(pattern=_SHA256_PATTERN)
    calendar_sha256: str = Field(pattern=_SHA256_PATTERN)
    universe_sha256: str = Field(pattern=_SHA256_PATTERN)
    terms_sha256: str = Field(pattern=_SHA256_PATTERN)
    quota_sha256: str = Field(pattern=_SHA256_PATTERN)

    @computed_field
    @property
    def version_vector_sha256(self) -> str:
        return _digest(_VECTOR_DOMAIN, self)


class QualificationSessionRequestV1(_Frozen):
    provider_id: Literal["tushare"]
    session_date: date
    expected_symbols: tuple[str, ...]
    required_indexes: tuple[str, ...]
    previous_adjust_factors: dict[str, float]
    version_vector: CanonicalQualificationVersionVectorV1
    max_attempts: Literal[1] = 1


class RawDailyBarV1(_Frozen):
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    source_provider: Literal["tickflow", "tushare"]
    volume_lots: float = Field(ge=0, allow_inf_nan=False)
    amount_thousand_cny: float = Field(ge=0, allow_inf_nan=False)


class QualificationFetchResultV1(_Frozen):
    provider_id: Literal["tushare"]
    session_date: date
    bars: tuple[RawDailyBarV1, ...]
    suspended_symbols: tuple[str, ...]
    session_universe: tuple[str, ...]
    index_symbols: tuple[str, ...]
    adjust_factors: dict[str, float]
    end_marker_seen: bool
    pagination_complete: bool


class SessionQualificationResultV1(_Frozen):
    status: Literal["qualified", "rejected"]
    reason_code: (
        Literal[
            "PROVIDER_TIMEOUT",
            "PROVIDER_ERROR",
            "INCOMPLETE_RESPONSE",
            "SESSION_IDENTITY_MISMATCH",
            "UNIVERSE_MISMATCH",
            "REQUIRED_INDEX_MISSING",
            "DUPLICATE_SYMBOL",
            "MIXED_PROVIDER_SOURCE",
            "MISSING_SUSPENSION_EVIDENCE",
            "MISSING_ADJUSTMENT_FACTOR",
            "FACTOR_CONTINUITY_UNPROVEN",
        ]
        | None
    )
    candidate_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    normalized_volume_shares: float = Field(ge=0, allow_inf_nan=False)
    normalized_amount_cny: float = Field(ge=0, allow_inf_nan=False)
    attempts: Literal[1] = 1
    provider_requests: Literal[1] = 1
    writes: Literal[False] = False


class SessionProvider(Protocol):
    def fetch_session(
        self, request: QualificationSessionRequestV1
    ) -> QualificationFetchResultV1: ...


class OfflineQualificationHarness:
    def __init__(self, provider: SessionProvider) -> None:
        self.provider = provider

    @staticmethod
    def _rejected(reason: str) -> SessionQualificationResultV1:
        return SessionQualificationResultV1(
            status="rejected",
            reason_code=reason,
            normalized_volume_shares=0,
            normalized_amount_cny=0,
        )

    def run(self, request: QualificationSessionRequestV1) -> SessionQualificationResultV1:
        try:
            fetched = self.provider.fetch_session(request)
        except TimeoutError:
            return self._rejected("PROVIDER_TIMEOUT")
        except Exception:
            return self._rejected("PROVIDER_ERROR")
        if type(fetched) is not QualificationFetchResultV1:
            return self._rejected("PROVIDER_ERROR")
        try:
            result = QualificationFetchResultV1.model_validate(fetched.model_dump(mode="python"))
        except (TypeError, ValueError):
            return self._rejected("PROVIDER_ERROR")
        if not result.end_marker_seen or not result.pagination_complete:
            return self._rejected("INCOMPLETE_RESPONSE")
        if result.provider_id != request.provider_id or result.session_date != request.session_date:
            return self._rejected("SESSION_IDENTITY_MISMATCH")
        expected = set(request.expected_symbols)
        if set(result.session_universe) != expected or len(result.session_universe) != len(
            expected
        ):
            return self._rejected("UNIVERSE_MISMATCH")
        if not set(request.required_indexes).issubset(result.index_symbols):
            return self._rejected("REQUIRED_INDEX_MISSING")
        symbols = tuple(item.symbol for item in result.bars)
        if len(symbols) != len(set(symbols)):
            return self._rejected("DUPLICATE_SYMBOL")
        if any(item.source_provider != request.provider_id for item in result.bars):
            return self._rejected("MIXED_PROVIDER_SOURCE")
        suspended = set(result.suspended_symbols)
        if len(suspended) != len(result.suspended_symbols):
            return self._rejected("DUPLICATE_SYMBOL")
        if set(symbols) & suspended or set(symbols) | suspended != expected:
            return self._rejected("MISSING_SUSPENSION_EVIDENCE")
        if set(result.adjust_factors) != expected:
            return self._rejected("MISSING_ADJUSTMENT_FACTOR")
        if set(request.previous_adjust_factors) != expected or any(
            factor <= 0
            for factor in (
                *result.adjust_factors.values(),
                *request.previous_adjust_factors.values(),
            )
        ):
            return self._rejected("FACTOR_CONTINUITY_UNPROVEN")
        volume_shares = sum(item.volume_lots for item in result.bars) * 100
        amount_cny = sum(item.amount_thousand_cny for item in result.bars) * 1000
        candidate = {
            "provider_id": result.provider_id,
            "session_date": result.session_date.isoformat(),
            "symbols": sorted(symbols),
            "bars": sorted(
                (
                    item.symbol,
                    item.source_provider,
                    item.volume_lots,
                    item.amount_thousand_cny,
                )
                for item in result.bars
            ),
            "suspended_symbols": sorted(suspended),
            "index_symbols": sorted(result.index_symbols),
            "adjust_factors": sorted(result.adjust_factors.items()),
            "normalized_volume_shares": volume_shares,
            "normalized_amount_cny": amount_cny,
            "version_vector_sha256": request.version_vector.version_vector_sha256,
        }
        return SessionQualificationResultV1(
            status="qualified",
            reason_code=None,
            candidate_sha256=_digest(_CANDIDATE_DOMAIN, candidate),
            normalized_volume_shares=volume_shares,
            normalized_amount_cny=amount_cny,
        )


class SessionQualificationObservationV1(_Frozen):
    session_date: date
    status: Literal["qualified", "rejected"]
    candidate_sha256: str = Field(pattern=_SHA256_PATTERN)
    evidence_sha256: str = Field(pattern=_SHA256_PATTERN)
    gate_sha256: str = Field(pattern=_SHA256_PATTERN)
    selection_sha256: str = Field(pattern=_SHA256_PATTERN)
    version_vector_sha256: str = Field(pattern=_SHA256_PATTERN)


class QualificationWindowResultV1(_Frozen):
    status: Literal["qualified", "blocked"]
    reason_code: (
        Literal[
            "WINDOW_INCOMPLETE", "SESSION_REJECTED", "SESSION_ORDER_INVALID", "VERSION_VECTOR_DRIFT"
        ]
        | None
    )
    consecutive_sessions: int = Field(ge=0, le=20)
    canonical_failover_state: Literal["UNQUALIFIED"] = "UNQUALIFIED"
    effective_auto_failover_enabled: Literal[False] = False
    provider_requests: Literal[0] = 0
    writes: Literal[False] = False


def evaluate_qualification_window(
    observations: tuple[SessionQualificationObservationV1, ...],
    *,
    expected_sessions: tuple[date, ...],
) -> QualificationWindowResultV1:
    reason: str | None = None
    if len(observations) != 20:
        reason = "WINDOW_INCOMPLETE"
    elif any(item.status != "qualified" for item in observations):
        reason = "SESSION_REJECTED"
    elif (
        len(expected_sessions) != 20
        or len(set(expected_sessions)) != 20
        or tuple(sorted(expected_sessions)) != expected_sessions
        or tuple(item.session_date for item in observations) != expected_sessions
    ):
        reason = "SESSION_ORDER_INVALID"
    elif len({item.version_vector_sha256 for item in observations}) != 1:
        reason = "VERSION_VECTOR_DRIFT"
    return QualificationWindowResultV1(
        status="qualified" if reason is None else "blocked",
        reason_code=reason,
        consecutive_sessions=len(observations) if reason is None else 0,
    )


__all__ = [
    "CanonicalQualificationVersionVectorV1",
    "OfflineQualificationHarness",
    "QualificationFetchResultV1",
    "QualificationSessionRequestV1",
    "QualificationWindowResultV1",
    "RawDailyBarV1",
    "SessionQualificationObservationV1",
    "SessionQualificationResultV1",
    "evaluate_qualification_window",
]
