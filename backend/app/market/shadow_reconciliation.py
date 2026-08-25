"""Deterministic, comparison-only R2-F3 reconciliation."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .shadow_normalize import SHADOW_PROVIDERS, ShadowNormalizedCandidate, ShadowNormalizedRow


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        + "\n"
    ).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class ReconciliationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: str = "r2f3-v1"
    legal_tick: float = 0.01
    volume_relative_error: float = 0.001
    amount_relative_error: float = 0.005
    adjusted_return_basis_points: float = 5.0
    sample_limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def frozen_r2f3_v1(self) -> ReconciliationPolicy:
        expected = {
            "version": "r2f3-v1",
            "legal_tick": 0.01,
            "volume_relative_error": 0.001,
            "amount_relative_error": 0.005,
            "adjusted_return_basis_points": 5.0,
            "sample_limit": 20,
        }
        if self.model_dump() != expected:
            raise ValueError("r2f3-v1 reconciliation policy is frozen")
        return self


class ShadowReconciliationReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    reconciliation_id: str
    policy_version: str
    trade_date: Any
    universe_id: str
    left_candidate_id: str
    right_candidate_id: str
    status: str
    compared_counts: dict[str, int]
    mismatch_counts: dict[str, int]
    bounded_sanitized_samples: tuple[dict[str, Any], ...] = ()
    quarantine_secondary: bool = False
    report_sha256: str = "0" * 64

    @model_validator(mode="after")
    def bind_hash(self) -> ShadowReconciliationReport:
        values = self.model_dump(mode="json")
        values.pop("report_sha256", None)
        expected = _sha(values)
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("shadow reconciliation hash mismatch")
        return self


def _candidate_id(candidate: ShadowNormalizedCandidate) -> str:
    return _sha(candidate.normalized_sha256)[:24]


def _relative_error(left: float, right: float) -> float:
    scale = max(abs(left), abs(right), 1.0)
    return abs(left - right) / scale


def _return(row: ShadowNormalizedRow) -> float:
    factor = row.factor or 1.0
    previous_factor = row.factor_previous or factor
    return (row.close * factor) / (row.preclose * previous_factor) - 1.0


def reconcile(
    left: ShadowNormalizedCandidate,
    right: ShadowNormalizedCandidate,
    *,
    policy: ReconciliationPolicy | None = None,
) -> ShadowReconciliationReport:
    if left.provider_id not in SHADOW_PROVIDERS or right.provider_id not in SHADOW_PROVIDERS:
        raise ValueError("shadow provider identity is not allowlisted")
    policy = policy or ReconciliationPolicy()
    policy = ReconciliationPolicy.model_validate(policy.model_dump())
    left_id, right_id = _candidate_id(left), _candidate_id(right)
    identity = {
        "trade_date": left.trade_date,
        "universe_id": left.universe_id,
        "left_candidate_id": left_id,
        "right_candidate_id": right_id,
    }
    mismatch: dict[str, int] = {
        "identity": 0,
        "universe": 0,
        "suspension": 0,
        "ohlc": 0,
        "volume": 0,
        "amount": 0,
        "adjusted_return": 0,
    }
    samples: list[dict[str, Any]] = []
    if (
        left.trade_date != right.trade_date
        or left.universe_id != right.universe_id
        or not left.complete
        or not right.complete
        or not left.factor_semantics
        or not right.factor_semantics
        or not left.factor_anchor
        or not right.factor_anchor
        or not left.factor_direction
        or not right.factor_direction
        or left.factor_anchor != right.factor_anchor
        or left.factor_direction != right.factor_direction
    ):
        mismatch["identity"] = 1
    if left.expected_symbols != right.expected_symbols or left.index_symbols != right.index_symbols:
        mismatch["universe"] = 1
    left_rows = {row.symbol: row for row in left.rows}
    right_rows = {row.symbol: row for row in right.rows}
    if set(left_rows) != set(right_rows):
        mismatch["coverage"] = abs(len(set(left_rows) ^ set(right_rows)))
    for symbol in sorted(set(left_rows) & set(right_rows)):
        lrow, rrow = left_rows[symbol], right_rows[symbol]
        if (
            lrow.factor is None
            or lrow.factor_previous is None
            or rrow.factor is None
            or rrow.factor_previous is None
        ):
            mismatch["adjusted_return"] += 1
            continue
        if lrow.suspension != rrow.suspension:
            mismatch["suspension"] += 1
        if any(
            abs(getattr(lrow, field) - getattr(rrow, field)) > policy.legal_tick + 1e-12
            for field in ("open", "high", "low", "close", "preclose")
        ):
            mismatch["ohlc"] += 1
        if _relative_error(lrow.volume, rrow.volume) > policy.volume_relative_error:
            mismatch["volume"] += 1
        if _relative_error(lrow.amount, rrow.amount) > policy.amount_relative_error:
            mismatch["amount"] += 1
        if abs(_return(lrow) - _return(rrow)) * 10000 > policy.adjusted_return_basis_points:
            mismatch["adjusted_return"] += 1
        if any(
            mismatch[key] and len(samples) < policy.sample_limit
            for key in ("ohlc", "volume", "amount", "adjusted_return")
        ):
            samples.append(
                {
                    "symbol": symbol,
                    "fields": tuple(
                        key
                        for key, count in mismatch.items()
                        if count and key not in {"identity", "universe"}
                    ),
                }
            )
    compared = {
        "symbols": len(set(left_rows) & set(right_rows)),
        "left_rows": len(left.rows),
        "right_rows": len(right.rows),
    }
    active_count = max(len(set(left_rows) & set(right_rows)), 1)
    rate_limited = (
        mismatch["volume"] / active_count > 0.001 or mismatch["amount"] / active_count > 0.001
    )
    status = (
        "unavailable"
        if mismatch["identity"] or mismatch["universe"] or mismatch.get("coverage", 0)
        else (
            "material_mismatch"
            if mismatch["suspension"]
            or mismatch["ohlc"]
            or rate_limited
            or mismatch["adjusted_return"]
            else "ready"
        )
    )
    return ShadowReconciliationReport(
        reconciliation_id=_sha({"policy": policy.version, **identity, "mismatch": mismatch})[:32],
        policy_version=policy.version,
        trade_date=left.trade_date,
        universe_id=left.universe_id,
        left_candidate_id=left_id,
        right_candidate_id=right_id,
        status=status,
        compared_counts=compared,
        mismatch_counts=mismatch,
        bounded_sanitized_samples=tuple(samples[: policy.sample_limit]),
        quarantine_secondary=status == "material_mismatch",
    )


reconcile_candidates = reconcile
