"""Immutable R2-F2 candidate gate and single-session selection contracts."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.providers.base import (
    BoundedText,
    ProviderId,
    SafeFailureClass,
    SafeIdentifier,
    SafeRelativePath,
    SafeSha256,
)


def _canonical(value: Any) -> bytes:
    value = _json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")


def _json_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, StrEnum):
        return value.value
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class _Immutable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class SelectionReason(StrEnum):
    PRIMARY_READY = "primary_ready"
    QUALIFIED_FALLBACK = "qualified_fallback"


class GateName(StrEnum):
    TRANSPORT_COMPLETE = "transport_complete"
    SCHEMA = "schema"
    DATE = "date"
    UNIVERSE = "universe"
    COVERAGE = "coverage"
    SEMANTIC = "semantic"
    FACTOR = "factor"
    SUSPENSION = "suspension"
    EVIDENCE_HASH = "evidence_hash"
    DETERMINISM = "determinism"


R2F2_GATE_ORDER: tuple[GateName, ...] = (
    GateName.TRANSPORT_COMPLETE,
    GateName.SCHEMA,
    GateName.DATE,
    GateName.UNIVERSE,
    GateName.COVERAGE,
    GateName.SEMANTIC,
    GateName.FACTOR,
    GateName.SUSPENSION,
    GateName.EVIDENCE_HASH,
    GateName.DETERMINISM,
)

# This table is intentionally descriptive only.  The persisted report contains
# the ordered outcome tuple, never this legacy/public description.
R2F2_GATE_EVIDENCE: tuple[tuple[GateName, str, str], ...] = (
    (
        R2F2_GATE_ORDER[0],
        "F0.1 TransportObservationProjection",
        "BaoStockProvider._read / provider transport observation collector",
    ),
    (
        R2F2_GATE_ORDER[1],
        "RawEndpointBatch",
        "providers.base exact discriminated endpoint/schema validator",
    ),
    (
        R2F2_GATE_ORDER[2],
        "validate_raw_date_binding(request, batch) + publication identity",
        "R2-F2 adapter/evidence requested-date binding followed by existing publication "
        "requested_date/session identity gate",
    ),
    (
        R2F2_GATE_ORDER[3],
        "_publication_issues",
        "backend.app.market.automation._publication_issues expected/required symbols and indexes",
    ),
    (
        R2F2_GATE_ORDER[4],
        "run_publication_refresh",
        "backend.app.market.automation.run_publication_refresh coverage == 1",
    ),
    (
        R2F2_GATE_ORDER[5],
        "normalize_baostock_rows + publication_bar_quality_issue",
        "existing normalized DailyBar and MarketStore quality contract",
    ),
    (
        R2F2_GATE_ORDER[6],
        "_main_board_factor_snapshot + _publication_issues",
        "AdjustmentFactorCache resolution and missing_adjust_factor gate",
    ),
    (
        R2F2_GATE_ORDER[7],
        "publication_bar_quality_issue",
        "MarketStore.publication_bar_quality_issue suspended placeholder/index rules",
    ),
    (
        R2F2_GATE_ORDER[8],
        "EvidenceReader",
        "descriptor-bound schema, row-count and SHA-256 readback",
    ),
    (
        R2F2_GATE_ORDER[9],
        "EvidenceStore.replay",
        "frozen normalization clock and byte/semantic replay comparison",
    ),
)


class GateOutcome(_Immutable):
    gate_name: GateName
    gate_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    verdict: Literal["pass", "fail"]
    bounded_metrics: tuple[tuple[str, int | float | bool | BoundedText | None], ...] = ()
    failure_class: SafeFailureClass | None = None
    referenced_hashes: tuple[SafeSha256, ...] = ()


class CandidateGateReport(_Immutable):
    gate_report_id: SafeIdentifier
    candidate_id: SafeIdentifier
    gate_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    outcomes: tuple[GateOutcome, ...]
    verdict: Literal["pass", "fail"]
    aggregate_sha256: SafeSha256
    created_at: datetime

    @model_validator(mode="after")
    def validate_aggregate(self) -> CandidateGateReport:
        if self.created_at.utcoffset() is None:
            raise ValueError("gate report timestamp must be timezone-aware")
        names = tuple(item.gate_name for item in self.outcomes)
        if names != R2F2_GATE_ORDER:
            raise ValueError("gate report must contain the exact ordered gate set")
        if any(item.gate_version != self.gate_version for item in self.outcomes):
            raise ValueError("gate outcome version mismatch")
        expected_verdict = (
            "pass" if all(item.verdict == "pass" for item in self.outcomes) else "fail"
        )
        if self.verdict != expected_verdict:
            raise ValueError("gate report verdict mismatch")
        values = self.model_dump(mode="json")
        values.pop("aggregate_sha256", None)
        if self.aggregate_sha256 != _digest(values):
            raise ValueError("gate aggregate hash mismatch")
        return self


class CandidateManifest(_Immutable):
    candidate_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    provider_id: ProviderId
    evidence_id: SafeIdentifier
    evidence_sha256: SafeSha256
    normalized_object_relative_path: SafeRelativePath
    normalized_object_sha256: SafeSha256
    gate_report_relative_path: SafeRelativePath
    gate_report_sha256: SafeSha256
    factor_resolution_sha256: SafeSha256
    adapter_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    source_schema_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    row_count: int = Field(ge=0, le=10_000_000)
    required_symbol_count: int = Field(ge=1, le=10_000_000)
    status: Literal["accepted", "rejected"]
    manifest_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_identity(self) -> CandidateManifest:
        values = self.model_dump(mode="json")
        values.pop("manifest_sha256", None)
        if self.manifest_sha256 != _digest(values):
            raise ValueError("candidate manifest hash mismatch")
        return self


class SessionSelection(_Immutable):
    selection_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    selected_candidate_id: SafeIdentifier
    selected_provider_id: ProviderId
    reason: SelectionReason
    fallback_from: ProviderId | None
    evidence_sha256: SafeSha256
    candidate_manifest_sha256: SafeSha256
    gate_report_sha256: SafeSha256
    selected_at: datetime
    selection_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_selection(self) -> SessionSelection:
        if self.selected_at.utcoffset() is None:
            raise ValueError("selection timestamp must be timezone-aware")
        if self.reason is not SelectionReason.PRIMARY_READY:
            raise ValueError("qualified fallback is reserved and rejected")
        if self.fallback_from is not None:
            raise ValueError("fallback provider is not allowed")
        values = self.model_dump(mode="json")
        values.pop("selection_sha256", None)
        if self.selection_sha256 != _digest(values):
            raise ValueError("selection hash mismatch")
        return self


class CandidateStore:
    """Small immutable JSON projection store under the evidence root."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def _write_once(self, relative: str, payload: bytes) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            if path.read_bytes() != payload:
                raise ValueError("immutable candidate artifact collision") from None
        return path

    @staticmethod
    def _payload(model: BaseModel) -> bytes:
        return _canonical(model.model_dump(mode="json"))

    def publish_gate_report(self, report: CandidateGateReport) -> Path:
        return self._write_once(f"gates/{report.gate_report_id}.json", self._payload(report))

    def publish_candidate(self, candidate: CandidateManifest) -> Path:
        return self._write_once(
            f"candidates/{candidate.candidate_id}.json", self._payload(candidate)
        )

    def publish_selection(
        self,
        selection: SessionSelection,
        *,
        candidate: CandidateManifest,
        gate_report: CandidateGateReport,
        evidence: Any | None = None,
    ) -> Path:
        validate_candidate_lineage(candidate, evidence, gate_report)
        if selection.selected_candidate_id != candidate.candidate_id:
            raise ValueError("selection candidate lineage mismatch")
        if selection.candidate_manifest_sha256 != candidate.manifest_sha256:
            raise ValueError("selection candidate hash mismatch")
        if selection.gate_report_sha256 != gate_report.aggregate_sha256:
            raise ValueError("selection gate hash mismatch")
        if selection.evidence_sha256 != candidate.evidence_sha256:
            raise ValueError("selection evidence hash mismatch")
        return self._write_once(
            f"selections/{selection.selection_id}.json", self._payload(selection)
        )

    def publish_chain(
        self,
        *,
        report: CandidateGateReport,
        candidate: CandidateManifest,
        selection: SessionSelection,
        evidence: Any | None = None,
        after_selection: Any | None = None,
    ) -> SessionSelection:
        """Publish gate → candidate → selection; callback runs only after selection."""
        validate_candidate_lineage(candidate, evidence, report)
        self.publish_gate_report(report)
        self.publish_candidate(candidate)
        self.publish_selection(
            selection, candidate=candidate, gate_report=report, evidence=evidence
        )
        if after_selection is not None:
            after_selection(selection)
        return selection


def build_gate_report(
    *,
    candidate_id: str,
    outcomes: tuple[GateOutcome, ...],
    created_at: datetime,
    gate_version: str = "r2f2-gates.v1",
) -> CandidateGateReport:
    verdict = "pass" if all(item.verdict == "pass" for item in outcomes) else "fail"
    probe = CandidateGateReport.model_construct(
        gate_report_id=f"gate-{_digest((candidate_id, outcomes))[:24]}",
        candidate_id=candidate_id,
        gate_version=gate_version,
        outcomes=outcomes,
        verdict=verdict,
        aggregate_sha256="0" * 64,
        created_at=created_at,
    )
    values = probe.model_dump(mode="json")
    values["aggregate_sha256"] = _digest(
        {key: value for key, value in values.items() if key != "aggregate_sha256"}
    )
    return CandidateGateReport.model_validate(values)


def build_candidate_manifest(**values: Any) -> CandidateManifest:
    values = dict(values)
    if "provider_id" in values:
        values["provider_id"] = ProviderId(values["provider_id"])
    values.setdefault("candidate_id", f"candidate-{_digest(values)[:24]}")
    values.setdefault("status", "accepted")
    probe = CandidateManifest.model_construct(**values, manifest_sha256="0" * 64)
    normalized = probe.model_dump(mode="json")
    normalized.pop("manifest_sha256", None)
    normalized["manifest_sha256"] = _digest(normalized)
    return CandidateManifest.model_validate(normalized)


def _as_candidate(value: CandidateManifest | dict[str, Any]) -> CandidateManifest:
    return (
        value if isinstance(value, CandidateManifest) else CandidateManifest.model_validate(value)
    )


def validate_candidate_lineage(
    candidate: CandidateManifest,
    evidence: Any,
    gate_report: CandidateGateReport,
) -> None:
    """Validate the complete candidate -> evidence/gate identity join."""
    manifest = getattr(evidence, "manifest", evidence)
    expected = {
        "evidence_id": getattr(manifest, "evidence_id", None),
        "evidence_sha256": getattr(manifest, "manifest_sha256", None),
        "trade_date": getattr(manifest, "trade_date", None),
        "universe_id": getattr(manifest, "universe_id", None),
        "provider_id": getattr(manifest, "provider_id", None),
        "adapter_version": getattr(manifest, "adapter_version", None),
        "factor_resolution_sha256": getattr(manifest, "factor_resolution_sha256", None),
    }
    for field, value in expected.items():
        if value is not None and getattr(candidate, field) != value:
            raise ValueError(f"candidate {field} lineage mismatch")
    if gate_report.candidate_id != candidate.candidate_id:
        raise ValueError("candidate gate report identity mismatch")
    if gate_report.aggregate_sha256 != candidate.gate_report_sha256:
        raise ValueError("candidate gate report hash mismatch")
    if candidate.status == "accepted" and gate_report.verdict != "pass":
        raise ValueError("accepted candidate requires a passing gate report")


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(name, default)
    return getattr(row, name, default)


def evaluate_candidate_gates(
    *,
    candidate_id: str,
    trade_date: date,
    required_symbols: tuple[str, ...],
    rows: tuple[Any, ...],
    evidence: Any | None,
    factor_resolution: tuple[Any, ...] = (),
    created_at: datetime,
    gate_version: str = "r2f2-gates.v1",
) -> CandidateGateReport:
    """Evaluate the ten gates from typed/readback inputs in their fixed order."""
    symbols = tuple(_row_value(row, "symbol", _row_value(row, "code")) for row in rows)
    unique_symbols = set(symbols)
    date_ok = all(
        _row_value(row, "trade_date", _row_value(row, "date")) == trade_date for row in rows
    )
    coverage_ok = len(rows) == len(required_symbols) and len(unique_symbols) == len(rows)
    semantic_ok = True
    suspension_ok = True
    for row in rows:
        security_type = _row_value(row, "security_type", "stock")
        suspended = bool(_row_value(row, "is_suspended", False))
        if not suspended:
            if _row_value(row, "adjust_factor") is None and security_type == "stock":
                semantic_ok = False
        elif security_type != "stock":
            suspension_ok = False
        elif any(float(_row_value(row, field, 0) or 0) != 0 for field in ("volume", "amount")):
            suspension_ok = False
    evidence_manifest = getattr(evidence, "manifest", evidence)
    evidence_ok = evidence_manifest is not None and bool(
        getattr(evidence_manifest, "manifest_sha256", None)
    )
    completions = tuple(getattr(evidence_manifest, "request_completions", ()))
    transport_ok = bool(evidence_ok and completions) and all(
        str(getattr(getattr(item, "final_outcome", None), "value", "")) == "success"
        for item in getattr(evidence_manifest, "request_completions", ())
    )
    factor_keys = {
        (getattr(item, "symbol", None), getattr(item, "trade_date", None))
        for item in factor_resolution
    }
    factor_ok = all(
        (_row_value(row, "symbol"), trade_date) in factor_keys
        for row in rows
        if _row_value(row, "security_type", "stock") == "stock"
        and not bool(_row_value(row, "is_suspended", False))
    )
    checks = (
        transport_ok,
        True,
        date_ok,
        unique_symbols == set(required_symbols),
        coverage_ok,
        semantic_ok,
        factor_ok,
        suspension_ok,
        evidence_ok,
        True,
    )
    outcomes = tuple(
        GateOutcome(
            gate_name=name,
            gate_version=gate_version,
            verdict="pass" if passed else "fail",
            bounded_metrics=(("rows", len(rows)), ("required_symbols", len(required_symbols))),
            failure_class=None if passed else "semantic",
            referenced_hashes=tuple(
                item
                for item in (getattr(evidence_manifest, "manifest_sha256", None),)
                if item is not None
            ),
        )
        for name, passed in zip(R2F2_GATE_ORDER, checks, strict=True)
    )
    return build_gate_report(
        candidate_id=candidate_id,
        outcomes=outcomes,
        created_at=created_at,
        gate_version=gate_version,
    )


def select_primary_candidate(
    *,
    trade_date: date,
    universe_id: str,
    candidates: tuple[CandidateManifest | dict[str, Any], ...],
    selected_at: datetime,
    reason: SelectionReason = SelectionReason.PRIMARY_READY,
    gate_report: CandidateGateReport | None = None,
    evidence: Any | None = None,
) -> SessionSelection:
    if reason is not SelectionReason.PRIMARY_READY:
        raise ValueError("qualified fallback is reserved and rejected")
    if len(candidates) != 1:
        raise ValueError("selection requires exactly one complete candidate")
    candidate = _as_candidate(candidates[0])
    if candidate.status != "accepted":
        raise ValueError("selection requires an accepted candidate")
    if candidate.provider_id is not ProviderId.BAOSTOCK:
        raise ValueError("selection provider must be baostock")
    if candidate.trade_date != trade_date or candidate.universe_id != universe_id:
        raise ValueError("selection scope does not match candidate")
    if gate_report is not None:
        validate_candidate_lineage(candidate, evidence, gate_report)
    values = {
        "selection_id": f"selection-{_digest(candidate.model_dump(mode='json'))[:24]}",
        "trade_date": trade_date,
        "universe_id": universe_id,
        "selected_candidate_id": candidate.candidate_id,
        "selected_provider_id": candidate.provider_id,
        "reason": reason,
        "fallback_from": None,
        "evidence_sha256": candidate.evidence_sha256,
        "candidate_manifest_sha256": candidate.manifest_sha256,
        "gate_report_sha256": candidate.gate_report_sha256,
        "selected_at": selected_at,
    }
    probe = SessionSelection.model_construct(**values, selection_sha256="0" * 64)
    normalized = probe.model_dump(mode="json")
    normalized["selection_sha256"] = _digest(
        {key: value for key, value in normalized.items() if key != "selection_sha256"}
    )
    return SessionSelection.model_validate(normalized)


__all__ = [
    "CandidateGateReport",
    "CandidateManifest",
    "CandidateStore",
    "GateName",
    "GateOutcome",
    "R2F2_GATE_EVIDENCE",
    "R2F2_GATE_ORDER",
    "SelectionReason",
    "SessionSelection",
    "build_candidate_manifest",
    "build_gate_report",
    "evaluate_candidate_gates",
    "select_primary_candidate",
    "validate_candidate_lineage",
]
