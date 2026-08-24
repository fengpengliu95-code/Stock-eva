"""Immutable R2-F2 candidate gate and single-session selection contracts."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from contextlib import contextmanager
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.evidence import EvidenceError, EvidenceReader, PublishedEvidence
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

    @contextmanager
    def _bound_root(self):
        try:
            root_fd = os.open(
                self.root,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            )
        except OSError as exc:
            raise EvidenceError(
                "candidate root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
            ) from exc
        identity = os.fstat(root_fd)
        expected = (identity.st_dev, identity.st_ino)
        try:
            self._assert_root(root_fd, expected)
            yield root_fd, expected
        finally:
            os.close(root_fd)

    def _assert_root(self, root_fd: int, expected: tuple[int, int]) -> None:
        current = os.fstat(root_fd)
        if (current.st_dev, current.st_ino) != expected:
            raise EvidenceError("candidate root changed during publish", "EVIDENCE_HASH_MISMATCH")
        try:
            configured = os.stat(self.root, follow_symlinks=False)
        except OSError as exc:
            raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH") from exc
        if (configured.st_dev, configured.st_ino) != expected:
            raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH")

    @staticmethod
    def _parts(relative: str) -> tuple[str, ...]:
        parts = tuple(relative.split("/"))
        if not parts or any(not part or part in {".", ".."} for part in parts):
            raise EvidenceError("unsafe candidate path", "EVIDENCE_UNSAFE_PATH")
        return parts

    @staticmethod
    def _mkdir_open(parent: int, component: str) -> int:
        try:
            return os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except FileNotFoundError:
            try:
                os.mkdir(component, 0o700, dir_fd=parent)
                return os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent,
                )
            except OSError as exc:
                raise EvidenceError(
                    "candidate artifact directory is unsafe", "EVIDENCE_UNSAFE_PATH"
                ) from exc
        except OSError as exc:
            raise EvidenceError(
                "candidate artifact directory is unsafe", "EVIDENCE_UNSAFE_PATH"
            ) from exc

    def _write_once(
        self,
        relative: str,
        payload: bytes,
        *,
        root_fd: int,
        root_identity: tuple[int, int],
    ) -> Path:
        self._assert_root(root_fd, root_identity)
        parts = self._parts(relative)
        parent = os.dup(root_fd)
        created: tuple[int, str] | None = None
        try:
            for component in parts[:-1]:
                child = self._mkdir_open(parent, component)
                os.close(parent)
                parent = child
            name = parts[-1]
            try:
                existing = os.open(
                    name,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent,
                )
            except FileNotFoundError:
                existing = None
            if existing is not None:
                try:
                    data = os.read(existing, len(payload) + 1)
                    if data != payload:
                        raise ValueError("immutable candidate artifact collision")
                finally:
                    os.close(existing)
            else:
                staging = self._mkdir_open(root_fd, "staging")
                try:
                    temp_name = f"candidate-{os.getpid()}-{id(payload)}.partial"
                    temp = os.open(
                        temp_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                        0o600,
                        dir_fd=staging,
                    )
                    created = (staging, temp_name)
                    try:
                        os.write(temp, payload)
                        os.fsync(temp)
                    finally:
                        os.close(temp)
                    try:
                        os.link(
                            temp_name,
                            name,
                            src_dir_fd=staging,
                            dst_dir_fd=parent,
                            follow_symlinks=False,
                        )
                        os.fsync(parent)
                    except FileExistsError:
                        existing = os.open(
                            name,
                            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=parent,
                        )
                        try:
                            if os.read(existing, len(payload) + 1) != payload:
                                raise ValueError("immutable candidate artifact collision")
                        finally:
                            os.close(existing)
                    os.unlink(temp_name, dir_fd=staging)
                    created = None
                finally:
                    os.close(staging)
            self._assert_root(root_fd, root_identity)
            path = self.root / relative
            readback = self._readback(root_fd, relative)
            if readback != payload:
                raise EvidenceError(
                    "candidate artifact readback mismatch", "EVIDENCE_HASH_MISMATCH"
                )
            return path
        except BaseException:
            if created is not None:
                try:
                    os.unlink(created[1], dir_fd=created[0])
                except OSError:
                    pass
            raise
        finally:
            os.close(parent)

    @staticmethod
    def _readback(root_fd: int, relative: str) -> bytes:
        from backend.app.market.evidence import open_evidence_relative

        descriptor = open_evidence_relative(root_fd, relative)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise EvidenceError("candidate artifact is not regular", "EVIDENCE_UNSAFE_PATH")
            return os.read(descriptor, info.st_size + 1)
        finally:
            os.close(descriptor)

    @staticmethod
    def _payload(model: BaseModel) -> bytes:
        return _canonical(model.model_dump(mode="json"))

    def publish_gate_report(self, report: CandidateGateReport) -> Path:
        with self._bound_root() as (fd, identity):
            return self._write_once(
                f"gates/{report.gate_report_id}.json",
                self._payload(report),
                root_fd=fd,
                root_identity=identity,
            )

    def publish_candidate(self, candidate: CandidateManifest) -> Path:
        with self._bound_root() as (fd, identity):
            return self._write_once(
                f"candidates/{candidate.candidate_id}.json",
                self._payload(candidate),
                root_fd=fd,
                root_identity=identity,
            )

    def publish_selection(
        self,
        selection: SessionSelection,
        *,
        candidate: CandidateManifest,
        gate_report: CandidateGateReport,
        evidence: PublishedEvidence,
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
        with self._bound_root() as (fd, identity):
            return self._write_once(
                f"selections/{selection.selection_id}.json",
                self._payload(selection),
                root_fd=fd,
                root_identity=identity,
            )

    def publish_chain(
        self,
        *,
        report: CandidateGateReport,
        candidate: CandidateManifest,
        selection: SessionSelection,
        evidence: PublishedEvidence,
        normalized_payload: bytes | None = None,
        after_selection: Any | None = None,
    ) -> SessionSelection:
        """Publish gate → candidate → selection; callback runs only after selection."""
        validate_candidate_lineage(candidate, evidence, report)
        with self._bound_root() as (fd, identity):
            if normalized_payload is not None:
                if (
                    hashlib.sha256(normalized_payload).hexdigest()
                    != candidate.normalized_object_sha256
                ):
                    raise ValueError("normalized candidate object hash mismatch")
                self._write_once(
                    candidate.normalized_object_relative_path,
                    normalized_payload,
                    root_fd=fd,
                    root_identity=identity,
                )
            self._write_once(
                f"gates/{report.gate_report_id}.json",
                self._payload(report),
                root_fd=fd,
                root_identity=identity,
            )
            self._write_once(
                f"candidates/{candidate.candidate_id}.json",
                self._payload(candidate),
                root_fd=fd,
                root_identity=identity,
            )
            self._write_once(
                f"selections/{selection.selection_id}.json",
                self._payload(selection),
                root_fd=fd,
                root_identity=identity,
            )
            self._assert_root(fd, identity)
            if after_selection is not None:
                after_selection(selection)
            self._assert_root(fd, identity)
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
    evidence: PublishedEvidence,
    gate_report: CandidateGateReport,
) -> None:
    """Validate the complete candidate -> evidence/gate identity join."""
    if not isinstance(evidence, PublishedEvidence):
        raise ValueError("candidate lineage requires bound PublishedEvidence")
    try:
        evidence.read_rows()
    except EvidenceError as exc:
        raise ValueError("published evidence readback failed") from exc
    manifest = evidence.manifest
    expected = {
        "evidence_id": manifest.evidence_id,
        "evidence_sha256": manifest.manifest_sha256,
        "trade_date": manifest.trade_date,
        "universe_id": manifest.universe_id,
        "provider_id": manifest.provider_id,
        "adapter_version": manifest.adapter_version,
        "factor_resolution_sha256": manifest.factor_resolution_sha256,
    }
    for field, value in expected.items():
        if getattr(candidate, field) != value:
            raise ValueError(f"candidate {field} lineage mismatch")
    if gate_report.candidate_id != candidate.candidate_id:
        raise ValueError("candidate gate report identity mismatch")
    if gate_report.aggregate_sha256 != candidate.gate_report_sha256:
        raise ValueError("candidate gate report hash mismatch")
    if any(manifest.manifest_sha256 not in item.referenced_hashes for item in gate_report.outcomes):
        raise ValueError("candidate gate report is not bound to published evidence")
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
    evidence: PublishedEvidence,
    factor_resolution: tuple[Any, ...] | None = None,
    created_at: datetime,
    gate_version: str = "r2f2-gates.v1",
) -> CandidateGateReport:
    """Evaluate the ten gates from typed/readback inputs in their fixed order."""
    if not isinstance(evidence, PublishedEvidence):
        raise ValueError("candidate gates require bound PublishedEvidence")
    try:
        evidence_pages = evidence.read_rows()
        reader = evidence._reader
        if reader is None:
            raise EvidenceError("published evidence has no reader", "EVIDENCE_MANIFEST_INVALID")
        replay_reader = EvidenceReader(reader.root)
        try:
            replay = replay_reader.replay(evidence.manifest.evidence_id)
        finally:
            replay_reader.close()
    except EvidenceError as exc:
        raise ValueError("candidate evidence readback failed") from exc
    if (
        factor_resolution is not None
        and tuple(factor_resolution) != evidence.manifest.factor_resolution
    ):
        raise ValueError("candidate factor resolution is not the published snapshot")
    published_resolution = evidence.manifest.factor_resolution
    symbols = tuple(_row_value(row, "symbol", _row_value(row, "code")) for row in rows)
    unique_symbols = set(symbols)
    date_ok = all(
        _row_value(row, "trade_date", _row_value(row, "date")) == trade_date for row in rows
    )
    coverage_ok = len(rows) == len(required_symbols) and len(unique_symbols) == len(rows)
    semantic_ok = bool(rows)
    suspension_ok = bool(rows)
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
    evidence_manifest = evidence.manifest
    evidence_ok = bool(evidence_manifest.manifest_sha256) and bool(evidence.reader_identity)
    completions = tuple(evidence_manifest.request_completions)
    transport_ok = bool(evidence_ok and completions) and all(
        str(getattr(getattr(item, "final_outcome", None), "value", "")) == "success"
        for item in completions
    )
    factor_keys = {
        (getattr(item, "symbol", None), getattr(item, "trade_date", None))
        for item in published_resolution
    }
    factor_ok = all(
        (_row_value(row, "symbol"), trade_date) in factor_keys
        for row in rows
        if _row_value(row, "security_type", "stock") == "stock"
        and not bool(_row_value(row, "is_suspended", False))
    )
    raw_descriptors = tuple(
        item for item in evidence.manifest.objects if item.object_kind.value == "raw_endpoint_page"
    )
    descriptor_fields_ok = len(evidence_pages) == len(raw_descriptors) and all(
        tuple(page.get("fields", ())) == tuple(descriptor.fields)
        for page, descriptor in zip(evidence_pages, raw_descriptors, strict=True)
    )
    replay_ok = (
        replay.status == "ready"
        and replay.normalization_clock_utc == evidence_manifest.normalization_clock_utc
    )
    checks = (
        transport_ok,
        descriptor_fields_ok,
        date_ok,
        unique_symbols == set(required_symbols),
        coverage_ok,
        semantic_ok,
        factor_ok,
        suspension_ok,
        evidence_ok and replay_ok,
        replay_ok,
    )
    outcomes = tuple(
        GateOutcome(
            gate_name=name,
            gate_version=gate_version,
            verdict="pass" if passed else "fail",
            bounded_metrics=(("rows", len(rows)), ("required_symbols", len(required_symbols))),
            failure_class=None if passed else "semantic",
            referenced_hashes=tuple(
                item for item in (evidence_manifest.manifest_sha256,) if item is not None
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
    gate_report: CandidateGateReport,
    evidence: PublishedEvidence,
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
