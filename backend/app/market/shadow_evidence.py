"""Immutable, successful-attempt-only evidence for the R2-F3 shadow lane.

The implementation is deliberately independent from the R2-F2 canonical evidence models.  A
bundle is visible only after its owner marker, bounded objects, manifest and ``COMMIT`` have
been fsynced and the staging directory has been atomically renamed into ``bundles``.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import stat
import sys
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ShadowEvidenceUnavailable(RuntimeError):
    """Sanitized unavailable result for missing/corrupt/orphan shadow evidence."""


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe(value: str, *, limit: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError("shadow identifier is invalid")
    if any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in value
    ):
        raise ValueError("shadow identifier is invalid")
    return value


def _request_contract_values(request: Any) -> dict[str, Any]:
    values = {
        key: getattr(request, key)
        for key in (
            "ordinal",
            "request_id",
            "endpoint",
            "endpoint_class",
            "role",
            "trade_date",
            "symbol_or_index_shard",
            "schema_contract_hash",
            "unit_contract_hash",
            "request_hash",
        )
    }
    values["trade_date"] = values["trade_date"].isoformat()
    return values


def _safe_object_ref(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ShadowEvidenceUnavailable("shadow evidence unavailable")
    if value.startswith(("http:", "https:", "//")) or ".." in Path(value).parts:
        raise ShadowEvidenceUnavailable("shadow evidence unavailable")
    if any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/-:"
        for char in value
    ):
        raise ShadowEvidenceUnavailable("shadow evidence unavailable")
    return value


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class ShadowLogicalRequest(_Frozen):
    job_id: str = "job-offline"
    window_id: str = "window-offline"
    ordinal: int = Field(ge=0)
    request_id: str = Field(min_length=1, max_length=128)
    provider_id: str = ""
    endpoint: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    endpoint_class: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    role: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    trade_date: date
    symbol_or_index_shard: str = Field(min_length=1, max_length=128)
    schema_contract_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    unit_contract_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def bind_request_hash(self) -> ShadowLogicalRequest:
        values = _request_contract_values(self)
        values.pop("request_hash")
        expected = _sha(b"stock-eva/r2f3/shadow-request/v1\n" + _json_bytes(values))
        if self.request_hash == "0" * 64:
            object.__setattr__(self, "request_hash", expected)
        elif self.request_hash != expected:
            raise ValueError("shadow request hash mismatch")
        return self


class ShadowLogicalRequestPlan(_Frozen):
    plan_id: str = "plan-offline"
    job_id: str = "job-offline"
    provider_id: str = ""
    window_id: str = "window-offline"
    requests: tuple[ShadowLogicalRequest, ...] = Field(min_length=1)
    exact_ordinal_set: frozenset[int] | None = None
    request_plan_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_plan(self) -> ShadowLogicalRequestPlan:
        ordinals = tuple(item.ordinal for item in self.requests)
        if ordinals != tuple(range(len(ordinals))):
            raise ValueError("shadow request ordinals must be contiguous")
        expected_ordinals = frozenset(ordinals)
        if self.exact_ordinal_set is None:
            object.__setattr__(self, "exact_ordinal_set", expected_ordinals)
        elif self.exact_ordinal_set != expected_ordinals:
            raise ValueError("shadow request ordinal set mismatch")
        provider_id = self.provider_id or self.requests[0].provider_id
        window_id = self.window_id
        if self.job_id == "job-offline" and self.requests[0].job_id:
            object.__setattr__(self, "job_id", self.requests[0].job_id)
        if window_id == "window-offline" and self.requests[0].window_id:
            window_id = self.requests[0].window_id
            object.__setattr__(self, "window_id", window_id)
        if any(item.provider_id and item.provider_id != provider_id for item in self.requests):
            raise ValueError("shadow request provider mismatch")
        if any(item.job_id != self.job_id for item in self.requests):
            raise ValueError("shadow request plan identity mismatch")
        object.__setattr__(self, "provider_id", provider_id)
        expected = _sha(
            b"stock-eva/r2f3/request-plan/v1\n"
            + _json_bytes(
                {
                    "job_id": self.job_id,
                    "provider_id": provider_id,
                    "window_id": window_id,
                    "requests": [_request_contract_values(item) for item in self.requests],
                }
            )
        )
        if self.request_plan_sha256 == "0" * 64:
            object.__setattr__(self, "request_plan_sha256", expected)
        elif self.request_plan_sha256 != expected:
            raise ValueError("shadow request plan hash mismatch")
        return self

    @property
    def request_count(self) -> int:
        return len(self.requests)


class ShadowAttempt(_Frozen):
    attempt_id: str = Field(min_length=1, max_length=128)
    job_id: str = "job-offline"
    provider_id: str = ""
    trade_date: date | None = None
    universe_id: str = ""
    attempt_number: int = Field(default=1, ge=0)
    started_at: str = ""
    completed_at: str = ""
    ordinal: int = Field(ge=0)
    outcome: Literal["success", "failure", "skip", "unavailable", "mismatch"]
    rows: tuple[dict[str, Any], ...] = ()
    pages: tuple[dict[str, Any], ...] = ()
    failure_class: str = ""
    request_count: int = Field(default=1, ge=0)
    retry_count: int = Field(default=0, ge=0)
    rate_limit_count: int = Field(default=0, ge=0)
    final_page_refs: tuple[str, ...] = ()
    sanitized_orphan_audit_id: str | None = None


class ShadowRequestCompletion(_Frozen):
    ordinal: int = Field(ge=0)
    request_id: str = Field(min_length=1, max_length=128)
    endpoint: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    endpoint_class: str = Field(default="", pattern=r"^[A-Za-z0-9_.-]{0,64}$")
    final_attempt_id: str = Field(min_length=1, max_length=128)
    outcome: Literal["success", "failure", "skip", "unavailable", "mismatch"] = "success"
    attempt_ids: tuple[str, ...] = ()
    final_success: bool = True
    contiguous_page_refs: tuple[str, ...] = ()
    pages: tuple[dict[str, Any], ...] = ()
    completion_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def normalize_page_refs(self) -> ShadowRequestCompletion:
        if not self.pages and self.contiguous_page_refs:
            object.__setattr__(
                self,
                "pages",
                tuple(
                    {"page_identity": page_id, "rows": []} for page_id in self.contiguous_page_refs
                ),
            )
        if not self.attempt_ids:
            object.__setattr__(self, "attempt_ids", (self.final_attempt_id,))
        if self.endpoint_class == "":
            object.__setattr__(self, "endpoint_class", self.endpoint)
        expected_identities = tuple(
            f"{self.request_id}:page-{index:06d}" for index in range(1, len(self.pages) + 1)
        )
        identities = tuple(page.get("page_identity") for page in self.pages)
        if identities != expected_identities:
            raise ValueError("shadow page identity namespace is not contiguous")
        if self.outcome != "success" and (
            self.pages or self.final_success or self.contiguous_page_refs
        ):
            raise ValueError("non-success completion cannot carry pages")
        expected = _sha(
            b"stock-eva/r2f3/completion/v1\n" + _json_bytes(_completion_contract_values(self))
        )
        if self.completion_sha256 == "0" * 64:
            object.__setattr__(self, "completion_sha256", expected)
        elif self.completion_sha256 != expected:
            raise ValueError("shadow request completion hash mismatch")
        return self


def _completion_contract_values(completion: ShadowRequestCompletion) -> dict[str, Any]:
    pages = []
    for ordinal, page in enumerate(completion.pages, start=1):
        rows = page.get("rows", [])
        pages.append(
            {
                "ordinal": ordinal,
                "page_identity": page["page_identity"],
                "object_ref": page.get("object_ref", page["page_identity"]),
                "content_sha256": page.get("content_sha256", _sha(_json_bytes(rows))),
                "row_count": page.get("row_count", len(rows)),
            }
        )
    return {
        "ordinal": completion.ordinal,
        "request_id": completion.request_id,
        "endpoint": completion.endpoint,
        "endpoint_class": completion.endpoint_class,
        "final_attempt_id": completion.final_attempt_id,
        "final_outcome": completion.outcome,
        "page_refs": pages,
    }


class ShadowCompletion(_Frozen):
    session_id: str = "session-offline"
    job_id: str = "job-offline"
    evidence_id: str
    request_plan_sha256: str
    requests: tuple[ShadowRequestCompletion, ...]
    exact_ordinal_set: frozenset[int] | None = None
    aggregate_page_count: int = Field(default=0, ge=0)
    aggregate_row_count: int = Field(default=0, ge=0)
    committed_at: str = ""
    request_completions: tuple[ShadowRequestCompletion, ...] = ()
    completion_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def bind_completion_hash(self) -> ShadowCompletion:
        if not self.request_completions:
            object.__setattr__(self, "request_completions", self.requests)
        if self.exact_ordinal_set is None:
            object.__setattr__(
                self, "exact_ordinal_set", frozenset(item.ordinal for item in self.requests)
            )
        expected = _sha(
            b"stock-eva/r2f3/completion/v1\n"
            + _json_bytes(
                {
                    "job_id": self.job_id,
                    "provider_id": "",
                    "window_id": "",
                    "session_id": self.session_id,
                    "evidence_id": self.evidence_id,
                    "request_plan_sha256": self.request_plan_sha256,
                    "requests": [_completion_contract_values(item) for item in self.requests],
                }
            )
        )
        if self.completion_sha256 == "0" * 64:
            object.__setattr__(self, "completion_sha256", expected)
        elif self.completion_sha256 != expected:
            raise ValueError("shadow completion hash mismatch")
        return self


class ShadowAttemptReport(_Frozen):
    """Bounded report projection.  Raw payloads, URLs and exception text are excluded."""

    report_id: str
    attempt_id: str
    job_id: str = "offline"
    provider_id: str = ""
    window_id: str = "offline"
    session_id: str = "offline"
    logical_request_ordinal: int = 0
    request_id: str = ""
    endpoint_class: str = ""
    outcome: Literal["evidence_ready", "success", "failure", "skip", "unavailable", "mismatch"]
    started_at: str = ""
    completed_at: str = ""
    coverage_expected: int = Field(default=0, ge=0)
    coverage_observed: int = Field(default=0, ge=0)
    request_count: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    rate_limit_count: int = Field(default=0, ge=0)
    failure_class: str = ""
    page_identities: tuple[str, ...] = ()
    page_count: int = Field(default=0, ge=0)
    row_count: int = Field(default=0, ge=0)
    evidence_refs: tuple[str, ...] = ()
    evidence_id: str | None = None
    evidence_sha256: str | None = None
    candidate_sha256: str | None = None
    terminal_session_report_id: str | None = None
    durable_report_ref: str = ""
    report_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def sanitize_outcome(self) -> ShadowAttemptReport:
        if self.outcome in {"failure", "skip", "unavailable", "mismatch"} and (
            self.page_identities
            or self.evidence_refs
            or self.page_count
            or self.row_count
            or self.evidence_id
            or self.evidence_sha256
            or self.candidate_sha256
        ):
            raise ValueError("failed shadow report cannot reference evidence")
        if self.outcome in {"evidence_ready", "success"} and (
            not self.page_identities
            or not self.evidence_refs
            or not self.evidence_id
            or not self.evidence_sha256
            or self.page_count != len(self.page_identities)
            or self.row_count < 0
        ):
            raise ValueError("successful shadow report is incomplete")
        if self.outcome == "evidence_ready" and self.candidate_sha256 is not None:
            raise ValueError("evidence-ready report cannot carry a candidate")
        if self.outcome == "success" and self.candidate_sha256 is None:
            raise ValueError("terminal shadow report requires a candidate")
        expected = _sha(
            b"stock-eva/r2f3/shadow-attempt-report/v1\n" + self.canonical_bytes(include_hash=False)
        )
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("shadow report hash mismatch")
        return self

    def canonical_bytes(self, *, include_hash: bool = True) -> bytes:
        fields = self.model_dump(mode="json")
        if not include_hash:
            fields.pop("report_sha256", None)
        return _json_bytes(fields)


class ShadowEvidenceBundle(_Frozen):
    evidence_id: str
    completion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: tuple[dict[str, Any], ...]
    page_refs: tuple[str, ...]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_ready: bool = True


class ShadowRecoveryResult(_Frozen):
    removed: int = Field(ge=0)
    skipped: int = Field(ge=0)
    audit_id: str


class ShadowControlResult(_Frozen):
    """The only lifecycle result exposed by the Task11 control boundary."""

    outcome: Literal["evidence_ready", "failure", "skip", "unavailable", "mismatch"]
    session_report_id: str
    report_ids: tuple[str, ...] = ()
    evidence_id: str | None = None
    candidate_id: str | None = None


class ShadowEvidenceControlSink:
    """Append-only registry projection for successful shadow evidence.

    This class deliberately uses the registry's writer transaction as a friend.  It
    never inserts candidates or terminal attestations, and all failure paths carry
    empty evidence references.
    """

    def __init__(self, registry: Any):
        self.registry = registry

    @staticmethod
    def _report_digest(
        session_report_id: str, reports: tuple[ShadowAttemptReport, ...]
    ) -> tuple[bytes, str]:
        canonical = _json_bytes(
            {
                "session_report_id": session_report_id,
                "report_version": 1,
                "reports": [json.loads(item.canonical_bytes().decode("utf-8")) for item in reports],
            }
        )
        return canonical, _sha(b"stock-eva/r2f3/report-digest/v1\n" + canonical)

    def persist_evidence_ready(
        self,
        *,
        evidence: ShadowEvidenceBundle,
        plan: ShadowLogicalRequestPlan,
        completions: Iterable[ShadowRequestCompletion],
        attempts: Iterable[ShadowAttempt],
        session_id: str,
        trade_date: date,
        calendar_generation: str,
        calendar_sha256: str,
        universe_sha256: str,
        version_vector_sha256: str,
        bundle_ref: str | None = None,
        bundle_sha256: str | None = None,
    ) -> ShadowControlResult:
        completion_items = tuple(completions)
        attempt_by_id = {item.attempt_id: item for item in attempts}
        if (
            len(completion_items) != len(plan.requests)
            or {item.ordinal for item in completion_items} != plan.exact_ordinal_set
        ):
            raise ShadowEvidenceUnavailable("shadow completion is incomplete")
        reports: list[ShadowAttemptReport] = []
        refs: list[tuple[ShadowRequestCompletion, ShadowAttempt]] = []
        for completion in sorted(completion_items, key=lambda item: item.ordinal):
            request = plan.requests[completion.ordinal]
            attempt = attempt_by_id.get(completion.final_attempt_id)
            if (
                attempt is None
                or attempt.outcome != "success"
                or attempt.ordinal != completion.ordinal
                or completion.request_id != request.request_id
                or completion.endpoint != request.endpoint
            ):
                raise ShadowEvidenceUnavailable("shadow final attempt unavailable")
            page_ids = tuple(str(page["page_identity"]) for page in completion.pages)
            row_count = sum(len(page.get("rows", [])) for page in completion.pages)
            report_id = f"report-{_safe(session_id)}-{completion.ordinal:06d}"
            report = ShadowAttemptReport(
                report_id=report_id,
                attempt_id=_safe(attempt.attempt_id),
                job_id=plan.job_id,
                provider_id=plan.provider_id or request.provider_id,
                window_id=plan.window_id,
                session_id=_safe(session_id),
                logical_request_ordinal=completion.ordinal,
                request_id=request.request_id,
                endpoint_class=completion.endpoint_class,
                outcome="evidence_ready",
                started_at=attempt.started_at,
                completed_at=attempt.completed_at,
                coverage_expected=1,
                coverage_observed=row_count,
                request_count=attempt.request_count,
                retry_count=attempt.retry_count,
                rate_limit_count=attempt.rate_limit_count,
                page_identities=page_ids,
                page_count=len(page_ids),
                row_count=row_count,
                evidence_refs=page_ids,
                evidence_id=evidence.evidence_id,
                evidence_sha256=evidence.manifest_sha256,
                durable_report_ref=bundle_ref or f"evidence/{evidence.evidence_id}",
            )
            reports.append(report)
            refs.append((completion, attempt))
        reports_tuple = tuple(reports)
        session_report_id = f"session-report-{_safe(session_id)}-v1"
        report_json, report_sha = self._report_digest(session_report_id, reports_tuple)
        bundle_ref_value = bundle_ref or f"evidence/{evidence.evidence_id}"
        bundle_sha_value = bundle_sha256 or evidence.manifest_sha256

        def save(connection):
            row = connection.execute(
                "SELECT run_status,state_version,trade_date,universe_id "
                "FROM shadow_job WHERE job_id=? AND provider_id=? AND window_id=?",
                (plan.job_id, plan.provider_id, plan.window_id),
            ).fetchone()
            if row is None or row[0] not in {"leased", "pending"}:
                raise ShadowEvidenceUnavailable("shadow job is not writable")
            old_version = int(row[1])
            changed = connection.execute(
                "UPDATE shadow_job SET run_status='pending_normalization',"
                "state_version=state_version+1 WHERE job_id=? AND provider_id=? "
                "AND window_id=? AND state_version=? AND run_status IN ('leased','pending')",
                (plan.job_id, plan.provider_id, plan.window_id, old_version),
            )
            if changed.rowcount != 1:
                raise ShadowEvidenceUnavailable("shadow job state changed")
            state_version = old_version + 1
            connection.execute(
                "INSERT INTO shadow_evidence_ref VALUES (?,?,?,?,?,?,?,?,?,NULL)",
                (
                    evidence.evidence_id,
                    plan.job_id,
                    plan.provider_id,
                    plan.window_id,
                    session_id,
                    evidence.completion_sha256,
                    evidence.manifest_sha256,
                    bundle_ref_value,
                    bundle_sha_value,
                ),
            )
            for report in reports_tuple:
                connection.execute(
                    "INSERT INTO shadow_attempt_report VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        report.attempt_id,
                        report.report_id,
                        report.job_id,
                        report.provider_id,
                        report.window_id,
                        report.session_id,
                        report.request_id,
                        report.endpoint_class,
                        report.endpoint_class,
                        report.logical_request_ordinal,
                        1,
                        version_vector_sha256,
                        report.outcome,
                        report.started_at,
                        report.completed_at,
                        report.coverage_expected,
                        report.coverage_observed,
                        report.request_count,
                        report.retry_count,
                        report.rate_limit_count,
                        report.failure_class,
                        json.dumps(report.page_identities, separators=(",", ":")),
                        report.page_count,
                        report.row_count,
                        1,
                        report.durable_report_ref,
                        report.report_sha256,
                        json.dumps(report.evidence_refs, separators=(",", ":")),
                        report.evidence_id,
                        report.evidence_sha256,
                        None,
                        None,
                        state_version,
                    ),
                )
            for completion, attempt in refs:
                connection.execute(
                    "INSERT INTO shadow_evidence_attempt_ref VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        evidence.evidence_id,
                        plan.provider_id,
                        plan.job_id,
                        plan.window_id,
                        session_id,
                        completion.ordinal,
                        attempt.attempt_id,
                        completion.endpoint,
                        completion.request_id,
                        json.dumps(
                            tuple(page["page_identity"] for page in completion.pages),
                            separators=(",", ":"),
                        ),
                        len(completion.pages),
                        sum(len(page.get("rows", [])) for page in completion.pages),
                    ),
                )
            connection.execute(
                "INSERT INTO session_report VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    session_report_id,
                    plan.provider_id,
                    plan.job_id,
                    plan.window_id,
                    session_id,
                    reports_tuple[0].attempt_id,
                    evidence.evidence_id,
                    None,
                    None,
                    1,
                    trade_date.isoformat(),
                    "evidence_ready",
                    calendar_generation,
                    calendar_sha256,
                    universe_sha256,
                    version_vector_sha256,
                    evidence.manifest_sha256,
                    None,
                    f"session/{session_report_id}",
                    report_sha,
                    state_version,
                ),
            )
            return ShadowControlResult(
                outcome="evidence_ready",
                session_report_id=session_report_id,
                report_ids=tuple(report.report_id for report in reports_tuple),
                evidence_id=evidence.evidence_id,
            )

        try:
            return self.registry._with_transaction(save)
        except ShadowEvidenceUnavailable:
            raise
        except Exception as exc:
            raise ShadowEvidenceUnavailable("shadow lifecycle unavailable") from exc

    # Explicit aliases keep callers from accidentally treating evidence_ready as terminal success.
    persist = persist_evidence_ready
    record_evidence_ready = persist_evidence_ready

    def persist_failure(
        self,
        *,
        job_id: str,
        provider_id: str,
        window_id: str,
        session_id: str,
        trade_date: date,
        outcome: Literal["failure", "skip", "unavailable", "mismatch"] = "failure",
        ordinals: Iterable[int] = (),
        calendar_generation: str = "",
        calendar_sha256: str = "0" * 64,
        universe_sha256: str = "0" * 64,
        version_vector_sha256: str = "0" * 64,
    ) -> ShadowControlResult:
        """Persist a sanitized non-success graph with no evidence or candidate refs."""
        safe_job = _safe(job_id)
        safe_provider = _safe(provider_id)
        safe_window = _safe(window_id)
        safe_session = _safe(session_id)
        ordinals_tuple = tuple(sorted(set(int(item) for item in ordinals)))
        reports = tuple(
            ShadowAttemptReport(
                report_id=f"report-{safe_session}-{ordinal:06d}",
                attempt_id=f"attempt-{safe_session}-{ordinal:06d}",
                job_id=safe_job,
                provider_id=safe_provider,
                window_id=safe_window,
                session_id=safe_session,
                logical_request_ordinal=ordinal,
                request_id=f"request-{ordinal:06d}",
                endpoint_class="unknown",
                outcome=outcome,
                failure_class="provider_failure" if outcome == "failure" else outcome,
                durable_report_ref=f"session/{safe_session}",
            )
            for ordinal in ordinals_tuple
        )
        report_ids = tuple(item.report_id for item in reports)
        session_report_id = f"session-report-{safe_session}-v1"
        _report_json, report_sha = self._report_digest(session_report_id, reports)
        terminal_status = "unavailable" if outcome == "unavailable" else "failed"

        def save(connection):
            row = connection.execute(
                "SELECT run_status,state_version FROM shadow_job "
                "WHERE job_id=? AND provider_id=? AND window_id=?",
                (safe_job, safe_provider, safe_window),
            ).fetchone()
            if row is None or row[0] not in {"leased", "pending", "pending_normalization"}:
                raise ShadowEvidenceUnavailable("shadow job is not writable")
            old_version = int(row[1])
            changed = connection.execute(
                "UPDATE shadow_job SET run_status=?,state_version=state_version+1 "
                "WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=?",
                (terminal_status, safe_job, safe_provider, safe_window, old_version),
            )
            if changed.rowcount != 1:
                raise ShadowEvidenceUnavailable("shadow job state changed")
            state_version = old_version + 1
            for report in reports:
                connection.execute(
                    "INSERT INTO shadow_attempt_report VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        report.attempt_id,
                        report.report_id,
                        report.job_id,
                        report.provider_id,
                        report.window_id,
                        report.session_id,
                        report.request_id,
                        "unknown",
                        report.endpoint_class,
                        report.logical_request_ordinal,
                        1,
                        version_vector_sha256,
                        report.outcome,
                        "",
                        "",
                        0,
                        0,
                        0,
                        0,
                        0,
                        report.failure_class,
                        "[]",
                        0,
                        0,
                        0,
                        report.durable_report_ref,
                        report.report_sha256,
                        "[]",
                        None,
                        None,
                        None,
                        None,
                        state_version,
                    ),
                )
            connection.execute(
                "INSERT INTO session_report VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    session_report_id,
                    safe_provider,
                    safe_job,
                    safe_window,
                    safe_session,
                    None,
                    None,
                    None,
                    None,
                    1,
                    trade_date.isoformat(),
                    outcome,
                    calendar_generation,
                    calendar_sha256,
                    universe_sha256,
                    version_vector_sha256,
                    None,
                    None,
                    f"session/{session_report_id}",
                    report_sha,
                    state_version,
                ),
            )
            return ShadowControlResult(
                outcome=outcome, session_report_id=session_report_id, report_ids=report_ids
            )

        try:
            return self.registry._with_transaction(save)
        except ShadowEvidenceUnavailable:
            raise
        except Exception as exc:
            raise ShadowEvidenceUnavailable("shadow lifecycle unavailable") from exc

    record_failure = persist_failure


class ShadowAttemptCompletion(_Frozen):
    ordinal: int = Field(ge=0)
    attempt_id: str
    request_id: str
    endpoint: str
    session_id: str = "offline"
    outcome: Literal["success", "failure", "skip", "unavailable", "mismatch"]
    page_identities: tuple[str, ...] = ()
    page_count: int = Field(default=0, ge=0)
    row_count: int = Field(default=0, ge=0)
    terminal_marker: bool = False
    evidence_refs: tuple[str, ...] = ()
    evidence_sha256: str | None = None

    @model_validator(mode="after")
    def validate_failure_shape(self) -> ShadowAttemptCompletion:
        if self.outcome != "success" and (
            self.page_identities
            or self.page_count
            or self.row_count
            or self.evidence_refs
            or self.evidence_sha256
        ):
            raise ValueError("failed shadow completion cannot carry evidence")
        return self


class ShadowEvidenceAttemptRef(_Frozen):
    evidence_id: str
    provider_id: str = ""
    job_id: str = "job-offline"
    window_id: str = "window-offline"
    session_id: str = "session-offline"
    ordinal: int = Field(ge=0)
    attempt_id: str
    endpoint: str
    request_id: str
    page_refs: tuple[str, ...] = ()
    page_count: int = Field(default=0, ge=0)
    row_count: int = Field(default=0, ge=0)


class ShadowEvidenceManifest(_Frozen):
    evidence_id: str
    provider_id: str = ""
    adapter_version: str = "r2f3-task11"
    endpoint_contract_version: str = "r2f3-task11"
    trade_date: date | None = None
    universe_id: str = ""
    request_plan_hash: str
    request_plan_sha256: str | None = None
    completion_sha256: str
    final_attempt_ids: tuple[str, ...] = ()
    source_object_refs: tuple[str, ...] = ()
    page_identities: tuple[str, ...] = ()
    object_count: int = Field(default=0, ge=0)
    row_count: int = Field(default=0, ge=0)
    request_count: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    rate_limit_count: int = Field(default=0, ge=0)
    source_schema_hash: str = ""
    schema_version: str = "r2f3-task11"
    bundle_relative_path: str = ""
    bundle_commit_sha256: str = ""
    orphan_audit_id: str | None = None
    manifest_sha256: str

    @model_validator(mode="after")
    def bind_plan_hash(self) -> ShadowEvidenceManifest:
        if self.request_plan_sha256 is None:
            object.__setattr__(self, "request_plan_sha256", self.request_plan_hash)
        elif self.request_plan_sha256 != self.request_plan_hash:
            raise ValueError("shadow evidence plan hash mismatch")
        return self


def _open_verified(path: Path, *, max_bytes: int) -> bytes:
    # macOS exposes /var and /tmp as system symlinks.  Resolve only these trusted
    # aliases before the no-follow ancestor walk; user-created symlinks remain rejected.
    absolute = Path(os.path.abspath(path))
    if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
        path = Path("/private/var", *absolute.parts[2:])
    elif absolute.parts[1:2] == ("tmp",) and Path("/tmp").is_symlink():
        path = Path("/private/tmp", *absolute.parts[2:])
    if path.name in {".", ".."} or "/" in path.name:
        raise ShadowEvidenceUnavailable("shadow evidence unavailable")
    try:
        for ancestor in reversed(path.parents):
            info = os.lstat(ancestor)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
    except OSError:
        raise ShadowEvidenceUnavailable("shadow evidence unavailable") from None
    try:
        parent_fd = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        fd = os.open(path.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)
    except OSError:
        raise ShadowEvidenceUnavailable("shadow evidence unavailable") from None
    finally:
        if "parent_fd" in locals():
            os.close(parent_fd)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size > max_bytes
            or before.st_mode & 0o022
        ):
            raise ShadowEvidenceUnavailable("shadow evidence unavailable")
        data = os.read(fd, max_bytes + 1)
        after = os.fstat(fd)
        if len(data) > max_bytes or (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ShadowEvidenceUnavailable("shadow evidence unavailable")
        return data
    finally:
        os.close(fd)


def _write_private_file(path: Path, payload: bytes) -> None:
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        fd = os.open(
            path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent_fd,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o022:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
        finally:
            os.close(fd)
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def _exclusive_rename(source: Path, destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(destination)
    # macOS exposes renameatx_np(2), whose RENAME_EXCL flag makes this one
    # atomic no-clobber operation.  Keep a conservative fallback for Linux test
    # runners; the destination preflight still makes ordinary collisions fail
    # closed, and never replaces an existing bundle.
    if sys.platform == "darwin":
        import ctypes

        renameatx_np = getattr(ctypes.CDLL(None), "renameatx_np", None)
        if renameatx_np is None:
            raise ShadowEvidenceUnavailable("shadow atomic publish unavailable")
        renameatx_np.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameatx_np.restype = ctypes.c_int
        source_parent = os.open(
            source.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        destination_parent = os.open(
            destination.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            if (
                renameatx_np(
                    source_parent,
                    source.name.encode(),
                    destination_parent,
                    destination.name.encode(),
                    0x00000004,
                )
                != 0
            ):
                raise ShadowEvidenceUnavailable("shadow atomic publish unavailable")
        finally:
            os.close(source_parent)
            os.close(destination_parent)
        return
    os.rename(source, destination)


class ShadowEvidenceStore:
    def __init__(
        self,
        root: Path | str,
        *,
        max_object_bytes: int = 64 * 1024 * 1024,
        max_rows: int = 10_000_000,
    ):
        self.root = Path(root)
        self.max_object_bytes = max_object_bytes
        self.max_rows = max_rows

    def _prepare_root(self) -> None:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.staging = self.root / "staging"
        self.bundles = self.root / "bundles"
        self.audit = self.root / "audit"
        for path in (self.staging, self.bundles, self.audit):
            path.mkdir(mode=0o700, exist_ok=True)

    def recover_orphans(self) -> ShadowRecoveryResult:
        self._prepare_root()
        removed = 0
        skipped = 0
        for entry in os.scandir(self.staging):
            try:
                info = entry.stat(follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode) or info.st_nlink < 1:
                    skipped += 1
                    continue
                owner = Path(entry.path) / "OWNER"
                marker = _open_verified(owner, max_bytes=4096)
                values = json.loads(marker.decode("utf-8"))
                if (
                    set(values) != {"nonce", "evidence_id", "job_id"}
                    or values["nonce"] != entry.name
                    or not _safe(values["evidence_id"])
                    or not _safe(values["job_id"])
                ):
                    skipped += 1
                    continue
                descendants = list(os.walk(entry.path, topdown=True, followlinks=False))
                if any(Path(path).is_symlink() for path, _dirs, _files in descendants):
                    skipped += 1
                    continue
                shutil.rmtree(entry.path)
                removed += 1
            except Exception:
                skipped += 1
        audit_id = f"audit-{secrets.token_hex(8)}"
        audit = self.audit / f"{audit_id}.json"
        payload = _json_bytes({"audit_id": audit_id, "removed": removed, "skipped": skipped})
        _write_private_file(audit, payload)
        return ShadowRecoveryResult(removed=removed, skipped=skipped, audit_id=audit_id)

    def record_failure(
        self, attempt_id: str, *, outcome: str = "failure", error: str = ""
    ) -> ShadowAttemptReport:
        self._prepare_root()
        safe_outcome = (
            outcome
            if outcome in {"evidence_ready", "failure", "skip", "unavailable", "mismatch"}
            else "failure"
        )
        failure_class = (
            "timeout"
            if outcome == "timeout"
            else ("provider_failure" if safe_outcome == "failure" else "")
        )
        # The input is intentionally ignored; it must never cross the report boundary.
        report = ShadowAttemptReport(
            report_id=f"report-{_sha(attempt_id.encode())[:24]}",
            attempt_id=_safe(attempt_id),
            outcome=safe_outcome,
            failure_class=failure_class,
            durable_report_ref="audit/report.json",
        )
        path = self.audit / f"{report.report_id}.json"
        path.write_bytes(report.canonical_bytes())
        os.chmod(path, 0o600)
        return report

    def publish(
        self,
        *,
        plan: Iterable[ShadowLogicalRequest] | ShadowLogicalRequestPlan,
        completions: Iterable[ShadowRequestCompletion],
        attempts: Iterable[ShadowAttempt],
        simulate_crash: bool = False,
    ) -> ShadowEvidenceBundle:
        self._prepare_root()
        plan_obj = (
            plan
            if isinstance(plan, ShadowLogicalRequestPlan)
            else ShadowLogicalRequestPlan(requests=tuple(plan))
        )
        completion_items = tuple(completions)
        attempt_items = tuple(attempts)
        if {item.ordinal for item in completion_items} != plan_obj.exact_ordinal_set or len(
            completion_items
        ) != len(plan_obj.requests):
            raise ShadowEvidenceUnavailable("shadow completion is incomplete")
        by_id = {item.attempt_id: item for item in attempt_items}
        for completion in completion_items:
            attempt = by_id.get(completion.final_attempt_id)
            planned = plan_obj.requests[completion.ordinal]
            if (
                attempt is None
                or attempt.ordinal != completion.ordinal
                or attempt.outcome != "success"
                or completion.request_id != planned.request_id
                or completion.endpoint != planned.endpoint
                or not completion.pages
            ):
                raise ShadowEvidenceUnavailable("shadow final attempt unavailable")
            identities = tuple(page.get("page_identity") for page in completion.pages)
            expected = tuple(
                f"{completion.request_id}:page-{index:06d}"
                for index in range(1, len(identities) + 1)
            )
            if (
                identities != expected
                or len(set(identities)) != len(identities)
                or any(not isinstance(page.get("rows", []), list) for page in completion.pages)
            ):
                raise ShadowEvidenceUnavailable("shadow final pages are not contiguous")
        completion_preimage = _json_bytes(
            [
                _completion_contract_values(item)
                for item in sorted(completion_items, key=lambda item: item.ordinal)
            ]
        )
        completion_sha = _sha(b"stock-eva/r2f3/completion/v1\n" + completion_preimage)
        evidence_id = (
            "ev-" + _sha(plan_obj.request_plan_sha256.encode() + completion_sha.encode())[:24]
        )
        rows: list[dict[str, Any]] = []
        page_refs: list[str] = []
        page_descriptors: list[dict[str, Any]] = []
        nonce = f"{evidence_id}-{secrets.token_hex(8)}"
        staging = self.staging / nonce
        try:
            staging.mkdir(mode=0o700)
        except FileExistsError:
            raise ShadowEvidenceUnavailable("shadow staging identity unavailable") from None
        owner = staging / "OWNER"
        _write_private_file(
            owner,
            _json_bytes({"nonce": nonce, "evidence_id": evidence_id, "job_id": plan_obj.job_id}),
        )
        pages = staging / "pages"
        pages.mkdir(mode=0o700)
        try:
            for completion in sorted(completion_items, key=lambda item: item.ordinal):
                attempt = by_id[completion.final_attempt_id]
                rows.extend(dict(row) for row in attempt.rows)
                for page_number, page in enumerate(completion.pages, start=1):
                    page_id = str(page["page_identity"])
                    page_path = pages / f"{completion.ordinal}-{page_number}.json"
                    payload = _json_bytes(page)
                    if len(payload) > self.max_object_bytes:
                        raise ShadowEvidenceUnavailable("shadow object exceeds bound")
                    _write_private_file(page_path, payload)
                    page_refs.append(page_id)
                    page_descriptors.append(
                        {
                            "ordinal": completion.ordinal,
                            "page_identity": page_id,
                            "object_ref": _safe_object_ref(page.get("object_ref", page_id)),
                            "relative_path": f"pages/{completion.ordinal}-{page_number}.json",
                            "content_sha256": _sha(payload),
                            "row_count": len(page.get("rows", [])),
                        }
                    )
            if len(rows) > self.max_rows:
                raise ShadowEvidenceUnavailable("shadow row budget exhausted")
            manifest_values = {
                "evidence_id": evidence_id,
                "plan_sha256": plan_obj.request_plan_sha256,
                "completion_sha256": completion_sha,
                "page_refs": page_refs,
                "pages": page_descriptors,
                "row_count": len(rows),
            }
            manifest_sha = _sha(
                b"stock-eva/r2f3/shadow-evidence/v1\n" + _json_bytes(manifest_values)
            )
            manifest = {**manifest_values, "manifest_sha256": manifest_sha, "rows": rows}
            manifest_path = staging / "manifest.json"
            payload = _json_bytes(manifest)
            if len(payload) > self.max_object_bytes:
                raise ShadowEvidenceUnavailable("shadow manifest exceeds bound")
            _write_private_file(manifest_path, payload)
            _write_private_file(staging / "COMMIT", b"COMMIT\n")
            pages_fd = os.open(pages, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(pages_fd)
            finally:
                os.close(pages_fd)
            if simulate_crash:
                raise RuntimeError("shadow publish interrupted")
            destination = self.bundles / evidence_id
            if destination.exists():
                existing = ShadowEvidenceReader(self.root).read(evidence_id)
                if (
                    existing.plan_sha256 == plan_obj.request_plan_sha256
                    and existing.completion_sha256 == completion_sha
                    and existing.rows == tuple(rows)
                    and existing.page_refs == tuple(page_refs)
                ):
                    shutil.rmtree(staging)
                    return existing
                raise ShadowEvidenceUnavailable("shadow evidence identity conflict")
            staging_fd = os.open(staging, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            _exclusive_rename(staging, destination)
            parent_fd = os.open(self.bundles, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
            return ShadowEvidenceBundle(
                evidence_id=evidence_id,
                completion_sha256=completion_sha,
                plan_sha256=plan_obj.request_plan_sha256,
                rows=tuple(rows),
                page_refs=tuple(page_refs),
                manifest_sha256=manifest_sha,
            )
        except Exception:
            # Staging is intentionally left as a bounded, owner-marked orphan for a scanner.
            raise


class ShadowEvidenceReader:
    def __init__(self, root: Path | str, *, max_object_bytes: int = 64 * 1024 * 1024):
        self.root = Path(root)
        self.max_object_bytes = max_object_bytes

    def read(self, evidence_id: str) -> ShadowEvidenceBundle:
        try:
            evidence_id = _safe(evidence_id)
            bundle = self.root / "bundles" / evidence_id
            bundle_info = os.lstat(bundle)
            if not stat.S_ISDIR(bundle_info.st_mode) or bundle_info.st_nlink < 2:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            allowed_root = {"OWNER", "COMMIT", "manifest.json", "pages"}
            root_entries = list(os.scandir(bundle))
            if {entry.name for entry in root_entries} != allowed_root:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            pages_dir = bundle / "pages"
            pages_info = os.lstat(pages_dir)
            if not stat.S_ISDIR(pages_info.st_mode) or pages_info.st_nlink < 2:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            commit = _open_verified(bundle / "COMMIT", max_bytes=64)
            if commit != b"COMMIT\n":
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            raw = _open_verified(bundle / "manifest.json", max_bytes=self.max_object_bytes)
            payload = json.loads(raw.decode("utf-8"))
            required = {
                "evidence_id",
                "plan_sha256",
                "completion_sha256",
                "page_refs",
                "pages",
                "row_count",
                "manifest_sha256",
                "rows",
            }
            if set(payload) != required or payload["evidence_id"] != evidence_id:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            manifest_values = {
                key: payload[key]
                for key in (
                    "evidence_id",
                    "plan_sha256",
                    "completion_sha256",
                    "page_refs",
                    "pages",
                    "row_count",
                )
            }
            if payload["manifest_sha256"] != _sha(
                b"stock-eva/r2f3/shadow-evidence/v1\n" + _json_bytes(manifest_values)
            ):
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            if (
                not isinstance(payload["rows"], list)
                or len(payload["rows"]) != payload["row_count"]
            ):
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            if not isinstance(payload["page_refs"], list) or len(set(payload["page_refs"])) != len(
                payload["page_refs"]
            ):
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            pages = payload["pages"]
            if (
                not isinstance(pages, list)
                or [item.get("page_identity") for item in pages] != payload["page_refs"]
            ):
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            expected_files = {Path(item["relative_path"]).name for item in pages}
            actual_files = set()
            for entry in os.scandir(pages_dir):
                info = entry.stat(follow_symlinks=False)
                if (
                    entry.name in actual_files
                    or not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                ):
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
                actual_files.add(entry.name)
            if actual_files != expected_files:
                raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            seen_page_ids: set[str] = set()
            page_ordinals: dict[int, int] = {}
            for item in pages:
                if (
                    not isinstance(item, dict)
                    or set(item)
                    != {
                        "ordinal",
                        "page_identity",
                        "object_ref",
                        "relative_path",
                        "content_sha256",
                        "row_count",
                    }
                    or not isinstance(item["relative_path"], str)
                    or _safe_object_ref(item["object_ref"]) != item["object_ref"]
                    or ".." in Path(item["relative_path"]).parts
                    or not item["relative_path"].startswith("pages/")
                    or item["page_identity"] in seen_page_ids
                    or not isinstance(item["ordinal"], int)
                    or item["ordinal"] < 0
                ):
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
                page_identity = item["page_identity"]
                if (
                    ":page-" not in page_identity
                    or not page_identity.rsplit(":page-", 1)[1].isdigit()
                ):
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
                page_number = int(page_identity.rsplit(":page-", 1)[1])
                if page_number != page_ordinals.get(item["ordinal"], 0) + 1:
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
                page_ordinals[item["ordinal"]] = page_number
                seen_page_ids.add(page_identity)
                page_path = bundle / item["relative_path"]
                page_raw = _open_verified(page_path, max_bytes=self.max_object_bytes)
                if _sha(page_raw) != item["content_sha256"]:
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
                page = json.loads(page_raw.decode("utf-8"))
                if (
                    not isinstance(page, dict)
                    or page.get("page_identity") != item["page_identity"]
                    or not isinstance(page.get("rows"), list)
                    or len(page["rows"]) != item["row_count"]
                ):
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
            return ShadowEvidenceBundle(
                evidence_id=evidence_id,
                completion_sha256=payload["completion_sha256"],
                plan_sha256=payload["plan_sha256"],
                rows=tuple(payload["rows"]),
                page_refs=tuple(payload["page_refs"]),
                manifest_sha256=payload["manifest_sha256"],
            )
        except ShadowEvidenceUnavailable:
            raise
        except Exception:
            raise ShadowEvidenceUnavailable("shadow evidence unavailable") from None
