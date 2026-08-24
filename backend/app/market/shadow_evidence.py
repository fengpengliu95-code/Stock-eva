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
import stat
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


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class ShadowLogicalRequest(_Frozen):
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
        values = self.model_dump(mode="json", exclude={"request_hash"})
        expected = _sha(b"stock-eva/r2f3/shadow-request/v1\n" + _json_bytes(values))
        if self.request_hash == "0" * 64:
            object.__setattr__(self, "request_hash", expected)
        elif self.request_hash != expected:
            raise ValueError("shadow request hash mismatch")
        return self


class ShadowLogicalRequestPlan(_Frozen):
    plan_id: str = "plan-offline"
    job_id: str = "job-offline"
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
        expected = _sha(
            b"stock-eva/r2f3/shadow-request-plan/v1\n"
            + _json_bytes([item.model_dump(mode="json") for item in self.requests])
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
    final_attempt_id: str = Field(min_length=1, max_length=128)
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
        expected = _sha(
            b"stock-eva/r2f3/shadow-request-completion/v1\n"
            + _json_bytes(self.model_dump(mode="json", exclude={"completion_sha256"}))
        )
        if self.completion_sha256 == "0" * 64:
            object.__setattr__(self, "completion_sha256", expected)
        elif self.completion_sha256 != expected:
            raise ValueError("shadow request completion hash mismatch")
        return self


class ShadowCompletion(_Frozen):
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
            b"stock-eva/r2f3/shadow-completion/v1\n"
            + _json_bytes(self.model_dump(mode="json", exclude={"completion_sha256"}))
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
            or self.row_count < 1
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
        if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes or before.st_mode & 0o022:
            raise ShadowEvidenceUnavailable("shadow evidence unavailable")
        data = os.read(fd, max_bytes + 1)
        after = os.fstat(fd)
        if len(data) > max_bytes or (before.st_dev, before.st_ino, before.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ):
            raise ShadowEvidenceUnavailable("shadow evidence unavailable")
        return data
    finally:
        os.close(fd)


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
            expected = tuple(f"page-{index}" for index in range(1, len(identities) + 1))
            if (
                identities != expected
                or len(set(identities)) != len(identities)
                or any(not isinstance(page.get("rows", []), list) for page in completion.pages)
            ):
                raise ShadowEvidenceUnavailable("shadow final pages are not contiguous")
        completion_preimage = _json_bytes(
            [item.model_dump(mode="json") for item in completion_items]
        )
        completion_sha = _sha(b"stock-eva/r2f3/shadow-completion/v1\n" + completion_preimage)
        evidence_id = (
            "ev-" + _sha(plan_obj.request_plan_sha256.encode() + completion_sha.encode())[:24]
        )
        rows: list[dict[str, Any]] = []
        page_refs: list[str] = []
        page_descriptors: list[dict[str, Any]] = []
        nonce = f"{evidence_id}-{secrets.token_hex(8)}"
        staging = self.staging / nonce
        staging.mkdir(mode=0o700)
        owner = staging / "OWNER"
        owner.write_text("shadow-evidence-store\n", encoding="utf-8")
        os.chmod(owner, 0o600)
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
                    page_path.write_bytes(payload)
                    os.chmod(page_path, 0o600)
                    page_refs.append(page_id)
                    page_descriptors.append(
                        {
                            "ordinal": completion.ordinal,
                            "page_identity": page_id,
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
            manifest_path.write_bytes(payload)
            os.chmod(manifest_path, 0o600)
            with manifest_path.open("rb") as handle:
                os.fsync(handle.fileno())
            (staging / "COMMIT").write_bytes(b"COMMIT\n")
            os.chmod(staging / "COMMIT", 0o600)
            if simulate_crash:
                raise RuntimeError("shadow publish interrupted")
            destination = self.bundles / evidence_id
            if destination.exists():
                raise ShadowEvidenceUnavailable("shadow evidence identity already exists")
            staging_fd = os.open(staging, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            os.rename(staging, destination)
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
            for item in pages:
                if (
                    not isinstance(item, dict)
                    or set(item)
                    != {
                        "ordinal",
                        "page_identity",
                        "relative_path",
                        "content_sha256",
                        "row_count",
                    }
                    or not isinstance(item["relative_path"], str)
                    or ".." in Path(item["relative_path"]).parts
                    or not item["relative_path"].startswith("pages/")
                ):
                    raise ShadowEvidenceUnavailable("shadow evidence unavailable")
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
