"""Read-only terminal graph validation and the sole terminal transaction writer."""

# Frozen SQL projections are intentionally kept byte-readable for review.
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import os
import unicodedata
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .providers.registry import ShadowRegistry
from .shadow_calendar import ConfirmedSessionSnapshot
from .shadow_candidates import ShadowCandidateReader
from .shadow_evidence import ShadowAttemptReport, ShadowEvidenceReader, ShadowLogicalRequestPlan
from .shadow_jobs import ShadowBundlePublisher


class ShadowTerminalUnavailable(RuntimeError):
    """Terminal graph cannot be proven complete."""


def _nfc(value: Any) -> Any:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_nfc(item) for item in value]
    if isinstance(value, dict):
        values = {unicodedata.normalize("NFC", str(key)): _nfc(item) for key, item in value.items()}
        if len(values) != len(value):
            raise ShadowTerminalUnavailable("terminal canonical key collision")
        return values
    if value is None:
        raise ShadowTerminalUnavailable("terminal canonical null")
    return value


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(_nfc(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def digest(domain: str, value: Any) -> tuple[bytes, str]:
    raw = value if isinstance(value, bytes) else canonical_json(value)
    return raw, hashlib.sha256(f"stock-eva/r2f3/{domain}/v1\n".encode() + raw).hexdigest()


def request_plan_digest(value: Any) -> tuple[bytes, str]:
    return digest("request-plan", value)


def completion_digest(value: Any) -> tuple[bytes, str]:
    return digest("completion", value)


def attempt_ordinal_closure_digest(value: Any) -> tuple[bytes, str]:
    return digest("attempt-ordinal-closure", value)


def report_digest(value: Any) -> tuple[bytes, str]:
    return digest("report-digest", value)


def canonical_preimage(domain: str, value: Any) -> bytes:
    """Return the exact domain-separated canonical JSON preimage."""
    raw, _ = digest(domain, value)
    return f"stock-eva/r2f3/{domain}/v1\n".encode() + raw


def compute_terminal_digests(
    request_plan: Any,
    completion: Any,
    ordinal_closure: Any,
    reports: Any,
) -> dict[str, tuple[bytes, str]]:
    """Compute all four frozen digest pairs without touching a registry."""
    return {
        "request_plan": request_plan_digest(request_plan),
        "completion": completion_digest(completion),
        "attempt_ordinal_closure": attempt_ordinal_closure_digest(ordinal_closure),
        "report_digest": report_digest(reports),
    }


@dataclass(frozen=True)
class TerminalGraphIdentity:
    provider_id: str
    job_id: str
    window_id: str
    session_id: str
    evidence_id: str
    candidate_id: str
    session_report_id: str


class ShadowTerminalAttestation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attestation_id: str
    provider_id: str
    job_id: str
    window_id: str
    session_id: str
    evidence_id: str
    candidate_id: str
    session_report_id: str
    session_report_version: int = Field(ge=2)
    request_plan_canonical_json: bytes
    completion_canonical_json: bytes
    attempt_ordinal_closure_canonical_json: bytes
    report_digest_canonical_json: bytes
    attempt_ordinal_closure_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_digest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    terminal_outcome: str = "success"
    immutable_version: int = Field(default=1, ge=1)


def _report_payload(report: ShadowAttemptReport) -> dict[str, Any]:
    return {
        "report_id": report.report_id,
        "attempt_id": report.attempt_id,
        "ordinal": report.logical_request_ordinal,
        "outcome": report.outcome,
        "report_sha256": report.report_sha256,
    }


def _strict_bundle_validation(
    *,
    plan: ShadowLogicalRequestPlan,
    evidence_reader,
    candidate_reader,
    evidence_id: str,
    candidate_id: str,
    identity: TerminalGraphIdentity,
):
    """Read Task11/Task12 objects through their descriptor-bound readers only."""
    if (
        type(plan) is not ShadowLogicalRequestPlan
        or type(evidence_reader) is not ShadowEvidenceReader
        or type(candidate_reader) is not ShadowCandidateReader
    ):
        raise ShadowTerminalUnavailable("terminal request plan unavailable")
    try:
        evidence, descriptor = evidence_reader.read_descriptor(evidence_id)
        candidate_bundle = candidate_reader.read(candidate_id)
    except Exception as exc:
        raise ShadowTerminalUnavailable("terminal evidence or candidate unavailable") from exc
    manifest = candidate_bundle.manifest
    candidate = candidate_bundle.candidate
    if (
        not getattr(evidence, "evidence_ready", False)
        or evidence.manifest_sha256 != manifest.evidence_sha256
        or manifest.evidence_id != evidence_id
        or manifest.candidate_id != candidate_id
        or manifest.provider_id != identity.provider_id
        or manifest.trade_date.isoformat()
        != str(descriptor.get("trade_date", manifest.trade_date))[:10]
        or manifest.universe_id != descriptor.get("universe_id", manifest.universe_id)
        or manifest.status != "accepted"
        or candidate.job_id != identity.job_id
        or candidate.window_id != identity.window_id
        or candidate.session_id != identity.session_id
        or candidate.quality_status != "ready"
        or candidate_bundle.quality_report.verdict != "pass"
        or plan.job_id != identity.job_id
        or plan.provider_id != identity.provider_id
        or plan.window_id != identity.window_id
    ):
        raise ShadowTerminalUnavailable("terminal object binding unavailable")
    if tuple(item.ordinal for item in plan.requests) != tuple(range(len(plan.requests))):
        raise ShadowTerminalUnavailable("terminal ordinal set unavailable")
    if descriptor.get("plan_sha256") != plan.request_plan_sha256:
        raise ShadowTerminalUnavailable("terminal request plan hash unavailable")
    completion = descriptor.get("completion", {})
    if completion.get("request_plan_sha256") != plan.request_plan_sha256:
        raise ShadowTerminalUnavailable("terminal completion hash unavailable")
    if evidence.completion_sha256 != completion_digest(completion)[1]:
        raise ShadowTerminalUnavailable("terminal completion digest unavailable")
    completion_items = completion.get("requests", [])
    if (
        not isinstance(completion_items, list)
        or not all(isinstance(item, dict) for item in completion_items)
        or [item.get("ordinal") for item in completion_items] != sorted(plan.exact_ordinal_set)
        or {item.get("ordinal") for item in completion_items} != set(plan.exact_ordinal_set)
    ):
        raise ShadowTerminalUnavailable("terminal completion ordinals unavailable")
    descriptor_pages = descriptor.get("pages", [])
    if not isinstance(descriptor_pages, list) or not descriptor_pages:
        raise ShadowTerminalUnavailable("terminal page closure unavailable")
    flattened_pages = []
    for item in completion_items:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("page_refs"), list)
            or not isinstance(item.get("final_attempt_id"), str)
            or not item.get("final_attempt_id")
            or item.get("final_outcome") != "success"
        ):
            raise ShadowTerminalUnavailable("terminal page closure unavailable")
        request = plan.requests[item["ordinal"]]
        if (
            item.get("request_id") != request.request_id
            or item.get("endpoint") != request.endpoint
            or item.get("endpoint_class") != request.endpoint_class
        ):
            raise ShadowTerminalUnavailable("terminal endpoint binding unavailable")
        for page in item.get("page_refs", []):
            if (
                not page.get("page_identity")
                or not page.get("object_ref")
                or len(page.get("content_sha256", "")) != 64
                or any(char not in "0123456789abcdef" for char in page.get("content_sha256", ""))
                or not isinstance(page.get("row_count"), int)
                or page.get("row_count", -1) < 0
            ):
                raise ShadowTerminalUnavailable("terminal page binding unavailable")
            flattened_pages.append(
                {
                    key: page.get(key)
                    for key in (
                        "ordinal",
                        "page_identity",
                        "object_ref",
                        "content_sha256",
                        "row_count",
                    )
                }
            )
    if flattened_pages != [
        {
            key: page.get(key)
            for key in (
                "ordinal",
                "page_identity",
                "object_ref",
                "content_sha256",
                "row_count",
            )
        }
        for page in descriptor_pages
    ]:
        raise ShadowTerminalUnavailable("terminal page closure unavailable")
    return evidence, candidate_bundle, completion_items


def validate_terminal_digests(attestation: ShadowTerminalAttestation) -> None:
    for domain, raw, expected in (
        ("request-plan", attestation.request_plan_canonical_json, attestation.request_plan_sha256),
        ("completion", attestation.completion_canonical_json, attestation.completion_sha256),
        (
            "attempt-ordinal-closure",
            attestation.attempt_ordinal_closure_canonical_json,
            attestation.attempt_ordinal_closure_sha256,
        ),
        (
            "report-digest",
            attestation.report_digest_canonical_json,
            attestation.report_digest_sha256,
        ),
    ):
        canonical, actual = digest(domain, raw)
        if canonical != raw or actual != expected:
            raise ShadowTerminalUnavailable("terminal digest mismatch")


def _write_terminal_success_graph(
    registry: ShadowRegistry,
    *,
    plan: ShadowLogicalRequestPlan,
    evidence_reader,
    candidate_reader,
    identity: TerminalGraphIdentity,
    calendar_generation: str,
    calendar_sha256: str,
    universe_sha256: str,
    version_vector_sha256: str,
    expected_job_state_version: int,
    expected_window_state_version: int,
    snapshot: ConfirmedSessionSnapshot,
    bundle_publisher: ShadowBundlePublisher,
    now=None,
) -> ShadowTerminalAttestation:
    """Complete one Task13 graph using strict Task11/Task12 readers and one CAS transaction."""
    if (
        type(snapshot) is not ConfirmedSessionSnapshot
        or type(bundle_publisher) is not ShadowBundlePublisher
    ):
        raise ShadowTerminalUnavailable("terminal snapshot unavailable")
    for name, expected in (
        ("calendar_generation", calendar_generation),
        ("calendar_sha256", calendar_sha256),
        ("universe_sha256", universe_sha256),
    ):
        if getattr(snapshot, name, None) != expected:
            raise ShadowTerminalUnavailable("terminal snapshot drift")
    evidence, candidate_bundle, completion_items = _strict_bundle_validation(
        plan=plan,
        evidence_reader=evidence_reader,
        candidate_reader=candidate_reader,
        evidence_id=identity.evidence_id,
        candidate_id=identity.candidate_id,
        identity=identity,
    )
    evidence_sha = evidence.manifest_sha256
    candidate_sha = candidate_bundle.manifest.candidate_sha256
    if candidate_sha != candidate_bundle.manifest.candidate_sha256:
        raise ShadowTerminalUnavailable("terminal candidate hash unavailable")
    from datetime import UTC, datetime

    started = (now or datetime.now(UTC)).isoformat()
    report_id = identity.session_report_id
    reports = []
    for item in sorted(completion_items, key=lambda value: value["ordinal"]):
        request = plan.requests[item["ordinal"]]
        pages = tuple(item.get("page_refs", ()))
        page_ids = tuple(page["page_identity"] for page in pages)
        row_count = sum(int(page.get("row_count", 0)) for page in pages)
        reports.append(
            ShadowAttemptReport(
                report_id=f"terminal-report-{identity.session_id}-{request.ordinal:06d}",
                attempt_id=f"terminal-attempt-{identity.session_id}-{request.ordinal:06d}",
                job_id=identity.job_id,
                provider_id=identity.provider_id,
                window_id=identity.window_id,
                session_id=identity.session_id,
                logical_request_ordinal=request.ordinal,
                request_id=request.request_id,
                endpoint_class=request.endpoint_class,
                outcome="success",
                started_at=started,
                completed_at=started,
                coverage_expected=1,
                coverage_observed=row_count,
                request_count=1,
                page_identities=page_ids,
                page_count=len(page_ids),
                row_count=row_count,
                evidence_refs=page_ids,
                evidence_id=identity.evidence_id,
                evidence_sha256=evidence_sha,
                candidate_sha256=candidate_sha,
                terminal_session_report_id=report_id,
                durable_report_ref=f"bundles/{report_id}",
            )
        )
    if not reports:
        raise ShadowTerminalUnavailable("terminal report closure unavailable")
    plan_value = {
        "job_id": plan.job_id,
        "provider_id": plan.provider_id,
        "window_id": plan.window_id,
        "requests": [
            {
                "ordinal": item.ordinal,
                "request_id": item.request_id,
                "endpoint": item.endpoint,
                "endpoint_class": item.endpoint_class,
                "role": item.role,
                "trade_date": item.trade_date.isoformat(),
                "symbol_or_index_shard": item.symbol_or_index_shard,
                "schema_contract_hash": item.schema_contract_hash,
                "unit_contract_hash": item.unit_contract_hash,
                "request_hash": item.request_hash,
            }
            for item in plan.requests
        ],
    }
    completion_value = {
        "job_id": identity.job_id,
        "provider_id": identity.provider_id,
        "window_id": identity.window_id,
        "session_id": identity.session_id,
        "evidence_id": identity.evidence_id,
        "request_plan_sha256": request_plan_digest(plan_value)[1],
        "requests": completion_items,
    }
    closure_value = {
        "exact_ordinal_set": [item.ordinal for item in plan.requests],
        "ordinals": [
            {
                "ordinal": report_item.logical_request_ordinal,
                "attempt_id": report_item.attempt_id,
                "endpoint": plan.requests[report_item.logical_request_ordinal].endpoint,
                "endpoint_class": report_item.endpoint_class,
                "request_id": report_item.request_id,
                "page_identities": list(report_item.page_identities),
                "page_refs": list(report_item.evidence_refs),
                "page_count": report_item.page_count,
                "row_count": report_item.row_count,
                "page_hashes": [
                    page.get("content_sha256")
                    for page in next(
                        completion_item
                        for completion_item in completion_items
                        if completion_item["ordinal"] == report_item.logical_request_ordinal
                    ).get("page_refs", [])
                ],
            }
            for report_item in reports
        ],
    }
    report_value = {
        "session_report_id": report_id,
        "report_version": 2,
        "reports": [_report_payload(item) for item in reports],
    }
    plan_raw, plan_sha = request_plan_digest(plan_value)
    completion_raw, completion_sha = completion_digest(completion_value)
    closure_raw, closure_sha = attempt_ordinal_closure_digest(closure_value)
    report_raw, report_sha = report_digest(report_value)
    attestation = ShadowTerminalAttestation(
        attestation_id=f"attestation-{identity.session_id}",
        provider_id=identity.provider_id,
        job_id=identity.job_id,
        window_id=identity.window_id,
        session_id=identity.session_id,
        evidence_id=identity.evidence_id,
        candidate_id=identity.candidate_id,
        session_report_id=report_id,
        session_report_version=2,
        request_plan_canonical_json=plan_raw,
        completion_canonical_json=completion_raw,
        attempt_ordinal_closure_canonical_json=closure_raw,
        report_digest_canonical_json=report_raw,
        attempt_ordinal_closure_sha256=closure_sha,
        request_plan_sha256=plan_sha,
        completion_sha256=completion_sha,
        report_digest_sha256=report_sha,
        evidence_sha256=evidence_sha,
        candidate_sha256=candidate_sha,
    )

    with registry._lock(shared=True):
        read_connection = registry._connection_for_read()
        try:
            current_job = read_connection.execute(
                "SELECT job_id,provider_id,window_id,run_status,state_version,version_vector_sha256 FROM shadow_job WHERE job_id=? AND provider_id=? AND window_id=?",
                (identity.job_id, identity.provider_id, identity.window_id),
            ).fetchone()
            current_window = read_connection.execute(
                "SELECT consecutive_sessions,window_state,state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
                (identity.provider_id, identity.window_id),
            ).fetchone()
        finally:
            if not registry._memory:
                read_connection.close()
    if (
        current_job is None
        or current_window is None
        or current_job[4] != expected_job_state_version
        or current_window[2] != expected_window_state_version
        or current_job[3] not in {"leased", "pending_normalization"}
        or current_job[5] != version_vector_sha256
    ):
        raise ShadowTerminalUnavailable("terminal expected version conflict")
    bundle_payload = {
        "kind": "shadow-terminal-success",
        "report_version": 2,
        "identity": {
            "provider_id": identity.provider_id,
            "job_id": identity.job_id,
            "window_id": identity.window_id,
            "session_id": identity.session_id,
            "evidence_id": identity.evidence_id,
            "candidate_id": identity.candidate_id,
            "session_report_id": identity.session_report_id,
        },
        "snapshot": snapshot.model_dump(mode="json"),
        "job_snapshot": {
            "job_id": current_job[0],
            "provider_id": current_job[1],
            "window_id": current_job[2],
            "run_status": current_job[3],
            "state_version": current_job[4],
            "version_vector_sha256": current_job[5],
        },
        "window_snapshot": {
            "consecutive_sessions": current_window[0],
            "window_state": current_window[1],
            "state_version": current_window[2],
        },
        "completion_envelope": json.loads(completion_raw.decode("utf-8")),
        "digests": {
            "request_plan_sha256": plan_sha,
            "completion_sha256": completion_sha,
            "attempt_ordinal_closure_sha256": closure_sha,
            "report_digest_sha256": report_sha,
            "evidence_sha256": evidence_sha,
            "candidate_sha256": candidate_sha,
        },
        "attestation": {
            "attestation_id": attestation.attestation_id,
            "request_plan_canonical_json": plan_raw.decode("utf-8"),
            "completion_canonical_json": completion_raw.decode("utf-8"),
            "attempt_ordinal_closure_canonical_json": closure_raw.decode("utf-8"),
            "report_digest_canonical_json": report_raw.decode("utf-8"),
        },
        "reports": report_value["reports"],
    }
    try:
        bundle_path = bundle_publisher.publish(report_id, bundle_payload)
        report_fd = os.open(bundle_path / "report.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            bundle_raw = os.read(report_fd, 16 * 1024 * 1024 + 1)
        finally:
            os.close(report_fd)
        marker_fd = os.open(bundle_path / "COMMIT", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            marker = os.read(marker_fd, 128).decode("ascii")
        finally:
            os.close(marker_fd)
        if (
            len(bundle_raw) > 16 * 1024 * 1024
            or hashlib.sha256(bundle_raw).hexdigest() + "\n" != marker
            or json.loads(bundle_raw.decode("utf-8")) != bundle_payload
        ):
            raise ShadowTerminalUnavailable("terminal report bundle changed")
    except ShadowTerminalUnavailable:
        raise
    except Exception as exc:
        raise ShadowTerminalUnavailable("terminal report bundle unavailable") from exc

    def transaction(connection):
        job = connection.execute(
            "SELECT run_status,state_version FROM shadow_job WHERE job_id=? AND provider_id=? AND window_id=?",
            (identity.job_id, identity.provider_id, identity.window_id),
        ).fetchone()
        window = connection.execute(
            "SELECT consecutive_sessions,state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
            (identity.provider_id, identity.window_id),
        ).fetchone()
        if (
            job is None
            or window is None
            or job[1] != expected_job_state_version
            or window[1] != expected_window_state_version
            or job[0] not in {"leased", "pending_normalization"}
        ):
            raise ShadowTerminalUnavailable("terminal expected version conflict")
        for item in completion_items:
            page_refs = item["page_refs"]
            evidence_ref = connection.execute(
                "SELECT r.attempt_id,r.endpoint,r.request_id,r.page_refs_json,r.page_count,r.row_count,a.endpoint,a.endpoint_class,a.outcome,a.page_identities_json,a.page_count,a.row_count,a.evidence_id,a.evidence_sha256 FROM shadow_evidence_attempt_ref r JOIN shadow_attempt_report a ON a.attempt_id=r.attempt_id AND a.provider_id=r.provider_id AND a.job_id=r.job_id AND a.window_id=r.window_id AND a.session_id=r.session_id AND a.logical_request_ordinal=r.logical_request_ordinal WHERE r.evidence_id=? AND r.provider_id=? AND r.job_id=? AND r.window_id=? AND r.session_id=? AND r.logical_request_ordinal=?",
                (
                    identity.evidence_id,
                    identity.provider_id,
                    identity.job_id,
                    identity.window_id,
                    identity.session_id,
                    item["ordinal"],
                ),
            ).fetchone()
            expected_refs = json.dumps(
                [page["page_identity"] for page in page_refs], separators=(",", ":")
            )
            expected_rows = sum(int(page.get("row_count", 0)) for page in page_refs)
            if (
                evidence_ref is None
                or evidence_ref[0] != item["final_attempt_id"]
                or evidence_ref[1] != item["endpoint"]
                or evidence_ref[2] != item["request_id"]
                or evidence_ref[3] != expected_refs
                or evidence_ref[4] != len(page_refs)
                or evidence_ref[5] != expected_rows
                or evidence_ref[6] != item["endpoint"]
                or evidence_ref[7] != item["endpoint_class"]
                or evidence_ref[8] != "evidence_ready"
                or evidence_ref[9] != expected_refs
                or evidence_ref[10] != len(page_refs)
                or evidence_ref[11] != expected_rows
                or evidence_ref[12] != identity.evidence_id
                or evidence_ref[13] != evidence_sha
            ):
                raise ShadowTerminalUnavailable("terminal evidence attempt binding unavailable")
        for report in reports:
            connection.execute(
                "INSERT INTO shadow_attempt_report (attempt_id,report_id,job_id,provider_id,window_id,session_id,request_id,endpoint,endpoint_class,logical_request_ordinal,attempt_number,version_vector_sha256,outcome,started_at,completed_at,coverage_expected,coverage_observed,request_count,retry_count,rate_limit_count,failure_class,page_identities_json,page_count,row_count,terminal_marker,durable_report_ref,report_sha256,evidence_refs_json,evidence_id,evidence_sha256,candidate_sha256,terminal_session_report_id,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    report.attempt_id,
                    report.report_id,
                    report.job_id,
                    report.provider_id,
                    report.window_id,
                    report.session_id,
                    report.request_id,
                    plan.requests[report.logical_request_ordinal].endpoint,
                    report.endpoint_class,
                    report.logical_request_ordinal,
                    2,
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
                    report.candidate_sha256,
                    report.terminal_session_report_id,
                    job[1] + 1,
                ),
            )
        connection.execute(
            "INSERT INTO session_report (session_report_id,provider_id,job_id,window_id,session_id,successful_attempt_id,evidence_id,candidate_id,terminal_attestation_id,report_version,trade_date,outcome,calendar_generation,calendar_sha256,universe_sha256,version_vector_sha256,evidence_sha256,candidate_sha256,report_ref,report_sha256,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                report_id,
                identity.provider_id,
                identity.job_id,
                identity.window_id,
                identity.session_id,
                reports[0].attempt_id,
                identity.evidence_id,
                identity.candidate_id,
                attestation.attestation_id,
                2,
                str(plan.requests[0].trade_date),
                "success",
                calendar_generation,
                calendar_sha256,
                universe_sha256,
                version_vector_sha256,
                evidence_sha,
                candidate_sha,
                f"bundles/{report_id}",
                report_sha,
                job[1] + 1,
            ),
        )
        # This FK is intentionally immediate in the frozen Task10 schema; the
        # session row must exist before the evidence-ready ref can be attached.
        if (
            connection.execute(
                "UPDATE shadow_evidence_ref SET attached_session_report_id=? WHERE evidence_id=? AND provider_id=? AND job_id=? AND window_id=? AND session_id=? AND attached_session_report_id IS NULL",
                (
                    report_id,
                    identity.evidence_id,
                    identity.provider_id,
                    identity.job_id,
                    identity.window_id,
                    identity.session_id,
                ),
            ).rowcount
            != 1
        ):
            raise ShadowTerminalUnavailable("terminal evidence attachment conflict")
        connection.execute(
            "INSERT INTO shadow_terminal_attestation (attestation_id,provider_id,job_id,window_id,session_id,evidence_id,candidate_id,session_report_id,session_report_version,request_plan_canonical_json,completion_canonical_json,attempt_ordinal_closure_canonical_json,report_digest_canonical_json,attempt_ordinal_closure_sha256,request_plan_sha256,completion_sha256,report_digest_sha256,evidence_sha256,candidate_sha256,terminal_outcome,immutable_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(
                getattr(attestation, name)
                for name in (
                    "attestation_id",
                    "provider_id",
                    "job_id",
                    "window_id",
                    "session_id",
                    "evidence_id",
                    "candidate_id",
                    "session_report_id",
                    "session_report_version",
                    "request_plan_canonical_json",
                    "completion_canonical_json",
                    "attempt_ordinal_closure_canonical_json",
                    "report_digest_canonical_json",
                    "attempt_ordinal_closure_sha256",
                    "request_plan_sha256",
                    "completion_sha256",
                    "report_digest_sha256",
                    "evidence_sha256",
                    "candidate_sha256",
                    "terminal_outcome",
                    "immutable_version",
                )
            ),
        )
        new_count = window[0] + 1
        window_state = "qualified" if new_count >= 20 else "observing"
        if (
            connection.execute(
                "UPDATE shadow_job SET run_status='completed',successful_evidence_sha256=?,successful_candidate_sha256=?,completion_sha256=?,terminal_attestation_id=?,lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=? AND run_status IN ('leased','pending_normalization')",
                (
                    evidence_sha,
                    candidate_sha,
                    completion_sha,
                    attestation.attestation_id,
                    identity.job_id,
                    identity.provider_id,
                    identity.window_id,
                    expected_job_state_version,
                ),
            ).rowcount
            != 1
        ):
            raise ShadowTerminalUnavailable("terminal job CAS conflict")
        if (
            connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=?,window_state=?,last_session_report_id=?,qualification_evidence_sha256=?,qualification_candidate_sha256=?,terminal_attestation_id=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (
                    new_count,
                    window_state,
                    report_id,
                    evidence_sha,
                    candidate_sha,
                    attestation.attestation_id,
                    identity.provider_id,
                    identity.window_id,
                    expected_window_state_version,
                ),
            ).rowcount
            != 1
        ):
            raise ShadowTerminalUnavailable("terminal window CAS conflict")
        return attestation

    return registry._with_transaction(transaction)


def write_terminal_success(
    registry: ShadowRegistry,
    *,
    plan: ShadowLogicalRequestPlan,
    evidence_reader: ShadowEvidenceReader,
    candidate_reader: ShadowCandidateReader,
    identity: TerminalGraphIdentity,
    calendar_generation: str,
    calendar_sha256: str,
    universe_sha256: str,
    version_vector_sha256: str,
    expected_job_state_version: int,
    expected_window_state_version: int,
    snapshot: ConfirmedSessionSnapshot,
    bundle_publisher: ShadowBundlePublisher,
    now=None,
) -> ShadowTerminalAttestation:
    """The sole public terminal writer for the complete Task13 graph."""
    return _write_terminal_success_graph(
        registry,
        plan=plan,
        evidence_reader=evidence_reader,
        candidate_reader=candidate_reader,
        identity=identity,
        calendar_generation=calendar_generation,
        calendar_sha256=calendar_sha256,
        universe_sha256=universe_sha256,
        version_vector_sha256=version_vector_sha256,
        expected_job_state_version=expected_job_state_version,
        expected_window_state_version=expected_window_state_version,
        snapshot=snapshot,
        bundle_publisher=bundle_publisher,
        now=now,
    )


class ShadowTerminalWriter:
    """Named Task13 terminal writer; no other module may perform terminal CAS."""

    write_success = staticmethod(write_terminal_success)


__all__ = [
    "ShadowTerminalAttestation",
    "ShadowTerminalWriter",
    "ShadowTerminalUnavailable",
    "TerminalGraphIdentity",
    "attempt_ordinal_closure_digest",
    "canonical_preimage",
    "canonical_json",
    "completion_digest",
    "compute_terminal_digests",
    "report_digest",
    "request_plan_digest",
    "validate_terminal_digests",
    "write_terminal_success",
]
