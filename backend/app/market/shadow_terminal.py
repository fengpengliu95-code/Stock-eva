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
    expected_descriptor = {
        "provider_id": identity.provider_id,
        "job_id": identity.job_id,
        "window_id": identity.window_id,
        "session_id": identity.session_id,
        "evidence_id": evidence_id,
    }
    if (
        not isinstance(descriptor, dict)
        or any(descriptor.get(key) != value for key, value in expected_descriptor.items())
        or not getattr(evidence, "evidence_ready", False)
        or evidence.manifest_sha256 != manifest.evidence_sha256
        or manifest.evidence_id != evidence_id
        or manifest.candidate_id != candidate_id
        or manifest.provider_id != identity.provider_id
        or manifest.trade_date.isoformat()
        != str(descriptor.get("trade_date", manifest.trade_date))[:10]
        or manifest.universe_id != descriptor.get("universe_id", manifest.universe_id)
        or manifest.status != "accepted"
        or not isinstance(manifest.manifest_sha256, str)
        or len(manifest.manifest_sha256) != 64
        or any(char not in "0123456789abcdef" for char in manifest.manifest_sha256)
        or manifest.reconciliation_status != "ready"
        or not isinstance(manifest.reconciliation_sha256, str)
        or len(manifest.reconciliation_sha256) != 64
        or any(char not in "0123456789abcdef" for char in manifest.reconciliation_sha256)
        or not manifest.canonical_comparison_snapshot_sha256
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
    expected_completion = {
        "job_id": identity.job_id,
        "provider_id": identity.provider_id,
        "window_id": identity.window_id,
        "session_id": identity.session_id,
        "evidence_id": evidence_id,
        "request_plan_sha256": plan.request_plan_sha256,
    }
    if not isinstance(completion, dict) or any(
        completion.get(key) != value for key, value in expected_completion.items()
    ):
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
        for page_number, page in enumerate(item.get("page_refs", []), start=1):
            if (
                page.get("ordinal") != page_number
                or not page.get("page_identity")
                or not page.get("object_ref")
                or len(page.get("content_sha256", "")) != 64
                or any(char not in "0123456789abcdef" for char in page.get("content_sha256", ""))
                or not isinstance(page.get("row_count"), int)
                or page.get("row_count", -1) < 0
            ):
                raise ShadowTerminalUnavailable("terminal page binding unavailable")
            flattened_pages.append(
                {
                    key: item["ordinal"] if key == "ordinal" else page.get(key)
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
    if (
        snapshot.provider_id != plan.provider_id
        or snapshot.window_id != plan.window_id
        or snapshot.universe_id == ""
        or not set(item.trade_date for item in plan.requests).issubset(
            set(snapshot.confirmed_next_sessions)
        )
    ):
        raise ShadowTerminalUnavailable("terminal session snapshot drift")
    evidence, candidate_bundle, completion_items = _strict_bundle_validation(
        plan=plan,
        evidence_reader=evidence_reader,
        candidate_reader=candidate_reader,
        evidence_id=identity.evidence_id,
        candidate_id=identity.candidate_id,
        identity=identity,
    )
    evidence_sha = evidence.manifest_sha256
    # The immutable Task 12 candidate identity is the manifest digest.  The
    # frozen Task 10 registry column keeps its historical candidate_sha256
    # name, but Task 12 does not expose a separate candidate_sha256 field.
    candidate_sha = candidate_bundle.manifest.manifest_sha256
    from datetime import UTC, datetime

    if not plan.requests:
        raise ShadowTerminalUnavailable("terminal report closure unavailable")
    # A retry must address the same immutable pending identity.  When the
    # caller does not provide a run timestamp, anchor it to the requested
    # session date instead of introducing a fresh wall-clock payload.
    started = (
        now or datetime.combine(plan.requests[0].trade_date, datetime.min.time(), tzinfo=UTC)
    ).isoformat()
    report_id = identity.session_report_id
    pending_bundle_id = f"{report_id}-pending"
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
                durable_report_ref=f"bundles/{pending_bundle_id}",
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

    evidence_ref_payload = {
        "evidence_id": identity.evidence_id,
        "job_id": identity.job_id,
        "provider_id": identity.provider_id,
        "window_id": identity.window_id,
        "session_id": identity.session_id,
        "completion_sha256": evidence.completion_sha256,
        "evidence_sha256": evidence_sha,
        "bundle_ref": f"evidence/{identity.evidence_id}",
        "bundle_sha256": evidence_sha,
        "attached_session_report_id": report_id,
    }
    candidate_manifest = candidate_bundle.manifest
    candidate_manifest_payload = (
        candidate_manifest.model_dump(mode="json")
        if hasattr(candidate_manifest, "model_dump")
        else None
    )
    candidate_ref_payload = {
        "candidate_id": identity.candidate_id,
        "evidence_id": identity.evidence_id,
        "job_id": identity.job_id,
        "provider_id": identity.provider_id,
        "window_id": identity.window_id,
        "session_id": identity.session_id,
        "candidate_ref": candidate_manifest.normalized_object_ref,
        "candidate_sha256": candidate_sha,
        "quality_report_ref": candidate_manifest.quality_report_ref,
        "quality_report_sha256": candidate_manifest.quality_report_sha256,
        "canonical_manifest_generation": candidate_manifest.canonical_manifest_generation,
        "canonical_manifest_sha256": candidate_manifest.canonical_manifest_sha256,
        "reconciliation_id": candidate_manifest.reconciliation_id,
        "reconciliation_sha256": candidate_manifest.reconciliation_sha256,
        "reconciliation_status": candidate_manifest.reconciliation_status,
        "reconciliation_compared_counts": candidate_manifest.reconciliation_compared_counts,
        "reconciliation_mismatch_counts": candidate_manifest.reconciliation_mismatch_counts,
        "canonical_comparison_snapshot_sha256": candidate_manifest.canonical_comparison_snapshot_sha256,
    }

    with registry._lock(shared=True):
        read_connection = registry._connection_for_read()
        try:
            current_job = read_connection.execute(
                "SELECT job_id,provider_id,window_id,universe_id,run_status,state_version,version_vector_sha256 FROM shadow_job WHERE job_id=? AND provider_id=? AND window_id=?",
                (identity.job_id, identity.provider_id, identity.window_id),
            ).fetchone()
            current_window = read_connection.execute(
                "SELECT consecutive_sessions,window_state,calendar_generation,calendar_sha256,version_vector_sha256,state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
                (identity.provider_id, identity.window_id),
            ).fetchone()
        finally:
            if not registry._memory:
                read_connection.close()
    if (
        current_job is None
        or current_window is None
        or snapshot.provider_id != identity.provider_id
        or snapshot.window_id != identity.window_id
        or snapshot.universe_id != current_job[3]
        or current_job[5] != expected_job_state_version
        or current_window[5] != expected_window_state_version
        or current_job[4] not in {"leased", "pending_normalization"}
        or current_job[6] != version_vector_sha256
        or current_window[2] != snapshot.calendar_generation
        or current_window[3] != snapshot.calendar_sha256
        or current_window[4] != version_vector_sha256
        or not set(item.trade_date for item in plan.requests).issubset(
            set(snapshot.confirmed_next_sessions)
        )
    ):
        raise ShadowTerminalUnavailable("terminal expected version conflict")
    trade_dates = {item.trade_date for item in plan.requests}
    if len(trade_dates) != 1:
        raise ShadowTerminalUnavailable("terminal session date unavailable")
    current_trade_date = next(iter(trade_dates))
    confirmed = tuple(snapshot.confirmed_next_sessions)
    continuity_ok = False
    with registry._lock(shared=True):
        continuity_connection = registry._connection_for_read()
        try:
            duplicate = continuity_connection.execute(
                "SELECT 1 FROM session_report WHERE provider_id=? AND window_id=? AND trade_date=? "
                "AND outcome IN ('success','failure','skip','unavailable','mismatch') LIMIT 1",
                (identity.provider_id, identity.window_id, current_trade_date.isoformat()),
            ).fetchone()
            previous = continuity_connection.execute(
                "SELECT trade_date FROM session_report WHERE provider_id=? AND window_id=? AND outcome='success' ORDER BY trade_date DESC LIMIT 1",
                (identity.provider_id, identity.window_id),
            ).fetchone()
        finally:
            if not registry._memory:
                continuity_connection.close()
    current_index = confirmed.index(current_trade_date) if current_trade_date in confirmed else -1
    if duplicate is None and current_index >= 0:
        if current_window[0] == 0:
            # A reset retains prior reports for audit; the next confirmed
            # session starts a fresh sequence even when an older success row
            # remains in the append-only history.
            continuity_ok = current_index == 0
        else:
            continuity_ok = (
                current_index == current_window[0]
                and previous is not None
                and str(previous[0])[:10] == confirmed[current_index - 1].isoformat()
            )
    if not continuity_ok:

        def reset_continuity(connection):
            connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',last_session_report_id=NULL,qualification_evidence_sha256=NULL,qualification_candidate_sha256=NULL,terminal_attestation_id=NULL,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (identity.provider_id, identity.window_id, expected_window_state_version),
            )
            connection.execute(
                "UPDATE shadow_job SET run_status='failed',lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=?",
                (
                    identity.job_id,
                    identity.provider_id,
                    identity.window_id,
                    expected_job_state_version,
                ),
            )

        registry._with_transaction(reset_continuity)
        raise ShadowTerminalUnavailable("terminal confirmed session continuity unavailable")
    snapshot_payload = snapshot.model_dump(mode="json")
    snapshot_payload["version_vector_sha256"] = version_vector_sha256
    bundle_payload = {
        # The DB CAS is the success decision.  Until that commit is durable,
        # this immutable bundle is deliberately non-success and quarantinable.
        "kind": "shadow-terminal-pending",
        "outcome": "pending",
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
        "snapshot": snapshot_payload,
        "job_snapshot": {
            "job_id": current_job[0],
            "provider_id": current_job[1],
            "window_id": current_job[2],
            "universe_id": current_job[3],
            "run_status": current_job[4],
            "state_version": current_job[5],
            "version_vector_sha256": current_job[6],
        },
        "window_snapshot": {
            "provider_id": identity.provider_id,
            "window_id": identity.window_id,
            "consecutive_sessions": current_window[0],
            "window_state": current_window[1],
            "calendar_generation": current_window[2],
            "calendar_sha256": current_window[3],
            "version_vector_sha256": current_window[4],
            "state_version": current_window[5],
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
        "evidence_ref": evidence_ref_payload,
        "candidate_ref": candidate_ref_payload,
        "candidate_manifest": candidate_manifest_payload,
        "reports": report_value["reports"],
    }
    try:
        bundle_path = bundle_publisher.publish(pending_bundle_id, bundle_payload)
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
                f"bundles/{pending_bundle_id}",
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
        # Reaching twenty is evidence for the explicit registry qualification
        # validator, never an implicit promotion by the terminal writer.
        window_state = "observing"
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

    try:
        attested = registry._with_transaction(transaction)
    except Exception as exc:
        # A pending graph is retained for quarantine/audit.  A terminal
        # failure report is best-effort; if its publisher is unavailable the
        # original transaction failure remains the authoritative result.
        try:
            bundle_publisher.publish(
                f"{report_id}-failure",
                {
                    "kind": "shadow-terminal-failure",
                    "outcome": "failure",
                    "report_version": 2,
                    "reason_code": "terminal_commit_failed",
                    "identity": bundle_payload["identity"],
                },
            )
        except Exception:
            pass
        if isinstance(exc, ShadowTerminalUnavailable):
            raise
        raise ShadowTerminalUnavailable("terminal transaction unavailable") from exc

    success_payload = dict(bundle_payload)
    success_payload["kind"] = "shadow-terminal-success"
    success_payload["outcome"] = "success"
    success_payload["pending_bundle_id"] = pending_bundle_id
    success_payload["attestation_ref"] = attested.attestation_id
    with registry._lock(shared=True):
        post_connection = registry._connection_for_read()
        try:
            post_job = post_connection.execute(
                "SELECT state_version,run_status FROM shadow_job WHERE job_id=?",
                (identity.job_id,),
            ).fetchone()
            post_window = post_connection.execute(
                "SELECT consecutive_sessions,state_version,window_state FROM qualification_window "
                "WHERE provider_id=? AND window_id=?",
                (identity.provider_id, identity.window_id),
            ).fetchone()
        finally:
            if not registry._memory:
                post_connection.close()
    if post_job is None or post_window is None:
        raise ShadowTerminalUnavailable("terminal committed graph unavailable")
    success_payload["job_snapshot"] = dict(bundle_payload["job_snapshot"])
    success_payload["job_snapshot"]["state_version"] = post_job[0]
    success_payload["job_snapshot"]["run_status"] = post_job[1]
    success_payload["window_snapshot"] = dict(bundle_payload["window_snapshot"])
    success_payload["window_snapshot"]["state_version"] = post_window[1]
    success_payload["window_snapshot"]["consecutive_sessions"] = post_window[0]
    success_payload["window_snapshot"]["window_state"] = post_window[2]
    try:
        bundle_publisher.publish(report_id, success_payload)
    except Exception as exc:
        # The committed DB graph is recoverable from its pending bundle.  Do
        # not manufacture a second success result when the marker is absent.
        raise ShadowTerminalUnavailable("terminal success marker unavailable") from exc
    return attested


def _write_terminal_success(
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
    """Private graph implementation; production callers use ShadowTerminalWriter."""
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
    """Typed production terminal dependency bound to one immutable publisher."""

    def __init__(self, bundle_publisher: ShadowBundlePublisher):
        if type(bundle_publisher) is not ShadowBundlePublisher:
            raise TypeError("terminal writer requires ShadowBundlePublisher")
        self.bundle_publisher = bundle_publisher

    def write_success(self, registry: ShadowRegistry, **context: Any) -> ShadowTerminalAttestation:
        context = dict(context)
        if "bundle_publisher" in context:
            raise ShadowTerminalUnavailable("terminal publisher override unavailable")
        context["bundle_publisher"] = self.bundle_publisher
        return _write_terminal_success(registry, **context)

    def recover_success(self, registry: ShadowRegistry, *, report_id: str):
        """Materialize a success marker after a committed DB transaction.

        Recovery is descriptor-bound to the pending bundle and only permitted
        when the immutable session row already says success.  A pending bundle
        is never treated as a successful outcome by itself.
        """
        if type(registry) is not ShadowRegistry or not isinstance(report_id, str) or not report_id:
            raise ShadowTerminalUnavailable("terminal recovery unavailable")
        pending_id = f"{report_id}-pending"
        try:
            pending = self.bundle_publisher.read_verified(pending_id)
            if (
                not isinstance(pending, dict)
                or pending.get("kind") != "shadow-terminal-pending"
                or pending.get("outcome") != "pending"
                or pending.get("identity", {}).get("session_report_id") != report_id
            ):
                raise ShadowTerminalUnavailable("terminal pending bundle unavailable")
        except ShadowTerminalUnavailable:
            raise
        except Exception as exc:
            raise ShadowTerminalUnavailable("terminal pending bundle unavailable") from exc
        with registry._lock(shared=True):
            connection = registry._connection_for_read()
            try:
                committed = connection.execute(
                    "SELECT provider_id,job_id,window_id,session_id,evidence_id,candidate_id,"
                    "terminal_attestation_id,report_version,trade_date,calendar_generation,"
                    "calendar_sha256,universe_sha256,version_vector_sha256,evidence_sha256,"
                    "candidate_sha256,report_ref,report_sha256,state_version FROM session_report "
                    "WHERE session_report_id=? AND outcome='success'",
                    (report_id,),
                ).fetchone()
                if committed is not None:
                    attestation = connection.execute(
                        "SELECT attestation_id,provider_id,job_id,window_id,session_id,evidence_id,"
                        "candidate_id,session_report_id,session_report_version,request_plan_canonical_json,"
                        "completion_canonical_json,attempt_ordinal_closure_canonical_json,"
                        "report_digest_canonical_json,attempt_ordinal_closure_sha256,request_plan_sha256,"
                        "completion_sha256,report_digest_sha256,evidence_sha256,candidate_sha256,"
                        "terminal_outcome FROM shadow_terminal_attestation WHERE attestation_id=?",
                        (committed[6],),
                    ).fetchone()
                    attempts = connection.execute(
                        "SELECT attempt_id,report_id,logical_request_ordinal,outcome,report_sha256 "
                        "FROM shadow_attempt_report WHERE terminal_session_report_id=? "
                        "ORDER BY logical_request_ordinal",
                        (report_id,),
                    ).fetchall()
                    job = connection.execute(
                        "SELECT job_id,provider_id,window_id,universe_id,state_version,"
                        "version_vector_sha256,run_status,canonical_manifest_generation,"
                        "canonical_manifest_sha256 FROM shadow_job WHERE job_id=?",
                        (committed[1],),
                    ).fetchone()
                    window = connection.execute(
                        "SELECT provider_id,window_id,consecutive_sessions,calendar_generation,"
                        "calendar_sha256,version_vector_sha256,state_version FROM qualification_window "
                        "WHERE provider_id=? AND window_id=?",
                        (committed[0], committed[2]),
                    ).fetchone()
                    evidence_ref = connection.execute(
                        "SELECT evidence_id,job_id,provider_id,window_id,session_id,"
                        "completion_sha256,evidence_sha256,bundle_ref,bundle_sha256,"
                        "attached_session_report_id FROM shadow_evidence_ref "
                        "WHERE evidence_id=? AND attached_session_report_id=?",
                        (committed[4], report_id),
                    ).fetchone()
                    candidate_ref = connection.execute(
                        "SELECT candidate_id,evidence_id,job_id,provider_id,window_id,session_id,"
                        "candidate_ref,candidate_sha256,quality_report_ref,quality_report_sha256 "
                        "FROM shadow_candidate_ref WHERE candidate_id=?",
                        (committed[5],),
                    ).fetchone()
            finally:
                if not registry._memory:
                    connection.close()
        if (
            committed is None
            or not committed[6]
            or attestation is None
            or job is None
            or window is None
        ):
            raise ShadowTerminalUnavailable("terminal success is not committed")
        identity_payload = pending.get("identity")
        snapshot_payload = pending.get("snapshot")
        job_payload = pending.get("job_snapshot")
        window_payload = pending.get("window_snapshot")
        digests = pending.get("digests")
        attestation_payload = pending.get("attestation")
        evidence_ref_payload = pending.get("evidence_ref")
        candidate_ref_payload = pending.get("candidate_ref")
        candidate_manifest_payload = pending.get("candidate_manifest")
        if not all(
            isinstance(value, dict)
            for value in (
                identity_payload,
                snapshot_payload,
                job_payload,
                window_payload,
                digests,
                attestation_payload,
                evidence_ref_payload,
                candidate_ref_payload,
            )
        ):
            raise ShadowTerminalUnavailable("terminal recovery graph unavailable")
        expected_identity = {
            "provider_id": committed[0],
            "job_id": committed[1],
            "window_id": committed[2],
            "session_id": committed[3],
            "evidence_id": committed[4],
            "candidate_id": committed[5],
            "session_report_id": report_id,
        }
        candidate_manifest_ok = False
        if isinstance(candidate_manifest_payload, dict):
            manifest_for_hash = dict(candidate_manifest_payload)
            manifest_sha = manifest_for_hash.pop("manifest_sha256", None)
            manifest_raw = (
                json.dumps(
                    manifest_for_hash,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            candidate_manifest_ok = (
                manifest_sha == committed[14]
                and hashlib.sha256(manifest_raw).hexdigest() == committed[14]
                and candidate_manifest_payload.get("candidate_id") == committed[5]
                and candidate_manifest_payload.get("evidence_id") == committed[4]
                and candidate_manifest_payload.get("provider_id") == committed[0]
                and candidate_manifest_payload.get("job_id") is None
            )
        candidate_ref_metadata_ok = True
        if isinstance(candidate_manifest_payload, dict):
            candidate_ref_metadata_ok = all(
                candidate_ref_payload.get(
                    "candidate_ref" if field == "normalized_object_ref" else field
                )
                == candidate_manifest_payload.get(field)
                for field in (
                    "candidate_id",
                    "evidence_id",
                    "provider_id",
                    "normalized_object_ref",
                    "quality_report_ref",
                    "quality_report_sha256",
                    "canonical_manifest_generation",
                    "canonical_manifest_sha256",
                    "reconciliation_id",
                    "reconciliation_sha256",
                    "reconciliation_status",
                    "reconciliation_compared_counts",
                    "reconciliation_mismatch_counts",
                    "canonical_comparison_snapshot_sha256",
                )
            )
        else:
            candidate_ref_metadata_ok = False
        if identity_payload != expected_identity:
            raise ShadowTerminalUnavailable("terminal recovery identity drift")
        if (
            snapshot_payload.get("provider_id") != committed[0]
            or snapshot_payload.get("window_id") != committed[2]
            or snapshot_payload.get("calendar_generation") != committed[9]
            or snapshot_payload.get("calendar_sha256") != committed[10]
            or snapshot_payload.get("universe_sha256") != committed[11]
            or snapshot_payload.get("version_vector_sha256") != committed[12]
            or job_payload.get("job_id") != job[0]
            or job_payload.get("provider_id") != job[1]
            or job_payload.get("window_id") != job[2]
            or job_payload.get("universe_id") != job[3]
            or job_payload.get("version_vector_sha256") != job[5]
            or candidate_ref_payload.get("canonical_manifest_generation") != job[7]
            or candidate_ref_payload.get("canonical_manifest_sha256") != job[8]
            or job_payload.get("state_version") != job[4] - 1
            or job_payload.get("run_status") != "pending_normalization"
            or window_payload.get("provider_id") != window[0]
            or window_payload.get("window_id") != window[1]
            or window_payload.get("calendar_generation") != window[3]
            or window_payload.get("calendar_sha256") != window[4]
            or window_payload.get("version_vector_sha256") != window[5]
            or window_payload.get("state_version") != window[6] - 1
            or window_payload.get("consecutive_sessions") != window[2] - 1
            or window_payload.get("window_state") != "observing"
            or evidence_ref is None
            or candidate_ref is None
            or not candidate_manifest_ok
            or not candidate_ref_metadata_ok
            or evidence_ref_payload
            != {
                "evidence_id": evidence_ref[0],
                "job_id": evidence_ref[1],
                "provider_id": evidence_ref[2],
                "window_id": evidence_ref[3],
                "session_id": evidence_ref[4],
                "completion_sha256": evidence_ref[5],
                "evidence_sha256": evidence_ref[6],
                "bundle_ref": evidence_ref[7],
                "bundle_sha256": evidence_ref[8],
                "attached_session_report_id": evidence_ref[9],
            }
            or candidate_ref_payload
            != {
                "candidate_id": candidate_ref[0],
                "evidence_id": candidate_ref[1],
                "job_id": candidate_ref[2],
                "provider_id": candidate_ref[3],
                "window_id": candidate_ref[4],
                "session_id": candidate_ref[5],
                "candidate_ref": candidate_ref[6],
                "candidate_sha256": candidate_ref[7],
                "quality_report_ref": candidate_ref[8],
                "quality_report_sha256": candidate_ref[9],
                "canonical_manifest_generation": candidate_ref_payload.get(
                    "canonical_manifest_generation"
                ),
                "canonical_manifest_sha256": candidate_ref_payload.get("canonical_manifest_sha256"),
                "reconciliation_id": candidate_ref_payload.get("reconciliation_id"),
                "reconciliation_sha256": candidate_ref_payload.get("reconciliation_sha256"),
                "reconciliation_status": candidate_ref_payload.get("reconciliation_status"),
                "reconciliation_compared_counts": candidate_ref_payload.get(
                    "reconciliation_compared_counts"
                ),
                "reconciliation_mismatch_counts": candidate_ref_payload.get(
                    "reconciliation_mismatch_counts"
                ),
                "canonical_comparison_snapshot_sha256": candidate_ref_payload.get(
                    "canonical_comparison_snapshot_sha256"
                ),
            }
            or digests.get("evidence_sha256") != committed[13]
            or digests.get("candidate_sha256") != committed[14]
            or digests.get("request_plan_sha256") != attestation[14]
            or digests.get("completion_sha256") != attestation[15]
            or digests.get("attempt_ordinal_closure_sha256") != attestation[13]
            or digests.get("report_digest_sha256") != attestation[16]
            or attestation_payload.get("attestation_id") != committed[6]
            or committed[15] != f"bundles/{pending_id}"
            or tuple(attestation[1:8])
            != (
                committed[0],
                committed[1],
                committed[2],
                committed[3],
                committed[4],
                committed[5],
                report_id,
            )
            or attestation[8] != pending.get("report_version")
            or committed[16] != attestation[16]
            or attestation[19] != "success"
        ):
            raise ShadowTerminalUnavailable("terminal recovery graph drift")
        if len(attempts) != len(pending.get("reports", ())):
            raise ShadowTerminalUnavailable("terminal recovery attempts drift")
        for row, item in zip(attempts, pending["reports"], strict=True):
            if (
                not isinstance(item, dict)
                or item.get("attempt_id") != row[0]
                or item.get("report_id") != row[1]
                or item.get("ordinal") != row[2]
                or item.get("outcome") != row[3]
                or item.get("report_sha256") != row[4]
            ):
                raise ShadowTerminalUnavailable("terminal recovery attempts drift")
        expected_attestation = {
            "attestation_id": attestation[0],
            "request_plan_canonical_json": attestation[9].decode()
            if isinstance(attestation[9], bytes)
            else attestation[9],
            "completion_canonical_json": attestation[10].decode()
            if isinstance(attestation[10], bytes)
            else attestation[10],
            "attempt_ordinal_closure_canonical_json": attestation[11].decode()
            if isinstance(attestation[11], bytes)
            else attestation[11],
            "report_digest_canonical_json": attestation[12].decode()
            if isinstance(attestation[12], bytes)
            else attestation[12],
        }
        try:
            committed_completion = json.loads(
                attestation[10].decode() if isinstance(attestation[10], bytes) else attestation[10]
            )
        except (TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ShadowTerminalUnavailable("terminal recovery attestation drift") from exc
        if (
            attestation_payload != expected_attestation
            or pending.get("completion_envelope") != committed_completion
        ):
            raise ShadowTerminalUnavailable("terminal recovery attestation drift")
        success = dict(pending)
        success["kind"] = "shadow-terminal-success"
        success["outcome"] = "success"
        success["pending_bundle_id"] = pending_id
        success["attestation_ref"] = committed[6]
        success["job_snapshot"] = dict(job_payload)
        success["job_snapshot"]["state_version"] = job[4]
        success["job_snapshot"]["run_status"] = job[6]
        success["window_snapshot"] = dict(window_payload)
        success["window_snapshot"]["state_version"] = window[6]
        success["window_snapshot"]["consecutive_sessions"] = window[2]
        success["window_snapshot"]["window_state"] = "qualified" if window[2] >= 20 else "observing"
        try:
            return self.bundle_publisher.publish(report_id, success)
        except Exception as exc:
            raise ShadowTerminalUnavailable("terminal success marker unavailable") from exc


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
]
