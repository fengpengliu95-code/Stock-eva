"""Read-only terminal graph validation and the sole terminal transaction writer."""

# Frozen SQL projections are intentionally kept byte-readable for review.
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .providers.registry import ShadowRegistry


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


def terminal_graph_validator(
    connection, identity: TerminalGraphIdentity, attestation: ShadowTerminalAttestation
) -> None:
    """Validate immutable graph before writing any terminal row or CAS state."""
    validate_terminal_digests(attestation)
    if attestation.terminal_outcome != "success" or attestation.session_report_version < 2:
        raise ShadowTerminalUnavailable("terminal outcome unavailable")
    if (
        attestation.provider_id,
        attestation.job_id,
        attestation.window_id,
        attestation.session_id,
        attestation.evidence_id,
        attestation.candidate_id,
        attestation.session_report_id,
    ) != (
        identity.provider_id,
        identity.job_id,
        identity.window_id,
        identity.session_id,
        identity.evidence_id,
        identity.candidate_id,
        identity.session_report_id,
    ):
        raise ShadowTerminalUnavailable("terminal identity mismatch")
    report = connection.execute(
        "SELECT outcome,report_version,evidence_id,candidate_id,terminal_attestation_id,evidence_sha256,candidate_sha256 FROM session_report WHERE session_report_id=? AND provider_id=? AND job_id=? AND window_id=? AND session_id=?",
        (
            identity.session_report_id,
            identity.provider_id,
            identity.job_id,
            identity.window_id,
            identity.session_id,
        ),
    ).fetchone()
    if (
        report is None
        or report[0] != "success"
        or report[1] < 2
        or report[2:]
        != (
            identity.evidence_id,
            identity.candidate_id,
            attestation.attestation_id,
            attestation.evidence_sha256,
            attestation.candidate_sha256,
        )
    ):
        raise ShadowTerminalUnavailable("terminal session report unavailable")
    rows = connection.execute(
        "SELECT logical_request_ordinal,outcome,terminal_marker,evidence_id,evidence_sha256,candidate_sha256,terminal_session_report_id FROM shadow_attempt_report WHERE provider_id=? AND job_id=? AND window_id=? AND session_id=? AND terminal_session_report_id=? ORDER BY logical_request_ordinal",
        (
            identity.provider_id,
            identity.job_id,
            identity.window_id,
            identity.session_id,
            identity.session_report_id,
        ),
    ).fetchall()
    if not rows or any(
        row[1] != "success"
        or row[2] != 1
        or row[3] != identity.evidence_id
        or row[4] != attestation.evidence_sha256
        or row[5] != attestation.candidate_sha256
        or row[6] != identity.session_report_id
        for row in rows
    ):
        raise ShadowTerminalUnavailable("terminal ordinal closure unavailable")


def write_terminal_success(
    registry: ShadowRegistry,
    *,
    identity: TerminalGraphIdentity,
    attestation: ShadowTerminalAttestation,
    expected_job_state_version: int,
    expected_window_state_version: int,
    window_state: str = "observing",
) -> ShadowTerminalAttestation:
    """Insert one immutable attestation and CAS both state rows in one transaction."""
    if type(registry) is not ShadowRegistry:
        raise TypeError("terminal writer requires ShadowRegistry")
    validate_terminal_digests(attestation)
    if attestation.attestation_id == "":
        raise ShadowTerminalUnavailable("terminal attestation identity unavailable")

    def transaction(connection):
        terminal_graph_validator(connection, identity, attestation)
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
        job = connection.execute(
            "UPDATE shadow_job SET run_status='completed',successful_evidence_sha256=?,successful_candidate_sha256=?,completion_sha256=?,terminal_attestation_id=?,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND run_status='pending_normalization' AND state_version=?",
            (
                attestation.evidence_sha256,
                attestation.candidate_sha256,
                attestation.completion_sha256,
                attestation.attestation_id,
                identity.job_id,
                identity.provider_id,
                identity.window_id,
                expected_job_state_version,
            ),
        )
        if job.rowcount != 1:
            raise ShadowTerminalUnavailable("terminal job CAS conflict")
        window = connection.execute(
            "UPDATE qualification_window SET window_state=?,last_session_report_id=?,qualification_evidence_sha256=?,qualification_candidate_sha256=?,terminal_attestation_id=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
            (
                window_state,
                identity.session_report_id,
                attestation.evidence_sha256,
                attestation.candidate_sha256,
                attestation.attestation_id,
                identity.provider_id,
                identity.window_id,
                expected_window_state_version,
            ),
        )
        if window.rowcount != 1:
            raise ShadowTerminalUnavailable("terminal window CAS conflict")
        return attestation

    return registry._with_transaction(transaction)


class ShadowTerminalValidator:
    """Small object form for workers that inject a registry connection."""

    validate = staticmethod(terminal_graph_validator)


class ShadowTerminalWriter:
    """Named Task13 terminal writer; no other module may perform terminal CAS."""

    write_success = staticmethod(write_terminal_success)


__all__ = [
    "ShadowTerminalAttestation",
    "ShadowTerminalValidator",
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
    "terminal_graph_validator",
    "validate_terminal_digests",
    "write_terminal_success",
]
