"""Durable, isolated shadow outbox and lease state machine."""

# SQL statements are kept as reviewable one-line projections of the frozen DDL.
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .providers.registry import ShadowRegistry
from .shadow_calendar import ConfirmedSessionSnapshot
from .shadow_evidence import ShadowAttemptReport


class ShadowJobUnavailable(RuntimeError):
    """The shadow outbox is unavailable; callers must preserve canonical outcome."""


class ShadowJob(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    provider_id: str
    window_id: str
    trade_date: str
    universe_id: str
    canonical_manifest_generation: str
    canonical_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_status: str
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    attempt_count: int = Field(ge=0)
    state_version: int = Field(ge=0)
    successful_evidence_sha256: str | None = None
    successful_candidate_sha256: str | None = None
    completion_sha256: str | None = None
    terminal_attestation_id: str | None = None


class ShadowSessionReport(BaseModel):
    """Immutable sanitized session projection used by the qualification window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session_report_id: str
    provider_id: str
    job_id: str
    window_id: str
    session_id: str
    trade_date: str
    outcome: str
    calendar_generation: str
    calendar_sha256: str
    universe_sha256: str
    version_vector_sha256: str
    report_ref: str
    report_sha256: str
    report_version: int = 1


def _safe(value: str) -> str:
    if (
        not value
        or len(value) > 160
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for c in value
        )
    ):
        raise ValueError("shadow identity is invalid")
    return value


class ShadowJobStore:
    """Registry-backed outbox.  It never touches canonical storage."""

    def __init__(self, registry: ShadowRegistry, shadow_root: Path | str | None = None):
        if type(registry) is not ShadowRegistry:
            raise TypeError("shadow job store requires ShadowRegistry")
        self.registry = registry
        self.shadow_root = Path(shadow_root) if shadow_root is not None else None

    def _row(self, connection: sqlite3.Connection, job_id: str) -> ShadowJob | None:
        row = connection.execute(
            "SELECT job_id,provider_id,window_id,trade_date,universe_id,canonical_manifest_generation,canonical_manifest_sha256,version_vector_sha256,run_status,lease_owner,lease_expires_at,attempt_count,state_version,successful_evidence_sha256,successful_candidate_sha256,completion_sha256,terminal_attestation_id FROM shadow_job WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        fields = (
            "job_id",
            "provider_id",
            "window_id",
            "trade_date",
            "universe_id",
            "canonical_manifest_generation",
            "canonical_manifest_sha256",
            "version_vector_sha256",
            "run_status",
            "lease_owner",
            "lease_expires_at",
            "attempt_count",
            "state_version",
            "successful_evidence_sha256",
            "successful_candidate_sha256",
            "completion_sha256",
            "terminal_attestation_id",
        )
        return ShadowJob.model_validate(dict(zip(fields, row, strict=True)))

    def get(self, job_id: str) -> ShadowJob | None:
        with self.registry._lock(shared=True):
            connection = self.registry._connection_for_read()
            try:
                return self._row(connection, _safe(job_id))
            finally:
                if not self.registry._memory:
                    connection.close()

    def enqueue(
        self,
        *,
        job_id: str,
        provider_id: str,
        window_id: str,
        trade_date: str,
        universe_id: str,
        canonical_manifest_generation: str,
        canonical_manifest_sha256: str,
        version_vector_sha256: str,
    ) -> ShadowJob:
        values = (
            _safe(job_id),
            _safe(provider_id),
            _safe(window_id),
            trade_date,
            _safe(universe_id),
            _safe(canonical_manifest_generation),
            canonical_manifest_sha256,
            version_vector_sha256,
        )

        def save(connection):
            connection.execute(
                "INSERT OR IGNORE INTO shadow_job (job_id,provider_id,window_id,trade_date,universe_id,canonical_manifest_generation,canonical_manifest_sha256,version_vector_sha256,run_status,lease_owner,lease_expires_at,attempt_count,state_version,successful_evidence_sha256,successful_candidate_sha256,completion_sha256,terminal_attestation_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (*values, "pending", None, None, 0, 0, None, None, None, None),
            )
            row = self._row(connection, job_id)
            if row is None:
                raise ShadowJobUnavailable("shadow job unavailable")
            return row

        return self.registry._with_transaction(save)

    create = enqueue

    def lease(
        self, job_id: str, *, owner: str, now: datetime | None = None, lease_seconds: int = 300
    ) -> ShadowJob:
        owner = _safe(owner)
        instant = now or datetime.now(UTC)
        expires = instant + timedelta(seconds=max(1, lease_seconds))

        def acquire(connection):
            row = self._row(connection, _safe(job_id))
            if row is None or row.run_status not in {"pending", "leased"}:
                raise ShadowJobUnavailable("shadow job is not leasable")
            if (
                row.run_status == "leased"
                and row.lease_expires_at
                and row.lease_expires_at > instant.isoformat()
            ):
                raise ShadowJobUnavailable("shadow job is leased")
            cursor = connection.execute(
                "UPDATE shadow_job SET run_status='leased',lease_owner=?,lease_expires_at=?,attempt_count=attempt_count+1,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status IN ('pending','leased')",
                (owner, expires.isoformat(), job_id, row.state_version),
            )
            if cursor.rowcount != 1:
                raise ShadowJobUnavailable("shadow job lease conflict")
            return self._row(connection, job_id)

        return self.registry._with_transaction(acquire)

    acquire_lease = lease

    def reclaim_expired(self, *, now: datetime | None = None, limit: int = 64) -> int:
        instant = (now or datetime.now(UTC)).isoformat()

        def recover(connection):
            rows = connection.execute(
                "SELECT job_id,state_version FROM shadow_job WHERE run_status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at<=? ORDER BY job_id LIMIT ?",
                (instant, max(0, limit)),
            ).fetchall()
            changed = 0
            for job_id, version in rows:
                changed += connection.execute(
                    "UPDATE shadow_job SET run_status='pending',lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status='leased'",
                    (job_id, version),
                ).rowcount
            return changed

        return self.registry._with_transaction(recover)

    recover_expired_leases = reclaim_expired

    def cancel(self, job_id: str, *, expected_state_version: int) -> ShadowJob:
        def update(connection):
            cursor = connection.execute(
                "UPDATE shadow_job SET run_status='cancelled',lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status IN ('pending','leased','pending_normalization')",
                (_safe(job_id), expected_state_version),
            )
            if cursor.rowcount != 1:
                raise ShadowJobUnavailable("shadow job state conflict")
            return self._row(connection, job_id)

        return self.registry._with_transaction(update)

    def persist_outcome(
        self,
        *,
        plan,
        session_id: str,
        outcome: str,
        trade_date: str,
        calendar_generation: str,
        calendar_sha256: str,
        universe_sha256: str,
        version_vector_sha256: str,
        expected_job_state_version: int | None = None,
        expected_window_state_version: int | None = None,
        failure_class: str | None = None,
    ) -> tuple[ShadowAttemptReport, ...]:
        """Persist one complete sanitized failure graph and atomically reset the window.

        Success/evidence/candidate transitions remain owned by Task 11/12 and the
        terminal writer.  This method deliberately refuses to attach any object for
        non-success outcomes.
        """
        if outcome not in {"failure", "skip", "unavailable", "mismatch"}:
            raise ShadowJobUnavailable("shadow outcome requires terminal writer")
        reports = tuple(
            ShadowAttemptReport(
                report_id=f"report-{session_id}-{request.ordinal:06d}",
                attempt_id=f"attempt-{session_id}-{request.ordinal:06d}",
                job_id=plan.job_id,
                provider_id=plan.provider_id,
                window_id=plan.window_id,
                session_id=session_id,
                logical_request_ordinal=request.ordinal,
                request_id=request.request_id,
                endpoint_class=request.endpoint_class,
                outcome=outcome,
                started_at="",
                completed_at="",
                failure_class=failure_class or outcome,
                durable_report_ref=f"reports/{session_id}",
            )
            for request in plan.requests
        )
        report_id = f"session-report-{session_id}-v1"
        report_raw = json.dumps(
            [item.model_dump(mode="json") for item in reports],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        report_sha = hashlib.sha256(report_raw.encode()).hexdigest()
        terminal_status = "unavailable" if outcome == "unavailable" else "failed"

        def save(connection):
            job = self._row(connection, plan.job_id)
            window = connection.execute(
                "SELECT state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
                (plan.provider_id, plan.window_id),
            ).fetchone()
            if job is None or window is None:
                raise ShadowJobUnavailable("shadow lifecycle unavailable")
            if (
                expected_job_state_version is not None
                and job.state_version != expected_job_state_version
            ):
                raise ShadowJobUnavailable("shadow job CAS conflict")
            if (
                expected_window_state_version is not None
                and window[0] != expected_window_state_version
            ):
                raise ShadowJobUnavailable("shadow window CAS conflict")
            changed = connection.execute(
                "UPDATE shadow_job SET run_status=?,lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=? AND run_status IN ('leased','pending','pending_normalization')",
                (terminal_status, plan.job_id, plan.provider_id, plan.window_id, job.state_version),
            )
            if changed.rowcount != 1:
                raise ShadowJobUnavailable("shadow job state changed")
            new_state = job.state_version + 1
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
                        report.endpoint_class,
                        report.endpoint_class,
                        report.logical_request_ordinal,
                        1,
                        version_vector_sha256,
                        report.outcome,
                        report.started_at,
                        report.completed_at,
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
                        new_state,
                    ),
                )
            connection.execute(
                "INSERT INTO session_report (session_report_id,provider_id,job_id,window_id,session_id,successful_attempt_id,evidence_id,candidate_id,terminal_attestation_id,report_version,trade_date,outcome,calendar_generation,calendar_sha256,universe_sha256,version_vector_sha256,evidence_sha256,candidate_sha256,report_ref,report_sha256,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    report_id,
                    plan.provider_id,
                    plan.job_id,
                    plan.window_id,
                    session_id,
                    None,
                    None,
                    None,
                    None,
                    1,
                    trade_date,
                    outcome,
                    calendar_generation,
                    calendar_sha256,
                    universe_sha256,
                    version_vector_sha256,
                    None,
                    None,
                    f"reports/{report_id}",
                    report_sha,
                    new_state,
                ),
            )
            reset = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',last_session_report_id=NULL,qualification_evidence_sha256=NULL,qualification_candidate_sha256=NULL,terminal_attestation_id=NULL,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (plan.provider_id, plan.window_id, window[0]),
            )
            if reset.rowcount != 1:
                raise ShadowJobUnavailable("shadow window CAS conflict")
            return reports

        return self.registry._with_transaction(save)

    persist_failure = persist_outcome

    def publish_bundle(self, identity: str, payload: dict[str, Any]) -> Path:
        """Publish one content-addressed bundle before any registry CAS."""
        if self.shadow_root is None or not self.shadow_root.is_absolute():
            raise ShadowJobUnavailable("shadow root unavailable")
        identity = _safe(identity)
        bundles = self.shadow_root / "bundles"
        staging = self.shadow_root / "staging" / f"{identity}-{secrets.token_hex(8)}"
        bundles.mkdir(parents=True, exist_ok=True)
        staging.mkdir(parents=True, exist_ok=False)
        raw = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        (staging / "payload.json").write_bytes(raw)
        (staging / "COMMIT").write_text(hashlib.sha256(raw).hexdigest() + "\n")
        for path in (staging / "payload.json", staging / "COMMIT"):
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        destination = bundles / identity
        if destination.exists():
            if (destination / "COMMIT").read_text() != (staging / "COMMIT").read_text():
                raise ShadowJobUnavailable("shadow bundle identity conflict")
            return destination
        os.rename(staging, destination)
        return destination

    def scan_recovery(
        self, *, limit: int = 64, attach: Callable[[dict[str, Any]], Any] | None = None
    ) -> int:
        if self.shadow_root is None:
            return 0
        bundles = self.shadow_root / "bundles"
        if not bundles.is_dir():
            return 0
        count = 0
        for directory in sorted(bundles.iterdir())[: max(0, limit)]:
            payload = directory / "payload.json"
            marker = directory / "COMMIT"
            if not directory.is_dir() or not payload.is_file() or not marker.is_file():
                continue
            try:
                raw = payload.read_bytes()
                if hashlib.sha256(raw).hexdigest() + "\n" != marker.read_text():
                    continue
                item = json.loads(raw)
                if attach is not None:
                    attach(item)
                count += 1
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
        return count

    def recover_published_manifests(
        self,
        manifests,
        *,
        provider_id: str | None = None,
        window_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Attach every ready canonical manifest identity idempotently.

        ``manifests`` is an injected iterable of immutable, already-read descriptors;
        this method never opens a canonical root or asks a provider for a fresh value.
        """
        count = 0
        for manifest in manifests:
            if not isinstance(manifest, dict):
                continue
            try:
                if provider_id is not None and manifest["provider_id"] != provider_id:
                    continue
                if window_id is not None and manifest["window_id"] != window_id:
                    continue
                trade_date = str(manifest["trade_date"])
                if start_date is not None and trade_date < start_date:
                    continue
                if end_date is not None and trade_date > end_date:
                    continue
                self.enqueue(
                    job_id=str(manifest["job_id"]),
                    provider_id=str(manifest["provider_id"]),
                    window_id=str(manifest["window_id"]),
                    trade_date=trade_date,
                    universe_id=str(manifest["universe_id"]),
                    canonical_manifest_generation=str(manifest["canonical_manifest_generation"]),
                    canonical_manifest_sha256=str(manifest["canonical_manifest_sha256"]),
                    version_vector_sha256=str(manifest["version_vector_sha256"]),
                )
                count += 1
            except (KeyError, TypeError, ValueError, ShadowJobUnavailable):
                continue
        return count


class ShadowHandoff:
    """Post-lock, zero-wait handoff adapter used by automation."""

    def __init__(
        self, job_store: ShadowJobStore, enqueue_outcome: Callable[[Any], Any] | None = None
    ):
        self.job_store = job_store
        self.enqueue_outcome = enqueue_outcome
        self._offered: set[str] = set()

    def offer(self, outcome: Any, *, wait_budget: float = 0, nonblocking: bool = True) -> bool:
        key = getattr(getattr(outcome, "result", None), "run_id", None) or getattr(
            getattr(outcome, "decision", None), "target_session", None
        )
        key = str(key or "none")
        if key in self._offered:
            return False
        self._offered.add(key)
        if self.enqueue_outcome is not None:
            self.enqueue_outcome(outcome)
        return True


__all__ = [
    "ConfirmedSessionSnapshot",
    "ShadowAttemptReport",
    "ShadowHandoff",
    "ShadowJob",
    "ShadowJobStore",
    "ShadowJobUnavailable",
    "ShadowSessionReport",
]
