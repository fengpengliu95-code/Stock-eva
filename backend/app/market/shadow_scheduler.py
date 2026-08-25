"""Independent, bounded shadow worker; canonical refresh is never called here."""

# ruff: noqa: E501

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from .shadow_jobs import (
    CanonicalOutcomeScanner,
    ShadowJob,
    ShadowJobStore,
    ShadowJobUnavailable,
)
from .shadow_terminal import ShadowTerminalWriter


class ShadowSchedulerOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: str
    job_id: str | None = None
    failure_class: str | None = None
    request_count: int = 0
    elapsed_ms: int = 0


class ShadowScheduler:
    def __init__(
        self,
        job_store: ShadowJobStore,
        worker: Callable[..., Any] | None = None,
        *,
        owner: str | None = None,
        lease_seconds: int = 300,
        max_requests: int = 256,
        deadline_seconds: float = 30.0,
        cancellation: Callable[[], bool] | None = None,
        handoff=None,
        canonical_scanner: CanonicalOutcomeScanner,
        terminal_writer: ShadowTerminalWriter | None = None,
    ) -> None:
        if type(canonical_scanner) is not CanonicalOutcomeScanner:
            raise TypeError("shadow scheduler requires CanonicalOutcomeScanner")
        if terminal_writer is not None and type(terminal_writer) is not ShadowTerminalWriter:
            raise TypeError("shadow scheduler requires ShadowTerminalWriter")
        self.job_store = job_store
        self.worker = worker
        self.owner = owner or f"shadow-{uuid.uuid4().hex[:12]}"
        self.lease_seconds = max(1, lease_seconds)
        self.max_requests = max(1, max_requests)
        self.deadline_seconds = max(0.01, deadline_seconds)
        self.cancellation = cancellation or (lambda: False)
        self.handoff = handoff
        self.canonical_scanner = canonical_scanner
        self.terminal_writer = terminal_writer

    def run_once(self, *, now: datetime | None = None) -> ShadowSchedulerOutcome:
        started = time.monotonic()
        if self.handoff is not None:
            try:
                self.handoff.drain(limit=1)
            except Exception:
                pass
        self.job_store.reclaim_expired(now=now)
        # A scanner/queue caller may pass the identity; default worker selection is bounded.
        job = self._next_job()
        if job is None:
            return ShadowSchedulerOutcome(
                status="idle", elapsed_ms=int((time.monotonic() - started) * 1000)
            )
        try:
            leased = self.job_store.lease(
                job.job_id, owner=self.owner, now=now, lease_seconds=self.lease_seconds
            )
        except ShadowJobUnavailable:
            return ShadowSchedulerOutcome(
                status="busy",
                job_id=job.job_id,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        if self.cancellation():
            self.job_store.release_after_worker(leased, status="cancelled")
            return ShadowSchedulerOutcome(
                status="cancelled",
                job_id=leased.job_id,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        if self.worker is None:
            self.job_store.release_after_worker(leased, status="pending")
            return ShadowSchedulerOutcome(
                status="leased",
                job_id=leased.job_id,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        try:
            result = self.worker(
                leased,
                deadline=started + self.deadline_seconds,
                max_requests=self.max_requests,
                cancelled=self.cancellation,
            )
            status = (
                getattr(result, "status", None)
                or (result.get("status") if isinstance(result, dict) else None)
                or "completed"
            )
            if time.monotonic() - started > self.deadline_seconds:
                status = "budget_exhausted"
            if status in {"failed", "failure", "unavailable", "budget_exhausted"}:
                self._persist_or_release(
                    leased,
                    result,
                    outcome="unavailable" if status == "unavailable" else "failure",
                )
            elif status not in {"completed", "success"}:
                self.job_store.release_after_worker(leased, status="pending")
            elif status in {"completed", "success"}:
                context = self._terminal_context(result)
                if self.terminal_writer is None or context is None:
                    self.job_store.release_after_worker(leased, status="failed")
                    status = "failed"
                    failure_class = "terminal_context_unavailable"
                else:
                    self.terminal_writer.write_success(self.job_store.registry, **context)
            return ShadowSchedulerOutcome(
                status=str(status),
                job_id=leased.job_id,
                failure_class=locals().get("failure_class"),
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception:
            self.job_store.release_after_worker(leased, status="failed")
            return ShadowSchedulerOutcome(
                status="failed",
                job_id=leased.job_id,
                failure_class="worker_error",
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )

    @staticmethod
    def _terminal_context(result: Any) -> dict[str, Any] | None:
        context = (
            result.get("terminal_context")
            if isinstance(result, dict)
            else getattr(result, "terminal_context", None)
        )
        return context if isinstance(context, dict) else None

    def _persist_or_release(self, leased: ShadowJob, result: Any, *, outcome: str) -> None:
        context = (
            result.get("failure_context")
            if isinstance(result, dict)
            else getattr(result, "failure_context", None)
        )
        if isinstance(context, dict):
            self.job_store.persist_outcome(
                **context,
                outcome=outcome,
                expected_job_state_version=leased.state_version,
            )
            return
        self.job_store.release_after_worker(
            leased, status="unavailable" if outcome == "unavailable" else "failed"
        )

    def _next_job(self) -> ShadowJob | None:
        registry = self.job_store.registry
        with registry._lock(shared=True):
            connection = registry._connection_for_read()
            try:
                row = connection.execute(
                    "SELECT job_id FROM shadow_job WHERE run_status='pending' ORDER BY trade_date,job_id LIMIT 1"
                ).fetchone()
                return self.job_store.get(row[0]) if row else None
            finally:
                if not registry._memory:
                    connection.close()

    def scan(self, *, limit: int = 64) -> int:
        return self.canonical_scanner.enqueue(self.job_store, limit=limit)


__all__ = ["ShadowScheduler", "ShadowSchedulerOutcome"]
