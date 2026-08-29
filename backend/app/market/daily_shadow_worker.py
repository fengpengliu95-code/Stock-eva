"""One-session orchestration for credentialless TickFlow Free Daily shadow work."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .daily_shadow_candidates import (
    DailyCandidateMismatch,
    DailyCandidateStore,
    DailyCandidateUnavailable,
    build_daily_candidate,
    build_daily_evidence_inputs,
    daily_version_vector_sha256,
)
from .daily_shadow_canonical import PublishedDailyCanonicalProjection
from .daily_shadow_models import (
    DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
    DailyCanonicalReadResult,
    DailyShadowFetchResult,
    DailyShadowPlan,
    domain_sha256,
    validate_daily_failure_class,
)
from .daily_shadow_registry import (
    DailyAttemptAudit,
    DailyCircuitAction,
    DailySessionFailure,
    DailySessionLease,
    DailySessionSuccess,
    DailyShadowRegistry,
    DailyShadowRegistryUnavailable,
    DailyWindowBinding,
    DailyWindowSnapshot,
)
from .providers.tickflow_daily_shadow import TickFlowFreeDailyShadowAdapter
from .shadow_calendar import ConfirmedSessionSnapshot
from .shadow_evidence import (
    ShadowEvidencePublishError,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowEvidenceUnavailable,
)

_FAILURE_CLASS = r"^[a-z0-9_]{1,64}$"


class DailyHalfOpenProbeResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal["SUCCESS", "FAILURE"]
    provider_requests: int = Field(default=1, ge=0, le=1)
    expected_symbols: Literal[5] = 5
    observed_symbols: int = Field(ge=0, le=5)
    failure_class: str | None = Field(default=None, pattern=_FAILURE_CLASS)

    @field_validator("failure_class")
    @classmethod
    def allowlisted_failure_class(cls, value: str | None) -> str | None:
        return validate_daily_failure_class(value)

    @model_validator(mode="after")
    def validate_probe(self) -> DailyHalfOpenProbeResult:
        if self.outcome == "SUCCESS":
            if self.observed_symbols != 5 or self.failure_class is not None:
                raise ValueError("Daily probe success is incomplete")
        elif self.failure_class is None:
            raise ValueError("Daily probe failure is not classified")
        return self


class DailyShadowWorkerResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal[
        "SUCCESS",
        "FAILURE",
        "MISMATCH",
        "UNAVAILABLE",
        "SKIPPED_CIRCUIT_OPEN",
        "SKIPPED_HALF_OPEN",
        "HALF_OPEN_PROBE_SUCCESS",
        "HALF_OPEN_PROBE_FAILURE",
        "BUSY",
        "ALREADY_TERMINAL",
        "RESET",
    ]
    trade_date: date
    provider_requests: int = Field(ge=0, le=40)
    expected_symbols: int = Field(ge=0, le=4000)
    observed_symbols: int = Field(ge=0, le=4000)
    failure_class: str | None = Field(default=None, pattern=_FAILURE_CLASS)
    evidence_id: str | None = None
    evidence_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    candidate_id: str | None = None
    candidate_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    reconciliation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    window_state: Literal["OBSERVING", "RESET", "SHADOW_QUALIFIED"] | None = None
    consecutive_sessions: int = Field(default=0, ge=0, le=20)
    circuit_state: Literal["CLOSED", "OPEN", "HALF_OPEN"] | None = None
    canonical_writes: Literal[False] = False
    publication_enabled: Literal[False] = False
    failover_enabled: Literal[False] = False

    @field_validator("failure_class")
    @classmethod
    def allowlisted_failure_class(cls, value: str | None) -> str | None:
        return validate_daily_failure_class(value)

    @model_validator(mode="after")
    def validate_result(self) -> DailyShadowWorkerResult:
        success_refs = (
            self.evidence_id,
            self.evidence_sha256,
            self.candidate_id,
            self.candidate_sha256,
            self.reconciliation_sha256,
        )
        if self.outcome == "SUCCESS":
            if any(value is None for value in success_refs) or self.failure_class is not None:
                raise ValueError("Daily worker success graph is incomplete")
        elif any(value is not None for value in success_refs):
            raise ValueError("Daily worker non-success cannot expose object refs")
        if (
            self.outcome
            in {
                "FAILURE",
                "MISMATCH",
                "UNAVAILABLE",
                "HALF_OPEN_PROBE_FAILURE",
            }
            and self.failure_class is None
        ):
            raise ValueError("Daily worker failure is not classified")
        return self


class DailyShadowWorker:
    def __init__(
        self,
        *,
        registry: DailyShadowRegistry,
        canonical_factory: Callable[[date], Any],
        confirmed_calendar: ConfirmedSessionSnapshot,
        fetcher: Callable[[DailyShadowPlan], DailyShadowFetchResult],
        evidence_store: ShadowEvidenceStore,
        evidence_reader: ShadowEvidenceReader,
        candidate_store: DailyCandidateStore,
        terms_evidence_sha256: str,
        owner: str,
        probe_runner: Callable[[date], DailyHalfOpenProbeResult] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.registry = registry
        self.canonical_factory = canonical_factory
        self.confirmed_calendar = ConfirmedSessionSnapshot.model_validate(
            confirmed_calendar.model_dump(mode="python")
        )
        self.fetcher = fetcher
        self.evidence_store = evidence_store
        self.evidence_reader = evidence_reader
        self.candidate_store = candidate_store
        self.terms_evidence_sha256 = terms_evidence_sha256
        self.owner = owner
        self.probe_runner = probe_runner
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _identities(*, epoch_id: str, trade_date: date, plan: DailyShadowPlan) -> dict[str, str]:
        digest = domain_sha256(
            "stock-eva/r2f3/free-daily-session-identity/v1",
            {
                "epoch_id": epoch_id,
                "trade_date": trade_date.isoformat(),
                "request_plan_sha256": plan.request_plan_sha256,
                "canonical_snapshot_sha256": plan.canonical_snapshot_sha256,
            },
        )
        suffix = digest[:24]
        return {
            "job_id": f"daily-job-{suffix}",
            "session_id": f"daily-session-{suffix}",
            "evidence_id": f"daily-evidence-{suffix}",
            "session_report_id": f"daily-report-{suffix}",
            "attestation_id": f"daily-attestation-{suffix}",
        }

    @staticmethod
    def _attempts(
        plan: DailyShadowPlan, fetch: DailyShadowFetchResult
    ) -> tuple[DailyAttemptAudit, ...]:
        return tuple(
            DailyAttemptAudit(
                ordinal=item.ordinal,
                request_id=plan.shards[item.ordinal].request_id,
                outcome=item.outcome,
                elapsed_ms=item.elapsed_ms,
                response_bytes=item.response_bytes,
                expected_rows=item.expected_rows,
                observed_rows=item.observed_rows,
                failure_class=item.failure_class,
            )
            for item in fetch.observations
        )

    def _result(
        self,
        *,
        outcome: str,
        trade_date: date,
        provider_requests: int = 0,
        expected_symbols: int = 0,
        observed_symbols: int = 0,
        failure_class: str | None = None,
        window: DailyWindowSnapshot | None = None,
        circuit_state: str | None = None,
        **refs: Any,
    ) -> DailyShadowWorkerResult:
        return DailyShadowWorkerResult(
            outcome=outcome,
            trade_date=trade_date,
            provider_requests=provider_requests,
            expected_symbols=expected_symbols,
            observed_symbols=observed_symbols,
            failure_class=failure_class,
            window_state=window.state if window is not None else None,
            consecutive_sessions=window.consecutive_sessions if window is not None else 0,
            circuit_state=circuit_state,
            **refs,
        )

    def _terminal_failure(
        self,
        *,
        trade_date: date,
        plan: DailyShadowPlan,
        lease: DailySessionLease,
        identities: dict[str, str],
        outcome: Literal["FAILURE", "MISMATCH", "UNAVAILABLE"],
        failure_class: str,
        fetch: DailyShadowFetchResult | None,
        circuit_state: str | None,
    ) -> DailyShadowWorkerResult:
        provider_requests = fetch.request_count if fetch is not None else 0
        observed = len(fetch.rows) if fetch is not None and fetch.status == "ready" else 0
        try:
            window = self.registry.commit_failure(
                DailySessionFailure(
                    epoch_id=lease.epoch_id,
                    job_id=identities["job_id"],
                    session_id=identities["session_id"],
                    session_report_id=identities["session_report_id"],
                    trade_date=trade_date,
                    outcome=outcome,
                    failure_class=failure_class,
                    attempts=self._attempts(plan, fetch) if fetch is not None else (),
                ),
                lease=lease,
            )
        except Exception:
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=trade_date,
                provider_requests=provider_requests,
                expected_symbols=plan.symbol_count,
                observed_symbols=observed,
                failure_class="terminal_unavailable",
                circuit_state=circuit_state,
            )
        return self._result(
            outcome=outcome,
            trade_date=trade_date,
            provider_requests=provider_requests,
            expected_symbols=plan.symbol_count,
            observed_symbols=observed,
            failure_class=failure_class,
            window=window,
            circuit_state=circuit_state,
        )

    def _run_probe(self, trade_date: date, decision: Any) -> DailyShadowWorkerResult:
        try:
            probe = (
                self.probe_runner(trade_date)
                if self.probe_runner is not None
                else DailyHalfOpenProbeResult(
                    outcome="FAILURE",
                    provider_requests=0,
                    observed_symbols=0,
                    failure_class="probe_unavailable",
                )
            )
            probe = DailyHalfOpenProbeResult.model_validate(probe.model_dump(mode="python"))
        except Exception:
            probe = DailyHalfOpenProbeResult(
                outcome="FAILURE",
                provider_requests=0,
                observed_symbols=0,
                failure_class="probe_unavailable",
            )
        success = probe.outcome == "SUCCESS"
        try:
            circuit = self.registry.resolve_probe(
                decision.probe_lease_id,
                owner=self.owner,
                success=success,
                now=self.clock(),
            )
            circuit_state = circuit.state
        except Exception:
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=trade_date,
                provider_requests=probe.provider_requests,
                expected_symbols=5,
                observed_symbols=probe.observed_symbols,
                failure_class="terminal_unavailable",
                circuit_state="HALF_OPEN",
            )
        return self._result(
            outcome=("HALF_OPEN_PROBE_SUCCESS" if success else "HALF_OPEN_PROBE_FAILURE"),
            trade_date=trade_date,
            provider_requests=probe.provider_requests,
            expected_symbols=5,
            observed_symbols=probe.observed_symbols,
            failure_class=probe.failure_class,
            circuit_state=circuit_state,
        )

    def run_one(
        self, trade_date: date, *, simulate_db_crash_at: str | None = None
    ) -> DailyShadowWorkerResult:
        if type(trade_date) is not date:
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=date.min,
                failure_class="worker_unavailable",
            )
        try:
            decision = self.registry.circuit_action(owner=self.owner, now=self.clock())
        except Exception:
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=trade_date,
                failure_class="terminal_unavailable",
            )
        if decision.action == DailyCircuitAction.SKIPPED_CIRCUIT_OPEN:
            return self._result(
                outcome="SKIPPED_CIRCUIT_OPEN",
                trade_date=trade_date,
                circuit_state=decision.state,
            )
        if decision.action == DailyCircuitAction.SKIPPED_HALF_OPEN:
            return self._result(
                outcome="SKIPPED_HALF_OPEN",
                trade_date=trade_date,
                circuit_state=decision.state,
            )
        if decision.action == DailyCircuitAction.RUN_FIXED_FIVE_HALF_OPEN_PROBE:
            return self._run_probe(trade_date, decision)

        canonical: PublishedDailyCanonicalProjection | None = None
        try:
            opened = self.canonical_factory(trade_date)
            if type(opened) is not PublishedDailyCanonicalProjection:
                if isinstance(opened, DailyCanonicalReadResult):
                    return self._result(
                        outcome="UNAVAILABLE",
                        trade_date=trade_date,
                        failure_class="canonical_unavailable",
                        circuit_state=decision.state,
                    )
                raise TypeError("Daily canonical capability unavailable")
            canonical = opened
            snapshot = canonical.snapshot
            plan = TickFlowFreeDailyShadowAdapter().plan(snapshot)
            dates = self.confirmed_calendar.confirmed_next_sessions
            if (
                self.confirmed_calendar.provider_id != "tickflow"
                or len(dates) != 20
                or dates != tuple(sorted(set(dates)))
                or trade_date not in dates
                or snapshot.trade_date != trade_date
                or self.confirmed_calendar.universe_sha256 != DAILY_CANONICAL_UNIVERSE_POLICY_SHA256
            ):
                return self._result(
                    outcome="UNAVAILABLE",
                    trade_date=trade_date,
                    expected_symbols=plan.symbol_count,
                    failure_class="calendar_unavailable",
                    circuit_state=decision.state,
                )
            version_vector = daily_version_vector_sha256(self.terms_evidence_sha256)
            window = self.registry.ensure_window(
                DailyWindowBinding(
                    calendar_generation=self.confirmed_calendar.calendar_generation,
                    calendar_sha256=self.confirmed_calendar.calendar_sha256,
                    universe_policy_sha256=self.confirmed_calendar.universe_sha256,
                    version_vector_sha256=version_vector,
                    terms_evidence_sha256=self.terms_evidence_sha256,
                    expected_dates=dates,
                )
            )
            identities = self._identities(
                epoch_id=window.epoch_id, trade_date=trade_date, plan=plan
            )
            lease = self.registry.lease_session(
                epoch_id=window.epoch_id,
                job_id=identities["job_id"],
                session_id=identities["session_id"],
                trade_date=trade_date,
                request_plan_sha256=plan.request_plan_sha256,
                canonical_snapshot_sha256=snapshot.snapshot_sha256,
                canonical_symbol_set_sha256=plan.canonical_symbol_set_sha256,
                owner=self.owner,
                now=self.clock(),
            )
            if lease.outcome != "LEASED":
                return self._result(
                    outcome=lease.outcome,
                    trade_date=trade_date,
                    expected_symbols=plan.symbol_count,
                    window=window,
                    circuit_state=decision.state,
                )
            try:
                fetch = self.fetcher(plan)
                fetch = DailyShadowFetchResult.model_validate(fetch.model_dump(mode="python"))
            except Exception:
                circuit_state = decision.state
                try:
                    circuit = self.registry.record_endpoint_failure(
                        f"event-{identities['job_id']}-provider-unavailable",
                        failure_class="provider_unavailable",
                        now=self.clock(),
                    )
                    circuit_state = circuit.state
                except Exception:
                    pass
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="UNAVAILABLE",
                    failure_class="provider_unavailable",
                    fetch=None,
                    circuit_state=circuit_state,
                )
            if fetch.status != "ready":
                try:
                    circuit = self.registry.record_endpoint_failure(
                        f"event-{identities['job_id']}-failure",
                        failure_class=fetch.failure_class or "provider_failure",
                        now=self.clock(),
                    )
                    circuit_state = circuit.state
                except Exception:
                    circuit_state = decision.state
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="FAILURE",
                    failure_class=fetch.failure_class or "provider_failure",
                    fetch=fetch,
                    circuit_state=circuit_state,
                )
            try:
                evidence_inputs = build_daily_evidence_inputs(
                    plan,
                    fetch,
                    job_id=identities["job_id"],
                    window_id=window.epoch_id,
                    session_id=identities["session_id"],
                    evidence_id=identities["evidence_id"],
                )
            except Exception:
                try:
                    circuit = self.registry.record_endpoint_failure(
                        f"event-{identities['job_id']}-contract-failure",
                        failure_class="response_contract_invalid",
                        now=self.clock(),
                    )
                    circuit_state = circuit.state
                except Exception:
                    circuit_state = decision.state
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="FAILURE",
                    failure_class="response_contract_invalid",
                    fetch=fetch,
                    circuit_state=circuit_state,
                )
            circuit_state = decision.state
            try:
                circuit = self.registry.record_endpoint_success(
                    f"event-{identities['job_id']}-success", now=self.clock()
                )
                circuit_state = circuit.state
                verified = canonical.verify()
                if (
                    verified.status != "ready"
                    or verified.snapshot is None
                    or verified.snapshot.snapshot_sha256 != snapshot.snapshot_sha256
                ):
                    raise DailyCandidateUnavailable("Daily canonical snapshot changed")
                evidence = self.evidence_store.publish(
                    plan=evidence_inputs.plan,
                    completion=evidence_inputs.completion,
                    attempts=evidence_inputs.attempts,
                )
                artifacts = build_daily_candidate(
                    plan=plan,
                    evidence_plan=evidence_inputs.plan,
                    evidence=evidence,
                    evidence_reader=self.evidence_reader,
                    canonical=canonical,
                    terms_evidence_sha256=self.terms_evidence_sha256,
                )
                published = self.candidate_store.publish(artifacts)
                final_canonical = canonical.verify()
                if (
                    final_canonical.status != "ready"
                    or final_canonical.snapshot is None
                    or final_canonical.snapshot.snapshot_sha256 != snapshot.snapshot_sha256
                    or published.candidate.version_vector_sha256 != version_vector
                    or published.candidate.canonical_symbol_set_sha256
                    != plan.canonical_symbol_set_sha256
                ):
                    raise DailyCandidateUnavailable("Daily canonical snapshot changed")
            except DailyCandidateMismatch:
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="MISMATCH",
                    failure_class="reconciliation_mismatch",
                    fetch=fetch,
                    circuit_state=circuit_state,
                )
            except (
                DailyCandidateUnavailable,
                ShadowEvidenceUnavailable,
                ShadowEvidencePublishError,
            ):
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="UNAVAILABLE",
                    failure_class="candidate_unavailable",
                    fetch=fetch,
                    circuit_state=circuit_state,
                )
            except Exception:
                return self._terminal_failure(
                    trade_date=trade_date,
                    plan=plan,
                    lease=lease,
                    identities=identities,
                    outcome="UNAVAILABLE",
                    failure_class="worker_unavailable",
                    fetch=fetch,
                    circuit_state=circuit_state,
                )
            try:
                terminal = self.registry.commit_success(
                    DailySessionSuccess(
                        epoch_id=window.epoch_id,
                        job_id=identities["job_id"],
                        session_id=identities["session_id"],
                        session_report_id=identities["session_report_id"],
                        attestation_id=identities["attestation_id"],
                        trade_date=trade_date,
                        canonical_snapshot_sha256=snapshot.snapshot_sha256,
                        canonical_symbol_set_sha256=(
                            published.candidate.canonical_symbol_set_sha256
                        ),
                        request_plan_sha256=plan.request_plan_sha256,
                        completion_sha256=evidence.completion_sha256,
                        evidence_id=evidence.evidence_id,
                        evidence_sha256=evidence.manifest_sha256,
                        evidence_bundle_sha256=evidence.manifest_sha256,
                        evidence_bundle_ref=f"bundles/{evidence.evidence_id}",
                        candidate_id=published.candidate.candidate_id,
                        candidate_sha256=published.candidate.candidate_sha256,
                        candidate_bundle_sha256=published.bundle_sha256,
                        candidate_bundle_ref=(
                            f"daily-candidates/{published.candidate.candidate_id}"
                        ),
                        quality_report_sha256=published.quality.report_sha256,
                        reconciliation_report_sha256=(published.reconciliation.report_sha256),
                        attempts=self._attempts(plan, fetch),
                    ),
                    lease=lease,
                    simulate_crash_at=simulate_db_crash_at,
                )
            except Exception:
                return self._result(
                    outcome="UNAVAILABLE",
                    trade_date=trade_date,
                    provider_requests=fetch.request_count,
                    expected_symbols=plan.symbol_count,
                    observed_symbols=len(fetch.rows),
                    failure_class="terminal_unavailable",
                    circuit_state=circuit_state,
                )
            return self._result(
                outcome="SUCCESS",
                trade_date=trade_date,
                provider_requests=fetch.request_count,
                expected_symbols=plan.symbol_count,
                observed_symbols=len(fetch.rows),
                window=terminal,
                circuit_state=circuit_state,
                evidence_id=evidence.evidence_id,
                evidence_sha256=evidence.manifest_sha256,
                candidate_id=published.candidate.candidate_id,
                candidate_sha256=published.candidate.candidate_sha256,
                reconciliation_sha256=published.reconciliation.report_sha256,
            )
        except (DailyShadowRegistryUnavailable, ValueError, TypeError):
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=trade_date,
                failure_class="worker_unavailable",
                circuit_state=decision.state,
            )
        except Exception:
            return self._result(
                outcome="UNAVAILABLE",
                trade_date=trade_date,
                failure_class="worker_unavailable",
                circuit_state=decision.state,
            )
        finally:
            if canonical is not None:
                canonical.close()


__all__ = [
    "DailyHalfOpenProbeResult",
    "DailyShadowWorker",
    "DailyShadowWorkerResult",
]
