"""Read-only Release 1 replay and classification-coverage audit."""

import hashlib
from collections import Counter
from datetime import UTC, date, datetime, time
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.classification.models import TAXONOMY_BAOSTOCK_INDUSTRY
from backend.app.classification.store import eligibility_reason
from backend.app.regime.models import MarketRegimeResult
from backend.app.regime.service import MarketRegimeService
from backend.app.regime.snapshots import RegimeSnapshotStore, RegimeSnapshotUnavailable
from backend.app.storage.dataset import PublishedReadSnapshot

SHANGHAI = ZoneInfo("Asia/Shanghai")
FORMULA_VERSION = "market-regime-v1"
MINIMUM_RELEASE_ONE_SESSIONS = 20

_BLOCKING_ISSUES = {
    "classification_coverage_below_target",
    "classification_coverage_unavailable",
    "classification_counts_unreconciled",
    "dataset_unavailable",
    "future_effective_window_reads",
    "future_evidence_reads",
    "future_lineage_reads",
    "future_membership_reads",
    "future_price_reads",
    "insufficient_sessions",
    "missing_regime_snapshots",
    "snapshot_lineage_mismatch",
    "snapshot_result_mismatch",
    "snapshot_store_unavailable",
}


class ClassificationAuditReader(Protocol):
    def read_snapshot(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
        include_securities: bool = False,
        taxonomy_id: str | None = None,
    ): ...


class ReleaseOneCoverage(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    eligible_count: int = Field(ge=0)
    mapped_count: int = Field(ge=0)
    coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    unmapped_symbols: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_counts(self) -> "ReleaseOneCoverage":
        if self.mapped_count > self.eligible_count:
            raise ValueError("mapped count must not exceed eligible count")
        if len(self.unmapped_symbols) != self.eligible_count - self.mapped_count:
            raise ValueError("coverage counts must reconcile with unmapped symbols")
        return self


class ReleaseOneReplayReport(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    status: Literal["ready", "not_ready"]
    requested_sessions: int = Field(ge=MINIMUM_RELEASE_ONE_SESSIONS)
    session_count: int = Field(ge=0)
    start: date | None = None
    end: date | None = None
    snapshot_count: int = Field(ge=0)
    matching_result_count: int = Field(ge=0)
    future_price_reads: int = Field(ge=0)
    future_lineage_reads: int = Field(ge=0)
    future_evidence_reads: int = Field(ge=0)
    future_membership_reads: int = Field(ge=0)
    future_effective_window_reads: int = Field(ge=0)
    classification_available_sessions: int = Field(ge=0)
    contemporaneous_classification_sessions: int = Field(ge=0)
    post_hoc_verified_sessions: int = Field(ge=0)
    contemporaneous_unverified_sessions: int = Field(ge=0)
    coverage: ReleaseOneCoverage
    capture_modes: dict[str, int]
    audit_cutoff_at: datetime
    dataset_generation: str | None = None
    quality_issues: list[str] = Field(default_factory=list)


class ReleaseOneAcceptanceService:
    """Verify persisted regime evidence without initializing any writer."""

    def __init__(
        self,
        *,
        regime_store,
        snapshot_store: RegimeSnapshotStore,
        classification_store: ClassificationAuditReader,
        evaluator: MarketRegimeService | None = None,
    ) -> None:
        self.regime_store = regime_store
        self.snapshot_store = snapshot_store
        self.classification_store = classification_store
        self.evaluator = evaluator or MarketRegimeService()

    def run(
        self,
        *,
        sessions: int = MINIMUM_RELEASE_ONE_SESSIONS,
        audit_cutoff_at: datetime,
    ) -> ReleaseOneReplayReport:
        if sessions < MINIMUM_RELEASE_ONE_SESSIONS:
            raise ValueError("release one audit requires at least 20 sessions")
        if audit_cutoff_at.tzinfo is None:
            raise ValueError("release one audit cutoff must be timezone-aware")
        cutoff = audit_cutoff_at.astimezone(UTC)
        issues: set[str] = set()
        publication = self._publication(issues)
        selected_dates = (
            tuple(publication.trade_dates[-sessions:]) if publication is not None else ()
        )
        if len(selected_dates) < sessions:
            issues.add("insufficient_sessions")

        snapshot_count = 0
        matching_count = 0
        future_price = 0
        future_lineage = 0
        future_evidence = 0
        capture_modes: Counter[str] = Counter()
        classification_available = 0
        contemporaneous = 0
        future_membership = 0
        future_effective = 0
        current_classification = None
        expected_identity_hash = (
            hashlib.sha256(publication.identity.encode("utf-8")).hexdigest()
            if publication is not None
            else None
        )

        for as_of in selected_dates:
            persisted = self._read_snapshot(as_of, issues)
            if persisted is not None:
                snapshot_count += 1
                capture_modes[persisted.capture_mode] += 1
                if publication is not None and (
                    persisted.dataset_generation != publication.generation
                    or persisted.dataset_identity_hash != expected_identity_hash
                ):
                    issues.add("snapshot_lineage_mismatch")
                try:
                    recomputed = self.evaluator.evaluate(
                        self.regime_store.read_bound(as_of, publication)
                    )
                except Exception:
                    issues.add("dataset_unavailable")
                else:
                    price, lineage, evidence = _future_result_counts(recomputed, as_of)
                    future_price += price
                    future_lineage += lineage
                    future_evidence += evidence
                    if recomputed.model_dump(mode="json") == persisted.result.model_dump(
                        mode="json"
                    ):
                        matching_count += 1
                    else:
                        issues.add("snapshot_result_mismatch")

            classification = self._classification(as_of, cutoff, issues)
            if classification is None or classification.generation is None:
                continue
            classification_available += 1
            if _is_contemporaneous(classification.generation.observed_at, as_of):
                contemporaneous += 1
            membership_count, effective_count = _future_classification_counts(
                classification,
                as_of,
                cutoff,
            )
            future_membership += membership_count
            future_effective += effective_count
            if as_of == selected_dates[-1]:
                current_classification = classification

        if snapshot_count != len(selected_dates):
            issues.add("missing_regime_snapshots")
        if future_price:
            issues.add("future_price_reads")
        if future_lineage:
            issues.add("future_lineage_reads")
        if future_evidence:
            issues.add("future_evidence_reads")
        if future_membership:
            issues.add("future_membership_reads")
        if future_effective:
            issues.add("future_effective_window_reads")
        if classification_available < len(selected_dates):
            issues.add("classification_history_unavailable")
        if contemporaneous < classification_available:
            issues.add("classification_history_observed_late")

        coverage = _coverage(current_classification, selected_dates[-1] if selected_dates else None)
        if coverage.coverage_ratio is None:
            issues.add("classification_coverage_unavailable")
        elif coverage.coverage_ratio < 0.95:
            issues.add("classification_coverage_below_target")
        if current_classification is not None and not _coverage_reconciles(
            current_classification,
            coverage,
        ):
            issues.add("classification_counts_unreconciled")

        status = "not_ready" if issues & _BLOCKING_ISSUES else "ready"
        post_hoc = capture_modes["post_hoc_backfill"]
        return ReleaseOneReplayReport(
            status=status,
            requested_sessions=sessions,
            session_count=len(selected_dates),
            start=selected_dates[0] if selected_dates else None,
            end=selected_dates[-1] if selected_dates else None,
            snapshot_count=snapshot_count,
            matching_result_count=matching_count,
            future_price_reads=future_price,
            future_lineage_reads=future_lineage,
            future_evidence_reads=future_evidence,
            future_membership_reads=future_membership,
            future_effective_window_reads=future_effective,
            classification_available_sessions=classification_available,
            contemporaneous_classification_sessions=contemporaneous,
            post_hoc_verified_sessions=post_hoc,
            contemporaneous_unverified_sessions=len(selected_dates) - contemporaneous,
            coverage=coverage,
            capture_modes={
                "after_close": capture_modes["after_close"],
                "post_hoc_backfill": post_hoc,
            },
            audit_cutoff_at=cutoff,
            dataset_generation=publication.generation if publication is not None else None,
            quality_issues=sorted(issues),
        )

    def _publication(self, issues: set[str]) -> PublishedReadSnapshot | None:
        capture = getattr(self.regime_store.reader, "cache_snapshot", None)
        if not callable(capture):
            issues.add("dataset_unavailable")
            return None
        try:
            publication = capture()
        except Exception:
            issues.add("dataset_unavailable")
            return None
        if (
            not isinstance(publication, PublishedReadSnapshot)
            or not publication.generation
            or tuple(sorted(set(publication.trade_dates))) != publication.trade_dates
        ):
            issues.add("dataset_unavailable")
            return None
        return publication

    def _read_snapshot(self, as_of: date, issues: set[str]):
        try:
            return self.snapshot_store.read_exact(as_of, FORMULA_VERSION)
        except RegimeSnapshotUnavailable:
            issues.add("snapshot_store_unavailable")
            return None

    def _classification(
        self,
        as_of: date,
        cutoff: datetime,
        issues: set[str],
    ):
        try:
            return self.classification_store.read_snapshot(
                as_of,
                known_at=cutoff,
                include_securities=True,
                taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
            )
        except Exception:
            issues.add("classification_coverage_unavailable")
            return None


def _future_result_counts(result: MarketRegimeResult, as_of: date) -> tuple[int, int, int]:
    future_price = int(result.data_as_of is not None and result.data_as_of > as_of)
    lineage = {
        (item.source, item.source_version, item.earliest_input_date, item.latest_input_date)
        for item in result.source_lineage
    }
    evidence = {
        (item.component, item.code, item.as_of, item.source)
        for item in (*result.supporting_evidence, *result.contrary_evidence)
    }
    for component in result.component_scores:
        lineage.update(
            (
                item.source,
                item.source_version,
                item.earliest_input_date,
                item.latest_input_date,
            )
            for item in component.source_lineage
        )
        evidence.update(
            (item.component, item.code, item.as_of, item.source)
            for item in (*component.supporting_evidence, *component.contrary_evidence)
        )
    return (
        future_price,
        sum(item[3] > as_of for item in lineage),
        sum(item[2] > as_of for item in evidence),
    )


def _future_classification_counts(classification, as_of: date, cutoff: datetime) -> tuple[int, int]:
    generation = classification.generation
    membership = int(
        getattr(generation, "source_snapshot_date", as_of) > as_of
        or generation.observed_at.tzinfo is None
        or generation.observed_at.astimezone(UTC) > cutoff
    )
    effective = 0
    for row in classification.securities:
        if getattr(row, "source_snapshot_date", as_of) > as_of:
            membership += 1
    for row in classification.sector_memberships:
        observed_at = row.observed_at
        if observed_at.tzinfo is None or observed_at.astimezone(UTC) > cutoff:
            membership += 1
        if row.source_snapshot_date > as_of:
            membership += 1
        if row.effective_from is not None and row.effective_from > as_of:
            effective += 1
        if row.effective_to is not None and row.effective_to < as_of:
            effective += 1
    return membership, effective


def _is_contemporaneous(observed_at: datetime, as_of: date) -> bool:
    if observed_at.tzinfo is None:
        return False
    cutoff = datetime.combine(as_of, time.max, SHANGHAI)
    return observed_at.astimezone(UTC) <= cutoff.astimezone(UTC)


def _coverage(classification, as_of: date | None) -> ReleaseOneCoverage:
    if classification is None or classification.generation is None or as_of is None:
        return ReleaseOneCoverage(
            eligible_count=0,
            mapped_count=0,
            coverage_ratio=None,
            unmapped_symbols=[],
        )
    eligible = {
        row.security_id: row.symbol
        for row in classification.securities
        if eligibility_reason(row, as_of) is None
    }
    mapped_ids = {row.security_id for row in classification.sector_memberships}
    unmapped = sorted(
        symbol for security_id, symbol in eligible.items() if security_id not in mapped_ids
    )
    mapped = len(eligible) - len(unmapped)
    return ReleaseOneCoverage(
        eligible_count=len(eligible),
        mapped_count=mapped,
        coverage_ratio=(mapped / len(eligible) if eligible else None),
        unmapped_symbols=unmapped,
    )


def _coverage_reconciles(classification, coverage: ReleaseOneCoverage) -> bool:
    audits = [
        item
        for item in classification.generation.coverage_audits
        if item.taxonomy_id == TAXONOMY_BAOSTOCK_INDUSTRY
    ]
    if len(audits) != 1:
        return False
    audit = audits[0]
    return (
        audit.eligible_count == coverage.eligible_count
        and audit.mapped_count == coverage.mapped_count
        and sorted(audit.unmapped_symbols) == coverage.unmapped_symbols
        and (
            (audit.coverage_ratio is None and coverage.coverage_ratio is None)
            or (
                audit.coverage_ratio is not None
                and coverage.coverage_ratio is not None
                and abs(audit.coverage_ratio - coverage.coverage_ratio) < 1e-12
            )
        )
    )
