from collections import Counter
from datetime import date

from backend.app.classification.models import (
    INDEX_CATALOG,
    TAXONOMY_BAOSTOCK_INDUSTRY,
    Actionability,
    CoverageAudit,
    Eligibility,
    IndexComponentsResponse,
    SectorMembersResponse,
    SectorResponse,
    SectorSummary,
    SecurityAtAsOf,
    SecurityResponse,
)
from backend.app.classification.store import (
    ClassificationConflictError,
    ClassificationStore,
    actionability_reasons,
    eligibility_reason,
    market_scope,
)


class UnknownTaxonomyError(ValueError):
    pass


class UnknownIndexError(ValueError):
    pass


class ClassificationService:
    def __init__(self, store: ClassificationStore) -> None:
        self.store = store

    def securities(self, as_of: date, *, symbol: str | None = None) -> SecurityResponse:
        rows, generation_id, snapshot_date = self.store.securities_at(as_of)
        generation = self.store.generation_at(as_of)
        scope = generation.market_scope if generation is not None else market_scope([])
        if symbol is not None:
            rows = [row for row in rows if row.symbol == symbol]
        if not rows:
            return SecurityResponse(
                status=(
                    "not_available"
                    if generation is not None or self.store.ready_generation() is not None
                    else "empty"
                ),
                as_of=as_of,
                generation_id=generation_id,
                source_snapshot_date=snapshot_date,
                securities=[],
                market_scope=scope,
                quality_issues=["no_trusted_snapshot_at_as_of"],
            )
        securities = [
            SecurityAtAsOf(
                **row.model_dump(),
                eligibility=Eligibility(
                    eligible=eligibility_reason(row, as_of) is None,
                    exclusion_reason=eligibility_reason(row, as_of),
                ),
                actionability=Actionability(
                    actionable=not actionability_reasons(row),
                    reasons=actionability_reasons(row),
                ),
            )
            for row in rows
        ]
        if all(item.eligibility.exclusion_reason == "not_listed_yet" for item in securities):
            return SecurityResponse(
                status="not_available",
                as_of=as_of,
                generation_id=generation_id,
                source_snapshot_date=snapshot_date,
                securities=[],
                market_scope=scope,
                quality_issues=["security_not_listed_at_as_of"],
            )
        return SecurityResponse(
            status="ready",
            as_of=as_of,
            generation_id=generation_id,
            source_snapshot_date=snapshot_date,
            securities=securities,
            market_scope=scope,
        )

    def index_components(self, index_id: str, as_of: date) -> IndexComponentsResponse:
        metadata = INDEX_CATALOG.get(index_id)
        if metadata is None:
            raise UnknownIndexError(index_id)
        rows, generation_id, snapshot_date = self.store.index_components_at(index_id, as_of)
        if not rows:
            issue = (
                "component_history_not_supplied_by_source"
                if metadata.component_history_capability == "not_supplied"
                else "no_trusted_snapshot_at_as_of"
            )
            return IndexComponentsResponse(
                status="not_available",
                as_of=as_of,
                generation_id=generation_id,
                index=metadata,
                source_snapshot_date=snapshot_date,
                components=[],
                quality_issues=[issue],
            )
        unverified = metadata.component_history_capability == "unverified"
        return IndexComponentsResponse(
            status="degraded" if unverified else "ready",
            as_of=as_of,
            generation_id=generation_id,
            index=metadata,
            source_snapshot_date=snapshot_date,
            components=rows,
            quality_issues=["component_history_unverified"] if unverified else [],
        )

    def _ensure_taxonomy(self, taxonomy_id: str) -> None:
        generation = self.store.generation_at(date.max)
        known = {TAXONOMY_BAOSTOCK_INDUSTRY}
        if generation is not None:
            known.update(item.taxonomy_id for item in generation.coverage_audits)
        if taxonomy_id not in known:
            raise UnknownTaxonomyError(taxonomy_id)

    def sectors(self, taxonomy_id: str, as_of: date) -> SectorResponse:
        self._ensure_taxonomy(taxonomy_id)
        rows, generation_id, snapshot_date = self.store.sector_memberships_at(
            taxonomy_id, as_of
        )
        if not rows:
            return SectorResponse(
                status="not_available",
                as_of=as_of,
                generation_id=generation_id,
                taxonomy_id=taxonomy_id,
                source_snapshot_date=snapshot_date,
                sectors=[],
                quality_issues=["no_trusted_snapshot_at_as_of"],
            )
        grouped: dict[tuple[str, str], int] = Counter(
            (row.sector_id, row.sector_name) for row in rows
        )
        return SectorResponse(
            status="ready",
            as_of=as_of,
            generation_id=generation_id,
            taxonomy_id=taxonomy_id,
            source_snapshot_date=snapshot_date,
            sectors=[
                SectorSummary(
                    sector_id=sector_id,
                    sector_name=sector_name,
                    member_count=count,
                )
                for (sector_id, sector_name), count in sorted(grouped.items())
            ],
        )

    def sector_members(
        self,
        taxonomy_id: str,
        sector_id: str,
        as_of: date,
    ) -> SectorMembersResponse:
        self._ensure_taxonomy(taxonomy_id)
        before = self.store.query_count
        rows, generation_id, snapshot_date = self.store.sector_memberships_at(
            taxonomy_id, as_of
        )
        members = [row for row in rows if row.sector_id == sector_id]
        return SectorMembersResponse(
            status="ready" if members else "not_available",
            as_of=as_of,
            generation_id=generation_id,
            taxonomy_id=taxonomy_id,
            sector_id=sector_id,
            source_snapshot_date=snapshot_date,
            members=members,
            database_query_count=self.store.query_count - before,
            quality_issues=[] if members else ["sector_not_available_at_as_of"],
        )

    def coverage(self, taxonomy_id: str, as_of: date) -> CoverageAudit:
        self._ensure_taxonomy(taxonomy_id)
        generation = self.store.generation_at(as_of)
        if generation is None:
            return CoverageAudit(
                status="empty",
                as_of=as_of,
                generation_id=None,
                taxonomy_id=taxonomy_id,
                eligible_count=0,
                mapped_count=0,
                coverage_ratio=None,
                unmapped_symbols=[],
                exclusion_reasons={},
                source_lineage=[],
                quality_issues=["no_classification_generation"],
            )
        securities, _, _ = self.store.securities_at(as_of)
        memberships, _, _ = self.store.sector_memberships_at(taxonomy_id, as_of)
        eligible = {
            row.security_id: row.symbol
            for row in securities
            if eligibility_reason(row, as_of) is None
        }
        reasons = Counter(
            reason
            for row in securities
            if (reason := eligibility_reason(row, as_of)) is not None
        )
        actionability = Counter(
            reason
            for row in securities
            if row.security_id in eligible
            for reason in actionability_reasons(row)
        )
        mapped_ids = {row.security_id for row in memberships}
        unmapped = sorted(
            symbol for security_id, symbol in eligible.items() if security_id not in mapped_ids
        )
        mapped_count = len(eligible) - len(unmapped)
        ratio = mapped_count / len(eligible) if eligible else None
        issues = []
        status = "ready"
        if ratio is None:
            status = "degraded"
            issues.append("no_eligible_securities")
        elif ratio < 0.95:
            status = "degraded"
            issues.append("classification_coverage_below_target")
        return CoverageAudit(
            status=status,
            as_of=as_of,
            generation_id=generation.generation_id,
            taxonomy_id=taxonomy_id,
            eligible_count=len(eligible),
            mapped_count=mapped_count,
            coverage_ratio=ratio,
            unmapped_symbols=unmapped,
            exclusion_reasons=dict(sorted(reasons.items())),
            actionability_reasons=dict(sorted(actionability.items())),
            source_lineage=[f"{generation.source}@{generation.source_version}"],
            quality_issues=issues,
        )


__all__ = [
    "ClassificationConflictError",
    "ClassificationService",
    "UnknownIndexError",
    "UnknownTaxonomyError",
]
