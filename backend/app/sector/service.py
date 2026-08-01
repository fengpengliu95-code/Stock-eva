import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from statistics import fmean, pstdev

import duckdb

from backend.app.classification.models import (
    TAXONOMY_BAOSTOCK_INDUSTRY,
    GenerationSummary,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.classification.store import (
    ClassificationStore,
    eligibility_reason,
)
from backend.app.market.models import DailyBar
from backend.app.regime.store import (
    CANONICAL_SOURCE_VERSION,
    MarketBarsReader,
)
from backend.app.sector.models import (
    ActualSectorScope,
    ClassificationLineage,
    Confidence,
    EvidenceReason,
    LeaderCandidate,
    LeaderExclusion,
    LeaderRankingResponse,
    MarketLineage,
    MetricScore,
    MissingFundFlowEvidence,
    SectorRanking,
    SectorRotationResponse,
)

SECTOR_LOOKBACK_SESSIONS = 80
FUND_FLOW_MISSING = "fund_flow.r2_evidence_not_integrated"
NARROW_SCOPE_ISSUE = "narrow_main_board_scope_not_full_a_share"
LIMIT_LOCK_MISSING = "leader.limit_lock_status"
LEADER_QUALIFICATION_VERSION = "research-leader-qualification-v1"


class UnknownTaxonomyError(ValueError):
    pass


class ClassificationStorageUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class SectorPolicy:
    formula_version: str = "sector-rotation-v1"
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "relative_strength_5d": 0.08,
            "relative_strength_20d": 0.12,
            "relative_strength_60d": 0.12,
            "advancing_breadth": 0.08,
            "above_ma20_breadth": 0.08,
            "above_ma60_breadth": 0.08,
            "turnover_change_5d": 0.04,
            "turnover_change_20d": 0.04,
            "turnover_concentration_top3": 0.04,
            "persistence_days": 0.08,
            "cross_section_dispersion": 0.04,
            "leader_count": 0.05,
            "leader_diffusion": 0.10,
            "leader_persistence_days": 0.05,
        }
    )
    minimum_member_coverage: float = 0.8
    minimum_rank_members: int = 2


@dataclass(frozen=True)
class LeaderPolicy:
    formula_version: str = "leader-ranking-v1"
    qualification_version: str = LEADER_QUALIFICATION_VERSION
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "tradability": 0.10,
            "relative_strength_5d": 0.10,
            "relative_strength_20d": 0.15,
            "relative_strength_60d": 0.15,
            "trading_activity_20d": 0.10,
            "trend_quality": 0.15,
            "sector_contribution_20d": 0.10,
            "persistence_days": 0.10,
            "dispersion_consistency": 0.05,
        }
    )


@dataclass
class _Context:
    as_of: date
    taxonomy_id: str
    data_as_of: date | None
    securities: dict[str, SecurityMasterRecord]
    memberships: list[SectorMembershipRecord]
    histories: dict[str, list[DailyBar]]
    cache: "_MarketCache"
    raw_current: dict[str, DailyBar]
    market_lineage: MarketLineage | None
    classification_lineage: ClassificationLineage | None
    actual_scope: ActualSectorScope
    quality_issues: list[str]


@dataclass(frozen=True)
class _MarketCache:
    valid_histories: dict[str, list[DailyBar]]
    adjusted_closes: dict[str, list[float]]
    period_returns: dict[int, dict[str, float]]
    benchmark_period_returns: dict[int, float | None]
    daily_returns: dict[str, dict[date, float]]
    market_daily_returns: dict[date, float]
    current_returns: dict[str, float]


@dataclass(frozen=True)
class _SectorCache:
    symbols: tuple[str, ...]
    daily_returns: dict[date, float]
    current_mean: float | None
    return_counts: dict[int, int]


class SectorRotationService:
    """Compute R1-C from one promoted classification read and one market read."""

    def __init__(
        self,
        classification_store: ClassificationStore,
        market_reader: MarketBarsReader,
        *,
        sector_policy: SectorPolicy | None = None,
        leader_policy: LeaderPolicy | None = None,
    ) -> None:
        self.classification_store = classification_store
        self.market_reader = market_reader
        self.sector_policy = sector_policy or SectorPolicy()
        self.leader_policy = leader_policy or LeaderPolicy()

    def rotation(self, as_of: date, taxonomy_id: str) -> SectorRotationResponse:
        context = self._read_context(as_of, taxonomy_id)
        if context.classification_lineage is None:
            return self._empty_rotation(
                context,
                missing_inputs=["classification.promoted_generation"],
                quality_issues=["no_promoted_classification_generation"],
            )
        if context.data_as_of is None:
            return self._empty_rotation(
                context,
                missing_inputs=["market.price_history"],
                quality_issues=["no_market_data_at_or_before_as_of"],
            )

        grouped: dict[tuple[str, str], list[SectorMembershipRecord]] = defaultdict(list)
        for membership in context.memberships:
            reason = self._membership_eligibility_reason(context, membership)
            if reason is not None:
                context.quality_issues.append(f"{reason}_excluded")
                continue
            grouped[(membership.sector_id, membership.sector_name)].append(membership)
        context.quality_issues = sorted(set(context.quality_issues))
        rankings = [
            self._sector_ranking(context, sector_id, sector_name, members)
            for (sector_id, sector_name), members in sorted(grouped.items())
        ]
        rankings.sort(
            key=lambda item: (
                item.total_score is None,
                -(item.total_score or 0),
                item.sector_id,
            )
        )
        rankings = [
            item.model_copy(update={"rank": index}) for index, item in enumerate(rankings, start=1)
        ]
        missing = sorted(
            {
                FUND_FLOW_MISSING,
                *(value for ranking in rankings for value in ranking.missing_inputs),
            }
        )
        issues = sorted(
            {
                *context.quality_issues,
                NARROW_SCOPE_ISSUE,
                "fund_flow_evidence_missing",
                *(value for ranking in rankings for value in ranking.quality_issues),
            }
        )
        status = "degraded" if rankings else "empty"
        result_id = _stable_id(
            "sector-rotation",
            {
                "as_of": as_of,
                "data_as_of": context.data_as_of,
                "taxonomy_id": taxonomy_id,
                "classification": context.classification_lineage,
                "market": context.market_lineage,
                "rankings": rankings,
                "formula_version": self.sector_policy.formula_version,
                "weights": self.sector_policy.weights,
            },
        )
        return SectorRotationResponse(
            result_id=result_id,
            formula_version=self.sector_policy.formula_version,
            status=status,
            quality_status=status,
            as_of=as_of,
            data_as_of=context.data_as_of,
            taxonomy_id=taxonomy_id,
            classification_lineage=context.classification_lineage,
            market_lineage=context.market_lineage,
            actual_scope=context.actual_scope,
            rankings=rankings,
            fund_flow_evidence=MissingFundFlowEvidence(),
            missing_inputs=missing,
            quality_issues=issues,
        )

    def leaders(
        self,
        as_of: date,
        taxonomy_id: str,
        sector_id: str,
    ) -> LeaderRankingResponse:
        context = self._read_context(as_of, taxonomy_id)
        sector_members = [item for item in context.memberships if item.sector_id == sector_id]
        sector_name = sector_members[0].sector_name if sector_members else None
        sector_cache = self._sector_cache(context, sector_members)
        candidates: list[LeaderCandidate] = []
        exclusions: list[LeaderExclusion] = []
        if context.classification_lineage is not None and context.data_as_of is not None:
            for member in sorted(sector_members, key=lambda item: item.symbol):
                reasons = self._leader_exclusion_reasons(context, member)
                if reasons:
                    exclusions.append(
                        LeaderExclusion(symbol=member.symbol, reasons=sorted(set(reasons)))
                    )
                    continue
                candidates.append(self._leader_candidate(context, member, sector_cache))
        candidates.sort(
            key=lambda item: (
                item.total_score is None,
                -(item.total_score or 0),
                item.symbol,
            )
        )
        candidates = [
            item.model_copy(update={"rank": index})
            for index, item in enumerate(candidates, start=1)
        ]
        missing = {FUND_FLOW_MISSING}
        issues = {
            *context.quality_issues,
            NARROW_SCOPE_ISSUE,
            "fund_flow_evidence_missing",
        }
        if context.classification_lineage is None:
            missing.add("classification.promoted_generation")
            issues.add("no_promoted_classification_generation")
        if context.data_as_of is None:
            missing.add("market.price_history")
            issues.add("no_market_data_at_or_before_as_of")
        if not sector_members and context.classification_lineage is not None:
            missing.add("classification.sector_members")
            issues.add("sector_not_available_at_as_of")
        for candidate in candidates:
            missing.update(candidate.missing_inputs)
            issues.update(candidate.quality_issues)
        if context.classification_lineage is None or context.data_as_of is None:
            status = "empty"
        else:
            status = "degraded"
        result_id = _stable_id(
            "leader-ranking",
            {
                "as_of": as_of,
                "data_as_of": context.data_as_of,
                "taxonomy_id": taxonomy_id,
                "sector_id": sector_id,
                "classification": context.classification_lineage,
                "market": context.market_lineage,
                "candidates": candidates,
                "exclusions": exclusions,
                "formula_version": self.leader_policy.formula_version,
                "weights": self.leader_policy.weights,
            },
        )
        return LeaderRankingResponse(
            result_id=result_id,
            formula_version=self.leader_policy.formula_version,
            status=status,
            quality_status=status,
            as_of=as_of,
            data_as_of=context.data_as_of,
            taxonomy_id=taxonomy_id,
            sector_id=sector_id,
            sector_name=sector_name,
            classification_lineage=context.classification_lineage,
            market_lineage=context.market_lineage,
            actual_scope=context.actual_scope,
            candidates=candidates,
            exclusions=exclusions,
            fund_flow_evidence=MissingFundFlowEvidence(),
            missing_inputs=sorted(missing),
            quality_issues=sorted(issues),
        )

    def _read_context(self, as_of: date, taxonomy_id: str) -> _Context:
        try:
            selected = self.classification_store.read_snapshot(
                as_of,
                include_securities=True,
                taxonomy_id=taxonomy_id,
            )
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise ClassificationStorageUnavailableError from exc
        raw_bars = self.market_reader.bars_through(
            as_of,
            max_sessions=SECTOR_LOOKBACK_SESSIONS,
        )
        future_bars = [bar for bar in raw_bars if bar.trade_date > as_of]
        bars = [bar for bar in raw_bars if bar.trade_date <= as_of]
        trusted_main_dates = sorted({bar.trade_date for bar in bars if _usable_bar(bar)})
        data_as_of = trusted_main_dates[-1] if trusted_main_dates else None
        later_out_of_scope = data_as_of is not None and any(
            bar.trade_date > data_as_of for bar in bars
        )
        if data_as_of is not None:
            allowed = set(trusted_main_dates[-SECTOR_LOOKBACK_SESSIONS:])
            bars = [bar for bar in bars if bar.trade_date in allowed]
        else:
            bars = []
        quality_issues = []
        if future_bars:
            quality_issues.append("future_market_rows_discarded")
        if later_out_of_scope:
            quality_issues.append("non_main_rows_after_data_as_of_excluded")
        if data_as_of is not None and data_as_of < as_of:
            quality_issues.append("market_data_stale_for_requested_as_of")

        generation = selected.generation
        self._ensure_taxonomy(taxonomy_id, generation)
        if generation is not None and generation.source_date_semantics == "requested_unverified":
            quality_issues.append("classification_source_date_requested_unverified")
        coverage = self._coverage_for(generation, taxonomy_id)
        classification_lineage = (
            ClassificationLineage(
                generation_id=generation.generation_id,
                schema_version=generation.schema_version,
                source=generation.source,
                source_version=generation.source_version,
                source_snapshot_date=generation.source_snapshot_date,
                source_date_semantics=generation.source_date_semantics,
                taxonomy_id=taxonomy_id,
                coverage_ratio=coverage,
            )
            if generation is not None and coverage is not None
            else None
        )
        securities = {row.security_id: row for row in selected.securities}
        memberships = []
        for membership in selected.sector_memberships:
            if membership.security_id not in securities:
                quality_issues.append("unknown_classification_member_excluded")
                continue
            memberships.append(membership)

        histories = _histories(
            bar for bar in bars if bar.security_type == "stock" and bar.board == "main"
        )
        cache = _market_cache(histories, data_as_of)
        raw_current = {
            bar.symbol: bar
            for bar in bars
            if data_as_of is not None
            and bar.trade_date == data_as_of
            and bar.security_type == "stock"
        }
        market_lineage = _market_lineage(bars) if bars else None
        actual_scope = self._actual_scope(
            as_of,
            selected.securities,
            memberships,
            raw_current,
        )
        return _Context(
            as_of=as_of,
            taxonomy_id=taxonomy_id,
            data_as_of=data_as_of,
            securities=securities,
            memberships=memberships,
            histories=histories,
            cache=cache,
            raw_current=raw_current,
            market_lineage=market_lineage,
            classification_lineage=classification_lineage,
            actual_scope=actual_scope,
            quality_issues=sorted(set(quality_issues)),
        )

    @staticmethod
    def _ensure_taxonomy(
        taxonomy_id: str,
        generation: GenerationSummary | None,
    ) -> None:
        if generation is None:
            if taxonomy_id != TAXONOMY_BAOSTOCK_INDUSTRY:
                raise UnknownTaxonomyError(taxonomy_id)
            return
        known = {audit.taxonomy_id for audit in generation.coverage_audits}
        if taxonomy_id not in known:
            raise UnknownTaxonomyError(taxonomy_id)

    @staticmethod
    def _coverage_for(
        generation: GenerationSummary | None,
        taxonomy_id: str,
    ) -> float | None:
        if generation is None:
            return None
        audit = next(
            (item for item in generation.coverage_audits if item.taxonomy_id == taxonomy_id),
            None,
        )
        if (
            audit is None
            or audit.status != "ready"
            or audit.coverage_ratio is None
            or audit.coverage_ratio < audit.target_ratio
        ):
            return None
        return audit.coverage_ratio

    @staticmethod
    def _actual_scope(
        as_of: date,
        securities: list[SecurityMasterRecord],
        memberships: list[SectorMembershipRecord],
        current: dict[str, DailyBar],
    ) -> ActualSectorScope:
        eligible = [row for row in securities if eligibility_reason(row, as_of) is None]
        membership_ids = {row.security_id for row in memberships}
        eligible_members = [row for row in eligible if row.security_id in membership_ids]
        observed = {
            bar.symbol for bar in current.values() if bar.board == "main" and _usable_bar(bar)
        }
        priced = {
            row.symbol for row in eligible_members if row.board == "main" and row.symbol in observed
        }
        markets = sorted(
            {
                "SSE" if row.exchange == "sh" else "SZSE"
                for row in eligible_members
                if row.board == "main" and row.symbol in observed
            }
        )
        excluded_boards = sorted({row.board for row in eligible_members if row.board != "main"})
        return ActualSectorScope(
            included_markets=markets,
            included_boards=["main"],
            excluded_classification_boards=excluded_boards,
            classification_eligible_symbols=len(eligible),
            observed_market_symbols=len(observed),
            priced_classified_symbols=len(priced),
            conclusion_disclaimer=(
                "R1-C v1 uses only observed SSE/SZSE main-board canonical prices and "
                "one promoted point-in-time classification generation. It cannot "
                "support a full A-share or all-industry conclusion."
            ),
        )

    def _sector_ranking(
        self,
        context: _Context,
        sector_id: str,
        sector_name: str,
        members: list[SectorMembershipRecord],
    ) -> SectorRanking:
        eligibility_reason = self._membership_eligibility_reason
        members = [item for item in members if eligibility_reason(context, item) is None]
        member_symbols = [
            item.symbol
            for item in members
            if (
                (security := context.securities.get(item.security_id)) is not None
                and security.board == "main"
            )
        ]
        priced_symbols = [
            symbol
            for symbol in member_symbols
            if (
                (rows := context.histories.get(symbol))
                and context.data_as_of is not None
                and rows[-1].trade_date == context.data_as_of
                and _usable_bar(rows[-1])
            )
        ]
        sector_cache = self._sector_cache(context, members)
        research_candidates = [
            self._leader_candidate(context, member, sector_cache)
            for member in sorted(members, key=lambda item: item.symbol)
            if not self._leader_exclusion_reasons(context, member)
        ]
        metrics = self._sector_metrics(
            context,
            priced_symbols,
            sector_cache=sector_cache,
            target_count=len(member_symbols),
        )
        metrics.extend(
            self._sector_leadership_metrics(
                research_candidates,
                target_count=len(member_symbols),
            )
        )
        total_score = _weighted_total(metrics)
        missing = sorted({value for metric in metrics for value in metric.missing_inputs})
        issues = {value for metric in metrics for value in metric.quality_issues}
        if len(priced_symbols) < len(members):
            issues.add("sector.member_price_missing")
        if len(member_symbols) < len(members):
            issues.add("sector.member_outside_narrow_main_board_scope")
        confidence = _confidence(
            metrics,
            coverage=(len(priced_symbols) / len(members) if members else 0),
            cap=0.75,
            reasons=["narrow_main_board_scope", "fund_flow_evidence_missing"],
        )
        supporting, contrary = _metric_reasons(metrics)
        ranking_id = _stable_id(
            "sector",
            {
                "generation": context.classification_lineage,
                "market": context.market_lineage,
                "sector_id": sector_id,
                "members": sorted(item.symbol for item in members),
                "metrics": metrics,
                "formula_version": self.sector_policy.formula_version,
                "weights": self.sector_policy.weights,
                "leader_qualification_version": self.leader_policy.qualification_version,
            },
        )
        ranking_exclusion_reasons = []
        if len(priced_symbols) < self.sector_policy.minimum_rank_members:
            ranking_exclusion_reasons.append("insufficient_priced_members")
            issues.add("sector.insufficient_members_for_ranking")
        target_count = len(member_symbols)
        priced_coverage = len(priced_symbols) / target_count if target_count else 0
        if target_count and priced_coverage < self.sector_policy.minimum_member_coverage:
            ranking_exclusion_reasons.append("insufficient_comparable_member_coverage")
            issues.add("sector.insufficient_comparable_member_coverage")
        if any(
            "sector.insufficient_comparable_member_coverage" in metric.quality_issues
            for metric in metrics
        ):
            ranking_exclusion_reasons.append("insufficient_horizon_member_coverage")
            issues.add("sector.insufficient_comparable_member_coverage")
        ranking_exclusion_reasons = sorted(set(ranking_exclusion_reasons))
        ranking_eligible = not ranking_exclusion_reasons
        return SectorRanking(
            rank=1,
            ranking_id=ranking_id,
            sector_id=sector_id,
            sector_name=sector_name,
            member_count=len(members),
            priced_member_count=len(priced_symbols),
            ranking_eligible=ranking_eligible,
            ranking_exclusion_reasons=ranking_exclusion_reasons,
            total_score=(total_score if ranking_eligible else None),
            confidence=confidence,
            metric_scores=metrics,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            quality_status=(
                "missing"
                if total_score is None or not ranking_eligible
                else "degraded"
                if missing or issues
                else "ready"
            ),
            missing_inputs=missing,
            quality_issues=sorted(issues),
        )

    def _sector_metrics(
        self,
        context: _Context,
        symbols: list[str],
        *,
        sector_cache: _SectorCache,
        target_count: int,
    ) -> list[MetricScore]:
        metrics = []
        for period, weight in (
            (5, self.sector_policy.weights["relative_strength_5d"]),
            (20, self.sector_policy.weights["relative_strength_20d"]),
            (60, self.sector_policy.weights["relative_strength_60d"]),
        ):
            member_returns = [
                returns[symbol]
                for symbol in symbols
                if symbol in (returns := context.cache.period_returns[period])
            ]
            benchmark = context.cache.benchmark_period_returns[period]
            member_mean = _finite_mean(member_returns)
            raw = (
                member_mean - benchmark
                if member_mean is not None and benchmark is not None
                else None
            )
            coverage_ok, coverage_missing, coverage_issues = _member_coverage(
                len(member_returns),
                target_count,
                minimum=self.sector_policy.minimum_member_coverage,
                minimum_count=self.sector_policy.minimum_rank_members,
            )
            missing = [f"sector.relative_strength_{period}d_warmup"] if raw is None else []
            missing.extend(coverage_missing)
            metrics.append(
                _metric(
                    f"relative_strength_{period}d",
                    raw=raw,
                    score=(_clamp(raw * 1000) if raw is not None and coverage_ok else None),
                    weight=weight,
                    unit="decimal_relative_return",
                    formula=f"sector-equal-weight-minus-observed-main-benchmark-{period}d-v1",
                    effective_count=len(member_returns),
                    target_count=target_count,
                    missing=sorted(set(missing)),
                    issues=coverage_issues,
                )
            )

        current_returns = [
            context.cache.current_returns[symbol]
            for symbol in symbols
            if symbol in context.cache.current_returns
        ]
        advance = (
            sum(value > 0 for value in current_returns) / len(current_returns)
            if current_returns
            else None
        )
        coverage_ok, coverage_missing, coverage_issues = _member_coverage(
            len(current_returns),
            target_count,
            minimum=self.sector_policy.minimum_member_coverage,
            minimum_count=self.sector_policy.minimum_rank_members,
        )
        metrics.append(
            _metric(
                "advancing_breadth",
                raw=advance,
                score=(_ratio_score(advance) if advance is not None and coverage_ok else None),
                weight=self.sector_policy.weights["advancing_breadth"],
                unit="ratio",
                formula="sector-advancing-member-ratio-v1",
                effective_count=len(current_returns),
                target_count=target_count,
                missing=sorted(
                    {
                        *(["sector.advancing_breadth"] if advance is None else []),
                        *coverage_missing,
                    }
                ),
                issues=coverage_issues,
            )
        )
        for period in (20, 60):
            eligible = [
                context.cache.adjusted_closes[symbol]
                for symbol in symbols
                if len(context.cache.adjusted_closes.get(symbol, [])) >= period
            ]
            ma_signals = [
                closes[-1] > mean
                for closes in eligible
                if (mean := _finite_mean(closes[-period:])) is not None
            ]
            raw = sum(ma_signals) / len(ma_signals) if ma_signals else None
            coverage_ok, coverage_missing, coverage_issues = _member_coverage(
                len(ma_signals),
                target_count,
                minimum=self.sector_policy.minimum_member_coverage,
                minimum_count=self.sector_policy.minimum_rank_members,
            )
            name = f"above_ma{period}_breadth"
            metrics.append(
                _metric(
                    name,
                    raw=raw,
                    score=(_ratio_score(raw) if raw is not None and coverage_ok else None),
                    weight=self.sector_policy.weights[name],
                    unit="ratio",
                    formula=f"sector-qfq-close-above-ma{period}-ratio-v1",
                    effective_count=len(ma_signals),
                    target_count=target_count,
                    missing=sorted(
                        {
                            *([f"sector.above_ma{period}_breadth_warmup"] if raw is None else []),
                            *coverage_missing,
                        }
                    ),
                    issues=coverage_issues,
                )
            )
        for period in (5, 20):
            raw, effective_count, non_finite = _turnover_change(
                context.cache.valid_histories,
                symbols,
                period,
                context.data_as_of,
            )
            coverage_ok, coverage_missing, coverage_issues = _member_coverage(
                effective_count,
                target_count,
                minimum=self.sector_policy.minimum_member_coverage,
                minimum_count=self.sector_policy.minimum_rank_members,
            )
            issues = list(coverage_issues)
            if non_finite:
                issues.append("sector.turnover_non_finite")
            name = f"turnover_change_{period}d"
            metrics.append(
                _metric(
                    name,
                    raw=raw,
                    score=(
                        _clamp(raw * 100)
                        if raw is not None and coverage_ok and not non_finite
                        else None
                    ),
                    weight=self.sector_policy.weights[name],
                    unit="decimal_change",
                    formula=f"sector-current-turnover-vs-prior-{period}d-mean-v1",
                    effective_count=effective_count,
                    target_count=target_count,
                    missing=sorted(
                        {
                            *([f"sector.turnover_change_{period}d_warmup"] if raw is None else []),
                            *coverage_missing,
                        }
                    ),
                    issues=sorted(set(issues)),
                )
            )
        current_amounts = [
            bar.amount
            for symbol in symbols
            if (
                bar := _current_bar(
                    context.histories.get(symbol, []),
                    context.data_as_of,
                )
            )
            is not None
            and _usable_bar(bar)
        ]
        amount_total = _finite_sum(current_amounts)
        concentration_numerator = _finite_sum(sorted(current_amounts, reverse=True)[:3])
        concentration = (
            concentration_numerator / amount_total
            if (
                concentration_numerator is not None
                and amount_total is not None
                and amount_total > 0
            )
            else None
        )
        non_finite_amount = bool(current_amounts) and (
            amount_total is None or concentration_numerator is None
        )
        coverage_ok, coverage_missing, coverage_issues = _member_coverage(
            len(current_amounts),
            target_count,
            minimum=self.sector_policy.minimum_member_coverage,
            minimum_count=self.sector_policy.minimum_rank_members,
        )
        concentration_issues = list(coverage_issues)
        if non_finite_amount:
            concentration_issues.append("sector.turnover_non_finite")
        metrics.append(
            _metric(
                "turnover_concentration_top3",
                raw=concentration,
                score=(
                    _clamp((0.75 - concentration) * 200)
                    if (concentration is not None and coverage_ok and not non_finite_amount)
                    else None
                ),
                weight=self.sector_policy.weights["turnover_concentration_top3"],
                unit="ratio",
                formula="sector-top3-turnover-share-cohesion-v1",
                effective_count=len(current_amounts),
                target_count=target_count,
                missing=sorted(
                    {
                        *(["sector.turnover_concentration"] if concentration is None else []),
                        *coverage_missing,
                    }
                ),
                issues=sorted(set(concentration_issues)),
            )
        )
        persistence = _sector_persistence_days(
            sector_cache.daily_returns,
            context.cache.market_daily_returns,
            context.data_as_of,
        )
        coverage_ok, coverage_missing, coverage_issues = _member_coverage(
            len(current_returns),
            target_count,
            minimum=self.sector_policy.minimum_member_coverage,
            minimum_count=self.sector_policy.minimum_rank_members,
        )
        metrics.append(
            _metric(
                "persistence_days",
                raw=float(persistence) if persistence is not None else None,
                score=(
                    _clamp(persistence / 20 * 100)
                    if persistence is not None and coverage_ok
                    else None
                ),
                weight=self.sector_policy.weights["persistence_days"],
                unit="signed_sessions",
                formula="sector-consecutive-relative-return-sign-max20-v1",
                effective_count=len(current_returns),
                target_count=target_count,
                missing=sorted(
                    {
                        *(["sector.persistence_history"] if persistence is None else []),
                        *coverage_missing,
                    }
                ),
                issues=coverage_issues,
            )
        )
        try:
            dispersion = pstdev(current_returns) if len(current_returns) >= 2 else None
        except (OverflowError, ValueError):
            dispersion = None
        dispersion = _finite_number(dispersion)
        coverage_ok, coverage_missing, coverage_issues = _member_coverage(
            len(current_returns),
            target_count,
            minimum=self.sector_policy.minimum_member_coverage,
            minimum_count=self.sector_policy.minimum_rank_members,
        )
        metrics.append(
            _metric(
                "cross_section_dispersion",
                raw=dispersion,
                score=(
                    _low_dispersion_score(dispersion)
                    if dispersion is not None and coverage_ok
                    else None
                ),
                weight=self.sector_policy.weights["cross_section_dispersion"],
                unit="return_standard_deviation",
                formula="sector-current-return-population-dispersion-v1",
                effective_count=len(current_returns),
                target_count=target_count,
                missing=sorted(
                    {
                        *(["sector.cross_section_dispersion"] if dispersion is None else []),
                        *coverage_missing,
                    }
                ),
                issues=coverage_issues,
            )
        )
        return metrics

    def _sector_leadership_metrics(
        self,
        candidates: list[LeaderCandidate],
        *,
        target_count: int,
    ) -> list[MetricScore]:
        effective_count = len(candidates)
        coverage_ok, coverage_missing, coverage_issues = _member_coverage(
            effective_count,
            target_count,
            minimum=self.sector_policy.minimum_member_coverage,
            minimum_count=self.sector_policy.minimum_rank_members,
        )
        qualified = [item for item in candidates if item.leader_qualified]
        leader_count = len(qualified)
        diffusion = leader_count / effective_count if effective_count else None
        persistence_values = [
            metric.raw_value
            for candidate in qualified
            for metric in candidate.metric_scores
            if metric.metric == "persistence_days" and metric.raw_value is not None
        ]
        persistence = _finite_mean(persistence_values)
        common = {
            "effective_count": effective_count,
            "target_count": target_count,
            "issues": coverage_issues,
        }
        qualification_version = self.leader_policy.qualification_version
        return [
            _metric(
                "leader_count",
                raw=leader_count,
                score=(_clamp((leader_count - 1) * 50) if coverage_ok else None),
                weight=self.sector_policy.weights["leader_count"],
                unit="qualified_research_leaders",
                formula=f"qualified-research-leader-count-v1+{qualification_version}",
                missing=coverage_missing,
                **common,
            ),
            _metric(
                "leader_diffusion",
                raw=diffusion,
                score=(_ratio_score(diffusion) if diffusion is not None and coverage_ok else None),
                weight=self.sector_policy.weights["leader_diffusion"],
                unit="qualified_candidate_ratio",
                formula=f"qualified-research-leader-diffusion-v1+{qualification_version}",
                missing=coverage_missing,
                **common,
            ),
            _metric(
                "leader_persistence_days",
                raw=persistence,
                score=(
                    _clamp(persistence / 20 * 100)
                    if persistence is not None and coverage_ok
                    else None
                ),
                weight=self.sector_policy.weights["leader_persistence_days"],
                unit="mean_signed_sessions",
                formula=f"qualified-leader-mean-persistence-max20-v1+{qualification_version}",
                missing=sorted(
                    {
                        *(["sector.qualified_leader_persistence"] if persistence is None else []),
                        *coverage_missing,
                    }
                ),
                **common,
            ),
        ]

    def _leader_exclusion_reasons(
        self,
        context: _Context,
        member: SectorMembershipRecord,
    ) -> list[str]:
        security = context.securities.get(member.security_id)
        if security is None:
            return ["unknown_classification_member"]
        reasons = []
        eligibility = eligibility_reason(security, context.as_of)
        if eligibility is not None:
            reasons.append(f"classification_ineligible_{eligibility}")
        if security.board != "main":
            reasons.append("outside_narrow_main_board_scope")
        if not security.is_tradable:
            reasons.append("classification_not_tradable")
        bar = context.raw_current.get(member.symbol)
        if bar is None:
            reasons.append("missing_current_price")
            return reasons
        if bar.is_suspended:
            reasons.append("suspended")
        if not bar.is_trading:
            reasons.append("not_trading")
        if bar.quality_status != "ready" or bar.quality_issues:
            reasons.append("bad_price_quality")
        if not _finite_positive(bar.close) or not _finite_positive(bar.preclose):
            reasons.append("invalid_price")
        if (
            bar.adjust_factor is None
            or not _finite_positive(bar.adjust_factor)
            or not math.isfinite(bar.amount)
            or bar.amount < 0
        ):
            reasons.append("invalid_price_or_activity_input")
        return reasons

    @staticmethod
    def _membership_eligibility_reason(
        context: _Context,
        member: SectorMembershipRecord,
    ) -> str | None:
        security = context.securities.get(member.security_id)
        if security is None:
            return "unknown_classification_member"
        reason = eligibility_reason(security, context.as_of)
        return f"classification_ineligible_{reason}" if reason is not None else None

    @staticmethod
    def _sector_cache(
        context: _Context,
        sector_members: list[SectorMembershipRecord],
    ) -> _SectorCache:
        symbols = tuple(
            item.symbol
            for item in sector_members
            if (
                (security := context.securities.get(item.security_id)) is not None
                and eligibility_reason(security, context.as_of) is None
                and security.board == "main"
                and item.symbol in context.histories
            )
        )
        daily = _daily_group_returns_from_cache(context.cache.daily_returns, symbols)
        current_values = [
            context.cache.current_returns[symbol]
            for symbol in symbols
            if symbol in context.cache.current_returns
        ]
        return _SectorCache(
            symbols=symbols,
            daily_returns=daily,
            current_mean=_finite_mean(current_values),
            return_counts={
                period: sum(symbol in context.cache.period_returns[period] for symbol in symbols)
                for period in (5, 20, 60)
            },
        )

    def _leader_candidate(
        self,
        context: _Context,
        member: SectorMembershipRecord,
        sector_cache: _SectorCache,
    ) -> LeaderCandidate:
        symbol = member.symbol
        metrics = [
            _metric(
                "tradability",
                raw=1,
                score=100,
                weight=self.leader_policy.weights["tradability"],
                unit="boolean",
                formula="classification-and-canonical-current-tradeability-v1",
            )
        ]
        for period in (5, 20, 60):
            stock_return = context.cache.period_returns[period].get(symbol)
            benchmark = context.cache.benchmark_period_returns[period]
            raw = (
                stock_return - benchmark
                if stock_return is not None and benchmark is not None
                else None
            )
            name = f"relative_strength_{period}d"
            metrics.append(
                _metric(
                    name,
                    raw=raw,
                    score=_clamp(raw * 1000) if raw is not None else None,
                    weight=self.leader_policy.weights[name],
                    unit="decimal_relative_return",
                    formula=f"security-return-minus-observed-main-benchmark-{period}d-v1",
                    missing=[f"leader.relative_strength_{period}d_warmup"] if raw is None else [],
                )
            )
        activity = _activity_ratio(
            context.cache.valid_histories.get(symbol, []),
            20,
            context.data_as_of,
            history_is_valid=True,
        )
        metrics.append(
            _metric(
                "trading_activity_20d",
                raw=activity,
                score=_clamp((activity - 1) * 100) if activity is not None else None,
                weight=self.leader_policy.weights["trading_activity_20d"],
                unit="ratio",
                formula="security-current-turnover-vs-prior20d-mean-v1",
                missing=["leader.trading_activity_20d_warmup"] if activity is None else [],
            )
        )
        closes = context.cache.adjusted_closes.get(symbol, [])
        trend_signals = [
            100 if closes[-1] > fmean(closes[-period:]) else -100
            for period in (20, 60)
            if len(closes) >= period
        ]
        trend = fmean(trend_signals) if trend_signals else None
        metrics.append(
            _metric(
                "trend_quality",
                raw=trend / 100 if trend is not None else None,
                score=trend,
                weight=self.leader_policy.weights["trend_quality"],
                unit="mean_ma_signal",
                formula="security-qfq-close-vs-ma20-ma60-v1",
                missing=["leader.trend_quality_warmup"] if trend is None else [],
            )
        )
        stock_20 = context.cache.period_returns[20].get(symbol)
        contribution = (
            stock_20 / sector_cache.return_counts[20]
            if stock_20 is not None and sector_cache.return_counts[20]
            else None
        )
        metrics.append(
            _metric(
                "sector_contribution_20d",
                raw=contribution,
                score=_clamp(contribution * 1000) if contribution is not None else None,
                weight=self.leader_policy.weights["sector_contribution_20d"],
                unit="equal_weight_return_contribution",
                formula="security-20d-equal-weight-sector-contribution-v1",
                missing=["leader.sector_contribution_20d_warmup"] if contribution is None else [],
            )
        )
        persistence = _leader_persistence_days(
            context.cache.daily_returns.get(symbol, {}),
            sector_cache.daily_returns,
            context.data_as_of,
        )
        metrics.append(
            _metric(
                "persistence_days",
                raw=float(persistence) if persistence is not None else None,
                score=_clamp(persistence / 20 * 100) if persistence is not None else None,
                weight=self.leader_policy.weights["persistence_days"],
                unit="signed_sessions",
                formula="security-consecutive-outperformance-vs-sector-max20-v1",
                missing=["leader.persistence_history"] if persistence is None else [],
            )
        )
        current = context.cache.current_returns.get(symbol)
        deviation = (
            abs(current - sector_cache.current_mean)
            if current is not None and sector_cache.current_mean is not None
            else None
        )
        metrics.append(
            _metric(
                "dispersion_consistency",
                raw=deviation,
                score=_low_dispersion_score(deviation) if deviation is not None else None,
                weight=self.leader_policy.weights["dispersion_consistency"],
                unit="absolute_return_deviation",
                formula="security-current-return-deviation-from-sector-v1",
                missing=["leader.dispersion_consistency"] if deviation is None else [],
            )
        )
        total_score = _weighted_total(metrics)
        missing = sorted(
            {
                LIMIT_LOCK_MISSING,
                *(value for metric in metrics for value in metric.missing_inputs),
            }
        )
        issues = sorted(
            {
                "leader.limit_lock_risk_input_unavailable",
                *(value for metric in metrics for value in metric.quality_issues),
            }
        )
        confidence = _confidence(
            metrics,
            coverage=1,
            cap=0.5,
            reasons=[
                "limit_lock_risk_input_unavailable",
                "narrow_main_board_scope",
            ],
        )
        supporting, contrary = _metric_reasons(metrics)
        qualified, qualification_reasons, disqualification_reasons = _leader_qualification(metrics)
        qualification_version = self.leader_policy.qualification_version
        candidate_id = _stable_id(
            "leader",
            {
                "generation": context.classification_lineage,
                "market": context.market_lineage,
                "sector_id": member.sector_id,
                "symbol": symbol,
                "metrics": metrics,
                "formula_version": self.leader_policy.formula_version,
                "weights": self.leader_policy.weights,
                "qualification_version": qualification_version,
                "leader_qualified": qualified,
                "qualification_reasons": qualification_reasons,
                "disqualification_reasons": disqualification_reasons,
            },
        )
        security = context.securities[member.security_id]
        return LeaderCandidate(
            rank=1,
            candidate_id=candidate_id,
            symbol=symbol,
            name=security.name,
            total_score=total_score,
            confidence=confidence,
            actionable_primary=False,
            limit_lock_status="unavailable",
            leader_qualified=qualified,
            qualification_version=qualification_version,
            qualification_reasons=qualification_reasons,
            disqualification_reasons=disqualification_reasons,
            metric_scores=metrics,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            quality_status="degraded",
            missing_inputs=missing,
            quality_issues=issues,
        )

    def _empty_rotation(
        self,
        context: _Context,
        *,
        missing_inputs: list[str],
        quality_issues: list[str],
    ) -> SectorRotationResponse:
        missing = sorted({FUND_FLOW_MISSING, *missing_inputs})
        issues = sorted(
            {
                *context.quality_issues,
                *quality_issues,
                NARROW_SCOPE_ISSUE,
                "fund_flow_evidence_missing",
            }
        )
        result_id = _stable_id(
            "sector-rotation",
            {
                "as_of": context.as_of,
                "data_as_of": context.data_as_of,
                "taxonomy_id": context.taxonomy_id,
                "classification": context.classification_lineage,
                "market": context.market_lineage,
                "missing_inputs": missing,
                "quality_issues": issues,
                "formula_version": self.sector_policy.formula_version,
            },
        )
        return SectorRotationResponse(
            result_id=result_id,
            formula_version=self.sector_policy.formula_version,
            status="empty",
            quality_status="empty",
            as_of=context.as_of,
            data_as_of=context.data_as_of,
            taxonomy_id=context.taxonomy_id,
            classification_lineage=context.classification_lineage,
            market_lineage=context.market_lineage,
            actual_scope=context.actual_scope,
            rankings=[],
            fund_flow_evidence=MissingFundFlowEvidence(),
            missing_inputs=missing,
            quality_issues=issues,
        )


def _histories(bars) -> dict[str, list[DailyBar]]:
    result: dict[str, list[DailyBar]] = defaultdict(list)
    for bar in bars:
        result[bar.symbol].append(bar)
    for rows in result.values():
        rows.sort(key=lambda item: item.trade_date)
    return dict(result)


def _market_cache(
    histories: dict[str, list[DailyBar]],
    data_as_of: date | None,
) -> _MarketCache:
    valid_histories = {
        symbol: valid
        for symbol, bars in histories.items()
        if (valid := _valid_history(bars, data_as_of))
    }
    adjusted_closes: dict[str, list[float]] = {}
    for symbol, bars in valid_histories.items():
        target = _finite_number(bars[-1].adjust_factor)
        if target is None or target <= 0:
            continue
        closes = []
        for bar in bars:
            factor = _finite_number(bar.adjust_factor)
            if factor is None or factor <= 0:
                closes = []
                break
            close = _finite_number(bar.close * factor / target)
            if close is None or close <= 0:
                closes = []
                break
            closes.append(close)
        if closes:
            adjusted_closes[symbol] = closes

    period_returns: dict[int, dict[str, float]] = {}
    benchmark_period_returns: dict[int, float | None] = {}
    for period in (5, 20, 60):
        returns = {
            symbol: value
            for symbol, closes in adjusted_closes.items()
            if (value := _period_return_from_closes(closes, period)) is not None
        }
        period_returns[period] = returns
        benchmark_period_returns[period] = _finite_mean(returns.values())

    daily_returns: dict[str, dict[date, float]] = {}
    grouped: dict[date, list[float]] = defaultdict(list)
    for symbol, bars in histories.items():
        symbol_returns = {
            bar.trade_date: value for bar in bars if (value := _daily_return(bar)) is not None
        }
        if not symbol_returns:
            continue
        daily_returns[symbol] = symbol_returns
        for day, value in symbol_returns.items():
            grouped[day].append(value)
    market_daily_returns = {
        day: value for day, values in grouped.items() if (value := _finite_mean(values)) is not None
    }
    current_returns = {
        symbol: rows[data_as_of]
        for symbol, rows in daily_returns.items()
        if data_as_of is not None and data_as_of in rows
    }
    return _MarketCache(
        valid_histories=valid_histories,
        adjusted_closes=adjusted_closes,
        period_returns=period_returns,
        benchmark_period_returns=benchmark_period_returns,
        daily_returns=daily_returns,
        market_daily_returns=market_daily_returns,
        current_returns=current_returns,
    )


def _market_lineage(bars: list[DailyBar]) -> MarketLineage:
    stable = [
        {
            "trade_date": bar.trade_date.isoformat(),
            "symbol": bar.symbol,
            "security_type": bar.security_type,
            "exchange": bar.exchange,
            "board": bar.board,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "preclose": bar.preclose,
            "volume": bar.volume,
            "amount": bar.amount,
            "turnover_rate": bar.turnover_rate,
            "pct_change": bar.pct_change,
            "adjust_factor": bar.adjust_factor,
            "price_adjustment": bar.price_adjustment,
            "is_trading": bar.is_trading,
            "is_suspended": bar.is_suspended,
            "is_st": bar.is_st,
            "source": bar.source,
            "quality_status": bar.quality_status,
            "quality_issues": bar.quality_issues,
            "source_record_id": bar.source_record_id,
        }
        for bar in sorted(bars, key=lambda item: (item.trade_date, item.symbol))
    ]
    encoded = json.dumps(
        stable,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    dates = [bar.trade_date for bar in bars]
    return MarketLineage(
        source="baostock",
        source_version=CANONICAL_SOURCE_VERSION,
        earliest_input_date=min(dates),
        latest_input_date=max(dates),
        record_count=len(bars),
        content_hash=hashlib.sha256(encoded.encode()).hexdigest(),
    )


def _usable_bar(bar: DailyBar) -> bool:
    return (
        bar.security_type == "stock"
        and bar.board == "main"
        and bar.quality_status == "ready"
        and not bar.quality_issues
        and bar.is_trading
        and not bar.is_suspended
        and _finite_positive(bar.close)
        and _finite_positive(bar.preclose)
        and bar.adjust_factor is not None
        and _finite_positive(bar.adjust_factor)
        and math.isfinite(bar.amount)
        and bar.amount >= 0
    )


def _finite_positive(value) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(number) and number > 0


def _current_bar(
    bars: list[DailyBar],
    data_as_of: date | None,
) -> DailyBar | None:
    if not bars or data_as_of is None or bars[-1].trade_date != data_as_of:
        return None
    return bars[-1]


def _valid_history(
    bars: list[DailyBar],
    data_as_of: date | None,
) -> list[DailyBar]:
    valid = [bar for bar in bars if _usable_bar(bar)]
    if not valid or data_as_of is None or valid[-1].trade_date != data_as_of:
        return []
    return valid


def _adjusted_closes(
    bars: list[DailyBar],
    data_as_of: date | None,
) -> list[float]:
    valid = _valid_history(bars, data_as_of)
    if not valid:
        return []
    target = float(valid[-1].adjust_factor)
    return [
        bar.close * float(bar.adjust_factor) / target
        for bar in valid
        if bar.adjust_factor is not None
    ]


def _period_return(
    bars: list[DailyBar],
    period: int,
    data_as_of: date | None,
) -> float | None:
    closes = _adjusted_closes(bars, data_as_of)
    return _period_return_from_closes(closes, period)


def _period_return_from_closes(
    closes: list[float],
    period: int,
) -> float | None:
    if len(closes) < period + 1:
        return None
    result = closes[-1] / closes[-period - 1] - 1
    return result if math.isfinite(result) else None


def _daily_return(bar: DailyBar | None) -> float | None:
    if bar is None or not _usable_bar(bar):
        return None
    result = bar.close / bar.preclose - 1
    return result if math.isfinite(result) else None


def _market_period_return(
    histories: dict[str, list[DailyBar]],
    period: int,
    data_as_of: date | None,
) -> float | None:
    values = [
        value
        for bars in histories.values()
        if (value := _period_return(bars, period, data_as_of)) is not None
    ]
    return fmean(values) if values else None


def _turnover_change(
    histories: dict[str, list[DailyBar]],
    symbols: list[str],
    period: int,
    data_as_of: date | None,
) -> tuple[float | None, int, bool]:
    comparable: dict[str, dict[date, float]] = {}
    for symbol in symbols:
        valid = histories.get(symbol, [])
        if len(valid) >= period + 1:
            comparable[symbol] = {bar.trade_date: bar.amount for bar in valid}
    if not comparable or data_as_of is None:
        return None, len(comparable), False
    common_dates = set.intersection(*(set(rows) for rows in comparable.values()))
    dates = sorted(common_dates)
    if len(dates) < period + 1 or dates[-1] != data_as_of:
        return None, len(comparable), False
    window = dates[-period - 1 :]
    totals = []
    for day in window:
        total = _finite_sum(rows[day] for rows in comparable.values())
        if total is None:
            return None, len(comparable), True
        totals.append(total)
    baseline = _finite_mean(totals[:-1])
    if baseline is None or baseline <= 0:
        return None, len(comparable), baseline is None
    result = totals[-1] / baseline - 1
    return (
        (result if math.isfinite(result) else None),
        len(comparable),
        not math.isfinite(result),
    )


def _daily_market_returns(
    histories: dict[str, list[DailyBar]],
) -> dict[date, float]:
    grouped: dict[date, list[float]] = defaultdict(list)
    for bars in histories.values():
        for bar in bars:
            value = _daily_return(bar)
            if value is not None:
                grouped[bar.trade_date].append(value)
    return {day: fmean(values) for day, values in grouped.items() if values}


def _daily_group_returns(
    histories: dict[str, list[DailyBar]],
    symbols: list[str],
) -> dict[date, float]:
    grouped: dict[date, list[float]] = defaultdict(list)
    for symbol in symbols:
        for bar in histories.get(symbol, []):
            value = _daily_return(bar)
            if value is not None:
                grouped[bar.trade_date].append(value)
    return {day: fmean(values) for day, values in grouped.items() if values}


def _daily_group_returns_from_cache(
    daily_returns: dict[str, dict[date, float]],
    symbols: tuple[str, ...] | list[str],
) -> dict[date, float]:
    grouped: dict[date, list[float]] = defaultdict(list)
    for symbol in symbols:
        for day, value in daily_returns.get(symbol, {}).items():
            grouped[day].append(value)
    return {
        day: mean for day, values in grouped.items() if (mean := _finite_mean(values)) is not None
    }


def _signed_persistence(spreads: dict[date, float], data_as_of: date | None) -> int | None:
    dates = sorted(spreads)
    if not dates or data_as_of is None or dates[-1] != data_as_of:
        return None
    latest = spreads[dates[-1]]
    sign = 1 if latest > 0 else -1 if latest < 0 else 0
    if sign == 0:
        return 0
    count = 0
    for day in reversed(dates[-20:]):
        value = spreads[day]
        if (value > 0) != (sign > 0) or value == 0:
            break
        count += 1
    return sign * count


def _sector_persistence_days(
    sector: dict[date, float],
    market: dict[date, float],
    data_as_of: date | None,
) -> int | None:
    spreads = {day: value - market[day] for day, value in sector.items() if day in market}
    return _signed_persistence(spreads, data_as_of)


def _leader_persistence_days(
    stock: dict[date, float],
    sector: dict[date, float],
    data_as_of: date | None,
) -> int | None:
    spreads = {day: value - sector[day] for day, value in stock.items() if day in sector}
    return _signed_persistence(spreads, data_as_of)


def _activity_ratio(
    bars: list[DailyBar],
    period: int,
    data_as_of: date | None,
    *,
    history_is_valid: bool = False,
) -> float | None:
    valid = bars if history_is_valid else _valid_history(bars, data_as_of)
    if len(valid) < period + 1:
        return None
    baseline = _finite_mean(bar.amount for bar in valid[-period - 1 : -1])
    if baseline is None or baseline <= 0:
        return None
    result = valid[-1].amount / baseline
    return result if math.isfinite(result) else None


def _metric(
    name: str,
    *,
    raw: float | int | None,
    score: float | int | None,
    weight: float,
    unit: str,
    formula: str,
    effective_count: int | None = None,
    target_count: int | None = None,
    missing: list[str] | None = None,
    issues: list[str] | None = None,
) -> MetricScore:
    numeric_raw = _finite_number(raw)
    numeric_score = _finite_number(score)
    numeric_score = round(numeric_score, 6) if numeric_score is not None else None
    coverage_ratio = (
        round(effective_count / target_count, 6)
        if effective_count is not None and target_count
        else None
    )
    quality = "missing" if numeric_score is None else "degraded" if missing or issues else "ready"
    return MetricScore(
        metric=name,
        raw_value=round(numeric_raw, 8) if numeric_raw is not None else None,
        unit=unit,
        score=numeric_score,
        weight=weight,
        weighted_score=(round(numeric_score * weight, 6) if numeric_score is not None else None),
        formula_version=formula,
        quality_status=quality,
        effective_count=effective_count,
        target_count=target_count,
        coverage_ratio=coverage_ratio,
        missing_inputs=missing or [],
        quality_issues=issues or [],
    )


def _member_coverage(
    effective_count: int,
    target_count: int,
    *,
    minimum: float,
    minimum_count: int,
) -> tuple[bool, list[str], list[str]]:
    ratio = effective_count / target_count if target_count else 0
    missing = []
    if effective_count < minimum_count:
        missing.append("sector.minimum_comparable_members")
    if target_count == 0 or ratio < minimum:
        missing.append("sector.minimum_comparable_member_coverage")
    issues = ["sector.insufficient_comparable_member_coverage"] if missing else []
    return not missing, missing, issues


def _weighted_total(metrics: list[MetricScore]) -> float | None:
    available = [metric for metric in metrics if metric.score is not None]
    weight = sum(metric.weight for metric in available)
    if weight <= 0:
        return None
    return round(
        sum(float(metric.score) * metric.weight for metric in available) / weight,
        4,
    )


def _metric_reasons(
    metrics: list[MetricScore],
) -> tuple[list[EvidenceReason], list[EvidenceReason]]:
    supporting = []
    contrary = []
    for metric in metrics:
        if metric.score is None or metric.score == 0:
            continue
        reason = EvidenceReason(
            code=f"{metric.metric}_{'supporting' if metric.score > 0 else 'contrary'}",
            detail=(
                f"{metric.metric} is "
                f"{'supportive' if metric.score > 0 else 'contrary'} under "
                f"{metric.formula_version}"
            ),
            metric=metric.metric,
            raw_value=metric.raw_value,
            score=metric.score,
        )
        (supporting if metric.score > 0 else contrary).append(reason)
    return supporting, contrary


def _leader_qualification(
    metrics: list[MetricScore],
) -> tuple[bool, list[str], list[str]]:
    by_name = {metric.metric: metric for metric in metrics}
    checks = {
        "positive_relative_strength_20d": (
            by_name.get("relative_strength_20d") is not None
            and by_name["relative_strength_20d"].raw_value is not None
            and by_name["relative_strength_20d"].raw_value > 0
        ),
        "active_turnover_20d": (
            by_name.get("trading_activity_20d") is not None
            and by_name["trading_activity_20d"].raw_value is not None
            and by_name["trading_activity_20d"].raw_value >= 1
        ),
        "positive_trend_quality": (
            by_name.get("trend_quality") is not None
            and by_name["trend_quality"].score is not None
            and by_name["trend_quality"].score > 0
        ),
        "positive_sector_contribution_20d": (
            by_name.get("sector_contribution_20d") is not None
            and by_name["sector_contribution_20d"].raw_value is not None
            and by_name["sector_contribution_20d"].raw_value > 0
        ),
    }
    supporting = sorted(name for name, passed in checks.items() if passed)
    contrary = sorted(name for name, passed in checks.items() if not passed)
    return not contrary, supporting, contrary


def _confidence(
    metrics: list[MetricScore],
    *,
    coverage: float,
    cap: float,
    reasons: list[str],
) -> Confidence:
    available_weight = sum(metric.weight for metric in metrics if metric.score is not None)
    value = round(min(cap, available_weight * max(0, min(1, coverage)) * cap), 4)
    level = "high" if value >= 0.9 else "medium" if value >= 0.5 else "low"
    detail = list(reasons)
    if available_weight < 1:
        detail.append("metric_inputs_incomplete")
    if coverage < 1:
        detail.append("member_price_coverage_incomplete")
    return Confidence(value=value, level=level, reasons=sorted(set(detail)))


def _clamp(value: float) -> float:
    return max(-100.0, min(100.0, value))


def _finite_number(value) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _finite_sum(values) -> float | None:
    try:
        result = math.fsum(values)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _finite_mean(values) -> float | None:
    rows = list(values)
    if not rows:
        return None
    total = _finite_sum(rows)
    if total is None:
        return None
    result = total / len(rows)
    return result if math.isfinite(result) else None


def _ratio_score(value: float) -> float:
    return _clamp((value - 0.5) * 200)


def _low_dispersion_score(value: float) -> float:
    if value <= 0.015:
        return 100.0
    if value >= 0.04:
        return -100.0
    return 100 - 200 * (value - 0.015) / (0.04 - 0.015)


def _stable_id(prefix: str, payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return f"{prefix}-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"


def _json_default(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")
