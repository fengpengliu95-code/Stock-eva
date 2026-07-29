import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from statistics import fmean, pstdev

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


class UnknownTaxonomyError(ValueError):
    pass


@dataclass(frozen=True)
class SectorPolicy:
    formula_version: str = "sector-rotation-v1"
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "relative_strength_5d": 0.10,
            "relative_strength_20d": 0.15,
            "relative_strength_60d": 0.15,
            "advancing_breadth": 0.10,
            "above_ma20_breadth": 0.10,
            "above_ma60_breadth": 0.10,
            "turnover_change_5d": 0.05,
            "turnover_change_20d": 0.05,
            "turnover_concentration_top3": 0.05,
            "persistence_days": 0.10,
            "cross_section_dispersion": 0.05,
        }
    )


@dataclass(frozen=True)
class LeaderPolicy:
    formula_version: str = "leader-ranking-v1"
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
    raw_current: dict[str, DailyBar]
    market_lineage: MarketLineage | None
    classification_lineage: ClassificationLineage | None
    actual_scope: ActualSectorScope
    quality_issues: list[str]


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
            grouped[(membership.sector_id, membership.sector_name)].append(membership)
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
                "weights": self.sector_policy.weights,
            },
        )
        return SectorRotationResponse(
            result_id=result_id,
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
                candidates.append(self._leader_candidate(context, member, sector_members))
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
                "weights": self.leader_policy.weights,
            },
        )
        return LeaderRankingResponse(
            result_id=result_id,
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
        selected = self.classification_store.read_snapshot(
            as_of,
            include_securities=True,
            taxonomy_id=taxonomy_id,
        )
        raw_bars = self.market_reader.bars_through(
            as_of,
            max_sessions=SECTOR_LOOKBACK_SESSIONS,
        )
        future_bars = [bar for bar in raw_bars if bar.trade_date > as_of]
        bars = [bar for bar in raw_bars if bar.trade_date <= as_of]
        dates = sorted({bar.trade_date for bar in bars})
        if len(dates) > SECTOR_LOOKBACK_SESSIONS:
            allowed = set(dates[-SECTOR_LOOKBACK_SESSIONS:])
            bars = [bar for bar in bars if bar.trade_date in allowed]
            dates = dates[-SECTOR_LOOKBACK_SESSIONS:]
        data_as_of = dates[-1] if dates else None
        quality_issues = []
        if future_bars:
            quality_issues.append("future_market_rows_discarded")
        if data_as_of is not None and data_as_of < as_of:
            quality_issues.append("market_data_stale_for_requested_as_of")

        generation = selected.generation
        self._ensure_taxonomy(taxonomy_id, generation)
        coverage = self._coverage_for(generation, taxonomy_id)
        classification_lineage = (
            ClassificationLineage(
                generation_id=generation.generation_id,
                schema_version=generation.schema_version,
                source=generation.source,
                source_version=generation.source_version,
                source_snapshot_date=generation.source_snapshot_date,
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
        metrics = self._sector_metrics(context, priced_symbols)
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
                "weights": self.sector_policy.weights,
            },
        )
        return SectorRanking(
            rank=1,
            ranking_id=ranking_id,
            sector_id=sector_id,
            sector_name=sector_name,
            member_count=len(members),
            priced_member_count=len(priced_symbols),
            total_score=total_score,
            confidence=confidence,
            metric_scores=metrics,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            quality_status=(
                "missing" if total_score is None else "degraded" if missing or issues else "ready"
            ),
            missing_inputs=missing,
            quality_issues=sorted(issues),
        )

    def _sector_metrics(
        self,
        context: _Context,
        symbols: list[str],
    ) -> list[MetricScore]:
        metrics = []
        for period, weight in (
            (5, self.sector_policy.weights["relative_strength_5d"]),
            (20, self.sector_policy.weights["relative_strength_20d"]),
            (60, self.sector_policy.weights["relative_strength_60d"]),
        ):
            member_returns = [
                value
                for symbol in symbols
                if (
                    value := _period_return(
                        context.histories.get(symbol, []),
                        period,
                        context.data_as_of,
                    )
                )
                is not None
            ]
            benchmark = _market_period_return(
                context.histories,
                period,
                context.data_as_of,
            )
            raw = (
                fmean(member_returns) - benchmark
                if member_returns and benchmark is not None
                else None
            )
            metrics.append(
                _metric(
                    f"relative_strength_{period}d",
                    raw=raw,
                    score=_clamp(raw * 1000) if raw is not None else None,
                    weight=weight,
                    unit="decimal_relative_return",
                    formula=f"sector-equal-weight-minus-observed-main-benchmark-{period}d-v1",
                    missing=([f"sector.relative_strength_{period}d_warmup"] if raw is None else []),
                )
            )

        current_returns = [
            value
            for symbol in symbols
            if (
                value := _daily_return(
                    _current_bar(context.histories.get(symbol, []), context.data_as_of)
                )
            )
            is not None
        ]
        advance = (
            sum(value > 0 for value in current_returns) / len(current_returns)
            if current_returns
            else None
        )
        metrics.append(
            _metric(
                "advancing_breadth",
                raw=advance,
                score=_ratio_score(advance) if advance is not None else None,
                weight=self.sector_policy.weights["advancing_breadth"],
                unit="ratio",
                formula="sector-advancing-member-ratio-v1",
                missing=["sector.advancing_breadth"] if advance is None else [],
            )
        )
        for period in (20, 60):
            eligible = [
                closes
                for symbol in symbols
                if len(
                    closes := _adjusted_closes(
                        context.histories.get(symbol, []),
                        context.data_as_of,
                    )
                )
                >= period
            ]
            raw = (
                sum(closes[-1] > fmean(closes[-period:]) for closes in eligible) / len(eligible)
                if eligible
                else None
            )
            name = f"above_ma{period}_breadth"
            metrics.append(
                _metric(
                    name,
                    raw=raw,
                    score=_ratio_score(raw) if raw is not None else None,
                    weight=self.sector_policy.weights[name],
                    unit="ratio",
                    formula=f"sector-qfq-close-above-ma{period}-ratio-v1",
                    missing=[f"sector.above_ma{period}_breadth_warmup"] if raw is None else [],
                )
            )
        for period in (5, 20):
            raw = _turnover_change(
                context.histories,
                symbols,
                period,
                context.data_as_of,
            )
            name = f"turnover_change_{period}d"
            metrics.append(
                _metric(
                    name,
                    raw=raw,
                    score=_clamp(raw * 100) if raw is not None else None,
                    weight=self.sector_policy.weights[name],
                    unit="decimal_change",
                    formula=f"sector-current-turnover-vs-prior-{period}d-mean-v1",
                    missing=[f"sector.turnover_change_{period}d_warmup"] if raw is None else [],
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
        amount_total = sum(current_amounts)
        concentration = (
            sum(sorted(current_amounts, reverse=True)[:3]) / amount_total
            if amount_total > 0
            else None
        )
        metrics.append(
            _metric(
                "turnover_concentration_top3",
                raw=concentration,
                score=_clamp((0.75 - concentration) * 200) if concentration is not None else None,
                weight=self.sector_policy.weights["turnover_concentration_top3"],
                unit="ratio",
                formula="sector-top3-turnover-share-cohesion-v1",
                missing=["sector.turnover_concentration"] if concentration is None else [],
            )
        )
        persistence = _sector_persistence_days(
            context.histories,
            symbols,
            context.data_as_of,
        )
        metrics.append(
            _metric(
                "persistence_days",
                raw=float(persistence) if persistence is not None else None,
                score=_clamp(persistence / 20 * 100) if persistence is not None else None,
                weight=self.sector_policy.weights["persistence_days"],
                unit="signed_sessions",
                formula="sector-consecutive-relative-return-sign-max20-v1",
                missing=["sector.persistence_history"] if persistence is None else [],
            )
        )
        dispersion = pstdev(current_returns) if len(current_returns) >= 2 else None
        metrics.append(
            _metric(
                "cross_section_dispersion",
                raw=dispersion,
                score=_low_dispersion_score(dispersion) if dispersion is not None else None,
                weight=self.sector_policy.weights["cross_section_dispersion"],
                unit="return_standard_deviation",
                formula="sector-current-return-population-dispersion-v1",
                missing=["sector.cross_section_dispersion"] if dispersion is None else [],
            )
        )
        return metrics

    def _leader_exclusion_reasons(
        self,
        context: _Context,
        member: SectorMembershipRecord,
    ) -> list[str]:
        security = context.securities.get(member.security_id)
        if security is None:
            return ["unknown_classification_member"]
        reasons = []
        if security.board != "main":
            reasons.append("outside_narrow_main_board_scope")
        if not security.is_tradable:
            reasons.append("classification_not_tradable")
        if security.price_available is not True:
            reasons.append("classification_price_not_verified")
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

    def _leader_candidate(
        self,
        context: _Context,
        member: SectorMembershipRecord,
        sector_members: list[SectorMembershipRecord],
    ) -> LeaderCandidate:
        symbol = member.symbol
        rows = context.histories[symbol]
        sector_symbols = [
            item.symbol
            for item in sector_members
            if (
                (security := context.securities.get(item.security_id)) is not None
                and security.board == "main"
                and item.symbol in context.histories
            )
        ]
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
            stock_return = _period_return(rows, period, context.data_as_of)
            benchmark = _market_period_return(
                context.histories,
                period,
                context.data_as_of,
            )
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
        activity = _activity_ratio(rows, 20, context.data_as_of)
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
        closes = _adjusted_closes(rows, context.data_as_of)
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
        stock_20 = _period_return(rows, 20, context.data_as_of)
        valid_sector_20 = [
            value
            for item in sector_symbols
            if (
                value := _period_return(
                    context.histories[item],
                    20,
                    context.data_as_of,
                )
            )
            is not None
        ]
        contribution = (
            stock_20 / len(valid_sector_20) if stock_20 is not None and valid_sector_20 else None
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
            context.histories,
            symbol,
            sector_symbols,
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
        current = _daily_return(_current_bar(rows, context.data_as_of))
        sector_current = [
            value
            for item in sector_symbols
            if (value := _daily_return(_current_bar(context.histories[item], context.data_as_of)))
            is not None
        ]
        deviation = (
            abs(current - fmean(sector_current)) if current is not None and sector_current else None
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
        candidate_id = _stable_id(
            "leader",
            {
                "generation": context.classification_lineage,
                "market": context.market_lineage,
                "sector_id": member.sector_id,
                "symbol": symbol,
                "metrics": metrics,
                "weights": self.leader_policy.weights,
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
            },
        )
        return SectorRotationResponse(
            result_id=result_id,
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


def _market_lineage(bars: list[DailyBar]) -> MarketLineage:
    stable = [
        {
            "trade_date": bar.trade_date.isoformat(),
            "symbol": bar.symbol,
            "close": bar.close,
            "preclose": bar.preclose,
            "amount": bar.amount,
            "adjust_factor": bar.adjust_factor,
            "is_suspended": bar.is_suspended,
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
) -> float | None:
    daily: dict[date, float] = defaultdict(float)
    for symbol in symbols:
        for bar in _valid_history(histories.get(symbol, []), data_as_of):
            daily[bar.trade_date] += bar.amount
    dates = sorted(daily)
    if len(dates) < period + 1 or not dates or dates[-1] != data_as_of:
        return None
    prior = [daily[item] for item in dates[-period - 1 : -1]]
    baseline = fmean(prior)
    if baseline <= 0:
        return None
    return daily[dates[-1]] / baseline - 1


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
    histories: dict[str, list[DailyBar]],
    symbols: list[str],
    data_as_of: date | None,
) -> int | None:
    sector = _daily_group_returns(histories, symbols)
    market = _daily_market_returns(histories)
    spreads = {day: value - market[day] for day, value in sector.items() if day in market}
    return _signed_persistence(spreads, data_as_of)


def _leader_persistence_days(
    histories: dict[str, list[DailyBar]],
    symbol: str,
    sector_symbols: list[str],
    data_as_of: date | None,
) -> int | None:
    stock = _daily_group_returns(histories, [symbol])
    sector = _daily_group_returns(histories, sector_symbols)
    spreads = {day: value - sector[day] for day, value in stock.items() if day in sector}
    return _signed_persistence(spreads, data_as_of)


def _activity_ratio(
    bars: list[DailyBar],
    period: int,
    data_as_of: date | None,
) -> float | None:
    valid = _valid_history(bars, data_as_of)
    if len(valid) < period + 1:
        return None
    baseline = fmean(bar.amount for bar in valid[-period - 1 : -1])
    if baseline <= 0:
        return None
    return valid[-1].amount / baseline


def _metric(
    name: str,
    *,
    raw: float | int | None,
    score: float | int | None,
    weight: float,
    unit: str,
    formula: str,
    missing: list[str] | None = None,
    issues: list[str] | None = None,
) -> MetricScore:
    numeric_raw = float(raw) if raw is not None else None
    numeric_score = round(float(score), 6) if score is not None else None
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
        missing_inputs=missing or [],
        quality_issues=issues or [],
    )


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
