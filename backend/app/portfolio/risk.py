import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Protocol

import duckdb

from backend.app.classification.models import TAXONOMY_BAOSTOCK_INDUSTRY
from backend.app.classification.store import ClassificationReadSnapshot
from backend.app.market.models import DailyBar
from backend.app.portfolio.ledger import PortfolioLedger, portfolio_evidence_cutoff
from backend.app.portfolio.models import (
    AtrBudget,
    ClassificationEvidenceLineage,
    DrawdownMetrics,
    MarketEvidenceLineage,
    MarketRegimeContext,
    PortfolioDailySnapshot,
    PortfolioExposure,
    PortfolioNav,
    PortfolioRiskResult,
    PositionRisk,
    RiskGuardrail,
    RiskPolicyMetadata,
    SectorConcentration,
    SectorExposure,
    SnapshotLineage,
    TargetExposureBand,
)
from backend.app.regime.service import MarketRegimeService
from backend.app.regime.store import (
    CANONICAL_SOURCE_VERSION,
    MarketBarsReader,
    MarketReadUnavailable,
    MarketRegimeStore,
)


class ClassificationReader(Protocol):
    def read_snapshot(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
        include_securities: bool = False,
        taxonomy_id: str | None = None,
    ) -> ClassificationReadSnapshot: ...


class RegimeReader(Protocol):
    def read(self, as_of: date, *, known_at: datetime | None = None): ...


class PortfolioRiskDependencyUnavailable(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class DeterministicRegimeReader:
    def __init__(self, store: MarketRegimeStore) -> None:
        self.store = store

    def read(self, as_of: date, *, known_at: datetime | None = None):
        return MarketRegimeService().evaluate(self.store.read(as_of, known_at=known_at))


@dataclass(frozen=True)
class PortfolioRiskPolicy:
    formula_version: str = "portfolio-risk-v1"
    guardrail_version: str = "research-default-v1"
    target_band_version: str = "market-regime-exposure-band-v1"
    max_single_weight: Decimal = Decimal("0.20")
    max_sector_weight: Decimal = Decimal("0.35")
    max_total_atr_risk_ratio: Decimal = Decimal("0.06")
    max_drawdown: Decimal = Decimal("0.15")
    sector_mapping_minimum: Decimal = Decimal("0.95")
    manual_nav_tolerance: Decimal = Decimal("0.01")
    degraded_band_widening: Decimal = Decimal("0.10")
    atr_period: int = 14
    market_lookback_sessions: int = 260
    target_bands: dict[tuple[str, str], tuple[Decimal, Decimal]] = field(
        default_factory=lambda: {
            ("bull", "risk_on"): (Decimal("0.60"), Decimal("0.85")),
            ("bull", "neutral"): (Decimal("0.50"), Decimal("0.75")),
            ("bull", "risk_off"): (Decimal("0.35"), Decimal("0.60")),
            ("range", "risk_on"): (Decimal("0.45"), Decimal("0.70")),
            ("range", "neutral"): (Decimal("0.35"), Decimal("0.60")),
            ("range", "risk_off"): (Decimal("0.20"), Decimal("0.45")),
            ("bear", "risk_on"): (Decimal("0.25"), Decimal("0.50")),
            ("bear", "neutral"): (Decimal("0.15"), Decimal("0.40")),
            ("bear", "risk_off"): (Decimal("0.05"), Decimal("0.25")),
        }
    )


class PortfolioRiskService:
    def __init__(
        self,
        ledger: PortfolioLedger,
        market_reader: MarketBarsReader,
        classification_reader: ClassificationReader,
        regime_reader: RegimeReader,
        *,
        policy: PortfolioRiskPolicy | None = None,
    ) -> None:
        self.ledger = ledger
        self.market_reader = market_reader
        self.classification_reader = classification_reader
        self.regime_reader = regime_reader
        self.policy = policy or PortfolioRiskPolicy()

    def evaluate(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
    ) -> PortfolioRiskResult:
        cutoff = portfolio_evidence_cutoff(as_of, known_at)
        view = self.ledger.read_risk_view(as_of, known_at=cutoff)
        snapshot = view.snapshot
        if snapshot is None:
            return self._empty(as_of, cutoff)
        history = view.history
        issues: set[str] = set()
        missing: set[str] = set()
        if snapshot.as_of < as_of:
            issues.add("portfolio_snapshot_stale_for_requested_as_of")

        raw_bars = self.market_reader.bars_through(
            as_of,
            max_sessions=self.policy.market_lookback_sessions,
        )
        if any(bar.trade_date > as_of for bar in raw_bars):
            issues.add("future_market_rows_discarded")
        post_cutoff_market_rows = [
            bar
            for bar in raw_bars
            if _normalized_observed_at(bar.ingested_at) is None
            or _normalized_observed_at(bar.ingested_at) > cutoff
        ]
        if post_cutoff_market_rows:
            issues.add("post_cutoff_market_rows_discarded")
        bars = sorted(
            (
                bar
                for bar in raw_bars
                if bar.trade_date <= as_of
                and (observed := _normalized_observed_at(bar.ingested_at)) is not None
                and observed <= cutoff
            ),
            key=lambda item: (item.trade_date, item.symbol),
        )
        data_as_of = max((bar.trade_date for bar in bars), default=None)
        if data_as_of is None:
            missing.add("market.published_price_history")
        elif data_as_of < as_of:
            issues.add("published_market_data_stale_for_requested_as_of")
        histories: dict[str, list[DailyBar]] = defaultdict(list)
        for bar in bars:
            histories[bar.symbol].append(bar)

        market_lineage = _market_lineage(bars)
        if market_lineage is not None:
            issues.add("market.publication_visibility_unverifiable")
        classification = self._classification(as_of, cutoff)
        classification_lineage = _classification_lineage(classification)
        if classification_lineage is not None:
            lineage_dates = [
                classification_lineage.source_snapshot_date,
                classification_lineage.security_snapshot_date,
                classification_lineage.sector_snapshot_date,
            ]
            if classification_lineage.observed_at > cutoff or any(
                value is not None and value > as_of for value in lineage_dates
            ):
                classification_lineage = None
        sector_map, mapping_ratio, complete_sectors = self._sector_map(
            as_of,
            cutoff,
            snapshot,
            classification,
            issues,
            missing,
        )
        regime = self._regime(as_of, cutoff, issues, missing)

        preliminary: list[dict[str, object]] = []
        for position in snapshot.positions:
            rows = histories.get(position.symbol, [])
            close, price_as_of, price_issue = self._published_close(rows, data_as_of)
            position_missing: set[str] = set()
            position_issues: set[str] = set()
            if price_issue is not None:
                if price_issue.startswith("published_price_missing"):
                    position_missing.add(price_issue)
                else:
                    position_issues.add(price_issue)
            market_value = position.quantity * close if close is not None else None
            atr14, atr_issue = self._atr14(rows, data_as_of)
            if atr_issue is not None:
                position_missing.add(f"{atr_issue}:{position.symbol}")
            atr_risk = position.quantity * atr14 if atr14 is not None else None
            if position.avg_cost is None:
                position_missing.add(f"position.avg_cost:{position.symbol}")
            missing.update(position_missing)
            issues.update(position_issues)
            sector_id, sector_name = sector_map.get(
                position.symbol,
                ("unknown", "Unknown / unmapped"),
            )
            preliminary.append(
                {
                    "position": position,
                    "price_as_of": price_as_of,
                    "close": close,
                    "market_value": market_value,
                    "atr14": atr14,
                    "atr_risk": atr_risk,
                    "sector_id": sector_id,
                    "sector_name": sector_name,
                    "missing": sorted(position_missing),
                    "issues": sorted(position_issues),
                }
            )

        all_prices = all(item["market_value"] is not None for item in preliminary)
        gross_market_value = (
            sum((item["market_value"] for item in preliminary), Decimal("0"))
            if all_prices
            else None
        )
        net_market_value = gross_market_value
        if snapshot.cash is None:
            missing.add("portfolio.cash")
        derived_nav = (
            snapshot.cash + gross_market_value
            if snapshot.cash is not None and gross_market_value is not None
            else None
        )
        if snapshot.manual_nav is not None:
            selected_nav = snapshot.manual_nav
            valuation_basis = "manual_nav"
        elif derived_nav is not None:
            selected_nav = derived_nav
            valuation_basis = "derived_nav"
        else:
            selected_nav = None
            valuation_basis = "unavailable"
            missing.add("portfolio.nav")
        if selected_nav is not None and selected_nav <= 0:
            missing.add("portfolio.positive_nav")
            selected_nav = None
            valuation_basis = "unavailable"
        if snapshot.manual_nav is not None and derived_nav is None:
            issues.add("manual_nav_cannot_be_reconciled")
        elif (
            snapshot.manual_nav is not None
            and derived_nav is not None
            and abs(snapshot.manual_nav - derived_nav) > self.policy.manual_nav_tolerance
        ):
            issues.add("manual_nav_differs_from_derived_nav")

        gross_ratio = _ratio(gross_market_value, selected_nav)
        net_ratio = _ratio(net_market_value, selected_nav)
        positions = []
        for item in preliminary:
            market_value = item["market_value"]
            atr_risk = item["atr_risk"]
            position_missing = item["missing"]
            position_issues = item["issues"]
            quality = (
                "missing"
                if market_value is None
                else "degraded"
                if position_missing or position_issues
                else "ready"
            )
            positions.append(
                PositionRisk(
                    symbol=item["position"].symbol,
                    quantity=item["position"].quantity,
                    avg_cost=item["position"].avg_cost,
                    price_as_of=item["price_as_of"],
                    close=_q(item["close"]),
                    market_value=_q(market_value),
                    weight=_q(_ratio(market_value, selected_nav)),
                    sector_id=item["sector_id"],
                    sector_name=item["sector_name"],
                    atr14=_q(item["atr14"]),
                    atr_risk_amount=_q(atr_risk),
                    atr_risk_ratio=_q(_ratio(atr_risk, selected_nav)),
                    quality_status=quality,
                    missing_inputs=position_missing,
                    quality_issues=position_issues,
                )
            )
        positions.sort(key=lambda item: item.symbol)
        weights = [item.weight for item in positions if item.weight is not None]
        max_single = max(weights, default=Decimal("0")) if all_prices and selected_nav else None

        sector_concentration = self._sector_concentration(
            positions,
            selected_nav,
            mapping_ratio,
            complete_sectors,
            all_prices,
        )
        if not complete_sectors and snapshot.positions:
            missing.add("classification.coverage_95_percent")

        atr_values = [item.atr_risk_amount for item in positions]
        all_atr = all_prices and all(value is not None for value in atr_values)
        total_atr = (
            sum((value for value in atr_values if value is not None), Decimal("0"))
            if all_atr
            else None
        )
        total_atr_ratio = _ratio(total_atr, selected_nav)
        atr_budget = AtrBudget(
            status=("ready" if total_atr_ratio is not None else "missing"),
            total_atr_risk=_q(total_atr),
            total_atr_risk_ratio=_q(total_atr_ratio),
        )
        drawdown = self._drawdown(history, histories, issues)
        guardrails = self._guardrails(
            max_single=max_single,
            max_sector=(
                sector_concentration.max_sector_weight
                if sector_concentration.can_publish_complete
                else None
            ),
            total_atr=total_atr_ratio,
            max_drawdown=(drawdown.max_drawdown if drawdown.status == "ready" else None),
        )
        target = self._target_band(regime, gross_ratio)
        if regime is not None:
            if regime.status == "degraded":
                issues.add("market_regime_degraded")
            if not regime.can_support_full_a_share_conclusion:
                issues.add("market_regime_narrow_scope_not_full_a_share")
        else:
            missing.add("market.regime")

        nav = PortfolioNav(
            cash=_q(snapshot.cash),
            manual_nav=_q(snapshot.manual_nav),
            derived_nav=_q(derived_nav),
            selected_nav=_q(selected_nav),
            valuation_basis=valuation_basis,
        )
        exposure = PortfolioExposure(
            gross_market_value=_q(gross_market_value),
            net_market_value=_q(net_market_value),
            gross_ratio=_q(gross_ratio),
            net_ratio=_q(net_ratio),
            max_single_weight=_q(max_single),
        )
        policy = self._policy_metadata()
        status = "degraded" if missing or issues else "ready"
        lineage = SnapshotLineage(
            snapshot_id=snapshot.snapshot_id,
            as_of=snapshot.as_of,
            revision=snapshot.revision,
            content_hash=snapshot.content_hash,
            recorded_at=snapshot.recorded_at,
            source=snapshot.source,
        )
        payload = {
            "formula_version": self.policy.formula_version,
            "as_of": as_of,
            "evidence_cutoff_at": cutoff,
            "computed_at": cutoff,
            "data_as_of": data_as_of,
            "snapshot": lineage,
            "policy": policy,
            "market_lineage": market_lineage,
            "classification_lineage": classification_lineage,
            "regime": regime,
            "nav": nav,
            "exposure": exposure,
            "positions": positions,
            "sector_concentration": sector_concentration,
            "atr_budget": atr_budget,
            "drawdown": drawdown,
            "guardrails": guardrails,
            "target": target,
            "missing": sorted(missing),
            "issues": sorted(issues),
        }
        return PortfolioRiskResult(
            result_id=_stable_id(payload),
            formula_version=self.policy.formula_version,
            status=status,
            quality_status=status,
            as_of=as_of,
            evidence_cutoff_at=cutoff,
            computed_at=cutoff,
            data_as_of=data_as_of,
            snapshot=lineage,
            market_lineage=market_lineage,
            classification_lineage=classification_lineage,
            policy=policy,
            nav=nav,
            exposure=exposure,
            positions=positions,
            sector_concentration=sector_concentration,
            atr_budget=atr_budget,
            drawdown=drawdown,
            guardrails=guardrails,
            target_exposure=target,
            market_regime=regime,
            missing_inputs=sorted(missing),
            quality_issues=sorted(issues),
        )

    def _classification(
        self,
        as_of: date,
        cutoff: datetime,
    ) -> ClassificationReadSnapshot:
        try:
            return self.classification_reader.read_snapshot(
                as_of,
                known_at=cutoff,
                include_securities=True,
                taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
            )
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise PortfolioRiskDependencyUnavailable("classification_storage_unavailable") from exc

    def _regime(
        self,
        as_of: date,
        cutoff: datetime,
        issues: set[str],
        missing: set[str],
    ) -> MarketRegimeContext | None:
        try:
            result = self.regime_reader.read(as_of, known_at=cutoff)
        except MarketReadUnavailable:
            raise
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise PortfolioRiskDependencyUnavailable("market_regime_unavailable") from exc
        if result.as_of > as_of or (result.data_as_of is not None and result.data_as_of > as_of):
            issues.add("future_market_regime_discarded")
            return None
        if result.status == "empty":
            missing.add("market.regime")
        return MarketRegimeContext(
            result_id=result.result_id,
            formula_version=result.formula_version,
            as_of=result.as_of,
            data_as_of=result.data_as_of,
            status=result.status,
            strategic_state=result.strategic_state,
            tactical_state=result.tactical_state,
            confidence_value=_q(Decimal(str(result.confidence.value))),
            confidence_level=result.confidence.level,
            scope_status=result.actual_market_scope.scope_status,
            can_support_full_a_share_conclusion=(
                result.actual_market_scope.can_support_full_a_share_conclusion
            ),
        )

    def _sector_map(
        self,
        as_of: date,
        cutoff: datetime,
        snapshot: PortfolioDailySnapshot,
        selected: ClassificationReadSnapshot,
        issues: set[str],
        missing: set[str],
    ) -> tuple[dict[str, tuple[str, str]], Decimal, bool]:
        if not snapshot.positions:
            return {}, Decimal("1"), True
        generation = selected.generation
        if generation is None:
            missing.add("classification.promoted_generation")
            return {}, Decimal("0"), False
        generation_observed = _normalized_observed_at(generation.observed_at)
        if (
            generation.source_snapshot_date > as_of
            or generation_observed is None
            or generation_observed > cutoff
        ):
            issues.add("future_classification_generation_discarded")
            return {}, Decimal("0"), False
        audit = next(
            (
                item
                for item in generation.coverage_audits
                if item.taxonomy_id == TAXONOMY_BAOSTOCK_INDUSTRY
            ),
            None,
        )
        global_ready = bool(
            audit is not None
            and audit.status == "ready"
            and audit.coverage_ratio is not None
            and Decimal(str(audit.coverage_ratio)) >= self.policy.sector_mapping_minimum
            and Decimal(str(audit.coverage_ratio)) >= Decimal(str(audit.target_ratio))
        )
        valid_security_ids = {
            item.security_id
            for item in selected.securities
            if item.source_snapshot_date <= as_of
            and (observed := _normalized_observed_at(item.observed_at)) is not None
            and observed <= cutoff
            and (item.list_date is None or item.list_date <= as_of)
            and (item.delist_date is None or item.delist_date > as_of)
        }
        mapping = {
            item.symbol: (item.sector_id, item.sector_name)
            for item in selected.sector_memberships
            if item.security_id in valid_security_ids
            and item.source_snapshot_date <= as_of
            and (observed := _normalized_observed_at(item.observed_at)) is not None
            and observed <= cutoff
            and (item.effective_from is None or item.effective_from <= as_of)
            and (item.effective_to is None or item.effective_to >= as_of)
        }
        position_symbols = {item.symbol for item in snapshot.positions}
        mapped = len(position_symbols & set(mapping))
        ratio = Decimal(mapped) / Decimal(len(position_symbols))
        complete = global_ready and ratio >= self.policy.sector_mapping_minimum
        if not complete:
            missing.add("classification.coverage_95_percent")
        return mapping, _q(ratio), complete

    @staticmethod
    def _published_close(
        rows: list[DailyBar],
        data_as_of: date | None,
    ) -> tuple[Decimal | None, date | None, str | None]:
        if data_as_of is None or not rows or rows[-1].trade_date != data_as_of:
            return None, None, "published_price_missing_for_snapshot"
        bar = rows[-1]
        if bar.is_suspended or not bar.is_trading:
            return None, bar.trade_date, f"published_price_suspended:{bar.symbol}"
        if bar.quality_status != "ready" or bar.quality_issues:
            return None, bar.trade_date, f"published_price_bad_quality:{bar.symbol}"
        if not _valid_price_bar(bar):
            return None, bar.trade_date, f"published_price_invalid:{bar.symbol}"
        return Decimal(str(bar.close)), bar.trade_date, None

    def _atr14(
        self,
        rows: list[DailyBar],
        data_as_of: date | None,
    ) -> tuple[Decimal | None, str | None]:
        required = self.policy.atr_period + 1
        if data_as_of is None:
            return None, "atr14_warmup"
        usable = [bar for bar in rows if _valid_effective_price_bar(bar)]
        if len(usable) < required:
            return None, "atr14_warmup"
        window = usable[-required:]
        if any(bar.adjust_factor is None for bar in window):
            return None, "atr14_adjust_factor_missing"
        factors = [Decimal(str(bar.adjust_factor)) for bar in window]
        if any(not factor.is_finite() or factor <= 0 for factor in factors):
            return None, "atr14_adjust_factor_invalid"
        latest_factor = factors[-1]
        adjusted = []
        for bar, factor in zip(window, factors, strict=True):
            multiplier = factor / latest_factor
            high = Decimal(str(bar.high)) * multiplier
            low = Decimal(str(bar.low)) * multiplier
            close = Decimal(str(bar.close)) * multiplier
            if (
                not multiplier.is_finite()
                or multiplier <= 0
                or not all(value.is_finite() and value > 0 for value in (high, low, close))
                or high < low
            ):
                return None, "atr14_qfq_invalid"
            adjusted.append((high, low, close))
        true_ranges = []
        for previous, current in zip(adjusted, adjusted[1:], strict=False):
            high, low, _ = current
            previous_close = previous[2]
            true_ranges.append(
                max(
                    high - low,
                    abs(high - previous_close),
                    abs(low - previous_close),
                )
            )
        return _q(sum(true_ranges, Decimal("0")) / Decimal(self.policy.atr_period)), None

    def _sector_concentration(
        self,
        positions: list[PositionRisk],
        nav: Decimal | None,
        mapping_ratio: Decimal,
        complete: bool,
        all_prices: bool,
    ) -> SectorConcentration:
        grouped: dict[tuple[str, str], list[PositionRisk]] = defaultdict(list)
        for position in positions:
            grouped[(position.sector_id, position.sector_name)].append(position)
        sectors = []
        for (sector_id, sector_name), items in sorted(grouped.items()):
            market_value = sum(
                (item.market_value for item in items if item.market_value is not None),
                Decimal("0"),
            )
            sectors.append(
                SectorExposure(
                    sector_id=sector_id,
                    sector_name=sector_name,
                    symbol_count=len(items),
                    market_value=_q(market_value),
                    weight=_q(_ratio(market_value, nav)),
                    is_unknown=sector_id == "unknown",
                )
            )
        weights = [item.weight for item in sectors if item.weight is not None]
        max_weight = max(weights, default=Decimal("0")) if all_prices and nav else None
        if not all_prices or nav is None:
            status = "missing"
        elif complete:
            status = "ready"
        else:
            status = "degraded"
        return SectorConcentration(
            status=status,
            mapping_ratio=_q(mapping_ratio),
            can_publish_complete=complete and all_prices and nav is not None,
            max_sector_weight=_q(max_weight),
            sectors=sectors,
        )

    def _drawdown(
        self,
        history: list[PortfolioDailySnapshot],
        histories: dict[str, list[DailyBar]],
        issues: set[str],
    ) -> DrawdownMetrics:
        exact_bars = {
            (bar.trade_date, bar.symbol): Decimal(str(bar.close))
            for rows in histories.values()
            for bar in rows
            if _valid_price_bar(bar)
        }
        navs = []
        unavailable = 0
        for snapshot in history:
            if snapshot.manual_nav is not None and snapshot.manual_nav > 0:
                navs.append(snapshot.manual_nav)
                continue
            values = [
                position.quantity * exact_bars[(snapshot.as_of, position.symbol)]
                for position in snapshot.positions
                if (snapshot.as_of, position.symbol) in exact_bars
            ]
            if (
                snapshot.cash is not None
                and len(values) == len(snapshot.positions)
                and snapshot.cash + sum(values, Decimal("0")) > 0
            ):
                navs.append(snapshot.cash + sum(values, Decimal("0")))
            else:
                unavailable += 1
        if unavailable:
            issues.add("daily_nav_history_incomplete")
        if not navs:
            return DrawdownMetrics(
                status="missing",
                observation_count=0,
                current_drawdown=None,
                max_drawdown=None,
            )
        peak = navs[0]
        drawdowns = []
        for nav in navs:
            peak = max(peak, nav)
            drawdowns.append(nav / peak - Decimal("1"))
        return DrawdownMetrics(
            status="degraded" if unavailable else "ready",
            observation_count=len(navs),
            current_drawdown=_q(drawdowns[-1]),
            max_drawdown=_q(min(drawdowns)),
        )

    def _guardrails(
        self,
        *,
        max_single: Decimal | None,
        max_sector: Decimal | None,
        total_atr: Decimal | None,
        max_drawdown: Decimal | None,
    ) -> list[RiskGuardrail]:
        return [
            self._guardrail("max_single_weight", max_single, self.policy.max_single_weight),
            self._guardrail("max_sector_weight", max_sector, self.policy.max_sector_weight),
            self._guardrail(
                "max_total_atr_risk_ratio",
                total_atr,
                self.policy.max_total_atr_risk_ratio,
            ),
            self._guardrail(
                "max_drawdown",
                abs(max_drawdown) if max_drawdown is not None else None,
                self.policy.max_drawdown,
            ),
        ]

    def _guardrail(
        self,
        code: str,
        current: Decimal | None,
        limit: Decimal,
    ) -> RiskGuardrail:
        if current is None:
            status = "unavailable"
            reason = "required evidence is unavailable"
        elif current > limit:
            status = "breach"
            reason = f"exceeds {self.policy.guardrail_version} research limit"
        else:
            status = "pass"
            reason = f"within {self.policy.guardrail_version} research limit"
        return RiskGuardrail(
            code=code,
            status=status,
            current_value=_q(current),
            limit_value=_q(limit),
            reason=reason,
        )

    def _target_band(
        self,
        regime: MarketRegimeContext | None,
        current: Decimal | None,
    ) -> TargetExposureBand:
        if regime is None or regime.status == "empty":
            return TargetExposureBand(
                status="unavailable",
                base_lower=None,
                base_upper=None,
                lower=None,
                upper=None,
                current=_q(current),
                within_band=None,
                reasons=["market_regime_unavailable"],
            )
        base_lower, base_upper = self.policy.target_bands[
            (regime.strategic_state, regime.tactical_state)
        ]
        reasons = [
            f"market_regime:{regime.strategic_state}",
            f"tactical_state:{regime.tactical_state}",
        ]
        widen = False
        if regime.status == "degraded":
            widen = True
            reasons.append("degraded_market_regime_band_widened")
        if regime.confidence_value < Decimal("0.5"):
            widen = True
            reasons.append("low_regime_confidence_band_widened")
        if not regime.can_support_full_a_share_conclusion:
            widen = True
            reasons.append("narrow_market_scope_band_widened")
        lower = max(
            Decimal("0"),
            base_lower - (self.policy.degraded_band_widening if widen else Decimal("0")),
        )
        upper = min(
            Decimal("1"),
            base_upper + (self.policy.degraded_band_widening if widen else Decimal("0")),
        )
        return TargetExposureBand(
            status="degraded" if widen else "ready",
            base_lower=_q(base_lower),
            base_upper=_q(base_upper),
            lower=_q(lower),
            upper=_q(upper),
            current=_q(current),
            within_band=(lower <= current <= upper if current is not None else None),
            reasons=sorted(reasons),
        )

    def _policy_metadata(self) -> RiskPolicyMetadata:
        return RiskPolicyMetadata(
            formula_version=self.policy.formula_version,
            guardrail_version=self.policy.guardrail_version,
            thresholds={
                "max_single_weight": self.policy.max_single_weight,
                "max_sector_weight": self.policy.max_sector_weight,
                "max_total_atr_risk_ratio": self.policy.max_total_atr_risk_ratio,
                "max_drawdown": self.policy.max_drawdown,
                "sector_mapping_minimum": self.policy.sector_mapping_minimum,
                "manual_nav_tolerance_cny": self.policy.manual_nav_tolerance,
                "degraded_band_widening": self.policy.degraded_band_widening,
                "atr_period_sessions": Decimal(self.policy.atr_period),
                "market_lookback_sessions": Decimal(self.policy.market_lookback_sessions),
            },
            target_band_version=self.policy.target_band_version,
            target_bands={
                f"{strategic}:{tactical}": bounds
                for (strategic, tactical), bounds in sorted(self.policy.target_bands.items())
            },
        )

    def _empty(self, as_of: date, cutoff: datetime) -> PortfolioRiskResult:
        policy = self._policy_metadata()
        nav = PortfolioNav(
            cash=None,
            manual_nav=None,
            derived_nav=None,
            selected_nav=None,
            valuation_basis="unavailable",
        )
        exposure = PortfolioExposure(
            gross_market_value=None,
            net_market_value=None,
            gross_ratio=None,
            net_ratio=None,
            max_single_weight=None,
        )
        sectors = SectorConcentration(
            status="missing",
            mapping_ratio=None,
            can_publish_complete=False,
            max_sector_weight=None,
            sectors=[],
        )
        atr = AtrBudget(
            status="missing",
            total_atr_risk=None,
            total_atr_risk_ratio=None,
        )
        drawdown = DrawdownMetrics(
            status="missing",
            observation_count=0,
            current_drawdown=None,
            max_drawdown=None,
        )
        guardrails = self._guardrails(
            max_single=None,
            max_sector=None,
            total_atr=None,
            max_drawdown=None,
        )
        target = self._target_band(None, None)
        missing = ["portfolio.daily_snapshot"]
        payload = {
            "formula_version": self.policy.formula_version,
            "as_of": as_of,
            "evidence_cutoff_at": cutoff,
            "computed_at": cutoff,
            "policy": policy,
            "missing": missing,
        }
        return PortfolioRiskResult(
            result_id=_stable_id(payload),
            formula_version=self.policy.formula_version,
            status="empty",
            quality_status="empty",
            as_of=as_of,
            evidence_cutoff_at=cutoff,
            computed_at=cutoff,
            data_as_of=None,
            snapshot=None,
            market_lineage=None,
            classification_lineage=None,
            policy=policy,
            nav=nav,
            exposure=exposure,
            positions=[],
            sector_concentration=sectors,
            atr_budget=atr,
            drawdown=drawdown,
            guardrails=guardrails,
            target_exposure=target,
            market_regime=None,
            missing_inputs=missing,
            quality_issues=[],
        )


def _valid_price_bar(bar: DailyBar) -> bool:
    return _valid_effective_price_bar(bar) and (
        bar.adjust_factor is not None and math.isfinite(bar.adjust_factor) and bar.adjust_factor > 0
    )


def _valid_effective_price_bar(bar: DailyBar) -> bool:
    values = [bar.open, bar.high, bar.low, bar.close, bar.preclose, bar.amount]
    return (
        bar.security_type == "stock"
        and bar.board == "main"
        and bar.quality_status == "ready"
        and not bar.quality_issues
        and bar.is_trading
        and not bar.is_suspended
        and all(math.isfinite(value) for value in values)
        and bar.high >= bar.low
        and bar.close > 0
        and bar.preclose > 0
        and bar.amount >= 0
    )


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _q(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if value == 0:
        return Decimal("0")
    return value.quantize(Decimal("0.000001")).normalize()


def _market_hash(bars: list[DailyBar]) -> str:
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
            "adjust_factor": bar.adjust_factor,
            "is_trading": bar.is_trading,
            "is_suspended": bar.is_suspended,
            "quality_status": bar.quality_status,
            "quality_issues": bar.quality_issues,
            "source_record_id": bar.source_record_id,
            "ingested_at": bar.ingested_at.astimezone(UTC).isoformat(),
        }
        for bar in sorted(bars, key=lambda item: (item.trade_date, item.symbol))
    ]
    encoded = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _market_lineage(bars: list[DailyBar]) -> MarketEvidenceLineage | None:
    if not bars:
        return None
    dates = [bar.trade_date for bar in bars]
    content_hash = _market_hash(bars)
    publication_payload = {
        "source": "baostock",
        "source_version": CANONICAL_SOURCE_VERSION,
        "data_as_of": max(dates),
        "earliest_input_date": min(dates),
        "latest_input_date": max(dates),
        "record_count": len(bars),
        "content_hash": content_hash,
        "publication_visibility_status": "unverifiable",
    }
    digest = _stable_hash(publication_payload)
    return MarketEvidenceLineage(
        publication_id=None,
        logical_content_id=f"market-content-{digest[:24]}",
        **publication_payload,
    )


def _classification_lineage(
    selected: ClassificationReadSnapshot,
) -> ClassificationEvidenceLineage | None:
    generation = selected.generation
    if generation is None:
        return None
    content = {
        "generation": generation.model_dump(mode="json"),
        "security_snapshot_date": selected.security_snapshot_date,
        "sector_snapshot_date": selected.sector_snapshot_date,
        "securities": [
            item.model_dump(mode="json")
            for item in sorted(selected.securities, key=lambda value: value.record_id)
        ],
        "sector_memberships": [
            item.model_dump(mode="json")
            for item in sorted(
                selected.sector_memberships,
                key=lambda value: value.record_id,
            )
        ],
        "taxonomy_id": TAXONOMY_BAOSTOCK_INDUSTRY,
    }
    observed_at = _normalized_observed_at(generation.observed_at)
    if observed_at is None:
        return None
    return ClassificationEvidenceLineage(
        generation_id=generation.generation_id,
        schema_version=generation.schema_version,
        source=generation.source,
        source_version=generation.source_version,
        source_snapshot_date=generation.source_snapshot_date,
        observed_at=observed_at,
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
        security_snapshot_date=selected.security_snapshot_date,
        sector_snapshot_date=selected.sector_snapshot_date,
        content_hash=_stable_hash(content),
    )


def _normalized_observed_at(value: datetime) -> datetime | None:
    if value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def _stable_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _stable_id(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return f"portfolio-risk-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"


def _json_default(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, (date, Decimal)):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")
