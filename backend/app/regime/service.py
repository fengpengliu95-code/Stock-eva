import hashlib
import json
from dataclasses import dataclass, field

from backend.app.regime.models import (
    ComponentScore,
    MarketRegimeInput,
    MarketRegimeResult,
    RegimeConfidence,
)


@dataclass(frozen=True)
class RegimePolicy:
    formula_version: str = "market-regime-v1"
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "trend": 0.30,
            "breadth": 0.25,
            "liquidity": 0.15,
            "risk": 0.20,
            "leadership": 0.10,
        }
    )
    thresholds: dict[str, float] = field(
        default_factory=lambda: {
            "strategic_bull_min": 25.0,
            "strategic_bear_max": -25.0,
            "tactical_risk_on_min": 10.0,
            "tactical_risk_off_max": -10.0,
        }
    )


class MarketRegimeService:
    def __init__(self, policy: RegimePolicy | None = None) -> None:
        self.policy = policy or RegimePolicy()

    def evaluate(self, inputs: MarketRegimeInput) -> MarketRegimeResult:
        named_inputs = [
            inputs.trend,
            inputs.breadth,
            inputs.liquidity,
            inputs.risk,
            inputs.leadership,
        ]
        components = [
            ComponentScore(
                **item.model_dump(),
                weight=self.policy.weights[item.name],
                weighted_score=(
                    round(item.score * self.policy.weights[item.name], 6)
                    if item.score is not None
                    else None
                ),
            )
            for item in named_inputs
        ]
        available = [item for item in components if item.score is not None]
        available_weight = sum(item.weight for item in available)
        total_score = (
            round(
                sum(item.score * item.weight for item in available) / available_weight,
                4,
            )
            if available_weight
            else None
        )
        strategic_state = self._strategic_state(total_score)
        tactical_state = self._tactical_state(total_score)
        missing_inputs = sorted(
            {
                missing
                for component in components
                for missing in component.missing_inputs
            }
        )
        issues = sorted(
            {
                *inputs.quality_issues,
                *(
                    issue
                    for component in components
                    for issue in component.quality_issues
                ),
            }
        )
        if missing_inputs:
            issues.append("missing_component_inputs")
        if any(item.quality_status == "degraded" for item in components):
            issues.append("degraded_component_quality")
        issues.append("narrow_scope_not_full_a_share")
        issues.append("market_scope_authoritative_coverage_unavailable")
        if inputs.actual_market_scope.missing_boards:
            issues.append("market_scope_missing_expected_boards")
        if inputs.actual_market_scope.missing_index_series:
            issues.append("market_scope_missing_expected_index_series")
        issues = sorted(set(issues))

        if not available:
            status = "empty"
        elif issues:
            status = "degraded"
        else:
            status = "ready"
        confidence = self._confidence(components)
        supporting = [
            evidence
            for component in components
            for evidence in component.supporting_evidence
        ]
        contrary = [
            evidence
            for component in components
            for evidence in component.contrary_evidence
        ]
        result_id = self._result_id(inputs)
        return MarketRegimeResult(
            result_id=result_id,
            formula_version=self.policy.formula_version,
            as_of=inputs.as_of,
            data_as_of=inputs.data_as_of,
            status=status,
            quality_status=status,
            strategic_state=strategic_state,
            tactical_state=tactical_state,
            total_score=total_score,
            component_scores=components,
            weights=dict(self.policy.weights),
            thresholds=dict(self.policy.thresholds),
            confidence=confidence,
            missing_inputs=missing_inputs,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            quality_issues=issues,
            actual_market_scope=inputs.actual_market_scope,
            source_lineage=inputs.source_lineage,
        )

    def _strategic_state(self, score: float | None) -> str:
        if score is None:
            return "range"
        if score >= self.policy.thresholds["strategic_bull_min"]:
            return "bull"
        if score <= self.policy.thresholds["strategic_bear_max"]:
            return "bear"
        return "range"

    def _tactical_state(self, score: float | None) -> str:
        if score is None:
            return "neutral"
        if score >= self.policy.thresholds["tactical_risk_on_min"]:
            return "risk_on"
        if score <= self.policy.thresholds["tactical_risk_off_max"]:
            return "risk_off"
        return "neutral"

    def _confidence(self, components) -> RegimeConfidence:
        available_weight = sum(
            item.weight for item in components if item.score is not None
        )
        quality_factor = sum(
            item.weight
            * (
                1.0
                if item.quality_status == "ready"
                else 0.75
                if item.quality_status == "degraded"
                else 0.0
            )
            for item in components
        )
        value = 0.0
        level = "high" if value >= 0.9 else "medium" if value >= 0.5 else "low"
        reasons = []
        if available_weight < 1:
            reasons.append("component_inputs_incomplete")
        if quality_factor < 1:
            reasons.append("component_quality_degraded")
        reasons.append("market_scope_coverage_not_audited")
        return RegimeConfidence(value=value, level=level, reasons=reasons)

    def _result_id(self, inputs: MarketRegimeInput) -> str:
        payload = {
            "inputs": inputs.model_dump(mode="json"),
            "formula_version": self.policy.formula_version,
            "weights": self.policy.weights,
            "thresholds": self.policy.thresholds,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return f"regime-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"
