import hashlib
import json
import math
from datetime import date, timedelta
from statistics import fmean, pstdev
from zoneinfo import ZoneInfo

from backend.app.fund_flow.models import (
    FundFlowConclusion,
    FundFlowConfidence,
    FundFlowEvidencePolicy,
    FundFlowEvidenceResult,
    FundFlowEvidenceSnapshot,
    FundFlowMetrics,
    FundFlowObservedPoint,
    FundFlowSemanticLineage,
    FundFlowSource,
    UnavailableEvidenceLevel,
)
from backend.app.market.akshare_supplemental import (
    FUND_FLOW_CONTRACT_BY_SCOPE,
)
from backend.app.market.calendar import TradingCalendar
from backend.app.market.supplement_ingestion import PINNED_PROVIDER_VERSION
from backend.app.market.supplemental import ReportedFundFlowPoint

WINDOW_SESSIONS = 20
DEFAULT_FUND_FLOW_POLICY = FundFlowEvidencePolicy(
    formula_version="fund-flow-evidence-l1-v1",
    unavailable_levels=(
        UnavailableEvidenceLevel(
            evidence_level="L2",
            reason="public_institutional_activity_source_not_configured",
        ),
        UnavailableEvidenceLevel(
            evidence_level="L3",
            reason="price_volume_inference_not_implemented_in_this_slice",
        ),
    ),
)


class FundFlowEvidenceService:
    def __init__(
        self,
        calendar: TradingCalendar,
        policy: FundFlowEvidencePolicy = DEFAULT_FUND_FLOW_POLICY,
    ) -> None:
        self.calendar = calendar
        self.policy = policy

    def evaluate(
        self,
        snapshot: FundFlowEvidenceSnapshot,
    ) -> FundFlowEvidenceResult:
        issues = set(snapshot.quality_issues)
        points = sorted(
            (point for point in snapshot.points if point.trade_date <= snapshot.as_of),
            key=lambda point: (point.trade_date, point.observed_at),
        )
        discarded_future = snapshot.future_point_count + sum(
            point.trade_date > snapshot.as_of for point in snapshot.points
        )
        if discarded_future:
            issues.add("future_source_points_discarded")
        analysis_session_status = self.calendar.session_status(snapshot.as_of)
        if analysis_session_status == "closed":
            issues.add("analysis_as_of_closed_session")
        elif analysis_session_status == "unknown":
            issues.add("trading_calendar_unverifiable")

        if snapshot.source_status in {"not_configured", "empty", "error"}:
            return self._empty_result(snapshot, issues=issues)
        invalid_issue = self._invalid_issue(points, snapshot)
        if invalid_issue is not None:
            issues.add(invalid_issue)
            historical_unverifiable = invalid_issue == "historical_observation_not_visible_as_of"
            if historical_unverifiable:
                issues.add("historical_publication_knowledge_unverifiable")
            return self._build_result(
                snapshot,
                availability=("insufficient_history" if historical_unverifiable else "error"),
                points=[],
                data_as_of=None,
                last_trusted_date=None,
                staleness_sessions=None,
                publication_knowledge=(
                    "unverifiable" if historical_unverifiable else "not_applicable"
                ),
                source=None,
                metrics=None,
                missing_sessions=[],
                missing_inputs=[
                    (
                        "historically_visible_observation"
                        if historical_unverifiable
                        else "valid_single_semantics_l1_history"
                    )
                ],
                issues=issues,
                confidence=self._confidence(
                    "insufficient_history" if historical_unverifiable else "error",
                    historical_unverifiable=historical_unverifiable,
                ),
                can_publish=False,
                conclusion=None,
                lineage=[],
            )
        if not points:
            issues.add("fund_flow_scope_has_no_points_at_or_before_as_of")
            return self._empty_result(
                snapshot.model_copy(update={"source_status": "empty"}),
                issues=issues,
            )

        source = FundFlowSource(
            source=points[0].source,
            upstream=points[0].upstream,
            endpoint=points[0].source_endpoint,
        )
        data_as_of = points[-1].trade_date
        publication_session_status = (
            self.calendar.session_status(snapshot.publication_as_of)
            if snapshot.publication_as_of is not None
            else "unknown"
        )
        if publication_session_status == "closed":
            issues.add("publication_as_of_closed_session")
        elif publication_session_status == "unknown":
            issues.add("trading_calendar_unverifiable")
        expected, calendar_issue = self._expected_sessions(data_as_of)
        if calendar_issue:
            issues.add(calendar_issue)
        present_dates = {point.trade_date for point in points}
        missing_sessions = (
            sorted(session for session in expected if session not in present_dates)
            if expected
            else []
        )
        if missing_sessions:
            issues.add("non_contiguous_trading_sessions")
        staleness_sessions, staleness_issue = self._staleness_sessions(
            data_as_of,
            snapshot.as_of,
        )
        if staleness_issue:
            issues.add(staleness_issue)
        published_on = None
        if snapshot.published_at is not None and snapshot.published_at.utcoffset() is not None:
            published_on = snapshot.published_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        else:
            issues.add("publication_timestamp_unavailable")
        publication_knowledge = (
            "unverifiable"
            if snapshot.publication_as_of is None
            or snapshot.as_of < snapshot.publication_as_of
            or published_on is None
            or published_on > snapshot.as_of
            or analysis_session_status != "open"
            or publication_session_status != "open"
            else "verified"
        )
        if publication_knowledge == "unverifiable":
            issues.add("historical_publication_knowledge_unverifiable")
        if snapshot.publication_as_of is not None and data_as_of > snapshot.publication_as_of:
            issues.add("source_data_exceeds_publication_as_of")
        if snapshot.object_sha256 is None:
            issues.add("semantic_lineage_unavailable")

        metrics = self._metrics(points, expected, present_dates)
        missing_inputs = [
            "member_level_evidence_for_diffusion",
            "price_series_for_divergence",
        ]
        concentration_available = (
            metrics.concentration_component_count_1d == 4
            and metrics.concentration_denominator_abs_1d not in {None, 0}
        )
        if metrics.concentration_component_count_1d < 4:
            missing_inputs.append("complete_order_size_components_for_concentration")
            issues.add(
                "concentration_components_incomplete_"
                f"{metrics.concentration_component_count_1d}_of_4"
            )
        elif metrics.concentration_denominator_abs_1d == 0:
            missing_inputs.append("nonzero_order_size_denominator_for_concentration")
            issues.add("concentration_denominator_is_zero")
        history_ready = (
            len(expected) == WINDOW_SESSIONS
            and not missing_sessions
            and metrics.net_inflow_20d is not None
        )
        can_publish = (
            history_ready
            and staleness_sessions == 0
            and publication_knowledge == "verified"
            and snapshot.source_status == "ready"
            and snapshot.object_sha256 is not None
            and "source_data_exceeds_publication_as_of" not in issues
            and "publication_timestamp_unavailable" not in issues
            and not discarded_future
            and analysis_session_status == "open"
            and publication_session_status == "open"
        )
        if can_publish:
            availability = "ready"
            conclusion = self._conclusion(metrics)
        elif history_ready and staleness_sessions and staleness_sessions > 0:
            availability = "stale"
            conclusion = None
            issues.add("last_trusted_point_is_stale")
        else:
            availability = "insufficient_history"
            conclusion = None
            if not history_ready:
                issues.add("twenty_contiguous_sessions_required")
        lineage = self._lineage(snapshot, points, source)
        return self._build_result(
            snapshot,
            availability=availability,
            points=points,
            data_as_of=data_as_of,
            last_trusted_date=data_as_of,
            staleness_sessions=staleness_sessions,
            publication_knowledge=publication_knowledge,
            source=source,
            metrics=metrics,
            missing_sessions=missing_sessions,
            missing_inputs=missing_inputs,
            issues=issues,
            confidence=self._confidence(
                availability,
                historical_unverifiable=(publication_knowledge == "unverifiable"),
                concentration_available=concentration_available,
            ),
            can_publish=can_publish,
            conclusion=conclusion,
            lineage=lineage,
        )

    def _empty_result(
        self,
        snapshot: FundFlowEvidenceSnapshot,
        *,
        issues: set[str],
    ) -> FundFlowEvidenceResult:
        availability = snapshot.source_status
        if availability not in {"not_configured", "empty", "error"}:
            availability = "empty"
        return self._build_result(
            snapshot,
            availability=availability,
            points=[],
            data_as_of=None,
            last_trusted_date=None,
            staleness_sessions=None,
            publication_knowledge="not_applicable",
            source=None,
            metrics=None,
            missing_sessions=[],
            missing_inputs=["l1_upstream_reported_fund_flow"],
            issues=issues,
            confidence=self._confidence(availability),
            can_publish=False,
            conclusion=None,
            lineage=[],
        )

    def _invalid_issue(
        self,
        points: list[ReportedFundFlowPoint],
        snapshot: FundFlowEvidenceSnapshot,
    ) -> str | None:
        if any(
            not math.isfinite(value)
            for point in points
            for value in (
                point.reported_main_net_inflow,
                point.reported_main_net_inflow_ratio,
                point.reported_super_large_net_inflow,
                point.reported_large_net_inflow,
                point.reported_medium_net_inflow,
                point.reported_small_net_inflow,
            )
            if value is not None
        ):
            return "non_finite_reported_value"
        identities = [(point.trade_date, point.scope, point.scope_name) for point in points]
        if len(identities) != len(set(identities)):
            return "duplicate_source_points"
        semantic_keys = {
            (
                point.source,
                point.upstream,
                point.source_endpoint,
                point.scope,
                point.scope_name,
            )
            for point in points
        }
        if len(semantic_keys) > 1:
            return "mixed_source_or_semantics"
        contract = FUND_FLOW_CONTRACT_BY_SCOPE[snapshot.scope]
        expected_source_scope = str(contract["source_scope"])
        if (
            snapshot.provider_contract != PINNED_PROVIDER_VERSION
            or self.policy.units.model_dump(mode="json") != contract["units"]
            or any(
                point.source != contract["source"]
                or point.upstream != contract["upstream"]
                or point.source_endpoint != contract["endpoint"]
                for point in points
            )
        ):
            return "provider_contract_mismatch"
        if any(
            point.scope != expected_source_scope
            or point.scope_name != snapshot.upstream_scope_identity
            for point in points
        ):
            return "mixed_source_or_semantics"
        session_statuses = {self.calendar.session_status(point.trade_date) for point in points}
        if "unknown" in session_statuses:
            return "trading_calendar_unverifiable"
        if "closed" in session_statuses:
            return "source_point_on_closed_session"
        for point in points:
            if point.observed_at.utcoffset() is None:
                return "source_observation_timestamp_unverifiable"
            if (
                snapshot.published_at is not None
                and snapshot.published_at.utcoffset() is not None
                and point.observed_at > snapshot.published_at
            ):
                return "source_observed_after_publication"
            if point.observed_at.astimezone(ZoneInfo("Asia/Shanghai")).date() > snapshot.as_of:
                return "historical_observation_not_visible_as_of"
        return None

    def _expected_sessions(self, end: date) -> tuple[list[date], str | None]:
        if self.calendar.session_status(end) != "open":
            return [], "trading_calendar_unverifiable"
        sessions = [end]
        while len(sessions) < WINDOW_SESSIONS:
            previous = self.calendar.previous_session(sessions[0])
            if previous is None:
                return [], "trading_calendar_unverifiable"
            sessions.insert(0, previous)
        return sessions, None

    def _staleness_sessions(
        self,
        data_as_of: date,
        as_of: date,
    ) -> tuple[int | None, str | None]:
        if data_as_of > as_of:
            return None, "future_source_points_discarded"
        stale = 0
        current = data_as_of + timedelta(days=1)
        while current <= as_of:
            status = self.calendar.session_status(current)
            if status == "unknown":
                return None, "trading_calendar_unverifiable"
            if status == "open":
                stale += 1
            current += timedelta(days=1)
        return stale, None

    @staticmethod
    def _metrics(
        points: list[ReportedFundFlowPoint],
        expected: list[date],
        present_dates: set[date],
    ) -> FundFlowMetrics:
        by_date = {point.trade_date: point for point in points}

        def values(window: int) -> list[float] | None:
            if len(expected) < window:
                return None
            dates = expected[-window:]
            if any(item not in present_dates for item in dates):
                return None
            return [by_date[item].reported_main_net_inflow for item in dates]

        one = values(1)
        five = values(5)
        twenty = values(20)
        latest = points[-1]
        components = [
            latest.reported_super_large_net_inflow,
            latest.reported_large_net_inflow,
            latest.reported_medium_net_inflow,
            latest.reported_small_net_inflow,
        ]
        available_components = [float(item) for item in components if item is not None]
        component_count = len(available_components)
        denominator = (
            sum(abs(item) for item in available_components) if available_components else None
        )
        concentration = None
        if component_count == 4 and denominator:
            concentration = round(
                (
                    abs(float(latest.reported_super_large_net_inflow))
                    + abs(float(latest.reported_large_net_inflow))
                )
                / denominator,
                6,
            )
        z_score = None
        if twenty is not None:
            deviation = pstdev(twenty)
            z_score = round((twenty[-1] - fmean(twenty)) / deviation, 6) if deviation else 0.0
        return FundFlowMetrics(
            net_inflow_1d=round(sum(one), 6) if one is not None else None,
            net_inflow_5d=round(sum(five), 6) if five is not None else None,
            net_inflow_20d=round(sum(twenty), 6) if twenty is not None else None,
            positive_ratio_20d=(
                round(sum(value > 0 for value in twenty) / len(twenty), 6)
                if twenty is not None
                else None
            ),
            continuity_ratio_20d=round(
                (sum(session in present_dates for session in expected) / WINDOW_SESSIONS)
                if expected
                else 0.0,
                6,
            ),
            z_score_20d=z_score,
            concentration_1d=concentration,
            concentration_component_count_1d=component_count,
            concentration_denominator_abs_1d=(
                round(denominator, 6) if denominator is not None else None
            ),
            price_divergence_20d=None,
            sector_diffusion_20d=None,
        )

    @staticmethod
    def _conclusion(metrics: FundFlowMetrics) -> FundFlowConclusion:
        assert metrics.net_inflow_20d is not None
        assert metrics.positive_ratio_20d is not None
        if metrics.net_inflow_20d > 0 and metrics.positive_ratio_20d >= 0.65:
            direction = "persistent_reported_inflow"
        elif metrics.net_inflow_20d < 0 and metrics.positive_ratio_20d <= 0.35:
            direction = "persistent_reported_outflow"
        else:
            direction = "mixed_reported_flow"
        return FundFlowConclusion(
            direction=direction,
            statement=(
                "The upstream data source reports an order-size fund-flow proxy; "
                "this is not an exchange-certified institutional identity."
            ),
        )

    @staticmethod
    def _confidence(
        availability: str,
        *,
        historical_unverifiable: bool = False,
        concentration_available: bool = True,
    ) -> FundFlowConfidence:
        if availability == "ready":
            value = 0.75
            reasons = [
                "single_upstream_l1_proxy",
                "price_divergence_unavailable",
                "institutional_identity_not_verified",
            ]
        elif availability in {"insufficient_history", "stale"}:
            value = 0.2 if historical_unverifiable else 0.3
            reasons = [availability, "trend_publication_blocked"]
        else:
            value = 0.0
            reasons = [f"source_{availability}"]
        if not concentration_available and availability not in {
            "not_configured",
            "empty",
            "error",
        }:
            value = max(0.0, value - 0.1)
            reasons.append("concentration_unavailable")
        level = "high" if value >= 0.9 else "medium" if value >= 0.5 else "low"
        return FundFlowConfidence(value=value, level=level, reasons=reasons)

    @staticmethod
    def _lineage(
        snapshot: FundFlowEvidenceSnapshot,
        points: list[ReportedFundFlowPoint],
        source: FundFlowSource,
    ) -> list[FundFlowSemanticLineage]:
        if not snapshot.object_sha256 or not points:
            return []
        payload = {
            "source_version": snapshot.provider_contract or PINNED_PROVIDER_VERSION,
            "object_sha256": snapshot.object_sha256,
            "source": source.model_dump(mode="json"),
            "scope": snapshot.scope,
            "scope_id": snapshot.scope_id,
            "analysis_universe": snapshot.scope_id,
            "upstream_scope_identity": snapshot.upstream_scope_identity,
            "metric": "reported_main_net_inflow",
            "date_semantics": "explicit_trade_date",
            "earliest": points[0].trade_date.isoformat(),
            "latest": points[-1].trade_date.isoformat(),
            "record_count": len(points),
            "earliest_observed_at": min(point.observed_at for point in points).isoformat(),
            "latest_observed_at": max(point.observed_at for point in points).isoformat(),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return [
            FundFlowSemanticLineage(
                lineage_id=(f"fund-lineage-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"),
                source_version=snapshot.provider_contract or PINNED_PROVIDER_VERSION,
                object_sha256=snapshot.object_sha256,
                source=source,
                scope=snapshot.scope,
                scope_id=snapshot.scope_id,
                analysis_universe=snapshot.scope_id,
                upstream_scope_identity=snapshot.upstream_scope_identity,
                earliest_input_date=points[0].trade_date,
                latest_input_date=points[-1].trade_date,
                earliest_observed_at=min(point.observed_at for point in points),
                latest_observed_at=max(point.observed_at for point in points),
                record_count=len(points),
            )
        ]

    def _build_result(
        self,
        snapshot: FundFlowEvidenceSnapshot,
        *,
        availability: str,
        points: list[ReportedFundFlowPoint],
        data_as_of: date | None,
        last_trusted_date: date | None,
        staleness_sessions: int | None,
        publication_knowledge: str,
        source: FundFlowSource | None,
        metrics: FundFlowMetrics | None,
        missing_sessions: list[date],
        missing_inputs: list[str],
        issues: set[str],
        confidence: FundFlowConfidence,
        can_publish: bool,
        conclusion: FundFlowConclusion | None,
        lineage: list[FundFlowSemanticLineage],
    ) -> FundFlowEvidenceResult:
        raw_points = [
            FundFlowObservedPoint(
                trade_date=point.trade_date,
                observed_at=point.observed_at,
                reported_main_net_inflow=point.reported_main_net_inflow,
                reported_main_net_inflow_ratio=point.reported_main_net_inflow_ratio,
                reported_super_large_net_inflow=point.reported_super_large_net_inflow,
                reported_large_net_inflow=point.reported_large_net_inflow,
                reported_medium_net_inflow=point.reported_medium_net_inflow,
                reported_small_net_inflow=point.reported_small_net_inflow,
                source=point.source,
                upstream=point.upstream,
                endpoint=point.source_endpoint,
                scope=snapshot.scope,
                scope_id=snapshot.scope_id,
                analysis_universe=snapshot.scope_id,
                upstream_scope_identity=point.scope_name,
            )
            for point in points
        ]
        candidate = FundFlowEvidenceResult(
            result_id="fund-flow-000000000000000000000000",
            evidence_basis=self.policy.evidence_basis,
            interpretation=self.policy.interpretation,
            availability=availability,
            scope=snapshot.scope,
            scope_id=snapshot.scope_id,
            analysis_universe=snapshot.scope_id,
            upstream_scope_identity=(snapshot.upstream_scope_identity if points else None),
            as_of=snapshot.as_of,
            data_as_of=data_as_of,
            last_trusted_date=last_trusted_date,
            staleness_sessions=staleness_sessions,
            publication_knowledge=publication_knowledge,
            computed_at=snapshot.published_at,
            source=source,
            source_version=snapshot.provider_contract or PINNED_PROVIDER_VERSION,
            formula_version=self.policy.formula_version,
            date_semantics=self.policy.date_semantics,
            units=self.policy.units,
            raw_observed_points=raw_points,
            metrics=metrics,
            missing_sessions=missing_sessions,
            missing_inputs=sorted(set(missing_inputs)),
            quality_issues=sorted(issues),
            confidence=confidence,
            can_publish_trend=can_publish,
            conclusion=conclusion,
            semantic_lineage=lineage,
            unavailable_levels=list(self.policy.unavailable_levels),
        )
        encoded = json.dumps(
            candidate.model_dump(mode="json", exclude={"result_id", "computed_at"}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return candidate.model_copy(
            update={"result_id": (f"fund-flow-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}")}
        )
