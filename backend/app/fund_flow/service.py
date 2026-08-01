import hashlib
import json
import math
from datetime import date, timedelta
from statistics import fmean, pstdev
from zoneinfo import ZoneInfo

from backend.app.fund_flow.models import (
    FundFlowConclusion,
    FundFlowConfidence,
    FundFlowEvidenceResult,
    FundFlowEvidenceSnapshot,
    FundFlowMetrics,
    FundFlowObservedPoint,
    FundFlowSemanticLineage,
    FundFlowSource,
    UnavailableEvidenceLevel,
)
from backend.app.market.calendar import TradingCalendar
from backend.app.market.supplement_ingestion import PINNED_PROVIDER_VERSION
from backend.app.market.supplemental import ReportedFundFlowPoint

FORMULA_VERSION = "fund-flow-evidence-l1-v1"
WINDOW_SESSIONS = 20
_UNAVAILABLE_LEVELS = [
    UnavailableEvidenceLevel(
        evidence_level="L2",
        reason="public_institutional_activity_source_not_configured",
    ),
    UnavailableEvidenceLevel(
        evidence_level="L3",
        reason="price_volume_inference_not_implemented_in_this_slice",
    ),
]


class FundFlowEvidenceService:
    def __init__(self, calendar: TradingCalendar) -> None:
        self.calendar = calendar

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

        if snapshot.source_status in {"not_configured", "empty", "error"}:
            return self._empty_result(snapshot, issues=issues)
        invalid_issue = self._invalid_issue(points, snapshot)
        if invalid_issue is not None:
            issues.add(invalid_issue)
            return self._build_result(
                snapshot,
                availability="error",
                points=[],
                data_as_of=None,
                last_trusted_date=None,
                staleness_sessions=None,
                publication_knowledge="not_applicable",
                source=None,
                metrics=None,
                missing_sessions=[],
                missing_inputs=["valid_single_semantics_l1_history"],
                issues=issues,
                confidence=self._confidence("error"),
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

    @staticmethod
    def _invalid_issue(
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
        expected_source_scope = "market" if snapshot.scope == "market" else "industry"
        if any(
            point.scope != expected_source_scope or point.scope_name != snapshot.scope_id
            for point in points
        ):
            return "mixed_source_or_semantics"
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
        concentration = None
        if all(item is not None for item in components):
            denominator = sum(abs(float(item)) for item in components)
            if denominator:
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
            "metric": "reported_main_net_inflow",
            "date_semantics": "explicit_trade_date",
            "earliest": points[0].trade_date.isoformat(),
            "latest": points[-1].trade_date.isoformat(),
            "record_count": len(points),
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
                earliest_input_date=points[0].trade_date,
                latest_input_date=points[-1].trade_date,
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
            )
            for point in points
        ]
        payload = {
            "evidence_level": "L1",
            "availability": availability,
            "scope": snapshot.scope,
            "scope_id": snapshot.scope_id,
            "as_of": snapshot.as_of.isoformat(),
            "data_as_of": data_as_of.isoformat() if data_as_of else None,
            "last_trusted_date": (last_trusted_date.isoformat() if last_trusted_date else None),
            "staleness_sessions": staleness_sessions,
            "publication_knowledge": publication_knowledge,
            "computed_at": (snapshot.published_at.isoformat() if snapshot.published_at else None),
            "source": source.model_dump(mode="json") if source else None,
            "source_version": snapshot.provider_contract or PINNED_PROVIDER_VERSION,
            "formula_version": FORMULA_VERSION,
            "raw_observed_points": [point.model_dump(mode="json") for point in raw_points],
            "metrics": metrics.model_dump(mode="json") if metrics else None,
            "missing_sessions": [item.isoformat() for item in missing_sessions],
            "missing_inputs": sorted(set(missing_inputs)),
            "quality_issues": sorted(issues),
            "confidence": confidence.model_dump(mode="json"),
            "can_publish_trend": can_publish,
            "conclusion": conclusion.model_dump(mode="json") if conclusion else None,
            "semantic_lineage": [item.model_dump(mode="json") for item in lineage],
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return FundFlowEvidenceResult(
            result_id=f"fund-flow-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}",
            availability=availability,
            scope=snapshot.scope,
            scope_id=snapshot.scope_id,
            as_of=snapshot.as_of,
            data_as_of=data_as_of,
            last_trusted_date=last_trusted_date,
            staleness_sessions=staleness_sessions,
            publication_knowledge=publication_knowledge,
            computed_at=snapshot.published_at,
            source=source,
            source_version=snapshot.provider_contract or PINNED_PROVIDER_VERSION,
            raw_observed_points=raw_points,
            metrics=metrics,
            missing_sessions=missing_sessions,
            missing_inputs=sorted(set(missing_inputs)),
            quality_issues=sorted(issues),
            confidence=confidence,
            can_publish_trend=can_publish,
            conclusion=conclusion,
            semantic_lineage=lineage,
            unavailable_levels=_UNAVAILABLE_LEVELS,
        )
