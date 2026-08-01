import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from backend.app.api import fund_flow as fund_flow_api
from backend.app.config import Settings, get_settings
from backend.app.fund_flow.models import (
    FundFlowEvidencePolicy,
    FundFlowEvidenceSnapshot,
    UnavailableEvidenceLevel,
)
from backend.app.fund_flow.service import (
    DEFAULT_FUND_FLOW_POLICY,
    FundFlowEvidenceService,
)
from backend.app.fund_flow.store import FundFlowEvidenceStore
from backend.app.main import app
from backend.app.market.akshare_supplemental import AKShareSupplementalProvider
from backend.app.market.calendar import TradingCalendar, get_trading_calendar
from backend.app.market.supplement_ingestion import (
    _COLUMNS,
    PINNED_PROVIDER_VERSION,
    SupplementalDatasetInitializer,
    SupplementalIngestionRequest,
    SupplementalIngestionService,
    SupplementalStore,
    _fund_flow_from_row,
)
from backend.app.market.supplemental import ReportedFundFlowPoint

AS_OF = date(2026, 7, 29)
OBSERVED_AT = datetime(2026, 7, 29, 10, 30, tzinfo=UTC)
ENDPOINT = "stock_market_fund_flow"
MARKET_ANALYSIS_UNIVERSE = "sh_sz_market"
MARKET_UPSTREAM_SCOPE = "沪深市场"


def _sessions(calendar: TradingCalendar, *, end: date, count: int) -> list[date]:
    sessions = [end]
    while len(sessions) < count:
        previous = calendar.previous_session(sessions[0])
        assert previous is not None
        sessions.insert(0, previous)
    return sessions


def _points(
    sessions: list[date],
    *,
    scope: str = "market",
    scope_name: str = MARKET_UPSTREAM_SCOPE,
    endpoint: str = ENDPOINT,
    source: str = "akshare",
    upstream: str = "eastmoney",
) -> list[ReportedFundFlowPoint]:
    return [
        ReportedFundFlowPoint(
            trade_date=session,
            scope=scope,
            scope_name=scope_name,
            reported_main_net_inflow=float((index + 1) * 100_000_000),
            reported_main_net_inflow_ratio=float(index + 1) / 10,
            reported_super_large_net_inflow=float((index + 1) * 60_000_000),
            reported_large_net_inflow=float((index + 1) * 40_000_000),
            reported_medium_net_inflow=float(-(index + 1) * 20_000_000),
            reported_small_net_inflow=float(-(index + 1) * 80_000_000),
            source=source,
            upstream=upstream,
            source_endpoint=endpoint,
            observed_at=OBSERVED_AT,
        )
        for index, session in enumerate(sessions)
    ]


def _snapshot(
    points: list[ReportedFundFlowPoint],
    *,
    as_of: date = AS_OF,
    publication_as_of: date | None = AS_OF,
    published_at: datetime | None = OBSERVED_AT,
    source_status: str = "ready",
    future_point_count: int = 0,
    quality_issues: list[str] | None = None,
) -> FundFlowEvidenceSnapshot:
    return FundFlowEvidenceSnapshot(
        source_status=source_status,
        as_of=as_of,
        scope="market",
        scope_id=MARKET_ANALYSIS_UNIVERSE,
        upstream_scope_identity=MARKET_UPSTREAM_SCOPE,
        publication_as_of=publication_as_of,
        published_at=published_at,
        provider_contract="akshare-1.18.78-contract",
        object_sha256="a" * 64 if points else None,
        points=points,
        future_point_count=future_point_count,
        quality_issues=quality_issues or [],
    )


def test_exactly_19_is_insufficient_and_exactly_20_publishes_all_windows() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    service = FundFlowEvidenceService(calendar)

    nineteen = service.evaluate(_snapshot(_points(sessions[-19:])))
    twenty = service.evaluate(_snapshot(_points(sessions)))

    assert nineteen.availability == "insufficient_history"
    assert nineteen.can_publish_trend is False
    assert nineteen.conclusion is None
    assert nineteen.metrics is not None
    assert nineteen.metrics.net_inflow_1d is not None
    assert nineteen.metrics.net_inflow_5d is not None
    assert nineteen.metrics.net_inflow_20d is None
    assert twenty.availability == "ready"
    assert twenty.can_publish_trend is True
    assert twenty.conclusion is not None
    assert twenty.metrics is not None
    assert twenty.metrics.net_inflow_20d == sum(
        point.reported_main_net_inflow for point in _points(sessions)
    )
    assert twenty.metrics.positive_ratio_20d == 1
    assert twenty.metrics.continuity_ratio_20d == 1
    assert twenty.metrics.sector_diffusion_20d is None
    assert "member_level_evidence_for_diffusion" in twenty.missing_inputs


def test_complete_history_without_object_lineage_never_publishes_a_trend() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    snapshot = _snapshot(_points(sessions)).model_copy(update={"object_sha256": None})

    result = FundFlowEvidenceService(calendar).evaluate(snapshot)

    assert result.availability == "insufficient_history"
    assert result.semantic_lineage == []
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert "semantic_lineage_unavailable" in result.quality_issues


def test_missing_middle_session_fails_closed_even_with_twenty_points() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=21)
    missing = sessions[-8]
    observed = [session for session in sessions if session != missing]

    result = FundFlowEvidenceService(calendar).evaluate(_snapshot(_points(observed)))

    assert len(observed) == 20
    assert result.availability == "insufficient_history"
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert missing in result.missing_sessions
    assert "non_contiguous_trading_sessions" in result.quality_issues


@pytest.mark.parametrize("closed_date", [date(2026, 7, 25), date(2026, 5, 1)])
def test_closed_weekend_or_holiday_source_point_blocks_publication(
    closed_date: date,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    assert calendar.session_status(closed_date) == "closed"

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(_points([*sessions, closed_date]))
    )

    assert result.availability == "error"
    assert result.can_publish_trend is False
    assert result.raw_observed_points == []
    assert result.semantic_lineage == []
    assert "source_point_on_closed_session" in result.quality_issues


def test_unknown_source_session_blocks_publication_as_calendar_unverifiable() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    unknown_date = date(1900, 1, 2)
    assert calendar.session_status(unknown_date) == "unknown"

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(_points([*sessions, unknown_date]))
    )

    assert result.availability == "error"
    assert result.can_publish_trend is False
    assert result.raw_observed_points == []
    assert result.semantic_lineage == []
    assert "trading_calendar_unverifiable" in result.quality_issues


@pytest.mark.parametrize(
    ("mutation", "issue"),
    [
        ("duplicate", "duplicate_source_points"),
        ("endpoint", "mixed_source_or_semantics"),
        ("source", "mixed_source_or_semantics"),
        ("upstream", "mixed_source_or_semantics"),
        ("non_finite", "non_finite_reported_value"),
    ],
)
def test_invalid_or_mixed_observations_are_never_merged(
    mutation: str,
    issue: str,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    points = _points(sessions)
    if mutation == "duplicate":
        points.append(points[-1].model_copy())
    elif mutation == "endpoint":
        points[-1] = points[-1].model_copy(update={"source_endpoint": "changed_endpoint"})
    elif mutation == "source":
        points[-1] = points[-1].model_copy(update={"source": "other_source"})
    elif mutation == "upstream":
        points[-1] = points[-1].model_copy(update={"upstream": "other_upstream"})
    else:
        points[-1] = points[-1].model_copy(update={"reported_main_net_inflow": float("nan")})

    result = FundFlowEvidenceService(calendar).evaluate(_snapshot(points))

    assert result.availability == "error"
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert issue in result.quality_issues


def test_published_row_cannot_mask_a_different_upstream_as_eastmoney() -> None:
    row = dict.fromkeys(_COLUMNS)
    row.update(
        {
            "trade_date": AS_OF,
            "scope": "market",
            "scope_name": MARKET_UPSTREAM_SCOPE,
            "reported_main_net_inflow": 1.0,
            "source": "akshare",
            "upstream": "different_upstream",
            "endpoint": ENDPOINT,
            "observed_at": OBSERVED_AT,
            "quality_status": "ready",
        }
    )

    with pytest.raises(ValueError, match="upstream"):
        _fund_flow_from_row(row)


def test_stale_last_trusted_point_is_exposed_but_never_republished() -> None:
    calendar = get_trading_calendar()
    previous = calendar.previous_session(AS_OF)
    assert previous is not None
    sessions = _sessions(calendar, end=previous, count=20)

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(
            _points(sessions),
            publication_as_of=previous,
        )
    )

    assert result.availability == "stale"
    assert result.data_as_of == previous
    assert result.last_trusted_date == previous
    assert result.staleness_sessions == 1
    assert result.can_publish_trend is False
    assert result.conclusion is None


def test_historical_as_of_with_only_later_publication_is_unverifiable() -> None:
    calendar = get_trading_calendar()
    historical_as_of = calendar.previous_session(AS_OF)
    assert historical_as_of is not None
    sessions = _sessions(calendar, end=historical_as_of, count=20)

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(
            _points(sessions),
            as_of=historical_as_of,
            publication_as_of=AS_OF,
        )
    )

    assert result.availability == "insufficient_history"
    assert result.publication_knowledge == "unverifiable"
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert "historical_publication_knowledge_unverifiable" in result.quality_issues


def test_later_manifest_publication_cannot_retroactively_verify_history() -> None:
    calendar = get_trading_calendar()
    historical_as_of = calendar.previous_session(AS_OF)
    assert historical_as_of is not None
    sessions = _sessions(calendar, end=historical_as_of, count=20)

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(
            _points(sessions),
            as_of=historical_as_of,
            publication_as_of=historical_as_of,
            published_at=datetime(2026, 7, 29, 18, 30, tzinfo=UTC),
        )
    )

    assert result.publication_knowledge == "unverifiable"
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert "historical_publication_knowledge_unverifiable" in result.quality_issues


def test_partial_snapshot_preserves_last_trusted_raw_evidence_without_false_gap() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)

    result = FundFlowEvidenceService(calendar).evaluate(
        _snapshot(
            _points(sessions),
            source_status="partial",
            quality_issues=["supplemental_snapshot_partial"],
        )
    )

    assert result.availability == "insufficient_history"
    assert result.last_trusted_date == AS_OF
    assert len(result.raw_observed_points) == 20
    assert result.can_publish_trend is False
    assert result.conclusion is None
    assert "supplemental_snapshot_partial" in result.quality_issues
    assert "twenty_contiguous_sessions_required" not in result.quality_issues


def test_future_points_are_excluded_from_values_and_stable_result_id() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    future = date(2026, 7, 30)
    clean = _snapshot(_points(sessions))
    with_future = _snapshot(_points([*sessions, future]), future_point_count=1)
    service = FundFlowEvidenceService(calendar)

    clean_result = service.evaluate(clean)
    future_result = service.evaluate(with_future)
    repeated = service.evaluate(clean)

    assert future not in [point.trade_date for point in future_result.raw_observed_points]
    assert future_result.data_as_of == AS_OF
    assert "future_source_points_discarded" in future_result.quality_issues
    assert clean_result.metrics == future_result.metrics
    assert clean_result.result_id == repeated.result_id


def test_formula_and_unavailable_semantics_are_reflected_in_response_and_result_id() -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    snapshot = _snapshot(_points(sessions))
    baseline = FundFlowEvidenceService(calendar).evaluate(snapshot)

    v2_policy = FundFlowEvidencePolicy(
        **DEFAULT_FUND_FLOW_POLICY.model_dump(exclude={"formula_version"}),
        formula_version="fund-flow-evidence-l1-v2",
    )
    formula_changed = FundFlowEvidenceService(calendar, v2_policy).evaluate(snapshot)

    assert formula_changed.formula_version == "fund-flow-evidence-l1-v2"
    assert formula_changed.result_id != baseline.result_id

    changed_unavailable_policy = FundFlowEvidencePolicy(
        **v2_policy.model_dump(exclude={"unavailable_levels"}),
        unavailable_levels=(
            UnavailableEvidenceLevel(
                evidence_level="L2",
                reason="different_l2_availability_semantics",
            ),
            v2_policy.unavailable_levels[1],
        ),
    )
    unavailable_changed = FundFlowEvidenceService(
        calendar,
        changed_unavailable_policy,
    ).evaluate(snapshot)

    assert unavailable_changed.unavailable_levels[0].reason == (
        "different_l2_availability_semantics"
    )
    assert unavailable_changed.result_id != formula_changed.result_id


class HistoryProvider:
    def __init__(self, points: list[ReportedFundFlowPoint]) -> None:
        self.points = points

    def market_fund_flow_history(self, *, through_date: date):
        return [point for point in self.points if point.trade_date <= through_date]

    def sector_fund_flow_history(self, sector_name: str, *, through_date: date):
        return [
            point
            for point in self.points
            if point.scope_name == sector_name and point.trade_date <= through_date
        ]


class RealContractMarketClient:
    def __init__(self, sessions: list[date]) -> None:
        self.sessions = sessions

    def stock_market_fund_flow(self):
        return [
            {
                "日期": session,
                "主力净流入-净额": float((index + 1) * 100_000_000),
                "主力净流入-净占比": float(index + 1) / 10,
            }
            for index, session in enumerate(self.sessions)
        ]


def _settings(tmp_path: Path, *, enabled: bool = True) -> Settings:
    return Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        supplemental_data_dir=tmp_path / "supplemental",
        akshare_supplemental_enabled=enabled,
    )


def _set_synthetic_publication_time(settings: Settings, published_on: date) -> None:
    manifest_path = settings.supplemental_data_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["published_at"] = datetime(
        published_on.year,
        published_on.month,
        published_on.day,
        10,
        30,
        tzinfo=UTC,
    ).isoformat()
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _publish_market_history(
    tmp_path: Path,
    points: list[ReportedFundFlowPoint],
    *,
    through_date: date,
) -> Settings:
    settings = _settings(tmp_path)
    SupplementalDatasetInitializer(
        settings.supplemental_data_dir,
        require_smb=False,
    ).initialize()
    store = SupplementalStore(
        settings.supplemental_data_dir,
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental.lock",
    )
    SupplementalIngestionService(store, HistoryProvider(points)).execute(
        SupplementalIngestionRequest(
            through_date=through_date,
            include_market_flow=True,
            canary=True,
        )
    )
    _set_synthetic_publication_time(settings, through_date)
    return settings


def _publish_sector_history(
    tmp_path: Path,
    points: list[ReportedFundFlowPoint],
    *,
    through_date: date,
    sector_name: str,
) -> Settings:
    settings = _settings(tmp_path)
    SupplementalDatasetInitializer(
        settings.supplemental_data_dir,
        require_smb=False,
    ).initialize()
    store = SupplementalStore(
        settings.supplemental_data_dir,
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental.lock",
    )
    SupplementalIngestionService(store, HistoryProvider(points)).execute(
        SupplementalIngestionRequest(
            through_date=through_date,
            sector_names=[sector_name],
            canary=True,
        )
    )
    _set_synthetic_publication_time(settings, through_date)
    return settings


def _publish_real_contract_market_history(
    tmp_path: Path,
    sessions: list[date],
) -> Settings:
    settings = _settings(tmp_path)
    SupplementalDatasetInitializer(
        settings.supplemental_data_dir,
        require_smb=False,
    ).initialize()
    store = SupplementalStore(
        settings.supplemental_data_dir,
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental.lock",
    )
    provider = AKShareSupplementalProvider(
        client=RealContractMarketClient(sessions),
        clock=lambda: OBSERVED_AT,
    )
    source_points = provider.market_fund_flow_history(through_date=AS_OF)
    assert {point.scope_name for point in source_points} == {MARKET_UPSTREAM_SCOPE}
    SupplementalIngestionService(store, provider).execute(
        SupplementalIngestionRequest(
            through_date=AS_OF,
            include_market_flow=True,
            canary=True,
        )
    )
    _set_synthetic_publication_time(settings, AS_OF)
    return settings


@pytest.mark.anyio
async def test_api_is_read_only_and_returns_structured_not_configured_empty_and_error(
    tmp_path: Path,
) -> None:
    missing_root = tmp_path / "missing" / "supplemental"
    settings = _settings(tmp_path, enabled=False).model_copy(
        update={"supplemental_data_dir": missing_root}
    )

    async def get_payload(active: Settings) -> dict:
        app.dependency_overrides[get_settings] = lambda: active
        app.dependency_overrides[fund_flow_api.get_fund_flow_today] = lambda: AS_OF
        transport = httpx.ASGITransport(app=app)
        try:
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/api/v1/analysis/fund-flow-evidence",
                    params={"as_of": AS_OF.isoformat(), "scope": "market"},
                )
        finally:
            app.dependency_overrides.clear()
        assert response.status_code == 200
        return response.json()

    not_configured = await get_payload(settings)
    assert not_configured["availability"] == "not_configured"
    assert missing_root.exists() is False

    enabled = settings.model_copy(update={"akshare_supplemental_enabled": True})
    empty = await get_payload(enabled)
    assert empty["availability"] == "empty"
    assert missing_root.exists() is False

    missing_root.mkdir(parents=True)
    (missing_root / "manifest.json").write_text("{broken", encoding="utf-8")
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    error = await get_payload(enabled)
    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert error["availability"] == "error"
    assert error["quality_issues"] == ["supplemental_published_snapshot_unavailable"]
    assert after == before


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_contract", [PINNED_PROVIDER_VERSION]),
        ("provider_contract", "unsupported-provider-contract"),
        ("source", "different-source"),
    ],
)
def test_manifest_semantic_metadata_corruption_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    settings = _publish_market_history(
        tmp_path,
        _points(sessions),
        through_date=AS_OF,
    )
    manifest_path = settings.supplemental_data_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0][field] = value
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    snapshot = FundFlowEvidenceStore.from_settings(settings).read(
        as_of=AS_OF,
        scope="market",
        scope_id=MARKET_ANALYSIS_UNIVERSE,
    )
    result = FundFlowEvidenceService(calendar).evaluate(snapshot)

    assert snapshot.source_status == "error"
    assert result.availability == "error"
    assert result.can_publish_trend is False
    assert result.source_version == PINNED_PROVIDER_VERSION
    assert result.raw_observed_points == []
    assert result.quality_issues == ["supplemental_published_snapshot_unavailable"]


@pytest.mark.anyio
async def test_api_published_history_preserves_semantics_and_never_masquerades_amount(
    tmp_path: Path,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    settings = _publish_market_history(
        tmp_path,
        _points(sessions),
        through_date=AS_OF,
    )
    files_before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[fund_flow_api.get_fund_flow_today] = lambda: AS_OF
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={"as_of": AS_OF.isoformat(), "scope": "market"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["availability"] == "ready"
    assert payload["evidence_level"] == "L1"
    assert payload["evidence_basis"] == "upstream_reported_main_net_inflow_proxy"
    assert payload["interpretation"] == "upstream_reported_not_ohlcv_inferred"
    assert payload["source"]["source"] == "akshare"
    assert payload["source"]["upstream"] == "eastmoney"
    assert payload["source"]["endpoint"] == ENDPOINT
    assert payload["date_semantics"] == "explicit_trade_date"
    assert payload["units"] == {
        "reported_main_net_inflow": "CNY",
        "reported_main_net_inflow_ratio": "percent",
        "reported_super_large_net_inflow": "CNY",
        "reported_large_net_inflow": "CNY",
        "reported_medium_net_inflow": "CNY",
        "reported_small_net_inflow": "CNY",
    }
    assert payload["unavailable_levels"] == [
        {
            "evidence_level": "L2",
            "status": "unavailable",
            "reason": "public_institutional_activity_source_not_configured",
        },
        {
            "evidence_level": "L3",
            "status": "unavailable",
            "reason": "price_volume_inference_not_implemented_in_this_slice",
        },
    ]
    forbidden = {"open", "high", "low", "close", "volume", "amount", "net_inflow"}
    assert forbidden.isdisjoint(payload)
    assert all(forbidden.isdisjoint(point) for point in payload["raw_observed_points"])
    files_after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert files_after == files_before


@pytest.mark.anyio
async def test_real_provider_market_scope_is_readable_without_all_a_share_overclaim(
    tmp_path: Path,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    settings = _publish_real_contract_market_history(tmp_path, sessions)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[fund_flow_api.get_fund_flow_today] = lambda: AS_OF
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={"as_of": AS_OF.isoformat(), "scope": "market"},
            )
            legacy_overclaim = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={
                    "as_of": AS_OF.isoformat(),
                    "scope": "market",
                    "scope_id": "all_a_share",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["availability"] == "ready"
    assert payload["scope_id"] == MARKET_ANALYSIS_UNIVERSE
    assert payload["analysis_universe"] == MARKET_ANALYSIS_UNIVERSE
    assert payload["upstream_scope_identity"] == MARKET_UPSTREAM_SCOPE
    assert {point["upstream_scope_identity"] for point in payload["raw_observed_points"]} == {
        MARKET_UPSTREAM_SCOPE
    }
    assert {item["upstream_scope_identity"] for item in payload["semantic_lineage"]} == {
        MARKET_UPSTREAM_SCOPE
    }
    assert legacy_overclaim.status_code == 422


@pytest.mark.anyio
async def test_scope_validation_future_rejection_and_sector_selection(
    tmp_path: Path,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    settings = _settings(tmp_path)
    store = FundFlowEvidenceStore.from_settings(settings)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[fund_flow_api.get_fund_flow_store] = lambda: store
    app.dependency_overrides[fund_flow_api.get_fund_flow_today] = lambda: AS_OF
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            invalid_market = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={
                    "as_of": AS_OF.isoformat(),
                    "scope": "market",
                    "scope_id": "银行",
                },
            )
            missing_sector = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={"as_of": AS_OF.isoformat(), "scope": "sector"},
            )
            unsafe_sector = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={
                    "as_of": AS_OF.isoformat(),
                    "scope": "sector",
                    "scope_id": "../银行",
                },
            )
            future = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={"as_of": "2026-07-30", "scope": "market"},
            )
    finally:
        app.dependency_overrides.clear()

    assert invalid_market.status_code == 422
    assert missing_sector.status_code == 422
    assert unsafe_sector.status_code == 422
    assert future.status_code == 422
    assert future.json()["detail"]["code"] == "future_as_of"
    assert len(sessions) == 20


@pytest.mark.anyio
async def test_sector_scope_reads_only_the_exact_published_sector_semantics(
    tmp_path: Path,
) -> None:
    calendar = get_trading_calendar()
    sessions = _sessions(calendar, end=AS_OF, count=20)
    points = _points(
        sessions,
        scope="industry",
        scope_name="银行",
        endpoint="stock_sector_fund_flow_hist",
    )
    settings = _publish_sector_history(
        tmp_path,
        points,
        through_date=AS_OF,
        sector_name="银行",
    )
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[fund_flow_api.get_fund_flow_today] = lambda: AS_OF
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={
                    "as_of": AS_OF.isoformat(),
                    "scope": "sector",
                    "scope_id": "银行",
                },
            )
            absent = await client.get(
                "/api/v1/analysis/fund-flow-evidence",
                params={
                    "as_of": AS_OF.isoformat(),
                    "scope": "sector",
                    "scope_id": "半导体",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["availability"] == "ready"
    assert payload["scope"] == "sector"
    assert payload["scope_id"] == "银行"
    assert payload["source"]["endpoint"] == "stock_sector_fund_flow_hist"
    assert {point["scope_id"] for point in payload["raw_observed_points"]} == {"银行"}
    assert absent.status_code == 200
    assert absent.json()["availability"] == "empty"


def test_failed_ingestion_keeps_last_trusted_manifest_for_analysis(
    tmp_path: Path,
) -> None:
    calendar = get_trading_calendar()
    previous = calendar.previous_session(AS_OF)
    assert previous is not None
    sessions = _sessions(calendar, end=previous, count=20)
    settings = _publish_market_history(
        tmp_path,
        _points(sessions),
        through_date=previous,
    )
    manifest = settings.supplemental_data_dir / "manifest.json"
    before = manifest.read_bytes()

    class FailedProvider:
        def market_fund_flow_history(self, *, through_date: date):
            raise RuntimeError("source failed")

    supplemental_store = SupplementalStore(
        settings.supplemental_data_dir,
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental.lock",
    )
    with pytest.raises(RuntimeError, match="source failed"):
        SupplementalIngestionService(supplemental_store, FailedProvider()).execute(
            SupplementalIngestionRequest(
                through_date=AS_OF,
                include_market_flow=True,
                canary=True,
            )
        )

    snapshot = FundFlowEvidenceStore.from_settings(settings).read(
        as_of=AS_OF,
        scope="market",
        scope_id=MARKET_ANALYSIS_UNIVERSE,
    )
    result = FundFlowEvidenceService(calendar).evaluate(snapshot)
    assert manifest.read_bytes() == before
    assert result.last_trusted_date == previous
    assert result.availability == "stale"
    assert result.can_publish_trend is False
