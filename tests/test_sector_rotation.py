import asyncio
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path

import httpx
import pytest

from backend.app.classification.models import (
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.classification.store import ClassificationReadSnapshot, ClassificationStore
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.models import DailyBar
from backend.app.market.store import MarketStore

AS_OF = date(2026, 1, 31)
CLASSIFICATION_DATE = date(2025, 11, 1)
OBSERVED_AT = datetime(2025, 11, 1, 12, tzinfo=UTC)


def _api_get(
    path: str,
    overrides: dict | None = None,
    *,
    raise_app_exceptions: bool = True,
) -> httpx.Response:
    app.dependency_overrides.update(overrides or {})

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(
            app=app,
            raise_app_exceptions=raise_app_exceptions,
        )
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def _security(
    symbol: str,
    *,
    snapshot_date: date = CLASSIFICATION_DATE,
    board: str = "main",
    tradable: bool = True,
    price_available: bool | None = True,
) -> SecurityMasterRecord:
    observed_at = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC)
    exchange = symbol.split(".", 1)[0]
    return SecurityMasterRecord(
        record_id=f"security:{snapshot_date}:{symbol}",
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        name=f"Fixture {symbol}",
        exchange=exchange,
        board=board,
        security_type="stock",
        list_date=date(2000, 1, 1),
        delist_date=None,
        is_tradable=tradable,
        listing_status="1",
        daily_trade_status="1" if tradable else "0",
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=date(2000, 1, 1),
        effective_to=None,
        observed_at=observed_at,
        source_record_id=f"query_all_stock:{snapshot_date}:{symbol}",
        lineage_hash=f"security:{snapshot_date}:{symbol}:{tradable}",
        price_available=price_available,
    )


def _membership(
    symbol: str,
    sector_id: str,
    *,
    snapshot_date: date = CLASSIFICATION_DATE,
) -> SectorMembershipRecord:
    observed_at = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC)
    return SectorMembershipRecord(
        record_id=f"membership:{snapshot_date}:{symbol}",
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
        taxonomy_name="BaoStock industryClassification",
        sector_id=sector_id,
        sector_name=sector_id.title(),
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        raw_industry=sector_id.title(),
        raw_classification="证监会行业分类",
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=snapshot_date,
        effective_to=None,
        observed_at=observed_at,
        source_record_id=f"query_stock_industry:{snapshot_date}:{symbol}",
        lineage_hash=f"membership:{snapshot_date}:{symbol}:{sector_id}",
    )


def _publish_classification(
    path: Path,
    sector_by_symbol: dict[str, str],
    *,
    snapshot_date: date = CLASSIFICATION_DATE,
    security_overrides: dict[str, dict[str, object]] | None = None,
) -> tuple[ClassificationStore, str, bool]:
    overrides = security_overrides or {}
    securities = [
        _security(
            symbol,
            snapshot_date=snapshot_date,
            **overrides.get(symbol, {}),
        )
        for symbol in sector_by_symbol
    ]
    memberships = [
        _membership(symbol, sector_id, snapshot_date=snapshot_date)
        for symbol, sector_id in sector_by_symbol.items()
    ]
    store = ClassificationStore(path)
    outcome = store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=snapshot_date,
            observed_at=datetime.combine(
                snapshot_date,
                datetime.min.time(),
                tzinfo=UTC,
            ),
            securities=securities,
            sector_memberships=memberships,
        )
    )
    return store, outcome.generation.generation_id, outcome.promoted


def _sessions(count: int, *, end: date = AS_OF) -> list[date]:
    return [end - timedelta(days=count - offset - 1) for offset in range(count)]


def _bars(
    symbol: str,
    *,
    drift: float,
    count: int = 65,
    end: date = AS_OF,
    amount: float = 1_000_000,
    amount_drift: float = 0,
    board: str = "main",
    suspended_on_last: bool = False,
    quality_on_last: str = "ready",
    issues_on_last: list[str] | None = None,
) -> list[DailyBar]:
    rows = []
    close = 100.0
    dates = _sessions(count, end=end)
    for index, trade_date in enumerate(dates):
        preclose = close
        close *= 1 + drift
        is_last = index == len(dates) - 1
        rows.append(
            DailyBar(
                trade_date=trade_date,
                symbol=symbol,
                security_type="stock",
                exchange=symbol.split(".", 1)[0],
                board=board,
                open=preclose,
                high=max(preclose, close) * 1.01,
                low=min(preclose, close) * 0.99,
                close=close,
                preclose=preclose,
                volume=100_000,
                amount=amount * (1 + amount_drift * index),
                turnover_rate=1,
                pct_change=drift * 100,
                adjust_factor=1,
                is_trading=not (is_last and suspended_on_last),
                is_suspended=is_last and suspended_on_last,
                is_st=False,
                source="baostock",
                source_record_id=f"{symbol}:{trade_date}",
                ingested_at=datetime.combine(
                    trade_date,
                    datetime.min.time(),
                    tzinfo=UTC,
                ),
                quality_status=quality_on_last if is_last else "ready",
                quality_issues=(issues_on_last or []) if is_last else [],
            )
        )
    return rows


class _BarsReader:
    def __init__(self, bars: list[DailyBar]) -> None:
        self.bars = bars
        self.calls = 0

    def bars_through(self, as_of: date, *, max_sessions: int) -> list[DailyBar]:
        self.calls += 1
        return list(self.bars)


def _service(
    tmp_path: Path,
    sector_by_symbol: dict[str, str],
    bars: list[DailyBar],
    *,
    security_overrides: dict[str, dict[str, object]] | None = None,
):
    sector_service = import_module("backend.app.sector.service")
    classification, generation_id, promoted = _publish_classification(
        tmp_path / "classification.duckdb",
        sector_by_symbol,
        security_overrides=security_overrides,
    )
    assert promoted is True
    reader = _BarsReader(bars)
    service = sector_service.SectorRotationService(classification, reader)
    return service, reader, generation_id


def test_sector_rotation_api_rejects_future_shanghai_date() -> None:
    response = _api_get(
        "/api/v1/analysis/sector-rotation"
        "?as_of=2999-01-01&taxonomy_id=baostock.industry_classification"
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "future_as_of"


def test_sector_ranking_exposes_v1_metrics_reasons_and_missing_flow(
    tmp_path: Path,
) -> None:
    sector_by_symbol = {
        "sh.600001": "growth",
        "sz.000001": "growth",
        "sh.600002": "value",
        "sz.000002": "value",
    }
    bars = [
        *_bars("sh.600001", drift=0.006, amount_drift=0.01),
        *_bars("sz.000001", drift=0.005, amount_drift=0.008),
        *_bars("sh.600002", drift=-0.002),
        *_bars("sz.000002", drift=-0.001),
        *_bars("sh.600999", drift=0.001),
    ]
    service, reader, generation_id = _service(
        tmp_path,
        sector_by_symbol,
        bars,
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert reader.calls == 1
    assert result.status == "degraded"
    assert result.classification_lineage.generation_id == generation_id
    assert result.data_as_of == AS_OF
    assert [item.sector_id for item in result.rankings] == ["growth", "value"]
    growth = result.rankings[0]
    assert growth.total_score > result.rankings[1].total_score
    assert {item.metric for item in growth.metric_scores} == {
        "relative_strength_5d",
        "relative_strength_20d",
        "relative_strength_60d",
        "advancing_breadth",
        "above_ma20_breadth",
        "above_ma60_breadth",
        "turnover_change_5d",
        "turnover_change_20d",
        "turnover_concentration_top3",
        "persistence_days",
        "cross_section_dispersion",
    }
    assert all(item.formula_version for item in growth.metric_scores)
    assert all(item.raw_value is not None for item in growth.metric_scores)
    assert growth.supporting_evidence
    assert result.rankings[1].contrary_evidence
    assert result.fund_flow_evidence.status == "missing"
    assert result.fund_flow_evidence.evidence_tier is None
    assert "fund_flow.r2_evidence_not_integrated" in result.missing_inputs
    assert result.actual_scope.scope_status == "narrow_main_board"
    assert result.actual_scope.can_support_full_a_share_conclusion is False
    assert result.actual_scope.can_support_all_industry_conclusion is False


def test_sector_metric_warmup_is_explicit_for_5_20_60_sessions(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth"},
        _bars("sh.600001", drift=0.005, count=5),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    metrics = {item.metric: item for item in result.rankings[0].metric_scores}

    assert result.status == "degraded"
    assert metrics["relative_strength_5d"].raw_value is None
    assert metrics["relative_strength_20d"].raw_value is None
    assert metrics["relative_strength_60d"].raw_value is None
    assert metrics["above_ma20_breadth"].raw_value is None
    assert metrics["above_ma60_breadth"].raw_value is None
    assert "sector.relative_strength_5d_warmup" in result.rankings[0].missing_inputs
    assert "sector.relative_strength_20d_warmup" in result.rankings[0].missing_inputs
    assert "sector.relative_strength_60d_warmup" in result.rankings[0].missing_inputs


def test_historical_replay_uses_promoted_generation_visible_at_as_of(
    tmp_path: Path,
) -> None:
    path = tmp_path / "classification.duckdb"
    store, historical_id, historical_promoted = _publish_classification(
        path,
        {"sh.600001": "historical-sector"},
    )
    assert historical_promoted is True
    _, future_id, future_promoted = _publish_classification(
        path,
        {"sh.600001": "future-sector"},
        snapshot_date=AS_OF + timedelta(days=1),
    )
    assert future_promoted is True
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        store,
        _BarsReader(_bars("sh.600001", drift=0.003)),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.classification_lineage.generation_id == historical_id
    assert result.classification_lineage.generation_id != future_id
    assert [item.sector_id for item in result.rankings] == ["historical-sector"]


def test_degraded_classification_candidate_never_enters_rotation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "classification.duckdb"
    store, ready_id, promoted = _publish_classification(
        path,
        {"sh.600001": "trusted", "sz.000001": "trusted"},
    )
    assert promoted is True
    degraded = store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=AS_OF,
            observed_at=datetime.combine(AS_OF, datetime.min.time(), tzinfo=UTC),
            securities=[
                _security("sh.600001", snapshot_date=AS_OF),
                _security("sz.000001", snapshot_date=AS_OF),
            ],
            sector_memberships=[
                _membership("sh.600001", "candidate", snapshot_date=AS_OF),
            ],
        )
    )
    assert degraded.promoted is False
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        store,
        _BarsReader(
            [
                *_bars("sh.600001", drift=0.003),
                *_bars("sz.000001", drift=0.002),
            ]
        ),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.classification_lineage.generation_id == ready_id
    assert [item.sector_id for item in result.rankings] == ["trusted"]


def test_below_95_percent_initial_classification_is_not_readable(
    tmp_path: Path,
) -> None:
    snapshot_date = CLASSIFICATION_DATE
    securities = [
        _security(f"sh.{600000 + index:06d}", snapshot_date=snapshot_date) for index in range(20)
    ]
    memberships = [
        _membership(row.symbol, "mapped", snapshot_date=snapshot_date) for row in securities[:18]
    ]
    store = ClassificationStore(tmp_path / "classification.duckdb")
    outcome = store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=snapshot_date,
            observed_at=OBSERVED_AT,
            securities=securities,
            sector_memberships=memberships,
        )
    )
    assert outcome.generation.coverage_audits[0].coverage_ratio == 0.9
    assert outcome.promoted is False
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        store,
        _BarsReader(_bars(securities[0].symbol, drift=0.003)),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.status == "empty"
    assert result.classification_lineage is None
    assert result.rankings == []
    assert "classification.promoted_generation" in result.missing_inputs


def test_unknown_classification_member_is_excluded_defensively(
    tmp_path: Path,
) -> None:
    store, _, promoted = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "known"},
    )
    assert promoted is True
    selected = store.read_snapshot(
        AS_OF,
        include_securities=True,
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
    )
    unknown = _membership("sh.609999", "unknown")
    selected_with_unknown = ClassificationReadSnapshot(
        ready_generation_id=selected.ready_generation_id,
        generation=selected.generation,
        securities=selected.securities,
        security_snapshot_date=selected.security_snapshot_date,
        index_components=selected.index_components,
        index_snapshot_date=selected.index_snapshot_date,
        sector_memberships=[*selected.sector_memberships, unknown],
        sector_snapshot_date=selected.sector_snapshot_date,
    )

    class SnapshotStore:
        def read_snapshot(self, *_args, **_kwargs):
            return selected_with_unknown

    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        SnapshotStore(),
        _BarsReader(
            [
                *_bars("sh.600001", drift=0.003),
                *_bars("sh.609999", drift=0.02),
            ]
        ),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert [item.sector_id for item in result.rankings] == ["known"]
    assert "unknown_classification_member_excluded" in result.quality_issues


def test_stale_and_missing_prices_are_truthful(
    tmp_path: Path,
) -> None:
    stale_end = AS_OF - timedelta(days=2)
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", "sz.000001": "growth"},
        _bars("sh.600001", drift=0.003, end=stale_end),
    )

    stale = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert stale.status == "degraded"
    assert stale.data_as_of == stale_end
    assert "market_data_stale_for_requested_as_of" in stale.quality_issues
    assert stale.rankings[0].member_count == 2
    assert stale.rankings[0].priced_member_count == 1
    assert "sector.member_price_missing" in stale.rankings[0].quality_issues

    missing_service, _, _ = _service(
        tmp_path / "missing",
        {"sh.600001": "growth"},
        [],
    )
    missing = missing_service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    assert missing.status == "empty"
    assert missing.data_as_of is None
    assert missing.rankings == []
    assert "market.price_history" in missing.missing_inputs


def test_leaders_exclude_untradable_and_bad_quality_and_never_guess_limit_lock(
    tmp_path: Path,
) -> None:
    sector_by_symbol = {
        "sh.600001": "growth",
        "sh.600002": "growth",
        "sh.600003": "growth",
    }
    bars = [
        *_bars("sh.600001", drift=0.003),
        *_bars("sh.600002", drift=0.02, suspended_on_last=True),
        *_bars(
            "sh.600003",
            drift=0.01,
            quality_on_last="error",
            issues_on_last=["fixture_bad_price"],
        ),
    ]
    service, _, _ = _service(
        tmp_path,
        sector_by_symbol,
        bars,
        security_overrides={
            "sh.600002": {"tradable": False},
        },
    )

    result = service.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "growth",
    )

    assert result.status == "degraded"
    assert [item.symbol for item in result.candidates] == ["sh.600001"]
    assert result.candidates[0].actionable_primary is False
    assert result.candidates[0].limit_lock_status == "unavailable"
    assert "leader.limit_lock_status" in result.candidates[0].missing_inputs
    exclusions = {item.symbol: item.reasons for item in result.exclusions}
    assert "classification_not_tradable" in exclusions["sh.600002"]
    assert "suspended" in exclusions["sh.600002"]
    assert "bad_price_quality" in exclusions["sh.600003"]


def test_leader_metrics_include_required_reasons_and_flow_is_missing(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", "sh.600002": "growth"},
        [
            *_bars("sh.600001", drift=0.006, amount_drift=0.01),
            *_bars("sh.600002", drift=0.002),
            *_bars("sz.000999", drift=0.001),
        ],
    )

    result = service.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "growth",
    )
    leader = result.candidates[0]

    assert leader.symbol == "sh.600001"
    assert {item.metric for item in leader.metric_scores} == {
        "tradability",
        "relative_strength_5d",
        "relative_strength_20d",
        "relative_strength_60d",
        "trading_activity_20d",
        "trend_quality",
        "sector_contribution_20d",
        "persistence_days",
        "dispersion_consistency",
    }
    assert all(item.formula_version for item in leader.metric_scores)
    assert leader.supporting_evidence
    assert leader.total_score is not None
    assert result.fund_flow_evidence.status == "missing"
    assert "fund_flow.r2_evidence_not_integrated" in result.missing_inputs


def test_sector_and_leader_ties_and_ids_are_deterministic(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600002": "zeta", "sh.600001": "alpha"},
        [
            *_bars("sh.600002", drift=0.003),
            *_bars("sh.600001", drift=0.003),
        ],
    )

    first = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    second = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    alpha_first = service.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "alpha",
    )
    alpha_second = service.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "alpha",
    )

    assert [item.sector_id for item in first.rankings] == ["alpha", "zeta"]
    assert first.result_id == second.result_id
    assert [item.ranking_id for item in first.rankings] == [
        item.ranking_id for item in second.rankings
    ]
    assert alpha_first.result_id == alpha_second.result_id
    assert alpha_first.candidates[0].candidate_id == (alpha_second.candidates[0].candidate_id)


def test_leader_tie_breaks_by_symbol_and_uses_one_snapshot_and_market_read(
    tmp_path: Path,
) -> None:
    classification, _, promoted = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600002": "growth", "sh.600001": "growth"},
    )
    assert promoted is True

    class CountingStore:
        def __init__(self, wrapped: ClassificationStore) -> None:
            self.wrapped = wrapped
            self.calls = 0

        def read_snapshot(self, *args, **kwargs):
            self.calls += 1
            return self.wrapped.read_snapshot(*args, **kwargs)

    counting = CountingStore(classification)
    reader = _BarsReader(
        [
            *_bars("sh.600002", drift=0.003),
            *_bars("sh.600001", drift=0.003),
        ]
    )
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(counting, reader)

    result = service.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "growth",
    )

    assert [item.symbol for item in result.candidates] == [
        "sh.600001",
        "sh.600002",
    ]
    assert counting.calls == 1
    assert reader.calls == 1


def test_unknown_market_symbols_and_non_main_board_prices_do_not_expand_scope(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "main", "sz.300001": "chinext"},
        [
            *_bars("sh.600001", drift=0.003),
            *_bars("sz.300001", drift=0.02, board="chinext"),
            *_bars("sh.601999", drift=0.05),
        ],
        security_overrides={
            "sz.300001": {"board": "chinext"},
        },
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.actual_scope.included_boards == ["main"]
    assert "chinext" in result.actual_scope.excluded_classification_boards
    assert result.actual_scope.priced_classified_symbols == 1
    assert [item.sector_id for item in result.rankings] == ["main", "chinext"]
    chinext = next(item for item in result.rankings if item.sector_id == "chinext")
    assert chinext.total_score is None
    assert "sector.member_outside_narrow_main_board_scope" in chinext.quality_issues


def test_api_future_date_is_rejected_before_sector_store_read() -> None:
    sector_api = import_module("backend.app.api.sector")

    class RejectReadService:
        def rotation(self, *_args):
            raise AssertionError("future as_of reached sector service")

    response = _api_get(
        "/api/v1/analysis/sector-rotation"
        f"?as_of={AS_OF + timedelta(days=1)}"
        f"&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: RejectReadService(),
            sector_api.get_sector_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 422


def test_api_leaders_reject_future_date_before_sector_store_read() -> None:
    sector_api = import_module("backend.app.api.sector")

    class RejectReadService:
        def leaders(self, *_args):
            raise AssertionError("future as_of reached sector service")

    response = _api_get(
        "/api/v1/analysis/sectors/growth/leaders"
        f"?as_of={AS_OF + timedelta(days=1)}"
        f"&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: RejectReadService(),
            sector_api.get_sector_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 422


def test_api_missing_databases_are_empty_and_create_no_state(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    root = tmp_path / "absent-runtime"
    settings = Settings(
        market_data_dir=root / "market",
        local_control_dir=root / "control",
        local_staging_dir=root / "staging",
        local_lock_dir=root / "locks",
        local_temp_dir=root / "temp",
        user_data_dir=root / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )
    service = sector_api.get_sector_rotation_service(settings)

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: service,
            sector_api.get_sector_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "empty"
    assert response.json()["classification_lineage"] is None
    assert not root.exists()


@pytest.mark.parametrize(
    "dataset_setting",
    ["local_market_dataset_root", "nas_market_dataset_root"],
)
def test_api_missing_dataset_manifest_is_empty_without_writes(
    tmp_path: Path,
    dataset_setting: str,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    runtime = tmp_path / "runtime"
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    settings = Settings(
        market_data_dir=runtime / "market",
        local_control_dir=runtime / "control",
        local_staging_dir=runtime / "staging",
        local_lock_dir=runtime / "locks",
        local_temp_dir=runtime / "temp",
        user_data_dir=runtime / "user",
        local_market_dataset_root=(
            dataset_root if dataset_setting == "local_market_dataset_root" else None
        ),
        nas_market_dataset_root=(
            dataset_root if dataset_setting == "nas_market_dataset_root" else None
        ),
    )
    service = sector_api.get_sector_rotation_service(settings)

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: service,
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "empty"
    assert not runtime.exists()
    assert list(dataset_root.iterdir()) == []


def test_api_corrupt_market_dataset_is_explicit_503(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    runtime = tmp_path / "runtime"
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    manifest = dataset_root / "manifest.json"
    manifest.write_text("{not-json", encoding="utf-8")
    settings = Settings(
        market_data_dir=runtime / "market",
        local_control_dir=runtime / "control",
        local_staging_dir=runtime / "staging",
        local_lock_dir=runtime / "locks",
        local_temp_dir=runtime / "temp",
        user_data_dir=runtime / "user",
        local_market_dataset_root=dataset_root,
        nas_market_dataset_root=None,
    )
    service = sector_api.get_sector_rotation_service(settings)

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: service,
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "market_storage_unavailable"
    assert manifest.read_text(encoding="utf-8") == "{not-json"
    assert not runtime.exists()


def test_real_duckdb_stores_integrate_through_read_only_api(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    classification, generation_id, promoted = _publish_classification(
        market_dir / "classification.duckdb",
        {"sh.600001": "growth"},
    )
    assert promoted is True
    assert classification.ready_generation() == generation_id
    market_database = market_dir / "market.duckdb"
    MarketStore(
        market_database,
        temp_directory=tmp_path / "writer-temp",
    ).upsert_bars(_bars("sh.600001", drift=0.003))
    settings = Settings(
        market_data_dir=market_dir,
        market_database_name=market_database.name,
        classification_database_name="classification.duckdb",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "reader-temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )
    service = sector_api.get_sector_rotation_service(settings)
    classification_before = (market_dir / "classification.duckdb").read_bytes()
    market_before = market_database.read_bytes()

    rotation = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: service,
            sector_api.get_sector_today: lambda: AS_OF,
        },
    )
    leaders = _api_get(
        "/api/v1/analysis/sectors/growth/leaders"
        f"?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: service,
            sector_api.get_sector_today: lambda: AS_OF,
        },
    )

    assert rotation.status_code == 200
    assert rotation.json()["classification_lineage"]["generation_id"] == generation_id
    assert leaders.status_code == 200
    assert leaders.json()["candidates"][0]["symbol"] == "sh.600001"
    assert (market_dir / "classification.duckdb").read_bytes() == classification_before
    assert market_database.read_bytes() == market_before
    assert not settings.local_temp_dir.exists()
