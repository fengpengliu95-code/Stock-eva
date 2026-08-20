import asyncio
import time
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path

import duckdb
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
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.regime.store import (
    MarketReadUnavailable,
    PublishedSnapshotDuckDbMarketReader,
)

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
    exchange: str | None = None,
    tradable: bool = True,
    price_available: bool | None = True,
) -> SecurityMasterRecord:
    observed_at = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC)
    resolved_exchange = exchange or symbol.split(".", 1)[0]
    return SecurityMasterRecord(
        record_id=f"security:{snapshot_date}:{symbol}",
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        name=f"Fixture {symbol}",
        exchange=resolved_exchange,
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


def _refresh(
    run_id: str,
    requested_date: date,
    *,
    status: str = "ready",
    count: int = 1,
) -> RefreshResult:
    return RefreshResult(
        run_id=run_id,
        requested_date=requested_date,
        source="baostock",
        status=status,
        requested_count=count,
        succeeded_count=count if status == "ready" else 0,
        coverage_ratio=1 if status == "ready" else 0,
        quality_issues=[] if status == "ready" else ["fixture_incomplete"],
        started_at=datetime.combine(requested_date, datetime.min.time(), tzinfo=UTC),
        completed_at=datetime.combine(requested_date, datetime.min.time(), tzinfo=UTC),
    )


def _publish_history(
    store: MarketStore,
    bars: list[DailyBar],
    *,
    run_prefix: str,
) -> None:
    by_date: dict[date, list[DailyBar]] = {}
    for bar in bars:
        by_date.setdefault(bar.trade_date, []).append(bar)
    for trade_date, partition in sorted(by_date.items()):
        store.save_refresh(
            partition,
            _refresh(
                f"{run_prefix}-{trade_date.isoformat()}",
                trade_date,
                count=len({bar.symbol for bar in partition}),
            ),
            publish=True,
        )


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
        "leader_count",
        "leader_diffusion",
        "leader_persistence_days",
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
    assert [item.sector_id for item in result.rankings] == ["chinext", "main"]
    chinext = next(item for item in result.rankings if item.sector_id == "chinext")
    assert chinext.total_score is None
    assert "sector.member_outside_narrow_main_board_scope" in chinext.quality_issues


def test_out_of_scope_members_do_not_reduce_in_scope_price_coverage_or_confidence(
    tmp_path: Path,
) -> None:
    main_symbols = ["sh.600001", "sh.600002", "sz.000001"]
    chinext_symbols = [f"sz.{300000 + index:06d}" for index in range(14)]
    star_symbols = [f"sh.{688000 + index:06d}" for index in range(14)]
    out_of_scope_symbols = [*chinext_symbols, *star_symbols]
    symbols = [*main_symbols, *out_of_scope_symbols]
    service, _, _ = _service(
        tmp_path,
        {symbol: "mixed-board" for symbol in symbols},
        [
            *(
                bar
                for symbol, drift in zip(main_symbols, (0.002, 0.003, 0.004), strict=True)
                for bar in _bars(symbol, drift=drift, amount_drift=0.01)
            ),
            *(
                bar
                for symbol in chinext_symbols
                for bar in _bars(symbol, drift=0.02, board="chinext")
            ),
            *(bar for symbol in star_symbols for bar in _bars(symbol, drift=0.02, board="star")),
        ],
        security_overrides={
            **{symbol: {"board": "chinext"} for symbol in chinext_symbols},
            **{symbol: {"board": "star"} for symbol in star_symbols},
        },
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    ranking = result.rankings[0]

    assert ranking.member_count == 31
    assert ranking.priced_member_count == 3
    assert ranking.ranking_eligible is True
    assert "sector.member_price_missing" not in ranking.quality_issues
    assert ranking.confidence.value == 0.75
    assert "sector.member_outside_narrow_main_board_scope" in ranking.quality_issues
    assert ranking.quality_status == "degraded"


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
def test_api_missing_dataset_manifest_is_unavailable_without_writes(
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

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "market_storage_unavailable",
        "storage_status": "unavailable",
    }
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
    integration_bars = _bars("sh.600001", drift=0.003)
    market_store = MarketStore(
        market_database,
        temp_directory=tmp_path / "writer-temp",
    )
    _publish_history(
        market_store,
        integration_bars,
        run_prefix="integration-published",
    )
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


def test_r1c_reads_only_published_rows_after_same_day_partial_overwrite(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    _publish_classification(
        market_dir / "classification.duckdb",
        {"sh.600001": "growth", "sh.600002": "growth"},
    )
    market_database = market_dir / "market.duckdb"
    store = MarketStore(market_database, temp_directory=tmp_path / "writer-temp")
    published = [
        *_bars("sh.600001", drift=0.006, amount_drift=0.01),
        *_bars("sh.600002", drift=0.002),
    ]
    _publish_history(
        store,
        published,
        run_prefix="published",
    )
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
    before = sector_api.get_sector_rotation_service(settings)
    rotation_before = before.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders_before = before.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    poisoned = published[-1].model_copy(
        update={
            "close": published[-1].close + 999,
            "quality_status": "partial",
            "quality_issues": ["fixture_partial"],
        }
    )
    store.save_refresh(
        [poisoned],
        _refresh("partial", AS_OF, status="partial"),
        publish=False,
    )
    after = sector_api.get_sector_rotation_service(settings)
    rotation_after = after.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders_after = after.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert rotation_after.result_id == rotation_before.result_id
    assert rotation_after.market_lineage == rotation_before.market_lineage
    assert leaders_after.result_id == leaders_before.result_id


def test_historical_partial_never_becomes_published_after_later_pointer(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    old_date = AS_OF - timedelta(days=2)
    partial_date = AS_OF - timedelta(days=1)
    market_dir = tmp_path / "market"
    _publish_classification(
        market_dir / "classification.duckdb",
        {"sh.600001": "growth"},
    )
    market_database = market_dir / "market.duckdb"
    store = MarketStore(market_database, temp_directory=tmp_path / "writer-temp")
    old_bars = _bars("sh.600001", drift=0.003, end=old_date)
    _publish_history(
        store,
        old_bars,
        run_prefix="old-published",
    )
    store.save_refresh(
        _bars("sh.600001", drift=0.5, count=1, end=partial_date),
        _refresh("historical-partial", partial_date, status="partial"),
        publish=False,
    )
    store.save_refresh(
        _bars("sh.600001", drift=0.004, count=1, end=AS_OF),
        _refresh("later-published", AS_OF),
        publish=True,
    )
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

    result = sector_api.get_sector_rotation_service(settings).rotation(
        partial_date,
        TAXONOMY_BAOSTOCK_INDUSTRY,
    )

    assert result.data_as_of == old_date
    assert result.market_lineage.latest_input_date == old_date


def test_existing_unpublished_market_database_fails_closed_with_structured_503(
    tmp_path: Path,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    _publish_classification(
        market_dir / "classification.duckdb",
        {"sh.600001": "growth"},
    )
    MarketStore(market_dir / "market.duckdb").upsert_bars(_bars("sh.600001", drift=0.003))
    settings = Settings(
        market_data_dir=market_dir,
        market_database_name="market.duckdb",
        classification_database_name="classification.duckdb",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "reader-temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: sector_api.get_sector_rotation_service(
                settings
            ),
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "market_publication_state_unavailable"


def test_real_provider_shape_with_unverified_classification_price_can_rank(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", "sh.600002": "growth"},
        [
            *_bars("sh.600001", drift=0.006, amount_drift=0.01),
            *_bars("sh.600002", drift=0.002),
        ],
        security_overrides={
            "sh.600001": {"price_available": None},
            "sh.600002": {"price_available": None},
        },
    )

    result = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert {item.symbol for item in result.candidates} == {
        "sh.600001",
        "sh.600002",
    }
    assert all(
        "classification_price_not_verified" not in item.reasons for item in result.exclusions
    )


def test_low_horizon_member_coverage_is_missing_and_not_rank_eligible(
    tmp_path: Path,
) -> None:
    symbols = [f"sh.{600100 + index:06d}" for index in range(10)]
    bars = [
        *_bars(symbols[0], drift=0.006),
        *[_bars(symbol, drift=0.002, count=1)[0] for symbol in symbols[1:]],
    ]
    service, _, _ = _service(
        tmp_path,
        {symbol: "thin-history" for symbol in symbols},
        bars,
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    ranking = result.rankings[0]
    metrics = {item.metric: item for item in ranking.metric_scores}

    for name in (
        "relative_strength_60d",
        "above_ma60_breadth",
        "turnover_change_20d",
    ):
        assert metrics[name].effective_count == 1
        assert metrics[name].target_count == 10
        assert metrics[name].coverage_ratio == 0.1
        assert metrics[name].quality_status == "missing"
        assert metrics[name].score is None
    assert ranking.ranking_eligible is False
    assert ranking.total_score is None
    assert "sector.insufficient_comparable_member_coverage" in ranking.quality_issues


def test_non_main_update_does_not_advance_sector_data_as_of(
    tmp_path: Path,
) -> None:
    main_as_of = AS_OF - timedelta(days=1)
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth"},
        [
            *_bars("sh.600001", drift=0.003, end=main_as_of),
            *_bars("sz.300001", drift=0.02, count=1, board="chinext"),
        ],
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.data_as_of == main_as_of
    assert result.market_lineage.latest_input_date == main_as_of
    assert "non_main_rows_after_data_as_of_excluded" in result.quality_issues


def test_market_lineage_hashes_semantic_filter_fields_and_changes_result_id(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth"},
        _bars("sh.600001", drift=0.003),
    )
    ready = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    changed_bars = _bars("sh.600001", drift=0.003)
    changed_bars[-1] = changed_bars[-1].model_copy(update={"is_trading": False})
    changed_service, _, _ = _service(
        tmp_path / "changed",
        {"sh.600001": "growth"},
        changed_bars,
    )
    changed = changed_service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert ready.market_lineage.content_hash != changed.market_lineage.content_hash
    assert ready.result_id != changed.result_id


def test_formula_version_drives_response_and_all_stable_ids(tmp_path: Path) -> None:
    sector_service = import_module("backend.app.sector.service")
    classification, _, _ = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "growth", "sh.600002": "growth"},
    )
    bars = [
        *_bars("sh.600001", drift=0.006, amount_drift=0.01),
        *_bars("sh.600002", drift=0.002),
    ]
    v1 = sector_service.SectorRotationService(
        classification,
        _BarsReader(bars),
    )
    v9 = sector_service.SectorRotationService(
        classification,
        _BarsReader(bars),
        sector_policy=sector_service.SectorPolicy(formula_version="sector-rotation-v9"),
        leader_policy=sector_service.LeaderPolicy(formula_version="leader-ranking-v9"),
    )

    rotation_v1 = v1.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation_v9 = v9.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders_v1 = v1.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")
    leaders_v9 = v9.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert rotation_v9.formula_version == "sector-rotation-v9"
    assert leaders_v9.formula_version == "leader-ranking-v9"
    assert rotation_v1.result_id != rotation_v9.result_id
    assert rotation_v1.rankings[0].ranking_id != rotation_v9.rankings[0].ranking_id
    assert leaders_v1.result_id != leaders_v9.result_id
    assert leaders_v1.candidates[0].candidate_id != leaders_v9.candidates[0].candidate_id


def test_sector_leadership_metrics_and_research_qualification_are_explicit(
    tmp_path: Path,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {
            "sh.600001": "growth",
            "sh.600002": "growth",
            "sh.600003": "growth",
        },
        [
            *_bars("sh.600001", drift=0.008, amount_drift=0.02),
            *_bars("sh.600002", drift=0.004, amount_drift=0.01),
            *_bars("sh.600003", drift=-0.002),
            *_bars("sz.000999", drift=0.001),
        ],
    )

    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    metrics = {item.metric: item for item in rotation.rankings[0].metric_scores}
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert {"leader_count", "leader_diffusion", "leader_persistence_days"} <= set(metrics)
    assert metrics["leader_count"].raw_value >= 1
    assert metrics["leader_diffusion"].raw_value > 0
    assert metrics["leader_persistence_days"].raw_value is not None
    assert any(item.leader_qualified for item in leaders.candidates)
    assert all(item.actionable_primary is False for item in leaders.candidates)
    assert all(item.limit_lock_status == "unavailable" for item in leaders.candidates)
    assert all(item.qualification_version for item in leaders.candidates)


def test_requested_unverified_classification_provenance_is_degraded(
    tmp_path: Path,
) -> None:
    path = tmp_path / "classification.duckdb"
    securities = [
        _security("sh.600001").model_copy(update={"source_date_semantics": "requested_unverified"})
    ]
    memberships = [
        _membership("sh.600001", "growth").model_copy(
            update={"source_date_semantics": "requested_unverified"}
        )
    ]
    store = ClassificationStore(path)
    outcome = store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=CLASSIFICATION_DATE,
            source_date_semantics="requested_unverified",
            observed_at=OBSERVED_AT,
            securities=securities,
            sector_memberships=memberships,
        )
    )
    assert outcome.promoted is True
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        store,
        _BarsReader(_bars("sh.600001", drift=0.003)),
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.classification_lineage.source_date_semantics == "requested_unverified"
    assert "classification_source_date_requested_unverified" in result.quality_issues
    assert result.status == "degraded"


def test_extreme_amount_aggregation_fails_closed_without_500(
    tmp_path: Path,
) -> None:
    huge = 1.7e308
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", "sh.600002": "growth"},
        [
            *_bars("sh.600001", drift=0.003, amount=huge),
            *_bars("sh.600002", drift=0.002, amount=huge),
        ],
    )

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    metrics = {item.metric: item for item in result.rankings[0].metric_scores}

    assert metrics["turnover_change_5d"].score is None
    assert metrics["turnover_change_20d"].score is None
    assert metrics["turnover_concentration_top3"].score is None
    assert "sector.turnover_non_finite" in result.rankings[0].quality_issues


@pytest.mark.parametrize(
    ("database", "expected_code"),
    [
        ("classification", "classification_storage_unavailable"),
        ("market", "market_storage_unavailable"),
    ],
)
def test_corrupt_analysis_database_returns_stable_structured_503(
    tmp_path: Path,
    database: str,
    expected_code: str,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    if database == "classification":
        market_dir.mkdir(parents=True)
        (market_dir / "classification.duckdb").write_bytes(b"not-a-duckdb")
    else:
        _publish_classification(
            market_dir / "classification.duckdb",
            {"sh.600001": "growth"},
        )
        (market_dir / "market.duckdb").write_bytes(b"not-a-duckdb")
    settings = Settings(
        market_data_dir=market_dir,
        market_database_name="market.duckdb",
        classification_database_name="classification.duckdb",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "reader-temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: sector_api.get_sector_rotation_service(
                settings
            ),
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == expected_code


@pytest.mark.parametrize(
    ("database", "expected_code"),
    [
        ("classification", "classification_storage_unavailable"),
        ("market", "market_storage_unavailable"),
    ],
)
def test_semantically_corrupt_analysis_rows_return_structured_503(
    tmp_path: Path,
    database: str,
    expected_code: str,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    classification_path = market_dir / "classification.duckdb"
    _publish_classification(classification_path, {"sh.600001": "growth"})
    market_path = market_dir / "market.duckdb"
    bars = _bars("sh.600001", drift=0.003)
    _publish_history(
        MarketStore(market_path),
        bars,
        run_prefix="published",
    )
    target = classification_path if database == "classification" else market_path
    connection = duckdb.connect(str(target))
    try:
        if database == "classification":
            connection.execute("UPDATE classification_generations SET coverage_audits = '[{}]'")
        else:
            connection.execute("UPDATE published_daily_bars SET quality_issues = '{}'")
    finally:
        connection.close()
    settings = Settings(
        market_data_dir=market_dir,
        market_database_name=market_path.name,
        classification_database_name=classification_path.name,
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "reader-temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: sector_api.get_sector_rotation_service(
                settings
            ),
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == expected_code


def test_leader_ranking_representative_scale_has_stable_runtime_budget(
    tmp_path: Path,
) -> None:
    sector_service = import_module("backend.app.sector.service")
    symbols = [f"sh.{prefix}{index:03d}" for prefix in (600, 601, 603, 605) for index in range(750)]
    securities = [_security(symbol) for symbol in symbols]
    memberships = [_membership(symbol, "market") for symbol in symbols]
    generation = import_module("backend.app.classification.models").GenerationSummary(
        generation_id="fixture-generation",
        sequence=1,
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=CLASSIFICATION_DATE,
        observed_at=OBSERVED_AT,
        row_counts={"securities": len(symbols), "sector_memberships": len(symbols)},
        coverage_audits=[
            import_module("backend.app.classification.models").CoverageAudit(
                status="ready",
                as_of=CLASSIFICATION_DATE,
                generation_id="fixture-generation",
                taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
                eligible_count=len(symbols),
                mapped_count=len(symbols),
                coverage_ratio=1,
                unmapped_symbols=[],
                exclusion_reasons={},
                source_lineage=[],
            )
        ],
        market_scope=import_module("backend.app.classification.store").market_scope(securities),
    )

    class SnapshotStore:
        def read_snapshot(self, *_args, **_kwargs):
            return ClassificationReadSnapshot(
                ready_generation_id=generation.generation_id,
                generation=generation,
                securities=securities,
                security_snapshot_date=CLASSIFICATION_DATE,
                index_components=[],
                index_snapshot_date=None,
                sector_memberships=memberships,
                sector_snapshot_date=CLASSIFICATION_DATE,
            )

    bars = [
        bar
        for index, symbol in enumerate(symbols)
        for bar in _bars(
            symbol,
            drift=0.001 + (index % 7) * 0.0001,
            amount_drift=0.001,
        )
    ]
    service = sector_service.SectorRotationService(SnapshotStore(), _BarsReader(bars))

    started = time.perf_counter()
    result = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "market")
    elapsed = time.perf_counter() - started

    assert len(result.candidates) == 3000
    assert elapsed < 15


def test_fake_ready_publication_cannot_replace_complete_same_day_partition(
    tmp_path: Path,
) -> None:
    market_path = tmp_path / "market.duckdb"
    store = MarketStore(market_path)
    first = [
        _bars("sh.600001", drift=0.003, count=1)[0],
        _bars("sh.600002", drift=0.002, count=1)[0],
    ]
    store.save_refresh(first, _refresh("complete", AS_OF, count=2), publish=True)
    pointer_before = store.published_refresh()
    fake_ready = _refresh("fake-ready", AS_OF, count=2).model_copy(
        update={
            "succeeded_count": 1,
            "coverage_ratio": 0.5,
            "failed_symbols": ["sh.600002"],
            "quality_issues": ["incomplete_symbol_coverage"],
        }
    )

    with pytest.raises(ValueError, match="ready publication must be complete"):
        store.save_refresh([first[0].model_copy(update={"close": 999})], fake_ready, publish=True)

    connection = duckdb.connect(str(market_path))
    try:
        published = connection.execute(
            "SELECT symbol, close FROM published_daily_bars ORDER BY symbol"
        ).fetchall()
    finally:
        connection.close()
    assert published == [("sh.600001", first[0].close), ("sh.600002", first[1].close)]
    assert store.published_refresh().run_id == pointer_before.run_id


def test_ready_publication_rejects_active_partial_bar_and_preserves_partition(
    tmp_path: Path,
) -> None:
    market_path = tmp_path / "market.duckdb"
    store = MarketStore(market_path)
    first = [
        _bars("sh.600001", drift=0.003, count=1)[0],
        _bars("sh.600002", drift=0.002, count=1)[0],
    ]
    store.save_refresh(first, _refresh("complete", AS_OF, count=2), publish=True)
    pointer_before = store.published_refresh()
    poisoned = [
        first[0].model_copy(
            update={
                "quality_status": "partial",
                "quality_issues": ["fixture_active_partial"],
            }
        ),
        first[1],
    ]

    with pytest.raises(ValueError, match="active_bar_not_ready"):
        store.save_refresh(
            poisoned,
            _refresh("fake-ready-quality", AS_OF, count=2),
            publish=True,
        )

    connection = duckdb.connect(str(market_path))
    try:
        published = connection.execute(
            "SELECT symbol, quality_status FROM published_daily_bars ORDER BY symbol"
        ).fetchall()
    finally:
        connection.close()
    assert published == [("sh.600001", "ready"), ("sh.600002", "ready")]
    assert store.published_refresh().run_id == pointer_before.run_id


def test_ready_publication_allows_explicit_suspended_placeholder(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    suspended = _bars("sh.600001", drift=0.003, count=1)[0].model_copy(
        update={
            "is_trading": False,
            "is_suspended": True,
            "adjust_factor": None,
            "volume": 0.0,
            "amount": 0.0,
            "quality_status": "partial",
            "quality_issues": ["suspended_placeholder"],
        }
    )

    store.save_refresh(
        [suspended],
        _refresh("suspended-placeholder", AS_OF),
        publish=True,
    )

    assert store.published_refresh().run_id == "suspended-placeholder"


@pytest.mark.parametrize(
    ("updates", "expected_issue"),
    [
        (
            {
                "is_trading": False,
                "is_suspended": False,
                "quality_status": "ready",
                "quality_issues": [],
            },
            "non_trading_bar_not_suspended",
        ),
        (
            {
                "is_trading": False,
                "is_suspended": True,
                "quality_status": "partial",
                "quality_issues": ["unexpected_placeholder_issue"],
            },
            "invalid_suspended_placeholder",
        ),
    ],
)
def test_ready_publication_rejects_malformed_non_trading_placeholders(
    tmp_path: Path,
    updates: dict[str, object],
    expected_issue: str,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    bar = _bars("sh.600001", drift=0.003, count=1)[0].model_copy(update=updates)

    with pytest.raises(ValueError, match=expected_issue):
        store.save_refresh(
            [bar],
            _refresh("invalid-placeholder", AS_OF),
            publish=True,
        )

    assert store.published_refresh() is None


def test_local_publication_requires_one_complete_explicit_date_partition(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    prior = AS_OF - timedelta(days=1)
    multi_date = [
        _bars("sh.600001", drift=0.003, count=1, end=prior)[0],
        _bars("sh.600001", drift=0.003, count=1, end=AS_OF)[0],
    ]

    with pytest.raises(ValueError, match="one explicit trade-date partition"):
        store.save_refresh(
            multi_date,
            _refresh("implicit-multi-date", AS_OF, count=1).model_copy(
                update={"run_kind": "backfill"}
            ),
            publish=True,
        )

    for trade_date, close in ((prior, 10.5), (AS_OF, 11.5)):
        row = _bars("sh.600001", drift=0.003, count=1, end=trade_date)[0].model_copy(
            update={"close": close}
        )
        store.save_refresh(
            [row],
            _refresh(f"explicit-{trade_date}", trade_date),
            publish=True,
        )
    rerun = _bars("sh.600001", drift=0.003, count=1, end=AS_OF)[0].model_copy(
        update={"close": 12.5}
    )
    store.save_refresh([rerun], _refresh("same-day-rerun", AS_OF), publish=True)

    connection = duckdb.connect(str(store.path))
    try:
        rows = connection.execute(
            "SELECT trade_date, close FROM published_daily_bars ORDER BY trade_date"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [(prior, 10.5), (AS_OF, 12.5)]


@pytest.mark.parametrize("corruption", ["partial", "coverage", "missing_run", "row_count"])
def test_publication_audit_corruption_returns_structured_503(
    tmp_path: Path,
    corruption: str,
) -> None:
    sector_api = import_module("backend.app.api.sector")
    market_dir = tmp_path / "market"
    _publish_classification(
        market_dir / "classification.duckdb",
        {"sh.600001": "growth", "sh.600002": "growth"},
    )
    market_path = market_dir / "market.duckdb"
    store = MarketStore(market_path)
    bars = [
        _bars("sh.600001", drift=0.003, count=1)[0],
        _bars("sh.600002", drift=0.002, count=1)[0],
    ]
    store.save_refresh(bars, _refresh("published", AS_OF, count=2), publish=True)
    connection = duckdb.connect(str(market_path))
    try:
        if corruption == "partial":
            connection.execute("UPDATE refresh_runs SET status = 'partial'")
        elif corruption == "coverage":
            connection.execute("UPDATE refresh_runs SET coverage_ratio = 0.5")
        elif corruption == "missing_run":
            connection.execute("DELETE FROM refresh_runs")
        else:
            connection.execute("DELETE FROM published_daily_bars WHERE symbol = 'sh.600002'")
    finally:
        connection.close()
    settings = Settings(
        market_data_dir=market_dir,
        market_database_name=market_path.name,
        classification_database_name="classification.duckdb",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )

    response = _api_get(
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        {
            sector_api.get_sector_rotation_service: lambda: sector_api.get_sector_rotation_service(
                settings
            ),
            sector_api.get_sector_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "market_publication_state_unavailable"


def test_published_reader_rejects_pointer_rollback_to_valid_older_partition(
    tmp_path: Path,
) -> None:
    market_path = tmp_path / "market.duckdb"
    store = MarketStore(market_path)
    older = AS_OF - timedelta(days=1)
    for trade_date, run_id in ((older, "older-run"), (AS_OF, "latest-run")):
        store.save_refresh(
            [_bars("sh.600001", drift=0.003, count=1, end=trade_date)[0]],
            _refresh(run_id, trade_date),
            publish=True,
        )
    connection = duckdb.connect(str(market_path))
    try:
        connection.execute(
            "UPDATE published_snapshots SET run_id = ?, trade_date = ?",
            ["older-run", older],
        )
    finally:
        connection.close()

    with pytest.raises(
        MarketReadUnavailable,
        match="market_publication_state_unavailable",
    ):
        PublishedSnapshotDuckDbMarketReader(market_path).bars_through(
            AS_OF,
            max_sessions=80,
        )


def test_published_reader_rejects_source_tampered_historical_partition(
    tmp_path: Path,
) -> None:
    market_path = tmp_path / "market.duckdb"
    store = MarketStore(market_path)
    older = AS_OF - timedelta(days=1)
    for trade_date, run_id in ((older, "older-run"), (AS_OF, "latest-run")):
        store.save_refresh(
            [_bars("sh.600001", drift=0.003, count=1, end=trade_date)[0]],
            _refresh(run_id, trade_date),
            publish=True,
        )
    connection = duckdb.connect(str(market_path))
    try:
        connection.execute(
            "UPDATE published_daily_bars SET source = 'tampered' WHERE trade_date = ?",
            [older],
        )
    finally:
        connection.close()

    with pytest.raises(
        MarketReadUnavailable,
        match="market_publication_state_unavailable",
    ):
        PublishedSnapshotDuckDbMarketReader(market_path).bars_through(
            AS_OF,
            max_sessions=80,
        )


def test_as_of_ineligible_members_never_rank_or_become_leader_candidates(
    tmp_path: Path,
) -> None:
    classification_path = tmp_path / "classification.duckdb"
    symbols = ["sh.600001", "sh.600002"]
    securities = [
        _security(symbol).model_copy(update={"delist_date": AS_OF, "is_tradable": True})
        for symbol in symbols
    ]
    store = ClassificationStore(classification_path)
    outcome = store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=CLASSIFICATION_DATE,
            observed_at=OBSERVED_AT,
            securities=securities,
            sector_memberships=[_membership(symbol, "delisted") for symbol in symbols],
        )
    )
    assert outcome.promoted is True
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        store,
        _BarsReader(_bars(symbols[0], drift=0.003) + _bars(symbols[1], drift=0.002)),
    )

    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "delisted")

    assert rotation.actual_scope.classification_eligible_symbols == 0
    assert rotation.rankings == []
    assert "classification_ineligible_delisted_excluded" in rotation.quality_issues
    assert leaders.candidates == []
    assert {item.symbol for item in leaders.exclusions} == set(symbols)
    assert all("classification_ineligible_delisted" in item.reasons for item in leaders.exclusions)


def test_qualification_version_changes_candidate_sector_and_result_ids(
    tmp_path: Path,
) -> None:
    sector_service = import_module("backend.app.sector.service")
    classification, _, _ = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "growth", "sh.600002": "growth"},
    )
    bars = [
        *_bars("sh.600001", drift=0.006, amount_drift=0.01),
        *_bars("sh.600002", drift=0.002),
    ]
    v1 = sector_service.SectorRotationService(classification, _BarsReader(bars))
    v9 = sector_service.SectorRotationService(
        classification,
        _BarsReader(bars),
        leader_policy=sector_service.LeaderPolicy(
            qualification_version="research-leader-qualification-v9"
        ),
    )

    rotation_v1 = v1.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation_v9 = v9.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders_v1 = v1.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")
    leaders_v9 = v9.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert leaders_v9.candidates[0].qualification_version.endswith("v9")
    assert leaders_v1.candidates[0].candidate_id != leaders_v9.candidates[0].candidate_id
    assert leaders_v1.result_id != leaders_v9.result_id
    assert rotation_v1.rankings[0].ranking_id != rotation_v9.rankings[0].ranking_id
    assert rotation_v1.result_id != rotation_v9.result_id
    leadership_v9 = {
        item.metric: item.formula_version
        for item in rotation_v9.rankings[0].metric_scores
        if item.metric.startswith("leader_")
    }
    assert all("research-leader-qualification-v9" in value for value in leadership_v9.values())


def test_empty_result_ids_include_complete_policy_identity(
    tmp_path: Path,
) -> None:
    sector_service = import_module("backend.app.sector.service")
    classification, _, _ = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "growth"},
    )
    v1 = sector_service.SectorRotationService(classification, _BarsReader([]))
    changed_sector_weights = dict(sector_service.SectorPolicy().weights)
    changed_sector_weights["relative_strength_5d"] = 0.07
    sector_changed = sector_service.SectorRotationService(
        classification,
        _BarsReader([]),
        sector_policy=sector_service.SectorPolicy(weights=changed_sector_weights),
    )
    leader_changed = sector_service.SectorRotationService(
        classification,
        _BarsReader([]),
        leader_policy=sector_service.LeaderPolicy(
            qualification_version="research-leader-qualification-v9"
        ),
    )

    rotation_v1 = v1.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation_sector_changed = sector_changed.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation_leader_changed = leader_changed.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders_v1 = v1.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")
    leaders_sector_changed = sector_changed.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "growth",
    )
    leaders_leader_changed = leader_changed.leaders(
        AS_OF,
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "growth",
    )

    assert rotation_v1.status == rotation_sector_changed.status == "empty"
    assert rotation_v1.status == rotation_leader_changed.status == "empty"
    assert leaders_v1.status == leaders_sector_changed.status == "empty"
    assert leaders_v1.status == leaders_leader_changed.status == "empty"
    assert rotation_v1.result_id != rotation_sector_changed.result_id
    assert rotation_v1.result_id != rotation_leader_changed.result_id
    assert leaders_v1.result_id != leaders_sector_changed.result_id
    assert leaders_v1.result_id != leaders_leader_changed.result_id


def test_no_candidate_degraded_leader_id_includes_qualification_version(
    tmp_path: Path,
) -> None:
    sector_service = import_module("backend.app.sector.service")
    classification, _, _ = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "growth"},
        security_overrides={"sh.600001": {"tradable": False}},
    )
    bars = _bars("sh.600001", drift=0.003)
    v1 = sector_service.SectorRotationService(classification, _BarsReader(bars))
    v9 = sector_service.SectorRotationService(
        classification,
        _BarsReader(bars),
        leader_policy=sector_service.LeaderPolicy(
            qualification_version="research-leader-qualification-v9"
        ),
    )

    leaders_v1 = v1.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")
    leaders_v9 = v9.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert leaders_v1.status == leaders_v9.status == "degraded"
    assert leaders_v1.candidates == leaders_v9.candidates == []
    assert leaders_v1.result_id != leaders_v9.result_id


def test_sector_score_ties_break_only_by_sector_id_even_with_different_sizes(
    tmp_path: Path,
) -> None:
    mapping = {
        "sh.600001": "alpha",
        "sh.600002": "alpha",
        "sh.600003": "zeta",
        "sh.600004": "zeta",
        "sh.600005": "zeta",
    }
    bars = [bar for symbol in mapping for bar in _bars(symbol, drift=0.003, amount=100, count=65)]
    service, _, _ = _service(tmp_path, mapping, bars)

    result = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)

    assert result.rankings[0].total_score == result.rankings[1].total_score
    assert [item.sector_id for item in result.rankings] == ["alpha", "zeta"]


@pytest.mark.parametrize(
    ("current_bar_updates", "expected_reason"),
    [
        ({"board": "chinext"}, "canonical_board_mismatch"),
        ({"exchange": "sz"}, "canonical_exchange_mismatch"),
        (
            {"security_type": "index", "board": "index"},
            "canonical_security_type_mismatch",
        ),
    ],
)
def test_canonical_classification_scope_mismatch_is_excluded_everywhere(
    tmp_path: Path,
    current_bar_updates: dict[str, object],
    expected_reason: str,
) -> None:
    anchor = _bars("sh.600001", drift=0.003)
    mismatched = _bars("sh.600002", drift=0.02)
    mismatched[-1] = mismatched[-1].model_copy(update=current_bar_updates)
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", "sh.600002": "growth"},
        [*anchor, *mismatched],
    )

    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert [item.symbol for item in leaders.candidates] == ["sh.600001"]
    exclusions = {item.symbol: item.reasons for item in leaders.exclusions}
    assert expected_reason in exclusions["sh.600002"]

    ranking = rotation.rankings[0]
    metrics = {item.metric: item for item in ranking.metric_scores}
    assert rotation.actual_scope.observed_market_symbols == 1
    assert rotation.actual_scope.priced_classified_symbols == 1
    assert ranking.priced_member_count == 1
    assert f"sector.{expected_reason}" in ranking.quality_issues
    assert metrics["relative_strength_20d"].effective_count == 1
    assert metrics["relative_strength_20d"].target_count == 2
    assert metrics["relative_strength_20d"].score is None
    for name in ("leader_count", "leader_diffusion", "leader_persistence_days"):
        assert metrics[name].effective_count == 1
        assert metrics[name].target_count == 2
        assert metrics[name].score is None


@pytest.mark.parametrize(
    ("symbol", "expected_reason"),
    [
        ("sz.300001", "canonical_board_prefix_mismatch"),
        ("sz.301001", "canonical_board_prefix_mismatch"),
        ("sh.688001", "canonical_board_prefix_mismatch"),
        ("sh.689001", "canonical_board_prefix_mismatch"),
        ("sh.999999", "canonical_symbol_prefix_unsupported"),
    ],
)
def test_canonical_symbol_prefix_scope_mismatch_is_excluded_everywhere(
    tmp_path: Path,
    symbol: str,
    expected_reason: str,
) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth", symbol: "growth"},
        [
            *_bars("sh.600001", drift=0.003),
            *_bars(symbol, drift=0.02, board="main"),
        ],
    )

    context = service._read_context(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert [item.symbol for item in leaders.candidates] == ["sh.600001"]
    exclusions = {item.symbol: item.reasons for item in leaders.exclusions}
    assert expected_reason in exclusions[symbol]
    assert symbol not in context.cache.valid_histories
    assert symbol not in service._sector_cache(context, context.memberships).symbols

    ranking = rotation.rankings[0]
    metrics = {item.metric: item for item in ranking.metric_scores}
    assert rotation.actual_scope.observed_market_symbols == 1
    assert rotation.actual_scope.priced_classified_symbols == 1
    assert ranking.priced_member_count == 1
    assert f"sector.{expected_reason}" in ranking.quality_issues
    assert metrics["relative_strength_20d"].effective_count == 1
    for name in ("leader_count", "leader_diffusion", "leader_persistence_days"):
        assert metrics[name].effective_count == 1
        assert metrics[name].target_count == 2


def test_symbol_exchange_prefix_mismatch_is_excluded_when_sources_agree_on_wrong_exchange(
    tmp_path: Path,
) -> None:
    classification, _, _ = _publish_classification(
        tmp_path / "classification.duckdb",
        {"sh.600001": "growth", "sh.600002": "growth"},
        security_overrides={"sh.600002": {"exchange": "sz"}},
    )
    mismatched = [
        bar.model_copy(update={"exchange": "sz"}) for bar in _bars("sh.600002", drift=0.02)
    ]
    sector_service = import_module("backend.app.sector.service")
    service = sector_service.SectorRotationService(
        classification,
        _BarsReader([*_bars("sh.600001", drift=0.003), *mismatched]),
    )

    context = service._read_context(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert [item.symbol for item in leaders.candidates] == ["sh.600001"]
    exclusions = {item.symbol: item.reasons for item in leaders.exclusions}
    assert "canonical_exchange_mismatch" in exclusions["sh.600002"]
    assert "sh.600002" not in context.cache.valid_histories
    assert rotation.actual_scope.observed_market_symbols == 1
    assert rotation.actual_scope.priced_classified_symbols == 1
    assert "sector.canonical_exchange_mismatch" in rotation.rankings[0].quality_issues


def test_supported_main_board_prefixes_and_index_history_remain_compatible(
    tmp_path: Path,
) -> None:
    main_symbols = [
        "sh.600001",
        "sh.601001",
        "sh.603001",
        "sh.605001",
        "sz.000001",
        "sz.001001",
        "sz.002001",
        "sz.003001",
    ]
    index_rows = [
        bar.model_copy(update={"security_type": "index", "board": "index", "adjust_factor": None})
        for bar in _bars("sh.000001", drift=0.001)
    ]
    service, _, _ = _service(
        tmp_path,
        {symbol: "growth" for symbol in main_symbols},
        [
            *(bar for symbol in main_symbols for bar in _bars(symbol, drift=0.003)),
            *index_rows,
        ],
    )

    context = service._read_context(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    rotation = service.rotation(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY)
    leaders = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")

    assert sorted(item.symbol for item in leaders.candidates) == main_symbols
    assert rotation.actual_scope.observed_market_symbols == len(main_symbols)
    assert rotation.actual_scope.priced_classified_symbols == len(main_symbols)
    assert sorted(context.cache.valid_histories) == main_symbols
    assert "sh.000001" not in context.histories
    assert rotation.market_lineage.record_count == (len(main_symbols) + 1) * 65


def test_tradability_metric_formula_uses_contract_spelling(tmp_path: Path) -> None:
    service, _, _ = _service(
        tmp_path,
        {"sh.600001": "growth"},
        _bars("sh.600001", drift=0.003),
    )

    result = service.leaders(AS_OF, TAXONOMY_BAOSTOCK_INDUSTRY, "growth")
    metrics = {item.metric: item for item in result.candidates[0].metric_scores}

    assert metrics["tradability"].formula_version == (
        "classification-and-canonical-current-tradability-v1"
    )
