import asyncio
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

import backend.app.market.normalize as normalize_module
from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market.models import DailyBar
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.store import MarketStore

AS_OF = date(2026, 7, 29)


def _regime_modules():
    return (
        import_module("backend.app.regime.models"),
        import_module("backend.app.regime.service"),
        import_module("backend.app.regime.store"),
    )


def _lineage(models, *, as_of: date = AS_OF):
    return models.SourceLineage(
        source="fixture",
        source_version="fixture-v1",
        earliest_input_date=as_of,
        latest_input_date=as_of,
        record_count=5,
        content_hash="a" * 64,
    )


def _scope(models, *, complete: bool = True):
    expected_boards = ["sse_main", "szse_main", "chinext", "star"]
    expected_indexes = [
        "sh.000001",
        "sz.399001",
        "sh.000300",
        "sh.000905",
        "sh.000852",
        "sz.399006",
    ]
    observed_boards = expected_boards if complete else expected_boards[:2]
    observed_indexes = expected_indexes if complete else expected_indexes[:2]
    observed = len(observed_boards) + len(observed_indexes)
    expected = len(expected_boards) + len(expected_indexes)
    return models.ActualMarketScope(
        expected_boards=expected_boards,
        observed_boards=observed_boards,
        missing_boards=sorted(set(expected_boards) - set(observed_boards)),
        expected_index_series=expected_indexes,
        observed_index_series=observed_indexes,
        missing_index_series=sorted(set(expected_indexes) - set(observed_indexes)),
        board_coverage_ratio=len(observed_boards) / len(expected_boards),
        index_coverage_ratio=len(observed_indexes) / len(expected_indexes),
        coverage_ratio=observed / expected,
        scope_status="full_a_share_capable" if complete else "narrow_provisional",
        can_support_full_a_share_conclusion=complete,
        conclusion_disclaimer=(
            None
            if complete
            else (
                "Observed price history is narrow-scope and cannot support a full A-share "
                "bull/bear conclusion."
            )
        ),
    )


def _component(
    models,
    name: str,
    score: float | None,
    *,
    quality_status: str = "ready",
    missing_inputs: list[str] | None = None,
):
    supporting = []
    contrary = []
    if score is not None and score > 0:
        supporting.append(
            models.EvidenceItem(
                component=name,
                code=f"{name}_positive",
                detail=f"{name} fixture is supportive",
                value=score,
                as_of=AS_OF,
                source="fixture",
            )
        )
    if score is not None and score < 0:
        contrary.append(
            models.EvidenceItem(
                component=name,
                code=f"{name}_negative",
                detail=f"{name} fixture is contrary",
                value=score,
                as_of=AS_OF,
                source="fixture",
            )
        )
    return models.RegimeComponentInput(
        name=name,
        score=score,
        formula_version=f"{name}-fixture-v1",
        quality_status=quality_status,
        missing_inputs=missing_inputs or [],
        supporting_evidence=supporting,
        contrary_evidence=contrary,
        source_lineage=[_lineage(models)],
    )


def _inputs(
    models,
    score: float,
    *,
    complete_scope: bool = True,
    overrides: dict[str, object] | None = None,
):
    components = {
        name: _component(models, name, score)
        for name in ("trend", "breadth", "liquidity", "risk", "leadership")
    }
    components.update(overrides or {})
    return models.MarketRegimeInput(
        as_of=AS_OF,
        data_as_of=AS_OF,
        trend=components["trend"],
        breadth=components["breadth"],
        liquidity=components["liquidity"],
        risk=components["risk"],
        leadership=components["leadership"],
        actual_market_scope=_scope(models, complete=complete_scope),
        source_lineage=[_lineage(models)],
    )


@pytest.mark.parametrize(
    ("score", "strategic_state", "tactical_state"),
    [
        (80.0, "bull", "risk_on"),
        (0.0, "range", "neutral"),
        (-80.0, "bear", "risk_off"),
    ],
)
def test_deterministic_states_cover_bull_range_bear_and_tactical_modes(
    score: float,
    strategic_state: str,
    tactical_state: str,
) -> None:
    models, service_module, _ = _regime_modules()

    result = service_module.MarketRegimeService().evaluate(_inputs(models, score))

    assert result.strategic_state == strategic_state
    assert result.tactical_state == tactical_state
    assert result.total_score == score
    assert result.status == "ready"
    assert result.quality_status == "ready"
    assert result.formula_version == "market-regime-v1"
    assert [item.name for item in result.component_scores] == [
        "trend",
        "breadth",
        "liquidity",
        "risk",
        "leadership",
    ]
    assert all(item.formula_version.endswith("-fixture-v1") for item in result.component_scores)


@pytest.mark.parametrize(
    ("score", "strategic_state", "tactical_state"),
    [
        (25.0, "bull", "risk_on"),
        (-25.0, "bear", "risk_off"),
        (10.0, "range", "risk_on"),
        (-10.0, "range", "risk_off"),
        (9.9999, "range", "neutral"),
        (-9.9999, "range", "neutral"),
    ],
)
def test_threshold_boundaries_are_inclusive_and_replayable(
    score: float,
    strategic_state: str,
    tactical_state: str,
) -> None:
    models, service_module, _ = _regime_modules()

    result = service_module.MarketRegimeService().evaluate(_inputs(models, score))

    assert result.strategic_state == strategic_state
    assert result.tactical_state == tactical_state
    assert result.thresholds == {
        "strategic_bull_min": 25.0,
        "strategic_bear_max": -25.0,
        "tactical_risk_on_min": 10.0,
        "tactical_risk_off_max": -10.0,
    }
    assert result.weights == {
        "trend": 0.3,
        "breadth": 0.25,
        "liquidity": 0.15,
        "risk": 0.2,
        "leadership": 0.1,
    }


def test_missing_inputs_fail_degraded_without_guessing() -> None:
    models, service_module, _ = _regime_modules()
    missing_leadership = _component(
        models,
        "leadership",
        None,
        quality_status="missing",
        missing_inputs=["leadership.sector_persistence", "leadership.leader_diffusion"],
    )
    degraded_liquidity = _component(
        models,
        "liquidity",
        20,
        quality_status="degraded",
        missing_inputs=["liquidity.fund_flow_evidence"],
    )

    result = service_module.MarketRegimeService().evaluate(
        _inputs(
            models,
            40,
            overrides={
                "leadership": missing_leadership,
                "liquidity": degraded_liquidity,
            },
        )
    )

    assert result.status == "degraded"
    assert result.quality_status == "degraded"
    assert result.confidence.level in {"low", "medium"}
    assert result.missing_inputs == [
        "leadership.leader_diffusion",
        "leadership.sector_persistence",
        "liquidity.fund_flow_evidence",
    ]
    leadership = next(item for item in result.component_scores if item.name == "leadership")
    assert leadership.score is None
    assert leadership.weighted_score is None
    assert "missing_component_inputs" in result.quality_issues


def test_narrow_price_scope_is_truthful_and_low_confidence() -> None:
    models, service_module, _ = _regime_modules()

    result = service_module.MarketRegimeService().evaluate(
        _inputs(models, 80, complete_scope=False)
    )

    scope = result.actual_market_scope
    assert result.strategic_state == "bull"
    assert result.status == "degraded"
    assert result.confidence.level == "low"
    assert scope.observed_boards == ["sse_main", "szse_main"]
    assert scope.missing_boards == ["chinext", "star"]
    assert scope.observed_index_series == ["sh.000001", "sz.399001"]
    assert set(scope.missing_index_series) == {
        "sh.000300",
        "sh.000852",
        "sh.000905",
        "sz.399006",
    }
    assert scope.can_support_full_a_share_conclusion is False
    assert scope.scope_status == "narrow_provisional"
    assert "cannot support a full A-share bull/bear conclusion" in (
        scope.conclusion_disclaimer or ""
    )
    assert "narrow_scope_not_full_a_share" in result.quality_issues
    assert "market_scope_missing_expected_boards" in result.quality_issues
    assert "market_scope_missing_expected_index_series" in result.quality_issues


def test_same_inputs_replay_to_same_id_score_and_evidence() -> None:
    models, service_module, _ = _regime_modules()
    inputs = _inputs(models, 35)
    service = service_module.MarketRegimeService()

    first = service.evaluate(inputs)
    second = service.evaluate(inputs.model_copy(deep=True))

    assert first == second
    assert first.result_id == second.result_id
    assert first.total_score == 35
    assert [item.code for item in first.supporting_evidence] == [
        "trend_positive",
        "breadth_positive",
        "liquidity_positive",
        "risk_positive",
        "leadership_positive",
    ]
    assert first.contrary_evidence == []


def _bar(
    trade_date: date,
    symbol: str,
    *,
    security_type: str = "stock",
    board: str = "main",
    close: float = 10,
    amount: float = 10_000,
    pct_change: float = 0,
) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        symbol=symbol,
        security_type=security_type,
        exchange=symbol[:2],
        board=board,
        open=close,
        high=close,
        low=close,
        close=close,
        preclose=close,
        volume=1_000,
        amount=amount,
        turnover_rate=1,
        pct_change=pct_change,
        adjust_factor=None if security_type == "index" else 1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id=f"fixture:{symbol}:{trade_date}",
        ingested_at=datetime(2026, 7, 29, tzinfo=UTC),
        quality_status="ready",
        quality_issues=[],
    )


def test_store_never_accepts_rows_after_requested_as_of() -> None:
    models, service_module, store_module = _regime_modules()

    class FutureLeakingReader:
        def __init__(self) -> None:
            self.calls: list[tuple[date, int]] = []

        def bars_through(self, as_of: date, *, max_sessions: int):
            self.calls.append((as_of, max_sessions))
            return [
                _bar(as_of, "sh.600000"),
                _bar(as_of + timedelta(days=1), "sh.600001", close=999),
            ]

    reader = FutureLeakingReader()
    inputs = store_module.MarketRegimeStore(reader).read(AS_OF)
    result = service_module.MarketRegimeService().evaluate(inputs)

    assert reader.calls == [(AS_OF, store_module.REGIME_LOOKBACK_SESSIONS)]
    assert inputs.data_as_of == AS_OF
    assert all(item.latest_input_date <= AS_OF for item in inputs.source_lineage)
    assert result.as_of == AS_OF
    assert "future_market_rows_discarded" in result.quality_issues
    assert models.EXPECTED_BOARDS == ["sse_main", "szse_main", "chinext", "star"]


def test_service_input_rejects_future_data_lineage() -> None:
    models, _, _ = _regime_modules()
    payload = _inputs(models, 20).model_dump()
    payload["data_as_of"] = AS_OF + timedelta(days=1)

    with pytest.raises(ValidationError, match="data_as_of must not exceed as_of"):
        models.MarketRegimeInput.model_validate(payload)


def test_store_derives_versioned_price_components_and_full_observed_scope() -> None:
    _, service_module, store_module = _regime_modules()
    start = AS_OF - timedelta(days=129)
    stock_specs = [
        ("sh.600000", "main"),
        ("sz.000001", "main"),
        ("sz.300001", "chinext"),
        ("sh.688001", "star"),
    ]
    index_symbols = [
        "sh.000001",
        "sz.399001",
        "sh.000300",
        "sh.000905",
        "sh.000852",
        "sz.399006",
    ]
    bars = []
    for offset in range(130):
        trade_date = start + timedelta(days=offset)
        bars.extend(
            _bar(
                trade_date,
                symbol,
                board=board,
                close=10 + offset,
                amount=10_000 + offset * 100,
                pct_change=1,
            )
            for symbol, board in stock_specs
        )
        bars.extend(
            _bar(
                trade_date,
                symbol,
                security_type="index",
                board="index",
                close=1_000 + offset,
            )
            for symbol in index_symbols
        )

    class InMemoryReader:
        def bars_through(self, as_of: date, *, max_sessions: int):
            assert as_of == AS_OF
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            return bars

    inputs = store_module.MarketRegimeStore(InMemoryReader()).read(AS_OF)
    result = service_module.MarketRegimeService().evaluate(inputs)

    assert inputs.trend.quality_status == "ready"
    assert inputs.breadth.quality_status == "ready"
    assert inputs.liquidity.quality_status == "degraded"
    assert inputs.risk.quality_status == "ready"
    assert inputs.leadership.quality_status == "missing"
    assert inputs.actual_market_scope.can_support_full_a_share_conclusion is True
    assert inputs.actual_market_scope.coverage_ratio == 1
    assert result.strategic_state == "bull"
    assert result.tactical_state == "risk_on"
    assert result.status == "degraded"
    assert result.total_score is not None
    assert result.total_score > 25


def test_canonical_contract_identifies_chinext_star_and_representative_indexes() -> None:
    fields = [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "isST",
    ]
    rows = [
        [
            str(AS_OF),
            "sz.300001",
            "10",
            "11",
            "9",
            "10",
            "10",
            "100",
            "1000",
            "3",
            "1",
            "1",
            "0",
            "0",
        ],
        [
            str(AS_OF),
            "sh.688001",
            "10",
            "11",
            "9",
            "10",
            "10",
            "100",
            "1000",
            "3",
            "1",
            "1",
            "0",
            "0",
        ],
        [
            str(AS_OF),
            "sh.000300",
            "10",
            "11",
            "9",
            "10",
            "10",
            "100",
            "1000",
            "3",
            "",
            "1",
            "0",
            "0",
        ],
    ]
    factors = [
        ["sz.300001", str(AS_OF), "1"],
        ["sh.688001", str(AS_OF), "1"],
    ]

    bars = normalize_baostock_rows(
        fields=fields,
        rows=rows,
        factor_fields=["code", "dividOperateDate", "backAdjustFactor"],
        factor_rows=factors,
    )

    assert normalize_module.DAILY_INDEX_SYMBOLS >= {
        "sh.000001",
        "sz.399001",
        "sh.000300",
        "sh.000905",
        "sh.000852",
        "sz.399006",
    }
    assert [(bar.symbol, bar.security_type, bar.board) for bar in bars] == [
        ("sz.300001", "stock", "chinext"),
        ("sh.688001", "stock", "star"),
        ("sh.000300", "index", "index"),
    ]


def _api_request(path: str, overrides: dict | None = None) -> httpx.Response:
    app.dependency_overrides.update(overrides or {})

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def test_api_rejects_future_shanghai_date_before_store_read() -> None:
    analysis_api = import_module("backend.app.api.analysis")

    class RejectReadStore:
        def read(self, _as_of: date):
            raise AssertionError("future as_of reached regime store")

    response = _api_request(
        "/api/v1/analysis/market-regime?as_of=2026-07-30",
        {
            analysis_api.get_market_regime_store: lambda: RejectReadStore(),
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "future_as_of",
        "as_of": "2026-07-30",
        "today": "2026-07-29",
        "timezone": "Asia/Shanghai",
    }


def test_api_missing_database_is_empty_and_creates_no_filesystem_state(
    tmp_path: Path,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    root = tmp_path / "absent-runtime"
    settings = Settings(
        market_data_dir=root / "market",
        local_control_dir=root / "control",
        local_staging_dir=root / "staging",
        local_lock_dir=root / "locks",
        local_temp_dir=root / "temp",
        user_data_dir=root / "user",
    )

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            get_settings: lambda: settings,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "empty"
    assert payload["quality_status"] == "empty"
    assert payload["total_score"] is None
    assert payload["missing_inputs"] == [
        "breadth.market_breadth",
        "leadership.leader_diffusion",
        "leadership.sector_persistence",
        "liquidity.turnover_history",
        "risk.market_risk",
        "trend.representative_indexes",
    ]
    assert not root.exists()


def test_api_existing_database_executes_select_only_and_preserves_bytes(
    tmp_path: Path,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    database = tmp_path / "market" / "market.duckdb"
    MarketStore(
        database,
        temp_directory=tmp_path / "writer-temp",
    ).upsert_bars([_bar(AS_OF, "sh.600000", pct_change=1)])
    before = database.read_bytes()
    reader_temp = tmp_path / "reader-temp"
    settings = Settings(
        market_data_dir=database.parent,
        market_database_name=database.name,
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=reader_temp,
        user_data_dir=tmp_path / "user",
    )

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            get_settings: lambda: settings,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 200
    assert response.json()["data_as_of"] == str(AS_OF)
    assert database.read_bytes() == before
    assert not reader_temp.exists()
