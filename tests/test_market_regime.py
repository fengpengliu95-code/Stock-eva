import asyncio
import json
import math
import os
import shutil
import sqlite3
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

import backend.app.market.normalize as normalize_module
import backend.app.storage.dataset as dataset_module
from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore

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


def _scope(models, *, all_expected_symbols_observed: bool = True):
    expected_boards = ["sse_main", "szse_main", "chinext", "star"]
    expected_indexes = [
        "sh.000001",
        "sz.399001",
        "sh.000300",
        "sh.000905",
        "sh.000852",
        "sz.399006",
    ]
    observed_boards = expected_boards if all_expected_symbols_observed else expected_boards[:2]
    observed_indexes = expected_indexes if all_expected_symbols_observed else expected_indexes[:2]
    return models.ActualMarketScope(
        expected_boards=expected_boards,
        observed_boards=observed_boards,
        missing_boards=sorted(set(expected_boards) - set(observed_boards)),
        expected_index_series=expected_indexes,
        observed_index_series=observed_indexes,
        missing_index_series=sorted(set(expected_indexes) - set(observed_indexes)),
        coverage_basis="symbol_presence_only",
        coverage_evidence_status="unavailable",
        observed_universe_count=4 if all_expected_symbols_observed else 2,
        index_coverage_ratio=len(observed_indexes) / len(expected_indexes),
        scope_status="narrow_provisional",
        can_support_full_a_share_conclusion=False,
        conclusion_disclaimer=(
            "Observed price history is narrow-scope and cannot support a full A-share "
            "bull/bear conclusion."
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
    all_expected_symbols_observed: bool = True,
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
        actual_market_scope=_scope(
            models,
            all_expected_symbols_observed=all_expected_symbols_observed,
        ),
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
    assert result.status == "degraded"
    assert result.quality_status == "degraded"
    assert result.confidence.level == "low"
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
        _inputs(models, 80, all_expected_symbols_observed=False)
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
    preclose: float | None = None,
    amount: float = 10_000,
    pct_change: float | None = 0,
) -> DailyBar:
    preclose = close if preclose is None else preclose
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
        preclose=preclose,
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


def test_published_reader_projects_only_regime_fields_from_validated_parquet(
    tmp_path: Path,
) -> None:
    _, _, store_module = _regime_modules()
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "temp",
        dataset_root=tmp_path / "dataset",
        staging_root=tmp_path / "staging",
    )
    bar = _bar(AS_OF, "sh.600000", close=12, preclose=10, pct_change=20)
    captured: dict[str, str] = {}

    def query(sql: str, _parameters: list[object]):
        captured["sql"] = sql
        return [
            (
                bar.trade_date,
                bar.symbol,
                bar.security_type,
                bar.exchange,
                bar.board,
                bar.close,
                bar.preclose,
                bar.amount,
                bar.pct_change,
                bar.adjust_factor,
                bar.is_trading,
                bar.is_suspended,
                bar.is_st,
                bar.source_record_id,
                bar.ingested_at,
                bar.quality_status,
                "[]",
            )
        ], []

    reader.cache_snapshot = lambda: SimpleNamespace(identity="fixture")  # type: ignore[method-assign]
    reader.store._query_snapshot = (  # type: ignore[method-assign]
        lambda _snapshot, sql, parameters: query(sql, parameters)
    )

    rows = reader.bars_through(AS_OF, max_sessions=130)

    assert "open" not in captured["sql"]
    assert "volume" not in captured["sql"]
    assert len(rows) == 1
    assert rows[0].model_dump(
        include={
            "trade_date",
            "symbol",
            "security_type",
            "exchange",
            "board",
            "close",
            "preclose",
            "amount",
            "pct_change",
            "adjust_factor",
            "is_trading",
            "is_suspended",
            "is_st",
            "source_record_id",
            "ingested_at",
            "quality_status",
            "quality_issues",
        }
    ) == bar.model_dump(
        include={
            "trade_date",
            "symbol",
            "security_type",
            "exchange",
            "board",
            "close",
            "preclose",
            "amount",
            "pct_change",
            "adjust_factor",
            "is_trading",
            "is_suspended",
            "is_st",
            "source_record_id",
            "ingested_at",
            "quality_status",
            "quality_issues",
        }
    )


def test_published_cache_revalidates_same_generation_object_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cache hit must fail closed when a referenced immutable object changes."""
    _, _, store_module = _regime_modules()
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    publisher = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb", temp_directory=tmp_path / "writer-temp"),
        dataset_root,
        tmp_path / "staging",
    )
    publisher.save_refresh(
        [_bar(AS_OF, "sh.600000")],
        RefreshResult(
            run_id="r1b-cache-fixture",
            requested_date=AS_OF,
            source="baostock",
            status="ready",
            requested_count=1,
            succeeded_count=1,
            coverage_ratio=1,
            started_at=datetime(2026, 7, 29, tzinfo=UTC),
            completed_at=datetime(2026, 7, 29, 1, tzinfo=UTC),
        ),
        publish=True,
        lineage_input={"mode": "legacy"},
    )
    manifest = json.loads((dataset_root / "manifest.json").read_text(encoding="utf-8"))
    parquet = dataset_root / manifest["files"][0]["path"]
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "reader-temp",
        dataset_root=dataset_root,
        staging_root=tmp_path / "staging",
    )
    calls = 0
    original_validation = dataset_module.NasMarketStore._validate_published_object

    def counted_validation(self, descriptor, item, expected_fingerprint) -> None:
        nonlocal calls
        calls += 1
        original_validation(self, descriptor, item, expected_fingerprint)

    monkeypatch.setattr(
        dataset_module.NasMarketStore,
        "_validate_published_object",
        counted_validation,
    )
    reader.store.ensure_readiness()
    assert calls >= 1
    calls = 0
    reader.store._query("SELECT count(*) FROM daily_bars")
    assert calls == 0
    reader.cache_snapshot()
    reader.cache_snapshot()
    assert calls == 2
    second_root = tmp_path / "dataset-second"
    shutil.copytree(dataset_root, second_root)
    second_reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control-second.duckdb",
        control_temp=tmp_path / "reader-temp-second",
        dataset_root=second_root,
        staging_root=tmp_path / "staging-second",
    )
    barrier = threading.Barrier(2)
    barrier_roots: set[Path] = set()
    barrier_lock = threading.Lock()

    def barrier_validation(self, descriptor, item, expected_fingerprint) -> None:
        with barrier_lock:
            first_for_root = self.root not in barrier_roots
            barrier_roots.add(self.root)
        if first_for_root:
            barrier.wait(timeout=2)
        original_validation(self, descriptor, item, expected_fingerprint)

    monkeypatch.setattr(
        dataset_module.NasMarketStore,
        "_validate_published_object",
        barrier_validation,
    )
    errors: list[BaseException] = []

    def strict_snapshot(target) -> None:
        try:
            target.cache_snapshot()
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first_thread = threading.Thread(target=strict_snapshot, args=(reader,))
    second_thread = threading.Thread(target=strict_snapshot, args=(second_reader,))
    first_thread.start()
    second_thread.start()
    first_thread.join(timeout=3)
    second_thread.join(timeout=3)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    monkeypatch.setattr(
        dataset_module.NasMarketStore,
        "_validate_published_object",
        counted_validation,
    )
    store = store_module.MarketRegimeStore(reader)

    store.read(AS_OF)
    original_stat = parquet.stat()
    payload = bytearray(parquet.read_bytes())
    payload[-1] ^= 1
    parquet.write_bytes(payload)
    os.utime(parquet, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    with pytest.raises(store_module.MarketReadUnavailable, match="market_storage_unavailable"):
        store.read(AS_OF)


def test_published_snapshot_binds_query_to_verified_manifest_view(tmp_path: Path) -> None:
    _, _, store_module = _regime_modules()
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}), encoding="utf-8"
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {"dataset": "stock-eva-market", "schema_version": 2, "generation": "empty", "files": []}
        ),
        encoding="utf-8",
    )
    publisher = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb", temp_directory=tmp_path / "writer-temp"),
        dataset_root,
        tmp_path / "staging",
    )

    def publish(close: float, run_id: str) -> None:
        publisher.save_refresh(
            [_bar(AS_OF, "sh.600000", close=close)],
            RefreshResult(
                run_id=run_id,
                requested_date=AS_OF,
                source="baostock",
                status="ready",
                requested_count=1,
                succeeded_count=1,
                coverage_ratio=1,
                started_at=datetime(2026, 7, 29, tzinfo=UTC),
                completed_at=datetime(2026, 7, 29, 1, tzinfo=UTC),
            ),
            publish=True,
            lineage_input={"mode": "legacy"},
        )

    publish(10, "snapshot-a")
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "reader-temp",
        dataset_root=dataset_root,
        staging_root=tmp_path / "staging",
    )
    snapshot_a = reader.cache_snapshot()
    publish(20, "snapshot-b")

    rows, _ = reader.store._query_snapshot(snapshot_a, "SELECT close FROM daily_bars")

    assert rows == [(10.0,)]
    assert snapshot_a.identity != reader.cache_snapshot().identity


def test_snapshot_backfill_fails_closed_before_insert_on_future_bound_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A capture must not turn discarded future rows into a persisted result."""
    _, _, store_module = _regime_modules()
    snapshots = import_module("backend.app.regime.snapshots")
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    publisher = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb", temp_directory=tmp_path / "writer-temp"),
        dataset_root,
        tmp_path / "staging",
    )
    publisher.upsert_bars([_bar(AS_OF, "sh.600000")], lineage_input={"mode": "legacy"})
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "reader-temp",
        dataset_root=dataset_root,
        staging_root=tmp_path / "staging",
    )
    future_bar = _bar(AS_OF + timedelta(days=1), "sh.600000")
    monkeypatch.setattr(
        reader,
        "bars_through_snapshot",
        lambda *_args, **_kwargs: [future_bar],
    )
    snapshot_database = tmp_path / "derived" / "snapshots.sqlite3"
    service = snapshots.RegimeSnapshotCaptureService(
        regime_store=store_module.MarketRegimeStore(reader),
        snapshot_store=snapshots.RegimeSnapshotStore(snapshot_database),
    )

    outcome = service.capture_range(
        AS_OF,
        AS_OF,
        evidence_cutoff_at=datetime(2026, 7, 30, tzinfo=UTC),
    )

    assert outcome.inserted == 0
    assert outcome.errors == 1
    assert not snapshot_database.exists()


def test_snapshot_backfill_selects_twenty_manifest_dates_with_one_checksum_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, store_module = _regime_modules()
    snapshots = import_module("backend.app.regime.snapshots")
    dates = tuple(sorted(AS_OF - timedelta(days=offset) for offset in range(20)))
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    publisher = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb", temp_directory=tmp_path / "writer-temp"),
        dataset_root,
        tmp_path / "staging",
    )
    publisher.upsert_bars(
        [_bar(trade_date, "sh.600000") for trade_date in dates], lineage_input={"mode": "legacy"}
    )
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "reader-temp",
        dataset_root=dataset_root,
        staging_root=tmp_path / "staging",
    )
    original_validation = dataset_module.NasMarketStore._validate_published_object
    calls = 0

    def counted_validation(self, descriptor, item, expected_fingerprint) -> None:
        nonlocal calls
        calls += 1
        original_validation(self, descriptor, item, expected_fingerprint)

    monkeypatch.setattr(
        dataset_module.NasMarketStore,
        "_validate_published_object",
        counted_validation,
    )
    service = snapshots.RegimeSnapshotCaptureService(
        regime_store=store_module.MarketRegimeStore(reader),
        snapshot_store=snapshots.RegimeSnapshotStore(tmp_path / "derived" / "snapshots.sqlite3"),
    )

    outcome = service.capture_range(
        dates[0],
        dates[-1],
        evidence_cutoff_at=datetime(2026, 7, 30, tzinfo=UTC),
    )

    assert outcome.selected_dates == dates
    assert outcome.inserted == 20
    assert outcome.errors == 0
    assert calls == len(dates)


def test_snapshot_backfill_does_not_insert_when_bound_object_changes_during_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a captured object's drift invalidates a bound immutable view.

    A later manifest pointer is intentionally not checked here: it may publish
    a new generation while an in-flight capture safely reads its old objects.
    """
    _, _, store_module = _regime_modules()
    snapshots = import_module("backend.app.regime.snapshots")
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    publisher = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb", temp_directory=tmp_path / "writer-temp"),
        dataset_root,
        tmp_path / "staging",
    )
    publisher.upsert_bars([_bar(AS_OF, "sh.600000")], lineage_input={"mode": "legacy"})
    reader = store_module.PublishedDatasetMarketReader(
        control_path=tmp_path / "control.duckdb",
        control_temp=tmp_path / "reader-temp",
        dataset_root=dataset_root,
        staging_root=tmp_path / "staging",
    )
    original_assert = reader.store._assert_snapshot_unchanged

    def mutate_then_assert(snapshot) -> None:
        with snapshot.paths[0].open("ab") as handle:
            handle.write(b"changed")
        original_assert(snapshot)

    monkeypatch.setattr(reader.store, "_assert_snapshot_unchanged", mutate_then_assert)
    snapshot_database = tmp_path / "derived" / "snapshots.sqlite3"
    service = snapshots.RegimeSnapshotCaptureService(
        regime_store=store_module.MarketRegimeStore(reader),
        snapshot_store=snapshots.RegimeSnapshotStore(snapshot_database),
    )

    outcome = service.capture_range(
        AS_OF,
        AS_OF,
        evidence_cutoff_at=datetime(2026, 7, 30, tzinfo=UTC),
    )

    assert outcome.inserted == 0
    assert outcome.errors == 1
    assert not snapshot_database.exists()


def test_store_reuses_immutable_generation_input_and_reloads_on_pit_key_change() -> None:
    """Avoid re-reading the same published generation for every overview refresh."""
    _, _, store_module = _regime_modules()

    class ImmutableGenerationReader:
        def __init__(self) -> None:
            self.generation = "generation-one"
            self.bars_calls = 0
            self.identity_calls = 0

        def cache_snapshot(self):
            # The real reader performs its manifest/filesystem availability
            # preflight here before allowing a cached result to be returned.
            self.identity_calls += 1
            return SimpleNamespace(identity=f"fixture:{id(self)}:{self.generation}")

        def bars_through_snapshot(
            self, _snapshot, _as_of: date, *, max_sessions: int
        ) -> list[DailyBar]:
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            self.bars_calls += 1
            return _market_bars(130)

    reader = ImmutableGenerationReader()
    store = store_module.MarketRegimeStore(reader)

    first = store.read(AS_OF)
    same_request = store.read(AS_OF)
    pit_request = store.read(AS_OF, known_at=datetime(2026, 7, 30, tzinfo=UTC))
    reader.generation = "generation-two"
    republished = store.read(AS_OF)

    assert first == same_request
    assert reader.identity_calls == 4
    assert reader.bars_calls == 3
    assert pit_request == first
    assert republished == first


def test_store_never_returns_cached_input_when_immutable_preflight_fails() -> None:
    _, _, store_module = _regime_modules()

    class PreflightReader:
        def __init__(self) -> None:
            self.available = True
            self.bars_calls = 0

        def cache_snapshot(self):
            if not self.available:
                raise store_module.MarketReadUnavailable("market_storage_unavailable")
            return SimpleNamespace(identity=f"fixture:{id(self)}:generation-one")

        def bars_through_snapshot(
            self, _snapshot, _as_of: date, *, max_sessions: int
        ) -> list[DailyBar]:
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            self.bars_calls += 1
            return _market_bars(130)

    reader = PreflightReader()
    store = store_module.MarketRegimeStore(reader)
    store.read(AS_OF)
    reader.available = False

    with pytest.raises(store_module.MarketReadUnavailable, match="market_storage_unavailable"):
        store.read(AS_OF)
    assert reader.bars_calls == 1


def test_service_input_rejects_future_data_lineage() -> None:
    models, _, _ = _regime_modules()
    payload = _inputs(models, 20).model_dump()
    payload["data_as_of"] = AS_OF + timedelta(days=1)

    with pytest.raises(ValidationError, match="data_as_of must not exceed as_of"):
        models.MarketRegimeInput.model_validate(payload)


def _market_bars(session_count: int) -> list[DailyBar]:
    start = AS_OF - timedelta(days=session_count - 1)
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
    bars: list[DailyBar] = []
    for offset in range(session_count):
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
    return bars


@pytest.mark.parametrize("session_count", [21, 130])
def test_symbol_presence_never_proves_authoritative_full_a_scope(
    session_count: int,
) -> None:
    _, service_module, store_module = _regime_modules()
    bars = _market_bars(session_count)

    class InMemoryReader:
        def bars_through(self, as_of: date, *, max_sessions: int):
            assert as_of == AS_OF
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            return bars

    inputs = store_module.MarketRegimeStore(InMemoryReader()).read(AS_OF)
    result = service_module.MarketRegimeService().evaluate(inputs)

    scope = inputs.actual_market_scope
    assert scope.observed_boards == ["chinext", "sse_main", "star", "szse_main"]
    assert scope.observed_index_series == [
        "sh.000001",
        "sh.000300",
        "sh.000852",
        "sh.000905",
        "sz.399001",
        "sz.399006",
    ]
    assert scope.missing_boards == []
    assert scope.missing_index_series == []
    assert scope.index_coverage_ratio == 1
    assert scope.coverage_basis == "symbol_presence_only"
    assert scope.coverage_evidence_status == "unavailable"
    assert scope.observed_universe_count == 4
    assert scope.can_support_full_a_share_conclusion is False
    assert scope.scope_status == "narrow_provisional"
    assert result.status == "degraded"
    assert result.confidence.level == "low"
    assert "market_scope_authoritative_coverage_unavailable" in result.quality_issues


def test_risk_requires_60_valid_index_sessions_for_drawdown() -> None:
    _, _, store_module = _regime_modules()

    class InMemoryReader:
        def bars_through(self, _as_of: date, *, max_sessions: int):
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            return _market_bars(21)

    inputs = store_module.MarketRegimeStore(InMemoryReader()).read(AS_OF)

    assert inputs.risk.score is not None
    assert inputs.risk.quality_status == "degraded"
    assert "risk.index_drawdown_60d_warmup" in inputs.risk.missing_inputs
    assert "index_volatility_20d" in {
        item.code
        for item in (
            *inputs.risk.supporting_evidence,
            *inputs.risk.contrary_evidence,
        )
    }
    assert "index_drawdown_60d" not in {
        item.code
        for item in (
            *inputs.risk.supporting_evidence,
            *inputs.risk.contrary_evidence,
        )
    }


def test_risk_is_ready_at_60_valid_index_sessions() -> None:
    _, _, store_module = _regime_modules()

    class InMemoryReader:
        def bars_through(self, _as_of: date, *, max_sessions: int):
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            return _market_bars(60)

    inputs = store_module.MarketRegimeStore(InMemoryReader()).read(AS_OF)

    assert inputs.risk.score is not None
    assert inputs.risk.quality_status == "ready"
    assert inputs.risk.missing_inputs == []
    assert "index_drawdown_60d" in {
        item.code
        for item in (
            *inputs.risk.supporting_evidence,
            *inputs.risk.contrary_evidence,
        )
    }


def test_zero_preclose_excludes_finite_provider_return_from_breadth_and_risk() -> None:
    _, service_module, store_module = _regime_modules()
    bars = [
        bar.model_copy(update={"preclose": 0, "pct_change": 0})
        if bar.trade_date == AS_OF and bar.security_type == "stock"
        else bar
        for bar in _market_bars(130)
    ]

    class InMemoryReader:
        def bars_through(self, _as_of: date, *, max_sessions: int):
            assert max_sessions == store_module.REGIME_LOOKBACK_SESSIONS
            return bars

    inputs = store_module.MarketRegimeStore(InMemoryReader()).read(AS_OF)
    result = service_module.MarketRegimeService().evaluate(inputs)

    assert inputs.breadth.quality_status != "ready"
    assert inputs.risk.quality_status != "ready"
    assert "breadth.invalid_return_input_excluded" in inputs.breadth.quality_issues
    assert "risk.invalid_return_input_excluded" in inputs.risk.quality_issues
    assert "breadth.invalid_return_input_excluded" in result.quality_issues
    assert "risk.invalid_return_input_excluded" in result.quality_issues
    assert "advancing_ratio" not in {
        item.code
        for item in (
            *inputs.breadth.supporting_evidence,
            *inputs.breadth.contrary_evidence,
        )
    }
    assert "cross_section_dispersion" not in {
        item.code
        for item in (
            *inputs.risk.supporting_evidence,
            *inputs.risk.contrary_evidence,
        )
    }


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


def _api_request(
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


def test_snapshot_api_reads_exact_persisted_result_without_recomputation(
    tmp_path: Path,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    snapshots = import_module("backend.app.regime.snapshots")
    result = (
        import_module("backend.app.regime.service")
        .MarketRegimeService()
        .evaluate(_inputs(import_module("backend.app.regime.models"), 0))
    )
    snapshot_store = snapshots.RegimeSnapshotStore(tmp_path / "control" / "snapshots.sqlite3")
    persisted, _ = snapshot_store.capture(
        snapshots.SnapshotCaptureRequest(
            result=result,
            capture_mode="post_hoc_backfill",
            evidence_cutoff_at=datetime(2026, 7, 29, tzinfo=UTC),
            dataset_generation="generation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            dataset_identity="/private/dataset/fixture",
        )
    )

    response = _api_request(
        f"/api/v1/analysis/market-regime/snapshots/{AS_OF}",
        {
            analysis_api.get_regime_snapshot_store: lambda: snapshot_store,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 200
    assert response.json()["snapshot_id"] == persisted.snapshot_id
    assert response.json()["result"] == result.model_dump(mode="json")


@pytest.mark.parametrize("failure", ["schema", "payload", "hash", "future_lineage", "lock"])
def test_snapshot_api_failures_are_sanitized_and_read_only(
    tmp_path: Path,
    failure: str,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    snapshots = import_module("backend.app.regime.snapshots")
    result = (
        import_module("backend.app.regime.service")
        .MarketRegimeService()
        .evaluate(_inputs(import_module("backend.app.regime.models"), 0))
    )
    snapshot_store = snapshots.RegimeSnapshotStore(tmp_path / "control" / "snapshots.sqlite3")
    snapshot_store.capture(
        snapshots.SnapshotCaptureRequest(
            result=result,
            capture_mode="post_hoc_backfill",
            evidence_cutoff_at=datetime(2026, 7, 29, tzinfo=UTC),
            dataset_generation="generation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            dataset_identity="/private/dataset/fixture",
        )
    )
    before = snapshot_store.path.read_bytes()
    connection = sqlite3.connect(snapshot_store.path)
    if failure == "schema":
        connection.execute("DROP TABLE regime_snapshots")
        connection.commit()
        before = snapshot_store.path.read_bytes()
    elif failure == "payload":
        connection.execute("UPDATE regime_snapshots SET result_payload = '[]'")
        connection.commit()
        before = snapshot_store.path.read_bytes()
    elif failure == "hash":
        connection.execute("UPDATE regime_snapshots SET content_hash = ?", ["0" * 64])
        connection.commit()
        before = snapshot_store.path.read_bytes()
    elif failure == "future_lineage":
        payload = json.loads(
            connection.execute("SELECT result_payload FROM regime_snapshots").fetchone()[0]
        )
        payload["source_lineage"][0]["latest_input_date"] = "2026-07-30"
        connection.execute(
            "UPDATE regime_snapshots SET result_payload = ?",
            [json.dumps(payload)],
        )
        connection.commit()
        before = snapshot_store.path.read_bytes()
    else:
        connection.execute("BEGIN EXCLUSIVE")

    try:
        response = _api_request(
            f"/api/v1/analysis/market-regime/snapshots/{AS_OF}",
            {
                analysis_api.get_regime_snapshot_store: lambda: snapshot_store,
                analysis_api.get_regime_today: lambda: AS_OF,
            },
            raise_app_exceptions=False,
        )
    finally:
        if failure == "lock":
            connection.rollback()
        connection.close()

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "regime_snapshot_store_unavailable",
        "storage_status": "unavailable",
    }
    assert snapshot_store.path.read_bytes() == before


@pytest.mark.parametrize("path", ["not-a-date", "2026-07-30"])
def test_snapshot_api_rejects_invalid_or_future_before_reader(path: str) -> None:
    analysis_api = import_module("backend.app.api.analysis")

    class RejectReader:
        def read_exact(self, *_args):
            raise AssertionError("invalid request reached snapshot reader")

    response = _api_request(
        f"/api/v1/analysis/market-regime/snapshots/{path}",
        {
            analysis_api.get_regime_snapshot_store: lambda: RejectReader(),
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 422


def test_api_missing_database_is_empty_and_creates_no_filesystem_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )
    hostile_dataset = tmp_path / "hostile-dataset"
    hostile_dataset.mkdir()
    (hostile_dataset / "manifest.json").write_text("{not-json", encoding="utf-8")
    monkeypatch.setenv(
        "STOCK_EVA_LOCAL_MARKET_DATASET_ROOT",
        str(hostile_dataset),
    )
    get_settings.cache_clear()

    regime_store = analysis_api.get_market_regime_store(settings)

    try:
        response = _api_request(
            f"/api/v1/analysis/market-regime?as_of={AS_OF}",
            {
                analysis_api.get_market_regime_store: lambda: regime_store,
                analysis_api.get_regime_today: lambda: AS_OF,
            },
            raise_app_exceptions=False,
        )
    finally:
        get_settings.cache_clear()

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
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )
    regime_store = analysis_api.get_market_regime_store(settings)

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            analysis_api.get_market_regime_store: lambda: regime_store,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
    )

    assert response.status_code == 200
    assert response.json()["data_as_of"] == str(AS_OF)
    assert database.read_bytes() == before
    assert not reader_temp.exists()


@pytest.mark.parametrize(
    ("dataset_setting", "create_root"),
    [
        ("local_market_dataset_root", False),
        ("nas_market_dataset_root", False),
        ("local_market_dataset_root", True),
        ("nas_market_dataset_root", True),
    ],
)
def test_api_missing_dataset_root_or_manifest_is_unavailable_without_writes(
    tmp_path: Path,
    dataset_setting: str,
    create_root: bool,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    runtime = tmp_path / "absent-runtime"
    dataset_root = tmp_path / f"{dataset_setting}-dataset"
    if create_root:
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
    regime_store = analysis_api.get_market_regime_store(settings)

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            analysis_api.get_market_regime_store: lambda: regime_store,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "market_storage_unavailable",
        "storage_status": "unavailable",
    }
    assert not runtime.exists()
    if create_root:
        assert list(dataset_root.iterdir()) == []
    else:
        assert not dataset_root.exists()


def test_api_corrupt_existing_dataset_manifest_remains_explicit_failure(
    tmp_path: Path,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
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
    regime_store = analysis_api.get_market_regime_store(settings)

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            analysis_api.get_market_regime_store: lambda: regime_store,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "market_storage_unavailable"
    assert manifest.read_text(encoding="utf-8") == "{not-json"
    assert not runtime.exists()


def test_api_non_finite_pct_change_falls_back_or_excludes_without_500(
    tmp_path: Path,
) -> None:
    analysis_api = import_module("backend.app.api.analysis")
    database = tmp_path / "market" / "market.duckdb"
    MarketStore(
        database,
        temp_directory=tmp_path / "writer-temp",
    ).upsert_bars(
        [
            _bar(
                AS_OF,
                "sh.600000",
                close=11,
                preclose=10,
                pct_change=float("nan"),
            ),
            _bar(
                AS_OF,
                "sz.000001",
                close=9,
                preclose=10,
                pct_change=float("inf"),
            ),
            _bar(
                AS_OF,
                "sz.300001",
                close=10,
                preclose=0,
                pct_change=float("-inf"),
            ),
        ]
    )
    settings = Settings(
        market_data_dir=database.parent,
        market_database_name=database.name,
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "reader-temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )
    regime_store = analysis_api.get_market_regime_store(settings)

    response = _api_request(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        {
            analysis_api.get_market_regime_store: lambda: regime_store,
            analysis_api.get_regime_today: lambda: AS_OF,
        },
        raise_app_exceptions=False,
    )

    assert response.status_code == 200
    payload = response.json()
    assert "breadth.pct_change_fallback_used" in payload["quality_issues"]
    assert "breadth.invalid_return_input_excluded" in payload["quality_issues"]
    assert "risk.pct_change_fallback_used" in payload["quality_issues"]
    assert "risk.invalid_return_input_excluded" in payload["quality_issues"]

    def assert_finite(value) -> None:
        if isinstance(value, float):
            assert math.isfinite(value)
        elif isinstance(value, list):
            for item in value:
                assert_finite(item)
        elif isinstance(value, dict):
            for item in value.values():
                assert_finite(item)

    assert_finite(payload)


@pytest.mark.parametrize(
    "non_finite",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
    ],
)
def test_regime_models_reject_non_finite_float_and_decimal(non_finite) -> None:
    models, _, _ = _regime_modules()
    payload = _inputs(models, 20).model_dump()
    payload["trend"]["score"] = non_finite

    with pytest.raises(ValidationError):
        models.MarketRegimeInput.model_validate(payload)

    payload = _inputs(models, 20).model_dump()
    payload["trend"]["supporting_evidence"][0]["value"] = non_finite

    with pytest.raises(ValidationError):
        models.MarketRegimeInput.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("coverage_basis", "authoritative_universe_audit"),
        ("coverage_evidence_status", "available"),
        ("scope_status", "full_a_share_capable"),
        ("can_support_full_a_share_conclusion", True),
        ("expected_universe_count", 4),
        ("board_coverage_ratio", 1),
        ("coverage_ratio", 1),
    ],
)
def test_scope_model_rejects_r1b_full_a_promotion_fields(
    field: str,
    value,
) -> None:
    models, _, _ = _regime_modules()
    payload = _scope(models).model_dump()
    payload[field] = value

    with pytest.raises(ValidationError):
        models.ActualMarketScope.model_validate(payload)


def test_r1b_scope_rejects_complete_caller_supplied_authoritative_claim() -> None:
    models, _, _ = _regime_modules()
    payload = _scope(models).model_dump()
    payload.update(
        {
            "coverage_basis": "authoritative_universe_audit",
            "expected_universe_count": 4,
            "board_coverage_ratio": 1,
            "coverage_ratio": 1,
            "scope_status": "full_a_share_capable",
            "can_support_full_a_share_conclusion": True,
            "conclusion_disclaimer": None,
        }
    )

    with pytest.raises(ValidationError):
        models.ActualMarketScope.model_validate(payload)
