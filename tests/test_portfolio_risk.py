import asyncio
import hashlib
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import ValidationError

from backend.app.api import portfolio_risk as portfolio_api
from backend.app.classification.models import (
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    CoverageAudit,
    GenerationSummary,
    MarketScope,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.classification.store import ClassificationReadSnapshot, ClassificationStore
from backend.app.main import app
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.portfolio.ledger import (
    PortfolioLedger,
    PortfolioLedgerConflict,
    PortfolioLedgerUnavailable,
)
from backend.app.portfolio.models import (
    PortfolioDailySnapshotCreate,
    PortfolioDailySnapshotUpdate,
    PortfolioPositionInput,
)
from backend.app.portfolio.risk import PortfolioRiskPolicy, PortfolioRiskService
from backend.app.regime.store import PublishedSnapshotDuckDbMarketReader

AS_OF = date(2026, 7, 29)
EARLIER = AS_OF - timedelta(days=1)
RECORDED_AT = datetime(2026, 7, 28, 10, tzinfo=UTC)


def _filesystem_state(root: Path) -> dict[str, tuple[str, int, int]]:
    state: dict[str, tuple[str, int, int]] = {}
    for entry in [root, *sorted(root.rglob("*"))]:
        relative = "." if entry == root else str(entry.relative_to(root))
        stat = entry.stat()
        digest = hashlib.sha256(entry.read_bytes()).hexdigest() if entry.is_file() else "dir"
        state[relative] = (digest, stat.st_mtime_ns, stat.st_size)
    return state


def _database_family_state(path: Path) -> dict[str, tuple[str, int, int]]:
    return {
        candidate.name: (
            hashlib.sha256(candidate.read_bytes()).hexdigest(),
            candidate.stat().st_mtime_ns,
            candidate.stat().st_size,
        )
        for candidate in sorted(path.parent.glob(f"{path.name}*"))
    }


def _position(
    symbol: str = "sh.600001",
    *,
    quantity: str = "10",
    avg_cost: str | None = "90",
) -> PortfolioPositionInput:
    return PortfolioPositionInput(
        symbol=symbol,
        quantity=Decimal(quantity),
        avg_cost=None if avg_cost is None else Decimal(avg_cost),
    )


def _create(
    *,
    as_of: date = AS_OF,
    cash: str | None = "8000",
    positions: list[PortfolioPositionInput] | None = None,
    manual_nav: str | None = "9000",
) -> PortfolioDailySnapshotCreate:
    return PortfolioDailySnapshotCreate(
        as_of=as_of,
        cash=None if cash is None else Decimal(cash),
        positions=positions if positions is not None else [_position()],
        manual_nav=None if manual_nav is None else Decimal(manual_nav),
    )


def _update(
    *,
    expected_revision: int,
    as_of: date = AS_OF,
    cash: str | None = "7000",
    positions: list[PortfolioPositionInput] | None = None,
    manual_nav: str | None = "8000",
) -> PortfolioDailySnapshotUpdate:
    return PortfolioDailySnapshotUpdate(
        as_of=as_of,
        cash=None if cash is None else Decimal(cash),
        positions=positions if positions is not None else [_position()],
        manual_nav=None if manual_nav is None else Decimal(manual_nav),
        expected_revision=expected_revision,
    )


def _bars(
    symbol: str,
    *,
    count: int = 20,
    end: date = AS_OF,
    close: float = 100,
    spread: float = 2,
    suspended_last: bool = False,
    bad_quality_last: bool = False,
) -> list[DailyBar]:
    rows = []
    for index in range(count):
        trade_date = end - timedelta(days=count - index - 1)
        last = index == count - 1
        rows.append(
            DailyBar(
                trade_date=trade_date,
                symbol=symbol,
                security_type="stock",
                exchange=symbol.split(".", 1)[0],
                board="main",
                open=close,
                high=close + spread,
                low=close - spread,
                close=close,
                preclose=close,
                volume=100_000,
                amount=10_000_000,
                turnover_rate=1,
                pct_change=0,
                adjust_factor=1,
                is_trading=not (last and suspended_last),
                is_suspended=last and suspended_last,
                is_st=False,
                source="baostock",
                source_record_id=f"{symbol}:{trade_date}",
                ingested_at=datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC),
                quality_status="partial" if last and bad_quality_last else "ready",
                quality_issues=["fixture_bad_quality"] if last and bad_quality_last else [],
            )
        )
    return rows


def _refresh(trade_date: date, *, count: int = 1) -> RefreshResult:
    timestamp = datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC)
    return RefreshResult(
        run_id=f"ready-{trade_date}",
        requested_date=trade_date,
        source="baostock",
        status="ready",
        requested_count=count,
        succeeded_count=count,
        coverage_ratio=1,
        started_at=timestamp,
        completed_at=timestamp,
    )


def _publish_bars(store: MarketStore, bars: list[DailyBar]) -> None:
    by_date: dict[date, list[DailyBar]] = {}
    for bar in bars:
        by_date.setdefault(bar.trade_date, []).append(bar)
    for trade_date, partition in sorted(by_date.items()):
        store.save_refresh(
            partition,
            _refresh(trade_date, count=len(partition)),
            publish=True,
        )


class _MarketReader:
    def __init__(self, bars: list[DailyBar]) -> None:
        self.bars = bars
        self.calls: list[tuple[date, int]] = []

    def bars_through(self, as_of: date, *, max_sessions: int) -> list[DailyBar]:
        self.calls.append((as_of, max_sessions))
        return list(self.bars)


def _security(symbol: str, *, snapshot_date: date = EARLIER) -> SecurityMasterRecord:
    return SecurityMasterRecord(
        record_id=f"security:{snapshot_date}:{symbol}",
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        name=f"Fixture {symbol}",
        exchange=symbol.split(".", 1)[0],
        board="main",
        security_type="stock",
        list_date=date(2000, 1, 1),
        delist_date=None,
        is_tradable=True,
        listing_status="1",
        daily_trade_status="1",
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=date(2000, 1, 1),
        effective_to=None,
        observed_at=datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC),
        source_record_id=f"security:{symbol}",
        lineage_hash=f"security:{symbol}:{snapshot_date}",
        price_available=True,
    )


def _membership(
    symbol: str,
    sector_id: str,
    *,
    snapshot_date: date = EARLIER,
) -> SectorMembershipRecord:
    return SectorMembershipRecord(
        record_id=f"membership:{snapshot_date}:{symbol}",
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
        taxonomy_name="BaoStock industryClassification",
        sector_id=sector_id,
        sector_name=sector_id.title(),
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        raw_industry=sector_id.title(),
        raw_classification="fixture",
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=snapshot_date,
        effective_to=None,
        observed_at=datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC),
        source_record_id=f"membership:{symbol}",
        lineage_hash=f"membership:{symbol}:{sector_id}:{snapshot_date}",
    )


class _ClassificationReader:
    def __init__(self, snapshot: ClassificationReadSnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[date, bool, str | None]] = []

    def read_snapshot(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
        include_securities: bool = False,
        taxonomy_id: str | None = None,
    ) -> ClassificationReadSnapshot:
        del known_at
        self.calls.append((as_of, include_securities, taxonomy_id))
        return self.snapshot


def _classification(
    sector_by_symbol: dict[str, str],
    *,
    coverage: float = 1,
    snapshot_date: date = EARLIER,
) -> _ClassificationReader:
    securities = [_security(symbol, snapshot_date=snapshot_date) for symbol in sector_by_symbol]
    memberships = [
        _membership(symbol, sector, snapshot_date=snapshot_date)
        for symbol, sector in sector_by_symbol.items()
    ]
    generation_id = f"classification-fixture-{snapshot_date}"
    audit = CoverageAudit(
        status="ready" if coverage >= 0.95 else "degraded",
        as_of=snapshot_date,
        generation_id=generation_id,
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
        eligible_count=max(1, len(securities)),
        mapped_count=int(max(1, len(securities)) * coverage),
        coverage_ratio=coverage,
        unmapped_symbols=[],
        exclusion_reasons={},
        source_lineage=["fixture@v1"],
    )
    generation = GenerationSummary(
        generation_id=generation_id,
        sequence=1,
        source="fixture",
        source_version="v1",
        source_snapshot_date=snapshot_date,
        observed_at=datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC),
        row_counts={
            "security_master_history": len(securities),
            "index_component_history": 0,
            "sector_membership_history": len(memberships),
        },
        coverage_audits=[audit],
        market_scope=MarketScope(
            covered_markets=["SSE", "SZSE"],
            covered_boards=["main"],
            eligible_markets=["SSE", "SZSE"],
            eligible_boards=["main", "chinext", "star"],
            excluded_markets=["BSE"],
            excluded_security_types=[],
            price_coverage_status="verified",
        ),
    )
    return _ClassificationReader(
        ClassificationReadSnapshot(
            ready_generation_id=generation_id,
            generation=generation,
            securities=securities,
            security_snapshot_date=snapshot_date,
            index_components=[],
            index_snapshot_date=None,
            sector_memberships=memberships,
            sector_snapshot_date=snapshot_date,
        )
    )


class _RegimeReader:
    def __init__(
        self,
        strategic: str = "bull",
        tactical: str = "risk_on",
        *,
        status: str = "degraded",
        confidence: float = 0,
        as_of: date = AS_OF,
    ) -> None:
        self.result = SimpleNamespace(
            result_id=f"regime-{strategic}-{tactical}-{status}-{as_of}",
            formula_version="market-regime-v1",
            as_of=as_of,
            data_as_of=as_of,
            status=status,
            strategic_state=strategic,
            tactical_state=tactical,
            confidence=SimpleNamespace(
                value=confidence,
                level="high" if confidence >= 0.9 else "medium" if confidence >= 0.5 else "low",
            ),
            actual_market_scope=SimpleNamespace(
                scope_status="narrow_provisional",
                can_support_full_a_share_conclusion=False,
            ),
        )
        self.calls: list[date] = []

    def read(self, as_of: date, *, known_at: datetime | None = None):
        del known_at
        self.calls.append(as_of)
        return self.result


def _service(
    tmp_path: Path,
    *,
    snapshot: PortfolioDailySnapshotCreate | None = None,
    bars: list[DailyBar] | None = None,
    sectors: dict[str, str] | None = None,
    coverage: float = 1,
    regime: _RegimeReader | None = None,
    policy: PortfolioRiskPolicy | None = None,
) -> tuple[PortfolioRiskService, PortfolioLedger]:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    if snapshot is not None:
        ledger.create(snapshot)
    service = PortfolioRiskService(
        ledger,
        _MarketReader(bars or []),
        _classification(sectors or {}, coverage=coverage),
        regime or _RegimeReader(),
        policy=policy,
    )
    return service, ledger


def test_ledger_is_idempotent_append_only_revisioned_and_point_in_time(tmp_path: Path) -> None:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    first = ledger.create(_create(as_of=EARLIER))
    repeated = ledger.create(_create(as_of=EARLIER))

    assert first.write_status == "created"
    assert repeated.write_status == "idempotent"
    assert repeated.snapshot == first.snapshot
    assert first.snapshot.revision == 1

    with pytest.raises(PortfolioLedgerConflict, match="expected_revision"):
        ledger.create(_create(as_of=EARLIER, cash="7000", manual_nav="8000"))

    second = ledger.revise(_update(as_of=EARLIER, expected_revision=1))
    assert second.write_status == "revised"
    assert second.snapshot.revision == 2
    assert second.snapshot.previous_revision == 1
    assert ledger.read_revision(EARLIER, 1) == first.snapshot
    assert ledger.read_selected(EARLIER) == second.snapshot

    with pytest.raises(PortfolioLedgerConflict, match="revision conflict"):
        ledger.revise(_update(as_of=EARLIER, expected_revision=1, cash="6000"))

    future = ledger.create(_create(as_of=AS_OF, cash="6000", manual_nav="7000"))
    assert ledger.read_selected(EARLIER) == second.snapshot
    assert ledger.read_selected(AS_OF) == future.snapshot
    assert [item.as_of for item in ledger.read_history(EARLIER)] == [EARLIER]
    assert ledger.read_selected(EARLIER - timedelta(days=1)) is None


def test_ledger_never_treats_historical_content_as_an_idempotent_retry(
    tmp_path: Path,
) -> None:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    original = _create(cash="8000", manual_nav="9000")
    current = _update(expected_revision=1, cash="7000", manual_nav="8000")
    ledger.create(original)
    revised = ledger.revise(current)

    with pytest.raises(PortfolioLedgerConflict, match="revision conflict"):
        ledger.revise(
            PortfolioDailySnapshotUpdate(
                **original.model_dump(),
                expected_revision=1,
            )
        )
    with pytest.raises(PortfolioLedgerConflict, match="expected_revision"):
        ledger.create(original)

    retry = ledger.revise(current)
    assert retry.write_status == "idempotent"
    assert retry.snapshot == revised.snapshot

    revert = PortfolioDailySnapshotUpdate(
        **original.model_dump(),
        expected_revision=2,
    )
    reverted = ledger.revise(revert)
    assert reverted.write_status == "revised"
    assert reverted.snapshot.revision == 3
    assert reverted.snapshot.content_hash == ledger.read_revision(AS_OF, 1).content_hash
    assert ledger.revise(revert).snapshot == reverted.snapshot


def test_ledger_selection_uses_recorded_at_cutoff_and_normalizes_to_utc(
    tmp_path: Path,
) -> None:
    recorded = iter(
        [
            datetime(2026, 7, 29, 12, tzinfo=UTC),
            datetime(2026, 8, 1, 12, tzinfo=UTC),
        ]
    )
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: next(recorded))
    first = ledger.create(_create(cash="8000", manual_nav="9000"))
    second = ledger.revise(_update(expected_revision=1, cash="7000", manual_nav="8000"))

    selected = ledger.read_selected(AS_OF)
    history = ledger.read_history(AS_OF)

    assert selected == first.snapshot
    assert selected != second.snapshot
    assert history == [first.snapshot]
    assert ledger.read_revision(AS_OF, 2) is None
    assert selected.recorded_at == datetime(2026, 7, 29, 12, tzinfo=UTC)


def test_checkpointed_ledger_get_preserves_complete_source_tree(
    tmp_path: Path,
) -> None:
    ledger = PortfolioLedger(tmp_path / "private" / "user.sqlite3", clock=lambda: RECORDED_AT)
    created = ledger.create(_create())
    before = _filesystem_state(tmp_path)

    selected = ledger.read_selected(AS_OF)

    assert selected == created.snapshot
    assert _filesystem_state(tmp_path) == before


def test_ledger_get_fails_closed_on_wrong_wal_mode_without_touching_tree(
    tmp_path: Path,
) -> None:
    database = tmp_path / "private" / "user.sqlite3"
    ledger = PortfolioLedger(database, clock=lambda: RECORDED_AT)
    ledger.create(_create())
    writer = sqlite3.connect(database)
    try:
        writer.execute("PRAGMA journal_mode = WAL")
        writer.execute("CREATE TABLE concurrent_writer_probe (value INTEGER)")
        writer.execute("INSERT INTO concurrent_writer_probe VALUES (1)")
        writer.commit()
        assert database.with_name(f"{database.name}-wal").stat().st_size > 0
        before = _filesystem_state(tmp_path)

        with pytest.raises(PortfolioLedgerUnavailable, match="rollback-journal"):
            ledger.read_selected(AS_OF)

        assert _filesystem_state(tmp_path) == before
    finally:
        writer.close()


def test_portfolio_database_is_separate_delete_mode_and_shared_wal_is_untouched(
    tmp_path: Path,
) -> None:
    user_dir = tmp_path / "user"
    user_dir.mkdir()
    shared = user_dir / "stock_eva_user.sqlite3"
    portfolio = user_dir / "stock_eva_portfolio.sqlite3"
    shared_connection = sqlite3.connect(shared)
    try:
        assert shared_connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
        shared_connection.execute("CREATE TABLE positions (id TEXT PRIMARY KEY)")
        shared_connection.execute("INSERT INTO positions VALUES ('preserve')")
        shared_connection.commit()
        shared_before = _database_family_state(shared)

        ledger = PortfolioLedger(portfolio, clock=lambda: RECORDED_AT)
        created = ledger.create(_create())
        assert ledger.read_selected(AS_OF) == created.snapshot

        assert _database_family_state(shared) == shared_before
        assert shared_connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert {
            row[0]
            for row in shared_connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        } == {"positions"}
        with sqlite3.connect(portfolio) as portfolio_connection:
            assert portfolio_connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert stat.S_IMODE(portfolio.stat().st_mode) == 0o600
    finally:
        shared_connection.close()


def test_ledger_get_fails_closed_on_hot_journal_without_recovery_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "private" / "portfolio.sqlite3"
    ledger = PortfolioLedger(database, clock=lambda: RECORDED_AT)
    ledger.create(_create())
    writer = sqlite3.connect(database)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE portfolio_daily_snapshots SET cash = '7000' WHERE revision = 1")
        journal = database.with_name(f"{database.name}-journal")
        assert journal.stat().st_size > 0
        before = _filesystem_state(tmp_path)

        with pytest.raises(PortfolioLedgerUnavailable, match="hot rollback journal"):
            ledger.read_selected(AS_OF)

        assert _filesystem_state(tmp_path) == before
    finally:
        writer.rollback()
        writer.close()


def test_read_risk_view_is_one_snapshot_during_concurrent_revision(
    tmp_path: Path,
) -> None:
    database = tmp_path / "portfolio.sqlite3"
    PortfolioLedger(database, clock=lambda: RECORDED_AT).create(_create())
    selected_read = Event()
    release_reader = Event()
    writer_ready = Event()

    class BlockingReader(PortfolioLedger):
        def __init__(self, path: Path) -> None:
            super().__init__(path)
            self.blocked = False

        def _snapshot(self, row: sqlite3.Row):
            snapshot = super()._snapshot(row)
            if not self.blocked:
                self.blocked = True
                selected_read.set()
                assert release_reader.wait(timeout=5)
            return snapshot

    reader = BlockingReader(database)
    read_result: list[object] = []
    write_result: list[object] = []

    def read_view() -> None:
        read_result.append(
            reader.read_risk_view(
                AS_OF,
                known_at=datetime(2026, 8, 2, tzinfo=UTC),
            )
        )

    def writer_clock() -> datetime:
        writer_ready.set()
        return datetime(2026, 8, 1, 12, tzinfo=UTC)

    def revise() -> None:
        write_result.append(
            PortfolioLedger(database, clock=writer_clock).revise(_update(expected_revision=1))
        )

    reader_thread = Thread(target=read_view)
    writer_thread = Thread(target=revise)
    reader_thread.start()
    assert selected_read.wait(timeout=5)
    writer_thread.start()
    assert writer_ready.wait(timeout=5)
    release_reader.set()
    reader_thread.join(timeout=5)
    writer_thread.join(timeout=5)

    assert not reader_thread.is_alive()
    assert not writer_thread.is_alive()
    view = read_result[0]
    assert view.snapshot.revision == 1
    assert [item.revision for item in view.history] == [1]
    assert write_result[0].snapshot.revision == 2
    after = PortfolioLedger(database).read_risk_view(
        AS_OF,
        known_at=datetime(2026, 8, 2, tzinfo=UTC),
    )
    assert after.snapshot.revision == 2
    assert [item.revision for item in after.history] == [2]
    assert not database.with_name(f"{database.name}-wal").exists()
    assert not database.with_name(f"{database.name}-shm").exists()


def test_ledger_concurrent_revision_allows_one_writer_and_preserves_no_order_schema(
    tmp_path: Path,
) -> None:
    database = tmp_path / "user.sqlite3"
    ledger = PortfolioLedger(database, clock=lambda: RECORDED_AT)
    ledger.create(_create())

    def revise(cash: str) -> str:
        try:
            return ledger.revise(
                _update(expected_revision=1, cash=cash, manual_nav=str(int(cash) + 1000))
            ).write_status
        except PortfolioLedgerConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(executor.map(revise, ["7000", "6000"]))

    assert outcomes == ["conflict", "revised"]
    assert ledger.read_selected(AS_OF).revision == 2
    connection = sqlite3.connect(database)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        connection.close()
    assert tables == {"portfolio_daily_snapshots"}
    assert not any("broker" in name or "order" in name for name in tables)


@pytest.mark.parametrize(
    "payload",
    [
        {"symbol": "600001", "quantity": Decimal("1"), "avg_cost": Decimal("1")},
        {"symbol": "sh.600001", "quantity": Decimal("NaN"), "avg_cost": Decimal("1")},
        {"symbol": "sh.600001", "quantity": Decimal("-1"), "avg_cost": Decimal("1")},
        {"symbol": "sh.600001", "quantity": Decimal("1"), "avg_cost": Decimal("Infinity")},
    ],
)
def test_ledger_models_reject_invalid_symbol_nonfinite_and_negative(payload: dict) -> None:
    with pytest.raises(ValidationError):
        PortfolioPositionInput(**payload)


def test_ledger_models_reject_duplicate_symbols_and_nonfinite_cash() -> None:
    with pytest.raises(ValidationError, match="duplicate position symbol"):
        _create(positions=[_position(), _position()])
    with pytest.raises(ValidationError):
        _create(cash="NaN")


def test_risk_calculates_exposure_sector_atr_drawdown_and_guardrails(tmp_path: Path) -> None:
    positions = [
        _position("sh.600001", quantity="10"),
        _position("sz.000001", quantity="10"),
    ]
    service, ledger = _service(
        tmp_path,
        snapshot=_create(positions=positions, cash="8000", manual_nav="10000"),
        bars=[*_bars("sh.600001"), *_bars("sz.000001")],
        sectors={"sh.600001": "bank", "sz.000001": "bank"},
    )
    ledger.create(
        _create(
            as_of=EARLIER - timedelta(days=2),
            positions=[],
            cash="12000",
            manual_nav="12000",
        )
    )
    ledger.create(
        _create(
            as_of=EARLIER,
            positions=[],
            cash="9000",
            manual_nav="9000",
        )
    )

    result = service.evaluate(AS_OF)

    assert result.snapshot.revision == 1
    assert result.nav.selected_nav == Decimal("10000")
    assert result.exposure.gross_ratio == Decimal("0.2")
    assert result.exposure.net_ratio == Decimal("0.2")
    assert result.exposure.max_single_weight == Decimal("0.1")
    assert result.sector_concentration.can_publish_complete is True
    assert result.sector_concentration.max_sector_weight == Decimal("0.2")
    assert result.sector_concentration.sectors[0].sector_id == "bank"
    assert {item.atr14 for item in result.positions} == {Decimal("4")}
    assert result.atr_budget.total_atr_risk == Decimal("80")
    assert result.atr_budget.total_atr_risk_ratio == Decimal("0.008")
    assert result.drawdown.max_drawdown == Decimal("-0.25")
    assert {item.code: item.status for item in result.guardrails} == {
        "max_single_weight": "pass",
        "max_sector_weight": "pass",
        "max_total_atr_risk_ratio": "pass",
        "max_drawdown": "breach",
    }
    assert result.target_exposure.advisory_only is True
    assert result.target_exposure.personalization_allowed is False
    assert "narrow_market_scope_band_widened" in result.target_exposure.reasons


def test_incomplete_nav_history_keeps_observed_drawdown_but_disables_guardrail(
    tmp_path: Path,
) -> None:
    service, ledger = _service(
        tmp_path,
        snapshot=_create(cash="9000", manual_nav="10000"),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
    )
    ledger.create(
        _create(
            as_of=AS_OF - timedelta(days=3),
            positions=[],
            cash="12000",
            manual_nav="12000",
        )
    )
    ledger.create(
        _create(
            as_of=AS_OF - timedelta(days=2),
            positions=[_position("sz.000001")],
            cash=None,
            manual_nav=None,
        )
    )
    ledger.create(
        _create(
            as_of=AS_OF - timedelta(days=1),
            positions=[],
            cash="9000",
            manual_nav="9000",
        )
    )

    result = service.evaluate(AS_OF)
    drawdown_guardrail = next(item for item in result.guardrails if item.code == "max_drawdown")

    assert result.drawdown.status == "degraded"
    assert result.drawdown.observation_count == 3
    assert result.drawdown.max_drawdown == Decimal("-0.25")
    assert drawdown_guardrail.status == "unavailable"
    assert drawdown_guardrail.current_value is None


def test_missing_cash_cost_and_manual_nav_mismatch_degrade_without_guessing(
    tmp_path: Path,
) -> None:
    service, _ = _service(
        tmp_path,
        snapshot=_create(
            cash=None,
            positions=[_position(avg_cost=None)],
            manual_nav="9500",
        ),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)

    assert result.status == "degraded"
    assert result.nav.cash is None
    assert result.nav.derived_nav is None
    assert result.nav.selected_nav == Decimal("9500")
    assert result.positions[0].avg_cost is None
    assert "portfolio.cash" in result.missing_inputs
    assert "position.avg_cost:sh.600001" in result.missing_inputs
    assert "manual_nav_cannot_be_reconciled" in result.quality_issues


def test_low_sector_mapping_keeps_unknown_separate_and_blocks_complete_concentration(
    tmp_path: Path,
) -> None:
    positions = [_position("sh.600001"), _position("sz.000001")]
    service, _ = _service(
        tmp_path,
        snapshot=_create(positions=positions, cash="8000", manual_nav="10000"),
        bars=[*_bars("sh.600001"), *_bars("sz.000001")],
        sectors={"sh.600001": "bank"},
        coverage=0.5,
    )

    result = service.evaluate(AS_OF)

    assert result.sector_concentration.can_publish_complete is False
    assert result.sector_concentration.mapping_ratio == Decimal("0.5")
    assert {item.sector_id for item in result.sector_concentration.sectors} == {
        "bank",
        "unknown",
    }
    assert (
        next(item for item in result.guardrails if item.code == "max_sector_weight").status
        == "unavailable"
    )
    assert "classification.coverage_95_percent" in result.missing_inputs


@pytest.mark.parametrize(
    "bars, expected_issue, expected_atr",
    [
        (_bars("sh.600001", count=14), "atr14_warmup:sh.600001", None),
        (
            _bars("sh.600001", suspended_last=True),
            "published_price_suspended:sh.600001",
            Decimal("4"),
        ),
        (
            _bars("sh.600001", bad_quality_last=True),
            "published_price_bad_quality:sh.600001",
            Decimal("4"),
        ),
    ],
)
def test_atr_warmup_suspension_and_bad_quality_are_missing_not_guessed(
    tmp_path: Path,
    bars: list[DailyBar],
    expected_issue: str,
    expected_atr: Decimal | None,
) -> None:
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=bars,
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)

    assert result.status == "degraded"
    assert result.positions[0].atr14 == expected_atr
    assert expected_issue in [*result.missing_inputs, *result.quality_issues]
    assert (
        next(item for item in result.guardrails if item.code == "max_total_atr_risk_ratio").status
        == "unavailable"
    )


def test_atr14_uses_qfq_prices_across_a_split_instead_of_raw_gap(
    tmp_path: Path,
) -> None:
    bars = _bars("sh.600001", count=15, close=100, spread=1)
    bars[-1] = bars[-1].model_copy(
        update={
            "open": 50,
            "high": 50.5,
            "low": 49.5,
            "close": 50,
            "preclose": 100,
            "adjust_factor": 2,
        }
    )
    raw_ranges = []
    for previous, current in zip(bars, bars[1:], strict=False):
        raw_ranges.append(
            max(
                Decimal(str(current.high - current.low)),
                Decimal(str(abs(current.high - previous.close))),
                Decimal(str(abs(current.low - previous.close))),
            )
        )
    assert (sum(raw_ranges) / Decimal("14")).quantize(Decimal("0.000001")) == Decimal("5.464286")
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=bars,
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)

    assert result.positions[0].atr14 == Decimal("1")


def test_atr14_filters_suspended_rows_before_selecting_15_valid_sessions(
    tmp_path: Path,
) -> None:
    bars = _bars("sh.600001", count=16, close=100, spread=1)
    bars[7] = bars[7].model_copy(
        update={
            "is_trading": False,
            "is_suspended": True,
            "adjust_factor": None,
            "quality_status": "partial",
            "quality_issues": ["suspended_placeholder"],
        }
    )
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=bars,
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)

    assert result.positions[0].atr14 == Decimal("2")


def test_atr14_missing_adjust_factor_fails_closed_without_raw_fallback(
    tmp_path: Path,
) -> None:
    bars = _bars("sh.600001", count=15, close=100, spread=1)
    bars[3] = bars[3].model_copy(update={"adjust_factor": None})
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=bars,
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)

    assert result.positions[0].atr14 is None
    assert "atr14_adjust_factor_missing:sh.600001" in result.missing_inputs


def test_single_sector_and_total_atr_limits_breach_deterministically(tmp_path: Path) -> None:
    service, _ = _service(
        tmp_path,
        snapshot=_create(
            positions=[_position(quantity="100")],
            cash="0",
            manual_nav="10000",
        ),
        bars=_bars("sh.600001", spread=10),
        sectors={"sh.600001": "bank"},
    )

    result = service.evaluate(AS_OF)
    guardrails = {item.code: item.status for item in result.guardrails}

    assert result.exposure.max_single_weight == Decimal("1")
    assert result.sector_concentration.max_sector_weight == Decimal("1")
    assert result.atr_budget.total_atr_risk_ratio == Decimal("0.2")
    assert guardrails["max_single_weight"] == "breach"
    assert guardrails["max_sector_weight"] == "breach"
    assert guardrails["max_total_atr_risk_ratio"] == "breach"


@pytest.mark.parametrize(
    ("strategic", "tactical", "expected_base"),
    [
        ("bull", "risk_on", (Decimal("0.6"), Decimal("0.85"))),
        ("range", "neutral", (Decimal("0.35"), Decimal("0.6"))),
        ("bear", "risk_off", (Decimal("0.05"), Decimal("0.25"))),
    ],
)
def test_market_state_target_bands_are_versioned_and_degraded_bands_widen(
    tmp_path: Path,
    strategic: str,
    tactical: str,
    expected_base: tuple[Decimal, Decimal],
) -> None:
    regime = _RegimeReader(strategic, tactical, status="degraded", confidence=0)
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
        regime=regime,
    )

    result = service.evaluate(AS_OF)

    assert result.target_exposure.base_lower == expected_base[0]
    assert result.target_exposure.base_upper == expected_base[1]
    assert result.target_exposure.lower == max(Decimal("0"), expected_base[0] - Decimal("0.1"))
    assert result.target_exposure.upper == min(Decimal("1"), expected_base[1] + Decimal("0.1"))
    assert result.target_exposure.status == "degraded"
    assert result.policy.formula_version == "portfolio-risk-v1"
    assert result.policy.guardrail_version == "research-default-v1"


def test_no_future_snapshot_market_classification_or_regime_leaks_into_result(
    tmp_path: Path,
) -> None:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    ledger.create(_create(as_of=EARLIER, cash="8000", manual_nav="9000"))
    ledger.create(_create(as_of=AS_OF + timedelta(days=1), cash="0", manual_nav="1000"))
    future_date = AS_OF + timedelta(days=1)
    future_bar = _bars("sh.600001", count=1, end=future_date, close=999)[0]
    classification = _classification(
        {"sh.600001": "future-sector"},
        snapshot_date=future_date,
    )
    service = PortfolioRiskService(
        ledger,
        _MarketReader([*_bars("sh.600001"), future_bar]),
        classification,
        _RegimeReader(as_of=future_date),
    )

    result = service.evaluate(AS_OF)

    assert result.snapshot.as_of == EARLIER
    assert result.data_as_of == AS_OF
    assert result.positions[0].close == Decimal("100")
    assert result.positions[0].sector_id == "unknown"
    assert result.market_regime is None
    assert "future_market_rows_discarded" in result.quality_issues
    assert "future_classification_generation_discarded" in result.quality_issues
    assert "future_market_regime_discarded" in result.quality_issues
    assert result.classification_lineage is None


def test_real_published_market_and_classification_stores_do_not_fallback_to_newer_raw_rows(
    tmp_path: Path,
) -> None:
    symbol = "sh.600001"
    market_path = tmp_path / "market.duckdb"
    market_store = MarketStore(market_path)
    published = _bars(symbol, count=20, close=100)
    _publish_bars(market_store, published)
    unpublished_future = _bars(
        symbol,
        count=1,
        end=AS_OF + timedelta(days=1),
        close=999,
    )[0]
    market_store.upsert_bars([unpublished_future])

    classification = ClassificationStore(tmp_path / "classification.duckdb")
    older_date = EARLIER - timedelta(days=10)
    older = classification.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=older_date,
            observed_at=datetime.combine(older_date, datetime.min.time(), tzinfo=UTC),
            securities=[_security(symbol, snapshot_date=older_date)],
            sector_memberships=[_membership(symbol, "bank", snapshot_date=older_date)],
        )
    )
    future_date = AS_OF + timedelta(days=1)
    newer = classification.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=future_date,
            observed_at=datetime.combine(future_date, datetime.min.time(), tzinfo=UTC),
            securities=[_security(symbol, snapshot_date=future_date)],
            sector_memberships=[_membership(symbol, "future-sector", snapshot_date=future_date)],
        )
    )
    assert older.promoted is True
    assert newer.promoted is True

    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    ledger.create(_create())
    service = PortfolioRiskService(
        ledger,
        PublishedSnapshotDuckDbMarketReader(market_path),
        classification,
        _RegimeReader(),
    )

    result = service.evaluate(AS_OF)

    assert result.data_as_of == AS_OF
    assert result.positions[0].close == Decimal("100")
    assert result.positions[0].sector_id == "bank"
    assert result.positions[0].sector_id != "future-sector"


def test_same_inputs_repeat_same_result_and_policy_version_changes_stable_id(
    tmp_path: Path,
) -> None:
    service, ledger = _service(
        tmp_path,
        snapshot=_create(),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
    )

    first = service.evaluate(AS_OF)
    second = service.evaluate(AS_OF)
    changed = PortfolioRiskService(
        ledger,
        service.market_reader,
        service.classification_reader,
        service.regime_reader,
        policy=PortfolioRiskPolicy(formula_version="portfolio-risk-v9"),
    ).evaluate(AS_OF)
    changed_threshold = PortfolioRiskService(
        ledger,
        service.market_reader,
        service.classification_reader,
        service.regime_reader,
        policy=PortfolioRiskPolicy(market_lookback_sessions=200),
    ).evaluate(AS_OF)

    assert first == second
    assert first.result_id == second.result_id
    assert changed.result_id != first.result_id
    assert changed.formula_version == "portfolio-risk-v9"
    assert changed_threshold.result_id != first.result_id


def test_risk_exposes_replayable_lineage_cutoff_and_deterministic_computed_time(
    tmp_path: Path,
) -> None:
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
    )
    cutoff = datetime(2026, 7, 29, 20, tzinfo=ZoneInfo("Asia/Shanghai"))

    first = service.evaluate(AS_OF, known_at=cutoff)
    second = service.evaluate(
        AS_OF,
        known_at=datetime(2026, 7, 29, 12, tzinfo=UTC),
    )

    assert first.model_dump_json() == second.model_dump_json()
    assert first.result_id == second.result_id
    assert first.evidence_cutoff_at == datetime(2026, 7, 29, 12, tzinfo=UTC)
    assert first.computed_at == first.evidence_cutoff_at
    assert first.market_lineage is not None
    assert first.market_lineage.source == "baostock"
    assert first.market_lineage.source_version == "canonical-market-v2"
    assert first.market_lineage.publication_id is None
    assert first.market_lineage.logical_content_id.startswith("market-content-")
    assert first.market_lineage.data_as_of == AS_OF
    assert first.market_lineage.record_count == 20
    assert len(first.market_lineage.content_hash) == 64
    assert first.market_lineage.publication_visibility_status == "unverifiable"
    assert "market.publication_visibility_unverifiable" in first.quality_issues
    assert first.classification_lineage is not None
    assert first.classification_lineage.generation_id.startswith("classification-fixture-")
    assert first.classification_lineage.source == "fixture"
    assert first.classification_lineage.source_version == "v1"
    assert first.classification_lineage.sector_snapshot_date == EARLIER
    assert len(first.classification_lineage.content_hash) == 64
    assert first.market_regime is not None
    assert first.market_regime.result_id.startswith("regime-")
    assert first.market_regime.formula_version == "market-regime-v1"
    assert first.policy.formula_version == "portfolio-risk-v1"
    assert first.policy.guardrail_version == "research-default-v1"


def test_risk_snapshot_selection_changes_only_when_revision_is_known(
    tmp_path: Path,
) -> None:
    recorded = iter(
        [
            datetime(2026, 7, 29, 12, tzinfo=UTC),
            datetime(2026, 8, 1, 12, tzinfo=UTC),
        ]
    )
    ledger = PortfolioLedger(tmp_path / "portfolio.sqlite3", clock=lambda: next(recorded))
    first = ledger.create(_create(cash="8000", manual_nav="9000"))
    second = ledger.revise(_update(expected_revision=1, cash="7000", manual_nav="8000"))
    service = PortfolioRiskService(
        ledger,
        _MarketReader(_bars("sh.600001")),
        _classification({"sh.600001": "bank"}),
        _RegimeReader(),
    )

    historical = service.evaluate(AS_OF)
    known_later = service.evaluate(
        AS_OF,
        known_at=datetime(2026, 8, 1, 13, tzinfo=UTC),
    )

    assert historical.snapshot.snapshot_id == first.snapshot.snapshot_id
    assert historical.snapshot.revision == 1
    assert historical.evidence_cutoff_at == datetime(
        2026,
        7,
        29,
        15,
        59,
        59,
        999999,
        tzinfo=UTC,
    )
    assert known_later.snapshot.snapshot_id == second.snapshot.snapshot_id
    assert known_later.snapshot.revision == 2
    assert known_later.result_id != historical.result_id


async def _request(method: str, path: str, *, json: dict | None = None) -> httpx.Response:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, json=json)


def _json_content(*, as_of: date = AS_OF, cash: object = 8000) -> dict:
    return {
        "as_of": as_of.isoformat(),
        "cash": cash,
        "positions": [{"symbol": "sh.600001", "quantity": 10, "avg_cost": 90}],
        "manual_nav": 9000,
    }


def test_snapshot_get_api_preserves_checkpointed_database_tree(tmp_path: Path) -> None:
    database = tmp_path / "private" / "stock_eva_portfolio.sqlite3"
    ledger = PortfolioLedger(database, clock=lambda: RECORDED_AT)
    created = ledger.create(_create())
    before = _filesystem_state(tmp_path)
    app.dependency_overrides[portfolio_api.get_portfolio_ledger] = lambda: ledger
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: AS_OF
    try:
        response = asyncio.run(_request("GET", f"/api/v1/portfolio/daily-snapshots?as_of={AS_OF}"))
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["snapshot_id"] == created.snapshot.snapshot_id
    assert _filesystem_state(tmp_path) == before


def test_snapshot_post_put_api_is_idempotent_and_conflicts_are_409(tmp_path: Path) -> None:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    app.dependency_overrides[portfolio_api.get_portfolio_ledger] = lambda: ledger
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: AS_OF
    try:
        created = asyncio.run(
            _request("POST", "/api/v1/portfolio/daily-snapshots", json=_json_content())
        )
        repeated = asyncio.run(
            _request("POST", "/api/v1/portfolio/daily-snapshots", json=_json_content())
        )
        conflict = asyncio.run(
            _request(
                "POST",
                "/api/v1/portfolio/daily-snapshots",
                json=_json_content(cash=7000),
            )
        )
        update = _json_content(cash=7000) | {"expected_revision": 1}
        revised = asyncio.run(
            _request(
                "PUT",
                f"/api/v1/portfolio/daily-snapshots/{AS_OF}",
                json=update,
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert created.status_code == 201
    assert created.json()["write_status"] == "created"
    assert repeated.status_code == 200
    assert repeated.json()["write_status"] == "idempotent"
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "portfolio_revision_conflict"
    assert revised.status_code == 200
    assert revised.json()["snapshot"]["revision"] == 2


def test_known_at_requires_timezone_rejects_future_and_normalizes_equivalent_offsets(
    tmp_path: Path,
) -> None:
    service, _ = _service(
        tmp_path,
        snapshot=_create(),
        bars=_bars("sh.600001"),
        sectors={"sh.600001": "bank"},
    )
    request_started = datetime(2026, 8, 2, 12, tzinfo=UTC)
    app.dependency_overrides[portfolio_api.get_portfolio_risk_service] = lambda: service
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: date(2026, 8, 2)
    app.dependency_overrides[portfolio_api.get_portfolio_request_started_at] = lambda: (
        request_started
    )
    try:
        utc_response = asyncio.run(
            _request(
                "GET",
                f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}&known_at=2026-08-01T12:00:00Z",
            )
        )
        offset_response = asyncio.run(
            _request(
                "GET",
                f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}"
                "&known_at=2026-08-01T20:00:00%2B08:00",
            )
        )
        naive = asyncio.run(
            _request(
                "GET",
                f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}&known_at=2026-08-01T12:00:00",
            )
        )
        future = asyncio.run(
            _request(
                "GET",
                f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}"
                "&known_at=2026-08-02T12:00:00.000001Z",
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert utc_response.status_code == 200
    assert offset_response.status_code == 200
    assert utc_response.json() == offset_response.json()
    assert utc_response.json()["evidence_cutoff_at"] == "2026-08-01T12:00:00Z"
    assert naive.status_code == 422
    assert naive.json()["detail"]["code"] == "invalid_known_at"
    assert future.status_code == 422
    assert future.json()["detail"]["code"] == "future_known_at"


def test_classification_generation_is_selected_by_full_known_at_cutoff(
    tmp_path: Path,
) -> None:
    symbol = "sh.600001"
    classification = ClassificationStore(tmp_path / "classification.duckdb")
    old_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)
    new_observed = datetime(2026, 8, 1, 12, tzinfo=UTC)

    def classification_snapshot(
        sector_id: str,
        observed_at: datetime,
        snapshot_date: date,
    ) -> ClassificationSnapshot:
        security = _security(symbol, snapshot_date=snapshot_date).model_copy(
            update={
                "observed_at": observed_at,
                "lineage_hash": f"security:{symbol}:{sector_id}:{observed_at.isoformat()}",
            }
        )
        membership = _membership(symbol, sector_id, snapshot_date=snapshot_date).model_copy(
            update={
                "observed_at": observed_at,
                "lineage_hash": f"membership:{symbol}:{sector_id}:{observed_at.isoformat()}",
            }
        )
        return ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=snapshot_date,
            observed_at=observed_at,
            securities=[security],
            sector_memberships=[membership],
        )

    old = classification.publish(
        classification_snapshot("bank", old_observed, AS_OF - timedelta(days=1))
    )
    new = classification.publish(classification_snapshot("technology", new_observed, AS_OF))
    ledger = PortfolioLedger(tmp_path / "portfolio.sqlite3", clock=lambda: RECORDED_AT)
    ledger.create(_create())
    service = PortfolioRiskService(
        ledger,
        _MarketReader(_bars(symbol)),
        classification,
        _RegimeReader(),
    )

    historical = service.evaluate(AS_OF)
    known_later = service.evaluate(AS_OF, known_at=new_observed)

    assert old.promoted is True
    assert new.promoted is True
    assert historical.classification_lineage.generation_id == old.generation.generation_id
    assert historical.positions[0].sector_id == "bank"
    assert known_later.classification_lineage.generation_id == new.generation.generation_id
    assert known_later.positions[0].sector_id == "technology"
    assert historical.result_id != known_later.result_id


def test_risk_get_missing_database_is_read_only_and_future_is_rejected(tmp_path: Path) -> None:
    missing = tmp_path / "private" / "user.sqlite3"
    service = PortfolioRiskService(
        PortfolioLedger(missing),
        _MarketReader([]),
        _classification({}),
        _RegimeReader(),
    )
    app.dependency_overrides[portfolio_api.get_portfolio_risk_service] = lambda: service
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: AS_OF
    try:
        empty = asyncio.run(_request("GET", f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}"))
        future = asyncio.run(
            _request(
                "GET",
                f"/api/v1/analysis/portfolio-risk?as_of={AS_OF + timedelta(days=1)}",
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert empty.status_code == 200
    assert empty.json()["status"] == "empty"
    assert missing.exists() is False
    assert missing.parent.exists() is False
    assert future.status_code == 422
    assert future.json()["detail"]["code"] == "future_as_of"


def test_corrupt_portfolio_database_returns_structured_503(tmp_path: Path) -> None:
    database = tmp_path / "user.sqlite3"
    database.write_bytes(b"not-sqlite")
    service = PortfolioRiskService(
        PortfolioLedger(database),
        _MarketReader([]),
        _classification({}),
        _RegimeReader(),
    )
    app.dependency_overrides[portfolio_api.get_portfolio_risk_service] = lambda: service
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: AS_OF
    try:
        response = asyncio.run(_request("GET", f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}"))
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "portfolio_ledger_unavailable",
        "storage_status": "unavailable",
    }


@pytest.mark.parametrize(
    ("dependency", "expected_code"),
    [
        ("classification", "classification_storage_unavailable"),
        ("market", "market_storage_unavailable"),
    ],
)
def test_corrupt_published_dependencies_return_structured_503(
    tmp_path: Path,
    dependency: str,
    expected_code: str,
) -> None:
    ledger = PortfolioLedger(tmp_path / "user.sqlite3", clock=lambda: RECORDED_AT)
    ledger.create(_create())
    if dependency == "classification":
        corrupt = tmp_path / "classification.duckdb"
        corrupt.write_bytes(b"not-duckdb")
        market_reader = _MarketReader(_bars("sh.600001"))
        classification_reader = ClassificationStore(corrupt)
    else:
        corrupt = tmp_path / "market.duckdb"
        corrupt.write_bytes(b"not-duckdb")
        market_reader = PublishedSnapshotDuckDbMarketReader(corrupt)
        classification_reader = _classification({"sh.600001": "bank"})
    service = PortfolioRiskService(
        ledger,
        market_reader,
        classification_reader,
        _RegimeReader(),
    )
    app.dependency_overrides[portfolio_api.get_portfolio_risk_service] = lambda: service
    app.dependency_overrides[portfolio_api.get_portfolio_today] = lambda: AS_OF
    try:
        response = asyncio.run(_request("GET", f"/api/v1/analysis/portfolio-risk?as_of={AS_OF}"))
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": expected_code,
        "storage_status": "unavailable",
    }
