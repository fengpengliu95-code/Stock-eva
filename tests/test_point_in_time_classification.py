import asyncio
from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter

import httpx
import pytest

from backend.app.api.classification import get_classification_store
from backend.app.classification.models import (
    INDEX_CATALOG,
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    IndexComponentRecord,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.classification.provider import (
    BaoStockClassificationProvider,
    ClassificationProviderError,
    derive_security_identity,
)
from backend.app.classification.service import (
    ClassificationConflictError,
    ClassificationService,
)
from backend.app.classification.store import ClassificationStore
from backend.app.classification.sync import run_classification_sync
from backend.app.cli import build_parser
from backend.app.main import app

OBSERVED = datetime(2026, 7, 28, 12, tzinfo=UTC)
SNAPSHOT_DATE = date(2026, 7, 28)


def security(
    symbol: str,
    *,
    board: str = "main",
    security_type: str = "stock",
    listed: date | None = date(2000, 1, 1),
    delisted: date | None = None,
    tradable: bool = True,
    price_available: bool | None = True,
    snapshot_date: date = SNAPSHOT_DATE,
    observed_at: datetime = OBSERVED,
) -> SecurityMasterRecord:
    exchange = symbol.split(".", 1)[0]
    return SecurityMasterRecord(
        record_id=f"security:{snapshot_date}:{symbol}",
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        name=f"Fixture {symbol}",
        exchange=exchange,
        board=board,
        security_type=security_type,
        list_date=listed,
        delist_date=delisted,
        is_tradable=tradable,
        price_available=price_available,
        source_status="1" if tradable else "0",
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=listed,
        effective_to=delisted,
        observed_at=observed_at,
        source_record_id=f"query_all_stock:{snapshot_date}:{symbol}",
        lineage_hash=f"security-lineage:{snapshot_date}:{symbol}:{tradable}:{delisted}",
    )


def membership(
    symbol: str,
    sector_id: str = "bank",
    *,
    taxonomy_id: str = TAXONOMY_BAOSTOCK_INDUSTRY,
    snapshot_date: date = SNAPSHOT_DATE,
    observed_at: datetime = OBSERVED,
    lineage_suffix: str = "",
) -> SectorMembershipRecord:
    return SectorMembershipRecord(
        record_id=f"sector:{taxonomy_id}:{snapshot_date}:{symbol}",
        taxonomy_id=taxonomy_id,
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
        lineage_hash=f"sector-lineage:{snapshot_date}:{symbol}:{sector_id}:{lineage_suffix}",
    )


def component(
    symbol: str,
    *,
    index_id: str = "hs300",
    snapshot_date: date = SNAPSHOT_DATE,
    observed_at: datetime = OBSERVED,
) -> IndexComponentRecord:
    return IndexComponentRecord(
        record_id=f"index:{index_id}:{snapshot_date}:{symbol}",
        index_id=index_id,
        index_name=INDEX_CATALOG[index_id].name,
        security_id=f"baostock:{symbol}",
        symbol=symbol,
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        effective_from=snapshot_date,
        effective_to=None,
        observed_at=observed_at,
        source_record_id=f"query_{index_id}_stocks:{snapshot_date}:{symbol}",
        lineage_hash=f"index-lineage:{index_id}:{snapshot_date}:{symbol}",
    )


def snapshot(
    *,
    snapshot_date: date = SNAPSHOT_DATE,
    observed_at: datetime = OBSERVED,
    securities: list[SecurityMasterRecord] | None = None,
    memberships: list[SectorMembershipRecord] | None = None,
    components: list[IndexComponentRecord] | None = None,
) -> ClassificationSnapshot:
    return ClassificationSnapshot(
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        observed_at=observed_at,
        securities=securities or [],
        index_components=components or [],
        sector_memberships=memberships or [],
    )


def service(tmp_path: Path) -> tuple[ClassificationStore, ClassificationService]:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    return store, ClassificationService(store)


def test_source_without_end_date_never_gets_inferred_end_date() -> None:
    row = membership("sh.600000")

    assert row.effective_to is None


def test_security_list_and_delist_dates_are_applied_at_as_of(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[
                security(
                    "sh.600000",
                    listed=date(2020, 1, 1),
                    delisted=date(2026, 7, 29),
                )
            ]
        )
    )

    before = classification.securities(date(2019, 12, 31), symbol="sh.600000")
    listed = classification.securities(date(2026, 7, 28), symbol="sh.600000")
    delisted = classification.securities(date(2026, 7, 29), symbol="sh.600000")
    after_delist = classification.securities(date(2026, 7, 30), symbol="sh.600000")

    assert before.status == "not_available"
    assert listed.status == "ready"
    assert listed.securities[0].eligibility.eligible is True
    assert delisted.status == "ready"
    assert delisted.securities[0].eligibility.exclusion_reason == "delisted"
    assert after_delist.status == "ready"
    assert after_delist.securities[0].eligibility.exclusion_reason == "delisted"


def test_snapshot_boundary_and_future_snapshot_never_leak(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    first_date = date(2026, 7, 20)
    second_date = date(2026, 7, 28)
    store.publish(
        snapshot(
            snapshot_date=first_date,
            observed_at=datetime(2026, 7, 20, 12, tzinfo=UTC),
            securities=[
                security(
                    "sh.600000",
                    snapshot_date=first_date,
                    observed_at=datetime(2026, 7, 20, 12, tzinfo=UTC),
                )
            ],
            memberships=[
                membership(
                    "sh.600000",
                    "bank",
                    snapshot_date=first_date,
                    observed_at=datetime(2026, 7, 20, 12, tzinfo=UTC),
                )
            ],
        )
    )
    store.publish(
        snapshot(
            snapshot_date=second_date,
            securities=[security("sh.600000", snapshot_date=second_date)],
            memberships=[membership("sh.600000", "finance", snapshot_date=second_date)],
        )
    )

    before = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, date(2026, 7, 19))
    equal = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, first_date)
    between = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, date(2026, 7, 27))
    after = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, second_date)

    assert before.status == "not_available"
    assert [item.sector_id for item in equal.sectors] == ["bank"]
    assert [item.sector_id for item in between.sectors] == ["bank"]
    assert [item.sector_id for item in after.sectors] == ["finance"]


def test_observation_after_as_of_is_never_visible(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    historical = date(2020, 1, 2)
    observed_later = datetime(2026, 7, 28, 12, tzinfo=UTC)
    store.publish(
        snapshot(
            snapshot_date=historical,
            observed_at=observed_later,
            securities=[
                security(
                    "sh.600000",
                    snapshot_date=historical,
                    observed_at=observed_later,
                )
            ],
            memberships=[
                membership(
                    "sh.600000",
                    snapshot_date=historical,
                    observed_at=observed_later,
                )
            ],
        )
    )

    response = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, historical)

    assert response.status == "not_available"
    assert response.quality_issues == ["no_trusted_snapshot_at_as_of"]


def test_future_effective_membership_is_not_visible(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    source_date = date(2026, 7, 20)
    effective_date = date(2026, 7, 25)
    observed = datetime(2026, 7, 20, 12, tzinfo=UTC)
    future_membership = membership(
        "sh.600000",
        snapshot_date=source_date,
        observed_at=observed,
    ).model_copy(update={"effective_from": effective_date})
    store.publish(
        snapshot(
            snapshot_date=source_date,
            observed_at=observed,
            securities=[
                security(
                    "sh.600000",
                    snapshot_date=source_date,
                    observed_at=observed,
                )
            ],
            memberships=[future_membership],
        )
    )

    before = classification.sectors(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        date(2026, 7, 24),
    )
    equal = classification.sectors(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        effective_date,
    )

    assert before.status == "not_available"
    assert [row.sector_id for row in equal.sectors] == ["bank"]


def test_index_component_history_uses_latest_whole_snapshot(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    first_date = date(2026, 7, 20)
    first_observed = datetime(2026, 7, 20, 12, tzinfo=UTC)
    store.publish(
        snapshot(
            snapshot_date=first_date,
            observed_at=first_observed,
            securities=[
                security("sh.600000", snapshot_date=first_date, observed_at=first_observed),
                security("sz.000001", snapshot_date=first_date, observed_at=first_observed),
            ],
            components=[
                component("sh.600000", snapshot_date=first_date, observed_at=first_observed),
                component("sz.000001", snapshot_date=first_date, observed_at=first_observed),
            ],
        )
    )
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            components=[component("sh.600000")],
        )
    )

    old = classification.index_components("hs300", first_date)
    current = classification.index_components("hs300", SNAPSHOT_DATE)

    assert [item.symbol for item in old.components] == ["sh.600000", "sz.000001"]
    assert [item.symbol for item in current.components] == ["sh.600000"]
    assert current.source_snapshot_date == SNAPSHOT_DATE
    assert current.components[0].effective_to is None


def test_taxonomies_are_isolated(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[
                membership("sh.600000", "bank"),
                membership(
                    "sh.600000",
                    "large-cap",
                    taxonomy_id="fixture.size_taxonomy",
                ),
            ],
        )
    )

    industry = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)
    size = classification.sectors("fixture.size_taxonomy", SNAPSHOT_DATE)

    assert [item.sector_id for item in industry.sectors] == ["bank"]
    assert [item.sector_id for item in size.sectors] == ["large-cap"]


def test_exact_duplicate_rows_and_republication_are_idempotent(tmp_path: Path) -> None:
    store, _ = service(tmp_path)
    payload = snapshot(
        securities=[security("sh.600000"), security("sh.600000")],
        memberships=[membership("sh.600000"), membership("sh.600000")],
    )

    first = store.publish(payload)
    second = store.publish(payload)

    assert second.generation_id == first.generation_id
    assert store.generation_count() == 1
    assert first.row_counts == {
        "security_master_history": 1,
        "index_component_history": 0,
        "sector_membership_history": 1,
    }


def test_conflicting_same_source_snapshot_fails_closed(tmp_path: Path) -> None:
    store, _ = service(tmp_path)
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000", "bank")],
        )
    )
    ready_before = store.ready_generation()

    with pytest.raises(ClassificationConflictError, match="conflicting sector membership"):
        store.publish(
            snapshot(
                securities=[security("sh.600000")],
                memberships=[
                    membership(
                        "sh.600000",
                        "finance",
                        lineage_suffix="conflict",
                    )
                ],
            )
        )

    assert store.ready_generation() == ready_before
    assert store.generation_count() == 1


def test_atomic_failure_keeps_previous_ready_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000", "bank")],
        )
    )
    ready_before = store.ready_generation()

    def fail_insert(*_args, **_kwargs):
        raise RuntimeError("synthetic publication failure")

    monkeypatch.setattr(store, "_insert_sector_rows", fail_insert)
    next_date = date(2026, 7, 29)
    with pytest.raises(RuntimeError, match="synthetic publication failure"):
        store.publish(
            snapshot(
                snapshot_date=next_date,
                observed_at=datetime(2026, 7, 29, 12, tzinfo=UTC),
                securities=[
                    security(
                        "sh.600000",
                        snapshot_date=next_date,
                        observed_at=datetime(2026, 7, 29, 12, tzinfo=UTC),
                    )
                ],
                memberships=[
                    membership(
                        "sh.600000",
                        "finance",
                        snapshot_date=next_date,
                        observed_at=datetime(2026, 7, 29, 12, tzinfo=UTC),
                    )
                ],
            )
        )

    assert store.ready_generation() == ready_before
    assert [item.sector_id for item in classification.sectors(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        SNAPSHOT_DATE,
    ).sectors] == ["bank"]


def test_unknown_security_membership_fails_publication(tmp_path: Path) -> None:
    store, _ = service(tmp_path)

    with pytest.raises(ClassificationConflictError, match="unknown security"):
        store.publish(
            snapshot(
                securities=[security("sh.600000")],
                memberships=[membership("sz.000001")],
            )
        )

    assert store.ready_generation() is None


@pytest.mark.parametrize(
    ("symbol", "board", "expected_reason"),
    [
        ("sh.600000", "main", None),
        ("sz.000001", "main", None),
        ("sz.300001", "chinext", None),
        ("sh.688001", "star", None),
        ("bj.830001", "bse", "unsupported_exchange"),
        ("sh.000001", "index", "non_stock"),
    ],
)
def test_market_scope_and_eligibility_are_truthful(
    tmp_path: Path,
    symbol: str,
    board: str,
    expected_reason: str | None,
) -> None:
    store, classification = service(tmp_path)
    security_type = "index" if board == "index" else "stock"
    store.publish(
        snapshot(
            securities=[
                security(
                    symbol,
                    board=board,
                    security_type=security_type,
                )
            ]
        )
    )

    response = classification.securities(SNAPSHOT_DATE, symbol=symbol)

    assert response.securities[0].eligibility.exclusion_reason == expected_reason
    assert "BSE" in response.market_scope.excluded_markets
    assert response.market_scope.derivation_version == "cn-symbol-prefix-v1"


def test_suspended_security_is_excluded_with_reason(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    store.publish(snapshot(securities=[security("sz.300001", board="chinext", tradable=False)]))

    response = classification.securities(SNAPSHOT_DATE, symbol="sz.300001")

    assert response.securities[0].eligibility.eligible is False
    assert response.securities[0].eligibility.exclusion_reason == "suspended"


@pytest.mark.parametrize(
    ("price_available", "expected_reason"),
    [(False, "no_price"), (None, "price_not_audited")],
)
def test_price_coverage_uncertainty_fails_closed(
    tmp_path: Path,
    price_available: bool | None,
    expected_reason: str,
) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[
                security(
                    "sh.600000",
                    price_available=price_available,
                )
            ]
        )
    )

    response = classification.securities(SNAPSHOT_DATE, symbol="sh.600000")

    assert response.securities[0].eligibility.eligible is False
    assert response.securities[0].eligibility.exclusion_reason == expected_reason


@pytest.mark.parametrize(
    ("mapped", "expected_status"),
    [(94, "degraded"), (95, "ready")],
)
def test_coverage_threshold_boundary(
    tmp_path: Path,
    mapped: int,
    expected_status: str,
) -> None:
    store, classification = service(tmp_path)
    securities = [security(f"sh.{600000 + index:06d}") for index in range(100)]
    memberships = [membership(row.symbol) for row in securities[:mapped]]
    store.publish(snapshot(securities=securities, memberships=memberships))

    audit = classification.coverage(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)

    assert audit.status == expected_status
    assert audit.eligible_count == 100
    assert audit.mapped_count == mapped
    assert audit.coverage_ratio == pytest.approx(mapped / 100)
    assert audit.unmapped_symbols == [row.symbol for row in securities[mapped:]]


def test_coverage_zero_denominator_is_degraded_not_one_hundred_percent(
    tmp_path: Path,
) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[
                security("sh.000001", board="index", security_type="index"),
            ]
        )
    )

    audit = classification.coverage(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)

    assert audit.status == "degraded"
    assert audit.eligible_count == 0
    assert audit.coverage_ratio is None
    assert audit.quality_issues == ["no_eligible_securities"]


def test_coverage_is_reproducible_and_reports_unmapped_symbols(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[security("sh.600000"), security("sz.000001")],
            memberships=[membership("sh.600000")],
        )
    )

    first = classification.coverage(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)
    second = classification.coverage(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)

    assert first == second
    assert first.unmapped_symbols == ["sz.000001"]
    assert first.generation_id is not None
    assert first.source_lineage == ["baostock@0.9.3"]


def test_large_member_query_uses_bounded_queries_and_time(tmp_path: Path) -> None:
    store, classification = service(tmp_path)
    rows = [security(f"sh.{600000 + index:06d}") for index in range(5_000)]
    store.publish(
        snapshot(
            securities=rows,
            memberships=[membership(row.symbol) for row in rows],
        )
    )

    started = perf_counter()
    response = classification.sector_members(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "bank",
        SNAPSHOT_DATE,
    )
    elapsed = perf_counter() - started

    assert len(response.members) == 5_000
    assert response.database_query_count <= 3
    assert elapsed < 1.5


class FakeResult:
    def __init__(self, fields: list[str], rows: list[list[str]]) -> None:
        self.fields = fields
        self._rows = iter(rows)
        self.error_code = "0"
        self.error_msg = ""
        self._current = None

    def next(self) -> bool:
        self._current = next(self._rows, None)
        return self._current is not None

    def get_row_data(self):
        return self._current


class FakeClassificationClient:
    def __init__(self, *, mixed_industry_dates: bool = False) -> None:
        self.calls: list[str] = []
        self.logged_out = False
        self.mixed_industry_dates = mixed_industry_dates

    def login(self):
        return FakeResult([], [])

    def logout(self):
        self.logged_out = True

    def query_all_stock(self, **_kwargs):
        self.calls.append("query_all_stock")
        return FakeResult(
            ["code", "tradeStatus", "code_name"],
            [
                ["sh.600000", "1", "浦发银行"],
                ["sz.300001", "1", "创业板样本"],
                ["sh.688001", "1", "科创板样本"],
                ["sh.000001", "1", "上证综指"],
                ["sz.200001", "1", "深市B股样本"],
            ],
        )

    def query_stock_basic(self, **_kwargs):
        self.calls.append("query_stock_basic")
        return FakeResult(
            ["code", "code_name", "ipoDate", "outDate", "type", "status"],
            [
                ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"],
                ["sz.300001", "创业板样本", "2009-10-30", "", "1", "1"],
                ["sh.688001", "科创板样本", "2019-07-22", "", "1", "1"],
                ["sh.000001", "上证综指", "1991-07-15", "", "2", "1"],
                ["sz.200001", "深市B股样本", "1995-01-01", "", "1", "1"],
            ],
        )

    def query_stock_industry(self, **_kwargs):
        self.calls.append("query_stock_industry")
        second_date = "2026-07-21" if self.mixed_industry_dates else "2026-07-20"
        return FakeResult(
            ["updateDate", "code", "code_name", "industry", "industryClassification"],
            [
                ["2026-07-20", "sh.600000", "浦发银行", "银行", "证监会行业分类"],
                [second_date, "sz.300001", "创业板样本", "软件服务", "证监会行业分类"],
                ["2026-07-20", "sh.688001", "科创板样本", "半导体", "证监会行业分类"],
            ],
        )

    def _index(self, call: str, rows: list[list[str]]):
        self.calls.append(call)
        return FakeResult(["updateDate", "code", "code_name"], rows)

    def query_hs300_stocks(self, **_kwargs):
        return self._index(
            "query_hs300_stocks",
            [["2026-07-20", "sh.600000", "浦发银行"]],
        )

    def query_sz50_stocks(self, **_kwargs):
        return self._index(
            "query_sz50_stocks",
            [["2026-07-20", "sh.600000", "浦发银行"]],
        )

    def query_zz500_stocks(self, **_kwargs):
        return self._index(
            "query_zz500_stocks",
            [["2026-07-20", "sz.300001", "创业板样本"]],
        )


@pytest.mark.parametrize(
    ("symbol", "source_type", "expected"),
    [
        ("sh.600000", "1", ("sh", "main", "stock")),
        ("sh.688001", "1", ("sh", "star", "stock")),
        ("sh.689009", "1", ("sh", "star", "stock")),
        ("sz.000001", "1", ("sz", "main", "stock")),
        ("sz.002001", "1", ("sz", "main", "stock")),
        ("sz.300001", "1", ("sz", "chinext", "stock")),
        ("sz.301001", "1", ("sz", "chinext", "stock")),
        ("sh.900001", "1", ("sh", "b_share", "stock")),
        ("sz.200001", "1", ("sz", "b_share", "stock")),
        ("sh.000001", "2", ("sh", "index", "index")),
        ("bj.830001", "1", ("bj", "bse", "stock")),
    ],
)
def test_versioned_board_derivation_boundaries(
    symbol: str,
    source_type: str,
    expected: tuple[str, str, str],
) -> None:
    identity = derive_security_identity(symbol, source_type)

    assert (identity.exchange, identity.board, identity.security_type) == expected
    assert identity.derivation_version == "cn-symbol-prefix-v1"


def test_provider_builds_extended_security_and_classification_snapshot() -> None:
    client = FakeClassificationClient()
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
    )

    payload = provider.fetch(SNAPSHOT_DATE)

    by_symbol = {row.symbol: row for row in payload.securities}
    assert by_symbol["sz.300001"].board == "chinext"
    assert by_symbol["sh.688001"].board == "star"
    assert by_symbol["sh.000001"].security_type == "index"
    assert by_symbol["sz.200001"].board == "b_share"
    assert by_symbol["sh.600000"].list_date == date(1999, 11, 10)
    assert by_symbol["sh.600000"].delist_date is None
    assert by_symbol["sh.600000"].price_available is None
    assert {row.index_id for row in payload.index_components} == {
        "hs300",
        "sz50",
        "csi500",
    }
    assert all(row.effective_to is None for row in payload.index_components)
    assert all(row.effective_to is None for row in payload.sector_memberships)
    assert {
        (row.raw_industry, row.raw_classification)
        for row in payload.sector_memberships
    } == {
        ("银行", "证监会行业分类"),
        ("软件服务", "证监会行业分类"),
        ("半导体", "证监会行业分类"),
    }
    assert client.calls == [
        "query_all_stock",
        "query_stock_basic",
        "query_stock_industry",
        "query_hs300_stocks",
        "query_sz50_stocks",
        "query_zz500_stocks",
    ]
    assert client.logged_out is True


def test_provider_fails_closed_on_mixed_source_snapshot_dates() -> None:
    provider = BaoStockClassificationProvider(
        client=FakeClassificationClient(mixed_industry_dates=True),
        clock=lambda: OBSERVED,
    )

    with pytest.raises(ClassificationProviderError, match="mixed industry snapshot dates"):
        provider.fetch(SNAPSHOT_DATE)


def api_request(store: ClassificationStore, path: str) -> httpx.Response:
    app.dependency_overrides[get_classification_store] = lambda: store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def test_api_ready_and_degraded_coverage_contract(tmp_path: Path) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    store.publish(
        snapshot(
            securities=[
                security("sh.600000"),
                security("sz.300001", board="chinext"),
            ],
            memberships=[membership("sh.600000")],
            components=[component("sh.600000")],
        )
    )

    securities = api_request(
        store,
        f"/api/v1/classification/securities?as_of={SNAPSHOT_DATE}",
    )
    components = api_request(
        store,
        f"/api/v1/classification/indexes/hs300/components?as_of={SNAPSHOT_DATE}",
    )
    sectors = api_request(
        store,
        (
            "/api/v1/classification/taxonomies/"
            f"{TAXONOMY_BAOSTOCK_INDUSTRY}/sectors?as_of={SNAPSHOT_DATE}"
        ),
    )
    members = api_request(
        store,
        (
            "/api/v1/classification/taxonomies/"
            f"{TAXONOMY_BAOSTOCK_INDUSTRY}/sectors/bank/members"
            f"?as_of={SNAPSHOT_DATE}"
        ),
    )
    coverage = api_request(
        store,
        (
            "/api/v1/classification/coverage"
            f"?as_of={SNAPSHOT_DATE}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}"
        ),
    )

    assert securities.status_code == 200
    assert securities.json()["status"] == "ready"
    assert {row["board"] for row in securities.json()["securities"]} == {
        "main",
        "chinext",
    }
    assert "BSE" in securities.json()["market_scope"]["excluded_markets"]
    assert components.status_code == 200
    assert [row["symbol"] for row in components.json()["components"]] == ["sh.600000"]
    assert sectors.status_code == 200
    assert sectors.json()["sectors"][0]["sector_id"] == "bank"
    assert members.status_code == 200
    assert members.json()["members"][0]["raw_classification"] == "证监会行业分类"
    assert coverage.status_code == 200
    assert coverage.json()["status"] == "degraded"
    assert coverage.json()["coverage_ratio"] == 0.5
    assert coverage.json()["unmapped_symbols"] == ["sz.300001"]


def test_api_empty_and_not_available_states(tmp_path: Path) -> None:
    empty_store = ClassificationStore(tmp_path / "empty.duckdb")

    empty = api_request(
        empty_store,
        f"/api/v1/classification/securities?as_of={SNAPSHOT_DATE}",
    )
    before_first = api_request(
        empty_store,
        (
            "/api/v1/classification/indexes/hs300/components"
            f"?as_of={SNAPSHOT_DATE}"
        ),
    )
    metadata_only = api_request(
        empty_store,
        (
            "/api/v1/classification/indexes/sse_composite/components"
            f"?as_of={SNAPSHOT_DATE}"
        ),
    )

    assert empty.status_code == 200
    assert empty.json()["status"] == "empty"
    assert before_first.status_code == 200
    assert before_first.json()["status"] == "not_available"
    assert before_first.json()["quality_issues"] == ["no_trusted_snapshot_at_as_of"]
    assert metadata_only.status_code == 200
    assert metadata_only.json()["quality_issues"] == [
        "component_history_not_supplied_by_source"
    ]


def test_api_rejects_invalid_date_and_unknown_classifiers(tmp_path: Path) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")

    invalid_date = api_request(
        store,
        "/api/v1/classification/securities?as_of=2026-99-99",
    )
    unknown_index = api_request(
        store,
        f"/api/v1/classification/indexes/not-real/components?as_of={SNAPSHOT_DATE}",
    )
    unknown_taxonomy = api_request(
        store,
        (
            "/api/v1/classification/taxonomies/not-real/sectors"
            f"?as_of={SNAPSHOT_DATE}"
        ),
    )

    assert invalid_date.status_code == 422
    assert unknown_index.status_code == 404
    assert unknown_index.json()["detail"]["code"] == "unknown_index"
    assert unknown_taxonomy.status_code == 404
    assert unknown_taxonomy.json()["detail"]["code"] == "unknown_taxonomy"


class RecordingClassificationProvider:
    def __init__(self, payload: ClassificationSnapshot) -> None:
        self.payload = payload
        self.calls: list[date] = []

    def fetch(self, as_of: date) -> ClassificationSnapshot:
        self.calls.append(as_of)
        return self.payload


def test_classification_sync_is_dry_run_by_default(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["classification-sync", "--as-of", SNAPSHOT_DATE.isoformat()]
    )
    store = ClassificationStore(tmp_path / "classification.duckdb")
    provider = RecordingClassificationProvider(
        snapshot(securities=[security("sh.600000")])
    )

    result = run_classification_sync(
        as_of=args.as_of,
        execute=args.execute,
        store=store,
        provider=provider,
    )

    assert result.status == "dry-run"
    assert result.network_requests == 0
    assert result.writes_classification_data is False
    assert result.execute_requires == "--execute"
    assert provider.calls == []
    assert store.ready_generation() is None


def test_classification_sync_execute_observes_and_publishes(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
        ]
    )
    store = ClassificationStore(tmp_path / "classification.duckdb")
    provider = RecordingClassificationProvider(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    result = run_classification_sync(
        as_of=args.as_of,
        execute=args.execute,
        store=store,
        provider=provider,
    )

    assert result.status == "ready"
    assert result.network_requests == 6
    assert result.writes_classification_data is True
    assert result.generation is not None
    assert provider.calls == [SNAPSHOT_DATE]
    assert store.ready_generation() == result.generation.generation_id
