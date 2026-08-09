import asyncio
import gc
import json
import sys
import threading
import traceback
import weakref
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter

import duckdb
import httpx
import pytest

from backend.app import cli
from backend.app.api.classification import get_classification_store
from backend.app.classification.failures import (
    ClassificationFailure,
    ClassificationSyncError,
)
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
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.baostock import BaoStockError

OBSERVED = datetime(2026, 7, 28, 12, tzinfo=UTC)
SNAPSHOT_DATE = date(2026, 7, 28)
CLASSIFICATION_FAILURE_KEYS = {
    "status",
    "as_of",
    "quality_issues",
    "writes_classification_data",
    "failure_stage",
    "failure_class",
    "elapsed_seconds",
    "provider_request_count",
    "configured_timeout_seconds",
    "configured_max_attempts",
}


def _owned_traceback_locals(error: BaseException) -> str:
    captured = traceback.TracebackException.from_exception(
        error,
        capture_locals=True,
    )
    return "\n".join(
        f"{name}={value}"
        for frame in captured.stack
        if "/backend/app/" in frame.filename
        for name, value in (frame.locals or {}).items()
    )


class _RawExceptionProbe:
    def __init__(self) -> None:
        self.reference: weakref.ReferenceType[BaseException] | None = None

    def remember(self, error: BaseException) -> None:
        self.reference = weakref.ref(error)

    def __repr__(self) -> str:
        return "_RawExceptionProbe()"


@pytest.mark.parametrize(
    "override",
    [
        {"failure_stage": "unknown"},
        {"raw_detail": "must-not-enter-contract"},
    ],
)
def test_classification_failure_contract_rejects_unstable_fields(
    override: dict[str, object],
) -> None:
    payload = {
        "failure_stage": "validation",
        "failure_class": "internal",
        "elapsed_seconds": 0,
        "provider_request_count": 0,
        **override,
    }

    with pytest.raises(ValueError):
        ClassificationFailure.model_validate(payload)


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
        listing_status="1" if delisted is None else "0",
        daily_trade_status="1" if tradable else "0",
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
    declared_taxonomies: list[str] | None = None,
) -> ClassificationSnapshot:
    return ClassificationSnapshot(
        source="baostock",
        source_version="0.9.3",
        source_snapshot_date=snapshot_date,
        observed_at=observed_at,
        securities=securities or [],
        index_components=components or [],
        sector_memberships=memberships or [],
        declared_taxonomies=(
            [TAXONOMY_BAOSTOCK_INDUSTRY]
            if declared_taxonomies is None
            else declared_taxonomies
        ),
    )


def service(tmp_path: Path) -> tuple[ClassificationStore, ClassificationService]:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    return store, ClassificationService(store)


def test_source_without_end_date_never_gets_inferred_end_date() -> None:
    row = membership("sh.600000")

    assert row.effective_to is None


def test_index_component_history_capabilities_are_truthful() -> None:
    assert {
        index_id: metadata.component_history_capability
        for index_id, metadata in INDEX_CATALOG.items()
    } == {
        "sse_composite": "not_supplied",
        "szse_component": "not_supplied",
        "hs300": "unverified",
        "sz50": "unverified",
        "csi500": "unverified",
        "csi1000": "not_supplied",
        "chinext_index": "not_supplied",
    }
    assert all(
        metadata.component_history_capability != "verified"
        for metadata in INDEX_CATALOG.values()
    )
    assert (
        "component_history_supported"
        not in type(INDEX_CATALOG["hs300"]).model_fields
    )
    assert (
        INDEX_CATALOG["csi1000"].symbol,
        INDEX_CATALOG["csi1000"].component_source,
    ) == ("sh.000852", None)
    assert (
        INDEX_CATALOG["chinext_index"].symbol,
        INDEX_CATALOG["chinext_index"].component_source,
    ) == ("sz.399006", None)


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
            ],
            memberships=[membership("sh.600000")],
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
            memberships=[
                membership(
                    "sh.600000",
                    snapshot_date=first_date,
                    observed_at=first_observed,
                ),
                membership(
                    "sz.000001",
                    snapshot_date=first_date,
                    observed_at=first_observed,
                ),
            ],
        )
    )
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            components=[component("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    old = classification.index_components("hs300", first_date)
    current = classification.index_components("hs300", SNAPSHOT_DATE)

    assert [item.symbol for item in old.components] == ["sh.600000", "sz.000001"]
    assert [item.symbol for item in current.components] == ["sh.600000"]
    assert current.source_snapshot_date == SNAPSHOT_DATE
    assert current.components[0].effective_to is None


def test_repeated_source_dates_select_only_the_latest_visible_generation(
    tmp_path: Path,
) -> None:
    store, classification = service(tmp_path)
    first_generation_date = date(2026, 7, 27)
    source_update_date = date(2026, 7, 20)
    first_observed = datetime(2026, 7, 27, 12, tzinfo=UTC)
    store.publish(
        snapshot(
            snapshot_date=first_generation_date,
            observed_at=first_observed,
            securities=[
                security(
                    "sh.600000",
                    snapshot_date=first_generation_date,
                    observed_at=first_observed,
                ),
                security(
                    "sz.000001",
                    snapshot_date=first_generation_date,
                    observed_at=first_observed,
                ),
            ],
            components=[
                component(
                    "sh.600000",
                    snapshot_date=source_update_date,
                    observed_at=first_observed,
                ),
                component(
                    "sz.000001",
                    snapshot_date=source_update_date,
                    observed_at=first_observed,
                ),
            ],
            memberships=[
                membership(
                    "sh.600000",
                    snapshot_date=source_update_date,
                    observed_at=first_observed,
                ),
                membership(
                    "sz.000001",
                    snapshot_date=source_update_date,
                    observed_at=first_observed,
                ),
            ],
        )
    )
    second = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            components=[
                component(
                    "sh.600000",
                    snapshot_date=source_update_date,
                    observed_at=OBSERVED,
                )
            ],
            memberships=[
                membership(
                    "sh.600000",
                    snapshot_date=source_update_date,
                    observed_at=OBSERVED,
                )
            ],
        )
    )

    components = classification.index_components("hs300", SNAPSHOT_DATE)
    sectors = classification.sectors(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)
    members = classification.sector_members(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        "bank",
        SNAPSHOT_DATE,
    )

    assert [row.symbol for row in components.components] == ["sh.600000"]
    assert components.generation_id == second.generation.generation_id
    assert [(row.sector_id, row.member_count) for row in sectors.sectors] == [
        ("bank", 1)
    ]
    assert sectors.generation_id == second.generation.generation_id
    assert [row.symbol for row in members.members] == ["sh.600000"]
    assert members.generation_id == second.generation.generation_id


def _publish_after_snapshot_read(
    store: ClassificationStore,
    monkeypatch: pytest.MonkeyPatch,
    candidate: ClassificationSnapshot,
) -> None:
    if hasattr(store, "read_snapshot"):
        original = store.read_snapshot

        def read_then_publish(*args, **kwargs):
            selected = original(*args, **kwargs)
            store.publish(candidate)
            return selected

        monkeypatch.setattr(store, "read_snapshot", read_then_publish)
        return

    original = store.securities_at

    def read_then_publish(*args, **kwargs):
        selected = original(*args, **kwargs)
        store.publish(candidate)
        return selected

    monkeypatch.setattr(store, "securities_at", read_then_publish)


def test_securities_response_keeps_records_generation_and_scope_atomic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, classification = service(tmp_path)
    first = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    candidate = snapshot(
        securities=[
            security("sh.600000"),
            security("sh.688001", board="star"),
        ],
        memberships=[
            membership("sh.600000"),
            membership("sh.688001"),
        ],
    )
    _publish_after_snapshot_read(store, monkeypatch, candidate)

    response = classification.securities(SNAPSHOT_DATE)

    assert [row.symbol for row in response.securities] == ["sh.600000"]
    assert response.generation_id == first.generation.generation_id
    assert response.market_scope.covered_boards == ["main"]


def test_coverage_keeps_security_and_membership_records_in_one_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, classification = service(tmp_path)
    first = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    candidate = snapshot(
        securities=[
            security("sh.600000"),
            security("sh.688001", board="star"),
        ],
        memberships=[membership("sh.688001")],
    )
    _publish_after_snapshot_read(store, monkeypatch, candidate)

    response = classification.coverage(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        SNAPSHOT_DATE,
    )

    assert response.generation_id == first.generation.generation_id
    assert response.eligible_count == 1
    assert response.mapped_count == 1
    assert response.coverage_ratio == 1
    assert response.unmapped_symbols == []


@pytest.mark.parametrize(
    "endpoint",
    ["securities", "index_components", "sectors", "sector_members", "coverage"],
)
def test_service_endpoints_do_not_reselect_generation_through_legacy_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    store, classification = service(tmp_path)
    published = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            components=[component("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    def fail_reselection(*_args, **_kwargs):
        raise AssertionError("endpoint reselected classification generation")

    monkeypatch.setattr(store, "generation_at", fail_reselection)
    monkeypatch.setattr(store, "securities_at", fail_reselection)
    monkeypatch.setattr(store, "index_components_at", fail_reselection)
    monkeypatch.setattr(store, "sector_memberships_at", fail_reselection)
    calls = {
        "securities": lambda: classification.securities(SNAPSHOT_DATE),
        "index_components": lambda: classification.index_components(
            "hs300",
            SNAPSHOT_DATE,
        ),
        "sectors": lambda: classification.sectors(
            TAXONOMY_BAOSTOCK_INDUSTRY,
            SNAPSHOT_DATE,
        ),
        "sector_members": lambda: classification.sector_members(
            TAXONOMY_BAOSTOCK_INDUSTRY,
            "bank",
            SNAPSHOT_DATE,
        ),
        "coverage": lambda: classification.coverage(
            TAXONOMY_BAOSTOCK_INDUSTRY,
            SNAPSHOT_DATE,
        ),
    }

    response = calls[endpoint]()

    assert response.generation_id == published.generation.generation_id


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
            declared_taxonomies=[
                TAXONOMY_BAOSTOCK_INDUSTRY,
                "fixture.size_taxonomy",
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

    assert second.generation.generation_id == first.generation.generation_id
    assert first.inserted is True
    assert second.inserted is False
    assert store.generation_count() == 1
    assert first.generation.row_counts == {
        "security_master_history": 1,
        "index_component_history": 0,
        "sector_membership_history": 1,
    }


def test_generation_identity_includes_source_date_semantics(tmp_path: Path) -> None:
    store, _ = service(tmp_path)

    base = snapshot(securities=[security("sh.600000")])
    observed = store.publish(base)
    request_only = store.publish(
        base.model_copy(
            update={"source_date_semantics": "requested_unverified"}
        )
    )

    assert (
        request_only.generation.generation_id
        != observed.generation.generation_id
    )
    assert store.generation_count() == 2


def test_generation_identity_includes_declared_taxonomies(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    payload = snapshot(
        securities=[security("sh.600000")],
        memberships=[membership("sh.600000")],
    )

    promoted = store.publish(payload)
    candidate = store.publish(
        payload.model_copy(update={"declared_taxonomies": []})
    )

    assert promoted.promoted is True
    assert promoted.generation.schema_version == "classification-v3"
    assert candidate.generation.generation_id != promoted.generation.generation_id
    assert candidate.inserted is True
    assert candidate.generation.coverage_audits == []
    assert candidate.promoted is False
    assert store.ready_generation() == promoted.generation.generation_id


def test_declared_taxonomy_order_and_duplicates_do_not_change_identity(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    size_taxonomy = "fixture.size_taxonomy"
    memberships = [
        membership("sh.600000"),
        membership(
            "sh.600000",
            "large-cap",
            taxonomy_id=size_taxonomy,
        ),
    ]
    first_payload = snapshot(
        securities=[security("sh.600000")],
        memberships=memberships,
        declared_taxonomies=[
            size_taxonomy,
            TAXONOMY_BAOSTOCK_INDUSTRY,
            size_taxonomy,
        ],
    )
    second_payload = snapshot(
        securities=[security("sh.600000")],
        memberships=memberships,
        declared_taxonomies=[
            TAXONOMY_BAOSTOCK_INDUSTRY,
            size_taxonomy,
        ],
    )

    normalized = store._normalized(first_payload)
    first = store.publish(first_payload)
    second = store.publish(second_payload)

    assert normalized.declared_taxonomies == [
        TAXONOMY_BAOSTOCK_INDUSTRY,
        size_taxonomy,
    ]
    assert first.generation.generation_id == second.generation.generation_id
    assert first.inserted is True
    assert second.inserted is False


def test_changed_record_content_with_same_lineage_reaches_conflict_guard(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    payload = snapshot(
        securities=[security("sh.600000")],
        memberships=[membership("sh.600000")],
    )
    store.publish(payload)
    changed_security = payload.securities[0].model_copy(
        update={"name": "Changed without lineage update"}
    )

    with pytest.raises(
        ClassificationConflictError,
        match="conflicting security master",
    ):
        store.publish(
            payload.model_copy(update={"securities": [changed_security]})
        )

    assert store.generation_count() == 1


@pytest.mark.parametrize(
    "read_path",
    ["idempotent_publish", "generation_at", "read_snapshot"],
)
def test_persisted_legacy_generation_preserves_schema_version(
    tmp_path: Path,
    read_path: str,
) -> None:
    store, _ = service(tmp_path)
    payload = snapshot(
        securities=[security("sh.600000")],
        memberships=[membership("sh.600000")],
    )
    published = store.publish(payload)
    writer = store._connect_writer()
    writer.execute(
        """
        UPDATE classification_generations
        SET schema_version = 'classification-v2'
        WHERE generation_id = ?
        """,
        [published.generation.generation_id],
    )
    writer.close()

    generation = {
        "idempotent_publish": lambda: store.publish(payload).generation,
        "generation_at": lambda: store.generation_at(SNAPSHOT_DATE),
        "read_snapshot": lambda: store.read_snapshot(SNAPSHOT_DATE).generation,
    }[read_path]()

    assert generation is not None
    assert generation.schema_version == "classification-v2"


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


@pytest.mark.parametrize("record_kind", ["security", "index", "sector"])
@pytest.mark.parametrize(
    ("violation", "expected_error"),
    [
        ("future_source_date", "source_snapshot_date exceeds enclosing snapshot"),
        ("source", "source does not match enclosing snapshot"),
        ("source_version", "source_version does not match enclosing snapshot"),
        ("observed_at", "observed_at exceeds enclosing snapshot"),
    ],
)
def test_store_rejects_records_that_violate_snapshot_envelope(
    tmp_path: Path,
    record_kind: str,
    violation: str,
    expected_error: str,
) -> None:
    store, _ = service(tmp_path)
    trusted = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    candidate_date = date(2026, 7, 29)
    candidate_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)
    securities = [
        security(
            "sh.600000",
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
        )
    ]
    components = [
        component(
            "sh.600000",
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
        )
    ]
    memberships = [
        membership(
            "sh.600000",
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
        )
    ]
    rows = {
        "security": securities,
        "index": components,
        "sector": memberships,
    }[record_kind]
    update = {
        "future_source_date": {"source_snapshot_date": date(2026, 7, 30)},
        "source": {"source": "not-baostock"},
        "source_version": {"source_version": "not-0.9.3"},
        "observed_at": {"observed_at": candidate_observed + timedelta(seconds=1)},
    }[violation]
    rows[0] = rows[0].model_copy(update=update)

    with pytest.raises(ClassificationConflictError, match=expected_error):
        store.publish(
            snapshot(
                snapshot_date=candidate_date,
                observed_at=candidate_observed,
                securities=securities,
                components=components,
                memberships=memberships,
            )
        )

    assert store.ready_generation() == trusted.generation.generation_id
    assert store.generation_count() == 1


def test_store_rejects_empty_security_master_without_ready_pointer(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)

    with pytest.raises(
        ClassificationConflictError,
        match="security master must not be empty",
    ):
        store.publish(snapshot())

    assert store.ready_generation() is None
    assert store.generation_count() == 0


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
    rows = [
        security(
            symbol,
            board=board,
            security_type=security_type,
        )
    ]
    memberships = [membership(symbol)]
    if expected_reason is not None:
        rows.insert(0, security("sh.600000"))
        memberships = [membership("sh.600000")]
    store.publish(
        snapshot(
            securities=rows,
            memberships=memberships,
        )
    )

    response = classification.securities(SNAPSHOT_DATE, symbol=symbol)

    assert response.securities[0].eligibility.exclusion_reason == expected_reason
    assert "BSE" in response.market_scope.excluded_markets
    assert response.market_scope.derivation_version == "cn-symbol-prefix-v1"


def test_suspended_security_remains_coverage_eligible_with_actionability_reason(
    tmp_path: Path,
) -> None:
    store, classification = service(tmp_path)
    store.publish(
        snapshot(
            securities=[
                security("sz.300001", board="chinext", tradable=False)
            ],
            memberships=[membership("sz.300001")],
        )
    )

    response = classification.securities(SNAPSHOT_DATE, symbol="sz.300001")

    assert response.securities[0].eligibility.eligible is True
    assert response.securities[0].eligibility.exclusion_reason is None
    assert response.securities[0].actionability.actionable is False
    assert response.securities[0].actionability.reasons == ["suspended"]


@pytest.mark.parametrize(
    ("price_available", "expected_reason"),
    [(False, "no_price"), (None, "price_not_audited")],
)
def test_price_uncertainty_remains_coverage_eligible_but_not_actionable(
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
            ],
            memberships=[membership("sh.600000")],
        )
    )

    response = classification.securities(SNAPSHOT_DATE, symbol="sh.600000")

    assert response.securities[0].eligibility.eligible is True
    assert response.securities[0].eligibility.exclusion_reason is None
    assert response.securities[0].actionability.actionable is False
    assert response.securities[0].actionability.reasons == [expected_reason]


def test_coverage_denominator_excludes_only_out_of_scope_or_unlisted_securities(
    tmp_path: Path,
) -> None:
    store, classification = service(tmp_path)
    eligible = [
        security("sh.600000"),
        security("sz.300001", board="chinext", tradable=False),
        security("sh.688001", board="star", price_available=None),
        security("sz.000002", price_available=False),
    ]
    excluded = [
        security("sh.600001", listed=date(2026, 7, 29)),
        security("sz.000003", delisted=SNAPSHOT_DATE),
        security("bj.830001", board="bse"),
        security("sz.200001", board="b_share"),
        security("sh.000001", board="index", security_type="index"),
    ]
    store.publish(
        snapshot(
            securities=[*eligible, *excluded],
            memberships=[membership(row.symbol) for row in eligible],
        )
    )

    audit = classification.coverage(TAXONOMY_BAOSTOCK_INDUSTRY, SNAPSHOT_DATE)

    assert audit.eligible_count == 4
    assert audit.mapped_count == 4
    assert audit.coverage_ratio == 1
    assert audit.exclusion_reasons == {
        "delisted": 1,
        "non_stock": 1,
        "not_listed_yet": 1,
        "unsupported_board": 1,
        "unsupported_exchange": 1,
    }
    assert audit.actionability_reasons == {
        "no_price": 1,
        "price_not_audited": 1,
        "suspended": 1,
    }


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
    outcome = store.publish(
        snapshot(securities=securities, memberships=memberships)
    )

    audit = outcome.generation.coverage_audits[0]

    assert audit.status == expected_status
    assert audit.eligible_count == 100
    assert audit.mapped_count == mapped
    assert audit.coverage_ratio == pytest.approx(mapped / 100)
    assert audit.unmapped_symbols == [row.symbol for row in securities[mapped:]]


def test_coverage_zero_denominator_is_degraded_not_one_hundred_percent(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    outcome = store.publish(
        snapshot(
            securities=[
                security("sh.000001", board="index", security_type="index"),
            ],
            memberships=[],
        )
    )

    audit = outcome.generation.coverage_audits[0]

    assert audit.status == "degraded"
    assert audit.eligible_count == 0
    assert audit.coverage_ratio is None
    assert audit.quality_issues == ["no_eligible_securities"]


def test_low_coverage_candidate_is_persisted_without_initial_ready_pointer(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    securities = [security(f"sh.{600000 + index:06d}") for index in range(100)]

    outcome = store.publish(
        snapshot(
            securities=securities,
            memberships=[membership(row.symbol) for row in securities[:94]],
        )
    )

    assert outcome.inserted is True
    assert outcome.promoted is False
    assert outcome.generation.coverage_audits[0].status == "degraded"
    assert store.generation_count() == 1
    assert store.ready_generation() is None


def test_empty_declared_taxonomies_never_vacuously_promote(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)

    outcome = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            declared_taxonomies=[],
        )
    )

    assert outcome.inserted is True
    assert outcome.generation.coverage_audits == []
    assert outcome.promoted is False
    assert store.ready_generation() is None


def test_empty_declared_taxonomies_candidate_does_not_replace_ready(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    trusted = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    candidate_date = date(2026, 7, 29)
    candidate_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)

    outcome = store.publish(
        snapshot(
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
            securities=[
                security(
                    "sh.688001",
                    board="star",
                    snapshot_date=candidate_date,
                    observed_at=candidate_observed,
                )
            ],
            declared_taxonomies=[],
        )
    )

    assert outcome.generation.coverage_audits == []
    assert outcome.promoted is False
    assert store.ready_generation() == trusted.generation.generation_id


def test_degraded_candidate_does_not_replace_existing_ready_generation(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    trusted = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    candidate_date = date(2026, 7, 29)
    candidate_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)
    securities = [
        security(
            f"sh.{600000 + index:06d}",
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
        )
        for index in range(100)
    ]

    outcome = store.publish(
        snapshot(
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
            securities=securities,
            memberships=[
                membership(
                    row.symbol,
                    snapshot_date=candidate_date,
                    observed_at=candidate_observed,
                )
                for row in securities[:94]
            ],
        )
    )

    assert outcome.inserted is True
    assert outcome.promoted is False
    assert outcome.generation.coverage_audits[0].coverage_ratio == pytest.approx(
        0.94
    )
    assert store.generation_count() == 2
    assert store.ready_generation() == trusted.generation.generation_id


def test_zero_denominator_candidate_never_becomes_ready(tmp_path: Path) -> None:
    store, _ = service(tmp_path)

    outcome = store.publish(
        snapshot(
            securities=[
                security("sh.000001", board="index", security_type="index"),
            ],
            memberships=[],
        )
    )

    assert outcome.inserted is True
    assert outcome.promoted is False
    assert outcome.generation.coverage_audits[0].quality_issues == [
        "no_eligible_securities"
    ]
    assert store.ready_generation() is None


def test_degraded_candidate_never_leaks_through_later_ready_pointer(
    tmp_path: Path,
) -> None:
    store, classification = service(tmp_path)
    candidate_date = date(2026, 7, 29)
    candidate_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)
    candidate = store.publish(
        snapshot(
            snapshot_date=candidate_date,
            observed_at=candidate_observed,
            securities=[
                security(
                    "sh.688001",
                    board="star",
                    snapshot_date=candidate_date,
                    observed_at=candidate_observed,
                ),
                security(
                    "sz.300001",
                    board="chinext",
                    snapshot_date=candidate_date,
                    observed_at=candidate_observed,
                ),
            ],
            memberships=[
                membership(
                    "sh.688001",
                    snapshot_date=candidate_date,
                    observed_at=candidate_observed,
                )
            ],
        )
    )
    trusted = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    selected = store.read_snapshot(
        candidate_date,
        include_securities=True,
        taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
    )
    securities = classification.securities(candidate_date)
    coverage = classification.coverage(
        TAXONOMY_BAOSTOCK_INDUSTRY,
        candidate_date,
    )

    assert candidate.promoted is False
    assert store.ready_generation() == trusted.generation.generation_id
    assert selected.generation == trusted.generation
    assert [row.symbol for row in selected.securities] == ["sh.600000"]
    assert selected.generation.market_scope.covered_boards == ["main"]
    assert securities.status == "ready"
    assert [row.symbol for row in securities.securities] == ["sh.600000"]
    assert coverage.status == "ready"
    assert coverage.coverage_ratio == 1


def test_older_ready_history_is_readable_without_regressing_current_pointer(
    tmp_path: Path,
) -> None:
    store, _ = service(tmp_path)
    current_date = date(2026, 7, 29)
    current_observed = datetime(2026, 7, 29, 12, tzinfo=UTC)
    current = store.publish(
        snapshot(
            snapshot_date=current_date,
            observed_at=current_observed,
            securities=[
                security(
                    "sh.688001",
                    board="star",
                    snapshot_date=current_date,
                    observed_at=current_observed,
                )
            ],
            memberships=[
                membership(
                    "sh.688001",
                    snapshot_date=current_date,
                    observed_at=current_observed,
                )
            ],
        )
    )
    historical = store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    historical_read = store.read_snapshot(
        SNAPSHOT_DATE,
        include_securities=True,
    )
    current_read = store.read_snapshot(
        current_date,
        include_securities=True,
    )

    assert historical.promoted is True
    assert store.ready_generation() == current.generation.generation_id
    assert historical_read.generation == historical.generation
    assert [row.symbol for row in historical_read.securities] == ["sh.600000"]
    assert current_read.generation == current.generation
    assert [row.symbol for row in current_read.securities] == ["sh.688001"]


def test_coverage_is_reproducible_and_reports_unmapped_symbols(tmp_path: Path) -> None:
    store, _ = service(tmp_path)
    first = store.publish(
        snapshot(
            securities=[security("sh.600000"), security("sz.000001")],
            memberships=[membership("sh.600000")],
        )
    )
    second = store.publish(
        snapshot(
            securities=[security("sh.600000"), security("sz.000001")],
            memberships=[membership("sh.600000")],
        )
    )

    first_audit = first.generation.coverage_audits[0]
    second_audit = second.generation.coverage_audits[0]

    assert first_audit == second_audit
    assert first_audit.unmapped_symbols == ["sz.000001"]
    assert first_audit.generation_id is not None
    assert first_audit.source_lineage == ["baostock@0.9.3"]


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
    def __init__(
        self,
        *,
        mixed_industry_dates: bool = False,
        industry_date: str = "2026-07-20",
        index_date: str = "2026-07-20",
    ) -> None:
        self.calls: list[str] = []
        self.logged_out = False
        self.mixed_industry_dates = mixed_industry_dates
        self.industry_date = industry_date
        self.index_date = index_date
        self.query_all_stock_day: str | None = None

    def login(self):
        return FakeResult([], [])

    def logout(self):
        self.logged_out = True

    def query_all_stock(self, **kwargs):
        self.calls.append("query_all_stock")
        self.query_all_stock_day = kwargs["day"]
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
        second_date = "2026-07-21" if self.mixed_industry_dates else self.industry_date
        return FakeResult(
            ["updateDate", "code", "code_name", "industry", "industryClassification"],
            [
                [self.industry_date, "sh.600000", "浦发银行", "银行", "证监会行业分类"],
                [second_date, "sz.300001", "创业板样本", "软件服务", "证监会行业分类"],
                [self.industry_date, "sh.688001", "科创板样本", "半导体", "证监会行业分类"],
            ],
        )

    def _index(self, call: str, rows: list[list[str]]):
        self.calls.append(call)
        return FakeResult(["updateDate", "code", "code_name"], rows)

    def query_hs300_stocks(self, **_kwargs):
        return self._index(
            "query_hs300_stocks",
            [[self.index_date, "sh.600000", "浦发银行"]],
        )

    def query_sz50_stocks(self, **_kwargs):
        return self._index(
            "query_sz50_stocks",
            [[self.index_date, "sh.600000", "浦发银行"]],
        )

    def query_zz500_stocks(self, **_kwargs):
        return self._index(
            "query_zz500_stocks",
            [[self.index_date, "sz.300001", "创业板样本"]],
        )


class BlockingClassificationClient(FakeClassificationClient):
    def __init__(self, failure_stage: str) -> None:
        super().__init__()
        self.failure_stage = failure_stage
        self.login_calls = 0

    def _block(self):
        threading.Event().wait()

    def login(self):
        self.login_calls += 1
        if self.failure_stage == "login":
            self._block()
        return super().login()

    def query_all_stock(self, **kwargs):
        if self.failure_stage == "security_universe":
            self._block()
        return super().query_all_stock(**kwargs)

    def query_stock_basic(self, **kwargs):
        if self.failure_stage == "security_basic":
            self._block()
        return super().query_stock_basic(**kwargs)

    def query_stock_industry(self, **kwargs):
        if self.failure_stage == "industry":
            self._block()
        return super().query_stock_industry(**kwargs)

    def query_hs300_stocks(self, **kwargs):
        if self.failure_stage == "hs300":
            self._block()
        return super().query_hs300_stocks(**kwargs)

    def query_sz50_stocks(self, **kwargs):
        if self.failure_stage == "sz50":
            self._block()
        return super().query_sz50_stocks(**kwargs)

    def query_zz500_stocks(self, **kwargs):
        if self.failure_stage == "csi500":
            self._block()
        return super().query_zz500_stocks(**kwargs)


@pytest.mark.parametrize(
    ("exception_type", "expected_class", "expected_requests", "expected_logout"),
    [
        (OSError, "transport", 2, False),
        (BaoStockError, "internal", 2, False),
        (RuntimeError, "internal", 1, True),
    ],
)
def test_provider_sanitizes_transport_and_internal_stage_failures(
    exception_type: type[Exception],
    expected_class: str,
    expected_requests: int,
    expected_logout: bool,
) -> None:
    class FailingClient(FakeClassificationClient):
        def query_all_stock(self, **_kwargs):
            raise exception_type("STAGE_FAILURE_SECRET /Users/private/stage")

    client = FailingClient()
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.failure.failure_stage == "security_universe"
    assert error.value.failure.failure_class == expected_class
    assert error.value.failure.provider_request_count == expected_requests
    assert provider.session._session_usable is False
    assert client.logged_out is expected_logout
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    formatted = _owned_traceback_locals(error.value)
    assert "STAGE_FAILURE_SECRET" not in formatted
    assert "/Users/private/stage" not in formatted


def test_provider_does_not_retain_raw_exception_reference() -> None:
    probe = _RawExceptionProbe()

    class SecretProviderError(RuntimeError):
        pass

    class FailingClient(FakeClassificationClient):
        def query_all_stock(self, **_kwargs):
            raw_error = SecretProviderError(
                "PROVIDER_RETAINED_SECRET /Users/private/provider-retained"
            )
            probe.remember(raw_error)
            raise raw_error

    provider = BaoStockClassificationProvider(
        client=FailingClient(),
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as captured:
        provider.fetch(SNAPSHOT_DATE)

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    formatted = _owned_traceback_locals(captured.value)
    assert "PROVIDER_RETAINED_SECRET" not in formatted
    assert "/Users/private/provider-retained" not in formatted
    del captured
    gc.collect()
    assert probe.reference is not None
    assert probe.reference() is None


def test_provider_counts_only_metadata_operations_when_relogin_fails() -> None:
    class QueryThenReloginFailsClient(FakeClassificationClient):
        def __init__(self) -> None:
            super().__init__()
            self.login_calls = 0
            self.query_calls = 0

        def login(self):
            self.login_calls += 1
            if self.login_calls == 1:
                return super().login()
            raise OSError("RELOGIN_SECRET /Users/private/relogin.sock")

        def query_all_stock(self, **_kwargs):
            self.query_calls += 1
            raise OSError("RELOGIN_SECRET /Users/private/relogin.sock")

    client = QueryThenReloginFailsClient()
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.failure.failure_stage == "security_universe"
    assert error.value.failure.failure_class == "transport"
    assert error.value.failure.provider_request_count == 1
    assert provider.last_request_count == 1
    assert client.query_calls == 1
    assert client.login_calls == 3
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    formatted = _owned_traceback_locals(error.value)
    assert "RELOGIN_SECRET" not in formatted
    assert "/Users/private/relogin.sock" not in formatted


@pytest.mark.parametrize(
    "exception_type",
    [RuntimeError, BaoStockError],
)
def test_provider_discards_half_initialized_login_before_sanitizing(
    exception_type: type[Exception],
) -> None:
    class HalfInitializedSocket:
        def __init__(self) -> None:
            self.shutdown_calls = 0
            self.closed = False

        def shutdown(self, _how) -> None:
            self.shutdown_calls += 1

        def close(self) -> None:
            self.closed = True

    class HalfInitializedClient(FakeClassificationClient):
        def __init__(self) -> None:
            super().__init__()
            self.context = type("Context", (), {"default_socket": None})()
            self.socket = HalfInitializedSocket()

        def login(self):
            self.context.default_socket = self.socket
            raise exception_type("HALF_LOGIN_SECRET /Users/private/login.sock")

    client = HalfInitializedClient()
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "login"
    assert error.value.failure.failure_class == "internal"
    assert error.value.failure.provider_request_count == 0
    assert client.socket.shutdown_calls == 1
    assert client.socket.closed is True
    assert client.context.default_socket is None
    assert provider.session._session_usable is False
    formatted = _owned_traceback_locals(error.value)
    assert "/Users/private" not in formatted
    assert "HALF_LOGIN_SECRET" not in formatted


def test_provider_maps_active_session_conflict_to_internal() -> None:
    provider = BaoStockClassificationProvider(
        client=FakeClassificationClient(),
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )
    provider.session._session_usable = True

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "login"
    assert error.value.failure.failure_class == "internal"
    assert error.value.failure.provider_request_count == 0


def test_provider_maps_arbitrary_clock_value_error_to_validation_internal() -> None:
    def invalid_clock():
        raise ValueError("CLOCK_SECRET /Users/private/clock")

    provider = BaoStockClassificationProvider(
        client=FakeClassificationClient(),
        clock=invalid_clock,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "validation"
    assert error.value.failure.failure_class == "internal"
    assert error.value.failure.provider_request_count == 0
    formatted = _owned_traceback_locals(error.value)
    assert "CLOCK_SECRET" not in formatted
    assert "/Users/private/clock" not in formatted


def test_provider_converts_invalid_source_date_to_typed_schema_failure() -> None:
    provider = BaoStockClassificationProvider(
        client=FakeClassificationClient(industry_date="not-a-date"),
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "industry"
    assert error.value.failure.failure_class == "schema"
    assert error.value.failure.provider_request_count == 3


def test_provider_converts_pydantic_record_error_to_typed_schema_failure() -> None:
    provider = BaoStockClassificationProvider(
        client=FakeClassificationClient(),
        clock=lambda: "not-a-datetime",
        min_request_interval_seconds=0,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "validation"
    assert error.value.failure.failure_class == "schema"
    assert error.value.failure.provider_request_count == 2


@pytest.mark.parametrize(
    ("stage", "expected_requests"),
    [
        ("login", 0),
        ("security_universe", 2),
        ("security_basic", 3),
        ("industry", 4),
        ("hs300", 5),
        ("sz50", 6),
        ("csi500", 7),
    ],
)
def test_provider_deadline_failure_reports_exact_sanitized_stage(
    stage: str,
    expected_requests: int,
) -> None:
    client = BlockingClassificationClient(stage)
    ticks = iter([10.0, 12.345])
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
        min_request_interval_seconds=0,
        socket_timeout_seconds=0.02,
        monotonic_fn=lambda: next(ticks),
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    failure = getattr(error.value, "failure", None)
    assert failure is not None
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert failure.failure_stage == stage
    assert failure.failure_class == "deadline"
    assert failure.provider_request_count == expected_requests
    assert failure.elapsed_seconds == 2.345
    assert failure.configured_timeout_seconds == 0.02
    assert failure.configured_max_attempts == 2
    assert provider.last_request_count == expected_requests
    assert provider.session._session_usable is False
    assert client.login_calls == 2
    assert client.logged_out is False


class IncompleteClassificationClient(FakeClassificationClient):
    def __init__(self, failure: str) -> None:
        super().__init__()
        self.failure = failure

    def query_all_stock(self, **kwargs):
        if self.failure == "empty_all":
            self.calls.append("query_all_stock")
            self.query_all_stock_day = kwargs["day"]
            return FakeResult(["code", "tradeStatus", "code_name"], [])
        if self.failure == "missing_all_field":
            self.calls.append("query_all_stock")
            self.query_all_stock_day = kwargs["day"]
            return FakeResult(
                ["code", "code_name"],
                [["sh.600000", "浦发银行"]],
            )
        return super().query_all_stock(**kwargs)

    def query_stock_basic(self, **kwargs):
        if self.failure == "empty_basic":
            self.calls.append("query_stock_basic")
            return FakeResult(
                ["code", "code_name", "ipoDate", "outDate", "type", "status"],
                [],
            )
        if self.failure == "missing_basic_field":
            self.calls.append("query_stock_basic")
            return FakeResult(
                ["code", "code_name", "ipoDate", "outDate", "type"],
                [["sh.600000", "浦发银行", "1999-11-10", "", "1"]],
            )
        if self.failure in {"missing_target_basic", "unusable_target_basic"}:
            result = super().query_stock_basic(**kwargs)
            rows = [
                ["sz.300001", "创业板样本", "2009-10-30", "", "1", "1"],
                ["sh.688001", "科创板样本", "2019-07-22", "", "1", "1"],
                ["sh.000001", "上证综指", "1991-07-15", "", "2", "1"],
                ["sz.200001", "深市B股样本", "1995-01-01", "", "1", "1"],
            ]
            if self.failure == "unusable_target_basic":
                rows.append(["sh.600000", "浦发银行", "1999-11-10", "", "", ""])
            return FakeResult(result.fields, rows)
        return super().query_stock_basic(**kwargs)

    def query_stock_industry(self, **kwargs):
        if self.failure == "empty_industry":
            self.calls.append("query_stock_industry")
            return FakeResult(
                [
                    "updateDate",
                    "code",
                    "code_name",
                    "industry",
                    "industryClassification",
                ],
                [],
            )
        if self.failure == "missing_industry_field":
            self.calls.append("query_stock_industry")
            return FakeResult(
                ["updateDate", "code", "code_name", "industry"],
                [["2026-07-20", "sh.600000", "浦发银行", "银行"]],
            )
        return super().query_stock_industry(**kwargs)

    def query_hs300_stocks(self, **kwargs):
        if self.failure == "empty_component":
            return self._index("query_hs300_stocks", [])
        if self.failure == "missing_component_field":
            self.calls.append("query_hs300_stocks")
            return FakeResult(
                ["updateDate", "code"],
                [["2026-07-20", "sh.600000"]],
            )
        return super().query_hs300_stocks(**kwargs)

    def query_sz50_stocks(self, **kwargs):
        if self.failure == "empty_sz50_component":
            return self._index("query_sz50_stocks", [])
        if self.failure == "missing_sz50_component_field":
            self.calls.append("query_sz50_stocks")
            return FakeResult(
                ["updateDate", "code"],
                [["2026-07-20", "sh.600000"]],
            )
        return super().query_sz50_stocks(**kwargs)

    def query_zz500_stocks(self, **kwargs):
        if self.failure == "empty_csi500_component":
            return self._index("query_zz500_stocks", [])
        if self.failure == "missing_csi500_component_field":
            self.calls.append("query_zz500_stocks")
            return FakeResult(
                ["updateDate", "code"],
                [["2026-07-20", "sz.300001"]],
            )
        return super().query_zz500_stocks(**kwargs)


@pytest.mark.parametrize(
    ("failure", "expected_stage", "expected_class", "expected_requests"),
    [
        ("empty_all", "security_universe", "data_quality", 1),
        ("missing_all_field", "security_universe", "schema", 1),
        ("empty_basic", "security_basic", "data_quality", 2),
        ("missing_basic_field", "security_basic", "schema", 2),
        ("missing_target_basic", "validation", "data_quality", 2),
        ("unusable_target_basic", "validation", "data_quality", 2),
        ("empty_industry", "industry", "data_quality", 3),
        ("missing_industry_field", "industry", "schema", 3),
        ("empty_component", "hs300", "data_quality", 4),
        ("missing_component_field", "hs300", "schema", 4),
        ("empty_sz50_component", "sz50", "data_quality", 5),
        ("missing_sz50_component_field", "sz50", "schema", 5),
        ("empty_csi500_component", "csi500", "data_quality", 6),
        ("missing_csi500_component_field", "csi500", "schema", 6),
    ],
)
def test_provider_rejects_empty_or_incomplete_required_metadata(
    failure: str,
    expected_stage: str,
    expected_class: str,
    expected_requests: int,
) -> None:
    ticks = iter([20.0, 20.75])
    provider = BaoStockClassificationProvider(
        client=IncompleteClassificationClient(failure),
        clock=lambda: OBSERVED,
        monotonic_fn=lambda: next(ticks),
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.failure.failure_stage == expected_stage
    assert error.value.failure.failure_class == expected_class
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.provider_request_count == expected_requests
    assert error.value.failure.elapsed_seconds == 0.75
    assert provider.last_request_count == expected_requests
    assert provider.session._session_usable is False
    assert provider.session.client.logged_out is True


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
    assert by_symbol["sh.600000"].listing_status == "1"
    assert by_symbol["sh.600000"].daily_trade_status == "1"
    assert payload.source_date_semantics == "requested_unverified"
    assert by_symbol["sh.600000"].source_date_semantics == "requested_unverified"
    assert all(
        row.source_date_semantics == "source_observed"
        for row in [*payload.index_components, *payload.sector_memberships]
    )
    assert client.query_all_stock_day == SNAPSHOT_DATE.isoformat()
    assert "source_status" not in type(by_symbol["sh.600000"]).model_fields
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

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.failure.failure_stage == "industry"
    assert error.value.failure.failure_class == "data_quality"
    assert error.value.failure.provider_request_count == 3


@pytest.mark.parametrize(
    ("client", "expected_stage", "expected_requests"),
    [
        (FakeClassificationClient(industry_date="2026-07-29"), "industry", 3),
        (FakeClassificationClient(index_date="2026-07-29"), "hs300", 4),
    ],
)
def test_provider_rejects_source_update_date_after_requested_as_of(
    client: FakeClassificationClient,
    expected_stage: str,
    expected_requests: int,
) -> None:
    provider = BaoStockClassificationProvider(
        client=client,
        clock=lambda: OBSERVED,
    )

    with pytest.raises(ClassificationProviderError) as error:
        provider.fetch(SNAPSHOT_DATE)

    assert error.value.failure.failure_stage == expected_stage
    assert error.value.failure.failure_class == "data_quality"
    assert error.value.failure.provider_request_count == expected_requests


def api_request(
    store: ClassificationStore,
    path: str,
    *,
    overrides: dict | None = None,
) -> httpx.Response:
    app.dependency_overrides[get_classification_store] = lambda: store
    app.dependency_overrides.update(overrides or {})

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def test_api_ready_coverage_contract(tmp_path: Path) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    store.publish(
        snapshot(
            securities=[
                security("sh.600000"),
                security("sz.300001", board="chinext"),
            ],
            memberships=[
                membership("sh.600000"),
                membership("sz.300001"),
            ],
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
    assert securities.json()["securities"][0]["listing_status"] == "1"
    assert securities.json()["securities"][0]["daily_trade_status"] == "1"
    assert securities.json()["securities"][0]["actionability"] == {
        "actionable": True,
        "reasons": [],
    }
    assert "BSE" in securities.json()["market_scope"]["excluded_markets"]
    assert components.status_code == 200
    assert components.json()["status"] == "degraded"
    assert components.json()["index"]["component_history_capability"] == "unverified"
    assert components.json()["quality_issues"] == [
        "component_history_unverified"
    ]
    assert [row["symbol"] for row in components.json()["components"]] == ["sh.600000"]
    assert sectors.status_code == 200
    assert sectors.json()["sectors"][0]["sector_id"] == "bank"
    assert members.status_code == 200
    assert members.json()["members"][0]["raw_classification"] == "证监会行业分类"
    assert coverage.status_code == 200
    assert coverage.json()["status"] == "ready"
    assert coverage.json()["coverage_ratio"] == 1
    assert coverage.json()["unmapped_symbols"] == []


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
    csi1000_metadata_only = api_request(
        empty_store,
        (
            "/api/v1/classification/indexes/csi1000/components"
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
    assert csi1000_metadata_only.status_code == 200
    assert csi1000_metadata_only.json()["index"]["symbol"] == "sh.000852"
    assert csi1000_metadata_only.json()["components"] == []
    assert csi1000_metadata_only.json()["quality_issues"] == [
        "component_history_not_supplied_by_source"
    ]


def test_missing_classification_database_get_has_no_filesystem_side_effects(
    tmp_path: Path,
) -> None:
    database = tmp_path / "missing-parent" / "classification.duckdb"
    temp_directory = tmp_path / "missing-temp" / "duckdb"
    store = ClassificationStore(database, temp_directory=temp_directory)

    response = api_request(
        store,
        f"/api/v1/classification/securities?as_of={SNAPSHOT_DATE}",
    )

    assert response.status_code == 200
    assert response.json()["status"] == "empty"
    assert not database.exists()
    assert not database.parent.exists()
    assert not temp_directory.exists()
    assert not temp_directory.parent.exists()


def test_existing_classification_database_get_executes_no_write_or_schema_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    store_module = __import__(
        "backend.app.classification.store",
        fromlist=["duckdb"],
    )
    original_connect = store_module.duckdb.connect
    database_before = store.path.read_bytes()

    class RejectingDdlConnection:
        def __init__(self, connection) -> None:
            self.connection = connection

        def execute(self, sql, parameters=None):
            operation = sql.lstrip().split(None, 1)[0].upper()
            if operation in {
                "CREATE",
                "ALTER",
                "DROP",
                "INSERT",
                "UPDATE",
                "DELETE",
            }:
                raise AssertionError(
                    f"read path executed write or schema DDL: {operation}"
                )
            return self.connection.execute(sql, parameters or [])

        def close(self) -> None:
            self.connection.close()

    def connect_without_ddl(*args, **kwargs):
        return RejectingDdlConnection(original_connect(*args, **kwargs))

    monkeypatch.setattr(store_module.duckdb, "connect", connect_without_ddl)

    response = api_request(
        store,
        f"/api/v1/classification/securities?as_of={SNAPSHOT_DATE}",
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert store.path.read_bytes() == database_before


def test_classification_read_is_compatible_with_same_process_writer(
    tmp_path: Path,
) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    store.publish(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )
    writer = store._connect_writer()
    try:
        response = api_request(
            store,
            f"/api/v1/classification/securities?as_of={SNAPSHOT_DATE}",
        )
    finally:
        writer.close()

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_legacy_schema_migration_is_writer_only(tmp_path: Path) -> None:
    database = tmp_path / "classification.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        CREATE TABLE classification_generations (
            sequence BIGINT PRIMARY KEY,
            generation_id VARCHAR UNIQUE NOT NULL,
            schema_version VARCHAR NOT NULL,
            source VARCHAR NOT NULL,
            source_version VARCHAR NOT NULL,
            source_snapshot_date DATE NOT NULL,
            source_date_semantics VARCHAR NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            row_counts JSON NOT NULL,
            coverage_audits JSON NOT NULL,
            market_scope JSON NOT NULL
        )
        """
    )
    connection.close()
    store = ClassificationStore(
        database,
        temp_directory=tmp_path / "classification-temp",
    )

    reader = store._connect_reader()
    assert reader is not None
    reader_columns = {
        row[1]
        for row in reader.execute(
            "PRAGMA table_info('classification_generations')"
        ).fetchall()
    }
    reader.close()

    assert "promoted" not in reader_columns
    assert not store.temp_directory.exists()

    writer = store._connect_writer()
    writer_columns = {
        row[1]
        for row in writer.execute(
            "PRAGMA table_info('classification_generations')"
        ).fetchall()
    }
    writer.close()

    assert "promoted" in writer_columns
    assert store.temp_directory.exists()


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/classification/securities?as_of=2026-07-30",
        "/api/v1/classification/indexes/hs300/components?as_of=2026-07-30",
        (
            "/api/v1/classification/taxonomies/"
            f"{TAXONOMY_BAOSTOCK_INDUSTRY}/sectors?as_of=2026-07-30"
        ),
        (
            "/api/v1/classification/taxonomies/"
            f"{TAXONOMY_BAOSTOCK_INDUSTRY}/sectors/bank/members"
            "?as_of=2026-07-30"
        ),
        (
            "/api/v1/classification/coverage?as_of=2026-07-30"
            f"&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}"
        ),
    ],
)
def test_classification_api_rejects_future_as_of_before_store_access(
    path: str,
) -> None:
    import backend.app.api.classification as classification_api

    class RejectReadStore:
        def read_snapshot(self, *_args, **_kwargs):
            raise AssertionError("future as_of reached classification store")

    response = api_request(
        RejectReadStore(),
        path,
        overrides={
            classification_api.get_classification_today: lambda: date(2026, 7, 29)
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "future_as_of"


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


class ObservableClassificationProvider(RecordingClassificationProvider):
    def __init__(self, payload: ClassificationSnapshot) -> None:
        super().__init__(payload)
        self.last_request_count = 6
        self.session = type(
            "SessionDiagnostics",
            (),
            {"socket_timeout_seconds": 17.0, "max_attempts": 2},
        )()


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


def test_sync_persists_degraded_candidate_without_ready_pointer(tmp_path: Path) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    provider = RecordingClassificationProvider(
        snapshot(
            securities=[
                security("sh.600000"),
                security("sz.000001"),
            ],
            memberships=[membership("sh.600000")],
        )
    )

    result = run_classification_sync(
        as_of=SNAPSHOT_DATE,
        execute=True,
        store=store,
        provider=provider,
    )

    assert result.status == "degraded"
    assert result.writes_classification_data is True
    assert result.generation is not None
    assert store.ready_generation() is None
    assert store.generation_count() == 1


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


def test_classification_sync_idempotent_rerun_reports_no_write(
    tmp_path: Path,
) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    provider = RecordingClassificationProvider(
        snapshot(
            securities=[security("sh.600000")],
            memberships=[membership("sh.600000")],
        )
    )

    first = run_classification_sync(
        as_of=SNAPSHOT_DATE,
        execute=True,
        store=store,
        provider=provider,
    )
    second = run_classification_sync(
        as_of=SNAPSHOT_DATE,
        execute=True,
        store=store,
        provider=provider,
    )

    assert first.writes_classification_data is True
    assert first.new_generation is True
    assert second.writes_classification_data is False
    assert second.new_generation is False
    assert second.generation == first.generation
    assert store.generation_count() == 1


def test_classification_sync_execute_rejects_future_as_of_before_provider_call(
    tmp_path: Path,
) -> None:
    store = ClassificationStore(tmp_path / "classification.duckdb")
    provider = RecordingClassificationProvider(
        snapshot(securities=[security("sh.600000")])
    )

    with pytest.raises(ClassificationSyncError) as error:
        run_classification_sync(
            as_of=date.max,
            execute=True,
            store=store,
            provider=provider,
        )

    assert error.value.failure.failure_stage == "validation"
    assert error.value.failure.failure_class == "data_quality"
    assert error.value.failure.provider_request_count == 0
    assert provider.calls == []
    assert store.ready_generation() is None


@pytest.mark.parametrize(
    ("exception_type", "expected_stage", "expected_class"),
    [
        (ClassificationConflictError, "publication", "conflict"),
        (OSError, "publication", "storage"),
        (RuntimeError, "publication", "internal"),
    ],
)
def test_classification_sync_sanitizes_publication_failures(
    exception_type: type[Exception],
    expected_stage: str,
    expected_class: str,
) -> None:
    provider = ObservableClassificationProvider(snapshot(securities=[security("sh.600000")]))

    class FailingStore:
        def publish(self, _snapshot: ClassificationSnapshot):
            raise exception_type("PUBLICATION_FAILURE_SECRET /Users/private/publication.db")

    with pytest.raises(ClassificationSyncError) as error:
        run_classification_sync(
            as_of=SNAPSHOT_DATE,
            execute=True,
            store=FailingStore(),
            provider=provider,
        )

    failure = error.value.failure
    assert failure.failure_stage == expected_stage
    assert failure.failure_class == expected_class
    assert failure.provider_request_count == 6
    assert failure.elapsed_seconds >= 0
    assert failure.configured_timeout_seconds == 17
    assert failure.configured_max_attempts == 2
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    formatted = _owned_traceback_locals(error.value)
    assert "PUBLICATION_FAILURE_SECRET" not in formatted
    assert "/Users/private/publication.db" not in formatted


def test_classification_sync_sanitizes_unstructured_provider_failure() -> None:
    provider = ObservableClassificationProvider(snapshot(securities=[security("sh.600000")]))

    def hostile_fetch(_as_of: date) -> ClassificationSnapshot:
        raise RuntimeError("token=do-not-leak /Users/private/provider.db")

    provider.fetch = hostile_fetch

    with pytest.raises(ClassificationSyncError) as error:
        run_classification_sync(
            as_of=SNAPSHOT_DATE,
            execute=True,
            store=object(),
            provider=provider,
        )

    failure = error.value.failure
    assert failure.failure_stage == "validation"
    assert failure.failure_class == "internal"
    assert failure.provider_request_count == 6
    assert failure.configured_timeout_seconds == 17
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    formatted = _owned_traceback_locals(error.value)
    assert "/Users/private" not in formatted
    assert "token=do-not-leak" not in formatted


def test_classification_sync_does_not_retain_raw_exception_reference() -> None:
    probe = _RawExceptionProbe()
    provider = ObservableClassificationProvider(snapshot(securities=[security("sh.600000")]))

    class SecretSyncError(RuntimeError):
        pass

    def hostile_fetch(_as_of: date) -> ClassificationSnapshot:
        raw_error = SecretSyncError("SYNC_RETAINED_SECRET /Users/private/sync-retained")
        probe.remember(raw_error)
        raise raw_error

    provider.fetch = hostile_fetch

    with pytest.raises(ClassificationSyncError) as captured:
        run_classification_sync(
            as_of=SNAPSHOT_DATE,
            execute=True,
            store=object(),
            provider=provider,
        )

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    formatted = _owned_traceback_locals(captured.value)
    assert "SYNC_RETAINED_SECRET" not in formatted
    assert "/Users/private/sync-retained" not in formatted
    del captured
    gc.collect()
    assert probe.reference is not None
    assert probe.reference() is None


def test_classification_sync_strips_raw_cause_from_structured_provider_failure() -> None:
    provider = ObservableClassificationProvider(snapshot(securities=[security("sh.600000")]))

    def hostile_fetch(_as_of: date) -> ClassificationSnapshot:
        try:
            raise ValueError("STRUCTURED_PROVIDER_SECRET /Users/private/structured-provider.db")
        except ValueError as raw_error:
            raise ClassificationProviderError(
                ClassificationFailure(
                    failure_stage="security_basic",
                    failure_class="schema",
                    elapsed_seconds=1.25,
                    provider_request_count=2,
                    configured_timeout_seconds=17,
                    configured_max_attempts=2,
                )
            ) from raw_error

    provider.fetch = hostile_fetch

    with pytest.raises(ClassificationSyncError) as error:
        run_classification_sync(
            as_of=SNAPSHOT_DATE,
            execute=True,
            store=object(),
            provider=provider,
        )

    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert error.value.failure.failure_stage == "security_basic"
    assert error.value.failure.failure_class == "schema"
    assert error.value.failure.provider_request_count == 2
    formatted = _owned_traceback_locals(error.value)
    assert "STRUCTURED_PROVIDER_SECRET" not in formatted
    assert "/Users/private/structured-provider.db" not in formatted


def test_classification_cli_deadline_failure_is_json_and_creates_no_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    market_dir = tmp_path / "market"
    temp_dir = tmp_path / "tmp"
    settings = Settings(
        _env_file=None,
        market_data_dir=market_dir,
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=temp_dir,
        local_market_dataset_root=tmp_path / "dataset",
    )

    class DeadlineProvider:
        def __init__(self, **_kwargs) -> None:
            pass

        def fetch(self, _as_of: date) -> ClassificationSnapshot:
            raise ClassificationProviderError(
                ClassificationFailure(
                    failure_stage="security_universe",
                    failure_class="deadline",
                    elapsed_seconds=120.25,
                    provider_request_count=2,
                    configured_timeout_seconds=120,
                    configured_max_attempts=2,
                )
            )

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockClassificationProvider", DeadlineProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
            "--socket-timeout-seconds",
            "120",
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "error",
        "as_of": SNAPSHOT_DATE.isoformat(),
        "quality_issues": ["classification_sync_failed"],
        "writes_classification_data": False,
        "failure_stage": "security_universe",
        "failure_class": "deadline",
        "elapsed_seconds": 120.25,
        "provider_request_count": 2,
        "configured_timeout_seconds": 120.0,
        "configured_max_attempts": 2,
    }
    assert not (market_dir / "classification.duckdb").exists()
    assert not market_dir.exists()
    assert not temp_dir.exists()
    assert list(tmp_path.iterdir()) == []


def test_classification_cli_dry_run_output_remains_compatible_and_write_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        local_market_dataset_root=tmp_path / "dataset",
    )

    class RejectProviderConstruction:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("dry-run constructed provider")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        cli,
        "BaoStockClassificationProvider",
        RejectProviderConstruction,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
        ],
    )

    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "dry-run",
        "as_of": SNAPSHOT_DATE.isoformat(),
        "network_requests": 0,
        "writes_classification_data": False,
        "new_generation": False,
        "execute_requires": "--execute",
        "generation": None,
        "quality_issues": [],
    }
    assert list(tmp_path.iterdir()) == []


def test_classification_cli_initial_login_timeout_is_controlled_and_write_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    market_dir = tmp_path / "market"
    temp_dir = tmp_path / "tmp"
    settings = Settings(
        _env_file=None,
        market_data_dir=market_dir,
        local_temp_dir=temp_dir,
    )

    class NoOpCloseSocket:
        def settimeout(self, _seconds: float) -> None:
            pass

        def shutdown(self, _how: int) -> None:
            pass

        def close(self) -> None:
            pass

    class BlockingLoginClient:
        def __init__(self) -> None:
            self.context = type("Context", (), {"default_socket": None})()
            self.release = threading.Event()
            self.continued = False
            self.login_calls = 0

        def login(self):
            self.login_calls += 1
            self.context.default_socket = NoOpCloseSocket()
            self.release.wait()
            self.continued = True
            raise OSError("synthetic late login")

    client = BlockingLoginClient()

    def actual_provider(**_kwargs) -> BaoStockClassificationProvider:
        return BaoStockClassificationProvider(
            client=client,
            min_request_interval_seconds=0,
            socket_timeout_seconds=0.02,
        )

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockClassificationProvider", actual_provider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
            "--socket-timeout-seconds",
            "1",
        ],
    )

    try:
        started = perf_counter()
        assert cli.main() == 1
        assert perf_counter() - started < 0.5
        payload = json.loads(capsys.readouterr().out)
        assert set(payload) == CLASSIFICATION_FAILURE_KEYS
        assert payload["status"] == "error"
        assert payload["as_of"] == SNAPSHOT_DATE.isoformat()
        assert payload["quality_issues"] == ["classification_sync_failed"]
        assert payload["writes_classification_data"] is False
        assert payload["failure_stage"] == "login"
        assert payload["failure_class"] == "deadline"
        assert payload["elapsed_seconds"] >= 0.04
        assert payload["provider_request_count"] == 0
        assert payload["configured_timeout_seconds"] == 0.02
        assert payload["configured_max_attempts"] == 2
        assert client.continued is False
        assert client.login_calls == 2
        assert client.context.default_socket is None
        assert not [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("stock-eva-baostock-operation-login") and thread.is_alive()
        ]
        assert not (market_dir / "classification.duckdb").exists()
        assert not market_dir.exists()
        assert not temp_dir.exists()
    finally:
        client.release.set()
        for thread in threading.enumerate():
            if thread.name.startswith("stock-eva-baostock-operation-login"):
                thread.join(0.2)


def test_classification_cli_unexpected_constructor_failure_is_sanitized_and_write_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        local_market_dataset_root=tmp_path / "dataset",
    )
    hostile = "token=do-not-leak /Users/private/secret.db"

    class HostileProvider:
        def __init__(self, **_kwargs) -> None:
            raise RuntimeError(hostile)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockClassificationProvider", HostileProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
            "--socket-timeout-seconds",
            "17",
        ],
    )

    assert cli.main() == 1
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert set(payload) == CLASSIFICATION_FAILURE_KEYS
    assert payload["quality_issues"] == ["classification_sync_failed"]
    assert payload["writes_classification_data"] is False
    assert payload["failure_stage"] == "validation"
    assert payload["failure_class"] == "internal"
    assert payload["elapsed_seconds"] >= 0
    assert payload["provider_request_count"] == 0
    assert payload["configured_timeout_seconds"] == 17
    assert payload["configured_max_attempts"] is None
    assert hostile not in output
    assert list(tmp_path.iterdir()) == []


def test_classification_cli_settings_failure_is_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hostile = "token=do-not-leak /Users/private/settings.env"

    def hostile_settings():
        raise RuntimeError(hostile)

    monkeypatch.setattr(cli, "get_settings", hostile_settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
            "--socket-timeout-seconds",
            "19",
        ],
    )

    assert cli.main() == 1
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert set(payload) == CLASSIFICATION_FAILURE_KEYS
    assert payload["failure_stage"] == "validation"
    assert payload["failure_class"] == "internal"
    assert payload["provider_request_count"] == 0
    assert payload["configured_timeout_seconds"] == 19
    assert payload["configured_max_attempts"] is None
    assert hostile not in output
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("publication_error", "expected_class"),
    [
        (ClassificationConflictError("hostile conflict /Users/private"), "conflict"),
        (OSError("hostile storage /Users/private"), "storage"),
    ],
)
def test_classification_cli_publication_failure_is_structured_and_write_free(
    publication_error: Exception,
    expected_class: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        local_market_dataset_root=tmp_path / "dataset",
    )

    class SuccessfulProvider:
        def __init__(self, **kwargs) -> None:
            self.last_request_count = 6
            self.session = type(
                "SessionDiagnostics",
                (),
                {
                    "socket_timeout_seconds": kwargs["socket_timeout_seconds"],
                    "max_attempts": 2,
                },
            )()

        def fetch(self, _as_of: date) -> ClassificationSnapshot:
            return snapshot(securities=[security("sh.600000")])

    class FailingStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def publish(self, _snapshot: ClassificationSnapshot):
            raise publication_error

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockClassificationProvider", SuccessfulProvider)
    monkeypatch.setattr(cli, "ClassificationStore", FailingStore)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "classification-sync",
            "--as-of",
            SNAPSHOT_DATE.isoformat(),
            "--execute",
            "--socket-timeout-seconds",
            "11",
        ],
    )

    assert cli.main() == 1
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert set(payload) == CLASSIFICATION_FAILURE_KEYS
    assert payload["failure_stage"] == "publication"
    assert payload["failure_class"] == expected_class
    assert payload["provider_request_count"] == 6
    assert payload["configured_timeout_seconds"] == 11
    assert payload["configured_max_attempts"] == 2
    assert "/Users/private" not in output
    assert list(tmp_path.iterdir()) == []


def test_classification_fetch_does_not_logout_session_it_did_not_acquire() -> None:
    class CountingClient:
        def __init__(self) -> None:
            self.context = type("Context", (), {"default_socket": object()})()
            self.login_calls = 0
            self.query_calls = 0
            self.logout_calls = 0

        def login(self):
            self.login_calls += 1
            raise AssertionError("worker fetch must fail before login")

        def query_all_stock(self, **_kwargs):
            self.query_calls += 1
            raise AssertionError("worker fetch must fail before query")

        def logout(self):
            self.logout_calls += 1

    client = CountingClient()
    provider = BaoStockClassificationProvider(client=client)
    original_socket = client.context.default_socket
    provider.session._session_usable = True
    errors: list[BaseException] = []

    def fetch_from_worker() -> None:
        try:
            provider.fetch(SNAPSHOT_DATE)
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=fetch_from_worker)
    worker.start()
    worker.join(0.2)

    assert worker.is_alive() is False
    assert len(errors) == 1
    assert isinstance(errors[0], ClassificationProviderError)
    assert client.login_calls == 0
    assert client.query_calls == 0
    assert client.logout_calls == 0
    assert provider.session._session_usable is True
    assert client.context.default_socket is original_socket
    assert provider.session.client is client
