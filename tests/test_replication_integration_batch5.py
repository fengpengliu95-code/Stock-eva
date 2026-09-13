"""Final R2-F4.3 Batch5 integration anchors and adversarial inputs."""

from datetime import date

import pytest

from backend.app.market.backfill import BackfillService
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.replication import LineageResolver
from tests.test_market_backfill import RangeProvider, trading_days
from tests.test_nas_dataset import _bars, _ready_result


def _dataset(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}', encoding="utf-8"
    )
    (root / "manifest.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2,'
        '"generation":"generation-empty","files":[]}',
        encoding="utf-8",
    )
    return root


def test_disabled_replication_still_seals_local_publication_binding(tmp_path) -> None:
    root = _dataset(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
        replication_enabled=False,
    )

    store.save_refresh(_bars(), _ready_result(), publish=True)

    bindings = tuple((root / "_replication" / "source-commits").glob("*.json"))
    assert len(bindings) == 1
    assert not (tmp_path / "control" / "replication").exists()


def test_pointer_identity_distinguishes_valid_absent_from_invalid_partial(tmp_path) -> None:
    root = _dataset(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    connection = control._connect()
    connection.close()
    store = NasMarketStore(control, root, tmp_path / "staging")

    absent = store._pointer_identity()
    assert absent.kind == "ABSENT"
    assert absent.device is not None and absent.inode is not None
    assert absent.schema_digest is not None

    control.path.write_bytes(b"not-a-duckdb")
    invalid = store._pointer_identity()
    assert invalid.kind == "INVALID"
    assert invalid.reason_code == "CONTROL_STATE_UNAVAILABLE"


def test_modern_lineage_requires_retained_success_evidence_reader() -> None:
    modern = {
        "mode": "modern",
        "exact": {
            "provider_id": "baostock",
            "universe_id": "u",
            "evidence_id": "e",
            "evidence_sha256": "a" * 64,
            "candidate_id": "c",
            "candidate_manifest_sha256": "b" * 64,
            "gate_report_sha256": "c" * 64,
            "adapter_version": "v",
            "source_schema_version": "s",
        },
    }
    result = LineageResolver().resolve(modern, "2026-07-23")
    assert result.kind == "UNAVAILABLE"
    assert result.reason_code == "SOURCE_UNAVAILABLE"


def test_dataset_plan_rejects_duplicate_and_out_of_order_dates(tmp_path) -> None:
    root = _dataset(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    service = BackfillService(store, RangeProvider(trading_days(date(2026, 7, 20), 3)))
    lineage = [(item, {"mode": "legacy"}) for item in trading_days(date(2026, 7, 20), 2)]

    with pytest.raises(ValueError):
        service.plan_dataset(
            start_date=lineage[0][0],
            end_date=lineage[-1][0],
            symbols=["sh.600000"],
            symbol_batch_size=1,
            date_batch_size=2,
            max_batches=4,
            trading_dates=[lineage[0][0], lineage[0][0]],
            lineage_pairs=lineage,
        )
    with pytest.raises(ValueError):
        service.plan_dataset(
            start_date=lineage[0][0],
            end_date=lineage[-1][0],
            symbols=["sh.600000"],
            symbol_batch_size=1,
            date_batch_size=2,
            max_batches=4,
            trading_dates=[lineage[1][0], lineage[0][0]],
            lineage_pairs=lineage,
        )


def test_manifest_corruption_is_not_treated_as_empty(tmp_path) -> None:
    root = _dataset(tmp_path)
    (root / "manifest.json").write_text("{", encoding="utf-8")
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    with pytest.raises(DatasetError, match="metadata is unavailable"):
        store._manifest()


def test_manifest_only_does_not_create_binding_or_pointer_sidecar(tmp_path) -> None:
    root = _dataset(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
        replication_enabled=True,
    )

    store.upsert_bars(_bars())

    assert not (root / "_replication" / "source-commits").exists()
    assert not (tmp_path / "control" / "replication-sidecar").exists()
    assert store._pointer_identity().kind == "ABSENT"


def test_pointer_identity_rejects_extra_table_and_column(tmp_path) -> None:
    root = _dataset(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    connection = control._connect()
    connection.execute("ALTER TABLE daily_bars ADD COLUMN injected VARCHAR")
    connection.close()
    store = NasMarketStore(control, root, tmp_path / "staging")

    assert store._pointer_identity().as_dict() == {
        "kind": "INVALID",
        "reason_code": "CONTROL_STATE_UNAVAILABLE",
    }

    connection = control._connect()
    connection.execute("DROP TABLE daily_bars")
    connection.execute("CREATE TABLE unexpected (value VARCHAR)")
    connection.close()
    assert store._pointer_identity().kind == "INVALID"


def test_binding_reuse_requires_exact_immutable_fields(tmp_path) -> None:
    root = _dataset(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    result = _ready_result()
    store.save_refresh(_bars(), result, publish=True)
    binding_path = next((root / "_replication" / "source-commits").glob("*.json"))
    original = binding_path.read_text(encoding="utf-8")

    # A second publication of the same immutable generation may reuse the
    # exact record, but a changed run/digest must fail closed.
    store.coordinator.publish_dataset_and_pointer(
        {
            "bars": _bars(),
            "source": result.source,
            "result": result,
            "selection": None,
            "lineage_input": {"mode": "legacy"},
            "dataset_already_ready": True,
        }
    )
    assert binding_path.read_text(encoding="utf-8") == original
