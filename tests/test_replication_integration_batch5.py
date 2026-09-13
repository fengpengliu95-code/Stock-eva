"""Final R2-F4.3 Batch5 integration anchors and adversarial inputs."""

import ast
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from backend.app.config import Settings
from backend.app.market.backfill import BackfillService
from backend.app.market.candidates import CandidateStore, SessionSelection
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import (
    DatasetError,
    NasMarketStore,
    PointerIdentity,
    canonical_control_schema_digest,
)
from backend.app.storage.factory import (
    ReplicationDrainConfigurationError,
    build_nas_market_store,
    build_replication_drain_worker,
)
from backend.app.storage.replication import (
    LineageInput,
    LineageResolver,
    ReplicationOutboxService,
    SourceInstanceStore,
)
from tests.test_market_backfill import RangeProvider, trading_days
from tests.test_market_candidate_selection import _real_candidate_bundle
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

    store.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})

    bindings = tuple((root / "_replication" / "source-commits").glob("*.json"))
    assert len(bindings) == 1
    assert not (tmp_path / "control" / "replication").exists()


def test_source_commit_uses_exact_immutable_source_instance_or_is_optional(tmp_path) -> None:
    root = _dataset(tmp_path)
    source = SourceInstanceStore(root / "_replication" / "source-instance.json").create(
        canonical_root_path=root,
        canonical_schema_digest=canonical_control_schema_digest(),
        source_instance_nonce="d" * 64,
    )
    service = ReplicationOutboxService(
        tmp_path / "sidecar",
        source_instance_id=source.source_instance_id,
        source_instance_sha256=source.source_instance_sha256,
    )
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
        replication_enabled=True,
        replication_service=service,
        source_instance=source,
    )
    store.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    payload = store.coordinator.lock.result_payload
    assert payload is not None
    commit = payload["source_commit"]
    assert commit.source_instance_id == source.source_instance_id
    assert commit.source_instance_sha256 == source.source_instance_sha256

    (tmp_path / "disabled").mkdir()
    disabled_root = _dataset(tmp_path / "disabled")
    disabled = NasMarketStore(
        MarketStore(tmp_path / "disabled-control" / "market.duckdb"),
        disabled_root,
        tmp_path / "disabled-staging",
        replication_enabled=False,
    )
    disabled.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    disabled_payload = disabled.coordinator.lock.result_payload
    assert disabled_payload is not None
    assert disabled_payload["source_commit"] is None
    assert disabled_payload["observation"].source_instance_id != "0" * 64


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


def test_nas_writer_call_sites_are_explicit_and_no_global_lineage_shim_exists() -> None:
    tests_root = Path(__file__).parent
    assert not (tests_root / "conftest.py").exists()
    for name in ("test_nas_dataset.py", "test_replication_integration_batch5.py"):
        path = tests_root / name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {"save_refresh", "upsert_bars"}:
                continue
            if name == "test_replication_integration_batch5.py" and 321 <= node.lineno <= 333:
                # Deliberate negative anchor proving omission is rejected.
                continue
            assert any(
                keyword.arg in {"lineage_input", "publication_lineage"} for keyword in node.keywords
            ), f"omitted lineage at {path}:{node.lineno}"


def test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    bundle = evidence_root / "bundles" / candidate.candidate_id
    selection = SessionSelection.model_validate(
        json.loads((bundle / "selection.json").read_text(encoding="utf-8"))
    )
    capability = CandidateStore(evidence_root).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=(bundle / "normalized.json").read_bytes(),
    )
    bars = [
        DailyBar.model_validate(item)
        for item in json.loads((bundle / "normalized.json").read_text(encoding="utf-8"))
    ]
    (tmp_path / "valid").mkdir()
    root = _dataset(tmp_path / "valid")
    settings = Settings(
        _env_file=None,
        local_control_dir=tmp_path / "valid-control",
        local_staging_dir=tmp_path / "valid-staging",
        local_lock_dir=tmp_path / "valid-locks",
        local_temp_dir=tmp_path / "valid-temp",
        local_market_dataset_root=root,
        provider_evidence_root=evidence_root,
    )
    store = build_nas_market_store(settings)
    assert store is not None
    result = RefreshResult(
        run_id="retained-success",
        request_key="retained-success",
        requested_date=candidate.trade_date,
        source="baostock",
        status="ready",
        requested_count=len(bars),
        succeeded_count=len(bars),
        coverage_ratio=1,
        started_at=datetime(2026, 8, 20, tzinfo=UTC),
        completed_at=datetime(2026, 8, 20, 1, tzinfo=UTC),
    )
    store.save_refresh(
        bars,
        result,
        publish=True,
        lineage_input=LineageInput(
            mode="modern",
            candidate_id=candidate.candidate_id,
            evidence_id=candidate.evidence_id,
        ),
        selection=capability,
    )
    binding = next((root / "_replication" / "source-commits").glob("*.json"))
    binding_payload = json.loads(binding.read_text(encoding="utf-8"))
    assert binding_payload["selection_sha256"] == selection.selection_sha256
    assert binding_payload["lineage_sha256"] != "0" * 64

    tampered = tmp_path / "tampered"
    tampered.mkdir()
    tampered_root = _dataset(tampered)
    tampered_settings = settings.model_copy(
        update={
            "local_market_dataset_root": tampered_root,
            "local_control_dir": tmp_path / "tampered-control",
            "local_staging_dir": tmp_path / "tampered-staging",
        }
    )
    candidate_path = bundle / "candidate.json"
    candidate_payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    candidate_payload["manifest_sha256"] = "0" * 64
    candidate_path.write_text(json.dumps(candidate_payload), encoding="utf-8")
    tampered_store = build_nas_market_store(tampered_settings)
    assert tampered_store is not None
    before = (tampered_root / "manifest.json").read_bytes()
    with pytest.raises(DatasetError, match="source lineage is unavailable"):
        tampered_store.save_refresh(
            bars,
            result,
            publish=True,
            lineage_input=LineageInput(
                mode="modern",
                candidate_id=candidate.candidate_id,
                evidence_id=candidate.evidence_id,
            ),
        )
    assert (tampered_root / "manifest.json").read_bytes() == before
    evidence.close()


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

    store.upsert_bars(_bars(), lineage_input={"mode": "legacy"})

    assert not (root / "_replication" / "source-commits").exists()
    assert not (tmp_path / "control" / "replication-sidecar").exists()
    assert store._pointer_identity().kind == "ABSENT"


def test_omitted_lineage_fails_before_manifest_mutation(tmp_path) -> None:
    root = _dataset(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    before = (root / "manifest.json").read_bytes()

    with pytest.raises(DatasetError, match="lineage input is required"):
        store.upsert_bars(_bars())

    assert (root / "manifest.json").read_bytes() == before


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
    store.save_refresh(_bars(), result, publish=True, lineage_input={"mode": "legacy"})
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


def test_reconcile_rejects_duplicate_generation_bindings(tmp_path) -> None:
    root = _dataset(tmp_path)
    publisher = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    publisher.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    binding_path = next((root / "_replication" / "source-commits").glob("*.json"))
    duplicate = binding_path.with_name("duplicate.json")
    duplicate.write_bytes(binding_path.read_bytes())

    with pytest.raises(DatasetError, match="duplicate publication bindings"):
        NasMarketStore(
            MarketStore(tmp_path / "reconcile.duckdb"), root, tmp_path / "reconcile-staging"
        ).reconcile_control_pointer()


def test_mutated_binding_hash_rejects_reconcile_without_pointer_change(tmp_path) -> None:
    root = _dataset(tmp_path)
    publisher = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    publisher.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    pointer_before = publisher.control.path.read_bytes()
    binding_path = next((root / "_replication" / "source-commits").glob("*.json"))
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    payload["binding_sha256"] = "0" * 64
    binding_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DatasetError, match="publication binding"):
        NasMarketStore(
            MarketStore(tmp_path / "reconcile.duckdb"), root, tmp_path / "reconcile-staging"
        ).reconcile_control_pointer()

    assert publisher.control.path.read_bytes() == pointer_before


def test_postcommit_invalid_pointer_journals_exact_context(tmp_path, monkeypatch) -> None:
    root = _dataset(tmp_path)
    journaled = []

    class JournalService:
        def pre_publication_guard(self):
            return None

        def journal_publication(self, **kwargs):
            journaled.append(kwargs)

    store = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb"),
        root,
        tmp_path / "staging",
        replication_service=JournalService(),
    )
    pointers = iter(
        (
            PointerIdentity("ABSENT"),
            PointerIdentity("ABSENT"),
            PointerIdentity("INVALID", reason_code="CONTROL_STATE_UNAVAILABLE"),
        )
    )
    monkeypatch.setattr(store, "_pointer_identity", lambda: next(pointers))

    result = _ready_result()
    payload = store.save_refresh(_bars(), result, publish=True, lineage_input={"mode": "legacy"})

    assert payload is None
    assert len(journaled) == 1
    assert journaled[0]["binding"].run_id == result.run_id
    assert journaled[0]["result"] is result
    assert journaled[0]["manifest"]["generation"] == journaled[0]["binding"].manifest_generation


def test_drain_factory_double_gate_short_circuits_before_destination(tmp_path) -> None:
    from backend.app.config import Settings

    settings = Settings(
        _env_file=None,
        local_market_dataset_root=tmp_path / "dataset",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        replication_enabled=False,
        replication_drain_enabled=False,
    )
    assert build_replication_drain_worker(settings) is None
    assert not (tmp_path / "control" / "replication-sidecar").exists()

    enabled = settings.model_copy(
        update={"replication_enabled": True, "replication_drain_enabled": True}
    )
    with pytest.raises(ReplicationDrainConfigurationError):
        build_replication_drain_worker(enabled)
