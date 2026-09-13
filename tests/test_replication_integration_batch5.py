"""Final R2-F4.3 Batch5 integration anchors and adversarial inputs."""

import ast
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter

import pytest
from fastapi.testclient import TestClient

import backend.app.storage.replication as replication
from backend.app.config import Settings, get_settings
from backend.app.main import app
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
    build_journal_record,
    build_source_checkpoint,
    domain_sha256,
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
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            if function.name in {
                "test_plain_market_store_artifact_is_byte_stable_when_dataset_adapter_is_constructed",
                "test_omitted_lineage_fails_before_manifest_mutation",
            }:
                continue
            for node in ast.walk(function):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr not in {"save_refresh", "upsert_bars"}:
                    continue
                if name == "test_replication_integration_batch5.py" and 321 <= node.lineno <= 333:
                    # Deliberate negative anchor proving omission is rejected.
                    continue
                assert any(
                    keyword.arg in {"lineage_input", "publication_lineage"}
                    for keyword in node.keywords
                ), f"omitted lineage at {path}:{node.lineno}"


def test_production_dataset_writer_inventory_has_no_pointer_bypass() -> None:
    """FR-3b: inspect production seams, not only test call sites."""
    root = Path(__file__).parents[1]
    dataset = (root / "backend/app/storage/dataset.py").read_text(encoding="utf-8")
    backfill = (root / "backend/app/market/backfill.py").read_text(encoding="utf-8")
    dataset_tree = ast.parse(dataset)
    writer_methods = {
        node.name: node
        for node in ast.walk(dataset_tree)
        if isinstance(node, ast.FunctionDef) and node.name in {"save_refresh", "upsert_bars"}
    }
    assert {"save_refresh", "upsert_bars"} <= set(writer_methods)
    for node in writer_methods.values():
        calls = {
            call.func.attr
            for call in ast.walk(node)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
        }
        assert "publish_dataset_and_pointer" in calls or "publish_manifest_only" in calls
        assert "write_pointer" not in calls
        assert "save_external_publication" not in calls
    assert "publish_dataset_and_pointer" in dataset
    assert "publish_manifest_only" in dataset
    assert "NasMarketStore" in backfill
    assert "lineage_input" in backfill


def test_launchagent_refresh_uses_tcc_safe_path_and_disabled_replication_gates() -> None:
    """FR-22/AC-19: verify the real install/plist assets and safe defaults."""
    root = Path(__file__).parents[1]
    installer = (root / "scripts/stock_eva_launchagents_install.sh").read_text(encoding="utf-8")
    refresh = (root / "launchd/com.finlay.stock-eva.refresh.plist.in").read_text(encoding="utf-8")
    env = (root / ".env.example").read_text(encoding="utf-8")
    assert 'APP_SUPPORT_ROOT="$HOME/Library/Application Support/Stock EVA"' in installer
    assert "__CONFIG_ROOT__" in refresh
    assert "auto-refresh-once" in refresh
    assert "STOCK_EVA_REPLICATION_ENABLED=false" in env
    assert "STOCK_EVA_MARKET_REPLICATION_DRAIN_ENABLED=false" in env
    assert "mount_smbfs" not in installer


def test_replication_config_defaults_are_portable_and_operable(tmp_path) -> None:
    """NFR-13/NFR-15: exact disabled defaults and local portable layout."""
    settings = Settings(
        _env_file=None,
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        local_market_dataset_root=tmp_path / "dataset",
        replication_enabled=False,
        replication_drain_enabled=False,
        replication_destination_root=None,
    )
    assert settings.replication_enabled is False
    assert settings.replication_drain_enabled is False
    assert settings.replication_destination_root is None
    assert settings.replication_direction == "local_to_nas"
    assert settings.local_market_dataset_root == tmp_path / "dataset"
    assert not (tmp_path / "dataset").exists()


def test_plain_market_store_artifact_is_byte_stable_when_dataset_adapter_is_constructed(
    tmp_path,
) -> None:
    """NFR-14: dataset replication setup cannot alter plain MarketStore bytes."""
    control_path = tmp_path / "plain.duckdb"
    plain = MarketStore(control_path)
    plain.save_refresh(_bars(), _ready_result(), publish=True)
    before = control_path.read_bytes()
    root = _dataset(tmp_path)
    NasMarketStore(plain, root, tmp_path / "staging", replication_enabled=True)
    assert control_path.read_bytes() == before
    assert plain.published_refresh() == plain.published_refresh()


def test_canonical_ready_precedes_enqueue_failure_and_next_guard_reconciles(tmp_path) -> None:
    """NFR-10/FR-3: injected enqueue failure cannot roll back canonical ready."""
    root = _dataset(tmp_path)
    source = SourceInstanceStore(root / "_replication" / "source-instance.json").create(
        canonical_root_path=root,
        canonical_schema_digest=canonical_control_schema_digest(),
        source_instance_nonce="d" * 64,
        created_at="2026-09-09T00:00:00Z",
    )

    class FailingEnqueue:
        source_instance_id = source.source_instance_id
        source_instance_sha256 = source.source_instance_sha256

        def pre_publication_guard(self):
            return None

        def on_canonical_committed(self, _source_commit):
            raise RuntimeError("injected enqueue failure")

        def journal_publication(self, **_kwargs):
            return None

    store = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb"),
        root,
        tmp_path / "staging",
        replication_service=FailingEnqueue(),
        source_instance=source,
    )
    store.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    assert store.control.published_refresh() is not None
    assert (
        store.coordinator.lock.result_payload["observation"].reason_code
        == "CONTROL_STATE_UNAVAILABLE"
    )
    reconciled = store.reconcile_control_pointer()
    assert reconciled is not None
    assert reconciled.status == "ready"


def test_ec16_postcommit_enqueue_and_journal_failures_reconcile_without_duplicate(tmp_path) -> None:
    """EC-16: ready wins over both post-commit durability failures."""
    root = _dataset(tmp_path)
    source = SourceInstanceStore(root / "_replication" / "source-instance.json").create(
        canonical_root_path=root,
        canonical_schema_digest=canonical_control_schema_digest(),
        source_instance_nonce="e" * 64,
    )
    calls = {"enqueue": 0, "journal": 0}

    class BrokenPostCommit:
        source_instance_id = source.source_instance_id
        source_instance_sha256 = source.source_instance_sha256

        def pre_publication_guard(self):
            return None

        def on_canonical_committed(self, _source_commit):
            calls["enqueue"] += 1
            raise RuntimeError("outbox unavailable")

        def journal_publication(self, **_kwargs):
            calls["journal"] += 1
            raise OSError("journal unavailable")

    store = NasMarketStore(
        MarketStore(tmp_path / "control.duckdb"),
        root,
        tmp_path / "staging",
        replication_service=BrokenPostCommit(),
        source_instance=source,
    )
    store.save_refresh(_bars(), _ready_result(), publish=True, lineage_input={"mode": "legacy"})
    assert store.control.published_refresh() is not None
    observation = store.coordinator.lock.result_payload["observation"]
    assert observation.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert observation.effects.canonical_writes is True
    assert observation.effects.outbox_writes is False
    assert calls == {"enqueue": 1, "journal": 1}
    assert store.reconcile_control_pointer() is not None
    assert len(tuple((root / "_replication" / "source-commits").glob("*.json"))) == 1
    assert calls == {"enqueue": 1, "journal": 1}


@pytest.mark.parametrize(
    "fault_point",
    ("manifest_mutation", "binding_install", "pre_pointer", "pointer_transaction", "postcommit"),
)
def test_ec25_publication_fault_points_never_report_partial_success(
    tmp_path, monkeypatch, fault_point
) -> None:
    """EC-25: faults preserve the prior proof, or expose one new ready proof."""
    root = _dataset(tmp_path)
    store = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    first = _ready_result()
    store.save_refresh(_bars(), first, publish=True, lineage_input={"mode": "legacy"})
    old_pointer = store.control.published_refresh()
    assert old_pointer is not None
    control_path = store.control.path
    old_control = (control_path.stat().st_ino, control_path.read_bytes())
    binding_root = root / "_replication" / "source-commits"
    old_bindings = {
        path.name: (path.stat().st_ino, path.read_bytes()) for path in binding_root.glob("*.json")
    }
    incoming = _bars()
    incoming[0] = incoming[0].model_copy(update={"close": incoming[0].close + 0.01})
    second = first.model_copy(update={"run_id": "nas-ready-2", "request_key": "nas-ready-2"})
    original_hashes = store.coordinator._manifest_hashes
    original_pointer = store._pointer_identity
    hash_calls = 0
    pointer_calls = 0

    if fault_point == "manifest_mutation":
        monkeypatch.setattr(
            store,
            "_publish_locked",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                DatasetError("manifest mutation crash")
            ),
        )
    elif fault_point == "binding_install":
        monkeypatch.setattr(
            store,
            "_write_binding",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(DatasetError("binding install crash")),
        )
    elif fault_point == "pre_pointer":

        def fail_final_manifest(*args, **kwargs):
            nonlocal hash_calls
            hash_calls += 1
            if hash_calls == 2:
                raise DatasetError("pre-pointer CAS crash")
            return original_hashes(*args, **kwargs)

        monkeypatch.setattr(store.coordinator, "_manifest_hashes", fail_final_manifest)
    elif fault_point == "pointer_transaction":
        monkeypatch.setattr(
            store.control,
            "save_external_publication",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                DatasetError("pointer transaction crash")
            ),
        )
    else:

        def fail_postcommit(*args, **kwargs):
            nonlocal pointer_calls
            pointer_calls += 1
            if pointer_calls >= 4:
                return PointerIdentity("INVALID", reason_code="CONTROL_STATE_UNAVAILABLE")
            return original_pointer(*args, **kwargs)

        monkeypatch.setattr(store, "_pointer_identity", fail_postcommit)

    if fault_point in {"pointer_transaction", "postcommit"}:
        store.save_refresh(incoming, second, publish=True, lineage_input={"mode": "legacy"})
    else:
        with pytest.raises(DatasetError):
            store.save_refresh(incoming, second, publish=True, lineage_input={"mode": "legacy"})
    pointer = store.control.published_refresh()
    assert pointer is not None
    assert pointer.status == "ready"
    if fault_point == "postcommit":
        assert pointer.run_id == second.run_id
        assert (control_path.stat().st_ino, control_path.read_bytes()) != old_control
        assert store.reconcile_control_pointer().run_id == second.run_id
    else:
        assert pointer.model_dump() == old_pointer.model_dump()
        assert (control_path.stat().st_ino, control_path.read_bytes()) == old_control
        assert store.reconcile_control_pointer().model_dump() == old_pointer.model_dump()
    assert {
        path.name: (path.stat().st_ino, path.read_bytes()) for path in binding_root.glob("*.json")
    }.items() >= old_bindings.items()
    assert store.control.published_refresh().status == "ready"
    if fault_point == "postcommit":
        assert (
            store.coordinator.lock.result_payload["observation"].enqueue_state == "not_configured"
        )


def test_status_matrix_covers_all_public_states_without_writes(tmp_path, monkeypatch) -> None:
    """AC-15/EC-14/EC-37: every public status state is zero-provider/zero-write."""
    from backend.app.storage.layout import StorageLayout
    from backend.app.storage.replication import (
        ReplicationSidecarStore,
        ReplicationStatusService,
        SourceInstanceStore,
    )

    expected = {
        "missing": ("unavailable", "REPLICATION_STATE_UNAVAILABLE", 0, 0, 0),
        "corrupt": ("unavailable", "REPLICATION_STATE_UNAVAILABLE", 0, 0, 0),
        "locked": ("unavailable", "REPLICATION_STATE_UNAVAILABLE", 0, 0, 0),
        "empty": ("ready", "NONE", 0, 0, 0),
        "healthy": ("ready", "NONE", 0, 0, 0),
        "retry_wait": ("degraded", "NONE", 0, 1, 0),
        "dead_lettered": ("degraded", "NONE", 0, 0, 1),
        "replicated": ("degraded", "NONE", 0, 0, 0),
    }

    def fingerprint(path: Path):
        if not path.exists():
            return None
        return tuple(
            (
                str(item.relative_to(path)),
                item.is_dir(),
                item.stat().st_ino,
                None if item.is_dir() else item.read_bytes(),
            )
            for item in sorted(path.rglob("*"))
        )

    def http_status(settings):
        app.dependency_overrides[get_settings] = lambda settings=settings: settings
        try:
            return TestClient(app).get("/api/v1/storage/replication")
        finally:
            app.dependency_overrides.clear()

    for state in expected:
        settings = Settings(
            _env_file=None,
            local_control_dir=tmp_path / state / "control",
            local_staging_dir=tmp_path / state / "staging",
            local_lock_dir=tmp_path / state / "locks",
            local_temp_dir=tmp_path / state / "temp",
            local_market_dataset_root=tmp_path / state / "dataset",
            replication_enabled=True,
        )
        layout = StorageLayout(settings)
        if state == "corrupt":
            layout.replication_sidecar_root.mkdir(parents=True)
            (layout.replication_sidecar_root / "garbage").write_text("corrupt", encoding="utf-8")
        elif state == "locked":
            layout.replication_sidecar_root.mkdir(parents=True)
            (layout.replication_sidecar_root / "replication.lock").write_text(
                "held", encoding="utf-8"
            )
            with monkeypatch.context() as locked_patch:
                from backend.app.storage.replication import ReplicationStateUnavailable

                locked_patch.setattr(
                    "backend.app.storage.replication._open_generation_readonly",
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        ReplicationStateUnavailable("locked")
                    ),
                )
                result = ReplicationStatusService(settings).read()
                response = http_status(settings)
            assert response.status_code == 200
            assert str(tmp_path) not in response.text
            assert response.json()["provider_requests"] == 0
            assert (
                result.status,
                result.reason_code,
                result.pending_count,
                result.retry_wait_count,
                result.dead_letter_count,
            ) == expected[state]
            assert result.provider_requests == 0
            assert result.effects.writes is False
            continue
        elif state in {"empty", "healthy", "retry_wait", "dead_lettered", "replicated"}:
            source_path = layout.replication_source_instance
            assert source_path is not None
            source = SourceInstanceStore(source_path).create(
                canonical_root_path=settings.local_market_dataset_root,
                canonical_schema_digest="c" * 64,
                source_instance_nonce="d" * 64,
                created_at="2026-09-09T00:00:00Z",
            )
            ReplicationSidecarStore(layout.replication_sidecar_root).initialize(
                source_instance_id=source.source_instance_id,
                source_instance_sha256=source.source_instance_sha256,
            )
            sidecar = ReplicationSidecarStore(layout.replication_sidecar_root)
            if state in {"retry_wait", "dead_lettered", "replicated"}:
                checkpoint = build_source_checkpoint(
                    source_instance_id=source.source_instance_id,
                    source_instance_sha256=source.source_instance_sha256,
                    publication_binding_sha256="1" * 64,
                    pointer_row_sha256="2" * 64,
                    pointer_generation="generation-status",
                    source_run_id="status-run",
                    source_trade_date="2026-09-09",
                    source_published_at="2026-09-09T00:00:00Z",
                    pointer_db_device=1,
                    pointer_db_inode=2,
                    pointer_db_schema_digest="3" * 64,
                    manifest_canonical_sha256="4" * 64,
                    source_manifest_bytes_sha256="5" * 64,
                    source_object_set_sha256=domain_sha256("stock-eva/r2f4.3/object-set/v1", []),
                    object_inventory=(),
                )
                sidecar.enqueue(
                    checkpoint,
                    operation_day="2026-09-09",
                    destination_id="6" * 32,
                    created_at="2026-09-09T00:00:00Z",
                )

                if state == "replicated":
                    claim = sidecar.claim_due(worker_id="status-worker", now="2026-09-09T00:00:01Z")
                    assert claim is not None
                    sidecar.transition(
                        intent_id=claim.intent_id,
                        worker_id="status-worker",
                        expected_state_version=claim.state_version,
                        to_state="verifying",
                        now="2026-09-09T00:00:02Z",
                    )

                def terminalize(connection, state=state):
                    intent_id = str(
                        connection.execute("SELECT intent_id FROM replication_heads").fetchone()[0]
                    )
                    to_state = (
                        "retry_wait"
                        if state == "retry_wait"
                        else "dead_letter"
                        if state == "dead_lettered"
                        else "replicated"
                    )
                    replication.ImmutableReplicationSidecarStore._append_event_and_update_head(
                        connection,
                        intent_id=intent_id,
                        from_state="verifying" if state == "replicated" else "pending",
                        to_state=to_state,
                        attempt=1,
                        reason_code=(
                            "NONE"
                            if to_state == "replicated"
                            else "RETRY_WAIT"
                            if to_state == "retry_wait"
                            else "DEAD_LETTER"
                        ),
                        occurred_at="2026-09-09T00:00:03Z",
                        lease_owner=None,
                        lease_until=None,
                        next_attempt_at="2026-09-09T00:00:02Z",
                        event_type="transition" if to_state != "dead_letter" else "terminal",
                        destination_replication_generation=(
                            "7" * 64 if state == "replicated" else None
                        ),
                        destination_record_sha256=("8" * 64 if state == "replicated" else None),
                        destination_head_sha256=("9" * 64 if state == "replicated" else None),
                    )
                    return replication._MutationOutcome(None, True)

                sidecar._mutate_generation(terminalize)
        before = fingerprint(layout.replication_sidecar_root)
        result = ReplicationStatusService(settings).read()
        assert fingerprint(layout.replication_sidecar_root) == before
        assert (
            result.status,
            result.reason_code,
            result.pending_count,
            result.retry_wait_count,
            result.dead_letter_count,
        ) == expected[state]
        assert result.provider_requests == 0
        assert result.effects.writes is False
        if state == "replicated":
            assert result.last_replicated_source_manifest_sha256 == "4" * 64
            assert result.last_replicated_at == "2026-09-09T00:00:03Z"
        response = http_status(settings)
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == result.status
        assert body["reason_code"] == result.reason_code
        assert body["provider_requests"] == 0
        assert body["effects"]["writes"] is False
        assert str(tmp_path) not in response.text


def test_nfr11_status_read_p95_with_10000_local_rows(tmp_path) -> None:
    """NFR-11: warm public status reads over 10,000 terminal rows stay bounded."""
    from backend.app.storage.layout import StorageLayout
    from backend.app.storage.replication import (
        ImmutableReplicationSidecarStore,
        ReplicationSidecarStore,
        ReplicationStatusService,
    )

    settings = Settings(
        _env_file=None,
        local_market_dataset_root=tmp_path / "dataset",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        replication_enabled=True,
    )
    settings.local_market_dataset_root.mkdir()
    layout = StorageLayout(settings)
    source = SourceInstanceStore(layout.replication_source_instance).create(
        canonical_root_path=settings.local_market_dataset_root,
        canonical_schema_digest="c" * 64,
        source_instance_nonce="d" * 64,
    )
    source_id = source.source_instance_id
    source_sha = source.source_instance_sha256
    inventory = (
        {
            "relative_path": "bars.parquet",
            "object_sha256": "2" * 64,
            "size_bytes": 10,
            "row_count": 1,
            "trade_date": "2026-09-09",
            "source": "bao_stock",
        },
    )
    object_set = domain_sha256("stock-eva/r2f4.3/object-set/v1", list(inventory))
    journals = []
    for index in range(10_000):
        checkpoint = build_source_checkpoint(
            source_instance_id=source_id,
            source_instance_sha256=source_sha,
            publication_binding_sha256=f"{index + 1:064x}",
            pointer_row_sha256=f"{index + 2:064x}",
            pointer_generation=f"generation-{index + 1}",
            source_run_id=f"run-{index + 1}",
            source_trade_date="2026-09-09",
            source_published_at=(datetime(2026, 9, 9, tzinfo=UTC) + timedelta(microseconds=index))
            .isoformat()
            .replace("+00:00", "Z"),
            pointer_db_device=1,
            pointer_db_inode=index + 2,
            pointer_db_schema_digest="e" * 64,
            manifest_canonical_sha256="f" * 64,
            source_manifest_bytes_sha256="0" * 64,
            source_object_set_sha256=object_set,
            object_inventory=inventory,
        )
        journals.append(
            build_journal_record(
                checkpoint_id=checkpoint.checkpoint_id,
                source_instance_id=source_id,
                source_instance_sha256=source_sha,
                publication_binding_sha256=checkpoint.publication_binding_sha256,
                source_published_at=checkpoint.source_published_at,
                checkpoint_projection=checkpoint,
                created_at=checkpoint.source_published_at,
            )
        )
    sidecar = ReplicationSidecarStore(layout.replication_sidecar_root)
    sidecar.initialize(source_instance_id=source_id, source_instance_sha256=source_sha)
    result = sidecar.import_journals(
        journals,
        operation_day="2026-09-09",
        destination_id="1" * 32,
        created_at="2026-09-09T00:00:00Z",
    )
    assert result.imported_count == 10_000

    def terminalize(connection):
        for (intent_id,) in connection.execute(
            "SELECT intent_id FROM replication_heads ORDER BY intent_id"
        ).fetchall():
            ImmutableReplicationSidecarStore._append_event_and_update_head(
                connection,
                intent_id=str(intent_id),
                from_state="pending",
                to_state="dead_letter",
                attempt=1,
                reason_code="DEAD_LETTER",
                occurred_at="2026-09-09T00:01:00Z",
                lease_owner=None,
                lease_until=None,
                next_attempt_at="2026-09-09T00:01:00Z",
                event_type="terminal",
            )
        return replication._MutationOutcome(None, True)

    sidecar._mutate_generation(terminalize)
    public_status = ReplicationStatusService(settings)

    def physical_fingerprint(path: Path):
        return tuple(
            (
                str(item.relative_to(path)),
                "dir" if item.is_dir() else "file",
                item.stat().st_ino,
                item.stat().st_size,
                item.stat().st_mtime_ns,
                None if item.is_dir() else hashlib.sha256(item.read_bytes()).hexdigest(),
            )
            for item in sorted(path.rglob("*"))
        )

    before = physical_fingerprint(sidecar.path)
    public_status.read()  # warmup
    samples = []
    for _ in range(5):
        started = perf_counter()
        status = public_status.read()
        samples.append(perf_counter() - started)
        assert status.dead_letter_count == 10_000
        assert status.pending_count == 0
        assert status.effects.writes is False
        assert status.provider_requests == 0
    p95 = sorted(samples)[int(len(samples) * 0.95) - 1]
    assert p95 < 0.5, f"status p95={p95:.3f}s samples={samples!r}"
    assert physical_fingerprint(sidecar.path) == before


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
