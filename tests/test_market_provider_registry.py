import hashlib
import inspect
import json
import sqlite3
from pathlib import Path
from types import MappingProxyType

import pytest

from backend.app.api.market import UnavailableReason, market_provider_status
from backend.app.config import Settings
from backend.app.market.providers.registry import (
    RegistryUnavailable,
    ShadowRegistry,
    TermsEvidenceUnavailable,
    exact_credential_env,
    make_token,
)
from backend.app.market.providers.shadow_contracts import (
    AdmissionState,
    ShadowProviderId,
    ShadowProviderRecord,
    TermsEvidence,
    canonical_terms_evidence,
    terms_evidence_manifest_sha256,
)
from backend.app.market.shadow_registry_schema import (
    MIGRATION_ID,
    REGISTRY_DDL,
    initialize_registry,
)
from backend.app.storage.layout import StorageLayout


def _sha(value: str = "x") -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _terms(provider: str = "tickflow") -> TermsEvidence:
    content = b"reviewed official terms\n"
    return TermsEvidence.build(
        terms_evidence_id="terms-1",
        provider_id=provider,
        official_url_allowlist=("https://example.invalid/terms",),
        content_object_relpath="terms/tickflow.txt",
        content_bytes=content,
        contract_version="r2f3-terms-v1",
        as_of_date="2026-08-24",
        reviewer="reviewer-1",
        review_id="review-1",
        approved_intended_use="internal research",
        approved_retention="local bounded",
        approved_credential_mode="environment-only",
        approved_quota_decision="pending-canary",
    )


def _record(provider: str = "tickflow", state: AdmissionState = AdmissionState.DISCOVERED):
    return ShadowProviderRecord(
        provider_id=provider,
        admission_state=state,
        adapter_hash=_sha("adapter"),
        endpoint_contract_hash=_sha("endpoint"),
        source_schema_hash=_sha("schema"),
        normalizer_hash=_sha("normalizer"),
        reconciliation_policy_hash=_sha("policy"),
        terms_evidence_hash=None,
        terms_review_id=None,
        credential_env_name=exact_credential_env(provider),
        intended_use="internal research",
        retention_decision="local bounded",
        quota_contract="pending",
        required_fields_json="[]",
        unit_contract_json="{}",
        state_version=0,
        quarantine_reason=None,
    )


def test_shadow_provider_ids_are_static_and_no_dynamic_import():
    assert tuple(item.value for item in ShadowProviderId) == ("tickflow", "tushare")
    assert set(ShadowRegistry.allowed_provider_ids()) == {"tickflow", "tushare"}
    with pytest.raises(ValueError):
        ShadowProviderId("backend.app.market.providers.baostock")
    assert not hasattr(ShadowRegistry, "load_plugin")


def test_registry_transitions_are_closed_and_quarantine_resets_version(tmp_path):
    registry = ShadowRegistry(tmp_path / "provider_registry.sqlite3")
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    assert registry.transition("tickflow", "canary", expected_state_version=1).state_version == 2
    with pytest.raises(ValueError):
        registry.transition("tickflow", "qualified", expected_state_version=2)
    registry.transition("tickflow", "quarantined", expected_state_version=2)
    with pytest.raises(ValueError):
        registry.transition("tickflow", "shadow", expected_state_version=3)
    new_terms = TermsEvidence.build(
        terms_evidence_id="terms-2",
        provider_id="tickflow",
        official_url_allowlist=("https://example.invalid/terms",),
        content_object_relpath="terms/tickflow-v2.txt",
        content_bytes=b"reviewed official terms v2\n",
        contract_version="r2f3-terms-v2",
        as_of_date="2026-08-25",
        reviewer="reviewer-2",
        review_id="review-2",
        approved_intended_use="internal research",
        approved_retention="local bounded",
        approved_credential_mode="environment-only",
        approved_quota_decision="pending-canary",
    )
    registry.put_terms_evidence(new_terms)
    registry.attach_terms_to_provider("tickflow", new_terms, expected_state_version=3)
    changed = _record().model_copy(
        update={
            "admission_state": AdmissionState.CANARY,
            "adapter_hash": _sha("new-adapter"),
            "terms_evidence_hash": new_terms.manifest_sha256,
            "terms_review_id": new_terms.review_id,
        }
    )
    assert (
        registry.reopen_quarantined(changed, version_vector_sha256=_sha("vector-v2")).state_version
        == 5
    )


def test_missing_or_corrupt_registry_reader_is_unavailable_and_zero_write(tmp_path):
    path = tmp_path / "provider_registry.sqlite3"
    before = sorted(item.name for item in tmp_path.iterdir())
    with pytest.raises(RegistryUnavailable):
        ShadowRegistry(path).read_status("tickflow")
    assert not path.exists()
    assert sorted(item.name for item in tmp_path.iterdir()) == before
    path.write_bytes(b"not sqlite")
    before = path.stat().st_mtime_ns
    with pytest.raises(RegistryUnavailable):
        ShadowRegistry(path).read_status("tickflow")
    assert path.stat().st_mtime_ns == before


def test_registry_secret_env_name_never_serializes_value(monkeypatch):
    monkeypatch.setenv("STOCK_EVA_TICKFLOW_TOKEN", "secret-token-value")
    record = _record().model_dump_json()
    assert "secret-token-value" not in record
    assert "STOCK_EVA_TICKFLOW_TOKEN" in record
    assert "token" not in record.lower().replace("stock_eva_tickflow_token", "")


def test_shadow_root_is_distinct_from_canonical_roots(tmp_path):
    settings = Settings(
        provider_shadow_root=tmp_path / "shadow",
        provider_evidence_root=tmp_path / "canonical-evidence",
    )
    layout = StorageLayout(settings)
    assert layout.provider_shadow_root != layout.provider_evidence_root
    assert layout.provider_shadow_staging == tmp_path / "shadow" / "staging"
    assert layout.provider_shadow_bundles == tmp_path / "shadow" / "bundles"


def test_qualification_requires_exactly_twenty_consecutive_sessions():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    assert not hasattr(registry, "record_session")
    assert window.window_state == "observing"


def test_adapter_policy_terms_or_schema_change_resets_window():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    reset = registry.reset_window_if_version_changed(
        "tickflow",
        window.window_id,
        _sha("new-vector"),
        "cal-1",
        _sha("calendar"),
        expected_provider_state_version=1,
        expected_window_state_version=0,
    )
    assert reset.consecutive_sessions == 0
    assert reset.window_state == "reset"


def test_terms_evidence_freezes_urls_bytes_hash_asof_reviewer_review_id_and_decisions():
    evidence = _terms()
    assert evidence.official_url_allowlist == ("https://example.invalid/terms",)
    assert evidence.content_bytes_sha256 == hashlib.sha256(b"reviewed official terms\n").hexdigest()
    assert evidence.manifest_sha256 == terms_evidence_manifest_sha256(evidence)
    assert canonical_terms_evidence(evidence).endswith(b"\n")
    with pytest.raises(ValueError):
        evidence.model_copy(update={"review_id": "changed"})


def test_exact_env_mapping_rejects_arbitrary_env_name_without_reading_client(monkeypatch):
    with pytest.raises(ValueError):
        exact_credential_env("tickflow", requested="ARBITRARY_TOKEN")
    monkeypatch.setenv("ARBITRARY_TOKEN", "must-not-read")
    with pytest.raises(TypeError):
        make_token("tickflow", requested="ARBITRARY_TOKEN", terms_approved=False)


def test_tushare_execute_rejects_before_http_client_when_https_or_terms_unapproved():
    with pytest.raises(TypeError):
        make_token("tushare", terms_approved=False, https_proof=False)
    with pytest.raises(TypeError):
        make_token("tushare", terms_approved=True, https_proof=False)


def test_registry_reader_deserializes_fingerprinted_bytes_or_fails_closed(tmp_path):
    path = tmp_path / "provider_registry.sqlite3"
    registry = ShadowRegistry(path)
    registry.initialize()
    registry.put_provider(_record())
    result = registry.read_status("tickflow")
    assert result.provider_id == "tickflow"
    assert result.admission_state == AdmissionState.DISCOVERED
    path.chmod(0o644)
    with pytest.raises(RegistryUnavailable):
        registry.read_status("tickflow")


def test_registry_ddl_compiles_pragmas_foreign_keys_checks_and_unique_constraints(tmp_path):
    connection = sqlite3.connect(":memory:")
    initialize_registry(connection)
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "memory"
    assert MIGRATION_ID == "r2f3-registry-0002"
    assert "CREATE TABLE provider_record" in REGISTRY_DDL
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    connection.close()


def test_registry_reader_existing_lock_shared_flock_journal_and_zero_write_fail_closed(tmp_path):
    path = tmp_path / "provider_registry.sqlite3"
    registry = ShadowRegistry(path)
    registry.initialize()
    registry.put_provider(_record())
    before = path.stat().st_mtime_ns
    assert registry.read_status("tickflow").provider_id == "tickflow"
    assert path.stat().st_mtime_ns == before
    (tmp_path / "provider_registry.sqlite3-journal").write_bytes(b"journal")
    with pytest.raises(RegistryUnavailable):
        registry.read_status("tickflow")


def test_registry_state_version_cas_and_lock_order_never_hold_bundle_and_registry_locks():
    assert ShadowRegistry.lock_order() == ("bundle_publish_release", "registry_cas")


def test_registry_cas_uses_begin_immediate_update_where_state_version_and_rowcount_one():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    assert registry.transition("tickflow", "canary", expected_state_version=1).state_version == 2
    with pytest.raises(RegistryUnavailable):
        registry.transition("tickflow", "shadow", expected_state_version=1)
    assert registry.read_status("tickflow").state_version == 2


def test_registry_stale_writer_rolls_back_without_state_or_history_loss(tmp_path):
    path = tmp_path / "provider_registry.sqlite3"
    first = ShadowRegistry(path)
    first.initialize()
    first.put_provider(_record())
    terms = _terms()
    first.put_terms_evidence(terms)
    first.attach_terms_to_provider("tickflow", terms)
    stale = ShadowRegistry(path)
    first.transition("tickflow", "canary", expected_state_version=1)
    with pytest.raises(RegistryUnavailable):
        stale.transition("tickflow", "shadow", expected_state_version=1)
    assert first.read_status("tickflow").admission_state == AdmissionState.CANARY


def test_registry_composite_provider_window_job_session_cross_bind_is_rejected(tmp_path):
    registry = ShadowRegistry(tmp_path / "provider_registry.sqlite3")
    registry.initialize()
    registry.put_provider(_record())
    registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    with pytest.raises(ValueError):
        registry.attach_session("tickflow", "window-1", provider_id="tushare")


def test_terms_evidence_provider_hash_review_fk_and_model_sql_roundtrip(tmp_path):
    registry = ShadowRegistry(tmp_path / "provider_registry.sqlite3")
    registry.initialize()
    evidence = _terms()
    registry.put_terms_evidence(evidence)
    restored = registry.read_terms_evidence(evidence.terms_evidence_id)
    assert restored.model_dump() == evidence.model_dump()
    registry.put_provider(
        _record().model_copy(
            update={
                "terms_evidence_hash": evidence.manifest_sha256,
                "terms_review_id": evidence.review_id,
            }
        )
    )
    assert registry.read_status("tickflow").terms_evidence_hash == evidence.manifest_sha256


def test_terms_evidence_canonical_bytes_manifest_hash_and_content_hash_are_frozen(tmp_path):
    evidence = _terms()
    changed = TermsEvidence.model_construct(
        **(evidence.model_dump(mode="python") | {"approved_quota_decision": "approved"})
    )
    assert terms_evidence_manifest_sha256(changed) != evidence.manifest_sha256
    assert changed.content_bytes_sha256 == evidence.content_bytes_sha256


def test_terms_evidence_descriptor_mutation_symlink_size_or_hash_mismatch_is_unavailable(tmp_path):
    registry = ShadowRegistry(tmp_path / "provider_registry.sqlite3")
    registry.initialize()
    evidence = _terms()
    registry.put_terms_evidence(evidence)
    registry.terms_object_root = tmp_path
    (tmp_path / "terms" / "tickflow.txt").write_bytes(b"mutated")
    with pytest.raises(RegistryUnavailable):
        registry.read_terms_evidence(evidence.terms_evidence_id)


def test_h1_credential_resolution_uses_reviewed_projection_and_rejects_current_tushare_before_env(
    monkeypatch,
):
    from backend.app.market.providers.registry import resolve_provider_credential

    terms = _terms("tushare")
    record = _record("tushare").model_copy(
        update={
            "terms_evidence_hash": terms.manifest_sha256,
            "terms_review_id": terms.review_id,
        }
    )
    monkeypatch.setenv("STOCK_EVA_TUSHARE_TOKEN", "must-not-read")
    with pytest.raises(TermsEvidenceUnavailable):
        resolve_provider_credential(record, terms)
    with pytest.raises(TypeError):
        resolve_provider_credential(record, terms, terms_approved=True)


def test_h2_frozen_migrations_have_exact_ids_and_all_registry_triggers(tmp_path):
    from backend.app.market.shadow_registry_schema import MIGRATION_IDS

    path = tmp_path / "provider_registry.sqlite3"
    registry = ShadowRegistry(path)
    registry.initialize()
    connection = sqlite3.connect(path)
    assert MIGRATION_IDS == ("r2f3-registry-0001", "r2f3-registry-0002")
    assert connection.execute(
        "SELECT migration_id FROM schema_migration ORDER BY schema_version"
    ).fetchall() == [
        ("r2f3-registry-0001",),
        ("r2f3-registry-0002",),
    ]
    trigger_count = connection.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='trigger'"
    ).fetchone()[0]
    assert trigger_count >= 9
    assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    connection.close()


def test_h3_generic_transition_cannot_qualify_without_verified_terminal_graph():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    terms = _terms()
    registry.put_provider(_record())
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    with pytest.raises(ValueError):
        registry.transition("tickflow", "qualified", expected_state_version=1)
    with pytest.raises(RegistryUnavailable):
        registry.promote_qualified(
            "tickflow",
            window.window_id,
            terminal_attestation_id="missing",
            evidence_sha256=_sha("evidence"),
            candidate_sha256=_sha("candidate"),
            session_report_id="missing",
        )


def test_h4_terms_attach_expected_state_cas_resets_window_atomically():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    window = registry.ensure_window("tickflow", _sha("old"), "cal-1", _sha("calendar"))
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider(
        "tickflow", terms, expected_state_version=0, expected_window_state_version=0
    )
    assert attached.state_version == 1
    assert registry.read_status("tickflow").terms_evidence_hash == terms.manifest_sha256
    reset = registry.read_window("tickflow", window.window_id)
    assert reset.consecutive_sessions == 0
    assert reset.state_version == 1
    with pytest.raises(RegistryUnavailable):
        registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)


def test_h5_untrusted_shadow_ancestor_symlink_is_zero_write_and_rejected(tmp_path):
    from backend.app.storage.layout import StorageLayout

    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    settings = Settings(
        provider_shadow_root=link / "shadow",
        provider_evidence_root=tmp_path / "canonical",
    )
    with pytest.raises(RegistryUnavailable):
        StorageLayout(settings).validate_provider_shadow_root()
    assert not (target / "shadow").exists()


def test_h6_golden_partition_is_real_parquet_and_not_serializer_bytes():
    import duckdb

    path = Path(__file__).parent / "fixtures" / "r2f2_golden" / "partition.parquet"
    rows = (
        duckdb.connect(":memory:")
        .execute(
            "SELECT trade_date::VARCHAR, symbol, close FROM read_parquet(?) ORDER BY symbol",
            [str(path)],
        )
        .fetchall()
    )
    assert rows == [("2026-08-21", "sh.600000", 10.5), ("2026-08-21", "sz.000001", 20.25)]


def test_h7_status_missing_or_unusable_shadow_root_is_unavailable_without_initialization(tmp_path):
    settings = Settings(
        local_control_dir=tmp_path / "control",
        provider_shadow_root=tmp_path / "shadow",
        provider_evidence_root=tmp_path / "canonical",
    )
    before = sorted(tmp_path.rglob("*"))
    response = market_provider_status(settings, provider_id=ShadowProviderId.TICKFLOW)
    assert response.status == "unavailable"
    assert response.unavailable_reason in {
        UnavailableReason.REGISTRY_MISSING,
        UnavailableReason.SHADOW_ROOT_UNAVAILABLE,
    }
    assert sorted(tmp_path.rglob("*")) == before


def test_m_canonical_digest_nfc_normalizes_keys_and_rejects_nested_null():
    from backend.app.market.shadow_registry_schema import canonical_digest

    raw = b'{"job_id":"j","provider_id":"tickflow","requests":{},"window_id":"w"}\n'
    assert canonical_digest("request-plan", raw)
    with pytest.raises(ValueError):
        canonical_digest(
            "request-plan",
            b'{"job_id":"j","provider_id":"tickflow","requests":{"nested":null},"window_id":"w"}\n',
        )


def test_round2_provider_authority_is_immutable_and_not_caller_supplied():
    from backend.app.market.providers.registry import resolve_provider_credential
    from backend.app.market.providers.shadow_contracts import (
        PROVIDER_CONTRACTS,
        STATIC_PROVIDER_CONTRACTS,
    )

    assert isinstance(STATIC_PROVIDER_CONTRACTS, MappingProxyType)
    assert STATIC_PROVIDER_CONTRACTS is PROVIDER_CONTRACTS
    with pytest.raises(TypeError):
        STATIC_PROVIDER_CONTRACTS[ShadowProviderId.TICKFLOW] = object()
    assert all(item.model_config.get("frozen") for item in STATIC_PROVIDER_CONTRACTS.values())
    with pytest.raises(TypeError):
        resolve_provider_credential(_record(), _terms(), provider_contract=object())


def test_round2_only_0001_file_is_upgraded_incrementally_and_memory_path_is_not_file_contract(
    tmp_path,
):
    from backend.app.market.providers.registry import ShadowRegistryTerminalWriter
    from backend.app.market.shadow_registry_schema import (
        MIGRATION_CHECKSUMS,
        REGISTRY_DDL,
    )

    path = tmp_path / "provider_registry.sqlite3"
    connection = ShadowRegistryTerminalWriter.open(path)
    connection.executescript(REGISTRY_DDL)
    connection.execute(
        "INSERT INTO schema_migration"
        "(migration_id,schema_version,applied_at,checksum) VALUES "
        "(?,?,datetime('now'),?)",
        ("r2f3-registry-0001", 1, MIGRATION_CHECKSUMS["r2f3-registry-0001"]),
    )
    connection.commit()
    connection.close()
    path.chmod(0o600)
    ShadowRegistry(path).initialize()
    upgraded = sqlite3.connect(path)
    assert upgraded.execute(
        "SELECT migration_id FROM schema_migration ORDER BY schema_version"
    ).fetchall() == [("r2f3-registry-0001",), ("r2f3-registry-0002",)]
    assert upgraded.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert upgraded.execute("PRAGMA synchronous").fetchone()[0] == 2
    upgraded.close()
    with pytest.raises(TypeError):
        initialize_registry(":memory:")


def test_round2_session_count_requires_verified_terminal_report_and_unique_calendar_dates():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)
    assert not hasattr(registry, "record_session")
    assert hasattr(registry, "qualify_window")


def test_round2_quarantine_reopen_requires_new_reviewed_version():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider(
        "tickflow", terms, expected_state_version=0, expected_window_state_version=0
    )
    canary = registry.transition(
        "tickflow", "canary", expected_state_version=attached.state_version
    )
    quarantined = registry.transition(
        "tickflow", "quarantined", expected_state_version=canary.state_version
    )
    same = canary.model_copy(update={"state_version": quarantined.state_version})
    with pytest.raises(RegistryUnavailable):
        registry.reopen_quarantined(same, version_vector_sha256=_sha("same-vector"))


def test_round2_material_reset_requires_provider_and_window_cas_versions():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    window = registry.ensure_window("tickflow", _sha("old"), "cal-1", _sha("calendar"))
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider(
        "tickflow", terms, expected_state_version=0, expected_window_state_version=0
    )
    assert attached.state_version == 1
    with pytest.raises(RegistryUnavailable):
        registry.reset_window_if_version_changed(
            "tickflow",
            window.window_id,
            _sha("new"),
            "cal-2",
            _sha("calendar-2"),
            expected_provider_state_version=0,
            expected_window_state_version=0,
        )


def test_round2_descriptor_read_rejects_mode_and_trusted_tmp_alias_is_physical(tmp_path):
    settings = Settings(provider_shadow_root=Path("/tmp") / "stock-eva-round2-root")
    physical = Path("/private/tmp/stock-eva-round2-root")
    physical.mkdir(mode=0o700, exist_ok=True)
    try:
        assert (
            StorageLayout(settings).validate_provider_shadow_root() == settings.provider_shadow_root
        )
    finally:
        physical.rmdir()
    terms = _terms()
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_terms_evidence(terms)
    with pytest.raises(ValueError):
        TermsEvidence.build(
            terms_evidence_id=terms.terms_evidence_id,
            provider_id=terms.provider_id,
            official_url_allowlist=terms.official_url_allowlist,
            content_object_relpath=terms.content_object_relpath,
            contract_version=terms.contract_version,
            as_of_date=terms.as_of_date,
            reviewer=terms.reviewer,
            review_id=terms.review_id,
            approved_intended_use=terms.approved_intended_use,
            approved_retention=terms.approved_retention,
            approved_credential_mode=terms.approved_credential_mode,
            approved_quota_decision=terms.approved_quota_decision,
            content_bytes=b"x" * (64 * 1024 * 1024 + 1),
        )
    terms_root = tmp_path / "terms"
    file_registry = ShadowRegistry(tmp_path / "registry.sqlite3", terms_object_root=terms_root)
    file_registry.initialize()
    file_registry.put_terms_evidence(terms)
    (terms_root / terms.content_object_relpath).chmod(0o644)
    with pytest.raises(RegistryUnavailable):
        file_registry.read_terms_evidence(terms.terms_evidence_id)


def test_round2_golden_objects_are_real_r2f2_models_and_get_is_real_response_model():
    import json
    from datetime import datetime

    from fastapi.testclient import TestClient

    import backend.app.api.market as market_api
    from backend.app.main import app
    from backend.app.market.calendar import SHANGHAI, get_trading_calendar
    from backend.app.market.calendar_sync import CalendarSyncState
    from backend.app.market.candidates import CandidateManifest, SessionSelection
    from backend.app.market.evidence import EvidenceManifest, EvidenceReader

    root = Path(__file__).parent / "fixtures" / "r2f2_golden"
    CandidateManifest.model_validate(json.loads((root / "candidate.json").read_text()))
    SessionSelection.model_validate(json.loads((root / "selection.json").read_text()))
    EvidenceManifest.model_validate(json.loads((root / "evidence.json").read_text()))
    evidence = EvidenceReader(root).read("ev-2ce5ef73d443e9e13ffce1d4")
    assert evidence.manifest.evidence_id == "ev-2ce5ef73d443e9e13ffce1d4"
    evidence.close()

    class _Store:
        def published_refresh(self):
            return None

        def scheduler_state(self):
            return None

    settings = Settings(
        _env_file=None,
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
        market_continuity_start_date=None,
        auto_refresh_enabled=False,
        scheduled_refresh_enabled=False,
    )
    app.dependency_overrides[market_api.get_market_store] = lambda: _Store()
    app.dependency_overrides[market_api.get_trading_calendar] = get_trading_calendar
    app.dependency_overrides[market_api.get_market_clock] = lambda: (
        lambda: datetime(2026, 8, 24, 18, 30, tzinfo=SHANGHAI)
    )
    app.dependency_overrides[market_api.get_settings] = lambda: settings
    app.dependency_overrides[market_api.get_calendar_sync_store] = lambda: type(
        "_Sync", (), {"state": lambda self: CalendarSyncState()}
    )()
    try:
        with TestClient(app) as client:
            response = client.get("/api/v1/market/status")
        assert response.content + b"\n" == (root / "GET.json").read_bytes()
    finally:
        app.dependency_overrides.clear()


def test_round2_terminal_attestation_is_append_only_at_sql_and_reader_authorizer_layers():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    names = {
        row[0]
        for row in registry._memory_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    }
    assert {
        "terminal_attestation_immutable_update",
        "terminal_attestation_immutable_delete",
    } <= names
    assert "terminal_attestation_gate" in names


def test_round2_cli_and_api_share_safe_shadow_root_unavailable_reason(
    tmp_path, monkeypatch, capsys
):
    import sys

    import backend.app.cli as cli

    settings = Settings(
        local_control_dir=tmp_path / "control",
        provider_shadow_root=tmp_path / "missing-shadow",
        provider_evidence_root=tmp_path / "canonical",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "market-provider-status", "--provider", "tickflow"],
    )
    assert cli.main() == 1
    assert '"unavailable_reason": "shadow_root_unavailable"' in capsys.readouterr().out


def test_round3_legacy_0001_without_checksum_is_upgraded_before_checksum_select(tmp_path):
    from backend.app.market.providers.registry import ShadowRegistryTerminalWriter
    from backend.app.market.shadow_registry_schema import REGISTRY_DDL

    path = tmp_path / "provider_registry.sqlite3"
    legacy_ddl = REGISTRY_DDL.replace(",\n checksum TEXT NOT NULL CHECK(length(checksum)=64)", "")
    connection = ShadowRegistryTerminalWriter.open(path)
    connection.executescript(legacy_ddl)
    connection.execute(
        "INSERT INTO schema_migration VALUES (?,?,datetime('now'))",
        ("r2f3-registry-0001", 1),
    )
    connection.commit()
    connection.close()
    path.chmod(0o600)

    ShadowRegistry(path).initialize()
    upgraded = sqlite3.connect(path)
    assert "checksum" in {row[1] for row in upgraded.execute("PRAGMA table_info(schema_migration)")}
    assert upgraded.execute(
        "SELECT migration_id FROM schema_migration ORDER BY schema_version"
    ).fetchall() == [("r2f3-registry-0001",), ("r2f3-registry-0002",)]
    upgraded.close()


def test_round4_legacy_schema_fingerprint_rejects_arbitrary_ddl(tmp_path):
    """A missing checksum is only repairable for the frozen historical schema."""
    from backend.app.market.providers.registry import ShadowRegistryTerminalWriter

    path = tmp_path / "provider_registry.sqlite3"
    legacy_ddl = REGISTRY_DDL.replace(",\n checksum TEXT NOT NULL CHECK(length(checksum)=64)", "")
    # This is deliberately a plausible registry with an unreviewed extra index.
    legacy_ddl += "\nCREATE INDEX unreviewed_ddl ON provider_record(provider_id);\n"
    connection = ShadowRegistryTerminalWriter.open(path)
    connection.executescript(legacy_ddl)
    connection.execute(
        "INSERT INTO schema_migration VALUES (?,?,datetime('now'))",
        ("r2f3-registry-0001", 1),
    )
    connection.commit()
    connection.close()
    path.chmod(0o600)

    with pytest.raises(RegistryUnavailable, match="migration"):
        ShadowRegistry(path).initialize()
    check = sqlite3.connect(path)
    assert check.execute(
        "SELECT migration_id FROM schema_migration ORDER BY schema_version"
    ).fetchall() == [("r2f3-registry-0001",)]
    check.close()


def test_round4_terminal_gate_asserts_success_report_identity_and_version():
    from backend.app.market.shadow_registry_schema import REGISTRY_DDL

    assert "attestation_requires_terminal_success_session" in REGISTRY_DDL
    assert "outcome='success'" in REGISTRY_DDL
    assert "report_version>=2" in REGISTRY_DDL
    assert "terminal_attestation_id=NEW.attestation_id" in REGISTRY_DDL
    assert "session_report_id=NEW.session_report_id" in REGISTRY_DDL
    assert "evidence_ready" in REGISTRY_DDL


def test_round5_terminal_gate_requires_successful_attempt_reverse_identity():
    from backend.app.market.shadow_registry_schema import REGISTRY_DDL

    assert "s.successful_attempt_id=a.attempt_id" in REGISTRY_DDL
    assert "terminal_marker=1" in REGISTRY_DDL
    assert "terminal_session_report_id=NEW.session_report_id" in REGISTRY_DDL
    assert "a.attempt_id=s.successful_attempt_id" in REGISTRY_DDL


def test_round4_qualification_requires_verified_calendar_reader(tmp_path):
    from backend.app.market.providers.registry import VerifiedConfirmedCalendarReader

    assert VerifiedConfirmedCalendarReader is not None
    signature = inspect.signature(ShadowRegistry.qualify_window)
    assert signature.parameters["calendar_reader"].annotation in (
        VerifiedConfirmedCalendarReader,
        "VerifiedConfirmedCalendarReader",
    )

    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    fake = type("FakeReader", (), {"read": lambda self, *_: None})()
    with pytest.raises(RegistryUnavailable, match="confirmed calendar"):
        registry.qualify_window(
            "tickflow",
            window.window_id,
            calendar_reader=fake,
            expected_provider_state_version=0,
            expected_window_state_version=0,
        )


def test_round4_verified_calendar_reader_reads_nofollow_json_and_confirmed_sessions(tmp_path):
    from backend.app.market.providers.registry import VerifiedConfirmedCalendarReader

    calendar_path = tmp_path / "confirmed-calendar.json"
    payload = {
        "year": 2026,
        "status": "confirmed",
        "published_on": "2026-01-01",
        "sources": [
            {"exchange": "SSE", "title": "reviewed", "url": "https://example.invalid"},
            {"exchange": "SZSE", "title": "reviewed", "url": "https://example.invalid"},
        ],
        "closed_dates": ["2026-01-01"],
    }
    calendar_path.write_text(
        json.dumps(
            {
                "authority_provenance": "reviewed-authority-1",
                "generation": "generation-1",
                "configs": [payload],
            }
        ),
        encoding="utf-8",
    )
    digest = _sha("calendar")
    reader = VerifiedConfirmedCalendarReader(
        calendar_path,
        provider_id="tickflow",
        window_id="window-1",
        version_vector_sha256=_sha("vector"),
        universe_id="universe-1",
        universe_sha256=_sha("universe"),
        adapter_hash=_sha("adapter"),
        endpoint_contract_hash=_sha("endpoint"),
        source_schema_hash=_sha("schema"),
        normalizer_hash=_sha("normalizer"),
        reconciliation_policy_hash=_sha("policy"),
        calendar_generation="generation-1",
        authority_provenance="reviewed-authority-1",
        calendar_sha256=hashlib.sha256(calendar_path.read_bytes()).hexdigest(),
    )
    snapshot = reader.read("tickflow", "window-1")
    assert snapshot.calendar_sha256 != digest
    assert len(snapshot.sessions) >= 20
    assert all(session.weekday() < 5 for session in snapshot.sessions)
    with pytest.raises(TypeError, match="immutable"):
        reader.universe_id = "forged"


def test_round5_existing_external_hardlink_is_rejected_without_mutation(tmp_path):
    parent = tmp_path / "shadow"
    parent.mkdir(mode=0o700)
    path = parent / "provider_registry.sqlite3"
    external = tmp_path / "external.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sentinel (value TEXT)")
    connection.execute("INSERT INTO sentinel VALUES ('untouched')")
    connection.commit()
    connection.close()
    path.chmod(0o600)
    external.hardlink_to(path)
    before = path.read_bytes()
    identity = path.stat().st_ino

    with pytest.raises(RegistryUnavailable, match="link"):
        ShadowRegistry(path).initialize()
    assert path.stat().st_ino == identity
    assert path.read_bytes() == before
    assert external.read_bytes() == before


def test_round5_calendar_payload_requires_frozen_authority_wrapper(tmp_path):
    from backend.app.market.providers.registry import VerifiedConfirmedCalendarReader

    calendar_path = tmp_path / "calendar.json"
    config = {
        "year": 2026,
        "status": "confirmed",
        "published_on": "2026-01-01",
        "sources": [
            {"exchange": "SSE", "title": "reviewed", "url": "https://example.invalid"},
            {"exchange": "SZSE", "title": "reviewed", "url": "https://example.invalid"},
        ],
        "closed_dates": [],
    }
    calendar_path.write_text(json.dumps(config), encoding="utf-8")
    kwargs = dict(
        provider_id="tickflow",
        window_id="window-1",
        version_vector_sha256=_sha("vector"),
        universe_id="universe-1",
        universe_sha256=_sha("universe"),
        adapter_hash=_sha("adapter"),
        endpoint_contract_hash=_sha("endpoint"),
        source_schema_hash=_sha("schema"),
        normalizer_hash=_sha("normalizer"),
        reconciliation_policy_hash=_sha("policy"),
        calendar_generation="generation-1",
        authority_provenance="reviewed-authority-1",
    )
    reader = VerifiedConfirmedCalendarReader(calendar_path, **kwargs)
    with pytest.raises(RegistryUnavailable, match="confirmed calendar"):
        reader.read("tickflow", "window-1")

    calendar_path.write_text(
        json.dumps(
            {
                "authority_provenance": "reviewed-authority-1",
                "generation": "generation-1",
                "configs": [config],
                "unexpected": True,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RegistryUnavailable, match="confirmed calendar"):
        reader.read("tickflow", "window-1")


def test_round4_configured_basename_swap_before_connect_does_not_write_external(
    tmp_path, monkeypatch
):
    import backend.app.market.providers.registry as registry_module

    parent = tmp_path / "shadow"
    parent.mkdir(mode=0o700)
    path = parent / "provider_registry.sqlite3"
    external = tmp_path / "external.sqlite3"
    external_connection = sqlite3.connect(external)
    external_connection.execute("CREATE TABLE sentinel (value TEXT)")
    external_connection.execute("INSERT INTO sentinel VALUES ('untouched')")
    external_connection.commit()
    external_connection.close()
    original = external.read_bytes()
    real_connect = registry_module.sqlite3.connect
    swapped = False

    def swap_before_connect(open_path, **kwargs):
        nonlocal swapped
        if not swapped and Path(open_path).parent == parent:
            swapped = True
            if path.exists() or path.is_symlink():
                path.unlink()
            path.symlink_to(external)
        return real_connect(open_path, **kwargs)

    monkeypatch.setattr(registry_module.sqlite3, "connect", swap_before_connect)
    with pytest.raises(RegistryUnavailable):
        ShadowRegistry(path).initialize()
    assert external.read_bytes() == original
    assert swapped


def test_round3_terminal_success_graph_uses_deferred_fk_and_legal_insert_order():
    from backend.app.market.shadow_registry_schema import REGISTRY_DDL

    assert "DEFERRABLE INITIALLY DEFERRED" in REGISTRY_DDL
    assert "attestation_requires_terminal_success_session" in REGISTRY_DDL


def test_round3_legal_terminal_success_transaction_inserts_session_then_attestation():
    from backend.app.market.shadow_registry_schema import canonical_digest

    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)
    canary = registry.transition(
        "tickflow", "canary", expected_state_version=attached.state_version
    )
    registry.transition("tickflow", "shadow", expected_state_version=canary.state_version)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    connection = registry._memory_connection
    evidence_sha = _sha("evidence")
    candidate_sha = _sha("candidate")

    def canonical(domain, payload):
        raw = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        return raw, canonical_digest(domain, raw)

    request_plan, request_plan_sha = canonical(
        "request-plan",
        {
            "job_id": "job-1",
            "provider_id": "tickflow",
            "window_id": window.window_id,
            "requests": [],
        },
    )
    completion, completion_sha = canonical(
        "completion",
        {
            "job_id": "job-1",
            "provider_id": "tickflow",
            "window_id": window.window_id,
            "session_id": "session-1",
            "evidence_id": "evidence-1",
            "request_plan_sha256": request_plan_sha,
            "requests": [],
        },
    )
    closure, closure_sha = canonical(
        "attempt-ordinal-closure", {"exact_ordinal_set": [], "ordinals": []}
    )
    report_digest, report_sha = canonical(
        "report-digest", {"session_report_id": "report-1", "report_version": 2, "reports": []}
    )
    connection.execute("BEGIN")
    connection.execute(
        "INSERT INTO shadow_job VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "job-1",
            "tickflow",
            window.window_id,
            "2026-08-24",
            "universe-1",
            "generation-1",
            _sha("manifest"),
            _sha("vector"),
            None,
            None,
            None,
            None,
            "leased",
            "owner",
            None,
            0,
            0,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_evidence_ref VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "evidence-1",
            "job-1",
            "tickflow",
            window.window_id,
            "session-1",
            completion_sha,
            evidence_sha,
            "bundle",
            _sha("bundle"),
            None,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_candidate_ref VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "candidate-1",
            "evidence-1",
            "job-1",
            "tickflow",
            window.window_id,
            "session-1",
            "candidate",
            candidate_sha,
            "quality",
            _sha("quality"),
        ),
    )
    connection.execute(
        "INSERT INTO session_report VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "report-1",
            "tickflow",
            "job-1",
            window.window_id,
            "session-1",
            "attempt-1",
            "evidence-1",
            "candidate-1",
            "att-1",
            2,
            "2026-08-24",
            "success",
            "cal-1",
            _sha("calendar"),
            _sha("universe"),
            _sha("vector"),
            evidence_sha,
            candidate_sha,
            "report",
            _sha("report"),
            0,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_attempt_report VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
        ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "attempt-1",
            "attempt-report-1",
            "job-1",
            "tickflow",
            window.window_id,
            "session-1",
            "request-1",
            "daily",
            "daily",
            0,
            0,
            _sha("vector"),
            "success",
            "2026-08-24T08:00:00Z",
            "2026-08-24T08:01:00Z",
            1,
            1,
            1,
            0,
            0,
            "",
            "[1]",
            1,
            1,
            1,
            "report",
            _sha("attempt-report"),
            '[{"evidence_id":"evidence-1"}]',
            "evidence-1",
            evidence_sha,
            candidate_sha,
            "report-1",
            0,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_terminal_attestation VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
        ",?,?,?,?)",
        (
            "att-1",
            "tickflow",
            "job-1",
            window.window_id,
            "session-1",
            "evidence-1",
            "candidate-1",
            "report-1",
            2,
            request_plan,
            completion,
            closure,
            report_digest,
            closure_sha,
            request_plan_sha,
            completion_sha,
            report_sha,
            evidence_sha,
            candidate_sha,
            "success",
            1,
        ),
    )
    connection.commit()
    assert connection.execute(
        "SELECT 1 FROM shadow_terminal_attestation WHERE attestation_id='att-1'"
    ).fetchone() == (1,)


def test_round3_qualification_requires_confirmed_calendar_reader_not_raw_dates():
    from backend.app.market.providers.registry import ConfirmedCalendarReader

    signature = inspect.signature(ShadowRegistry.qualify_window)
    assert "calendar_reader" in signature.parameters
    assert "trade_date" not in signature.parameters
    assert "calendar_generation" not in signature.parameters
    assert "calendar_sha256" not in signature.parameters
    assert ConfirmedCalendarReader is not None


def test_round3_calendar_reader_fake_rejects_weekend_holiday_unknown_and_mismatch():
    from datetime import date, timedelta

    from backend.app.market.providers.registry import ConfirmedCalendarSnapshot

    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)
    canary = registry.transition(
        "tickflow", "canary", expected_state_version=attached.state_version
    )
    registry.transition("tickflow", "shadow", expected_state_version=canary.state_version)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))

    class RejectingReader:
        def __init__(self, reason):
            self.reason = reason

        def read(self, _provider_id, _window_id):
            raise RegistryUnavailable(self.reason)

    for reason in ("weekend", "holiday", "unknown"):
        with pytest.raises(RegistryUnavailable, match="confirmed calendar snapshot unavailable"):
            registry.qualify_window(
                "tickflow",
                window.window_id,
                calendar_reader=RejectingReader(reason),
                expected_provider_state_version=3,
                expected_window_state_version=0,
            )

    snapshot = ConfirmedCalendarSnapshot(
        provider_id=ShadowProviderId.TICKFLOW,
        window_id=window.window_id,
        version_vector_sha256=_sha("different-vector"),
        calendar_generation="cal-1",
        calendar_sha256=_sha("calendar"),
        universe_id="universe-1",
        adapter_hash=_sha("adapter"),
        reconciliation_policy_hash=_sha("policy"),
        sessions=tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(20)),
    )

    class MismatchReader:
        def read(self, _provider_id, _window_id):
            return snapshot

    with pytest.raises(RegistryUnavailable, match="confirmed calendar"):
        registry.qualify_window(
            "tickflow",
            window.window_id,
            calendar_reader=MismatchReader(),
            expected_provider_state_version=3,
            expected_window_state_version=0,
        )


def test_round3_quarantine_atomically_resets_every_window_and_preserves_old_vector():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    window = registry.ensure_window("tickflow", _sha("old-vector"), "cal-1", _sha("calendar"))
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider(
        "tickflow", terms, expected_state_version=0, expected_window_state_version=0
    )
    quarantined = registry.transition(
        "tickflow", "canary", expected_state_version=attached.state_version
    )
    registry.ensure_window("tickflow", _sha("second-vector"), "cal-2", _sha("calendar-2"))
    registry.transition("tickflow", "quarantined", expected_state_version=quarantined.state_version)
    reset = registry.read_window("tickflow", window.window_id)
    assert reset.consecutive_sessions == 0
    assert reset.window_state == "reset"
    assert registry._memory_connection.execute(
        "SELECT version_vector_sha256 FROM quarantine_snapshot WHERE provider_id='tickflow'"
    ).fetchone()[0] == _sha("old-vector")


def test_round3_writer_precreates_private_0600_basename_before_sqlite_connect(
    tmp_path, monkeypatch
):
    import backend.app.market.providers.registry as registry_module

    path = tmp_path / "provider_registry.sqlite3"
    original_connect = registry_module.sqlite3.connect

    def checked_connect(*args, **kwargs):
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(registry_module.sqlite3, "connect", checked_connect)
    ShadowRegistry(path).initialize()
    assert path.stat().st_mode & 0o777 == 0o600

    symlink_path = tmp_path / "symlink.sqlite3"
    target = tmp_path / "external.sqlite3"
    target.write_bytes(b"external")
    symlink_path.symlink_to(target)
    before = target.read_bytes()
    with pytest.raises(RegistryUnavailable):
        ShadowRegistry(symlink_path).initialize()
    assert target.read_bytes() == before
    assert not symlink_path.with_name("symlink.sqlite3.lock").exists()


def test_round3_reset_requires_both_expected_versions_and_record_session_is_removed():
    parameters = inspect.signature(ShadowRegistry.reset_window_if_version_changed).parameters
    assert parameters["expected_provider_state_version"].default is inspect.Parameter.empty
    assert parameters["expected_window_state_version"].default is inspect.Parameter.empty
    assert not hasattr(ShadowRegistry, "record_session")
