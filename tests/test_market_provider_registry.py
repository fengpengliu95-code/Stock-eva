import hashlib
import sqlite3
from pathlib import Path

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
    changed = _record().model_copy(
        update={
            "admission_state": AdmissionState.CANARY,
            "adapter_hash": _sha("new-adapter"),
            "terms_evidence_hash": terms.manifest_sha256,
            "terms_review_id": terms.review_id,
        }
    )
    assert registry.reopen_quarantined(changed).state_version == 4


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
    for _index in range(19):
        window = registry.record_session("tickflow", window.window_id, success=True)
    assert window.consecutive_sessions == 19
    assert window.window_state == "observing"
    window = registry.record_session("tickflow", window.window_id, success=False)
    assert window.consecutive_sessions == 0
    assert window.window_state == "reset"
    for _index in range(20):
        window = registry.record_session("tickflow", window.window_id, success=True)
    assert window.consecutive_sessions == 20
    assert window.window_state == "observing"


def test_adapter_policy_terms_or_schema_change_resets_window():
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    window = registry.record_session("tickflow", window.window_id, success=True)
    reset = registry.reset_window_if_version_changed(
        "tickflow", window.window_id, _sha("new-vector"), "cal-1", _sha("calendar")
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
    from backend.app.market.providers.shadow_contracts import STATIC_PROVIDER_CONTRACTS

    terms = _terms("tushare")
    record = _record("tushare").model_copy(
        update={
            "terms_evidence_hash": terms.manifest_sha256,
            "terms_review_id": terms.review_id,
        }
    )
    monkeypatch.setenv("STOCK_EVA_TUSHARE_TOKEN", "must-not-read")
    with pytest.raises(TermsEvidenceUnavailable):
        resolve_provider_credential(
            record,
            terms,
            provider_contract=STATIC_PROVIDER_CONTRACTS[ShadowProviderId.TUSHARE],
        )
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
    with pytest.raises(ValueError):
        registry.transition("tickflow", "qualified", expected_state_version=1)
    window = registry.ensure_window("tickflow", _sha("vector"), "cal-1", _sha("calendar"))
    for _index in range(20):
        window = registry.record_session("tickflow", window.window_id, success=True)
    assert window.window_state == "observing"
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
    registry.record_session("tickflow", window.window_id, success=True)
    terms = _terms()
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider("tickflow", terms, expected_state_version=0)
    assert attached.state_version == 1
    assert registry.read_status("tickflow").terms_evidence_hash == terms.manifest_sha256
    reset = registry.read_window("tickflow", window.window_id)
    assert reset.consecutive_sessions == 0
    assert reset.state_version == 2
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
