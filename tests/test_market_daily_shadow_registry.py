from __future__ import annotations

import fcntl
import hashlib
import os
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from backend.app.config import Settings, TickFlowFreeRuntimeSettings
from backend.app.market.daily_shadow_models import domain_sha256
from backend.app.market.daily_shadow_registry import (
    DAILY_SHADOW_TERMS_CONTRACT_VERSION,
    DailyAttemptAudit,
    DailyCircuitAction,
    DailySessionFailure,
    DailySessionSuccess,
    DailyShadowContract,
    DailyShadowRegistry,
    DailyShadowRegistryReader,
    DailyShadowRegistryUnavailable,
    DailyWindowBinding,
)
from backend.app.market.daily_shadow_schema import (
    DAILY_SHADOW_DDL,
    DAILY_SHADOW_SCHEMA_SHA256,
    MIGRATION_ID,
    SCHEMA_VERSION,
    canonical_json_bytes,
    validate_daily_shadow_schema,
)
from backend.app.market.providers.shadow_contracts import TermsEvidence
from backend.app.storage.layout import StorageLayout

DATES = tuple(date(2026, 7, 14) + timedelta(days=index) for index in range(20))
HASH = "a" * 64


def _terms(*, contract_version: str = DAILY_SHADOW_TERMS_CONTRACT_VERSION):
    return TermsEvidence.build(
        content_bytes=b"reviewed TickFlow Free Daily Bar terms",
        official_url_allowlist=("https://free-api.tickflow.org",),
        terms_evidence_id="tickflow-free-daily-bar-20260828",
        provider_id="tickflow",
        content_object_relpath="terms/tickflow-free-daily-bar-20260828.txt",
        contract_version=contract_version,
        as_of_date="2026-08-28",
        reviewer="stock-eva-owner",
        review_id="r2f3-free-daily-bar-review-20260828",
        approved_intended_use="free-historical-daily-ohlc-shadow-only",
        approved_retention="final-success-source-evidence-only",
        approved_credential_mode="credentialless-free",
        approved_quota_decision="unqualified-max-40-sequential-one-attempt",
    )


def _contract(terms=None):
    terms = terms or _terms()
    return DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256)


def _binding(terms=None, **updates):
    terms = terms or _terms()
    values = {
        "calendar_generation": "calendar-20260828",
        "calendar_sha256": "1" * 64,
        "universe_sha256": "2" * 64,
        "version_vector_sha256": "3" * 64,
        "terms_evidence_sha256": terms.manifest_sha256,
        "expected_dates": DATES,
    }
    values.update(updates)
    return DailyWindowBinding(**values)


def _registry(tmp_path, *, clock=None):
    root = tmp_path / "control"
    root.mkdir(mode=0o700)
    terms = _terms()
    registry = DailyShadowRegistry(
        root / "daily_bar_shadow.sqlite3",
        clock=clock or (lambda: datetime(2026, 8, 28, tzinfo=UTC)),
    )
    registry.initialize(_contract(terms), terms)
    window = registry.ensure_window(_binding(terms))
    return registry, terms, window


def _success(epoch_id: str, trade_date: date, ordinal: int):
    suffix = f"{ordinal:02d}"
    return DailySessionSuccess(
        epoch_id=epoch_id,
        job_id=f"daily-job-{suffix}",
        session_id=f"daily-session-{suffix}",
        session_report_id=f"daily-report-{suffix}",
        attestation_id=f"daily-attestation-{suffix}",
        trade_date=trade_date,
        canonical_snapshot_sha256=(f"{ordinal % 10}" * 64),
        request_plan_sha256="4" * 64,
        completion_sha256="5" * 64,
        evidence_id=f"daily-evidence-{suffix}",
        evidence_sha256="6" * 64,
        evidence_bundle_sha256="7" * 64,
        evidence_bundle_ref=f"evidence/{suffix}",
        candidate_id=f"daily-candidate-{'8' * 30}{suffix}",
        candidate_sha256="9" * 64,
        candidate_bundle_sha256="b" * 64,
        candidate_bundle_ref=f"candidates/{suffix}",
        quality_report_sha256="c" * 64,
        reconciliation_report_sha256="d" * 64,
        attempts=(
            DailyAttemptAudit(
                ordinal=0,
                request_id=f"request-{suffix}",
                outcome="SUCCESS",
                elapsed_ms=1,
                response_bytes=100,
                expected_rows=2,
                observed_rows=2,
            ),
        ),
    )


def _lease(registry, window, trade_date, ordinal):
    return registry.lease_session(
        epoch_id=window.epoch_id,
        job_id=f"daily-job-{ordinal:02d}",
        session_id=f"daily-session-{ordinal:02d}",
        trade_date=trade_date,
        request_plan_sha256="4" * 64,
        canonical_snapshot_sha256=f"{ordinal % 10}" * 64,
        owner="pytest",
        now=datetime(2026, 8, 28, tzinfo=UTC),
    )


def test_schema_source_and_checksum_are_frozen():
    assert MIGRATION_ID == "r2f3-daily-shadow-0001"
    assert SCHEMA_VERSION == 1
    assert (
        DAILY_SHADOW_SCHEMA_SHA256 == hashlib.sha256(DAILY_SHADOW_DDL.encode("utf-8")).hexdigest()
    )
    assert "PRAGMA journal_mode=WAL" not in DAILY_SHADOW_DDL
    assert "daily_shadow_terminal_attestation" in DAILY_SHADOW_DDL


def test_layout_uses_private_isolated_daily_paths(tmp_path):
    (tmp_path / "control").mkdir(mode=0o700)
    (tmp_path / "shadow").mkdir(mode=0o700)
    settings = Settings(
        local_control_dir=tmp_path / "control",
        provider_shadow_root=(tmp_path / "shadow").absolute(),
    )
    layout = StorageLayout(settings)
    assert layout.daily_bar_shadow_database.name == "daily_bar_shadow.sqlite3"
    assert layout.daily_bar_shadow_lock.name == "daily_bar_shadow.sqlite3.lock"
    assert layout.daily_bar_shadow_evidence_root == settings.provider_shadow_root / "daily-evidence"
    assert (
        layout.daily_bar_shadow_candidate_root == settings.provider_shadow_root / "daily-candidates"
    )
    assert layout.validate_daily_bar_shadow_layout() == (
        layout.daily_bar_shadow_database,
        layout.daily_bar_shadow_evidence_root,
        layout.daily_bar_shadow_candidate_root,
    )
    with pytest.raises(ValueError):
        Settings(daily_bar_shadow_database_name="../shadow.sqlite3")
    with pytest.raises(ValueError):
        Settings(daily_bar_shadow_database_name="calendar_sync.sqlite3")
    with pytest.raises(ValueError):
        TickFlowFreeRuntimeSettings(daily_bar_shadow_database_name="provider_registry.sqlite3")


def test_initialize_requires_exact_daily_contract_terms_and_private_paths(tmp_path):
    root = tmp_path / "control"
    root.mkdir(mode=0o700)
    registry = DailyShadowRegistry(root / "daily_bar_shadow.sqlite3")
    wrong = _terms(contract_version="r2f3-tickflow-free-daily-v2-task14")
    with pytest.raises(DailyShadowRegistryUnavailable, match="terms"):
        registry.initialize(_contract(wrong), wrong)
    assert not (root / "daily_bar_shadow.sqlite3-wal").exists()

    symlink = tmp_path / "linked"
    symlink.symlink_to(root, target_is_directory=True)
    with pytest.raises(DailyShadowRegistryUnavailable, match="path"):
        DailyShadowRegistry(symlink / "daily.sqlite3").initialize(_contract(), _terms())


def test_reader_missing_corrupt_and_changed_schema_is_zero_write_unavailable(tmp_path):
    database = tmp_path / "missing.sqlite3"
    lock = database.with_name(database.name + ".lock")
    before = set(tmp_path.iterdir())
    result = DailyShadowRegistryReader(database).read()
    assert result.status == "UNAVAILABLE"
    assert set(tmp_path.iterdir()) == before
    assert not lock.exists()

    database.write_bytes(b"not sqlite")
    database.chmod(0o600)
    assert DailyShadowRegistryReader(database).read().status == "UNAVAILABLE"


def test_reader_lock_contention_is_zero_write_unavailable(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    before = registry.path.read_bytes()
    lock_fd = os.open(registry.lock_path, os.O_RDONLY | os.O_NOFOLLOW)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert DailyShadowRegistryReader(registry.path).read().status == "UNAVAILABLE"
        assert registry.path.read_bytes() == before
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def test_reader_and_writer_reject_hardlinked_control_files(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    outside = tmp_path / "outside.sqlite3"
    os.link(registry.path, outside)
    before = registry.path.read_bytes()
    assert DailyShadowRegistryReader(registry.path).read().status == "UNAVAILABLE"
    with pytest.raises(DailyShadowRegistryUnavailable, match="database"):
        registry.record_endpoint_success("must-not-write")
    assert registry.path.read_bytes() == before


def test_raw_sql_cannot_bypass_terminal_graph_validator(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    success = _success(window.epoch_id, DATES[0], 0)
    registry.commit_success(success, lease=lease)

    connection = sqlite3.connect(registry.path)
    with pytest.raises(sqlite3.DatabaseError, match="daily_validate_terminal_graph"):
        connection.execute(
            "INSERT INTO daily_shadow_terminal_attestation SELECT "
            "'forged',epoch_id,job_id,session_id,session_report_id,evidence_id,candidate_id,"
            "request_plan_json,request_plan_sha256,completion_json,completion_sha256,"
            "attempt_closure_json,attempt_closure_sha256,report_graph_json,report_graph_sha256,"
            "lower(hex(randomblob(32))),1 FROM daily_shadow_terminal_attestation LIMIT 1"
        )
    connection.close()


def test_completed_job_requires_terminal_attestation_at_sql_boundary(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    _lease(registry, window, DATES[0], 0)
    connection = sqlite3.connect(registry.path)
    connection.execute("PRAGMA foreign_keys=ON")
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE daily_shadow_job SET run_status='COMPLETED',lease_owner=NULL,"
            "lease_expires_at=NULL WHERE job_id='daily-job-00'"
        )
    connection.close()


def test_exact_twenty_session_progression_qualifies_only_daily_sidecar(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    provider_registry = tmp_path / "control" / "provider_registry.sqlite3"
    for ordinal, trade_date in enumerate(DATES):
        lease = _lease(registry, window, trade_date, ordinal)
        snapshot = registry.commit_success(
            _success(window.epoch_id, trade_date, ordinal), lease=lease
        )
        assert snapshot.consecutive_sessions == ordinal + 1
        assert snapshot.state == ("SHADOW_QUALIFIED" if ordinal == 19 else "OBSERVING")
    assert not provider_registry.exists()
    assert registry.read().publication_enabled is False
    assert registry.read().failover_enabled is False
    assert registry.read().observation_mode == "HISTORICAL_SHADOW"


def test_exact_terminal_retry_is_idempotent(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    success = _success(window.epoch_id, DATES[0], 0)
    first = registry.commit_success(success, lease=lease)
    second = registry.commit_success(success, lease=lease)
    assert first == second
    assert _lease(registry, window, DATES[0], 0).outcome == "ALREADY_TERMINAL"
    assert registry.read().window.state == "OBSERVING"
    assert registry.read().session_report_count == 1


def test_expired_session_lease_cannot_terminalize_and_may_be_reclaimed(tmp_path):
    now = [datetime(2026, 8, 28, tzinfo=UTC)]
    registry, _terms_value, window = _registry(tmp_path, clock=lambda: now[0])
    stale = _lease(registry, window, DATES[0], 0)
    now[0] += timedelta(seconds=1801)
    with pytest.raises(DailyShadowRegistryUnavailable, match="lease"):
        registry.commit_success(_success(window.epoch_id, DATES[0], 0), lease=stale)
    assert registry.read().session_report_count == 0

    reclaimed = registry.lease_session(
        epoch_id=window.epoch_id,
        job_id="daily-job-00",
        session_id="daily-session-00",
        trade_date=DATES[0],
        request_plan_sha256="4" * 64,
        canonical_snapshot_sha256="0" * 64,
        owner="reclaimer",
        now=now[0],
    )
    assert reclaimed.outcome == "LEASED"
    assert reclaimed.state_version == stale.state_version + 1
    assert (
        registry.commit_success(
            _success(window.epoch_id, DATES[0], 0), lease=reclaimed
        ).consecutive_sessions
        == 1
    )


def test_terminal_retry_with_changed_completion_identity_resets_epoch(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    success = _success(window.epoch_id, DATES[0], 0)
    registry.commit_success(success, lease=lease)
    conflict = registry.commit_success(
        success.model_copy(update={"completion_sha256": "e" * 64}), lease=lease
    )
    assert conflict.state == "RESET"
    assert conflict.consecutive_sessions == 0
    assert registry.read().session_report_count == 1


@pytest.mark.parametrize(
    "crash_at", ["after_evidence", "after_candidate", "after_report", "after_attestation"]
)
def test_terminal_attachment_crash_rolls_back_every_sidecar_row(tmp_path, crash_at):
    case_root = tmp_path / crash_at
    case_root.mkdir(mode=0o700)
    registry, _terms_value, window = _registry(case_root)
    lease = _lease(registry, window, DATES[0], 0)
    success = _success(window.epoch_id, DATES[0], 0)
    with pytest.raises(DailyShadowRegistryUnavailable, match="transaction"):
        registry.commit_success(success, lease=lease, simulate_crash_at=crash_at)
    status = registry.read()
    assert status.session_report_count == 0
    assert status.window.consecutive_sessions == 0
    assert registry.commit_success(success, lease=lease).consecutive_sessions == 1


@pytest.mark.parametrize("outcome", ["FAILURE", "MISMATCH", "UNAVAILABLE"])
def test_non_success_resets_epoch_and_retains_prior_reports(tmp_path, outcome):
    registry, _terms_value, window = _registry(tmp_path)
    first_lease = _lease(registry, window, DATES[0], 0)
    registry.commit_success(_success(window.epoch_id, DATES[0], 0), lease=first_lease)
    second_lease = _lease(registry, window, DATES[1], 1)
    reset = registry.commit_failure(
        DailySessionFailure(
            epoch_id=window.epoch_id,
            job_id="daily-job-01",
            session_id="daily-session-01",
            session_report_id="daily-failure-01",
            trade_date=DATES[1],
            outcome=outcome,
            failure_class="provider_failure",
            attempts=(),
        ),
        lease=second_lease,
    )
    assert reset.state == "RESET"
    assert reset.consecutive_sessions == 0
    assert reset.first_trade_date is None
    assert reset.last_trade_date is None
    assert registry.read().session_report_count == 2


def test_gap_duplicate_and_binding_change_reset_without_deleting_history(tmp_path):
    registry, terms, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    registry.commit_success(_success(window.epoch_id, DATES[0], 0), lease=lease)

    gap = registry.lease_session(
        epoch_id=window.epoch_id,
        job_id="daily-job-gap",
        session_id="daily-session-gap",
        trade_date=DATES[2],
        request_plan_sha256="4" * 64,
        canonical_snapshot_sha256="f" * 64,
        owner="pytest",
        now=datetime(2026, 8, 28, tzinfo=UTC),
    )
    assert gap.outcome == "RESET"
    assert registry.read().session_report_count == 1

    replacement = registry.ensure_window(_binding(terms, version_vector_sha256="e" * 64))
    assert replacement.epoch_id != window.epoch_id
    assert replacement.state == "OBSERVING"
    assert registry.read().epoch_count == 2


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("calendar_generation", "calendar-20260829"),
        ("calendar_sha256", "e" * 64),
        ("universe_sha256", "f" * 64),
        ("version_vector_sha256", "d" * 64),
    ],
)
def test_each_daily_window_binding_change_starts_new_zero_count_epoch(tmp_path, field, changed):
    registry, terms, window = _registry(tmp_path)
    replacement = registry.ensure_window(_binding(terms, **{field: changed}))
    assert replacement.epoch_id != window.epoch_id
    assert replacement.state == "OBSERVING"
    assert replacement.consecutive_sessions == 0
    assert registry.read().epoch_count == 2


def test_new_reviewed_terms_reset_binding_and_start_new_immutable_epoch(tmp_path):
    registry, _old_terms, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    registry.commit_success(_success(window.epoch_id, DATES[0], 0), lease=lease)
    new_terms = TermsEvidence.build(
        content_bytes=b"reviewed TickFlow Free Daily Bar terms revision 2",
        official_url_allowlist=("https://free-api.tickflow.org",),
        terms_evidence_id="tickflow-free-daily-bar-20260828-r2",
        provider_id="tickflow",
        content_object_relpath="terms/tickflow-free-daily-bar-20260828-r2.txt",
        contract_version=DAILY_SHADOW_TERMS_CONTRACT_VERSION,
        as_of_date="2026-08-28",
        reviewer="stock-eva-owner",
        review_id="r2f3-free-daily-bar-review-20260828-r2",
        approved_intended_use="free-historical-daily-ohlc-shadow-only",
        approved_retention="final-success-source-evidence-only",
        approved_credential_mode="credentialless-free",
        approved_quota_decision="unqualified-max-40-sequential-one-attempt",
    )
    registry.initialize(_contract(new_terms), new_terms)
    replacement = registry.ensure_window(_binding(new_terms))
    assert replacement.epoch_id != window.epoch_id
    assert replacement.consecutive_sessions == 0
    assert registry.read().epoch_count == 2
    assert registry.read().session_report_count == 1


def test_circuit_open_skip_half_open_probe_and_safe_recovery(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    now = datetime(2026, 8, 28, tzinfo=UTC)
    for ordinal in range(3):
        state = registry.record_endpoint_failure(f"failure-{ordinal}", now=now)
    assert state.state == "OPEN"
    assert registry.circuit_action(owner="slot-a", now=now).action == (
        DailyCircuitAction.SKIPPED_CIRCUIT_OPEN
    )

    probe = registry.circuit_action(owner="slot-b", now=now + timedelta(seconds=901))
    assert probe.action == DailyCircuitAction.RUN_FIXED_FIVE_HALF_OPEN_PROBE
    assert probe.max_attempts == 1
    assert probe.zero_write is True
    assert probe.ends_slot is True
    assert (
        registry.circuit_action(
            owner="slot-c", now=now + timedelta(seconds=901, milliseconds=1)
        ).action
        == DailyCircuitAction.SKIPPED_HALF_OPEN
    )
    closed = registry.resolve_probe(
        probe.probe_lease_id,
        owner="slot-b",
        success=True,
        now=now + timedelta(seconds=902),
    )
    assert closed.state == "CLOSED"
    assert registry.circuit_action(owner="next-slot", now=now + timedelta(seconds=903)).action == (
        DailyCircuitAction.RUN_FULL_SESSION
    )


def test_closed_endpoint_success_resets_consecutive_failure_count(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    now = datetime(2026, 8, 28, tzinfo=UTC)
    registry.record_endpoint_failure("failure-a", now=now)
    registry.record_endpoint_failure("failure-b", now=now)
    state = registry.record_endpoint_success("success-a", now=now)
    assert state.state == "CLOSED"
    assert state.consecutive_failures == 0


def test_schema_drift_and_unknown_migration_fail_closed(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    connection = sqlite3.connect(registry.path)
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA ignore_check_constraints=ON")
    connection.execute("DROP TRIGGER schema_migration_immutable_update")
    connection.execute("UPDATE schema_migration SET migration_id='unknown' WHERE schema_version=1")
    connection.commit()
    connection.close()
    assert DailyShadowRegistryReader(registry.path).read().status == "UNAVAILABLE"
    with pytest.raises(DailyShadowRegistryUnavailable, match="schema"):
        registry.read()


def test_semantic_graph_tamper_fails_closed_even_when_frozen_schema_is_intact(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    connection = sqlite3.connect(registry.path)
    trigger_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='daily_terms_immutable_update'"
    ).fetchone()[0]
    canonical = connection.execute(
        "SELECT canonical_json FROM daily_shadow_terms_evidence"
    ).fetchone()[0]
    connection.execute("DROP TRIGGER daily_terms_immutable_update")
    connection.execute(
        "UPDATE daily_shadow_terms_evidence SET canonical_json=?",
        (bytes(canonical).replace(b"stock-eva-owner", b"stock-eva-other"),),
    )
    connection.execute(trigger_sql)
    connection.commit()
    validate_daily_shadow_schema(connection)
    connection.close()

    before = registry.path.read_bytes()
    assert DailyShadowRegistryReader(registry.path).read().status == "UNAVAILABLE"
    with pytest.raises(DailyShadowRegistryUnavailable, match="graph"):
        registry.record_endpoint_success("must-not-write")
    assert registry.path.read_bytes() == before


def test_terminal_preimage_must_match_attached_job_even_with_valid_digest(tmp_path):
    registry, _terms_value, window = _registry(tmp_path)
    lease = _lease(registry, window, DATES[0], 0)
    registry.commit_success(_success(window.epoch_id, DATES[0], 0), lease=lease)
    connection = sqlite3.connect(registry.path)
    connection.row_factory = sqlite3.Row
    trigger_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' "
        "AND name='daily_attestation_immutable_update'"
    ).fetchone()[0]
    row = connection.execute("SELECT * FROM daily_shadow_terminal_attestation").fetchone()
    forged_plan = canonical_json_bytes(
        {
            "job_id": row["job_id"],
            "trade_date": DATES[1].isoformat(),
            "request_plan_sha256": "4" * 64,
        }
    )
    forged_plan_sha = hashlib.sha256(forged_plan).hexdigest()
    graph_hashes = (
        forged_plan_sha,
        row["completion_sha256"],
        row["attempt_closure_sha256"],
        row["report_graph_sha256"],
    )
    forged_attestation = domain_sha256(
        "stock-eva/r2f3/free-daily-terminal-attestation/v1", graph_hashes
    )
    connection.execute("DROP TRIGGER daily_attestation_immutable_update")
    connection.execute(
        "UPDATE daily_shadow_terminal_attestation SET request_plan_json=?,"
        "request_plan_sha256=?,attestation_sha256=?",
        (forged_plan, forged_plan_sha, forged_attestation),
    )
    connection.execute(trigger_sql)
    connection.commit()
    validate_daily_shadow_schema(connection)
    connection.close()

    assert DailyShadowRegistryReader(registry.path).read().status == "UNAVAILABLE"


def test_database_and_lock_are_private_delete_journal_files(tmp_path):
    registry, _terms_value, _window = _registry(tmp_path)
    assert stat_mode(registry.path) == 0o600
    assert stat_mode(registry.lock_path) == 0o600
    connection = sqlite3.connect(registry.path)
    assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    connection.close()
    assert not registry.path.with_name(registry.path.name + "-wal").exists()


def stat_mode(path):
    return os.stat(path).st_mode & 0o777
