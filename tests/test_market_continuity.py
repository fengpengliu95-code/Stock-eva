import hashlib
import importlib
import multiprocessing
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

import backend.app.market.store as market_store_module
from backend.app.config import Settings
from backend.app.market.automation import RefreshAlreadyRunning, RefreshRunLock
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore

NOW = datetime(2026, 8, 13, 4, 0, tzinfo=UTC)
UNIVERSE_ID = "all-main-board"


def continuity_module():
    try:
        return importlib.import_module("backend.app.market.continuity")
    except ModuleNotFoundError:
        pytest.fail("backend.app.market.continuity is not implemented")


def initialize_continuity(store: MarketStore, lock_path: Path) -> None:
    store.initialize_schema()
    with RefreshRunLock(lock_path):
        store.initialize_continuity_schema()


def enqueue(
    store: MarketStore,
    lock_path: Path,
    *trade_dates: date,
    now: datetime = NOW,
):
    with RefreshRunLock(lock_path):
        return store.enqueue_repair_jobs(
            trade_dates,
            universe_id=UNIVERSE_ID,
            now=now,
        )


def error_refresh(
    trade_date: date,
    *,
    attempt: int,
    retryable: bool = True,
) -> RefreshResult:
    return RefreshResult(
        run_id=f"repair-run-{trade_date}-{attempt}",
        request_key=f"repair:{trade_date}:{UNIVERSE_ID}",
        run_kind="repair",
        requested_date=trade_date,
        source="baostock",
        status="error",
        requested_count=1,
        succeeded_count=0,
        coverage_ratio=0,
        failed_symbols=["sh.600000"],
        quality_issues=["provider_error"],
        error_message="provider request failed",
        failure_stage="fetch",
        failure_class="transport_timeout" if retryable else "auth",
        retryable=retryable,
        started_at=NOW,
        completed_at=NOW + timedelta(minutes=1),
    )


def ready_refresh(trade_date: date) -> RefreshResult:
    return RefreshResult(
        run_id=f"repair-run-{trade_date}-ready",
        request_key=f"repair:{trade_date}:{UNIVERSE_ID}",
        run_kind="repair",
        requested_date=trade_date,
        source="baostock",
        status="ready",
        requested_count=1,
        succeeded_count=1,
        coverage_ratio=1,
        started_at=NOW,
        completed_at=NOW + timedelta(minutes=1),
    )


def file_fingerprint(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def _claim_in_process(
    database_path: str,
    lock_path: str,
    job_id: str,
    owner: str,
    output,
) -> None:
    store = MarketStore(Path(database_path))
    try:
        with RefreshRunLock(Path(lock_path)):
            lease = store.claim_repair_job(
                job_id,
                owner=owner,
                expected_version=1,
                now=NOW,
                lease_seconds=1800,
            )
    except RefreshAlreadyRunning:
        lease = None
    output.put(None if lease is None else lease.model_dump(mode="json"))


def test_continuity_settings_are_fail_closed_and_bounded() -> None:
    settings = Settings(_env_file=None)

    assert settings.market_continuity_start_date is None
    assert settings.market_repair_enabled is False
    assert settings.market_repair_max_attempts == 4
    assert settings.market_repair_lease_seconds == 1800
    assert settings.market_repair_retry_base_seconds == 3600

    for field, value in (
        ("market_repair_max_attempts", 0),
        ("market_repair_max_attempts", 21),
        ("market_repair_lease_seconds", 59),
        ("market_repair_lease_seconds", 86401),
        ("market_repair_retry_base_seconds", 899),
        ("market_repair_retry_base_seconds", 86401),
    ):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **{field: value})


def test_writer_migration_is_additive_and_preserves_legacy_refresh_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    store = MarketStore(path)
    store.initialize_schema()
    daily = error_refresh(date(2026, 8, 11), attempt=1).model_copy(
        update={"run_id": "legacy-daily", "request_key": None, "run_kind": "daily"}
    )
    backfill = error_refresh(date(2026, 8, 12), attempt=1).model_copy(
        update={"run_id": "legacy-backfill", "request_key": None, "run_kind": "backfill"}
    )
    store.save_refresh([], daily, publish=False)
    store.save_refresh([], backfill, publish=False)
    before = [(item.run_id, item.run_kind, item.model_dump()) for item in store.list_refreshes()]
    with duckdb.connect(str(path), read_only=True) as connection:
        before_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    assert "repair_jobs" not in before_tables
    assert "repair_attempts" not in before_tables

    with RefreshRunLock(tmp_path / "market-refresh.lock"):
        store.initialize_continuity_schema()

    after = [(item.run_id, item.run_kind, item.model_dump()) for item in store.list_refreshes()]
    assert after == before
    with duckdb.connect(str(path), read_only=True) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    assert {"repair_jobs", "repair_attempts"} <= tables


def test_read_only_missing_or_corrupt_tables_return_unavailable_without_initialization(
    tmp_path: Path,
) -> None:
    missing = MarketStore(tmp_path / "missing.duckdb")
    snapshot = missing.repair_queue_snapshot()
    assert snapshot.status == "unavailable"
    assert snapshot.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert not missing.path.exists()

    corrupt_path = tmp_path / "corrupt.duckdb"
    with duckdb.connect(str(corrupt_path)) as connection:
        connection.execute("CREATE TABLE repair_jobs (job_id VARCHAR)")
    before = file_fingerprint(corrupt_path)
    corrupt = MarketStore(corrupt_path)

    snapshot = corrupt.repair_queue_snapshot()

    assert snapshot.status == "unavailable"
    assert snapshot.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert snapshot.jobs == ()
    assert snapshot.attempts == ()
    assert file_fingerprint(corrupt_path) == before


def test_read_only_snapshot_rejects_invalid_state_shape_without_repairing_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    enqueue(store, lock_path, date(2026, 8, 10))
    with duckdb.connect(str(path)) as connection:
        connection.execute("UPDATE repair_jobs SET state = 'retry_wait'")
    before = file_fingerprint(path)

    snapshot = store.repair_queue_snapshot()

    assert snapshot.status == "unavailable"
    assert file_fingerprint(path) == before


def test_read_only_snapshot_rejects_orphaned_running_attempt_without_repairing_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=1800,
        )
    assert lease is not None
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM repair_attempts")
    before = file_fingerprint(path)

    snapshot = store.repair_queue_snapshot()

    assert snapshot.status == "unavailable"
    assert file_fingerprint(path) == before


def test_repair_schema_has_no_payload_url_token_path_or_exception_columns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    store = MarketStore(path)
    initialize_continuity(store, tmp_path / "market-refresh.lock")

    with duckdb.connect(str(path), read_only=True) as connection:
        columns = {
            table: tuple(
                row[1] for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
            )
            for table in ("repair_jobs", "repair_attempts")
        }

    assert columns["repair_jobs"] == (
        "job_id",
        "trade_date",
        "universe_id",
        "state",
        "state_version",
        "attempt_count",
        "abandoned_attempt_count",
        "next_attempt_at",
        "lease_id",
        "lease_owner",
        "lease_expires_at",
        "last_attempt_id",
        "last_failure_stage",
        "last_failure_class",
        "created_at",
        "updated_at",
        "published_at",
    )
    assert columns["repair_attempts"] == (
        "attempt_id",
        "job_id",
        "attempt_number",
        "lease_id",
        "lease_owner",
        "outcome",
        "refresh_run_id",
        "failure_stage",
        "failure_class",
        "retryable",
        "started_at",
        "completed_at",
    )
    forbidden = ("payload", "url", "token", "path", "exception", "message", "error")
    assert not any(
        term in column for names in columns.values() for column in names for term in forbidden
    )


def test_enqueue_is_deterministic_and_idempotent_across_restart(tmp_path: Path) -> None:
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    dates = (date(2026, 8, 10), date(2026, 8, 11))

    first = enqueue(store, lock_path, *dates)
    restarted = MarketStore(path)
    second = enqueue(restarted, lock_path, *reversed(dates))
    snapshot = restarted.repair_queue_snapshot()

    assert [item.job_id for item in first] == [
        "repair:2026-08-10:all-main-board",
        "repair:2026-08-11:all-main-board",
    ]
    assert second == []
    assert [item.trade_date for item in snapshot.jobs] == list(dates)
    assert snapshot.attempts == ()


def test_two_processes_under_shared_refresh_lock_cannot_claim_the_same_job(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    processes = [
        context.Process(
            target=_claim_in_process,
            args=(str(path), str(lock_path), job.job_id, f"worker-{index}", output),
        )
        for index in range(2)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    results = [output.get(timeout=2), output.get(timeout=2)]
    snapshot = MarketStore(path).repair_queue_snapshot()

    assert sum(result is not None for result in results) == 1
    assert len(snapshot.attempts) == 1
    assert snapshot.attempts[0].outcome == "running"
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.jobs[0].state_version == 2


def test_claim_contract_failure_rolls_back_job_and_attempt_atomically(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]

    def reject_lease(**_values):
        raise ValueError("synthetic lease validation failure")

    monkeypatch.setattr(market_store_module, "RepairLease", reject_lease)
    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.claim_repair_job(
                job.job_id,
                owner="worker-a",
                expected_version=job.state_version,
                now=NOW,
                lease_seconds=1800,
            )

    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0] == job
    assert snapshot.attempts == ()


def test_lease_finalize_requires_matching_owner_id_and_state_version(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=1800,
        )
    assert lease is not None
    policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600)

    for invalid in (
        lease.model_copy(update={"owner": "worker-b"}),
        lease.model_copy(update={"lease_id": "wrong-lease"}),
        lease.model_copy(update={"state_version": lease.state_version + 1}),
    ):
        with RefreshRunLock(lock_path):
            with pytest.raises(module.RepairQueueConflictError):
                store.finalize_repair_attempt(
                    invalid,
                    outcome="failed",
                    refresh_result=error_refresh(job.trade_date, attempt=1),
                    now=NOW + timedelta(minutes=1),
                    retry_policy=policy,
                )

    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.attempts[0].outcome == "running"


def test_naive_datetime_is_rejected_and_persisted_times_are_utc(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.enqueue_repair_jobs(
                [date(2026, 8, 10)],
                universe_id=UNIVERSE_ID,
                now=datetime(2026, 8, 13, 4, 0),
            )
    job = enqueue(
        store,
        lock_path,
        date(2026, 8, 10),
        now=datetime(2026, 8, 13, 12, 0, tzinfo=timezone(timedelta(hours=8))),
    )[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=datetime(2026, 8, 13, 12, 1, tzinfo=timezone(timedelta(hours=8))),
            lease_seconds=1800,
        )
    assert lease is not None
    assert job.created_at.tzinfo == UTC
    assert lease.expires_at.tzinfo == UTC


def test_expired_lease_becomes_one_abandoned_attempt_and_retry_wait(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=60,
        )
    assert lease is not None
    policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600)
    before = store.repair_queue_snapshot()

    with RefreshRunLock(lock_path):
        early = store.reap_expired_repair_leases(
            now=NOW + timedelta(seconds=59),
            retry_policy=policy,
        )
    assert early == []
    assert store.repair_queue_snapshot() == before

    recovered_at = NOW + timedelta(seconds=60)
    with RefreshRunLock(lock_path):
        recovered = store.reap_expired_repair_leases(
            now=recovered_at,
            retry_policy=policy,
        )
        duplicate = store.reap_expired_repair_leases(
            now=recovered_at,
            retry_policy=policy,
        )
    snapshot = store.repair_queue_snapshot()
    recovered_job = snapshot.jobs[0]
    attempt = snapshot.attempts[0]

    assert len(recovered) == 1
    assert duplicate == []
    assert recovered_job.state == "retry_wait"
    assert recovered_job.state_version == lease.state_version + 1
    assert recovered_job.abandoned_attempt_count == 1
    assert recovered_job.next_attempt_at == recovered_at + timedelta(hours=1)
    assert attempt.outcome == "abandoned"
    assert attempt.completed_at == recovered_at
    assert attempt.retryable is True


def test_expired_worker_cannot_finalize_before_lease_recovery(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=60,
        )
    assert lease is not None

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueConflictError):
            store.finalize_repair_attempt(
                lease,
                outcome="succeeded",
                refresh_result=ready_refresh(job.trade_date),
                now=NOW + timedelta(seconds=61),
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )

    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.attempts[0].outcome == "running"


def test_retry_is_exponential_capped_and_budgeted(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    policy = module.RepairRetryPolicy(max_attempts=7, base_seconds=3600)
    due = NOW
    expected_hours = (1, 2, 4, 8, 16, 24)

    for attempt_number, expected_hours_wait in enumerate(expected_hours, start=1):
        with RefreshRunLock(lock_path):
            lease = store.claim_repair_job(
                job.job_id,
                owner="worker-a",
                expected_version=job.state_version,
                now=due,
                lease_seconds=1800,
            )
            assert lease is not None
            completed_at = due + timedelta(minutes=1)
            job = store.finalize_repair_attempt(
                lease,
                outcome="failed",
                refresh_result=error_refresh(job.trade_date, attempt=attempt_number),
                now=completed_at,
                retry_policy=policy,
            )
        assert job.state == "retry_wait"
        assert job.next_attempt_at == completed_at + timedelta(hours=expected_hours_wait)
        due = job.next_attempt_at

    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=due,
            lease_seconds=1800,
        )
        assert lease is not None
        job = store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(job.trade_date, attempt=7),
            now=due + timedelta(minutes=1),
            retry_policy=policy,
        )
    assert job.state == "dead_letter"
    assert job.next_attempt_at is None


def test_non_retryable_dead_letter_and_terminal_jobs_never_reopen(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    trade_date = date(2026, 8, 10)
    job = enqueue(store, lock_path, trade_date)[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        assert lease is not None
        dead = store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(trade_date, attempt=1, retryable=False),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    assert dead.state == "dead_letter"

    assert enqueue(store, lock_path, trade_date) == []
    with RefreshRunLock(lock_path):
        assert (
            store.claim_repair_job(
                dead.job_id,
                owner="worker-a",
                expected_version=dead.state_version,
                now=NOW + timedelta(days=1),
                lease_seconds=1800,
            )
            is None
        )
    unchanged = store.repair_queue_snapshot().jobs[0]
    assert unchanged.state == "dead_letter"
    assert unchanged.state_version == dead.state_version


def test_dead_letter_does_not_block_a_newer_pending_job(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    old, new = enqueue(store, lock_path, date(2026, 8, 10), date(2026, 8, 11))
    with RefreshRunLock(lock_path):
        old_lease = store.claim_repair_job(
            old.job_id,
            owner="worker-a",
            expected_version=old.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        assert old_lease is not None
        store.finalize_repair_attempt(
            old_lease,
            outcome="failed",
            refresh_result=error_refresh(old.trade_date, attempt=1, retryable=False),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
        new_lease = store.claim_repair_job(
            new.job_id,
            owner="worker-b",
            expected_version=new.state_version,
            now=NOW + timedelta(minutes=1),
            lease_seconds=1800,
        )
    assert new_lease is not None
    assert new_lease.target_session == new.trade_date


def test_strict_published_reconciliation_finalizes_leased_attempt_consistently(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    pending, leased_job = enqueue(store, lock_path, date(2026, 8, 10), date(2026, 8, 11))
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            leased_job.job_id,
            owner="worker-a",
            expected_version=leased_job.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        assert lease is not None
        reconciled = store.reconcile_published_repair_jobs(
            [pending.trade_date, leased_job.trade_date],
            now=NOW + timedelta(minutes=2),
        )
    snapshot = store.repair_queue_snapshot()

    assert [item.trade_date for item in reconciled] == [pending.trade_date, leased_job.trade_date]
    assert all(item.state == "published" for item in snapshot.jobs)
    assert snapshot.jobs[0].state_version == pending.state_version + 1
    assert snapshot.jobs[1].state_version == lease.state_version + 1
    assert snapshot.jobs[1].lease_id is None
    assert snapshot.attempts[0].outcome == "succeeded"
    assert snapshot.attempts[0].retryable is False
    assert snapshot.attempts[0].completed_at == NOW + timedelta(minutes=2)


def test_repair_run_kind_is_additive_and_legacy_values_remain_readable(tmp_path: Path) -> None:
    path = tmp_path / "market.duckdb"
    store = MarketStore(path)
    store.initialize_schema()
    rows = [
        error_refresh(date(2026, 8, 10), attempt=1).model_copy(
            update={"run_id": "daily", "run_kind": "daily"}
        ),
        error_refresh(date(2026, 8, 11), attempt=1).model_copy(
            update={"run_id": "backfill", "run_kind": "backfill"}
        ),
        error_refresh(date(2026, 8, 12), attempt=1).model_copy(
            update={"run_id": "repair", "run_kind": "repair"}
        ),
    ]
    for item in rows:
        store.save_refresh([], item, publish=False)

    assert {(item.run_id, item.run_kind) for item in store.list_refreshes()} == {
        ("daily", "daily"),
        ("backfill", "backfill"),
        ("repair", "repair"),
    }


def test_typed_contracts_reject_unknown_fields_unsafe_ids_and_naive_times() -> None:
    module = continuity_module()
    valid = {
        "job_id": "repair:2026-08-10:all-main-board",
        "trade_date": date(2026, 8, 10),
        "universe_id": UNIVERSE_ID,
        "state": "pending",
        "state_version": 1,
        "attempt_count": 0,
        "abandoned_attempt_count": 0,
        "created_at": NOW,
        "updated_at": NOW,
    }
    with pytest.raises(ValidationError):
        module.RepairJob(**valid, payload="secret")
    with pytest.raises(ValidationError):
        module.RepairJob(**{**valid, "job_id": "/private/repair"})
    with pytest.raises(ValidationError):
        module.RepairJob(
            **{
                **valid,
                "job_id": "repair:2026-08-10:other-universe",
                "universe_id": "other-universe",
            }
        )
    with pytest.raises(ValidationError):
        module.RepairJob(**{**valid, "created_at": NOW.replace(tzinfo=None)})
