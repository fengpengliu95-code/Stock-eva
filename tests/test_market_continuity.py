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


def create_two_retryable_failed_attempts(
    store: MarketStore,
    lock_path: Path,
    job,
):
    module = continuity_module()
    policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=900)
    due = NOW
    for attempt_number in (1, 2):
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
                refresh_result=error_refresh(
                    job.trade_date,
                    attempt=attempt_number,
                    started_at=due,
                ),
                now=due + timedelta(minutes=1),
                retry_policy=policy,
            )
        assert job.next_attempt_at is not None
        due = job.next_attempt_at
    return job


def error_refresh(
    trade_date: date,
    *,
    attempt: int,
    retryable: bool = True,
    started_at: datetime = NOW,
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
        started_at=started_at,
        completed_at=started_at + timedelta(minutes=1),
    )


def ready_refresh(trade_date: date, *, started_at: datetime = NOW) -> RefreshResult:
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
        started_at=started_at,
        completed_at=started_at + timedelta(minutes=1),
    )


def file_fingerprint(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def create_task2_continuity_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    fake_checks: bool = False,
    attempt_foreign_key: bool = False,
) -> None:
    job_checks = (
        ["CHECK (TRUE)"] * 6
        if fake_checks
        else [
            "CHECK (state IN ('pending', 'leased', 'retry_wait', 'published', 'dead_letter'))",
            "CHECK (state_version >= 1)",
            "CHECK (attempt_count >= 0)",
            "CHECK (abandoned_attempt_count >= 0 AND abandoned_attempt_count <= attempt_count)",
            """CHECK (
                (state = 'leased' AND lease_id IS NOT NULL AND lease_owner IS NOT NULL
                 AND lease_expires_at IS NOT NULL)
                OR
                (state <> 'leased' AND lease_id IS NULL AND lease_owner IS NULL
                 AND lease_expires_at IS NULL)
            )""",
            """CHECK (
                (state = 'published' AND published_at IS NOT NULL)
                OR (state <> 'published' AND published_at IS NULL)
            )""",
        ]
    )
    attempt_checks = (
        ["CHECK (TRUE)"] * 3
        if fake_checks
        else [
            "CHECK (attempt_number > 0)",
            "CHECK (outcome IN ('running', 'succeeded', 'failed', 'abandoned'))",
            """CHECK (
                (outcome = 'running' AND completed_at IS NULL AND retryable IS NULL)
                OR
                (outcome <> 'running' AND completed_at IS NOT NULL AND retryable IS NOT NULL)
            )""",
        ]
    )
    connection.execute(
        f"""
        CREATE TABLE repair_jobs (
            job_id VARCHAR PRIMARY KEY,
            trade_date DATE NOT NULL,
            universe_id VARCHAR NOT NULL,
            state VARCHAR NOT NULL,
            state_version BIGINT NOT NULL,
            attempt_count INTEGER NOT NULL,
            abandoned_attempt_count INTEGER NOT NULL,
            next_attempt_at TIMESTAMPTZ,
            lease_id VARCHAR UNIQUE,
            lease_owner VARCHAR,
            lease_expires_at TIMESTAMPTZ,
            last_attempt_id VARCHAR,
            last_failure_stage VARCHAR,
            last_failure_class VARCHAR,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            published_at TIMESTAMPTZ,
            UNIQUE (trade_date, universe_id),
            {", ".join(job_checks)}
        )
        """
    )
    connection.execute(
        f"""
        CREATE TABLE repair_attempts (
            attempt_id VARCHAR PRIMARY KEY,
            job_id VARCHAR NOT NULL,
            attempt_number INTEGER NOT NULL,
            lease_id VARCHAR NOT NULL UNIQUE,
            lease_owner VARCHAR NOT NULL,
            outcome VARCHAR NOT NULL,
            refresh_run_id VARCHAR,
            failure_stage VARCHAR,
            failure_class VARCHAR,
            retryable BOOLEAN,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            UNIQUE (job_id, attempt_number),
            {", ".join(attempt_checks)}
            {", FOREIGN KEY (job_id) REFERENCES repair_jobs(job_id)" if attempt_foreign_key else ""}
        )
        """
    )


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


def test_legacy_task2_schema_is_unavailable_until_explicit_writer_migration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    store = MarketStore(path)
    store.initialize_schema()
    with duckdb.connect(str(path)) as connection:
        create_task2_continuity_tables(connection)

    assert store.repair_queue_snapshot().status == "unavailable"
    with RefreshRunLock(tmp_path / "market-refresh.lock"):
        store.initialize_continuity_schema()
    assert store.repair_queue_snapshot().status == "ready"


def test_schema_with_fake_check_constraints_is_unavailable_even_with_claimed_meta(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    with duckdb.connect(str(path)) as connection:
        create_task2_continuity_tables(connection, fake_checks=True)
        connection.execute(
            """
            CREATE TABLE continuity_schema_meta (
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                schema_hash VARCHAR NOT NULL,
                CHECK (singleton = 1),
                CHECK (schema_version = 1)
            )
            """
        )
        connection.execute(
            "INSERT INTO continuity_schema_meta VALUES (1, 1, ?)",
            [getattr(market_store_module, "_CONTINUITY_SCHEMA_HASH", "pending-schema-hash")],
        )

    assert MarketStore(path).repair_queue_snapshot().status == "unavailable"


def test_schema_with_extra_foreign_key_is_unavailable_even_with_claimed_meta(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.duckdb"
    with duckdb.connect(str(path)) as connection:
        create_task2_continuity_tables(connection, attempt_foreign_key=True)
        connection.execute(
            """
            CREATE TABLE continuity_schema_meta (
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                schema_hash VARCHAR NOT NULL,
                CHECK (singleton = 1),
                CHECK (schema_version = 1)
            )
            """
        )
        connection.execute(
            "INSERT INTO continuity_schema_meta VALUES (1, 1, ?)",
            [getattr(market_store_module, "_CONTINUITY_SCHEMA_HASH", "pending-schema-hash")],
        )

    assert MarketStore(path).repair_queue_snapshot().status == "unavailable"


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


def test_finalize_revalidates_copied_lease_and_matches_database_trade_date(
    tmp_path: Path,
) -> None:
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
    other_date = date(2026, 8, 11)
    bypassed_lease = lease.model_copy(update={"target_session": other_date})
    bypassed_result = ready_refresh(other_date).model_copy(update={"request_key": job.job_id})
    before = store.repair_queue_snapshot()

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                bypassed_lease,
                outcome="succeeded",
                refresh_result=bypassed_result,
                now=NOW + timedelta(minutes=1),
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )

    assert store.repair_queue_snapshot() == before


def test_finalize_revalidates_copied_refresh_and_retry_policy(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    first, second = enqueue(store, lock_path, date(2026, 8, 10), date(2026, 8, 11))
    with RefreshRunLock(lock_path):
        first_lease = store.claim_repair_job(
            first.job_id,
            owner="worker-a",
            expected_version=first.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        second_lease = store.claim_repair_job(
            second.job_id,
            owner="worker-b",
            expected_version=second.state_version,
            now=NOW,
            lease_seconds=1800,
        )
    assert first_lease is not None and second_lease is not None
    invalid_ready = ready_refresh(first.trade_date).model_copy(update={"succeeded_count": 0})
    invalid_policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600).model_copy(
        update={"max_attempts": 0}
    )

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                first_lease,
                outcome="succeeded",
                refresh_result=invalid_ready,
                now=NOW + timedelta(minutes=1),
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                second_lease,
                outcome="failed",
                refresh_result=error_refresh(second.trade_date, attempt=1),
                now=NOW + timedelta(minutes=1),
                retry_policy=invalid_policy,
            )

    snapshot = store.repair_queue_snapshot()
    assert all(item.state == "leased" for item in snapshot.jobs)
    assert all(item.outcome == "running" for item in snapshot.attempts)


def test_success_finalize_requires_deterministic_job_request_key(tmp_path: Path) -> None:
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

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                lease,
                outcome="succeeded",
                refresh_result=ready_refresh(job.trade_date).model_copy(
                    update={"request_key": "unrelated-request"}
                ),
                now=NOW + timedelta(minutes=1),
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )

    assert store.repair_queue_snapshot().jobs[0].state == "leased"


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize(
    "evidence_mutation",
    ["request_key", "requested_date", "started_before_attempt", "completed_after_finalize"],
)
def test_finalize_requires_bound_and_monotonic_refresh_evidence_for_every_outcome(
    tmp_path: Path,
    outcome: str,
    evidence_mutation: str,
) -> None:
    module = continuity_module()
    path = tmp_path / f"{outcome}-{evidence_mutation}.duckdb"
    lock_path = tmp_path / f"{outcome}-{evidence_mutation}.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    claimed_at = NOW + timedelta(minutes=10)
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=claimed_at,
            lease_seconds=1800,
        )
    assert lease is not None
    finalize_at = claimed_at + timedelta(minutes=2)
    if outcome == "succeeded":
        result = ready_refresh(job.trade_date, started_at=claimed_at)
    else:
        result = error_refresh(job.trade_date, attempt=1, started_at=claimed_at)
    if evidence_mutation == "request_key":
        result = result.model_copy(update={"request_key": "unrelated-request"})
    elif evidence_mutation == "requested_date":
        result = result.model_copy(update={"requested_date": date(2026, 8, 11)})
    elif evidence_mutation == "started_before_attempt":
        result = result.model_copy(
            update={
                "started_at": claimed_at - timedelta(seconds=1),
                "completed_at": claimed_at + timedelta(minutes=1),
            }
        )
    else:
        result = result.model_copy(update={"completed_at": finalize_at + timedelta(seconds=1)})
    before = store.repair_queue_snapshot()

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                lease,
                outcome=outcome,
                refresh_result=result,
                now=finalize_at,
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )

    assert store.repair_queue_snapshot() == before


@pytest.mark.parametrize(
    ("outcome", "expected_state"),
    [("succeeded", "published"), ("failed", "retry_wait")],
)
def test_finalize_accepts_exact_bound_monotonic_refresh_evidence(
    tmp_path: Path,
    outcome: str,
    expected_state: str,
) -> None:
    module = continuity_module()
    path = tmp_path / f"valid-{outcome}.duckdb"
    lock_path = tmp_path / f"valid-{outcome}.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    claimed_at = NOW + timedelta(minutes=10)
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=claimed_at,
            lease_seconds=1800,
        )
        assert lease is not None
        result = (
            ready_refresh(job.trade_date, started_at=claimed_at)
            if outcome == "succeeded"
            else error_refresh(job.trade_date, attempt=1, started_at=claimed_at)
        )
        finalized = store.finalize_repair_attempt(
            lease,
            outcome=outcome,
            refresh_result=result,
            now=claimed_at + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )

    assert finalized.state == expected_state


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


def test_claim_rejects_time_before_job_update_without_mutation(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10), now=NOW)[0]

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.claim_repair_job(
                job.job_id,
                owner="worker-a",
                expected_version=job.state_version,
                now=NOW - timedelta(seconds=1),
                lease_seconds=1800,
            )

    assert store.repair_queue_snapshot().jobs[0] == job
    assert store.repair_queue_snapshot().attempts == ()


def test_finalize_and_reconcile_reject_time_before_attempt_started(tmp_path: Path) -> None:
    module = continuity_module()
    policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600)
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    first, second = enqueue(store, lock_path, date(2026, 8, 10), date(2026, 8, 11))
    claimed_at = NOW + timedelta(minutes=10)
    with RefreshRunLock(lock_path):
        first_lease = store.claim_repair_job(
            first.job_id,
            owner="worker-a",
            expected_version=first.state_version,
            now=claimed_at,
            lease_seconds=1800,
        )
        second_lease = store.claim_repair_job(
            second.job_id,
            owner="worker-b",
            expected_version=second.state_version,
            now=claimed_at,
            lease_seconds=1800,
        )
    assert first_lease is not None and second_lease is not None
    before = store.repair_queue_snapshot()
    backdated = NOW + timedelta(minutes=5)

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.finalize_repair_attempt(
                first_lease,
                outcome="failed",
                refresh_result=error_refresh(first.trade_date, attempt=1),
                now=backdated,
                retry_policy=policy,
            )
        with pytest.raises(module.RepairQueueError):
            store.reconcile_published_repair_jobs([second.trade_date], now=backdated)

    assert store.repair_queue_snapshot() == before


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


def test_reap_revalidates_copied_retry_policy_without_mutation(tmp_path: Path) -> None:
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
    before = store.repair_queue_snapshot()
    invalid_policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600).model_copy(
        update={"base_seconds": 1}
    )

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.reap_expired_repair_leases(
                now=NOW + timedelta(seconds=60),
                retry_policy=invalid_policy,
            )

    assert store.repair_queue_snapshot() == before


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
                refresh_result=error_refresh(
                    job.trade_date,
                    attempt=attempt_number,
                    started_at=due,
                ),
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
            refresh_result=error_refresh(job.trade_date, attempt=7, started_at=due),
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


def test_snapshot_rejects_pending_with_terminal_attempt(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    first = enqueue(store, lock_path, date(2026, 8, 10))[0]
    with RefreshRunLock(lock_path):
        first_lease = store.claim_repair_job(
            first.job_id,
            owner="worker-a",
            expected_version=first.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        assert first_lease is not None
        store.finalize_repair_attempt(
            first_lease,
            outcome="failed",
            refresh_result=error_refresh(first.trade_date, attempt=1),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            UPDATE repair_jobs
            SET state = 'pending', next_attempt_at = NULL
            WHERE job_id = ?
            """,
            [first.job_id],
        )

    assert store.repair_queue_snapshot().status == "unavailable"


def test_snapshot_rejects_retry_wait_with_successful_latest_attempt(tmp_path: Path) -> None:
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
        store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(job.trade_date, attempt=1),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            UPDATE repair_attempts
            SET outcome = 'succeeded', failure_stage = NULL, failure_class = NULL,
                retryable = FALSE
            WHERE job_id = ?
            """,
            [job.job_id],
        )

    assert store.repair_queue_snapshot().status == "unavailable"


def test_snapshot_requires_abandoned_count_to_equal_abandoned_attempts(tmp_path: Path) -> None:
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
        store.reap_expired_repair_leases(
            now=NOW + timedelta(seconds=60),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE repair_jobs SET abandoned_attempt_count = 0 WHERE job_id = ?",
            [job.job_id],
        )

    assert store.repair_queue_snapshot().status == "unavailable"


def test_snapshot_checks_attempt_sequence_without_allocating_attempt_count_range(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
        store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(job.trade_date, attempt=1, retryable=False),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE repair_jobs SET attempt_count = 2000000000 WHERE job_id = ?",
            [job.job_id],
        )

    def forbidden_range(*_args):
        raise AssertionError("snapshot must not allocate a range from corrupt attempt_count")

    monkeypatch.setattr(module, "range", forbidden_range, raising=False)
    assert store.repair_queue_snapshot().status == "unavailable"


@pytest.mark.parametrize("first_outcome", ["succeeded", "nonretryable_failed"])
def test_snapshot_rejects_terminal_attempt_followed_by_another_attempt(
    tmp_path: Path,
    first_outcome: str,
) -> None:
    path = tmp_path / f"terminal-lineage-{first_outcome}.duckdb"
    lock_path = tmp_path / f"terminal-lineage-{first_outcome}.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    create_two_retryable_failed_attempts(store, lock_path, job)
    with duckdb.connect(str(path)) as connection:
        if first_outcome == "succeeded":
            connection.execute(
                """
                UPDATE repair_attempts
                SET outcome = 'succeeded', failure_stage = NULL, failure_class = NULL,
                    retryable = FALSE
                WHERE job_id = ? AND attempt_number = 1
                """,
                [job.job_id],
            )
        else:
            connection.execute(
                """
                UPDATE repair_attempts
                SET retryable = FALSE
                WHERE job_id = ? AND attempt_number = 1
                """,
                [job.job_id],
            )

    assert store.repair_queue_snapshot().status == "unavailable"


def test_snapshot_rejects_overlapping_attempt_time_lineage(tmp_path: Path) -> None:
    path = tmp_path / "overlapping-lineage.duckdb"
    lock_path = tmp_path / "overlapping-lineage.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    create_two_retryable_failed_attempts(store, lock_path, job)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            UPDATE repair_attempts
            SET started_at = ?
            WHERE job_id = ? AND attempt_number = 2
            """,
            [NOW + timedelta(seconds=30), job.job_id],
        )

    assert store.repair_queue_snapshot().status == "unavailable"


def test_claim_validates_existing_full_lineage_before_mutation(tmp_path: Path) -> None:
    module = continuity_module()
    path = tmp_path / "pre-mutation-lineage.duckdb"
    lock_path = tmp_path / "pre-mutation-lineage.lock"
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
        retry_wait = store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(job.trade_date, attempt=1),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
        )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE repair_attempts SET retryable = FALSE WHERE job_id = ?",
            [job.job_id],
        )
        before_jobs = connection.execute("SELECT * FROM repair_jobs").fetchall()
        before_attempts = connection.execute("SELECT * FROM repair_attempts").fetchall()
    assert retry_wait.next_attempt_at is not None

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.claim_repair_job(
                job.job_id,
                owner="worker-b",
                expected_version=retry_wait.state_version,
                now=retry_wait.next_attempt_at,
                lease_seconds=1800,
            )

    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("SELECT * FROM repair_jobs").fetchall() == before_jobs
        assert connection.execute("SELECT * FROM repair_attempts").fetchall() == before_attempts


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
    assert snapshot.jobs[1].abandoned_attempt_count == 1
    assert snapshot.attempts[0].outcome == "abandoned"
    assert snapshot.attempts[0].retryable is True
    assert snapshot.attempts[0].refresh_run_id is None
    assert snapshot.attempts[0].completed_at == NOW + timedelta(minutes=2)
    assert snapshot.jobs[1].updated_at == snapshot.attempts[0].completed_at
    with RefreshRunLock(lock_path):
        assert (
            store.reconcile_published_repair_jobs(
                [leased_job.trade_date], now=NOW + timedelta(minutes=3)
            )
            == []
        )
    assert store.repair_queue_snapshot() == snapshot


def test_succeeded_attempt_requires_internal_refresh_audit_reference() -> None:
    module = continuity_module()

    with pytest.raises(ValidationError):
        module.RepairAttempt(
            attempt_id="attempt-a",
            job_id=module.repair_job_id(date(2026, 8, 10), UNIVERSE_ID),
            attempt_number=1,
            lease_id="lease-a",
            lease_owner="worker-a",
            outcome="succeeded",
            retryable=False,
            started_at=NOW,
            completed_at=NOW + timedelta(minutes=1),
        )


@pytest.mark.parametrize("failed_state", ["retry_wait", "dead_letter"])
def test_manifest_reconciliation_publishes_failed_job_without_rewriting_attempt(
    tmp_path: Path,
    failed_state: str,
) -> None:
    module = continuity_module()
    path = tmp_path / f"{failed_state}.duckdb"
    lock_path = tmp_path / f"{failed_state}.lock"
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
        failed = store.finalize_repair_attempt(
            lease,
            outcome="failed",
            refresh_result=error_refresh(
                job.trade_date,
                attempt=1,
                retryable=failed_state == "retry_wait",
            ),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
        )
    assert failed.state == failed_state
    before_attempt = store.repair_queue_snapshot().attempts[0]

    with RefreshRunLock(lock_path):
        reconciled = store.reconcile_published_repair_jobs(
            [job.trade_date], now=NOW + timedelta(minutes=2)
        )
        repeated = store.reconcile_published_repair_jobs(
            [job.trade_date], now=NOW + timedelta(minutes=3)
        )

    snapshot = store.repair_queue_snapshot()
    assert [item.state for item in reconciled] == ["published"]
    assert repeated == []
    assert snapshot.status == "ready"
    assert snapshot.jobs[0].state == "published"
    assert snapshot.jobs[0].last_failure_stage is None
    assert snapshot.jobs[0].last_failure_class is None
    assert snapshot.attempts == (before_attempt,)
    assert snapshot.attempts[0].completed_at is not None
    assert snapshot.attempts[0].completed_at <= snapshot.jobs[0].updated_at


@pytest.mark.parametrize(
    ("corruption", "mutation"),
    [
        (corruption, mutation)
        for corruption in (
            "leased_transition_mismatch",
            "terminal_after_job_update",
            "published_transition_mismatch",
        )
        for mutation in ("enqueue", "claim", "reconcile", "finalize", "reap")
    ],
)
def test_time_corrupt_snapshot_blocks_every_mutation_without_writes(
    tmp_path: Path,
    corruption: str,
    mutation: str,
) -> None:
    module = continuity_module()
    path = tmp_path / f"{corruption}-{mutation}.duckdb"
    lock_path = tmp_path / f"{corruption}-{mutation}.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    terminal_job, active_job, pending_job = enqueue(
        store,
        lock_path,
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 12),
    )
    policy = module.RepairRetryPolicy(max_attempts=4, base_seconds=3600)
    first_claimed_at = NOW + timedelta(minutes=1)
    with RefreshRunLock(lock_path):
        first_lease = store.claim_repair_job(
            terminal_job.job_id,
            owner="worker-a",
            expected_version=terminal_job.state_version,
            now=first_claimed_at,
            lease_seconds=1800,
        )
        assert first_lease is not None
        terminal_job = store.finalize_repair_attempt(
            first_lease,
            outcome="failed",
            refresh_result=error_refresh(
                terminal_job.trade_date,
                attempt=1,
                started_at=first_claimed_at,
            ),
            now=first_claimed_at + timedelta(minutes=1),
            retry_policy=policy,
        )
        active_lease = store.claim_repair_job(
            active_job.job_id,
            owner="worker-b",
            expected_version=active_job.state_version,
            now=first_claimed_at + timedelta(minutes=2),
            lease_seconds=1800,
        )
    assert active_lease is not None
    with duckdb.connect(str(path)) as connection:
        if corruption == "leased_transition_mismatch":
            connection.execute(
                "UPDATE repair_jobs SET updated_at = updated_at + INTERVAL 1 SECOND "
                "WHERE job_id = ?",
                [active_job.job_id],
            )
        elif corruption == "terminal_after_job_update":
            connection.execute(
                "UPDATE repair_attempts SET completed_at = completed_at + INTERVAL 1 SECOND "
                "WHERE job_id = ?",
                [terminal_job.job_id],
            )
        else:
            connection.execute(
                "UPDATE repair_jobs SET state = 'published', next_attempt_at = NULL, "
                "last_failure_stage = NULL, last_failure_class = NULL, "
                "published_at = updated_at + INTERVAL 1 SECOND WHERE job_id = ?",
                [terminal_job.job_id],
            )
        before_jobs = connection.execute("SELECT * FROM repair_jobs ORDER BY job_id").fetchall()
        before_attempts = connection.execute(
            "SELECT * FROM repair_attempts ORDER BY attempt_id"
        ).fetchall()
    before_bytes = path.read_bytes()
    mutation_at = first_claimed_at + timedelta(minutes=4)

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            if mutation == "enqueue":
                store.enqueue_repair_jobs(
                    [date(2026, 8, 13)], universe_id=UNIVERSE_ID, now=mutation_at
                )
            elif mutation == "claim":
                store.claim_repair_job(
                    pending_job.job_id,
                    owner="worker-c",
                    expected_version=pending_job.state_version,
                    now=mutation_at,
                    lease_seconds=1800,
                )
            elif mutation == "reconcile":
                store.reconcile_published_repair_jobs([pending_job.trade_date], now=mutation_at)
            elif mutation == "finalize":
                store.finalize_repair_attempt(
                    active_lease,
                    outcome="failed",
                    refresh_result=error_refresh(
                        active_job.trade_date,
                        attempt=1,
                        started_at=first_claimed_at + timedelta(minutes=2, seconds=2),
                    ),
                    now=mutation_at,
                    retry_policy=policy,
                )
            else:
                store.reap_expired_repair_leases(
                    now=active_lease.expires_at + timedelta(seconds=1),
                    retry_policy=policy,
                )

    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("SELECT * FROM repair_jobs ORDER BY job_id").fetchall() == (
            before_jobs
        )
        assert (
            connection.execute("SELECT * FROM repair_attempts ORDER BY attempt_id").fetchall()
            == before_attempts
        )
    assert path.read_bytes() == before_bytes


@pytest.mark.parametrize("corruption", ["attempt_before_job", "invalid_lease_expiry"])
def test_corrupt_queue_time_lineage_blocks_reap_without_mutation(
    tmp_path: Path,
    corruption: str,
) -> None:
    module = continuity_module()
    path = tmp_path / f"{corruption}.duckdb"
    lock_path = tmp_path / f"{corruption}.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 10))[0]
    claimed_at = NOW + timedelta(minutes=1)
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="worker-a",
            expected_version=job.state_version,
            now=claimed_at,
            lease_seconds=60,
        )
    assert lease is not None
    with duckdb.connect(str(path)) as connection:
        if corruption == "attempt_before_job":
            connection.execute(
                "UPDATE repair_attempts SET started_at = ? WHERE job_id = ?",
                [NOW - timedelta(seconds=1), job.job_id],
            )
        else:
            connection.execute(
                "UPDATE repair_jobs SET lease_expires_at = updated_at WHERE job_id = ?",
                [job.job_id],
            )
        before_jobs = connection.execute("SELECT * FROM repair_jobs").fetchall()
        before_attempts = connection.execute("SELECT * FROM repair_attempts").fetchall()
    before_bytes = path.read_bytes()

    with RefreshRunLock(lock_path):
        with pytest.raises(module.RepairQueueError):
            store.reap_expired_repair_leases(
                now=claimed_at + timedelta(seconds=61),
                retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=3600),
            )

    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("SELECT * FROM repair_jobs").fetchall() == before_jobs
        assert connection.execute("SELECT * FROM repair_attempts").fetchall() == before_attempts
    assert path.read_bytes() == before_bytes


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
