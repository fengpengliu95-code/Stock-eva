import hashlib
import importlib
import json
import multiprocessing
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from pydantic import ValidationError

import backend.app.market.store as market_store_module
from backend.app.config import Settings
from backend.app.market.automation import RefreshAlreadyRunning, RefreshRunLock
from backend.app.market.calendar import CalendarConfig, TradingCalendar
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.provider_health import CircuitState, ProviderHealthError
from backend.app.market.store import MarketStore, MarketStoreReadError
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.models import VerifiedReadySessionInventory

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
        request_key=f"repair:baostock:{trade_date}:all-main-board",
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
        request_key=f"repair:baostock:{trade_date}:all-main-board",
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


def continuity_bar(trade_date: date) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        symbol="sh.600000",
        security_type="stock",
        exchange="sh",
        board="main",
        open=10,
        high=11,
        low=9,
        close=10.5,
        preclose=10,
        volume=100,
        amount=1000,
        turnover_rate=1,
        pct_change=5,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id=f"baostock:{trade_date}:sh.600000",
        ingested_at=NOW,
        quality_status="ready",
    )


def file_fingerprint(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def tree_fingerprint(root: Path) -> tuple[tuple[str, str, int, int], ...]:
    entries: list[tuple[str, str, int, int]] = []
    for path in sorted(root.rglob("*")):
        stat = path.stat()
        relative = str(path.relative_to(root))
        if path.is_dir():
            entries.append((relative, "directory", 0, stat.st_mtime_ns))
        else:
            entries.append(
                (
                    relative,
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                    stat.st_size,
                    stat.st_mtime_ns,
                )
            )
    return tuple(entries)


class RecordingInventoryReader:
    def __init__(
        self,
        inventory: VerifiedReadySessionInventory | None = None,
        *,
        failure: Exception | None = None,
    ) -> None:
        self.inventory = inventory
        self.failure = failure
        self.calls: list[str] = []

    def verified_ready_session_inventory(
        self,
        source: str = "baostock",
    ) -> VerifiedReadySessionInventory:
        self.calls.append(source)
        if self.failure is not None:
            raise self.failure
        assert self.inventory is not None
        return self.inventory


class RecordingConfirmedCalendar:
    status = "confirmed"

    def __init__(self, statuses: dict[date, str]) -> None:
        self.statuses = statuses
        self.calls: list[tuple[date, date]] = []

    def confirmed_open_sessions(self, start: date, end: date) -> tuple[date, ...] | None:
        self.calls.append((start, end))
        current = start
        opened: list[date] = []
        while current <= end:
            status = self.statuses.get(current, "unknown")
            if status == "unknown":
                return None
            if status == "open":
                opened.append(current)
            current += timedelta(days=1)
        return tuple(opened)


class SnapshottingConfirmedCalendar:
    """Return a different immutable reader on each operation boundary."""

    def __init__(self, *snapshots) -> None:
        self.snapshots = iter(snapshots)
        self.snapshot_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        return next(self.snapshots)


def ready_inventory(*sessions: date) -> VerifiedReadySessionInventory:
    return ready_inventory_generation("generation-task3", *sessions)


def ready_inventory_generation(
    generation: str,
    *sessions: date,
) -> VerifiedReadySessionInventory:
    return VerifiedReadySessionInventory(
        manifest_generation=generation,
        manifest_identity="a" * 64,
        sessions=tuple(sorted(sessions)),
        verified_at=NOW,
    )


class HookedRefreshLock:
    def __init__(self, path: Path, hook) -> None:
        self._lock = RefreshRunLock(path)
        self._hook = hook

    def __enter__(self):
        self._lock.__enter__()
        self._hook()
        return self

    def __exit__(self, *args) -> None:
        self._lock.__exit__(*args)


class HookedRefreshLockFactory:
    def __init__(self, hook) -> None:
        self._hook = hook
        self.calls: list[Path] = []

    def __call__(self, path: Path) -> HookedRefreshLock:
        self.calls.append(path)
        return HookedRefreshLock(path, self._hook)


def precreate_lock(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


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


def test_scan_is_confirmed_open_minus_strict_noncontiguous_inventory() -> None:
    module = continuity_module()
    sessions = tuple(date(2026, 8, day) for day in range(3, 8))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    reader = RecordingInventoryReader(ready_inventory(sessions[0], sessions[2], sessions[4]))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )

    result = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )

    assert result == module.ContinuityScanResult(
        status="gaps",
        effective_start=sessions[0],
        effective_end=sessions[-1],
        manifest_generation="generation-task3",
        confirmed_open_sessions=sessions,
        published_ready_sessions=(sessions[0], sessions[2], sessions[4]),
        missing_sessions=(sessions[1], sessions[3]),
    )
    assert calendar.calls == [(sessions[0], sessions[-1])]
    assert reader.calls == ["baostock"]
    assert result.writes_control_state is False
    assert result.provider_requests == 0
    assert set(result.model_dump()) == {
        "status",
        "effective_start",
        "effective_end",
        "manifest_generation",
        "confirmed_open_sessions",
        "published_ready_sessions",
        "missing_sessions",
        "writes_control_state",
        "provider_requests",
    }
    with pytest.raises(ValidationError, match="frozen"):
        result.missing_sessions = ()
    with pytest.raises(ValidationError):
        module.ContinuityScanResult(**result.model_dump(), payload="forbidden")


def test_status_summary_is_sanitized_and_health_snapshot_only() -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open" for session in sessions}),
        inventory_reader=RecordingInventoryReader(ready_inventory(sessions[0])),
        inventory_mode="immutable_dataset",
    )
    pending = module.RepairJob(
        job_id="repair:2026-08-04:all-main-board",
        trade_date=sessions[1],
        universe_id=UNIVERSE_ID,
        state="pending",
        state_version=1,
        attempt_count=0,
        abandoned_attempt_count=0,
        created_at=NOW,
        updated_at=NOW,
    )
    queue = module.RepairQueueSnapshot(status="ready", jobs=(pending,), attempts=())
    summary = module.build_continuity_status_summary(
        scanner=scanner,
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        queue_snapshot=queue,
        repair_enabled=True,
        provider_health=SimpleNamespace(state="CLOSED"),
        freshness_state=SimpleNamespace(refresh_state="success"),
        now=NOW,
    )

    assert summary.continuity_status == "gaps"
    assert summary.missing_session_count == 1
    assert summary.oldest_missing_session == sessions[1]
    assert summary.repair_pending_count == 1
    assert summary.continuity_reason_code is None
    assert set(summary.model_dump()) == {
        "continuity_status",
        "continuity_start_date",
        "missing_session_count",
        "oldest_missing_session",
        "repair_execution_enabled",
        "repair_pending_count",
        "repair_retry_wait_count",
        "repair_active_count",
        "repair_dead_letter_count",
        "active_lane",
        "continuity_reason_code",
    }


def test_status_summary_disabled_repair_is_blocked_without_health_read() -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        inventory_mode="immutable_dataset",
    )
    queue = module.RepairQueueSnapshot(status="ready", jobs=(), attempts=())

    summary = module.build_continuity_status_summary(
        scanner=scanner,
        configured_start=session,
        latest_completed_session=session,
        queue_snapshot=queue,
        repair_enabled=False,
        provider_health=SimpleNamespace(state="OPEN"),
        freshness_state=None,
        now=NOW,
    )

    assert summary.continuity_status == "blocked"
    assert summary.continuity_reason_code == "REPAIR_EXECUTION_DISABLED"


@pytest.mark.parametrize(
    ("inventory_sessions", "expected_missing"),
    [
        ((date(2026, 8, 3),), ()),
        ((), (date(2026, 8, 3),)),
    ],
)
def test_status_summary_requires_readable_queue_for_current_and_gaps(
    inventory_sessions: tuple[date, ...],
    expected_missing: tuple[date, ...],
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory(*inventory_sessions)),
        inventory_mode="immutable_dataset",
    )
    unavailable = module.RepairQueueSnapshot(
        status="unavailable",
        reason_code="CONTROL_STATE_UNAVAILABLE",
    )

    summary = module.build_continuity_status_summary(
        scanner=scanner,
        configured_start=session,
        latest_completed_session=session,
        queue_snapshot=unavailable,
        repair_enabled=True,
        provider_health=None,
        freshness_state=None,
        now=NOW,
    )

    assert summary.continuity_status == "unavailable"
    assert summary.continuity_reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert summary.missing_session_count == len(expected_missing)
    assert summary.oldest_missing_session == (expected_missing[0] if expected_missing else None)


def test_status_summary_revalidates_bypassed_scan_before_reading_internal_fields() -> None:
    module = continuity_module()
    session = date(2026, 8, 3)

    class BypassedScanner:
        def scan(self, **_kwargs):
            # model_construct intentionally bypasses the frozen scan contract.  The public
            # summary must not dereference missing fields or expose arbitrary internals.
            return module.ContinuityScanResult.model_construct(
                status="gaps",
                effective_start=session,
                effective_end=session,
                manifest_generation="raw token=secret /private/manifest",
            )

    summary = module.build_continuity_status_summary(
        scanner=BypassedScanner(),
        configured_start=session,
        latest_completed_session=session,
        queue_snapshot=None,
        repair_enabled=True,
        provider_health=None,
        freshness_state=None,
        now=NOW,
    )

    assert summary.continuity_status == "unavailable"
    assert summary.continuity_reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert summary.missing_session_count == 0
    serialized = json.dumps(summary.model_dump(mode="json"))
    assert "token=secret" not in serialized
    assert "/private/manifest" not in serialized


def test_status_summary_revalidates_bypassed_queue_and_provider_snapshots() -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open" for session in sessions}),
        inventory_reader=RecordingInventoryReader(ready_inventory(sessions[0])),
        inventory_mode="immutable_dataset",
    )
    valid_job = module.RepairJob(
        job_id="repair:2026-08-04:all-main-board",
        trade_date=sessions[1],
        universe_id=UNIVERSE_ID,
        state="pending",
        state_version=1,
        attempt_count=0,
        abandoned_attempt_count=0,
        created_at=NOW,
        updated_at=NOW,
    )
    bypassed_job = module.RepairJob.model_construct(
        job_id=valid_job.job_id,
        universe_id=UNIVERSE_ID,
        state="pending",
    )
    bypassed_queue = module.RepairQueueSnapshot.model_construct(
        status="ready",
        jobs=(bypassed_job,),
        attempts=(),
    )
    unavailable_queue = module.build_continuity_status_summary(
        scanner=scanner,
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        queue_snapshot=bypassed_queue,
        repair_enabled=True,
        provider_health=None,
        freshness_state=None,
        now=NOW,
    )
    assert unavailable_queue.continuity_status == "unavailable"
    assert unavailable_queue.continuity_reason_code == "CONTROL_STATE_UNAVAILABLE"

    bypassed_provider = module.ProviderHealth.model_construct(
        state=CircuitState.OPEN,
    )
    unavailable_provider = module.build_continuity_status_summary(
        scanner=scanner,
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        queue_snapshot=module.RepairQueueSnapshot(status="ready", jobs=(valid_job,), attempts=()),
        repair_enabled=True,
        provider_health=bypassed_provider,
        freshness_state=None,
        now=NOW,
    )
    assert unavailable_provider.continuity_status == "unavailable"
    assert unavailable_provider.continuity_reason_code == "CONTROL_STATE_UNAVAILABLE"


def test_scan_uses_one_narrow_calendar_query_and_filters_out_of_range_ready_dates() -> None:
    module = continuity_module()
    configured = date(2026, 8, 3)
    requested_start = date(2026, 8, 4)
    requested_end = date(2026, 8, 6)
    latest = date(2026, 8, 7)
    calendar = RecordingConfirmedCalendar(
        {
            date(2026, 8, 4): "open",
            date(2026, 8, 5): "closed",
            date(2026, 8, 6): "open",
        }
    )
    reader = RecordingInventoryReader(
        ready_inventory(
            date(2026, 8, 3),
            date(2026, 8, 4),
            date(2026, 8, 5),
            date(2026, 8, 7),
        )
    )
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )

    result = scanner.scan(
        configured_start=configured,
        requested_start=requested_start,
        requested_end=requested_end,
        latest_completed_session=latest,
    )

    assert result.status == "gaps"
    assert result.effective_start == requested_start
    assert result.effective_end == requested_end
    assert result.confirmed_open_sessions == (date(2026, 8, 4), date(2026, 8, 6))
    assert result.published_ready_sessions == (date(2026, 8, 4), date(2026, 8, 5))
    assert result.missing_sessions == (date(2026, 8, 6),)
    assert calendar.calls == [(requested_start, requested_end)]
    assert reader.calls == ["baostock"]


def test_scan_is_current_when_every_confirmed_open_session_is_verified_ready() -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=RecordingInventoryReader(ready_inventory(*sessions)),
        inventory_mode="immutable_dataset",
    )

    result = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )

    assert result.status == "current"
    assert result.missing_sessions == ()


def test_scan_pins_one_calendar_snapshot_and_next_scan_sees_new_snapshot() -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    first = RecordingConfirmedCalendar({session: "open" for session in sessions})
    second = RecordingConfirmedCalendar({sessions[0]: "open", sessions[1]: "closed"})
    calendar = SnapshottingConfirmedCalendar(first, second)
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=RecordingInventoryReader(ready_inventory(sessions[0])),
        inventory_mode="immutable_dataset",
    )

    initial = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )
    later = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )

    assert initial.status == "gaps"
    assert initial.confirmed_open_sessions == sessions
    assert initial.missing_sessions == (sessions[1],)
    assert later.status == "current"
    assert later.confirmed_open_sessions == (sessions[0],)
    assert calendar.snapshot_calls == 2


def test_scan_unexpected_snapshot_failure_propagates_without_inventory_access() -> None:
    module = continuity_module()

    class FailingCalendar:
        def snapshot(self):
            raise RuntimeError("calendar snapshot unavailable")

        @property
        def status(self):
            raise AssertionError("must not read the live calendar after snapshot failure")

    reader = RecordingInventoryReader(ready_inventory(date(2026, 8, 3)))
    scanner = module.ContinuityInventory(
        calendar=FailingCalendar(),
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )

    with pytest.raises(RuntimeError, match="calendar snapshot unavailable"):
        scanner.scan(
            configured_start=date(2026, 8, 3),
            latest_completed_session=date(2026, 8, 3),
        )
    assert reader.calls == []


def test_unknown_calendar_day_rejects_the_entire_scan_before_inventory() -> None:
    module = continuity_module()
    sessions = tuple(date(2026, 8, day) for day in range(3, 6))
    calendar = RecordingConfirmedCalendar(
        {sessions[0]: "open", sessions[1]: "unknown", sessions[2]: "open"}
    )
    reader = RecordingInventoryReader(ready_inventory(sessions[0], sessions[2]))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )

    result = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )

    assert result == module.ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
    assert calendar.calls == [(sessions[0], sessions[-1])]
    assert reader.calls == []


@pytest.mark.parametrize(
    ("configured_start", "requested_start", "requested_end", "latest", "reason"),
    [
        (None, None, None, date(2026, 8, 7), "CONTINUITY_START_UNCONFIGURED"),
        (
            date(2026, 8, 8),
            None,
            None,
            date(2026, 8, 7),
            "CONTINUITY_RANGE_INVALID",
        ),
        (
            date(2026, 8, 3),
            date(2026, 8, 2),
            None,
            date(2026, 8, 7),
            "CONTINUITY_RANGE_INVALID",
        ),
        (
            date(2026, 8, 3),
            None,
            date(2026, 8, 8),
            date(2026, 8, 7),
            "CONTINUITY_RANGE_INVALID",
        ),
        (
            date(2026, 8, 3),
            date(2026, 8, 6),
            date(2026, 8, 5),
            date(2026, 8, 7),
            "CONTINUITY_RANGE_INVALID",
        ),
        (
            date(2026, 8, 3),
            None,
            None,
            None,
            "CONTINUITY_RANGE_INVALID",
        ),
    ],
)
def test_scan_rejects_unconfigured_or_expanded_ranges_before_any_reader(
    configured_start: date | None,
    requested_start: date | None,
    requested_end: date | None,
    latest: date | None,
    reason: str,
) -> None:
    module = continuity_module()
    calendar = RecordingConfirmedCalendar({})
    reader = RecordingInventoryReader(ready_inventory())
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )

    result = scanner.scan(
        configured_start=configured_start,
        requested_start=requested_start,
        requested_end=requested_end,
        latest_completed_session=latest,
    )

    assert result == module.ContinuityUnavailable(reason_code=reason)
    assert calendar.calls == []
    assert reader.calls == []


def test_unknown_calendar_day_rejects_complete_scan_without_writes(tmp_path: Path) -> None:
    module = continuity_module()
    calendar = TradingCalendar(
        [
            CalendarConfig(
                year=2026,
                status="confirmed",
                published_on=date(2025, 12, 31),
                sources=[],
                closed_dates=[],
            )
        ]
    )
    reader = RecordingInventoryReader(ready_inventory(date(2026, 12, 31)))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )
    (tmp_path / "control.duckdb").write_bytes(b"unchanged-control")
    (tmp_path / "pointer.json").write_bytes(b"unchanged-pointer")
    before = tree_fingerprint(tmp_path)

    result = scanner.scan(
        configured_start=date(2026, 12, 31),
        latest_completed_session=date(2027, 1, 4),
    )

    assert result == module.ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
    assert reader.calls == []
    assert tree_fingerprint(tmp_path) == before


def test_calendar_conflict_rejects_complete_scan_before_calendar_or_inventory() -> None:
    module = continuity_module()
    calendar = RecordingConfirmedCalendar({date(2026, 8, 3): "open"})
    reader = RecordingInventoryReader(ready_inventory())
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=True,
    )

    result = scanner.scan(
        configured_start=date(2026, 8, 3),
        latest_completed_session=date(2026, 8, 3),
    )

    assert result == module.ContinuityUnavailable(reason_code="CALENDAR_CONFLICT")
    assert calendar.calls == []
    assert reader.calls == []


def test_local_mutable_inventory_is_unavailable_and_never_read() -> None:
    module = continuity_module()
    calendar = RecordingConfirmedCalendar({date(2026, 8, 3): "open"})
    reader = RecordingInventoryReader(
        failure=AssertionError("local mutable history must not be read")
    )
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="local_mutable",
    )

    result = scanner.scan(
        configured_start=date(2026, 8, 3),
        latest_completed_session=date(2026, 8, 3),
    )

    assert result == module.ContinuityUnavailable(reason_code="MANIFEST_INVENTORY_UNAVAILABLE")
    assert calendar.calls == []
    assert reader.calls == []


def test_inventory_failure_is_sanitized_and_preserves_full_tree(tmp_path: Path) -> None:
    module = continuity_module()
    raw_error = "token=secret url=https://provider.invalid /private/raw-manifest.json"
    calendar = RecordingConfirmedCalendar({date(2026, 8, 3): "open"})
    reader = RecordingInventoryReader(failure=RuntimeError(raw_error))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )
    (tmp_path / "market.duckdb").write_bytes(b"unchanged")
    before = tree_fingerprint(tmp_path)

    result = scanner.scan(
        configured_start=date(2026, 8, 3),
        latest_completed_session=date(2026, 8, 3),
    )

    assert result == module.ContinuityUnavailable(reason_code="MANIFEST_INVENTORY_UNAVAILABLE")
    payload = result.model_dump(mode="json")
    assert set(payload) == {"status", "reason_code", "writes_control_state", "provider_requests"}
    assert raw_error not in str(payload)
    assert "token" not in payload
    assert "url" not in payload
    assert "path" not in payload
    assert "exception" not in payload
    assert calendar.calls == [(date(2026, 8, 3), date(2026, 8, 3))]
    assert reader.calls == ["baostock"]
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_service_repeats_scan_inside_lock_and_ignores_stale_gap_result(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1", sessions[0]))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    stale = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )
    assert stale.status == "gaps"

    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    lock_factory = HookedRefreshLockFactory(
        lambda: setattr(
            reader,
            "inventory",
            ready_inventory_generation("generation-g2", *sessions),
        )
    )
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lock_factory,
    )

    result = service.execute(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        completed_scan=stale,
    )

    assert result.status == "current"
    assert result.reason_code is None
    assert result.fresh_scan is not None
    assert result.fresh_scan.status == "current"
    assert result.manifest_generation == "generation-g2"
    assert (
        result.scan_identity != hashlib.sha256(stale.model_dump_json().encode("utf-8")).hexdigest()
    )
    assert len(result.scan_identity) == 64
    assert result.created_jobs == ()
    assert result.created_count == 0
    assert result.existing_count == 0
    assert result.writes_control_state is True
    assert result.provider_requests == 0
    assert lock_factory.calls == [lock_path]
    assert reader.calls == ["baostock", "baostock", "baostock"]
    assert store.repair_queue_snapshot().jobs == ()


@pytest.mark.parametrize(
    ("transition", "reason"),
    [
        ("unknown_calendar", "CALENDAR_UNAVAILABLE"),
        ("calendar_conflict", "CALENDAR_CONFLICT"),
        ("corrupt_inventory", "MANIFEST_INVENTORY_UNAVAILABLE"),
    ],
)
def test_enqueue_service_rechecks_evidence_under_lock_before_schema_or_database_write(
    tmp_path: Path,
    monkeypatch,
    transition: str,
    reason: str,
) -> None:
    module = continuity_module()
    sessions = (date(2026, 8, 3), date(2026, 8, 4))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    conflict = {"detected": False}
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: conflict["detected"],
    )
    stale = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )
    assert stale.status == "gaps"

    def change_evidence() -> None:
        if transition == "unknown_calendar":
            calendar.statuses[sessions[-1]] = "unknown"
        elif transition == "calendar_conflict":
            conflict["detected"] = True
        else:
            reader.failure = RuntimeError(
                "token=secret url=https://provider.invalid /private/object.parquet"
            )

    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    schema_calls = 0

    def reject_schema_initialization() -> None:
        nonlocal schema_calls
        schema_calls += 1
        raise AssertionError("invalid fresh evidence must not initialize schema")

    monkeypatch.setattr(store, "initialize_continuity_schema", reject_schema_initialization)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=HookedRefreshLockFactory(change_evidence),
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        completed_scan=stale,
    )

    assert result.status == "unavailable"
    assert result.reason_code == reason
    assert result.fresh_scan is None
    assert result.manifest_generation is None
    assert result.created_count == 0
    assert result.existing_count == 0
    assert result.writes_control_state is False
    assert result.provider_requests == 0
    assert schema_calls == 0
    assert store.path.exists() is False
    assert tree_fingerprint(tmp_path) == before
    payload = result.model_dump(mode="json")
    assert not {"payload", "token", "url", "path", "exception", "error"} & set(payload)


@pytest.mark.parametrize("malformed_kind", ["generation", "status", "dates", "reason"])
def test_enqueue_service_revalidates_bypassed_fresh_scan_before_control_write(
    tmp_path: Path,
    malformed_kind: str,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    valid_scan = module.ContinuityScanResult(
        status="current",
        effective_start=session,
        effective_end=session,
        manifest_generation="generation-g1",
        confirmed_open_sessions=(session,),
        published_ready_sessions=(session,),
        missing_sessions=(),
    )
    if malformed_kind == "generation":
        payload = {**valid_scan.model_dump(), "manifest_generation": "bad token /private"}
        malformed = module.ContinuityScanResult.model_construct(
            **payload,
        )
    elif malformed_kind == "status":
        payload = {**valid_scan.model_dump(), "status": "unknown"}
        malformed = module.ContinuityScanResult.model_construct(
            **payload,
        )
    elif malformed_kind == "dates":
        payload = {**valid_scan.model_dump(), "effective_start": "not-a-date"}
        malformed = module.ContinuityScanResult.model_construct(
            **payload,
        )
    else:
        malformed = module.ContinuityUnavailable.model_construct(reason_code="UNKNOWN_REASON")

    class BypassedScanner:
        calendar_conflict_is_dynamic = True

        def __init__(self) -> None:
            self.calls = 0

        def scan(self, **_kwargs):
            self.calls += 1
            return valid_scan if self.calls == 1 else malformed

    scanner = BypassedScanner()
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    before = tree_fingerprint(tmp_path)
    store_factory_calls = 0

    def store_factory():
        nonlocal store_factory_calls
        store_factory_calls += 1
        raise AssertionError("invalid fresh scan must not construct the queue store")

    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store_factory=store_factory,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.fresh_scan is None
    assert result.writes_control_state is False
    assert result.provider_requests == 0
    assert store_factory_calls == 0
    assert scanner.calls == 2
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_service_rechecks_changed_latest_boundary_before_any_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    sessions = tuple(date(2026, 8, day) for day in range(3, 6))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    stale = scanner.scan(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )
    assert stale.status == "gaps"
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    schema_calls = 0

    def reject_schema_initialization() -> None:
        nonlocal schema_calls
        schema_calls += 1

    monkeypatch.setattr(store, "initialize_continuity_schema", reject_schema_initialization)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=sessions[0],
        requested_start=sessions[-1],
        latest_completed_session=sessions[-2],
        completed_scan=stale,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTINUITY_RANGE_INVALID"
    assert schema_calls == 0
    assert reader.calls == ["baostock"]
    assert store.path.exists() is False
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_service_corrupt_object_while_waiting_fails_before_control_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    dataset = NasMarketStore(control, dataset_root, tmp_path / "staging")
    dataset.save_refresh(
        [continuity_bar(session)],
        ready_refresh(session),
        publish=True,
    )
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=dataset,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    stale = scanner.scan(
        configured_start=session,
        latest_completed_session=session,
    )
    assert stale.status == "current"
    manifest = json.loads((dataset_root / "manifest.json").read_text(encoding="utf-8"))
    published = dataset_root / manifest["files"][0]["path"]
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    after_corruption: tuple[tuple[str, str, int, int], ...] | None = None

    def corrupt_published_object() -> None:
        nonlocal after_corruption
        published.write_bytes(b"corrupt-while-waiting")
        after_corruption = tree_fingerprint(tmp_path)

    schema_calls = 0

    def reject_schema_initialization() -> None:
        nonlocal schema_calls
        schema_calls += 1
        raise AssertionError("corrupt object must block schema initialization")

    monkeypatch.setattr(control, "initialize_continuity_schema", reject_schema_initialization)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=control,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=HookedRefreshLockFactory(corrupt_published_object),
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
        completed_scan=stale,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "MANIFEST_INVENTORY_UNAVAILABLE"
    assert schema_calls == 0
    assert after_corruption is not None
    assert tree_fingerprint(tmp_path) == after_corruption


def test_enqueue_service_busy_lock_is_typed_sanitized_and_full_tree_unchanged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    calendar = RecordingConfirmedCalendar({session: "open"})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    schema_calls = 0

    def reject_schema_initialization() -> None:
        nonlocal schema_calls
        schema_calls += 1

    monkeypatch.setattr(store, "initialize_continuity_schema", reject_schema_initialization)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
    )

    with RefreshRunLock(lock_path):
        before = tree_fingerprint(tmp_path)
        result = service.execute(
            configured_start=session,
            latest_completed_session=session,
        )
        assert tree_fingerprint(tmp_path) == before

    assert result.status == "already_running"
    assert result.reason_code == "REFRESH_ALREADY_RUNNING"
    assert result.fresh_scan is None
    assert result.created_count == 0
    assert result.existing_count == 0
    assert result.writes_control_state is False
    assert result.provider_requests == 0
    assert reader.calls == ["baostock"]
    assert schema_calls == 0
    assert store.path.exists() is False
    with pytest.raises(ValidationError):
        module.ContinuityEnqueueResult(**result.model_dump(), path="/private/lock")


def test_enqueue_service_fresh_gaps_initialize_and_enqueue_oldest_first_idempotently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    import backend.app.market.baostock as baostock_module

    sessions = tuple(date(2026, 8, day) for day in range(3, 6))
    calendar = RecordingConfirmedCalendar({session: "open" for session in sessions})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1", sessions[1]))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    provider_constructions = 0

    def reject_provider(*args, **kwargs):
        nonlocal provider_constructions
        provider_constructions += 1
        raise AssertionError("continuity enqueue must not construct a provider")

    monkeypatch.setattr(baostock_module, "BaoStockProvider", reject_provider)
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
    )

    first = service.execute(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
    )
    second = service.execute(
        configured_start=sessions[0],
        latest_completed_session=sessions[-1],
        completed_scan=first.fresh_scan,
    )

    expected_missing = (sessions[0], sessions[2])
    assert first.status == "enqueued"
    assert first.reason_code is None
    assert first.fresh_scan is not None
    assert first.fresh_scan.missing_sessions == expected_missing
    assert first.manifest_generation == "generation-g1"
    assert tuple(job.trade_date for job in first.created_jobs) == expected_missing
    assert first.created_count == 2
    assert first.existing_count == 0
    assert first.writes_control_state is True
    assert second.status == "enqueued"
    assert second.scan_identity == first.scan_identity
    assert second.created_jobs == ()
    assert second.created_count == 0
    assert second.existing_count == 2
    snapshot = store.repair_queue_snapshot()
    assert tuple(job.trade_date for job in snapshot.jobs) == expected_missing
    assert snapshot.attempts == ()
    assert reader.calls == ["baostock", "baostock", "baostock", "baostock"]
    assert provider_constructions == 0
    assert first.provider_requests == second.provider_requests == 0


def test_enqueue_service_invalid_clock_fails_closed_before_schema_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory_generation("generation-g1")),
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    schema_calls = 0

    def reject_schema_initialization() -> None:
        nonlocal schema_calls
        schema_calls += 1

    monkeypatch.setattr(store, "initialize_continuity_schema", reject_schema_initialization)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW.replace(tzinfo=None),
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert schema_calls == 0
    assert store.path.exists() is False
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_service_rejects_bypassed_completed_scan_without_lock_or_write(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    calendar = RecordingConfirmedCalendar({session: "open"})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )
    completed = scanner.scan(
        configured_start=session,
        latest_completed_session=session,
    )
    bypassed = completed.model_copy(update={"missing_sessions": (), "status": "current"})
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    lock_factory = HookedRefreshLockFactory(
        lambda: pytest.fail("invalid completed scan must be rejected before locking")
    )
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lock_factory,
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
        completed_scan=bypassed,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert lock_factory.calls == []
    assert store.path.exists() is False
    assert tree_fingerprint(tmp_path) == before


@pytest.mark.parametrize("malformed_kind", ["generation", "status", "date", "reason", "copy"])
def test_enqueue_service_rejects_invalid_completed_scan_as_control_unavailable(
    tmp_path: Path,
    malformed_kind: str,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    valid_scan = module.ContinuityScanResult(
        status="current",
        effective_start=session,
        effective_end=session,
        manifest_generation="generation-g1",
        confirmed_open_sessions=(session,),
        published_ready_sessions=(session,),
        missing_sessions=(),
    )
    if malformed_kind == "generation":
        completed = module.ContinuityScanResult.model_construct(
            **{**valid_scan.model_dump(), "manifest_generation": "bad token /private"},
        )
    elif malformed_kind == "status":
        completed = module.ContinuityScanResult.model_construct(
            **{**valid_scan.model_dump(), "status": "unknown"},
        )
    elif malformed_kind == "date":
        completed = module.ContinuityScanResult.model_construct(
            **{**valid_scan.model_dump(), "effective_start": "not-a-date"},
        )
    elif malformed_kind == "reason":
        completed = module.ContinuityUnavailable.model_construct(reason_code="UNKNOWN_REASON")
    else:
        completed = valid_scan.model_copy(update={"missing_sessions": (), "status": "gaps"})

    calendar = RecordingConfirmedCalendar({session: "open"})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
    )
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    precreate_lock(lock_path)
    lock_factory = HookedRefreshLockFactory(
        lambda: pytest.fail("invalid completed scan must be rejected before locking")
    )
    store_factory_calls = 0

    def store_factory():
        nonlocal store_factory_calls
        store_factory_calls += 1
        raise AssertionError("invalid completed scan must not construct the queue store")

    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store_factory=store_factory,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lock_factory,
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
        completed_scan=completed,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.writes_control_state is False
    assert result.provider_requests == 0
    assert lock_factory.calls == []
    assert store_factory_calls == 0
    assert calendar.calls == []
    assert reader.calls == []
    assert tree_fingerprint(tmp_path) == before


@pytest.mark.parametrize(
    "preflight_failure",
    [
        "invalid_bounds",
        "unknown_calendar",
        "calendar_conflict",
        "corrupt_inventory",
        "local_mutable",
    ],
)
def test_enqueue_preflight_failure_never_resolves_or_touches_lock_path(
    tmp_path: Path,
    preflight_failure: str,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    calendar = RecordingConfirmedCalendar({session: "open"})
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    conflict = {"detected": preflight_failure == "calendar_conflict"}
    scanner = module.ContinuityInventory(
        calendar=calendar,
        inventory_reader=reader,
        inventory_mode=(
            "local_mutable" if preflight_failure == "local_mutable" else "immutable_dataset"
        ),
        calendar_conflict=lambda: conflict["detected"],
    )
    if preflight_failure == "unknown_calendar":
        calendar.statuses[session] = "unknown"
    elif preflight_failure == "corrupt_inventory":
        reader.failure = RuntimeError(
            "token=secret url=https://provider.invalid /private/object.parquet"
        )
    lock_path = tmp_path / "runtime" / "locks" / "market-refresh.lock"
    store = MarketStore(tmp_path / "runtime" / "control" / "market.duckdb")
    lock_calls = 0

    def reject_lock_resolution(path: Path):
        nonlocal lock_calls
        lock_calls += 1
        raise AssertionError("failed pure preflight must not resolve a lock")

    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=reject_lock_resolution,
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=session,
        requested_start=(
            session + timedelta(days=1) if preflight_failure == "invalid_bounds" else None
        ),
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code in {
        "CONTINUITY_RANGE_INVALID",
        "CALENDAR_UNAVAILABLE",
        "CALENDAR_CONFLICT",
        "MANIFEST_INVENTORY_UNAVAILABLE",
    }
    assert lock_calls == 0
    assert lock_path.exists() is False
    assert lock_path.parent.exists() is False
    assert store.path.exists() is False
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_invalid_clock_precedes_preflight_and_leaves_empty_root_untouched(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    lock_path = tmp_path / "runtime" / "locks" / "market-refresh.lock"
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=MarketStore(tmp_path / "runtime" / "control" / "market.duckdb"),
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW.replace(tzinfo=None),
        lock_factory=lambda path: pytest.fail("invalid clock must precede lock resolution"),
    )
    before = tree_fingerprint(tmp_path)

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert reader.calls == []
    assert lock_path.parent.exists() is False
    assert tree_fingerprint(tmp_path) == before


def test_enqueue_rejects_static_conflict_source_before_lock_or_inventory_read(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=False,
    )
    lock_path = tmp_path / "runtime" / "locks" / "market-refresh.lock"
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=MarketStore(tmp_path / "runtime" / "control" / "market.duckdb"),
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lambda path: pytest.fail("static conflict source must not reach lock"),
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert scanner.calendar_conflict_is_dynamic is False
    assert result.status == "unavailable"
    assert result.reason_code == "CALENDAR_CONFLICT_SOURCE_UNAVAILABLE"
    assert reader.calls == []
    assert lock_path.parent.exists() is False
    assert tree_fingerprint(tmp_path) == ()


def test_enqueue_dynamic_conflict_is_read_once_per_scan_and_changes_while_waiting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    conflict = {"detected": False, "calls": 0}

    def conflict_reader() -> bool:
        conflict["calls"] += 1
        return bool(conflict["detected"])

    reader = RecordingInventoryReader(ready_inventory_generation("generation-g1"))
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=reader,
        inventory_mode="immutable_dataset",
        calendar_conflict=conflict_reader,
    )
    lock_path = tmp_path / "locks" / "market-refresh.lock"
    store = MarketStore(tmp_path / "control" / "market.duckdb")
    schema_calls = 0

    def reject_schema() -> None:
        nonlocal schema_calls
        schema_calls += 1

    monkeypatch.setattr(store, "initialize_continuity_schema", reject_schema)
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=HookedRefreshLockFactory(lambda: conflict.__setitem__("detected", True)),
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert scanner.calendar_conflict_is_dynamic is True
    assert result.status == "unavailable"
    assert result.reason_code == "CALENDAR_CONFLICT"
    assert conflict["calls"] == 2
    assert reader.calls == ["baostock"]
    assert schema_calls == 0
    assert store.path.exists() is False
    assert lock_path.exists() is True


def test_enqueue_dynamic_conflict_reader_exception_is_sanitized_before_lock(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)

    def broken_conflict_reader() -> bool:
        raise RuntimeError("token=secret url=https://provider.invalid /private/calendar")

    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory_generation("generation-g1")),
        inventory_mode="immutable_dataset",
        calendar_conflict=broken_conflict_reader,
    )
    lock_path = tmp_path / "runtime" / "locks" / "market-refresh.lock"
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=MarketStore(tmp_path / "runtime" / "control" / "market.duckdb"),
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lambda path: pytest.fail("broken conflict reader must not reach lock"),
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert "token" not in result.model_dump_json()
    assert lock_path.parent.exists() is False


@pytest.mark.parametrize("failure_stage", ["resolution", "construction", "acquire"])
def test_enqueue_lock_boundary_exceptions_are_typed_and_sanitized(
    tmp_path: Path,
    monkeypatch,
    failure_stage: str,
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory_generation("generation-g1")),
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )
    raw = "token=secret url=https://provider.invalid /private/lock"

    class BrokenAcquire:
        def __enter__(self):
            raise RuntimeError(raw)

        def __exit__(self, *args):
            return None

    if failure_stage == "resolution":
        monkeypatch.setattr(
            module.ContinuityEnqueueService,
            "_resolve_default_lock",
            staticmethod(lambda: (_ for _ in ()).throw(RuntimeError(raw))),
        )
        lock_factory = None
    elif failure_stage == "construction":

        def lock_factory(path: Path):
            raise RuntimeError(raw)
    else:

        def lock_factory(path: Path):
            return BrokenAcquire()

    lock_path = tmp_path / "runtime" / "locks" / "market-refresh.lock"
    store = MarketStore(tmp_path / "runtime" / "control" / "market.duckdb")
    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=store,
        lock_path=lock_path,
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=lock_factory,
    )

    result = service.execute(
        configured_start=session,
        latest_completed_session=session,
    )

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert raw not in result.model_dump_json()
    assert store.path.exists() is False


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_enqueue_lock_boundary_does_not_swallow_base_exceptions(
    tmp_path: Path,
    interrupt: type[BaseException],
) -> None:
    module = continuity_module()
    session = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({session: "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory_generation("generation-g1")),
        inventory_mode="immutable_dataset",
        calendar_conflict=lambda: False,
    )

    def interrupted_lock_factory(path: Path):
        raise interrupt()

    service = module.ContinuityEnqueueService(
        scanner=scanner,
        store=MarketStore(tmp_path / "control" / "market.duckdb"),
        lock_path=tmp_path / "locks" / "market-refresh.lock",
        universe_id=UNIVERSE_ID,
        clock=lambda: NOW,
        lock_factory=interrupted_lock_factory,
    )

    with pytest.raises(interrupt):
        service.execute(
            configured_start=session,
            latest_completed_session=session,
        )


def continuity_policy_job(
    trade_date: date,
    *,
    state: str = "pending",
    next_attempt_at: datetime | None = None,
) -> object:
    module = continuity_module()
    attempted = state != "pending"
    return module.RepairJob(
        job_id=f"repair:{trade_date}:{UNIVERSE_ID}",
        trade_date=trade_date,
        universe_id=UNIVERSE_ID,
        state=state,
        state_version=2 if attempted else 1,
        attempt_count=1 if attempted else 0,
        abandoned_attempt_count=0,
        next_attempt_at=next_attempt_at,
        last_attempt_id="attempt-1" if attempted else None,
        last_failure_stage="fetch" if attempted else None,
        last_failure_class="transport_timeout" if attempted else None,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.mark.parametrize(
    ("freshness", "jobs", "enabled", "expected"),
    [
        (
            {
                "action": "run",
                "target_session": date(2026, 8, 13),
                "refresh_state": "running",
            },
            (date(2026, 8, 3),),
            True,
            ("run", "freshness", date(2026, 8, 13), None, "FRESHNESS_DUE"),
        ),
        (
            {
                "action": "none",
                "target_session": date(2026, 8, 13),
                "refresh_state": "success",
            },
            (date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)),
            True,
            (
                "run",
                "repair",
                date(2026, 8, 3),
                f"repair:2026-08-03:{UNIVERSE_ID}",
                "REPAIR_READY",
            ),
        ),
        (
            {
                "action": "wait",
                "target_session": date(2026, 8, 13),
                "refresh_state": "retry_wait",
                "next_run_at": NOW + timedelta(hours=1),
            },
            (date(2026, 8, 3),),
            True,
            (
                "run",
                "repair",
                date(2026, 8, 3),
                f"repair:2026-08-03:{UNIVERSE_ID}",
                "REPAIR_READY",
            ),
        ),
        (
            {
                "action": "run",
                "target_session": date(2026, 8, 13),
                "refresh_state": "running",
            },
            (date(2026, 8, 3),),
            False,
            ("run", "freshness", date(2026, 8, 13), None, "FRESHNESS_DUE"),
        ),
        (
            {
                "action": "none",
                "target_session": date(2026, 8, 13),
                "refresh_state": "success",
            },
            (date(2026, 8, 3),),
            False,
            ("none", "freshness", date(2026, 8, 13), None, "REPAIR_DISABLED"),
        ),
    ],
)
def test_continuity_policy_preserves_freshness_priority_and_selects_oldest_repair(
    freshness: dict[str, object],
    jobs: tuple[date, ...],
    enabled: bool,
    expected: tuple[object, ...],
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    decision = module.ContinuityPolicy().decide(
        freshness=automation.ScheduleDecision(**freshness),
        jobs=tuple(continuity_policy_job(item) for item in reversed(jobs)),
        repair_enabled=enabled,
        now=NOW,
    )

    assert (
        decision.action,
        decision.lane,
        decision.target_session,
        decision.repair_job_id,
        decision.reason_code,
    ) == expected
    assert decision.provider_requests == 0


def test_continuity_policy_retry_due_uses_existing_freshness_decision_without_retry_copy() -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    retry_slot = NOW
    decision = module.ContinuityPolicy().decide(
        freshness=automation.ScheduleDecision(
            action="run",
            target_session=date(2026, 8, 13),
            refresh_state="running",
        ),
        jobs=(continuity_policy_job(date(2026, 8, 3)),),
        repair_enabled=True,
        now=retry_slot,
    )

    assert decision.action == "run"
    assert decision.lane == "freshness"
    assert decision.reason_code == "FRESHNESS_DUE"
    assert "RETRY_TIMES" not in vars(module.ContinuityPolicy)


def test_continuity_policy_excludes_latest_until_calendar_advances_and_skips_dead_letter() -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    policy = module.ContinuityPolicy()
    same_day = continuity_policy_job(date(2026, 8, 13))
    dead = continuity_policy_job(date(2026, 8, 3), state="dead_letter")
    pending = continuity_policy_job(date(2026, 8, 4))

    current = policy.decide(
        freshness=automation.ScheduleDecision(
            action="none",
            target_session=date(2026, 8, 13),
            refresh_state="success",
        ),
        jobs=(dead, same_day, pending),
        repair_enabled=True,
        now=NOW,
    )
    advanced = policy.decide(
        freshness=automation.ScheduleDecision(
            action="none",
            target_session=date(2026, 8, 14),
            refresh_state="success",
        ),
        jobs=(same_day,),
        repair_enabled=True,
        now=NOW,
    )

    assert current.target_session == date(2026, 8, 4)
    assert current.repair_job_id == pending.job_id
    assert advanced.target_session == date(2026, 8, 13)
    assert advanced.repair_job_id == same_day.job_id


def test_continuity_policy_retry_eligibility_queue_unavailable_and_no_job_are_typed() -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    policy = module.ContinuityPolicy()
    freshness = automation.ScheduleDecision(
        action="wait",
        target_session=date(2026, 8, 13),
        refresh_state="retry_wait",
        next_run_at=NOW + timedelta(hours=1),
    )
    future_job = continuity_policy_job(
        date(2026, 8, 3),
        state="retry_wait",
        next_attempt_at=NOW + timedelta(hours=2),
    )

    unavailable = policy.decide(
        freshness=freshness,
        jobs=None,
        repair_enabled=True,
        now=NOW,
    )
    no_eligible = policy.decide(
        freshness=freshness,
        jobs=(future_job,),
        repair_enabled=True,
        now=NOW,
    )

    assert unavailable.action == no_eligible.action == "wait"
    assert unavailable.lane == no_eligible.lane == "freshness"
    assert unavailable.next_run_at == no_eligible.next_run_at == freshness.next_run_at
    assert unavailable.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert no_eligible.reason_code == "NO_ELIGIBLE_REPAIR"


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            {
                "action": "run",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "FRESHNESS_DUE",
            },
            id="run-freshness-due",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                "reason_code": "REPAIR_READY",
            },
            id="run-repair-ready-current",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                "next_run_at": NOW + timedelta(hours=1),
                "reason_code": "REPAIR_READY",
            },
            id="run-repair-ready-freshness-retry-slot",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                "reason_code": "REPAIR_CLAIMED",
            },
            id="run-repair-claimed",
        ),
        *(
            pytest.param(
                {
                    "action": "wait",
                    "lane": "freshness",
                    "target_session": date(2026, 8, 13),
                    "next_run_at": (
                        datetime(2026, 8, 13, 13, 0, tzinfo=timezone(timedelta(hours=8)))
                        if reason == "FRESHNESS_WAIT"
                        else NOW + timedelta(hours=1)
                    ),
                    "reason_code": reason,
                },
                id=f"wait-freshness-{reason.lower()}",
            )
            for reason in (
                "FRESHNESS_WAIT",
                "REPAIR_DISABLED",
                "CONTROL_STATE_UNAVAILABLE",
                "NO_ELIGIBLE_REPAIR",
            )
        ),
        *(
            pytest.param(
                {
                    "action": "wait",
                    "lane": "repair",
                    "target_session": date(2026, 8, 3),
                    "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                    "next_run_at": (
                        NOW + timedelta(hours=1) if reason == "ALREADY_RUNNING" else None
                    ),
                    "reason_code": reason,
                },
                id=f"wait-repair-{reason.lower()}",
            )
            for reason in (
                "PROVIDER_HEALTH_UNAVAILABLE",
                "PROVIDER_NOT_CLOSED",
                "CONTROL_STATE_UNAVAILABLE",
                "NO_ELIGIBLE_REPAIR",
                "ALREADY_RUNNING",
                "REPAIR_CONSUMER_FAILED",
            )
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": None,
                "reason_code": "CALENDAR_UNAVAILABLE",
            },
            id="none-calendar-unavailable",
        ),
        *(
            pytest.param(
                {
                    "action": "none",
                    "lane": "freshness",
                    "target_session": date(2026, 8, 13),
                    "reason_code": reason,
                },
                id=f"none-freshness-{reason.lower()}",
            )
            for reason in (
                "FRESHNESS_WAIT",
                "REPAIR_DISABLED",
                "CONTROL_STATE_UNAVAILABLE",
                "NO_ELIGIBLE_REPAIR",
            )
        ),
    ],
)
def test_continuity_decision_accepts_only_actual_production_matrix_rows(
    payload: dict[str, object],
) -> None:
    module = continuity_module()
    decision = module.ContinuityDecision.model_validate(payload)

    assert module.ContinuityDecision.model_validate(decision.model_dump(mode="python")) == decision
    if decision.next_run_at is not None:
        assert decision.next_run_at.tzinfo == UTC


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            {
                "action": "none",
                "lane": None,
                "target_session": None,
                "reason_code": "CALENDAR_UNAVAILABLE",
            },
            id="lane-none",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "repair_job_id": f"repair:2026-08-13:{UNIVERSE_ID}",
                "reason_code": "FRESHNESS_DUE",
            },
            id="freshness-with-repair-job",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-04:{UNIVERSE_ID}",
                "reason_code": "REPAIR_READY",
            },
            id="repair-job-target-mismatch",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "reason_code": "REPAIR_READY",
            },
            id="repair-without-job",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "REPAIR_READY",
            },
            id="run-freshness-wrong-reason",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "next_run_at": NOW + timedelta(hours=1),
                "reason_code": "FRESHNESS_DUE",
            },
            id="run-freshness-with-next-slot",
        ),
        pytest.param(
            {
                "action": "run",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                "reason_code": "PROVIDER_NOT_CLOSED",
            },
            id="run-repair-wrong-reason",
        ),
        pytest.param(
            {
                "action": "wait",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "next_run_at": NOW + timedelta(hours=1),
                "reason_code": "FRESHNESS_DUE",
            },
            id="wait-freshness-wrong-reason",
        ),
        pytest.param(
            {
                "action": "wait",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "FRESHNESS_WAIT",
            },
            id="wait-freshness-without-next-slot",
        ),
        pytest.param(
            {
                "action": "wait",
                "lane": "repair",
                "target_session": date(2026, 8, 3),
                "repair_job_id": f"repair:2026-08-03:{UNIVERSE_ID}",
                "reason_code": "REPAIR_READY",
            },
            id="wait-repair-ready",
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "CALENDAR_UNAVAILABLE",
            },
            id="calendar-unavailable-with-target",
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": None,
                "reason_code": "REPAIR_DISABLED",
            },
            id="none-freshness-without-target",
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "REPAIR_READY",
            },
            id="none-repair-ready",
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "next_run_at": NOW + timedelta(hours=1),
                "reason_code": "NO_ELIGIBLE_REPAIR",
            },
            id="none-with-next-slot",
        ),
        pytest.param(
            {
                "action": "wait",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "next_run_at": datetime(2026, 8, 13, 5, 0),
                "reason_code": "FRESHNESS_WAIT",
            },
            id="naive-next-slot",
        ),
        pytest.param(
            {
                "action": "none",
                "lane": "freshness",
                "target_session": date(2026, 8, 13),
                "reason_code": "token=secret /private/provider",
            },
            id="raw-reason",
        ),
    ],
)
def test_continuity_decision_rejects_cross_state_combinations(
    payload: dict[str, object],
) -> None:
    module = continuity_module()

    with pytest.raises(ValidationError):
        module.ContinuityDecision.model_validate(payload)


def test_continuity_decision_roundtrip_rejects_model_copy_bypass_and_raw_fields() -> None:
    module = continuity_module()
    valid = module.ContinuityDecision(
        action="run",
        lane="repair",
        target_session=date(2026, 8, 3),
        repair_job_id=f"repair:2026-08-03:{UNIVERSE_ID}",
        reason_code="REPAIR_READY",
    )

    assert set(valid.model_dump()) == {
        "action",
        "lane",
        "target_session",
        "repair_job_id",
        "next_run_at",
        "reason_code",
        "provider_requests",
    }
    with pytest.raises(ValidationError, match="frozen"):
        valid.action = "none"
    with pytest.raises(ValidationError):
        module.ContinuityDecision(**valid.model_dump(), payload="secret")
    with pytest.raises(ValidationError):
        module.ContinuityDecision(
            action="run",
            lane="repair",
            target_session=date(2026, 8, 3),
            repair_job_id="/private/job",
            reason_code="REPAIR_READY",
        )
    with pytest.raises(ValidationError):
        module.ContinuityDecision(
            action="run",
            lane="freshness",
            target_session=date(2026, 8, 13),
            repair_job_id=f"repair:2026-08-03:{UNIVERSE_ID}",
            reason_code="FRESHNESS_DUE",
        )
    bypassed = valid.model_copy(update={"action": "none"})
    with pytest.raises(ValidationError):
        module.ContinuityDecision.model_validate(bypassed.model_dump(mode="python"))


class RecordingHealthGate:
    def __init__(self, *states: object) -> None:
        self.states = list(states)
        self.calls = 0
        self.acquire_probe_calls = 0
        self.resolve_probe_calls = 0

    def provider_health(self):
        self.calls += 1
        state = self.states[min(self.calls - 1, len(self.states) - 1)]
        if isinstance(state, Exception):
            raise state
        return state

    def acquire_probe(self, *args, **kwargs):
        self.acquire_probe_calls += 1
        raise AssertionError("repair claim must not acquire a provider probe")

    def resolve_probe(self, *args, **kwargs):
        self.resolve_probe_calls += 1
        raise AssertionError("repair claim must not resolve a provider probe")


def circuit_health(state: str):
    module = importlib.import_module("backend.app.market.provider_health")
    return module.ProviderHealth(
        state=module.CircuitState(state),
        endpoints=(),
        blocking_endpoints=(),
        probe_endpoints=(),
    )


@pytest.mark.parametrize("blocked", ["OPEN", "HALF_OPEN", "unavailable"])
def test_repair_claim_health_gate_blocks_without_claim_attempt_or_probe(
    tmp_path: Path,
    blocked: str,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    path = tmp_path / "market.duckdb"
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(path)
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 3))[0]
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(
        ProviderHealthError("token=secret /private/provider-health.db")
        if blocked == "unavailable"
        else circuit_health(blocked)
    )
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    before = store.repair_queue_snapshot()

    result = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=lambda lease: pytest.fail("blocked repair cannot produce a lease"),
    )

    assert result.action == "wait"
    assert result.lane == "repair"
    assert result.target_session == job.trade_date
    assert result.reason_code == "PROVIDER_HEALTH_UNAVAILABLE"
    assert store.repair_queue_snapshot() == before
    assert health.calls == 1
    assert health.acquire_probe_calls == health.resolve_probe_calls == 0


def test_repair_claim_rechecks_health_under_lock_and_only_consumer_owns_lease(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 3))[0]
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("OPEN"))
    consumed = []
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=consumed.append,
    )

    assert decision.action == "wait"
    assert decision.reason_code == "PROVIDER_NOT_CLOSED"
    assert consumed == []
    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0] == job
    assert snapshot.attempts == ()
    assert health.calls == 2


def test_repair_claim_closed_claims_exactly_oldest_one_and_hands_it_to_consumer(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    jobs = enqueue(
        store,
        lock_path,
        date(2026, 8, 5),
        date(2026, 8, 3),
        date(2026, 8, 4),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    consumed = []
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=consumed.append,
    )

    assert decision.action == "run"
    assert decision.lane == "repair"
    assert decision.target_session == date(2026, 8, 3)
    assert decision.reason_code == "REPAIR_CLAIMED"
    assert len(consumed) == 1
    assert consumed[0].job_id == f"repair:2026-08-03:{UNIVERSE_ID}"
    snapshot = store.repair_queue_snapshot()
    assert sum(job.state == "leased" for job in snapshot.jobs) == 1
    assert len(snapshot.attempts) == 1
    assert snapshot.attempts[0].outcome == "running"
    assert {job.trade_date for job in jobs if job.state == "pending"} == {
        date(2026, 8, 3),
        date(2026, 8, 4),
        date(2026, 8, 5),
    }


def test_repair_claim_without_consumer_never_leases_or_mutates(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    enqueue(store, lock_path, date(2026, 8, 3))
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"))
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    before = store.repair_queue_snapshot()

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=None,
    )

    assert decision.action == "run"
    assert decision.reason_code == "REPAIR_READY"
    assert store.repair_queue_snapshot() == before
    assert health.calls == 1


def test_repair_claim_consumer_failure_is_sanitized_and_left_for_stale_recovery(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    enqueue(store, lock_path, date(2026, 8, 3))
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=lambda lease: (_ for _ in ()).throw(
            RuntimeError("token=secret /private/provider")
        ),
    )

    snapshot = store.repair_queue_snapshot()
    assert decision.action == "wait"
    assert decision.reason_code == "REPAIR_CONSUMER_FAILED"
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.attempts[0].outcome == "running"
    assert health.acquire_probe_calls == health.resolve_probe_calls == 0
    assert not {"payload", "token", "url", "path", "exception", "error"} & set(
        decision.model_dump(mode="json")
    )


def test_repair_claim_busy_lock_and_freshness_change_leave_queue_unchanged(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    enqueue(store, lock_path, date(2026, 8, 3))
    waiting = automation.ScheduleDecision(
        action="wait",
        target_session=date(2026, 8, 13),
        refresh_state="retry_wait",
        next_run_at=NOW + timedelta(hours=1),
    )
    due = automation.ScheduleDecision(
        action="run",
        target_session=date(2026, 8, 13),
        refresh_state="running",
    )
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=RecordingHealthGate(circuit_health("CLOSED")),
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    before = store.repair_queue_snapshot()

    changed = coordinator.claim_ready_once(
        freshness=waiting,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: due,
        lease_consumer=lambda lease: pytest.fail("freshness due must not claim"),
    )
    with RefreshRunLock(lock_path):
        busy = coordinator.claim_ready_once(
            freshness=waiting,
            latest_expected_session=date(2026, 8, 13),
            repair_enabled=True,
            now=NOW,
            revalidator=lambda: waiting,
            lease_consumer=lambda lease: pytest.fail("busy lock must not claim"),
        )

    assert changed.lane == "freshness"
    assert changed.reason_code == "FRESHNESS_DUE"
    assert busy.action == "wait"
    assert busy.reason_code == "ALREADY_RUNNING"
    assert store.repair_queue_snapshot() == before


def test_repair_preclaim_maintenance_reaps_expired_lease_before_eligibility_check(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    job = enqueue(store, lock_path, date(2026, 8, 3))[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            job.job_id,
            owner="crashed-worker",
            expected_version=job.state_version,
            now=NOW,
            lease_seconds=60,
        )
    assert lease is not None
    expired = NOW + timedelta(seconds=61)
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"))
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=expired,
        revalidator=lambda: freshness,
        lease_consumer=lambda _lease: pytest.fail("expired maintenance must not fetch"),
        pre_claim=lambda: store.reap_expired_repair_leases(
            now=expired,
            retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
        ),
    )

    assert decision.reason_code == "NO_ELIGIBLE_REPAIR"
    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "retry_wait"
    assert snapshot.attempts[0].outcome == "abandoned"
    assert health.calls == 0


def test_repair_preclaim_reconciles_leased_or_dead_letter_manifest_ready_without_fetch(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    target = date(2026, 8, 3)
    leased_job = enqueue(store, lock_path, target)[0]
    with RefreshRunLock(lock_path):
        lease = store.claim_repair_job(
            leased_job.job_id,
            owner="crashed-worker",
            expected_version=leased_job.state_version,
            now=NOW,
            lease_seconds=1800,
        )
    assert lease is not None
    second = enqueue(store, lock_path, date(2026, 8, 4))[0]
    with RefreshRunLock(lock_path):
        second_lease = store.claim_repair_job(
            second.job_id,
            owner="repair-worker",
            expected_version=second.state_version,
            now=NOW,
            lease_seconds=1800,
        )
        assert second_lease is not None
        store.finalize_repair_attempt(
            second_lease,
            outcome="failed",
            refresh_result=error_refresh(date(2026, 8, 4), attempt=1, retryable=False),
            now=NOW + timedelta(minutes=1),
            retry_policy=module.RepairRetryPolicy(max_attempts=1, base_seconds=900),
        )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"))
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW + timedelta(minutes=2),
        revalidator=lambda: freshness,
        lease_consumer=lambda _lease: pytest.fail("manifest reconciliation must not fetch"),
        pre_claim=lambda: store.reconcile_published_repair_jobs(
            (target, date(2026, 8, 4)),
            now=NOW + timedelta(minutes=2),
        ),
    )

    assert decision.reason_code == "NO_ELIGIBLE_REPAIR"
    snapshot = store.repair_queue_snapshot()
    assert {job.state for job in snapshot.jobs} == {"published"}
    assert {attempt.outcome for attempt in snapshot.attempts} == {"abandoned", "failed"}
    assert health.calls == 0


def test_repair_preclaim_runs_when_initial_policy_has_no_pending_job(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    calls = 0

    def maintenance() -> None:
        nonlocal calls
        calls += 1

    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=RecordingHealthGate(circuit_health("CLOSED")),
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=lambda _lease: pytest.fail("no pending repair must not fetch"),
        pre_claim=maintenance,
    )

    assert calls == 1
    assert decision.reason_code == "NO_ELIGIBLE_REPAIR"


def test_repair_preclaim_reconciles_before_health_gate_and_blocks_remaining_claim(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(store, lock_path)
    first, second = enqueue(store, lock_path, date(2026, 8, 3), date(2026, 8, 4))
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )
    health = RecordingHealthGate(ProviderHealthError("token=secret /private/health.db"))
    coordinator = module.RepairClaimCoordinator(
        store=store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    decision = coordinator.claim_ready_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        now=NOW,
        revalidator=lambda: freshness,
        lease_consumer=lambda _lease: pytest.fail("unavailable health must block provider"),
        pre_claim=lambda: store.reconcile_published_repair_jobs(
            (first.trade_date,),
            now=NOW,
        ),
    )

    assert decision.reason_code == "PROVIDER_HEALTH_UNAVAILABLE"
    snapshot = store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "published"
    assert snapshot.jobs[1].state == "pending"
    assert snapshot.attempts == ()
    assert health.calls == 1


def test_full_session_repair_executor_uses_canonical_chain_and_exact_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    provider = object()
    calls: list[dict[str, object]] = []

    def fake_refresh(store, received_provider, **kwargs):
        calls.append({"store": store, "provider": received_provider, **kwargs})
        return ready_refresh(target, started_at=NOW)

    monkeypatch.setattr(automation, "run_publication_refresh", fake_refresh)
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )

    class SequencedReader:
        def __init__(self) -> None:
            self.values = [ready_inventory(), ready_inventory(target)]

        def verified_ready_session_inventory(self, source: str = "baostock"):
            return self.values.pop(0)

    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=provider,
        inventory_reader=SequencedReader(),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "published"
    assert len(calls) == 1
    assert calls[0]["store"] is executor.canonical_store
    assert calls[0]["provider"] is provider
    assert calls[0]["required_symbols"] == {"sh.600000"}
    assert calls[0]["run_kind"] == "repair"
    assert calls[0]["request_key"] == "repair:baostock:2026-08-03:all-main-board"
    assert calls[0]["run_id"].startswith("repair-2026-08-03-")
    assert queue_store.repair_queue_snapshot().jobs[0].state == "published"


def test_full_session_repair_executor_rejects_post_publish_inventory_without_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    monkeypatch.setattr(
        automation,
        "run_publication_refresh",
        lambda *_args, **_kwargs: ready_refresh(target, started_at=NOW),
    )
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "failed"
    assert result.reason_code == "POST_PUBLISH_INVENTORY_MISSING"
    snapshot = queue_store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "dead_letter"
    assert snapshot.attempts[0].outcome == "failed"
    assert snapshot.attempts[0].failure_stage == "publish"
    assert snapshot.attempts[0].failure_class == "storage"


@pytest.mark.parametrize(
    ("inventory_failure", "expected_state", "expected_retryable"),
    [
        (DatasetError("manifest path /private/token changed"), "dead_letter", False),
        (
            MarketStoreReadError("control database /tmp/private/path unavailable"),
            "retry_wait",
            True,
        ),
        (RuntimeError("unexpected provider token/path"), "dead_letter", False),
    ],
)
def test_repair_post_publish_inventory_exception_is_finalized_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_failure: Exception,
    expected_state: str,
    expected_retryable: bool,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    monkeypatch.setattr(
        automation,
        "run_publication_refresh",
        lambda *_args, **_kwargs: ready_refresh(target, started_at=NOW),
    )

    class SequencedReader:
        def __init__(self) -> None:
            self.calls = 0

        def verified_ready_session_inventory(self, source: str = "baostock"):
            self.calls += 1
            if self.calls == 1:
                return ready_inventory()
            raise inventory_failure

    reader = SequencedReader()
    finalize_calls = 0
    original_finalize = queue_store.finalize_repair_attempt

    def finalize_once(*args, **kwargs):
        nonlocal finalize_calls
        finalize_calls += 1
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(queue_store, "finalize_repair_attempt", finalize_once)
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=reader,
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "failed"
    assert result.reason_code == "POST_PUBLISH_INVENTORY_MISSING"
    assert result.provider_requests == 1
    assert result.refresh_result is not None
    assert result.refresh_result.status == "error"
    assert result.refresh_result.failure_stage == "publish"
    assert result.refresh_result.failure_class == "storage"
    assert result.refresh_result.retryable is expected_retryable
    serialized = json.dumps(result.model_dump(mode="json"))
    assert "/private/" not in serialized
    assert "token" not in serialized
    assert finalize_calls == 1
    snapshot = queue_store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == expected_state
    assert snapshot.attempts[0].outcome == "failed"
    assert snapshot.attempts[0].refresh_run_id == result.refresh_result.run_id


def test_repair_process_kill_after_publish_keeps_lease_for_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))

    class SimulatedKill(BaseException):
        pass

    monkeypatch.setattr(
        automation,
        "run_publication_refresh",
        lambda *_args, **_kwargs: ready_refresh(target, started_at=NOW),
    )

    class KillReader:
        def __init__(self) -> None:
            self.calls = 0

        def verified_ready_session_inventory(self, source: str = "baostock"):
            self.calls += 1
            if self.calls == 1:
                return ready_inventory()
            raise SimulatedKill()

    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=KillReader(),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    with pytest.raises(SimulatedKill):
        executor.execute_once(
            freshness=freshness,
            latest_expected_session=date(2026, 8, 13),
            repair_enabled=True,
            revalidator=lambda: freshness,
        )

    snapshot = queue_store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.attempts[0].outcome == "running"


def test_full_session_repair_executor_propagates_process_kill_after_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))

    class SimulatedKill(BaseException):
        pass

    monkeypatch.setattr(
        automation,
        "run_publication_refresh",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedKill()),
    )
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    with pytest.raises(SimulatedKill):
        executor.execute_once(
            freshness=freshness,
            latest_expected_session=date(2026, 8, 13),
            repair_enabled=True,
            revalidator=lambda: freshness,
        )

    snapshot = queue_store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == "leased"
    assert snapshot.attempts[0].outcome == "running"


@pytest.mark.parametrize(
    "retryable, expected_state", [(True, "retry_wait"), (False, "dead_letter")]
)
def test_full_session_repair_executor_finalizes_failure_once_without_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    retryable: bool,
    expected_state: str,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    calls = 0

    def fake_refresh(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return error_refresh(target, attempt=1, retryable=retryable, started_at=NOW)

    monkeypatch.setattr(automation, "run_publication_refresh", fake_refresh)
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "failed"
    assert result.reason_code == "REPAIR_RESULT_FAILED"
    assert calls == 1
    snapshot = queue_store.repair_queue_snapshot()
    assert snapshot.jobs[0].state == expected_state
    assert len(snapshot.attempts) == 1
    assert snapshot.attempts[0].outcome == "failed"


def test_repair_executor_rejects_unknown_calendar_before_queue_schema_write(
    tmp_path: Path,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    initialize_continuity(queue_store, lock_path)
    target = date(2026, 8, 3)
    enqueue(queue_store, lock_path, target)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({target: "unknown"}),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        inventory_mode="immutable_dataset",
    )
    health = RecordingHealthGate(circuit_health("CLOSED"))
    calls = 0

    def initializer() -> None:
        nonlocal calls
        calls += 1

    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
        scanner=scanner,
        configured_start=target,
        queue_schema_initializer=initializer,
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=date(2026, 8, 13),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=date(2026, 8, 13),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "skipped"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert calls == 0
    assert queue_store.repair_queue_snapshot().jobs[0].state == "pending"


def test_repair_executor_initializes_and_claims_missing_queue_only_after_strict_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = continuity_module()
    automation = importlib.import_module("backend.app.market.automation")
    lock_path = tmp_path / "market-refresh.lock"
    queue_store = MarketStore(tmp_path / "market.duckdb")
    queue_store.initialize_schema()
    target = date(2026, 8, 3)
    scanner = module.ContinuityInventory(
        calendar=RecordingConfirmedCalendar({target: "open", target + timedelta(days=1): "open"}),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        inventory_mode="immutable_dataset",
    )
    monkeypatch.setattr(
        automation,
        "run_publication_refresh",
        lambda *_args, **_kwargs: error_refresh(target, attempt=1),
    )
    health = RecordingHealthGate(circuit_health("CLOSED"), circuit_health("CLOSED"))
    coordinator = module.RepairClaimCoordinator(
        store=queue_store,
        health_store=health,
        lock_path=lock_path,
        owner="repair-worker",
        lease_seconds=1800,
    )
    executor = module.ContinuityRepairExecutor(
        coordinator=coordinator,
        queue_store=queue_store,
        canonical_store=object(),
        provider=object(),
        inventory_reader=RecordingInventoryReader(ready_inventory()),
        required_symbols=lambda: {"sh.600000"},
        clock=lambda: NOW,
        retry_policy=module.RepairRetryPolicy(max_attempts=4, base_seconds=900),
        scanner=scanner,
        configured_start=target,
        queue_schema_initializer=queue_store.initialize_continuity_schema,
    )
    freshness = automation.ScheduleDecision(
        action="none",
        target_session=target + timedelta(days=1),
        refresh_state="success",
    )

    result = executor.execute_once(
        freshness=freshness,
        latest_expected_session=target + timedelta(days=1),
        repair_enabled=True,
        revalidator=lambda: freshness,
    )

    assert result.status == "failed"
    assert queue_store.repair_queue_snapshot().jobs[0].state == "retry_wait"
    assert queue_store.repair_queue_snapshot().attempts[0].outcome == "failed"
