import json
import logging
import socket
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

import backend.app.cli as cli
from backend.app.market.automation import run_publication_refresh
from backend.app.market.baostock import (
    DAILY_FIELDS,
    BaoStockError,
    BaoStockProvider,
    BaoStockTransportError,
    ProviderBatch,
    _read_result,
)
from backend.app.market.failures import (
    MarketFailure,
    MarketFailureError,
    market_failure_from_exception,
)
from backend.app.market.models import RefreshResult
from backend.app.market.refresh import MarketRefreshService
from backend.app.market.store import MarketStore


class Result:
    def __init__(self, fields, rows, *, error_code="0", error_msg="") -> None:
        self.fields = fields
        self.rows = iter(rows)
        self.error_code = error_code
        self.error_msg = error_msg
        self.current = None

    def next(self):
        self.current = next(self.rows, None)
        return self.current is not None

    def get_row_data(self):
        return self.current


class AdapterFailureClient:
    def __init__(self, history_result: Result, *, trading: bool = True) -> None:
        self.history_result = history_result
        self.trading = trading

    def login(self):
        return Result([], [])

    def logout(self):
        return Result([], [])

    def query_trade_dates(self, **_kwargs):
        return Result(
            ["calendar_date", "is_trading_day"],
            [["2026-07-23", "1" if self.trading else "0"]],
        )

    def query_history_k_data_plus(self, *_args, **_kwargs):
        return self.history_result

    def query_adjust_factor(self, *_args, **_kwargs):
        return Result([], [])


def refresh_result(**updates) -> RefreshResult:
    payload = {
        "run_id": "failure-fixture",
        "requested_date": date(2026, 7, 23),
        "source": "baostock",
        "status": "error",
        "requested_count": 1,
        "succeeded_count": 0,
        "coverage_ratio": 0,
        "quality_issues": ["provider_error"],
        "started_at": datetime(2026, 7, 23, 10, tzinfo=UTC),
        "completed_at": datetime(2026, 7, 23, 10, 1, tzinfo=UTC),
    }
    payload.update(updates)
    return RefreshResult(**payload)


def test_market_failure_public_contract_is_typed_immutable_and_minimal() -> None:
    failure = MarketFailure(
        failure_stage="fetch",
        failure_class="transport_timeout",
        retryable=True,
    )

    assert failure.model_dump() == {
        "failure_stage": "fetch",
        "failure_class": "transport_timeout",
        "retryable": True,
    }
    with pytest.raises(ValidationError):
        failure.retryable = False
    with pytest.raises(ValidationError):
        MarketFailure(
            failure_stage="login",
            failure_class="transport_timeout",
            retryable=True,
        )
    with pytest.raises(ValidationError):
        MarketFailure(
            failure_stage="fetch",
            failure_class="raw_exception",
            retryable=True,
        )


def test_refresh_result_failure_fields_are_optional_typed_and_ready_fails_closed() -> None:
    legacy = refresh_result()
    assert legacy.failure_stage is None
    assert legacy.failure_class is None
    assert legacy.retryable is None

    structured = refresh_result(
        failure_stage="fetch",
        failure_class="dns",
        retryable=True,
    )
    assert json.loads(structured.model_dump_json())["failure_class"] == "dns"

    with pytest.raises(ValidationError, match="ready refresh must be complete"):
        refresh_result(
            status="ready",
            requested_count=1,
            succeeded_count=1,
            coverage_ratio=1,
            quality_issues=[],
            failure_stage="fetch",
            failure_class="dns",
            retryable=True,
        )


def test_refresh_failure_fields_round_trip_through_market_store(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    expected = refresh_result(
        failure_stage="fetch",
        failure_class="dns",
        retryable=True,
        error_message="provider request failed",
    )

    store.save_refresh([], expected, publish=False)

    latest = store.latest_refresh()
    listed = store.list_refreshes()
    assert latest is not None
    assert latest.failure_stage == "fetch"
    assert latest.failure_class == "dns"
    assert latest.retryable is True
    assert [item.model_dump() for item in listed] == [latest.model_dump()]


def test_existing_refresh_schema_migrates_failure_fields_as_null(tmp_path: Path) -> None:
    path = tmp_path / "legacy.duckdb"
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE refresh_runs (
                run_id VARCHAR PRIMARY KEY,
                request_key VARCHAR,
                run_kind VARCHAR NOT NULL DEFAULT 'daily',
                requested_date DATE NOT NULL,
                source VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                requested_count INTEGER NOT NULL,
                succeeded_count INTEGER NOT NULL,
                coverage_ratio DOUBLE,
                failed_symbols JSON NOT NULL,
                quality_issues JSON NOT NULL,
                error_message VARCHAR,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO refresh_runs VALUES (
                'legacy-run', NULL, 'daily', '2026-07-23', 'baostock', 'error',
                1, 0, 0, '["sh.600000"]', '["provider_error"]',
                'provider request failed', '2026-07-23T10:00:00Z',
                '2026-07-23T10:01:00Z'
            )
            """
        )
    finally:
        connection.close()

    store = MarketStore(path)
    store.initialize_schema()
    connection = duckdb.connect(str(path), read_only=True)
    try:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(refresh_runs)").fetchall()
        }
    finally:
        connection.close()

    legacy = store.latest_refresh()
    assert {"failure_stage", "failure_class", "retryable"} <= columns
    assert legacy is not None
    assert legacy.run_id == "legacy-run"
    assert legacy.failure_stage is None
    assert legacy.failure_class is None
    assert legacy.retryable is None


def test_market_schema_migration_cli_upgrades_legacy_control_store(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    path = tmp_path / "market" / "stock_eva.duckdb"
    path.parent.mkdir()
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE refresh_runs (
                run_id VARCHAR PRIMARY KEY, request_key VARCHAR,
                run_kind VARCHAR NOT NULL DEFAULT 'daily', requested_date DATE NOT NULL,
                source VARCHAR NOT NULL, status VARCHAR NOT NULL,
                requested_count INTEGER NOT NULL, succeeded_count INTEGER NOT NULL,
                coverage_ratio DOUBLE, failed_symbols JSON NOT NULL,
                quality_issues JSON NOT NULL, error_message VARCHAR,
                started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO refresh_runs VALUES (
                'legacy-cli', NULL, 'daily', '2026-07-23', 'baostock', 'error',
                1, 0, 0, '["sh.600000"]', '["provider_error"]',
                'provider request failed', '2026-07-23T10:00:00Z',
                '2026-07-23T10:01:00Z'
            )
            """
        )
    finally:
        connection.close()
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": path.parent,
            "market_database_name": path.name,
            "local_temp_dir": tmp_path / "tmp",
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "migration": "market_control_schema",
        "status": "ready",
        "writes_market_control_schema": True,
    }
    migrated = MarketStore(path).latest_refresh()
    assert migrated is not None and migrated.run_id == "legacy-cli"
    assert migrated.failure_stage is None
    assert migrated.failure_class is None
    assert migrated.retryable is None
    connection = duckdb.connect(str(path), read_only=True)
    try:
        legacy_projection = connection.execute(
            """
            SELECT run_id, requested_date, source, status, requested_count,
                   succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                   error_message, started_at, completed_at, request_key, run_kind
            FROM refresh_runs WHERE run_id = 'legacy-cli'
            """
        ).fetchone()
    finally:
        connection.close()
    assert legacy_projection is not None
    assert MarketStore._refresh_result_from_row(legacy_projection).run_id == "legacy-cli"


def test_market_schema_migration_cli_is_idempotent_for_empty_and_current_store(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "local_temp_dir": tmp_path / "tmp",
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 0
    first = json.loads(capsys.readouterr().out)
    assert cli.main() == 0
    second = json.loads(capsys.readouterr().out)

    assert first == second
    store = MarketStore(settings.market_data_dir / settings.market_database_name)
    connection = duckdb.connect(str(store.path), read_only=True)
    try:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(refresh_runs)").fetchall()
        }
    finally:
        connection.close()
    assert {"failure_stage", "failure_class", "retryable"} <= columns
    assert store.list_refreshes() == []


def test_market_schema_migration_cli_sanitizes_failure_output(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    secret = "token=migration-secret /private/stock_eva.duckdb SELECT * FROM private"
    settings = cli.get_settings().model_copy(update={"market_data_dir": tmp_path / "market"})
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        MarketStore,
        "initialize_schema",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(secret)),
    )
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-schema-migrate"])

    assert cli.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "error_code": "market_control_schema_migration_failed",
        "status": "error",
        "writes_market_control_schema": False,
    }
    assert secret not in captured.out + captured.err


def test_legacy_fourteen_column_refresh_projection_defaults_failure_fields() -> None:
    legacy_row = (
        "legacy-projection",
        date(2026, 7, 23),
        "baostock",
        "error",
        1,
        0,
        0.0,
        '["sh.600000"]',
        '["provider_error"]',
        "provider request failed",
        datetime(2026, 7, 23, 10, tzinfo=UTC),
        datetime(2026, 7, 23, 10, 1, tzinfo=UTC),
        None,
        "daily",
    )

    result = MarketStore._refresh_result_from_row(legacy_row)

    assert result.failure_stage is None
    assert result.failure_class is None
    assert result.retryable is None


def test_refresh_runs_cli_preserves_sanitized_structured_failure(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    secret_parts = [
        "token=persisted-secret",
        "https://provider.invalid/private",
        "SELECT secret FROM audit",
        "/private/market.duckdb",
    ]
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    store = MarketStore(settings.market_data_dir / settings.market_database_name)

    class UnexpectedProvider:
        def fetch(self, trade_date, symbols=None):
            raise RuntimeError(" ".join(secret_parts))

    failed = MarketRefreshService(store, UnexpectedProvider()).refresh(
        date(2026, 7, 23),
        symbols=["sh.600000"],
    )
    assert failed.failure_class == "internal"
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "refresh-runs"])

    assert cli.main() == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload[0]["failure_stage"] == "fetch"
    assert payload[0]["failure_class"] == "internal"
    assert payload[0]["retryable"] is False
    assert all(part not in captured.out + captured.err for part in secret_parts)


@pytest.mark.parametrize(
    ("error", "expected_class", "retryable"),
    [
        (TimeoutError("private timeout detail"), "transport_timeout", True),
        (ConnectionRefusedError("private connect detail"), "transport_connect", True),
        (socket.gaierror("private DNS detail"), "dns", True),
        (RuntimeError("private unexpected detail"), "internal", False),
    ],
)
def test_transport_and_unexpected_failure_mapping_is_deterministic(
    error: Exception,
    expected_class: str,
    retryable: bool,
) -> None:
    failure = market_failure_from_exception(error, stage="fetch")

    assert failure.failure_stage == "fetch"
    assert failure.failure_class == expected_class
    assert failure.retryable is retryable


@pytest.mark.parametrize(
    ("error_code", "expected_class", "retryable"),
    [
        ("401", "auth", False),
        ("429", "rate_limit", True),
        ("404", "provider_4xx", False),
        ("503", "provider_5xx", True),
        ("1001", "provider_5xx", True),
    ],
)
def test_baostock_provider_status_failure_is_mapped_without_message_text(
    error_code: str,
    expected_class: str,
    retryable: bool,
) -> None:
    secret = "token=market-secret https://provider.invalid/private"

    with pytest.raises(BaoStockTransportError) as captured:
        _read_result(Result([], [], error_code=error_code, error_msg=secret))

    failure = market_failure_from_exception(captured.value, stage="fetch")
    assert failure.failure_class == expected_class
    assert failure.retryable is retryable
    assert secret not in str(captured.value)


def test_degraded_provider_batch_preserves_retryable_transport_failure() -> None:
    secret = "token=batch-secret https://provider.invalid/private"
    provider = BaoStockProvider(
        client=AdapterFailureClient(
            Result([], [], error_code="503", error_msg=secret),
        ),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert batch.failed_symbols == ["sh.600000"]
    assert batch.failure is not None
    assert batch.failure.failure_stage == "fetch"
    assert batch.failure.failure_class == "provider_5xx"
    assert batch.failure.retryable is True
    assert secret not in repr(batch)


def test_partial_publication_retains_provider_error_and_structured_failure(
    tmp_path: Path,
) -> None:
    failure = MarketFailure(
        failure_stage="fetch",
        failure_class="transport_connect",
        retryable=True,
    )

    class DegradedProvider:
        def fetch(self, trade_date, symbols=None):
            return ProviderBatch(
                bars=[],
                expected_symbols=["sh.600000"],
                failed_symbols=["sh.600000"],
                failure=failure,
            )

    result = run_publication_refresh(
        MarketStore(tmp_path / "market.duckdb"),
        DegradedProvider(),
        trade_date=date(2026, 7, 23),
        required_symbols=set(),
    )

    assert result.status == "partial"
    assert "provider_error" in result.quality_issues
    assert result.failure_stage == "fetch"
    assert result.failure_class == "transport_connect"
    assert result.retryable is True


@pytest.mark.parametrize(
    ("failure_class", "stage", "retryable"),
    [
        ("schema", "normalize", False),
        ("semantic", "validate", False),
        ("calendar", "validate", False),
        ("storage", "publish", False),
    ],
)
def test_typed_schema_semantic_calendar_and_storage_failures_are_preserved(
    failure_class: str,
    stage: str,
    retryable: bool,
) -> None:
    expected = MarketFailure(
        failure_stage=stage,
        failure_class=failure_class,
        retryable=retryable,
    )

    actual = market_failure_from_exception(MarketFailureError(expected), stage="fetch")

    assert actual == expected


def test_baostock_adapter_maps_schema_failure_at_normalize_boundary() -> None:
    provider = BaoStockProvider(
        client=AdapterFailureClient(Result(["date"], [["2026-07-23"]])),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError) as captured:
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    failure = market_failure_from_exception(captured.value, stage="fetch")
    assert failure.failure_stage == "normalize"
    assert failure.failure_class == "schema"
    assert failure.retryable is False


def test_baostock_adapter_maps_semantic_failure_at_normalize_boundary() -> None:
    fields = DAILY_FIELDS.split(",")
    row = [
        "2026-07-23",
        "sh.600000",
        "10",
        "11",
        "9",
        "10.5",
        "10",
        "100",
        "1000",
        "2",
        "1",
        "1",
        "5",
        "0",
    ]
    provider = BaoStockProvider(
        client=AdapterFailureClient(Result(fields, [row])),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError) as captured:
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    failure = market_failure_from_exception(captured.value, stage="fetch")
    assert failure.failure_stage == "normalize"
    assert failure.failure_class == "semantic"
    assert failure.retryable is False


def test_baostock_adapter_maps_closed_session_as_calendar_failure() -> None:
    provider = BaoStockProvider(
        client=AdapterFailureClient(Result([], []), trading=False),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError) as captured:
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    failure = market_failure_from_exception(captured.value, stage="fetch")
    assert failure.failure_class == "calendar"
    assert failure.retryable is False


@pytest.mark.parametrize(
    ("entrypoint", "fields", "rows"),
    [
        ("trading_dates", ["is_trading_day"], [["1"]]),
        ("fetch", ["calendar_date"], [["2026-07-23"]]),
    ],
)
def test_baostock_calendar_schema_drift_is_typed_non_retryable_schema_failure(
    entrypoint: str,
    fields: list[str],
    rows: list[list[str]],
) -> None:
    class CalendarSchemaClient(AdapterFailureClient):
        def query_trade_dates(self, **_kwargs):
            return Result(fields, rows)

    provider = BaoStockProvider(
        client=CalendarSchemaClient(Result([], [])),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError) as captured:
        if entrypoint == "trading_dates":
            provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23))
        else:
            provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert captured.value.failure is not None
    assert captured.value.failure.failure_stage == "normalize"
    assert captured.value.failure.failure_class == "schema"
    assert captured.value.failure.retryable is False


class MainBoardFactorFailureClient:
    factor_fields = [
        "code",
        "dividOperateDate",
        "foreAdjustFactor",
        "backAdjustFactor",
        "adjustFactor",
    ]
    stock_row = [
        "2026-07-23",
        "sh.600000",
        "10",
        "11",
        "9",
        "10.5",
        "10",
        "100",
        "1000",
        "3",
        "1",
        "1",
        "5",
        "0",
    ]
    index_rows = {
        "sh.000001": [
            "2026-07-23",
            "sh.000001",
            "3000",
            "3010",
            "2990",
            "3005",
            "3000",
            "100",
            "1000",
            "3",
            "",
            "1",
            "0.2",
            "0",
        ],
        "sz.399001": [
            "2026-07-23",
            "sz.399001",
            "9000",
            "9050",
            "8950",
            "9020",
            "9000",
            "100",
            "1000",
            "3",
            "",
            "1",
            "0.2",
            "0",
        ],
    }

    def __init__(self, bootstrap_outcome) -> None:
        self.bootstrap_outcome = bootstrap_outcome

    def login(self):
        return Result([], [])

    def logout(self):
        return Result([], [])

    def query_trade_dates(self, **_kwargs):
        return Result(
            ["calendar_date", "is_trading_day"],
            [["2026-07-23", "1"]],
        )

    def query_all_stock(self, **_kwargs):
        return Result(
            ["code", "tradeStatus", "code_name"],
            [["sh.600000", "1", "fixture"]],
        )

    def query_daily_history_k_AStock(self, **_kwargs):
        return Result(DAILY_FIELDS.split(","), [self.stock_row])

    def query_daily_adjust_factor(self, **_kwargs):
        return Result(self.factor_fields, [])

    def query_adjust_factor(self, *_args, **_kwargs):
        if isinstance(self.bootstrap_outcome, Exception):
            raise self.bootstrap_outcome
        return self.bootstrap_outcome

    def query_history_k_data_plus(self, symbol, _fields, **_kwargs):
        return Result(DAILY_FIELDS.split(","), [self.index_rows[symbol]])


@pytest.mark.parametrize(
    ("bootstrap_outcome", "expected_class"),
    [
        (TimeoutError("private timeout token=factor-secret"), "transport_timeout"),
        (
            Result(
                [],
                [],
                error_code="503",
                error_msg="https://provider.invalid/private token=factor-secret",
            ),
            "provider_5xx",
        ),
    ],
)
def test_main_board_factor_bootstrap_transport_failure_reaches_public_result(
    tmp_path: Path,
    caplog,
    bootstrap_outcome,
    expected_class: str,
) -> None:
    provider = BaoStockProvider(
        client=MainBoardFactorFailureClient(bootstrap_outcome),
        max_attempts=1,
        min_request_interval_seconds=0,
        factor_cache_path=str(tmp_path / "factor-cache.sqlite3"),
    )
    caplog.set_level(logging.INFO)

    result = run_publication_refresh(
        MarketStore(tmp_path / "market.duckdb"),
        provider,
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert "provider_error" in result.quality_issues
    assert result.failure_stage == "fetch"
    assert result.failure_class == expected_class
    assert result.retryable is True
    public = result.model_dump_json() + caplog.text
    assert "factor-secret" not in public
    assert "provider.invalid" not in public


def test_main_board_invalid_factor_is_isolated_as_non_retryable_semantic_failure(
    tmp_path: Path,
    caplog,
) -> None:
    secret = "token=factor-semantic-secret"
    invalid = Result(
        MainBoardFactorFailureClient.factor_fields,
        [["sh.600000", "2020-01-01", "1", "nan", secret]],
    )

    def provider(cache_name: str) -> BaoStockProvider:
        outcome = Result(
            invalid.fields,
            [["sh.600000", "2020-01-01", "1", "nan", secret]],
        )
        return BaoStockProvider(
            client=MainBoardFactorFailureClient(outcome),
            max_attempts=1,
            min_request_interval_seconds=0,
            factor_cache_path=str(tmp_path / cache_name),
        )

    caplog.set_level(logging.INFO)

    batch = provider("factor-cache-batch.sqlite3").fetch(date(2026, 7, 23))
    assert batch.failure is not None
    assert batch.failure.failure_stage == "normalize"
    assert batch.failure.failure_class == "semantic"
    assert batch.failure.retryable is False
    assert next(bar for bar in batch.bars if bar.symbol == "sh.600000").adjust_factor is None

    store = MarketStore(tmp_path / "market.duckdb")
    result = run_publication_refresh(
        store,
        provider("factor-cache-publication.sqlite3"),
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert result.failure_stage == "normalize"
    assert result.failure_class == "semantic"
    assert result.retryable is False
    assert store.published_refresh() is None
    public = result.model_dump_json() + caplog.text
    assert "factor-semantic-secret" not in public


def test_publication_storage_failure_is_structured_and_sanitized() -> None:
    secret = "SELECT * FROM private_table /private/market.duckdb"

    class EmptyProvider:
        def fetch(self, trade_date, symbols=None):
            return ProviderBatch(bars=[], expected_symbols=[], failed_symbols=[])

    class FailingStore:
        def save_refresh(self, *_args, **_kwargs):
            raise RuntimeError(secret)

    result = run_publication_refresh(
        FailingStore(),
        EmptyProvider(),
        trade_date=date(2026, 7, 23),
        required_symbols=set(),
    )

    assert result.failure_stage == "publish"
    assert result.failure_class == "storage"
    assert result.retryable is False
    assert secret not in result.model_dump_json()


def test_publication_failure_is_sanitized_in_model_and_logs(
    tmp_path: Path,
    caplog,
) -> None:
    secret_parts = [
        "token=market-secret",
        "https://provider.invalid/private",
        "SELECT secret FROM audit",
        "/private/market.duckdb",
    ]
    message = " ".join(secret_parts)

    class UnexpectedProvider:
        def fetch(self, trade_date, symbols=None):
            raise RuntimeError(message)

    caplog.set_level(logging.INFO, logger="stock_eva.market.automation")
    result = run_publication_refresh(
        MarketStore(tmp_path / "market.duckdb"),
        UnexpectedProvider(),
        trade_date=date(2026, 7, 23),
        required_symbols=set(),
    )
    public_output = result.model_dump_json() + caplog.text

    assert result.failure_stage == "fetch"
    assert result.failure_class == "internal"
    assert result.retryable is False
    assert result.error_message == "market refresh failed"
    assert all(part not in public_output for part in secret_parts)


def test_refresh_cli_unexpected_failure_json_is_structured_and_sanitized(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    secret_parts = [
        "token=cli-secret",
        "https://provider.invalid/private",
        "SELECT secret FROM audit",
        "/private/market.duckdb",
    ]
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        MarketRefreshService,
        "refresh",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(" ".join(secret_parts))),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "refresh",
            "--date",
            "2026-07-23",
            "--symbols",
            "sh.600000",
        ],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["failure_stage"] == "fetch"
    assert payload["failure_class"] == "internal"
    assert payload["retryable"] is False
    assert all(part not in captured.out + captured.err for part in secret_parts)


def test_auto_refresh_cli_unexpected_failure_json_is_structured_and_sanitized(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    secret = "token=auto-secret https://provider.invalid/private /private/control.duckdb"
    settings = cli.get_settings().model_copy(
        update={
            "market_data_dir": tmp_path / "market",
            "user_data_dir": tmp_path / "user",
            "local_control_dir": tmp_path / "control",
            "local_staging_dir": tmp_path / "staging",
            "local_lock_dir": tmp_path / "locks",
            "local_temp_dir": tmp_path / "tmp",
            "nas_market_dataset_root": None,
            "local_market_dataset_root": None,
        }
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        cli.MarketAutomationService,
        "run_due_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(secret)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "auto-refresh-once", "--execute"],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["failure_stage"] == "fetch"
    assert payload["failure_class"] == "internal"
    assert payload["retryable"] is False
    assert secret not in captured.out + captured.err
