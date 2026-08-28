from __future__ import annotations

import fcntl
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import cli
from backend.app.api import market as market_api
from backend.app.config import Settings, TickFlowFreeRuntimeSettings, get_settings
from backend.app.main import app
from backend.app.market import daily_shadow_canonical as canonical_module
from backend.app.market.daily_shadow_models import (
    CanonicalDailyOhlcRow,
    DailyCanonicalReadResult,
    DailyCanonicalSnapshot,
    DailyShadowFetchObservation,
    DailyShadowFetchResult,
    DailyShadowSourceRow,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)
from backend.app.market.daily_shadow_registry import (
    DAILY_SHADOW_TERMS_CONTRACT_VERSION,
    DailyShadowContract,
    DailyShadowRegistry,
)
from backend.app.market.providers.shadow_contracts import TermsEvidence
from tests.test_market_daily_shadow_canonical import TRADE_DATE, _write_dataset
from tests.test_market_shadow_jobs import _calendar_root


def _terms() -> TermsEvidence:
    return TermsEvidence.build(
        content_bytes=b"reviewed TickFlow Free Daily Bar terms",
        official_url_allowlist=("https://free-api.tickflow.org",),
        terms_evidence_id="tickflow-free-daily-cli-api-20260828",
        provider_id="tickflow",
        content_object_relpath="terms/tickflow-free-daily-cli-api-20260828.txt",
        contract_version=DAILY_SHADOW_TERMS_CONTRACT_VERSION,
        as_of_date="2026-08-28",
        reviewer="stock-eva-owner",
        review_id="r2f3-free-daily-cli-api-review-20260828",
        approved_intended_use="free-historical-daily-ohlc-shadow-only",
        approved_retention="final-success-source-evidence-only",
        approved_credential_mode="credentialless-free",
        approved_quota_decision="unqualified-max-40-sequential-one-attempt",
    )


def _runtime(tmp_path: Path, dataset: Path, *, initialize: bool = True):
    shadow_root = tmp_path / "shadow"
    control = tmp_path / "control"
    shadow_root.mkdir(mode=0o700)
    control.mkdir(mode=0o700)
    calendar_parent = tmp_path / "calendar-parent"
    calendar_parent.mkdir()
    calendar_root = _calendar_root(calendar_parent).resolve()
    settings = TickFlowFreeRuntimeSettings(
        provider_shadow_enabled=True,
        provider_shadow_execute_enabled=True,
        provider_shadow_root=shadow_root.resolve(),
        provider_evidence_root=(tmp_path / "provider-evidence").resolve(),
        local_control_dir=control.resolve(),
        local_market_dataset_root=dataset.resolve(),
        nas_market_dataset_root=None,
        daily_bar_shadow_calendar_root=calendar_root,
    )
    registry = DailyShadowRegistry(control / settings.daily_bar_shadow_database_name)
    if initialize:
        terms = _terms()
        registry.initialize(DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256), terms)
    return settings, registry


def _fetch(plan) -> DailyShadowFetchResult:
    timestamp = int(
        datetime.combine(plan.trade_date, datetime.min.time(), tzinfo=UTC).timestamp() * 1000
    )
    prices = {
        "sh.600000": (10.0, 10.4, 9.9, 10.2),
        "sz.000001": (10.0, 10.4, 9.9, 10.2),
    }
    rows = tuple(
        DailyShadowSourceRow(
            trade_date=plan.trade_date,
            timestamp=timestamp,
            provider_symbol=canonical_to_tickflow_daily_symbol(symbol),
            symbol=symbol,
            open=prices[symbol][0],
            high=prices[symbol][1],
            low=prices[symbol][2],
            close=prices[symbol][3],
            volume=1000,
            amount=10000.0,
        )
        for shard in plan.shards
        for symbol in shard.canonical_symbols
    )
    return DailyShadowFetchResult(
        status="ready",
        trade_date=plan.trade_date,
        request_plan_sha256=plan.request_plan_sha256,
        request_count=len(plan.shards),
        rows=rows,
        observations=tuple(
            DailyShadowFetchObservation(
                ordinal=shard.ordinal,
                outcome="SUCCESS",
                elapsed_ms=1,
                response_bytes=100,
                expected_rows=len(shard.canonical_symbols),
                observed_rows=len(shard.canonical_symbols),
            )
            for shard in plan.shards
        ),
    )


def test_daily_cli_parser_is_closed_to_tickflow_and_requires_explicit_date():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
        ]
    )
    assert args.provider == "tickflow"
    assert args.trade_date == TRADE_DATE
    assert args.execute is False


def test_daily_cli_plan_is_zero_network_zero_write_and_hash_bound(tmp_path, monkeypatch, capsys):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    settings, registry = _runtime(tmp_path, dataset)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
        ],
    )

    assert cli.main() == 0

    payload = json.loads(capsys.readouterr().out)
    after = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    assert payload["status"] == "planned"
    assert payload["profile"] == "TICKFLOW_FREE_DAILY_BAR_OHLC_V1"
    assert payload["expected_symbols"] == 2
    assert payload["request_count"] == 1
    assert len(payload["request_plan_sha256"]) == 64
    assert len(payload["canonical_snapshot_sha256"]) == 64
    assert len(payload["descriptor_sha256"]) == 64
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False
    assert payload["publication_enabled"] is False
    assert payload["failover_enabled"] is False
    assert registry.read().epoch_count == 0
    assert before == after


def test_daily_cli_execute_rejects_missing_gates_before_provider(tmp_path, monkeypatch, capsys):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    settings, registry = _runtime(tmp_path, dataset)
    provider_calls = 0

    def forbidden_execute(**_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("provider must remain behind execute gates")

    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        "backend.app.market.providers.tickflow_daily_shadow.TickFlowFreeDailyShadowFetcher.execute",
        forbidden_execute,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
            "--execute",
        ],
    )

    assert cli.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "external_authorization_required"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False
    assert provider_calls == 0
    assert registry.read().epoch_count == 0


def test_daily_cli_acknowledgement_gate_precedes_settings_and_filesystem(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "get_tickflow_free_runtime_settings",
        lambda: (_ for _ in ()).throw(AssertionError("settings gate must not run")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
            "--execute",
            "--external-authorization-id",
            "approved-daily-shadow-once",
        ],
    )

    assert cli.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "provider_requests_acknowledgement_required"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False


def test_daily_cli_missing_sidecar_plan_is_zero_write(tmp_path, monkeypatch, capsys):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    settings, registry = _runtime(tmp_path, dataset, initialize=False)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "control_state_unavailable"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False
    assert not registry.path.exists()
    assert before == {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}


def test_daily_cli_disabled_execute_and_missing_calendar_stop_before_provider(
    tmp_path, monkeypatch, capsys
):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    settings, registry = _runtime(tmp_path, dataset)
    provider_calls = 0

    def forbidden_execute(**_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("blocked execute must not call provider")

    monkeypatch.setattr(
        "backend.app.market.providers.tickflow_daily_shadow.TickFlowFreeDailyShadowFetcher.execute",
        forbidden_execute,
    )
    disabled = TickFlowFreeRuntimeSettings.model_validate(
        {
            **settings.model_dump(mode="python"),
            "provider_shadow_execute_enabled": False,
        }
    )
    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: disabled)
    argv = [
        "stock-eva",
        "market-provider-daily-shadow",
        "--provider",
        "tickflow",
        "--date",
        TRADE_DATE.isoformat(),
        "--execute",
        "--external-authorization-id",
        "approved-daily-shadow-once",
        "--acknowledge-provider-requests",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert cli.main() == 2
    disabled_payload = json.loads(capsys.readouterr().out)
    assert disabled_payload["error_code"] == "daily_shadow_execution_disabled"

    no_calendar = TickFlowFreeRuntimeSettings.model_validate(
        {**settings.model_dump(mode="python"), "daily_bar_shadow_calendar_root": None}
    )
    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: no_calendar)

    assert cli.main() == 1
    calendar_payload = json.loads(capsys.readouterr().out)
    assert calendar_payload["error_code"] == "calendar_unavailable"
    assert provider_calls == 0
    assert registry.read().epoch_count == 0


def test_daily_cli_fake_execute_writes_only_daily_shadow_lane(tmp_path, monkeypatch, capsys):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    settings, registry = _runtime(tmp_path, dataset)
    canonical_before = {path: path.read_bytes() for path in dataset.rglob("*") if path.is_file()}
    monkeypatch.setattr(cli, "get_tickflow_free_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        "backend.app.market.providers.tickflow_daily_shadow.TickFlowFreeDailyShadowFetcher.execute",
        lambda **kwargs: _fetch(kwargs["plan"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            TRADE_DATE.isoformat(),
            "--execute",
            "--external-authorization-id",
            "approved-daily-shadow-once",
            "--acknowledge-provider-requests",
        ],
    )

    assert cli.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "SUCCESS"
    assert payload["provider_requests"] == 1
    assert payload["canonical_writes"] is False
    assert payload["publication_enabled"] is False
    assert payload["failover_enabled"] is False
    assert registry.read().window.consecutive_sessions == 1
    assert not (settings.local_control_dir / settings.provider_registry_database_name).exists()
    assert {
        path: path.read_bytes() for path in dataset.rglob("*") if path.is_file()
    } == canonical_before
    api_settings = Settings(
        _env_file=None,
        local_control_dir=settings.local_control_dir,
        provider_shadow_root=settings.provider_shadow_root,
        provider_evidence_root=settings.provider_evidence_root,
        local_market_dataset_root=settings.local_market_dataset_root,
        nas_market_dataset_root=None,
    )
    status = market_api.market_provider_daily_bar_shadow(settings=api_settings, provider="tickflow")
    assert status.status == "ready"
    assert status.state == "OBSERVING"
    assert status.last_outcome == "SUCCESS"
    assert status.consecutive_sessions == 1
    assert len(status.version_vector_sha256) == 64
    assert status.circuit_state == "CLOSED"

    first_snapshot = (
        canonical_module.DailyCanonicalReader(dataset, trade_date=TRADE_DATE).read().snapshot
    )
    assert first_snapshot is not None
    next_date = TRADE_DATE + timedelta(days=1)
    next_rows = tuple(
        CanonicalDailyOhlcRow(
            trade_date=next_date,
            symbol=row.symbol,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
        )
        for row in first_snapshot.rows
    )
    next_snapshot = DailyCanonicalSnapshot.model_validate(
        {
            **first_snapshot.model_dump(mode="python"),
            "trade_date": next_date,
            "manifest_generation": "generation-next-session",
            "manifest_sha256": domain_sha256(
                "stock-eva/test/daily-cli-manifest/v1", next_date.isoformat()
            ),
            "partition_relative_path": (
                f"bars/source=baostock/date={next_date.isoformat()}.parquet"
            ),
            "partition_sha256": domain_sha256(
                "stock-eva/test/daily-cli-partition/v1", next_date.isoformat()
            ),
            "ohlc_sha256": domain_sha256(
                "stock-eva/test/daily-cli-ohlc/v1",
                tuple(row.model_dump(mode="json") for row in next_rows),
            ),
            "rows": next_rows,
            "snapshot_sha256": "0" * 64,
        }
    )

    class NextSessionReader:
        def __init__(self, _root, *, trade_date):
            assert trade_date == next_date

        def read(self):
            return DailyCanonicalReadResult(status="ready", snapshot=next_snapshot)

        def verify(self):
            return DailyCanonicalReadResult(status="ready", snapshot=next_snapshot)

    monkeypatch.setattr(canonical_module, "DailyCanonicalReader", NextSessionReader)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-daily-shadow",
            "--provider",
            "tickflow",
            "--date",
            next_date.isoformat(),
            "--execute",
            "--external-authorization-id",
            "approved-daily-shadow-two",
            "--acknowledge-provider-requests",
        ],
    )

    assert cli.main() == 0
    second = json.loads(capsys.readouterr().out)
    assert second["outcome"] == "SUCCESS"
    assert second["consecutive_sessions"] == 2
    assert registry.read().epoch_count == 1
    assert registry.read().window.consecutive_sessions == 2


def test_daily_api_missing_and_ready_sidecar_are_read_only(tmp_path):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    runtime = tmp_path / "runtime"
    shadow = tmp_path / "shadow"
    control = runtime / "control"
    shadow.mkdir(mode=0o700)
    control.mkdir(parents=True, mode=0o700)
    settings = Settings(
        _env_file=None,
        local_control_dir=control,
        provider_shadow_root=shadow.resolve(),
        local_market_dataset_root=dataset.resolve(),
        nas_market_dataset_root=None,
    )
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}

    missing = market_api.market_provider_daily_bar_shadow(settings=settings, provider="tickflow")

    assert missing.status == "unavailable"
    assert missing.state == "UNAVAILABLE"
    assert missing.unavailable_reason == "CONTROL_STATE_UNAVAILABLE"
    assert before == {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}

    terms = _terms()
    registry = DailyShadowRegistry(control / settings.daily_bar_shadow_database_name)
    registry.initialize(DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256), terms)
    ready = market_api.market_provider_daily_bar_shadow(settings=settings, provider="tickflow")

    assert ready.status == "ready"
    assert ready.state == "PENDING"
    assert ready.consecutive_sessions == 0
    assert ready.required_sessions == 20
    assert ready.units_state == "UNKNOWN"
    assert ready.suspension_semantics_state == "UNKNOWN"
    assert ready.factor_evidence_state == "UNQUALIFIED"
    assert ready.daily_bar_qualified is False
    assert ready.adjustment_factor_qualified is False
    assert ready.publication_eligible is False
    assert ready.failover_enabled is False
    assert not hasattr(ready, "rows")
    assert not hasattr(ready, "symbols")


def test_daily_api_locked_sidecar_and_http_route_fail_closed_without_writes(tmp_path):
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    shadow = tmp_path / "shadow"
    control = tmp_path / "control"
    shadow.mkdir(mode=0o700)
    control.mkdir(mode=0o700)
    settings = Settings(
        _env_file=None,
        local_control_dir=control,
        provider_shadow_root=shadow.resolve(),
        provider_evidence_root=(tmp_path / "provider-evidence").resolve(),
        local_market_dataset_root=dataset.resolve(),
        nas_market_dataset_root=None,
    )
    terms = _terms()
    registry = DailyShadowRegistry(control / settings.daily_bar_shadow_database_name)
    registry.initialize(DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256), terms)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    lock_fd = os.open(registry.lock_path, os.O_RDONLY)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = market_api.market_provider_daily_bar_shadow(settings=settings, provider="tickflow")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
    assert locked.status == "unavailable"
    assert locked.unavailable_reason == "CONTROL_STATE_UNAVAILABLE"
    assert before == {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}

    app.dependency_overrides[get_settings] = lambda: settings
    try:
        response = TestClient(app).get(
            "/api/v1/market/provider-daily-bar-shadow",
            params={"provider": "tickflow"},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["state"] == "PENDING"
    assert "rows" not in body
    assert "symbols" not in body
    assert "url" not in json.dumps(body).lower()
