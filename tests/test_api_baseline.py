import asyncio
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import httpx

import backend.app.cli as cli_module
from backend.app.api.market import get_market_store
from backend.app.cli import build_parser
from backend.app.config import Settings
from backend.app.market.store import MarketStore


def api_app():
    try:
        app_spec = importlib.util.find_spec("backend.app.main")
    except ModuleNotFoundError:
        app_spec = None

    assert app_spec is not None, "backend.app.main has not been implemented"

    from backend.app.main import app

    return app


def request(method: str, path: str, **kwargs) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=api_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(send())


def test_health_reports_service_identity() -> None:
    response = request("GET", "/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "stock-eva-api",
        "version": "0.1.0",
    }


def test_market_status_model_adds_defaulted_continuity_contract() -> None:
    from backend.app.market.models import MarketDataStatus

    status = MarketDataStatus(
        market_phase="closed",
        calendar_status="unavailable",
        latest_expected_session=None,
        published_as_of=None,
        refresh_state="disabled",
        last_success_at=None,
        next_retry_at=None,
    )

    assert status.continuity_status == "unavailable"
    assert status.continuity_start_date is None
    assert status.missing_session_count == 0
    assert status.oldest_missing_session is None
    assert status.repair_execution_enabled is False
    assert status.repair_pending_count == 0
    assert status.repair_retry_wait_count == 0
    assert status.repair_active_count == 0
    assert status.repair_dead_letter_count == 0
    assert status.active_lane is None
    assert status.continuity_reason_code is None


def test_market_continuity_cli_parser_accepts_bounded_enqueue_mode() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "market-continuity",
            "--start",
            "2026-08-03",
            "--end",
            "2026-08-13",
            "--execute",
        ]
    )

    assert args.command == "market-continuity"
    assert args.start.isoformat() == "2026-08-03"
    assert args.end.isoformat() == "2026-08-13"
    assert args.execute is True


def test_market_continuity_plan_from_empty_runtime_creates_no_paths(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    runtime = tmp_path / "runtime"
    settings = Settings(
        _env_file=None,
        market_data_dir=runtime / "market",
        local_control_dir=runtime / "control",
        local_staging_dir=runtime / "staging",
        local_lock_dir=runtime / "locks",
        local_temp_dir=runtime / "temp",
        market_continuity_start_date=date(2026, 8, 3),
    )
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-continuity"])

    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "unavailable"
    assert payload["provider_requests"] == 0
    assert payload["writes_control_state"] is False
    assert payload["writes_parquet"] is False
    assert not runtime.exists()


def test_market_continuity_invalid_range_with_execute_creates_no_paths(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    runtime = tmp_path / "runtime"
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset / "manifest.json").write_text(
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
    settings = Settings(
        _env_file=None,
        market_data_dir=runtime / "market",
        local_control_dir=runtime / "control",
        local_staging_dir=runtime / "staging",
        local_lock_dir=runtime / "locks",
        local_temp_dir=runtime / "temp",
        local_market_dataset_root=dataset,
        market_continuity_start_date=date(2026, 8, 10),
    )
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-continuity",
            "--start",
            "2026-08-09",
            "--execute",
        ],
    )

    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert payload["reason_code"] == "CONTINUITY_RANGE_INVALID"
    assert payload["provider_requests"] == 0
    assert not runtime.exists()


def test_market_summary_is_safe_before_data_is_loaded(tmp_path: Path) -> None:
    app = api_app()
    app.dependency_overrides[get_market_store] = lambda: MarketStore(
        tmp_path / "empty-market.duckdb"
    )
    try:
        response = request("GET", "/api/v1/market/summary")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "status": "empty",
        "as_of": None,
        "source": None,
        "completeness": {
            "requested": 0,
            "loaded": 0,
            "failed": 0,
            "failed_symbols": [],
            "coverage_ratio": None,
            "required_ratio": 1.0,
            "is_complete": False,
        },
        "freshness": {
            "evaluated": False,
            "expected_as_of": None,
            "is_fresh": None,
            "lag_calendar_days": None,
        },
        "breadth": {
            "advancing": 0,
            "declining": 0,
            "unchanged": 0,
            "suspended": 0,
            "eligible": 0,
        },
        "turnover": {
            "amount": None,
            "unit": "CNY",
            "coverage": 0,
        },
        "indexes": [],
        "quality_issues": ["no_market_data"],
    }


def test_development_frontend_origin_is_allowed() -> None:
    response = request(
        "OPTIONS",
        "/api/v1/health",
        headers={
            "Origin": "http://localhost:8080",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:8080"


def test_development_frontend_can_preflight_local_crud() -> None:
    for method in ("POST", "PUT", "DELETE"):
        response = request(
            "OPTIONS",
            "/api/v1/portfolio/positions",
            headers={
                "Origin": "http://127.0.0.1:8080",
                "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": "content-type",
            },
        )

        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == ("http://127.0.0.1:8080")
        assert method in response.headers["access-control-allow-methods"]
