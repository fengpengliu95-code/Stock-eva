import asyncio
import importlib.util
from pathlib import Path

import httpx

from backend.app.api.market import get_market_store
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
