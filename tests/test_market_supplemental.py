from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market.akshare_supplemental import (
    AKShareSupplementalProvider,
    SupplementalDataError,
)

OBSERVED_AT = datetime(2026, 7, 24, 18, 30, tzinfo=UTC)


class FakeAKShareClient:
    def stock_industry_clf_hist_sw(self):
        return [
            {
                "symbol": "600000",
                "start_date": date(2021, 12, 13),
                "industry_code": "801780",
                "update_time": date(2024, 1, 2),
            },
            {
                "symbol": "000001",
                "start_date": date(2021, 12, 13),
                "industry_code": "801780",
                "update_time": date(2024, 1, 2),
            },
        ]

    def stock_market_fund_flow(self):
        return [
            {
                "日期": date(2026, 7, 23),
                "上证-收盘价": 3515.0,
                "深证-收盘价": 11220.0,
                "主力净流入-净额": 1_200_000_000,
                "主力净流入-净占比": 1.2,
                "超大单净流入-净额": 700_000_000,
                "超大单净流入-净占比": 0.7,
                "大单净流入-净额": 500_000_000,
                "大单净流入-净占比": 0.5,
                "中单净流入-净额": -300_000_000,
                "中单净流入-净占比": -0.3,
                "小单净流入-净额": -900_000_000,
                "小单净流入-净占比": -0.9,
            },
            {
                "日期": date(2026, 7, 24),
                "上证-收盘价": 3520.0,
                "深证-收盘价": 11250.0,
                "主力净流入-净额": 2_000_000_000,
                "主力净流入-净占比": 2.0,
                "超大单净流入-净额": 1_200_000_000,
                "超大单净流入-净占比": 1.2,
                "大单净流入-净额": 800_000_000,
                "大单净流入-净占比": 0.8,
                "中单净流入-净额": -500_000_000,
                "中单净流入-净占比": -0.5,
                "小单净流入-净额": -1_500_000_000,
                "小单净流入-净占比": -1.5,
            },
        ]

    def stock_sector_fund_flow_hist(self, *, symbol: str):
        assert symbol == "银行"
        return [
            {
                "日期": date(2026, 7, 23),
                "主力净流入-净额": 300_000_000,
                "主力净流入-净占比": 3.0,
                "超大单净流入-净额": 200_000_000,
                "超大单净流入-净占比": 2.0,
                "大单净流入-净额": 100_000_000,
                "大单净流入-净占比": 1.0,
                "中单净流入-净额": -50_000_000,
                "中单净流入-净占比": -0.5,
                "小单净流入-净额": -250_000_000,
                "小单净流入-净占比": -2.5,
            }
        ]


def provider(client=None) -> AKShareSupplementalProvider:
    return AKShareSupplementalProvider(
        client=client or FakeAKShareClient(),
        clock=lambda: OBSERVED_AT,
    )


def test_classification_history_preserves_source_dates_without_inventing_end_date() -> None:
    records = provider().classification_history(through_date=date(2026, 7, 24))

    assert [item.source_symbol for item in records] == ["000001", "600000"]
    assert records[0].effective_from == date(2021, 12, 13)
    assert records[0].source_updated_on == date(2024, 1, 2)
    assert records[0].industry_code == "801780"
    assert records[0].source == "akshare"
    assert records[0].upstream == "swsresearch"
    assert not hasattr(records[0], "effective_to")


def test_market_fund_flow_uses_explicit_dates_and_never_exposes_ohlcv() -> None:
    records = provider().market_fund_flow_history(
        through_date=date(2026, 7, 23)
    )

    assert len(records) == 1
    point = records[0]
    assert point.trade_date == date(2026, 7, 23)
    assert point.scope == "market"
    assert point.reported_main_net_inflow == 1_200_000_000
    assert point.upstream == "eastmoney"
    payload = point.model_dump()
    assert not {"open", "high", "low", "close", "volume"} & payload.keys()
    assert all(item.trade_date <= date(2026, 7, 23) for item in records)


def test_sector_fund_flow_preserves_upstream_reported_labels() -> None:
    records = provider().sector_fund_flow_history(
        "银行",
        through_date=date(2026, 7, 24),
    )

    assert len(records) == 1
    point = records[0]
    assert point.scope == "industry"
    assert point.scope_name == "银行"
    assert point.reported_main_net_inflow == 300_000_000
    assert point.observed_at == OBSERVED_AT


class MissingDateClient(FakeAKShareClient):
    def stock_market_fund_flow(self):
        return [{"日期": None, "主力净流入-净额": 1}]


def test_missing_source_date_is_rejected_instead_of_using_observation_date() -> None:
    with pytest.raises(SupplementalDataError, match="explicit source date"):
        provider(MissingDateClient()).market_fund_flow_history(
            through_date=date(2026, 7, 24)
        )


def api_settings(tmp_path: Path, *, enabled: bool) -> Settings:
    return Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        akshare_supplemental_enabled=enabled,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("enabled", "expected_status", "expected_issue"),
    [
        (False, "not_configured", "akshare_supplemental_disabled"),
        (True, "empty", "supplemental_ingestion_not_implemented"),
    ],
)
async def test_supplemental_api_never_fetches_on_get_and_is_explicitly_empty(
    tmp_path: Path,
    enabled: bool,
    expected_status: str,
    expected_issue: str,
) -> None:
    app.dependency_overrides[get_settings] = lambda: api_settings(
        tmp_path,
        enabled=enabled,
    )
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/api/v1/market/supplemental")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == expected_status
    assert payload["quality_issues"] == [expected_issue]
    assert payload["market_flow"] is None
    assert payload["sector_flows"] == []
    capabilities = {item["name"]: item for item in payload["capabilities"]}
    assert capabilities["market_fund_flow_history"]["date_semantics"] == (
        "explicit_trade_date"
    )
    assert capabilities["sector_fund_flow_rank"]["status"] == "rejected"


def test_workspace_has_supplemental_empty_state_and_loader() -> None:
    html = Path("workspace/index.html").read_text(encoding="utf-8")
    javascript = Path("workspace/app.js").read_text(encoding="utf-8")

    assert 'id="supplemental-status"' in html
    assert 'id="market-flow-summary"' in html
    assert 'id="sector-flow-body"' in html
    assert 'api("/market/supplemental")' in javascript
    assert "reported_main_net_inflow" in javascript
