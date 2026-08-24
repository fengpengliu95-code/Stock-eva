"""Offline RED/GREEN contract tests for the Tushare shadow adapter."""

from datetime import date

import pytest

from backend.app.market.providers.http import BoundedHttpClient, HttpPolicy
from backend.app.market.providers.tushare import TushareAdapter, TushareExecutionBlocked


class FakeResponse:
    status_code = 200
    headers = {}
    content = b"{}"

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeClient:
    def __init__(self):
        self.calls = []

    def request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        return FakeResponse({"data": {"fields": [], "items": []}})


def test_tushare_daily_units_are_raw_lots_and_thousand_cny():
    adapter = TushareAdapter(BoundedHttpClient(FakeClient()), plan_only=True)
    daily = adapter.source_contract("daily")
    assert daily.units["vol"] == "lots"
    assert daily.units["amount"] == "thousand_cny"
    assert daily.normalization_required is True


def test_tushare_daily_adj_factor_suspend_calendar_index_calls_share_session():
    client = FakeClient()
    adapter = TushareAdapter(BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)))
    result = adapter.fetch(date(2026, 8, 20), symbols=("000001.SZ",))
    assert result.request_count == 5
    assert {call[2]["json"]["trade_date"] for call in client.calls} == {"20260820"}
    assert [call[1] for call in client.calls] == [
        "daily",
        "adj_factor",
        "suspend_d",
        "trade_cal",
        "index_daily",
    ]


def test_tushare_execute_rejects_before_http_client_when_https_or_terms_unapproved():
    client = FakeClient()
    adapter = TushareAdapter(BoundedHttpClient(client), plan_only=True)
    with pytest.raises(TushareExecutionBlocked):
        adapter.execute(date(2026, 8, 20), symbols=("000001.SZ",))
    assert client.calls == []
