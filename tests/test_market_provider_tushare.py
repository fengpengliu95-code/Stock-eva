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


def test_tushare_static_https_gate_precedes_fake_http_transport():
    client = FakeClient()
    adapter = TushareAdapter(BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)))
    with pytest.raises(PermissionError):
        adapter.fetch(date(2026, 8, 20), symbols=("000001.SZ",))
    assert client.calls == []


def test_tushare_execute_rejects_before_http_client_when_https_or_terms_unapproved():
    client = FakeClient()
    adapter = TushareAdapter(BoundedHttpClient(client), plan_only=True)
    with pytest.raises(TushareExecutionBlocked):
        adapter.execute(date(2026, 8, 20), symbols=("000001.SZ",))
    assert client.calls == []


def test_tushare_boolean_authority_flags_are_not_constructor_arguments():
    with pytest.raises(TypeError):
        TushareAdapter(terms_approved=True, official_https_proof=True)
