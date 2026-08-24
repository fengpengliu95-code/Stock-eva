"""Offline RED/GREEN contract tests for the TickFlow shadow adapter."""

import copy
import json
from datetime import date

import pytest

from backend.app.market.providers.http import (
    BoundedHttpClient,
    HttpPolicy,
    ProviderHttpError,
    _CanaryPermit,
)
from backend.app.market.providers.tickflow import TickFlowAdapter


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = payload if isinstance(payload, bytes) else json.dumps(payload or {}).encode()
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        return self.responses.pop(0)


def test_tickflow_requests_unadjusted_daily_and_universe_without_inference():
    client = FakeClient(
        [
            FakeResponse(
                payload={"data": [{"date": "2026-08-20", "symbol": "sh.600000", "close": 10}]}
            ),
            FakeResponse(payload={"data": [{"symbol": "sh.600000"}]}),
            FakeResponse(payload={"data": [{"symbol": "sh.000001"}]}),
        ]
    )
    adapter = TickFlowAdapter(BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)))
    with pytest.raises(PermissionError):
        adapter.fetch(date(2026, 8, 20), symbols=("sh.600000",))
    assert client.calls == []


def test_tickflow_factor_or_corporate_action_requirement_is_explicit():
    adapter = TickFlowAdapter(plan_only=True)
    plan = adapter.plan(date(2026, 8, 20), symbols=("sh.600000",))
    assert plan.requires_factor_evidence is True
    assert plan.factor_status == "unavailable"


def test_http_client_has_bounded_timeout_retry_after_and_request_budget():
    client = FakeClient(
        [
            FakeResponse(status_code=429, headers={"Retry-After": "999"}),
            FakeResponse(status_code=200, payload={"data": []}),
        ]
    )
    bounded = BoundedHttpClient(
        client,
        policy=HttpPolicy(max_attempts=2, max_requests=2, max_retry_after_seconds=0),
    )
    assert bounded.request_json("daily") == {"data": []}
    assert bounded.request_count == 2
    assert client.calls[0][2]["timeout"]["read"] <= 30


def test_canary_plan_makes_zero_network_calls(tmp_path):
    client = FakeClient([])
    adapter = TickFlowAdapter(BoundedHttpClient(client), plan_only=True)
    plan = adapter.plan(date(2026, 8, 20), symbols=("sh.600000",))
    assert plan.request_count >= 1
    assert client.calls == []
    assert list(tmp_path.iterdir()) == []


def test_http_or_token_never_enters_exception_or_evidence():
    client = FakeClient([FakeResponse(status_code=401, payload={"token": "secret"})])
    with pytest.raises(ProviderHttpError) as captured:
        BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)).request_json("daily")
    assert "secret" not in str(captured.value)
    assert "http" not in str(captured.value).lower()


def test_public_tickflow_fetch_is_plan_only_and_never_networks_without_permit():
    client = FakeClient([])
    adapter = TickFlowAdapter(BoundedHttpClient(client))
    with pytest.raises(PermissionError):
        adapter.fetch(date(2026, 8, 20), symbols=("sh.600000",))
    assert client.calls == []


def test_unissued_permit_constructor_object_new_and_copy_are_rejected_before_network():
    client = FakeClient([])
    adapter = TickFlowAdapter(BoundedHttpClient(client))
    with pytest.raises(PermissionError):
        _CanaryPermit("tickflow", "external")
    forged = object.__new__(_CanaryPermit)
    with pytest.raises(PermissionError):
        adapter.fetch(date(2026, 8, 20), permit=forged)
    with pytest.raises((PermissionError, TypeError)):
        adapter.fetch(date(2026, 8, 20), permit=copy.copy(forged))
    assert client.calls == []


def test_http_uses_bounded_content_bytes_not_response_json_method():
    class LyingResponse(FakeResponse):
        def json(self):
            return {"token": "secret", "wrong": True}

    client = FakeClient([LyingResponse(payload={"data": [{"ok": True}]})])
    result = BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)).request_json("daily")
    assert result == {"data": [{"ok": True}]}


def test_http_rejects_empty_content_even_when_json_method_claims_payload():
    class EmptyContentResponse(FakeResponse):
        content = b""

        def json(self):
            return {"data": []}

    response = EmptyContentResponse(payload={"data": []})
    response.content = b""
    client = FakeClient([response])
    with pytest.raises(ProviderHttpError) as captured:
        BoundedHttpClient(client, policy=HttpPolicy(max_attempts=1)).request_json("daily")
    assert captured.value.failure_class == "malformed_json"
