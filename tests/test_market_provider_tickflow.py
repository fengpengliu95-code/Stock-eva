"""Offline RED/GREEN contract tests for the TickFlow shadow adapter."""

import copy
import hashlib
import json
from datetime import date

import pytest

from backend.app.market.providers.http import (
    AuthorizedCanarySession,
    BoundedHttpClient,
    CanaryPermissionError,
    HttpPolicy,
    ProviderHttpError,
    _CanaryPermit,
    build_authorized_canary_session,
)
from backend.app.market.providers.registry import ShadowRegistry, exact_credential_env
from backend.app.market.providers.shadow_contracts import (
    AdmissionState,
    ShadowProviderRecord,
    TermsEvidence,
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
    adapter = TickFlowAdapter()
    result = adapter.parse(
        date(2026, 8, 20),
        {
            "daily": {"data": [{"date": "2026-08-20", "symbol": "sh.600000", "close": 10}]},
            "universe": {"data": [{"symbol": "sh.600000"}]},
            "indexes": {"data": [{"symbol": "sh.000001"}]},
        },
        request_count=3,
    )
    assert result.daily[0]["close"] == 10
    assert result.adjusted is False
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
    adapter = TickFlowAdapter(plan_only=True)
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
    adapter = TickFlowAdapter()
    assert adapter.fetch(date(2026, 8, 20), symbols=("sh.600000",)).request_count == 3
    assert client.calls == []


def test_unissued_permit_constructor_object_new_and_copy_are_rejected_before_network():
    client = FakeClient([])
    with pytest.raises(PermissionError):
        _CanaryPermit("tickflow", "external")
    forged = object.__new__(_CanaryPermit)
    with pytest.raises((PermissionError, TypeError)):
        copy.copy(forged)
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


def test_authorized_session_revalidates_real_canary_registry_before_fake_transport(
    tmp_path, monkeypatch
):
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    registry = ShadowRegistry(tmp_path / "provider-registry.sqlite3")
    registry.initialize()
    registry.put_provider(
        ShadowProviderRecord(
            provider_id="tickflow",
            admission_state=AdmissionState.DISCOVERED,
            adapter_hash=TickFlowAdapter.ADAPTER_HASH,
            endpoint_contract_hash=TickFlowAdapter.ENDPOINT_CONTRACT_HASH,
            source_schema_hash=TickFlowAdapter.SOURCE_SCHEMA_HASH,
            normalizer_hash=digest("normalizer"),
            reconciliation_policy_hash=digest("policy"),
            credential_env_name=exact_credential_env("tickflow"),
            intended_use="internal research",
            retention_decision="local bounded",
            quota_contract="pending",
            required_fields_json="[]",
            unit_contract_json="{}",
            state_version=0,
            quarantine_reason=None,
        )
    )
    terms = TermsEvidence.build(
        terms_evidence_id="terms-tickflow",
        provider_id="tickflow",
        official_url_allowlist=("https://example.invalid/terms",),
        content_object_relpath="terms.txt",
        content_bytes=b"reviewed terms",
        contract_version="r2f3-terms-v1",
        as_of_date="2026-08-25",
        reviewer="reviewer-1",
        review_id="review-1",
        approved_intended_use="internal research",
        approved_retention="local bounded",
        approved_credential_mode="environment-only",
        approved_quota_decision="pending-canary",
    )
    registry.put_terms_evidence(terms)
    attached = registry.attach_terms_to_provider("tickflow", terms)
    registry.transition("tickflow", "canary", expected_state_version=attached.state_version)
    monkeypatch.setenv("STOCK_EVA_TICKFLOW_TOKEN", "fake-token")
    transport = FakeClient([FakeResponse(payload={"data": []}) for _ in range(3)])
    session = build_authorized_canary_session(
        "tickflow",
        registry,
        external_authorization_id="auth-round3",
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "fake-token"},
        client_factory=lambda: transport,
    )
    result = TickFlowAdapter().execute(date(2026, 8, 20), session=session)
    assert result.request_count == 3
    assert len(transport.calls) == 3
    assert all(
        call[2]["headers"] == {"Authorization": "Bearer fake-token"} for call in transport.calls
    )
    monkeypatch.setenv("STOCK_EVA_TICKFLOW_TOKEN", "fake-token-2")
    registry.transition("tickflow", "quarantined", expected_state_version=2)
    with pytest.raises(CanaryPermissionError):
        TickFlowAdapter().execute(date(2026, 8, 20), session=session)
    assert len(transport.calls) == 3


def test_authorized_session_has_no_public_constructor_or_duck_execution_authority():
    with pytest.raises(PermissionError):
        AuthorizedCanarySession(
            provider_id="tickflow",
            registry_path="/does/not/exist",
            external_authorization_id="auth-round4",
            expected_state_version=2,
            expected_terms_evidence_hash="a" * 64,
            expected_terms_review_id="review-1",
            credential_env_name="STOCK_EVA_TICKFLOW_TOKEN",
            environ={},
            client_factory=lambda: FakeClient([]),
        )

    class DuckSession:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("duck session must not execute")

    with pytest.raises((PermissionError, TypeError)):
        TickFlowAdapter().execute(date(2026, 8, 20), session=DuckSession())

    forged = object.__new__(AuthorizedCanarySession)
    object.__setattr__(forged, "provider_id", "tickflow")
    object.__setattr__(forged, "registry_path", "/does/not/exist")
    object.__setattr__(forged, "external_authorization_id", "auth-round4")
    with pytest.raises(PermissionError):
        TickFlowAdapter().execute(date(2026, 8, 20), session=forged)
