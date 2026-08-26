"""Task14 RED contract: all tests use an injected offline transport."""

import base64
import json
from datetime import date

import pytest

from backend.app.market.providers.http import HttpPolicy, ProviderHttpError
from backend.app.market.providers.tickflow import (
    TICKFLOW_SERVER,
    TickFlowAdapter,
    TickFlowCanaryRunner,
    TickFlowContract,
    TickFlowDiscoveryError,
)
from backend.app.market.shadow_evidence import ShadowEvidenceReader, ShadowEvidenceStore


class Response:
    def __init__(self, status_code=200, payload=None, *, url=None):
        self.status_code = status_code
        self.headers = {}
        self.content = json.dumps(payload if payload is not None else {}).encode()
        self.url = url or f"{TICKFLOW_SERVER}/v1/ok"


class Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        return self.responses.pop(0)


def _valid_payloads(trade_date=date(2026, 8, 20)):
    day = trade_date.isoformat()
    return [
        Response(payload={"data": [{"date": day, "symbol": "sh.600000", "close": 10.0}]}),
        Response(payload={"data": [{"date": day, "symbol": "sh.600000", "factor": 1.0}]}),
        Response(payload={"data": [{"symbol": "sh.600000", "date": day}]}),
        Response(payload={"data": [{"symbol": "sh.000001", "date": day}]}),
    ]


def test_task14_contract_has_exact_four_requests_and_fixed_server():
    contract = TickFlowContract()
    assert contract.server == TICKFLOW_SERVER == "https://api.tickflow.org"
    assert contract.endpoints == (
        "daily_batch",
        "ex_factors",
        "universe",
        "indexes",
    )
    plan = TickFlowAdapter().plan(date(2026, 8, 20), symbols=("sh.600000",) * 5)
    assert plan.request_count == 4
    assert tuple(item.endpoint for item in plan.requests) == contract.endpoints
    assert all(len(item.schema_contract_hash) == 64 for item in plan.requests)
    assert all(len(item.unit_contract_hash) == 64 for item in plan.requests)


def test_task14_exact_params_and_x_api_key_header_without_secret_in_report():
    transport = Transport(_valid_payloads())
    result = TickFlowCanaryRunner.offline(
        transport=transport,
        trade_date=date(2026, 8, 20),
        symbols=("sh.600000", "sz.000001", "sh.600004", "sz.000002", "sh.600006"),
        token="top-secret",
    )
    assert len(transport.calls) == 4
    assert [call[1] for call in transport.calls] == [
        "/v1/klines/batch",
        "/v1/klines/ex-factors",
        "/v1/universes/CN_Equity_A",
        "/v1/universes/CN_Index",
    ]
    assert transport.calls[0][2]["params"]["adjust"] == "none"
    assert transport.calls[0][2]["params"]["period"] == "1d"
    assert transport.calls[0][2]["headers"] == {"x-api-key": "top-secret"}
    assert result.request_count == 4
    assert "top-secret" not in result.model_dump_json()


def test_task14_http_policy_is_one_attempt_and_four_requests():
    policy = HttpPolicy(max_attempts=1, max_requests=4)
    assert policy.max_attempts == 1
    assert policy.max_requests == 4
    assert policy.connect_timeout_seconds == 5
    assert policy.read_timeout_seconds == 30


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_task14_status_failure_is_typed_and_no_retry(status):
    transport = Transport([Response(status_code=status)])
    with pytest.raises(ProviderHttpError) as exc:
        TickFlowCanaryRunner.offline(
            transport=transport,
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            token="secret",
        )
    assert exc.value.attempts == 1
    assert len(transport.calls) == 1
    assert "secret" not in str(exc.value)


def test_task14_unknown_units_are_discovery_not_complete():
    adapter = TickFlowAdapter()
    payloads = {
        "daily_batch": {"data": [{"date": "2026-08-20", "symbol": "sh.600000"}]},
        "ex_factors": {"data": [{"date": "2026-08-20", "symbol": "sh.600000"}]},
        "universe": {"data": [{"date": "2026-08-20", "symbol": "sh.600000"}]},
        "indexes": {"data": [{"date": "2026-08-20", "symbol": "sh.000001"}]},
    }
    parsed = adapter.parse(date(2026, 8, 20), payloads, request_count=4)
    assert parsed.units_status == "unknown"
    assert parsed.complete_candidate is False


def test_task14_missing_or_wrong_date_fails_closed():
    payloads = _valid_payloads()
    payloads[0] = Response(payload={"data": [{"date": "2026-08-19", "symbol": "sh.600000"}]})
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowCanaryRunner.offline(
            transport=Transport(payloads),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000",) * 5,
            token="secret",
        )


def test_task14_runner_default_disabled_stops_before_client_or_network(tmp_path):
    class Settings:
        provider_shadow_enabled = False
        provider_shadow_execute_enabled = True
        local_market_dataset_root = None
        nas_market_dataset_root = None

    class Layout:
        def validate_provider_shadow_root(self, *, canonical_roots):
            raise AssertionError("root gate must not run when disabled")

    class Registry:
        def read_status(self, _provider):
            raise AssertionError("registry gate must not run when disabled")

    called = []
    runner = TickFlowCanaryRunner(
        settings=Settings(),
        layout=Layout(),
        registry=Registry(),
        client_factory=lambda: called.append(True),
    )
    with pytest.raises(PermissionError):
        runner.execute(
            date(2026, 8, 20),
            symbols=("sh.600000",) * 5,
            external_authorization_id="auth-1",
            acknowledge_provider_requests=True,
        )
    assert called == []


@pytest.mark.parametrize("payload", [{"data": {"items": []}}, {"data": ["not-a-row"]}])
def test_task14_schema_drift_is_typed_and_stops_after_no_retry(payload):
    transport = Transport([Response(payload=payload), *_valid_payloads()[1:]])
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowCanaryRunner.offline(
            transport=transport,
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            token="secret",
        )
    assert len(transport.calls) == 1


def test_task14_redirect_and_oversize_are_fail_closed():
    class Redirect(Response):
        history = (object(),)

    with pytest.raises(ProviderHttpError) as redirect:
        TickFlowCanaryRunner.offline(
            transport=Transport(
                [Redirect(payload={"data": [{"ok": True}]})] + _valid_payloads()[1:]
            ),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            token="secret",
        )
    assert redirect.value.failure_class == "redirect_or_endpoint_mismatch"

    class Oversize(Response):
        pass

    oversize_response = Oversize(payload={"data": [{"ok": True}]})
    oversize_response.content = b"x" * (8 * 1024 * 1024 + 1)

    with pytest.raises(ProviderHttpError) as oversized:
        TickFlowCanaryRunner.offline(
            transport=Transport([oversize_response] + _valid_payloads()[1:]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            token="secret",
        )
    assert oversized.value.failure_class == "response_oversize"


def test_task14_raw_response_bytes_are_in_immutable_isolated_evidence(tmp_path):
    transport = Transport(_valid_payloads())
    first_raw = transport.responses[0].content
    store = ShadowEvidenceStore(tmp_path)
    result = TickFlowCanaryRunner.offline(
        transport=transport,
        trade_date=date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        token="secret",
        evidence_store=store,
    )
    assert result.writes_evidence is True
    bundle = ShadowEvidenceReader(tmp_path).read(result.evidence_id)
    page = next((tmp_path / "bundles" / result.evidence_id / "pages").glob("*.json"))
    encoded = json.loads(page.read_text())["raw_response_b64"]
    assert base64.b64decode(encoded) == first_raw
    assert bundle.evidence_id == result.evidence_id
