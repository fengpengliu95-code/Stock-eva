"""Task14 RED contract: all tests use an injected offline transport."""

import base64
import json
import os
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.app.market.providers.http import HttpPolicy, ProviderHttpError
from backend.app.market.providers.tickflow import (
    TICKFLOW_SERVER,
    TickFlowAdapter,
    TickFlowCanaryRunner,
    TickFlowContract,
    TickFlowDiscoveryError,
    canonical_to_tickflow_symbol,
    tickflow_to_canonical_symbol,
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
        response = self.responses.pop(0)
        if response.url == f"{TICKFLOW_SERVER}/v1/ok":
            response.url = f"{TICKFLOW_SERVER}{endpoint}"
        return response


def _valid_payloads(trade_date=date(2026, 8, 20)):
    start = int(datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)
    provider_symbols = (
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "600003.SH",
        "600004.SH",
    )
    daily = {
        symbol: {
            "timestamp": [start],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000.0],
            "amount": [20000.0],
        }
        for symbol in provider_symbols
    }
    factors = {symbol: [{"timestamp": start, "factor": 1.0}] for symbol in provider_symbols}
    return [
        Response(payload={"data": daily}),
        Response(payload={"data": factors}),
        Response(payload={"data": {"symbols": list(provider_symbols)}}),
        Response(payload={"data": {"symbols": ["000001.SZ"]}}),
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
    result = TickFlowCanaryRunner._offline_for_tests(
        transport=transport,
        trade_date=date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "top-secret"},
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
        TickFlowCanaryRunner._offline_for_tests(
            transport=transport,
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
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
    invalid_payload = json.loads(_valid_payloads()[0].content)
    invalid_payload["data"]["600000.SH"]["timestamp"] = [
        int(datetime.combine(date(2026, 8, 20), datetime.min.time(), tzinfo=UTC).timestamp() * 1000)
        - 86_400_000
    ]
    payloads[0] = Response(payload=invalid_payload)
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport(payloads),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
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


def test_task14_missing_terms_has_zero_credential_lookup_client_or_write(tmp_path, monkeypatch):
    registry = SimpleNamespace(
        read_status=lambda _provider: SimpleNamespace(
            admission_state=SimpleNamespace(value="canary")
        )
    )

    class Settings:
        provider_shadow_enabled = True
        provider_shadow_execute_enabled = True
        local_market_dataset_root = None
        nas_market_dataset_root = None

    class Layout:
        provider_shadow_root = tmp_path / "isolated-shadow"

        def validate_provider_shadow_root(self, *, canonical_roots):
            assert canonical_roots == ()

    class SpyEnvironment(dict):
        lookups = 0

        def get(self, key, default=None):
            self.lookups += 1
            return super().get(key, default)

    spy_environment = SpyEnvironment(os.environ)
    monkeypatch.setattr(os, "environ", spy_environment)
    import backend.app.market.providers.http as http_module

    def missing_terms(*_args, **_kwargs):
        raise PermissionError("reviewed terms evidence unavailable")

    monkeypatch.setattr(http_module, "_read_canary_descriptor", missing_terms)
    runner = TickFlowCanaryRunner(
        settings=Settings(),
        layout=Layout(),
        registry=registry,
        client_factory=lambda: (_ for _ in ()).throw(AssertionError("client constructed")),
    )
    with pytest.raises(PermissionError):
        runner.execute(
            date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            external_authorization_id="auth-terms-missing",
            acknowledge_provider_requests=True,
        )
    assert spy_environment.lookups == 0
    assert not (tmp_path / "isolated-shadow").exists()


@pytest.mark.parametrize("payload", [{"data": {"items": []}}, {"data": ["not-a-row"]}])
def test_task14_schema_drift_is_typed_and_stops_after_no_retry(payload):
    transport = Transport([Response(payload=payload), *_valid_payloads()[1:]])
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowCanaryRunner._offline_for_tests(
            transport=transport,
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert len(transport.calls) == 4


def test_task14_redirect_and_oversize_are_fail_closed():
    class Redirect(Response):
        history = (object(),)

    with pytest.raises(ProviderHttpError) as redirect:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport(
                [Redirect(payload={"data": [{"ok": True}]})] + _valid_payloads()[1:]
            ),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert redirect.value.failure_class == "redirect_or_endpoint_mismatch"

    class Oversize(Response):
        pass

    oversize_response = Oversize(payload={"data": [{"ok": True}]})
    oversize_response.content = b"x" * (8 * 1024 * 1024 + 1)

    with pytest.raises(ProviderHttpError) as oversized:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([oversize_response] + _valid_payloads()[1:]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert oversized.value.failure_class == "response_oversize"


@pytest.mark.parametrize(
    "evil_url",
    [
        "https://evil.example/v1/klines/batch",
        "https://api.tickflow.org.evil.example/v1/klines/batch",
        "https://user:secret@api.tickflow.org/v1/klines/batch",
        "http://api.tickflow.org/v1/klines/batch",
    ],
)
def test_task14_response_url_requires_exact_pinned_origin(evil_url):
    response = Response(payload={"data": []}, url=evil_url)
    with pytest.raises(ProviderHttpError) as exc:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([response] + _valid_payloads()[1:]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert exc.value.failure_class == "redirect_or_endpoint_mismatch"


def test_task14_streaming_response_is_bounded_before_content_access():
    class StreamingResponse:
        status_code = 200
        headers = {}
        history = ()
        url = f"{TICKFLOW_SERVER}/v1/klines/batch"

        @property
        def content(self):
            raise AssertionError("streaming response must not read content wholesale")

        def iter_bytes(self):
            yield b"x" * (8 * 1024 * 1024)
            yield b"x"

    with pytest.raises(ProviderHttpError) as exc:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([StreamingResponse()]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert exc.value.failure_class == "response_oversize"


def test_task14_raw_response_bytes_are_in_immutable_isolated_evidence(tmp_path):
    transport = Transport(_valid_payloads())
    first_raw = transport.responses[0].content
    store = ShadowEvidenceStore(tmp_path)
    result = TickFlowCanaryRunner._offline_for_tests(
        transport=transport,
        trade_date=date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        evidence_store=store,
    )
    assert result.writes_evidence is True
    bundle = ShadowEvidenceReader(tmp_path).read(result.evidence_id)
    page = next((tmp_path / "bundles" / result.evidence_id / "pages").glob("*.json"))
    encoded = json.loads(page.read_text())["raw_response_b64"]
    assert base64.b64decode(encoded) == first_raw
    assert bundle.evidence_id == result.evidence_id


def official_payloads():
    start = 1787184000000
    compact = {
        "timestamp": [start],
        "open": [10.0],
        "high": [11.0],
        "low": [9.0],
        "close": [10.5],
        "volume": [1000.0],
        "amount": [20000.0],
    }
    return {
        "daily_batch": {"data": {"600000.SH": compact}},
        "ex_factors": {"data": {"600000.SH": [{"timestamp": start, "factor": 1.0}]}},
        "universe": {"data": {"symbols": ["600000.SH"]}},
        "indexes": {"data": {"symbols": ["000001.SZ"]}},
    }


def test_task14_official_schema_maps_provider_symbols_without_changing_evidence_identity():
    assert tickflow_to_canonical_symbol("600000.SH") == "sh.600000"
    assert tickflow_to_canonical_symbol("000001.SZ") == "sz.000001"
    assert canonical_to_tickflow_symbol("sh.600000") == "600000.SH"
    parsed = TickFlowAdapter().parse(date(2026, 8, 20), official_payloads(), request_count=4)
    assert parsed.daily[0]["provider_symbol"] == "600000.SH"
    assert parsed.daily[0]["symbol"] == "sh.600000"
    assert parsed.complete_candidate is False


def test_task14_official_request_params_use_provider_symbols_and_no_universe_query():
    adapter = TickFlowAdapter()
    symbols = ("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004")
    assert adapter.request_params("daily_batch", date(2026, 8, 20), symbols)["symbols"] == (
        "600000.SH,600001.SH,600002.SH,600003.SH,600004.SH"
    )
    assert set(adapter.request_params("ex_factors", date(2026, 8, 20), symbols)) == {
        "symbols",
        "start_time",
        "end_time",
    }
    assert adapter.request_params("universe", date(2026, 8, 20), symbols) == {}
    assert adapter.request_params("indexes", date(2026, 8, 20), symbols) == {}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["daily_batch"]["data"]["600000.SH"].update({"extra": [1]}),
        lambda p: p["daily_batch"]["data"]["600000.SH"].update({"close": [10, 11]}),
        lambda p: p["daily_batch"]["data"].update({"600000.SH": {}}),
        lambda p: p["universe"]["data"].update({"extra": []}),
    ],
)
def test_task14_official_schema_drift_is_rejected(mutate):
    payloads = official_payloads()
    mutate(payloads)
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowAdapter().parse(date(2026, 8, 20), payloads, request_count=4)


def test_task14_missing_response_url_is_rejected_when_host_pinned():
    class MissingUrl(Response):
        url = None

    missing_url = MissingUrl(payload={"data": []})
    missing_url.url = None
    with pytest.raises(ProviderHttpError) as exc:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([missing_url] + _valid_payloads()[1:]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert exc.value.failure_class == "endpoint_url_unavailable"
