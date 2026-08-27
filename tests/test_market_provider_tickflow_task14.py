"""Task14 RED contract: all tests use an injected offline transport."""

import base64
import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.app.market.providers.http import (
    BoundedHttpClient,
    HttpPolicy,
    ProviderHttpError,
    _AuthorizedCanarySession,
    _StreamingHttpxResponse,
)
from backend.app.market.providers.registry import ShadowRegistry, exact_credential_env
from backend.app.market.providers.shadow_contracts import (
    AdmissionState,
    ShadowProviderRecord,
    TermsEvidence,
)
from backend.app.market.providers.tickflow import (
    TICKFLOW_SERVER,
    TickFlowAdapter,
    TickFlowCanaryRunner,
    TickFlowContract,
    TickFlowDiscoveryError,
    _official_universe,
    canonical_to_tickflow_symbol,
    tickflow_to_canonical_symbol,
)
from backend.app.market.shadow_evidence import (
    ShadowEvidencePublishError,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowEvidenceUnavailable,
)


class Response:
    def __init__(self, status_code=200, payload=None, *, url=None):
        self.status_code = status_code
        self.headers = {}
        self.content = json.dumps(payload if payload is not None else {}).encode()
        self.url = url or f"{TICKFLOW_SERVER}/v1/ok"
        self.history = ()
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


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

    def close(self):
        pass


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
            "volume": [1000],
            "amount": [20000.0],
        }
        for symbol in provider_symbols
    }
    factors = {symbol: [{"timestamp": start, "ex_factor": 1.0}] for symbol in provider_symbols}
    return [
        Response(payload={"data": daily}),
        Response(payload={"data": factors}),
        Response(
            payload={
                "data": {
                    "id": "CN_Equity_A",
                    "name": "CN Equity",
                    "region": "CN",
                    "category": "equity",
                    "symbol_count": len(provider_symbols),
                    "symbols": list(provider_symbols),
                    "description": None,
                }
            }
        ),
        Response(
            payload={
                "data": {
                    "id": "CN_Index",
                    "name": "CN Index",
                    "region": "CN",
                    "category": "index",
                    "symbol_count": 1,
                    "symbols": ["000001.SZ"],
                    "description": None,
                }
            }
        ),
    ]


def _task14_registry(tmp_path):
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
        official_url_allowlist=(
            "https://docs.tickflow.org/zh-Hans/api-reference/openapi.json",
            "https://tickflow.org/legal/terms-of-service.md",
        ),
        content_object_relpath="terms.txt",
        content_bytes=b"reviewed terms",
        contract_version="r2f3-terms-v1",
        as_of_date="2026-08-26",
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
    return registry


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


@pytest.mark.parametrize(
    "symbols",
    [
        ("bad", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        ("sh.600000", "sh.600001", "sh.600002", "sh.600003"),
        ("sh.600000", "sh.600000", "sh.600002", "sh.600003", "sh.600004"),
    ],
)
def test_task14_malformed_symbols_are_typed_and_sanitized(symbols):
    with pytest.raises(TickFlowDiscoveryError) as exc:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport(_valid_payloads()),
            trade_date=date(2026, 8, 20),
            symbols=symbols,
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert exc.value.failure_class in {
        "symbol_schema",
        "symbol_count_invalid",
        "symbol_not_main_board",
    }
    assert "bad" not in str(exc.value)


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


@pytest.mark.parametrize("timeout_error", ["read", "connect"])
def test_task14_httpx_timeout_exception_maps_to_timeout_attempt_one(timeout_error):
    import httpx

    class TimeoutTransport(Transport):
        def __init__(self):
            super().__init__([])

        def request(self, method, endpoint, **kwargs):
            self.calls.append((method, endpoint, kwargs))
            if timeout_error == "read":
                raise httpx.ReadTimeout("secret timeout", request=None)
            raise httpx.ConnectTimeout("secret timeout", request=None)

    transport = TimeoutTransport()
    with pytest.raises(ProviderHttpError) as exc:
        TickFlowCanaryRunner._offline_for_tests(
            transport=transport,
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert exc.value.failure_class == "timeout"
    assert exc.value.attempts == 1
    assert exc.value.request_count == 1
    assert "secret timeout" not in str(exc.value)


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


def test_task14_shadow_root_overlap_checks_control_and_registry_before_getenv(
    tmp_path, monkeypatch
):
    registry = SimpleNamespace(
        path=tmp_path / "control" / "provider-registry.sqlite3",
        read_status=lambda _provider: pytest.fail(
            "registry read must not occur after root overlap"
        ),
    )

    class Settings:
        provider_shadow_enabled = True
        provider_shadow_execute_enabled = True
        local_market_dataset_root = tmp_path / "canonical"
        nas_market_dataset_root = tmp_path / "nas"
        local_control_dir = tmp_path / "control"

    class Layout:
        provider_shadow_root = tmp_path / "shadow"

        def validate_provider_shadow_root(self, *, canonical_roots):
            assert canonical_roots == (
                tmp_path / "canonical",
                tmp_path / "nas",
                tmp_path / "control",
                tmp_path / "control",
            )
            raise PermissionError("shadow root overlaps canonical root")

    class SpyEnvironment(dict):
        lookups = 0

        def get(self, key, default=None):
            self.lookups += 1
            return super().get(key, default)

    spy_environment = SpyEnvironment(os.environ)
    monkeypatch.setattr(os, "environ", spy_environment)
    runner = TickFlowCanaryRunner(
        settings=Settings(),
        layout=Layout(),
        registry=registry,
        client_factory=lambda: pytest.fail("client constructed"),
    )
    with pytest.raises(PermissionError, match="shadow root overlaps canonical root"):
        runner.execute(
            date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            external_authorization_id="auth-overlap",
            acknowledge_provider_requests=True,
        )
    assert spy_environment.lookups == 0


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
    redirect_response = Response(payload={"data": [{"ok": True}]})
    redirect_response.history = (object(),)
    with pytest.raises(ProviderHttpError) as redirect:
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([redirect_response] + _valid_payloads()[1:]),
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


def test_task14_response_objects_close_on_success_and_failure():
    success_payloads = _valid_payloads()
    successful = success_payloads[0]
    TickFlowCanaryRunner._offline_for_tests(
        transport=Transport(success_payloads),
        trade_date=date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
    )
    assert successful.close_calls == 1

    failing = Response(payload={"data": []}, url="http://api.tickflow.org/v1/klines/batch")
    with pytest.raises(ProviderHttpError):
        TickFlowCanaryRunner._offline_for_tests(
            transport=Transport([failing] + _valid_payloads()[1:]),
            trade_date=date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        )
    assert failing.close_calls == 1


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


def test_task14_authorized_session_closes_owned_client_on_success_and_failure(tmp_path):
    registry = _task14_registry(tmp_path)

    class ClosableTransport(Transport):
        def __init__(self, responses):
            super().__init__(responses)
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    success_transport = ClosableTransport(_valid_payloads())
    session = __import__(
        "backend.app.market.providers.http", fromlist=["build_authorized_canary_session"]
    ).build_authorized_canary_session(
        "tickflow",
        registry,
        external_authorization_id="auth-close-success",
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        client_factory=lambda: success_transport,
    )
    TickFlowAdapter().execute(
        date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
        session=session,
    )
    assert success_transport.close_calls == 1

    failing_payloads = _valid_payloads()
    bad = json.loads(failing_payloads[1].content)
    bad["data"]["600000.SH"][0]["factor"] = 1.0
    failing_payloads[1] = Response(payload=bad)
    failing_transport = ClosableTransport(failing_payloads)
    failing_session = __import__(
        "backend.app.market.providers.http", fromlist=["build_authorized_canary_session"]
    ).build_authorized_canary_session(
        "tickflow",
        registry,
        external_authorization_id="auth-close-failure",
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        client_factory=lambda: failing_transport,
    )
    with pytest.raises(TickFlowDiscoveryError) as exc:
        TickFlowAdapter().execute(
            date(2026, 8, 20),
            symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
            session=failing_session,
        )
    assert exc.value.request_count == 4
    assert failing_transport.close_calls == 1


def official_payloads():
    start = 1787184000000
    compact = {
        "timestamp": [start],
        "open": [10.0],
        "high": [11.0],
        "low": [9.0],
        "close": [10.5],
        "volume": [1000],
        "amount": [20000.0],
    }
    return {
        "daily_batch": {"data": {"600000.SH": compact}},
        "ex_factors": {"data": {"600000.SH": [{"timestamp": start, "ex_factor": 1.0}]}},
        "universe": {
            "data": {
                "id": "CN_Equity_A",
                "name": "CN Equity",
                "region": "CN",
                "category": "equity",
                "symbol_count": 1,
                "symbols": ["600000.SH"],
                "description": None,
            }
        },
        "indexes": {
            "data": {
                "id": "CN_Index",
                "name": "CN Index",
                "region": "CN",
                "category": "index",
                "symbol_count": 1,
                "symbols": ["000001.SZ"],
                "description": None,
            }
        },
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


def test_task14_official_factor_entry_uses_ex_factor():
    parsed = TickFlowAdapter().parse(date(2026, 8, 20), official_payloads(), request_count=4)
    assert parsed.factors[0]["ex_factor"] == 1.0
    assert "factor" not in parsed.factors[0]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["ex_factors"]["data"]["600000.SH"][0].update({"factor": 1.0}),
        lambda p: p["universe"]["data"].pop("id"),
        lambda p: p["universe"]["data"].update({"description_extra": None}),
        lambda p: p["universe"]["data"].update({"symbol_count": 2}),
        lambda p: p["universe"]["data"].update({"symbols": [1]}),
        lambda p: p["indexes"]["data"].update({"id": "CN_Equity_A"}),
    ],
)
def test_task14_official_factor_and_universe_boundaries_are_rejected(mutate):
    payloads = official_payloads()
    mutate(payloads)
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowAdapter().parse(date(2026, 8, 20), payloads, request_count=4)


def test_task14_kline_allowlisted_optional_columns_are_preserved():
    payloads = official_payloads()
    compact = payloads["daily_batch"]["data"]["600000.SH"]
    compact.update(
        {
            "open_interest": [0.0],
            "prev_close": [9.5],
            "settlement_price": [10.25],
        }
    )
    parsed = TickFlowAdapter().parse(date(2026, 8, 20), payloads, request_count=4)
    assert parsed.daily[0]["open_interest"] == 0.0


@pytest.mark.parametrize(
    "optional_value",
    [[0.0, 1.0], [float("nan")], [None, 1.0]],
)
def test_task14_kline_optional_columns_require_strict_values(optional_value):
    payloads = official_payloads()
    payloads["daily_batch"]["data"]["600000.SH"]["open_interest"] = optional_value
    with pytest.raises(TickFlowDiscoveryError):
        TickFlowAdapter().parse(date(2026, 8, 20), payloads, request_count=4)


def test_task14_cli_preclient_failure_keeps_zero_provider_requests(tmp_path, monkeypatch, capsys):
    import backend.app.cli as cli_module
    import backend.app.market.providers.tickflow as tickflow_module

    monkeypatch.setattr(
        cli_module,
        "get_settings",
        lambda: SimpleNamespace(
            local_control_dir=tmp_path / "control",
            provider_registry_database_name="provider-registry.sqlite3",
            provider_shadow_root=tmp_path / "shadow",
            provider_evidence_root=tmp_path / "evidence",
        ),
    )

    def blocked_execute(self, *args, **kwargs):
        raise PermissionError("provider shadow execution is disabled")

    monkeypatch.setattr(tickflow_module.TickFlowCanaryRunner, "execute", blocked_execute)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--capability",
            "authenticated",
            "--date",
            "2026-08-20",
            "--symbols",
            "sh.600000",
            "sh.600001",
            "sh.600002",
            "sh.600003",
            "sh.600004",
            "--execute",
            "--external-authorization-id",
            "auth-cli-preclient",
            "--acknowledge-provider-requests",
        ],
    )
    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "unavailable"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False
    assert payload["error_code"] == "canary_execution_blocked"


def test_task14_cli_postclient_failure_reports_sanitized_failure_and_request_count(
    tmp_path, monkeypatch, capsys
):
    import backend.app.cli as cli_module
    import backend.app.market.providers.tickflow as tickflow_module

    monkeypatch.setattr(
        cli_module,
        "get_settings",
        lambda: SimpleNamespace(
            local_control_dir=tmp_path / "control",
            provider_registry_database_name="provider-registry.sqlite3",
            provider_shadow_root=tmp_path / "shadow",
            provider_evidence_root=tmp_path / "evidence",
        ),
    )

    def failing_execute(self, *args, **kwargs):
        raise ProviderHttpError(
            "timeout",
            endpoint="/v1/klines/batch",
            attempts=1,
            request_count=1,
        )

    monkeypatch.setattr(tickflow_module.TickFlowCanaryRunner, "execute", failing_execute)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--capability",
            "authenticated",
            "--date",
            "2026-08-20",
            "--symbols",
            "sh.600000",
            "sh.600001",
            "sh.600002",
            "sh.600003",
            "sh.600004",
            "--execute",
            "--external-authorization-id",
            "auth-cli-postclient",
            "--acknowledge-provider-requests",
        ],
    )
    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "unavailable"
    assert payload["provider_requests"] == 1
    assert payload["failure_class"] == "timeout"
    assert payload["endpoint"] == "/v1/klines/batch"
    assert payload["writes"] is False


def _strict_official_payloads():
    payloads = official_payloads()
    for compact in payloads["daily_batch"]["data"].values():
        compact["volume"] = [1000]
    return payloads


@pytest.mark.parametrize("variant", ["missing", "extra"])
def test_task14_factor_coverage_uses_map_keys_and_rejects_missing_or_extra(variant):
    payloads = _strict_official_payloads()
    factors = payloads["ex_factors"]["data"]
    if variant == "missing":
        factors.pop("600000.SH")
    else:
        factors["600099.SH"] = []
    with pytest.raises(TickFlowDiscoveryError) as exc:
        TickFlowAdapter().parse(
            date(2026, 8, 20),
            payloads,
            request_count=4,
            symbols=("sh.600000",),
        )
    assert exc.value.failure_class == "symbol_set_invalid"


def test_task14_factor_empty_lists_are_no_event_not_schema_error():
    payloads = _strict_official_payloads()
    for entries in payloads["ex_factors"]["data"].values():
        entries.clear()
    parsed = TickFlowAdapter().parse(
        date(2026, 8, 20),
        payloads,
        request_count=4,
        symbols=("sh.600000",),
    )
    assert parsed.factors == ()
    assert parsed.factor_status == "no_event"
    assert parsed.complete_candidate is False


@pytest.mark.parametrize("index_symbols", [[], [123]])
def test_task14_cn_index_empty_or_non_string_fails_closed(index_symbols):
    data = {
        "id": "CN_Index",
        "name": "CN Index",
        "region": "CN",
        "category": "index",
        "symbol_count": len(index_symbols),
        "symbols": index_symbols,
    }
    with pytest.raises(TickFlowDiscoveryError):
        _official_universe(data, expected_id="CN_Index")


def test_task14_factor_mixed_empty_and_nonempty_entries_preserve_coverage():
    payloads = _strict_official_payloads()
    daily = payloads["daily_batch"]["data"]
    daily["600001.SH"] = dict(daily["600000.SH"])
    payloads["universe"]["data"]["symbols"].append("600001.SH")
    payloads["universe"]["data"]["symbol_count"] += 1
    payloads["ex_factors"]["data"]["600001.SH"] = list(payloads["ex_factors"]["data"]["600000.SH"])
    payloads["ex_factors"]["data"]["600000.SH"].clear()
    parsed = TickFlowAdapter().parse(
        date(2026, 8, 20),
        payloads,
        request_count=4,
        symbols=("sh.600000", "sh.600001"),
    )
    assert len(parsed.factors) == 1
    assert parsed.factor_status == "unavailable"
    assert parsed.complete_candidate is False


@pytest.mark.parametrize("field_value", [1000.0, True, -1, 2**63])
def test_task14_kline_volume_requires_nonnegative_int64(field_value):
    payloads = _strict_official_payloads()
    payloads["daily_batch"]["data"]["600000.SH"]["volume"] = [field_value]
    with pytest.raises(TickFlowDiscoveryError) as exc:
        TickFlowAdapter().parse(date(2026, 8, 20), payloads, request_count=4)
    assert exc.value.failure_class == "numeric_value_invalid"


def test_task14_cli_exception_metadata_is_allowlisted(monkeypatch, capsys):
    import backend.app.cli as cli_module
    import backend.app.market.providers.tickflow as tickflow_module

    class MaliciousError(RuntimeError):
        failure_class = "https://attacker.invalid/?token=secret"
        endpoint = "/v1/klines/batch?authorization=secret"
        request_count = 4
        raw = "raw-token"

    monkeypatch.setattr(
        cli_module,
        "get_settings",
        lambda: (_ for _ in ()).throw(MaliciousError("raw URL token")),
    )
    monkeypatch.setattr(tickflow_module.TickFlowCanaryRunner, "execute", lambda *_a, **_k: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--capability",
            "authenticated",
            "--date",
            "2026-08-20",
            "--symbols",
            "sh.600000",
            "sh.600001",
            "sh.600002",
            "sh.600003",
            "sh.600004",
            "--execute",
            "--external-authorization-id",
            "auth-malicious",
            "--acknowledge-provider-requests",
        ],
    )
    assert cli_module.main() == 1
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["provider_requests"] == 0
    assert "failure_class" not in payload
    assert "endpoint" not in payload
    assert "secret" not in output


def test_task14_evidence_publish_errors_are_fixed_and_leave_no_bundle(tmp_path, monkeypatch):
    plan = TickFlowAdapter().plan(
        date(2026, 8, 20),
        symbols=("sh.600000", "sh.600001", "sh.600002", "sh.600003", "sh.600004"),
    )

    def fail_publish(*_args, **_kwargs):
        raise RuntimeError("https://provider.invalid/?token=secret")

    monkeypatch.setattr(ShadowEvidenceStore, "publish", fail_publish)
    store = ShadowEvidenceStore(tmp_path)
    with pytest.raises(ShadowEvidenceUnavailable) as exc:
        store.publish_raw_responses(
            plan=plan,
            response_bytes_by_ordinal={item.ordinal: b"{}" for item in plan.requests},
            evidence_id="task14-publish-failure",
        )
    assert getattr(exc.value, "failure_class", None) == "evidence_publish_error"
    assert getattr(exc.value, "request_count", None) == 4
    assert "secret" not in str(exc.value)
    assert not (tmp_path / "bundles" / "task14-publish-failure").exists()


def test_task14_cli_evidence_publish_failure_reports_four_requests(monkeypatch, capsys):
    import backend.app.cli as cli_module
    import backend.app.market.providers.tickflow as tickflow_module

    monkeypatch.setattr(cli_module, "get_settings", lambda: object())
    monkeypatch.setattr(
        cli_module,
        "StorageLayout",
        lambda _settings: type("Layout", (), {"provider_registry_database": "/tmp/unused"})(),
    )
    monkeypatch.setattr(
        tickflow_module.TickFlowCanaryRunner,
        "execute",
        lambda *_a, **_k: (_ for _ in ()).throw(ShadowEvidencePublishError()),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--capability",
            "authenticated",
            "--date",
            "2026-08-20",
            "--symbols",
            "sh.600000",
            "sh.600001",
            "sh.600002",
            "sh.600003",
            "sh.600004",
            "--execute",
            "--external-authorization-id",
            "auth-publish",
            "--acknowledge-provider-requests",
        ],
    )
    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_requests"] == 4
    assert payload["failure_class"] == "evidence_publish_error"
    assert "message" not in payload


def test_task14_streaming_response_close_is_exactly_once():
    class RawResponse:
        status_code = 200
        headers = {}
        url = f"{TICKFLOW_SERVER}/v1/klines/batch"
        history = ()

        def __init__(self):
            self.close_count = 0

        def iter_bytes(self):
            yield b"{}"

        def close(self):
            self.close_count += 1

    raw = RawResponse()
    wrapped = _StreamingHttpxResponse(raw)
    assert list(wrapped.iter_bytes()) == [b"{}"]
    wrapped.close()
    wrapped.close()
    assert raw.close_count == 1


@pytest.mark.parametrize("case", ["url", "history", "status", "oversize", "malformed", "success"])
def test_task14_bounded_http_closes_each_response_path_once(case):
    class PathResponse:
        status_code = 500 if case == "status" else 200
        headers = {}
        url = (
            "http://api.tickflow.org/v1/klines/batch"
            if case == "url"
            else f"{TICKFLOW_SERVER}/v1/klines/batch"
        )
        history = (object(),) if case == "history" else ()

        def __init__(self):
            self.close_count = 0

        def iter_bytes(self):
            if case == "malformed":
                yield "not-bytes"
            elif case == "oversize":
                yield b"{}x"
            else:
                yield b"{}"

        def close(self):
            self.close_count += 1

    response = PathResponse()

    class OneResponseTransport:
        def request(self, *_args, **_kwargs):
            return response

    try:
        BoundedHttpClient(
            OneResponseTransport(),
            policy=HttpPolicy(max_attempts=1, max_response_bytes=2),
            allowed_server=TICKFLOW_SERVER,
        ).request("/v1/klines/batch")
    except ProviderHttpError:
        pass
    assert response.close_count == 1


def test_task14_stream_timeout_is_typed_and_counted():
    import httpx

    class TimeoutResponse(Response):
        def iter_bytes(self):
            raise httpx.ReadTimeout("raw timeout token", request=None)
            yield b""

    response = TimeoutResponse(url=f"{TICKFLOW_SERVER}/daily")

    class OneResponseTransport(Transport):
        def request(self, method, endpoint, **kwargs):
            self.calls.append((method, endpoint, kwargs))
            response.url = f"{TICKFLOW_SERVER}{endpoint}"
            return response

    transport = OneResponseTransport([])
    with pytest.raises(ProviderHttpError) as exc:
        BoundedHttpClient(
            transport,
            policy=HttpPolicy(max_attempts=1, max_requests=4),
        ).request("/v1/klines/batch")
    assert exc.value.failure_class == "timeout"
    assert exc.value.attempts == 1
    assert exc.value.request_count == 1
    assert response.close_calls == 1


def test_task14_execute_empty_symbols_fails_before_client_creation():
    client_factory_calls = 0

    def client_factory():
        nonlocal client_factory_calls
        client_factory_calls += 1
        return Transport([])

    session = _AuthorizedCanarySession(
        provider_id="tickflow",
        registry_path="/does/not/exist",
        external_authorization_id="auth-empty",
        expected_state_version=0,
        expected_terms_evidence_hash="a" * 64,
        expected_terms_review_id="review",
        credential_env_name="STOCK_EVA_TICKFLOW_TOKEN",
        environ={"STOCK_EVA_TICKFLOW_TOKEN": "secret"},
        client_factory=client_factory,
    )
    with pytest.raises(PermissionError):
        TickFlowAdapter().execute(date(2026, 8, 20), symbols=(), session=session)
    assert client_factory_calls == 0
