"""R2-F3 Task14 Free discovery RED/GREEN contract; no test opens a socket."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.market.providers.http import build_tickflow_free_client
from backend.app.market.providers.registry import ShadowRegistry, exact_credential_env
from backend.app.market.providers.shadow_contracts import (
    AdmissionState,
    ShadowProviderRecord,
    TermsEvidence,
)
from backend.app.market.providers.tickflow import (
    TICKFLOW_FREE_PROVIDER_SYMBOLS,
    TICKFLOW_FREE_REVIEW_URLS,
    TICKFLOW_FREE_SERVER,
    TICKFLOW_FREE_TERMS_CONTRACT_VERSION,
    TickFlowAdapter,
    TickFlowCapabilityState,
    TickFlowFreeAdapter,
    TickFlowFreeCanaryReport,
    TickFlowFreeCanaryRunner,
    TickFlowFreeDiscoveryError,
    initialize_tickflow_free_sdk,
)

TRADE_DATE = date(2026, 8, 20)


class Response:
    def __init__(self, payload, *, endpoint: str, status_code: int = 200):
        self.status_code = status_code
        self.headers = {}
        self.content = json.dumps(payload).encode()
        self.url = f"{TICKFLOW_FREE_SERVER}{endpoint}"
        self.history = ()
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.close_calls = 0

    def request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        return self.responses.pop(0)

    def close(self):
        self.close_calls += 1


class FakeSdk:
    factory_calls = []
    instances = []

    def __init__(self, api_key=None, **kwargs):
        if api_key is None:
            api_key = os.environ.get("TICKFLOW_API_KEY")
        self.api_key = api_key
        self.base_url = kwargs["base_url"]
        self.close_calls = 0
        self.resource_calls = 0
        type(self).instances.append(self)

    @classmethod
    def free(cls, **kwargs):
        cls.factory_calls.append(dict(kwargs))
        print("sdk free notice that must be suppressed")
        return cls(api_key=None, **kwargs)

    def close(self):
        self.close_calls += 1


@pytest.fixture(autouse=True)
def reset_fake_sdk():
    FakeSdk.factory_calls = []
    FakeSdk.instances = []


def _valid_payloads(trade_date: date = TRADE_DATE):
    start = int(datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)
    instruments = [
        {
            "symbol": symbol,
            "code": symbol[:6],
            "exchange": symbol[-2:],
            "region": "CN",
            "name": f"sample-{index}",
            "type": "stock",
            "ext": None,
        }
        for index, symbol in enumerate(TICKFLOW_FREE_PROVIDER_SYMBOLS)
    ]
    klines = {
        symbol: {
            "timestamp": [start],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [20000.0],
        }
        for symbol in TICKFLOW_FREE_PROVIDER_SYMBOLS
    }
    return [
        Response(
            {
                "data": [
                    {"exchange": "SH", "region": "CN", "count": 2500},
                    {"exchange": "SZ", "region": "CN", "count": 2800},
                ]
            },
            endpoint="/v1/exchanges",
        ),
        Response({"data": instruments}, endpoint="/v1/instruments"),
        Response(
            {
                "data": {
                    "id": "CN_Equity_A",
                    "name": "CN Equity A",
                    "region": "CN",
                    "category": "equity",
                    "symbol_count": len(TICKFLOW_FREE_PROVIDER_SYMBOLS),
                    "symbols": list(TICKFLOW_FREE_PROVIDER_SYMBOLS),
                    "description": None,
                }
            },
            endpoint="/v1/universes/CN_Equity_A",
        ),
        Response({"data": klines}, endpoint="/v1/klines/batch"),
    ]


def _run(transport: Transport):
    return TickFlowFreeCanaryRunner._offline_for_tests(
        transport=transport,
        trade_date=TRADE_DATE,
        sdk_class=FakeSdk,
        sdk_version="0.1.24",
    )


def _task14_registry(
    tmp_path: Path, *, contract_version: str = TICKFLOW_FREE_TERMS_CONTRACT_VERSION
) -> ShadowRegistry:
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
        terms_evidence_id="terms-tickflow-free",
        provider_id="tickflow",
        official_url_allowlist=(*TICKFLOW_FREE_REVIEW_URLS,),
        content_object_relpath="terms.txt",
        content_bytes=b"reviewed terms",
        contract_version=contract_version,
        as_of_date="2026-08-27",
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


def test_free_plan_is_fixed_five_symbol_four_request_contract():
    adapter = TickFlowFreeAdapter()
    plan = adapter.plan(TRADE_DATE)

    assert plan.provider_mode == "FREE_DAILY_DISCOVERY"
    assert plan.fixed_symbols == TICKFLOW_FREE_PROVIDER_SYMBOLS
    assert plan.request_count == 4
    assert tuple(item.endpoint for item in plan.requests) == (
        "connectivity",
        "instrument_metadata",
        "universe_metadata",
        "historical_daily_1d",
    )
    with pytest.raises(TickFlowFreeDiscoveryError, match="fixed Free sample"):
        adapter.plan(TRADE_DATE, symbols=("sh.600000",))


def test_official_free_sdk_is_pinned_credentialless_silent_and_closed(capsys, tmp_path):
    os.environ["TICKFLOW_API_KEY"] = "must-not-be-used"
    try:
        probe = initialize_tickflow_free_sdk(
            sdk_class=FakeSdk,
            sdk_version="0.1.24",
            cache_dir=tmp_path / "must-remain-absent",
        )
    finally:
        os.environ.pop("TICKFLOW_API_KEY", None)

    assert probe.sdk_version == "0.1.24"
    assert probe.base_url == TICKFLOW_FREE_SERVER
    assert FakeSdk.factory_calls == [
        {
            "base_url": TICKFLOW_FREE_SERVER,
            "timeout": 30.0,
            "max_retries": 0,
            "cache_dir": str(tmp_path / "must-remain-absent"),
        }
    ]
    assert FakeSdk.instances[0].api_key == ""
    assert FakeSdk.instances[0].close_calls == 1
    assert not (tmp_path / "must-remain-absent").exists()
    assert capsys.readouterr().out == ""


def test_production_sdk_probe_uses_isolated_closed_environment(monkeypatch):
    import subprocess

    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout="FREE_SDK_OK\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    probe = initialize_tickflow_free_sdk(sdk_version="0.1.24")

    assert probe.provider_requests == 0
    argv, kwargs = calls[0]
    assert argv[1:3] == ["-I", "-B"]
    assert kwargs["env"] == {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"}
    assert "STOCK_EVA_TICKFLOW_TOKEN" not in kwargs["env"]
    assert "TICKFLOW_API_KEY" not in kwargs["env"]
    assert kwargs["stdin"] is subprocess.DEVNULL


def test_free_http_factory_disables_environment_proxy_lookup(monkeypatch):
    import httpx

    calls = []

    class Client:
        def close(self):
            pass

    monkeypatch.setattr(httpx, "Client", lambda **kwargs: calls.append(kwargs) or Client())
    client = build_tickflow_free_client()
    client.close()

    assert calls[0]["base_url"] == TICKFLOW_FREE_SERVER
    assert calls[0]["follow_redirects"] is False
    assert calls[0]["verify"] is True
    assert calls[0]["trust_env"] is False


def test_free_canary_makes_exact_requests_without_credentials(monkeypatch):
    monkeypatch.setenv("STOCK_EVA_TICKFLOW_TOKEN", "must-not-be-read-or-sent")
    monkeypatch.setenv("TICKFLOW_API_KEY", "must-not-be-read-or-sent")
    responses = _valid_payloads()
    transport = Transport(responses)

    report = _run(transport)

    assert report.status == "discovered"
    assert report.provider_mode == "FREE_DAILY_DISCOVERY"
    assert report.request_count == 4
    assert [(method, endpoint) for method, endpoint, _ in transport.calls] == [
        ("GET", "/v1/exchanges"),
        ("POST", "/v1/instruments"),
        ("GET", "/v1/universes/CN_Equity_A"),
        ("GET", "/v1/klines/batch"),
    ]
    assert transport.calls[1][2]["json"] == {"symbols": list(TICKFLOW_FREE_PROVIDER_SYMBOLS)}
    assert transport.calls[3][2]["params"]["symbols"] == ",".join(TICKFLOW_FREE_PROVIDER_SYMBOLS)
    assert transport.calls[3][2]["params"]["period"] == "1d"
    assert transport.calls[3][2]["params"]["adjust"] == "none"
    assert all(call[2]["headers"] == {} for call in transport.calls)
    assert all(call[2]["follow_redirects"] is False for call in transport.calls)
    assert all(call[2]["verify"] is True for call in transport.calls)
    assert transport.close_calls == 1
    assert all(item.close_calls == 1 for item in responses)


def test_free_success_capabilities_are_explicit_and_qualifications_stay_false():
    report = _run(Transport(_valid_payloads()))

    assert report.capabilities.connectivity == TickFlowCapabilityState.DISCOVERED
    assert report.capabilities.instrument_metadata == TickFlowCapabilityState.DISCOVERED
    assert report.capabilities.universe_metadata == TickFlowCapabilityState.DISCOVERED
    assert report.capabilities.historical_daily_1d == TickFlowCapabilityState.DISCOVERED
    assert report.capabilities.realtime_quote == TickFlowCapabilityState.FORBIDDEN
    assert report.capabilities.minute_kline == TickFlowCapabilityState.FORBIDDEN
    assert report.capabilities.adjustment_factor == TickFlowCapabilityState.UNQUALIFIED
    assert report.capabilities.suspension_semantics == TickFlowCapabilityState.UNKNOWN
    assert report.capabilities.units == TickFlowCapabilityState.UNKNOWN
    assert report.capabilities.rate_limit_quota == TickFlowCapabilityState.UNKNOWN
    assert report.capabilities.raw_retention_contract == TickFlowCapabilityState.UNKNOWN
    assert report.daily_bar_qualified is False
    assert report.adjustment_factor_qualified is False
    assert report.starts_shadow is False


def test_free_success_is_zero_write(tmp_path):
    before = tuple(tmp_path.rglob("*"))
    report = _run(Transport(_valid_payloads()))

    assert report.writes is False
    assert report.writes_evidence is False
    assert report.writes_canonical is False
    assert tuple(tmp_path.rglob("*")) == before == ()


def test_production_free_runner_reads_registry_without_token_or_any_write(tmp_path, monkeypatch):
    registry = _task14_registry(tmp_path)
    shadow_root = tmp_path / "shadow"
    shadow_root.mkdir()
    transport = Transport(_valid_payloads())
    original_environment = dict(os.environ)

    class CredentialReadSpy(dict):
        def get(self, key, default=None):
            if key in {"STOCK_EVA_TICKFLOW_TOKEN", "TICKFLOW_API_KEY"}:
                raise AssertionError("Free mode must not read a credential")
            return super().get(key, default)

    monkeypatch.setattr(os, "environ", CredentialReadSpy(original_environment))
    settings = SimpleNamespace(
        provider_shadow_enabled=True,
        provider_shadow_execute_enabled=True,
        local_market_dataset_root=tmp_path / "canonical",
        nas_market_dataset_root=None,
        local_control_dir=tmp_path / "control",
    )

    class Layout:
        provider_shadow_root = shadow_root

        def validate_provider_shadow_root(self, *, canonical_roots):
            assert shadow_root not in canonical_roots

    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    report = TickFlowFreeCanaryRunner(
        settings=settings,
        layout=Layout(),
        registry=registry,
        client_factory=lambda: transport,
    ).execute(
        TRADE_DATE,
        external_authorization_id="free-canary-once",
        acknowledge_provider_requests=True,
    )
    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert report.status == "discovered"
    assert report.writes is False
    assert report.starts_shadow is False
    assert before == after
    assert registry.read_status("tickflow").admission_state == AdmissionState.CANARY


def test_production_free_runner_requires_separately_reviewed_free_descriptor(tmp_path):
    registry = _task14_registry(tmp_path, contract_version="r2f3-authenticated-only")
    shadow_root = tmp_path / "shadow"
    shadow_root.mkdir()
    client_calls = []
    settings = SimpleNamespace(
        provider_shadow_enabled=True,
        provider_shadow_execute_enabled=True,
        local_market_dataset_root=tmp_path / "canonical",
        nas_market_dataset_root=None,
        local_control_dir=tmp_path / "control",
    )

    class Layout:
        provider_shadow_root = shadow_root

        def validate_provider_shadow_root(self, *, canonical_roots):
            assert shadow_root not in canonical_roots

    runner = TickFlowFreeCanaryRunner(
        settings=settings,
        layout=Layout(),
        registry=registry,
        client_factory=lambda: client_calls.append("called"),
    )
    with pytest.raises(PermissionError, match="Free provider review unavailable"):
        runner.execute(
            TRADE_DATE,
            external_authorization_id="free-canary-once",
            acknowledge_provider_requests=True,
        )
    assert client_calls == []


@pytest.mark.parametrize("failure_ordinal", range(4))
def test_each_free_endpoint_failure_is_one_attempt_zero_write_and_unqualified(failure_ordinal):
    responses = _valid_payloads()
    failed = responses[failure_ordinal]
    responses[failure_ordinal] = Response(
        {"private": "must-not-leak"},
        endpoint=(
            "/v1/exchanges",
            "/v1/instruments",
            "/v1/universes/CN_Equity_A",
            "/v1/klines/batch",
        )[failure_ordinal],
        status_code=429,
    )
    transport = Transport(responses)

    report = _run(transport)

    assert report.status == "unavailable"
    assert report.failure_class == "rate_limited"
    assert report.request_count == failure_ordinal + 1
    assert len(transport.calls) == failure_ordinal + 1
    assert report.capabilities.connectivity == TickFlowCapabilityState.UNKNOWN
    assert report.daily_bar_qualified is False
    assert report.adjustment_factor_qualified is False
    assert report.writes is False
    assert report.starts_shadow is False
    assert "private" not in report.model_dump_json()
    assert failed.close_calls == 0


@pytest.mark.parametrize("mutation", ["missing_instrument", "bad_universe", "bad_ohlc"])
def test_free_parser_fails_closed_for_partial_or_impossible_data(mutation):
    responses = _valid_payloads()
    if mutation == "missing_instrument":
        payload = json.loads(responses[1].content)
        payload["data"].pop()
        responses[1] = Response(payload, endpoint="/v1/instruments")
    elif mutation == "bad_universe":
        payload = json.loads(responses[2].content)
        payload["data"]["symbols"].pop()
        payload["data"]["symbol_count"] -= 1
        responses[2] = Response(payload, endpoint="/v1/universes/CN_Equity_A")
    else:
        payload = json.loads(responses[3].content)
        payload["data"][TICKFLOW_FREE_PROVIDER_SYMBOLS[0]]["high"] = [8.0]
        responses[3] = Response(payload, endpoint="/v1/klines/batch")

    report = _run(Transport(responses))

    assert report.status == "unavailable"
    assert report.request_count == 4
    assert report.failure_class in {"symbol_set_invalid", "numeric_value_invalid"}
    assert report.capabilities.historical_daily_1d == TickFlowCapabilityState.UNKNOWN
    assert report.writes is False


def test_cli_tickflow_plan_defaults_to_free_without_caller_symbols(monkeypatch, capsys):
    from backend.app import cli

    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "market-provider-canary", "--provider", "tickflow", "--date", "2026-08-20"],
    )

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_mode"] == "FREE_DAILY_DISCOVERY"
    assert payload["fixed_symbols"] == list(TICKFLOW_FREE_PROVIDER_SYMBOLS)
    assert payload["request_count"] == 4
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False


def test_cli_tickflow_execute_defaults_only_to_free_runner(tmp_path, monkeypatch, capsys):
    from backend.app import cli
    from backend.app.market.providers import tickflow as tickflow_module

    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("general Settings must not be constructed")),
    )

    class ClosedEnvironment(dict):
        def items(self):
            raise AssertionError("Free mode must not enumerate the environment")

        def get(self, key, default=None):
            if key in {"STOCK_EVA_TICKFLOW_TOKEN", "TICKFLOW_API_KEY"}:
                raise AssertionError("Free mode must not read a credential")
            return super().get(key, default)

    monkeypatch.setattr(os, "environ", ClosedEnvironment(dict(os.environ)))
    monkeypatch.setattr(
        cli,
        "StorageLayout",
        lambda _settings: SimpleNamespace(
            provider_registry_database=tmp_path / "provider-registry.sqlite3"
        ),
    )
    monkeypatch.setattr(
        tickflow_module.TickFlowCanaryRunner,
        "execute",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("authenticated runner must not be selected")
        ),
    )
    monkeypatch.setattr(
        tickflow_module.TickFlowFreeCanaryRunner,
        "execute",
        lambda _self, trade_date, **_kwargs: TickFlowFreeCanaryReport(
            status="discovered",
            outcome="discovery",
            trade_date=trade_date,
            request_count=4,
            capabilities=tickflow_module.TickFlowFreeCapabilities.discovered(),
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--date",
            "2026-08-20",
            "--execute",
            "--external-authorization-id",
            "free-canary-once",
            "--acknowledge-provider-requests",
        ],
    )

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_mode"] == "FREE_DAILY_DISCOVERY"
    assert payload["provider_requests"] == 4
    assert payload["writes"] is False
    assert payload["starts_shadow"] is False


def test_cli_authenticated_capability_remains_explicit():
    from backend.app import cli

    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "market-provider-canary",
            "--provider",
            "tickflow",
            "--capability",
            "authenticated",
            "--date",
            "2026-08-20",
        ]
    )
    assert args.capability == "authenticated"


def test_free_contract_contains_no_quote_minute_factor_or_realtime_endpoint():
    adapter = TickFlowFreeAdapter()
    joined = " ".join(adapter.CONTRACT.endpoint_paths).lower()
    assert "quote" not in joined
    assert "minute" not in joined
    assert "factor" not in joined
    assert "realtime" not in joined


def test_free_sdk_version_mismatch_fails_before_any_transport():
    with pytest.raises(TickFlowFreeDiscoveryError, match="pinned SDK"):
        initialize_tickflow_free_sdk(
            sdk_class=FakeSdk,
            sdk_version="0.1.25",
            cache_dir=Path("/dev/null/stock-eva-free"),
        )
    assert FakeSdk.factory_calls == []


def test_free_sdk_close_failure_is_a_sanitized_pre_request_failure(tmp_path):
    class CloseFailSdk(FakeSdk):
        def close(self):
            self.close_calls += 1
            raise RuntimeError("private close details")

    with pytest.raises(TickFlowFreeDiscoveryError) as error:
        initialize_tickflow_free_sdk(
            sdk_class=CloseFailSdk,
            sdk_version="0.1.24",
            cache_dir=tmp_path / "absent",
        )
    assert error.value.failure_class == "sdk_contract_invalid"
    assert "private close details" not in str(error.value)
    assert CloseFailSdk.instances[0].close_calls == 1


def test_free_models_reject_capability_escalation_and_sample_widening():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        TickFlowFreeAdapter().CONTRACT.model_copy(update={"endpoint_paths": ("/v1/quotes",) * 4})
    report = _run(Transport(_valid_payloads()))
    with pytest.raises(ValidationError):
        report.capabilities.model_copy(
            update={"adjustment_factor": TickFlowCapabilityState.DISCOVERED}
        )
    with pytest.raises(ValidationError):
        report.model_copy(update={"fixed_symbols": ("AAPL.US",) * 5})
    with pytest.raises(ValidationError):
        report.model_copy(update={"failure_class": "private-provider-payload"})
    with pytest.raises(ValidationError):
        report.model_copy(update={"endpoint": "https://free-api.tickflow.org/private"})
