import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest

from backend.app import cli
from backend.app.market import provider_canary as provider_canary_module
from backend.app.market.baostock_vendor import emit_terminal_observation
from backend.app.market.provider_canary import (
    ProviderCanaryError,
    plan_provider_canary,
    run_provider_canary,
)
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportOutcome,
    provider_session_scope,
    refresh_scope,
    request_scope,
)

FIXED_ENDPOINTS = tuple(ProviderEndpoint)
TRADE_DATE = date(2026, 7, 23)
STOCK_SYMBOL = "sh.600000"
INDEX_SYMBOL = "sh.000001"


class EmittingProvider:
    sequence = 0
    calls: list[ProviderEndpoint] = []
    refresh_ids: list[str] = []
    fail_endpoint: ProviderEndpoint | None = None

    def __init__(self, *, max_attempts: int, factor_cache: object | None = None) -> None:
        del factor_cache
        type(self).sequence += 1
        self.number = type(self).sequence
        self.max_attempts = max_attempts

    @contextmanager
    def refresh_operation(self, refresh_id: str) -> Iterator[None]:
        type(self).refresh_ids.append(refresh_id)
        with refresh_scope(refresh_id):
            yield

    def probe_endpoint(
        self,
        endpoint: ProviderEndpoint,
        *,
        trade_date: date,
        stock_symbol: str,
        index_symbol: str,
    ) -> None:
        assert trade_date == TRADE_DATE
        assert stock_symbol == STOCK_SYMBOL
        assert index_symbol == INDEX_SYMBOL
        type(self).calls.append(endpoint)
        with provider_session_scope(f"session-{self.number}"):
            with request_scope(
                endpoint,
                attempt=1,
                request_id=f"request-{self.number}",
            ):
                if endpoint == type(self).fail_endpoint:
                    emit_terminal_observation(
                        started_at=0.0,
                        protocol_stage=ProtocolStage.RECEIVE,
                        recv_calls=1,
                        response_bytes=0,
                        end_marker_seen=False,
                        provider_code=None,
                        normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                    )
                    print("token=private-token https://provider.invalid/private")
                    print("/Users/private/control.sqlite3", file=sys.stderr)
                    raise RuntimeError("raw provider failure SELECT * FROM secrets")
                emit_terminal_observation(
                    started_at=0.0,
                    protocol_stage=ProtocolStage.COMPLETE,
                    recv_calls=1,
                    response_bytes=32,
                    end_marker_seen=True,
                    provider_code="0",
                    normalized_error=None,
                )


@pytest.fixture(autouse=True)
def reset_emitting_provider() -> Iterator[None]:
    EmittingProvider.sequence = 0
    EmittingProvider.calls = []
    EmittingProvider.refresh_ids = []
    EmittingProvider.fail_endpoint = None
    yield


def _create_roots(tmp_path: Path) -> tuple[Path, Path]:
    control = tmp_path / "control"
    data = tmp_path / "data"
    control.mkdir()
    data.mkdir()
    return control, data


def _write_sentinels(control: Path, data: Path) -> dict[Path, bytes]:
    sentinels = {
        control / "provider-health.sqlite3": b"sqlite-health\x00sentinel",
        control / "factor-cache.sqlite3": b"sqlite-factor\x00sentinel",
        control / "refresh-runs.duckdb": b"duckdb-refresh\x00sentinel",
        data / "bars.parquet": b"PAR1\x00immutable",
        data / "manifest.json": b'{"manifest":"immutable"}',
        data / "current.json": b'{"pointer":"immutable"}',
    }
    nested = data / "nested"
    nested.mkdir()
    sentinels[nested / "part.parquet"] = b"PAR1\x00nested"
    for path, contents in sentinels.items():
        path.write_bytes(contents)
    return sentinels


def _cli_args(control: Path, data: Path) -> list[str]:
    return [
        "stock-eva",
        "provider-canary",
        "--date",
        TRADE_DATE.isoformat(),
        "--stock-symbol",
        STOCK_SYMBOL,
        "--index-symbol",
        INDEX_SYMBOL,
        "--control-root",
        str(control),
        "--data-root",
        str(data),
    ]


def test_plan_is_fixed_order_and_does_not_inspect_roots_or_construct_provider(
    tmp_path: Path,
) -> None:
    control = tmp_path / "does-not-exist-control"
    data = tmp_path / "does-not-exist-data"

    report = plan_provider_canary(
        trade_date=TRADE_DATE,
        stock_symbol=STOCK_SYMBOL,
        index_symbol=INDEX_SYMBOL,
        control_root=control,
        data_root=data,
    )

    assert report.status == "planned"
    assert [item.endpoint for item in report.endpoints] == list(FIXED_ENDPOINTS)
    assert report.network_requests == 0
    assert report.observations == ()
    assert report.zero_write_verified is False
    assert report.writes_database is False
    assert report.writes_parquet is False
    assert report.writes_manifest is False
    assert report.writes_pointer is False
    assert report.refresh_triggered is False
    assert not control.exists()
    assert not data.exists()


def test_complete_canary_uses_independent_single_attempt_providers_and_changes_nothing(
    tmp_path: Path,
) -> None:
    control, data = _create_roots(tmp_path)
    sentinels = _write_sentinels(control, data)
    before_tree = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    attempts: list[int] = []

    def factory(*, max_attempts: int) -> EmittingProvider:
        attempts.append(max_attempts)
        return EmittingProvider(max_attempts=max_attempts)

    report = run_provider_canary(
        trade_date=TRADE_DATE,
        stock_symbol=STOCK_SYMBOL,
        index_symbol=INDEX_SYMBOL,
        control_root=control,
        data_root=data,
        provider_factory=factory,
    )

    assert report.status == "ready"
    assert report.error_code is None
    assert attempts == [1] * len(FIXED_ENDPOINTS)
    assert EmittingProvider.calls == list(FIXED_ENDPOINTS)
    assert len(set(EmittingProvider.refresh_ids)) == 1
    assert [item.endpoint for item in report.endpoints] == list(FIXED_ENDPOINTS)
    assert all(item.outcome == TransportOutcome.SUCCESS for item in report.endpoints)
    assert len(report.observations) == len(FIXED_ENDPOINTS)
    assert len({item.provider_session_id for item in report.observations}) == len(FIXED_ENDPOINTS)
    assert len({item.request_id for item in report.observations}) == len(FIXED_ENDPOINTS)
    assert {item.refresh_id for item in report.observations} == {report.canary_id}
    assert all(item.attempt == 1 for item in report.observations)
    assert report.zero_write_verified is True
    assert report.filesystem_entries_before == report.filesystem_entries_after
    assert report.writes_database is False
    assert report.writes_parquet is False
    assert report.writes_manifest is False
    assert report.writes_pointer is False
    assert report.refresh_triggered is False
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before_tree
    for path, contents in sentinels.items():
        assert path.read_bytes() == contents


def test_endpoint_failure_is_sanitized_and_does_not_stop_independent_diagnostics(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control, data = _create_roots(tmp_path)
    EmittingProvider.fail_endpoint = ProviderEndpoint.DAILY_ASTOCK

    report = run_provider_canary(
        trade_date=TRADE_DATE,
        stock_symbol=STOCK_SYMBOL,
        index_symbol=INDEX_SYMBOL,
        control_root=control,
        data_root=data,
        provider_factory=lambda *, max_attempts: EmittingProvider(max_attempts=max_attempts),
    )

    assert capsys.readouterr() == ("", "")
    assert report.status == "error"
    assert report.error_code == "PROVIDER_ENDPOINT_FAILED"
    assert EmittingProvider.calls == list(FIXED_ENDPOINTS)
    failed = next(
        item for item in report.endpoints if item.endpoint == ProviderEndpoint.DAILY_ASTOCK
    )
    assert failed.outcome == TransportOutcome.ERROR
    assert failed.normalized_error == NormalizedTransportError.RECV_TIMEOUT
    serialized = report.model_dump_json().lower()
    for forbidden in (
        "private-token",
        "provider.invalid",
        "/users/private",
        "select *",
        "raw provider failure",
        str(tmp_path).lower(),
    ):
        assert forbidden not in serialized
    assert report.zero_write_verified is True
    assert report.writes_database is False
    assert report.writes_parquet is False
    assert report.writes_manifest is False
    assert report.writes_pointer is False
    assert report.refresh_triggered is False


@pytest.mark.parametrize("unsafe", ["missing", "overlap", "symlink", "root"])
def test_unsafe_roots_fail_before_provider_construction(
    tmp_path: Path,
    unsafe: str,
) -> None:
    control, data = _create_roots(tmp_path)
    if unsafe == "missing":
        control = tmp_path / "missing"
    elif unsafe == "overlap":
        data = control / "nested"
        data.mkdir()
    elif unsafe == "symlink":
        linked = tmp_path / "linked-control"
        linked.symlink_to(control, target_is_directory=True)
        control = linked
    else:
        control = Path("/")
    constructed = 0

    def factory(*, max_attempts: int) -> EmittingProvider:
        nonlocal constructed
        constructed += 1
        return EmittingProvider(max_attempts=max_attempts)

    with pytest.raises(ProviderCanaryError) as captured:
        run_provider_canary(
            trade_date=TRADE_DATE,
            stock_symbol=STOCK_SYMBOL,
            index_symbol=INDEX_SYMBOL,
            control_root=control,
            data_root=data,
            provider_factory=factory,
        )

    assert captured.value.error_code == "FILESYSTEM_UNSAFE"
    assert constructed == 0
    assert str(tmp_path) not in str(captured.value)


def test_symlink_anywhere_below_a_root_is_rejected_before_provider(
    tmp_path: Path,
) -> None:
    control, data = _create_roots(tmp_path)
    target = tmp_path / "outside"
    target.write_bytes(b"private")
    (data / "linked").symlink_to(target)

    with pytest.raises(ProviderCanaryError) as captured:
        run_provider_canary(
            trade_date=TRADE_DATE,
            stock_symbol=STOCK_SYMBOL,
            index_symbol=INDEX_SYMBOL,
            control_root=control,
            data_root=data,
            provider_factory=lambda **_kwargs: pytest.fail("provider must not be constructed"),
        )

    assert captured.value.error_code == "FILESYSTEM_UNSAFE"


def test_normalized_root_alias_is_rejected_before_recursive_fingerprint() -> None:
    root_alias = Path("/private/..")

    with pytest.raises(ProviderCanaryError) as captured:
        provider_canary_module._safe_resolved_root(root_alias)

    assert captured.value.error_code == "FILESYSTEM_UNSAFE"


def test_post_probe_filesystem_change_fails_closed_without_reporting_path(
    tmp_path: Path,
) -> None:
    control, data = _create_roots(tmp_path)
    sentinel = data / "manifest.json"
    sentinel.write_bytes(b"before")

    class MutatingProvider(EmittingProvider):
        def probe_endpoint(self, endpoint: ProviderEndpoint, **kwargs) -> None:
            super().probe_endpoint(endpoint, **kwargs)
            if endpoint == ProviderEndpoint.TRADE_DATES:
                sentinel.write_bytes(b"after")

    report = run_provider_canary(
        trade_date=TRADE_DATE,
        stock_symbol=STOCK_SYMBOL,
        index_symbol=INDEX_SYMBOL,
        control_root=control,
        data_root=data,
        provider_factory=lambda *, max_attempts: MutatingProvider(max_attempts=max_attempts),
    )

    assert report.status == "error"
    assert report.error_code == "FILESYSTEM_CHANGED"
    assert report.zero_write_verified is False
    assert str(tmp_path) not in report.model_dump_json()


def test_cli_default_plan_and_single_confirmation_never_construct_provider_or_touch_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control = tmp_path / "missing-control"
    data = tmp_path / "missing-data"
    monkeypatch.setattr(
        cli,
        "BaoStockProvider",
        lambda **_kwargs: pytest.fail("provider must not be constructed"),
    )
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: pytest.fail("provider canary plan must not load settings"),
    )

    monkeypatch.setattr(sys, "argv", _cli_args(control, data))
    assert cli.main() == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["status"] == "planned"
    assert planned["network_requests"] == 0

    for flag in ("--execute", "--acknowledge-provider-requests"):
        monkeypatch.setattr(sys, "argv", [*_cli_args(control, data), flag])
        assert cli.main() == 2
        confirmation = json.loads(capsys.readouterr().out)
        assert confirmation["status"] == "confirmation_required"
        assert confirmation["error_code"] == "EXPLICIT_CONFIRMATION_REQUIRED"

    assert not control.exists()
    assert not data.exists()


def test_cli_two_flags_runs_sanitized_zero_write_canary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control, data = _create_roots(tmp_path)
    sentinels = _write_sentinels(control, data)
    monkeypatch.setattr(cli, "BaoStockProvider", EmittingProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            *_cli_args(control, data),
            "--execute",
            "--acknowledge-provider-requests",
        ],
    )

    assert cli.main() == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["status"] == "ready"
    assert payload["zero_write_verified"] is True
    assert payload["refresh_triggered"] is False
    assert payload["writes_database"] is False
    assert payload["writes_parquet"] is False
    assert payload["writes_manifest"] is False
    assert payload["writes_pointer"] is False
    for path, contents in sentinels.items():
        assert path.read_bytes() == contents


def test_cli_provider_failure_and_invalid_arguments_never_leak_input_or_sdk_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control, data = _create_roots(tmp_path)
    EmittingProvider.fail_endpoint = ProviderEndpoint.ADJUST_FACTOR
    monkeypatch.setattr(cli, "BaoStockProvider", EmittingProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            *_cli_args(control, data),
            "--execute",
            "--acknowledge-provider-requests",
        ],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "error"
    serialized = json.dumps(payload).lower()
    for forbidden in (
        "private-token",
        "provider.invalid",
        "/users/private",
        "select *",
        "raw provider failure",
        str(tmp_path).lower(),
    ):
        assert forbidden not in serialized
    assert captured.err == ""

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "provider-canary",
            "--date",
            TRADE_DATE.isoformat(),
            "--stock-symbol",
            "token=private-token",
            "--index-symbol",
            INDEX_SYMBOL,
            "--control-root",
            str(control),
            "--data-root",
            str(data),
        ],
    )
    assert cli.main() == 2
    invalid = capsys.readouterr()
    assert json.loads(invalid.out) == {
        "error_code": "invalid_cli_arguments",
        "status": "error",
        "writes_data": False,
    }
    assert "private-token" not in invalid.out
    assert invalid.err == ""


def test_transport_report_contains_only_allowlisted_fields(tmp_path: Path) -> None:
    control, data = _create_roots(tmp_path)
    report = run_provider_canary(
        trade_date=TRADE_DATE,
        stock_symbol=STOCK_SYMBOL,
        index_symbol=INDEX_SYMBOL,
        control_root=control,
        data_root=data,
        provider_factory=lambda *, max_attempts: EmittingProvider(max_attempts=max_attempts),
    )

    assert set(report.model_dump()) == {
        "status",
        "error_code",
        "canary_id",
        "trade_date",
        "stock_symbol",
        "index_symbol",
        "endpoints",
        "observations",
        "network_requests",
        "filesystem_entries_before",
        "filesystem_entries_after",
        "zero_write_verified",
        "writes_database",
        "writes_parquet",
        "writes_manifest",
        "writes_pointer",
        "refresh_triggered",
    }
    assert set(report.endpoints[0].model_dump()) == {
        "endpoint",
        "outcome",
        "normalized_error",
        "observation_count",
        "provider_session_ids",
        "request_ids",
    }
    assert set(report.observations[0].model_dump()) == {
        "refresh_id",
        "provider_session_id",
        "request_id",
        "provider_id",
        "endpoint",
        "attempt",
        "page",
        "protocol_stage",
        "elapsed_ms",
        "recv_calls",
        "response_bytes",
        "end_marker_seen",
        "provider_code",
        "normalized_error",
        "outcome",
        "observed_at",
    }
