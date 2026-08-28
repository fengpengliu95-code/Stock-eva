from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from backend.app.market.daily_shadow_models import (
    CanonicalDailyOhlcRow,
    DailyCanonicalLineageState,
    DailyCanonicalSnapshot,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)
from backend.app.market.providers.tickflow import TickFlowFreeAdapter
from backend.app.market.providers.tickflow_daily_shadow import (
    TICKFLOW_FREE_DAILY_ENDPOINT,
    TickFlowFreeDailyShadowAdapter,
    TickFlowFreeDailyShadowFetcher,
)

TRADE_DATE = date(2026, 8, 10)


def _snapshot(count: int = 201) -> DailyCanonicalSnapshot:
    rows = tuple(
        CanonicalDailyOhlcRow(
            trade_date=TRADE_DATE,
            symbol=f"sh.{600000 + index:06d}",
            open=Decimal("10.00"),
            high=Decimal("10.40"),
            low=Decimal("9.90"),
            close=Decimal("10.20"),
        )
        for index in range(count)
    )
    symbols = tuple(row.symbol for row in rows)
    mapping = tuple((symbol, canonical_to_tickflow_daily_symbol(symbol)) for symbol in symbols)
    return DailyCanonicalSnapshot(
        trade_date=TRADE_DATE,
        lineage_state=DailyCanonicalLineageState.LEGACY_UNAVAILABLE,
        manifest_generation="generation-daily-shadow",
        manifest_sha256="1" * 64,
        partition_relative_path=(
            "bars/source=baostock/year=2026/month=08/date=2026-08-10_222222222222.parquet"
        ),
        partition_sha256="2" * 64,
        partition_row_count=count + 2,
        eligible_symbol_count=count,
        excluded_symbol_count=2,
        canonical_universe_sha256=domain_sha256(
            "stock-eva/r2f3/daily-canonical-universe/v1", symbols
        ),
        canonical_exclusion_sha256="4" * 64,
        symbol_mapping_sha256=domain_sha256("stock-eva/r2f3/daily-symbol-mapping/v1", mapping),
        ohlc_sha256="6" * 64,
        rows=rows,
    )


def _provider_payload(symbols: tuple[str, ...], *, timestamp: int = 1786320000000):
    return {
        "data": {
            symbol: {
                "timestamp": [timestamp],
                "open": [10.0],
                "high": [10.4],
                "low": [9.9],
                "close": [10.2],
                "volume": [1000],
                "amount": [10200.0],
            }
            for symbol in symbols
        }
    }


class _Response:
    def __init__(self, payload, *, status=200, url=TICKFLOW_FREE_DAILY_ENDPOINT):
        self.status_code = status
        self.headers = {}
        self.url = f"https://free-api.tickflow.org{url}"
        self.history = ()
        self.content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.close_count = 0

    def iter_bytes(self):
        yield self.content

    def close(self):
        self.close_count += 1


class _Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.close_count = 0

    def request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        self.close_count += 1


class _StreamingFailureResponse(_Response):
    def __init__(self, *, first_chunk: bytes):
        super().__init__(b"unused")
        self.first_chunk = first_chunk

    def iter_bytes(self):
        yield self.first_chunk
        raise TimeoutError


def test_full_session_plan_is_sorted_chunked_and_deterministic():
    snapshot = _snapshot(3193)
    adapter = TickFlowFreeDailyShadowAdapter()

    first = adapter.plan(snapshot)
    second = adapter.plan(snapshot)

    assert first == second
    assert first.request_count == 32
    assert tuple(shard.ordinal for shard in first.shards) == tuple(range(32))
    assert all(1 <= len(shard.provider_symbols) <= 100 for shard in first.shards)
    assert first.shards[0].canonical_symbols[0] == "sh.600000"
    assert first.shards[0].provider_symbols[0] == "600000.SH"
    assert first.shards[-1].canonical_symbols[-1] == "sh.603192"
    assert len(first.request_plan_sha256) == 64
    assert first.adjustment_mode == "none"
    assert first.period == "1d"


def test_task14_fixed_five_plan_is_not_a_full_session_plan():
    task14 = TickFlowFreeAdapter().plan(TRADE_DATE)
    with pytest.raises(TypeError, match="Daily shadow plan"):
        TickFlowFreeDailyShadowFetcher._offline_for_tests(
            transport=_Transport([]),
            plan=task14,
            sdk_initializer=lambda: None,
        )


def test_success_is_sequential_one_attempt_credentialless_and_exact_shards():
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(101))
    responses = [_Response(_provider_payload(shard.provider_symbols)) for shard in plan.shards]
    transport = _Transport(responses)
    sdk_calls = []

    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=transport,
        plan=plan,
        sdk_initializer=lambda: sdk_calls.append("initialized"),
        monotonic=lambda: 1.0,
    )

    assert result.status == "ready"
    assert result.request_count == 2
    assert len(result.rows) == 101
    assert tuple(row.symbol for row in result.rows) == tuple(
        symbol for shard in plan.shards for symbol in shard.canonical_symbols
    )
    assert sdk_calls == ["initialized"]
    assert transport.close_count == 1
    assert [call[0:2] for call in transport.calls] == [
        ("GET", "/v1/klines/batch"),
        ("GET", "/v1/klines/batch"),
    ]
    for call, shard in zip(transport.calls, plan.shards, strict=True):
        kwargs = call[2]
        assert kwargs["headers"] == {}
        assert kwargs["params"]["symbols"] == ",".join(shard.provider_symbols)
        assert kwargs["params"]["period"] == "1d"
        assert kwargs["params"]["adjust"] == "none"
        assert kwargs["follow_redirects"] is False
        assert kwargs["verify"] is True
    assert all(item.attempt == 1 for item in result.observations)
    assert all(item.response_bytes > 0 for item in result.observations)
    assert all(response.close_count == 1 for response in responses)


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (TimeoutError(), "timeout"),
        (_Response({"error": "limited-upstream-payload"}, status=429), "rate_limited"),
        (_Response({"error": "server-upstream-payload"}, status=503), "server_error"),
        (_Response(b"{"), "malformed_json"),
    ],
)
def test_transport_failure_stops_next_shard_and_discards_all_rows(failure, expected):
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(201))
    first = _Response(_provider_payload(plan.shards[0].provider_symbols))
    transport = _Transport(
        [first, failure, _Response(_provider_payload(plan.shards[2].provider_symbols))]
    )

    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=transport,
        plan=plan,
        sdk_initializer=lambda: None,
    )

    assert result.status == "unavailable"
    assert result.failure_class == expected
    assert result.failed_ordinal == 1
    assert result.request_count == 2
    assert result.rows == ()
    assert len(transport.calls) == 2
    assert transport.close_count == 1
    if isinstance(failure, _Response):
        assert result.observations[-1].response_bytes == len(failure.content)
    else:
        assert result.observations[-1].response_bytes == 0
    assert "limited-upstream-payload" not in result.model_dump_json()
    assert "server-upstream-payload" not in result.model_dump_json()


@pytest.mark.parametrize(
    ("mutator", "expected"),
    [
        (
            lambda payload, _symbols: payload["data"].pop(next(iter(payload["data"]))),
            "symbol_set_invalid",
        ),
        (
            lambda payload, _symbols: payload["data"].update(
                {"000001.SZ": next(iter(payload["data"].values()))}
            ),
            "symbol_set_invalid",
        ),
        (
            lambda payload, symbols: payload["data"][symbols[0]].update(
                {"timestamp": [1786320000000, 1786320000001]}
            ),
            "column_length_mismatch",
        ),
        (
            lambda payload, symbols: payload["data"][symbols[0]]["timestamp"].__setitem__(0, 0),
            "wrong_date",
        ),
        (
            lambda payload, symbols: payload["data"][symbols[0]]["close"].__setitem__(
                0, float("nan")
            ),
            "numeric_value_invalid",
        ),
        (
            lambda payload, symbols: payload["data"][symbols[0]]["high"].__setitem__(0, 9.0),
            "numeric_value_invalid",
        ),
        (
            lambda payload, symbols: payload["data"][symbols[0]].update({"factor": [1.0]}),
            "schema_drift",
        ),
    ],
)
def test_strict_source_parser_fails_closed(mutator, expected):
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(2))
    symbols = plan.shards[0].provider_symbols
    payload = _provider_payload(symbols)
    mutator(payload, symbols)
    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=_Transport([_Response(payload)]),
        plan=plan,
        sdk_initializer=lambda: None,
    )
    assert result.status == "unavailable"
    assert result.failure_class == expected
    assert result.rows == ()


def test_volume_and_amount_are_opaque_and_semantic_states_remain_unqualified():
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(1))
    payload = _provider_payload(plan.shards[0].provider_symbols)
    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=_Transport([_Response(payload)]),
        plan=plan,
        sdk_initializer=lambda: None,
    )
    assert result.status == "ready"
    assert result.rows[0].volume == 1000
    assert result.rows[0].amount == 10200.0
    assert result.units_state == "UNKNOWN"
    assert result.suspension_semantics_state == "UNKNOWN"
    assert result.factor_evidence_state == "UNQUALIFIED"
    assert result.adjustment_request_state == "NONE_REQUESTED"


def test_partial_receive_timeout_records_only_socket_level_byte_count():
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(1))
    response = _StreamingFailureResponse(first_chunk=b"1234567")
    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=_Transport([response]),
        plan=plan,
        sdk_initializer=lambda: None,
    )
    assert result.status == "unavailable"
    assert result.failure_class == "timeout"
    assert result.observations[0].response_bytes == 7
    assert result.rows == ()
    assert response.close_count == 1


def test_sdk_initialization_failure_constructs_no_transport_and_makes_no_request():
    plan = TickFlowFreeDailyShadowAdapter().plan(_snapshot(1))
    transport = _Transport([])

    def fail_sdk():
        raise RuntimeError("secret upstream text")

    result = TickFlowFreeDailyShadowFetcher._offline_for_tests(
        transport=transport,
        plan=plan,
        sdk_initializer=fail_sdk,
    )
    assert result.status == "unavailable"
    assert result.failure_class == "sdk_initialization_failed"
    assert result.request_count == 0
    assert result.rows == ()
    assert transport.calls == []
    assert transport.close_count == 0
    assert "secret upstream text" not in result.model_dump_json()
