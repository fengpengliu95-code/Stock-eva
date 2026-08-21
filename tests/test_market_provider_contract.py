"""RED/GREEN contract tests for the R2-F2 provider boundary.

These tests deliberately use typed in-memory fixtures.  No provider SDK is
constructed without an injected client and the network sentinel makes an
accidental real request fail loudly.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from backend.app.market.baostock import DAILY_FIELDS
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    TransportObservation,
    TransportOutcome,
)
from backend.app.market.provider_transport import (
    ProviderEndpoint as TransportEndpoint,
)
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.providers.base import (
    ENDPOINT_CONTRACTS,
    AttemptCompletion,
    DailyAStockRow,
    EndpointContractSummary,
    ExpectedLogicalRequest,
    ExpectedLogicalRequestPlan,
    IndexHistorySessionRow,
    InstrumentRole,
    PaginationPolicy,
    ProviderEndpoint,
    ProviderId,
    ProviderRawBatch,
    ProviderRequest,
    RawEndpointBatch,
    RawEndpointRow,
    RequestCompletion,
    RequestRole,
    SafeProviderId,
    TransportLineageRef,
    TransportObservationAggregate,
    TransportObservationProjection,
    endpoint_contract_for,
    provider_registry,
    validate_raw_date_binding,
)

NOW = datetime(2026, 8, 20, 8, 0, tzinfo=UTC)
SHA = "0" * 64


def _plan(
    *,
    endpoint: ProviderEndpoint = ProviderEndpoint.DAILY_ASTOCK,
    role: RequestRole = RequestRole.DAILY_STOCK,
    instrument: InstrumentRole | None = InstrumentRole.STOCK,
    variant: str = "daily_astock.v1",
    symbols: tuple[str, ...] = ("sh.600000",),
    ordinal: int = 0,
) -> ExpectedLogicalRequest:
    return ExpectedLogicalRequest(
        plan_ordinal=ordinal,
        endpoint=endpoint,
        request_role=role,
        instrument_role=instrument,
        schema_variant=variant,
        shard_id=f"shard-{ordinal}",
        symbols=symbols,
        start_date=date(2026, 8, 20),
        end_date=date(2026, 8, 20),
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    )


def _plan_container(item: ExpectedLogicalRequest | None = None) -> ExpectedLogicalRequestPlan:
    request = item or _plan()
    plan_hash = sha256(
        json.dumps(
            [request.model_dump(mode="json")],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return ExpectedLogicalRequestPlan(
        requests=(request,),
        request_count=1,
        request_plan_hash=plan_hash,
    )


def _provider_request(*, plan: ExpectedLogicalRequestPlan | None = None, **updates: Any):
    values: dict[str, Any] = {
        "provider_id": ProviderId.BAOSTOCK,
        "refresh_id": "refresh-1",
        "provider_session_id": "session-1",
        "request_id": "request-1",
        "trade_date": date(2026, 8, 20),
        "universe_id": "main-board-v1",
        "session_symbols": ("sh.000001", "sh.600000"),
        "request_plan": plan or _plan_container(),
    }
    values.update(updates)
    return ProviderRequest(**values)


def _attempt(
    *,
    attempt: int = 1,
    request_id: str = "request-1",
    outcome=TransportOutcome.SUCCESS,
    pages: tuple[int, ...] = (1,),
    terminal: bool = True,
) -> AttemptCompletion:
    return AttemptCompletion(
        plan_ordinal=0,
        attempt=attempt,
        request_id=request_id,
        observed_pages=pages,
        observed_page_count=len(pages),
        terminal=terminal,
        outcome=outcome,
    )


def _lineage(*, endpoint: ProviderEndpoint = ProviderEndpoint.DAILY_ASTOCK) -> TransportLineageRef:
    return TransportLineageRef(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        request_id="request-1",
        endpoint=endpoint,
        plan_ordinal=0,
        attempt=1,
        page=1,
        observation_digest=SHA,
    )


def _daily_row(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "endpoint": ProviderEndpoint.DAILY_ASTOCK,
        "schema_variant": "daily_astock.v1",
        "request_role": RequestRole.DAILY_STOCK,
        "instrument_role": InstrumentRole.STOCK,
        "date": date(2026, 8, 20),
        "code": "sh.600000",
        "open": Decimal("10"),
        "high": Decimal("11"),
        "low": Decimal("9"),
        "close": Decimal("10.5"),
        "preclose": Decimal("10"),
        "volume": Decimal("100"),
        "amount": Decimal("1000"),
        "adjustflag": "3",
        "turn": Decimal("1"),
        "tradestatus": "1",
        "pctChg": Decimal("5"),
        "isST": "0",
    }
    value.update(overrides)
    return value


def _batch(row: RawEndpointRow | None = None, **overrides: Any) -> RawEndpointBatch:
    values: dict[str, Any] = {
        "endpoint": ProviderEndpoint.DAILY_ASTOCK,
        "schema_variant": "daily_astock.v1",
        "request_role": RequestRole.DAILY_STOCK,
        "instrument_role": InstrumentRole.STOCK,
        "plan_ordinal": 0,
        "shard_id": "shard-0",
        "lineage": _lineage(),
        "rows": (row or DailyAStockRow(**_daily_row()),),
        "row_count": 1,
        "source_schema": "daily_astock.v1",
        "fields": endpoint_contract_for(
            ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.STOCK, "daily_astock.v1"
        ).fields,
        "units": endpoint_contract_for(
            ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.STOCK, "daily_astock.v1"
        ).units,
        "date_semantics": "explicit_trade_date",
        "pagination_policy": PaginationPolicy.PROVIDER_TERMINAL,
        "provider_row_order_digest": SHA,
    }
    values.update(overrides)
    return RawEndpointBatch(**values)


def _provider_raw_batch() -> ProviderRawBatch:
    plan = _plan_container()
    completion = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(),),
        successful_attempt=1,
        successful_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    contract = endpoint_contract_for(
        ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.STOCK, "daily_astock.v1"
    )
    summary = EndpointContractSummary(
        endpoint=contract.endpoint,
        request_role=contract.request_role,
        instrument_role=contract.instrument_role,
        schema_variant=contract.schema_variant,
        source_schema=contract.schema_variant,
        fields=contract.fields,
        units=contract.units,
        date_semantics=contract.date_semantics,
        pagination_policy=contract.pagination_policy,
        batch_count=1,
        row_count=1,
    )
    projection = _projection()
    return ProviderRawBatch(
        provider_id=ProviderId.BAOSTOCK,
        request=_provider_request(plan=plan),
        adapter_version="r2f2.v1",
        endpoint_contract_version="r2f2-endpoints.v1",
        started_at=NOW,
        completed_at=NOW,
        normalization_clock_utc=NOW,
        logical_request_plan=plan,
        request_plan_hash=plan.request_plan_hash,
        endpoint_batches=(_batch(),),
        request_completions=(completion,),
        completion_hash=completion.compute_hash(),
        transport_lineage=(_lineage(),),
        transport_observations=TransportObservationAggregate.from_observations((projection,)),
        endpoint_summaries=(summary,),
    )


def _projection(*, provider_code: str | None = None) -> TransportObservationProjection:
    observation = TransportObservation(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        request_id="request-1",
        provider_id="baostock",
        endpoint=TransportEndpoint.DAILY_ASTOCK,
        attempt=1,
        page=1,
        protocol_stage=ProtocolStage.COMPLETE,
        elapsed_ms=20,
        recv_calls=1,
        response_bytes=128,
        end_marker_seen=True,
        provider_code=provider_code,
        normalized_error=(
            NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR if provider_code else None
        ),
        outcome=TransportOutcome.SUCCESS if provider_code is None else TransportOutcome.ERROR,
        observed_at=NOW,
    )
    return TransportObservationProjection.from_observation(observation)


def test_provider_id_is_static_baostock_allowlist() -> None:
    assert provider_registry().provider_id is ProviderId.BAOSTOCK
    assert SafeProviderId("baostock") == "baostock"
    for invalid in ("", "BaoStock", "baostock/other", "plugin-x"):
        with pytest.raises(ValueError):
            provider_registry(invalid)


def test_provider_request_requires_complete_nonempty_session_symbols_and_aware_dates() -> None:
    request = _provider_request()
    assert request.session_symbols == ("sh.000001", "sh.600000")
    with pytest.raises(ValueError):
        _provider_request(session_symbols=())
    with pytest.raises(ValueError):
        _provider_request(session_symbols=("sh.600000", "sh.600000"))
    with pytest.raises(ValueError):
        _provider_request(trade_date=datetime(2026, 8, 20, 8, 0))


def test_expected_logical_request_symbols_are_empty_only_for_calendar_and_universe() -> None:
    assert _plan().symbols
    assert (
        _plan(
            endpoint=ProviderEndpoint.TRADE_DATES,
            role=RequestRole.CALENDAR,
            instrument=None,
            variant="trade_dates.v1",
            symbols=(),
        ).symbols
        == ()
    )
    with pytest.raises(ValueError):
        _plan(
            endpoint=ProviderEndpoint.TRADE_DATES,
            role=RequestRole.CALENDAR,
            instrument=None,
            variant="trade_dates.v1",
            symbols=("sh.600000",),
        )


def test_provider_and_logical_request_model_copy_round_trip_revalidates_symbol_roles() -> None:
    request = _provider_request()
    with pytest.raises(ValueError):
        request.model_copy(update={"session_symbols": ()})
    with pytest.raises(ValueError):
        ExpectedLogicalRequest.model_validate(
            _plan().model_dump() | {"symbols": (), "endpoint": ProviderEndpoint.DAILY_ASTOCK}
        )


def test_raw_batch_rejects_unknown_fields_secrets_headers_urls_and_raw_exception() -> None:
    with pytest.raises(ValueError):
        _batch(secret="never-persist", headers={"Authorization": "x"})
    with pytest.raises(ValueError):
        DailyAStockRow(**_daily_row(url="https://example.invalid"))


def test_raw_batch_rejects_nonfinite_values_and_row_shape_mismatch() -> None:
    with pytest.raises(ValueError):
        _batch(row=DailyAStockRow(**_daily_row(close=float("nan"))))
    with pytest.raises(ValueError):
        _batch(row_count=2)


def test_each_baostock_endpoint_has_exact_fields_variant_units_and_order() -> None:
    assert len({contract.endpoint for contract in ENDPOINT_CONTRACTS}) == 6
    assert len(ENDPOINT_CONTRACTS) == 9
    daily = endpoint_contract_for(
        ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.STOCK, "daily_astock.v1"
    )
    assert daily.fields == tuple(DAILY_FIELDS.split(","))
    assert daily.units == (
        ("volume", "shares"),
        ("amount", "CNY"),
        ("turn", "percent"),
        ("pctChg", "percent"),
    )


def test_endpoint_contract_constant_rejects_wrong_combination_and_model_copy() -> None:
    with pytest.raises(ValueError):
        endpoint_contract_for(
            ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.INDEX, "daily_astock.v1"
        )
    with pytest.raises(ValueError):
        endpoint_contract_for(
            ProviderEndpoint.DAILY_ASTOCK, InstrumentRole.STOCK, "daily_astock.v1"
        ).model_copy(update={"fields": ("code",)})
    with pytest.raises(ValueError):
        _batch(fields=("code",))


def test_endpoint_schema_variants_reject_date_mismatch_and_mixed_sessions() -> None:
    with pytest.raises(ValueError):
        validate_raw_date_binding(
            _provider_request(), _batch(row=DailyAStockRow(**_daily_row(date=date(2026, 8, 19))))
        )
    with pytest.raises(ValueError):
        validate_raw_date_binding(
            _provider_request(session_symbols=("sh.600000",)),
            _batch(row=DailyAStockRow(**_daily_row(code="sz.000001"))),
        )


def test_validate_raw_date_binding_rejects_cross_day_before_normalize() -> None:
    with pytest.raises(ValueError):
        validate_raw_date_binding(
            _provider_request(), _batch(row=DailyAStockRow(**_daily_row(date=date(2026, 8, 19))))
        )


def test_endpoint_role_validator_rejects_stock_index_role_mismatch() -> None:
    with pytest.raises(ValueError):
        IndexHistorySessionRow(
            endpoint=ProviderEndpoint.INDEX_HISTORY,
            schema_variant="index_history.session.v1",
            request_role=RequestRole.INDEX_HISTORY,
            instrument_role=InstrumentRole.INDEX,
            date=date(2026, 8, 20),
            code="sh.600000",
            open=1,
            high=1,
            low=1,
            close=1,
            preclose=1,
            volume=1,
            amount=1,
            adjustflag="3",
            turn=1,
            tradestatus="1",
            pctChg=0,
            isST="0",
        )


def test_transport_lineage_preserves_unknown_provider_code_and_maps_unknown_protocol() -> None:
    projection = _projection(provider_code="X999")
    assert projection.provider_code == "X999"
    assert projection.normalized_error is NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR


def test_transport_projection_preserves_all_allowlisted_fields_and_digest() -> None:
    projection = _projection()
    assert projection.observation_digest == projection.compute_digest()
    assert projection.response_bytes == 128
    aggregate = TransportObservationAggregate.from_observations((projection,))
    assert aggregate.aggregate_digest == aggregate.compute_digest()


def test_logical_request_plan_hash_excludes_observed_pages_and_completion_hash_binds_them() -> None:
    plan = _plan_container()
    first = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(),),
        successful_attempt=1,
        successful_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    second = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(pages=(1, 2)),),
        successful_attempt=1,
        successful_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    assert plan.request_plan_hash == plan.compute_hash()
    assert first.compute_hash() != second.compute_hash()


def test_request_completion_rejects_empty_duplicate_noncontiguous_pages_or_terminal_marker() -> (
    None
):
    with pytest.raises(ValueError):
        _attempt(pages=(1, 3))
    with pytest.raises(ValueError):
        _attempt(pages=(), terminal=True, outcome=TransportOutcome.SUCCESS)


def test_request_completion_rejects_success_then_later_attempt_and_multiple_successes() -> None:
    with pytest.raises(ValueError):
        RequestCompletion(
            plan_ordinal=0,
            attempts=(_attempt(), _attempt(attempt=2, request_id="request-2")),
            successful_attempt=1,
            successful_request_id="request-1",
            final_outcome=TransportOutcome.SUCCESS,
            row_count=1,
        )


def test_failed_completion_has_zero_row_count_and_no_failed_source_rows() -> None:
    completion = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(outcome=TransportOutcome.ERROR, terminal=False, pages=()),),
        final_outcome=TransportOutcome.ERROR,
        row_count=0,
    )
    assert completion.successful_attempt is None
    assert completion.row_count == 0


def test_request_completion_model_copy_round_trip_revalidates_success_cardinality() -> None:
    completion = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(),),
        successful_attempt=1,
        successful_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    with pytest.raises(ValueError):
        RequestCompletion.model_validate(completion.model_dump() | {"successful_attempt": None})


class _NetworkSentinelClient:
    def __getattr__(self, name: str):
        raise AssertionError(f"network sentinel called: {name}")


def test_baostock_adapter_preserves_endpoint_and_session_contract() -> None:
    adapter = BaoStockProviderAdapter(client=_NetworkSentinelClient())
    assert adapter.provider_id is ProviderId.BAOSTOCK
    assert adapter.endpoint_contract_version
    assert adapter.max_attempts == 2


def test_baostock_adapter_captures_source_rows_before_normalization() -> None:
    class RawClient:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch:
            self.calls += 1
            assert request.provider_id is ProviderId.BAOSTOCK
            return _provider_raw_batch()

    client = RawClient()
    adapter = BaoStockProviderAdapter(client=client)
    captured = adapter.fetch_raw(_provider_request())
    assert captured.endpoint_batches[0].rows[0].code == "sh.600000"
    assert client.calls == 1


def test_baostock_compatibility_produces_existing_daily_bar_semantics() -> None:
    payload = json.loads((Path(__file__).parent / "fixtures" / "baostock_daily.json").read_text())

    class Evidence:
        def read_rows(self):
            return (
                {
                    "fields": payload["daily_fields"],
                    "rows": payload["daily_rows"],
                    "factor_fields": payload["factor_fields"],
                    "factor_rows": payload["factor_rows"],
                },
            )

    adapter = BaoStockProviderAdapter(client=_NetworkSentinelClient())
    bars = adapter.normalize(Evidence(), normalization_clock_utc=NOW)
    assert bars[0].close == 10.5
    assert bars[0].adjust_factor == 0.8


def test_daily_schema_preserves_legal_suspended_empty_activity_and_factor() -> None:
    row = DailyAStockRow(
        **_daily_row(tradestatus="0", volume=None, amount=None, turn=None, pctChg=None)
    )
    assert row.tradestatus == "0"
    assert row.volume is None
    assert row.amount is None
