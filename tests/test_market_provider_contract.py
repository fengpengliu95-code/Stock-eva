"""RED/GREEN contract tests for the R2-F2 provider boundary.

These tests deliberately use typed in-memory fixtures.  No provider SDK is
constructed without an injected client and the network sentinel makes an
accidental real request fail loudly.
"""

import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any

import pytest

from backend.app.market.baostock import DAILY_FIELDS
from backend.app.market.baostock_vendor import emit_terminal_observation
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    TransportObservation,
    TransportOutcome,
    current_request_context,
)
from backend.app.market.provider_transport import (
    ProviderEndpoint as TransportEndpoint,
)
from backend.app.market.providers import base as provider_base
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.providers.base import (
    ENDPOINT_CONTRACTS,
    AttemptCompletion,
    DailyAStockRow,
    DailyFactorRow,
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
    TradeDatesRow,
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


def _full_plan() -> ExpectedLogicalRequestPlan:
    requests = (
        _plan(
            endpoint=ProviderEndpoint.TRADE_DATES,
            role=RequestRole.CALENDAR,
            instrument=None,
            variant="trade_dates.v1",
            symbols=(),
            ordinal=0,
        ),
        _plan(
            endpoint=ProviderEndpoint.ALL_STOCK,
            role=RequestRole.UNIVERSE,
            variant="all_stock.market.v1",
            symbols=(),
            ordinal=1,
        ),
        _plan(ordinal=2, symbols=("sh.600000",)),
        _plan(
            endpoint=ProviderEndpoint.DAILY_FACTOR,
            role=RequestRole.DAILY_FACTOR,
            variant="daily_factor.v1",
            symbols=("sh.600000",),
            ordinal=3,
        ),
        _plan(
            endpoint=ProviderEndpoint.ADJUST_FACTOR,
            role=RequestRole.ADJUST_FACTOR,
            variant="adjust_factor.session.v1",
            symbols=("sh.600000",),
            ordinal=4,
        ),
        _plan(
            endpoint=ProviderEndpoint.INDEX_HISTORY,
            role=RequestRole.INDEX_HISTORY,
            instrument=InstrumentRole.INDEX,
            variant="index_history.session.v1",
            symbols=("sh.000001",),
            ordinal=5,
        ),
    )
    plan_hash = sha256(
        json.dumps(
            [request.model_dump(mode="json") for request in requests],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return ExpectedLogicalRequestPlan(
        requests=requests,
        request_count=len(requests),
        request_plan_hash=plan_hash,
    )


def _provider_request(*, plan: ExpectedLogicalRequestPlan | None = None, **updates: Any):
    values: dict[str, Any] = {
        "provider_id": ProviderId.BAOSTOCK,
        "refresh_id": "refresh-1",
        "trade_date": date(2026, 8, 20),
        "universe_id": "main-board-v1",
        "session_symbols": ("sh.000001", "sh.600000"),
        "logical_request_plan": plan or _plan_container(),
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
        provider_session_id="session-1",
        attempt=attempt,
        root_request_id=request_id,
        operation_observation_digest="d" * 64,
        observed_pages=pages,
        page_request_ids=tuple(
            (page, "request-1" if page == 1 else f"page-{page}") for page in pages
        ),
        observed_page_count=len(pages),
        pagination_terminal_count=1 if outcome is TransportOutcome.SUCCESS else 0,
        terminal=terminal,
        outcome=outcome,
    )


def _lineage(
    *,
    endpoint: ProviderEndpoint = ProviderEndpoint.DAILY_ASTOCK,
    observation_digest: str = SHA,
) -> TransportLineageRef:
    return TransportLineageRef(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        root_request_id="request-1",
        page_request_id="request-1",
        endpoint=endpoint,
        plan_ordinal=0,
        attempt=1,
        page=1,
        protocol_stage=ProtocolStage.COMPLETE,
        observation_digest=observation_digest,
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
        "provider_row_order_digest": sha256(
            json.dumps(
                [item.model_dump(mode="json") for item in (row or DailyAStockRow(**_daily_row()),)],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    }
    values.update(overrides)
    return RawEndpointBatch(**values)


def _provider_raw_batch() -> ProviderRawBatch:
    plan = _plan_container()
    completion = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(),),
        successful_attempt=1,
        successful_root_request_id="request-1",
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
    operation = _projection(stage=ProtocolStage.OPERATION, end_marker_seen=False)
    projection = _projection(stage=ProtocolStage.COMPLETE, end_marker_seen=True)
    completion = completion.model_copy(
        update={
            "attempts": (
                completion.attempts[0].model_copy(
                    update={"operation_observation_digest": operation.observation_digest}
                ),
            )
        }
    )
    valid_lineage = _lineage(observation_digest=projection.observation_digest)
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
        endpoint_batches=(_batch(lineage=valid_lineage),),
        request_completions=(completion,),
        completion_hash=sha256(
            json.dumps(
                [completion.model_dump(mode="json")],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
        transport_lineage=(valid_lineage,),
        transport_observations=TransportObservationAggregate.from_observations(
            (operation, projection)
        ),
        endpoint_summaries=(summary,),
    )


def _projection(
    *,
    provider_code: str | None = None,
    stage: ProtocolStage = ProtocolStage.COMPLETE,
    end_marker_seen: bool = True,
) -> TransportObservationProjection:
    observation = TransportObservation(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        request_id="request-1",
        provider_id="baostock",
        endpoint=TransportEndpoint.DAILY_ASTOCK,
        attempt=1,
        page=1,
        protocol_stage=stage,
        elapsed_ms=20,
        recv_calls=1,
        response_bytes=128,
        end_marker_seen=end_marker_seen,
        provider_code=provider_code,
        normalized_error=(
            NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR if provider_code else None
        ),
        outcome=TransportOutcome.SUCCESS if provider_code is None else TransportOutcome.ERROR,
        observed_at=NOW,
    )
    lineage_kind = "page" if stage is ProtocolStage.PAGINATION else "query_root"
    return TransportObservationProjection.from_observation_with_lineage(
        observation, plan_ordinal=0, lineage_kind=lineage_kind
    )


class _SdkResult:
    """Complete offline analogue of BaoStock ResultData used by _read_result."""

    error_code = "0"
    error_msg = ""
    per_page_count = 2000

    def __init__(self, fields: list[str], rows: list[list[str]]) -> None:
        self.fields = fields
        self.data = [list(row) for row in rows]
        self.cur_page_num = "1"
        self.cur_row_num = 0
        self.current = None

    def next(self) -> bool:
        if self.cur_row_num >= len(self.data):
            return False
        self.current = self.data[self.cur_row_num]
        self.cur_row_num += 1
        return True

    def get_row_data(self):
        return self.current


class _CompleteSdkClient:
    """Offline BaoStock-shaped client with every incumbent query method."""

    def __init__(
        self,
        *,
        response_date: str = "2026-08-20",
        schema_overrides: dict[str, list[str]] | None = None,
    ) -> None:
        self.response_date = response_date
        self.schema_overrides = schema_overrides or {}
        self.calls: list[tuple[str, object, object]] = []
        self.login_calls = 0
        self.logout_calls = 0

    def login(self):
        self.login_calls += 1
        return _SdkResult([], [])

    def logout(self):
        self.logout_calls += 1

    def _record(self, name: str, *args: object) -> None:
        context = current_request_context()
        self.calls.append((name, args, context))
        if name != "login":
            emit_terminal_observation(
                started_at=monotonic() - 0.001,
                protocol_stage=ProtocolStage.COMPLETE,
                recv_calls=1,
                response_bytes=128,
                end_marker_seen=True,
                provider_code=None,
                normalized_error=None,
            )

    def _fields(self, name: str, default: list[str]) -> list[str]:
        return self.schema_overrides.get(name, default)

    def query_trade_dates(self, **kwargs):
        self._record("query_trade_dates", kwargs)
        return _SdkResult(
            self._fields("trade_dates", ["calendar_date", "is_trading_day"]),
            [[self.response_date, "1"]],
        )

    def query_all_stock(self, **kwargs):
        self._record("query_all_stock", kwargs)
        return _SdkResult(
            self._fields("all_stock", ["code", "tradeStatus", "code_name"]),
            [["sh.600000", "1", "fixture"]],
        )

    def query_daily_history_k_AStock(self, **kwargs):
        self._record("query_daily_history_k_AStock", kwargs)
        return _SdkResult(
            self._fields("daily_astock", DAILY_FIELDS.split(",")),
            [self._daily_row("sh.600000")],
        )

    def query_daily_adjust_factor(self, **kwargs):
        self._record("query_daily_adjust_factor", kwargs)
        return _SdkResult(
            self._fields(
                "daily_factor",
                [
                    "code",
                    "dividOperateDate",
                    "foreAdjustFactor",
                    "backAdjustFactor",
                    "adjustFactor",
                ],
            ),
            [["sh.600000", self.response_date, "1", "0.8", "0.8"]],
        )

    def query_adjust_factor(self, code, **kwargs):
        self._record("query_adjust_factor", code, kwargs)
        return _SdkResult(
            self._fields(
                "adjust_factor",
                [
                    "code",
                    "dividOperateDate",
                    "foreAdjustFactor",
                    "backAdjustFactor",
                    "adjustFactor",
                ],
            ),
            [[code, self.response_date, "1", "0.8", "0.8"]],
        )

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self._record("query_history_k_data_plus", code, fields, kwargs)
        return _SdkResult(
            self._fields("index_history", fields.split(",")),
            [self._daily_row(code)],
        )

    def _daily_row(self, code: str) -> list[str]:
        values = {
            "date": self.response_date,
            "code": code,
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10.5",
            "preclose": "10",
            "volume": "100",
            "amount": "1000",
            "adjustflag": "3",
            "turn": "1",
            "tradestatus": "1",
            "pctChg": "5",
            "isST": "0",
        }
        return [values[name] for name in DAILY_FIELDS.split(",")]


class _RetryingSdkClient(_CompleteSdkClient):
    """Fails after a captured full page, then succeeds on the retry."""

    def __init__(self) -> None:
        super().__init__()
        self.daily_calls = 0

    def query_daily_history_k_AStock(self, **kwargs):
        self.daily_calls += 1
        self._record("query_daily_history_k_AStock", kwargs)
        result = _SdkResult(DAILY_FIELDS.split(","), [self._daily_row("sh.600000")])
        if self.daily_calls == 1:
            result.per_page_count = 1
        return result


class _SessionRecordingClient(_CompleteSdkClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.login_provider_sessions: list[str] = []

    def login(self):
        self.login_provider_sessions.append(current_request_context().provider_session_id)
        return super().login()


class _PagedSdkResult:
    error_code = "0"
    error_msg = ""

    def __init__(self, pages: list[list[list[str]]], *, fail_transition: bool = False) -> None:
        self._pages = pages
        self._page_index = 0
        self._fail_transition = fail_transition
        self.data = [list(row) for row in pages[0]]
        self.fields = DAILY_FIELDS.split(",")
        self.cur_page_num = "1"
        self.cur_row_num = 0
        self.per_page_count = len(self.data) if len(pages) > 1 else len(self.data) + 1
        self.current = None

    def _emit_page_observation(self, *, error: NormalizedTransportError | None = None) -> None:
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.PAGINATION
            if error is not None
            else ProtocolStage.COMPLETE,
            recv_calls=1,
            response_bytes=max(1, len(repr(self.data).encode())),
            end_marker_seen=error is None,
            provider_code=None,
            normalized_error=error,
        )

    def next(self) -> bool:
        if self.cur_row_num < len(self.data):
            self.current = self.data[self.cur_row_num]
            self.cur_row_num += 1
            return True
        if self._page_index + 1 >= len(self._pages):
            return False
        if self._fail_transition:
            self._emit_page_observation(
                error=NormalizedTransportError.RECV_TIMEOUT,
            )
            raise TimeoutError("fixture page transition failure")
        self._page_index += 1
        self.data = [list(row) for row in self._pages[self._page_index]]
        self.cur_page_num = str(self._page_index + 1)
        self.cur_row_num = 1
        self.current = self.data[0]
        self._emit_page_observation()
        self.per_page_count = len(self.data) + 1
        return True

    def get_row_data(self):
        return self.current


class _TwoPageRetrySdkClient(_SessionRecordingClient):
    def __init__(self) -> None:
        super().__init__()
        self.daily_calls = 0

    def query_daily_history_k_AStock(self, **kwargs):
        self.daily_calls += 1
        self._record("query_daily_history_k_AStock", kwargs)
        second = self._daily_row("sh.600000")
        second[2] = "10.1"
        pages = [[self._daily_row("sh.600000")], [second]]
        result = _PagedSdkResult(pages, fail_transition=self.daily_calls == 1)
        return result


class _FailBeforePageOnce(_CompleteSdkClient):
    def __init__(self) -> None:
        super().__init__()
        self.daily_calls = 0

    def query_daily_history_k_AStock(self, **kwargs):
        self.daily_calls += 1
        if self.daily_calls == 1:
            raise TimeoutError("fixture fails before first page")
        return super().query_daily_history_k_AStock(**kwargs)


class _FullPageExhaustionClient(_CompleteSdkClient):
    def query_daily_history_k_AStock(self, **kwargs):
        self._record("query_daily_history_k_AStock", kwargs)
        result = _SdkResult(DAILY_FIELDS.split(","), [self._daily_row("sh.600000")])
        result.per_page_count = 1
        return result


class _FalseMarkerClient(_CompleteSdkClient):
    def _record(self, name: str, *args: object) -> None:
        context = current_request_context()
        self.calls.append((name, args, context))
        if name != "login":
            emit_terminal_observation(
                started_at=monotonic() - 0.001,
                protocol_stage=ProtocolStage.COMPLETE,
                recv_calls=1,
                response_bytes=128,
                end_marker_seen=False,
                provider_code=None,
                normalized_error=None,
            )


class _LoginAuditClient(_CompleteSdkClient):
    def login(self):
        self.login_calls += 1
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.OPERATION,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=None,
        )
        return _SdkResult([], [])


class _LoginCompleteAuditClient(_CompleteSdkClient):
    def login(self):
        self.login_calls += 1
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.COMPLETE,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=True,
            provider_code=None,
            normalized_error=None,
        )
        return _SdkResult([], [])


class _ReloginAuditClient(_CompleteSdkClient):
    def __init__(self) -> None:
        super().__init__()
        self.daily_calls = 0

    def login(self):
        self.login_calls += 1
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.OPERATION,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=None,
        )
        return _SdkResult([], [])

    def query_daily_history_k_AStock(self, **kwargs):
        self.daily_calls += 1
        if self.daily_calls == 1:
            raise TimeoutError("fixture relogin before first page")
        return super().query_daily_history_k_AStock(**kwargs)


class _DuplicateOperationClient(_CompleteSdkClient):
    def query_daily_history_k_AStock(self, **kwargs):
        self._record("query_daily_history_k_AStock", kwargs)
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.OPERATION,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=None,
        )
        return _SdkResult(DAILY_FIELDS.split(","), [self._daily_row("sh.600000")])


class _ReloginTwoPageClient(_TwoPageRetrySdkClient):
    def login(self):
        self.login_calls += 1
        emit_terminal_observation(
            started_at=monotonic() - 0.001,
            protocol_stage=ProtocolStage.OPERATION,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=None,
        )
        return _SdkResult([], [])


class _SuspendedStockClient(_CompleteSdkClient):
    def query_daily_history_k_AStock(self, **kwargs):
        self._record("query_daily_history_k_AStock", kwargs)
        row = self._daily_row("sh.600000")
        for field in ("volume", "amount", "turn", "pctChg"):
            row[DAILY_FIELDS.split(",").index(field)] = ""
        row[DAILY_FIELDS.split(",").index("tradestatus")] = "0"
        return _SdkResult(DAILY_FIELDS.split(","), [row])


class _SuspendedIndexClient(_CompleteSdkClient):
    def query_history_k_data_plus(self, code, fields, **kwargs):
        self._record("query_history_k_data_plus", code, fields, kwargs)
        row = self._daily_row(code)
        row[DAILY_FIELDS.split(",").index("volume")] = ""
        row[DAILY_FIELDS.split(",").index("amount")] = ""
        row[DAILY_FIELDS.split(",").index("tradestatus")] = "0"
        return _SdkResult(fields.split(","), [row])


class _FactorDateClient(_CompleteSdkClient):
    def __init__(self, event_date: str) -> None:
        super().__init__()
        self.event_date = event_date

    def query_daily_adjust_factor(self, **kwargs):
        self._record("query_daily_adjust_factor", kwargs)
        return _SdkResult(
            ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
            [["sh.600000", self.event_date, "1", "0.8", "0.8"]],
        )


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
    for contract in ENDPOINT_CONTRACTS:
        if contract.endpoint in {ProviderEndpoint.DAILY_ASTOCK, ProviderEndpoint.INDEX_HISTORY}:
            assert contract.fields == tuple(DAILY_FIELDS.split(","))
        elif contract.endpoint in {
            ProviderEndpoint.DAILY_FACTOR,
            ProviderEndpoint.ADJUST_FACTOR,
        }:
            assert contract.fields == (
                "code",
                "dividOperateDate",
                "foreAdjustFactor",
                "backAdjustFactor",
                "adjustFactor",
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


def test_session_and_range_request_date_rules_are_discriminated() -> None:
    with pytest.raises(ValueError):
        _plan().model_copy(update={"start_date": date(2026, 8, 19), "end_date": date(2026, 8, 20)})
    index_range = _plan(
        endpoint=ProviderEndpoint.INDEX_HISTORY,
        role=RequestRole.INDEX_HISTORY,
        instrument=InstrumentRole.INDEX,
        variant="index_history.range.v1",
        symbols=("sh.000001",),
    ).model_copy(update={"start_date": date(2026, 8, 19), "end_date": date(2026, 8, 20)})
    assert index_range.start_date < index_range.end_date


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
        successful_root_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    second = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(pages=(1, 2)),),
        successful_attempt=1,
        successful_root_request_id="request-1",
        final_outcome=TransportOutcome.SUCCESS,
        row_count=1,
    )
    assert plan.request_plan_hash == plan.compute_hash()
    assert first.compute_hash() != second.compute_hash()


def test_provider_raw_batch_rejects_arbitrary_completion_hash() -> None:
    with pytest.raises(ValueError):
        _provider_raw_batch().model_copy(update={"completion_hash": "f" * 64})


def test_provider_raw_batch_valid_baseline_binds_request_plan_and_projection() -> None:
    raw = _provider_raw_batch()
    assert raw.request.logical_request_plan == raw.logical_request_plan
    assert raw.request_plan_hash == raw.logical_request_plan.request_plan_hash
    assert raw.transport_lineage[0].observation_digest in {
        item.observation_digest for item in raw.transport_observations.observations
    }


def test_provider_raw_batch_rejects_duplicate_projection_and_lineage() -> None:
    raw = _provider_raw_batch()
    projections = raw.transport_observations.observations * 2
    with pytest.raises(ValueError):
        raw.model_copy(
            update={
                "transport_lineage": raw.transport_lineage * 2,
                "transport_observations": TransportObservationAggregate.from_observations(
                    projections
                ),
            }
        )


def test_request_completion_rejects_empty_duplicate_noncontiguous_pages_or_terminal_marker() -> (
    None
):
    with pytest.raises(ValueError):
        _attempt(pages=(1, 3))
    with pytest.raises(ValueError):
        _attempt(pages=(1, 1))
    with pytest.raises(ValueError):
        _attempt(pages=(), terminal=True, outcome=TransportOutcome.SUCCESS)


def test_request_completion_rejects_success_then_later_attempt_and_multiple_successes() -> None:
    with pytest.raises(ValueError):
        RequestCompletion(
            plan_ordinal=0,
            attempts=(_attempt(), _attempt(attempt=2, request_id="request-2")),
            successful_attempt=1,
            successful_root_request_id="request-1",
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
        successful_root_request_id="request-1",
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


def test_baostock_adapter_uses_ordinary_incumbent_sdk_boundary_for_all_endpoints() -> None:
    client = _CompleteSdkClient()
    request = _provider_request(
        plan=_full_plan(),
        session_symbols=("sh.000001", "sh.600000"),
    )
    adapter = BaoStockProviderAdapter(client=client, max_attempts=1, min_request_interval_seconds=0)

    raw = adapter.fetch_raw(request)

    assert [item.endpoint for item in raw.endpoint_batches] == list(ProviderEndpoint)
    assert {call[0] for call in client.calls} == {
        "query_trade_dates",
        "query_all_stock",
        "query_daily_history_k_AStock",
        "query_daily_adjust_factor",
        "query_adjust_factor",
        "query_history_k_data_plus",
    }
    assert client.login_calls == 1
    assert client.logout_calls == 1
    assert raw.endpoint_batches[2].rows[0].code == "sh.600000"


def test_baostock_adapter_login_and_relogin_share_request_session_identity() -> None:
    client = _SessionRecordingClient()
    adapter = BaoStockProviderAdapter(client=client, max_attempts=1, min_request_interval_seconds=0)

    adapter.fetch_raw(_provider_request())

    assert len(client.login_provider_sessions) == 1
    assert client.login_provider_sessions[0]
    assert {context.provider_session_id for _, _, context in client.calls} == {
        client.login_provider_sessions[0]
    }


@pytest.mark.parametrize(
    ("endpoint", "role", "instrument", "variant", "symbols", "override_name"),
    (
        (
            ProviderEndpoint.TRADE_DATES,
            RequestRole.CALENDAR,
            None,
            "trade_dates.v1",
            (),
            "trade_dates",
        ),
        (
            ProviderEndpoint.ALL_STOCK,
            RequestRole.UNIVERSE,
            InstrumentRole.STOCK,
            "all_stock.market.v1",
            (),
            "all_stock",
        ),
        (
            ProviderEndpoint.DAILY_ASTOCK,
            RequestRole.DAILY_STOCK,
            InstrumentRole.STOCK,
            "daily_astock.v1",
            ("sh.600000",),
            "daily_astock",
        ),
        (
            ProviderEndpoint.DAILY_FACTOR,
            RequestRole.DAILY_FACTOR,
            InstrumentRole.STOCK,
            "daily_factor.v1",
            ("sh.600000",),
            "daily_factor",
        ),
        (
            ProviderEndpoint.ADJUST_FACTOR,
            RequestRole.ADJUST_FACTOR,
            InstrumentRole.STOCK,
            "adjust_factor.session.v1",
            ("sh.600000",),
            "adjust_factor",
        ),
        (
            ProviderEndpoint.INDEX_HISTORY,
            RequestRole.INDEX_HISTORY,
            InstrumentRole.INDEX,
            "index_history.session.v1",
            ("sh.000001",),
            "index_history",
        ),
    ),
)
def test_baostock_adapter_rejects_every_nonexact_sdk_source_schema(
    endpoint,
    role,
    instrument,
    variant,
    symbols,
    override_name,
) -> None:
    logical = _plan(
        endpoint=endpoint,
        role=role,
        instrument=instrument,
        variant=variant,
        symbols=symbols,
    )
    request = _provider_request(
        plan=_plan_container(logical),
        session_symbols=("sh.000001", "sh.600000"),
    )
    defaults = {
        "trade_dates": ["calendar_date", "is_trading_day"],
        "all_stock": ["code", "tradeStatus", "code_name"],
        "daily_astock": DAILY_FIELDS.split(","),
        "daily_factor": [
            "code",
            "dividOperateDate",
            "foreAdjustFactor",
            "backAdjustFactor",
            "adjustFactor",
        ],
        "adjust_factor": [
            "code",
            "dividOperateDate",
            "foreAdjustFactor",
            "backAdjustFactor",
            "adjustFactor",
        ],
        "index_history": DAILY_FIELDS.split(","),
    }
    client = _CompleteSdkClient(
        schema_overrides={override_name: list(reversed(defaults[override_name]))}
    )
    adapter = BaoStockProviderAdapter(client=client, max_attempts=1, min_request_interval_seconds=0)

    with pytest.raises(ValueError, match="source schema"):
        adapter.fetch_raw(request)


def test_baostock_adapter_projects_exact_daily_wire_superset_to_contract() -> None:
    class DailyWireSupersetClient(_CompleteSdkClient):
        def query_daily_history_k_AStock(self, **kwargs):
            self._record("query_daily_history_k_AStock", kwargs)
            fields = DAILY_FIELDS.split(",")[:-1] + [
                "peTTM",
                "pbMRQ",
                "psTTM",
                "pcfNcfTTM",
                "isST",
            ]
            values = dict(zip(DAILY_FIELDS.split(","), self._daily_row("sh.600000"), strict=True))
            values.update({"peTTM": "8", "pbMRQ": "1", "psTTM": "2", "pcfNcfTTM": "3"})
            return _SdkResult(fields, [[values[field] for field in fields]])

    adapter = BaoStockProviderAdapter(
        client=DailyWireSupersetClient(),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    raw = adapter.fetch_raw(_provider_request())

    batch = raw.endpoint_batches[0]
    assert batch.fields == tuple(DAILY_FIELDS.split(","))
    assert batch.rows[0].code == "sh.600000"
    assert batch.rows[0].isST == "0"
    assert not hasattr(batch.rows[0], "peTTM")


def test_baostock_adapter_rejects_unknown_daily_wire_superset() -> None:
    fields = DAILY_FIELDS.split(",") + ["unknownMetric"]
    client = _CompleteSdkClient(schema_overrides={"daily_astock": fields})
    adapter = BaoStockProviderAdapter(
        client=client,
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(ValueError, match="source schema"):
        adapter.fetch_raw(_provider_request())


def test_baostock_adapter_maps_exact_daily_factor_wire_alias() -> None:
    class DailyFactorAliasClient(_CompleteSdkClient):
        def query_daily_adjust_factor(self, **kwargs):
            self._record("query_daily_adjust_factor", kwargs)
            return _SdkResult(
                [
                    "code",
                    "dividOperateDate",
                    "foreAdjustFactor",
                    "backAdjustFactor",
                    "adjustFacto",
                ],
                [
                    ["sz.000001", self.response_date, "1", "0.7", "0.7"],
                    ["sz.300001", self.response_date, "1", "0.9", "0.9"],
                    ["sh.600000", self.response_date, "1", "0.8", "0.8"],
                ],
            )

    logical = _plan(
        endpoint=ProviderEndpoint.DAILY_FACTOR,
        role=RequestRole.DAILY_FACTOR,
        instrument=InstrumentRole.STOCK,
        variant="daily_factor.v1",
        symbols=("sh.600000", "sz.000001"),
    )
    adapter = BaoStockProviderAdapter(
        client=DailyFactorAliasClient(),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    raw = adapter.fetch_raw(
        _provider_request(
            plan=_plan_container(logical),
            session_symbols=("sh.600000", "sz.000001"),
        )
    )

    batch = raw.endpoint_batches[0]
    assert batch.fields[-1] == "adjustFactor"
    assert batch.row_count == 2
    assert tuple(row.code for row in batch.rows) == ("sh.600000", "sz.000001")
    assert batch.rows[0].adjustFactor == 0.8


def test_baostock_adapter_publishes_one_source_batch_per_final_success_page() -> None:
    client = _TwoPageRetrySdkClient()
    adapter = BaoStockProviderAdapter(client=client, max_attempts=2, min_request_interval_seconds=0)

    raw = adapter.fetch_raw(_provider_request())

    completion = raw.request_completions[0]
    assert [(attempt.attempt, attempt.outcome) for attempt in completion.attempts] == [
        (1, TransportOutcome.ERROR),
        (2, TransportOutcome.SUCCESS),
    ]
    assert completion.observed_pages == (1, 2)
    assert [batch.lineage.page for batch in raw.endpoint_batches] == [1, 2]
    assert {batch.lineage.attempt for batch in raw.endpoint_batches} == {2}
    assert raw.endpoint_summaries[0].batch_count == 2
    assert raw.endpoint_summaries[0].row_count == completion.row_count == 2


def test_baostock_adapter_rejects_cross_day_sdk_rows_before_normalization() -> None:
    client = _CompleteSdkClient(response_date="2026-08-19")
    request = _provider_request(
        plan=_full_plan(),
        session_symbols=("sh.000001", "sh.600000"),
    )
    adapter = BaoStockProviderAdapter(client=client, max_attempts=1, min_request_interval_seconds=0)

    with pytest.raises(ValueError, match="date"):
        adapter.fetch_raw(request)


def test_baostock_adapter_discards_failed_partial_page_source_rows_on_retry() -> None:
    client = _RetryingSdkClient()
    adapter = BaoStockProviderAdapter(client=client, max_attempts=2, min_request_interval_seconds=0)

    raw = adapter.fetch_raw(_provider_request())

    completion = raw.request_completions[0]
    assert [(attempt.attempt, attempt.outcome) for attempt in completion.attempts] == [
        (1, TransportOutcome.ERROR),
        (2, TransportOutcome.SUCCESS),
    ]
    assert completion.row_count == 1
    assert len(raw.endpoint_batches) == 1
    assert raw.endpoint_batches[0].row_count == 1
    assert {item.attempt for item in raw.transport_observations.observations} == {1, 2}
    assert any(
        item.protocol_stage is ProtocolStage.PAGINATION
        and item.outcome is TransportOutcome.ERROR
        and item.normalized_error is not None
        for item in raw.transport_observations.observations
    )
    assert all(
        item.observation_digest == item.compute_digest()
        for item in raw.transport_observations.observations
    )


def test_baostock_adapter_captures_source_rows_before_normalization() -> None:
    client = _CompleteSdkClient()
    adapter = BaoStockProviderAdapter(
        client=client,
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    captured = adapter.fetch_raw(_provider_request())
    assert captured.endpoint_batches[0].rows[0].code == "sh.600000"
    assert client.calls[0][0] == "query_daily_history_k_AStock"


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

    with pytest.raises(TypeError):
        BaoStockProviderAdapter(client=_NetworkSentinelClient()).normalize(
            Evidence(), normalization_clock_utc=NOW
        )


def test_baostock_normalize_rejects_mutable_raw_duck_type_even_with_legal_page_shape() -> None:
    payload = json.loads((Path(__file__).parent / "fixtures" / "baostock_daily.json").read_text())

    class MutableRaw:
        def read_rows(self):
            return (
                {
                    "fields": payload["daily_fields"],
                    "rows": payload["daily_rows"],
                    "factor_fields": payload["factor_fields"],
                    "factor_rows": payload["factor_rows"],
                },
            )

    with pytest.raises(TypeError):
        BaoStockProviderAdapter(client=_NetworkSentinelClient()).normalize(
            MutableRaw(), normalization_clock_utc=NOW
        )


def test_daily_schema_preserves_legal_suspended_empty_activity_and_factor() -> None:
    row = DailyAStockRow(
        **_daily_row(tradestatus="0", volume=None, amount=None, turn=None, pctChg=None)
    )
    assert row.tradestatus == "0"
    assert row.volume is None
    assert row.amount is None


def test_raw_batch_binds_lineage_endpoint_and_request_identity() -> None:
    with pytest.raises(ValueError):
        _batch(lineage=_lineage(endpoint=ProviderEndpoint.INDEX_HISTORY))


def test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality() -> None:
    with pytest.raises(ValueError):
        _provider_raw_batch().model_copy(update={"endpoint_batches": ()})


def test_provider_raw_batch_rejects_failed_attempt_source_rows() -> None:
    failed = RequestCompletion(
        plan_ordinal=0,
        attempts=(_attempt(outcome=TransportOutcome.ERROR, pages=(), terminal=False),),
        final_outcome=TransportOutcome.ERROR,
        row_count=0,
    )
    with pytest.raises(ValueError):
        _provider_raw_batch().model_copy(
            update={"request_completions": (failed,), "endpoint_batches": ()}
        )


def test_active_daily_rows_reject_null_activity_and_suspended_rows_reject_activity() -> None:
    with pytest.raises(ValueError):
        DailyAStockRow(**_daily_row(volume=None))
    with pytest.raises(ValueError):
        DailyAStockRow(**_daily_row(tradestatus="0", volume="1", amount="0"))


def test_factor_rows_require_finite_positive_adjustment_factors() -> None:
    values = {
        "endpoint": ProviderEndpoint.DAILY_FACTOR,
        "schema_variant": "daily_factor.v1",
        "request_role": RequestRole.DAILY_FACTOR,
        "instrument_role": InstrumentRole.STOCK,
        "code": "sh.600000",
        "dividOperateDate": date(2026, 8, 20),
        "foreAdjustFactor": Decimal("1"),
        "backAdjustFactor": Decimal("0"),
        "adjustFactor": Decimal("0"),
    }
    with pytest.raises(ValueError):
        DailyFactorRow(**values)


def test_index_history_rows_reject_suspended_index() -> None:
    with pytest.raises(ValueError):
        IndexHistorySessionRow(
            endpoint=ProviderEndpoint.INDEX_HISTORY,
            schema_variant="index_history.session.v1",
            request_role=RequestRole.INDEX_HISTORY,
            instrument_role=InstrumentRole.INDEX,
            date=date(2026, 8, 20),
            code="sh.000001",
            open=1,
            high=1,
            low=1,
            close=1,
            preclose=1,
            volume=None,
            amount=None,
            adjustflag="3",
            turn=None,
            tradestatus="0",
            pctChg=None,
            isST="0",
        )


def test_safe_relative_path_rejects_traversal_repeated_and_trailing_components() -> None:
    for value in ("../x", "/abs", "a//b", "a/", "a/./b", "a/../b"):
        with pytest.raises(ValueError):
            provider_base.validate_safe_relative_path(value)


def test_registry_versions_are_safe_and_roundtrip_revalidated() -> None:
    registry = provider_registry()
    assert registry.adapter_version == "r2f2.v1"
    with pytest.raises(ValueError):
        registry.model_copy(update={"adapter_version": "not safe version"})
    with pytest.raises(ValueError):
        registry.model_copy(update={"adapter_version": "r2f2.v2"})
    with pytest.raises(ValueError):
        registry.model_copy(update={"endpoint_contract_version": "r2f2-endpoints.v2"})
    assert type(registry).model_validate(registry.model_dump()) == registry


def test_empty_daily_factor_event_is_valid_raw_task7_input() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.DAILY_FACTOR,
        role=RequestRole.DAILY_FACTOR,
        instrument=InstrumentRole.STOCK,
        variant="daily_factor.v1",
        symbols=("sh.600000",),
    )
    request = _provider_request(plan=_plan_container(logical))
    contract = endpoint_contract_for(
        ProviderEndpoint.DAILY_FACTOR, InstrumentRole.STOCK, "daily_factor.v1"
    )
    batch = RawEndpointBatch(
        endpoint=contract.endpoint,
        schema_variant=contract.schema_variant,
        request_role=contract.request_role,
        instrument_role=contract.instrument_role,
        plan_ordinal=0,
        shard_id="shard-0",
        lineage=_lineage(endpoint=ProviderEndpoint.DAILY_FACTOR),
        rows=(),
        row_count=0,
        source_schema=contract.schema_variant,
        fields=contract.fields,
        units=contract.units,
        date_semantics=contract.date_semantics,
        pagination_policy=contract.pagination_policy,
        provider_row_order_digest=sha256(b"[]").hexdigest(),
    )
    validate_raw_date_binding(request, batch)


def test_observation_digest_canonicalizes_observed_at_to_utc() -> None:
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
        outcome=TransportOutcome.SUCCESS,
        observed_at=datetime(2026, 8, 20, 16, 0, tzinfo=timezone(timedelta(hours=8))),
    )
    projection = TransportObservationProjection.from_observation(observation)
    assert projection.observed_at == datetime(2026, 8, 20, 8, 0, tzinfo=UTC)
    assert projection.observation_digest == projection.compute_digest()


def test_provider_public_exports_are_complete() -> None:
    exports = __import__("backend.app.market.providers", fromlist=["*"])
    for name in (
        "BaoStockDailyBarAdapter",
        "BaoStockProviderAdapter",
        "ENDPOINT_CONTRACTS",
        "AttemptCompletion",
        "EndpointContractSummary",
        "RawEndpointBatch",
        "ProviderId",
        "ProviderRawBatch",
        "TransportLineageRef",
        "TransportObservationProjection",
        "DailyAStockRow",
        "DailyFactorRow",
        "AdjustFactorRow",
        "IndexHistorySessionRow",
        "IndexHistoryRangeRow",
        "TradeDatesRow",
        "provider_registry",
    ):
        assert hasattr(exports, name), name


def test_task8_factor_provenance_models_are_not_task7_provider_fields() -> None:
    forbidden = {
        "factor_cache_records",
        "factor_resolution",
        "factor_resolution_sha256",
    }
    assert not forbidden.intersection(ProviderRawBatch.model_fields)


def test_provider_request_has_no_provider_session_or_transport_request_override_fields() -> None:
    assert set(ProviderRequest.model_fields) == {
        "provider_id",
        "refresh_id",
        "trade_date",
        "universe_id",
        "session_symbols",
        "logical_request_plan",
    }
    assert "provider_session_id" not in ProviderRequest.model_fields
    assert "request_id" not in ProviderRequest.model_fields


def test_attempt_completion_carries_actual_session_root_and_page_identities() -> None:
    assert {
        "provider_session_id",
        "root_request_id",
        "operation_observation_digest",
        "page_request_ids",
        "pagination_terminal_count",
    }.issubset(AttemptCompletion.model_fields)
    assert "request_id" not in AttemptCompletion.model_fields


def test_caller_cannot_supply_session_id_to_login_or_query_scope() -> None:
    provider = __import__(
        "backend.app.market.baostock", fromlist=["BaoStockProvider"]
    ).BaoStockProvider(client=_NetworkSentinelClient())
    with pytest.raises(TypeError):
        provider._login(TransportEndpoint.TRADE_DATES, provider_session_id="caller-id")
    with pytest.raises(TypeError):
        provider._ensure_session(TransportEndpoint.TRADE_DATES, provider_session_id="caller-id")


def test_capture_registry_projection_contains_derived_plan_and_lineage_kind() -> None:
    raw = BaoStockProviderAdapter(
        client=_ReloginAuditClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    assert any(
        item.lineage_kind == "login_audit" for item in raw.transport_observations.observations
    )
    assert any(
        item.lineage_kind == "query_root" for item in raw.transport_observations.observations
    )
    assert all(item.plan_ordinal == 0 for item in raw.transport_observations.observations)


def test_transport_lineage_closes_actual_root_and_page_tuple() -> None:
    assert set(TransportLineageRef.model_fields) == {
        "refresh_id",
        "provider_session_id",
        "root_request_id",
        "page_request_id",
        "endpoint",
        "plan_ordinal",
        "attempt",
        "page",
        "protocol_stage",
        "observation_digest",
    }


def test_success_attempt_requires_all_complete_frame_markers_and_one_pagination_terminal() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    assert raw.request_completions[0].attempts[-1].pagination_terminal_count == 1
    assert all(
        item.end_marker_seen
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE
    )


def test_page_one_frame_marker_true_still_enters_page_two() -> None:
    raw = BaoStockProviderAdapter(
        client=_TwoPageRetrySdkClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    assert raw.request_completions[0].attempts[-1].observed_pages == (1, 2)


def test_pagination_terminal_emitted_once_only_on_next_false() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    assert raw.request_completions[0].attempts[-1].pagination_terminal_count == 1


def test_page_after_pagination_terminal_is_rejected() -> None:
    with pytest.raises(Exception, match="full page"):
        BaoStockProviderAdapter(
            client=_FullPageExhaustionClient(), max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(_provider_request())


def test_baostock_capture_hook_is_additive_and_scope_registered() -> None:
    client = _CompleteSdkClient()
    adapter = BaoStockProviderAdapter(client=client, max_attempts=1, min_request_interval_seconds=0)
    raw = adapter.fetch_raw(_provider_request())
    assert raw.transport_observations.observations
    assert any(
        item.lineage_kind == "query_root" for item in raw.transport_observations.observations
    )


def test_capture_registry_derives_plan_ordinal_and_lineage_kind_into_digest() -> None:
    projection = _projection()
    changed = TransportObservationProjection.from_observation_with_lineage(
        TransportObservation.model_validate(
            projection.model_dump(exclude={"observation_digest", "plan_ordinal", "lineage_kind"})
        ),
        plan_ordinal=1,
        lineage_kind="page",
    )
    assert projection.observation_digest != changed.observation_digest


def test_unique_relogin_sessions_are_scope_generated_and_cannot_be_caller_overridden() -> None:
    provider = __import__(
        "backend.app.market.baostock", fromlist=["BaoStockProvider"]
    ).BaoStockProvider(client=_CompleteSdkClient())
    with provider.refresh_operation("refresh-identity"):
        provider._login(TransportEndpoint.TRADE_DATES)
        first = provider._provider_session_id
        provider._logout()
        provider._login(TransportEndpoint.TRADE_DATES)
        second = provider._provider_session_id
        provider._logout()
    assert first and second and first != second


def test_relogin_scope_generates_a_distinct_actual_session_id() -> None:
    test_unique_relogin_sessions_are_scope_generated_and_cannot_be_caller_overridden()


def test_query_request_scope_rebinds_the_saved_actual_login_session() -> None:
    provider = __import__(
        "backend.app.market.baostock", fromlist=["BaoStockProvider"]
    ).BaoStockProvider(client=_CompleteSdkClient())
    with provider.refresh_operation("refresh-rebind"):
        provider._login(TransportEndpoint.TRADE_DATES)
        actual = provider._provider_session_id
        with provider._request_scope(TransportEndpoint.DAILY_ASTOCK, attempt=1, page=1) as context:
            assert context.provider_session_id == actual
        provider._logout()


def test_query_root_completion_excludes_login_and_complete_observations() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    completion = raw.request_completions[0]
    operation = next(
        item
        for item in raw.transport_observations.observations
        if item.observation_digest == completion.attempts[0].operation_observation_digest
    )
    assert operation.protocol_stage is ProtocolStage.OPERATION
    assert all(item.protocol_stage is ProtocolStage.COMPLETE for item in raw.transport_lineage)


def test_multipage_page_request_identities_join_one_root_and_page() -> None:
    raw = BaoStockProviderAdapter(
        client=_TwoPageRetrySdkClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    attempt = raw.request_completions[0].attempts[-1]
    assert attempt.page_request_ids[0][1] == attempt.root_request_id
    assert len({request_id for _, request_id in attempt.page_request_ids}) == 2


def test_page_one_request_id_equals_query_root_id_and_page_n_has_own_id() -> None:
    test_multipage_page_request_identities_join_one_root_and_page()


def test_final_page_lineage_binds_complete_digest_not_operation_digest() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    completion = raw.request_completions[0].attempts[0]
    assert raw.transport_lineage[0].observation_digest != completion.operation_observation_digest


def test_page_capture_lineage_has_refresh_session_root_page_endpoint_attempt_page() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    lineage = raw.transport_lineage[0]
    assert lineage.refresh_id == "refresh-1"
    assert lineage.provider_session_id
    assert lineage.root_request_id == lineage.page_request_id
    assert lineage.endpoint is ProviderEndpoint.DAILY_ASTOCK
    assert lineage.attempt == 1 and lineage.page == 1


def test_failed_partial_page_has_no_lineage_or_descriptor() -> None:
    test_baostock_adapter_discards_failed_partial_page_source_rows_on_retry()


def test_missing_complete_frame_marker_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="marked COMPLETE"):
        BaoStockProviderAdapter(
            client=_FalseMarkerClient(), max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(_provider_request())


def test_provider_raw_batch_requires_exact_adapter_and_endpoint_contract_versions() -> None:
    registry = provider_registry()
    with pytest.raises(ValueError):
        registry.model_copy(update={"adapter_version": "r2f2.v2"})


def test_daily_factor_filters_rows_outside_exact_logical_symbols() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.DAILY_FACTOR,
        role=RequestRole.DAILY_FACTOR,
        instrument=InstrumentRole.STOCK,
        variant="daily_factor.v1",
        symbols=("sh.600000",),
    )
    client = _CompleteSdkClient()

    def wrong_factor(**kwargs):
        client._record("query_daily_adjust_factor", kwargs)
        return _SdkResult(
            ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
            [["sh.600001", "2026-08-20", "1", "0.8", "0.8"]],
        )

    client.query_daily_adjust_factor = wrong_factor
    raw = BaoStockProviderAdapter(
        client=client, max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(
        _provider_request(plan=_plan_container(logical), session_symbols=("sh.600000", "sh.600001"))
    )

    assert raw.endpoint_batches[0].row_count == 0


def test_daily_factor_event_date_equals_requested_session_and_rejects_older_or_future() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.DAILY_FACTOR,
        role=RequestRole.DAILY_FACTOR,
        instrument=InstrumentRole.STOCK,
        variant="daily_factor.v1",
        symbols=("sh.600000",),
    )
    with pytest.raises(ValueError, match="event date"):
        BaoStockProviderAdapter(
            client=_FactorDateClient("2026-08-19"), max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(
            _provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",))
        )
    raw = BaoStockProviderAdapter(
        client=_FactorDateClient("2026-08-20"), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",)))
    assert raw.endpoint_batches[0].rows[0].dividOperateDate == date(2026, 8, 20)


def test_adjust_factor_event_date_allows_history_through_requested_date_and_rejects_future() -> (
    None
):
    row = DailyFactorRow(
        endpoint=ProviderEndpoint.DAILY_FACTOR,
        schema_variant="daily_factor.v1",
        request_role=RequestRole.DAILY_FACTOR,
        instrument_role=InstrumentRole.STOCK,
        code="sh.600000",
        dividOperateDate=date(2026, 8, 19),
        foreAdjustFactor=1,
        backAdjustFactor=1,
        adjustFactor=1,
    )
    assert row.dividOperateDate <= date(2026, 8, 20)
    assert row.code == "sh.600000"


def test_factor_event_date_binds_through_date_without_invented_date_key() -> None:
    assert "date" not in provider_base.FactorFields.model_fields


def test_calendar_rows_are_unique_ordered_and_within_requested_range() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.TRADE_DATES,
        role=RequestRole.CALENDAR,
        instrument=None,
        variant="trade_dates.v1",
        symbols=(),
    )
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",)))
    assert raw.endpoint_batches[0].rows[0].calendar_date == date(2026, 8, 20)


def test_typed_adapter_maps_suspended_blank_numerics_to_none_only() -> None:
    test_typed_adapter_maps_suspended_source_blanks_only_for_stock_activity_fields()


def test_typed_adapter_rejects_required_blank_fields_and_suspended_index_placeholder() -> None:
    test_typed_adapter_rejects_required_blank_and_suspended_index_source_rows()


def test_task7_compatibility_normalize_uses_narrow_seam_not_published_evidence() -> None:
    test_baostock_compatibility_produces_existing_daily_bar_semantics()


def test_valid_fixture_survives_identity_and_schema_mutations_before_any_write() -> None:
    request = _provider_request()
    with pytest.raises(ValueError):
        request.model_copy(update={"session_symbols": ()})
    with pytest.raises(ValueError):
        _batch(fields=("code",))


def test_failed_attempt_before_page_one_retries_without_stop_iteration_or_failed_rows() -> None:
    client = _FailBeforePageOnce()
    raw = BaoStockProviderAdapter(
        client=client, max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    attempts = raw.request_completions[0].attempts
    assert attempts[0].outcome is TransportOutcome.ERROR
    assert attempts[0].observed_pages == ()
    assert attempts[1].outcome is TransportOutcome.SUCCESS
    assert all(batch.lineage.attempt == 2 for batch in raw.endpoint_batches)


def test_raw_batch_rejects_pseudo_digest_missing_and_conflicting_complete_lineage() -> None:
    raw = _provider_raw_batch()
    with pytest.raises(ValueError):
        raw.model_copy(update={"transport_lineage": ()})
    with pytest.raises(ValueError):
        raw.model_copy(
            update={
                "transport_lineage": (
                    raw.transport_lineage[0].model_copy(update={"observation_digest": "f" * 64}),
                )
            }
        )
    changed_completion = (
        raw.request_completions[0]
        .attempts[0]
        .model_copy(update={"operation_observation_digest": "f" * 64})
    )
    completion = raw.request_completions[0].model_copy(update={"attempts": (changed_completion,)})
    completion_hash = sha256(
        json.dumps(
            [completion.model_dump(mode="json")], sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError):
        raw.model_copy(
            update={"request_completions": (completion,), "completion_hash": completion_hash}
        )


def test_raw_endpoint_batch_recomputes_typed_row_order_digest_before_write() -> None:
    batch = _batch()
    changed = batch.rows[0].model_copy(update={"close": Decimal("12")})
    with pytest.raises(ValueError):
        batch.model_copy(update={"rows": (changed,)})


def test_raw_batch_binds_exact_logical_shard_role_schema_symbol_and_dates() -> None:
    logical = _plan(symbols=("sh.600000",), ordinal=0)
    request = _provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",))
    with pytest.raises(ValueError):
        validate_raw_date_binding(request, _batch(shard_id="wrong-shard"))
    with pytest.raises(ValueError):
        validate_raw_date_binding(
            request, _batch(row=DailyAStockRow(**_daily_row(code="sh.600001")))
        )
    with pytest.raises(ValueError):
        validate_raw_date_binding(
            request, _batch(row=DailyAStockRow(**_daily_row(date=date(2026, 8, 19))))
        )


def test_expected_logical_request_plan_rejects_duplicate_logical_identity_and_model_copy() -> None:
    first = _plan(ordinal=0)
    duplicate = first.model_copy(update={"plan_ordinal": 1})
    plan_hash = sha256(
        json.dumps(
            [item.model_dump(mode="json") for item in (first, duplicate)],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError):
        ExpectedLogicalRequestPlan(
            requests=(first, duplicate), request_count=2, request_plan_hash=plan_hash
        )


def test_capture_registry_scope_entry_marks_login_audit_and_query_root_without_adapter_guessing() -> (  # noqa: E501
    None
):
    raw = BaoStockProviderAdapter(
        client=_LoginAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    kinds = {item.lineage_kind for item in raw.transport_observations.observations}
    assert "login_audit" in kinds and "query_root" in kinds


def test_typed_adapter_maps_suspended_source_blanks_only_for_stock_activity_fields() -> None:
    logical = _plan()
    raw = BaoStockProviderAdapter(
        client=_SuspendedStockClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",)))
    row = raw.endpoint_batches[0].rows[0]
    assert row.volume is None and row.amount is None and row.turn is None and row.pctChg is None


def test_typed_adapter_rejects_required_blank_and_suspended_index_source_rows() -> None:
    client = _SuspendedStockClient()
    original = client._daily_row

    def blank_required(code: str):
        row = original(code)
        row[DAILY_FIELDS.split(",").index("close")] = ""
        return row

    client._daily_row = blank_required
    with pytest.raises(ValueError):
        BaoStockProviderAdapter(
            client=client, max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(_provider_request(session_symbols=("sh.600000",)))
    with pytest.raises(ValueError):
        BaoStockProviderAdapter(
            client=_SuspendedIndexClient(), max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(
            _provider_request(
                plan=_plan_container(
                    _plan(
                        endpoint=ProviderEndpoint.INDEX_HISTORY,
                        role=RequestRole.INDEX_HISTORY,
                        instrument=InstrumentRole.INDEX,
                        variant="index_history.session.v1",
                        symbols=("sh.000001",),
                    )
                ),
                session_symbols=("sh.000001",),
            )
        )


def test_trade_dates_batch_rejects_duplicate_out_of_order_and_out_of_range_rows() -> None:
    contract = endpoint_contract_for(ProviderEndpoint.TRADE_DATES, None, "trade_dates.v1")
    logical = _plan(
        endpoint=ProviderEndpoint.TRADE_DATES,
        role=RequestRole.CALENDAR,
        instrument=None,
        variant="trade_dates.v1",
        symbols=(),
    )
    request = _provider_request(plan=_plan_container(logical))

    def make(rows):
        batch = RawEndpointBatch(
            endpoint=contract.endpoint,
            schema_variant=contract.schema_variant,
            request_role=contract.request_role,
            instrument_role=None,
            plan_ordinal=0,
            shard_id="shard-0",
            lineage=_lineage(endpoint=ProviderEndpoint.TRADE_DATES),
            rows=tuple(rows),
            row_count=len(rows),
            source_schema=contract.schema_variant,
            fields=contract.fields,
            units=contract.units,
            date_semantics=contract.date_semantics,
            pagination_policy=contract.pagination_policy,
            provider_row_order_digest=sha256(
                json.dumps(
                    [item.model_dump(mode="json") for item in rows],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest(),
        )
        validate_raw_date_binding(request, batch)
        return batch

    row_a = TradeDatesRow(
        endpoint=ProviderEndpoint.TRADE_DATES,
        schema_variant="trade_dates.v1",
        request_role=RequestRole.CALENDAR,
        instrument_role=None,
        calendar_date=date(2026, 8, 20),
        is_trading_day="1",
    )
    row_b = row_a.model_copy(update={"calendar_date": date(2026, 8, 19)})
    with pytest.raises(ValueError):
        make((row_a, row_a))
    with pytest.raises(ValueError):
        make((row_a, row_b))
    with pytest.raises(ValueError):
        make((row_a.model_copy(update={"calendar_date": date(2026, 8, 21)}),))


def test_provider_registry_exposes_authoritative_supported_fields_without_plugins() -> None:
    registry = provider_registry()
    assert registry.supported_fields
    assert set(registry.supported_fields) == {
        contract.schema_variant for contract in ENDPOINT_CONTRACTS
    }


def _rebind_page_kind(raw: ProviderRawBatch, *, page: int, lineage_kind: str) -> ProviderRawBatch:
    target = next(
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE
        and item.page == page
        and item.attempt == raw.request_completions[0].successful_attempt
    )
    values = target.model_dump(mode="python")
    values["lineage_kind"] = lineage_kind
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    rebound = TransportObservationProjection.model_validate(values)
    observations = tuple(
        rebound if item.observation_digest == target.observation_digest else item
        for item in raw.transport_observations.observations
    )
    aggregate = TransportObservationAggregate.from_observations(observations)
    lineages = tuple(
        item.model_copy(update={"observation_digest": rebound.observation_digest})
        if item.observation_digest == target.observation_digest
        else item
        for item in raw.transport_lineage
    )
    batches = tuple(
        item.model_copy(
            update={
                "lineage": item.lineage.model_copy(
                    update={"observation_digest": rebound.observation_digest}
                )
            }
        )
        if item.lineage.observation_digest == target.observation_digest
        else item
        for item in raw.endpoint_batches
    )
    completion_hash = sha256(
        json.dumps(
            [item.model_dump(mode="json") for item in raw.request_completions],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return raw.model_copy(
        update={
            "transport_observations": aggregate,
            "transport_lineage": lineages,
            "endpoint_batches": batches,
            "completion_hash": completion_hash,
        }
    )


def _append_successful_projection(
    raw: ProviderRawBatch, *, request_id: str, page: int
) -> ProviderRawBatch:
    target = next(
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE
        and item.outcome is TransportOutcome.SUCCESS
        and item.end_marker_seen
        and item.attempt == raw.request_completions[0].successful_attempt
    )
    values = target.model_dump(mode="python")
    values.update(request_id=request_id, page=page, lineage_kind="page")
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    extra = TransportObservationProjection.model_validate(values)
    observations = (*raw.transport_observations.observations, extra)
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _two_logical_plan() -> ExpectedLogicalRequestPlan:
    requests = (
        _plan(
            endpoint=ProviderEndpoint.TRADE_DATES,
            role=RequestRole.CALENDAR,
            instrument=None,
            variant="trade_dates.v1",
            symbols=(),
            ordinal=0,
        ),
        _plan(ordinal=1),
    )
    plan_hash = sha256(
        json.dumps(
            [item.model_dump(mode="json") for item in requests],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return ExpectedLogicalRequestPlan(
        requests=requests,
        request_count=2,
        request_plan_hash=plan_hash,
    )


def _append_projection_for_attempt(
    raw: ProviderRawBatch,
    *,
    plan_ordinal: int,
    attempt: int,
    endpoint: ProviderEndpoint,
    projection_plan_ordinal: int,
    request_id: str,
    page: int,
) -> ProviderRawBatch:
    target = next(
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE
        and item.outcome is TransportOutcome.SUCCESS
        and item.end_marker_seen
        and item.plan_ordinal == plan_ordinal
        and item.attempt == attempt
    )
    values = target.model_dump(mode="python")
    values.update(
        endpoint=endpoint,
        plan_ordinal=projection_plan_ordinal,
        request_id=request_id,
        page=page,
        lineage_kind="query_root" if page == 1 else "page",
    )
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    extra = TransportObservationProjection.model_validate(values)
    observations = (*raw.transport_observations.observations, extra)
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _rebind_login_capture_identity(
    raw: ProviderRawBatch, *, full_query_identity: bool
) -> ProviderRawBatch:
    login = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == "login_audit" and item.protocol_stage is ProtocolStage.COMPLETE
    )
    query = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == "query_root" and item.protocol_stage is ProtocolStage.COMPLETE
    )
    values = login.model_dump(mode="python")
    values.update(
        refresh_id=query.refresh_id,
        provider_session_id=query.provider_session_id,
        request_id=query.request_id,
    )
    if full_query_identity:
        values.update(
            endpoint=query.endpoint,
            plan_ordinal=query.plan_ordinal,
            attempt=query.attempt,
            page=query.page,
        )
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    rebound = TransportObservationProjection.model_validate(values)
    observations = tuple(
        rebound if item.observation_digest == login.observation_digest else item
        for item in raw.transport_observations.observations
    )
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _append_login_protocol_stage(raw: ProviderRawBatch, **updates: Any) -> ProviderRawBatch:
    login = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == "login_audit" and item.protocol_stage is ProtocolStage.COMPLETE
    )
    values = login.model_dump(mode="python")
    values.update(protocol_stage=ProtocolStage.OPERATION, end_marker_seen=False, **updates)
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    extra = TransportObservationProjection.model_validate(values)
    observations = (*raw.transport_observations.observations, extra)
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _rebind_capture_lineage_kind(
    raw: ProviderRawBatch, *, source_kind: str, target_kind: str
) -> ProviderRawBatch:
    target = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == source_kind and item.protocol_stage is ProtocolStage.COMPLETE
    )
    values = target.model_dump(mode="python")
    values["lineage_kind"] = target_kind
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    rebound = TransportObservationProjection.model_validate(values)
    observations = tuple(
        rebound if item.observation_digest == target.observation_digest else item
        for item in raw.transport_observations.observations
    )
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _append_operation_projection(raw: ProviderRawBatch, **updates: Any) -> ProviderRawBatch:
    target = next(
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.OPERATION and item.lineage_kind == "query_root"
    )
    values = target.model_dump(mode="python")
    values.update(updates)
    values.pop("observation_digest")
    candidate = TransportObservationProjection.model_construct(
        **values, observation_digest="0" * 64
    )
    values["observation_digest"] = candidate.compute_digest()
    extra = TransportObservationProjection.model_validate(values)
    observations = (*raw.transport_observations.observations, extra)
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations)
        }
    )


def _rebind_failed_attempt_to_other_query_root(raw: ProviderRawBatch) -> ProviderRawBatch:
    source_completion = raw.request_completions[1]
    source_attempt = source_completion.attempts[0]
    target_root = raw.request_completions[0].attempts[-1].root_request_id
    changed_projection_by_digest: dict[str, TransportObservationProjection] = {}
    for projection in raw.transport_observations.observations:
        if not (
            projection.plan_ordinal == source_completion.plan_ordinal
            and projection.attempt == source_attempt.attempt
            and projection.request_id == source_attempt.root_request_id
        ):
            continue
        values = projection.model_dump(mode="python")
        values["request_id"] = target_root
        values.pop("observation_digest")
        candidate = TransportObservationProjection.model_construct(
            **values, observation_digest="0" * 64
        )
        values["observation_digest"] = candidate.compute_digest()
        changed = TransportObservationProjection.model_validate(values)
        changed_projection_by_digest[projection.observation_digest] = changed
    operation = next(
        item
        for item in changed_projection_by_digest.values()
        if item.protocol_stage is ProtocolStage.OPERATION
    )
    observations = tuple(
        changed_projection_by_digest.get(item.observation_digest, item)
        for item in raw.transport_observations.observations
    )
    changed_attempt = source_attempt.model_copy(
        update={
            "root_request_id": target_root,
            "page_request_ids": tuple(
                (page, target_root if page == 1 else request_id)
                for page, request_id in source_attempt.page_request_ids
            ),
            "operation_observation_digest": operation.observation_digest,
        }
    )
    changed_completion = source_completion.model_copy(
        update={"attempts": (changed_attempt, *source_completion.attempts[1:])}
    )
    completions = (*raw.request_completions[:1], changed_completion, *raw.request_completions[2:])
    completion_hash = sha256(
        json.dumps(
            [item.model_dump(mode="json") for item in completions],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return raw.model_copy(
        update={
            "transport_observations": TransportObservationAggregate.from_observations(observations),
            "request_completions": completions,
            "completion_hash": completion_hash,
        }
    )


def test_query_capture_identity_has_one_authoritative_owner_across_logicals() -> None:
    raw = BaoStockProviderAdapter(
        client=_ReloginTwoPageClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=_two_logical_plan()))
    with pytest.raises(ValueError, match="conflicting query owners|capture identity"):
        _rebind_failed_attempt_to_other_query_root(raw)


@pytest.mark.parametrize(
    "updates",
    (
        {"endpoint": ProviderEndpoint.TRADE_DATES},
        {"plan_ordinal": 1},
        {"attempt": 2},
        {"page": 2},
        {"lineage_kind": "page"},
    ),
)
def test_every_unbound_operation_projection_is_rejected(updates: dict[str, Any]) -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="operation"):
        _append_operation_projection(raw, **updates)


def test_duplicate_operation_digest_is_rejected() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="operation"):
        _append_operation_projection(raw)


@pytest.mark.parametrize("full_query_identity", (True, False))
def test_login_capture_identity_cannot_collide_with_query_namespace(
    full_query_identity: bool,
) -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="login capture identity collides with query"):
        _rebind_login_capture_identity(raw, full_query_identity=full_query_identity)


def test_login_capture_identity_and_multiple_login_protocol_stages_are_allowed() -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    rebound = _append_login_protocol_stage(raw)
    login_identities = {
        (item.refresh_id, item.provider_session_id, item.request_id)
        for item in rebound.transport_observations.observations
        if item.lineage_kind == "login_audit"
    }
    assert len(login_identities) == 1
    assert {
        item.protocol_stage
        for item in rebound.transport_observations.observations
        if item.lineage_kind == "login_audit"
    } == {ProtocolStage.COMPLETE, ProtocolStage.OPERATION}


def test_login_capture_identity_rejects_conflicting_owner_metadata() -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="conflicting login owners"):
        _append_login_protocol_stage(raw, plan_ordinal=1)


def test_query_complete_cannot_be_reclassified_as_login_audit() -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="login capture identity collides with query"):
        _rebind_capture_lineage_kind(raw, source_kind="query_root", target_kind="login_audit")


def test_login_capture_reclassified_as_query_requires_attempt_binding() -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError, match="owned by an attempt|bound to an attempt"):
        _rebind_capture_lineage_kind(raw, source_kind="login_audit", target_kind="query_root")


@pytest.mark.parametrize(
    ("endpoint", "projection_plan_ordinal", "request_id", "page"),
    (
        (ProviderEndpoint.TRADE_DATES, 0, "cross-logical-trade-dates", 3),
        (ProviderEndpoint.INDEX_HISTORY, 1, "cross-logical-index-history", 1),
    ),
)
def test_query_page_projection_must_bind_exact_logical_attempt(
    endpoint: ProviderEndpoint,
    projection_plan_ordinal: int,
    request_id: str,
    page: int,
) -> None:
    plan = _two_logical_plan()
    raw = BaoStockProviderAdapter(
        client=_ReloginTwoPageClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=plan))
    daily_attempt = raw.request_completions[1].successful_attempt
    with pytest.raises(ValueError, match="owned by an attempt|bound to an attempt"):
        _append_projection_for_attempt(
            raw,
            plan_ordinal=1,
            attempt=daily_attempt,
            endpoint=endpoint,
            projection_plan_ordinal=projection_plan_ordinal,
            request_id=request_id,
            page=page,
        )


def test_failed_partial_query_page_with_shared_session_attempt_is_aggregate_only() -> None:
    plan = _two_logical_plan()
    raw = BaoStockProviderAdapter(
        client=_ReloginTwoPageClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=plan))
    calendar_attempt = raw.request_completions[0].attempts[0]
    daily_failed_attempt = raw.request_completions[1].attempts[0]
    assert (calendar_attempt.provider_session_id, calendar_attempt.attempt) == (
        daily_failed_attempt.provider_session_id,
        daily_failed_attempt.attempt,
    )
    assert daily_failed_attempt.outcome is TransportOutcome.ERROR
    assert any(
        item.plan_ordinal == 1
        and item.attempt == daily_failed_attempt.attempt
        and item.protocol_stage is ProtocolStage.COMPLETE
        and item.outcome is TransportOutcome.SUCCESS
        for item in raw.transport_observations.observations
    )
    assert all(
        item.lineage.attempt == raw.request_completions[item.plan_ordinal].successful_attempt
        for item in raw.endpoint_batches
    )


def test_complete_login_audit_is_allowed_without_raw_page_lineage() -> None:
    raw = BaoStockProviderAdapter(
        client=_LoginCompleteAuditClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    login = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == "login_audit" and item.protocol_stage is ProtocolStage.COMPLETE
    )
    query = next(
        item
        for item in raw.transport_observations.observations
        if item.lineage_kind == "query_root" and item.protocol_stage is ProtocolStage.COMPLETE
    )
    assert login.provider_session_id == query.provider_session_id
    assert login.request_id != query.request_id


@pytest.mark.parametrize(
    ("request_id", "page"),
    (("extra-page", 3), ("duplicate-page", 1)),
)
def test_final_success_scope_rejects_unbound_successful_complete_projection(
    request_id: str, page: int
) -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError):
        _append_successful_projection(raw, request_id=request_id, page=page)


def test_page_one_kind_tamper_is_rejected_after_all_local_digests_are_rebound() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError):
        _rebind_page_kind(raw, page=1, lineage_kind="page")


def test_page_n_reverse_kind_tamper_is_rejected_after_all_local_digests_are_rebound() -> None:
    raw = BaoStockProviderAdapter(
        client=_TwoPageRetrySdkClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError):
        _rebind_page_kind(raw, page=2, lineage_kind="query_root")


def test_authoritative_page_kind_index_accepts_correct_page_one_and_page_n() -> None:
    raw = BaoStockProviderAdapter(
        client=_TwoPageRetrySdkClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    pages = [
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE
        and item.attempt == raw.request_completions[0].successful_attempt
    ]
    assert [(item.page, item.lineage_kind) for item in pages] == [
        (1, "query_root"),
        (2, "page"),
    ]


def test_authoritative_page_index_rejects_missing_page_lineage() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError):
        raw.model_copy(update={"transport_lineage": ()})


def test_authoritative_page_index_rejects_duplicate_page_lineage() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    with pytest.raises(ValueError):
        raw.model_copy(update={"transport_lineage": raw.transport_lineage * 2})


def test_lineage_join_rejects_forged_root_tuple_against_successful_page_projection() -> None:
    raw = _provider_raw_batch()
    forged = raw.transport_lineage[0].model_copy(update={"root_request_id": "forged-root"})
    with pytest.raises(ValueError):
        raw.model_copy(update={"transport_lineage": (forged,)})


def test_each_attempt_operation_digest_must_join_its_query_root_projection() -> None:
    raw = BaoStockProviderAdapter(
        client=_TwoPageRetrySdkClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    completion = raw.request_completions[0]
    failed = completion.attempts[0].model_copy(update={"operation_observation_digest": "f" * 64})
    changed = completion.model_copy(update={"attempts": (failed, completion.attempts[1])})
    completion_hash = sha256(
        json.dumps(
            [changed.model_dump(mode="json")], sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError):
        raw.model_copy(
            update={"request_completions": (changed,), "completion_hash": completion_hash}
        )


def test_capture_registry_preserves_query_root_kind_on_page_one_complete() -> None:
    raw = BaoStockProviderAdapter(
        client=_CompleteSdkClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request())
    page_one = next(
        item
        for item in raw.transport_observations.observations
        if item.protocol_stage is ProtocolStage.COMPLETE and item.page == 1
    )
    assert page_one.lineage_kind == "query_root"


def test_capture_registry_active_relogin_and_multipage_kinds_are_scope_authoritative() -> None:
    calendar = _plan(
        endpoint=ProviderEndpoint.TRADE_DATES,
        role=RequestRole.CALENDAR,
        instrument=None,
        variant="trade_dates.v1",
        symbols=(),
        ordinal=0,
    )
    daily = _plan(ordinal=1)
    requests = (calendar, daily)
    plan_hash = sha256(
        json.dumps(
            [item.model_dump(mode="json") for item in requests],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    plan = ExpectedLogicalRequestPlan(
        requests=requests, request_count=2, request_plan_hash=plan_hash
    )
    raw = BaoStockProviderAdapter(
        client=_ReloginTwoPageClient(), max_attempts=2, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=plan))
    observations = raw.transport_observations.observations
    assert any(
        item.lineage_kind == "login_audit" and item.plan_ordinal == 1 for item in observations
    )
    login_scopes = {
        (item.provider_session_id, item.request_id)
        for item in observations
        if item.lineage_kind == "login_audit"
    }
    assert len({session_id for session_id, _ in login_scopes}) >= 2
    assert len({request_id for _, request_id in login_scopes}) >= 2
    pages = [
        item
        for item in observations
        if item.plan_ordinal == 1
        and item.protocol_stage is ProtocolStage.COMPLETE
        and item.attempt == raw.request_completions[1].successful_attempt
    ]
    assert [(item.page, item.lineage_kind) for item in pages] == [(1, "query_root"), (2, "page")]
    assert pages[0].request_id != pages[1].request_id


def test_stock_role_index_history_maps_only_suspended_activity_blanks() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.INDEX_HISTORY,
        role=RequestRole.INDEX_HISTORY,
        instrument=InstrumentRole.STOCK,
        variant="index_history.session.v1",
        symbols=("sh.600000",),
    )
    raw = BaoStockProviderAdapter(
        client=_SuspendedIndexClient(), max_attempts=1, min_request_interval_seconds=0
    ).fetch_raw(_provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",)))
    row = raw.endpoint_batches[0].rows[0]
    assert row.volume is None and row.amount is None
    assert row.turn == 1 and row.pctChg == 5


def test_adjust_factor_future_date_rejected_through_real_adapter_source() -> None:
    logical = _plan(
        endpoint=ProviderEndpoint.ADJUST_FACTOR,
        role=RequestRole.ADJUST_FACTOR,
        instrument=InstrumentRole.STOCK,
        variant="adjust_factor.session.v1",
        symbols=("sh.600000",),
    )
    client = _CompleteSdkClient()
    client.query_adjust_factor = lambda *args, **kwargs: (
        client._record("query_adjust_factor", args, kwargs),
        _SdkResult(
            ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
            [["sh.600000", "2026-08-21", "1", "0.8", "0.8"]],
        ),
    )[1]
    with pytest.raises(ValueError, match="after requested"):
        BaoStockProviderAdapter(
            client=client, max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(
            _provider_request(plan=_plan_container(logical), session_symbols=("sh.600000",))
        )


def test_login_operation_cannot_be_reused_as_query_root_completion() -> None:
    with pytest.raises(ValueError):
        BaoStockProviderAdapter(
            client=_DuplicateOperationClient(), max_attempts=1, min_request_interval_seconds=0
        ).fetch_raw(_provider_request())


def test_evidence_object_kind_resolution_is_additive_without_changing_task7_members() -> None:
    assert provider_base.EvidenceObjectKind.RAW_ENDPOINT_PAGE.value == "raw_endpoint_page"
    assert provider_base.EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT.value == "factor_cache_snapshot"
    assert (
        provider_base.EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT.value
        == "factor_resolution_snapshot"
    )
