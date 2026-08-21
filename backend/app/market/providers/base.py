"""Frozen, provider-neutral contracts at the source capture boundary.

The module intentionally contains a closed BaoStock allow-list.  It is not a
plugin registry and it does not expose provider payloads or arbitrary mappings.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from backend.app.market.models import DailyBar
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderCode,
    TransportObservation,
    TransportOutcome,
)

_EMPTY_SHA256 = "0" * 64

SafeProviderId = Annotated[
    str, StringConstraints(min_length=1, max_length=32, pattern=r"^[a-z][a-z0-9-]{0,31}$")
]
SafeSymbol = Annotated[
    str, StringConstraints(min_length=9, max_length=9, pattern=r"^(sh|sz)\.[0-9]{6}$")
]
SafeVersion = Annotated[
    str,
    StringConstraints(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$"),
]
SafeIdentifier = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"),
]
SafeSha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SafeRelativePath = Annotated[
    str, StringConstraints(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9._/-]+$")
]
BoundedToken = Annotated[str, StringConstraints(max_length=64, pattern=r"^[A-Za-z0-9._:-]*$")]
BoundedText = Annotated[str, StringConstraints(max_length=256)]
SafeFailureClass = Literal[
    "transport_timeout",
    "transport_connect",
    "dns",
    "auth",
    "rate_limit",
    "provider_4xx",
    "provider_5xx",
    "schema",
    "semantic",
    "calendar",
    "universe",
    "coverage",
    "reconciliation",
    "storage",
    "internal",
    "CONNECT_ERROR",
    "SEND_ERROR",
    "RECV_TIMEOUT",
    "EOF",
    "SHORT_HEADER",
    "BAD_COMPRESSION",
    "PROTOCOL_ERROR",
    "PAGINATION_STALLED",
    "RATE_LIMIT",
    "UNKNOWN_PROVIDER_PROTOCOL_ERROR",
    "EVIDENCE_ROOT_UNAVAILABLE",
    "EVIDENCE_SCHEMA_MISMATCH",
    "EVIDENCE_HASH_MISMATCH",
    "EVIDENCE_OBJECT_OVERSIZE",
    "EVIDENCE_UNSAFE_PATH",
    "EVIDENCE_MANIFEST_INVALID",
    "REPLAY_NONDETERMINISTIC",
    "REPLAY_SEMANTIC_MISMATCH",
]


class ProviderEndpoint(StrEnum):
    TRADE_DATES = "trade_dates"
    ALL_STOCK = "all_stock"
    DAILY_ASTOCK = "daily_astock"
    DAILY_FACTOR = "daily_factor"
    ADJUST_FACTOR = "adjust_factor"
    INDEX_HISTORY = "index_history"


class InstrumentRole(StrEnum):
    STOCK = "stock"
    INDEX = "index"


class RequestRole(StrEnum):
    CALENDAR = "calendar"
    UNIVERSE = "universe"
    DAILY_STOCK = "daily_stock"
    DAILY_FACTOR = "daily_factor"
    ADJUST_FACTOR = "adjust_factor"
    INDEX_HISTORY = "index_history"


class PaginationPolicy(StrEnum):
    PROVIDER_TERMINAL = "provider_terminal"


class ProviderId(StrEnum):
    BAOSTOCK = "baostock"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode()


def _digest(value: object) -> str:
    return sha256(_canonical(value)).hexdigest()


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, object] | None = None, deep: bool = False):
        values = self.model_dump()
        if update:
            values.update(update)
        return type(self).model_validate(values)


class ExpectedLogicalRequest(_ContractModel):
    plan_ordinal: int = Field(ge=0, le=4096)
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    shard_id: SafeIdentifier
    symbols: tuple[SafeSymbol, ...]
    start_date: date
    end_date: date
    pagination_policy: PaginationPolicy

    @field_validator("symbols")
    @classmethod
    def sorted_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("symbols must be sorted and unique")
        return value

    @model_validator(mode="after")
    def validate_call_symbols(self) -> ExpectedLogicalRequest:
        if self.start_date > self.end_date:
            raise ValueError("request date range is invalid")
        if self.endpoint in {ProviderEndpoint.TRADE_DATES, ProviderEndpoint.ALL_STOCK}:
            if self.symbols:
                raise ValueError("calendar and universe calls require empty per-call symbols")
        elif not self.symbols:
            raise ValueError("non-calendar/non-universe calls require per-call symbols")
        expected = {
            ProviderEndpoint.TRADE_DATES: (RequestRole.CALENDAR, None),
            ProviderEndpoint.ALL_STOCK: (RequestRole.UNIVERSE, InstrumentRole.STOCK),
            ProviderEndpoint.DAILY_ASTOCK: (RequestRole.DAILY_STOCK, InstrumentRole.STOCK),
            ProviderEndpoint.DAILY_FACTOR: (RequestRole.DAILY_FACTOR, InstrumentRole.STOCK),
            ProviderEndpoint.ADJUST_FACTOR: (RequestRole.ADJUST_FACTOR, InstrumentRole.STOCK),
            ProviderEndpoint.INDEX_HISTORY: (RequestRole.INDEX_HISTORY, self.instrument_role),
        }
        role, instrument = expected[self.endpoint]
        if self.request_role is not role or self.instrument_role is not instrument:
            raise ValueError("endpoint and role combination is invalid")
        if self.endpoint is ProviderEndpoint.ADJUST_FACTOR and len(self.symbols) != 1:
            raise ValueError("adjust-factor shards contain one symbol")
        if (
            self.endpoint is ProviderEndpoint.INDEX_HISTORY
            and self.instrument_role is InstrumentRole.INDEX
        ):
            if any(symbol not in {"sh.000001", "sz.399001"} for symbol in self.symbols):
                raise ValueError("index-history index shard contains a stock symbol")
        return self


class ExpectedLogicalRequestPlan(_ContractModel):
    requests: tuple[ExpectedLogicalRequest, ...]
    request_count: int = Field(ge=1, le=4096)
    request_plan_hash: SafeSha256

    @model_validator(mode="after")
    def validate_count(self) -> ExpectedLogicalRequestPlan:
        if len(self.requests) != self.request_count:
            raise ValueError("request count mismatch")
        ordinals = tuple(item.plan_ordinal for item in self.requests)
        if ordinals != tuple(range(len(self.requests))):
            raise ValueError("request plan ordinals must be ordered")
        return self

    @model_validator(mode="after")
    def validate_hash(self) -> ExpectedLogicalRequestPlan:
        if self.request_plan_hash != self.compute_hash():
            raise ValueError("logical request plan hash mismatch")
        return self

    def compute_hash(self) -> str:
        return _digest([item.model_dump(mode="json") for item in self.requests])


class AttemptCompletion(_ContractModel):
    plan_ordinal: int = Field(ge=0, le=4096)
    attempt: int = Field(ge=1, le=20)
    request_id: SafeIdentifier
    observed_pages: tuple[int, ...]
    observed_page_count: int = Field(ge=0, le=16_384)
    terminal: bool
    outcome: TransportOutcome

    @model_validator(mode="after")
    def validate_observed_pages(self) -> AttemptCompletion:
        if self.observed_page_count != len(self.observed_pages):
            raise ValueError("observed page count mismatch")
        if self.observed_pages != tuple(range(1, self.observed_page_count + 1)):
            if self.observed_pages:
                raise ValueError("observed pages must be contiguous")
        if self.outcome is TransportOutcome.SUCCESS and (
            not self.terminal or not self.observed_pages
        ):
            raise ValueError("successful attempt must be terminal and have a page")
        return self


class RequestCompletion(_ContractModel):
    plan_ordinal: int = Field(ge=0, le=4096)
    attempts: tuple[AttemptCompletion, ...]
    successful_attempt: int | None = Field(default=None, ge=1, le=20)
    successful_request_id: SafeIdentifier | None = None
    final_outcome: TransportOutcome
    row_count: int = Field(default=0, ge=0, le=10_000_000)

    @model_validator(mode="after")
    def validate_final_attempt(self) -> RequestCompletion:
        if not self.attempts:
            raise ValueError("completion requires an attempt")
        if tuple(item.attempt for item in self.attempts) != tuple(range(1, len(self.attempts) + 1)):
            raise ValueError("attempts must be strictly ordered")
        if len({item.request_id for item in self.attempts}) != len(self.attempts):
            raise ValueError("attempt request IDs must be unique")
        if self.final_outcome is not self.attempts[-1].outcome:
            raise ValueError("final outcome must match final attempt")
        successes = [item for item in self.attempts if item.outcome is TransportOutcome.SUCCESS]
        if self.final_outcome is TransportOutcome.SUCCESS:
            final = self.attempts[-1]
            if len(successes) != 1 or self.successful_attempt != final.attempt:
                raise ValueError("a completion has exactly one final successful attempt")
            if self.successful_request_id != final.request_id:
                raise ValueError("successful request binding mismatch")
        elif (
            successes
            or self.successful_attempt is not None
            or self.successful_request_id is not None
        ):
            raise ValueError("failed completion cannot bind a successful attempt")
        if self.final_outcome is TransportOutcome.ERROR and self.row_count != 0:
            raise ValueError("failed completion row count must be zero")
        return self

    def compute_hash(self) -> str:
        return _digest(self.model_dump(mode="json"))


class ProviderRequest(_ContractModel):
    provider_id: ProviderId
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    session_symbols: tuple[SafeSymbol, ...]
    request_plan: ExpectedLogicalRequestPlan

    @field_validator("session_symbols")
    @classmethod
    def complete_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(set(value))):
            raise ValueError("session_symbols must be non-empty, sorted and unique")
        return value


class TransportLineageRef(_ContractModel):
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    endpoint: ProviderEndpoint
    plan_ordinal: int = Field(ge=0, le=4096)
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16_384)
    observation_digest: SafeSha256


class RawEndpointRowBase(_ContractModel):
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    request_role: RequestRole
    instrument_role: InstrumentRole | None

    @model_validator(mode="after")
    def role_matches(self) -> RawEndpointRowBase:
        contract = endpoint_contract_for(self.endpoint, self.instrument_role, self.schema_variant)
        if self.request_role is not contract.request_role:
            raise ValueError("row request role does not match endpoint contract")
        return self


class TradeDatesRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.TRADE_DATES]
    schema_variant: Literal["trade_dates.v1"]
    calendar_date: date
    is_trading_day: BoundedToken


class AllStockRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.ALL_STOCK]
    schema_variant: Literal["all_stock.market.v1"]
    code: SafeSymbol
    tradeStatus: BoundedToken
    code_name: BoundedText


class DailyAStockRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.DAILY_ASTOCK]
    schema_variant: Literal["daily_astock.v1"]
    date: date
    code: SafeSymbol
    open: float | Decimal
    high: float | Decimal
    low: float | Decimal
    close: float | Decimal
    preclose: float | Decimal
    volume: float | Decimal | None
    amount: float | Decimal | None
    adjustflag: Literal["3"]
    turn: float | Decimal | None
    tradestatus: BoundedToken
    pctChg: float | Decimal | None
    isST: BoundedToken


class FactorFields(_ContractModel):
    code: SafeSymbol
    dividOperateDate: date
    foreAdjustFactor: float | Decimal
    backAdjustFactor: float | Decimal
    adjustFactor: float | Decimal


class DailyFactorRow(RawEndpointRowBase, FactorFields):
    endpoint: Literal[ProviderEndpoint.DAILY_FACTOR]
    schema_variant: Literal["daily_factor.v1"]


class AdjustFactorRow(RawEndpointRowBase, FactorFields):
    endpoint: Literal[ProviderEndpoint.ADJUST_FACTOR]
    schema_variant: Literal["adjust_factor.session.v1"]


class IndexHistoryFields(_ContractModel):
    date: date
    code: SafeSymbol
    open: float | Decimal
    high: float | Decimal
    low: float | Decimal
    close: float | Decimal
    preclose: float | Decimal
    volume: float | Decimal | None
    amount: float | Decimal | None
    adjustflag: Literal["3"]
    turn: float | Decimal | None
    tradestatus: BoundedToken
    pctChg: float | Decimal | None
    isST: BoundedToken


class IndexHistorySessionRow(RawEndpointRowBase, IndexHistoryFields):
    endpoint: Literal[ProviderEndpoint.INDEX_HISTORY]
    schema_variant: Literal["index_history.session.v1"]

    @model_validator(mode="after")
    def validate_index_role(self) -> IndexHistorySessionRow:
        if self.instrument_role is InstrumentRole.INDEX and self.code not in {
            "sh.000001",
            "sz.399001",
        }:
            raise ValueError("index role requires required index symbol")
        return self


class IndexHistoryRangeRow(IndexHistorySessionRow):
    schema_variant: Literal["index_history.range.v1"]


RawEndpointRow = Annotated[
    TradeDatesRow
    | AllStockRow
    | DailyAStockRow
    | DailyFactorRow
    | AdjustFactorRow
    | IndexHistorySessionRow
    | IndexHistoryRangeRow,
    Field(discriminator="schema_variant"),
]


_DAILY_FIELDS = (
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "isST",
)
_FACTOR_FIELDS = (
    "code",
    "dividOperateDate",
    "foreAdjustFactor",
    "backAdjustFactor",
    "adjustFactor",
)
_INDEX_FIELDS = _DAILY_FIELDS


class EndpointContract(_ContractModel):
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy

    def model_copy(self, *, update: dict[str, object] | None = None, deep: bool = False):
        result = super().model_copy(update=update, deep=deep)
        # The table is authoritative; a copied entry cannot silently become a
        # second contract for the same endpoint/role/variant key.
        if "ENDPOINT_CONTRACTS" in globals():
            expected = endpoint_contract_for(
                result.endpoint, result.instrument_role, result.schema_variant
            )
            if result != expected:
                raise ValueError("endpoint contract copy diverges from authoritative table")
        return result


def _contract(endpoint, role, instrument, variant, fields, units=()):
    return EndpointContract(
        endpoint=endpoint,
        request_role=role,
        instrument_role=instrument,
        schema_variant=variant,
        fields=fields,
        units=units,
        date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    )


ENDPOINT_CONTRACTS: tuple[EndpointContract, ...] = (
    _contract(
        ProviderEndpoint.TRADE_DATES,
        RequestRole.CALENDAR,
        None,
        "trade_dates.v1",
        ("calendar_date", "is_trading_day"),
    ),
    _contract(
        ProviderEndpoint.ALL_STOCK,
        RequestRole.UNIVERSE,
        InstrumentRole.STOCK,
        "all_stock.market.v1",
        ("code", "tradeStatus", "code_name"),
    ),
    _contract(
        ProviderEndpoint.DAILY_ASTOCK,
        RequestRole.DAILY_STOCK,
        InstrumentRole.STOCK,
        "daily_astock.v1",
        _DAILY_FIELDS,
        (("volume", "shares"), ("amount", "CNY"), ("turn", "percent"), ("pctChg", "percent")),
    ),
    _contract(
        ProviderEndpoint.DAILY_FACTOR,
        RequestRole.DAILY_FACTOR,
        InstrumentRole.STOCK,
        "daily_factor.v1",
        _FACTOR_FIELDS,
        (
            ("foreAdjustFactor", "dimensionless"),
            ("backAdjustFactor", "dimensionless"),
            ("adjustFactor", "dimensionless"),
        ),
    ),
    _contract(
        ProviderEndpoint.ADJUST_FACTOR,
        RequestRole.ADJUST_FACTOR,
        InstrumentRole.STOCK,
        "adjust_factor.session.v1",
        _FACTOR_FIELDS,
        (
            ("foreAdjustFactor", "dimensionless"),
            ("backAdjustFactor", "dimensionless"),
            ("adjustFactor", "dimensionless"),
        ),
    ),
    *tuple(
        _contract(
            ProviderEndpoint.INDEX_HISTORY,
            RequestRole.INDEX_HISTORY,
            role,
            variant,
            _INDEX_FIELDS,
            (("volume", "shares"), ("amount", "CNY"), ("turn", "percent"), ("pctChg", "percent")),
        )
        for role in (InstrumentRole.INDEX, InstrumentRole.STOCK)
        for variant in ("index_history.session.v1", "index_history.range.v1")
    ),
)


def endpoint_contract_for(
    endpoint: ProviderEndpoint, instrument_role: InstrumentRole | None, schema_variant: str
) -> EndpointContract:
    matches = tuple(
        item
        for item in ENDPOINT_CONTRACTS
        if item.endpoint is endpoint
        and item.instrument_role is instrument_role
        and item.schema_variant == schema_variant
    )
    if len(matches) != 1:
        raise ValueError("unknown or duplicate endpoint contract")
    return matches[0]


class RawEndpointBatch(_ContractModel):
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    plan_ordinal: int = Field(ge=0, le=4096)
    shard_id: SafeIdentifier
    lineage: TransportLineageRef
    rows: tuple[RawEndpointRow, ...]
    row_count: int = Field(ge=0, le=10_000_000)
    source_schema: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy
    provider_row_order_digest: SafeSha256

    @model_validator(mode="after")
    def validate_contract(self) -> RawEndpointBatch:
        contract = endpoint_contract_for(self.endpoint, self.instrument_role, self.schema_variant)
        if (
            self.request_role is not contract.request_role
            or self.fields != contract.fields
            or self.units != contract.units
            or self.source_schema != self.schema_variant
            or self.pagination_policy is not contract.pagination_policy
        ):
            raise ValueError("raw endpoint batch does not match endpoint contract")
        if self.row_count != len(self.rows):
            raise ValueError("raw row count mismatch")
        for row in self.rows:
            if row.endpoint is not self.endpoint or row.schema_variant != self.schema_variant:
                raise ValueError("mixed endpoint rows")
        return self


class EndpointContractSummary(_ContractModel):
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    source_schema: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy
    batch_count: int = Field(ge=1, le=4096)
    row_count: int = Field(ge=0, le=10_000_000)

    @model_validator(mode="after")
    def validate_contract(self) -> EndpointContractSummary:
        contract = endpoint_contract_for(self.endpoint, self.instrument_role, self.schema_variant)
        if (
            self.request_role is not contract.request_role
            or self.source_schema != self.schema_variant
        ):
            raise ValueError("endpoint summary does not match endpoint contract")
        if self.fields != contract.fields or self.units != contract.units:
            raise ValueError("endpoint summary fields do not match endpoint contract")
        return self


class TransportObservationProjection(_ContractModel):
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    provider_id: ProviderId
    endpoint: ProviderEndpoint
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16_384)
    protocol_stage: ProtocolStage
    elapsed_ms: int = Field(ge=0, le=86_400_000)
    recv_calls: int = Field(ge=0, le=10_000_000)
    response_bytes: int = Field(ge=0, le=67_108_864)
    end_marker_seen: bool
    provider_code: ProviderCode | None = None
    normalized_error: NormalizedTransportError | None = None
    outcome: TransportOutcome
    observed_at: datetime
    observation_digest: SafeSha256

    @field_validator("observed_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value.astimezone(UTC)

    def _without_digest(self) -> dict[str, object]:
        value = self.model_dump(mode="json")
        value.pop("observation_digest", None)
        return value

    def compute_digest(self) -> str:
        return _digest(self._without_digest())

    @model_validator(mode="after")
    def validate_digest(self) -> TransportObservationProjection:
        if self.observation_digest != self.compute_digest():
            raise ValueError("transport observation digest mismatch")
        return self

    @classmethod
    def from_observation(cls, observation: TransportObservation) -> TransportObservationProjection:
        values = observation.model_dump()
        values["provider_id"] = ProviderId.BAOSTOCK
        values["endpoint"] = ProviderEndpoint(observation.endpoint.value)
        values["observation_digest"] = _digest(
            {**observation.model_dump(mode="json"), "provider_id": "baostock"}
        )
        return cls.model_validate(values)


class TransportObservationAggregate(_ContractModel):
    observations: tuple[TransportObservationProjection, ...]
    aggregate_digest: SafeSha256

    def compute_digest(self) -> str:
        return _digest([item._without_digest() for item in self.observations])

    @model_validator(mode="after")
    def validate_digest(self) -> TransportObservationAggregate:
        if self.aggregate_digest != self.compute_digest():
            raise ValueError("transport observation aggregate digest mismatch")
        return self

    @classmethod
    def from_observations(cls, observations: Sequence[TransportObservationProjection]):
        values = tuple(observations)
        return cls(
            observations=values,
            aggregate_digest=_digest([item._without_digest() for item in values]),
        )


class FactorCacheSnapshotRecord(_ContractModel):
    """Exact factor-cache row projection consumed by later evidence storage."""

    symbol: SafeSymbol
    trade_date: date
    fore_adjust_factor: Decimal
    back_adjust_factor: Decimal | None
    evidence_kind: BoundedToken
    evidence_effective_date: date
    evidence_observed_on: date | None
    source_row_hash: SafeSha256
    observed_at: datetime
    row_fingerprint: SafeSha256

    @field_validator("observed_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value.astimezone(UTC)


class FactorCacheSnapshotRecords(_ContractModel):
    cache_schema: tuple[SafeIdentifier, ...]
    rows: tuple[FactorCacheSnapshotRecord, ...]
    row_count: int = Field(ge=0, le=10_000_000)
    records_sha256: SafeSha256
    before_fingerprint: SafeSha256
    after_fingerprint: SafeSha256

    @model_validator(mode="after")
    def count_matches(self) -> FactorCacheSnapshotRecords:
        if self.row_count != len(self.rows):
            raise ValueError("factor snapshot row count mismatch")
        return self


class LiveFactorResolution(_ContractModel):
    descriptor_id: SafeIdentifier
    object_sha256: SafeSha256
    endpoint: Literal[ProviderEndpoint.DAILY_FACTOR, ProviderEndpoint.ADJUST_FACTOR]
    row_key: SafeIdentifier
    schema_variant: SafeVersion


class CacheFactorResolution(_ContractModel):
    cache_object_id: SafeIdentifier
    cache_object_sha256: SafeSha256
    record_key: SafeIdentifier


class FactorResolutionBinding(_ContractModel):
    plan_ordinal: int = Field(ge=0, le=4096)
    symbol: SafeSymbol
    trade_date: date
    selected_kind: Literal["factor_cache_snapshot", "daily_factor", "adjust_factor"]
    selected_value_semantic_hash: SafeSha256
    live: LiveFactorResolution | None = None
    cache: CacheFactorResolution | None = None
    resolution_sha256: SafeSha256

    @model_validator(mode="after")
    def require_one_source(self) -> FactorResolutionBinding:
        if self.selected_kind == "factor_cache_snapshot":
            if self.cache is None or self.live is not None:
                raise ValueError("cache factor resolution requires cache only")
        elif self.live is None or self.cache is not None:
            raise ValueError("live factor resolution requires live only")
        return self


class ProviderRawBatch(_ContractModel):
    provider_id: ProviderId
    request: ProviderRequest
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion
    started_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    logical_request_plan: ExpectedLogicalRequestPlan
    request_plan_hash: SafeSha256
    endpoint_batches: tuple[RawEndpointBatch, ...]
    request_completions: tuple[RequestCompletion, ...]
    completion_hash: SafeSha256
    transport_lineage: tuple[TransportLineageRef, ...]
    transport_observations: TransportObservationAggregate
    endpoint_summaries: tuple[EndpointContractSummary, ...]
    factor_resolution: tuple[FactorResolutionBinding, ...] = ()
    factor_resolution_sha256: SafeSha256 = _EMPTY_SHA256
    factor_cache_records: FactorCacheSnapshotRecords | None = None
    failure_class: SafeFailureClass | None = None

    @model_validator(mode="after")
    def validate_session_binding(self) -> ProviderRawBatch:
        if self.provider_id is not self.request.provider_id:
            raise ValueError("raw batch provider does not match request")
        if self.request_plan_hash != self.logical_request_plan.compute_hash():
            raise ValueError("raw batch request plan hash mismatch")
        if self.completed_at < self.started_at:
            raise ValueError("raw batch completion precedes start")
        if self.normalization_clock_utc < self.completed_at:
            raise ValueError("normalization clock precedes completion")
        if len(self.request_completions) != self.logical_request_plan.request_count:
            raise ValueError("raw batch completion cardinality mismatch")
        return self

    @field_validator("started_at", "completed_at", "normalization_clock_utc")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)


class DailyBarProvider(Protocol):
    provider_id: ProviderId
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion

    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch: ...

    def normalize(
        self, evidence: object, *, normalization_clock_utc: datetime
    ) -> tuple[DailyBar, ...]: ...


def provider_registry(provider_id: str | SafeProviderId | None = None):
    if provider_id is not None and provider_id != ProviderId.BAOSTOCK.value:
        raise ValueError("only baostock provider is admitted")
    return _ProviderRegistry()


class _ProviderRegistry(_ContractModel):
    provider_id: ProviderId = ProviderId.BAOSTOCK
    adapter_version: str = "r2f2.v1"
    endpoint_contract_version: str = "r2f2-endpoints.v1"
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"
    volume_unit: Literal["shares"] = "shares"
    amount_unit: Literal["CNY"] = "CNY"
    adjustment_semantics: Literal["unadjusted_ohlcv_back_adjust_factor"] = (
        "unadjusted_ohlcv_back_adjust_factor"
    )
    admission_state: Literal["qualified"] = "qualified"


def validate_raw_date_binding(request: ProviderRequest, batch: RawEndpointBatch) -> None:
    if (
        batch.lineage.refresh_id != request.refresh_id
        or batch.lineage.provider_session_id != request.provider_session_id
    ):
        raise ValueError("raw lineage does not match request")
    for row in batch.rows:
        row_date = getattr(row, "date", getattr(row, "calendar_date", None))
        if row_date is not None and not (request.trade_date == row_date):
            raise ValueError("raw row date does not match requested trade date")
        effective = getattr(row, "dividOperateDate", None)
        if effective is not None and effective > request.trade_date:
            raise ValueError("factor effective date is after requested trade date")
        code = getattr(row, "code", None)
        if code is not None and batch.endpoint not in {
            ProviderEndpoint.ALL_STOCK,
            ProviderEndpoint.TRADE_DATES,
        }:
            if code not in request.session_symbols:
                raise ValueError("raw row symbol is outside requested session symbols")
