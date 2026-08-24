"""Frozen, provider-neutral contracts at the source capture boundary.

The module intentionally contains a closed BaoStock allow-list.  It is not a
plugin registry and it does not expose provider payloads or arbitrary mappings.
"""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Literal, Protocol

from pydantic import (
    AfterValidator,
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
R2F2_ADAPTER_VERSION = "r2f2.v1"
R2F2_ENDPOINT_CONTRACT_VERSION = "r2f2-endpoints.v1"


def validate_safe_relative_path(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(
            char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-"
            for char in value
        )
        or value.startswith("/")
        or value.endswith("/")
        or "//" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("relative path is not lexically safe")
    return value


SafeIdentifier = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"),
]
SafeSha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SafeRelativePath = Annotated[
    str,
    StringConstraints(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9._/-]+$"),
    AfterValidator(validate_safe_relative_path),
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


class EvidenceObjectKind(StrEnum):
    RAW_ENDPOINT_PAGE = "raw_endpoint_page"
    FACTOR_CACHE_SNAPSHOT = "factor_cache_snapshot"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode()


def _digest(value: object) -> str:
    return sha256(_canonical(value)).hexdigest()


def _typed_row_order_digest(rows: Sequence[RawEndpointRow]) -> str:
    return _digest([row.model_dump(mode="json") for row in rows])


def _finite_number(value: float | Decimal | None, *, positive: bool = False) -> bool:
    if value is None:
        return False
    try:
        finite = math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False
    return finite and (not positive or value > 0)


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
        if self.endpoint is ProviderEndpoint.ALL_STOCK and self.start_date != self.end_date:
            raise ValueError("universe request requires one explicit date")
        if (
            self.endpoint is not ProviderEndpoint.TRADE_DATES
            and self.schema_variant != "index_history.range.v1"
            and self.start_date != self.end_date
        ):
            raise ValueError("session request requires one explicit date")
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
        identities = tuple(
            (
                item.endpoint,
                item.request_role,
                item.instrument_role,
                item.schema_variant,
                item.symbols,
                item.start_date,
                item.end_date,
                item.shard_id,
            )
            for item in self.requests
        )
        if len(set(identities)) != len(identities):
            raise ValueError("request plan contains duplicate logical identity")
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
    provider_session_id: SafeIdentifier
    attempt: int = Field(ge=1, le=20)
    root_request_id: SafeIdentifier
    operation_observation_digest: SafeSha256
    observed_pages: tuple[int, ...]
    page_request_ids: tuple[tuple[int, SafeIdentifier], ...]
    observed_page_count: int = Field(ge=0, le=16_384)
    pagination_terminal_count: int = Field(ge=0, le=1)
    terminal: bool
    outcome: TransportOutcome

    @model_validator(mode="after")
    def validate_observed_pages(self) -> AttemptCompletion:
        if self.observed_page_count != len(self.observed_pages):
            raise ValueError("observed page count mismatch")
        if self.observed_pages and self.observed_pages != tuple(
            range(1, self.observed_page_count + 1)
        ):
            raise ValueError("observed pages must be contiguous")
        if tuple(page for page, _ in self.page_request_ids) != self.observed_pages:
            raise ValueError("page request IDs must close over observed pages")
        if len({request_id for _, request_id in self.page_request_ids}) != len(
            self.page_request_ids
        ):
            raise ValueError("page request IDs must be unique")
        if self.outcome is TransportOutcome.SUCCESS and (
            not self.terminal or not self.observed_pages or self.pagination_terminal_count != 1
        ):
            raise ValueError("successful attempt requires terminal pagination")
        if self.outcome is TransportOutcome.ERROR and self.pagination_terminal_count:
            raise ValueError("failed attempt cannot claim pagination terminal")
        return self


class RequestCompletion(_ContractModel):
    plan_ordinal: int = Field(ge=0, le=4096)
    attempts: tuple[AttemptCompletion, ...]
    successful_attempt: int | None = Field(default=None, ge=1, le=20)
    successful_root_request_id: SafeIdentifier | None = None
    final_outcome: TransportOutcome
    row_count: int = Field(default=0, ge=0, le=10_000_000)
    observed_pages: tuple[int, ...] = ()

    @model_validator(mode="after")
    def validate_final_attempt(self) -> RequestCompletion:
        if not self.attempts:
            raise ValueError("completion requires an attempt")
        if tuple(item.attempt for item in self.attempts) != tuple(range(1, len(self.attempts) + 1)):
            raise ValueError("attempts must be strictly ordered")
        if len({item.root_request_id for item in self.attempts}) != len(self.attempts):
            raise ValueError("attempt request IDs must be unique")
        if self.final_outcome is not self.attempts[-1].outcome:
            raise ValueError("final outcome must match final attempt")
        successes = [item for item in self.attempts if item.outcome is TransportOutcome.SUCCESS]
        if self.final_outcome is TransportOutcome.SUCCESS:
            final = self.attempts[-1]
            if len(successes) != 1 or self.successful_attempt != final.attempt:
                raise ValueError("a completion has exactly one final successful attempt")
            if self.successful_root_request_id != final.root_request_id:
                raise ValueError("successful request binding mismatch")
        elif (
            successes
            or self.successful_attempt is not None
            or self.successful_root_request_id is not None
        ):
            raise ValueError("failed completion cannot bind a successful attempt")
        if self.final_outcome is TransportOutcome.ERROR and self.row_count != 0:
            raise ValueError("failed completion row count must be zero")
        final_pages = self.attempts[-1].observed_pages
        if self.observed_pages and self.observed_pages != final_pages:
            raise ValueError("completion observed pages do not match final attempt")
        object.__setattr__(self, "observed_pages", final_pages)
        return self

    def compute_hash(self) -> str:
        return _digest(self.model_dump(mode="json"))


class ProviderRequest(_ContractModel):
    provider_id: ProviderId
    refresh_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    session_symbols: tuple[SafeSymbol, ...]
    logical_request_plan: ExpectedLogicalRequestPlan

    @field_validator("session_symbols")
    @classmethod
    def complete_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(set(value))):
            raise ValueError("session_symbols must be non-empty, sorted and unique")
        return value


class TransportLineageRef(_ContractModel):
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    root_request_id: SafeIdentifier
    page_request_id: SafeIdentifier
    endpoint: ProviderEndpoint
    plan_ordinal: int = Field(ge=0, le=4096)
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16_384)
    protocol_stage: Literal[ProtocolStage.COMPLETE]
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

    @model_validator(mode="after")
    def validate_daily_semantics(self) -> DailyAStockRow:
        if not all(
            _finite_number(value)
            for value in (self.open, self.high, self.low, self.close, self.preclose)
        ):
            raise ValueError("daily OHLC values must be finite")
        for value in (self.turn, self.pctChg):
            if value is not None and not _finite_number(value):
                raise ValueError("daily optional values must be finite")
        if self.tradestatus == "1":
            if not _finite_number(self.volume) or not _finite_number(self.amount):
                raise ValueError("active daily rows require finite activity")
        elif any(value is not None and value != 0 for value in (self.volume, self.amount)):
            raise ValueError("suspended daily rows cannot contain activity")
        return self


class FactorFields(_ContractModel):
    code: SafeSymbol
    dividOperateDate: date
    foreAdjustFactor: float | Decimal
    backAdjustFactor: float | Decimal
    adjustFactor: float | Decimal

    @field_validator("foreAdjustFactor", "backAdjustFactor", "adjustFactor")
    @classmethod
    def require_positive_factor(cls, value: float | Decimal) -> float | Decimal:
        if not _finite_number(value, positive=True):
            raise ValueError("adjustment factors must be finite and positive")
        return value


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
        if self.instrument_role is InstrumentRole.INDEX and self.tradestatus != "1":
            raise ValueError("index rows cannot be suspended")
        if not all(
            _finite_number(value)
            for value in (self.open, self.high, self.low, self.close, self.preclose)
        ):
            raise ValueError("index OHLC values must be finite")
        if self.tradestatus == "1":
            if not _finite_number(self.volume) or not _finite_number(self.amount):
                raise ValueError("active index rows require finite activity")
        elif any(value is not None and value != 0 for value in (self.volume, self.amount)):
            raise ValueError("suspended index rows cannot contain activity")
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
            self.lineage.endpoint is not self.endpoint
            or self.lineage.plan_ordinal != self.plan_ordinal
        ):
            raise ValueError("raw endpoint lineage does not bind endpoint and plan")
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
        if self.provider_row_order_digest != _typed_row_order_digest(self.rows):
            raise ValueError("typed provider row-order digest mismatch")
        for row in self.rows:
            if row.endpoint is not self.endpoint or row.schema_variant != self.schema_variant:
                raise ValueError("mixed endpoint rows")
        if self.endpoint is ProviderEndpoint.ALL_STOCK:
            if not self.rows:
                raise ValueError("universe coverage cannot be empty")
            codes = tuple(row.code for row in self.rows)
            if codes != tuple(sorted(set(codes))):
                raise ValueError("universe rows must be unique and ordered")
        elif self.endpoint in {ProviderEndpoint.DAILY_ASTOCK, ProviderEndpoint.INDEX_HISTORY}:
            if self.instrument_role is InstrumentRole.INDEX and len(self.rows) != 1:
                raise ValueError("index session coverage requires one row")
            keys = tuple((row.code, row.date) for row in self.rows)
            if len(set(keys)) != len(keys) or keys != tuple(
                sorted(keys, key=lambda item: (item[1], item[0]))
            ):
                raise ValueError("daily rows must be unique and ordered")
        elif self.endpoint in {ProviderEndpoint.DAILY_FACTOR, ProviderEndpoint.ADJUST_FACTOR}:
            keys = tuple((row.code, row.dividOperateDate) for row in self.rows)
            if len(set(keys)) != len(keys) or keys != tuple(
                sorted(keys, key=lambda item: (item[1], item[0]))
            ):
                raise ValueError("factor rows must be unique and ordered")
        elif self.endpoint is ProviderEndpoint.TRADE_DATES:
            dates = tuple(row.calendar_date for row in self.rows)
            if len(set(dates)) != len(dates) or dates != tuple(sorted(dates)):
                raise ValueError("calendar rows must be unique and ordered")
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
    plan_ordinal: int = Field(ge=0, le=4096)
    lineage_kind: Literal["login_audit", "query_root", "page"]
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
        values["observed_at"] = observation.observed_at.astimezone(UTC)
        values["provider_id"] = ProviderId.BAOSTOCK
        values["endpoint"] = ProviderEndpoint(observation.endpoint.value)
        # Callers must use ``from_observation_with_lineage`` so the registry-derived
        # identity is part of the digest.  The default is only retained for old
        # transport fixtures and is never used by the adapter.
        values.setdefault("plan_ordinal", 0)
        values.setdefault("lineage_kind", "query_root")
        candidate = cls.model_construct(**values, observation_digest=_EMPTY_SHA256)
        values["observation_digest"] = candidate.compute_digest()
        return cls.model_validate(values)

    @classmethod
    def from_observation_with_lineage(
        cls,
        observation: TransportObservation,
        *,
        plan_ordinal: int,
        lineage_kind: Literal["login_audit", "query_root", "page"],
    ) -> TransportObservationProjection:
        values = observation.model_dump()
        values.update(
            observed_at=observation.observed_at.astimezone(UTC),
            provider_id=ProviderId.BAOSTOCK,
            endpoint=ProviderEndpoint(observation.endpoint.value),
            plan_ordinal=plan_ordinal,
            lineage_kind=lineage_kind,
        )
        candidate = cls.model_construct(**values, observation_digest=_EMPTY_SHA256)
        values["observation_digest"] = candidate.compute_digest()
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
    failure_class: SafeFailureClass | None = None

    @model_validator(mode="after")
    def validate_session_binding(self) -> ProviderRawBatch:
        if (
            self.adapter_version != "r2f2.v1"
            or self.endpoint_contract_version != "r2f2-endpoints.v1"
        ):
            raise ValueError("provider contract versions must match current constants")
        if self.provider_id is not self.request.provider_id:
            raise ValueError("raw batch provider does not match request")
        if self.logical_request_plan != self.request.logical_request_plan:
            raise ValueError("raw batch logical request plan does not bind request")
        if self.request_plan_hash != self.logical_request_plan.compute_hash():
            raise ValueError("raw batch request plan hash mismatch")
        if self.completed_at < self.started_at:
            raise ValueError("raw batch completion precedes start")
        if self.normalization_clock_utc < self.completed_at:
            raise ValueError("normalization clock precedes completion")
        expected_ordinals = tuple(range(self.logical_request_plan.request_count))
        if tuple(item.plan_ordinal for item in self.request_completions) != expected_ordinals:
            raise ValueError("raw batch completion cardinality mismatch")
        successful = {
            item.plan_ordinal: item
            for item in self.request_completions
            if item.final_outcome is TransportOutcome.SUCCESS
        }
        if any(item.plan_ordinal not in successful for item in self.endpoint_batches):
            raise ValueError("raw endpoint batch cardinality mismatch")
        page_key = tuple[str, str, str, str, int, int, int, int]
        projection_key = tuple[str, str, str, str, int, int, int]
        expected_operation_index: dict[projection_key, tuple[str, TransportOutcome]] = {}
        capture_identity = tuple[str, str, str]
        capture_owner = tuple[str, str, str, int, int, int, str]
        capture_identity_owner: dict[capture_identity, capture_owner] = {}
        attempt_page_index: dict[page_key, Literal["query_root", "page"]] = {}
        attempt_projection_keys: dict[
            projection_key, tuple[page_key, Literal["query_root", "page"]]
        ] = {}
        for completion in self.request_completions:
            logical = self.logical_request_plan.requests[completion.plan_ordinal]
            for attempt in completion.attempts:
                owner: capture_owner = (
                    "query",
                    attempt.root_request_id,
                    logical.endpoint.value,
                    completion.plan_ordinal,
                    attempt.attempt,
                    1,
                    "query_root",
                )
                root_identity = (
                    self.request.refresh_id,
                    attempt.provider_session_id,
                    attempt.root_request_id,
                )
                existing_owner = capture_identity_owner.get(root_identity)
                if existing_owner is not None and existing_owner != owner:
                    raise ValueError("capture identity has conflicting query owners")
                capture_identity_owner[root_identity] = owner
                for page, page_request_id in attempt.page_request_ids:
                    page_kind: Literal["query_root", "page"] = "query_root" if page == 1 else "page"
                    page_owner: capture_owner = (
                        "query",
                        attempt.root_request_id,
                        logical.endpoint.value,
                        completion.plan_ordinal,
                        attempt.attempt,
                        page,
                        page_kind,
                    )
                    page_identity = (
                        self.request.refresh_id,
                        attempt.provider_session_id,
                        page_request_id,
                    )
                    existing_owner = capture_identity_owner.get(page_identity)
                    if existing_owner is not None and existing_owner != page_owner:
                        raise ValueError("capture identity has conflicting query owners")
                    capture_identity_owner[page_identity] = page_owner
                    if page == 1 and page_request_id != attempt.root_request_id:
                        raise ValueError("page one must bind its query root")
                    if page > 1 and page_request_id == attempt.root_request_id:
                        raise ValueError("nested pages require distinct request IDs")
                    key = (
                        self.request.refresh_id,
                        attempt.provider_session_id,
                        attempt.root_request_id,
                        page_request_id,
                        logical.endpoint.value,
                        completion.plan_ordinal,
                        attempt.attempt,
                        page,
                    )
                    kind: Literal["query_root", "page"] = "query_root" if page == 1 else "page"
                    if key in attempt_page_index:
                        raise ValueError("duplicate authoritative attempt page")
                    attempt_page_index[key] = kind
                    pkey = (
                        self.request.refresh_id,
                        attempt.provider_session_id,
                        page_request_id,
                        logical.endpoint.value,
                        completion.plan_ordinal,
                        attempt.attempt,
                        page,
                    )
                    if pkey in attempt_projection_keys:
                        raise ValueError("duplicate authoritative attempt projection key")
                    attempt_projection_keys[pkey] = (key, kind)
                operation_key = (
                    self.request.refresh_id,
                    attempt.provider_session_id,
                    attempt.root_request_id,
                    logical.endpoint.value,
                    completion.plan_ordinal,
                    attempt.attempt,
                    1,
                )
                if operation_key in expected_operation_index:
                    raise ValueError("duplicate authoritative operation key")
                expected_operation_index[operation_key] = (
                    attempt.operation_observation_digest,
                    attempt.outcome,
                )
        expected_pages: dict[page_key, Literal["query_root", "page"]] = {}
        expected_projection_keys: dict[
            projection_key, tuple[page_key, Literal["query_root", "page"]]
        ] = {}
        for completion in self.request_completions:
            if completion.final_outcome is not TransportOutcome.SUCCESS:
                continue
            attempt = completion.attempts[-1]
            logical = self.logical_request_plan.requests[completion.plan_ordinal]
            for page, page_request_id in attempt.page_request_ids:
                key = (
                    self.request.refresh_id,
                    attempt.provider_session_id,
                    attempt.root_request_id,
                    page_request_id,
                    logical.endpoint.value,
                    completion.plan_ordinal,
                    attempt.attempt,
                    page,
                )
                kind = attempt_page_index.get(key)
                if kind is None:
                    raise ValueError("successful page is not bound to an attempt")
                if key in expected_pages:
                    raise ValueError("duplicate authoritative successful page")
                expected_pages[key] = kind
                pkey = (
                    self.request.refresh_id,
                    attempt.provider_session_id,
                    page_request_id,
                    logical.endpoint.value,
                    completion.plan_ordinal,
                    attempt.attempt,
                    page,
                )
                if pkey in expected_projection_keys:
                    raise ValueError("duplicate authoritative successful projection key")
                expected_projection_keys[pkey] = (key, kind)
        expected_batch_order = tuple(
            (item.plan_ordinal, item.lineage.attempt, item.lineage.page)
            for item in sorted(
                self.endpoint_batches,
                key=lambda item: (item.plan_ordinal, item.lineage.attempt, item.lineage.page),
            )
        )
        actual_batch_order = tuple(
            (item.plan_ordinal, item.lineage.attempt, item.lineage.page)
            for item in self.endpoint_batches
        )
        if actual_batch_order != expected_batch_order:
            raise ValueError("raw endpoint batches must be ordered by plan and page")
        batches_by_ordinal: dict[int, list[RawEndpointBatch]] = {}
        batch_keys: set[tuple[int, str, int, str, int]] = set()
        batch_lineage_index: dict[page_key, RawEndpointBatch] = {}
        for item in self.endpoint_batches:
            logical = self.logical_request_plan.requests[item.plan_ordinal]
            if (
                item.endpoint is not logical.endpoint
                or item.request_role is not logical.request_role
                or item.instrument_role is not logical.instrument_role
                or item.schema_variant != logical.schema_variant
                or item.shard_id != logical.shard_id
            ):
                raise ValueError("raw endpoint batch does not bind exact logical request")
            validate_raw_date_binding(self.request, item)
            key = (
                item.plan_ordinal,
                item.lineage.page_request_id,
                item.lineage.attempt,
                item.lineage.endpoint.value,
                item.lineage.page,
            )
            if key in batch_keys:
                raise ValueError("duplicate raw endpoint page")
            batch_keys.add(key)
            batches_by_ordinal.setdefault(item.plan_ordinal, []).append(item)
            lineage = item.lineage
            batch_key = (
                lineage.refresh_id,
                lineage.provider_session_id,
                lineage.root_request_id,
                lineage.page_request_id,
                lineage.endpoint.value,
                lineage.plan_ordinal,
                lineage.attempt,
                lineage.page,
            )
            if batch_key in batch_lineage_index:
                raise ValueError("duplicate raw endpoint batch lineage")
            batch_lineage_index[batch_key] = item
        if len(self.endpoint_summaries) != len(successful):
            raise ValueError("raw endpoint summary cardinality mismatch")

        def contract_key(item: RawEndpointBatch | EndpointContractSummary):
            return (
                str(item.endpoint),
                str(item.request_role),
                "" if item.instrument_role is None else str(item.instrument_role),
                str(item.schema_variant),
            )

        expected_summaries = sorted(
            (
                contract_key(group[0]),
                len(group),
                sum(item.row_count for item in group),
            )
            for group in batches_by_ordinal.values()
        )
        actual_summaries = sorted(
            (contract_key(item), item.batch_count, item.row_count)
            for item in self.endpoint_summaries
        )
        if expected_summaries != actual_summaries:
            raise ValueError("raw endpoint summaries do not bind source batches")
        for completion in self.request_completions:
            batches = batches_by_ordinal.get(completion.plan_ordinal, [])
            logical = self.logical_request_plan.requests[completion.plan_ordinal]
            if completion.final_outcome is TransportOutcome.ERROR:
                if completion.row_count != 0 or batches:
                    raise ValueError("failed completion cannot persist source rows")
                continue
            if not batches or completion.successful_root_request_id is None:
                raise ValueError("successful completion requires source batch")
            if any(
                item.lineage.attempt != completion.successful_attempt
                or item.lineage.plan_ordinal != completion.plan_ordinal
                or item.lineage.provider_session_id != completion.attempts[-1].provider_session_id
                or item.lineage.root_request_id != completion.successful_root_request_id
                or item.lineage.page_request_id
                not in {request_id for _, request_id in completion.attempts[-1].page_request_ids}
                for item in batches
            ):
                raise ValueError("source batch lineage does not bind completion")
            if batches[0].lineage.page != 1 or (
                batches[0].lineage.page_request_id != completion.successful_root_request_id
            ):
                raise ValueError("page one lineage must bind the successful query root")
            if any(
                item.lineage.page > 1
                and item.lineage.page_request_id == completion.successful_root_request_id
                for item in batches
            ):
                raise ValueError("nested page lineage must have its own request identity")
            pages = tuple(item.lineage.page for item in batches)
            if pages != tuple(sorted(pages)) or pages != completion.attempts[-1].observed_pages:
                raise ValueError("source batch pages do not bind completion")
            if completion.row_count != sum(item.row_count for item in batches):
                raise ValueError("successful completion row count mismatch")
        actual_sessions = {
            attempt.provider_session_id
            for completion in self.request_completions
            for attempt in completion.attempts
        }
        for item in self.transport_lineage:
            if (
                item.refresh_id != self.request.refresh_id
                or item.provider_session_id not in actual_sessions
            ):
                raise ValueError("transport lineage does not bind request session")
            if item.protocol_stage is not ProtocolStage.COMPLETE:
                raise ValueError("transport lineage must be a COMPLETE page projection")
        for item in self.transport_observations.observations:
            if (
                item.refresh_id != self.request.refresh_id
                or item.provider_session_id not in actual_sessions
            ):
                raise ValueError("transport observation does not bind request session")
        lineage_index: dict[page_key, TransportLineageRef] = {}
        for lineage in self.transport_lineage:
            key = (
                lineage.refresh_id,
                lineage.provider_session_id,
                lineage.root_request_id,
                lineage.page_request_id,
                lineage.endpoint.value,
                lineage.plan_ordinal,
                lineage.attempt,
                lineage.page,
            )
            if key in lineage_index:
                raise ValueError("duplicate or conflicting transport lineage projection")
            lineage_index[key] = lineage
        operation_index: dict[projection_key, TransportObservationProjection] = {}
        projection_index: dict[projection_key, TransportObservationProjection] = {}
        for projection in self.transport_observations.observations:
            identity = (
                projection.refresh_id,
                projection.provider_session_id,
                projection.request_id,
            )
            if projection.lineage_kind == "login_audit":
                existing_owner = capture_identity_owner.get(identity)
                if existing_owner is not None and existing_owner[0] == "query":
                    raise ValueError("login capture identity collides with query namespace")
                login_owner: capture_owner = (
                    "login",
                    "",
                    projection.endpoint.value,
                    projection.plan_ordinal,
                    projection.attempt,
                    projection.page,
                    "login_audit",
                )
                if existing_owner is not None and existing_owner != login_owner:
                    raise ValueError("capture identity has conflicting login owners")
                capture_identity_owner[identity] = login_owner
                continue
            is_operation = projection.protocol_stage is ProtocolStage.OPERATION
            is_successful_complete = (
                projection.protocol_stage is ProtocolStage.COMPLETE
                and projection.outcome is TransportOutcome.SUCCESS
                and projection.end_marker_seen
            )
            if not (is_operation or is_successful_complete):
                continue
            owner = capture_identity_owner.get(identity)
            if owner is None or owner[0] != "query":
                if is_operation:
                    raise ValueError("operation projection is not owned by an attempt")
                raise ValueError("query capture identity is not owned by an attempt")
            projection_kind: Literal["query_root", "page"] = projection.lineage_kind
            expected_owner: capture_owner = (
                "query",
                owner[1],
                projection.endpoint.value,
                projection.plan_ordinal,
                projection.attempt,
                projection.page,
                projection_kind,
            )
            if owner != expected_owner:
                if is_operation:
                    raise ValueError("operation projection has conflicting owner metadata")
                raise ValueError("query capture identity has conflicting owner metadata")
            if projection.protocol_stage is ProtocolStage.OPERATION:
                if projection_kind != "query_root":
                    raise ValueError("operation projection must use query_root lineage")
                key = (
                    projection.refresh_id,
                    projection.provider_session_id,
                    projection.request_id,
                    projection.endpoint.value,
                    projection.plan_ordinal,
                    projection.attempt,
                    projection.page,
                )
                expected = expected_operation_index.get(key)
                if (
                    expected is None
                    or projection.observation_digest != expected[0]
                    or projection.outcome is not expected[1]
                ):
                    raise ValueError("operation projection does not bind authoritative attempt")
                if key in operation_index:
                    raise ValueError("duplicate or conflicting operation projection")
                operation_index[key] = projection
                continue
            key = (
                projection.refresh_id,
                projection.provider_session_id,
                projection.request_id,
                projection.endpoint.value,
                projection.plan_ordinal,
                projection.attempt,
                projection.page,
            )
            if (
                projection.protocol_stage is ProtocolStage.COMPLETE
                and projection.outcome is TransportOutcome.SUCCESS
                and projection.end_marker_seen
            ):
                attempt_binding = attempt_projection_keys.get(key)
                if attempt_binding is None:
                    raise ValueError("successful COMPLETE projection is not bound to an attempt")
                if projection.lineage_kind != attempt_binding[1]:
                    raise ValueError("successful COMPLETE projection has wrong page kind")
                if key in projection_index:
                    raise ValueError("duplicate or conflicting successful page projection")
                projection_index[key] = projection
        if set(operation_index) != set(expected_operation_index):
            raise ValueError("operation projections do not exactly match attempts")
        if set(lineage_index) != set(expected_pages):
            raise ValueError("transport lineage does not exactly match successful pages")
        if set(batch_lineage_index) != set(expected_pages):
            raise ValueError("raw endpoint batches do not exactly match successful pages")
        if not set(expected_projection_keys).issubset(projection_index):
            raise ValueError("successful COMPLETE projections do not exactly match pages")
        for key, expected_kind in expected_pages.items():
            lineage = lineage_index[key]
            batch = batch_lineage_index[key]
            projection_key = (
                key[0],
                key[1],
                key[3],
                key[4],
                key[5],
                key[6],
                key[7],
            )
            projection = projection_index[projection_key]
            if (
                projection.lineage_kind != expected_kind
                or projection.request_id != key[3]
                or projection.endpoint.value != key[4]
                or projection.plan_ordinal != key[5]
                or projection.attempt != key[6]
                or projection.page != key[7]
                or lineage.observation_digest != projection.observation_digest
                or batch.lineage != lineage
            ):
                raise ValueError("successful page lineage joins are not exact")
        expected_completion_hash = _digest(
            [item.model_dump(mode="json") for item in self.request_completions]
        )
        if self.completion_hash != expected_completion_hash:
            raise ValueError("completion hash mismatch")
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
    adapter_version: SafeVersion = "r2f2.v1"
    endpoint_contract_version: SafeVersion = "r2f2-endpoints.v1"
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"
    volume_unit: Literal["shares"] = "shares"
    amount_unit: Literal["CNY"] = "CNY"
    adjustment_semantics: Literal["unadjusted_ohlcv_back_adjust_factor"] = (
        "unadjusted_ohlcv_back_adjust_factor"
    )
    admission_state: Literal["qualified"] = "qualified"
    supported_fields: tuple[SafeVersion, ...] = tuple(
        sorted({contract.schema_variant for contract in ENDPOINT_CONTRACTS})
    )

    @model_validator(mode="after")
    def validate_authoritative_versions(self) -> _ProviderRegistry:
        if self.adapter_version != "r2f2.v1":
            raise ValueError("unsupported provider adapter version")
        if self.endpoint_contract_version != "r2f2-endpoints.v1":
            raise ValueError("unsupported endpoint contract version")
        expected = tuple(sorted({contract.schema_variant for contract in ENDPOINT_CONTRACTS}))
        if self.supported_fields != expected:
            raise ValueError("provider supported fields do not match endpoint contracts")
        return self


def validate_raw_date_binding(request: ProviderRequest, batch: RawEndpointBatch) -> None:
    if batch.lineage.refresh_id != request.refresh_id:
        raise ValueError("raw lineage does not match request")
    logical = next(
        (
            item
            for item in request.logical_request_plan.requests
            if item.plan_ordinal == batch.plan_ordinal
        ),
        None,
    )
    if logical is None or logical.endpoint is not batch.endpoint:
        raise ValueError("raw batch does not bind to request plan")
    if (
        logical.schema_variant != batch.schema_variant
        or logical.request_role is not batch.request_role
        or logical.instrument_role is not batch.instrument_role
        or logical.shard_id != batch.shard_id
    ):
        raise ValueError("raw batch role or schema does not bind to request plan")
    expected_symbols = set(logical.symbols)
    seen_symbols: set[str] = set()
    for row in batch.rows:
        row_date = getattr(row, "date", getattr(row, "calendar_date", None))
        if row_date is not None:
            if batch.endpoint is ProviderEndpoint.TRADE_DATES:
                if not (logical.start_date <= row_date <= logical.end_date):
                    raise ValueError("calendar row date is outside requested range")
            elif batch.schema_variant.endswith(".session.v1") or batch.endpoint in {
                ProviderEndpoint.ALL_STOCK,
                ProviderEndpoint.DAILY_ASTOCK,
                ProviderEndpoint.DAILY_FACTOR,
            }:
                if request.trade_date != row_date:
                    raise ValueError("raw row date does not match requested trade date")
            elif not (logical.start_date <= row_date <= logical.end_date):
                raise ValueError("raw row date is outside requested range")
        effective = getattr(row, "dividOperateDate", None)
        if effective is not None:
            if batch.schema_variant == "daily_factor.v1" and effective != request.trade_date:
                raise ValueError("daily factor event date must equal requested trade date")
            if effective > request.trade_date:
                raise ValueError("factor effective date is after requested trade date")
        code = getattr(row, "code", None)
        if code is None:
            continue
        if batch.endpoint is ProviderEndpoint.ALL_STOCK:
            if code.startswith(("sh.000", "sz.399")):
                raise ValueError("universe rows cannot satisfy index coverage")
            continue
        if code not in logical.symbols:
            raise ValueError("raw row symbol is outside requested session symbols")
        if logical.instrument_role is InstrumentRole.INDEX:
            if code not in {"sh.000001", "sz.399001"}:
                raise ValueError("index request contains a stock symbol")
        elif code in {"sh.000001", "sz.399001"}:
            raise ValueError("stock request contains an index symbol")
        seen_symbols.add(code)
    if batch.endpoint in {
        ProviderEndpoint.DAILY_ASTOCK,
        ProviderEndpoint.INDEX_HISTORY,
    }:
        if seen_symbols != expected_symbols:
            raise ValueError("raw batch does not provide required symbol coverage")
