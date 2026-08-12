from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)


class ProviderEndpoint(StrEnum):
    TRADE_DATES = "trade_dates"
    ALL_STOCK = "all_stock"
    DAILY_ASTOCK = "daily_astock"
    DAILY_FACTOR = "daily_factor"
    ADJUST_FACTOR = "adjust_factor"
    INDEX_HISTORY = "index_history"


class ProtocolStage(StrEnum):
    CONNECT = "connect"
    SEND = "send"
    RECEIVE = "receive"
    FRAME = "frame"
    DECOMPRESS = "decompress"
    PAGINATION = "pagination"
    PROVIDER_STATUS = "provider_status"
    COMPLETE = "complete"


class NormalizedTransportError(StrEnum):
    CONNECT_ERROR = "CONNECT_ERROR"
    SEND_ERROR = "SEND_ERROR"
    RECV_TIMEOUT = "RECV_TIMEOUT"
    EOF = "EOF"
    SHORT_HEADER = "SHORT_HEADER"
    BAD_COMPRESSION = "BAD_COMPRESSION"
    PROTOCOL_ERROR = "PROTOCOL_ERROR"
    PAGINATION_STALLED = "PAGINATION_STALLED"
    RATE_LIMIT = "RATE_LIMIT"
    UNKNOWN_PROVIDER_PROTOCOL_ERROR = "UNKNOWN_PROVIDER_PROTOCOL_ERROR"


class TransportOutcome(StrEnum):
    SUCCESS = "success"
    ERROR = "error"


TransportId = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]
ProviderCode = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=32,
        pattern=r"^[A-Za-z0-9_-]+$",
    ),
]


class RefreshTransportContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    refresh_id: TransportId


class ProviderSessionContext(RefreshTransportContext):
    provider_session_id: TransportId


class ProviderRequestContext(ProviderSessionContext):
    request_id: TransportId
    endpoint: ProviderEndpoint
    attempt: int = Field(ge=1)
    page: int = Field(default=1, ge=1)


class TransportObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    refresh_id: TransportId
    provider_session_id: TransportId
    request_id: TransportId
    provider_id: Literal["baostock"] = "baostock"
    endpoint: ProviderEndpoint
    attempt: int = Field(ge=1)
    page: int = Field(ge=1)
    protocol_stage: ProtocolStage
    elapsed_ms: int = Field(ge=0)
    recv_calls: int = Field(ge=0)
    response_bytes: int = Field(ge=0)
    end_marker_seen: bool
    provider_code: ProviderCode | None = None
    normalized_error: NormalizedTransportError | None = None
    outcome: TransportOutcome
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def require_error_for_failed_outcome(self) -> "TransportObservation":
        if self.outcome == TransportOutcome.ERROR and self.normalized_error is None:
            raise ValueError("error observation requires normalized_error")
        if self.outcome == TransportOutcome.SUCCESS and self.normalized_error is not None:
            raise ValueError("successful observation cannot include normalized_error")
        return self


class TransportScopeError(RuntimeError):
    pass


_refresh_context: ContextVar[RefreshTransportContext | None] = ContextVar(
    "provider_refresh_context",
    default=None,
)
_session_context: ContextVar[ProviderSessionContext | None] = ContextVar(
    "provider_session_context",
    default=None,
)
_request_context: ContextVar[ProviderRequestContext | None] = ContextVar(
    "provider_request_context",
    default=None,
)


def _new_transport_id() -> str:
    return uuid4().hex


@contextmanager
def refresh_scope(refresh_id: str | None = None) -> Iterator[RefreshTransportContext]:
    context = RefreshTransportContext(refresh_id=refresh_id or _new_transport_id())
    token = _refresh_context.set(context)
    try:
        yield context
    finally:
        _refresh_context.reset(token)


@contextmanager
def provider_session_scope(
    provider_session_id: str | None = None,
) -> Iterator[ProviderSessionContext]:
    refresh = _refresh_context.get()
    if refresh is None:
        raise TransportScopeError("provider session requires an active refresh scope")
    context = ProviderSessionContext(
        refresh_id=refresh.refresh_id,
        provider_session_id=provider_session_id or _new_transport_id(),
    )
    token = _session_context.set(context)
    try:
        yield context
    finally:
        _session_context.reset(token)


@contextmanager
def request_scope(
    endpoint: ProviderEndpoint,
    *,
    attempt: int,
    page: int = 1,
    request_id: str | None = None,
) -> Iterator[ProviderRequestContext]:
    session = _session_context.get()
    if session is None:
        raise TransportScopeError("request requires an active provider session scope")
    context = ProviderRequestContext(
        refresh_id=session.refresh_id,
        provider_session_id=session.provider_session_id,
        request_id=request_id or _new_transport_id(),
        endpoint=endpoint,
        attempt=attempt,
        page=page,
    )
    token = _request_context.set(context)
    try:
        yield context
    finally:
        _request_context.reset(token)


def current_request_context() -> ProviderRequestContext:
    context = _request_context.get()
    if context is None:
        raise TransportScopeError("no active request scope")
    return context


_PROVIDER_CODE_ERRORS = {
    "429": NormalizedTransportError.RATE_LIMIT,
    "10001005": NormalizedTransportError.RATE_LIMIT,
    "10002001": NormalizedTransportError.CONNECT_ERROR,
    "10002002": NormalizedTransportError.CONNECT_ERROR,
    "10002003": NormalizedTransportError.CONNECT_ERROR,
    "10002004": NormalizedTransportError.EOF,
    "10002005": NormalizedTransportError.SEND_ERROR,
    "10002006": NormalizedTransportError.SEND_ERROR,
    "10002007": NormalizedTransportError.PROTOCOL_ERROR,
    "10002008": NormalizedTransportError.RECV_TIMEOUT,
    "10004001": NormalizedTransportError.PROTOCOL_ERROR,
    "10004002": NormalizedTransportError.BAD_COMPRESSION,
    "10004004": NormalizedTransportError.PROTOCOL_ERROR,
    "10004019": NormalizedTransportError.PROTOCOL_ERROR,
    "10004020": NormalizedTransportError.PROTOCOL_ERROR,
}


def normalize_provider_code(
    provider_code: str,
    *,
    provider_message: str | None = None,
) -> NormalizedTransportError | None:
    """Map only the provider code; message text is deliberately ignored."""
    del provider_message
    normalized_code = provider_code.strip()
    if normalized_code == "0":
        return None
    return _PROVIDER_CODE_ERRORS.get(
        normalized_code,
        NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR,
    )
