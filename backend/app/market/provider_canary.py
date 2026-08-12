import hashlib
import os
import re
import stat
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.baostock import INDEX_SYMBOLS, _is_main_board
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.provider_health import InMemoryProviderHealthStore
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)

_SAFE_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")

ProviderCanaryStatus = Literal["planned", "confirmation_required", "ready", "error"]
ProviderCanaryErrorCode = Literal[
    "EXPLICIT_CONFIRMATION_REQUIRED",
    "FILESYSTEM_UNSAFE",
    "FILESYSTEM_CHANGED",
    "PROVIDER_ENDPOINT_FAILED",
]
EndpointCanaryOutcome = Literal["planned"] | TransportOutcome


class ProviderCanaryError(RuntimeError):
    def __init__(self, error_code: ProviderCanaryErrorCode) -> None:
        self.error_code = error_code
        super().__init__("provider canary validation failed")


class CanaryProvider(Protocol):
    max_attempts: int

    def refresh_operation(self, refresh_id: str): ...

    def probe_endpoint(
        self,
        endpoint: ProviderEndpoint,
        *,
        trade_date: date,
        stock_symbol: str,
        index_symbol: str,
    ) -> None: ...


ProviderFactory = Callable[..., CanaryProvider]


class ProviderCanaryEndpointReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    endpoint: ProviderEndpoint
    outcome: EndpointCanaryOutcome
    normalized_error: NormalizedTransportError | None = None
    observation_count: int = Field(ge=0)
    provider_session_ids: tuple[str, ...] = ()
    request_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_outcome(self) -> "ProviderCanaryEndpointReport":
        if self.outcome == TransportOutcome.ERROR and self.normalized_error is None:
            raise ValueError("failed endpoint requires a normalized error")
        if self.outcome != TransportOutcome.ERROR and self.normalized_error is not None:
            raise ValueError("non-failed endpoint cannot have a normalized error")
        return self


class ProviderCanaryReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: ProviderCanaryStatus
    error_code: ProviderCanaryErrorCode | None = None
    canary_id: str | None = None
    trade_date: date
    stock_symbol: str
    index_symbol: str
    endpoints: tuple[ProviderCanaryEndpointReport, ...]
    observations: tuple[TransportObservation, ...] = ()
    network_requests: int = Field(ge=0)
    filesystem_entries_before: int | None = Field(default=None, ge=0)
    filesystem_entries_after: int | None = Field(default=None, ge=0)
    zero_write_verified: bool
    writes_database: Literal[False] = False
    writes_parquet: Literal[False] = False
    writes_manifest: Literal[False] = False
    writes_pointer: Literal[False] = False
    refresh_triggered: Literal[False] = False


@dataclass(frozen=True)
class _FingerprintEntry:
    root: Literal["control", "data"]
    relative_path: str
    entry_type: Literal["directory", "file"]
    mode: int
    size: int
    sha256: str | None


class _DiscardWriter:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


def _validate_request(
    trade_date: date,
    stock_symbol: str,
    index_symbol: str,
) -> None:
    if not isinstance(trade_date, date):
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")
    if _SAFE_SYMBOL.fullmatch(stock_symbol) is None:
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")
    if stock_symbol in INDEX_SYMBOLS or not _is_main_board(stock_symbol):
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")
    if _SAFE_SYMBOL.fullmatch(index_symbol) is None or index_symbol not in INDEX_SYMBOLS:
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")


def _safe_resolved_root(root: Path) -> Path:
    if not isinstance(root, Path) or not root.is_absolute():
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")
    try:
        if root.parent == root or root.is_symlink() or not root.is_dir():
            raise ProviderCanaryError("FILESYSTEM_UNSAFE")
        resolved = root.resolve(strict=True)
        if resolved.parent == resolved:
            raise ProviderCanaryError("FILESYSTEM_UNSAFE")
        return resolved
    except (OSError, RuntimeError):
        raise ProviderCanaryError("FILESYSTEM_UNSAFE") from None


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint_entry(
    root_name: Literal["control", "data"],
    base: Path,
    path: Path,
) -> list[_FingerprintEntry]:
    try:
        metadata = path.lstat()
        relative = "." if path == base else path.relative_to(base).as_posix()
        if stat.S_ISLNK(metadata.st_mode):
            raise ProviderCanaryError("FILESYSTEM_UNSAFE")
        if stat.S_ISREG(metadata.st_mode):
            return [
                _FingerprintEntry(
                    root=root_name,
                    relative_path=relative,
                    entry_type="file",
                    mode=stat.S_IMODE(metadata.st_mode),
                    size=metadata.st_size,
                    sha256=_hash_file(path),
                )
            ]
        if not stat.S_ISDIR(metadata.st_mode):
            raise ProviderCanaryError("FILESYSTEM_UNSAFE")
        entries = [
            _FingerprintEntry(
                root=root_name,
                relative_path=relative,
                entry_type="directory",
                mode=stat.S_IMODE(metadata.st_mode),
                size=metadata.st_size,
                sha256=None,
            )
        ]
        with os.scandir(path) as iterator:
            children = sorted((Path(item.path) for item in iterator), key=lambda item: item.name)
        for child in children:
            entries.extend(_fingerprint_entry(root_name, base, child))
        return entries
    except ProviderCanaryError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise ProviderCanaryError("FILESYSTEM_UNSAFE") from None


def _filesystem_fingerprint(
    control_root: Path,
    data_root: Path,
) -> tuple[_FingerprintEntry, ...]:
    control = _safe_resolved_root(control_root)
    data = _safe_resolved_root(data_root)
    if control == data or control in data.parents or data in control.parents:
        raise ProviderCanaryError("FILESYSTEM_UNSAFE")
    return tuple(
        _fingerprint_entry("control", control, control) + _fingerprint_entry("data", data, data)
    )


def _planned_endpoints() -> tuple[ProviderCanaryEndpointReport, ...]:
    return tuple(
        ProviderCanaryEndpointReport(
            endpoint=endpoint,
            outcome="planned",
            observation_count=0,
        )
        for endpoint in ProviderEndpoint
    )


def plan_provider_canary(
    *,
    trade_date: date,
    stock_symbol: str,
    index_symbol: str,
    control_root: Path,
    data_root: Path,
) -> ProviderCanaryReport:
    del control_root, data_root
    _validate_request(trade_date, stock_symbol, index_symbol)
    return ProviderCanaryReport(
        status="planned",
        trade_date=trade_date,
        stock_symbol=stock_symbol,
        index_symbol=index_symbol,
        endpoints=_planned_endpoints(),
        network_requests=0,
        zero_write_verified=False,
    )


def confirmation_required_report(
    *,
    trade_date: date,
    stock_symbol: str,
    index_symbol: str,
    control_root: Path,
    data_root: Path,
) -> ProviderCanaryReport:
    plan = plan_provider_canary(
        trade_date=trade_date,
        stock_symbol=stock_symbol,
        index_symbol=index_symbol,
        control_root=control_root,
        data_root=data_root,
    )
    return plan.model_copy(
        update={
            "status": "confirmation_required",
            "error_code": "EXPLICIT_CONFIRMATION_REQUIRED",
        }
    )


def _unique_values(values: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _normalized_exception(error: Exception) -> NormalizedTransportError | None:
    candidate = getattr(error, "normalized_error", None)
    return candidate if isinstance(candidate, NormalizedTransportError) else None


def _endpoint_report(
    endpoint: ProviderEndpoint,
    observations: tuple[TransportObservation, ...],
    error: Exception | None,
) -> ProviderCanaryEndpointReport:
    operation_observations = tuple(
        item for item in observations if item.protocol_stage == ProtocolStage.OPERATION
    )
    normalized_error = next(
        (item.normalized_error for item in reversed(observations) if item.normalized_error),
        None,
    )
    if error is not None and normalized_error is None:
        normalized_error = _normalized_exception(error)
    session_ids = _unique_values([item.provider_session_id for item in observations])
    request_ids = _unique_values([item.request_id for item in observations])
    valid_success = (
        error is None
        and normalized_error is None
        and bool(operation_observations)
        and len(session_ids) == 1
        and all(item.attempt == 1 for item in observations)
        and all(item.outcome == TransportOutcome.SUCCESS for item in observations)
        and any(item.outcome == TransportOutcome.SUCCESS for item in operation_observations)
    )
    if valid_success:
        outcome: EndpointCanaryOutcome = TransportOutcome.SUCCESS
    else:
        outcome = TransportOutcome.ERROR
        normalized_error = normalized_error or NormalizedTransportError.PROTOCOL_ERROR
    return ProviderCanaryEndpointReport(
        endpoint=endpoint,
        outcome=outcome,
        normalized_error=normalized_error,
        observation_count=len(observations),
        provider_session_ids=session_ids,
        request_ids=request_ids,
    )


def _ids_are_independent(
    endpoints: list[ProviderCanaryEndpointReport],
) -> bool:
    seen_sessions: set[str] = set()
    seen_requests: set[str] = set()
    for endpoint in endpoints:
        sessions = set(endpoint.provider_session_ids)
        requests = set(endpoint.request_ids)
        if len(sessions) != 1 or not requests:
            return False
        if sessions & seen_sessions or requests & seen_requests:
            return False
        seen_sessions.update(sessions)
        seen_requests.update(requests)
    return True


def run_provider_canary(
    *,
    trade_date: date,
    stock_symbol: str,
    index_symbol: str,
    control_root: Path,
    data_root: Path,
    provider_factory: ProviderFactory,
) -> ProviderCanaryReport:
    _validate_request(trade_date, stock_symbol, index_symbol)
    before = _filesystem_fingerprint(control_root, data_root)
    canary_id = f"canary-{uuid4().hex}"
    health_store = InMemoryProviderHealthStore()
    endpoint_reports: list[ProviderCanaryEndpointReport] = []
    discard = _DiscardWriter()

    for endpoint in ProviderEndpoint:
        first_observation = len(health_store.list_observations())
        error: Exception | None = None
        try:
            with redirect_stdout(discard), redirect_stderr(discard):
                provider = provider_factory(max_attempts=1)
                if getattr(provider, "max_attempts", None) != 1:
                    raise RuntimeError("provider canary attempt policy is invalid")
                with transport_observation_sink(health_store.record_observation):
                    with provider.refresh_operation(canary_id):
                        provider.probe_endpoint(
                            endpoint,
                            trade_date=trade_date,
                            stock_symbol=stock_symbol,
                            index_symbol=index_symbol,
                        )
        except Exception as caught:
            error = caught
        new_observations = tuple(health_store.list_observations()[first_observation:])
        endpoint_observations = tuple(
            item
            for item in new_observations
            if item.endpoint == endpoint and item.refresh_id == canary_id
        )
        endpoint_reports.append(_endpoint_report(endpoint, endpoint_observations, error))

    observations = tuple(
        item
        for item in health_store.list_observations()
        if isinstance(item.endpoint, ProviderEndpoint) and item.refresh_id == canary_id
    )
    ids_independent = _ids_are_independent(endpoint_reports)
    endpoint_failed = (
        any(item.outcome == TransportOutcome.ERROR for item in endpoint_reports)
        or not ids_independent
    )
    try:
        after = _filesystem_fingerprint(control_root, data_root)
    except ProviderCanaryError:
        after = None
    zero_write_verified = after is not None and before == after
    if not zero_write_verified:
        status: ProviderCanaryStatus = "error"
        error_code: ProviderCanaryErrorCode | None = "FILESYSTEM_CHANGED"
    elif endpoint_failed:
        status = "error"
        error_code = "PROVIDER_ENDPOINT_FAILED"
    else:
        status = "ready"
        error_code = None
    return ProviderCanaryReport(
        status=status,
        error_code=error_code,
        canary_id=canary_id,
        trade_date=trade_date,
        stock_symbol=stock_symbol,
        index_symbol=index_symbol,
        endpoints=tuple(endpoint_reports),
        observations=observations,
        network_requests=len({item.request_id for item in observations}),
        filesystem_entries_before=len(before),
        filesystem_entries_after=len(after) if after is not None else None,
        zero_write_verified=zero_write_verified,
    )
