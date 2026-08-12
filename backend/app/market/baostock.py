import logging
import re
import signal
import socket
import threading
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from time import monotonic, sleep
from typing import Any

from backend.app.market.baostock_vendor import (
    emit_terminal_observation,
    install_baostock_transport_patch,
)
from backend.app.market.factor_cache import AdjustmentFactorCache, FactorCacheError
from backend.app.market.failures import (
    MarketFailure,
    baostock_status_failure,
    market_failure_from_exception,
)
from backend.app.market.models import DailyBar
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    OperationDeadlineInterrupt,
    ProtocolStage,
    ProviderEndpoint,
    RefreshTransportContext,
    TransportEndpoint,
    normalize_provider_code,
    provider_session_scope,
    refresh_scope,
    request_scope,
)

DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
)
INDEX_SYMBOLS = ("sh.000001", "sz.399001")
SH_MAIN_PREFIXES = ("sh.600", "sh.601", "sh.603", "sh.605")
SZ_MAIN_PREFIXES = ("sz.000", "sz.001", "sz.002", "sz.003")
logger = logging.getLogger("stock_eva.market.baostock")
_LOGIN_SOCKET_TIMEOUT_LOCK = threading.Lock()
_SAFE_PROBE_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")


class _EofAwareSocket:
    """Turn BaoStock's otherwise-unbounded peer EOF into a transport error."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def recv(self, *args: Any, **kwargs: Any) -> bytes:
        payload = self._connection.recv(*args, **kwargs)
        if payload == b"":
            raise ConnectionResetError("BaoStock closed the connection before the response ended")
        return payload

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


@dataclass(frozen=True)
class ProviderBatch:
    bars: list[DailyBar]
    expected_symbols: list[str]
    failed_symbols: list[str]
    failure: MarketFailure | None = None


@dataclass(frozen=True)
class ProviderRangeBatch:
    bars: list[DailyBar]
    trading_dates: list[date]
    expected_symbols: list[str]
    failed_symbols: list[str]
    failure: MarketFailure | None = None


@dataclass(frozen=True)
class MainBoardInspection:
    trade_date: date
    main_board_count: int
    shanghai_count: int
    shenzhen_count: int
    total_expected_count: int
    metadata_provider_requests: int
    main_board_symbols: tuple[str, ...] = ()


class BaoStockError(RuntimeError):
    def __init__(self, message: str, *, failure: MarketFailure | None = None) -> None:
        self.failure = failure
        super().__init__(message)


class BaoStockTransportError(BaoStockError):
    pass


class BaoStockSessionStateError(BaoStockError):
    pass


class _OperationDeadlineExceeded(BaoStockError):
    pass


class _OperationDeadlineUnavailable(BaoStockError):
    pass


class _PreexistingTimerInterrupt(BaseException):
    pass


def _pagination_protocol_error(
    message: str,
    *,
    normalized_error: NormalizedTransportError = NormalizedTransportError.PROTOCOL_ERROR,
) -> BaoStockTransportError:
    emit_terminal_observation(
        started_at=monotonic(),
        protocol_stage=ProtocolStage.PAGINATION,
        recv_calls=0,
        response_bytes=0,
        end_marker_seen=False,
        provider_code=None,
        normalized_error=normalized_error,
    )
    error = BaoStockTransportError(
        message,
        failure=MarketFailure(
            failure_stage="fetch",
            failure_class="transport_connect",
            retryable=True,
        ),
    )
    error.normalized_error = normalized_error
    return error


def _normalized_operation_error(error: BaseException) -> NormalizedTransportError:
    candidate = getattr(error, "normalized_error", None)
    if isinstance(candidate, NormalizedTransportError):
        return candidate
    if isinstance(error, TimeoutError):
        return NormalizedTransportError.RECV_TIMEOUT
    failure = getattr(error, "failure", None)
    failure_class = getattr(failure, "failure_class", None)
    if failure_class == "transport_timeout":
        return NormalizedTransportError.RECV_TIMEOUT
    if failure_class == "rate_limit":
        return NormalizedTransportError.RATE_LIMIT
    return NormalizedTransportError.PROTOCOL_ERROR


def _emit_operation_outcome(error: BaseException | None = None) -> None:
    emit_terminal_observation(
        started_at=monotonic(),
        protocol_stage=ProtocolStage.OPERATION,
        recv_calls=0,
        response_bytes=0,
        end_marker_seen=False,
        provider_code=None,
        normalized_error=(_normalized_operation_error(error) if error is not None else None),
    )


def _page_number(result: Any) -> int:
    try:
        page = int(result.cur_page_num)
    except (AttributeError, TypeError, ValueError):
        raise _pagination_protocol_error("BaoStock pagination protocol is invalid") from None
    if page < 1:
        raise _pagination_protocol_error("BaoStock pagination protocol is invalid")
    return page


def _page_fingerprint(result: Any) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(str(value) for value in row) for row in result.data)


@contextmanager
def _wall_clock_deadline(seconds: float):
    if threading.current_thread() is not threading.main_thread():
        raise _OperationDeadlineUnavailable("BaoStock wall-clock deadlines require the main thread")
    if not all(
        hasattr(signal, name) for name in ("SIGALRM", "ITIMER_REAL", "getitimer", "setitimer")
    ):
        raise _OperationDeadlineUnavailable(
            "BaoStock wall-clock deadlines require POSIX interval timers"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    previous_fires_first = 0 < previous_delay <= seconds
    timer_delay = previous_delay if previous_fires_first else seconds
    previous_fired = False
    started_at = monotonic()

    def raise_deadline(signum, frame):
        nonlocal previous_fired
        if previous_fires_first:
            previous_fired = True
            if callable(previous_handler):
                previous_handler(signum, frame)
            raise _PreexistingTimerInterrupt(
                "pre-existing wall-clock timer expired during BaoStock operation"
            )
        raise OperationDeadlineInterrupt

    signal.signal(signal.SIGALRM, raise_deadline)
    signal.setitimer(signal.ITIMER_REAL, timer_delay)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_fired:
            restored_delay = previous_interval if previous_interval > 0 else 0
        elif previous_delay > 0:
            restored_delay = max(previous_delay - (monotonic() - started_at), 1e-6)
        else:
            restored_delay = 0
        signal.setitimer(
            signal.ITIMER_REAL,
            restored_delay,
            previous_interval,
        )


def _read_result(
    result: Any,
    *,
    before_page_request: Callable[[], None] | None = None,
    next_page_scope: Callable[[int], Any] | None = None,
) -> tuple[list[str], list[list[str]]]:
    if result.error_code != "0":
        failure = baostock_status_failure(result.error_code)
        raise BaoStockTransportError(
            f"BaoStock request failed ({failure.failure_class})",
            failure=failure,
        )
    rows: list[list[str]] = []
    page_fingerprints: set[tuple[tuple[str, ...], ...]] = set()
    while True:
        page_request_required = (
            before_page_request is not None
            and next_page_scope is not None
            and hasattr(result, "data")
            and hasattr(result, "cur_row_num")
            and hasattr(result, "per_page_count")
            and result.cur_row_num >= len(result.data)
        )
        if page_request_required:
            try:
                per_page_count = int(result.per_page_count)
            except (TypeError, ValueError):
                raise _pagination_protocol_error(
                    "BaoStock pagination protocol is invalid"
                ) from None
            if per_page_count < 1 or len(result.data) > per_page_count:
                raise _pagination_protocol_error("BaoStock pagination protocol is invalid")
            page_request_required = len(result.data) == per_page_count
        if page_request_required:
            current_page = _page_number(result)
            current_fingerprint = _page_fingerprint(result)
            if current_fingerprint in page_fingerprints:
                raise _pagination_protocol_error(
                    "BaoStock pagination protocol repeated page data",
                    normalized_error=NormalizedTransportError.PAGINATION_STALLED,
                )
            page_fingerprints.add(current_fingerprint)
            before_page_request()
            with next_page_scope(current_page + 1):
                has_row = result.next()
                if result.error_code != "0":
                    failure = baostock_status_failure(result.error_code)
                    raise BaoStockTransportError(
                        f"BaoStock pagination failed ({failure.failure_class})",
                        failure=failure,
                    )
                if not has_row:
                    raise _pagination_protocol_error(
                        "BaoStock pagination protocol ended after a full page"
                    )
                next_page = _page_number(result)
                next_fingerprint = _page_fingerprint(result)
                if next_page != current_page + 1 or next_fingerprint in page_fingerprints:
                    raise _pagination_protocol_error(
                        "BaoStock pagination protocol did not advance",
                        normalized_error=NormalizedTransportError.PAGINATION_STALLED,
                    )
        else:
            has_row = result.next()
        if not has_row:
            break
        rows.append(result.get_row_data())
    if result.error_code != "0":
        failure = baostock_status_failure(result.error_code)
        raise BaoStockTransportError(
            f"BaoStock pagination failed ({failure.failure_class})",
            failure=failure,
        )
    return list(result.fields), rows


def _normalize_provider_rows(
    *,
    fields: Sequence[str],
    rows: Sequence[Sequence[str]],
    factor_fields: Sequence[str],
    factor_rows: Sequence[Sequence[str]],
) -> list[DailyBar]:
    required_daily_fields = set(DAILY_FIELDS.split(","))
    factor_required_fields = {"code", "dividOperateDate", "backAdjustFactor"}
    malformed_shape = required_daily_fields - set(fields) or any(
        len(row) != len(fields) for row in rows
    )
    malformed_factor_shape = bool(factor_rows) and (
        factor_required_fields - set(factor_fields)
        or any(len(row) != len(factor_fields) for row in factor_rows)
    )
    if malformed_shape or malformed_factor_shape:
        raise BaoStockError(
            "BaoStock response schema is invalid",
            failure=MarketFailure(
                failure_stage="normalize",
                failure_class="schema",
                retryable=False,
            ),
        )
    try:
        return normalize_baostock_rows(
            fields=fields,
            rows=rows,
            factor_fields=factor_fields,
            factor_rows=factor_rows,
        )
    except (KeyError, TypeError, ValueError):
        raise BaoStockError(
            "BaoStock response semantics are invalid",
            failure=MarketFailure(
                failure_stage="normalize",
                failure_class="semantic",
                retryable=False,
            ),
        ) from None


def _required_field_positions(
    fields: Sequence[str],
    required: Sequence[str],
) -> dict[str, int]:
    if any(field not in fields for field in required):
        raise BaoStockError(
            "BaoStock response schema is invalid",
            failure=MarketFailure(
                failure_stage="normalize",
                failure_class="schema",
                retryable=False,
            ),
        )
    return {field: fields.index(field) for field in required}


def _candidate_integrity_error(message: str) -> BaoStockError:
    return BaoStockError(
        message,
        failure=MarketFailure(
            failure_stage="validate",
            failure_class="reconciliation",
            retryable=False,
        ),
    )


def _require_unique_symbols(symbols: Sequence[str]) -> None:
    if len(symbols) != len(set(symbols)):
        raise _candidate_integrity_error("BaoStock candidate contains duplicate symbols")


def _require_expected_symbol_rows(
    fields: Sequence[str],
    rows: Sequence[Sequence[str]],
    *,
    expected_symbol: str,
    unique_date_points: bool = False,
) -> None:
    required = ["code"]
    if unique_date_points:
        required.append("date")
    positions = _required_field_positions(fields, required)
    try:
        symbols = [row[positions["code"]] for row in rows]
        dates = [row[positions["date"]] for row in rows] if unique_date_points else []
    except IndexError:
        raise BaoStockError(
            "BaoStock response schema is invalid",
            failure=MarketFailure(
                failure_stage="normalize",
                failure_class="schema",
                retryable=False,
            ),
        ) from None
    if any(symbol != expected_symbol for symbol in symbols):
        raise _candidate_integrity_error("BaoStock candidate has universe divergence")
    if unique_date_points and len(dates) != len(set(dates)):
        raise _candidate_integrity_error("BaoStock candidate contains duplicate points")


def _is_main_board(symbol: str) -> bool:
    return symbol.startswith(SH_MAIN_PREFIXES + SZ_MAIN_PREFIXES)


def _safe_probe_symbol(symbol: str | None) -> bool:
    return isinstance(symbol, str) and _SAFE_PROBE_SYMBOL.fullmatch(symbol) is not None


class BaoStockProvider:
    def __init__(
        self,
        client: Any | None = None,
        *,
        max_explicit_symbols: int = 20,
        max_attempts: int = 2,
        min_request_interval_seconds: float | None = None,
        factor_cache_path: str | None = None,
        factor_cache: AdjustmentFactorCache | None = None,
        socket_timeout_seconds: float = 30.0,
        monotonic_fn=monotonic,
        sleep_fn=sleep,
    ) -> None:
        is_real_client = client is None
        if client is None:
            install_baostock_transport_patch()
            import baostock as client_module

            client = client_module
        self.client = client
        self.max_explicit_symbols = max_explicit_symbols
        self.max_attempts = max_attempts
        self.min_request_interval_seconds = (
            (0.2 if is_real_client else 0.0)
            if min_request_interval_seconds is None
            else min_request_interval_seconds
        )
        self._monotonic = monotonic_fn
        self._sleep = sleep_fn
        self._last_request_at: float | None = None
        self._provider_request_count = 0
        self.factor_cache = factor_cache or AdjustmentFactorCache(factor_cache_path)
        if socket_timeout_seconds <= 0:
            raise ValueError("socket timeout must be positive")
        self.socket_timeout_seconds = socket_timeout_seconds
        self._session_usable = False
        self._provider_session_id: str | None = None
        self._refresh_scope_depth = 0
        self._active_refresh_id: str | None = None

    @contextmanager
    def refresh_operation(self, refresh_id: str):
        try:
            validated = RefreshTransportContext(refresh_id=refresh_id).refresh_id
        except ValueError:
            raise BaoStockSessionStateError("BaoStock refresh identifier is invalid") from None
        if self._refresh_scope_depth > 0:
            if validated != self._active_refresh_id:
                raise BaoStockSessionStateError("BaoStock refresh scope cannot be replaced")
            yield
            return
        with refresh_scope(validated):
            self._refresh_scope_depth += 1
            self._active_refresh_id = validated
            try:
                yield
            finally:
                self._active_refresh_id = None
                self._refresh_scope_depth -= 1

    @contextmanager
    def _refresh_operation(self):
        if self._refresh_scope_depth > 0:
            yield
            return
        with refresh_scope() as context:
            self._refresh_scope_depth += 1
            self._active_refresh_id = context.refresh_id
            try:
                yield
            finally:
                self._active_refresh_id = None
                self._refresh_scope_depth -= 1

    @contextmanager
    def _request_scope(self, endpoint: TransportEndpoint, *, attempt: int, page: int):
        if self._provider_session_id is None:
            raise BaoStockSessionStateError("BaoStock provider session is unavailable")
        with provider_session_scope(self._provider_session_id):
            with request_scope(endpoint, attempt=attempt, page=page) as context:
                yield context

    def _login(self, endpoint: TransportEndpoint) -> None:
        with self._refresh_operation():
            self._login_scoped(endpoint)

    def _login_scoped(self, endpoint: TransportEndpoint) -> None:
        if self._session_usable:
            raise BaoStockSessionStateError("BaoStock session is already active")

        def emit_final_failure(error: BaseException, *, attempt: int) -> None:
            if attempt == self.max_attempts and isinstance(endpoint, ProviderEndpoint):
                _emit_operation_outcome(error)

        last_error: BaoStockError | None = None
        for attempt in range(1, self.max_attempts + 1):
            with provider_session_scope() as session:
                self._provider_session_id = session.provider_session_id
                with request_scope(endpoint, attempt=attempt, page=1):
                    try:
                        result = self._run_with_deadline(
                            self._call_login,
                            operation_name="login",
                        )
                    except _OperationDeadlineExceeded as exc:
                        last_error = exc
                        emit_final_failure(exc, attempt=attempt)
                        continue
                    except (TimeoutError, OSError) as exc:
                        last_error = BaoStockTransportError(
                            "BaoStock login transport failed",
                            failure=market_failure_from_exception(exc, stage="fetch"),
                        )
                        self._discard_session()
                        emit_final_failure(exc, attempt=attempt)
                        continue
                    except Exception as exc:
                        self._discard_session()
                        if isinstance(endpoint, ProviderEndpoint):
                            _emit_operation_outcome(exc)
                        raise
                    if result.error_code != "0":
                        failure = baostock_status_failure(result.error_code)
                        last_error = BaoStockTransportError(
                            f"BaoStock login failed ({failure.failure_class})",
                            failure=failure,
                        )
                        last_error.normalized_error = normalize_provider_code(result.error_code)
                        self._discard_session()
                        emit_final_failure(last_error, attempt=attempt)
                        continue
                    try:
                        self._configure_socket_timeout()
                    except (TimeoutError, OSError) as exc:
                        last_error = BaoStockTransportError(
                            "BaoStock socket timeout configuration failed",
                            failure=market_failure_from_exception(exc, stage="fetch"),
                        )
                        self._discard_session()
                        emit_final_failure(exc, attempt=attempt)
                        continue
                    except Exception as exc:
                        self._discard_session()
                        if isinstance(endpoint, ProviderEndpoint):
                            _emit_operation_outcome(exc)
                        raise
                    self._session_usable = True
                    return
        raise last_error or BaoStockTransportError("BaoStock login failed")

    def _call_login(self):
        """Bound the socket from connect through the initial login response."""
        if not self._uses_real_baostock_socket():
            return self.client.login()
        with _LOGIN_SOCKET_TIMEOUT_LOCK:
            previous = socket.getdefaulttimeout()
            socket.setdefaulttimeout(self.socket_timeout_seconds)
            try:
                return self.client.login()
            finally:
                socket.setdefaulttimeout(previous)

    def _uses_real_baostock_socket(self) -> bool:
        login = getattr(self.client, "login", None)
        globals_ = getattr(login, "__globals__", {})
        return "conx" in globals_ and "sock" in globals_

    def _client_context(self):
        context = getattr(self.client, "context", None)
        if context is not None:
            return context
        login = getattr(self.client, "login", None)
        globals_ = getattr(login, "__globals__", {})
        return globals_.get("conx")

    def _configure_socket_timeout(self) -> None:
        context = self._client_context()
        connection = getattr(context, "default_socket", None)
        if connection is not None:
            connection.settimeout(self.socket_timeout_seconds)
            if hasattr(connection, "recv") and not isinstance(connection, _EofAwareSocket):
                context.default_socket = _EofAwareSocket(connection)

    def _discard_session(self) -> None:
        context = self._client_context()
        connection = getattr(context, "default_socket", None)
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
            try:
                context.default_socket = None
            except Exception:
                pass
        self._session_usable = False
        self._provider_session_id = None

    def _run_with_deadline(self, operation, *, operation_name: str):
        try:
            with _wall_clock_deadline(self.socket_timeout_seconds):
                return operation()
        except OperationDeadlineInterrupt:
            self._discard_session()
            raise _OperationDeadlineExceeded(
                f"BaoStock {operation_name} wall-clock deadline exceeded",
                failure=MarketFailure(
                    failure_stage="fetch",
                    failure_class="transport_timeout",
                    retryable=True,
                ),
            ) from None
        except _PreexistingTimerInterrupt:
            self._discard_session()
            raise _OperationDeadlineUnavailable(
                "pre-existing timer expired during BaoStock operation",
                failure=MarketFailure(
                    failure_stage="fetch",
                    failure_class="internal",
                    retryable=False,
                ),
            ) from None

    def _logout(self) -> None:
        if not self._session_usable:
            self._discard_session()
            return
        try:
            self.client.logout()
        except Exception:
            pass
        finally:
            self._discard_session()

    def _ensure_session(self, endpoint: TransportEndpoint) -> None:
        if not self._session_usable:
            self._login(endpoint)

    def fetch(self, trade_date: date, symbols: Sequence[str] | None = None) -> ProviderBatch:
        if symbols is not None and len(set(symbols)) > self.max_explicit_symbols:
            raise BaoStockError(
                f"explicit refresh accepts at most {self.max_explicit_symbols} symbols",
                failure=MarketFailure(
                    failure_stage="validate",
                    failure_class="universe",
                    retryable=False,
                ),
            )
        with self._refresh_operation():
            self._login(ProviderEndpoint.TRADE_DATES)
            try:
                self._ensure_trading_day(trade_date)
                if symbols is None:
                    return self._fetch_main_board(trade_date)
                return self._fetch_symbols(trade_date, symbols)
            finally:
                self._logout()

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        if start_date > end_date:
            raise BaoStockError(
                "start date must not be after end date",
                failure=MarketFailure(
                    failure_stage="validate",
                    failure_class="calendar",
                    retryable=False,
                ),
            )
        with self._refresh_operation():
            self._login(ProviderEndpoint.TRADE_DATES)
            try:
                return self._trading_dates(start_date, end_date)
            finally:
                self._logout()

    def probe_endpoint(
        self,
        endpoint: ProviderEndpoint,
        *,
        trade_date: date,
        stock_symbol: str | None = None,
        index_symbol: str | None = None,
    ) -> None:
        if not isinstance(endpoint, ProviderEndpoint):
            raise BaoStockSessionStateError("BaoStock probe endpoint is invalid")
        if endpoint == ProviderEndpoint.ADJUST_FACTOR and not (
            _safe_probe_symbol(stock_symbol)
            and stock_symbol not in INDEX_SYMBOLS
            and _is_main_board(stock_symbol)
        ):
            raise BaoStockSessionStateError("BaoStock probe symbol is invalid")
        if endpoint == ProviderEndpoint.INDEX_HISTORY and index_symbol not in INDEX_SYMBOLS:
            raise BaoStockSessionStateError("BaoStock probe symbol is invalid")
        iso_date = trade_date.isoformat()
        operations = {
            ProviderEndpoint.TRADE_DATES: lambda: self.client.query_trade_dates(
                start_date=iso_date,
                end_date=iso_date,
            ),
            ProviderEndpoint.ALL_STOCK: lambda: self.client.query_all_stock(day=iso_date),
            ProviderEndpoint.DAILY_ASTOCK: lambda: self.client.query_daily_history_k_AStock(
                date=iso_date
            ),
            ProviderEndpoint.DAILY_FACTOR: lambda: self.client.query_daily_adjust_factor(
                date=iso_date
            ),
            ProviderEndpoint.ADJUST_FACTOR: lambda: self.client.query_adjust_factor(
                stock_symbol,
                start_date="1990-01-01",
                end_date=iso_date,
            ),
            ProviderEndpoint.INDEX_HISTORY: lambda: self.client.query_history_k_data_plus(
                index_symbol,
                DAILY_FIELDS,
                start_date=iso_date,
                end_date=iso_date,
                frequency="d",
                adjustflag="3",
            ),
        }
        with self._refresh_operation():
            self._login(endpoint)
            try:
                self._read(endpoint, operations[endpoint])
            finally:
                self._logout()

    def inspect_main_board(self, trade_date: date) -> MainBoardInspection:
        """Read only the calendar and security universe; never request OHLCV."""
        with self._refresh_operation():
            initial_request_count = self._provider_request_count
            self._login(ProviderEndpoint.TRADE_DATES)
            try:
                self._ensure_trading_day(trade_date)
                fields, rows = self._read(
                    ProviderEndpoint.ALL_STOCK,
                    lambda: self.client.query_all_stock(day=trade_date.isoformat()),
                )
                code_index = fields.index("code")
                symbol_rows = [row[code_index] for row in rows if _is_main_board(row[code_index])]
                _require_unique_symbols(symbol_rows)
                symbols = set(symbol_rows)
                shanghai = sum(symbol.startswith("sh.") for symbol in symbols)
                shenzhen = sum(symbol.startswith("sz.") for symbol in symbols)
                return MainBoardInspection(
                    trade_date=trade_date,
                    main_board_count=len(symbols),
                    shanghai_count=shanghai,
                    shenzhen_count=shenzhen,
                    total_expected_count=len(symbols) + len(INDEX_SYMBOLS),
                    metadata_provider_requests=(
                        self._provider_request_count - initial_request_count
                    ),
                    main_board_symbols=tuple(sorted(symbols)),
                )
            finally:
                self._logout()

    def _trading_dates(self, start_date: date, end_date: date) -> list[date]:
        fields, rows = self._read(
            ProviderEndpoint.TRADE_DATES,
            lambda: self.client.query_trade_dates(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            ),
        )
        positions = _required_field_positions(
            fields,
            ("calendar_date", "is_trading_day"),
        )
        date_index = positions["calendar_date"]
        trading_index = positions["is_trading_day"]
        return [date.fromisoformat(row[date_index]) for row in rows if row[trading_index] == "1"]

    def fetch_range(
        self,
        start_date: date,
        end_date: date,
        *,
        symbols: Sequence[str],
    ) -> ProviderRangeBatch:
        with self._refresh_operation():
            return self._fetch_range(start_date, end_date, symbols=symbols)

    def _fetch_range(
        self,
        start_date: date,
        end_date: date,
        *,
        symbols: Sequence[str],
    ) -> ProviderRangeBatch:
        expected_symbols = list(dict.fromkeys(symbols))
        if not expected_symbols:
            raise BaoStockError(
                "historical backfill requires explicit symbols",
                failure=MarketFailure(
                    failure_stage="validate",
                    failure_class="universe",
                    retryable=False,
                ),
            )
        if len(expected_symbols) > self.max_explicit_symbols:
            raise BaoStockError(
                f"explicit refresh accepts at most {self.max_explicit_symbols} symbols",
                failure=MarketFailure(
                    failure_stage="validate",
                    failure_class="universe",
                    retryable=False,
                ),
            )
        self._login(ProviderEndpoint.TRADE_DATES)
        try:
            trading_dates = self._trading_dates(start_date, end_date)
            if not trading_dates:
                raise BaoStockError(
                    "date range contains no trading days",
                    failure=MarketFailure(
                        failure_stage="validate",
                        failure_class="calendar",
                        retryable=False,
                    ),
                )
            daily_fields = DAILY_FIELDS.split(",")
            daily_rows: list[list[str]] = []
            factor_fields: list[str] = []
            factor_rows: list[list[str]] = []
            failed: list[str] = []
            batch_failure: MarketFailure | None = None
            for symbol in expected_symbols:
                try:
                    fields, rows = self._read(
                        ProviderEndpoint.INDEX_HISTORY,
                        lambda symbol=symbol: self.client.query_history_k_data_plus(
                            symbol,
                            DAILY_FIELDS,
                            start_date=start_date.isoformat(),
                            end_date=end_date.isoformat(),
                            frequency="d",
                            adjustflag="3",
                        ),
                    )
                    if fields != daily_fields:
                        raise BaoStockError(
                            "BaoStock response schema is invalid",
                            failure=MarketFailure(
                                failure_stage="normalize",
                                failure_class="schema",
                                retryable=False,
                            ),
                        )
                    if not rows:
                        failed.append(symbol)
                        continue
                    _require_expected_symbol_rows(
                        fields,
                        rows,
                        expected_symbol=symbol,
                        unique_date_points=True,
                    )
                    daily_rows.extend(rows)
                    if symbol not in INDEX_SYMBOLS:
                        current_fields, current_rows = self._read(
                            ProviderEndpoint.ADJUST_FACTOR,
                            lambda symbol=symbol: self.client.query_adjust_factor(
                                symbol,
                                start_date="1990-01-01",
                                end_date=end_date.isoformat(),
                            ),
                        )
                        if current_rows:
                            _require_expected_symbol_rows(
                                current_fields,
                                current_rows,
                                expected_symbol=symbol,
                            )
                            factor_fields = current_fields
                            factor_rows.extend(current_rows)
                except BaoStockError as exc:
                    if exc.failure is not None and not exc.failure.retryable:
                        raise
                    batch_failure = batch_failure or exc.failure
                    failed.append(symbol)
            return ProviderRangeBatch(
                bars=_normalize_provider_rows(
                    fields=daily_fields,
                    rows=daily_rows,
                    factor_fields=factor_fields,
                    factor_rows=factor_rows,
                ),
                trading_dates=trading_dates,
                expected_symbols=expected_symbols,
                failed_symbols=failed,
                failure=batch_failure,
            )
        finally:
            self._logout()

    def _read(
        self,
        endpoint: TransportEndpoint,
        operation: Callable[[], Any],
    ) -> tuple[list[str], list[list[str]]]:
        with self._refresh_operation():
            last_error: BaoStockError | None = None
            for attempt in range(1, self.max_attempts + 1):
                self._ensure_session(endpoint)
                self._pace_request()
                try:
                    with self._request_scope(endpoint, attempt=attempt, page=1):

                        def read_attempt(attempt=attempt):
                            result = operation()
                            return _read_result(
                                result,
                                before_page_request=self._pace_request,
                                next_page_scope=lambda page: self._request_scope(
                                    endpoint,
                                    attempt=attempt,
                                    page=page,
                                ),
                            )

                        try:
                            result = self._run_with_deadline(
                                read_attempt,
                                operation_name="request",
                            )
                        except (BaoStockError, TimeoutError, OSError) as exc:
                            if isinstance(endpoint, ProviderEndpoint) and (
                                attempt == self.max_attempts
                                or isinstance(exc, _OperationDeadlineUnavailable)
                            ):
                                _emit_operation_outcome(exc)
                            raise
                        if isinstance(endpoint, ProviderEndpoint):
                            _emit_operation_outcome()
                        return result
                except _OperationDeadlineUnavailable:
                    raise
                except (BaoStockError, TimeoutError, OSError) as exc:
                    last_error = (
                        exc
                        if isinstance(exc, BaoStockError)
                        else BaoStockTransportError(
                            "BaoStock transport failed",
                            failure=market_failure_from_exception(exc, stage="fetch"),
                        )
                    )
                    self._discard_session()
            raise last_error or BaoStockError("BaoStock request failed")

    def _pace_request(self) -> None:
        now = self._monotonic()
        if self._last_request_at is not None:
            remaining = self.min_request_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                self._sleep(remaining)
                now = self._monotonic()
        self._last_request_at = now
        self._provider_request_count += 1

    def _ensure_trading_day(self, trade_date: date) -> None:
        fields, rows = self._read(
            ProviderEndpoint.TRADE_DATES,
            lambda: self.client.query_trade_dates(
                start_date=trade_date.isoformat(),
                end_date=trade_date.isoformat(),
            ),
        )
        trading_index = _required_field_positions(fields, ("is_trading_day",))["is_trading_day"]
        if len(rows) != 1 or rows[0][trading_index] != "1":
            raise BaoStockError(
                f"{trade_date.isoformat()} is not a trading day",
                failure=MarketFailure(
                    failure_stage="validate",
                    failure_class="calendar",
                    retryable=False,
                ),
            )

    def _fetch_symbols(self, trade_date: date, symbols: Sequence[str]) -> ProviderBatch:
        daily_fields = DAILY_FIELDS.split(",")
        daily_rows: list[list[str]] = []
        factor_fields: list[str] = []
        factor_rows: list[list[str]] = []
        failed: list[str] = []
        batch_failure: MarketFailure | None = None
        iso_date = trade_date.isoformat()

        expected_symbols = list(dict.fromkeys(symbols))
        for symbol in expected_symbols:
            try:
                fields, rows = self._read(
                    ProviderEndpoint.INDEX_HISTORY,
                    lambda symbol=symbol: self.client.query_history_k_data_plus(
                        symbol,
                        DAILY_FIELDS,
                        start_date=iso_date,
                        end_date=iso_date,
                        frequency="d",
                        adjustflag="3",
                    ),
                )
                if fields != daily_fields:
                    raise BaoStockError(
                        "BaoStock response schema is invalid",
                        failure=MarketFailure(
                            failure_stage="normalize",
                            failure_class="schema",
                            retryable=False,
                        ),
                    )
                if not rows:
                    failed.append(symbol)
                    continue
                code_index = daily_fields.index("code")
                row_symbols = [row[code_index] for row in rows]
                _require_unique_symbols(row_symbols)
                if any(row_symbol != symbol for row_symbol in row_symbols):
                    raise _candidate_integrity_error("BaoStock candidate has universe divergence")
                daily_rows.extend(rows)
                if symbol not in INDEX_SYMBOLS:
                    current_factor_fields, current_factor_rows = self._read(
                        ProviderEndpoint.ADJUST_FACTOR,
                        lambda symbol=symbol: self.client.query_adjust_factor(
                            symbol,
                            start_date="1990-01-01",
                            end_date=iso_date,
                        ),
                    )
                    if current_factor_rows:
                        factor_fields = current_factor_fields
                        factor_rows.extend(current_factor_rows)
            except BaoStockError as exc:
                if exc.failure is not None and not exc.failure.retryable:
                    raise
                batch_failure = batch_failure or exc.failure
                failed.append(symbol)

        bars = _normalize_provider_rows(
            fields=daily_fields,
            rows=daily_rows,
            factor_fields=factor_fields,
            factor_rows=factor_rows,
        )
        _require_unique_symbols([bar.symbol for bar in bars])
        return ProviderBatch(
            bars=bars,
            expected_symbols=expected_symbols,
            failed_symbols=failed,
            failure=batch_failure,
        )

    def _fetch_main_board(self, trade_date: date) -> ProviderBatch:
        iso_date = trade_date.isoformat()
        universe_fields, universe_rows = self._read(
            ProviderEndpoint.ALL_STOCK, lambda: self.client.query_all_stock(day=iso_date)
        )
        universe_code_index = _required_field_positions(universe_fields, ("code",))["code"]
        main_symbol_rows = [
            row[universe_code_index]
            for row in universe_rows
            if _is_main_board(row[universe_code_index])
        ]
        _require_unique_symbols(main_symbol_rows)
        main_symbols = set(main_symbol_rows)
        daily_fields, daily_rows = self._read(
            ProviderEndpoint.DAILY_ASTOCK,
            lambda: self.client.query_daily_history_k_AStock(date=iso_date),
        )
        daily_positions = _required_field_positions(daily_fields, ("code", "tradestatus"))
        daily_code_index = daily_positions["code"]
        daily_status_index = daily_positions["tradestatus"]
        daily_rows = [row for row in daily_rows if _is_main_board(row[daily_code_index])]
        daily_symbols = [row[daily_code_index] for row in daily_rows]
        _require_unique_symbols(daily_symbols)
        if set(daily_symbols) != main_symbols:
            raise _candidate_integrity_error("BaoStock candidate has universe divergence")
        active_symbols = sorted(
            row[daily_code_index] for row in daily_rows if row[daily_status_index] == "1"
        )
        factor_fields, factor_rows, factor_failure = self._main_board_factor_snapshot(
            trade_date,
            active_symbols,
        )
        explicit_indexes = self._fetch_symbols(trade_date, INDEX_SYMBOLS)
        bars = _normalize_provider_rows(
            fields=daily_fields,
            rows=daily_rows,
            factor_fields=factor_fields,
            factor_rows=factor_rows,
        )
        combined_bars = bars + explicit_indexes.bars
        _require_unique_symbols([bar.symbol for bar in combined_bars])
        return ProviderBatch(
            bars=combined_bars,
            expected_symbols=sorted(main_symbols) + list(INDEX_SYMBOLS),
            failed_symbols=sorted(
                (main_symbols - {bar.symbol for bar in bars}) | set(explicit_indexes.failed_symbols)
            ),
            failure=factor_failure or explicit_indexes.failure,
        )

    def _main_board_factor_snapshot(
        self,
        trade_date: date,
        main_symbols: Sequence[str],
    ) -> tuple[list[str], list[list[str]], MarketFailure | None]:
        """Resolve an exact factor for every stock without guessing a default.

        The BaoStock daily endpoint is an event feed, not a full-universe
        snapshot.  A persistent per-symbol bootstrap establishes the baseline;
        later sessions advance it from the daily event stream.
        """
        stream_through = self.factor_cache.stream_through()
        if stream_through is None or trade_date <= stream_through:
            sessions = [trade_date]
        else:
            sessions = self._trading_dates(
                stream_through + date.resolution,
                trade_date,
            )
        for session in sessions:
            fields, rows = self._read(
                ProviderEndpoint.DAILY_FACTOR,
                lambda session=session: self.client.query_daily_adjust_factor(
                    date=session.isoformat()
                ),
            )
            self.factor_cache.record_daily_events(
                session,
                fields,
                rows,
                advance_stream=(
                    stream_through is None
                    or (stream_through is not None and session > stream_through)
                ),
            )

        self.factor_cache.materialize_from_stream(main_symbols, trade_date)
        resolved = self.factor_cache.exact_snapshots(main_symbols, trade_date)
        missing = [symbol for symbol in main_symbols if symbol not in resolved]
        bootstrap_failure: MarketFailure | None = None
        for position, symbol in enumerate(missing, start=1):
            try:
                fields, rows = self._read(
                    ProviderEndpoint.ADJUST_FACTOR,
                    lambda symbol=symbol: self.client.query_adjust_factor(
                        symbol,
                        start_date="1990-01-01",
                        end_date=trade_date.isoformat(),
                    ),
                )
                factor = self.factor_cache.record_bootstrap(
                    symbol,
                    trade_date,
                    fields,
                    rows,
                    allow_empty_history=True,
                )
                if factor is not None:
                    resolved[symbol] = factor
            except BaoStockError as exc:
                # The quality gate below remains authoritative.  A single
                # failed bootstrap must not discard already persisted progress.
                if exc.failure is None or not exc.failure.retryable:
                    raise
                bootstrap_failure = bootstrap_failure or exc.failure
                continue
            except (FactorCacheError, ValueError):
                bootstrap_failure = bootstrap_failure or MarketFailure(
                    failure_stage="normalize",
                    failure_class="semantic",
                    retryable=False,
                )
                continue
            if position % 100 == 0 or position == len(missing):
                logger.info(
                    "factor_bootstrap_progress completed=%d total=%d",
                    position,
                    len(missing),
                )
        fields, rows = self.factor_cache.normalized_rows(main_symbols, trade_date)
        return fields, rows, bootstrap_failure
