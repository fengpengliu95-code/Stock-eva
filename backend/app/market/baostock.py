import logging
import socket
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from time import monotonic, sleep
from typing import Any

from backend.app.market.factor_cache import AdjustmentFactorCache, FactorCacheError
from backend.app.market.models import DailyBar
from backend.app.market.normalize import normalize_baostock_rows

DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
)
INDEX_SYMBOLS = ("sh.000001", "sz.399001")
SH_MAIN_PREFIXES = ("sh.600", "sh.601", "sh.603", "sh.605")
SZ_MAIN_PREFIXES = ("sz.000", "sz.001", "sz.002", "sz.003")
logger = logging.getLogger("stock_eva.market.baostock")
_LOGIN_SOCKET_TIMEOUT_LOCK = threading.Lock()


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


@dataclass(frozen=True)
class ProviderRangeBatch:
    bars: list[DailyBar]
    trading_dates: list[date]
    expected_symbols: list[str]
    failed_symbols: list[str]


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
    pass


def _read_result(
    result: Any,
    *,
    before_page_request: Callable[[], None] | None = None,
) -> tuple[list[str], list[list[str]]]:
    if result.error_code != "0":
        raise BaoStockError(result.error_msg or f"BaoStock error {result.error_code}")
    rows: list[list[str]] = []
    while True:
        if (
            before_page_request is not None
            and hasattr(result, "data")
            and hasattr(result, "cur_row_num")
            and hasattr(result, "per_page_count")
            and result.cur_row_num >= len(result.data)
            and len(result.data) == int(result.per_page_count)
        ):
            before_page_request()
        if not result.next():
            break
        rows.append(result.get_row_data())
    if result.error_code != "0":
        raise BaoStockError(result.error_msg or f"BaoStock pagination error {result.error_code}")
    return list(result.fields), rows


def _is_main_board(symbol: str) -> bool:
    return symbol.startswith(SH_MAIN_PREFIXES + SZ_MAIN_PREFIXES)


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

    def _login(self) -> None:
        last_error: BaoStockError | None = None
        for _attempt in range(self.max_attempts):
            try:
                result = self._call_login()
            except (TimeoutError, OSError):
                last_error = BaoStockError("BaoStock login transport failed")
                self._discard_session()
                continue
            if result.error_code != "0":
                last_error = BaoStockError(result.error_msg or "BaoStock login failed")
                self._discard_session()
                continue
            try:
                self._configure_socket_timeout()
            except (TimeoutError, OSError):
                last_error = BaoStockError("BaoStock socket timeout configuration failed")
                self._discard_session()
                continue
            self._session_usable = True
            return
        raise last_error or BaoStockError("BaoStock login failed")

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

    def _ensure_session(self) -> None:
        if not self._session_usable:
            self._login()

    def fetch(self, trade_date: date, symbols: Sequence[str] | None = None) -> ProviderBatch:
        if symbols is not None and len(set(symbols)) > self.max_explicit_symbols:
            raise BaoStockError(
                f"explicit refresh accepts at most {self.max_explicit_symbols} symbols"
            )
        self._login()
        try:
            self._ensure_trading_day(trade_date)
            if symbols is None:
                return self._fetch_main_board(trade_date)
            return self._fetch_symbols(trade_date, symbols)
        finally:
            self._logout()

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        if start_date > end_date:
            raise BaoStockError("start date must not be after end date")
        self._login()
        try:
            return self._trading_dates(start_date, end_date)
        finally:
            self._logout()

    def inspect_main_board(self, trade_date: date) -> MainBoardInspection:
        """Read only the calendar and security universe; never request OHLCV."""
        initial_request_count = self._provider_request_count
        self._login()
        try:
            self._ensure_trading_day(trade_date)
            fields, rows = self._read(
                lambda: self.client.query_all_stock(day=trade_date.isoformat())
            )
            code_index = fields.index("code")
            symbols = {row[code_index] for row in rows if _is_main_board(row[code_index])}
            shanghai = sum(symbol.startswith("sh.") for symbol in symbols)
            shenzhen = sum(symbol.startswith("sz.") for symbol in symbols)
            return MainBoardInspection(
                trade_date=trade_date,
                main_board_count=len(symbols),
                shanghai_count=shanghai,
                shenzhen_count=shenzhen,
                total_expected_count=len(symbols) + len(INDEX_SYMBOLS),
                metadata_provider_requests=(self._provider_request_count - initial_request_count),
                main_board_symbols=tuple(sorted(symbols)),
            )
        finally:
            self._logout()

    def _trading_dates(self, start_date: date, end_date: date) -> list[date]:
        fields, rows = self._read(
            lambda: self.client.query_trade_dates(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        )
        date_index = fields.index("calendar_date")
        trading_index = fields.index("is_trading_day")
        return [date.fromisoformat(row[date_index]) for row in rows if row[trading_index] == "1"]

    def fetch_range(
        self,
        start_date: date,
        end_date: date,
        *,
        symbols: Sequence[str],
    ) -> ProviderRangeBatch:
        expected_symbols = list(dict.fromkeys(symbols))
        if not expected_symbols:
            raise BaoStockError("historical backfill requires explicit symbols")
        if len(expected_symbols) > self.max_explicit_symbols:
            raise BaoStockError(
                f"explicit refresh accepts at most {self.max_explicit_symbols} symbols"
            )
        self._login()
        try:
            trading_dates = self._trading_dates(start_date, end_date)
            if not trading_dates:
                raise BaoStockError("date range contains no trading days")
            daily_fields = DAILY_FIELDS.split(",")
            daily_rows: list[list[str]] = []
            factor_fields: list[str] = []
            factor_rows: list[list[str]] = []
            failed: list[str] = []
            for symbol in expected_symbols:
                try:
                    fields, rows = self._read(
                        lambda symbol=symbol: self.client.query_history_k_data_plus(
                            symbol,
                            DAILY_FIELDS,
                            start_date=start_date.isoformat(),
                            end_date=end_date.isoformat(),
                            frequency="d",
                            adjustflag="3",
                        )
                    )
                    if fields != daily_fields or not rows:
                        failed.append(symbol)
                        continue
                    daily_rows.extend(rows)
                    if symbol not in INDEX_SYMBOLS:
                        current_fields, current_rows = self._read(
                            lambda symbol=symbol: self.client.query_adjust_factor(
                                symbol,
                                start_date="1990-01-01",
                                end_date=end_date.isoformat(),
                            )
                        )
                        if current_rows:
                            factor_fields = current_fields
                            factor_rows.extend(current_rows)
                except BaoStockError:
                    failed.append(symbol)
            return ProviderRangeBatch(
                bars=normalize_baostock_rows(
                    fields=daily_fields,
                    rows=daily_rows,
                    factor_fields=factor_fields,
                    factor_rows=factor_rows,
                ),
                trading_dates=trading_dates,
                expected_symbols=expected_symbols,
                failed_symbols=failed,
            )
        finally:
            self._logout()

    def _read(self, operation) -> tuple[list[str], list[list[str]]]:
        last_error: BaoStockError | None = None
        for _attempt in range(self.max_attempts):
            self._pace_request()
            self._ensure_session()
            try:
                return _read_result(
                    operation(),
                    before_page_request=self._pace_request,
                )
            except (BaoStockError, TimeoutError, OSError) as exc:
                last_error = (
                    exc
                    if isinstance(exc, BaoStockError)
                    else BaoStockError("BaoStock transport failed")
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
            lambda: self.client.query_trade_dates(
                start_date=trade_date.isoformat(),
                end_date=trade_date.isoformat(),
            )
        )
        trading_index = fields.index("is_trading_day")
        if len(rows) != 1 or rows[0][trading_index] != "1":
            raise BaoStockError(f"{trade_date.isoformat()} is not a trading day")

    def _fetch_symbols(self, trade_date: date, symbols: Sequence[str]) -> ProviderBatch:
        daily_fields = DAILY_FIELDS.split(",")
        daily_rows: list[list[str]] = []
        factor_fields: list[str] = []
        factor_rows: list[list[str]] = []
        failed: list[str] = []
        iso_date = trade_date.isoformat()

        expected_symbols = list(dict.fromkeys(symbols))
        for symbol in expected_symbols:
            try:
                fields, rows = self._read(
                    lambda symbol=symbol: self.client.query_history_k_data_plus(
                        symbol,
                        DAILY_FIELDS,
                        start_date=iso_date,
                        end_date=iso_date,
                        frequency="d",
                        adjustflag="3",
                    )
                )
                if fields != daily_fields or not rows:
                    failed.append(symbol)
                    continue
                daily_rows.extend(rows)
                if symbol not in INDEX_SYMBOLS:
                    current_factor_fields, current_factor_rows = self._read(
                        lambda symbol=symbol: self.client.query_adjust_factor(
                            symbol,
                            start_date="1990-01-01",
                            end_date=iso_date,
                        )
                    )
                    if current_factor_rows:
                        factor_fields = current_factor_fields
                        factor_rows.extend(current_factor_rows)
            except BaoStockError:
                failed.append(symbol)

        return ProviderBatch(
            bars=normalize_baostock_rows(
                fields=daily_fields,
                rows=daily_rows,
                factor_fields=factor_fields,
                factor_rows=factor_rows,
            ),
            expected_symbols=expected_symbols,
            failed_symbols=failed,
        )

    def _fetch_main_board(self, trade_date: date) -> ProviderBatch:
        iso_date = trade_date.isoformat()
        universe_fields, universe_rows = self._read(
            lambda: self.client.query_all_stock(day=iso_date)
        )
        universe_code_index = universe_fields.index("code")
        main_symbols = {
            row[universe_code_index]
            for row in universe_rows
            if _is_main_board(row[universe_code_index])
        }
        daily_fields, daily_rows = self._read(
            lambda: self.client.query_daily_history_k_AStock(date=iso_date)
        )
        daily_rows = [row for row in daily_rows if row[daily_fields.index("code")] in main_symbols]
        active_symbols = sorted(
            row[daily_fields.index("code")]
            for row in daily_rows
            if row[daily_fields.index("tradestatus")] == "1"
        )
        factor_fields, factor_rows = self._main_board_factor_snapshot(
            trade_date,
            active_symbols,
        )
        explicit_indexes = self._fetch_symbols(trade_date, INDEX_SYMBOLS)
        bars = normalize_baostock_rows(
            fields=daily_fields,
            rows=daily_rows,
            factor_fields=factor_fields,
            factor_rows=factor_rows,
        )
        return ProviderBatch(
            bars=bars + explicit_indexes.bars,
            expected_symbols=sorted(main_symbols) + list(INDEX_SYMBOLS),
            failed_symbols=sorted(
                (main_symbols - {bar.symbol for bar in bars}) | set(explicit_indexes.failed_symbols)
            ),
        )

    def _main_board_factor_snapshot(
        self,
        trade_date: date,
        main_symbols: Sequence[str],
    ) -> tuple[list[str], list[list[str]]]:
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
                lambda session=session: self.client.query_daily_adjust_factor(
                    date=session.isoformat()
                )
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
        for position, symbol in enumerate(missing, start=1):
            try:
                fields, rows = self._read(
                    lambda symbol=symbol: self.client.query_adjust_factor(
                        symbol,
                        start_date="1990-01-01",
                        end_date=trade_date.isoformat(),
                    )
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
            except (BaoStockError, FactorCacheError, ValueError):
                # The quality gate below remains authoritative.  A single
                # failed bootstrap must not discard already persisted progress.
                continue
            if position % 100 == 0 or position == len(missing):
                logger.info(
                    "factor_bootstrap_progress completed=%d total=%d",
                    position,
                    len(missing),
                )
        return self.factor_cache.normalized_rows(main_symbols, trade_date)
