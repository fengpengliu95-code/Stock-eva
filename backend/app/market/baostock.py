from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from time import sleep
from typing import Any

from backend.app.market.models import DailyBar
from backend.app.market.normalize import normalize_baostock_rows

DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,"
    "adjustflag,turn,tradestatus,pctChg,isST"
)
INDEX_SYMBOLS = ("sh.000001", "sz.399001")
SH_MAIN_PREFIXES = ("sh.600", "sh.601", "sh.603", "sh.605")
SZ_MAIN_PREFIXES = ("sz.000", "sz.001", "sz.002", "sz.003")


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


class BaoStockError(RuntimeError):
    pass


def _read_result(result: Any) -> tuple[list[str], list[list[str]]]:
    if result.error_code != "0":
        raise BaoStockError(result.error_msg or f"BaoStock error {result.error_code}")
    rows: list[list[str]] = []
    while result.next():
        rows.append(result.get_row_data())
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
    ) -> None:
        is_real_client = client is None
        if client is None:
            import baostock as client_module

            client = client_module
        self.client = client
        self.max_explicit_symbols = max_explicit_symbols
        self.max_attempts = max_attempts
        self.min_request_interval_seconds = (
            0.2 if min_request_interval_seconds is None and is_real_client else 0.0
        )

    def _login(self) -> None:
        result = self.client.login()
        if result.error_code != "0":
            raise BaoStockError(result.error_msg or "BaoStock login failed")

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
            self.client.logout()

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        if start_date > end_date:
            raise BaoStockError("start date must not be after end date")
        self._login()
        try:
            return self._trading_dates(start_date, end_date)
        finally:
            self.client.logout()

    def _trading_dates(self, start_date: date, end_date: date) -> list[date]:
        fields, rows = self._read(
            lambda: self.client.query_trade_dates(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        )
        date_index = fields.index("calendar_date")
        trading_index = fields.index("is_trading_day")
        return [
            date.fromisoformat(row[date_index])
            for row in rows
            if row[trading_index] == "1"
        ]

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
            for index, symbol in enumerate(expected_symbols):
                if index:
                    sleep(self.min_request_interval_seconds)
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
            self.client.logout()

    def _read(self, operation) -> tuple[list[str], list[list[str]]]:
        last_error: BaoStockError | None = None
        for attempt in range(self.max_attempts):
            try:
                return _read_result(operation())
            except BaoStockError as exc:
                last_error = exc
                if attempt + 1 < self.max_attempts:
                    sleep(self.min_request_interval_seconds)
        raise last_error or BaoStockError("BaoStock request failed")

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
        for index, symbol in enumerate(expected_symbols):
            if index:
                sleep(self.min_request_interval_seconds)
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
        daily_rows = [
            row for row in daily_rows if row[daily_fields.index("code")] in main_symbols
        ]
        factor_fields, factor_rows = self._read(
            lambda: self.client.query_daily_adjust_factor(date=iso_date)
        )
        factor_rows = [
            row for row in factor_rows if row[factor_fields.index("code")] in main_symbols
        ]
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
                (main_symbols - {bar.symbol for bar in bars})
                | set(explicit_indexes.failed_symbols)
            ),
        )
