import math
from dataclasses import dataclass
from datetime import date
from typing import Literal

from backend.app.market.models import DailyBar, PriceSeriesPoint
from backend.app.market.store import MarketStore


class DataQualityError(ValueError):
    pass


class EmptyPriceSeriesError(DataQualityError):
    reason_code: Literal["no_market_data"] = "no_market_data"

    def __init__(self, symbol: str, as_of: date) -> None:
        super().__init__(f"{symbol} {as_of} no market data")


EmptyReason = Literal["no_market_data", "no_effective_trading_data"]


@dataclass(frozen=True)
class PriceSeriesReadResult:
    points: list[PriceSeriesPoint]
    empty_reason: EmptyReason | None = None


_PRICE_FIELDS = ("open", "high", "low", "close", "preclose")
_ACTIVITY_FIELDS = ("volume", "amount")


def _validate_effective_bar(bar: DailyBar, *, adjustment: Literal["none", "qfq"]) -> None:
    if bar.quality_status != "ready" or bar.quality_issues:
        issues = ",".join(bar.quality_issues) or "none"
        raise DataQualityError(
            f"{bar.symbol} {bar.trade_date} quality not ready: "
            f"status={bar.quality_status}; issues={issues}"
        )
    for field in (*_PRICE_FIELDS, *_ACTIVITY_FIELDS):
        value = getattr(bar, field)
        invalid = not math.isfinite(value)
        if field in _PRICE_FIELDS:
            invalid = invalid or value <= 0
        else:
            invalid = invalid or value < 0
        if invalid:
            raise DataQualityError(
                f"{bar.symbol} {bar.trade_date} invalid {field}={value!r}"
            )
    if adjustment == "qfq":
        if bar.adjust_factor is None:
            raise DataQualityError(f"{bar.symbol} {bar.trade_date} missing adjust_factor")
        if not math.isfinite(bar.adjust_factor) or bar.adjust_factor <= 0:
            raise DataQualityError(
                f"{bar.symbol} {bar.trade_date} invalid adjust_factor={bar.adjust_factor!r}"
            )


class PriceSeriesService:
    def __init__(self, store: MarketStore) -> None:
        self.store = store

    def read(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
        adjustment: Literal["none", "qfq"] = "qfq",
    ) -> list[PriceSeriesPoint]:
        result = self.read_result(
            symbol,
            start=start,
            end=end,
            adjustment=adjustment,
        )
        if adjustment == "qfq" and result.empty_reason == "no_market_data":
            raise EmptyPriceSeriesError(symbol, end)
        return result.points

    def read_result(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
        adjustment: Literal["none", "qfq"] = "qfq",
    ) -> PriceSeriesReadResult:
        all_bars = self.store.symbol_bars(symbol, start, end, source="baostock")
        if not all_bars:
            return PriceSeriesReadResult(points=[], empty_reason="no_market_data")
        bars = [bar for bar in all_bars if not bar.is_suspended]
        if not bars:
            return PriceSeriesReadResult(
                points=[],
                empty_reason="no_effective_trading_data",
            )
        for bar in bars:
            _validate_effective_bar(bar, adjustment=adjustment)
        target_factor = 1.0
        if adjustment == "qfq":
            assert bars[-1].adjust_factor is not None
            target_factor = float(bars[-1].adjust_factor)
        result: list[PriceSeriesPoint] = []
        for bar in bars:
            factor = 1.0
            if adjustment == "qfq":
                assert bar.adjust_factor is not None
                factor = float(bar.adjust_factor) / target_factor
            if not math.isfinite(factor) or factor <= 0:
                raise DataQualityError(
                    f"{bar.symbol} {bar.trade_date} invalid qfq multiplier={factor!r}"
                )
            adjusted_prices = {
                field: getattr(bar, field) * factor
                for field in _PRICE_FIELDS
            }
            for field, value in adjusted_prices.items():
                if not math.isfinite(value) or value <= 0:
                    raise DataQualityError(
                        f"{bar.symbol} {bar.trade_date} invalid qfq {field}={value!r}"
                    )
            result.append(
                PriceSeriesPoint(
                    trade_date=bar.trade_date,
                    symbol=bar.symbol,
                    open=adjusted_prices["open"],
                    high=adjusted_prices["high"],
                    low=adjusted_prices["low"],
                    close=adjusted_prices["close"],
                    preclose=adjusted_prices["preclose"],
                    volume=bar.volume,
                    amount=bar.amount,
                    adjust_factor=factor if adjustment == "qfq" else bar.adjust_factor,
                    price_adjustment=adjustment,
                    source=bar.source,
                )
            )
        return PriceSeriesReadResult(points=result)
