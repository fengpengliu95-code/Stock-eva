from datetime import date
from typing import Literal

from backend.app.market.models import PriceSeriesPoint
from backend.app.market.store import MarketStore


class DataQualityError(ValueError):
    pass


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
        all_bars = self.store.symbol_bars(symbol, start, end, source="baostock")
        bars = [bar for bar in all_bars if not bar.is_suspended]
        target_factor = None
        if adjustment == "qfq":
            if not all_bars or all_bars[-1].adjust_factor is None:
                target = all_bars[-1] if all_bars else None
                detail = (
                    f"{target.symbol} {target.trade_date}"
                    if target is not None
                    else f"{symbol} {end}"
                )
                raise DataQualityError(f"{detail} missing adjust_factor")
            target_factor = float(all_bars[-1].adjust_factor)
        result: list[PriceSeriesPoint] = []
        for bar in bars:
            if adjustment == "qfq" and bar.adjust_factor is None:
                raise DataQualityError(f"{bar.symbol} {bar.trade_date} missing adjust_factor")
            factor = float(bar.adjust_factor) / target_factor if adjustment == "qfq" else 1.0
            result.append(
                PriceSeriesPoint(
                    trade_date=bar.trade_date,
                    symbol=bar.symbol,
                    open=bar.open * factor,
                    high=bar.high * factor,
                    low=bar.low * factor,
                    close=bar.close * factor,
                    preclose=bar.preclose * factor,
                    volume=bar.volume,
                    amount=bar.amount,
                    adjust_factor=factor if adjustment == "qfq" else bar.adjust_factor,
                    price_adjustment=adjustment,
                    source=bar.source,
                )
            )
        return result
