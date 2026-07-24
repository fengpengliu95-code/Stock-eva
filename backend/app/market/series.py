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
        bars = [
            bar
            for bar in self.store.symbol_bars(symbol, start, end, source="baostock")
            if not bar.is_suspended
        ]
        result: list[PriceSeriesPoint] = []
        for bar in bars:
            if adjustment == "qfq" and bar.adjust_factor is None:
                raise DataQualityError(
                    f"{bar.symbol} {bar.trade_date} missing adjust_factor"
                )
            factor = bar.adjust_factor if adjustment == "qfq" else 1.0
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
                    adjust_factor=bar.adjust_factor,
                    price_adjustment=adjustment,
                    source=bar.source,
                )
            )
        return result
