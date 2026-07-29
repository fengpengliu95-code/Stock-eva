from datetime import date

from backend.app.analysis.indicators import FORMULA_VERSION, calculate_indicators
from backend.app.analysis.models import SecurityAnalysisResponse, TechnicalAnalysisPoint
from backend.app.market.series import PriceSeriesService
from backend.app.market.store import MarketStore


class SecurityAnalysisService:
    def __init__(self, store: MarketStore) -> None:
        self.store = store

    def read(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> SecurityAnalysisResponse:
        prices = PriceSeriesService(self.store).read(
            symbol,
            start=start,
            end=end,
            adjustment="qfq",
        )
        if not prices:
            return SecurityAnalysisResponse(
                symbol=symbol,
                status="empty",
                as_of=None,
                formula_version=FORMULA_VERSION,
                quality_issues=["no_market_data"],
            )

        indicators = calculate_indicators([point.close for point in prices])
        series = [
            TechnicalAnalysisPoint(
                trade_date=point.trade_date,
                open=point.open,
                high=point.high,
                low=point.low,
                close=point.close,
                volume=point.volume,
                amount=point.amount,
                **{
                    field: values[index]
                    for field, values in indicators.items()
                },
            )
            for index, point in enumerate(prices)
        ]
        return SecurityAnalysisResponse(
            symbol=symbol,
            status="ready",
            as_of=series[-1].trade_date,
            formula_version=FORMULA_VERSION,
            series=series,
        )
