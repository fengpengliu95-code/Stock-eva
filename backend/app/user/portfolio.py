from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from backend.app.market.store import MarketStore
from backend.app.user.models import (
    PortfolioValuation,
    PositionValuation,
    ValuationCoverage,
)
from backend.app.user.store import UserStore

MONEY = Decimal("0.01")
VALUATION_BASIS = "按用户成本与当日收盘估算、未计交易费用"


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


class PortfolioValuationService:
    def __init__(self, user_store: UserStore, market_store: MarketStore) -> None:
        self.user_store = user_store
        self.market_store = market_store

    def latest(self, expected_session: date | None = None) -> PortfolioValuation:
        positions = self.user_store.list_positions()
        if not positions:
            return self._empty()
        if not self.market_store.exists():
            return self._market_error(positions, "market_data_missing")
        refresh = self.market_store.published_refresh() or self.market_store.latest_refresh()
        if refresh is None or refresh.status == "error":
            return self._market_error(positions, "market_refresh_error")

        bars = {
            bar.symbol: bar
            for bar in self.market_store.canonical_bars(
                refresh.requested_date,
                source=refresh.source,
            )
        }
        missing: list[str] = []
        suspended: list[str] = []
        items: list[PositionValuation] = []
        market_value_total = Decimal("0")
        cost_total = Decimal("0")
        position_after_market = False

        for position in positions:
            bar = bars.get(position.symbol)
            if position.as_of_date > refresh.requested_date:
                position_after_market = True
                missing.append(position.symbol)
                items.append(self._unvalued(position, "missing"))
                continue
            if bar is None:
                missing.append(position.symbol)
                items.append(self._unvalued(position, "missing"))
                continue
            if bar.is_suspended:
                suspended.append(position.symbol)
                items.append(self._unvalued(position, "suspended"))
                continue
            close = Decimal(str(bar.close))
            market_value = _money(position.quantity * close)
            cost_basis = _money(position.quantity * position.avg_cost)
            market_value_total += market_value
            cost_total += cost_basis
            items.append(
                PositionValuation(
                    position_id=position.id,
                    symbol=position.symbol,
                    quantity=position.quantity,
                    avg_cost=position.avg_cost,
                    today_buy_qty=position.today_buy_qty,
                    status="valued",
                    close=close,
                    market_value=market_value,
                    cost_basis=cost_basis,
                    unrealized_pnl=_money(market_value - cost_basis),
                )
            )

        covered = len(positions) - len(missing) - len(suspended)
        issues: list[str] = []
        status = "ready"
        if missing:
            issues.append("missing_close")
        if suspended:
            issues.append("suspended_not_valued")
        if position_after_market:
            issues.append("position_after_market_date")
        if missing or suspended or refresh.status == "partial":
            status = "partial"
        if expected_session is None:
            issues.append("calendar_unavailable")
            status = "partial"
        elif refresh.requested_date < expected_session:
            issues.append("data_older_than_expected")
            status = "stale"

        return PortfolioValuation(
            status=status,
            data_date=refresh.requested_date,
            source=refresh.source,
            market_status=refresh.status,
            coverage=ValuationCoverage(
                covered=covered,
                total=len(positions),
                missing_symbols=missing,
                suspended_symbols=suspended,
            ),
            items=items,
            covered_market_value=_money(market_value_total) if covered else None,
            covered_cost_basis=_money(cost_total) if covered else None,
            covered_unrealized_pnl=(_money(market_value_total - cost_total) if covered else None),
            valuation_basis=VALUATION_BASIS,
            quality_issues=issues,
        )

    @staticmethod
    def _unvalued(position, status: str) -> PositionValuation:
        return PositionValuation(
            position_id=position.id,
            symbol=position.symbol,
            quantity=position.quantity,
            avg_cost=position.avg_cost,
            today_buy_qty=position.today_buy_qty,
            status=status,
        )

    @staticmethod
    def _empty() -> PortfolioValuation:
        return PortfolioValuation(
            status="empty",
            data_date=None,
            source=None,
            market_status=None,
            coverage=ValuationCoverage(
                covered=0,
                total=0,
                missing_symbols=[],
                suspended_symbols=[],
            ),
            items=[],
            covered_market_value=None,
            covered_cost_basis=None,
            covered_unrealized_pnl=None,
            valuation_basis=VALUATION_BASIS,
            quality_issues=["no_positions"],
        )

    @staticmethod
    def _market_error(positions, issue: str) -> PortfolioValuation:
        return PortfolioValuation(
            status="error",
            data_date=None,
            source=None,
            market_status="error",
            coverage=ValuationCoverage(
                covered=0,
                total=len(positions),
                missing_symbols=[position.symbol for position in positions],
                suspended_symbols=[],
            ),
            items=[
                PortfolioValuationService._unvalued(position, "missing") for position in positions
            ],
            covered_market_value=None,
            covered_cost_basis=None,
            covered_unrealized_pnl=None,
            valuation_basis=VALUATION_BASIS,
            quality_issues=[issue],
        )
