import math
from collections.abc import Callable
from datetime import UTC, date, datetime

from backend.app.market.supplemental import (
    IndustryClassificationRecord,
    ReportedFundFlowPoint,
)

_CLASSIFICATION_ENDPOINT = (
    "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"
)
_MARKET_FLOW_ENDPOINT = "https://data.eastmoney.com/zjlx/dpzjlx.html"
_SECTOR_FLOW_ENDPOINT = "https://data.eastmoney.com/bkzj/"


class SupplementalDataError(ValueError):
    pass


class SupplementalSourceUnavailableError(RuntimeError):
    pass


class AKShareSupplementalProvider:
    """Optional normalizer. Construction and API GETs never perform network calls."""

    def __init__(
        self,
        *,
        client=None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def client(self):
        if self._client is None:
            try:
                import akshare
            except ImportError as exc:
                raise SupplementalDataError("AKShare optional dependency is not installed") from exc
            self._client = akshare
        return self._client

    def classification_history(
        self,
        *,
        through_date: date,
    ) -> list[IndustryClassificationRecord]:
        observed_at = self._clock()
        records = []
        table = self._source_call(self.client.stock_industry_clf_hist_sw)
        for row in _rows(table):
            effective_from = _explicit_date(row.get("start_date"))
            updated_on = _explicit_date(row.get("update_time"))
            if effective_from > through_date or updated_on > through_date:
                continue
            symbol = str(row.get("symbol", "")).strip().zfill(6)
            industry_code = str(row.get("industry_code", "")).strip()
            if not symbol.isdigit() or len(symbol) != 6 or not industry_code:
                raise SupplementalDataError("invalid classification identity")
            records.append(
                IndustryClassificationRecord(
                    source_symbol=symbol,
                    industry_code=industry_code,
                    industry_name=_optional_text(row.get("industry_name") or row.get("行业名称")),
                    effective_from=effective_from,
                    source_updated_on=updated_on,
                    source_endpoint=_CLASSIFICATION_ENDPOINT,
                    observed_at=observed_at,
                )
            )
        return sorted(
            records,
            key=lambda item: (
                item.source_symbol,
                item.effective_from,
                item.industry_code,
            ),
        )

    def market_fund_flow_history(
        self,
        *,
        through_date: date,
    ) -> list[ReportedFundFlowPoint]:
        return self._fund_flow_records(
            self._source_call(self.client.stock_market_fund_flow),
            scope="market",
            scope_name="沪深市场",
            through_date=through_date,
            source_endpoint=_MARKET_FLOW_ENDPOINT,
        )

    def sector_fund_flow_history(
        self,
        sector_name: str,
        *,
        through_date: date,
    ) -> list[ReportedFundFlowPoint]:
        normalized_name = sector_name.strip()
        if not normalized_name:
            raise SupplementalDataError("sector name is required")
        return self._fund_flow_records(
            self._source_call(
                lambda: self.client.stock_sector_fund_flow_hist(
                    symbol=normalized_name
                )
            ),
            scope="industry",
            scope_name=normalized_name,
            through_date=through_date,
            source_endpoint=_SECTOR_FLOW_ENDPOINT,
        )

    @staticmethod
    def _source_call(call):
        try:
            return call()
        except SupplementalDataError:
            raise
        except Exception as exc:
            raise SupplementalSourceUnavailableError(
                "supplemental source request unavailable"
            ) from exc

    def _fund_flow_records(
        self,
        table,
        *,
        scope: str,
        scope_name: str,
        through_date: date,
        source_endpoint: str,
    ) -> list[ReportedFundFlowPoint]:
        observed_at = self._clock()
        records = []
        for row in _rows(table):
            trade_date = _explicit_date(row.get("日期"))
            if trade_date > through_date:
                continue
            records.append(
                ReportedFundFlowPoint(
                    trade_date=trade_date,
                    scope=scope,
                    scope_name=scope_name,
                    reported_main_net_inflow=_required_float(row.get("主力净流入-净额")),
                    reported_main_net_inflow_ratio=_optional_float(row.get("主力净流入-净占比")),
                    reported_super_large_net_inflow=_optional_float(row.get("超大单净流入-净额")),
                    reported_super_large_net_inflow_ratio=_optional_float(
                        row.get("超大单净流入-净占比")
                    ),
                    reported_large_net_inflow=_optional_float(row.get("大单净流入-净额")),
                    reported_large_net_inflow_ratio=_optional_float(row.get("大单净流入-净占比")),
                    reported_medium_net_inflow=_optional_float(row.get("中单净流入-净额")),
                    reported_medium_net_inflow_ratio=_optional_float(row.get("中单净流入-净占比")),
                    reported_small_net_inflow=_optional_float(row.get("小单净流入-净额")),
                    reported_small_net_inflow_ratio=_optional_float(row.get("小单净流入-净占比")),
                    source_endpoint=source_endpoint,
                    observed_at=observed_at,
                )
            )
        return sorted(records, key=lambda item: item.trade_date)


def _rows(table) -> list[dict]:
    if isinstance(table, list):
        return table
    if hasattr(table, "to_dict"):
        return table.to_dict(orient="records")
    raise SupplementalDataError("unsupported AKShare response shape")


def _explicit_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            pass
    raise SupplementalDataError("explicit source date is required")


def _required_float(value) -> float:
    result = _optional_float(value)
    if result is None:
        raise SupplementalDataError("reported main net inflow is required")
    return result


def _optional_float(value) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SupplementalDataError("invalid reported fund-flow value") from exc
    if math.isnan(result):
        return None
    if not math.isfinite(result):
        raise SupplementalDataError("invalid reported fund-flow value")
    return result


def _optional_text(value) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None
