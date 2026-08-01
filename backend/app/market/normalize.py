from collections.abc import Sequence
from datetime import UTC, date, datetime

from backend.app.market.models import DailyBar
from backend.app.security_identity import derive_security_identity

DAILY_INDEX_SYMBOLS = {
    "sh.000001",
    "sz.399001",
    "sh.000016",
    "sh.000300",
    "sh.000852",
    "sh.000905",
    "sz.399006",
}
# Compatibility for existing normalize callers. BaoStock's all-main-board provider keeps
# its own two-symbol summary-index set, so this broader normalization set does not widen it.
INDEX_SYMBOLS = DAILY_INDEX_SYMBOLS


def _record(fields: Sequence[str], row: Sequence[str]) -> dict[str, str]:
    if len(fields) != len(row):
        raise ValueError("BaoStock field and row lengths differ")
    return dict(zip(fields, row, strict=True))


def _number(value: str) -> float | None:
    return None if value == "" else float(value)


def _required_activity_number(value: str, *, suspended: bool, field: str) -> float:
    if value != "":
        return float(value)
    if suspended:
        return 0.0
    raise ValueError(f"BaoStock active daily bar has empty {field}")


def _factor_lookup(
    fields: Sequence[str],
    rows: Sequence[Sequence[str]],
) -> dict[str, list[tuple[date, float]]]:
    result: dict[str, list[tuple[date, float]]] = {}
    for row in rows:
        record = _record(fields, row)
        result.setdefault(record["code"], []).append(
            (
                date.fromisoformat(record["dividOperateDate"]),
                float(record["backAdjustFactor"]),
            )
        )
    return result


def _factor_on(
    factors: dict[str, list[tuple[date, float]]],
    symbol: str,
    trade_date: date,
) -> float | None:
    eligible = [item for item in factors.get(symbol, []) if item[0] <= trade_date]
    return max(eligible, default=(trade_date, None), key=lambda item: item[0])[1]


def _board_for(symbol: str, security_type: str) -> str:
    source_type = "2" if security_type == "index" else "1"
    return derive_security_identity(symbol, source_type).board


def normalize_baostock_rows(
    *,
    fields: Sequence[str],
    rows: Sequence[Sequence[str]],
    factor_fields: Sequence[str] = (),
    factor_rows: Sequence[Sequence[str]] = (),
    ingested_at: datetime | None = None,
) -> list[DailyBar]:
    """Map one BaoStock response to canonical, unadjusted daily bars."""
    timestamp = ingested_at or datetime.now(UTC)
    factors = _factor_lookup(factor_fields, factor_rows) if factor_rows else {}
    normalized: list[DailyBar] = []

    for row in rows:
        record = _record(fields, row)
        if record["adjustflag"] != "3":
            raise ValueError("canonical daily bars require BaoStock adjustflag=3")

        trade_date = date.fromisoformat(record["date"])
        symbol = record["code"].lower()
        security_type = "index" if symbol in DAILY_INDEX_SYMBOLS else "stock"
        suspended = record["tradestatus"] != "1"
        factor = None if security_type == "index" else _factor_on(factors, symbol, trade_date)
        issues: list[str] = []
        if suspended:
            issues.append("suspended_placeholder")
        if security_type == "stock" and factor is None:
            issues.append("missing_adjust_factor")

        normalized.append(
            DailyBar(
                trade_date=trade_date,
                symbol=symbol,
                security_type=security_type,
                exchange=symbol[:2],
                board=_board_for(symbol, security_type),
                open=float(record["open"]),
                high=float(record["high"]),
                low=float(record["low"]),
                close=float(record["close"]),
                preclose=float(record["preclose"]),
                volume=_required_activity_number(
                    record["volume"],
                    suspended=suspended,
                    field="volume",
                ),
                amount=_required_activity_number(
                    record["amount"],
                    suspended=suspended,
                    field="amount",
                ),
                turnover_rate=_number(record["turn"]),
                pct_change=_number(record["pctChg"]),
                adjust_factor=factor,
                is_trading=not suspended,
                is_suspended=suspended,
                is_st=record["isST"] == "1",
                source_record_id=f"baostock:{symbol}:{trade_date.isoformat()}:none",
                ingested_at=timestamp,
                quality_status="partial" if issues else "ready",
                quality_issues=issues,
            )
        )
    return normalized
