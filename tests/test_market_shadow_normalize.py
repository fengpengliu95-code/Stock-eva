from datetime import date

import pytest

from backend.app.market.shadow_normalize import (
    ShadowNormalizationError,
    normalize_tickflow,
    normalize_tushare,
)


def _tushare(**overrides):
    row = {
        "ts_code": "000001.SZ",
        "trade_date": "20260820",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "pre_close": 10,
        "vol": 100,
        "amount": 20,
    }
    row.update(overrides)
    return {
        "daily": [row],
        "universe": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "indexes": [],
        "adj_factor": [{"ts_code": "000001.SZ", "trade_date": "20260820", "adj_factor": 1}],
        "suspend_d": [],
        "trade_cal": [{"cal_date": "20260820", "is_open": 1}],
        "stock_basic": [{"ts_code": "000001.SZ", "list_status": "L"}],
    }


def test_canonical_comparison_units_are_explicit():
    result = normalize_tushare(_tushare(), trade_date=date(2026, 8, 20), universe_id="u")
    assert result.rows[0].symbol == "sz.000001"
    assert result.rows[0].volume == 10000
    assert result.rows[0].amount == 20000
    assert result.rows[0].price_unit == "CNY"


def test_tushare_lots_and_thousand_cny_convert_once():
    result = normalize_tushare(_tushare(), trade_date=date(2026, 8, 20), universe_id="u")
    assert result.rows[0].volume == 100 * 100
    assert result.rows[0].amount == 20 * 1000


def test_tickflow_units_are_not_guessed():
    with pytest.raises(ShadowNormalizationError):
        normalize_tickflow(
            {
                "daily": [
                    {
                        "symbol": "sh.600000",
                        "date": "2026-08-20",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 1,
                        "amount": 1,
                    }
                ],
                "universe": [],
                "indexes": [],
            },
            trade_date=date(2026, 8, 20),
            universe_id="u",
        )


def test_no_row_suspension_requires_universe_state():
    payload = _tushare()
    payload["daily"] = []
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(payload, trade_date=date(2026, 8, 20), universe_id="u")


def test_nonfinite_or_impossible_ohlc_is_self_gate_failure():
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(_tushare(high=8), trade_date=date(2026, 8, 20), universe_id="u")
