from datetime import date

import pytest

from backend.app.market.shadow_normalize import (
    ShadowNormalizationError,
    normalize_tickflow,
    normalize_tushare,
)

TRADE_DATE = date(2026, 8, 20)


def tushare_payload(
    *,
    units=True,
    factor_date="20260820",
    factor_semantics="multiplicative_back_adjust",
    include_factor=True,
):
    payload = {
        "daily": [
            {
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
        ],
        "universe": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "indexes": [{"ts_code": "000001.SZ"}],
        "suspend_d": [],
        "stock_basic": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "units": {
            "vol": "lots",
            "amount": "thousand_cny",
            "factor_semantics": factor_semantics,
            "factor_anchor": "trade_date",
            "factor_direction": "back_adjust",
        }
        if units
        else {},
    }
    if include_factor:
        payload["adj_factor"] = [
            {
                "ts_code": "000001.SZ",
                "trade_date": factor_date,
                "anchor_date": "2026-08-20",
                "adj_factor": 1,
                "prev_adj_factor": 1,
                "factor_semantics": factor_semantics,
            }
        ]
    else:
        payload["adj_factor"] = []
    return payload


def test_canonical_comparison_units_are_explicit():
    result = normalize_tushare(
        tushare_payload(), trade_date=TRADE_DATE, universe_id="u", required_indexes=("sz.000001",)
    )
    row = result.rows[0]
    assert (row.price_unit, row.volume_unit, row.amount_unit) == ("CNY", "shares", "CNY")
    assert result.factor_semantics == "multiplicative_back_adjust"


def test_tushare_lots_and_thousand_cny_convert_once():
    row = normalize_tushare(tushare_payload(), trade_date=TRADE_DATE, universe_id="u").rows[0]
    assert row.volume == 10_000
    assert row.amount == 20_000
    assert row.volume != 1_000_000 and row.amount != 20_000_000


@pytest.mark.parametrize(
    "payload",
    [
        tushare_payload(units=False),
        {
            **tushare_payload(),
            "units": {
                "vol": "shares",
                "amount": "CNY",
                "factor_semantics": "x",
                "factor_anchor": "trade_date",
                "factor_direction": "back_adjust",
            },
        },
    ],
)
def test_tushare_requires_nonempty_exact_typed_source_units(payload):
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(payload, trade_date=TRADE_DATE, universe_id="u")


def test_tickflow_units_are_not_guessed():
    payload = {
        "daily": [
            {
                "symbol": "sh.600000",
                "date": "2026-08-20",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "preclose": 1,
                "volume": 0,
                "amount": 0,
            }
        ],
        "universe": [{"symbol": "sh.600000", "list_status": "L"}],
        "indexes": [],
    }
    with pytest.raises(ShadowNormalizationError):
        normalize_tickflow(payload, trade_date=TRADE_DATE, universe_id="u")


def test_tickflow_unknown_factor_suspension_units_are_incomplete_not_complete():
    payload = {
        "daily": [
            {
                "symbol": "sh.600000",
                "date": "2026-08-20",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "preclose": 1,
                "volume": 0,
                "amount": 0,
            }
        ],
        "universe": [{"symbol": "sh.600000", "list_status": "L"}],
        "indexes": [],
        "units": {"volume": "shares", "amount": "CNY"},
    }
    result = normalize_tickflow(payload, trade_date=TRADE_DATE, universe_id="u")
    assert result.complete is False and result.quality_status == "unavailable"
    assert result.rows[0].suspension is False


def test_no_row_suspension_requires_explicit_universe_suspension_evidence():
    payload = tushare_payload()
    payload["daily"] = []
    payload["universe"] = [{"ts_code": "000001.SZ", "list_status": "L"}]
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(payload, trade_date=TRADE_DATE, universe_id="u")
    payload["suspend_d"] = [{"ts_code": "000001.SZ", "trade_date": "20260820", "suspend_type": "S"}]
    result = normalize_tushare(payload, trade_date=TRADE_DATE, universe_id="u")
    assert result.complete is True and result.suspended_symbols == ("sz.000001",)


def test_factor_row_trade_date_must_equal_requested_date_and_anchor_semantics_present():
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(
            tushare_payload(factor_date="20260821"), trade_date=TRADE_DATE, universe_id="u"
        )
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(
            tushare_payload(factor_semantics=""), trade_date=TRADE_DATE, universe_id="u"
        )


def test_nonfinite_or_impossible_ohlc_is_self_gate_failure():
    payload = tushare_payload()
    payload["daily"][0]["high"] = 8
    with pytest.raises(ShadowNormalizationError):
        normalize_tushare(payload, trade_date=TRADE_DATE, universe_id="u")


def test_shadow_provider_identity_is_static_and_rows_cannot_mix():
    result = normalize_tushare(tushare_payload(), trade_date=TRADE_DATE, universe_id="u")
    with pytest.raises(ValueError):
        result.__class__.model_validate(
            {**result.model_dump(mode="json"), "provider_id": "arbitrary"}
        )
