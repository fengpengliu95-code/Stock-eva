from datetime import date

from backend.app.market.shadow_normalize import normalize_tushare
from backend.app.market.shadow_reconciliation import reconcile


def _candidate(close=10.5, universe="u"):
    payload = {
        "daily": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260820",
                "open": 10,
                "high": max(11, close),
                "low": 9,
                "close": close,
                "pre_close": 10,
                "vol": 100,
                "amount": 20,
            }
        ],
        "universe": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "indexes": [],
        "adj_factor": [{"ts_code": "000001.SZ", "trade_date": "20260820", "adj_factor": 1}],
        "suspend_d": [],
        "trade_cal": [{"cal_date": "20260820", "is_open": 1}],
        "stock_basic": [{"ts_code": "000001.SZ", "list_status": "L"}],
    }
    return normalize_tushare(payload, trade_date=date(2026, 8, 20), universe_id=universe)


def test_mixed_provider_symbols_are_rejected():
    left = _candidate()
    right = left.model_copy(update={"universe_id": "other"})
    report = reconcile(left, right)
    assert report.status == "unavailable"


def test_reconciliation_requires_same_date_universe_and_complete_candidates():
    report = reconcile(_candidate(), _candidate(universe="other"))
    assert report.status == "unavailable"


def test_factor_anchor_equivalence_uses_adjusted_returns():
    report = reconcile(_candidate(), _candidate(close=10.5001))
    assert report.status == "ready"


def test_material_mismatch_quarantines_shadow_only():
    report = reconcile(_candidate(), _candidate(close=20))
    assert report.status == "material_mismatch"
    assert report.quarantine_secondary is True
