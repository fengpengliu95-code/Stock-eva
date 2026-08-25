from datetime import date

import pytest

from backend.app.market.shadow_normalize import normalize_tushare
from backend.app.market.shadow_reconciliation import ReconciliationPolicy, reconcile

TRADE_DATE = date(2026, 8, 20)


def candidate(*, close=10.5, factor=1, previous_factor=1, universe="u", provider="tushare"):
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
        "adj_factor": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260820",
                "anchor_date": "2026-08-20",
                "adj_factor": factor,
                "prev_adj_factor": previous_factor,
                "factor_semantics": "multiplicative_back_adjust",
            }
        ],
        "suspend_d": [],
        "stock_basic": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "units": {
            "vol": "lots",
            "amount": "thousand_cny",
            "factor_semantics": "multiplicative_back_adjust",
            "factor_anchor": "trade_date",
            "factor_direction": "back_adjust",
        },
    }
    return normalize_tushare(
        payload, trade_date=TRADE_DATE, universe_id=universe, provider_id=provider
    )


def test_mixed_provider_symbols_are_rejected():
    left = candidate()
    values = left.model_dump(mode="json")
    values["rows"][0]["provider_id"] = "tickflow"
    values["normalized_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        left.__class__.model_validate(values)


def test_reconciliation_requires_same_date_universe_and_complete_candidates():
    report = reconcile(candidate(), candidate(universe="other"))
    assert report.status == "unavailable"
    incomplete = candidate().__class__.model_validate(
        {**candidate().model_dump(mode="json"), "complete": False, "normalized_sha256": "0" * 64}
    )
    assert reconcile(candidate(), incomplete).status == "unavailable"


def test_factor_anchor_equivalence_uses_adjusted_returns_and_ignores_raw_scale():
    report = reconcile(
        candidate(factor=1, previous_factor=1), candidate(factor=7, previous_factor=7)
    )
    assert report.status == "ready"
    assert report.mismatch_counts["adjusted_return"] == 0


def test_adjusted_return_over_five_basis_points_is_material():
    report = reconcile(candidate(), candidate(close=10.6))
    assert report.status == "material_mismatch"
    assert report.mismatch_counts["adjusted_return"] == 1


def test_material_mismatch_quarantines_shadow_only():
    report = reconcile(candidate(), candidate(close=10.6))
    assert report.quarantine_secondary is True
    assert report.report_sha256 == reconcile(candidate(), candidate(close=10.6)).report_sha256


def test_reconciliation_policy_is_frozen_and_caller_cannot_relax_thresholds():
    with pytest.raises(ValueError):
        ReconciliationPolicy(legal_tick=1.0)
    with pytest.raises(ValueError):
        reconcile(
            candidate(),
            candidate(close=10.6),
            policy=ReconciliationPolicy.model_construct(
                legal_tick=1.0,
                version="r2f3-v1",
                volume_relative_error=0.001,
                amount_relative_error=0.005,
                adjusted_return_basis_points=5.0,
                sample_limit=20,
            ),
        )
