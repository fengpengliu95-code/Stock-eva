import json
from datetime import date

import pytest

from backend.app.market.shadow_normalize import (
    REVIEWED_TUSHARE_UNIT_CONTRACT,
    normalize_tushare,
)
from backend.app.market.shadow_reconciliation import (
    CanonicalSessionCandidate,
    CanonicalSessionCandidateReader,
    CanonicalSessionRow,
    ReconciliationPolicy,
    ShadowCanonicalUnavailable,
    reconcile,
)
from tests.test_market_shadow_jobs import _write_canonical_dataset

TRADE_DATE = date(2026, 8, 20)


def candidate(*, close=10.5, factor=1, previous_factor=1, universe="u"):
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
        payload,
        trade_date=TRADE_DATE,
        universe_id=universe,
        contract=REVIEWED_TUSHARE_UNIT_CONTRACT,
    )


def canonical_side(value):
    """Adapt the legacy comparison-unit fixture to the strict canonical side."""
    rows = tuple(
        CanonicalSessionRow.model_validate(
            {
                **row.model_dump(mode="json"),
                "provider_id": "baostock",
                "source": "baostock",
            }
        )
        for row in value.rows
    )
    return CanonicalSessionCandidate(
        trade_date=value.trade_date,
        universe_id=value.universe_id,
        rows=rows,
        expected_symbols=value.expected_symbols,
        index_symbols=value.index_symbols,
        complete=value.complete,
        quality_status=value.quality_status,
        factor_semantics=value.factor_semantics,
        factor_anchor=value.factor_anchor or "trade_date",
        factor_direction=value.factor_direction or "back_adjust",
        suspended_symbols=value.suspended_symbols,
        not_listed_symbols=value.not_listed_symbols,
        canonical_manifest_generation="legacy-unit-fixture",
        canonical_manifest_sha256="a" * 64,
        candidate_sha256=value.normalized_sha256,
    )


def compare(left, right, **kwargs):
    return reconcile(canonical_side(left), right, **kwargs)


def test_mixed_provider_symbols_are_rejected():
    left = candidate()
    values = left.model_dump(mode="json")
    values["rows"][0]["provider_id"] = "tickflow"
    values["normalized_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        left.__class__.model_validate(values)


def test_reconciliation_requires_same_date_universe_and_complete_candidates():
    report = compare(candidate(), candidate(universe="other"))
    assert report.status == "unavailable"
    incomplete = candidate().__class__.model_validate(
        {
            **candidate().model_dump(mode="json"),
            "complete": False,
            "unavailable_reason": "INCOMPLETE",
            "normalized_sha256": "0" * 64,
        }
    )
    assert compare(candidate(), incomplete).status == "unavailable"


def test_factor_anchor_equivalence_uses_adjusted_returns_and_ignores_raw_scale():
    report = compare(candidate(factor=1, previous_factor=1), candidate(factor=7, previous_factor=7))
    assert report.status == "ready"
    assert report.mismatch_counts["adjusted_return"] == 0


def test_factor_anchor_labels_may_differ_when_relative_returns_are_equivalent():
    right_values = candidate().model_dump(mode="json")
    right_values["factor_anchor"] = "calendar_anchor"
    right_values["normalized_sha256"] = "0" * 64
    right = candidate().__class__.model_validate(right_values)
    assert compare(candidate(), right).status == "ready"


def test_missing_adjacent_factor_is_unavailable_not_a_zero_return():
    values = candidate().model_dump(mode="json")
    values["rows"][0]["factor_previous"] = None
    values["normalized_sha256"] = "0" * 64
    incomplete = candidate().__class__.model_validate(values)
    assert compare(candidate(), incomplete).status == "unavailable"


def test_suspended_and_not_listed_sets_are_compared_as_material_status():
    base = candidate().model_dump(mode="json")
    base["expected_symbols"] = ["sz.000001", "sz.000002"]
    base["suspended_symbols"] = ["sz.000002"]
    base["normalized_sha256"] = "0" * 64
    suspended = candidate().__class__.model_validate(base)
    base["suspended_symbols"] = []
    base["not_listed_symbols"] = ["sz.000002"]
    base["normalized_sha256"] = "0" * 64
    not_listed = candidate().__class__.model_validate(base)
    assert compare(suspended, not_listed).status == "material_mismatch"


def test_adjusted_return_over_five_basis_points_is_material():
    report = compare(candidate(), candidate(close=10.6))
    assert report.status == "material_mismatch"
    assert report.mismatch_counts["adjusted_return"] == 1


def test_material_mismatch_quarantines_shadow_only():
    report = compare(candidate(), candidate(close=10.6))
    assert report.quarantine_secondary is True
    assert report.report_sha256 == compare(candidate(), candidate(close=10.6)).report_sha256


def test_reconciliation_policy_is_frozen_and_caller_cannot_relax_thresholds():
    with pytest.raises(ValueError):
        ReconciliationPolicy(legal_tick=1.0)
    with pytest.raises(ValueError):
        compare(
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


def test_real_canonical_baostock_reader_is_descriptor_bound_and_not_self_comparable(tmp_path):
    root = tmp_path / "dataset"
    _write_canonical_dataset(root)
    reader = CanonicalSessionCandidateReader(root, trade_date=date(2026, 1, 2))
    canonical = reader.read()
    assert canonical.provider_id == "baostock"
    assert canonical.trade_date == date(2026, 1, 2)
    assert canonical.rows[0].source == "baostock"
    with pytest.raises(ValueError, match="one canonical and one shadow"):
        reconcile(canonical, canonical)

    row = canonical.rows[0]
    secondary = normalize_tushare(
        {
            "daily": [
                {
                    "ts_code": "000001.SH",
                    "trade_date": "20260102",
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "pre_close": row.preclose,
                    "vol": row.volume / 100,
                    "amount": row.amount / 1000,
                }
            ],
            "universe": [{"ts_code": "000001.SH", "list_status": "L"}],
            "indexes": [],
            "adj_factor": [
                {
                    "ts_code": "000001.SH",
                    "trade_date": "20260102",
                    "anchor_date": "2026-01-02",
                    "adj_factor": row.factor,
                    "prev_adj_factor": row.factor_previous,
                    "factor_semantics": "multiplicative_back_adjust",
                }
            ],
            "suspend_d": [],
            "stock_basic": [{"ts_code": "000001.SH", "list_status": "L"}],
            "units": {
                "vol": "lots",
                "amount": "thousand_cny",
                "factor_semantics": "multiplicative_back_adjust",
                "factor_anchor": "trade_date",
                "factor_direction": "back_adjust",
            },
        },
        trade_date=date(2026, 1, 2),
        universe_id="u1",
        contract=REVIEWED_TUSHARE_UNIT_CONTRACT,
    )
    with pytest.raises(ValueError, match="one canonical and one shadow"):
        reconcile(secondary, secondary)
    assert reconcile(canonical, secondary).status == "ready"
    assert (
        reconcile(
            canonical,
            secondary.model_copy(
                update={"rows": (secondary.rows[0].model_copy(update={"close": row.close + 1}),)}
            ),
        ).status
        == "material_mismatch"
    )

    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["generation"] = "g2"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ShadowCanonicalUnavailable):
        reader.read()
