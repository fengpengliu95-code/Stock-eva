from datetime import date

import pytest

from backend.app.market.shadow_candidates import (
    ShadowCandidateReader,
    ShadowCandidateStore,
    ShadowCandidateUnavailable,
)
from backend.app.market.shadow_evidence import ShadowEvidenceBundle
from backend.app.market.shadow_normalize import normalize_tushare


class EvidenceFixture:
    def __init__(self, *, provider="tushare", universe="u", evidence_id="ev-1"):
        self.evidence_id = evidence_id
        self.manifest_sha256 = "a" * 64
        self.provider = provider
        self.universe = universe

    def read_descriptor(self, evidence_id):
        assert evidence_id == self.evidence_id
        bundle = ShadowEvidenceBundle(
            evidence_id=evidence_id,
            completion_sha256="b" * 64,
            plan_sha256="c" * 64,
            rows=(),
            page_refs=(),
            manifest_sha256=self.manifest_sha256,
        )
        return bundle, {
            "provider_id": self.provider,
            "universe_id": self.universe,
            "trade_date": "2026-08-20",
            "plan_requests": [{"trade_date": "20260820"}],
        }

    def read(self, evidence_id):
        return self.read_descriptor(evidence_id)[0]


def candidate(complete=True):
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
        "indexes": [],
        "adj_factor": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260820",
                "anchor_date": "2026-08-20",
                "adj_factor": 1,
                "prev_adj_factor": 1,
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
    value = normalize_tushare(payload, trade_date=date(2026, 8, 20), universe_id="u")
    return value.__class__.model_validate(
        {**value.model_dump(mode="json"), "complete": complete, "normalized_sha256": "0" * 64}
    )


def test_shadow_quality_report_and_candidate_bundle_are_complete_or_unavailable(tmp_path):
    store = ShadowCandidateStore(tmp_path / "shadow")
    manifest = store.publish(candidate(), evidence_reader=EvidenceFixture(), evidence_id="ev-1")
    bundle = ShadowCandidateReader(tmp_path / "shadow", evidence_reader=EvidenceFixture()).read(
        manifest.candidate_id
    )
    assert bundle.manifest.evidence_sha256 == "a" * 64
    assert bundle.quality_report.verdict == "pass"


def test_candidate_store_recomputes_quality_and_forged_or_incomplete_pass_has_zero_writes(tmp_path):
    root = tmp_path / "shadow"
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(root).publish(
            candidate(complete=False), evidence_reader=EvidenceFixture(), evidence_id="ev-1"
        )
    assert not root.exists()


def test_candidate_store_rejects_evidence_provider_date_universe_or_plan_drift(tmp_path):
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(tmp_path / "shadow").publish(
            candidate(), evidence_reader=EvidenceFixture(provider="tickflow"), evidence_id="ev-1"
        )
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(tmp_path / "shadow2").publish(
            candidate(), evidence_reader=EvidenceFixture(universe="other"), evidence_id="ev-1"
        )


def test_candidate_bundle_reader_rejects_symlink_extra_file_and_mutation(tmp_path):
    root = tmp_path / "shadow"
    manifest = ShadowCandidateStore(root).publish(
        candidate(), evidence_reader=EvidenceFixture(), evidence_id="ev-1"
    )
    bundle = root / "bundles" / manifest.candidate_id
    (bundle / "extra").write_text("x")
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateReader(root, evidence_reader=EvidenceFixture()).read(manifest.candidate_id)
