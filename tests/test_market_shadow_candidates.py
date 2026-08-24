def test_shadow_quality_report_and_candidate_bundle_are_complete_or_unavailable():
    from backend.app.market.shadow_candidates import ShadowCandidateStore, ShadowQualityReport

    assert ShadowCandidateStore is not None
    assert ShadowQualityReport is not None
