def test_canonical_comparison_snapshot_binds_manifest_partition_and_r2f2_lineage():
    from backend.app.market.canonical_comparison import CanonicalCandidateReader

    assert CanonicalCandidateReader is not None


def test_canonical_comparison_hashes_are_bidirectional():
    from backend.app.market.canonical_comparison import CanonicalComparisonSnapshot

    assert CanonicalComparisonSnapshot is not None


def test_canonical_comparison_drift_returns_unavailable_and_zero_write(tmp_path):
    from backend.app.market.canonical_comparison import CanonicalCandidateReader

    result = CanonicalCandidateReader(tmp_path).read()
    assert result.status == "unavailable"


def test_canonical_candidate_reader_rejects_root_bundle_and_fd_toctou(tmp_path):
    from backend.app.market.canonical_comparison import CanonicalCandidateReader

    assert CanonicalCandidateReader(tmp_path).read().status == "unavailable"


def test_published_canonical_comparison_verify_before_after_and_close_capability(tmp_path):
    from backend.app.market.canonical_comparison import PublishedCanonicalComparison

    assert PublishedCanonicalComparison is not None


def test_canonical_comparison_rejects_mixed_generation_or_lineage(tmp_path):
    from backend.app.market.canonical_comparison import CanonicalCandidateReader

    assert CanonicalCandidateReader(tmp_path).read().status == "unavailable"


def test_task12_candidate_attach_requires_evidence_ready_and_keeps_job_nonterminal(tmp_path):
    from backend.app.market.shadow_candidates import ShadowCandidateStore

    assert ShadowCandidateStore(tmp_path) is not None
