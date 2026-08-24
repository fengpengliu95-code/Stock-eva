import hashlib
import json
from pathlib import Path

from backend.app.market.candidates import CandidateManifest, SessionSelection

GOLDEN_ROOT = Path(__file__).parent / "fixtures" / "r2f2_golden"


def test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable(tmp_path):
    manifest = GOLDEN_ROOT / "sha256sums.txt"
    expected = {
        line.split("  ", 1)[1]: line.split("  ", 1)[0]
        for line in manifest.read_text().splitlines()
        if line.strip()
    }
    assert expected
    for relative, digest in expected.items():
        payload = (GOLDEN_ROOT / relative).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == digest
    assert (
        GOLDEN_ROOT / "manifest.json"
    ).read_bytes() == b'{"fixture":"r2f2-golden-v1","generation":"golden-0001"}\n'
    assert (GOLDEN_ROOT / "GET.json").read_bytes() == b'{"status":"ready","source":"baostock"}\n'


def test_r2f2_golden_reader_models_use_real_candidate_and_selection_readers():
    raw = json.loads((GOLDEN_ROOT / "reader_models.json").read_text())
    candidate = CandidateManifest.model_validate(raw["candidate"])
    selection = SessionSelection.model_validate(raw["selection"])
    assert selection.selected_candidate_id == candidate.candidate_id
    assert selection.candidate_manifest_sha256 == candidate.manifest_sha256
    assert selection.evidence_sha256 == candidate.evidence_sha256
