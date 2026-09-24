import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.market.secondary_capability import (
    CANONICAL_CAPABILITIES,
    CapabilityEvidenceReader,
    CapabilityState,
    build_capability_bundle,
    build_capability_evidence,
    build_tushare_candidate_bundle,
    evaluate_secondary_readiness,
    read_secondary_readiness,
)


def _evidence(provider: str, capability: str, state: CapabilityState):
    return build_capability_evidence(
        provider_id=provider,
        capability=capability,
        review_id=f"review-{provider}-{capability}",
        observed_at=datetime(2026, 9, 18, 8, tzinfo=UTC),
        official_document_sha256="1" * 64,
        contract_sha256="2" * 64,
        state=state,
    )


def test_tushare_partial_official_semantics_remain_ineligible_and_zero_effect() -> None:
    qualified = {"daily_bar", "activity_units", "suspension_semantics"}
    evidence = tuple(
        _evidence(
            "tushare",
            capability,
            CapabilityState.QUALIFIED if capability in qualified else CapabilityState.UNKNOWN,
        )
        for capability in CANONICAL_CAPABILITIES
    )

    result = evaluate_secondary_readiness("tushare", evidence)

    assert result.status == "blocked"
    assert result.eligible_for_full_session_qualification is False
    assert result.canonical_failover_state == CapabilityState.UNQUALIFIED
    assert result.effective_auto_failover_enabled is False
    assert result.provider_requests == 0
    assert result.writes is False
    assert result.blocked_reasons == (
        "ADJUSTMENT_FACTOR_UNKNOWN",
        "EXACT_SESSION_UNIVERSE_UNKNOWN",
        "REQUIRED_INDEXES_UNKNOWN",
        "CALENDAR_BINDING_UNKNOWN",
        "QUOTA_ACCOUNT_ENTITLEMENT_UNKNOWN",
        "TRANSPORT_SECURITY_UNKNOWN",
        "RAW_RETENTION_UNKNOWN",
        "FULL_SESSION_QUALIFICATION_UNQUALIFIED",
    )


def test_tickflow_daily_shadow_never_becomes_canonical_eligible() -> None:
    evidence = tuple(
        _evidence("tickflow", capability, CapabilityState.QUALIFIED)
        for capability in CANONICAL_CAPABILITIES
    )

    result = evaluate_secondary_readiness("tickflow", evidence)

    assert result.status == "blocked"
    assert result.eligible_for_full_session_qualification is False
    assert result.blocked_reasons == (
        "TICKFLOW_DAILY_ONLY",
        "FULL_SESSION_QUALIFICATION_UNQUALIFIED",
    )


def test_missing_duplicate_or_cross_provider_evidence_fails_closed() -> None:
    complete = tuple(
        _evidence("tushare", capability, CapabilityState.QUALIFIED)
        for capability in CANONICAL_CAPABILITIES
    )

    assert evaluate_secondary_readiness("tushare", complete[:-1]).status == "unavailable"
    assert evaluate_secondary_readiness("tushare", (*complete, complete[0])).status == "unavailable"
    crossed = complete[:-1] + (
        _evidence("tickflow", complete[-1].capability, CapabilityState.QUALIFIED),
    )
    assert evaluate_secondary_readiness("tushare", crossed).status == "unavailable"


def test_all_semantics_only_admit_qualification_start_not_failover() -> None:
    evidence = tuple(
        _evidence("tushare", capability, CapabilityState.QUALIFIED)
        for capability in CANONICAL_CAPABILITIES
    )

    result = evaluate_secondary_readiness("tushare", evidence)

    assert result.status == "ready"
    assert result.blocked_reasons == ("FULL_SESSION_QUALIFICATION_UNQUALIFIED",)
    assert result.eligible_for_full_session_qualification is True
    assert result.canonical_failover_state == CapabilityState.UNQUALIFIED
    assert result.effective_auto_failover_enabled is False


def test_capability_hash_and_closed_models_reject_tampering() -> None:
    evidence = _evidence("tushare", "activity_units", CapabilityState.QUALIFIED)

    with pytest.raises(ValidationError):
        type(evidence).model_validate(
            {**evidence.model_dump(mode="python"), "contract_sha256": "3" * 64}
        )
    with pytest.raises(ValidationError):
        type(evidence).model_validate({**evidence.model_dump(mode="python"), "token": "secret"})


def _bundle(provider: str = "tushare"):
    return build_capability_bundle(
        provider_id=provider,
        evidence=tuple(
            _evidence(provider, capability, CapabilityState.QUALIFIED)
            for capability in CANONICAL_CAPABILITIES
        ),
    )


def _write_bundle(path: Path, bundle=None) -> None:
    value = bundle or _bundle()
    path.write_text(json.dumps(value.model_dump(mode="json")), encoding="utf-8")


def _fingerprint(path: Path) -> tuple[int, int, int, int, str]:
    value = path.stat(follow_symlinks=False)
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def test_reader_returns_strict_bundle_without_modifying_input(tmp_path: Path) -> None:
    path = tmp_path / "capabilities.json"
    _write_bundle(path)
    before = _fingerprint(path)

    result = CapabilityEvidenceReader(path).read()

    assert result.status == "ready"
    assert result.reason_code is None
    assert result.bundle == _bundle()
    assert result.provider_requests == 0
    assert result.writes is False
    assert _fingerprint(path) == before


@pytest.mark.parametrize(
    "failure", ["missing", "relative", "symlink", "symlink_parent", "oversized"]
)
def test_reader_rejects_unsafe_or_unbounded_input(tmp_path: Path, failure: str) -> None:
    path = tmp_path / "capabilities.json"
    _write_bundle(path)
    if failure == "missing":
        target = tmp_path / "missing.json"
    elif failure == "relative":
        target = Path("capabilities.json")
    elif failure == "symlink":
        target = tmp_path / "alias.json"
        target.symlink_to(path)
    elif failure == "symlink_parent":
        alias = tmp_path / "alias-parent"
        alias.symlink_to(tmp_path, target_is_directory=True)
        target = alias / path.name
    else:
        target = path
    before = _fingerprint(path)

    result = CapabilityEvidenceReader(
        target,
        max_bytes=path.stat().st_size - 1 if failure == "oversized" else 128 * 1024,
    ).read()

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.bundle is None
    assert result.provider_requests == 0
    assert result.writes is False
    assert _fingerprint(path) == before


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version":1,"schema_version":1}',
        '{"schema_version":1',
        '{"schema_version":1,"provider_id":"unknown"}',
    ],
)
def test_reader_rejects_duplicate_truncated_or_unknown_json(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "capabilities.json"
    path.write_text(payload, encoding="utf-8")
    before = _fingerprint(path)

    result = CapabilityEvidenceReader(path).read()

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert _fingerprint(path) == before


def test_reader_detects_path_replacement_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "capabilities.json"
    replacement = tmp_path / "replacement.json"
    _write_bundle(path)
    _write_bundle(replacement, _bundle("tickflow"))
    real_read = os.read
    replaced = False

    def replacing_read(descriptor: int, size: int) -> bytes:
        nonlocal replaced
        data = real_read(descriptor, size)
        if data and not replaced:
            replaced = True
            os.replace(replacement, path)
        return data

    monkeypatch.setattr("backend.app.market.secondary_capability.os.read", replacing_read)

    result = CapabilityEvidenceReader(path).read()

    assert replaced is True
    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"


def test_reader_fails_closed_when_descriptor_close_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "capabilities.json"
    _write_bundle(path)

    def failed_close(descriptor: int) -> None:
        raise OSError("sanitized close failure")

    monkeypatch.setattr("backend.app.market.secondary_capability.os.close", failed_close)

    result = CapabilityEvidenceReader(path).read()

    assert result.status == "unavailable"
    assert result.reason_code == "CONTROL_STATE_UNAVAILABLE"
    assert result.provider_requests == 0
    assert result.writes is False


def test_tushare_official_candidate_qualifies_only_proven_semantics() -> None:
    bundle = build_tushare_candidate_bundle(
        review_id="review-tushare-official-20260918",
        observed_at=datetime(2026, 9, 18, 8, tzinfo=UTC),
        daily_document_sha256="a" * 64,
        adjustment_document_sha256="b" * 64,
        suspension_document_sha256="c" * 64,
    )

    states = {item.capability: item.state for item in bundle.evidence}
    assert states["daily_bar"] == CapabilityState.QUALIFIED
    assert states["activity_units"] == CapabilityState.QUALIFIED
    assert states["suspension_semantics"] == CapabilityState.QUALIFIED
    assert states["adjustment_factor"] == CapabilityState.UNQUALIFIED
    assert all(
        states[name] == CapabilityState.UNKNOWN
        for name in (
            "exact_session_universe",
            "required_indexes",
            "calendar_binding",
            "quota_account_entitlement",
            "transport_security",
            "raw_retention",
        )
    )

    readiness = evaluate_secondary_readiness("tushare", bundle.evidence)

    assert readiness.status == "blocked"
    assert readiness.eligible_for_full_session_qualification is False
    assert readiness.blocked_reasons[0] == "ADJUSTMENT_FACTOR_UNQUALIFIED"
    assert readiness.canonical_failover_state == CapabilityState.UNQUALIFIED
    assert readiness.effective_auto_failover_enabled is False
    assert readiness.provider_requests == 0
    assert readiness.writes is False


def test_tushare_candidate_rejects_invalid_review_or_document_hash() -> None:
    values = {
        "review_id": "review-tushare-official-20260918",
        "observed_at": datetime(2026, 9, 18, 8, tzinfo=UTC),
        "daily_document_sha256": "a" * 64,
        "adjustment_document_sha256": "b" * 64,
        "suspension_document_sha256": "c" * 64,
    }

    with pytest.raises(ValidationError):
        build_tushare_candidate_bundle(**{**values, "daily_document_sha256": "invalid"})
    with pytest.raises(ValidationError):
        build_tushare_candidate_bundle(**{**values, "review_id": "token=secret path/name"})
    with pytest.raises(ValidationError):
        build_tushare_candidate_bundle(**{**values, "observed_at": datetime(2026, 9, 18, 8)})


def test_readiness_projection_is_read_only_and_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "capabilities.json"
    _write_bundle(path, _bundle("tushare"))
    before = _fingerprint(path)

    projected = read_secondary_readiness(path, provider_id="tushare")

    assert projected.status == "ready"
    assert projected.eligible_for_full_session_qualification is True
    assert projected.canonical_failover_state == CapabilityState.UNQUALIFIED
    assert projected.effective_auto_failover_enabled is False
    assert projected.provider_requests == 0
    assert projected.writes is False
    assert _fingerprint(path) == before

    mismatch = read_secondary_readiness(path, provider_id="tickflow")
    missing = read_secondary_readiness(tmp_path / "missing.json", provider_id="tushare")
    assert mismatch.status == "unavailable"
    assert missing.status == "unavailable"
    assert mismatch.blocked_reasons[0] == "CONTROL_STATE_UNAVAILABLE"
    assert missing.provider_requests == 0
    assert missing.writes is False
    assert _fingerprint(path) == before
