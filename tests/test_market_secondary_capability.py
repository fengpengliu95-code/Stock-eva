from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from backend.app.market.secondary_capability import (
    CANONICAL_CAPABILITIES,
    CapabilityState,
    build_capability_evidence,
    evaluate_secondary_readiness,
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
