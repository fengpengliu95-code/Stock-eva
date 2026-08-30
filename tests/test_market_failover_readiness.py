from datetime import date
from hashlib import sha256

import pytest
from pydantic import ValidationError

from backend.app.market.failover import (
    BLOCKED_REASON_ORDER,
    POLICY_PAYLOAD,
    POLICY_SHA256,
    CapabilityState,
    DailyLifecycleState,
    FailoverReadinessV1,
    PreflightAction,
    PreflightDecisionV1,
    PrimaryState,
    SecondaryCapabilitySnapshotV1,
    _build_capability_snapshot,
    _build_preflight_decision,
    _build_readiness,
    build_capability_snapshot,
    build_readiness,
    canonical_json_bytes,
    domain_sha256,
    preflight,
)

SHA = "a" * 64


def _daily_status(state: str, *, status: str = "READY") -> object:
    from backend.app.market.daily_shadow_registry import (
        DailyShadowStatus,
        DailyWindowSnapshot,
    )

    if status == "UNAVAILABLE":
        return DailyShadowStatus(
            status="UNAVAILABLE",
            epoch_count=0,
            session_report_count=0,
            reason="CONTROL_STATE_UNAVAILABLE",
        )
    qualified = state == "SHADOW_QUALIFIED"
    window = None
    if state != "PENDING":
        window = DailyWindowSnapshot(
            epoch_id="epoch-1",
            state=state,
            consecutive_sessions=20 if qualified else 3,
            first_trade_date=date(2026, 8, 1),
            last_trade_date=date(2026, 8, 20),
            next_trade_date=None,
            state_version=1,
        )
    return DailyShadowStatus(
        status="READY",
        window=window,
        epoch_count=1,
        session_report_count=20 if qualified else 3,
        descriptor_sha256=SHA,
        terms_evidence_sha256=SHA,
        version_vector_sha256=SHA if window else None,
        calendar_generation="calendar-1" if window else None,
        calendar_sha256=SHA if window else None,
        expected_dates=(date(2026, 8, 1),) * 20 if window else (),
        last_outcome="SUCCESS" if qualified else None,
        circuit_state="CLOSED",
    )


def _qualified_snapshot() -> SecondaryCapabilitySnapshotV1:
    return build_capability_snapshot(_daily_status("SHADOW_QUALIFIED"))


def test_policy_and_canonical_bytes_have_frozen_vectors() -> None:
    assert POLICY_PAYLOAD == {
        "canonical_capability_profile": "CANONICAL_SESSION_FAILOVER_V1",
        "effective_auto_failover_enabled": False,
        "eligible_secondary": None,
        "policy_version": "r2f4.0-selection-shield-v1",
        "primary_provider": "baostock",
        "schema_version": 1,
        "selection_execution": "NOT_IMPLEMENTED",
    }
    expected = (
        b'{"canonical_capability_profile":"CANONICAL_SESSION_FAILOVER_V1",'
        b'"effective_auto_failover_enabled":false,"eligible_secondary":null,'
        b'"policy_version":"r2f4.0-selection-shield-v1","primary_provider":"baostock",'
        b'"schema_version":1,"selection_execution":"NOT_IMPLEMENTED"}\n'
    )
    assert canonical_json_bytes(POLICY_PAYLOAD) == expected
    assert (
        POLICY_SHA256
        == sha256(b"stock-eva/r2f4.0/selection-shield-policy/v1\n" + expected).hexdigest()
    )
    assert canonical_json_bytes({"消息": "中文", "zero": 0, "false": False}) == (
        b'{"false":false,"zero":0,"\xe6\xb6\x88\xe6\x81\xaf":"\xe4\xb8\xad\xe6\x96\x87"}\n'
    )


def test_daily_shadow_qualification_projects_only_daily_bar() -> None:
    snapshot = _qualified_snapshot()
    assert snapshot.daily_bar_state is CapabilityState.QUALIFIED
    assert snapshot.adjustment_factor_state is CapabilityState.UNQUALIFIED
    assert snapshot.activity_units_state is CapabilityState.UNKNOWN
    assert snapshot.suspension_semantics_state is CapabilityState.UNKNOWN
    assert snapshot.exact_session_universe_state is CapabilityState.UNKNOWN
    assert snapshot.promoted_calendar_state is CapabilityState.UNKNOWN
    assert snapshot.raw_retention_contract_state is CapabilityState.UNKNOWN
    assert snapshot.full_session_qualification_state == CapabilityState.UNQUALIFIED
    assert snapshot.canonical_session_failover_state == CapabilityState.UNQUALIFIED
    assert snapshot.source_exact_universe_sha256 is None
    assert snapshot.source_qualification_evidence_sha256 is None
    assert snapshot.source_qualification_candidate_sha256 is None
    assert snapshot.source_qualification_sessions == 20


@pytest.mark.parametrize("state", ["PENDING", "OBSERVING", "RESET"])
def test_each_unqualified_daily_lifecycle_is_fail_closed(state: str) -> None:
    snapshot = build_capability_snapshot(_daily_status(state))
    assert snapshot.source_lifecycle_state is DailyLifecycleState(state)
    assert snapshot.daily_bar_state is CapabilityState.UNQUALIFIED
    assert snapshot.source_qualification_sessions < 20
    assert snapshot.full_session_qualification_state is CapabilityState.UNQUALIFIED
    assert snapshot.canonical_session_failover_state is CapabilityState.UNQUALIFIED


def test_unavailable_daily_state_has_no_lineage_or_qualified_field() -> None:
    snapshot = build_capability_snapshot(_daily_status("PENDING", status="UNAVAILABLE"))
    assert snapshot.source_status == "UNAVAILABLE"
    assert snapshot.source_lifecycle_state is None
    assert snapshot.source_qualification_sessions == 0
    assert all(
        getattr(snapshot, field) is CapabilityState.UNKNOWN
        for field in (
            "daily_bar_state",
            "activity_units_state",
            "adjustment_factor_state",
            "suspension_semantics_state",
            "exact_session_universe_state",
            "promoted_calendar_state",
            "raw_retention_contract_state",
        )
    )
    assert snapshot.source_descriptor_sha256 is None
    assert snapshot.source_calendar_sha256 is None


def test_readiness_reasons_are_exact_and_configuration_cannot_create_authority() -> None:
    readiness = build_readiness(
        configured_auto_failover_enabled=True,
        provider_priority=("baostock", "tickflow"),
        secondary=_qualified_snapshot(),
    )
    assert readiness.status == "ready"
    assert readiness.configured_auto_failover_enabled is True
    assert readiness.effective_auto_failover_enabled is False
    assert readiness.eligible_secondary is None
    assert readiness.blocked_reasons == (
        "DAILY_BAR_ONLY",
        "ACTIVITY_UNITS_UNKNOWN",
        "FACTOR_UNQUALIFIED",
        "SUSPENSION_UNKNOWN",
        "EXACT_SESSION_UNIVERSE_UNKNOWN",
        "PROMOTED_CALENDAR_UNKNOWN",
        "RAW_RETENTION_UNKNOWN",
        "FULL_SESSION_QUALIFICATION_UNQUALIFIED",
        "SELECTION_EXECUTION_NOT_IMPLEMENTED",
    )
    assert set(readiness.blocked_reasons) <= set(BLOCKED_REASON_ORDER)
    assert readiness.provider_requests == readiness.secondary_requests == 0
    assert readiness.canonical_writes is False


@pytest.mark.parametrize(
    ("state", "first_reason"),
    [
        ("PENDING", "DAILY_BAR_UNQUALIFIED"),
        ("OBSERVING", "DAILY_BAR_UNQUALIFIED"),
        ("RESET", "DAILY_BAR_UNQUALIFIED"),
    ],
)
def test_pending_observing_reset_reasons_are_ordered(state: str, first_reason: str) -> None:
    readiness = build_readiness(
        provider_priority=("baostock", "tickflow"),
        secondary=build_capability_snapshot(_daily_status(state)),
    )
    assert readiness.blocked_reasons[0] == first_reason
    assert readiness.blocked_reasons[-1] == "SELECTION_EXECUTION_NOT_IMPLEMENTED"
    assert len(readiness.blocked_reasons) == len(set(readiness.blocked_reasons))


def test_primary_preflight_dominates_and_failure_preserves_pointer() -> None:
    readiness = build_readiness(
        provider_priority=("baostock", "tickflow"), secondary=_qualified_snapshot()
    )
    allowed = preflight(PrimaryState.READY, readiness)
    assert allowed.action is PreflightAction.PRIMARY_ALLOWED
    assert allowed.selected_provider == "baostock"
    assert allowed.readiness_sha256 == readiness.readiness_sha256
    blocked = preflight(PrimaryState.UNAVAILABLE, readiness)
    assert blocked.action is PreflightAction.SECONDARY_BLOCKED
    assert blocked.selected_provider is None
    assert blocked.pointer_action == "PRESERVE_POINTER"
    assert blocked.secondary_requests == 0
    assert blocked.canonical_writes is False


def test_primary_ready_never_needs_secondary_input() -> None:
    readiness = build_readiness(provider_priority=("baostock",))
    decision = preflight("READY", readiness)
    assert decision.action == "PRIMARY_ALLOWED"
    assert decision.selected_provider == "baostock"


def test_primary_only_and_missing_first_secondary_do_not_skip_priority() -> None:
    primary_only = build_readiness(provider_priority=("baostock",))
    assert primary_only.blocked_reasons == (
        "NO_SECONDARY_CONFIGURED",
        "SELECTION_EXECUTION_NOT_IMPLEMENTED",
    )
    missing = build_readiness(provider_priority=("baostock", "tushare", "tickflow"))
    assert missing.status == "unavailable"
    assert missing.secondary is None
    assert missing.blocked_reasons == (
        "CONTROL_STATE_UNAVAILABLE",
        "SELECTION_EXECUTION_NOT_IMPLEMENTED",
    )


def test_hashes_are_deterministic_and_tamper_evident() -> None:
    first = _qualified_snapshot()
    second = _qualified_snapshot()
    assert first.snapshot_sha256 == second.snapshot_sha256
    assert (
        build_readiness(secondary=first).readiness_sha256
        == build_readiness(secondary=second).readiness_sha256
    )
    assert (
        preflight("READY", build_readiness(secondary=first)).decision_sha256
        == preflight("READY", build_readiness(secondary=second)).decision_sha256
    )
    with pytest.raises(ValidationError):
        SecondaryCapabilitySnapshotV1.model_validate(
            first.model_dump(mode="json") | {"daily_bar_state": "QUALIFIED", "snapshot_sha256": SHA}
        )
    with pytest.raises(ValidationError):
        FailoverReadinessV1.model_validate(
            build_readiness(secondary=first).model_dump(mode="json") | {"readiness_sha256": SHA}
        )
    with pytest.raises(ValidationError):
        PreflightDecisionV1.model_validate(
            preflight("READY", build_readiness(secondary=first)).model_dump(mode="json")
            | {"decision_sha256": SHA}
        )


def test_null_false_zero_and_empty_tuple_are_included_in_hash_preimages() -> None:
    baseline = domain_sha256(
        "test/preimage/v1", {"empty": (), "false": False, "null": None, "zero": 0}
    )
    assert baseline != domain_sha256(
        "test/preimage/v1", {"empty": (), "false": False, "null": None}
    )
    assert baseline != domain_sha256(
        "test/preimage/v1", {"empty": (), "false": True, "null": None, "zero": 0}
    )


def test_closed_models_reject_generic_admission_and_contradictions() -> None:
    with pytest.raises((TypeError, ValueError, ValidationError)):
        build_capability_snapshot({"provider": "tickflow", "admission_state": "qualified"})
    with pytest.raises(ValidationError):
        build_capability_snapshot(_daily_status("SHADOW_QUALIFIED")).model_validate(
            {**_qualified_snapshot().model_dump(mode="json"), "daily_bar_state": "maybe"}
        )
    with pytest.raises(ValidationError):
        SecondaryCapabilitySnapshotV1(
            source_status="READY",
            source_lifecycle_state="SHADOW_QUALIFIED",
            daily_bar_state="QUALIFIED",
            source_qualification_sessions=0,
        )


def test_public_capability_boundary_requires_exact_verified_daily_status() -> None:
    class FakeDailyStatus:
        status = "UNAVAILABLE"

    class DerivedDailyStatus(FakeDailyStatus):
        pass

    for value in (
        {"status": "UNAVAILABLE"},
        FakeDailyStatus(),
        DerivedDailyStatus(),
    ):
        with pytest.raises((TypeError, ValueError, ValidationError)):
            build_capability_snapshot(value)


def test_explicit_zero_digest_is_not_treated_as_an_omitted_digest() -> None:
    capability = _qualified_snapshot()
    with pytest.raises(ValidationError):
        SecondaryCapabilitySnapshotV1.model_validate(
            capability.model_dump(mode="json") | {"snapshot_sha256": "0" * 64}
        )
    readiness = build_readiness(secondary=capability, provider_priority=("baostock", "tickflow"))
    with pytest.raises(ValidationError):
        FailoverReadinessV1.model_validate(
            readiness.model_dump(mode="json") | {"readiness_sha256": "0" * 64}
        )
    decision = preflight("READY", readiness)
    with pytest.raises(ValidationError):
        PreflightDecisionV1.model_validate(
            decision.model_dump(mode="json") | {"decision_sha256": "0" * 64}
        )


def test_preflight_revalidates_exact_readiness_and_rejects_constructed_tamper() -> None:
    readiness = build_readiness(
        secondary=_qualified_snapshot(), provider_priority=("baostock", "tickflow")
    )
    constructed = FailoverReadinessV1.model_construct(
        **readiness.model_dump(mode="python", exclude={"secondary"}),
        secondary=readiness.secondary,
    )
    object.__setattr__(constructed, "blocked_reasons", ("NO_SECONDARY_CONFIGURED",))
    with pytest.raises(ValidationError):
        preflight("READY", constructed)

    class FakeReadiness:
        readiness_sha256 = readiness.readiness_sha256

    with pytest.raises(TypeError):
        preflight("READY", FakeReadiness())


def test_readiness_status_priority_and_secondary_form_a_closed_contract() -> None:
    capability = _qualified_snapshot()
    unavailable = build_capability_snapshot(_daily_status("PENDING", status="UNAVAILABLE"))
    base = build_readiness(secondary=capability, provider_priority=("baostock", "tickflow"))
    cases = (
        base.model_dump(mode="json") | {"secondary": None},
        base.model_dump(mode="json")
        | {"secondary": capability.model_dump(mode="json"), "status": "unavailable"},
        base.model_dump(mode="json")
        | {"secondary": None, "provider_priority": ("baostock", "tushare", "tickflow")},
        base.model_dump(mode="json") | {"secondary": unavailable.model_dump(mode="json")},
    )
    for values in cases:
        with pytest.raises(ValidationError):
            FailoverReadinessV1.model_validate(values)


def test_public_digest_fields_are_required_nonnullable_and_serialized_as_sha256() -> None:
    for model, digest in (
        (SecondaryCapabilitySnapshotV1, "snapshot_sha256"),
        (FailoverReadinessV1, "readiness_sha256"),
        (PreflightDecisionV1, "decision_sha256"),
    ):
        schema = model.model_json_schema()
        assert digest in schema["required"]
        assert schema["properties"][digest]["type"] == "string"
        assert schema["properties"][digest]["pattern"] == "^[0-9a-f]{64}$"


def test_public_normal_construction_without_digest_fails() -> None:
    capability = _qualified_snapshot().model_dump(mode="json")
    capability.pop("snapshot_sha256")
    with pytest.raises(ValidationError):
        SecondaryCapabilitySnapshotV1.model_validate(capability)
    readiness = build_readiness(
        secondary=_qualified_snapshot(), provider_priority=("baostock", "tickflow")
    )
    readiness_values = readiness.model_dump(mode="json")
    readiness_values.pop("readiness_sha256")
    with pytest.raises(ValidationError):
        FailoverReadinessV1.model_validate(readiness_values)
    decision_values = preflight("READY", readiness).model_dump(mode="json")
    decision_values.pop("decision_sha256")
    with pytest.raises(ValidationError):
        PreflightDecisionV1.model_validate(decision_values)


def test_private_builders_generate_valid_public_digests() -> None:
    capability = _qualified_snapshot()
    capability_values = capability.model_dump(mode="python")
    capability_values.pop("snapshot_sha256")
    built_capability = _build_capability_snapshot(capability_values)
    assert len(built_capability.snapshot_sha256) == 64
    readiness = build_readiness(secondary=capability, provider_priority=("baostock", "tickflow"))
    readiness_values = readiness.model_dump(mode="python")
    readiness_values.pop("readiness_sha256")
    built_readiness = _build_readiness(readiness_values)
    assert len(built_readiness.readiness_sha256) == 64
    decision = preflight("READY", readiness)
    decision_values = decision.model_dump(mode="python")
    decision_values.pop("decision_sha256")
    built_decision = _build_preflight_decision(decision_values)
    assert len(built_decision.decision_sha256) == 64


@pytest.mark.parametrize("digest", [None, "0" * 64, "b" * 64])
def test_build_readiness_rejects_constructed_nested_snapshot_bypasses(digest: str | None) -> None:
    capability = _qualified_snapshot()
    constructed = SecondaryCapabilitySnapshotV1.model_construct(
        **capability.model_dump(mode="python")
    )
    object.__setattr__(constructed, "snapshot_sha256", digest)
    with pytest.raises((TypeError, ValueError, ValidationError)):
        build_readiness(provider_priority=("baostock", "tickflow"), secondary=constructed)


@pytest.mark.parametrize("value", [1, 0, "false", None])
def test_build_readiness_requires_strict_native_control_bool(value: object) -> None:
    with pytest.raises((TypeError, ValueError, ValidationError)):
        build_readiness(control_state_available=value)  # type: ignore[arg-type]


def test_private_builders_reject_any_supplied_digest_mismatch() -> None:
    cases = []
    capability = _qualified_snapshot()
    capability_values = capability.model_dump(mode="python")
    capability_digest = capability_values.pop("snapshot_sha256")
    cases.append(
        (_build_capability_snapshot, "snapshot_sha256", capability_values, capability_digest)
    )

    readiness = build_readiness(secondary=capability, provider_priority=("baostock", "tickflow"))
    readiness_values = readiness.model_dump(mode="python")
    readiness_digest = readiness_values.pop("readiness_sha256")
    cases.append((_build_readiness, "readiness_sha256", readiness_values, readiness_digest))

    decision = preflight("READY", readiness)
    decision_values = decision.model_dump(mode="python")
    decision_digest = decision_values.pop("decision_sha256")
    cases.append((_build_preflight_decision, "decision_sha256", decision_values, decision_digest))

    for builder, digest_field, values, matching_digest in cases:
        assert getattr(builder(values), digest_field) == matching_digest
        for supplied in ("0" * 64, "b" * 64, None, 1):
            with pytest.raises((TypeError, ValueError, ValidationError)):
                builder({**values, digest_field: supplied})
