"""Offline, fail-closed capability and selection readiness contracts.

This module deliberately has no provider, registry, storage, or pointer authority.  It
only projects an already-read R2-F3 Daily shadow status into immutable evidence and
builds an advisory preflight decision.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

POLICY_DOMAIN = "stock-eva/r2f4.0/selection-shield-policy/v1"
CAPABILITY_DOMAIN = "stock-eva/r2f4.0/capability-snapshot/v1"
READINESS_DOMAIN = "stock-eva/r2f4.0/failover-readiness/v1"
DECISION_DOMAIN = "stock-eva/r2f4.0/preflight-decision/v1"
POLICY_VERSION = "r2f4.0-selection-shield-v1"

POLICY_PAYLOAD: dict[str, object] = {
    "canonical_capability_profile": "CANONICAL_SESSION_FAILOVER_V1",
    "effective_auto_failover_enabled": False,
    "eligible_secondary": None,
    "policy_version": POLICY_VERSION,
    "primary_provider": "baostock",
    "schema_version": 1,
    "selection_execution": "NOT_IMPLEMENTED",
}


def canonical_json_bytes(value: Any) -> bytes:
    """Return the frozen UTF-8 canonical JSON representation."""

    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def domain_sha256(domain: str, value: Any) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()


POLICY_SHA256 = domain_sha256(POLICY_DOMAIN, POLICY_PAYLOAD)
SELECTION_POLICY_SHA256 = POLICY_SHA256


class CapabilityState(StrEnum):
    QUALIFIED = "QUALIFIED"
    UNQUALIFIED = "UNQUALIFIED"
    UNKNOWN = "UNKNOWN"


class DailyLifecycleState(StrEnum):
    PENDING = "PENDING"
    OBSERVING = "OBSERVING"
    RESET = "RESET"
    SHADOW_QUALIFIED = "SHADOW_QUALIFIED"


class SourceStatus(StrEnum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


class PrimaryState(StrEnum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


class PreflightAction(StrEnum):
    PRIMARY_ALLOWED = "PRIMARY_ALLOWED"
    SECONDARY_BLOCKED = "SECONDARY_BLOCKED"


class PointerAction(StrEnum):
    PRIMARY_PUBLICATION_MAY_PROCEED = "PRIMARY_PUBLICATION_MAY_PROCEED"
    PRESERVE_POINTER = "PRESERVE_POINTER"


BLOCKED_REASON_ORDER = (
    "CONTROL_STATE_UNAVAILABLE",
    "NO_SECONDARY_CONFIGURED",
    "DAILY_BAR_UNQUALIFIED",
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
_MISSING_SEMANTICS = (
    "ACTIVITY_UNITS_UNKNOWN",
    "FACTOR_UNQUALIFIED",
    "SUSPENSION_UNKNOWN",
    "EXACT_SESSION_UNIVERSE_UNKNOWN",
    "PROMOTED_CALENDAR_UNKNOWN",
    "RAW_RETENTION_UNKNOWN",
    "FULL_SESSION_QUALIFICATION_UNQUALIFIED",
)


def _identity(model: BaseModel, digest_field: str, domain: str) -> str:
    values = model.model_dump(mode="json", by_alias=False, exclude_none=False)
    values.pop(digest_field)
    return domain_sha256(domain, values)


def _build_model(
    model_type: type[BaseModel],
    values: dict[str, Any],
    *,
    digest_field: str,
    domain: str,
) -> BaseModel:
    candidate = model_type.model_construct(**values)
    preimage = candidate.model_dump(mode="json", by_alias=False, exclude_none=False)
    preimage.pop(digest_field, None)
    digest = domain_sha256(domain, preimage)
    return model_type.model_validate({**values, digest_field: digest})


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class SecondaryCapabilitySnapshotV1(_Frozen):
    schema_version: Literal[1] = 1
    capability_profile: Literal["DAILY_BAR_SHADOW_V1"] = "DAILY_BAR_SHADOW_V1"
    provider: Literal["tickflow"] = "tickflow"
    source_profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = "TICKFLOW_FREE_DAILY_BAR_OHLC_V1"
    source_status: SourceStatus
    source_lifecycle_state: DailyLifecycleState | None
    daily_bar_state: CapabilityState
    activity_units_state: CapabilityState
    adjustment_factor_state: CapabilityState
    suspension_semantics_state: CapabilityState
    exact_session_universe_state: CapabilityState
    promoted_calendar_state: CapabilityState
    raw_retention_contract_state: CapabilityState
    full_session_qualification_state: Literal[CapabilityState.UNQUALIFIED] = (
        CapabilityState.UNQUALIFIED
    )
    source_descriptor_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_version_vector_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_adapter_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_policy_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_terms_evidence_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_calendar_generation: str | None = Field(default=None, pattern=_SAFE_ID.pattern)
    source_calendar_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_universe_policy_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_exact_universe_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_qualification_evidence_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_qualification_candidate_sha256: str | None = Field(default=None, pattern=_SHA256.pattern)
    source_qualification_sessions: StrictInt = Field(ge=0, le=20)
    canonical_session_failover_state: Literal[CapabilityState.UNQUALIFIED] = (
        CapabilityState.UNQUALIFIED
    )
    snapshot_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def validate_snapshot(self) -> SecondaryCapabilitySnapshotV1:
        if self.source_status is SourceStatus.UNAVAILABLE:
            _validate_unavailable_snapshot(self)
        else:
            _validate_ready_snapshot(self)
        expected = _identity(self, "snapshot_sha256", CAPABILITY_DOMAIN)
        if self.snapshot_sha256 != expected:
            raise ValueError("capability snapshot digest mismatch")
        return self


def _validate_unavailable_snapshot(value: SecondaryCapabilitySnapshotV1) -> None:
    capability_fields = (
        "daily_bar_state",
        "activity_units_state",
        "adjustment_factor_state",
        "suspension_semantics_state",
        "exact_session_universe_state",
        "promoted_calendar_state",
        "raw_retention_contract_state",
    )
    lineage_fields = (
        "source_descriptor_sha256",
        "source_version_vector_sha256",
        "source_adapter_sha256",
        "source_policy_sha256",
        "source_terms_evidence_sha256",
        "source_calendar_generation",
        "source_calendar_sha256",
        "source_universe_policy_sha256",
        "source_exact_universe_sha256",
        "source_qualification_evidence_sha256",
        "source_qualification_candidate_sha256",
    )
    if (
        value.source_lifecycle_state is not None
        or value.source_qualification_sessions != 0
        or any(getattr(value, field) is not CapabilityState.UNKNOWN for field in capability_fields)
        or any(getattr(value, field) is not None for field in lineage_fields)
    ):
        raise ValueError("unavailable capability has lineage or qualification")


def _validate_ready_snapshot(value: SecondaryCapabilitySnapshotV1) -> None:
    if value.source_lifecycle_state is None:
        raise ValueError("ready capability requires a lifecycle state")
    if value.source_descriptor_sha256 is None or value.source_terms_evidence_sha256 is None:
        raise ValueError("ready capability requires descriptor and terms identity")
    if (
        value.activity_units_state is not CapabilityState.UNKNOWN
        or value.adjustment_factor_state is not CapabilityState.UNQUALIFIED
        or value.suspension_semantics_state is not CapabilityState.UNKNOWN
        or value.exact_session_universe_state is not CapabilityState.UNKNOWN
        or value.promoted_calendar_state is not CapabilityState.UNKNOWN
        or value.raw_retention_contract_state is not CapabilityState.UNKNOWN
    ):
        raise ValueError("Daily Bar capability cannot imply other semantics")
    adapter_hash, policy_hash, universe_hash, _ = _r2f3_projection_constants()
    if (
        value.source_adapter_sha256 not in {None, adapter_hash}
        or value.source_policy_sha256 not in {None, policy_hash}
        or value.source_universe_policy_sha256 not in {None, universe_hash}
    ):
        raise ValueError("Daily source component identity mismatch")
    if value.source_lifecycle_state is DailyLifecycleState.PENDING:
        _validate_pending_snapshot(value)
    elif value.source_lifecycle_state in {
        DailyLifecycleState.OBSERVING,
        DailyLifecycleState.RESET,
    }:
        _validate_observing_snapshot(value)
    elif (
        value.daily_bar_state is not CapabilityState.QUALIFIED
        or value.source_qualification_sessions != 20
        or any(
            item is None
            for item in (
                value.source_version_vector_sha256,
                value.source_adapter_sha256,
                value.source_policy_sha256,
                value.source_calendar_generation,
                value.source_calendar_sha256,
                value.source_universe_policy_sha256,
            )
        )
    ):
        raise ValueError("shadow-qualified capability matrix is inconsistent")
    if any(
        item is not None
        for item in (
            value.source_exact_universe_sha256,
            value.source_qualification_evidence_sha256,
            value.source_qualification_candidate_sha256,
        )
    ):
        raise ValueError("v1 does not accept canonical qualification evidence")


def _validate_pending_snapshot(value: SecondaryCapabilitySnapshotV1) -> None:
    if (
        value.daily_bar_state is not CapabilityState.UNQUALIFIED
        or value.source_qualification_sessions != 0
        or value.source_version_vector_sha256 is not None
        or value.source_calendar_generation is not None
        or value.source_calendar_sha256 is not None
    ):
        raise ValueError("pending capability matrix is inconsistent")


def _validate_observing_snapshot(value: SecondaryCapabilitySnapshotV1) -> None:
    if (
        value.daily_bar_state is not CapabilityState.UNQUALIFIED
        or value.source_qualification_sessions >= 20
        or any(
            item is None
            for item in (
                value.source_version_vector_sha256,
                value.source_calendar_generation,
                value.source_calendar_sha256,
            )
        )
    ):
        raise ValueError("observing capability matrix is inconsistent")


def _r2f3_projection_constants() -> tuple[str, str, str, str]:
    from .daily_shadow_candidates import DAILY_BAR_RECONCILIATION_POLICY
    from .daily_shadow_models import DAILY_CANONICAL_UNIVERSE_POLICY_SHA256
    from .providers.tickflow_daily_shadow import (
        DAILY_SHADOW_ADAPTER_HASH,
        DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
        DAILY_SHADOW_MAPPING_HASH,
        DAILY_SHADOW_SOURCE_SCHEMA_HASH,
    )

    # The first three values are kept as explicit imports to bind to reviewed R2-F3
    # code; endpoint/source/mapping hashes are included in the source descriptor.
    del (
        DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
        DAILY_SHADOW_MAPPING_HASH,
        DAILY_SHADOW_SOURCE_SCHEMA_HASH,
    )
    return (
        DAILY_SHADOW_ADAPTER_HASH,
        DAILY_BAR_RECONCILIATION_POLICY.policy_sha256,
        DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        "TICKFLOW_FREE_DAILY_BAR_OHLC_V1",
    )


def build_capability_snapshot(status: Any) -> SecondaryCapabilitySnapshotV1:
    """Project a strict R2-F3 DailyShadowStatus without consulting generic admission."""

    from .daily_shadow_registry import DailyShadowStatus

    if type(status) is not DailyShadowStatus:
        raise TypeError("capability input must be a Daily shadow status")
    status = DailyShadowStatus.model_validate(status.model_dump(mode="python"))
    adapter_hash, policy_hash, universe_hash, _profile = _r2f3_projection_constants()
    if status.status == "UNAVAILABLE":
        return _build_capability_snapshot(
            {
                "source_status": SourceStatus.UNAVAILABLE,
                "source_lifecycle_state": None,
                "daily_bar_state": CapabilityState.UNKNOWN,
                "activity_units_state": CapabilityState.UNKNOWN,
                "adjustment_factor_state": CapabilityState.UNKNOWN,
                "suspension_semantics_state": CapabilityState.UNKNOWN,
                "exact_session_universe_state": CapabilityState.UNKNOWN,
                "promoted_calendar_state": CapabilityState.UNKNOWN,
                "raw_retention_contract_state": CapabilityState.UNKNOWN,
                "source_qualification_sessions": 0,
            }
        )
    window = status.window
    lifecycle = DailyLifecycleState.PENDING if window is None else DailyLifecycleState(window.state)
    sessions = 0 if window is None else window.consecutive_sessions
    qualified = lifecycle is DailyLifecycleState.SHADOW_QUALIFIED
    return _build_capability_snapshot(
        {
            "source_status": SourceStatus.READY,
            "source_lifecycle_state": lifecycle,
            "daily_bar_state": CapabilityState.QUALIFIED
            if qualified
            else CapabilityState.UNQUALIFIED,
            "activity_units_state": CapabilityState.UNKNOWN,
            "adjustment_factor_state": CapabilityState.UNQUALIFIED,
            "suspension_semantics_state": CapabilityState.UNKNOWN,
            "exact_session_universe_state": CapabilityState.UNKNOWN,
            "promoted_calendar_state": CapabilityState.UNKNOWN,
            "raw_retention_contract_state": CapabilityState.UNKNOWN,
            "source_descriptor_sha256": status.descriptor_sha256,
            "source_version_vector_sha256": status.version_vector_sha256,
            "source_adapter_sha256": adapter_hash,
            "source_policy_sha256": policy_hash,
            "source_terms_evidence_sha256": status.terms_evidence_sha256,
            "source_calendar_generation": status.calendar_generation,
            "source_calendar_sha256": status.calendar_sha256,
            "source_universe_policy_sha256": universe_hash,
            "source_qualification_sessions": sessions,
        }
    )


def _build_capability_snapshot(values: dict[str, Any]) -> SecondaryCapabilitySnapshotV1:
    return _build_model(
        SecondaryCapabilitySnapshotV1,
        values,
        digest_field="snapshot_sha256",
        domain=CAPABILITY_DOMAIN,
    )  # type: ignore[return-value]


def capability_from_registry_record(record: Any) -> SecondaryCapabilitySnapshotV1:
    raise TypeError("generic provider admission is not a capability input")


def _expected_reasons(
    *,
    status: str,
    provider_priority: tuple[str, ...],
    secondary: SecondaryCapabilitySnapshotV1 | None,
) -> tuple[str, ...]:
    if status == "unavailable":
        return ("CONTROL_STATE_UNAVAILABLE", "SELECTION_EXECUTION_NOT_IMPLEMENTED")
    if provider_priority == ("baostock",):
        return ("NO_SECONDARY_CONFIGURED", "SELECTION_EXECUTION_NOT_IMPLEMENTED")
    if secondary is None or secondary.source_status is SourceStatus.UNAVAILABLE:
        return ("CONTROL_STATE_UNAVAILABLE", "SELECTION_EXECUTION_NOT_IMPLEMENTED")
    if secondary.source_lifecycle_state is DailyLifecycleState.SHADOW_QUALIFIED:
        return ("DAILY_BAR_ONLY", *_MISSING_SEMANTICS, "SELECTION_EXECUTION_NOT_IMPLEMENTED")
    return ("DAILY_BAR_UNQUALIFIED", *_MISSING_SEMANTICS, "SELECTION_EXECUTION_NOT_IMPLEMENTED")


def _validate_priority(value: tuple[str, ...]) -> tuple[str, ...]:
    known = {"baostock", "tickflow", "tushare"}
    if (
        not value
        or value[0] != "baostock"
        or len(set(value)) != len(value)
        or any(item not in known or not item or item.strip() != item for item in value)
    ):
        raise ValueError("invalid provider priority")
    return value


class FailoverReadinessV1(_Frozen):
    schema_version: Literal[1] = 1
    status: Literal["ready", "unavailable"]
    policy_version: Literal[POLICY_VERSION] = POLICY_VERSION
    policy_sha256: str = Field(default=POLICY_SHA256, pattern=_SHA256.pattern)
    primary_provider: Literal["baostock"] = "baostock"
    configured_auto_failover_enabled: StrictBool
    effective_auto_failover_enabled: Literal[False] = False
    provider_priority: tuple[str, ...]
    eligible_secondary: Literal[None] = None
    secondary: SecondaryCapabilitySnapshotV1 | None = None
    blocked_reasons: tuple[str, ...]
    provider_requests: Literal[0] = 0
    secondary_requests: Literal[0] = 0
    canonical_writes: Literal[False] = False
    readiness_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_secondary(cls, values: Any) -> Any:
        if not isinstance(values, dict) or values.get("secondary") is None:
            return values
        secondary = values["secondary"]
        if isinstance(secondary, SecondaryCapabilitySnapshotV1):
            if type(secondary) is not SecondaryCapabilitySnapshotV1:
                raise TypeError("secondary must be an exact capability snapshot")
            values = dict(values)
            values["secondary"] = SecondaryCapabilitySnapshotV1.model_validate(
                secondary.model_dump(mode="python")
            )
        return values

    @model_validator(mode="after")
    def validate_readiness(self) -> FailoverReadinessV1:
        _validate_priority(self.provider_priority)
        if self.policy_sha256 != POLICY_SHA256:
            raise ValueError("selection policy digest mismatch")
        if self.status == "ready":
            if self.provider_priority == ("baostock",):
                if self.secondary is not None:
                    raise ValueError("primary-only readiness cannot expose a secondary")
            elif (
                self.provider_priority[1] != "tickflow"
                or self.secondary is None
                or self.secondary.source_status is not SourceStatus.READY
            ):
                raise ValueError("ready readiness requires the first secondary capability")
        elif self.secondary is not None:
            raise ValueError("unavailable readiness cannot expose a secondary")
        if len(self.blocked_reasons) != len(set(self.blocked_reasons)):
            raise ValueError("blocked reasons must be unique")
        expected = _expected_reasons(
            status=self.status,
            provider_priority=self.provider_priority,
            secondary=self.secondary,
        )
        if self.blocked_reasons != expected:
            raise ValueError("blocked reasons are not the frozen readiness mapping")
        digest = _identity(self, "readiness_sha256", READINESS_DOMAIN)
        if self.readiness_sha256 != digest:
            raise ValueError("readiness digest mismatch")
        return self


def build_readiness(
    *,
    configured_auto_failover_enabled: bool = False,
    provider_priority: tuple[str, ...] = ("baostock",),
    secondary: SecondaryCapabilitySnapshotV1 | None = None,
    control_state_available: bool = True,
) -> FailoverReadinessV1:
    if type(control_state_available) is not bool:
        raise TypeError("control_state_available must be a native bool")
    priority = _validate_priority(tuple(provider_priority))
    if secondary is not None:
        if type(secondary) is not SecondaryCapabilitySnapshotV1:
            raise TypeError("secondary must be an exact capability snapshot")
        secondary = SecondaryCapabilitySnapshotV1.model_validate(
            secondary.model_dump(mode="python")
        )
    if not control_state_available:
        status = "unavailable"
        secondary = None
    elif priority == ("baostock",):
        status = "ready"
        secondary = None
    elif priority[1] != "tickflow" or secondary is None:
        # Do not skip an unproven first secondary in order to inspect a later one.
        status = "unavailable"
        secondary = None
    elif secondary.source_status is SourceStatus.UNAVAILABLE:
        status = "unavailable"
        secondary = None
    else:
        status = "ready"
    return _build_readiness(
        {
            "status": status,
            "configured_auto_failover_enabled": configured_auto_failover_enabled,
            "provider_priority": priority,
            "secondary": secondary,
            "blocked_reasons": _expected_reasons(
                status=status, provider_priority=priority, secondary=secondary
            ),
        }
    )


def _build_readiness(values: dict[str, Any]) -> FailoverReadinessV1:
    if isinstance(values.get("secondary"), dict):
        values = dict(values)
        values["secondary"] = SecondaryCapabilitySnapshotV1.model_validate(values["secondary"])
    return _build_model(
        FailoverReadinessV1,
        values,
        digest_field="readiness_sha256",
        domain=READINESS_DOMAIN,
    )  # type: ignore[return-value]


class PreflightDecisionV1(_Frozen):
    schema_version: Literal[1] = 1
    policy_version: Literal[POLICY_VERSION] = POLICY_VERSION
    policy_sha256: str = Field(default=POLICY_SHA256, pattern=_SHA256.pattern)
    primary_state: PrimaryState
    readiness_sha256: str = Field(pattern=_SHA256.pattern)
    action: PreflightAction
    selected_provider: Literal["baostock"] | None = None
    pointer_action: PointerAction
    reason: Literal["PRIMARY_READY", "SECONDARY_NOT_CANONICALLY_QUALIFIED"]
    provider_requests: Literal[0] = 0
    secondary_requests: Literal[0] = 0
    canonical_writes: Literal[False] = False
    decision_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def validate_decision(self) -> PreflightDecisionV1:
        if self.policy_sha256 != POLICY_SHA256:
            raise ValueError("selection policy digest mismatch")
        ready = self.primary_state is PrimaryState.READY
        expected = (
            (
                PreflightAction.PRIMARY_ALLOWED,
                "baostock",
                PointerAction.PRIMARY_PUBLICATION_MAY_PROCEED,
                "PRIMARY_READY",
            )
            if ready
            else (
                PreflightAction.SECONDARY_BLOCKED,
                None,
                PointerAction.PRESERVE_POINTER,
                "SECONDARY_NOT_CANONICALLY_QUALIFIED",
            )
        )
        if (self.action, self.selected_provider, self.pointer_action, self.reason) != expected:
            raise ValueError("preflight decision contradicts primary state")
        digest = _identity(self, "decision_sha256", DECISION_DOMAIN)
        if self.decision_sha256 != digest:
            raise ValueError("preflight decision digest mismatch")
        return self


def preflight(
    primary_state: PrimaryState | str, readiness: FailoverReadinessV1
) -> PreflightDecisionV1:
    if type(readiness) is not FailoverReadinessV1:
        raise TypeError("readiness must be an exact readiness snapshot")
    readiness = FailoverReadinessV1.model_validate(readiness.model_dump(mode="python"))
    state = PrimaryState(primary_state)
    return _build_preflight_decision(
        {
            "primary_state": state,
            "readiness_sha256": readiness.readiness_sha256,
            "action": (
                PreflightAction.PRIMARY_ALLOWED
                if state is PrimaryState.READY
                else PreflightAction.SECONDARY_BLOCKED
            ),
            "selected_provider": "baostock" if state is PrimaryState.READY else None,
            "pointer_action": (
                PointerAction.PRIMARY_PUBLICATION_MAY_PROCEED
                if state is PrimaryState.READY
                else PointerAction.PRESERVE_POINTER
            ),
            "reason": "PRIMARY_READY"
            if state is PrimaryState.READY
            else "SECONDARY_NOT_CANONICALLY_QUALIFIED",
        }
    )


def _build_preflight_decision(values: dict[str, Any]) -> PreflightDecisionV1:
    return _build_model(
        PreflightDecisionV1,
        values,
        digest_field="decision_sha256",
        domain=DECISION_DOMAIN,
    )  # type: ignore[return-value]


# Descriptive aliases keep the public v1 contract discoverable without introducing
# alternate semantics.
CapabilitySnapshotV1 = SecondaryCapabilitySnapshotV1
ReadinessSnapshotV1 = FailoverReadinessV1
FailoverPreflightV1 = PreflightDecisionV1
make_capability_snapshot = build_capability_snapshot
make_readiness = build_readiness
build_preflight = preflight
