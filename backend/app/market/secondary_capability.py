"""Pure, offline authority for secondary canonical-capability admission."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

EVIDENCE_DOMAIN = "stock-eva/r2f5.1/secondary-capability-evidence/v1"
READINESS_DOMAIN = "stock-eva/r2f5.1/secondary-canonical-readiness/v1"

CANONICAL_CAPABILITIES = (
    "daily_bar",
    "activity_units",
    "adjustment_factor",
    "suspension_semantics",
    "exact_session_universe",
    "required_indexes",
    "calendar_binding",
    "quota_account_entitlement",
    "transport_security",
    "raw_retention",
)

_REASON_BY_CAPABILITY = {
    capability: f"{capability.upper()}_UNKNOWN" for capability in CANONICAL_CAPABILITIES
}


class CapabilityState(StrEnum):
    QUALIFIED = "QUALIFIED"
    UNQUALIFIED = "UNQUALIFIED"
    UNKNOWN = "UNKNOWN"


def _digest(domain: str, value: object) -> str:
    def canonical(item: object) -> object:
        if isinstance(item, BaseModel):
            return canonical(item.model_dump(mode="json"))
        if isinstance(item, datetime):
            rendered = item.isoformat()
            return rendered.replace("+00:00", "Z")
        if isinstance(item, StrEnum):
            return item.value
        if isinstance(item, dict):
            return {key: canonical(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [canonical(child) for child in item]
        return item

    payload = json.dumps(
        canonical(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(domain.encode("ascii") + b"\n" + payload).hexdigest()


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ReviewedCapabilityEvidenceV1(_Frozen):
    schema_version: Literal[1] = 1
    provider_id: Literal["tickflow", "tushare"]
    capability: Literal[
        "daily_bar",
        "activity_units",
        "adjustment_factor",
        "suspension_semantics",
        "exact_session_universe",
        "required_indexes",
        "calendar_binding",
        "quota_account_entitlement",
        "transport_security",
        "raw_retention",
    ]
    review_id: str = Field(pattern=_SAFE_ID.pattern)
    observed_at: datetime
    official_document_sha256: str = Field(pattern=_SHA256.pattern)
    contract_sha256: str = Field(pattern=_SHA256.pattern)
    state: CapabilityState
    evidence_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def validate_identity(self) -> ReviewedCapabilityEvidenceV1:
        values = self.model_dump(mode="json", exclude={"evidence_sha256"})
        if self.evidence_sha256 != _digest(EVIDENCE_DOMAIN, values):
            raise ValueError("capability evidence digest mismatch")
        return self


class SecondaryCanonicalReadinessV1(_Frozen):
    schema_version: Literal[1] = 1
    status: Literal["ready", "blocked", "unavailable"]
    provider_id: Literal["tickflow", "tushare"]
    capability_profile: Literal["CANONICAL_SESSION_FAILOVER_V1"] = "CANONICAL_SESSION_FAILOVER_V1"
    capabilities: tuple[ReviewedCapabilityEvidenceV1, ...]
    blocked_reasons: tuple[str, ...]
    eligible_for_full_session_qualification: bool
    canonical_failover_state: Literal[CapabilityState.UNQUALIFIED] = CapabilityState.UNQUALIFIED
    effective_auto_failover_enabled: Literal[False] = False
    provider_requests: Literal[0] = 0
    writes: Literal[False] = False
    readiness_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def validate_identity(self) -> SecondaryCanonicalReadinessV1:
        if len(self.blocked_reasons) != len(set(self.blocked_reasons)):
            raise ValueError("blocked reasons must be unique")
        values = self.model_dump(mode="json", exclude={"readiness_sha256"})
        if self.readiness_sha256 != _digest(READINESS_DOMAIN, values):
            raise ValueError("secondary readiness digest mismatch")
        return self


def build_capability_evidence(
    *,
    provider_id: Literal["tickflow", "tushare"],
    capability: str,
    review_id: str,
    observed_at: datetime,
    official_document_sha256: str,
    contract_sha256: str,
    state: CapabilityState,
) -> ReviewedCapabilityEvidenceV1:
    values = {
        "schema_version": 1,
        "provider_id": provider_id,
        "capability": capability,
        "review_id": review_id,
        "observed_at": observed_at,
        "official_document_sha256": official_document_sha256,
        "contract_sha256": contract_sha256,
        "state": state,
    }
    return ReviewedCapabilityEvidenceV1.model_validate(
        {**values, "evidence_sha256": _digest(EVIDENCE_DOMAIN, values)}
    )


def evaluate_secondary_readiness(
    provider_id: Literal["tickflow", "tushare"],
    evidence: tuple[ReviewedCapabilityEvidenceV1, ...],
) -> SecondaryCanonicalReadinessV1:
    validated: list[ReviewedCapabilityEvidenceV1] = []
    try:
        for item in evidence:
            if type(item) is not ReviewedCapabilityEvidenceV1:
                raise ValueError("capability evidence type is invalid")
            value = ReviewedCapabilityEvidenceV1.model_validate(item.model_dump(mode="python"))
            if value.provider_id != provider_id:
                raise ValueError("capability provider mismatch")
            validated.append(value)
    except (TypeError, ValueError):
        validated = []
    by_capability = {item.capability: item for item in validated}
    complete = (
        len(validated) == len(CANONICAL_CAPABILITIES)
        and len(by_capability) == len(CANONICAL_CAPABILITIES)
        and set(by_capability) == set(CANONICAL_CAPABILITIES)
    )
    if not complete:
        status = "unavailable"
        ordered: tuple[ReviewedCapabilityEvidenceV1, ...] = ()
        reasons = ("CONTROL_STATE_UNAVAILABLE", "FULL_SESSION_QUALIFICATION_UNQUALIFIED")
        eligible = False
    else:
        ordered = tuple(by_capability[name] for name in CANONICAL_CAPABILITIES)
        if provider_id == "tickflow":
            status = "blocked"
            reasons = ("TICKFLOW_DAILY_ONLY", "FULL_SESSION_QUALIFICATION_UNQUALIFIED")
            eligible = False
        else:
            reasons = tuple(
                _REASON_BY_CAPABILITY[item.capability]
                for item in ordered
                if item.state is not CapabilityState.QUALIFIED
            ) + ("FULL_SESSION_QUALIFICATION_UNQUALIFIED",)
            eligible = len(reasons) == 1
            status = "ready" if eligible else "blocked"
    values = {
        "schema_version": 1,
        "status": status,
        "provider_id": provider_id,
        "capability_profile": "CANONICAL_SESSION_FAILOVER_V1",
        "capabilities": ordered,
        "blocked_reasons": reasons,
        "eligible_for_full_session_qualification": eligible,
        "canonical_failover_state": CapabilityState.UNQUALIFIED,
        "effective_auto_failover_enabled": False,
        "provider_requests": 0,
        "writes": False,
    }
    return SecondaryCanonicalReadinessV1.model_validate(
        {**values, "readiness_sha256": _digest(READINESS_DOMAIN, values)}
    )


__all__ = [
    "CANONICAL_CAPABILITIES",
    "CapabilityState",
    "ReviewedCapabilityEvidenceV1",
    "SecondaryCanonicalReadinessV1",
    "build_capability_evidence",
    "evaluate_secondary_readiness",
]
