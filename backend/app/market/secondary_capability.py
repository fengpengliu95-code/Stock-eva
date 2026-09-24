"""Pure, offline authority for secondary canonical-capability admission."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

EVIDENCE_DOMAIN = "stock-eva/r2f5.1/secondary-capability-evidence/v1"
READINESS_DOMAIN = "stock-eva/r2f5.1/secondary-canonical-readiness/v1"
BUNDLE_DOMAIN = "stock-eva/r2f5.1/secondary-capability-bundle/v1"
_DEFAULT_MAX_BUNDLE_BYTES = 128 * 1024
_READ_CHUNK_BYTES = 64 * 1024

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


class _TushareCandidateInput(_Frozen):
    review_id: str = Field(pattern=_SAFE_ID.pattern)
    observed_at: datetime
    daily_document_sha256: str = Field(pattern=_SHA256.pattern)
    adjustment_document_sha256: str = Field(pattern=_SHA256.pattern)
    suspension_document_sha256: str = Field(pattern=_SHA256.pattern)


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

    @field_validator("observed_at")
    @classmethod
    def require_utc_observation(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("observed_at must be UTC")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> ReviewedCapabilityEvidenceV1:
        values = self.model_dump(mode="json", exclude={"evidence_sha256"})
        if self.evidence_sha256 != _digest(EVIDENCE_DOMAIN, values):
            raise ValueError("capability evidence digest mismatch")
        return self


class ReviewedCapabilityBundleV1(_Frozen):
    schema_version: Literal[1] = 1
    provider_id: Literal["tickflow", "tushare"]
    evidence: tuple[ReviewedCapabilityEvidenceV1, ...]
    bundle_sha256: str = Field(pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def validate_bundle(self) -> ReviewedCapabilityBundleV1:
        capabilities = tuple(item.capability for item in self.evidence)
        if (
            any(item.provider_id != self.provider_id for item in self.evidence)
            or len(capabilities) != len(CANONICAL_CAPABILITIES)
            or len(set(capabilities)) != len(CANONICAL_CAPABILITIES)
            or set(capabilities) != set(CANONICAL_CAPABILITIES)
        ):
            raise ValueError("capability bundle is incomplete")
        values = self.model_dump(mode="json", exclude={"bundle_sha256"})
        if self.bundle_sha256 != _digest(BUNDLE_DOMAIN, values):
            raise ValueError("capability bundle digest mismatch")
        return self


class CapabilityEvidenceReadResult(_Frozen):
    status: Literal["ready", "unavailable"]
    reason_code: Literal["CONTROL_STATE_UNAVAILABLE"] | None = None
    bundle: ReviewedCapabilityBundleV1 | None = None
    provider_requests: Literal[0] = 0
    writes: Literal[False] = False

    @model_validator(mode="after")
    def validate_result(self) -> CapabilityEvidenceReadResult:
        ready = self.status == "ready"
        if ready != (self.bundle is not None) or ready != (self.reason_code is None):
            raise ValueError("capability evidence read result is inconsistent")
        return self


def _duplicate_rejecting_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


class CapabilityEvidenceReader:
    def __init__(self, path: Path, *, max_bytes: int = _DEFAULT_MAX_BUNDLE_BYTES) -> None:
        self.path = Path(path)
        self.max_bytes = max_bytes

    @staticmethod
    def _unavailable() -> CapabilityEvidenceReadResult:
        return CapabilityEvidenceReadResult(
            status="unavailable",
            reason_code="CONTROL_STATE_UNAVAILABLE",
        )

    @staticmethod
    def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    def _parents_are_directories(self) -> bool:
        current = Path(self.path.anchor)
        for component in self.path.parts[1:-1]:
            current /= component
            value = os.stat(current, follow_symlinks=False)
            if not stat.S_ISDIR(value.st_mode):
                return False
        return True

    def read(self) -> CapabilityEvidenceReadResult:
        descriptor = -1
        try:
            if (
                not self.path.is_absolute()
                or self.path == Path(self.path.anchor)
                or ".." in self.path.parts
                or type(self.max_bytes) is not int
                or self.max_bytes < 1
                or not self._parents_are_directories()
            ):
                return self._unavailable()
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            if not nofollow:
                return self._unavailable()
            entry_before = os.stat(self.path, follow_symlinks=False)
            if not stat.S_ISREG(entry_before.st_mode):
                return self._unavailable()
            descriptor = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | nofollow)
            opened_before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened_before.st_mode)
                or self._identity(entry_before) != self._identity(opened_before)
                or opened_before.st_size < 1
                or opened_before.st_size > self.max_bytes
            ):
                return self._unavailable()
            payload = bytearray()
            while True:
                chunk = os.read(
                    descriptor,
                    min(_READ_CHUNK_BYTES, self.max_bytes + 1 - len(payload)),
                )
                if not chunk:
                    break
                payload.extend(chunk)
                if len(payload) > self.max_bytes:
                    return self._unavailable()
            opened_after = os.fstat(descriptor)
            entry_after = os.stat(self.path, follow_symlinks=False)
            if self._identity(opened_before) != self._identity(opened_after) or self._identity(
                opened_after
            ) != self._identity(entry_after):
                return self._unavailable()
            decoded = bytes(payload).decode("utf-8")
            parsed = json.loads(decoded, object_pairs_hook=_duplicate_rejecting_object)
            if not isinstance(parsed, dict):
                return self._unavailable()
            bundle = ReviewedCapabilityBundleV1.model_validate_json(payload)
            return CapabilityEvidenceReadResult(status="ready", bundle=bundle)
        except (OSError, UnicodeError, ValueError):
            return self._unavailable()
        finally:
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    return self._unavailable()


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


def build_capability_bundle(
    *,
    provider_id: Literal["tickflow", "tushare"],
    evidence: tuple[ReviewedCapabilityEvidenceV1, ...],
) -> ReviewedCapabilityBundleV1:
    by_capability = {item.capability: item for item in evidence}
    if len(evidence) != len(CANONICAL_CAPABILITIES) or len(by_capability) != len(evidence):
        raise ValueError("capability bundle is incomplete")
    ordered = tuple(by_capability[name] for name in CANONICAL_CAPABILITIES if name in by_capability)
    values = {
        "schema_version": 1,
        "provider_id": provider_id,
        "evidence": ordered,
    }
    return ReviewedCapabilityBundleV1.model_validate(
        {**values, "bundle_sha256": _digest(BUNDLE_DOMAIN, values)}
    )


def build_tushare_candidate_bundle(
    *,
    review_id: str,
    observed_at: datetime,
    daily_document_sha256: str,
    adjustment_document_sha256: str,
    suspension_document_sha256: str,
) -> ReviewedCapabilityBundleV1:
    """Build offline evidence for only the semantics proven by reviewed documents."""
    source = _TushareCandidateInput.model_validate(
        {
            "review_id": review_id,
            "observed_at": observed_at,
            "daily_document_sha256": daily_document_sha256,
            "adjustment_document_sha256": adjustment_document_sha256,
            "suspension_document_sha256": suspension_document_sha256,
        }
    )
    document_set_sha256 = _digest(
        "stock-eva/r2f5.1/tushare-official-document-set/v1",
        {
            "daily": source.daily_document_sha256,
            "adjustment": source.adjustment_document_sha256,
            "suspension": source.suspension_document_sha256,
        },
    )
    definitions = {
        "daily_bar": (
            CapabilityState.QUALIFIED,
            source.daily_document_sha256,
            "unadjusted-daily-ohlcv-fields",
        ),
        "activity_units": (
            CapabilityState.QUALIFIED,
            source.daily_document_sha256,
            "volume-lots-and-amount-thousand-cny",
        ),
        "adjustment_factor": (
            CapabilityState.UNQUALIFIED,
            source.adjustment_document_sha256,
            "endpoint-does-not-prove-anchor-direction-or-restatement",
        ),
        "suspension_semantics": (
            CapabilityState.QUALIFIED,
            _digest(
                "stock-eva/r2f5.1/tushare-suspension-document-set/v1",
                {
                    "daily": source.daily_document_sha256,
                    "suspension": source.suspension_document_sha256,
                },
            ),
            "daily-omits-suspended-symbols-and-suspend-d-is-separate",
        ),
        "exact_session_universe": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "exact-session-universe-unproven",
        ),
        "required_indexes": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "required-index-set-unproven",
        ),
        "calendar_binding": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "canonical-calendar-binding-unproven",
        ),
        "quota_account_entitlement": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "intended-account-entitlement-and-quota-unproven",
        ),
        "transport_security": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "data-api-transport-security-unproven",
        ),
        "raw_retention": (
            CapabilityState.UNKNOWN,
            document_set_sha256,
            "private-raw-retention-contract-unproven",
        ),
    }
    evidence = tuple(
        build_capability_evidence(
            provider_id="tushare",
            capability=capability,
            review_id=source.review_id,
            observed_at=source.observed_at,
            official_document_sha256=definitions[capability][1],
            contract_sha256=_digest(
                "stock-eva/r2f5.1/tushare-capability-contract/v1",
                {"capability": capability, "contract": definitions[capability][2]},
            ),
            state=definitions[capability][0],
        )
        for capability in CANONICAL_CAPABILITIES
    )
    return build_capability_bundle(provider_id="tushare", evidence=evidence)


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
                f"{item.capability.upper()}_{item.state.value}"
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


def read_secondary_readiness(
    path: Path,
    *,
    provider_id: Literal["tickflow", "tushare"],
) -> SecondaryCanonicalReadinessV1:
    """Project a reviewed bundle without initializing state or contacting a provider."""
    result = CapabilityEvidenceReader(path).read()
    if (
        result.status != "ready"
        or result.bundle is None
        or result.bundle.provider_id != provider_id
    ):
        return evaluate_secondary_readiness(provider_id, ())
    return evaluate_secondary_readiness(provider_id, result.bundle.evidence)


__all__ = [
    "CANONICAL_CAPABILITIES",
    "CapabilityEvidenceReadResult",
    "CapabilityEvidenceReader",
    "CapabilityState",
    "ReviewedCapabilityEvidenceV1",
    "ReviewedCapabilityBundleV1",
    "SecondaryCanonicalReadinessV1",
    "build_capability_bundle",
    "build_capability_evidence",
    "build_tushare_candidate_bundle",
    "evaluate_secondary_readiness",
    "read_secondary_readiness",
]
