"""Static, secret-safe contracts for the isolated R2-F3 shadow lane."""

from __future__ import annotations

import hashlib
import json
import os
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

_TOKEN_ENV = {"tickflow": "STOCK_EVA_TICKFLOW_TOKEN", "tushare": "STOCK_EVA_TUSHARE_TOKEN"}
_PROVIDERS = frozenset(_TOKEN_ENV)
_TERMS_FIELDS = (
    "terms_evidence_id",
    "provider_id",
    "official_url_allowlist_json",
    "content_object_relpath",
    "content_bytes_sha256",
    "contract_version",
    "as_of_date",
    "reviewer",
    "review_id",
    "approved_intended_use",
    "approved_retention",
    "approved_credential_mode",
    "approved_quota_decision",
)


class ShadowProviderId(StrEnum):
    TICKFLOW = "tickflow"
    TUSHARE = "tushare"


class AdmissionState(StrEnum):
    DISCOVERED = "discovered"
    CANARY = "canary"
    SHADOW = "shadow"
    QUALIFIED = "qualified"
    QUARANTINED = "quarantined"


class _Immutable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class TermsEvidence(_Immutable):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, populate_by_name=True
    )

    terms_evidence_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    provider_id: ShadowProviderId
    official_url_allowlist: tuple[str, ...] = Field(
        min_length=1, alias="official_url_allowlist_json"
    )
    content_object_relpath: str = Field(min_length=1, max_length=512)
    content_bytes_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_version: str = Field(min_length=1, max_length=128)
    as_of_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    reviewer: str = Field(min_length=1, max_length=128)
    review_id: str = Field(min_length=1, max_length=128)
    approved_intended_use: str = Field(min_length=1, max_length=512)
    approved_retention: str = Field(min_length=1, max_length=512)
    approved_credential_mode: str = Field(min_length=1, max_length=128)
    approved_quota_decision: str = Field(min_length=1, max_length=512)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    _content_bytes: bytes = PrivateAttr(default=b"")

    @property
    def official_url_allowlist_json(self) -> str:
        return json.dumps(
            list(self.official_url_allowlist), ensure_ascii=False, separators=(",", ":")
        )

    @classmethod
    def build(cls, *, content_bytes: bytes, official_url_allowlist: tuple[str, ...], **values: Any):
        digest = hashlib.sha256(bytes(content_bytes)).hexdigest()
        urls = tuple(sorted(official_url_allowlist))
        provider_id = ShadowProviderId(values.pop("provider_id"))
        probe = cls.model_construct(
            **values,
            provider_id=provider_id,
            official_url_allowlist=urls,
            content_bytes_sha256=digest,
            manifest_sha256="0" * 64,
        )
        manifest = terms_evidence_manifest_sha256(probe)
        result = cls.model_validate(probe.model_dump(mode="python") | {"manifest_sha256": manifest})
        object.__setattr__(result, "_content_bytes", bytes(content_bytes))
        return result

    @model_validator(mode="after")
    def validate_manifest(self) -> TermsEvidence:
        if (
            self.manifest_sha256 != "0" * 64
            and self.manifest_sha256 != terms_evidence_manifest_sha256(self)
        ):
            raise ValueError("terms evidence manifest hash mismatch")
        if tuple(sorted(set(self.official_url_allowlist))) != self.official_url_allowlist:
            raise ValueError("official URL allowlist must be lexical and unique")
        if self.content_object_relpath.startswith("/") or ".." in self.content_object_relpath.split(
            "/"
        ):
            raise ValueError("terms content path must be relative")
        return self


class ShadowProviderRecord(_Immutable):
    provider_id: ShadowProviderId
    admission_state: AdmissionState
    adapter_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    endpoint_contract_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalizer_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reconciliation_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    terms_evidence_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    terms_review_id: str | None = None
    credential_env_name: str
    intended_use: str
    retention_decision: str
    quota_contract: str
    required_fields_json: str
    unit_contract_json: str
    state_version: int = Field(ge=0)
    quarantine_reason: str | None = None

    @model_validator(mode="after")
    def validate_admission(self) -> ShadowProviderRecord:
        if self.credential_env_name != exact_credential_env(self.provider_id.value):
            raise ValueError("credential environment mapping is frozen")
        if self.admission_state != AdmissionState.DISCOVERED and (
            self.terms_evidence_hash is None or self.terms_review_id is None
        ):
            raise ValueError("terms evidence is required after discovery")
        return self


class ShadowProviderContract(_Immutable):
    provider_id: ShadowProviderId
    admission_state: AdmissionState = AdmissionState.DISCOVERED
    credential_env_name: str
    endpoint_contract_version: str
    source_schema_version: str
    requires_official_https_proof: bool


STATIC_PROVIDER_CONTRACTS: dict[ShadowProviderId, ShadowProviderContract] = {
    ShadowProviderId.TICKFLOW: ShadowProviderContract(
        provider_id=ShadowProviderId.TICKFLOW,
        credential_env_name=_TOKEN_ENV["tickflow"],
        endpoint_contract_version="r2f3-tickflow-discovery-v1",
        source_schema_version="undiscovered",
        requires_official_https_proof=False,
    ),
    ShadowProviderId.TUSHARE: ShadowProviderContract(
        provider_id=ShadowProviderId.TUSHARE,
        credential_env_name=_TOKEN_ENV["tushare"],
        endpoint_contract_version="r2f3-tushare-discovery-v1",
        source_schema_version="undiscovered",
        requires_official_https_proof=True,
    ),
}

# Names used by the design and by read-only callers; this remains a static map.
ShadowAdmissionState = AdmissionState
PROVIDER_CONTRACTS = STATIC_PROVIDER_CONTRACTS


def exact_credential_env(provider_id: str, *, requested: str | None = None) -> str:
    if provider_id not in _PROVIDERS:
        raise ValueError("unknown shadow provider")
    expected = _TOKEN_ENV[provider_id]
    if requested is not None and requested != expected:
        raise ValueError("credential environment mapping is frozen")
    return expected


def canonical_terms_evidence(evidence: TermsEvidence) -> bytes:
    values = {
        "terms_evidence_id": evidence.terms_evidence_id,
        "provider_id": evidence.provider_id.value,
        "official_url_allowlist_json": json.dumps(
            list(evidence.official_url_allowlist), ensure_ascii=False, separators=(",", ":")
        ),
        "content_object_relpath": evidence.content_object_relpath,
        "content_bytes_sha256": evidence.content_bytes_sha256,
        "contract_version": evidence.contract_version,
        "as_of_date": evidence.as_of_date,
        "reviewer": evidence.reviewer,
        "review_id": evidence.review_id,
        "approved_intended_use": evidence.approved_intended_use,
        "approved_retention": evidence.approved_retention,
        "approved_credential_mode": evidence.approved_credential_mode,
        "approved_quota_decision": evidence.approved_quota_decision,
    }
    return (json.dumps(values, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def terms_evidence_manifest_sha256(evidence: TermsEvidence) -> str:
    return hashlib.sha256(
        b"stock-eva/r2f3/terms-evidence/v1\n" + canonical_terms_evidence(evidence)
    ).hexdigest()


def make_token(
    provider_id: str,
    *,
    requested: str | None = None,
    terms_approved: bool = False,
    https_proof: bool = True,
    environ: dict[str, str] | None = None,
) -> str:
    """Read the exact environment variable only after all admission gates pass."""
    from .registry import TermsEvidenceUnavailable

    env_name = exact_credential_env(provider_id, requested=requested)
    if not terms_approved or (provider_id == "tushare" and not https_proof):
        raise TermsEvidenceUnavailable("provider terms or transport proof unavailable")
    value = (os.environ if environ is None else environ).get(env_name)
    if not value:
        raise TermsEvidenceUnavailable("provider credential unavailable")
    return value


def static_provider_ids() -> tuple[str, ...]:
    return tuple(sorted(_PROVIDERS))
