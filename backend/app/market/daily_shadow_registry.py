"""Private sidecar state for TickFlow Free Daily Bar shadow qualification."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .daily_shadow_candidates import DAILY_BAR_RECONCILIATION_POLICY
from .daily_shadow_models import (
    DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
    DAILY_SHADOW_PROFILE,
    canonical_json_bytes,
    domain_sha256,
    validate_daily_failure_class,
)
from .daily_shadow_schema import (
    MAX_CANONICAL_BYTES,
    DailyShadowSchemaError,
    configure_daily_connection,
    daily_validate_terminal_graph,
    initialize_daily_shadow_schema,
    sha256_bytes,
    validate_daily_shadow_schema,
)
from .daily_shadow_schema import (
    canonical_json_bytes as schema_json_bytes,
)
from .providers.shadow_contracts import (
    ShadowProviderId,
    TermsEvidence,
    canonical_terms_evidence,
    terms_evidence_manifest_sha256,
)
from .providers.tickflow import TICKFLOW_FREE_SDK_VERSION, TICKFLOW_FREE_SDK_WHEEL_SHA256
from .providers.tickflow_daily_shadow import (
    DAILY_SHADOW_ADAPTER_HASH,
    DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
    DAILY_SHADOW_MAPPING_HASH,
    DAILY_SHADOW_SOURCE_SCHEMA_HASH,
    DAILY_SHADOW_UNIT_STATE_HASH,
)

DAILY_SHADOW_TERMS_CONTRACT_VERSION_PREFIX = "r2f3-tickflow-free-daily-bar-shadow-v1"
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_MAX_DATABASE_BYTES = 32 * 1024 * 1024
_REQUIRED_SESSIONS = 20
_FAILURE_THRESHOLD = 3
_COOLDOWN_SECONDS = 900
_PROBE_LEASE_SECONDS = 120
_JOB_LEASE_SECONDS = 1800
_FREE_ORIGIN = "https://free-api.tickflow.org"
_INTENDED_USE = "free-historical-daily-ohlc-shadow-only"
_RETENTION = "final-success-source-evidence-only"
_CREDENTIAL_MODE = "credentialless-free"
_QUOTA_DECISION = "unqualified-max-40-sequential-one-attempt"


def _contract_component_hash(label: str, value: Any) -> str:
    return domain_sha256(f"stock-eva/r2f3/free-daily-contract/{label}/v1", value)


DAILY_SHADOW_REQUEST_BUDGET_HASH = _contract_component_hash(
    "request-budget",
    {
        "max_shard_symbols": 100,
        "max_requests": 40,
        "max_attempts": 1,
        "max_response_bytes": 8 * 1024 * 1024,
        "sequential": True,
    },
)
DAILY_SHADOW_CIRCUIT_POLICY_HASH = _contract_component_hash(
    "circuit-policy",
    {
        "failure_threshold": _FAILURE_THRESHOLD,
        "cooldown_seconds": _COOLDOWN_SECONDS,
        "probe_lease_seconds": _PROBE_LEASE_SECONDS,
        "half_open_probe": "fixed-five-zero-write-one-attempt",
    },
)
DAILY_SHADOW_JOB_LEASE_POLICY_HASH = _contract_component_hash(
    "job-lease", {"lease_seconds": _JOB_LEASE_SECONDS}
)
DAILY_SHADOW_CANDIDATE_CONTRACT_HASH = _contract_component_hash(
    "candidate", "final-success-source-evidence-daily-ohlc-only"
)
DAILY_SHADOW_SYMBOL_SET_BINDING_HASH = _contract_component_hash(
    "symbol-set-binding",
    {
        "identity": "per-session-canonical-active-symbol-set-sha256",
        "domain": "stock-eva/r2f3/daily-canonical-universe/v1",
        "closed_by": (
            "request_plan",
            "candidate",
            "sidecar_job",
            "session_report",
            "terminal_attestation",
        ),
    },
)
DAILY_SHADOW_RETENTION_HASH = _contract_component_hash("retention", _RETENTION)
DAILY_SHADOW_DESCRIPTOR_BASE_HASH = _contract_component_hash(
    "descriptor",
    {
        "profile": DAILY_SHADOW_PROFILE,
        "origin": _FREE_ORIGIN,
        "adapter": DAILY_SHADOW_ADAPTER_HASH,
        "endpoint": DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
        "source_schema": DAILY_SHADOW_SOURCE_SCHEMA_HASH,
        "sdk_version": TICKFLOW_FREE_SDK_VERSION,
        "sdk_wheel_sha256": TICKFLOW_FREE_SDK_WHEEL_SHA256,
        "mapping": DAILY_SHADOW_MAPPING_HASH,
        "universe_policy": DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        "symbol_set_binding": DAILY_SHADOW_SYMBOL_SET_BINDING_HASH,
        "candidate": DAILY_SHADOW_CANDIDATE_CONTRACT_HASH,
        "reconciliation": DAILY_BAR_RECONCILIATION_POLICY.policy_sha256,
        "retention": DAILY_SHADOW_RETENTION_HASH,
        "request_budget": DAILY_SHADOW_REQUEST_BUDGET_HASH,
        "circuit_policy": DAILY_SHADOW_CIRCUIT_POLICY_HASH,
        "job_lease_policy": DAILY_SHADOW_JOB_LEASE_POLICY_HASH,
        "unit_state": DAILY_SHADOW_UNIT_STATE_HASH,
    },
)
DAILY_SHADOW_TERMS_CONTRACT_VERSION = (
    f"{DAILY_SHADOW_TERMS_CONTRACT_VERSION_PREFIX}-{DAILY_SHADOW_DESCRIPTOR_BASE_HASH}"
)


class DailyShadowRegistryUnavailable(RuntimeError):
    """Sanitized sidecar state failure."""


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class DailyShadowContract(_Frozen):
    provider: Literal["tickflow"] = "tickflow"
    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    free_origin: Literal["https://free-api.tickflow.org"] = _FREE_ORIGIN
    adapter_sha256: str = DAILY_SHADOW_ADAPTER_HASH
    endpoint_contract_sha256: str = DAILY_SHADOW_ENDPOINT_CONTRACT_HASH
    source_schema_sha256: str = DAILY_SHADOW_SOURCE_SCHEMA_HASH
    sdk_version: Literal["0.1.24"] = TICKFLOW_FREE_SDK_VERSION
    sdk_wheel_sha256: str = TICKFLOW_FREE_SDK_WHEEL_SHA256
    mapping_contract_sha256: str = DAILY_SHADOW_MAPPING_HASH
    universe_policy_sha256: str = DAILY_CANONICAL_UNIVERSE_POLICY_SHA256
    symbol_set_binding_sha256: str = DAILY_SHADOW_SYMBOL_SET_BINDING_HASH
    candidate_contract_sha256: str = DAILY_SHADOW_CANDIDATE_CONTRACT_HASH
    reconciliation_policy_sha256: str = DAILY_BAR_RECONCILIATION_POLICY.policy_sha256
    unit_state_sha256: str = DAILY_SHADOW_UNIT_STATE_HASH
    retention_decision_sha256: str = DAILY_SHADOW_RETENTION_HASH
    request_budget_sha256: str = DAILY_SHADOW_REQUEST_BUDGET_HASH
    circuit_policy_sha256: str = DAILY_SHADOW_CIRCUIT_POLICY_HASH
    job_lease_policy_sha256: str = DAILY_SHADOW_JOB_LEASE_POLICY_HASH
    terms_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    units_state: Literal["UNKNOWN"] = "UNKNOWN"
    suspension_semantics_state: Literal["UNKNOWN"] = "UNKNOWN"
    factor_evidence_state: Literal["UNQUALIFIED"] = "UNQUALIFIED"
    raw_retention_semantics_state: Literal["UNKNOWN"] = "UNKNOWN"
    required_sessions: Literal[20] = 20
    observation_mode: Literal["HISTORICAL_SHADOW"] = "HISTORICAL_SHADOW"
    publication_enabled: Literal[False] = False
    failover_enabled: Literal[False] = False
    descriptor_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_contract(self) -> DailyShadowContract:
        exact = {
            "adapter_sha256": DAILY_SHADOW_ADAPTER_HASH,
            "endpoint_contract_sha256": DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
            "source_schema_sha256": DAILY_SHADOW_SOURCE_SCHEMA_HASH,
            "sdk_wheel_sha256": TICKFLOW_FREE_SDK_WHEEL_SHA256,
            "mapping_contract_sha256": DAILY_SHADOW_MAPPING_HASH,
            "universe_policy_sha256": DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
            "symbol_set_binding_sha256": DAILY_SHADOW_SYMBOL_SET_BINDING_HASH,
            "candidate_contract_sha256": DAILY_SHADOW_CANDIDATE_CONTRACT_HASH,
            "reconciliation_policy_sha256": DAILY_BAR_RECONCILIATION_POLICY.policy_sha256,
            "unit_state_sha256": DAILY_SHADOW_UNIT_STATE_HASH,
            "retention_decision_sha256": DAILY_SHADOW_RETENTION_HASH,
            "request_budget_sha256": DAILY_SHADOW_REQUEST_BUDGET_HASH,
            "circuit_policy_sha256": DAILY_SHADOW_CIRCUIT_POLICY_HASH,
            "job_lease_policy_sha256": DAILY_SHADOW_JOB_LEASE_POLICY_HASH,
        }
        if any(getattr(self, key) != value for key, value in exact.items()):
            raise ValueError("Daily shadow contract component mismatch")
        values = self.model_dump(mode="json")
        values.pop("descriptor_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-descriptor/v1", values)
        if self.descriptor_sha256 == "0" * 64:
            object.__setattr__(self, "descriptor_sha256", expected)
        elif self.descriptor_sha256 != expected:
            raise ValueError("Daily shadow descriptor mismatch")
        return self


class DailyWindowBinding(_Frozen):
    calendar_generation: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    terms_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_dates: tuple[date, ...] = Field(min_length=20, max_length=20)
    binding_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_binding(self) -> DailyWindowBinding:
        if self.expected_dates != tuple(sorted(set(self.expected_dates))):
            raise ValueError("Daily shadow expected dates are invalid")
        values = self.model_dump(mode="json")
        values.pop("binding_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-window-binding/v1", values)
        if self.binding_sha256 == "0" * 64:
            object.__setattr__(self, "binding_sha256", expected)
        elif self.binding_sha256 != expected:
            raise ValueError("Daily shadow window binding mismatch")
        return self


class DailyAttemptAudit(_Frozen):
    ordinal: int = Field(ge=0, lt=40)
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    endpoint: Literal["historical_daily_1d"] = "historical_daily_1d"
    attempt: Literal[1] = 1
    outcome: Literal["SUCCESS", "FAILURE", "SKIPPED_CIRCUIT_OPEN", "HALF_OPEN_PROBE"]
    elapsed_ms: int = Field(ge=0)
    response_bytes: int = Field(ge=0, le=8 * 1024 * 1024)
    expected_rows: int = Field(ge=0, le=100)
    observed_rows: int = Field(ge=0, le=100)
    failure_class: str | None = Field(default=None, pattern=r"^[a-z0-9_]{1,64}$")
    audit_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @field_validator("failure_class")
    @classmethod
    def allowlisted_failure_class(cls, value: str | None) -> str | None:
        return validate_daily_failure_class(value)

    @model_validator(mode="after")
    def validate_audit(self) -> DailyAttemptAudit:
        if self.outcome == "SUCCESS":
            if self.failure_class is not None or self.observed_rows != self.expected_rows:
                raise ValueError("Daily success audit mismatch")
        elif self.failure_class is None or self.observed_rows > self.expected_rows:
            raise ValueError("Daily failure audit is not sanitized")
        values = self.model_dump(mode="json")
        values.pop("audit_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-attempt-audit/v1", values)
        if self.audit_sha256 == "0" * 64:
            object.__setattr__(self, "audit_sha256", expected)
        elif self.audit_sha256 != expected:
            raise ValueError("Daily attempt audit hash mismatch")
        return self


class DailySessionSuccess(_Frozen):
    epoch_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    job_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    session_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    session_report_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    attestation_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    trade_date: date
    canonical_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_symbol_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_bundle_ref: str = Field(min_length=1, max_length=256)
    candidate_id: str = Field(pattern=r"^daily-candidate-[0-9a-f]{32}$")
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_bundle_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_bundle_ref: str = Field(min_length=1, max_length=256)
    quality_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reconciliation_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    attempts: tuple[DailyAttemptAudit, ...] = Field(min_length=1, max_length=40)

    @model_validator(mode="after")
    def validate_success(self) -> DailySessionSuccess:
        if (
            tuple(item.ordinal for item in self.attempts) != tuple(range(len(self.attempts)))
            or any(item.outcome != "SUCCESS" for item in self.attempts)
            or any(
                ".." in Path(value).parts or value.startswith(("/", "http"))
                for value in (self.evidence_bundle_ref, self.candidate_bundle_ref)
            )
        ):
            raise ValueError("Daily terminal success graph is invalid")
        return self


class DailySessionFailure(_Frozen):
    epoch_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    job_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    session_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    session_report_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    trade_date: date
    outcome: Literal["FAILURE", "MISMATCH", "UNAVAILABLE", "SKIPPED_CIRCUIT_OPEN"]
    failure_class: str = Field(pattern=r"^[a-z0-9_]{1,64}$")
    attempts: tuple[DailyAttemptAudit, ...] = Field(default=(), max_length=40)

    @field_validator("failure_class")
    @classmethod
    def allowlisted_failure_class(cls, value: str) -> str:
        return validate_daily_failure_class(value)  # type: ignore[return-value]

    @model_validator(mode="after")
    def validate_failure(self) -> DailySessionFailure:
        if tuple(item.ordinal for item in self.attempts) != tuple(range(len(self.attempts))):
            raise ValueError("Daily failure attempt closure is invalid")
        return self


class DailySessionLease(_Frozen):
    outcome: Literal["LEASED", "BUSY", "ALREADY_TERMINAL", "RESET"]
    epoch_id: str
    job_id: str
    session_id: str
    trade_date: date
    owner: str | None = None
    expires_at: datetime | None = None
    state_version: int = Field(ge=0)


class DailyWindowSnapshot(_Frozen):
    epoch_id: str
    state: Literal["OBSERVING", "RESET", "SHADOW_QUALIFIED"]
    consecutive_sessions: int = Field(ge=0, le=20)
    required_sessions: Literal[20] = 20
    first_trade_date: date | None = None
    last_trade_date: date | None = None
    next_trade_date: date | None = None
    state_version: int = Field(ge=0)


class DailyShadowStatus(_Frozen):
    status: Literal["READY", "UNAVAILABLE"]
    window: DailyWindowSnapshot | None = None
    epoch_count: int = Field(ge=0)
    session_report_count: int = Field(ge=0)
    descriptor_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    terms_evidence_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    calendar_generation: str | None = None
    calendar_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    expected_dates: tuple[date, ...] = ()
    last_outcome: (
        Literal[
            "SUCCESS",
            "FAILURE",
            "MISMATCH",
            "UNAVAILABLE",
            "SKIPPED_CIRCUIT_OPEN",
        ]
        | None
    ) = None
    circuit_state: Literal["CLOSED", "OPEN", "HALF_OPEN"] | None = None
    observation_mode: Literal["HISTORICAL_SHADOW"] = "HISTORICAL_SHADOW"
    publication_enabled: Literal[False] = False
    failover_enabled: Literal[False] = False
    reason: str | None = None

    @model_validator(mode="after")
    def validate_status(self) -> DailyShadowStatus:
        ready_identity = (
            self.descriptor_sha256,
            self.terms_evidence_sha256,
            self.circuit_state,
        )
        window_identity = (
            self.version_vector_sha256,
            self.calendar_generation,
            self.calendar_sha256,
        )
        if self.status == "READY":
            if any(value is None for value in ready_identity) or self.reason is not None:
                raise ValueError("Daily shadow ready status is incomplete")
            if self.window is None:
                if any(value is not None for value in window_identity) or self.expected_dates:
                    raise ValueError("Daily shadow pending status is inconsistent")
            elif any(value is None for value in window_identity) or len(self.expected_dates) != 20:
                raise ValueError("Daily shadow window status is incomplete")
        elif (
            self.window is not None
            or any(value is not None for value in ready_identity + window_identity)
            or self.expected_dates
            or self.last_outcome is not None
            or self.reason is None
        ):
            raise ValueError("Daily shadow unavailable status is inconsistent")
        return self


class DailyCircuitAction(StrEnum):
    RUN_FULL_SESSION = "RUN_FULL_SESSION"
    SKIPPED_CIRCUIT_OPEN = "SKIPPED_CIRCUIT_OPEN"
    SKIPPED_HALF_OPEN = "SKIPPED_HALF_OPEN"
    RUN_FIXED_FIVE_HALF_OPEN_PROBE = "RUN_FIXED_FIVE_HALF_OPEN_PROBE"


class DailyCircuitDecision(_Frozen):
    action: DailyCircuitAction
    state: Literal["CLOSED", "OPEN", "HALF_OPEN"]
    probe_lease_id: str | None = None
    max_attempts: Literal[1] = 1
    zero_write: bool
    ends_slot: bool

    @model_validator(mode="after")
    def validate_decision(self) -> DailyCircuitDecision:
        full = self.action == DailyCircuitAction.RUN_FULL_SESSION
        if full == self.zero_write or full == self.ends_slot:
            raise ValueError("Daily circuit action contract mismatch")
        if (self.action == DailyCircuitAction.RUN_FIXED_FIVE_HALF_OPEN_PROBE) != (
            self.probe_lease_id is not None
        ):
            raise ValueError("Daily circuit probe identity mismatch")
        return self


class DailyCircuitSnapshot(_Frozen):
    state: Literal["CLOSED", "OPEN", "HALF_OPEN"]
    consecutive_failures: int = Field(ge=0)
    cooldown_until: datetime | None = None
    probe_lease_id: str | None = None
    state_version: int = Field(ge=0)


def _physical(path: Path) -> Path:
    absolute = Path(os.path.abspath(path))
    if absolute.parts[1:2] == ("tmp",) and Path("/tmp").is_symlink():
        return Path("/private/tmp", *absolute.parts[2:])
    if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
        return Path("/private/var", *absolute.parts[2:])
    return absolute


def _safe_id(value: Any) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise DailyShadowRegistryUnavailable("daily shadow identity unavailable")
    return value


def _safe_sha(value: Any) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise DailyShadowRegistryUnavailable("daily shadow hash unavailable")
    return value


def _validate_parent(parent: Path) -> int:
    if not parent.is_absolute():
        raise DailyShadowRegistryUnavailable("daily shadow path unavailable")
    fds: list[int] = []
    try:
        current = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        fds.append(current)
        for component in _physical(parent).parts[1:]:
            current = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=current,
            )
            fds.append(current)
        info = os.fstat(current)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o777 != 0o700
        ):
            raise DailyShadowRegistryUnavailable("daily shadow path permissions unavailable")
        return os.dup(current)
    except OSError as exc:
        raise DailyShadowRegistryUnavailable("daily shadow path unavailable") from exc
    finally:
        for descriptor in reversed(fds):
            os.close(descriptor)


def _open_or_create_private(parent_fd: int, name: str) -> int:
    if not name or "/" in name or name in {".", ".."}:
        raise DailyShadowRegistryUnavailable("daily shadow path unavailable")
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        try:
            descriptor = os.open(name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            descriptor = os.open(
                name,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=parent_fd,
            )
    except OSError as exc:
        raise DailyShadowRegistryUnavailable("daily shadow file unavailable") from exc
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o777 != 0o600
        or info.st_nlink != 1
    ):
        os.close(descriptor)
        raise DailyShadowRegistryUnavailable("daily shadow file permissions unavailable")
    return descriptor


def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _validate_database_bound(connection: sqlite3.Connection) -> None:
    page_count = connection.execute("PRAGMA page_count").fetchone()[0]
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    if (
        type(page_count) is not int
        or type(page_size) is not int
        or page_count < 1
        or page_size < 1
        or page_count * page_size > _MAX_DATABASE_BYTES
    ):
        raise DailyShadowRegistryUnavailable("daily shadow database bound unavailable")


def _window_from_row(row: sqlite3.Row) -> DailyWindowSnapshot:
    return DailyWindowSnapshot(
        epoch_id=row["epoch_id"],
        state=row["window_state"],
        consecutive_sessions=row["consecutive_sessions"],
        first_trade_date=row["first_trade_date"],
        last_trade_date=row["last_trade_date"],
        next_trade_date=row["next_trade_date"],
        state_version=row["state_version"],
    )


def _blob(value: Any) -> bytes:
    if not isinstance(value, bytes) or not value or len(value) > MAX_CANONICAL_BYTES:
        raise DailyShadowRegistryUnavailable("daily shadow control graph unavailable")
    return value


def _validate_terms_row(row: sqlite3.Row) -> None:
    raw = _blob(row["canonical_json"])
    try:
        values = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DailyShadowRegistryUnavailable("daily shadow terms graph unavailable") from exc
    required = {
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
    }
    if (
        not isinstance(values, dict)
        or set(values) != required
        or (json.dumps(values, ensure_ascii=False, separators=(",", ":")) + "\n").encode() != raw
        or hashlib.sha256(b"stock-eva/r2f3/terms-evidence/v1\n" + raw).hexdigest()
        != row["terms_evidence_sha256"]
        or values["terms_evidence_id"] != row["terms_evidence_id"]
        or values["review_id"] != row["review_id"]
        or values["contract_version"] != row["contract_version"]
        or values["content_bytes_sha256"] != row["content_bytes_sha256"]
        or values["provider_id"] != "tickflow"
        or values["contract_version"] != DAILY_SHADOW_TERMS_CONTRACT_VERSION
        or values["official_url_allowlist_json"] != '["https://free-api.tickflow.org"]'
        or values["approved_intended_use"] != _INTENDED_USE
        or values["approved_retention"] != _RETENTION
        or values["approved_credential_mode"] != _CREDENTIAL_MODE
        or values["approved_quota_decision"] != _QUOTA_DECISION
    ):
        raise DailyShadowRegistryUnavailable("daily shadow terms graph unavailable")


def _validate_session_report(row: sqlite3.Row) -> None:
    if row["outcome"] == "SUCCESS":
        values = {
            "session_report_id": row["session_report_id"],
            "evidence_id": row["evidence_id"],
            "evidence_sha256": row["evidence_sha256"],
            "candidate_id": row["candidate_id"],
            "candidate_sha256": row["candidate_sha256"],
            "reconciliation_report_sha256": row["reconciliation_report_sha256"],
            "epoch_id": row["epoch_id"],
            "job_id": row["job_id"],
            "session_id": row["session_id"],
            "trade_date": row["trade_date"],
            "canonical_snapshot_sha256": row["canonical_snapshot_sha256"],
            "canonical_symbol_set_sha256": row["canonical_symbol_set_sha256"],
            "request_plan_sha256": row["request_plan_sha256"],
            "completion_sha256": row["completion_sha256"],
            "terminal_attestation_id": row["terminal_attestation_id"],
            "outcome": "SUCCESS",
        }
    else:
        values = {
            "session_report_id": row["session_report_id"],
            "epoch_id": row["epoch_id"],
            "job_id": row["job_id"],
            "session_id": row["session_id"],
            "trade_date": row["trade_date"],
            "canonical_symbol_set_sha256": row["canonical_symbol_set_sha256"],
            "outcome": row["outcome"],
            "failure_class": row["failure_class"],
        }
    raw = _blob(row["report_json"])
    if raw != schema_json_bytes(values) or row["report_sha256"] != sha256_bytes(raw):
        raise DailyShadowRegistryUnavailable("daily shadow session graph unavailable")


def _validate_terminal_attestation(connection: sqlite3.Connection, row: sqlite3.Row) -> None:
    report = connection.execute(
        "SELECT * FROM daily_shadow_session_report WHERE session_report_id=?",
        (row["session_report_id"],),
    ).fetchone()
    job = connection.execute(
        "SELECT * FROM daily_shadow_job WHERE job_id=?", (row["job_id"],)
    ).fetchone()
    evidence = connection.execute(
        "SELECT * FROM daily_shadow_evidence_ref WHERE evidence_id=?",
        (row["evidence_id"],),
    ).fetchone()
    candidate = connection.execute(
        "SELECT * FROM daily_shadow_candidate_ref WHERE candidate_id=?",
        (row["candidate_id"],),
    ).fetchone()
    audits = connection.execute(
        "SELECT * FROM daily_shadow_attempt_audit WHERE job_id=? ORDER BY ordinal",
        (row["job_id"],),
    ).fetchall()
    if (
        report is None
        or job is None
        or evidence is None
        or candidate is None
        or not audits
        or tuple(item["ordinal"] for item in audits) != tuple(range(len(audits)))
        or any(item["outcome"] != "SUCCESS" for item in audits)
        or report["outcome"] != "SUCCESS"
        or job["run_status"] != "COMPLETED"
        or len(
            {
                row["attestation_id"],
                report["terminal_attestation_id"],
                job["terminal_attestation_id"],
            }
        )
        != 1
        or (
            row["epoch_id"],
            row["job_id"],
            row["session_id"],
        )
        != (report["epoch_id"], report["job_id"], report["session_id"])
        or (row["epoch_id"], row["session_id"]) != (job["epoch_id"], job["session_id"])
        or len({row["evidence_id"], report["evidence_id"], evidence["evidence_id"]}) != 1
        or len({row["candidate_id"], report["candidate_id"], candidate["candidate_id"]}) != 1
        or evidence["evidence_id"] != candidate["evidence_id"]
        or report["trade_date"] != job["trade_date"]
        or report["canonical_snapshot_sha256"] != job["canonical_snapshot_sha256"]
        or len(
            {
                row["canonical_symbol_set_sha256"],
                report["canonical_symbol_set_sha256"],
                job["canonical_symbol_set_sha256"],
                candidate["canonical_symbol_set_sha256"],
            }
        )
        != 1
        or report["request_plan_sha256"] != job["request_plan_sha256"]
        or report["completion_sha256"] != evidence["completion_sha256"]
        or report["evidence_sha256"] != evidence["evidence_sha256"]
        or report["candidate_sha256"] != candidate["candidate_sha256"]
        or report["reconciliation_report_sha256"] != candidate["reconciliation_report_sha256"]
    ):
        raise DailyShadowRegistryUnavailable("daily terminal graph unavailable")
    request_plan_json = schema_json_bytes(
        {
            "job_id": job["job_id"],
            "trade_date": job["trade_date"],
            "request_plan_sha256": job["request_plan_sha256"],
            "canonical_symbol_set_sha256": job["canonical_symbol_set_sha256"],
        }
    )
    completion_json = schema_json_bytes(
        {
            "evidence_id": evidence["evidence_id"],
            "completion_sha256": evidence["completion_sha256"],
        }
    )
    attempt_json = schema_json_bytes(
        tuple({"ordinal": item["ordinal"], "audit_sha256": item["audit_sha256"]} for item in audits)
    )
    report_graph_json = schema_json_bytes(
        {
            "session_report_id": report["session_report_id"],
            "evidence_id": report["evidence_id"],
            "evidence_sha256": report["evidence_sha256"],
            "candidate_id": report["candidate_id"],
            "candidate_sha256": report["candidate_sha256"],
            "canonical_symbol_set_sha256": report["canonical_symbol_set_sha256"],
            "reconciliation_report_sha256": report["reconciliation_report_sha256"],
        }
    )
    payloads = (
        request_plan_json,
        completion_json,
        attempt_json,
        report_graph_json,
    )
    hashes = tuple(sha256_bytes(value) for value in payloads)
    persisted = (
        _blob(row["request_plan_json"]),
        _blob(row["completion_json"]),
        _blob(row["attempt_closure_json"]),
        _blob(row["report_graph_json"]),
    )
    persisted_hashes = (
        row["request_plan_sha256"],
        row["completion_sha256"],
        row["attempt_closure_sha256"],
        row["report_graph_sha256"],
    )
    if (
        persisted != payloads
        or persisted_hashes != hashes
        or daily_validate_terminal_graph(
            *tuple(item for pair in zip(persisted, persisted_hashes, strict=True) for item in pair)
        )
        != 1
        or row["attestation_sha256"]
        != domain_sha256("stock-eva/r2f3/free-daily-terminal-attestation/v1", hashes)
    ):
        raise DailyShadowRegistryUnavailable("daily terminal graph unavailable")


def _validate_control_graph(connection: sqlite3.Connection) -> None:
    terms_rows = connection.execute("SELECT * FROM daily_shadow_terms_evidence").fetchall()
    for row in terms_rows:
        _validate_terms_row(row)
    terms_hashes = {row["terms_evidence_sha256"] for row in terms_rows}
    contracts = connection.execute("SELECT * FROM daily_shadow_contract").fetchall()
    for row in contracts:
        raw = _blob(row["descriptor_json"])
        try:
            contract = DailyShadowContract.model_validate_json(raw)
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow contract graph unavailable") from exc
        if (
            raw != canonical_json_bytes(contract.model_dump(mode="json"))
            or row["descriptor_sha256"] != contract.descriptor_sha256
            or row["terms_evidence_sha256"] != contract.terms_evidence_sha256
            or contract.terms_evidence_sha256 not in terms_hashes
        ):
            raise DailyShadowRegistryUnavailable("daily shadow contract graph unavailable")
    contract_terms = {row["terms_evidence_sha256"] for row in contracts}
    epochs = connection.execute(
        "SELECT * FROM daily_shadow_epoch ORDER BY epoch_ordinal"
    ).fetchall()
    epoch_dates: dict[str, tuple[str, ...]] = {}
    for row in epochs:
        raw = _blob(row["expected_dates_json"])
        try:
            values = tuple(json.loads(raw.decode("utf-8")))
            parsed = tuple(date.fromisoformat(item) for item in values)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise DailyShadowRegistryUnavailable("daily shadow epoch graph unavailable") from exc
        expected_sha = domain_sha256("stock-eva/r2f3/free-daily-expected-dates/v1", values)
        if (
            len(values) != _REQUIRED_SESSIONS
            or parsed != tuple(sorted(set(parsed)))
            or raw != schema_json_bytes(values)
            or row["expected_dates_sha256"] != expected_sha
            or row["terms_evidence_sha256"] not in contract_terms
        ):
            raise DailyShadowRegistryUnavailable("daily shadow epoch graph unavailable")
        epoch_dates[row["epoch_id"]] = values
    attempts = connection.execute(
        "SELECT * FROM daily_shadow_attempt_audit ORDER BY job_id,ordinal"
    ).fetchall()
    for row in attempts:
        try:
            DailyAttemptAudit(
                ordinal=row["ordinal"],
                request_id=row["request_id"],
                endpoint=row["endpoint"],
                attempt=row["attempt"],
                outcome=row["outcome"],
                elapsed_ms=row["elapsed_ms"],
                response_bytes=row["response_bytes"],
                expected_rows=row["expected_rows"],
                observed_rows=row["observed_rows"],
                failure_class=row["failure_class"],
                audit_sha256=row["audit_sha256"],
            )
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow attempt graph unavailable") from exc
    reports = connection.execute(
        "SELECT * FROM daily_shadow_session_report ORDER BY epoch_id,trade_date"
    ).fetchall()
    for row in reports:
        _validate_session_report(row)
    for epoch in epochs:
        dates = epoch_dates[epoch["epoch_id"]]
        epoch_reports = tuple(row for row in reports if row["epoch_id"] == epoch["epoch_id"])
        successes = tuple(row["trade_date"] for row in epoch_reports if row["outcome"] == "SUCCESS")
        failures = tuple(row for row in epoch_reports if row["outcome"] != "SUCCESS")
        if (
            successes != dates[: len(successes)]
            or len(successes) > _REQUIRED_SESSIONS
            or len(failures) > 1
            or (
                failures
                and (
                    len(successes) >= _REQUIRED_SESSIONS
                    or failures[0]["trade_date"] != dates[len(successes)]
                )
            )
            or (epoch["epoch_state"] == "SHADOW_QUALIFIED" and len(successes) != _REQUIRED_SESSIONS)
        ):
            raise DailyShadowRegistryUnavailable("daily shadow epoch report graph unavailable")
    jobs = connection.execute("SELECT * FROM daily_shadow_job").fetchall()
    jobs_by_id = {row["job_id"]: row for row in jobs}
    evidence_rows = connection.execute("SELECT * FROM daily_shadow_evidence_ref").fetchall()
    candidate_rows = connection.execute("SELECT * FROM daily_shadow_candidate_ref").fetchall()
    attestations = connection.execute("SELECT * FROM daily_shadow_terminal_attestation").fetchall()
    success_reports = tuple(row for row in reports if row["outcome"] == "SUCCESS")
    if (
        len(success_reports) != len(evidence_rows)
        or len(success_reports) != len(candidate_rows)
        or len(success_reports) != len(attestations)
        or {row["evidence_id"] for row in success_reports}
        != {row["evidence_id"] for row in evidence_rows}
        or {row["candidate_id"] for row in success_reports}
        != {row["candidate_id"] for row in candidate_rows}
        or {row["terminal_attestation_id"] for row in success_reports}
        != {row["attestation_id"] for row in attestations}
    ):
        raise DailyShadowRegistryUnavailable("daily shadow reference graph unavailable")
    terminal_status = {
        "SUCCESS": "COMPLETED",
        "FAILURE": "FAILURE",
        "MISMATCH": "MISMATCH",
        "UNAVAILABLE": "UNAVAILABLE",
        "SKIPPED_CIRCUIT_OPEN": "SKIPPED",
    }
    for report in reports:
        job = jobs_by_id.get(report["job_id"])
        job_attempts = tuple(row for row in attempts if row["job_id"] == report["job_id"])
        if (
            job is None
            or (report["epoch_id"], report["session_id"], report["trade_date"])
            != (job["epoch_id"], job["session_id"], job["trade_date"])
            or report["canonical_snapshot_sha256"] != job["canonical_snapshot_sha256"]
            or report["canonical_symbol_set_sha256"] != job["canonical_symbol_set_sha256"]
            or report["request_plan_sha256"] != job["request_plan_sha256"]
            or job["run_status"] != terminal_status[report["outcome"]]
            or tuple(item["ordinal"] for item in job_attempts) != tuple(range(len(job_attempts)))
        ):
            raise DailyShadowRegistryUnavailable("daily shadow job graph unavailable")
        if report["outcome"] != "SUCCESS":
            try:
                DailySessionFailure(
                    epoch_id=report["epoch_id"],
                    job_id=report["job_id"],
                    session_id=report["session_id"],
                    session_report_id=report["session_report_id"],
                    trade_date=report["trade_date"],
                    outcome=report["outcome"],
                    failure_class=report["failure_class"],
                    attempts=tuple(
                        DailyAttemptAudit(
                            ordinal=item["ordinal"],
                            request_id=item["request_id"],
                            endpoint=item["endpoint"],
                            attempt=item["attempt"],
                            outcome=item["outcome"],
                            elapsed_ms=item["elapsed_ms"],
                            response_bytes=item["response_bytes"],
                            expected_rows=item["expected_rows"],
                            observed_rows=item["observed_rows"],
                            failure_class=item["failure_class"],
                            audit_sha256=item["audit_sha256"],
                        )
                        for item in job_attempts
                    ),
                )
            except Exception as exc:
                raise DailyShadowRegistryUnavailable(
                    "daily shadow failure graph unavailable"
                ) from exc
    report_jobs = {row["job_id"] for row in reports}
    for job in jobs:
        job_attempts = tuple(row for row in attempts if row["job_id"] == job["job_id"])
        report_count = sum(row["job_id"] == job["job_id"] for row in reports)
        if (
            (job["run_status"] == "LEASED") != (job["job_id"] not in report_jobs)
            or (job["run_status"] == "LEASED" and job_attempts)
            or (job["run_status"] != "LEASED" and report_count != 1)
        ):
            raise DailyShadowRegistryUnavailable("daily shadow job closure unavailable")
    for row in attestations:
        _validate_terminal_attestation(connection, row)
    circuit_events = connection.execute(
        "SELECT * FROM daily_shadow_circuit_event ORDER BY rowid"
    ).fetchall()
    prior_circuit_state = "CLOSED"
    for row in circuit_events:
        try:
            validate_daily_failure_class(row["failure_class"])
        except ValueError as exc:
            raise DailyShadowRegistryUnavailable("daily circuit graph unavailable") from exc
        values = {
            "event_id": row["event_id"],
            "endpoint": row["endpoint"],
            "event_type": row["event_type"],
            "observed_at": row["observed_at"],
            "state_before": row["state_before"],
            "state_after": row["state_after"],
            "failure_class": row["failure_class"],
        }
        legal_event = {
            "FAILURE": (
                row["failure_class"] is not None and row["state_after"] in {"CLOSED", "OPEN"}
            ),
            "SUCCESS": (
                row["failure_class"] is None
                and row["state_before"] == row["state_after"] == "CLOSED"
            ),
            "HALF_OPEN_LEASED": (
                row["failure_class"] is None
                and row["state_before"] == "OPEN"
                and row["state_after"] == "HALF_OPEN"
            ),
            "PROBE_SUCCESS": (
                row["failure_class"] is None
                and row["state_before"] == "HALF_OPEN"
                and row["state_after"] == "CLOSED"
            ),
            "PROBE_FAILURE": (
                row["failure_class"] == "probe_failure"
                and row["state_before"] == "HALF_OPEN"
                and row["state_after"] == "OPEN"
            ),
        }.get(row["event_type"], False)
        legal_chain = row["state_before"] == prior_circuit_state or (
            prior_circuit_state == "HALF_OPEN"
            and row["event_type"] == "HALF_OPEN_LEASED"
            and row["state_before"] == "OPEN"
        )
        if (
            not legal_event
            or not legal_chain
            or row["event_sha256"]
            != domain_sha256("stock-eva/r2f3/free-daily-circuit-event/v1", values)
        ):
            raise DailyShadowRegistryUnavailable("daily circuit graph unavailable")
        prior_circuit_state = row["state_after"]
    circuits = connection.execute("SELECT * FROM daily_shadow_circuit").fetchall()
    if len(circuits) != 1:
        raise DailyShadowRegistryUnavailable("daily circuit graph unavailable")
    circuit = circuits[0]
    try:
        snapshot = DailyShadowRegistry._circuit_from_row(circuit)
        for value in (
            circuit["opened_at"],
            circuit["cooldown_until"],
            circuit["probe_expires_at"],
        ):
            if value is not None and datetime.fromisoformat(value).utcoffset() is None:
                raise ValueError("naive circuit time")
    except Exception as exc:
        raise DailyShadowRegistryUnavailable("daily circuit graph unavailable") from exc
    if (
        snapshot.state != prior_circuit_state
        or (snapshot.state == "CLOSED" and snapshot.consecutive_failures >= _FAILURE_THRESHOLD)
        or (
            snapshot.state in {"OPEN", "HALF_OPEN"}
            and snapshot.consecutive_failures < _FAILURE_THRESHOLD
        )
        or snapshot.state_version < len(circuit_events)
    ):
        raise DailyShadowRegistryUnavailable("daily circuit graph unavailable")
    window = connection.execute("SELECT * FROM daily_shadow_window").fetchone()
    if window is not None:
        epoch = connection.execute(
            "SELECT * FROM daily_shadow_epoch WHERE epoch_id=?", (window["epoch_id"],)
        ).fetchone()
        dates = epoch_dates.get(window["epoch_id"], ())
        successes = tuple(
            row["trade_date"]
            for row in reports
            if row["epoch_id"] == window["epoch_id"] and row["outcome"] == "SUCCESS"
        )
        count = window["consecutive_sessions"]
        expected_state = {
            "ACTIVE": "OBSERVING",
            "RESET": "RESET",
            "SHADOW_QUALIFIED": "SHADOW_QUALIFIED",
        }.get(epoch["epoch_state"] if epoch is not None else "")
        is_reset = window["window_state"] == "RESET"
        expected_count = 0 if is_reset else len(successes)
        expected_first = dates[0] if expected_count else None
        expected_last = dates[expected_count - 1] if expected_count else None
        expected_next = (
            dates[expected_count] if not is_reset and expected_count < _REQUIRED_SESSIONS else None
        )
        if (
            epoch is None
            or window["window_state"] != expected_state
            or count != expected_count
            or window["first_trade_date"] != expected_first
            or window["last_trade_date"] != expected_last
            or window["next_trade_date"] != expected_next
            or (window["window_state"] == "SHADOW_QUALIFIED" and count != _REQUIRED_SESSIONS)
        ):
            raise DailyShadowRegistryUnavailable("daily shadow window graph unavailable")


class DailyShadowRegistryReader:
    def __init__(
        self,
        path: Path | str,
        *,
        evidence_root: Path | str | None = None,
        candidate_root: Path | str | None = None,
        verify_external: bool = True,
    ):
        self.path = _physical(Path(path))
        self.lock_path = self.path.with_name(self.path.name + ".lock")
        self.evidence_root = _physical(Path(evidence_root)) if evidence_root is not None else None
        self.candidate_root = (
            _physical(Path(candidate_root)) if candidate_root is not None else None
        )
        self.verify_external = verify_external

    def _validate_external_bundles(self, refs: tuple[dict[str, Any], ...]) -> None:
        if not refs or not self.verify_external:
            return
        if self.evidence_root is None or self.candidate_root is None:
            raise DailyShadowRegistryUnavailable("daily shadow bundle roots unavailable")
        from .daily_shadow_candidates import DailyCandidateReader
        from .shadow_evidence import ShadowEvidenceReader

        evidence_reader = ShadowEvidenceReader(self.evidence_root)
        candidate_reader = DailyCandidateReader(self.candidate_root)
        for ref in refs:
            evidence, _descriptor = evidence_reader.read_descriptor(ref["evidence_id"])
            candidate = candidate_reader.read(ref["candidate_id"])
            if (
                ref["evidence_bundle_ref"] != f"bundles/{ref['evidence_id']}"
                or ref["candidate_bundle_ref"] != f"daily-candidates/{ref['candidate_id']}"
                or evidence.completion_sha256 != ref["completion_sha256"]
                or evidence.manifest_sha256 != ref["evidence_sha256"]
                or evidence.manifest_sha256 != ref["evidence_bundle_sha256"]
                or candidate.candidate.candidate_sha256 != ref["candidate_sha256"]
                or candidate.candidate.canonical_symbol_set_sha256
                != ref["canonical_symbol_set_sha256"]
                or candidate.bundle_sha256 != ref["candidate_bundle_sha256"]
                or candidate.quality.report_sha256 != ref["quality_report_sha256"]
                or candidate.reconciliation.report_sha256 != ref["reconciliation_report_sha256"]
                or candidate.candidate.evidence_id != evidence.evidence_id
                or candidate.candidate.evidence_sha256 != evidence.manifest_sha256
                or candidate.candidate.completion_sha256 != evidence.completion_sha256
            ):
                raise DailyShadowRegistryUnavailable("daily shadow bundle graph unavailable")

    def _read_or_raise(self) -> DailyShadowStatus:
        parent_fd = _validate_parent(self.path.parent)
        lock_fd: int | None = None
        database_fd: int | None = None
        try:
            lock_fd = os.open(
                self.lock_path.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            lock_info = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(lock_info.st_mode)
                or lock_info.st_uid != os.getuid()
                or lock_info.st_mode & 0o777 != 0o600
                or lock_info.st_nlink != 1
            ):
                raise DailyShadowRegistryUnavailable("daily shadow reader unavailable")
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise DailyShadowRegistryUnavailable("daily shadow reader locked") from exc
                raise
            if any(
                self.path.with_name(self.path.name + suffix).exists()
                for suffix in ("-wal", "-journal")
            ):
                raise DailyShadowRegistryUnavailable("daily shadow journal unavailable")
            database_fd = os.open(
                self.path.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            before = os.fstat(database_fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_mode & 0o777 != 0o600
                or before.st_nlink != 1
                or before.st_size < 1
                or before.st_size > _MAX_DATABASE_BYTES
            ):
                raise DailyShadowRegistryUnavailable("daily shadow reader unavailable")
            payload = os.read(database_fd, _MAX_DATABASE_BYTES + 1)
            after = os.fstat(database_fd)
            if len(payload) != before.st_size or _fingerprint(before) != _fingerprint(after):
                raise DailyShadowRegistryUnavailable("daily shadow database changed")
            if not payload.startswith(b"SQLite format 3"):
                raise DailyShadowRegistryUnavailable("daily shadow schema unavailable")
            memory = sqlite3.connect(":memory:")
            memory.row_factory = sqlite3.Row
            try:
                if not hasattr(memory, "deserialize"):
                    raise DailyShadowRegistryUnavailable("daily shadow reader unavailable")
                memory.deserialize(payload)
                memory.execute("PRAGMA query_only=ON")
                memory.execute("PRAGMA foreign_keys=ON")
                validate_daily_shadow_schema(memory)
                _validate_control_graph(memory)
                window_row = memory.execute(
                    "SELECT * FROM daily_shadow_window WHERE provider='tickflow' AND profile=?",
                    (DAILY_SHADOW_PROFILE,),
                ).fetchone()
                epoch_count = memory.execute("SELECT count(*) FROM daily_shadow_epoch").fetchone()[
                    0
                ]
                report_count = memory.execute(
                    "SELECT count(*) FROM daily_shadow_session_report"
                ).fetchone()[0]
                if window_row is None:
                    contracts = memory.execute(
                        "SELECT descriptor_sha256,terms_evidence_sha256 "
                        "FROM daily_shadow_contract WHERE provider='tickflow' AND profile=?",
                        (DAILY_SHADOW_PROFILE,),
                    ).fetchall()
                    if len(contracts) != 1:
                        raise DailyShadowRegistryUnavailable(
                            "daily shadow contract selection unavailable"
                        )
                    contract_row = contracts[0]
                    version_vector_sha256 = None
                    calendar_generation = None
                    calendar_sha256 = None
                    expected_dates = ()
                    last_outcome = None
                    external_refs: tuple[dict[str, Any], ...] = ()
                else:
                    epoch = memory.execute(
                        "SELECT version_vector_sha256,terms_evidence_sha256,"
                        "calendar_generation,calendar_sha256,expected_dates_json "
                        "FROM daily_shadow_epoch WHERE epoch_id=?",
                        (window_row["epoch_id"],),
                    ).fetchone()
                    contract_row = memory.execute(
                        "SELECT descriptor_sha256,terms_evidence_sha256 "
                        "FROM daily_shadow_contract WHERE provider='tickflow' AND profile=? "
                        "AND terms_evidence_sha256=?",
                        (DAILY_SHADOW_PROFILE, epoch["terms_evidence_sha256"]),
                    ).fetchone()
                    if contract_row is None:
                        raise DailyShadowRegistryUnavailable(
                            "daily shadow contract selection unavailable"
                        )
                    version_vector_sha256 = epoch["version_vector_sha256"]
                    calendar_generation = epoch["calendar_generation"]
                    calendar_sha256 = epoch["calendar_sha256"]
                    expected_dates = tuple(
                        date.fromisoformat(item)
                        for item in json.loads(bytes(epoch["expected_dates_json"]).decode())
                    )
                    report = memory.execute(
                        "SELECT outcome FROM daily_shadow_session_report WHERE epoch_id=? "
                        "ORDER BY rowid DESC LIMIT 1",
                        (window_row["epoch_id"],),
                    ).fetchone()
                    last_outcome = report["outcome"] if report is not None else None
                    rows = memory.execute(
                        "SELECT e.evidence_id,e.completion_sha256,e.evidence_sha256,"
                        "e.bundle_ref AS evidence_bundle_ref,"
                        "e.bundle_sha256 AS evidence_bundle_sha256,"
                        "c.candidate_id,c.candidate_sha256,"
                        "c.canonical_symbol_set_sha256,"
                        "c.bundle_ref AS candidate_bundle_ref,"
                        "c.bundle_sha256 AS candidate_bundle_sha256,"
                        "c.quality_report_sha256,c.reconciliation_report_sha256 "
                        "FROM daily_shadow_session_report r "
                        "JOIN daily_shadow_evidence_ref e ON e.evidence_id=r.evidence_id "
                        "JOIN daily_shadow_candidate_ref c ON c.candidate_id=r.candidate_id "
                        "WHERE r.epoch_id=? AND r.outcome='SUCCESS' "
                        "ORDER BY r.trade_date LIMIT 21",
                        (window_row["epoch_id"],),
                    ).fetchall()
                    if len(rows) > _REQUIRED_SESSIONS:
                        raise DailyShadowRegistryUnavailable(
                            "daily shadow bundle graph unavailable"
                        )
                    external_refs = tuple(dict(row) for row in rows)
                circuit = memory.execute(
                    "SELECT state FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
                ).fetchone()
                if circuit is None:
                    raise DailyShadowRegistryUnavailable("daily shadow circuit unavailable")
            finally:
                memory.close()
            self._validate_external_bundles(external_refs)
            return DailyShadowStatus(
                status="READY",
                window=_window_from_row(window_row) if window_row is not None else None,
                epoch_count=epoch_count,
                session_report_count=report_count,
                descriptor_sha256=contract_row["descriptor_sha256"],
                terms_evidence_sha256=contract_row["terms_evidence_sha256"],
                version_vector_sha256=version_vector_sha256,
                calendar_generation=calendar_generation,
                calendar_sha256=calendar_sha256,
                expected_dates=expected_dates,
                last_outcome=last_outcome,
                circuit_state=circuit["state"],
            )
        except (DailyShadowRegistryUnavailable, DailyShadowSchemaError):
            raise
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow reader unavailable") from exc
        finally:
            if database_fd is not None:
                os.close(database_fd)
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
            os.close(parent_fd)

    def read(self) -> DailyShadowStatus:
        try:
            return self._read_or_raise()
        except Exception:
            return DailyShadowStatus(
                status="UNAVAILABLE",
                epoch_count=0,
                session_report_count=0,
                reason="CONTROL_STATE_UNAVAILABLE",
            )


class DailyShadowRegistry:
    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        candidate = Path(path)
        if not candidate.is_absolute() or candidate.name in {"", ".", ".."}:
            raise DailyShadowRegistryUnavailable("daily shadow path unavailable")
        self.path = _physical(candidate)
        self.lock_path = self.path.with_name(self.path.name + ".lock")
        self._clock = clock or (lambda: datetime.now(UTC))

    @contextmanager
    def _lock(self, *, shared: bool = False) -> Iterator[None]:
        parent_fd = _validate_parent(self.path.parent)
        try:
            lock_fd = os.open(
                self.lock_path.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            os.close(parent_fd)
            raise DailyShadowRegistryUnavailable("daily shadow lock unavailable") from exc
        try:
            info = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o777 != 0o600
                or info.st_nlink != 1
            ):
                raise DailyShadowRegistryUnavailable("daily shadow lock unavailable")
            fcntl.flock(lock_fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            yield
        except OSError as exc:
            raise DailyShadowRegistryUnavailable("daily shadow lock unavailable") from exc
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
                os.close(parent_fd)

    @contextmanager
    def _bound_writer(self) -> Iterator[sqlite3.Connection]:
        parent_fd = _validate_parent(self.path.parent)
        database_fd: int | None = None
        alias_fd: int | None = None
        alias_name = f".{self.path.name}.daily-bound-{secrets.token_hex(12)}"
        alias_path = self.path.parent / alias_name
        connection: sqlite3.Connection | None = None
        try:
            database_fd = os.open(
                self.path.name,
                os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            before = os.fstat(database_fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_mode & 0o777 != 0o600
                or before.st_nlink != 1
                or before.st_size > _MAX_DATABASE_BYTES
            ):
                raise DailyShadowRegistryUnavailable("daily shadow database unavailable")
            os.link(
                self.path.name,
                alias_name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
            alias_fd = os.open(
                alias_name,
                os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            alias_info = os.fstat(alias_fd)
            if (alias_info.st_dev, alias_info.st_ino) != (
                before.st_dev,
                before.st_ino,
            ) or (
                alias_info.st_uid != os.getuid()
                or alias_info.st_mode & 0o777 != 0o600
                or alias_info.st_nlink != 2
            ):
                raise DailyShadowRegistryUnavailable("daily shadow database identity unavailable")
            connection = sqlite3.connect(alias_path)
            connection.row_factory = sqlite3.Row
            configure_daily_connection(connection)
            yield connection
            connection.close()
            connection = None
            after = os.fstat(database_fd)
            path_info = os.stat(self.path, follow_symlinks=False)
            if (
                (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                or (
                    path_info.st_dev,
                    path_info.st_ino,
                )
                != (before.st_dev, before.st_ino)
                or (
                    after.st_nlink != 2
                    or after.st_size > _MAX_DATABASE_BYTES
                    or path_info.st_nlink != 2
                    or path_info.st_uid != os.getuid()
                    or path_info.st_mode & 0o777 != 0o600
                )
            ):
                raise DailyShadowRegistryUnavailable("daily shadow database changed")
        except DailyShadowRegistryUnavailable:
            raise
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow database unavailable") from exc
        finally:
            if connection is not None:
                connection.close()
            if alias_fd is not None:
                os.close(alias_fd)
            try:
                os.unlink(alias_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
            except OSError as exc:
                os.close(database_fd) if database_fd is not None else None
                os.close(parent_fd)
                raise DailyShadowRegistryUnavailable(
                    "daily shadow database cleanup unavailable"
                ) from exc
            if database_fd is not None:
                os.close(database_fd)
            os.fsync(parent_fd)
            os.close(parent_fd)

    def _initialize_files(self) -> None:
        parent_fd = _validate_parent(self.path.parent)
        try:
            for name in (self.lock_path.name, self.path.name):
                descriptor = _open_or_create_private(parent_fd, name)
                os.close(descriptor)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    def initialize(self, contract: DailyShadowContract, terms: TermsEvidence) -> None:
        content = getattr(terms, "_content_bytes", b"")
        try:
            contract = DailyShadowContract.model_validate(contract.model_dump(mode="python"))
            terms = TermsEvidence.model_validate(terms.model_dump(mode="python"))
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow contract unavailable") from exc
        if (
            terms.provider_id != ShadowProviderId.TICKFLOW
            or terms.contract_version != DAILY_SHADOW_TERMS_CONTRACT_VERSION
            or terms.manifest_sha256 != terms_evidence_manifest_sha256(terms)
            or contract.terms_evidence_sha256 != terms.manifest_sha256
            or terms.official_url_allowlist != (_FREE_ORIGIN,)
            or terms.approved_intended_use != _INTENDED_USE
            or terms.approved_retention != _RETENTION
            or terms.approved_credential_mode != _CREDENTIAL_MODE
            or terms.approved_quota_decision != _QUOTA_DECISION
        ):
            raise DailyShadowRegistryUnavailable("daily shadow terms unavailable")
        if not content or hashlib.sha256(content).hexdigest() != terms.content_bytes_sha256:
            raise DailyShadowRegistryUnavailable("daily shadow terms object unavailable")
        self._initialize_files()
        now = self._clock().isoformat()
        with self._lock():
            with self._bound_writer() as connection:
                try:
                    initialize_daily_shadow_schema(connection, applied_at=now)
                    _validate_control_graph(connection)
                    existing = connection.execute(
                        "SELECT descriptor_json,terms_evidence_sha256 "
                        "FROM daily_shadow_contract WHERE descriptor_sha256=?",
                        (contract.descriptor_sha256,),
                    ).fetchone()
                    descriptor = canonical_json_bytes(contract.model_dump(mode="json"))
                    terms_json = canonical_terms_evidence(terms)
                    if existing is None:
                        connection.execute("BEGIN IMMEDIATE")
                        existing_terms = connection.execute(
                            "SELECT terms_evidence_id,review_id,contract_version,"
                            "content_bytes_sha256,canonical_json "
                            "FROM daily_shadow_terms_evidence WHERE terms_evidence_sha256=?",
                            (terms.manifest_sha256,),
                        ).fetchone()
                        expected_terms = (
                            terms.terms_evidence_id,
                            terms.review_id,
                            terms.contract_version,
                            terms.content_bytes_sha256,
                            terms_json,
                        )
                        if existing_terms is None:
                            connection.execute(
                                "INSERT INTO daily_shadow_terms_evidence VALUES (?,?,?,?,?,?,?)",
                                (
                                    terms.manifest_sha256,
                                    *expected_terms,
                                    now,
                                ),
                            )
                        elif tuple(existing_terms) != expected_terms:
                            raise DailyShadowRegistryUnavailable(
                                "daily shadow terms identity conflict"
                            )
                        connection.execute(
                            "INSERT INTO daily_shadow_contract VALUES (?,?,?,?,?,?)",
                            (
                                contract.descriptor_sha256,
                                "tickflow",
                                DAILY_SHADOW_PROFILE,
                                descriptor,
                                terms.manifest_sha256,
                                now,
                            ),
                        )
                        _validate_database_bound(connection)
                        connection.commit()
                    elif tuple(existing) != (
                        descriptor,
                        terms.manifest_sha256,
                    ):
                        raise DailyShadowRegistryUnavailable(
                            "daily shadow contract identity conflict"
                        )
                except Exception:
                    connection.rollback()
                    raise

    def _transaction(self, callback: Callable[[sqlite3.Connection], Any]) -> Any:
        with self._lock():
            with self._bound_writer() as connection:
                try:
                    validate_daily_shadow_schema(connection)
                    _validate_control_graph(connection)
                    connection.execute("BEGIN IMMEDIATE")
                    result = callback(connection)
                    _validate_database_bound(connection)
                    connection.commit()
                    return result
                except DailyShadowRegistryUnavailable:
                    connection.rollback()
                    raise
                except Exception as exc:
                    connection.rollback()
                    raise DailyShadowRegistryUnavailable(
                        "daily shadow transaction unavailable"
                    ) from exc

    def read(self) -> DailyShadowStatus:
        result = DailyShadowRegistryReader(self.path, verify_external=False).read()
        if result.status != "READY":
            raise DailyShadowRegistryUnavailable("daily shadow schema unavailable")
        return result

    @staticmethod
    def _binding_from_epoch(row: sqlite3.Row) -> tuple[Any, ...]:
        return (
            row["calendar_generation"],
            row["calendar_sha256"],
            row["universe_policy_sha256"],
            row["version_vector_sha256"],
            row["terms_evidence_sha256"],
            row["expected_dates_sha256"],
        )

    @staticmethod
    def _binding_values(binding: DailyWindowBinding) -> tuple[Any, ...]:
        dates_sha = domain_sha256(
            "stock-eva/r2f3/free-daily-expected-dates/v1",
            tuple(item.isoformat() for item in binding.expected_dates),
        )
        return (
            binding.calendar_generation,
            binding.calendar_sha256,
            binding.universe_policy_sha256,
            binding.version_vector_sha256,
            binding.terms_evidence_sha256,
            dates_sha,
        )

    def ensure_window(self, binding: DailyWindowBinding) -> DailyWindowSnapshot:
        try:
            binding = DailyWindowBinding.model_validate(binding.model_dump(mode="python"))
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily shadow window unavailable") from exc

        def save(connection: sqlite3.Connection) -> DailyWindowSnapshot:
            contract = connection.execute(
                "SELECT 1 FROM daily_shadow_contract WHERE provider='tickflow' "
                "AND profile=? AND terms_evidence_sha256=?",
                (DAILY_SHADOW_PROFILE, binding.terms_evidence_sha256),
            ).fetchone()
            if contract is None:
                raise DailyShadowRegistryUnavailable("daily shadow terms binding unavailable")
            current = connection.execute(
                "SELECT w.*,e.* FROM daily_shadow_window w JOIN daily_shadow_epoch e "
                "ON e.epoch_id=w.epoch_id WHERE w.provider='tickflow' AND w.profile=?",
                (DAILY_SHADOW_PROFILE,),
            ).fetchone()
            values = self._binding_values(binding)
            if (
                current is not None
                and current["window_state"] != "RESET"
                and self._binding_from_epoch(current) == values
            ):
                return _window_from_row(current)
            ordinal = connection.execute(
                "SELECT coalesce(max(epoch_ordinal),0)+1 FROM daily_shadow_epoch"
            ).fetchone()[0]
            prior = current["epoch_id"] if current is not None else None
            if current is not None and current["window_state"] != "RESET":
                connection.execute(
                    "UPDATE daily_shadow_epoch SET epoch_state='RESET',"
                    "reset_reason='BINDING_CHANGED' "
                    "WHERE epoch_id=? AND epoch_state IN ('ACTIVE','SHADOW_QUALIFIED')",
                    (prior,),
                )
            epoch_digest = domain_sha256(
                "stock-eva/r2f3/free-daily-epoch-id/v1",
                {"ordinal": ordinal, "binding": binding.binding_sha256},
            )
            epoch_id = f"daily-epoch-{ordinal:04d}-{epoch_digest[:16]}"
            dates_json = schema_json_bytes(
                tuple(item.isoformat() for item in binding.expected_dates)
            )
            connection.execute(
                "INSERT INTO daily_shadow_epoch VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    epoch_id,
                    ordinal,
                    prior,
                    "ACTIVE",
                    None,
                    binding.calendar_generation,
                    binding.calendar_sha256,
                    binding.universe_policy_sha256,
                    binding.version_vector_sha256,
                    binding.terms_evidence_sha256,
                    dates_json,
                    values[-1],
                    self._clock().isoformat(),
                ),
            )
            if current is None:
                connection.execute(
                    "INSERT INTO daily_shadow_window VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "tickflow",
                        DAILY_SHADOW_PROFILE,
                        epoch_id,
                        "OBSERVING",
                        None,
                        None,
                        binding.expected_dates[0].isoformat(),
                        0,
                        _REQUIRED_SESSIONS,
                        None,
                        0,
                    ),
                )
            else:
                changed = connection.execute(
                    "UPDATE daily_shadow_window SET epoch_id=?,window_state='OBSERVING',"
                    "first_trade_date=NULL,last_trade_date=NULL,next_trade_date=?,"
                    "consecutive_sessions=0,last_session_report_id=NULL,"
                    "state_version=state_version+1 "
                    "WHERE provider='tickflow' AND profile=? AND state_version=?",
                    (
                        epoch_id,
                        binding.expected_dates[0].isoformat(),
                        DAILY_SHADOW_PROFILE,
                        current["state_version"],
                    ),
                )
                if changed.rowcount != 1:
                    raise DailyShadowRegistryUnavailable("daily shadow window changed")
            row = connection.execute(
                "SELECT * FROM daily_shadow_window WHERE provider='tickflow' AND profile=?",
                (DAILY_SHADOW_PROFILE,),
            ).fetchone()
            return _window_from_row(row)

        return self._transaction(save)

    @staticmethod
    def _reset_window(
        connection: sqlite3.Connection,
        *,
        epoch_id: str,
        reason: str,
    ) -> DailyWindowSnapshot:
        window = connection.execute(
            "SELECT * FROM daily_shadow_window WHERE epoch_id=?", (epoch_id,)
        ).fetchone()
        if window is None:
            raise DailyShadowRegistryUnavailable("daily shadow window unavailable")
        if window["window_state"] != "RESET":
            connection.execute(
                "UPDATE daily_shadow_epoch SET epoch_state='RESET',reset_reason=? "
                "WHERE epoch_id=? AND epoch_state IN ('ACTIVE','SHADOW_QUALIFIED')",
                (reason, epoch_id),
            )
            changed = connection.execute(
                "UPDATE daily_shadow_window SET window_state='RESET',first_trade_date=NULL,"
                "last_trade_date=NULL,next_trade_date=NULL,consecutive_sessions=0,"
                "state_version=state_version+1 WHERE epoch_id=? AND state_version=?",
                (epoch_id, window["state_version"]),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily shadow window changed")
        row = connection.execute(
            "SELECT * FROM daily_shadow_window WHERE epoch_id=?", (epoch_id,)
        ).fetchone()
        return _window_from_row(row)

    def lease_session(
        self,
        *,
        epoch_id: str,
        job_id: str,
        session_id: str,
        trade_date: date,
        request_plan_sha256: str,
        canonical_snapshot_sha256: str,
        canonical_symbol_set_sha256: str,
        owner: str,
        now: datetime | None = None,
    ) -> DailySessionLease:
        for value in (epoch_id, job_id, session_id, owner):
            _safe_id(value)
        _safe_sha(request_plan_sha256)
        _safe_sha(canonical_snapshot_sha256)
        _safe_sha(canonical_symbol_set_sha256)
        observed_at = now or self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily shadow lease time unavailable")

        def lease(connection: sqlite3.Connection) -> DailySessionLease:
            window = connection.execute(
                "SELECT * FROM daily_shadow_window WHERE epoch_id=?", (epoch_id,)
            ).fetchone()
            existing = connection.execute(
                "SELECT * FROM daily_shadow_job WHERE job_id=?", (job_id,)
            ).fetchone()
            if existing is not None:
                same = (
                    existing["epoch_id"] == epoch_id
                    and existing["session_id"] == session_id
                    and existing["trade_date"] == trade_date.isoformat()
                    and existing["request_plan_sha256"] == request_plan_sha256
                    and existing["canonical_snapshot_sha256"] == canonical_snapshot_sha256
                    and existing["canonical_symbol_set_sha256"] == canonical_symbol_set_sha256
                )
                if not same:
                    state_version = 0
                    if window is not None and window["window_state"] != "RESET":
                        self._reset_window(
                            connection, epoch_id=epoch_id, reason="DUPLICATE_CONFLICT"
                        )
                        state_version = window["state_version"] + 1
                    return DailySessionLease(
                        outcome="RESET",
                        epoch_id=epoch_id,
                        job_id=job_id,
                        session_id=session_id,
                        trade_date=trade_date,
                        state_version=state_version,
                    )
                if existing["run_status"] != "LEASED":
                    return DailySessionLease(
                        outcome="ALREADY_TERMINAL",
                        epoch_id=epoch_id,
                        job_id=job_id,
                        session_id=session_id,
                        trade_date=trade_date,
                        state_version=existing["state_version"],
                    )
            if window is None or window["window_state"] != "OBSERVING":
                return DailySessionLease(
                    outcome="RESET",
                    epoch_id=epoch_id,
                    job_id=job_id,
                    session_id=session_id,
                    trade_date=trade_date,
                    state_version=0,
                )
            if window["next_trade_date"] != trade_date.isoformat():
                self._reset_window(connection, epoch_id=epoch_id, reason="CONTINUITY_GAP")
                return DailySessionLease(
                    outcome="RESET",
                    epoch_id=epoch_id,
                    job_id=job_id,
                    session_id=session_id,
                    trade_date=trade_date,
                    state_version=window["state_version"] + 1,
                )
            expires = observed_at + timedelta(seconds=_JOB_LEASE_SECONDS)
            if existing is not None:
                if datetime.fromisoformat(existing["lease_expires_at"]) > observed_at:
                    return DailySessionLease(
                        outcome="BUSY",
                        epoch_id=epoch_id,
                        job_id=job_id,
                        session_id=session_id,
                        trade_date=trade_date,
                        state_version=existing["state_version"],
                    )
                changed = connection.execute(
                    "UPDATE daily_shadow_job SET lease_owner=?,lease_expires_at=?,"
                    "state_version=state_version+1 WHERE job_id=? AND state_version=?",
                    (owner, expires.isoformat(), job_id, existing["state_version"]),
                )
                if changed.rowcount != 1:
                    raise DailyShadowRegistryUnavailable("daily shadow job changed")
                state_version = existing["state_version"] + 1
            else:
                connection.execute(
                    "INSERT INTO daily_shadow_job VALUES (?,?,?,?,?,?,?,'LEASED',?,?,NULL,0)",
                    (
                        job_id,
                        epoch_id,
                        session_id,
                        trade_date.isoformat(),
                        request_plan_sha256,
                        canonical_snapshot_sha256,
                        canonical_symbol_set_sha256,
                        owner,
                        expires.isoformat(),
                    ),
                )
                state_version = 0
            return DailySessionLease(
                outcome="LEASED",
                epoch_id=epoch_id,
                job_id=job_id,
                session_id=session_id,
                trade_date=trade_date,
                owner=owner,
                expires_at=expires,
                state_version=state_version,
            )

        return self._transaction(lease)

    @staticmethod
    def _require_lease(
        connection: sqlite3.Connection,
        lease: DailySessionLease,
        *,
        epoch_id: str,
        job_id: str,
        session_id: str,
        trade_date: date,
        observed_at: datetime,
    ) -> sqlite3.Row:
        if lease.outcome != "LEASED" or (
            lease.epoch_id,
            lease.job_id,
            lease.session_id,
            lease.trade_date,
        ) != (epoch_id, job_id, session_id, trade_date):
            raise DailyShadowRegistryUnavailable("daily shadow lease unavailable")
        row = connection.execute(
            "SELECT * FROM daily_shadow_job WHERE job_id=?", (job_id,)
        ).fetchone()
        if (
            row is None
            or row["run_status"] != "LEASED"
            or row["epoch_id"] != epoch_id
            or row["session_id"] != session_id
            or row["trade_date"] != trade_date.isoformat()
            or row["lease_owner"] != lease.owner
            or row["state_version"] != lease.state_version
            or datetime.fromisoformat(row["lease_expires_at"]) <= observed_at
        ):
            raise DailyShadowRegistryUnavailable("daily shadow lease changed")
        return row

    @staticmethod
    def _insert_audits(
        connection: sqlite3.Connection,
        *,
        epoch_id: str,
        job_id: str,
        session_id: str,
        attempts: tuple[DailyAttemptAudit, ...],
    ) -> None:
        for item in attempts:
            identity = hashlib.sha256(f"{job_id}:{item.ordinal}".encode()).hexdigest()
            audit_id = f"daily-audit-{identity[:24]}"
            connection.execute(
                "INSERT INTO daily_shadow_attempt_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    audit_id,
                    epoch_id,
                    job_id,
                    session_id,
                    item.ordinal,
                    item.request_id,
                    item.endpoint,
                    item.attempt,
                    item.outcome,
                    item.elapsed_ms,
                    item.response_bytes,
                    item.expected_rows,
                    item.observed_rows,
                    item.failure_class,
                    item.audit_sha256,
                ),
            )

    def commit_success(
        self,
        success: DailySessionSuccess,
        *,
        lease: DailySessionLease,
        simulate_crash_at: str | None = None,
    ) -> DailyWindowSnapshot:
        try:
            success = DailySessionSuccess.model_validate(success.model_dump(mode="python"))
            lease = DailySessionLease.model_validate(lease.model_dump(mode="python"))
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily terminal graph unavailable") from exc
        observed_at = self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily shadow lease time unavailable")

        def commit(connection: sqlite3.Connection) -> DailyWindowSnapshot:
            existing = connection.execute(
                "SELECT * FROM daily_shadow_session_report WHERE session_report_id=?",
                (success.session_report_id,),
            ).fetchone()
            if existing is not None:
                evidence = connection.execute(
                    "SELECT * FROM daily_shadow_evidence_ref WHERE evidence_id=?",
                    (existing["evidence_id"],),
                ).fetchone()
                candidate = connection.execute(
                    "SELECT * FROM daily_shadow_candidate_ref WHERE candidate_id=?",
                    (existing["candidate_id"],),
                ).fetchone()
                audits = connection.execute(
                    "SELECT audit_sha256 FROM daily_shadow_attempt_audit "
                    "WHERE job_id=? ORDER BY ordinal",
                    (existing["job_id"],),
                ).fetchall()
                if (
                    existing["outcome"] == "SUCCESS"
                    and existing["epoch_id"] == success.epoch_id
                    and existing["job_id"] == success.job_id
                    and existing["session_id"] == success.session_id
                    and existing["trade_date"] == success.trade_date.isoformat()
                    and existing["canonical_snapshot_sha256"] == success.canonical_snapshot_sha256
                    and existing["canonical_symbol_set_sha256"]
                    == success.canonical_symbol_set_sha256
                    and existing["request_plan_sha256"] == success.request_plan_sha256
                    and existing["completion_sha256"] == success.completion_sha256
                    and existing["evidence_id"] == success.evidence_id
                    and existing["evidence_sha256"] == success.evidence_sha256
                    and existing["candidate_id"] == success.candidate_id
                    and existing["candidate_sha256"] == success.candidate_sha256
                    and existing["reconciliation_report_sha256"]
                    == success.reconciliation_report_sha256
                    and existing["terminal_attestation_id"] == success.attestation_id
                    and evidence is not None
                    and evidence["bundle_ref"] == success.evidence_bundle_ref
                    and evidence["bundle_sha256"] == success.evidence_bundle_sha256
                    and candidate is not None
                    and candidate["bundle_ref"] == success.candidate_bundle_ref
                    and candidate["bundle_sha256"] == success.candidate_bundle_sha256
                    and candidate["canonical_symbol_set_sha256"]
                    == success.canonical_symbol_set_sha256
                    and candidate["quality_report_sha256"] == success.quality_report_sha256
                    and tuple(row["audit_sha256"] for row in audits)
                    == tuple(item.audit_sha256 for item in success.attempts)
                ):
                    return _window_from_row(
                        connection.execute(
                            "SELECT * FROM daily_shadow_window WHERE epoch_id=?",
                            (success.epoch_id,),
                        ).fetchone()
                    )
                return self._reset_window(
                    connection, epoch_id=success.epoch_id, reason="DUPLICATE_CONFLICT"
                )
            job = self._require_lease(
                connection,
                lease,
                epoch_id=success.epoch_id,
                job_id=success.job_id,
                session_id=success.session_id,
                trade_date=success.trade_date,
                observed_at=observed_at,
            )
            window = connection.execute(
                "SELECT * FROM daily_shadow_window WHERE epoch_id=?", (success.epoch_id,)
            ).fetchone()
            if (
                window is None
                or window["window_state"] != "OBSERVING"
                or window["next_trade_date"] != success.trade_date.isoformat()
                or job["request_plan_sha256"] != success.request_plan_sha256
                or job["canonical_snapshot_sha256"] != success.canonical_snapshot_sha256
                or job["canonical_symbol_set_sha256"] != success.canonical_symbol_set_sha256
            ):
                raise DailyShadowRegistryUnavailable("daily terminal graph identity mismatch")
            self._insert_audits(
                connection,
                epoch_id=success.epoch_id,
                job_id=success.job_id,
                session_id=success.session_id,
                attempts=success.attempts,
            )
            connection.execute(
                "INSERT INTO daily_shadow_evidence_ref VALUES (?,?,?,?,?,?,?,?)",
                (
                    success.evidence_id,
                    success.epoch_id,
                    success.job_id,
                    success.session_id,
                    success.completion_sha256,
                    success.evidence_sha256,
                    success.evidence_bundle_ref,
                    success.evidence_bundle_sha256,
                ),
            )
            if simulate_crash_at == "after_evidence":
                raise RuntimeError("daily shadow simulated crash")
            connection.execute(
                "INSERT INTO daily_shadow_candidate_ref VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    success.candidate_id,
                    success.evidence_id,
                    success.epoch_id,
                    success.job_id,
                    success.session_id,
                    success.candidate_sha256,
                    success.canonical_symbol_set_sha256,
                    success.candidate_bundle_ref,
                    success.candidate_bundle_sha256,
                    success.quality_report_sha256,
                    success.reconciliation_report_sha256,
                ),
            )
            if simulate_crash_at == "after_candidate":
                raise RuntimeError("daily shadow simulated crash")
            request_plan_json = schema_json_bytes(
                {
                    "job_id": success.job_id,
                    "trade_date": success.trade_date.isoformat(),
                    "request_plan_sha256": success.request_plan_sha256,
                    "canonical_symbol_set_sha256": success.canonical_symbol_set_sha256,
                }
            )
            completion_json = schema_json_bytes(
                {
                    "evidence_id": success.evidence_id,
                    "completion_sha256": success.completion_sha256,
                }
            )
            attempt_json = schema_json_bytes(
                tuple(
                    {"ordinal": item.ordinal, "audit_sha256": item.audit_sha256}
                    for item in success.attempts
                )
            )
            report_graph_values = {
                "session_report_id": success.session_report_id,
                "evidence_id": success.evidence_id,
                "evidence_sha256": success.evidence_sha256,
                "candidate_id": success.candidate_id,
                "candidate_sha256": success.candidate_sha256,
                "canonical_symbol_set_sha256": success.canonical_symbol_set_sha256,
                "reconciliation_report_sha256": success.reconciliation_report_sha256,
            }
            report_graph_json = schema_json_bytes(report_graph_values)
            graph_hashes = tuple(
                sha256_bytes(value)
                for value in (
                    request_plan_json,
                    completion_json,
                    attempt_json,
                    report_graph_json,
                )
            )
            report_values = {
                **report_graph_values,
                "epoch_id": success.epoch_id,
                "job_id": success.job_id,
                "session_id": success.session_id,
                "trade_date": success.trade_date.isoformat(),
                "canonical_snapshot_sha256": success.canonical_snapshot_sha256,
                "canonical_symbol_set_sha256": success.canonical_symbol_set_sha256,
                "request_plan_sha256": success.request_plan_sha256,
                "completion_sha256": success.completion_sha256,
                "terminal_attestation_id": success.attestation_id,
                "outcome": "SUCCESS",
            }
            report_json = schema_json_bytes(report_values)
            report_sha = sha256_bytes(report_json)
            connection.execute(
                "INSERT INTO daily_shadow_session_report VALUES "
                "(?,?,?,?,?,'SUCCESS',?,?,?,?,?,?,?,?,?,?,NULL,?,?,?)",
                (
                    success.session_report_id,
                    success.epoch_id,
                    success.job_id,
                    success.session_id,
                    success.trade_date.isoformat(),
                    success.canonical_snapshot_sha256,
                    success.canonical_symbol_set_sha256,
                    success.request_plan_sha256,
                    success.completion_sha256,
                    success.evidence_id,
                    success.evidence_sha256,
                    success.candidate_id,
                    success.candidate_sha256,
                    success.reconciliation_report_sha256,
                    success.attestation_id,
                    report_json,
                    report_sha,
                    self._clock().isoformat(),
                ),
            )
            if simulate_crash_at == "after_report":
                raise RuntimeError("daily shadow simulated crash")
            attestation_sha = domain_sha256(
                "stock-eva/r2f3/free-daily-terminal-attestation/v1", graph_hashes
            )
            connection.execute(
                "INSERT INTO daily_shadow_terminal_attestation VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    success.attestation_id,
                    success.epoch_id,
                    success.job_id,
                    success.session_id,
                    success.session_report_id,
                    success.evidence_id,
                    success.candidate_id,
                    success.canonical_symbol_set_sha256,
                    request_plan_json,
                    graph_hashes[0],
                    completion_json,
                    graph_hashes[1],
                    attempt_json,
                    graph_hashes[2],
                    report_graph_json,
                    graph_hashes[3],
                    attestation_sha,
                ),
            )
            if simulate_crash_at == "after_attestation":
                raise RuntimeError("daily shadow simulated crash")
            changed = connection.execute(
                "UPDATE daily_shadow_job SET run_status='COMPLETED',lease_owner=NULL,"
                "lease_expires_at=NULL,terminal_attestation_id=?,state_version=state_version+1 "
                "WHERE job_id=? AND state_version=? AND run_status='LEASED'",
                (success.attestation_id, success.job_id, lease.state_version),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily shadow job changed")
            epoch = connection.execute(
                "SELECT expected_dates_json FROM daily_shadow_epoch WHERE epoch_id=?",
                (success.epoch_id,),
            ).fetchone()
            expected_dates = tuple(
                date.fromisoformat(item) for item in json.loads(bytes(epoch[0]).decode("utf-8"))
            )
            count = window["consecutive_sessions"] + 1
            qualified = count == _REQUIRED_SESSIONS
            next_date = None if qualified else expected_dates[count].isoformat()
            state = "SHADOW_QUALIFIED" if qualified else "OBSERVING"
            changed = connection.execute(
                "UPDATE daily_shadow_window SET window_state=?,"
                "first_trade_date=coalesce(first_trade_date,?),last_trade_date=?,"
                "next_trade_date=?,consecutive_sessions=?,last_session_report_id=?,"
                "state_version=state_version+1 WHERE epoch_id=? AND state_version=?",
                (
                    state,
                    success.trade_date.isoformat(),
                    success.trade_date.isoformat(),
                    next_date,
                    count,
                    success.session_report_id,
                    success.epoch_id,
                    window["state_version"],
                ),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily shadow window changed")
            if qualified:
                connection.execute(
                    "UPDATE daily_shadow_epoch SET epoch_state='SHADOW_QUALIFIED' "
                    "WHERE epoch_id=? AND epoch_state='ACTIVE'",
                    (success.epoch_id,),
                )
            return _window_from_row(
                connection.execute(
                    "SELECT * FROM daily_shadow_window WHERE epoch_id=?",
                    (success.epoch_id,),
                ).fetchone()
            )

        return self._transaction(commit)

    def commit_failure(
        self,
        failure: DailySessionFailure,
        *,
        lease: DailySessionLease,
    ) -> DailyWindowSnapshot:
        try:
            failure = DailySessionFailure.model_validate(failure.model_dump(mode="python"))
            lease = DailySessionLease.model_validate(lease.model_dump(mode="python"))
        except Exception as exc:
            raise DailyShadowRegistryUnavailable("daily failure graph unavailable") from exc
        observed_at = self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily shadow lease time unavailable")

        def commit(connection: sqlite3.Connection) -> DailyWindowSnapshot:
            job = self._require_lease(
                connection,
                lease,
                epoch_id=failure.epoch_id,
                job_id=failure.job_id,
                session_id=failure.session_id,
                trade_date=failure.trade_date,
                observed_at=observed_at,
            )
            self._insert_audits(
                connection,
                epoch_id=failure.epoch_id,
                job_id=failure.job_id,
                session_id=failure.session_id,
                attempts=failure.attempts,
            )
            report_values = {
                "session_report_id": failure.session_report_id,
                "epoch_id": failure.epoch_id,
                "job_id": failure.job_id,
                "session_id": failure.session_id,
                "trade_date": failure.trade_date.isoformat(),
                "canonical_symbol_set_sha256": job["canonical_symbol_set_sha256"],
                "outcome": failure.outcome,
                "failure_class": failure.failure_class,
            }
            report_json = schema_json_bytes(report_values)
            connection.execute(
                "INSERT INTO daily_shadow_session_report VALUES "
                "(?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?,?,?)",
                (
                    failure.session_report_id,
                    failure.epoch_id,
                    failure.job_id,
                    failure.session_id,
                    failure.trade_date.isoformat(),
                    failure.outcome,
                    job["canonical_snapshot_sha256"],
                    job["canonical_symbol_set_sha256"],
                    job["request_plan_sha256"],
                    failure.failure_class,
                    report_json,
                    sha256_bytes(report_json),
                    self._clock().isoformat(),
                ),
            )
            job_outcome = (
                "SKIPPED" if failure.outcome == "SKIPPED_CIRCUIT_OPEN" else failure.outcome
            )
            changed = connection.execute(
                "UPDATE daily_shadow_job SET run_status=?,lease_owner=NULL,lease_expires_at=NULL,"
                "state_version=state_version+1 WHERE job_id=? AND state_version=? "
                "AND run_status='LEASED'",
                (job_outcome, failure.job_id, lease.state_version),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily shadow job changed")
            return self._reset_window(
                connection,
                epoch_id=failure.epoch_id,
                reason=failure.outcome,
            )

        return self._transaction(commit)

    @staticmethod
    def _circuit_from_row(row: sqlite3.Row) -> DailyCircuitSnapshot:
        return DailyCircuitSnapshot(
            state=row["state"],
            consecutive_failures=row["consecutive_failures"],
            cooldown_until=row["cooldown_until"],
            probe_lease_id=row["probe_lease_id"],
            state_version=row["state_version"],
        )

    @staticmethod
    def _insert_circuit_event(
        connection: sqlite3.Connection,
        *,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        before: str,
        after: str,
        failure_class: str | None = None,
    ) -> None:
        values = {
            "event_id": event_id,
            "endpoint": "historical_daily_1d",
            "event_type": event_type,
            "observed_at": observed_at.isoformat(),
            "state_before": before,
            "state_after": after,
            "failure_class": failure_class,
        }
        connection.execute(
            "INSERT INTO daily_shadow_circuit_event VALUES (?,?,?,?,?,?,?,?)",
            (
                event_id,
                "historical_daily_1d",
                event_type,
                observed_at.isoformat(),
                before,
                after,
                failure_class,
                domain_sha256("stock-eva/r2f3/free-daily-circuit-event/v1", values),
            ),
        )

    def record_endpoint_failure(
        self,
        event_id: str,
        *,
        failure_class: str = "provider_failure",
        now: datetime | None = None,
    ) -> DailyCircuitSnapshot:
        _safe_id(event_id)
        if re.fullmatch(r"[a-z0-9_]{1,64}", failure_class) is None:
            raise DailyShadowRegistryUnavailable("daily circuit failure unavailable")
        try:
            validate_daily_failure_class(failure_class)
        except ValueError as exc:
            raise DailyShadowRegistryUnavailable("daily circuit failure unavailable") from exc
        observed_at = now or self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily circuit time unavailable")

        def save(connection: sqlite3.Connection) -> DailyCircuitSnapshot:
            existing = connection.execute(
                "SELECT 1 FROM daily_shadow_circuit_event WHERE event_id=?", (event_id,)
            ).fetchone()
            row = connection.execute(
                "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
            ).fetchone()
            if existing is not None:
                return self._circuit_from_row(row)
            before = row["state"]
            count = row["consecutive_failures"] + 1
            if before == "HALF_OPEN" or count >= _FAILURE_THRESHOLD:
                state = "OPEN"
                opened = observed_at.isoformat()
                cooldown = (observed_at + timedelta(seconds=_COOLDOWN_SECONDS)).isoformat()
            elif before == "OPEN":
                state = "OPEN"
                opened = row["opened_at"]
                cooldown = row["cooldown_until"]
            else:
                state = "CLOSED"
                opened = None
                cooldown = None
            changed = connection.execute(
                "UPDATE daily_shadow_circuit SET state=?,consecutive_failures=?,opened_at=?,"
                "cooldown_until=?,probe_lease_id=NULL,probe_owner=NULL,probe_expires_at=NULL,"
                "state_version=state_version+1 WHERE endpoint='historical_daily_1d' "
                "AND state_version=?",
                (state, count, opened, cooldown, row["state_version"]),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily circuit changed")
            self._insert_circuit_event(
                connection,
                event_id=event_id,
                event_type="FAILURE",
                observed_at=observed_at,
                before=before,
                after=state,
                failure_class=failure_class,
            )
            return self._circuit_from_row(
                connection.execute(
                    "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
                ).fetchone()
            )

        return self._transaction(save)

    def record_endpoint_success(
        self,
        event_id: str,
        *,
        now: datetime | None = None,
    ) -> DailyCircuitSnapshot:
        _safe_id(event_id)
        observed_at = now or self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily circuit time unavailable")

        def save(connection: sqlite3.Connection) -> DailyCircuitSnapshot:
            existing = connection.execute(
                "SELECT 1 FROM daily_shadow_circuit_event WHERE event_id=?", (event_id,)
            ).fetchone()
            row = connection.execute(
                "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
            ).fetchone()
            if existing is not None:
                return self._circuit_from_row(row)
            if row["state"] != "CLOSED":
                raise DailyShadowRegistryUnavailable("daily circuit requires probe resolution")
            changed = connection.execute(
                "UPDATE daily_shadow_circuit SET consecutive_failures=0,"
                "state_version=state_version+1 WHERE endpoint='historical_daily_1d' "
                "AND state_version=?",
                (row["state_version"],),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily circuit changed")
            self._insert_circuit_event(
                connection,
                event_id=event_id,
                event_type="SUCCESS",
                observed_at=observed_at,
                before="CLOSED",
                after="CLOSED",
            )
            return self._circuit_from_row(
                connection.execute(
                    "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
                ).fetchone()
            )

        return self._transaction(save)

    def circuit_action(
        self,
        *,
        owner: str,
        now: datetime | None = None,
    ) -> DailyCircuitDecision:
        _safe_id(owner)
        observed_at = now or self._clock()
        if observed_at.utcoffset() is None:
            raise DailyShadowRegistryUnavailable("daily circuit time unavailable")

        def decide(connection: sqlite3.Connection) -> DailyCircuitDecision:
            row = connection.execute(
                "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
            ).fetchone()
            if row["state"] == "CLOSED":
                return DailyCircuitDecision(
                    action=DailyCircuitAction.RUN_FULL_SESSION,
                    state="CLOSED",
                    zero_write=False,
                    ends_slot=False,
                )
            if row["state"] == "HALF_OPEN":
                if datetime.fromisoformat(row["probe_expires_at"]) > observed_at:
                    return DailyCircuitDecision(
                        action=DailyCircuitAction.SKIPPED_HALF_OPEN,
                        state="HALF_OPEN",
                        zero_write=True,
                        ends_slot=True,
                    )
                changed = connection.execute(
                    "UPDATE daily_shadow_circuit SET state='OPEN',probe_lease_id=NULL,"
                    "probe_owner=NULL,probe_expires_at=NULL,state_version=state_version+1 "
                    "WHERE endpoint='historical_daily_1d' AND state_version=?",
                    (row["state_version"],),
                )
                if changed.rowcount != 1:
                    raise DailyShadowRegistryUnavailable("daily circuit changed")
                row = connection.execute(
                    "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
                ).fetchone()
            if datetime.fromisoformat(row["cooldown_until"]) > observed_at:
                return DailyCircuitDecision(
                    action=DailyCircuitAction.SKIPPED_CIRCUIT_OPEN,
                    state="OPEN",
                    zero_write=True,
                    ends_slot=True,
                )
            lease_id = f"daily-probe-{secrets.token_hex(12)}"
            expires = observed_at + timedelta(seconds=_PROBE_LEASE_SECONDS)
            changed = connection.execute(
                "UPDATE daily_shadow_circuit SET state='HALF_OPEN',probe_lease_id=?,"
                "probe_owner=?,probe_expires_at=?,state_version=state_version+1 "
                "WHERE endpoint='historical_daily_1d' AND state='OPEN' AND state_version=?",
                (lease_id, owner, expires.isoformat(), row["state_version"]),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily circuit changed")
            self._insert_circuit_event(
                connection,
                event_id=f"event-{lease_id}",
                event_type="HALF_OPEN_LEASED",
                observed_at=observed_at,
                before="OPEN",
                after="HALF_OPEN",
            )
            return DailyCircuitDecision(
                action=DailyCircuitAction.RUN_FIXED_FIVE_HALF_OPEN_PROBE,
                state="HALF_OPEN",
                probe_lease_id=lease_id,
                zero_write=True,
                ends_slot=True,
            )

        return self._transaction(decide)

    def resolve_probe(
        self,
        probe_lease_id: str | None,
        *,
        owner: str,
        success: bool,
        now: datetime | None = None,
    ) -> DailyCircuitSnapshot:
        if probe_lease_id is None:
            raise DailyShadowRegistryUnavailable("daily probe lease unavailable")
        _safe_id(probe_lease_id)
        _safe_id(owner)
        if type(success) is not bool:
            raise DailyShadowRegistryUnavailable("daily probe outcome unavailable")
        observed_at = now or self._clock()

        def resolve(connection: sqlite3.Connection) -> DailyCircuitSnapshot:
            row = connection.execute(
                "SELECT * FROM daily_shadow_circuit WHERE probe_lease_id=?",
                (probe_lease_id,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "HALF_OPEN"
                or row["probe_owner"] != owner
                or datetime.fromisoformat(row["probe_expires_at"]) <= observed_at
            ):
                raise DailyShadowRegistryUnavailable("daily probe lease unavailable")
            if success:
                state = "CLOSED"
                count = 0
                opened = None
                cooldown = None
                event_type = "PROBE_SUCCESS"
            else:
                state = "OPEN"
                count = max(row["consecutive_failures"] + 1, _FAILURE_THRESHOLD)
                opened = observed_at.isoformat()
                cooldown = (observed_at + timedelta(seconds=_COOLDOWN_SECONDS)).isoformat()
                event_type = "PROBE_FAILURE"
            changed = connection.execute(
                "UPDATE daily_shadow_circuit SET state=?,consecutive_failures=?,opened_at=?,"
                "cooldown_until=?,probe_lease_id=NULL,probe_owner=NULL,probe_expires_at=NULL,"
                "state_version=state_version+1 WHERE endpoint='historical_daily_1d' "
                "AND state_version=?",
                (state, count, opened, cooldown, row["state_version"]),
            )
            if changed.rowcount != 1:
                raise DailyShadowRegistryUnavailable("daily circuit changed")
            self._insert_circuit_event(
                connection,
                event_id=f"event-{probe_lease_id}-resolved",
                event_type=event_type,
                observed_at=observed_at,
                before="HALF_OPEN",
                after=state,
                failure_class=None if success else "probe_failure",
            )
            return self._circuit_from_row(
                connection.execute(
                    "SELECT * FROM daily_shadow_circuit WHERE endpoint='historical_daily_1d'"
                ).fetchone()
            )

        return self._transaction(resolve)
