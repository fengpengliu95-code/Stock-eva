"""R2-F4.2 exact-session universe models, pure gates and control sidecar.

This module is intentionally additive.  It does not call a provider, normalize bars or publish
canonical market data; those boundaries remain owned by the existing refresh pipeline.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.providers.base import ProviderRawBatch

DOMAIN_PREFIX = "stock-eva/r2f4.2/"
SCHEMA_VERSION = 1
STORE_ID = "stock-eva-universe-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^(sh|sz)\.[0-9]{6}$")
INDEX_SYMBOLS = ("sh.000001", "sz.399001")


class UniverseStoreUnavailable(RuntimeError):
    """The sidecar cannot be proven safe for authority use."""


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError("universe canonical JSON invalid") from exc


def domain_sha256(domain: str, value: Any) -> str:
    if not domain.startswith(DOMAIN_PREFIX) or not domain.isascii():
        raise ValueError("universe hash domain invalid")
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


class UniverseMemberV1(_Frozen):
    symbol: str = Field(pattern=r"^(sh|sz)\.[0-9]{6}$")
    security_id: str = Field(min_length=1, max_length=128)
    member_kind: Literal["stock", "index"]
    scope_roles: tuple[Literal["effective_main_board", "required_user", "required_index"], ...]
    exchange: Literal["SSE", "SZSE"]
    board: Literal["main", "chinext", "star", "index"]
    list_date: date | None
    delist_date: date | None
    expected_trading_state: Literal["trading", "suspended", "not_yet_listed", "delisted", "unknown"]
    st_state: Literal["yes", "no", "not_applicable", "unknown"]
    state_source: str = Field(min_length=1, max_length=128)
    effective_from: date | None
    effective_to: date | None
    instrument_evidence_id: str = Field(min_length=1, max_length=128)
    member_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_and_hash(self) -> UniverseMemberV1:
        if self.scope_roles != tuple(sorted(set(self.scope_roles))):
            raise ValueError("scope roles must be sorted and unique")
        if (
            self.delist_date is not None
            and self.list_date is not None
            and self.delist_date < self.list_date
        ):
            raise ValueError("delist date precedes list date")
        if self.member_kind == "index":
            if (
                self.symbol not in INDEX_SYMBOLS
                or self.board != "index"
                or self.scope_roles != ("required_index",)
            ):
                raise ValueError("index member identity invalid")
        if self.exchange != ("SSE" if self.symbol.startswith("sh.") else "SZSE"):
            raise ValueError("member exchange does not match symbol")
        expected = domain_sha256("stock-eva/r2f4.2/universe-member/v1", self.preimage())
        if self.member_sha256 not in (None, expected):
            raise ValueError("member hash mismatch")
        object.__setattr__(self, "member_sha256", expected)
        return self

    def preimage(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        value.pop("member_sha256", None)
        return value


class UniverseCountsV1(_Frozen):
    total: int = Field(ge=0)
    trading: int = Field(ge=0)
    suspended: int = Field(ge=0)
    not_yet_listed: int = Field(ge=0)
    delisted: int = Field(ge=0)
    unknown: int = Field(ge=0)
    session_expected: int = Field(ge=0)
    loaded: int | None = Field(default=None, ge=0)
    critical_attribute_unknown_count: int = Field(ge=0)

    @model_validator(mode="after")
    def equation(self) -> UniverseCountsV1:
        if (
            self.total
            != self.trading + self.suspended + self.not_yet_listed + self.delisted + self.unknown
        ):
            raise ValueError("universe count equation mismatch")
        if self.session_expected != self.trading + self.suspended:
            raise ValueError("session expected equation mismatch")
        return self


class SourceRefsV1(_Frozen):
    calendar_generation_id: str
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    classification_generation_id: str
    classification_generation_sequence: int = Field(ge=1)
    classification_source: str
    classification_source_version: str
    classification_source_snapshot_date: date
    classification_observed_at: datetime
    classification_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_symbol_snapshot_id: str
    required_symbol_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_evidence_ids: tuple[str, ...]
    semantic_mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_version_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_id: Literal["baostock"]
    trade_date: date
    exact_pit_cutoff: datetime
    source_date_semantics: Literal["source_observed", "requested_unverified"]

    @model_validator(mode="after")
    def validate_dates(self) -> SourceRefsV1:
        if self.classification_observed_at.tzinfo is None or self.exact_pit_cutoff.tzinfo is None:
            raise ValueError("source timestamps must be timezone aware")
        if self.classification_source_snapshot_date > self.trade_date:
            raise ValueError("classification source is future visible")
        if self.classification_observed_at.astimezone(UTC) > self.exact_pit_cutoff.astimezone(UTC):
            raise ValueError("classification observed after PIT cutoff")
        if not self.instrument_evidence_ids:
            raise ValueError("instrument evidence cannot be empty")
        return self


class UniverseContractV1(_Frozen):
    contract_id: str = Field(min_length=1)
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    sequence: int = Field(ge=1)
    parent_contract_id: str | None = None
    schema_version: Literal[1] = 1
    universe_id: Literal["all-main-board-plus-required-symbols"]
    scope: Literal["all-main-board-plus-required-symbols"]
    trade_date: date
    calendar_generation_id: str
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    classification_generation_id: str
    exact_pit_cutoff: datetime
    source_refs: SourceRefsV1
    members: tuple[UniverseMemberV1, ...]
    counts: UniverseCountsV1
    classification_evidence_ids: tuple[str, ...]
    effective_main_board_ids: tuple[str, ...]
    required_additions_ids: tuple[str, ...]
    classification_evidence_partition_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    effective_main_board_partition_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_additions_partition_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_json: str
    publication_eligible: bool

    @model_validator(mode="after")
    def validate_contract(self) -> UniverseContractV1:
        if (
            self.source_refs.trade_date != self.trade_date
            or self.source_refs.calendar_sha256 != self.calendar_sha256
        ):
            raise ValueError("contract source reference mismatch")
        symbols = tuple(item.symbol for item in self.members)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("contract members must be sorted and unique")
        if self.sequence == 1 and self.parent_contract_id is not None:
            raise ValueError("first contract cannot have a parent")
        if self.sequence > 1 and self.parent_contract_id is None:
            raise ValueError("non-first contract requires a parent")
        base = {item.symbol for item in self.members if "effective_main_board" in item.scope_roles}
        additions = {
            item.symbol
            for item in self.members
            if "required_user" in item.scope_roles or "required_index" in item.scope_roles
        }
        if base & additions and any(
            item.symbol in base and "required_user" in item.scope_roles for item in self.members
        ):
            raise ValueError("partition overlap")
        if self.counts.total != len(self.members):
            raise ValueError("contract member count mismatch")
        expected = domain_sha256(
            "stock-eva/r2f4.2/universe-contract/v1", json.loads(self.payload_json)
        )
        if expected != self.contract_sha256 or self.contract_id != expected[:32]:
            raise ValueError("contract hash mismatch")
        if self.counts.unknown or self.counts.critical_attribute_unknown_count:
            if self.publication_eligible:
                raise ValueError("unknown contract cannot be publication eligible")
        return self


class UniverseHeadV1(_Frozen):
    sequence: int = Field(ge=1)
    contract_id: str
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    head_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    updated_at: datetime


class UniverseCandidateGate(_Frozen):
    schema_version: Literal[1] = 1
    universe_id: Literal["all-main-board-plus-required-symbols"]
    trade_date: date
    provider_id: Literal["baostock"]
    refresh_id: str
    endpoint_counts: tuple[tuple[str, int], ...]
    loaded: int | None
    session_expected: int
    missing: int
    extra: int
    duplicate: int
    unknown: int
    universe_raw_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    result: Literal["pass", "reject"]
    reason_code: str | None = None


def _member_counts(
    members: tuple[UniverseMemberV1, ...], *, loaded: int | None = None
) -> UniverseCountsV1:
    values = {
        state: sum(item.expected_trading_state == state for item in members)
        for state in ("trading", "suspended", "not_yet_listed", "delisted", "unknown")
    }
    return UniverseCountsV1(
        total=len(members),
        trading=values["trading"],
        suspended=values["suspended"],
        not_yet_listed=values["not_yet_listed"],
        delisted=values["delisted"],
        unknown=values["unknown"],
        session_expected=values["trading"] + values["suspended"],
        loaded=loaded,
        critical_attribute_unknown_count=sum(item.st_state == "unknown" for item in members),
    )


def _source_refs(value: SourceRefsV1 | dict[str, Any]) -> SourceRefsV1:
    return (
        value
        if isinstance(value, SourceRefsV1)
        else SourceRefsV1.model_validate(value, strict=False)
    )


def _source_state_identity(refs: SourceRefsV1) -> tuple[str, str]:
    preimage = {
        "trade_date": refs.trade_date.isoformat(),
        "provider_id": refs.provider_id,
        "source_version_digest": refs.source_version_digest,
        "source_refs_json": canonical_json_bytes(refs.model_dump(mode="json")).decode(),
    }
    digest = domain_sha256("stock-eva/r2f4.2/universe-source-state/v1", preimage)
    return digest[:32], digest


def build_universe_contract(
    *,
    trade_date: date,
    calendar_generation_id: str,
    calendar_sha256: str,
    classification_generation_id: str,
    classification_generation_sequence: int,
    classification_source: str,
    classification_source_version: str,
    classification_source_snapshot_date: date,
    classification_observed_at: datetime,
    exact_pit_cutoff: str | datetime,
    source_refs: SourceRefsV1 | dict[str, Any],
    members: tuple[UniverseMemberV1, ...],
    sequence: int = 1,
    parent_contract_id: str | None = None,
    created_at: datetime | None = None,
) -> UniverseContractV1:
    refs = _source_refs(source_refs)
    if isinstance(exact_pit_cutoff, str):
        exact_pit_cutoff = datetime.fromisoformat(exact_pit_cutoff.replace("Z", "+00:00"))
    members = tuple(sorted(members, key=lambda item: item.symbol))
    counts = _member_counts(members)
    classification_ids = tuple(sorted({item.instrument_evidence_id for item in members}))
    base_ids = tuple(
        sorted(item.symbol for item in members if "effective_main_board" in item.scope_roles)
    )
    addition_ids = tuple(
        sorted(
            item.symbol
            for item in members
            if "required_user" in item.scope_roles or "required_index" in item.scope_roles
        )
    )
    payload = {
        "schema_version": 1,
        "universe_id": "all-main-board-plus-required-symbols",
        "scope": "all-main-board-plus-required-symbols",
        "trade_date": trade_date.isoformat(),
        "calendar_generation_id": calendar_generation_id,
        "calendar_sha256": calendar_sha256,
        "classification_generation_id": classification_generation_id,
        "exact_pit_cutoff": exact_pit_cutoff.isoformat()
        if isinstance(exact_pit_cutoff, datetime)
        else exact_pit_cutoff,
        "source_refs": refs.model_dump(mode="json"),
        "members": [item.model_dump(mode="json") for item in members],
        "counts": counts.model_dump(mode="json"),
        "classification_evidence_ids": classification_ids,
        "effective_main_board_ids": base_ids,
        "required_additions_ids": addition_ids,
        "classification_evidence_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/classification-evidence/v1", classification_ids
        ),
        "effective_main_board_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/effective-main-board/v1", base_ids
        ),
        "required_additions_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/required-additions/v1", addition_ids
        ),
        "parent_contract_id": parent_contract_id,
        "sequence": sequence,
    }
    digest = domain_sha256("stock-eva/r2f4.2/universe-contract/v1", payload)
    return UniverseContractV1(
        contract_id=digest[:32],
        contract_sha256=digest,
        created_at=created_at or datetime.now(UTC),
        sequence=sequence,
        parent_contract_id=parent_contract_id,
        universe_id=payload["universe_id"],
        scope=payload["scope"],
        trade_date=trade_date,
        calendar_generation_id=calendar_generation_id,
        calendar_sha256=calendar_sha256,
        classification_generation_id=classification_generation_id,
        exact_pit_cutoff=exact_pit_cutoff,
        source_refs=refs,
        members=members,
        counts=counts,
        classification_evidence_ids=classification_ids,
        effective_main_board_ids=base_ids,
        required_additions_ids=addition_ids,
        classification_evidence_partition_sha256=payload[
            "classification_evidence_partition_sha256"
        ],
        effective_main_board_partition_sha256=payload["effective_main_board_partition_sha256"],
        required_additions_partition_sha256=payload["required_additions_partition_sha256"],
        payload_json=canonical_json_bytes(payload).decode(),
        publication_eligible=(counts.unknown == 0 and counts.critical_attribute_unknown_count == 0),
    )


def validate_provider_raw_batch(
    batch: ProviderRawBatch, contract: UniverseContractV1
) -> UniverseCandidateGate:
    """Validate a complete existing raw batch; never return partial rows."""
    if not isinstance(batch, ProviderRawBatch):
        raise ValueError("provider raw batch must be the existing ProviderRawBatch")
    batch = ProviderRawBatch.model_validate(batch.model_dump(mode="python"))
    if batch.provider_id.value != "baostock" or batch.request.trade_date != contract.trade_date:
        raise ValueError("provider/session identity mismatch")
    raw_digest = domain_sha256(
        "stock-eva/r2f4.2/raw-batch-projection/v1", batch.model_dump(mode="json")
    )
    expected = {
        item.symbol
        for item in contract.members
        if item.expected_trading_state in {"trading", "suspended"}
    }
    loaded: list[str] = []
    counts: list[tuple[str, int]] = []
    reason: str | None = None
    for endpoint_batch in batch.endpoint_batches:
        counts.append((endpoint_batch.endpoint.value, endpoint_batch.row_count))
        if endpoint_batch.endpoint.value in {"daily_astock", "index_history"}:
            for row in endpoint_batch.rows:
                symbol = getattr(row, "code", None)
                row_date = getattr(row, "date", None)
                if symbol is None or row_date != contract.trade_date:
                    reason = "UNIVERSE_SESSION_DRIFT"
                elif symbol in loaded:
                    reason = "UNIVERSE_DUPLICATE_SYMBOL"
                loaded.append(symbol)
    observed = set(loaded)
    missing = len(expected - observed)
    extra = len(observed - expected)
    duplicate = len(loaded) - len(observed)
    if reason is None and missing:
        reason = "UNIVERSE_MISSING_SYMBOL"
    if reason is None and extra:
        reason = "UNIVERSE_EXTRA_SYMBOL"
    if reason is None and duplicate:
        reason = "UNIVERSE_DUPLICATE_SYMBOL"
    result = (
        "pass" if reason is None and len(loaded) == contract.counts.session_expected else "reject"
    )
    if result == "reject" and reason is None:
        reason = "UNIVERSE_COUNT_MISMATCH"
    return UniverseCandidateGate(
        universe_id=contract.universe_id,
        trade_date=contract.trade_date,
        provider_id="baostock",
        refresh_id=batch.request.refresh_id,
        endpoint_counts=tuple(counts),
        loaded=len(loaded),
        session_expected=contract.counts.session_expected,
        missing=missing,
        extra=extra,
        duplicate=duplicate,
        unknown=contract.counts.unknown,
        universe_raw_projection_sha256=raw_digest,
        result=result,
        reason_code=reason,
    )


UNIVERSE_DDL = """
PRAGMA user_version = 1;
CREATE TABLE universe_meta (
    meta_key TEXT PRIMARY KEY CHECK
    (meta_key IN ('schema_version','schema_digest','store_id','db_inode')),
    meta_value TEXT NOT NULL
);
CREATE TABLE universe_source_state (
    source_state_id TEXT PRIMARY KEY, source_state_sha256 TEXT NOT NULL UNIQUE,
    trade_date TEXT NOT NULL, provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    source_version_digest TEXT NOT NULL, source_refs_json TEXT NOT NULL, verified_at TEXT NOT NULL
);
CREATE TABLE universe_contract (
    contract_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE CHECK (sequence > 0),
    parent_contract_id TEXT REFERENCES universe_contract(contract_id), trade_date TEXT NOT NULL,
    universe_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    scope TEXT NOT NULL, calendar_generation_id TEXT NOT NULL, calendar_sha256 TEXT NOT NULL,
    classification_generation_id TEXT NOT NULL, classification_generation_sequence INTEGER NOT NULL,
    classification_source TEXT NOT NULL, classification_source_version TEXT NOT NULL,
    classification_source_snapshot_date TEXT NOT NULL, classification_observed_at TEXT NOT NULL,
    classification_snapshot_sha256 TEXT NOT NULL, exact_pit_cutoff TEXT NOT NULL,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'), source_date_semantics TEXT NOT NULL,
    source_state_id TEXT NOT NULL REFERENCES universe_source_state(source_state_id),
    source_state_sha256 TEXT NOT NULL, source_version_digest TEXT NOT NULL,
    required_symbol_snapshot_id TEXT NOT NULL, required_symbol_snapshot_sha256 TEXT NOT NULL,
    instrument_evidence_ids_json TEXT NOT NULL, counts_json TEXT NOT NULL,
    layer_counts_json TEXT NOT NULL, source_refs_json TEXT NOT NULL,
    classification_evidence_ids_json TEXT NOT NULL,
    classification_evidence_partition_sha256 TEXT NOT NULL,
    effective_main_board_ids_json TEXT NOT NULL,
    effective_main_board_partition_sha256 TEXT NOT NULL, required_additions_ids_json TEXT NOT NULL,
    required_additions_partition_sha256 TEXT NOT NULL, payload_json TEXT NOT NULL,
    contract_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE universe_member (
    contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id), symbol TEXT NOT NULL,
    security_id TEXT NOT NULL, member_sha256 TEXT NOT NULL, member_json TEXT NOT NULL,
    PRIMARY KEY (contract_id, symbol), UNIQUE (contract_id, security_id)
);
CREATE TABLE universe_semantic_mapping (
    mapping_id TEXT PRIMARY KEY, mapping_version TEXT NOT NULL,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'), source_schema TEXT NOT NULL,
    payload_json TEXT NOT NULL, mapping_sha256 TEXT NOT NULL UNIQUE,
    authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified'))
);
CREATE TABLE universe_instrument_evidence (
    evidence_id TEXT PRIMARY KEY, evidence_sha256 TEXT NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified')),
    artifact_origin TEXT NOT NULL CHECK
    (artifact_origin IN ('local_reviewed_fixture','production_reviewed_artifact')),
    security_id TEXT NOT NULL, symbol TEXT NOT NULL, mapping_id TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    mapping_sha256 TEXT NOT NULL REFERENCES universe_semantic_mapping(mapping_sha256),
    source_snapshot_date TEXT NOT NULL, evidence_trade_date TEXT NOT NULL,
    exclusion_reason TEXT NOT NULL,
    index_role TEXT NOT NULL CHECK (index_role IN ('required_index','not_applicable')),
    source_date_semantics TEXT NOT NULL, evidence_json TEXT NOT NULL
);
CREATE TABLE universe_required_symbol_snapshot (
    snapshot_id TEXT PRIMARY KEY, snapshot_sha256 TEXT NOT NULL UNIQUE,
    snapshot_token_digest TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    symbol_count INTEGER NOT NULL CHECK (symbol_count >= 0), snapshot_json TEXT NOT NULL
);
CREATE TABLE universe_publication_context (
    context_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, trade_date TEXT NOT NULL,
    manifest_ref TEXT NOT NULL, evidence_refs_json TEXT NOT NULL,
    publication_lineage_json TEXT NOT NULL,
    publication_lineage_sha256 TEXT NOT NULL, context_sha256 TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status = 'ready'), created_at TEXT NOT NULL,
    UNIQUE (run_id, trade_date)
);
CREATE TABLE contract_evidence (
    contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
    evidence_id TEXT NOT NULL REFERENCES universe_instrument_evidence(evidence_id),
    evidence_role TEXT NOT NULL CHECK
    (evidence_role IN ('classification_evidence','effective_main_board',
                       'required_additions','member')),
    security_id TEXT NOT NULL, symbol TEXT NOT NULL, exclusion_reason TEXT NOT NULL,
    PRIMARY KEY (contract_id, evidence_id, evidence_role)
);
CREATE TABLE contract_required_snapshot (
    contract_id TEXT PRIMARY KEY REFERENCES universe_contract(contract_id),
    snapshot_id TEXT NOT NULL REFERENCES universe_required_symbol_snapshot(snapshot_id),
    snapshot_sha256 TEXT NOT NULL
);
CREATE TABLE universe_attempt (
    attempt_id TEXT PRIMARY KEY, dedup_key TEXT NOT NULL UNIQUE,
    hook_kind TEXT NOT NULL CHECK (hook_kind = 'universe_post_success'), refresh_id TEXT NOT NULL,
    canonical_run_id TEXT NOT NULL, source_version_digest TEXT NOT NULL, trade_date TEXT NOT NULL,
    operation_day TEXT NOT NULL,
    attempt_status TEXT NOT NULL CHECK (attempt_status = 'RUNNING'),
    request_budget INTEGER NOT NULL CHECK (request_budget = 1),
    classification_max_attempts INTEGER NOT NULL CHECK (classification_max_attempts = 1),
    created_at TEXT NOT NULL, planned_sha256 TEXT NOT NULL UNIQUE,
    UNIQUE (trade_date, operation_day)
);
CREATE TABLE universe_attempt_result (
    attempt_id TEXT PRIMARY KEY REFERENCES universe_attempt(attempt_id),
    terminal_status TEXT NOT NULL,
    classification_request_count INTEGER NOT NULL CHECK (classification_request_count IN (0,1)),
    reason_code TEXT NOT NULL,
    source_state_id TEXT REFERENCES universe_source_state(source_state_id),
    finished_at TEXT NOT NULL, result_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE universe_head (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1), sequence INTEGER NOT NULL,
    contract_id TEXT NOT NULL, contract_sha256 TEXT NOT NULL, head_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL, FOREIGN KEY (contract_id) REFERENCES universe_contract(contract_id)
);
CREATE TRIGGER universe_meta_no_update BEFORE UPDATE ON universe_meta
BEGIN SELECT RAISE(ABORT,'immutable_meta'); END;
CREATE TRIGGER universe_meta_no_delete BEFORE DELETE ON universe_meta
BEGIN SELECT RAISE(ABORT,'immutable_meta'); END;
CREATE TRIGGER universe_source_state_no_update BEFORE UPDATE ON universe_source_state
BEGIN SELECT RAISE(ABORT,'immutable_source_state'); END;
CREATE TRIGGER universe_source_state_no_delete BEFORE DELETE ON universe_source_state
BEGIN SELECT RAISE(ABORT,'immutable_source_state'); END;
CREATE TRIGGER universe_contract_no_update BEFORE UPDATE ON universe_contract
BEGIN SELECT RAISE(ABORT,'immutable_contract'); END;
CREATE TRIGGER universe_contract_no_delete BEFORE DELETE ON universe_contract
BEGIN SELECT RAISE(ABORT,'immutable_contract'); END;
CREATE TRIGGER universe_member_no_update BEFORE UPDATE ON universe_member
BEGIN SELECT RAISE(ABORT,'immutable_member'); END;
CREATE TRIGGER universe_member_no_delete BEFORE DELETE ON universe_member
BEGIN SELECT RAISE(ABORT,'immutable_member'); END;
CREATE TRIGGER universe_mapping_no_update BEFORE UPDATE ON universe_semantic_mapping
BEGIN SELECT RAISE(ABORT,'immutable_mapping'); END;
CREATE TRIGGER universe_mapping_no_delete BEFORE DELETE ON universe_semantic_mapping
BEGIN SELECT RAISE(ABORT,'immutable_mapping'); END;
CREATE TRIGGER universe_evidence_no_update BEFORE UPDATE ON universe_instrument_evidence
BEGIN SELECT RAISE(ABORT,'immutable_evidence'); END;
CREATE TRIGGER universe_evidence_no_delete BEFORE DELETE ON universe_instrument_evidence
BEGIN SELECT RAISE(ABORT,'immutable_evidence'); END;
CREATE TRIGGER universe_snapshot_no_update BEFORE UPDATE ON universe_required_symbol_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_snapshot'); END;
CREATE TRIGGER universe_snapshot_no_delete BEFORE DELETE ON universe_required_symbol_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_snapshot'); END;
CREATE TRIGGER universe_publication_context_no_update BEFORE UPDATE ON universe_publication_context
BEGIN SELECT RAISE(ABORT,'immutable_publication_context'); END;
CREATE TRIGGER universe_publication_context_no_delete BEFORE DELETE ON universe_publication_context
BEGIN SELECT RAISE(ABORT,'immutable_publication_context'); END;
CREATE TRIGGER contract_evidence_no_update BEFORE UPDATE ON contract_evidence
BEGIN SELECT RAISE(ABORT,'immutable_contract_evidence'); END;
CREATE TRIGGER contract_evidence_no_delete BEFORE DELETE ON contract_evidence
BEGIN SELECT RAISE(ABORT,'immutable_contract_evidence'); END;
CREATE TRIGGER contract_snapshot_no_update BEFORE UPDATE ON contract_required_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_contract_snapshot'); END;
CREATE TRIGGER contract_snapshot_no_delete BEFORE DELETE ON contract_required_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_contract_snapshot'); END;
CREATE TRIGGER universe_attempt_no_update BEFORE UPDATE ON universe_attempt
BEGIN SELECT RAISE(ABORT,'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_no_delete BEFORE DELETE ON universe_attempt
BEGIN SELECT RAISE(ABORT,'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_result_no_update BEFORE UPDATE ON universe_attempt_result
BEGIN SELECT RAISE(ABORT,'immutable_attempt_result'); END;
CREATE TRIGGER universe_attempt_result_no_delete BEFORE DELETE ON universe_attempt_result
BEGIN SELECT RAISE(ABORT,'immutable_attempt_result'); END;
""".strip()
UNIVERSE_SCHEMA_DIGEST = domain_sha256("stock-eva/r2f4.2/universe-schema/v1", UNIVERSE_DDL)
_EXPECTED_TABLES = {
    "universe_meta",
    "universe_source_state",
    "universe_contract",
    "universe_member",
    "universe_semantic_mapping",
    "universe_instrument_evidence",
    "universe_required_symbol_snapshot",
    "universe_publication_context",
    "contract_evidence",
    "contract_required_snapshot",
    "universe_attempt",
    "universe_attempt_result",
    "universe_head",
}


def _head_digest(sequence: int, contract_id: str, contract_sha256: str) -> str:
    return domain_sha256(
        "stock-eva/r2f4.2/universe-head/v1",
        {
            "singleton_id": 1,
            "sequence": sequence,
            "contract_id": contract_id,
            "contract_sha256": contract_sha256,
        },
    )


@contextmanager
def _file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


class UniverseSidecarStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.lock_path = self.path.with_name(self.path.name + ".lock")

    def _connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            uri = f"file:{self.path.resolve()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=0.25)
        else:
            connection = sqlite3.connect(self.path, timeout=0.25)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=250")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _file_lock(self.lock_path):
            connection = self._connect()
            try:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if tables:
                    self._validate(connection)
                    return
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.executescript(UNIVERSE_DDL)
                inode = os.stat(self.path).st_ino
                connection.executemany(
                    "INSERT INTO universe_meta VALUES (?,?)",
                    (
                        ("schema_version", "1"),
                        ("schema_digest", UNIVERSE_SCHEMA_DIGEST),
                        ("store_id", STORE_ID),
                        ("db_inode", str(inode)),
                    ),
                )
                connection.commit()
            except (OSError, sqlite3.Error, UniverseStoreUnavailable) as exc:
                connection.rollback()
                raise UniverseStoreUnavailable("universe sidecar bootstrap unavailable") from exc
            finally:
                connection.close()

    def _validate(self, connection: sqlite3.Connection) -> None:
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if tables != _EXPECTED_TABLES:
                raise UniverseStoreUnavailable("universe schema table set invalid")
            if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise UniverseStoreUnavailable("universe schema version invalid")
            trigger_names = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
            }
            expected_triggers = {
                "universe_meta_no_update",
                "universe_meta_no_delete",
                "universe_source_state_no_update",
                "universe_source_state_no_delete",
                "universe_contract_no_update",
                "universe_contract_no_delete",
                "universe_member_no_update",
                "universe_member_no_delete",
                "universe_mapping_no_update",
                "universe_mapping_no_delete",
                "universe_evidence_no_update",
                "universe_evidence_no_delete",
                "universe_snapshot_no_update",
                "universe_snapshot_no_delete",
                "universe_publication_context_no_update",
                "universe_publication_context_no_delete",
                "contract_evidence_no_update",
                "contract_evidence_no_delete",
                "contract_snapshot_no_update",
                "contract_snapshot_no_delete",
                "universe_attempt_no_update",
                "universe_attempt_no_delete",
                "universe_attempt_result_no_update",
                "universe_attempt_result_no_delete",
            }
            if trigger_names != expected_triggers:
                raise UniverseStoreUnavailable("universe schema trigger set invalid")
            meta = dict(
                connection.execute("SELECT meta_key,meta_value FROM universe_meta").fetchall()
            )
            if meta != {
                "schema_version": "1",
                "schema_digest": UNIVERSE_SCHEMA_DIGEST,
                "store_id": STORE_ID,
                "db_inode": str(os.stat(self.path).st_ino),
            }:
                raise UniverseStoreUnavailable("universe sidecar identity invalid")
            if (
                connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                or connection.execute("PRAGMA foreign_key_check").fetchall()
            ):
                raise UniverseStoreUnavailable("universe sidecar integrity invalid")
        except (OSError, sqlite3.Error, KeyError) as exc:
            raise UniverseStoreUnavailable("universe sidecar unreadable") from exc

    def _read_contract(
        self, connection: sqlite3.Connection, contract_id: str
    ) -> UniverseContractV1:
        row = connection.execute(
            "SELECT payload_json,contract_sha256,contract_id,sequence,parent_contract_id, "
            "created_at "
            "FROM universe_contract WHERE contract_id=?",
            (contract_id,),
        ).fetchone()
        if row is None:
            raise UniverseStoreUnavailable("universe contract missing")
        try:
            payload = json.loads(row["payload_json"])
            digest = domain_sha256("stock-eva/r2f4.2/universe-contract/v1", payload)
            if (
                digest != row["contract_sha256"]
                or row["contract_id"] != digest[:32]
                or canonical_json_bytes(payload).decode() != row["payload_json"]
            ):
                raise UniverseStoreUnavailable("universe contract hash invalid")
            values = dict(payload)
            values.update(
                contract_id=row["contract_id"],
                contract_sha256=row["contract_sha256"],
                created_at=datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")),
                sequence=row["sequence"],
                parent_contract_id=row["parent_contract_id"],
                payload_json=row["payload_json"],
                publication_eligible=(
                    payload["counts"]["unknown"] == 0
                    and payload["counts"]["critical_attribute_unknown_count"] == 0
                ),
            )
            contract = UniverseContractV1.model_validate(values, strict=False)
            members = connection.execute(
                "SELECT symbol,member_sha256,member_json FROM universe_member "
                "WHERE contract_id=? ORDER BY symbol",
                (contract_id,),
            ).fetchall()
            if tuple(item["symbol"] for item in members) != tuple(
                item.symbol for item in contract.members
            ):
                raise UniverseStoreUnavailable("universe member rows mismatch")
            for item in members:
                member = UniverseMemberV1.model_validate_json(item["member_json"])
                if member.symbol != item["symbol"] or member.member_sha256 != item["member_sha256"]:
                    raise UniverseStoreUnavailable("universe member hash invalid")
            return contract
        except (TypeError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
            raise UniverseStoreUnavailable("universe contract unavailable") from exc

    def read_head(self) -> UniverseHeadV1 | None:
        if not self.path.exists():
            raise UniverseStoreUnavailable("universe sidecar missing")
        connection = self._connect(readonly=True)
        try:
            self._validate(connection)
            row = connection.execute(
                "SELECT sequence,contract_id,contract_sha256,head_sha256,updated_at "
                "FROM universe_head WHERE singleton_id=1"
            ).fetchone()
            if row is None:
                return None
            expected = _head_digest(row["sequence"], row["contract_id"], row["contract_sha256"])
            if expected != row["head_sha256"]:
                raise UniverseStoreUnavailable("universe head hash invalid")
            contract = self._read_contract(connection, row["contract_id"])
            if (
                contract.sequence != row["sequence"]
                or contract.contract_sha256 != row["contract_sha256"]
            ):
                raise UniverseStoreUnavailable("universe head contract mismatch")
            current = contract
            visited = {current.contract_id}
            while current.sequence > 1:
                if not current.parent_contract_id or current.parent_contract_id in visited:
                    raise UniverseStoreUnavailable("universe parent chain invalid")
                parent = self._read_contract(connection, current.parent_contract_id)
                if parent.sequence != current.sequence - 1:
                    raise UniverseStoreUnavailable("universe parent sequence invalid")
                visited.add(parent.contract_id)
                current = parent
            values = dict(row)
            values["updated_at"] = datetime.fromisoformat(
                values["updated_at"].replace("Z", "+00:00")
            )
            return UniverseHeadV1.model_validate(values)
        except sqlite3.Error as exc:
            raise UniverseStoreUnavailable("universe sidecar unreadable") from exc
        finally:
            connection.close()

    def promote(
        self,
        contract: UniverseContractV1,
        *,
        expected_sequence: int,
        expected_head_sha256: str | None,
    ) -> UniverseHeadV1:
        with _file_lock(self.lock_path):
            connection = self._connect()
            try:
                self._validate(connection)
                row = connection.execute(
                    "SELECT sequence,contract_sha256 FROM universe_head WHERE singleton_id=1"
                ).fetchone()
                actual_sequence = 0 if row is None else row["sequence"]
                actual_hash = None if row is None else self._current_head_hash(connection)
                if actual_sequence != expected_sequence or (
                    expected_head_sha256 is not None and actual_hash != expected_head_sha256
                ):
                    raise UniverseStoreUnavailable("universe head CAS conflict")
                if contract.sequence != expected_sequence + 1:
                    raise UniverseStoreUnavailable("universe sequence invalid")
                connection.execute("BEGIN IMMEDIATE")
                source_state_id, source_state_sha = _source_state_identity(contract.source_refs)
                source_refs_json = canonical_json_bytes(
                    contract.source_refs.model_dump(mode="json")
                ).decode()
                connection.execute(
                    "INSERT OR IGNORE INTO universe_source_state VALUES (?,?,?,?,?,?,?)",
                    (
                        source_state_id,
                        source_state_sha,
                        contract.trade_date.isoformat(),
                        "baostock",
                        contract.source_refs.source_version_digest,
                        source_refs_json,
                        contract.source_refs.classification_observed_at.isoformat(),
                    ),
                )
                snapshot_json = canonical_json_bytes(
                    {
                        "schema_version": 1,
                        "symbols": list(contract.required_additions_ids),
                    }
                ).decode()
                connection.execute(
                    "INSERT OR IGNORE INTO universe_required_symbol_snapshot VALUES (?,?,?,?,?,?)",
                    (
                        contract.source_refs.required_symbol_snapshot_id,
                        contract.source_refs.required_symbol_snapshot_sha256,
                        contract.source_refs.required_symbol_snapshot_sha256,
                        1,
                        len(contract.required_additions_ids),
                        snapshot_json,
                    ),
                )
                refs = contract.source_refs
                contract_columns = (
                    "contract_id",
                    "sequence",
                    "parent_contract_id",
                    "trade_date",
                    "universe_id",
                    "schema_version",
                    "scope",
                    "calendar_generation_id",
                    "calendar_sha256",
                    "classification_generation_id",
                    "classification_generation_sequence",
                    "classification_source",
                    "classification_source_version",
                    "classification_source_snapshot_date",
                    "classification_observed_at",
                    "classification_snapshot_sha256",
                    "exact_pit_cutoff",
                    "provider_id",
                    "source_date_semantics",
                    "source_state_id",
                    "source_state_sha256",
                    "source_version_digest",
                    "required_symbol_snapshot_id",
                    "required_symbol_snapshot_sha256",
                    "instrument_evidence_ids_json",
                    "counts_json",
                    "layer_counts_json",
                    "source_refs_json",
                    "classification_evidence_ids_json",
                    "classification_evidence_partition_sha256",
                    "effective_main_board_ids_json",
                    "effective_main_board_partition_sha256",
                    "required_additions_ids_json",
                    "required_additions_partition_sha256",
                    "payload_json",
                    "contract_sha256",
                    "created_at",
                )
                contract_values = (
                    contract.contract_id,
                    contract.sequence,
                    contract.parent_contract_id,
                    contract.trade_date.isoformat(),
                    contract.universe_id,
                    contract.schema_version,
                    contract.scope,
                    contract.calendar_generation_id,
                    contract.calendar_sha256,
                    contract.classification_generation_id,
                    refs.classification_generation_sequence,
                    refs.classification_source,
                    refs.classification_source_version,
                    refs.classification_source_snapshot_date.isoformat(),
                    refs.classification_observed_at.isoformat(),
                    refs.classification_snapshot_sha256,
                    contract.exact_pit_cutoff.isoformat(),
                    "baostock",
                    refs.source_date_semantics,
                    source_state_id,
                    source_state_sha,
                    refs.source_version_digest,
                    refs.required_symbol_snapshot_id,
                    refs.required_symbol_snapshot_sha256,
                    canonical_json_bytes(refs.instrument_evidence_ids).decode(),
                    canonical_json_bytes(contract.counts.model_dump(mode="json")).decode(),
                    canonical_json_bytes(
                        {
                            "classification_evidence": len(contract.classification_evidence_ids),
                            "effective_main_board": len(contract.effective_main_board_ids),
                            "required_additions": len(contract.required_additions_ids),
                        }
                    ).decode(),
                    source_refs_json,
                    canonical_json_bytes(contract.classification_evidence_ids).decode(),
                    contract.classification_evidence_partition_sha256,
                    canonical_json_bytes(contract.effective_main_board_ids).decode(),
                    contract.effective_main_board_partition_sha256,
                    canonical_json_bytes(contract.required_additions_ids).decode(),
                    contract.required_additions_partition_sha256,
                    contract.payload_json,
                    contract.contract_sha256,
                    contract.created_at.isoformat(),
                )
                connection.execute(
                    f"INSERT INTO universe_contract ({','.join(contract_columns)}) "
                    f"VALUES ({','.join('?' for _ in contract_columns)})",
                    contract_values,
                )
                for member in contract.members:
                    connection.execute(
                        "INSERT INTO universe_member "
                        "(contract_id,symbol,security_id,member_sha256,member_json) "
                        "VALUES (?,?,?,?,?)",
                        (
                            contract.contract_id,
                            member.symbol,
                            member.security_id,
                            member.member_sha256,
                            canonical_json_bytes(member.model_dump(mode="json")).decode(),
                        ),
                    )
                connection.execute(
                    "INSERT INTO contract_required_snapshot VALUES (?,?,?)",
                    (
                        contract.contract_id,
                        refs.required_symbol_snapshot_id,
                        refs.required_symbol_snapshot_sha256,
                    ),
                )
                head_sha = _head_digest(
                    contract.sequence, contract.contract_id, contract.contract_sha256
                )
                if row is None:
                    connection.execute(
                        "INSERT INTO universe_head VALUES (1,?,?,?,?,?)",
                        (
                            contract.sequence,
                            contract.contract_id,
                            contract.contract_sha256,
                            head_sha,
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                else:
                    updated = connection.execute(
                        "UPDATE universe_head SET sequence=?,contract_id=?,contract_sha256=?, "
                        "head_sha256=?,updated_at=? WHERE singleton_id=1 AND sequence=?",
                        (
                            contract.sequence,
                            contract.contract_id,
                            contract.contract_sha256,
                            head_sha,
                            datetime.now(UTC).isoformat(),
                            expected_sequence,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise UniverseStoreUnavailable("universe head CAS conflict")
                connection.commit()
                return self.read_head()  # type: ignore[return-value]
            except (sqlite3.Error, OSError, ValueError, UniverseStoreUnavailable) as exc:
                connection.rollback()
                if isinstance(exc, UniverseStoreUnavailable):
                    raise
                raise UniverseStoreUnavailable("universe promotion unavailable") from exc
            finally:
                connection.close()

    @staticmethod
    def _current_head_hash(connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            "SELECT head_sha256 FROM universe_head WHERE singleton_id=1"
        ).fetchone()
        return None if row is None else row[0]
