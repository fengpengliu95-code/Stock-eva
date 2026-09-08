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

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.app.market.providers.base import (
    ProviderEndpoint,
    ProviderRawBatch,
    TransportOutcome,
)

DOMAIN_PREFIX = "stock-eva/r2f4.2/"
SCHEMA_VERSION = 1
STORE_ID = "stock-eva-universe-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^(sh|sz)\.[0-9]{6}$")
INDEX_SYMBOLS = ("sh.000001", "sz.399001")
_MAPPING_SCHEMAS = frozenset(
    {"daily_astock.v1", "classification.instrument.v1", "classification.instrument.v2"}
)


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
    security_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
    )
    member_kind: Literal["stock", "index"]
    scope_roles: tuple[Literal["effective_main_board", "required_user", "required_index"], ...]
    exchange: Literal["SSE", "SZSE"]
    board: Literal["main", "chinext", "star", "index"]
    list_date: date | None
    delist_date: date | None
    expected_trading_state: Literal["trading", "suspended", "not_yet_listed", "delisted", "unknown"]
    st_state: Literal["yes", "no", "not_applicable", "unknown"]
    state_source: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
    )
    effective_from: date | None
    effective_to: date | None
    instrument_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
    )
    # The exclusion is retained on the evidence object, not used to silently
    # remove an effective member.  Keeping it on the member as well makes the
    # persisted member/evidence link completely bidirectional.
    exclusion_reason: str = Field(default="", max_length=128)
    member_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_and_hash(self) -> UniverseMemberV1:
        if not self.scope_roles or self.scope_roles != tuple(sorted(set(self.scope_roles))):
            raise ValueError("scope roles must be sorted and unique")
        if any(
            role not in {"effective_main_board", "required_user", "required_index"}
            for role in self.scope_roles
        ):
            raise ValueError("unknown member scope role")
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
                or self.expected_trading_state != "trading"
            ):
                raise ValueError("index member identity invalid")
        elif self.board == "index" or "required_index" in self.scope_roles:
            raise ValueError("stock/index member identity invalid")
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
    calendar_generation_id: str = Field(min_length=1, max_length=128)
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    classification_generation_id: str = Field(min_length=1, max_length=128)
    classification_generation_sequence: int = Field(ge=1)
    classification_source: str = Field(min_length=1, max_length=128)
    classification_source_version: str = Field(min_length=1, max_length=128)
    classification_source_snapshot_date: date
    classification_observed_at: datetime
    classification_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_symbol_snapshot_id: str = Field(min_length=1, max_length=128)
    required_symbol_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_evidence_ids: tuple[str, ...]
    semantic_mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_version_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_id: Literal["baostock"]
    trade_date: date
    exact_pit_cutoff: datetime
    source_date_semantics: Literal["source_observed", "requested_unverified"]

    @field_validator("classification_observed_at", "exact_pit_cutoff")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source timestamp must be timezone aware")
        return value.astimezone(UTC)

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
        if any(
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value)
            for value in self.instrument_evidence_ids
        ):
            raise ValueError("instrument evidence ID is unsafe")
        if self.instrument_evidence_ids != tuple(sorted(set(self.instrument_evidence_ids))):
            raise ValueError("instrument evidence IDs must be sorted and unique")
        if self.calendar_generation_id == "" or self.classification_generation_id == "":
            raise ValueError("source generation identity cannot be empty")
        return self


class UniverseSemanticMappingV1(_Frozen):
    mapping_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    mapping_version: str = Field(min_length=1, max_length=64)
    provider_id: Literal["baostock"]
    source_schema: str = Field(min_length=1, max_length=128)
    payload_json: str
    mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_status: Literal["reviewed", "unqualified"]

    @model_validator(mode="after")
    def validate_mapping(self) -> UniverseSemanticMappingV1:
        payload = json.loads(self.payload_json)
        if not isinstance(payload, dict):
            raise ValueError("semantic mapping payload must be an object")
        if self.source_schema not in _MAPPING_SCHEMAS:
            raise ValueError("semantic mapping schema is not closed")
        if self.authority_status == "reviewed" and not payload:
            raise ValueError("empty semantic mapping cannot establish authority")
        if self.source_schema == "daily_astock.v1":
            tradestatus = payload.get("tradestatus")
            if (
                not isinstance(tradestatus, dict)
                or tradestatus.get("trading") != "1"
                or tradestatus.get("suspended") != "0"
            ):
                raise ValueError("daily semantic mapping does not close tradestatus")
        if payload.get("mapping_id") not in (None, self.mapping_id):
            raise ValueError("semantic mapping ID mismatch")
        if payload.get("mapping_version") not in (None, self.mapping_version):
            raise ValueError("semantic mapping version mismatch")
        if self.source_schema == "classification.instrument.v2":
            required = {
                "mapping_id",
                "mapping_version",
                "security_type",
                "exchange",
                "board",
                "list_date",
                "delist_date",
                "listing_status",
                "daily_trade_status",
                "suspension_state",
                "st_state",
                "expected_trading_state",
            }
            if set(payload) != required:
                raise ValueError("reviewed instrument mapping payload is incomplete")
            if (
                payload["mapping_id"] != self.mapping_id
                or payload["mapping_version"] != self.mapping_version
            ):
                raise ValueError("semantic mapping identity is not authoritative")
            if payload["security_type"] not in {"stock", "index"}:
                raise ValueError("semantic mapping security type is not closed")
            if payload["exchange"] not in {"SSE", "SZSE"}:
                raise ValueError("semantic mapping exchange is not closed")
            if payload["board"] not in {"main", "chinext", "star", "bse", "index", "other"}:
                raise ValueError("semantic mapping board is not closed")
            if payload["suspension_state"] not in {
                "trading",
                "suspended",
                "not_supplied",
                "unknown",
            }:
                raise ValueError("semantic mapping suspension state is not closed")
            if payload["st_state"] not in {"yes", "no", "not_applicable", "unknown"}:
                raise ValueError("semantic mapping ST state is not closed")
            if payload["expected_trading_state"] not in {
                "trading",
                "suspended",
                "not_yet_listed",
                "delisted",
                "unknown",
            }:
                raise ValueError("semantic mapping trading state is not closed")
        expected = domain_sha256(
            "stock-eva/r2f4.2/semantic-mapping/v1",
            {
                "mapping_id": self.mapping_id,
                "mapping_version": self.mapping_version,
                "provider_id": self.provider_id,
                "source_schema": self.source_schema,
                "payload": payload,
            },
        )
        if expected != self.mapping_sha256:
            raise ValueError("semantic mapping hash mismatch")
        return self


class UniverseInstrumentEvidenceV1(_Frozen):
    evidence_id: str = Field(min_length=1, max_length=128)
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_id: Literal["baostock"]
    authority_status: Literal["reviewed", "unqualified"]
    artifact_origin: Literal["local_reviewed_fixture", "production_reviewed_artifact"]
    security_id: str = Field(min_length=1, max_length=128)
    symbol: str = Field(pattern=r"^(sh|sz)\.[0-9]{6}$")
    mapping_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
    )
    mapping_version: str
    mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_snapshot_date: date
    evidence_trade_date: date
    exclusion_reason: str
    index_role: Literal["required_index", "not_applicable"]
    source_date_semantics: Literal["source_observed", "requested_unverified"]
    evidence_json: str
    adapter_version: str = Field(default="r2f2.v1", min_length=1, max_length=64)
    source_schema: str = Field(default="classification.instrument.v1", min_length=1, max_length=128)
    security_type: Literal["stock", "index", "fund", "etf", "bond", "other"] = "stock"
    exchange: Literal["SSE", "SZSE"] | None = None
    board: Literal["main", "chinext", "star", "bse", "index", "other"] | None = None
    list_date: date | None = None
    delist_date: date | None = None
    listing_status: str = Field(default="", max_length=64)
    daily_trade_status: str | None = Field(default=None, max_length=64)
    suspension_state: Literal["trading", "suspended", "not_supplied", "unknown"] = "unknown"
    st_state: Literal["yes", "no", "not_applicable", "unknown"] = "unknown"
    expected_trading_state: (
        Literal["trading", "suspended", "not_yet_listed", "delisted", "unknown"] | None
    ) = None
    observed_at: datetime | None = None
    lineage_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_evidence(self) -> UniverseInstrumentEvidenceV1:
        try:
            payload = json.loads(self.evidence_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("instrument evidence JSON invalid") from exc
        if not isinstance(payload, dict):
            raise ValueError("instrument evidence JSON must be an object")
        if self.exchange is not None and self.exchange != (
            "SSE" if self.symbol.startswith("sh.") else "SZSE"
        ):
            raise ValueError("evidence exchange does not match symbol")
        if (
            self.list_date is not None
            and self.delist_date is not None
            and self.delist_date < self.list_date
        ):
            raise ValueError("evidence listing window invalid")
        if self.member_kind_for_evidence() == "index":
            if self.symbol not in INDEX_SYMBOLS or self.index_role != "required_index":
                raise ValueError("required index evidence identity invalid")
        elif self.index_role != "not_applicable":
            raise ValueError("stock evidence cannot have index role")
        if (
            self.source_date_semantics == "requested_unverified"
            and self.authority_status == "reviewed"
        ):
            raise ValueError("requested-unverified evidence cannot be reviewed")
        if self.source_schema == "classification.instrument.v2":
            if self.list_date is None:
                raise ValueError("instrument listing window evidence is incomplete")
            if self.member_kind_for_evidence() == "index":
                if (
                    self.security_type != "index"
                    or self.board != "index"
                    or self.expected_trading_state != "trading"
                    or self.suspension_state != "trading"
                    or self.st_state != "not_applicable"
                ):
                    raise ValueError("required index evidence semantics are incomplete")
        if (
            self.source_schema == "classification.instrument.v1"
            and self.authority_status == "reviewed"
            and self.artifact_origin == "production_reviewed_artifact"
        ):
            raise ValueError("legacy classification evidence cannot establish authority")
        if (
            self.artifact_origin == "production_reviewed_artifact"
            and self.source_schema != "classification.instrument.v2"
        ):
            raise ValueError("production evidence schema is not the reviewed closed version")
        expected = domain_sha256("stock-eva/r2f4.2/instrument-evidence/v1", self.preimage())
        if expected != self.evidence_sha256:
            raise ValueError("instrument evidence hash mismatch")
        return self

    def member_kind_for_evidence(self) -> str:
        return "index" if self.index_role == "required_index" else (self.security_type or "stock")

    def preimage(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        value.pop("evidence_sha256", None)
        return value


class RequiredSymbolSnapshotV1(_Frozen):
    snapshot_id: str = Field(min_length=1, max_length=128)
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_token_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: Literal[1] = 1
    symbols: tuple[tuple[str, tuple[str, ...]], ...]

    @model_validator(mode="after")
    def validate_snapshot(self) -> RequiredSymbolSnapshotV1:
        if self.symbols != tuple(sorted(self.symbols)):
            raise ValueError("required snapshot symbols must be sorted")
        symbol_values = tuple(symbol for symbol, _ in self.symbols)
        if symbol_values != tuple(sorted(set(symbol_values))):
            raise ValueError("required snapshot contains duplicate symbol identity")
        if any(
            not _SYMBOL.fullmatch(symbol)
            or not roles
            or tuple(roles) != tuple(sorted(set(roles)))
            or any(
                role not in {"position", "watchlist", "required_user", "required_index"}
                for role in roles
            )
            for symbol, roles in self.symbols
        ):
            raise ValueError("required snapshot identity invalid")
        if any(
            ("required_index" in roles) != (symbol in INDEX_SYMBOLS)
            for symbol, roles in self.symbols
        ):
            raise ValueError("required snapshot index role mismatch")
        expected = domain_sha256(
            "stock-eva/r2f4.2/required-symbol-snapshot/v1",
            {
                "schema_version": 1,
                "snapshot_token": self.snapshot_token_digest,
                "symbols": [
                    {"symbol": symbol, "roles": list(roles)} for symbol, roles in self.symbols
                ],
            },
        )
        if expected != self.snapshot_sha256:
            raise ValueError("required snapshot hash mismatch")
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
    classification_evidence_projection: tuple[dict[str, str], ...] = ()
    payload_json: str
    publication_eligible: bool
    classification_evidence_count: int | None = Field(default=None, ge=0)
    classification_evidence_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    effective_main_board_count: int | None = Field(default=None, ge=0)
    effective_main_board_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    required_additions_count: int | None = Field(default=None, ge=0)
    required_additions_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source_state_id: str | None = None
    source_state_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_contract(self) -> UniverseContractV1:
        if (
            self.source_refs.trade_date != self.trade_date
            or self.source_refs.calendar_sha256 != self.calendar_sha256
            or self.source_refs.calendar_generation_id != self.calendar_generation_id
            or self.source_refs.classification_generation_id != self.classification_generation_id
            or self.source_refs.exact_pit_cutoff != self.exact_pit_cutoff
        ):
            raise ValueError("contract source reference mismatch")
        symbols = tuple(item.symbol for item in self.members)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("contract members must be sorted and unique")
        if self.sequence == 1 and self.parent_contract_id is not None:
            raise ValueError("first contract cannot have a parent")
        if self.sequence > 1 and self.parent_contract_id is None:
            raise ValueError("non-first contract requires a parent")
        indexes = tuple(
            item
            for item in self.members
            if item.member_kind == "index" or "required_index" in item.scope_roles
        )
        if tuple(item.symbol for item in indexes) != INDEX_SYMBOLS:
            raise ValueError("required index set is incomplete")
        if any(item.expected_trading_state != "trading" for item in indexes):
            raise ValueError("required index must be trading")
        base = {item.symbol for item in self.members if "effective_main_board" in item.scope_roles}
        additions = {
            item.symbol
            for item in self.members
            if item.symbol not in base
            and ("required_user" in item.scope_roles or "required_index" in item.scope_roles)
        }
        if base & additions:
            raise ValueError("partition identity overlap")
        if self.counts.total != len(self.members):
            raise ValueError("contract member count mismatch")
        expected_counts = _member_counts(self.members)
        if self.counts.model_copy(update={"loaded": None}) != expected_counts:
            raise ValueError("contract counts are not derived from member states")
        expected_classification_ids = tuple(
            sorted(item.get("evidence_id", "") for item in self.classification_evidence_projection)
        )
        if expected_classification_ids != self.classification_evidence_ids:
            raise ValueError("classification evidence IDs are not derived from projection")
        expected_effective_ids = tuple(
            sorted(
                item.symbol for item in self.members if "effective_main_board" in item.scope_roles
            )
        )
        if expected_effective_ids != self.effective_main_board_ids:
            raise ValueError("effective main board IDs are not derived from members")
        expected_required_ids = tuple(
            sorted(
                item.symbol
                for item in self.members
                if item.symbol not in set(expected_effective_ids)
                and ("required_user" in item.scope_roles or "required_index" in item.scope_roles)
            )
        )
        if expected_required_ids != self.required_additions_ids:
            raise ValueError("required additions IDs are not derived from members")

        # Partition digests are over the normative projections, never over the
        # generated member digest.  The latter is a verification result and is
        # deliberately excluded from every partition preimage.
        def member_projection(item: UniverseMemberV1) -> dict[str, Any]:
            return item.preimage()

        partition_specs = (
            (
                self.classification_evidence_partition_sha256,
                "stock-eva/r2f4.2/universe-partition/classification-evidence/v1",
                lambda _item: True,
                "classification",
            ),
            (
                self.effective_main_board_partition_sha256,
                "stock-eva/r2f4.2/universe-partition/effective-main-board/v1",
                lambda item: "effective_main_board" in item.scope_roles,
                "effective main board",
            ),
            (
                self.required_additions_partition_sha256,
                "stock-eva/r2f4.2/universe-partition/required-additions/v1",
                lambda item: (
                    item.symbol
                    not in {
                        candidate.symbol
                        for candidate in self.members
                        if "effective_main_board" in candidate.scope_roles
                    }
                    and (
                        "required_user" in item.scope_roles or "required_index" in item.scope_roles
                    )
                ),
                "required additions",
            ),
        )
        for actual, domain, predicate, label in partition_specs:
            if label == "classification":
                projection = list(self.classification_evidence_projection) or [
                    {
                        "security_id": item.security_id,
                        "symbol": item.symbol,
                        "evidence_id": item.instrument_evidence_id,
                        "exclusion_reason": item.exclusion_reason,
                    }
                    for item in self.members
                ]
            else:
                projection = [member_projection(item) for item in self.members if predicate(item)]
            if actual != domain_sha256(domain, projection):
                raise ValueError(f"{label} partition hash mismatch")
        if self.classification_evidence_projection:
            projection_keys = tuple(
                (
                    item.get("security_id", ""),
                    item.get("symbol", ""),
                    item.get("evidence_id", ""),
                    item.get("exclusion_reason", ""),
                )
                for item in self.classification_evidence_projection
            )
            if projection_keys != tuple(sorted(set(projection_keys))):
                raise ValueError("classification evidence projection must be sorted and unique")
        layer_fields = (
            (
                self.classification_evidence_count,
                self.classification_evidence_sha256,
                "classification",
            ),
            (
                self.effective_main_board_count,
                self.effective_main_board_sha256,
                "effective_main_board",
            ),
            (self.required_additions_count, self.required_additions_sha256, "required_additions"),
        )
        for count, digest_value, label in layer_fields:
            if count is not None or digest_value is not None:
                if count is None or digest_value is None:
                    raise ValueError("layer count/hash must be paired")
                if label == "classification":
                    rows = list(self.classification_evidence_projection) or [
                        {
                            "security_id": item.security_id,
                            "symbol": item.symbol,
                            "evidence_id": item.instrument_evidence_id,
                            "exclusion_reason": item.exclusion_reason,
                        }
                        for item in self.members
                    ]
                else:
                    rows = [
                        member_projection(item)
                        for item in self.members
                        if (
                            (
                                label == "effective_main_board"
                                and "effective_main_board" in item.scope_roles
                            )
                            or (
                                label == "required_additions"
                                and item.symbol
                                not in {
                                    candidate.symbol
                                    for candidate in self.members
                                    if "effective_main_board" in candidate.scope_roles
                                }
                                and (
                                    "required_user" in item.scope_roles
                                    or "required_index" in item.scope_roles
                                )
                            )
                        )
                    ]
                digest_domain = {
                    "classification": (
                        "stock-eva/r2f4.2/universe-partition/classification-evidence/v1"
                    ),
                    "effective_main_board": (
                        "stock-eva/r2f4.2/universe-partition/effective-main-board/v1"
                    ),
                    "required_additions": (
                        "stock-eva/r2f4.2/universe-partition/required-additions/v1"
                    ),
                }[label]
                if count != len(rows) or digest_value != domain_sha256(digest_domain, rows):
                    raise ValueError("universe layer count/hash mismatch")
        expected = domain_sha256(
            "stock-eva/r2f4.2/universe-contract/v1", json.loads(self.payload_json)
        )
        if expected != self.contract_sha256 or self.contract_id != expected[:32]:
            raise ValueError("contract hash mismatch")
        if (
            self.source_refs.source_date_semantics == "requested_unverified"
            and self.publication_eligible
        ):
            raise ValueError("requested-unverified source cannot be publication eligible")
        if self.counts.unknown or self.counts.critical_attribute_unknown_count:
            if self.publication_eligible:
                raise ValueError("unknown contract cannot be publication eligible")
        if self.source_state_id is not None and self.source_state_sha256 is None:
            raise ValueError("source state identity incomplete")
        if self.source_state_id is not None:
            expected_id, expected_sha = _source_state_identity(self.source_refs)
            if (self.source_state_id, self.source_state_sha256) != (expected_id, expected_sha):
                raise ValueError("contract source state identity mismatch")
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
    scope: Literal["all-main-board-plus-required-symbols"]
    trade_date: date
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_id: Literal["baostock"]
    refresh_id: str
    contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_version: str = Field(min_length=1, max_length=64)
    endpoint_contract_version: str = Field(min_length=1, max_length=64)
    calendar_generation_id: str = Field(min_length=1, max_length=128)
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
    publication_eligible: bool = False


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


def derive_expected_trading_state(
    member: UniverseMemberV1, trade_date: date
) -> Literal["trading", "suspended", "not_yet_listed", "delisted", "unknown"]:
    """Derive listing-window state before honoring explicit session status."""
    if member.list_date is not None and trade_date < member.list_date:
        return "not_yet_listed"
    if member.delist_date is not None and trade_date >= member.delist_date:
        return "delisted"
    if (
        member.list_date is not None
        and member.delist_date is not None
        and member.delist_date < member.list_date
    ):
        return "unknown"
    return member.expected_trading_state


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


class UniverseSourceStateV1(_Frozen):
    """Immutable writer-verified source identity.

    ``verified_at`` is trusted ordering metadata and intentionally excluded
    from the digest so re-verifying the same source cannot manufacture a new
    source version.
    """

    source_state_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    source_state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    provider_id: Literal["baostock"]
    source_version_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_refs_json: str
    verified_at: datetime

    @field_validator("verified_at")
    @classmethod
    def normalize_verified_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("verified_at must be timezone aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_source_state(self) -> UniverseSourceStateV1:
        if (
            self.verified_at.tzinfo is None
            or self.verified_at.utcoffset() != datetime.now(UTC).utcoffset()
        ):
            raise ValueError("verified_at must be timezone aware")
        refs = SourceRefsV1.model_validate(json.loads(self.source_refs_json), strict=False)
        if refs.trade_date != self.trade_date or refs.provider_id != self.provider_id:
            raise ValueError("source state fields do not bind source refs")
        if refs.source_version_digest != self.source_version_digest:
            raise ValueError("source state source version mismatch")
        expected_id, expected_sha = _source_state_identity(refs)
        if self.source_state_id != expected_id or self.source_state_sha256 != expected_sha:
            raise ValueError("source state digest mismatch")
        return self


class UniversePublicationContextV1(_Frozen):
    context_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    run_id: str = Field(min_length=1, max_length=128)
    trade_date: date
    manifest_ref: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_refs: tuple[str, ...]
    publication_lineage_json: str
    publication_lineage_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["ready"]
    created_at: datetime
    context_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_context(self) -> UniversePublicationContextV1:
        if self.created_at.tzinfo is None:
            raise ValueError("publication context timestamp must be timezone aware")
        if self.evidence_refs != tuple(sorted(set(self.evidence_refs))):
            raise ValueError("publication evidence references must be sorted and unique")
        lineage = json.loads(self.publication_lineage_json)
        if not isinstance(lineage, dict):
            raise ValueError("publication lineage must be an object")
        expected_lineage_keys = {
            "provider_id",
            "universe_id",
            "evidence_id",
            "evidence_sha256",
            "candidate_id",
            "candidate_manifest_sha256",
            "gate_report_sha256",
            "adapter_version",
            "source_schema_version",
        }
        if set(lineage) != expected_lineage_keys:
            raise ValueError("publication lineage field set invalid")
        if canonical_json_bytes(lineage).decode() != self.publication_lineage_json:
            raise ValueError("publication lineage JSON is not canonical")
        if any(not _SHA256.fullmatch(value) for value in self.evidence_refs):
            raise ValueError("publication evidence reference is unsafe")
        expected_refs = tuple(
            sorted(
                {
                    lineage["evidence_id"],
                    lineage["evidence_sha256"],
                    lineage["candidate_id"],
                    lineage["gate_report_sha256"],
                }
            )
        )
        if (
            self.manifest_ref != lineage["candidate_manifest_sha256"]
            or self.evidence_refs != expected_refs
        ):
            raise ValueError("publication context lineage references mismatch")
        if (
            domain_sha256("stock-eva/r2f4.2/publication-lineage/v2", lineage)
            != self.publication_lineage_sha256
        ):
            raise ValueError("publication lineage hash mismatch")
        preimage = {
            "run_id": self.run_id,
            "trade_date": self.trade_date.isoformat(),
            "manifest_ref": self.manifest_ref,
            "evidence_refs": list(self.evidence_refs),
            "publication_lineage_json": canonical_json_bytes(lineage).decode(),
            "publication_lineage_sha256": self.publication_lineage_sha256,
            "status": self.status,
            "created_at": self.created_at.astimezone(UTC).isoformat(),
        }
        expected = domain_sha256("stock-eva/r2f4.2/universe-publication-context/v1", preimage)
        if self.context_sha256 != expected or self.context_id != expected[:32]:
            raise ValueError("publication context hash mismatch")
        return self


class UniverseAttemptPlanV1(_Frozen):
    attempt_id: str = Field(min_length=1, max_length=128)
    dedup_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    hook_kind: Literal["universe_post_success"]
    refresh_id: str = Field(min_length=1, max_length=128)
    canonical_run_id: str = Field(min_length=1, max_length=128)
    source_version_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    operation_day: date
    attempt_status: Literal["RUNNING"]
    request_budget: Literal[1]
    classification_max_attempts: Literal[1]
    created_at: datetime
    planned_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_plan(self) -> UniverseAttemptPlanV1:
        if self.created_at.tzinfo is None:
            raise ValueError("attempt timestamp must be timezone aware")
        key = {
            "trade_date": self.trade_date.isoformat(),
            "operation_day": self.operation_day.isoformat(),
            "canonical_run_id": self.canonical_run_id,
            "source_version_digest": self.source_version_digest,
            "hook_kind": self.hook_kind,
            "refresh_id": self.refresh_id,
            "request_budget": self.request_budget,
            "classification_max_attempts": self.classification_max_attempts,
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "attempt_status": self.attempt_status,
        }
        if self.dedup_key != domain_sha256("stock-eva/r2f4.2/universe-attempt-dedup/v1", key):
            raise ValueError("attempt dedup key mismatch")
        planned = dict(key)
        if self.planned_sha256 != domain_sha256(
            "stock-eva/r2f4.2/universe-attempt-plan/v1", planned
        ):
            raise ValueError("attempt plan hash mismatch")
        return self


class UniverseAttemptResultV1(_Frozen):
    attempt_id: str = Field(min_length=1, max_length=128)
    terminal_status: Literal["blocked", "deferred", "succeeded", "failed", "ATTEMPT_INDETERMINATE"]
    classification_request_count: Literal[0, 1]
    reason_code: str = Field(min_length=1, max_length=64)
    source_state_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    finished_at: datetime
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_result(self) -> UniverseAttemptResultV1:
        if self.finished_at.tzinfo is None:
            raise ValueError("attempt result timestamp must be timezone aware")
        if self.terminal_status in {"succeeded", "ATTEMPT_INDETERMINATE"}:
            if self.reason_code != "NONE" or self.source_state_id is None:
                raise ValueError("successful/indeterminate result identity invalid")
        elif self.reason_code == "NONE":
            raise ValueError("non-success result needs a blocking reason")
        preimage = {
            "attempt_id": self.attempt_id,
            "terminal_status": self.terminal_status,
            "classification_request_count": self.classification_request_count,
            "reason_code": self.reason_code,
            "source_state_id": self.source_state_id,
            "finished_at": self.finished_at.astimezone(UTC).isoformat(),
        }
        if self.result_sha256 != domain_sha256(
            "stock-eva/r2f4.2/universe-attempt-result/v1", preimage
        ):
            raise ValueError("attempt result hash mismatch")
        return self


class UniverseAuthorityBundleV1(_Frozen):
    """Writer-verified classification authority admitted to contract building."""

    mapping: UniverseSemanticMappingV1
    evidence: tuple[UniverseInstrumentEvidenceV1, ...]
    source_date_semantics: Literal["source_observed"]
    authority_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_authority(self) -> UniverseAuthorityBundleV1:
        if self.mapping.authority_status != "reviewed" or not self.evidence:
            raise ValueError("authority bundle is not reviewed")
        if any(
            item.authority_status != "reviewed"
            or item.source_date_semantics != "source_observed"
            or item.mapping_id != self.mapping.mapping_id
            or item.mapping_version != self.mapping.mapping_version
            or item.mapping_sha256 != self.mapping.mapping_sha256
            for item in self.evidence
        ):
            raise ValueError("authority bundle evidence is not reviewed")
        evidence_projection = [
            {
                "evidence_id": item.evidence_id,
                "evidence_sha256": item.evidence_sha256,
                "mapping_id": item.mapping_id,
                "mapping_version": item.mapping_version,
            }
            for item in sorted(self.evidence, key=lambda value: value.evidence_id)
        ]
        expected = domain_sha256(
            "stock-eva/r2f4.2/universe-authority-bundle/v1",
            {
                "mapping_sha256": self.mapping.mapping_sha256,
                "source_date_semantics": self.source_date_semantics,
                "evidence": evidence_projection,
            },
        )
        if self.authority_sha256 != expected:
            raise ValueError("authority bundle hash mismatch")
        return self


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
    classification_evidence: tuple[UniverseInstrumentEvidenceV1, ...] | None = None,
    authority_bundle: UniverseAuthorityBundleV1 | None = None,
    sequence: int = 1,
    parent_contract_id: str | None = None,
    created_at: datetime | None = None,
) -> UniverseContractV1:
    refs = _source_refs(source_refs)
    if authority_bundle is not None:
        if (
            classification_evidence is not None
            and classification_evidence != authority_bundle.evidence
        ):
            raise ValueError("classification evidence does not match authority bundle")
        classification_evidence = authority_bundle.evidence
    classification_evidence = classification_evidence or ()
    if isinstance(classification_observed_at, str):
        classification_observed_at = datetime.fromisoformat(
            classification_observed_at.replace("Z", "+00:00")
        )
    if isinstance(exact_pit_cutoff, str):
        exact_pit_cutoff = datetime.fromisoformat(exact_pit_cutoff.replace("Z", "+00:00"))
    if (
        refs.trade_date != trade_date
        or refs.calendar_generation_id != calendar_generation_id
        or refs.calendar_sha256 != calendar_sha256
        or refs.classification_generation_id != classification_generation_id
        or refs.classification_generation_sequence != classification_generation_sequence
        or refs.classification_source != classification_source
        or refs.classification_source_version != classification_source_version
        or refs.classification_source_snapshot_date != classification_source_snapshot_date
        or refs.classification_observed_at != classification_observed_at
        or refs.exact_pit_cutoff != exact_pit_cutoff
    ):
        raise ValueError("universe source refs do not bind builder arguments")
    normalized_members: list[UniverseMemberV1] = []
    for member in members:
        derived_state = derive_expected_trading_state(member, trade_date)
        if derived_state != member.expected_trading_state:
            member = member.model_copy(
                update={"expected_trading_state": derived_state, "member_sha256": None}
            )
        normalized_members.append(member)
    members = tuple(sorted(normalized_members, key=lambda item: item.symbol))
    if not members:
        raise ValueError("universe contract cannot be empty")
    if any(item.symbol != item.symbol.strip() for item in members):
        raise ValueError("universe symbol contains whitespace")
    counts = _member_counts(members)
    evidence_by_id = {item.evidence_id: item for item in classification_evidence}
    if classification_evidence and len(evidence_by_id) != len(classification_evidence):
        raise ValueError("duplicate classification evidence identity")
    natural_identity = tuple((item.security_id, item.symbol) for item in classification_evidence)
    if len(set(natural_identity)) != len(natural_identity):
        raise ValueError("UNIVERSE_IDENTITY_CONFLICT")
    classification_ids = tuple(
        sorted(evidence_by_id or {item.instrument_evidence_id: item for item in members})
    )
    if refs.instrument_evidence_ids != classification_ids:
        raise ValueError("source evidence identity does not bind classification evidence")
    base_ids = tuple(
        sorted(item.symbol for item in members if "effective_main_board" in item.scope_roles)
    )
    base_id_set = set(base_ids)
    addition_ids = tuple(
        sorted(
            item.symbol
            for item in members
            if item.symbol not in base_id_set
            and ("required_user" in item.scope_roles or "required_index" in item.scope_roles)
        )
    )
    classification_rows = (
        [
            {
                "security_id": item.security_id,
                "symbol": item.symbol,
                "evidence_id": item.evidence_id,
                "exclusion_reason": item.exclusion_reason,
            }
            for item in sorted(classification_evidence, key=lambda item: item.evidence_id)
        ]
        if classification_evidence
        else [
            {
                "security_id": item.security_id,
                "symbol": item.symbol,
                "evidence_id": item.instrument_evidence_id,
                "exclusion_reason": item.exclusion_reason,
            }
            for item in members
        ]
    )

    def member_projection(item: UniverseMemberV1) -> dict[str, Any]:
        return item.preimage()

    effective_rows = [
        member_projection(item) for item in members if "effective_main_board" in item.scope_roles
    ]
    required_rows = [
        member_projection(item)
        for item in members
        if item.symbol not in base_id_set
        and ("required_user" in item.scope_roles or "required_index" in item.scope_roles)
    ]
    layer_hashes = {
        "classification": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/classification-evidence/v1", classification_rows
        ),
        "effective_main_board": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/effective-main-board/v1", effective_rows
        ),
        "required_additions": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/required-additions/v1", required_rows
        ),
    }
    source_state_id, source_state_sha = _source_state_identity(refs)
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
        "source_state_id": source_state_id,
        "source_state_sha256": source_state_sha,
        "members": [item.model_dump(mode="json") for item in members],
        "counts": counts.model_dump(mode="json"),
        "classification_evidence_ids": classification_ids,
        "classification_evidence_projection": classification_rows,
        "effective_main_board_ids": base_ids,
        "required_additions_ids": addition_ids,
        "classification_evidence_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/classification-evidence/v1",
            classification_rows,
        ),
        "effective_main_board_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/effective-main-board/v1",
            [
                member_projection(item)
                for item in members
                if "effective_main_board" in item.scope_roles
            ],
        ),
        "required_additions_partition_sha256": domain_sha256(
            "stock-eva/r2f4.2/universe-partition/required-additions/v1",
            [
                member_projection(item)
                for item in members
                if item.symbol not in base_id_set
                and ("required_user" in item.scope_roles or "required_index" in item.scope_roles)
            ],
        ),
        "classification_evidence_count": len(classification_rows),
        "classification_evidence_sha256": layer_hashes["classification"],
        "effective_main_board_count": len(effective_rows),
        "effective_main_board_sha256": layer_hashes["effective_main_board"],
        "required_additions_count": len(required_rows),
        "required_additions_sha256": layer_hashes["required_additions"],
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
        classification_evidence_projection=tuple(classification_rows),
        effective_main_board_ids=base_ids,
        required_additions_ids=addition_ids,
        classification_evidence_partition_sha256=payload[
            "classification_evidence_partition_sha256"
        ],
        effective_main_board_partition_sha256=payload["effective_main_board_partition_sha256"],
        required_additions_partition_sha256=payload["required_additions_partition_sha256"],
        payload_json=canonical_json_bytes(payload).decode(),
        publication_eligible=(
            counts.unknown == 0
            and counts.critical_attribute_unknown_count == 0
            and refs.source_date_semantics == "source_observed"
            and (authority_bundle is not None or classification_source == "fixture")
        ),
        classification_evidence_count=len(classification_rows),
        classification_evidence_sha256=layer_hashes["classification"],
        effective_main_board_count=len(effective_rows),
        effective_main_board_sha256=layer_hashes["effective_main_board"],
        required_additions_count=len(required_rows),
        required_additions_sha256=layer_hashes["required_additions"],
        source_state_id=source_state_id,
        source_state_sha256=source_state_sha,
    )


def validate_provider_raw_batch(
    batch: ProviderRawBatch, contract: UniverseContractV1
) -> UniverseCandidateGate:
    """Validate one complete, typed raw session before normalization."""
    if not isinstance(batch, ProviderRawBatch):
        raise ValueError("provider raw batch must be the existing ProviderRawBatch")
    if not isinstance(contract, UniverseContractV1):
        raise ValueError("universe contract must be the strict v1 model")
    batch = ProviderRawBatch.model_validate(batch.model_dump(mode="python"), strict=False)
    if (
        batch.provider_id.value != "baostock"
        or batch.request.provider_id.value != "baostock"
        or batch.request.trade_date != contract.trade_date
        or batch.failure_class is not None
        or batch.adapter_version != "r2f2.v1"
        or batch.endpoint_contract_version != "r2f2-endpoints.v1"
    ):
        raise ValueError("provider/session identity mismatch")
    raw_digest = domain_sha256(
        "stock-eva/r2f4.2/raw-batch-projection/v1", batch.model_dump(mode="json")
    )
    expected_members = {
        item.symbol: item
        for item in contract.members
        if item.expected_trading_state in {"trading", "suspended"}
    }
    loaded: list[str] = []
    counts: list[tuple[str, int]] = []
    raw_row_identities: set[tuple[str, str, str]] = set()
    reason: str | None = "UNIVERSE_UNKNOWN_NONZERO" if contract.counts.unknown else None
    expected_session_symbols = {
        item.symbol
        for item in contract.members
        if item.expected_trading_state in {"trading", "suspended"}
    }
    scope_binding_invalid = (
        batch.request.universe_id != contract.universe_id
        or set(batch.request.session_symbols) != expected_session_symbols
    )
    if reason is None and not contract.publication_eligible:
        reason = (
            "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED"
            if contract.source_refs.source_date_semantics == "requested_unverified"
            else "UNIVERSE_COUNT_MISMATCH"
        )
    plan_by_ordinal = {item.plan_ordinal: item for item in batch.logical_request_plan.requests}
    completion_by_ordinal = {item.plan_ordinal: item for item in batch.request_completions}
    batches_by_ordinal: dict[int, list[Any]] = {}
    for endpoint_batch in batch.endpoint_batches:
        counts.append((endpoint_batch.endpoint.value, endpoint_batch.row_count))
        batches_by_ordinal.setdefault(endpoint_batch.plan_ordinal, []).append(endpoint_batch)
        if endpoint_batch.endpoint.value not in {"daily_astock", "index_history"}:
            for row in endpoint_batch.rows:
                if endpoint_batch.endpoint is ProviderEndpoint.ALL_STOCK and getattr(
                    row, "tradeStatus", None
                ) not in {"0", "1"}:
                    reason = reason or "UNIVERSE_STATE_MISMATCH"
                identity_value = getattr(row, "code", None) or getattr(row, "calendar_date", None)
                if hasattr(row, "dividOperateDate"):
                    identity_key = (
                        endpoint_batch.endpoint.value,
                        str(identity_value),
                        str(row.dividOperateDate),
                    )
                else:
                    identity_key = (endpoint_batch.endpoint.value, str(identity_value), "")
                if identity_key in raw_row_identities:
                    reason = reason or "UNIVERSE_SESSION_DRIFT"
                raw_row_identities.add(identity_key)
            continue
        for row in endpoint_batch.rows:
            identity_key = (
                endpoint_batch.endpoint.value,
                str(getattr(row, "code", "")),
                str(getattr(row, "date", "")),
            )
            if identity_key in raw_row_identities:
                reason = reason or "UNIVERSE_SESSION_DRIFT"
            raw_row_identities.add(identity_key)
            symbol = getattr(row, "code", None)
            row_date = getattr(row, "date", None)
            if symbol is None or row_date != contract.trade_date:
                reason = reason or "UNIVERSE_SESSION_DRIFT"
                continue
            member = expected_members.get(symbol)
            if member is None:
                reason = reason or "UNIVERSE_EXTRA_SYMBOL"
            elif endpoint_batch.endpoint.value == "index_history":
                if member.member_kind != "index" or row.tradestatus != "1":
                    reason = reason or "REQUIRED_INDEX_NOT_TRADING"
            elif member.member_kind != "stock":
                reason = reason or "UNIVERSE_STATE_MISMATCH"
            elif member.expected_trading_state == "trading" and row.tradestatus != "1":
                reason = reason or "UNIVERSE_STATE_MISMATCH"
            elif member.expected_trading_state == "suspended" and row.tradestatus != "0":
                reason = reason or "UNIVERSE_STATE_MISMATCH"
            if symbol in loaded:
                reason = reason or "UNIVERSE_DUPLICATE_SYMBOL"
            loaded.append(symbol)

    expected_ordinals = set(plan_by_ordinal)
    required_endpoints = set(ProviderEndpoint)
    observed_endpoints = {item.endpoint for item in plan_by_ordinal.values()}
    if scope_binding_invalid or observed_endpoints != required_endpoints:
        reason = reason or "UNIVERSE_SESSION_DRIFT"
    successful_ordinals = {
        ordinal
        for ordinal, completion in completion_by_ordinal.items()
        if completion.final_outcome is TransportOutcome.SUCCESS
    }
    if (
        set(completion_by_ordinal) != expected_ordinals
        or set(batches_by_ordinal) != successful_ordinals
    ):
        reason = reason or "UNIVERSE_SESSION_DRIFT"
    for ordinal, logical in plan_by_ordinal.items():
        completion = completion_by_ordinal.get(ordinal)
        pages = batches_by_ordinal.get(ordinal, [])
        if completion is None or completion.final_outcome is not TransportOutcome.SUCCESS:
            reason = reason or "UNIVERSE_SESSION_DRIFT"
            continue
        final_attempt = completion.attempts[-1]
        actual_pages = tuple(item.lineage.page for item in pages)
        if (
            actual_pages != tuple(sorted(actual_pages))
            or actual_pages != final_attempt.observed_pages
        ):
            reason = reason or "UNIVERSE_SESSION_DRIFT"
        if tuple(item.lineage.page_request_id for item in pages) != tuple(
            request_id for _, request_id in final_attempt.page_request_ids
        ):
            reason = reason or "UNIVERSE_SESSION_DRIFT"
        if logical.endpoint in {ProviderEndpoint.DAILY_FACTOR, ProviderEndpoint.ADJUST_FACTOR}:
            factor_codes = {row.code for item in pages for row in item.rows if hasattr(row, "code")}
            if factor_codes != set(logical.symbols):
                reason = reason or "UNIVERSE_SESSION_DRIFT"
        elif logical.endpoint is ProviderEndpoint.TRADE_DATES and not any(
            row.calendar_date == contract.trade_date and row.is_trading_day == "1"
            for item in pages
            for row in item.rows
        ):
            reason = reason or "UNIVERSE_SESSION_DRIFT"
    all_stock_codes = {
        row.code
        for ordinal, pages in batches_by_ordinal.items()
        if plan_by_ordinal.get(ordinal) is not None
        and plan_by_ordinal[ordinal].endpoint is ProviderEndpoint.ALL_STOCK
        for item in pages
        for row in item.rows
    }
    if any(plan.endpoint is ProviderEndpoint.ALL_STOCK for plan in plan_by_ordinal.values()):
        if all_stock_codes != {
            item.symbol for item in contract.members if item.member_kind == "stock"
        }:
            reason = reason or "UNIVERSE_SESSION_DRIFT"
    observed = set(loaded)
    missing = len(set(expected_members) - observed)
    extra = len(observed - set(expected_members))
    duplicate = len(loaded) - len(observed)
    missing_indexes = {
        item.symbol
        for item in contract.members
        if item.member_kind == "index" and item.symbol not in observed
    }
    if reason is None and missing:
        reason = "REQUIRED_INDEX_NOT_TRADING" if missing_indexes else "UNIVERSE_MISSING_SYMBOL"
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
        scope=contract.scope,
        trade_date=contract.trade_date,
        calendar_sha256=contract.calendar_sha256,
        provider_id="baostock",
        refresh_id=batch.request.refresh_id,
        contract_sha256=contract.contract_sha256,
        adapter_version=batch.adapter_version,
        endpoint_contract_version=batch.endpoint_contract_version,
        calendar_generation_id=contract.calendar_generation_id,
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
        publication_eligible=result == "pass" and contract.publication_eligible,
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
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    source_date_semantics TEXT NOT NULL CHECK
    (source_date_semantics IN ('source_observed','requested_unverified')),
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
    source_date_semantics TEXT NOT NULL CHECK
    (source_date_semantics IN ('source_observed','requested_unverified')),
    evidence_json TEXT NOT NULL, adapter_version TEXT NOT NULL,
    source_schema TEXT NOT NULL, security_type TEXT NOT NULL CHECK
    (security_type IN ('stock','index','fund','etf','bond','other')),
    exchange TEXT, board TEXT, list_date TEXT, delist_date TEXT,
    listing_status TEXT NOT NULL, daily_trade_status TEXT,
    suspension_state TEXT NOT NULL CHECK
    (suspension_state IN ('trading','suspended','not_supplied','unknown')),
    st_state TEXT NOT NULL CHECK (st_state IN ('yes','no','not_applicable','unknown')),
    expected_trading_state TEXT CHECK
    (expected_trading_state IS NULL OR expected_trading_state IN
    ('trading','suspended','not_yet_listed','delisted','unknown')),
    observed_at TEXT, lineage_hash TEXT
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
    terminal_status TEXT NOT NULL CHECK
    (terminal_status IN ('blocked','deferred','succeeded','failed','ATTEMPT_INDETERMINATE')),
    classification_request_count INTEGER NOT NULL CHECK (classification_request_count IN (0,1)),
    reason_code TEXT NOT NULL CHECK (reason_code IN
    ('CONTROL_STATE_UNAVAILABLE','PIT_VISIBILITY_INVALID','CALENDAR_UNAVAILABLE',
     'CALENDAR_CONFLICT','CLASSIFICATION_UNAVAILABLE','USER_STORE_UNAVAILABLE',
     'BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED','PIT_CUTOFF_VIOLATION','USER_SNAPSHOT_CHANGED',
     'REQUIRED_INDEX_NOT_TRADING','REQUIRED_SYMBOL_INVALID','UNIVERSE_UNKNOWN_NONZERO',
     'UNIVERSE_COUNT_MISMATCH','UNIVERSE_MISSING_SYMBOL','UNIVERSE_EXTRA_SYMBOL',
     'UNIVERSE_DUPLICATE_SYMBOL','UNIVERSE_SESSION_DRIFT','UNIVERSE_STATE_MISMATCH',
     'UNIVERSE_SOURCE_VERSION_CHANGED','DATE_MISMATCH','UNIVERSE_STORAGE_UNAVAILABLE',
     'UNIVERSE_SCHEMA_MISMATCH','UNIVERSE_HEAD_CAS_CONFLICT','BLOCKED_ENFORCE_NOT_ENABLED',
     'BLOCKED_PRODUCTION_MODE_OFF','LEGACY_SHADOW_DRIFT','ATTEMPT_INDETERMINATE',
     'UNIVERSE_IDENTITY_CONFLICT','NONE')),
    source_state_id TEXT REFERENCES universe_source_state(source_state_id),
    finished_at TEXT NOT NULL, result_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE universe_head (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1), sequence INTEGER NOT NULL,
    contract_id TEXT NOT NULL, contract_sha256 TEXT NOT NULL, head_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL, FOREIGN KEY (contract_id) REFERENCES universe_contract(contract_id)
);
CREATE TRIGGER universe_head_no_delete BEFORE DELETE ON universe_head
BEGIN SELECT RAISE(ABORT,'immutable_head'); END;
CREATE TRIGGER universe_head_update_guard BEFORE UPDATE ON universe_head
WHEN NEW.singleton_id != OLD.singleton_id OR NEW.sequence != OLD.sequence + 1
     OR NEW.contract_id = OLD.contract_id
BEGIN SELECT RAISE(ABORT,'invalid_head_transition'); END;
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


def _normalize_sql(sql: str) -> str:
    """Normalize SQLite DDL without changing quoted literals."""
    result: list[str] = []
    quote: str | None = None
    pending_space = False
    for char in sql.strip():
        if quote:
            result.append(char)
            if char == quote:
                quote = None
            continue
        if char in "'\"`":
            if pending_space and result and result[-1] not in "(,":
                result.append(" ")
            pending_space = False
            result.append(char)
            quote = char
        elif char.isspace():
            pending_space = True
        elif char in "(),":
            pending_space = False
            while result and result[-1] == " ":
                result.pop()
            result.append(char)
        else:
            if pending_space and result and result[-1] not in "(,":
                result.append(" ")
            pending_space = False
            result.append(char)
    return "".join(result).strip()


def _ddl_statements(ddl: str) -> tuple[str, ...]:
    # SQLite trigger bodies contain semicolons.  Match complete trigger
    # bodies first and split the remaining CREATE/PRAGMA statements normally.
    statements: list[str] = []
    trigger_pattern = re.compile(r"CREATE\s+TRIGGER\b.*?\bEND\s*;", re.I | re.S)
    cursor = 0
    for match in trigger_pattern.finditer(ddl):
        before = ddl[cursor : match.start()]
        if before.strip():
            statements.extend(_normalize_sql(part) for part in before.split(";") if part.strip())
        statements.append(_normalize_sql(match.group(0)[:-1]))
        cursor = match.end()
    remainder = ddl[cursor:]
    if remainder.strip():
        statements.extend(_normalize_sql(part) for part in remainder.split(";") if part.strip())
    return tuple(item for item in statements if item)


_NORMALIZED_UNIVERSE_DDL = "\n".join(_ddl_statements(UNIVERSE_DDL))
UNIVERSE_SCHEMA_DIGEST = domain_sha256(
    "stock-eva/r2f4.2/universe-schema/v1", _NORMALIZED_UNIVERSE_DDL
)
_EXPECTED_SCHEMA_SQL: dict[tuple[str, str], str] = {}
for _statement in _ddl_statements(UNIVERSE_DDL):
    _parts = _statement.split(" ", 3)
    if len(_parts) >= 3 and _parts[0].upper() == "CREATE":
        _kind = _parts[1].upper()
        _name = _parts[2].split("(", 1)[0]
        _EXPECTED_SCHEMA_SQL[(_kind, _name)] = _statement
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
_EXPECTED_COLUMNS = {
    "universe_meta": {"meta_key", "meta_value"},
    "universe_source_state": {
        "source_state_id",
        "source_state_sha256",
        "trade_date",
        "provider_id",
        "source_version_digest",
        "source_refs_json",
        "verified_at",
    },
    "universe_contract": {
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
    },
    "universe_member": {"contract_id", "symbol", "security_id", "member_sha256", "member_json"},
    "universe_semantic_mapping": {
        "mapping_id",
        "mapping_version",
        "provider_id",
        "source_schema",
        "payload_json",
        "mapping_sha256",
        "authority_status",
    },
    "universe_instrument_evidence": {
        "evidence_id",
        "evidence_sha256",
        "provider_id",
        "authority_status",
        "artifact_origin",
        "security_id",
        "symbol",
        "mapping_id",
        "mapping_version",
        "mapping_sha256",
        "source_snapshot_date",
        "evidence_trade_date",
        "exclusion_reason",
        "index_role",
        "source_date_semantics",
        "evidence_json",
        "adapter_version",
        "source_schema",
        "security_type",
        "exchange",
        "board",
        "list_date",
        "delist_date",
        "listing_status",
        "daily_trade_status",
        "suspension_state",
        "st_state",
        "expected_trading_state",
        "observed_at",
        "lineage_hash",
    },
    "universe_required_symbol_snapshot": {
        "snapshot_id",
        "snapshot_sha256",
        "snapshot_token_digest",
        "schema_version",
        "symbol_count",
        "snapshot_json",
    },
    "universe_publication_context": {
        "context_id",
        "run_id",
        "trade_date",
        "manifest_ref",
        "evidence_refs_json",
        "publication_lineage_json",
        "publication_lineage_sha256",
        "context_sha256",
        "status",
        "created_at",
    },
    "contract_evidence": {
        "contract_id",
        "evidence_id",
        "evidence_role",
        "security_id",
        "symbol",
        "exclusion_reason",
    },
    "contract_required_snapshot": {"contract_id", "snapshot_id", "snapshot_sha256"},
    "universe_attempt": {
        "attempt_id",
        "dedup_key",
        "hook_kind",
        "refresh_id",
        "canonical_run_id",
        "source_version_digest",
        "trade_date",
        "operation_day",
        "attempt_status",
        "request_budget",
        "classification_max_attempts",
        "created_at",
        "planned_sha256",
    },
    "universe_attempt_result": {
        "attempt_id",
        "terminal_status",
        "classification_request_count",
        "reason_code",
        "source_state_id",
        "finished_at",
        "result_sha256",
    },
    "universe_head": {
        "singleton_id",
        "sequence",
        "contract_id",
        "contract_sha256",
        "head_sha256",
        "updated_at",
    },
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
        try:
            if self.path.is_symlink():
                raise UniverseStoreUnavailable("universe sidecar symlink is not allowed")
        except OSError as exc:
            raise UniverseStoreUnavailable("universe sidecar path unavailable") from exc
        if readonly:
            uri = f"file:{self.path.absolute()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=0.25)
        else:
            connection = sqlite3.connect(self.path, timeout=0.25)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=250")
        return connection

    def initialize(self) -> None:
        if self.path.exists() and self.path.is_symlink():
            raise UniverseStoreUnavailable("universe sidecar symlink is not allowed")
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
            actual_schema = {
                (str(row[0]).upper(), str(row[1])): _normalize_sql(row[2])
                for row in connection.execute(
                    "SELECT type,name,sql FROM sqlite_master "
                    "WHERE sql IS NOT NULL AND type IN ('table','trigger','index','view') "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
                )
            }
            expected_schema = {
                key: value for key, value in _EXPECTED_SCHEMA_SQL.items() if key[0] != "INDEX"
            }
            if actual_schema != expected_schema:
                raise UniverseStoreUnavailable("universe schema DDL mismatch")
            for table, expected_columns in _EXPECTED_COLUMNS.items():
                columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
                if columns != expected_columns:
                    raise UniverseStoreUnavailable(f"universe columns invalid: {table}")
            if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise UniverseStoreUnavailable("universe schema version invalid")
            trigger_rows = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
                )
            }
            expected_triggers = {
                "universe_head_no_delete",
                "universe_head_update_guard",
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
            if set(trigger_rows) != expected_triggers or any(
                "RAISE(ABORT" not in sql for sql in trigger_rows.values()
            ):
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
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
            if str(journal_mode).lower() != "wal" or synchronous != 2:
                raise UniverseStoreUnavailable("universe durability mode invalid")
        except (OSError, sqlite3.Error, KeyError) as exc:
            raise UniverseStoreUnavailable("universe sidecar unreadable") from exc

    def _validate_global(
        self, connection: sqlite3.Connection, reachable_contract_ids: set[str]
    ) -> None:
        """Verify append-only global sets and reject orphaned authority rows."""
        try:
            contract_ids = {
                row[0] for row in connection.execute("SELECT contract_id FROM universe_contract")
            }
            if contract_ids != reachable_contract_ids:
                raise UniverseStoreUnavailable("universe orphan contract")
            links = connection.execute(
                "SELECT contract_id,evidence_id,evidence_role,security_id,symbol,exclusion_reason "
                "FROM contract_evidence"
            ).fetchall()
            link_evidence = {row[1] for row in links}
            all_evidence = {
                row[0]
                for row in connection.execute(
                    "SELECT evidence_id FROM universe_instrument_evidence"
                )
            }
            if (
                any(row[0] not in reachable_contract_ids for row in links)
                or link_evidence != all_evidence
            ):
                raise UniverseStoreUnavailable("universe orphan evidence link")
            # Every evidence row is identity-checked and every role link must
            # carry exactly the values from that row.
            evidence_rows = {
                row[0]: row
                for row in connection.execute("SELECT * FROM universe_instrument_evidence")
            }
            for row in evidence_rows.values():
                UniverseInstrumentEvidenceV1.model_validate(dict(row), strict=False)
            for contract_id in reachable_contract_ids:
                contract_evidence_ids = {row[1] for row in links if row[0] == contract_id}
                contract_identities = [
                    (evidence_rows[evidence_id][5], evidence_rows[evidence_id][6])
                    for evidence_id in contract_evidence_ids
                ]
                if len(set(contract_identities)) != len(contract_identities):
                    raise UniverseStoreUnavailable("universe evidence identity conflict")
            for link in links:
                ev = evidence_rows[link[1]]
                if tuple(link[3:]) != (ev[5], ev[6], ev[12]):
                    raise UniverseStoreUnavailable("universe evidence role identity mismatch")
            snapshot_rows = connection.execute(
                "SELECT snapshot_id,snapshot_sha256,snapshot_token_digest,schema_version,"
                "symbol_count,snapshot_json FROM universe_required_symbol_snapshot"
            ).fetchall()
            snapshot_links = connection.execute(
                "SELECT contract_id,snapshot_id,snapshot_sha256 FROM contract_required_snapshot"
            ).fetchall()
            if any(row[0] not in reachable_contract_ids for row in snapshot_links):
                raise UniverseStoreUnavailable("universe orphan snapshot link")
            if {row[0] for row in snapshot_rows} != {row[1] for row in snapshot_links}:
                raise UniverseStoreUnavailable("universe orphan snapshot")
            for row in snapshot_rows:
                payload = json.loads(row[5])
                if not isinstance(payload, dict) or payload.get("schema_version") != row[3]:
                    raise UniverseStoreUnavailable("universe snapshot payload invalid")
                symbols = payload.get("symbols")
                if not isinstance(symbols, list) or row[4] != len(symbols):
                    raise UniverseStoreUnavailable("universe snapshot count mismatch")
                digest = domain_sha256(
                    "stock-eva/r2f4.2/required-symbol-snapshot/v1",
                    {"schema_version": row[3], "snapshot_token": row[2], "symbols": symbols},
                )
                if digest != row[1]:
                    raise UniverseStoreUnavailable("universe snapshot hash invalid")
            # Mapping rows are a global immutable registry.  All mappings are
            # valid registry entries; evidence links above bind the ones used
            # by the current head.
            for row in connection.execute("SELECT * FROM universe_semantic_mapping"):
                UniverseSemanticMappingV1.model_validate(dict(row), strict=False)
            for row in connection.execute("SELECT * FROM universe_source_state"):
                UniverseSourceStateV1(
                    source_state_id=row[0],
                    source_state_sha256=row[1],
                    trade_date=date.fromisoformat(row[2]),
                    provider_id=row[3],
                    source_version_digest=row[4],
                    source_refs_json=row[5],
                    verified_at=datetime.fromisoformat(row[6].replace("Z", "+00:00")),
                )
            self._validate_attempts(connection)
            self._validate_contexts(connection)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError, sqlite3.Error) as exc:
            if isinstance(exc, UniverseStoreUnavailable):
                raise
            raise UniverseStoreUnavailable("universe global state unavailable") from exc

    @staticmethod
    def _validate_attempts(connection: sqlite3.Connection) -> None:
        rows = connection.execute("SELECT * FROM universe_attempt").fetchall()
        results = {
            row[0]: row for row in connection.execute("SELECT * FROM universe_attempt_result")
        }
        for row in rows:
            plan = UniverseAttemptPlanV1(
                attempt_id=row[0],
                dedup_key=row[1],
                hook_kind=row[2],
                refresh_id=row[3],
                canonical_run_id=row[4],
                source_version_digest=row[5],
                trade_date=date.fromisoformat(row[6]),
                operation_day=date.fromisoformat(row[7]),
                attempt_status=row[8],
                request_budget=row[9],
                classification_max_attempts=row[10],
                created_at=datetime.fromisoformat(row[11].replace("Z", "+00:00")),
                planned_sha256=row[12],
            )
            result = results.get(plan.attempt_id)
            if result is not None:
                result_model = UniverseAttemptResultV1(
                    attempt_id=result[0],
                    terminal_status=result[1],
                    classification_request_count=result[2],
                    reason_code=result[3],
                    source_state_id=result[4],
                    finished_at=datetime.fromisoformat(result[5].replace("Z", "+00:00")),
                    result_sha256=result[6],
                )
                if result_model.source_state_id is not None:
                    source_state = connection.execute(
                        "SELECT trade_date,source_version_digest FROM universe_source_state "
                        "WHERE source_state_id=?",
                        (result_model.source_state_id,),
                    ).fetchone()
                    if source_state is None or (
                        source_state[0] != plan.trade_date.isoformat()
                        or source_state[1] != plan.source_version_digest
                    ):
                        raise UniverseStoreUnavailable("universe attempt source lineage mismatch")
        if set(results) - {row[0] for row in rows}:
            raise UniverseStoreUnavailable("universe orphan attempt result")

    @staticmethod
    def _validate_contexts(connection: sqlite3.Connection) -> None:
        for row in connection.execute("SELECT * FROM universe_publication_context"):
            UniversePublicationContextV1(
                context_id=row[0],
                run_id=row[1],
                trade_date=date.fromisoformat(row[2]),
                manifest_ref=row[3],
                evidence_refs=tuple(json.loads(row[4])),
                publication_lineage_json=row[5],
                publication_lineage_sha256=row[6],
                context_sha256=row[8],
                status=row[9],
                created_at=datetime.fromisoformat(row[10].replace("Z", "+00:00")),
            )

    def _read_contract(
        self, connection: sqlite3.Connection, contract_id: str
    ) -> UniverseContractV1:
        row = connection.execute(
            "SELECT * FROM universe_contract WHERE contract_id=?",
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
                    and payload["source_refs"]["source_date_semantics"] == "source_observed"
                ),
            )
            contract = UniverseContractV1.model_validate(values, strict=False)
            if (
                row["trade_date"] != contract.trade_date.isoformat()
                or row["universe_id"] != contract.universe_id
                or row["schema_version"] != contract.schema_version
                or row["scope"] != contract.scope
                or row["calendar_generation_id"] != contract.calendar_generation_id
                or row["calendar_sha256"] != contract.calendar_sha256
                or row["classification_generation_id"] != contract.classification_generation_id
                or row["classification_generation_sequence"]
                != contract.source_refs.classification_generation_sequence
                or row["classification_source"] != contract.source_refs.classification_source
                or row["classification_source_version"]
                != contract.source_refs.classification_source_version
                or row["classification_source_snapshot_date"]
                != contract.source_refs.classification_source_snapshot_date.isoformat()
                or row["classification_observed_at"]
                != contract.source_refs.classification_observed_at.isoformat()
                or row["classification_snapshot_sha256"]
                != contract.source_refs.classification_snapshot_sha256
                or row["exact_pit_cutoff"] != contract.exact_pit_cutoff.isoformat()
                or row["provider_id"] != "baostock"
                or row["source_date_semantics"] != contract.source_refs.source_date_semantics
                or row["source_state_id"] != contract.source_state_id
                or row["source_state_sha256"] != contract.source_state_sha256
                or row["source_version_digest"] != contract.source_refs.source_version_digest
                or row["instrument_evidence_ids_json"]
                != canonical_json_bytes(contract.source_refs.instrument_evidence_ids).decode()
                or row["counts_json"]
                != canonical_json_bytes(contract.counts.model_dump(mode="json")).decode()
                or row["layer_counts_json"]
                != canonical_json_bytes(
                    {
                        "classification_evidence": contract.classification_evidence_count
                        if contract.classification_evidence_count is not None
                        else len(contract.classification_evidence_ids),
                        "effective_main_board": contract.effective_main_board_count
                        if contract.effective_main_board_count is not None
                        else len(contract.effective_main_board_ids),
                        "required_additions": contract.required_additions_count
                        if contract.required_additions_count is not None
                        else len(contract.required_additions_ids),
                    }
                ).decode()
                or row["source_refs_json"]
                != canonical_json_bytes(contract.source_refs.model_dump(mode="json")).decode()
                or row["classification_evidence_ids_json"]
                != canonical_json_bytes(contract.classification_evidence_ids).decode()
                or row["effective_main_board_ids_json"]
                != canonical_json_bytes(contract.effective_main_board_ids).decode()
                or row["required_additions_ids_json"]
                != canonical_json_bytes(contract.required_additions_ids).decode()
                or row["required_symbol_snapshot_id"]
                != contract.source_refs.required_symbol_snapshot_id
                or row["required_symbol_snapshot_sha256"]
                != contract.source_refs.required_symbol_snapshot_sha256
                or row["classification_evidence_partition_sha256"]
                != contract.classification_evidence_partition_sha256
                or row["effective_main_board_partition_sha256"]
                != contract.effective_main_board_partition_sha256
                or row["required_additions_partition_sha256"]
                != contract.required_additions_partition_sha256
            ):
                raise UniverseStoreUnavailable("universe contract column mismatch")
            members = connection.execute(
                "SELECT symbol,security_id,member_sha256,member_json FROM universe_member "
                "WHERE contract_id=? ORDER BY symbol",
                (contract_id,),
            ).fetchall()
            if tuple(item["symbol"] for item in members) != tuple(
                item.symbol for item in contract.members
            ):
                raise UniverseStoreUnavailable("universe member rows mismatch")
            for item in members:
                member = UniverseMemberV1.model_validate_json(item["member_json"])
                if (
                    member.symbol != item["symbol"]
                    or member.security_id != item["security_id"]
                    or member.member_sha256 != item["member_sha256"]
                ):
                    raise UniverseStoreUnavailable("universe member hash invalid")
            refs = contract.source_refs
            source_state_id, source_state_sha = _source_state_identity(refs)
            expected_columns = {
                "trade_date": contract.trade_date.isoformat(),
                "universe_id": contract.universe_id,
                "schema_version": contract.schema_version,
                "scope": contract.scope,
                "calendar_generation_id": contract.calendar_generation_id,
                "calendar_sha256": contract.calendar_sha256,
                "classification_generation_id": contract.classification_generation_id,
                "classification_generation_sequence": refs.classification_generation_sequence,
                "classification_source": refs.classification_source,
                "classification_source_version": refs.classification_source_version,
                "classification_source_snapshot_date": (
                    refs.classification_source_snapshot_date.isoformat()
                ),
                "classification_observed_at": refs.classification_observed_at.isoformat(),
                "classification_snapshot_sha256": refs.classification_snapshot_sha256,
                "exact_pit_cutoff": contract.exact_pit_cutoff.isoformat(),
                "provider_id": "baostock",
                "source_date_semantics": refs.source_date_semantics,
                "source_state_id": source_state_id,
                "source_state_sha256": source_state_sha,
                "source_version_digest": refs.source_version_digest,
                "required_symbol_snapshot_id": refs.required_symbol_snapshot_id,
                "required_symbol_snapshot_sha256": refs.required_symbol_snapshot_sha256,
                "instrument_evidence_ids_json": canonical_json_bytes(
                    refs.instrument_evidence_ids
                ).decode(),
                "counts_json": canonical_json_bytes(
                    contract.counts.model_dump(mode="json")
                ).decode(),
                "layer_counts_json": canonical_json_bytes(
                    {
                        "classification_evidence": len(contract.classification_evidence_ids),
                        "effective_main_board": len(contract.effective_main_board_ids),
                        "required_additions": len(contract.required_additions_ids),
                    }
                ).decode(),
                "source_refs_json": canonical_json_bytes(refs.model_dump(mode="json")).decode(),
                "classification_evidence_ids_json": canonical_json_bytes(
                    contract.classification_evidence_ids
                ).decode(),
                "classification_evidence_partition_sha256": (
                    contract.classification_evidence_partition_sha256
                ),
                "effective_main_board_ids_json": canonical_json_bytes(
                    contract.effective_main_board_ids
                ).decode(),
                "effective_main_board_partition_sha256": (
                    contract.effective_main_board_partition_sha256
                ),
                "required_additions_ids_json": canonical_json_bytes(
                    contract.required_additions_ids
                ).decode(),
                "required_additions_partition_sha256": contract.required_additions_partition_sha256,
            }
            if any(row[key] != value for key, value in expected_columns.items()):
                raise UniverseStoreUnavailable("universe contract explicit column mismatch")
            source_state = connection.execute(
                "SELECT source_state_id,source_state_sha256,trade_date,provider_id, "
                "source_version_digest,source_refs_json,verified_at FROM universe_source_state "
                "WHERE source_state_id=?",
                (source_state_id,),
            ).fetchone()
            if source_state is None or tuple(source_state[:2]) != (
                source_state_id,
                source_state_sha,
            ):
                raise UniverseStoreUnavailable("universe source state missing")
            if (
                source_state["trade_date"] != contract.trade_date.isoformat()
                or source_state["provider_id"] != "baostock"
                or source_state["source_version_digest"] != refs.source_version_digest
                or source_state["source_refs_json"]
                != canonical_json_bytes(refs.model_dump(mode="json")).decode()
            ):
                raise UniverseStoreUnavailable("universe source state mismatch")
            UniverseSourceStateV1(
                source_state_id=source_state["source_state_id"],
                source_state_sha256=source_state["source_state_sha256"],
                trade_date=date.fromisoformat(source_state["trade_date"]),
                provider_id=source_state["provider_id"],
                source_version_digest=source_state["source_version_digest"],
                source_refs_json=source_state["source_refs_json"],
                verified_at=datetime.fromisoformat(
                    source_state["verified_at"].replace("Z", "+00:00")
                ),
            )
            mapping = connection.execute(
                "SELECT mapping_id,mapping_version,provider_id,source_schema,payload_json, "
                "mapping_sha256,authority_status FROM universe_semantic_mapping "
                "WHERE mapping_sha256=?",
                (refs.semantic_mapping_sha256,),
            ).fetchone()
            if mapping is None:
                raise UniverseStoreUnavailable("universe semantic mapping missing")
            UniverseSemanticMappingV1.model_validate(dict(mapping), strict=False)
            evidence = connection.execute(
                "SELECT * FROM universe_instrument_evidence WHERE evidence_id IN "
                f"({','.join('?' for _ in contract.classification_evidence_ids)})",
                contract.classification_evidence_ids,
            ).fetchall()
            if {item["evidence_id"] for item in evidence} != set(
                contract.classification_evidence_ids
            ):
                raise UniverseStoreUnavailable("universe evidence set incomplete")
            evidence_models = {
                item["evidence_id"]: UniverseInstrumentEvidenceV1.model_validate(
                    dict(item), strict=False
                )
                for item in evidence
            }
            expected_projection = tuple(
                {
                    "security_id": item.security_id,
                    "symbol": item.symbol,
                    "evidence_id": item.evidence_id,
                    "exclusion_reason": item.exclusion_reason,
                }
                for item in sorted(evidence_models.values(), key=lambda value: value.evidence_id)
            )
            if tuple(contract.classification_evidence_projection) != expected_projection:
                raise UniverseStoreUnavailable("universe classification projection mismatch")
            if any(
                item.mapping_sha256 != refs.semantic_mapping_sha256
                or item.mapping_id != mapping["mapping_id"]
                or item.mapping_version != mapping["mapping_version"]
                or item.source_snapshot_date > contract.trade_date
                or item.evidence_trade_date != contract.trade_date
                for item in evidence_models.values()
            ):
                raise UniverseStoreUnavailable("universe evidence PIT binding invalid")
            mapping = UniverseSemanticMappingV1.model_validate(dict(mapping), strict=False)
            if mapping.authority_status != "reviewed":
                raise UniverseStoreUnavailable("universe semantic mapping is unqualified")
            members_by_evidence = {item.instrument_evidence_id: item for item in contract.members}
            for evidence_id, item in evidence_models.items():
                member = members_by_evidence.get(evidence_id)
                if member is not None:
                    if (
                        item.symbol != member.symbol
                        or item.security_id != member.security_id
                        or item.exclusion_reason != member.exclusion_reason
                    ):
                        raise UniverseStoreUnavailable("universe evidence member binding invalid")
                elif not item.exclusion_reason:
                    raise UniverseStoreUnavailable("excluded evidence lacks exclusion reason")
            snapshot = connection.execute(
                "SELECT snapshot_id,snapshot_sha256,snapshot_token_digest,schema_version, "
                "symbol_count,snapshot_json "
                "FROM universe_required_symbol_snapshot WHERE snapshot_id=?",
                (refs.required_symbol_snapshot_id,),
            ).fetchone()
            if (
                snapshot is None
                or snapshot["snapshot_sha256"] != refs.required_symbol_snapshot_sha256
            ):
                raise UniverseStoreUnavailable("universe required snapshot missing")
            snapshot_payload = json.loads(snapshot["snapshot_json"])
            snapshot_digest = domain_sha256(
                "stock-eva/r2f4.2/required-symbol-snapshot/v1",
                {
                    "schema_version": snapshot["schema_version"],
                    "snapshot_token": snapshot["snapshot_token_digest"],
                    "symbols": snapshot_payload["symbols"],
                },
            )
            if (
                snapshot["schema_version"] != 1
                or snapshot_digest != snapshot["snapshot_sha256"]
                or snapshot["symbol_count"] != len(snapshot_payload["symbols"])
                or tuple(
                    (item["symbol"], tuple(item["roles"])) for item in snapshot_payload["symbols"]
                )
                != tuple(
                    (item.symbol, item.scope_roles)
                    for item in contract.members
                    if "required_user" in item.scope_roles or "required_index" in item.scope_roles
                )
            ):
                raise UniverseStoreUnavailable("universe required snapshot mismatch")
            link = connection.execute(
                "SELECT snapshot_sha256 FROM contract_required_snapshot WHERE contract_id=? "
                "AND snapshot_id=?",
                (contract.contract_id, refs.required_symbol_snapshot_id),
            ).fetchone()
            if link is None or link["snapshot_sha256"] != refs.required_symbol_snapshot_sha256:
                raise UniverseStoreUnavailable("universe snapshot link missing")
            links = connection.execute(
                "SELECT evidence_id,evidence_role,security_id,symbol,exclusion_reason "
                "FROM contract_evidence WHERE contract_id=?",
                (contract.contract_id,),
            ).fetchall()
            expected_links: set[tuple[str, str, str, str, str]] = set()
            for evidence_id, item in evidence_models.items():
                member = members_by_evidence.get(evidence_id)
                if member is None:
                    expected_links.add(
                        (
                            evidence_id,
                            "classification_evidence",
                            item.security_id,
                            item.symbol,
                            item.exclusion_reason,
                        )
                    )
                    continue
                expected_links.add(
                    (
                        evidence_id,
                        "classification_evidence",
                        member.security_id,
                        member.symbol,
                        member.exclusion_reason,
                    )
                )
                expected_links.add(
                    (
                        evidence_id,
                        "member",
                        member.security_id,
                        member.symbol,
                        member.exclusion_reason,
                    )
                )
                if "effective_main_board" in member.scope_roles:
                    expected_links.add(
                        (
                            evidence_id,
                            "effective_main_board",
                            member.security_id,
                            member.symbol,
                            member.exclusion_reason,
                        )
                    )
                if "required_user" in member.scope_roles or "required_index" in member.scope_roles:
                    expected_links.add(
                        (
                            evidence_id,
                            "required_additions",
                            member.security_id,
                            member.symbol,
                            member.exclusion_reason,
                        )
                    )
            if {
                (
                    item["evidence_id"],
                    item["evidence_role"],
                    item["security_id"],
                    item["symbol"],
                    item["exclusion_reason"],
                )
                for item in links
            } != expected_links:
                raise UniverseStoreUnavailable("universe evidence link missing")
            return contract
        except (TypeError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
            raise UniverseStoreUnavailable("universe contract unavailable") from exc

    def read_head(self) -> UniverseHeadV1 | None:
        if not self.path.exists():
            raise UniverseStoreUnavailable("universe sidecar missing")
        connection = self._connect(readonly=True)
        try:
            connection.execute("BEGIN DEFERRED")
            self._validate(connection)
            row = connection.execute(
                "SELECT sequence,contract_id,contract_sha256,head_sha256,updated_at "
                "FROM universe_head WHERE singleton_id=1"
            ).fetchone()
            if row is None:
                self._validate_global(connection, set())
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
            self._validate_global(connection, visited)
            values = dict(row)
            values["updated_at"] = datetime.fromisoformat(
                values["updated_at"].replace("Z", "+00:00")
            )
            return UniverseHeadV1.model_validate(values)
        except sqlite3.Error as exc:
            raise UniverseStoreUnavailable("universe sidecar unreadable") from exc
        finally:
            connection.close()

    def read_verified_snapshot(self) -> tuple[UniverseHeadV1, UniverseContractV1]:
        """Read one strict sidecar snapshot; an empty store is unavailable."""
        if not self.path.exists() or self.path.is_symlink():
            raise UniverseStoreUnavailable("universe sidecar unavailable")
        connection = self._connect(readonly=True)
        try:
            connection.execute("BEGIN DEFERRED")
            self._validate(connection)
            row = connection.execute(
                "SELECT sequence,contract_id,contract_sha256,head_sha256,updated_at "
                "FROM universe_head WHERE singleton_id=1"
            ).fetchone()
            if row is None:
                raise UniverseStoreUnavailable("universe sidecar empty")
            contract = self._read_contract(connection, row["contract_id"])
            if (
                row["contract_sha256"] != contract.contract_sha256
                or row["sequence"] != contract.sequence
                or row["head_sha256"]
                != _head_digest(row["sequence"], row["contract_id"], row["contract_sha256"])
            ):
                raise UniverseStoreUnavailable("universe snapshot head mismatch")
            current = contract
            visited = {current.contract_id}
            while current.sequence > 1:
                if current.parent_contract_id is None or current.parent_contract_id in visited:
                    raise UniverseStoreUnavailable("universe parent chain invalid")
                current = self._read_contract(connection, current.parent_contract_id)
                visited.add(current.contract_id)
            self._validate_global(connection, visited)
            head = UniverseHeadV1(
                sequence=row["sequence"],
                contract_id=row["contract_id"],
                contract_sha256=row["contract_sha256"],
                head_sha256=row["head_sha256"],
                updated_at=datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")),
            )
            return head, contract
        except (sqlite3.Error, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise UniverseStoreUnavailable("universe snapshot unavailable") from exc
        finally:
            connection.close()

    def read_latest_source_state(self, trade_date: date) -> UniverseSourceStateV1 | None:
        """Return the newest writer-verified source row for one exact date."""
        if not self.path.exists() or self.path.is_symlink():
            raise UniverseStoreUnavailable("universe sidecar unavailable")
        connection = self._connect(readonly=True)
        try:
            connection.execute("BEGIN DEFERRED")
            self._validate(connection)
            rows = connection.execute(
                "SELECT * FROM universe_source_state WHERE trade_date=? "
                "ORDER BY verified_at DESC, source_state_id DESC",
                (trade_date.isoformat(),),
            ).fetchall()
            head = connection.execute(
                "SELECT sequence,contract_id FROM universe_head WHERE singleton_id=1"
            ).fetchone()
            reachable: set[str] = set()
            if head is not None:
                current_id = head[1]
                current_sequence = head[0]
                while current_id is not None:
                    contract = self._read_contract(connection, current_id)
                    if contract.sequence != current_sequence or contract.contract_id in reachable:
                        raise UniverseStoreUnavailable("universe source state head chain invalid")
                    reachable.add(contract.contract_id)
                    current_id = contract.parent_contract_id
                    current_sequence -= 1
                if current_sequence != 0:
                    raise UniverseStoreUnavailable("universe source state head chain incomplete")
            self._validate_global(connection, reachable)
            if not rows:
                return None
            row = rows[0]
            return UniverseSourceStateV1(
                source_state_id=row[0],
                source_state_sha256=row[1],
                trade_date=date.fromisoformat(row[2]),
                provider_id=row[3],
                source_version_digest=row[4],
                source_refs_json=row[5],
                verified_at=datetime.fromisoformat(row[6].replace("Z", "+00:00")),
            )
        except (sqlite3.Error, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise UniverseStoreUnavailable("universe source state unavailable") from exc
        finally:
            connection.close()

    def promote(
        self,
        contract: UniverseContractV1,
        *,
        expected_sequence: int,
        expected_head_sha256: str | None,
        mapping: UniverseSemanticMappingV1 | None = None,
        evidence: tuple[UniverseInstrumentEvidenceV1, ...] = (),
        required_snapshot: RequiredSymbolSnapshotV1 | None = None,
        authority_bundle: UniverseAuthorityBundleV1 | None = None,
    ) -> UniverseHeadV1:
        try:
            contract = UniverseContractV1.model_validate(
                contract.model_dump(mode="python"), strict=False
            )
            if mapping is not None:
                mapping = UniverseSemanticMappingV1.model_validate(
                    mapping.model_dump(mode="python"), strict=False
                )
            evidence = tuple(
                UniverseInstrumentEvidenceV1.model_validate(
                    item.model_dump(mode="python"), strict=False
                )
                for item in evidence
            )
            if authority_bundle is not None:
                authority_bundle = UniverseAuthorityBundleV1.model_validate(
                    authority_bundle.model_dump(mode="python"), strict=False
                )
                if mapping != authority_bundle.mapping or evidence != authority_bundle.evidence:
                    raise UniverseStoreUnavailable("universe authority bundle mismatch")
            if required_snapshot is not None:
                required_snapshot = RequiredSymbolSnapshotV1.model_validate(
                    required_snapshot.model_dump(mode="python"), strict=False
                )
                expected_snapshot_symbols = tuple(
                    (item.symbol, item.scope_roles)
                    for item in contract.members
                    if "required_user" in item.scope_roles or "required_index" in item.scope_roles
                )
                if required_snapshot.symbols != expected_snapshot_symbols:
                    raise UniverseStoreUnavailable("universe required snapshot roles mismatch")
        except (TypeError, ValueError) as exc:
            raise UniverseStoreUnavailable("universe candidate validation unavailable") from exc
        with _file_lock(self.lock_path):
            connection = self._connect()
            try:
                self._validate(connection)
                connection.execute("BEGIN IMMEDIATE")
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
                if not contract.publication_eligible:
                    raise UniverseStoreUnavailable("unpublishable universe contract")
                if (
                    contract.source_refs.classification_source != "fixture"
                    and authority_bundle is None
                ):
                    raise UniverseStoreUnavailable("universe authority bundle unavailable")
                if mapping is None or required_snapshot is None:
                    raise UniverseStoreUnavailable("universe evidence bundle unavailable")
                if (
                    mapping.authority_status != "reviewed"
                    or mapping.mapping_sha256 != contract.source_refs.semantic_mapping_sha256
                ):
                    raise UniverseStoreUnavailable("universe semantic mapping unavailable")
                expected_evidence = set(contract.source_refs.instrument_evidence_ids)
                actual_evidence = {item.evidence_id for item in evidence}
                if actual_evidence != expected_evidence or any(
                    item.authority_status != "reviewed"
                    or item.mapping_sha256 != mapping.mapping_sha256
                    or item.mapping_id != mapping.mapping_id
                    or item.mapping_version != mapping.mapping_version
                    or item.source_snapshot_date > contract.trade_date
                    or item.evidence_trade_date != contract.trade_date
                    for item in evidence
                ):
                    raise UniverseStoreUnavailable("universe instrument evidence unavailable")
                member_symbols = {member.symbol for member in contract.members}
                if any(
                    item.symbol not in member_symbols and not item.exclusion_reason
                    for item in evidence
                ):
                    raise UniverseStoreUnavailable("excluded evidence lacks exclusion reason")
                evidence_by_id = {item.evidence_id: item for item in evidence}
                expected_projection = tuple(
                    {
                        "security_id": item.security_id,
                        "symbol": item.symbol,
                        "evidence_id": item.evidence_id,
                        "exclusion_reason": item.exclusion_reason,
                    }
                    for item in sorted(evidence, key=lambda value: value.evidence_id)
                )
                if tuple(contract.classification_evidence_projection) != expected_projection:
                    raise UniverseStoreUnavailable("universe classification projection mismatch")
                for member in contract.members:
                    item = evidence_by_id.get(member.instrument_evidence_id)
                    if (
                        item is None
                        or item.security_id != member.security_id
                        or item.symbol != member.symbol
                    ):
                        raise UniverseStoreUnavailable("universe evidence member identity mismatch")
                    if item.exclusion_reason != member.exclusion_reason:
                        raise UniverseStoreUnavailable("universe evidence exclusion mismatch")
                    if (
                        item.expected_trading_state is not None
                        and item.expected_trading_state != member.expected_trading_state
                    ):
                        raise UniverseStoreUnavailable("universe evidence state mismatch")
                    if member.member_kind == "index":
                        if (
                            item.index_role != "required_index"
                            or item.symbol not in INDEX_SYMBOLS
                            or (
                                item.security_type not in {None, "index"}
                                and item.source_schema != "classification.instrument.v1"
                            )
                            or item.source_date_semantics != "source_observed"
                            or item.authority_status != "reviewed"
                            or (
                                item.expected_trading_state is not None
                                and item.expected_trading_state != "trading"
                            )
                        ):
                            raise UniverseStoreUnavailable("required index evidence unavailable")
                    elif item.index_role != "not_applicable":
                        raise UniverseStoreUnavailable("stock evidence index identity mismatch")
                if (
                    required_snapshot.snapshot_id
                    != contract.source_refs.required_symbol_snapshot_id
                    or required_snapshot.snapshot_sha256
                    != contract.source_refs.required_symbol_snapshot_sha256
                ):
                    raise UniverseStoreUnavailable("universe required snapshot unavailable")
                source_state_id, source_state_sha = _source_state_identity(contract.source_refs)
                source_refs_json = canonical_json_bytes(
                    contract.source_refs.model_dump(mode="json")
                ).decode()
                source_values = (
                    source_state_id,
                    source_state_sha,
                    contract.trade_date.isoformat(),
                    "baostock",
                    contract.source_refs.source_version_digest,
                    source_refs_json,
                    contract.source_refs.classification_observed_at.astimezone(UTC).isoformat(),
                )
                self._insert_or_match(
                    connection,
                    "universe_source_state",
                    "source_state_id",
                    source_values,
                )
                snapshot_values = (
                    contract.source_refs.required_symbol_snapshot_id,
                    contract.source_refs.required_symbol_snapshot_sha256,
                    required_snapshot.snapshot_token_digest,
                    1,
                    len(required_snapshot.symbols),
                    canonical_json_bytes(
                        {
                            "schema_version": required_snapshot.schema_version,
                            "symbols": [
                                {"symbol": symbol, "roles": list(roles)}
                                for symbol, roles in required_snapshot.symbols
                            ],
                        }
                    ).decode(),
                )
                self._insert_or_match(
                    connection,
                    "universe_required_symbol_snapshot",
                    "snapshot_id",
                    snapshot_values,
                )
                mapping_values = (
                    mapping.mapping_id,
                    mapping.mapping_version,
                    mapping.provider_id,
                    mapping.source_schema,
                    mapping.payload_json,
                    mapping.mapping_sha256,
                    mapping.authority_status,
                )
                self._insert_or_match(
                    connection,
                    "universe_semantic_mapping",
                    "mapping_id",
                    mapping_values,
                )
                for item in evidence:
                    self._insert_or_match(
                        connection,
                        "universe_instrument_evidence",
                        "evidence_id",
                        (
                            item.evidence_id,
                            item.evidence_sha256,
                            item.provider_id,
                            item.authority_status,
                            item.artifact_origin,
                            item.security_id,
                            item.symbol,
                            item.mapping_id,
                            item.mapping_version,
                            item.mapping_sha256,
                            item.source_snapshot_date.isoformat(),
                            item.evidence_trade_date.isoformat(),
                            item.exclusion_reason,
                            item.index_role,
                            item.source_date_semantics,
                            item.evidence_json,
                            item.adapter_version,
                            item.source_schema,
                            item.security_type,
                            item.exchange,
                            item.board,
                            item.list_date.isoformat() if item.list_date else None,
                            item.delist_date.isoformat() if item.delist_date else None,
                            item.listing_status,
                            item.daily_trade_status,
                            item.suspension_state,
                            item.st_state,
                            item.expected_trading_state,
                            item.observed_at.astimezone(UTC).isoformat()
                            if item.observed_at
                            else None,
                            item.lineage_hash,
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
                    roles = []
                    if "effective_main_board" in member.scope_roles:
                        roles.append("effective_main_board")
                    if (
                        "required_user" in member.scope_roles
                        or "required_index" in member.scope_roles
                    ):
                        roles.append("required_additions")
                    roles.extend(("classification_evidence", "member"))
                    for role in roles:
                        connection.execute(
                            "INSERT INTO contract_evidence VALUES (?,?,?,?,?,?)",
                            (
                                contract.contract_id,
                                member.instrument_evidence_id,
                                role,
                                member.security_id,
                                member.symbol,
                                member.exclusion_reason,
                            ),
                        )
                member_evidence_ids = {item.instrument_evidence_id for item in contract.members}
                for item in evidence:
                    if item.evidence_id in member_evidence_ids:
                        continue
                    connection.execute(
                        "INSERT INTO contract_evidence VALUES (?,?,?,?,?,?)",
                        (
                            contract.contract_id,
                            item.evidence_id,
                            "classification_evidence",
                            item.security_id,
                            item.symbol,
                            item.exclusion_reason,
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
                updated_at = datetime.now(UTC)
                if row is None:
                    connection.execute(
                        "INSERT INTO universe_head VALUES (1,?,?,?,?,?)",
                        (
                            contract.sequence,
                            contract.contract_id,
                            contract.contract_sha256,
                            head_sha,
                            updated_at.isoformat(),
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
                            updated_at.isoformat(),
                            expected_sequence,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise UniverseStoreUnavailable("universe head CAS conflict")
                # Re-open the candidate through the same transaction and
                # verify global reachability before the commit becomes
                # visible.  A malformed link can therefore never advance the
                # head and leave a partially authoritative sidecar.
                reachable = {contract.contract_id}
                parent_id = contract.parent_contract_id
                while parent_id is not None:
                    parent = self._read_contract(connection, parent_id)
                    reachable.add(parent.contract_id)
                    parent_id = parent.parent_contract_id
                self._validate_global(connection, reachable)
                connection.commit()
                return UniverseHeadV1(
                    sequence=contract.sequence,
                    contract_id=contract.contract_id,
                    contract_sha256=contract.contract_sha256,
                    head_sha256=head_sha,
                    updated_at=updated_at,
                )
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

    @staticmethod
    def _insert_or_match(
        connection: sqlite3.Connection, table: str, identity_column: str, values: tuple[Any, ...]
    ) -> None:
        """Insert an immutable row, allowing only byte-identical idempotence."""
        existing = connection.execute(
            f"SELECT * FROM {table} WHERE {identity_column}=?", (values[0],)
        ).fetchone()
        if existing is not None:
            if tuple(existing) != values:
                raise UniverseStoreUnavailable(f"{table} identity conflict")
            return
        placeholders = ",".join("?" for _ in values)
        try:
            connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)
        except sqlite3.IntegrityError as exc:
            raise UniverseStoreUnavailable(f"{table} immutable insert conflict") from exc
