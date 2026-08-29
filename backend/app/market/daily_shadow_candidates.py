"""Final-success evidence binding and Daily-only OHLC candidate reconciliation."""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .daily_shadow_canonical import PublishedDailyCanonicalProjection
from .daily_shadow_models import (
    DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
    DAILY_SHADOW_PROFILE,
    DailyCanonicalSnapshot,
    DailyShadowFetchResult,
    DailyShadowPlan,
    DailyShadowSourceRow,
    canonical_json_bytes,
    domain_sha256,
)
from .providers.tickflow_daily_shadow import (
    DAILY_SHADOW_ADAPTER_HASH,
    DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
    DAILY_SHADOW_MAPPING_HASH,
    DAILY_SHADOW_SOURCE_SCHEMA_HASH,
    DAILY_SHADOW_UNIT_STATE_HASH,
)
from .shadow_evidence import (
    ShadowAttempt,
    ShadowCompletion,
    ShadowEvidenceBundle,
    ShadowEvidenceReader,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
    ShadowRequestCompletion,
    _exclusive_rename,
    _mkdir_chain,
    _mkdir_exclusive,
    _open_directory_fd,
    _page_content_sha,
    _physical_path,
    _read_verified_at,
    _request_contract_values,
    _write_private_file,
)

_PRICE_TOLERANCE = Decimal("0.01")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class DailyCandidateUnavailable(RuntimeError):
    """Sanitized Daily candidate failure."""


class DailyCandidateMismatch(DailyCandidateUnavailable):
    def __init__(self, report: DailyBarReconciliationReport):
        self.report = report
        super().__init__("Daily OHLC reconciliation mismatch")


class DailyBarReconciliationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    version: Literal["r2f3-free-daily-ohlc-v1"] = "r2f3-free-daily-ohlc-v1"
    compared_fields: tuple[Literal["open", "high", "low", "close"], ...] = (
        "open",
        "high",
        "low",
        "close",
    )
    absolute_price_tolerance: Decimal = _PRICE_TOLERANCE
    activity_comparison: Literal["EXCLUDED_UNITS_UNKNOWN"] = "EXCLUDED_UNITS_UNKNOWN"
    factor_comparison: Literal["EXCLUDED_UNQUALIFIED"] = "EXCLUDED_UNQUALIFIED"
    policy_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_policy(self) -> DailyBarReconciliationPolicy:
        if self.compared_fields != ("open", "high", "low", "close"):
            raise ValueError("Daily reconciliation fields mismatch")
        if self.absolute_price_tolerance != _PRICE_TOLERANCE:
            raise ValueError("Daily reconciliation tolerance mismatch")
        values = self.model_dump(mode="json")
        values.pop("policy_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-reconciliation-policy/v1", values)
        if self.policy_sha256 == "0" * 64:
            object.__setattr__(self, "policy_sha256", expected)
        elif self.policy_sha256 != expected:
            raise ValueError("Daily reconciliation policy hash mismatch")
        return self


DAILY_BAR_RECONCILIATION_POLICY = DailyBarReconciliationPolicy()
_CANDIDATE_POLICY_HASH = DAILY_BAR_RECONCILIATION_POLICY.policy_sha256


def daily_version_vector_sha256(terms_evidence_sha256: str) -> str:
    if (
        not isinstance(terms_evidence_sha256, str)
        or _SHA256.fullmatch(terms_evidence_sha256) is None
    ):
        raise DailyCandidateUnavailable("Daily version vector unavailable")
    return domain_sha256(
        "stock-eva/r2f3/free-daily-version-vector/v1",
        {
            "profile": DAILY_SHADOW_PROFILE,
            "adapter": DAILY_SHADOW_ADAPTER_HASH,
            "endpoint": DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
            "source_schema": DAILY_SHADOW_SOURCE_SCHEMA_HASH,
            "mapping": DAILY_SHADOW_MAPPING_HASH,
            "unit_state": DAILY_SHADOW_UNIT_STATE_HASH,
            "reconciliation": _CANDIDATE_POLICY_HASH,
            "terms": terms_evidence_sha256,
            "universe_policy": DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        },
    )


class DailyEvidenceInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    plan: ShadowLogicalRequestPlan
    completion: ShadowCompletion
    attempts: tuple[ShadowAttempt, ...]


class DailyBarQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")
    trade_date: str
    expected_symbol_count: int = Field(ge=1, le=4000)
    observed_symbol_count: int = Field(ge=1, le=4000)
    exact_symbol_set: bool
    exact_one_row_coverage: bool
    exact_trade_date: bool
    finite_positive_ohlc: bool
    legal_ohlc_ordering: bool
    verdict: Literal["PASS", "FAIL"]
    report_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_report(self) -> DailyBarQualityReport:
        passed = all(
            (
                self.exact_symbol_set,
                self.exact_one_row_coverage,
                self.exact_trade_date,
                self.finite_positive_ohlc,
                self.legal_ohlc_ordering,
                self.expected_symbol_count == self.observed_symbol_count,
            )
        )
        if (self.verdict == "PASS") != passed:
            raise ValueError("Daily quality verdict mismatch")
        values = self.model_dump(mode="json")
        values.pop("report_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-quality/v1", values)
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("Daily quality report hash mismatch")
        return self


class DailyBarReconciliationReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    candidate_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")
    policy_version: Literal["r2f3-free-daily-ohlc-v1"] = "r2f3-free-daily-ohlc-v1"
    policy_sha256: str = Field(default=_CANDIDATE_POLICY_HASH, pattern=r"^[0-9a-f]{64}$")
    absolute_price_tolerance: Decimal = _PRICE_TOLERANCE
    expected_symbol_count: int = Field(ge=1, le=4000)
    observed_symbol_count: int = Field(ge=1, le=4000)
    price_cell_count: int = Field(ge=4, le=16000)
    within_tolerance_count: int = Field(ge=0, le=16000)
    max_open_delta: Decimal = Field(ge=0)
    max_high_delta: Decimal = Field(ge=0)
    max_low_delta: Decimal = Field(ge=0)
    max_close_delta: Decimal = Field(ge=0)
    verdict: Literal["PASS", "MISMATCH"]
    report_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_report(self) -> DailyBarReconciliationReport:
        if (
            self.policy_sha256 != _CANDIDATE_POLICY_HASH
            or self.absolute_price_tolerance != _PRICE_TOLERANCE
        ):
            raise ValueError("Daily reconciliation policy mismatch")
        if self.price_cell_count != self.expected_symbol_count * 4:
            raise ValueError("Daily reconciliation cell count mismatch")
        passed = (
            self.expected_symbol_count == self.observed_symbol_count
            and self.within_tolerance_count == self.price_cell_count
            and max(
                self.max_open_delta,
                self.max_high_delta,
                self.max_low_delta,
                self.max_close_delta,
            )
            <= _PRICE_TOLERANCE
        )
        if (self.verdict == "PASS") != passed:
            raise ValueError("Daily reconciliation verdict mismatch")
        values = self.model_dump(mode="json")
        values.pop("report_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-reconciliation/v1", values)
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("Daily reconciliation report hash mismatch")
        return self


class DailyBarShadowCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str = Field(pattern=r"^daily-candidate-[0-9a-f]{32}$")
    provider: Literal["tickflow"] = "tickflow"
    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    trade_date: str
    evidence_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    daily_request_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_request_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_partition_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_universe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_exclusion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    symbol_mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    endpoint_contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_schema_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    mapping_contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    unit_state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reconciliation_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    terms_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_symbol_count: int = Field(ge=1, le=4000)
    observed_symbol_count: int = Field(ge=1, le=4000)
    source_rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_ohlc_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    units_state: Literal["UNKNOWN"] = "UNKNOWN"
    suspension_semantics_state: Literal["UNKNOWN"] = "UNKNOWN"
    factor_evidence_state: Literal["UNQUALIFIED"] = "UNQUALIFIED"
    adjustment_request_state: Literal["NONE_REQUESTED"] = "NONE_REQUESTED"
    quality_verdict: Literal["PASS"] = "PASS"
    quality_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reconciliation_verdict: Literal["PASS"] = "PASS"
    reconciliation_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_candidate(self) -> DailyBarShadowCandidate:
        if self.expected_symbol_count != self.observed_symbol_count:
            raise ValueError("Daily candidate coverage mismatch")
        if (
            self.adapter_sha256 != DAILY_SHADOW_ADAPTER_HASH
            or self.endpoint_contract_sha256 != DAILY_SHADOW_ENDPOINT_CONTRACT_HASH
            or self.source_schema_sha256 != DAILY_SHADOW_SOURCE_SCHEMA_HASH
            or self.mapping_contract_sha256 != DAILY_SHADOW_MAPPING_HASH
            or self.universe_policy_sha256 != DAILY_CANONICAL_UNIVERSE_POLICY_SHA256
            or self.unit_state_sha256 != DAILY_SHADOW_UNIT_STATE_HASH
            or self.reconciliation_policy_sha256 != _CANDIDATE_POLICY_HASH
        ):
            raise ValueError("Daily candidate contract mismatch")
        values = self.model_dump(mode="json")
        values.pop("candidate_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-candidate/v1", values)
        if self.candidate_sha256 == "0" * 64:
            object.__setattr__(self, "candidate_sha256", expected)
        elif self.candidate_sha256 != expected:
            raise ValueError("Daily candidate hash mismatch")
        return self


class DailyCandidateArtifacts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate: DailyBarShadowCandidate
    quality: DailyBarQualityReport
    reconciliation: DailyBarReconciliationReport
    bundle_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_artifacts(self) -> DailyCandidateArtifacts:
        if (
            self.quality.candidate_id != self.candidate.candidate_id
            or self.reconciliation.candidate_id != self.candidate.candidate_id
            or self.quality.report_sha256 != self.candidate.quality_report_sha256
            or self.reconciliation.report_sha256 != self.candidate.reconciliation_report_sha256
            or self.quality.verdict != "PASS"
            or self.reconciliation.verdict != "PASS"
        ):
            raise ValueError("Daily candidate artifact graph mismatch")
        expected = domain_sha256(
            "stock-eva/r2f3/free-daily-candidate-bundle/v1",
            {
                "candidate_sha256": self.candidate.candidate_sha256,
                "quality_sha256": self.quality.report_sha256,
                "reconciliation_sha256": self.reconciliation.report_sha256,
            },
        )
        if self.bundle_sha256 == "0" * 64:
            object.__setattr__(self, "bundle_sha256", expected)
        elif self.bundle_sha256 != expected:
            raise ValueError("Daily candidate bundle hash mismatch")
        return self


def _daily_shard_identity(shard: Any) -> str:
    return (
        f"daily-shard-{shard.ordinal:02d}-{len(shard.canonical_symbols):03d}-{shard.request_sha256}"
    )


def _timestamp_matches_trade_date(timestamp: int, trade_date: date) -> bool:
    start = datetime.combine(trade_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000) <= timestamp < int(end.timestamp() * 1000)


def build_daily_evidence_inputs(
    plan: DailyShadowPlan,
    fetch: DailyShadowFetchResult,
    *,
    job_id: str,
    window_id: str,
    session_id: str,
    evidence_id: str,
) -> DailyEvidenceInputs:
    if type(plan) is not DailyShadowPlan:
        raise TypeError("Daily shadow plan is required")
    if type(fetch) is not DailyShadowFetchResult or fetch.status != "ready":
        raise DailyCandidateUnavailable("Daily shadow final success is unavailable")
    try:
        plan = DailyShadowPlan.model_validate(plan.model_dump(mode="python"))
        fetch = DailyShadowFetchResult.model_validate(fetch.model_dump(mode="python"))
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily shadow final success is invalid") from exc
    expected = tuple(symbol for shard in plan.shards for symbol in shard.canonical_symbols)
    by_symbol = {row.symbol: row for row in fetch.rows}
    if (
        fetch.request_plan_sha256 != plan.request_plan_sha256
        or fetch.trade_date != plan.trade_date
        or fetch.request_count != plan.request_count
        or len(by_symbol) != len(fetch.rows)
        or set(by_symbol) != set(expected)
        or any(row.trade_date != plan.trade_date for row in fetch.rows)
        or any(
            not _timestamp_matches_trade_date(row.timestamp, plan.trade_date) for row in fetch.rows
        )
        or len(fetch.observations) != plan.request_count
        or tuple(item.ordinal for item in fetch.observations) != tuple(range(plan.request_count))
        or any(
            item.outcome != "SUCCESS"
            or item.expected_rows != len(plan.shards[item.ordinal].canonical_symbols)
            or item.observed_rows != item.expected_rows
            for item in fetch.observations
        )
    ):
        raise DailyCandidateUnavailable("Daily shadow final success is incomplete")
    requests: list[ShadowLogicalRequest] = []
    attempts: list[ShadowAttempt] = []
    completions: list[ShadowRequestCompletion] = []
    for shard in plan.shards:
        request = ShadowLogicalRequest(
            job_id=job_id,
            window_id=window_id,
            ordinal=shard.ordinal,
            request_id=shard.request_id,
            provider_id="tickflow",
            endpoint="historical_daily_1d",
            endpoint_class="historical_daily_1d",
            role="daily_bar_ohlc",
            trade_date=plan.trade_date,
            symbol_or_index_shard=_daily_shard_identity(shard),
            schema_contract_hash=DAILY_SHADOW_SOURCE_SCHEMA_HASH,
            unit_contract_hash=DAILY_SHADOW_UNIT_STATE_HASH,
        )
        source_rows = tuple(by_symbol[symbol] for symbol in shard.canonical_symbols)
        page_id = f"{request.request_id}:page-000001"
        page = {
            "page_identity": page_id,
            "rows": [row.model_dump(mode="json") for row in source_rows],
        }
        attempt_id = f"{request.request_id}-attempt-1"
        requests.append(request)
        attempts.append(
            ShadowAttempt(
                attempt_id=attempt_id,
                job_id=job_id,
                provider_id="tickflow",
                trade_date=plan.trade_date,
                universe_id=plan.canonical_universe_sha256,
                attempt_number=1,
                ordinal=shard.ordinal,
                outcome="success",
                rows=tuple(row.model_dump(mode="json") for row in source_rows),
                pages=(page,),
                request_count=1,
                retry_count=0,
            )
        )
        completions.append(
            ShadowRequestCompletion(
                ordinal=shard.ordinal,
                request_id=request.request_id,
                endpoint=request.endpoint,
                endpoint_class=request.endpoint_class,
                final_attempt_id=attempt_id,
                pages=(page,),
            )
        )
    evidence_plan = ShadowLogicalRequestPlan(
        plan_id=f"daily-plan-{plan.trade_date.isoformat()}",
        job_id=job_id,
        provider_id="tickflow",
        window_id=window_id,
        requests=tuple(requests),
    )
    completion = ShadowCompletion(
        session_id=session_id,
        job_id=job_id,
        provider_id="tickflow",
        window_id=window_id,
        evidence_id=evidence_id,
        request_plan_sha256=evidence_plan.request_plan_sha256,
        requests=tuple(completions),
    )
    return DailyEvidenceInputs(
        plan=evidence_plan,
        completion=completion,
        attempts=tuple(attempts),
    )


def _candidate_id(
    plan: DailyShadowPlan,
    evidence: ShadowEvidenceBundle,
    snapshot: DailyCanonicalSnapshot,
    terms_evidence_sha256: str,
) -> str:
    digest = domain_sha256(
        "stock-eva/r2f3/free-daily-candidate-id/v1",
        {
            "plan": plan.request_plan_sha256,
            "evidence": evidence.manifest_sha256,
            "canonical": snapshot.snapshot_sha256,
            "terms": terms_evidence_sha256,
        },
    )
    return f"daily-candidate-{digest[:32]}"


def _normalized_decimal(value: int | float | Decimal) -> str:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise DailyCandidateUnavailable("Daily evidence rows are invalid")
    normalized = format(parsed.normalize(), "f")
    return "0" if normalized in {"-0", ""} else normalized


def _read_daily_page_rows(
    reader: ShadowEvidenceReader,
    evidence_id: str,
    descriptor: dict[str, Any],
    plan: DailyShadowPlan,
) -> tuple[DailyShadowSourceRow, ...]:
    held_fds: list[int] = []
    try:
        root_fd = _open_directory_fd(_physical_path(reader.root))
        held_fds.append(root_fd)
        bundles_fd = os.open(
            "bundles",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
        held_fds.append(bundles_fd)
        bundle_fd = os.open(
            evidence_id,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=bundles_fd,
        )
        held_fds.append(bundle_fd)
        pages_fd = os.open(
            "pages",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=bundle_fd,
        )
        held_fds.append(pages_fd)
        before = tuple(os.fstat(item) for item in held_fds)
        page_descriptors = descriptor.get("pages")
        if not isinstance(page_descriptors, list) or len(page_descriptors) != plan.request_count:
            raise DailyCandidateUnavailable("Daily evidence page graph mismatch")
        raw_rows: list[dict[str, Any]] = []
        parsed_rows: list[DailyShadowSourceRow] = []
        for shard, page_descriptor in zip(plan.shards, page_descriptors, strict=True):
            if (
                not isinstance(page_descriptor, dict)
                or page_descriptor.get("ordinal") != shard.ordinal
                or page_descriptor.get("row_count") != len(shard.canonical_symbols)
                or not isinstance(page_descriptor.get("relative_path"), str)
                or Path(page_descriptor["relative_path"]).parent != Path("pages")
            ):
                raise DailyCandidateUnavailable("Daily evidence page graph mismatch")
            raw = _read_verified_at(
                pages_fd,
                Path(page_descriptor["relative_path"]).name,
                max_bytes=8 * 1024 * 1024,
            )
            page = json.loads(raw.decode("utf-8"))
            if (
                not isinstance(page, dict)
                or set(page) != {"page_identity", "rows"}
                or page.get("page_identity") != page_descriptor.get("page_identity")
                or not isinstance(page.get("rows"), list)
                or _page_content_sha(page) != page_descriptor.get("content_sha256")
            ):
                raise DailyCandidateUnavailable("Daily evidence page graph mismatch")
            shard_rows = tuple(DailyShadowSourceRow.model_validate(row) for row in page["rows"])
            if (
                tuple(row.symbol for row in shard_rows) != shard.canonical_symbols
                or any(row.trade_date != plan.trade_date for row in shard_rows)
                or any(
                    not _timestamp_matches_trade_date(row.timestamp, plan.trade_date)
                    for row in shard_rows
                )
            ):
                raise DailyCandidateUnavailable("Daily evidence page graph mismatch")
            raw_rows.extend(page["rows"])
            parsed_rows.extend(shard_rows)
        if tuple(raw_rows) != tuple(descriptor.get("rows", ())):
            # The immutable manifest intentionally does not hash its aggregate rows directly;
            # Daily qualification closes that generic-model gap against the hashed page objects.
            raise DailyCandidateUnavailable("Daily evidence page graph mismatch")
        after = tuple(os.fstat(item) for item in held_fds)
        if any(
            (old.st_dev, old.st_ino, old.st_mode, old.st_ctime_ns)
            != (new.st_dev, new.st_ino, new.st_mode, new.st_ctime_ns)
            for old, new in zip(before, after, strict=True)
        ):
            raise DailyCandidateUnavailable("Daily evidence page graph changed")
        return tuple(parsed_rows)
    except DailyCandidateUnavailable:
        raise
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily evidence page graph unavailable") from exc
    finally:
        while held_fds:
            os.close(held_fds.pop())


def _quality(
    candidate_id: str,
    snapshot: DailyCanonicalSnapshot,
    rows: tuple[DailyShadowSourceRow, ...],
) -> DailyBarQualityReport:
    expected = tuple(row.symbol for row in snapshot.rows)
    observed = tuple(row.symbol for row in rows)
    exact_set = observed == expected and len(observed) == len(set(observed))
    exact_date = all(row.trade_date == snapshot.trade_date for row in rows)
    return DailyBarQualityReport(
        candidate_id=candidate_id,
        trade_date=snapshot.trade_date.isoformat(),
        expected_symbol_count=len(expected),
        observed_symbol_count=len(observed),
        exact_symbol_set=exact_set,
        exact_one_row_coverage=exact_set,
        exact_trade_date=exact_date,
        finite_positive_ohlc=True,
        legal_ohlc_ordering=True,
        verdict="PASS" if exact_set and exact_date else "FAIL",
    )


def _reconcile(
    candidate_id: str,
    snapshot: DailyCanonicalSnapshot,
    rows: tuple[DailyShadowSourceRow, ...],
) -> DailyBarReconciliationReport:
    canonical = {row.symbol: row for row in snapshot.rows}
    observed = {row.symbol: row for row in rows}
    deltas: dict[str, list[Decimal]] = {field: [] for field in ("open", "high", "low", "close")}
    within = 0
    if set(canonical) == set(observed):
        for symbol in sorted(canonical):
            for field in deltas:
                delta = abs(
                    Decimal(str(getattr(canonical[symbol], field)))
                    - Decimal(str(getattr(observed[symbol], field)))
                )
                deltas[field].append(delta)
                if delta <= _PRICE_TOLERANCE:
                    within += 1
    price_cells = len(canonical) * 4
    maxima = {field: max(values, default=Decimal("0")) for field, values in deltas.items()}
    passed = (
        set(canonical) == set(observed) and len(observed) == len(rows) and within == price_cells
    )
    return DailyBarReconciliationReport(
        candidate_id=candidate_id,
        expected_symbol_count=len(canonical),
        observed_symbol_count=len(observed),
        price_cell_count=price_cells,
        within_tolerance_count=within,
        max_open_delta=maxima["open"],
        max_high_delta=maxima["high"],
        max_low_delta=maxima["low"],
        max_close_delta=maxima["close"],
        verdict="PASS" if passed else "MISMATCH",
    )


def build_daily_candidate(
    *,
    plan: DailyShadowPlan,
    evidence_plan: ShadowLogicalRequestPlan,
    evidence: ShadowEvidenceBundle,
    evidence_reader: ShadowEvidenceReader,
    canonical: PublishedDailyCanonicalProjection,
    terms_evidence_sha256: str,
) -> DailyCandidateArtifacts:
    if (
        type(plan) is not DailyShadowPlan
        or type(evidence_plan) is not ShadowLogicalRequestPlan
        or type(evidence) is not ShadowEvidenceBundle
        or type(evidence_reader) is not ShadowEvidenceReader
        or type(canonical) is not PublishedDailyCanonicalProjection
    ):
        raise TypeError("Daily candidate requires concrete plan and canonical capability")
    if (
        not isinstance(terms_evidence_sha256, str)
        or _SHA256.fullmatch(terms_evidence_sha256) is None
    ):
        raise DailyCandidateUnavailable("Daily terms evidence is unavailable")
    try:
        plan = DailyShadowPlan.model_validate(plan.model_dump(mode="python"))
        evidence_plan = ShadowLogicalRequestPlan.model_validate(
            evidence_plan.model_dump(mode="python")
        )
        evidence = ShadowEvidenceBundle.model_validate(evidence.model_dump(mode="python"))
        snapshot = DailyCanonicalSnapshot.model_validate(
            canonical.snapshot.model_dump(mode="python")
        )
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily candidate inputs are invalid") from exc
    try:
        before = canonical.verify()
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily canonical snapshot unavailable") from exc
    if (
        before.status != "ready"
        or before.snapshot is None
        or before.snapshot.snapshot_sha256 != snapshot.snapshot_sha256
        or plan.canonical_snapshot_sha256 != snapshot.snapshot_sha256
        or plan.trade_date != snapshot.trade_date
        or plan.canonical_universe_sha256 != snapshot.canonical_universe_sha256
        or plan.canonical_exclusion_sha256 != snapshot.canonical_exclusion_sha256
        or plan.symbol_mapping_sha256 != snapshot.symbol_mapping_sha256
        or tuple(symbol for shard in plan.shards for symbol in shard.canonical_symbols)
        != tuple(row.symbol for row in snapshot.rows)
    ):
        raise DailyCandidateUnavailable("Daily canonical snapshot changed")
    try:
        verified, descriptor = evidence_reader.read_descriptor(evidence.evidence_id)
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily evidence unavailable") from exc
    if verified != evidence:
        raise DailyCandidateUnavailable("Daily evidence changed")
    if (
        not evidence.evidence_ready
        or evidence.plan_sha256 != evidence_plan.request_plan_sha256
        or descriptor.get("plan_sha256") != evidence_plan.request_plan_sha256
        or evidence.completion_sha256 != descriptor.get("completion_sha256")
        or descriptor.get("provider_id") != "tickflow"
        or descriptor.get("job_id") != evidence_plan.job_id
        or descriptor.get("window_id") != evidence_plan.window_id
        or descriptor.get("plan_requests")
        != [_request_contract_values(item) for item in evidence_plan.requests]
        or len(evidence_plan.requests) != plan.request_count
        or len(evidence.rows) != plan.symbol_count
        or len(evidence.page_refs) != plan.request_count
    ):
        raise DailyCandidateUnavailable("Daily evidence identity mismatch")
    for shard, request in zip(plan.shards, evidence_plan.requests, strict=True):
        if (
            request.ordinal != shard.ordinal
            or request.request_id != shard.request_id
            or request.provider_id != "tickflow"
            or request.endpoint != "historical_daily_1d"
            or request.endpoint_class != "historical_daily_1d"
            or request.role != "daily_bar_ohlc"
            or request.trade_date != plan.trade_date
            or request.symbol_or_index_shard != _daily_shard_identity(shard)
            or request.schema_contract_hash != DAILY_SHADOW_SOURCE_SCHEMA_HASH
            or request.unit_contract_hash != DAILY_SHADOW_UNIT_STATE_HASH
        ):
            raise DailyCandidateUnavailable("Daily evidence request mismatch")
    rows = _read_daily_page_rows(evidence_reader, evidence.evidence_id, descriptor, plan)
    candidate_id = _candidate_id(plan, evidence, snapshot, terms_evidence_sha256)
    quality = _quality(candidate_id, snapshot, rows)
    if quality.verdict != "PASS":
        raise DailyCandidateUnavailable("Daily candidate quality failed")
    reconciliation = _reconcile(candidate_id, snapshot, rows)
    try:
        after = canonical.verify()
    except Exception as exc:
        raise DailyCandidateUnavailable("Daily canonical snapshot unavailable") from exc
    if (
        after.status != "ready"
        or after.snapshot is None
        or after.snapshot.snapshot_sha256 != snapshot.snapshot_sha256
    ):
        raise DailyCandidateUnavailable("Daily canonical snapshot changed")
    if reconciliation.verdict != "PASS":
        raise DailyCandidateMismatch(reconciliation)
    source_values = tuple(row.model_dump(mode="json") for row in rows)
    normalized = tuple(
        {
            "symbol": row.symbol,
            "open": _normalized_decimal(row.open),
            "high": _normalized_decimal(row.high),
            "low": _normalized_decimal(row.low),
            "close": _normalized_decimal(row.close),
        }
        for row in rows
    )
    candidate = DailyBarShadowCandidate(
        candidate_id=candidate_id,
        trade_date=plan.trade_date.isoformat(),
        evidence_id=evidence.evidence_id,
        evidence_sha256=evidence.manifest_sha256,
        completion_sha256=evidence.completion_sha256,
        daily_request_plan_sha256=plan.request_plan_sha256,
        evidence_request_plan_sha256=evidence_plan.request_plan_sha256,
        canonical_snapshot_sha256=snapshot.snapshot_sha256,
        canonical_manifest_sha256=snapshot.manifest_sha256,
        canonical_partition_sha256=snapshot.partition_sha256,
        canonical_universe_sha256=snapshot.canonical_universe_sha256,
        canonical_exclusion_sha256=snapshot.canonical_exclusion_sha256,
        symbol_mapping_sha256=snapshot.symbol_mapping_sha256,
        adapter_sha256=DAILY_SHADOW_ADAPTER_HASH,
        endpoint_contract_sha256=DAILY_SHADOW_ENDPOINT_CONTRACT_HASH,
        source_schema_sha256=DAILY_SHADOW_SOURCE_SCHEMA_HASH,
        mapping_contract_sha256=DAILY_SHADOW_MAPPING_HASH,
        universe_policy_sha256=DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        unit_state_sha256=DAILY_SHADOW_UNIT_STATE_HASH,
        reconciliation_policy_sha256=_CANDIDATE_POLICY_HASH,
        terms_evidence_sha256=terms_evidence_sha256,
        version_vector_sha256=daily_version_vector_sha256(terms_evidence_sha256),
        expected_symbol_count=len(snapshot.rows),
        observed_symbol_count=len(rows),
        source_rows_sha256=domain_sha256("stock-eva/r2f3/free-daily-source-rows/v1", source_values),
        normalized_ohlc_sha256=domain_sha256(
            "stock-eva/r2f3/free-daily-normalized-ohlc/v1", normalized
        ),
        quality_report_sha256=quality.report_sha256,
        reconciliation_report_sha256=reconciliation.report_sha256,
    )
    return DailyCandidateArtifacts(
        candidate=candidate,
        quality=quality,
        reconciliation=reconciliation,
    )


class DailyCandidateStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.staging = self.root / "daily-candidate-staging"
        self.bundles = self.root / "daily-candidates"

    def _prepare(self) -> None:
        if not self.root.is_absolute():
            raise DailyCandidateUnavailable("Daily candidate root unavailable")
        try:
            self.root = _physical_path(self.root)
            self.staging = self.root / "daily-candidate-staging"
            self.bundles = self.root / "daily-candidates"
            for path in (self.root, self.staging, self.bundles):
                _mkdir_chain(path)
        except Exception as exc:
            raise DailyCandidateUnavailable("Daily candidate root unavailable") from exc

    def _read_existing(self, artifacts: DailyCandidateArtifacts) -> DailyCandidateArtifacts:
        existing = DailyCandidateReader(self.root).read(artifacts.candidate.candidate_id)
        if existing != artifacts:
            raise DailyCandidateUnavailable("Daily candidate identity conflict")
        return existing

    def publish(
        self, artifacts: DailyCandidateArtifacts, *, simulate_crash: bool = False
    ) -> DailyCandidateArtifacts:
        if type(artifacts) is not DailyCandidateArtifacts:
            raise TypeError("Daily candidate artifacts are required")
        try:
            artifacts = DailyCandidateArtifacts.model_validate(artifacts.model_dump(mode="python"))
        except Exception as exc:
            raise DailyCandidateUnavailable("Daily candidate artifacts are invalid") from exc
        self._prepare()
        destination = self.bundles / artifacts.candidate.candidate_id
        if destination.exists():
            return self._read_existing(artifacts)
        staging = self.staging / secrets.token_hex(16)
        try:
            _mkdir_exclusive(staging)
            _write_private_file(
                staging / "candidate.json",
                canonical_json_bytes(artifacts.candidate.model_dump(mode="json")),
            )
            _write_private_file(
                staging / "quality.json",
                canonical_json_bytes(artifacts.quality.model_dump(mode="json")),
            )
            _write_private_file(
                staging / "reconciliation.json",
                canonical_json_bytes(artifacts.reconciliation.model_dump(mode="json")),
            )
            _write_private_file(staging / "COMMIT", f"COMMIT\n{artifacts.bundle_sha256}\n".encode())
            staging_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            if simulate_crash:
                raise RuntimeError("Daily candidate publish interrupted")
            try:
                _exclusive_rename(staging, destination)
            except Exception as exc:
                if destination.exists():
                    return self._read_existing(artifacts)
                raise DailyCandidateUnavailable(
                    "Daily candidate atomic publish unavailable"
                ) from exc
            parent_fd = _open_directory_fd(self.bundles)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
            verified = DailyCandidateReader(self.root).read(artifacts.candidate.candidate_id)
            if verified != artifacts:
                raise DailyCandidateUnavailable("Daily candidate readback mismatch")
            return verified
        except (DailyCandidateUnavailable, RuntimeError):
            raise
        except Exception as exc:
            raise DailyCandidateUnavailable("Daily candidate publish unavailable") from exc


class DailyCandidateReader:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def read(self, candidate_id: str) -> DailyCandidateArtifacts:
        held_fds: list[int] = []

        def hold(descriptor: int) -> int:
            held_fds.append(descriptor)
            return descriptor

        try:
            if (
                not self.root.is_absolute()
                or re.fullmatch(r"daily-candidate-[0-9a-f]{32}", candidate_id) is None
            ):
                raise DailyCandidateUnavailable("Daily candidate bundle unavailable")
            root_fd = hold(_open_directory_fd(_physical_path(self.root)))
            bundles_fd = hold(
                os.open(
                    "daily-candidates",
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=root_fd,
                )
            )
            bundle_fd = hold(
                os.open(
                    candidate_id,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=bundles_fd,
                )
            )
            before = tuple(os.fstat(item) for item in (root_fd, bundles_fd, bundle_fd))
            if any(info.st_mode & 0o022 for info in before):
                raise DailyCandidateUnavailable("Daily candidate bundle unavailable")
            entries = list(os.scandir(bundle_fd))
            expected_names = {"candidate.json", "quality.json", "reconciliation.json", "COMMIT"}
            if {entry.name for entry in entries} != expected_names:
                raise DailyCandidateUnavailable("Daily candidate bundle unavailable")
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise DailyCandidateUnavailable("Daily candidate bundle unavailable")
            candidate_raw = _read_verified_at(bundle_fd, "candidate.json", max_bytes=256 * 1024)
            quality_raw = _read_verified_at(bundle_fd, "quality.json", max_bytes=64 * 1024)
            reconciliation_raw = _read_verified_at(
                bundle_fd, "reconciliation.json", max_bytes=64 * 1024
            )
            commit_raw = _read_verified_at(bundle_fd, "COMMIT", max_bytes=128)
            candidate = DailyBarShadowCandidate.model_validate_json(candidate_raw)
            quality = DailyBarQualityReport.model_validate_json(quality_raw)
            reconciliation = DailyBarReconciliationReport.model_validate_json(reconciliation_raw)
            artifacts = DailyCandidateArtifacts(
                candidate=candidate,
                quality=quality,
                reconciliation=reconciliation,
            )
            expected_id = (
                "daily-candidate-"
                + domain_sha256(
                    "stock-eva/r2f3/free-daily-candidate-id/v1",
                    {
                        "plan": candidate.daily_request_plan_sha256,
                        "evidence": candidate.evidence_sha256,
                        "canonical": candidate.canonical_snapshot_sha256,
                        "terms": candidate.terms_evidence_sha256,
                    },
                )[:32]
            )
            if (
                candidate.candidate_id != candidate_id
                or candidate_id != expected_id
                or candidate.trade_date != quality.trade_date
                or candidate.expected_symbol_count != quality.expected_symbol_count
                or candidate.observed_symbol_count != quality.observed_symbol_count
                or candidate.expected_symbol_count != reconciliation.expected_symbol_count
                or candidate.observed_symbol_count != reconciliation.observed_symbol_count
                or candidate_raw != canonical_json_bytes(candidate.model_dump(mode="json"))
                or quality_raw != canonical_json_bytes(quality.model_dump(mode="json"))
                or reconciliation_raw
                != canonical_json_bytes(reconciliation.model_dump(mode="json"))
            ):
                raise DailyCandidateUnavailable("Daily candidate identity mismatch")
            if commit_raw != f"COMMIT\n{artifacts.bundle_sha256}\n".encode():
                raise DailyCandidateUnavailable("Daily candidate commit mismatch")
            after = tuple(os.fstat(item) for item in (root_fd, bundles_fd, bundle_fd))
            if any(
                (old.st_dev, old.st_ino, old.st_mode, old.st_ctime_ns)
                != (new.st_dev, new.st_ino, new.st_mode, new.st_ctime_ns)
                for old, new in zip(before, after, strict=True)
            ):
                raise DailyCandidateUnavailable("Daily candidate bundle changed")
            return artifacts
        except DailyCandidateUnavailable:
            raise
        except Exception:
            raise DailyCandidateUnavailable("Daily candidate unavailable") from None
        finally:
            while held_fds:
                os.close(held_fds.pop())
