"""Deterministic, comparison-only R2-F3 reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import date
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .shadow_normalize import SHADOW_PROVIDERS, ShadowNormalizedCandidate, ShadowNormalizedRow


class ShadowCanonicalUnavailable(RuntimeError):
    """The immutable canonical comparison descriptor is unavailable."""


class CanonicalSessionRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    provider_id: Literal["baostock"] = "baostock"
    source: Literal["baostock"] = "baostock"
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    preclose: float
    volume: float
    amount: float
    suspension: bool = False
    listed: bool = True
    factor: float | None = None
    factor_previous: float | None = None
    factor_semantics: str = "multiplicative_back_adjust"
    price_unit: str = "CNY"
    volume_unit: str = "shares"
    amount_unit: str = "CNY"


class CanonicalSessionCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_id: Literal["baostock"] = "baostock"
    trade_date: date
    universe_id: str
    rows: tuple[CanonicalSessionRow, ...]
    expected_symbols: tuple[str, ...]
    index_symbols: tuple[str, ...] = ()
    complete: bool = True
    quality_status: str = "ready"
    factor_semantics: str = "multiplicative_back_adjust"
    factor_anchor: str = "trade_date"
    factor_direction: str = "back_adjust"
    suspended_symbols: tuple[str, ...] = ()
    not_listed_symbols: tuple[str, ...] = ()
    canonical_manifest_generation: str
    canonical_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class CanonicalSessionCandidateReader:
    """Read one BaoStock session from the held canonical manifest/object fds."""

    _MAX_BYTES = 64 * 1024 * 1024
    _GENERATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    _SCHEMA = (
        ("trade_date", "DATE"),
        ("symbol", "VARCHAR"),
        ("security_type", "VARCHAR"),
        ("exchange", "VARCHAR"),
        ("board", "VARCHAR"),
        ("open", "DOUBLE"),
        ("high", "DOUBLE"),
        ("low", "DOUBLE"),
        ("close", "DOUBLE"),
        ("preclose", "DOUBLE"),
        ("volume", "DOUBLE"),
        ("amount", "DOUBLE"),
        ("turnover_rate", "DOUBLE"),
        ("pct_change", "DOUBLE"),
        ("adjust_factor", "DOUBLE"),
        ("price_adjustment", "VARCHAR"),
        ("is_trading", "BOOLEAN"),
        ("is_suspended", "BOOLEAN"),
        ("is_st", "BOOLEAN"),
        ("source", "VARCHAR"),
        ("source_record_id", "VARCHAR"),
        ("ingested_at", "TIMESTAMP WITH TIME ZONE"),
        ("quality_status", "VARCHAR"),
        ("quality_issues", "JSON"),
    )

    def __init__(self, root: Path | str, *, trade_date: date):
        self.root = Path(root)
        self.trade_date = trade_date
        self._manifest_fingerprint: tuple[int, int, int, int] | None = None

    def _open_relative(self, relative: Path) -> int:
        if not self.root.is_absolute() or relative.is_absolute() or ".." in relative.parts:
            raise ShadowCanonicalUnavailable("canonical descriptor unavailable")
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        current = os.open(os.sep, flags)
        try:
            for component in self.root.parts[1:]:
                next_fd = os.open(component, flags, dir_fd=current)
                os.close(current)
                current = next_fd
            for component in relative.parts[:-1]:
                next_fd = os.open(component, flags, dir_fd=current)
                os.close(current)
                current = next_fd
            return os.open(
                relative.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=current
            )
        except OSError as exc:
            raise ShadowCanonicalUnavailable("canonical descriptor unavailable") from exc
        finally:
            os.close(current)

    @classmethod
    def _read_fd(cls, fd: int) -> tuple[bytes, tuple[int, int, int, int]]:
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > cls._MAX_BYTES:
                raise ShadowCanonicalUnavailable("canonical descriptor unavailable")
            raw = os.pread(fd, cls._MAX_BYTES + 1, 0)
            after = os.fstat(fd)
            before_fp = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            after_fp = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            if len(raw) != before.st_size or before_fp != after_fp:
                raise ShadowCanonicalUnavailable("canonical descriptor changed")
            return raw, before_fp
        except OSError as exc:
            raise ShadowCanonicalUnavailable("canonical descriptor unavailable") from exc

    def read(self) -> CanonicalSessionCandidate:
        manifest_fd = self._open_relative(Path("manifest.json"))
        try:
            manifest_raw, fingerprint = self._read_fd(manifest_fd)
        finally:
            os.close(manifest_fd)
        if self._manifest_fingerprint is not None and fingerprint != self._manifest_fingerprint:
            raise ShadowCanonicalUnavailable("canonical descriptor changed")
        self._manifest_fingerprint = fingerprint
        try:
            manifest = json.loads(manifest_raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ShadowCanonicalUnavailable("canonical descriptor unavailable") from exc
        if (
            not isinstance(manifest, dict)
            or set(manifest) != {"dataset", "schema_version", "generation", "files"}
            or manifest["dataset"] != "stock-eva-market"
            or manifest["schema_version"] != 2
            or not isinstance(manifest["generation"], str)
            or self._GENERATION.fullmatch(manifest["generation"]) is None
            or not isinstance(manifest["files"], list)
        ):
            raise ShadowCanonicalUnavailable("canonical descriptor unavailable")
        matching = []
        for item in manifest["files"]:
            if not isinstance(item, dict):
                raise ShadowCanonicalUnavailable("canonical descriptor unavailable")
            try:
                item_date = date.fromisoformat(str(item["trade_date"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise ShadowCanonicalUnavailable("canonical descriptor unavailable") from exc
            if item_date == self.trade_date:
                matching.append(item)
        if len(matching) != 1:
            raise ShadowCanonicalUnavailable("canonical partition unavailable")
        item = matching[0]
        sha = item.get("sha256")
        if (
            item.get("source") != "baostock"
            or not isinstance(sha, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha) is None
            or not isinstance(item.get("row_count"), int)
            or item["row_count"] <= 0
            or not isinstance(item.get("path"), str)
        ):
            raise ShadowCanonicalUnavailable("canonical partition unavailable")
        relative = Path(item["path"])
        expected_parent = (
            Path("bars/source=baostock")
            / f"year={self.trade_date:%Y}"
            / f"month={self.trade_date:%m}"
        )
        if (
            relative.is_absolute()
            or relative.parent != expected_parent
            or relative.suffix != ".parquet"
        ):
            raise ShadowCanonicalUnavailable("canonical partition unavailable")
        partition_fd = self._open_relative(relative)
        try:
            raw, _partition_fp = self._read_fd(partition_fd)
            if hashlib.sha256(raw).hexdigest() != sha:
                raise ShadowCanonicalUnavailable("canonical partition checksum mismatch")
            query_fd = os.dup(partition_fd)
            try:
                connection = duckdb.connect(":memory:")
                try:
                    descriptor = f"/dev/fd/{query_fd}"
                    schema = tuple(
                        (row[0], row[1])
                        for row in connection.execute(
                            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                            [descriptor],
                        ).fetchall()
                    )
                    if schema != self._SCHEMA:
                        raise ShadowCanonicalUnavailable("canonical schema unavailable")
                    rows = connection.execute(
                        "SELECT trade_date,symbol,exchange,open,high,low,close,preclose,"
                        "volume,amount,adjust_factor,is_trading,is_suspended,source "
                        "FROM read_parquet(?, hive_partitioning=false) ORDER BY symbol",
                        [descriptor],
                    ).fetchall()
                finally:
                    connection.close()
            except duckdb.Error as exc:
                raise ShadowCanonicalUnavailable("canonical partition unreadable") from exc
            finally:
                os.close(query_fd)
        finally:
            os.close(partition_fd)
        if len(rows) != item["row_count"] or not rows:
            raise ShadowCanonicalUnavailable("canonical row count unavailable")
        canonical_rows: list[CanonicalSessionRow] = []
        for row in rows:
            (
                row_date,
                symbol,
                exchange,
                open_,
                high,
                low,
                close,
                preclose,
                volume,
                amount,
                factor,
                is_trading,
                is_suspended,
                source,
            ) = row
            if source != "baostock" or row_date != self.trade_date or not isinstance(symbol, str):
                raise ShadowCanonicalUnavailable("canonical row lineage unavailable")
            prefix = {"SSE": "sh", "SZSE": "sz"}.get(str(exchange).upper())
            if prefix is None:
                raise ShadowCanonicalUnavailable("canonical exchange unavailable")
            canonical_rows.append(
                CanonicalSessionRow(
                    symbol=f"{prefix}.{symbol.zfill(6)}",
                    trade_date=row_date,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    preclose=preclose,
                    volume=volume,
                    amount=amount,
                    suspension=bool(is_suspended or not is_trading),
                    factor=factor,
                    factor_previous=factor,
                )
            )
        manifest_sha = hashlib.sha256(manifest_raw).hexdigest()
        universe_id = item.get("universe_id")
        if not isinstance(universe_id, str) or not universe_id:
            raise ShadowCanonicalUnavailable("canonical universe unavailable")
        candidate_preimage = {
            "provider_id": "baostock",
            "trade_date": self.trade_date.isoformat(),
            "universe_id": universe_id,
            "manifest_generation": manifest["generation"],
            "manifest_sha256": manifest_sha,
            "rows": [row.model_dump(mode="json") for row in canonical_rows],
        }
        candidate_sha = hashlib.sha256(_canonical(candidate_preimage)).hexdigest()
        return CanonicalSessionCandidate(
            trade_date=self.trade_date,
            universe_id=universe_id,
            rows=tuple(canonical_rows),
            expected_symbols=tuple(row.symbol for row in canonical_rows),
            canonical_manifest_generation=manifest["generation"],
            canonical_manifest_sha256=manifest_sha,
            candidate_sha256=candidate_sha,
        )


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        + "\n"
    ).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class ReconciliationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: str = "r2f3-v1"
    legal_tick: float = 0.01
    volume_relative_error: float = 0.001
    amount_relative_error: float = 0.005
    adjusted_return_basis_points: float = 5.0
    sample_limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def frozen_r2f3_v1(self) -> ReconciliationPolicy:
        expected = {
            "version": "r2f3-v1",
            "legal_tick": 0.01,
            "volume_relative_error": 0.001,
            "amount_relative_error": 0.005,
            "adjusted_return_basis_points": 5.0,
            "sample_limit": 20,
        }
        if self.model_dump() != expected:
            raise ValueError("r2f3-v1 reconciliation policy is frozen")
        return self


class ShadowReconciliationReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    reconciliation_id: str
    policy_version: str
    trade_date: Any
    universe_id: str
    left_candidate_id: str
    right_candidate_id: str
    status: str
    compared_counts: dict[str, int]
    mismatch_counts: dict[str, int]
    bounded_sanitized_samples: tuple[dict[str, Any], ...] = ()
    quarantine_secondary: bool = False
    report_sha256: str = "0" * 64

    @model_validator(mode="after")
    def bind_hash(self) -> ShadowReconciliationReport:
        values = self.model_dump(mode="json")
        values.pop("report_sha256", None)
        expected = _sha(values)
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("shadow reconciliation hash mismatch")
        return self


def _candidate_id(candidate: ShadowNormalizedCandidate | CanonicalSessionCandidate) -> str:
    identity = getattr(candidate, "normalized_sha256", None) or getattr(
        candidate, "candidate_sha256", None
    )
    if not isinstance(identity, str) or not identity:
        raise ValueError("reconciliation candidate identity unavailable")
    return _sha(identity)[:24]


def _relative_error(left: float, right: float) -> float:
    scale = max(abs(left), abs(right), 1.0)
    return abs(left - right) / scale


def _return(row: ShadowNormalizedRow) -> float:
    factor = row.factor or 1.0
    previous_factor = row.factor_previous or factor
    return (row.close * factor) / (row.preclose * previous_factor) - 1.0


def _compatible_factor_contract(
    left: ShadowNormalizedCandidate | CanonicalSessionCandidate,
    right: ShadowNormalizedCandidate | CanonicalSessionCandidate,
) -> bool:
    def normalized(value: str) -> str:
        return value.strip().lower().replace("-", "_").replace(" ", "_")

    return normalized(left.factor_direction) == normalized(right.factor_direction) and normalized(
        left.factor_semantics
    ) == normalized(right.factor_semantics)


def reconcile(
    left: ShadowNormalizedCandidate | CanonicalSessionCandidate,
    right: ShadowNormalizedCandidate | CanonicalSessionCandidate,
    *,
    policy: ReconciliationPolicy | None = None,
) -> ShadowReconciliationReport:
    allowed = SHADOW_PROVIDERS | {"baostock"}
    if left.provider_id not in allowed or right.provider_id not in allowed:
        raise ValueError("shadow provider identity is not allowlisted")
    if (left.provider_id == "baostock") == (right.provider_id == "baostock"):
        raise ValueError("reconciliation requires one canonical and one shadow provider")
    policy = policy or ReconciliationPolicy()
    policy = ReconciliationPolicy.model_validate(policy.model_dump())
    left_id, right_id = _candidate_id(left), _candidate_id(right)
    identity = {
        "trade_date": left.trade_date,
        "universe_id": left.universe_id,
        "left_candidate_id": left_id,
        "right_candidate_id": right_id,
    }
    mismatch: dict[str, int] = {
        "identity": 0,
        "universe": 0,
        "suspension": 0,
        "ohlc": 0,
        "volume": 0,
        "amount": 0,
        "adjusted_return": 0,
    }
    samples: list[dict[str, Any]] = []
    if (
        left.trade_date != right.trade_date
        or left.universe_id != right.universe_id
        or not left.complete
        or not right.complete
        or not left.factor_semantics
        or not right.factor_semantics
        or not left.factor_anchor
        or not right.factor_anchor
        or not left.factor_direction
        or not right.factor_direction
        or not _compatible_factor_contract(left, right)
    ):
        mismatch["identity"] = 1
    if left.expected_symbols != right.expected_symbols or left.index_symbols != right.index_symbols:
        mismatch["universe"] = 1
    if left.suspended_symbols != right.suspended_symbols:
        mismatch["suspension"] = 1
    if left.not_listed_symbols != right.not_listed_symbols:
        mismatch["not_listed"] = 1
    left_rows = {row.symbol: row for row in left.rows}
    right_rows = {row.symbol: row for row in right.rows}
    if set(left_rows) != set(right_rows):
        mismatch["coverage"] = abs(len(set(left_rows) ^ set(right_rows)))
    for symbol in sorted(set(left_rows) & set(right_rows)):
        lrow, rrow = left_rows[symbol], right_rows[symbol]
        if (
            lrow.factor is None
            or lrow.factor_previous is None
            or rrow.factor is None
            or rrow.factor_previous is None
        ):
            mismatch["adjusted_return"] += 1
            continue
        if lrow.suspension != rrow.suspension:
            mismatch["suspension"] += 1
        if any(
            abs(getattr(lrow, field) - getattr(rrow, field)) > policy.legal_tick + 1e-12
            for field in ("open", "high", "low", "close", "preclose")
        ):
            mismatch["ohlc"] += 1
        if _relative_error(lrow.volume, rrow.volume) > policy.volume_relative_error:
            mismatch["volume"] += 1
        if _relative_error(lrow.amount, rrow.amount) > policy.amount_relative_error:
            mismatch["amount"] += 1
        if abs(_return(lrow) - _return(rrow)) * 10000 > policy.adjusted_return_basis_points:
            mismatch["adjusted_return"] += 1
        if any(
            mismatch[key] and len(samples) < policy.sample_limit
            for key in ("ohlc", "volume", "amount", "adjusted_return")
        ):
            samples.append(
                {
                    "symbol": symbol,
                    "fields": tuple(
                        key
                        for key, count in mismatch.items()
                        if count and key not in {"identity", "universe"}
                    ),
                }
            )
    compared = {
        "symbols": len(set(left_rows) & set(right_rows)),
        "left_rows": len(left.rows),
        "right_rows": len(right.rows),
    }
    active_count = max(len(set(left_rows) & set(right_rows)), 1)
    if any(row.factor is None or row.factor_previous is None for row in (*left.rows, *right.rows)):
        mismatch["identity"] = 1
    rate_limited = (
        mismatch["volume"] / active_count > 0.001 or mismatch["amount"] / active_count > 0.001
    )
    status = (
        "unavailable"
        if mismatch["identity"] or mismatch["universe"] or mismatch.get("coverage", 0)
        else (
            "material_mismatch"
            if mismatch["suspension"]
            or mismatch.get("not_listed", 0)
            or mismatch["ohlc"]
            or rate_limited
            or mismatch["adjusted_return"]
            else "ready"
        )
    )
    return ShadowReconciliationReport(
        reconciliation_id=_sha({"policy": policy.version, **identity, "mismatch": mismatch})[:32],
        policy_version=policy.version,
        trade_date=left.trade_date,
        universe_id=left.universe_id,
        left_candidate_id=left_id,
        right_candidate_id=right_id,
        status=status,
        compared_counts=compared,
        mismatch_counts=mismatch,
        bounded_sanitized_samples=tuple(samples[: policy.sample_limit]),
        quarantine_secondary=status == "material_mismatch",
    )


reconcile_candidates = reconcile
