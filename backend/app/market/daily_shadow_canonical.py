"""Descriptor-bound read-only projection of canonical active-stock Daily OHLC."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from backend.app.storage.dataset import _R2F2_LINEAGE_FIELDS

from .daily_shadow_models import (
    DAILY_CANONICAL_SUSPENDED_PARTIAL_ISSUES,
    CanonicalDailyOhlcRow,
    DailyCanonicalLineageState,
    DailyCanonicalReadResult,
    DailyCanonicalSnapshot,
    DailyCanonicalUnavailableReason,
    canonical_to_tickflow_daily_symbol,
    daily_canonical_universe_sha256,
    domain_sha256,
)

_EXPECTED_SCHEMA = (
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
_MANIFEST_KEYS = {"dataset", "schema_version", "generation", "files"}
_LEGACY_DESCRIPTOR_KEYS = {"path", "row_count", "sha256", "source", "trade_date"}
_R2F2_DESCRIPTOR_KEYS = _LEGACY_DESCRIPTOR_KEYS | set(_R2F2_LINEAGE_FIELDS)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_GENERATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SAFE_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")
_MAX_METADATA_BYTES = 1024 * 1024
_MAX_PARTITION_BYTES = 64 * 1024 * 1024
_MAX_ROWS = 4000


@dataclass(frozen=True)
class _Fingerprint:
    path: Path
    sha256: str
    size: int
    dev: int
    inode: int
    mode: int
    ctime_ns: int
    mtime_ns: int


class _ReadFailure(RuntimeError):
    def __init__(self, reason: DailyCanonicalUnavailableReason):
        self.reason = reason
        super().__init__(reason.value)


def _assert_no_symlink_chain(path: Path) -> None:
    if not path.is_absolute():
        raise _ReadFailure(DailyCanonicalUnavailableReason.ROOT_UNAVAILABLE)
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = os.lstat(current)
        except OSError as exc:
            raise _ReadFailure(DailyCanonicalUnavailableReason.ROOT_UNAVAILABLE) from exc
        if stat.S_ISLNK(info.st_mode):
            raise _ReadFailure(DailyCanonicalUnavailableReason.ROOT_UNAVAILABLE)


def _read_file(path: Path, *, max_bytes: int) -> tuple[bytes, _Fingerprint]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size < 1
            or before.st_size > max_bytes
        ):
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_ctime_ns,
            before.st_mtime_ns,
        )
        if len(payload) != before.st_size or identity != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_ctime_ns,
            after.st_mtime_ns,
        ):
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_CHANGED)
        raw = bytes(payload)
        return raw, _Fingerprint(
            path=path,
            sha256=hashlib.sha256(raw).hexdigest(),
            size=before.st_size,
            dev=before.st_dev,
            inode=before.st_ino,
            mode=before.st_mode,
            ctime_ns=before.st_ctime_ns,
            mtime_ns=before.st_mtime_ns,
        )
    finally:
        os.close(descriptor)


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID) from exc
    if not isinstance(value, dict):
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    return value


def _lineage(
    descriptor: dict[str, Any],
) -> tuple[DailyCanonicalLineageState, dict[str, str] | None]:
    keys = set(descriptor)
    if keys == _LEGACY_DESCRIPTOR_KEYS:
        return DailyCanonicalLineageState.LEGACY_UNAVAILABLE, None
    if keys != _R2F2_DESCRIPTOR_KEYS:
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    values = {field: descriptor[field] for field in _R2F2_LINEAGE_FIELDS}
    if any(not isinstance(value, str) or not value for value in values.values()):
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    if values["provider_id"] != "baostock":
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    for field in ("evidence_sha256", "candidate_manifest_sha256", "gate_report_sha256"):
        if _SHA256.fullmatch(values[field]) is None:
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    return DailyCanonicalLineageState.R2F2_VERIFIED, values


def _validate_descriptor(
    root: Path, descriptor: dict[str, Any], trade_date: date
) -> tuple[Path, DailyCanonicalLineageState, dict[str, str] | None]:
    lineage_state, lineage = _lineage(descriptor)
    if (
        descriptor.get("source") != "baostock"
        or descriptor.get("trade_date") != trade_date.isoformat()
        or type(descriptor.get("row_count")) is not int
        or not 1 <= descriptor["row_count"] <= _MAX_ROWS
        or not isinstance(descriptor.get("sha256"), str)
        or _SHA256.fullmatch(descriptor["sha256"]) is None
        or not isinstance(descriptor.get("path"), str)
    ):
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    relative = Path(descriptor["path"])
    if relative.is_absolute() or ".." in relative.parts or relative.suffix.lower() != ".parquet":
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    expected_parent = (
        Path("bars") / "source=baostock" / f"year={trade_date:%Y}" / f"month={trade_date:%m}"
    )
    expected_name = f"date={trade_date.isoformat()}_{descriptor['sha256'][:12]}.parquet"
    if relative.parent != expected_parent or relative.name != expected_name:
        raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
    path = root / relative
    _assert_no_symlink_chain(path.parent)
    return path, lineage_state, lineage


def _validate_manifest_entries(root: Path, entries: list[Any]) -> None:
    seen_dates: set[date] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
        try:
            entry_date = date.fromisoformat(entry.get("trade_date", ""))
        except (TypeError, ValueError) as exc:
            raise _ReadFailure(
                DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID
            ) from exc
        if entry_date in seen_dates:
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
        seen_dates.add(entry_date)
        _validate_descriptor(root, entry, entry_date)


def _quality_issues(value: Any) -> tuple[str, ...]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID) from exc
    if (
        not isinstance(parsed, list)
        or any(not isinstance(item, str) or not item for item in parsed)
        or len(parsed) != len(set(parsed))
    ):
        raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
    return tuple(parsed)


def _partition_rows(
    path: Path, *, expected_sha256: str, expected_rows: int
) -> tuple[list[tuple[Any, ...]], _Fingerprint]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_UNAVAILABLE) from exc
    try:
        before = os.fstat(descriptor)
        configured = os.stat(path, follow_symlinks=False)
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_ctime_ns,
            before.st_mtime_ns,
        )
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or not 1 <= before.st_size <= _MAX_PARTITION_BYTES
            or identity
            != (
                configured.st_dev,
                configured.st_ino,
                configured.st_mode,
                configured.st_size,
                configured.st_ctime_ns,
                configured.st_mtime_ns,
            )
        ):
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_UNAVAILABLE)
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        if digest.hexdigest() != expected_sha256 or size != before.st_size:
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_UNAVAILABLE)
        os.lseek(descriptor, 0, os.SEEK_SET)
        connection = duckdb.connect(":memory:")
        try:
            parquet_ref = f"/dev/fd/{descriptor}"
            schema = tuple(
                (str(row[0]), str(row[1]).upper())
                for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{parquet_ref}', hive_partitioning=false)"
                ).fetchall()
            )
            if schema != _EXPECTED_SCHEMA:
                raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SCHEMA_INVALID)
            columns = ",".join(f'"{name}"' for name, _kind in _EXPECTED_SCHEMA)
            rows = connection.execute(
                f"SELECT {columns} FROM read_parquet('{parquet_ref}', hive_partitioning=false)"
            ).fetchall()
        except duckdb.Error as exc:
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_UNAVAILABLE) from exc
        finally:
            connection.close()
        after = os.fstat(descriptor)
        configured_after = os.stat(path, follow_symlinks=False)
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_ctime_ns,
            after.st_mtime_ns,
        )
        configured_after_identity = (
            configured_after.st_dev,
            configured_after.st_ino,
            configured_after.st_mode,
            configured_after.st_size,
            configured_after.st_ctime_ns,
            configured_after.st_mtime_ns,
        )
        if identity != after_identity or identity != configured_after_identity:
            raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_CHANGED)
        if len(rows) != expected_rows:
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
        return rows, _Fingerprint(
            path=path,
            sha256=expected_sha256,
            size=before.st_size,
            dev=before.st_dev,
            inode=before.st_ino,
            mode=before.st_mode,
            ctime_ns=before.st_ctime_ns,
            mtime_ns=before.st_mtime_ns,
        )
    finally:
        os.close(descriptor)


def _project_rows(
    rows: list[tuple[Any, ...]], trade_date: date
) -> tuple[tuple[CanonicalDailyOhlcRow, ...], tuple[tuple[str, str], ...]]:
    eligible: list[CanonicalDailyOhlcRow] = []
    excluded: list[tuple[str, str]] = []
    seen: set[str] = set()
    for values in rows:
        (
            row_date,
            symbol,
            security_type,
            exchange,
            _board,
            open_value,
            high,
            low,
            close,
            _preclose,
            _volume,
            _amount,
            _turnover_rate,
            _pct_change,
            _adjust_factor,
            _price_adjustment,
            is_trading,
            is_suspended,
            _is_st,
            source,
            _source_record_id,
            _ingested_at,
            quality_status,
            quality_issues,
        ) = values
        if (
            str(row_date)[:10] != trade_date.isoformat()
            or not isinstance(symbol, str)
            or _SAFE_SYMBOL.fullmatch(symbol) is None
            or exchange not in {"sh", "sz"}
            or symbol[:2] != exchange
            or symbol in seen
            or security_type not in {"stock", "index"}
            or type(is_trading) is not bool
            or type(is_suspended) is not bool
            or source != "baostock"
        ):
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
        seen.add(symbol)
        issues = _quality_issues(quality_issues)
        if security_type == "index":
            if quality_status != "ready" or issues:
                raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
            excluded.append((symbol, "security_type_index"))
            continue
        if (is_trading, is_suspended) == (False, True):
            legal_suspended_quality = (quality_status == "ready" and not issues) or (
                quality_status == "partial"
                and tuple(sorted(issues)) == DAILY_CANONICAL_SUSPENDED_PARTIAL_ISSUES
            )
            if not legal_suspended_quality:
                raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
            excluded.append((symbol, "canonical_nontrading"))
            continue
        if (is_trading, is_suspended) != (True, False) or quality_status != "ready" or issues:
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
        try:
            eligible.append(
                CanonicalDailyOhlcRow(
                    trade_date=trade_date,
                    symbol=symbol,
                    open=Decimal(str(open_value)),
                    high=Decimal(str(high)),
                    low=Decimal(str(low)),
                    close=Decimal(str(close)),
                )
            )
        except Exception as exc:
            raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID) from exc
    ordered = tuple(sorted(eligible, key=lambda row: row.symbol))
    if not ordered:
        raise _ReadFailure(DailyCanonicalUnavailableReason.PARTITION_SEMANTICS_INVALID)
    return ordered, tuple(sorted(excluded))


class DailyCanonicalReader:
    """Open one immutable canonical partition without creating state."""

    def __init__(self, root: Path | str, *, trade_date: date):
        self.root = Path(root)
        self.trade_date = trade_date
        self._snapshot: DailyCanonicalSnapshot | None = None
        self._fingerprints: tuple[_Fingerprint, ...] = ()

    @staticmethod
    def _unavailable(reason: DailyCanonicalUnavailableReason) -> DailyCanonicalReadResult:
        return DailyCanonicalReadResult(status="unavailable", unavailable_reason=reason)

    def read(self) -> DailyCanonicalReadResult:
        if self._snapshot is not None:
            return self.verify()
        try:
            _assert_no_symlink_chain(self.root)
            sentinel_raw, sentinel_fp = _read_file(
                self.root / ".stock-eva-dataset.json", max_bytes=_MAX_METADATA_BYTES
            )
            if _json_object(sentinel_raw) != {
                "dataset": "stock-eva-market",
                "schema_version": 2,
            }:
                raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
            manifest_raw, manifest_fp = _read_file(
                self.root / "manifest.json", max_bytes=_MAX_METADATA_BYTES
            )
            manifest = _json_object(manifest_raw)
            if (
                set(manifest) != _MANIFEST_KEYS
                or manifest.get("dataset") != "stock-eva-market"
                or manifest.get("schema_version") != 2
                or not isinstance(manifest.get("generation"), str)
                or _SAFE_GENERATION.fullmatch(manifest["generation"]) is None
                or not isinstance(manifest.get("files"), list)
            ):
                raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
            _validate_manifest_entries(self.root, manifest["files"])
            matches = [
                item
                for item in manifest["files"]
                if isinstance(item, dict)
                and item.get("source") == "baostock"
                and item.get("trade_date") == self.trade_date.isoformat()
            ]
            if len(matches) != 1:
                raise _ReadFailure(DailyCanonicalUnavailableReason.CANONICAL_DESCRIPTOR_INVALID)
            descriptor = matches[0]
            path, lineage_state, lineage = _validate_descriptor(
                self.root, descriptor, self.trade_date
            )
            rows, partition_fp = _partition_rows(
                path,
                expected_sha256=descriptor["sha256"],
                expected_rows=descriptor["row_count"],
            )
            projected, excluded = _project_rows(rows, self.trade_date)
            symbols = tuple(row.symbol for row in projected)
            mapping = tuple(
                (symbol, canonical_to_tickflow_daily_symbol(symbol)) for symbol in symbols
            )
            ohlc = tuple(
                {
                    "symbol": row.symbol,
                    "open": str(row.open),
                    "high": str(row.high),
                    "low": str(row.low),
                    "close": str(row.close),
                }
                for row in projected
            )
            exclusion_sha256 = domain_sha256(
                "stock-eva/r2f3/daily-canonical-exclusions/v1", excluded
            )
            universe_sha256 = daily_canonical_universe_sha256(
                symbols=symbols,
                exclusion_sha256=exclusion_sha256,
                manifest_generation=manifest["generation"],
                manifest_sha256=manifest_fp.sha256,
                partition_relative_path=descriptor["path"],
                partition_sha256=partition_fp.sha256,
                partition_row_count=descriptor["row_count"],
            )
            snapshot = DailyCanonicalSnapshot(
                trade_date=self.trade_date,
                lineage_state=lineage_state,
                r2f2_lineage=lineage,
                manifest_generation=manifest["generation"],
                manifest_sha256=manifest_fp.sha256,
                partition_relative_path=descriptor["path"],
                partition_sha256=partition_fp.sha256,
                partition_row_count=descriptor["row_count"],
                eligible_symbol_count=len(projected),
                excluded_symbol_count=len(excluded),
                canonical_universe_sha256=universe_sha256,
                canonical_exclusion_sha256=exclusion_sha256,
                symbol_mapping_sha256=domain_sha256(
                    "stock-eva/r2f3/daily-symbol-mapping/v1", mapping
                ),
                ohlc_sha256=domain_sha256("stock-eva/r2f3/daily-canonical-ohlc/v1", ohlc),
                rows=projected,
            )
            self._snapshot = snapshot
            self._fingerprints = (sentinel_fp, manifest_fp, partition_fp)
            return DailyCanonicalReadResult(status="ready", snapshot=snapshot)
        except _ReadFailure as exc:
            return self._unavailable(exc.reason)
        except Exception:
            return self._unavailable(DailyCanonicalUnavailableReason.PARTITION_UNAVAILABLE)

    def verify(self) -> DailyCanonicalReadResult:
        if self._snapshot is None:
            return self.read()
        try:
            current = (
                _read_file(self._fingerprints[0].path, max_bytes=_MAX_METADATA_BYTES)[1],
                _read_file(self._fingerprints[1].path, max_bytes=_MAX_METADATA_BYTES)[1],
                _read_file(self._fingerprints[2].path, max_bytes=_MAX_PARTITION_BYTES)[1],
            )
        except _ReadFailure:
            return self._unavailable(DailyCanonicalUnavailableReason.CANONICAL_CHANGED)
        if current != self._fingerprints:
            return self._unavailable(DailyCanonicalUnavailableReason.CANONICAL_CHANGED)
        return DailyCanonicalReadResult(status="ready", snapshot=self._snapshot)


class PublishedDailyCanonicalProjection:
    """Closeable proof that one exact canonical Daily snapshot remains unchanged."""

    def __init__(self, reader: DailyCanonicalReader, snapshot: DailyCanonicalSnapshot):
        self.reader = reader
        self.snapshot = snapshot
        self.closed = False

    @classmethod
    def open(
        cls, reader: DailyCanonicalReader
    ) -> PublishedDailyCanonicalProjection | DailyCanonicalReadResult:
        result = reader.read()
        if result.status != "ready" or result.snapshot is None:
            return result
        return cls(reader, result.snapshot)

    def verify(self) -> DailyCanonicalReadResult:
        if self.closed:
            return DailyCanonicalReadResult(
                status="unavailable",
                unavailable_reason=DailyCanonicalUnavailableReason.CANONICAL_CHANGED,
            )
        return self.reader.verify()

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> PublishedDailyCanonicalProjection:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
