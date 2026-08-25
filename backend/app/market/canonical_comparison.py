"""Read-only, descriptor-bound comparison with the published BaoStock snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .candidates import CandidateGateReport, CandidateManifest, SessionSelection
from .evidence import EvidenceReader
from .models import DailyBar


class UnavailableReason(StrEnum):
    REGISTRY_MISSING = "registry_missing"
    REGISTRY_SCHEMA_INVALID = "registry_schema_invalid"
    SHADOW_ROOT_UNAVAILABLE = "shadow_root_unavailable"
    JOB_STORE_UNAVAILABLE = "job_store_unavailable"
    CANONICAL_DESCRIPTOR_CHANGED = "canonical_descriptor_changed"
    CANDIDATE_DESCRIPTOR_CHANGED = "candidate_descriptor_changed"
    SELECTION_BINDING_INVALID = "selection_binding_invalid"
    EVIDENCE_INCOMPLETE = "evidence_incomplete"
    VERSION_VECTOR_DRIFT = "version_vector_drift"
    CALENDAR_SNAPSHOT_INVALID = "calendar_snapshot_invalid"


class CanonicalComparisonSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str
    dataset_manifest_generation: str
    dataset_manifest_sha256: str
    trade_date_partition_sha256: str
    trade_date_row_count: int = Field(ge=0)
    r2f2_publication_lineage: dict[str, Any]
    selection_id: str
    selection_sha256: str
    candidate_sha256: str
    evidence_sha256: str
    gate_sha256: str
    factor_sha256: str
    normalized_sha256: str
    trade_date: date
    universe_id: str
    version_vector: dict[str, str] = Field(default_factory=dict)
    snapshot_sha256: str = "0" * 64

    def model_post_init(self, __context: Any) -> None:
        values = self.model_dump(mode="json")
        values.pop("snapshot_sha256", None)
        expected = _sha(values)
        if self.snapshot_sha256 == "0" * 64:
            object.__setattr__(self, "snapshot_sha256", expected)
        elif self.snapshot_sha256 != expected:
            raise ValueError("canonical comparison snapshot hash mismatch")


class CanonicalComparisonResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: str
    snapshot: CanonicalComparisonSnapshot | None = None
    unavailable_reason: UnavailableReason | None = None


class _Fingerprint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str
    size: int
    dev: int
    inode: int
    mode: int
    ctime_ns: int
    mtime_ns: int


def _sha(value: bytes | Any) -> str:
    if not isinstance(value, bytes):
        value = (
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
            )
            + "\n"
        ).encode()
    return hashlib.sha256(value).hexdigest()


def _read_verified(path: Path, *, max_bytes: int = 64 * 1024 * 1024) -> tuple[bytes, _Fingerprint]:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise FileNotFoundError from exc
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size > max_bytes
        ):
            raise OSError
        payload = os.read(fd, max_bytes + 1)
        after = os.fstat(fd)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_ctime_ns,
            before.st_mtime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_ctime_ns,
            after.st_mtime_ns,
        )
        if len(payload) != before.st_size or before_identity != after_identity:
            raise OSError
        return payload, _Fingerprint(
            path=str(path),
            sha256=_sha(payload),
            size=before.st_size,
            dev=before.st_dev,
            inode=before.st_ino,
            mode=before.st_mode,
            ctime_ns=before.st_ctime_ns,
            mtime_ns=before.st_mtime_ns,
        )
    finally:
        os.close(fd)


def _json(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


def _assert_no_symlink_chain(path: Path) -> None:
    if not path.is_absolute():
        raise ValueError("descriptor path unavailable")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError("descriptor path unavailable")


def _assert_exact_json_tree(root: Path, names: set[str]) -> None:
    if not root.is_dir():
        raise ValueError("descriptor tree unavailable")
    entries = {entry.name for entry in root.iterdir()}
    if entries != names or any(entry.is_symlink() for entry in root.iterdir()):
        raise ValueError("descriptor tree unavailable")


class CanonicalCandidateReader:
    """Read exactly one published descriptor graph and never create state."""

    def __init__(
        self,
        dataset_root: Path | str,
        candidate_root: Path | str | None = None,
        evidence_root: Path | str | None = None,
        selection_root: Path | str | None = None,
        *,
        trade_date: date | None = None,
        universe_id: str | None = None,
        evidence_reader: Any | None = None,
    ):
        self.dataset_root = Path(dataset_root)
        self.candidate_root = Path(candidate_root) if candidate_root else self.dataset_root
        self.evidence_root = Path(evidence_root) if evidence_root else self.dataset_root
        self.selection_root = Path(selection_root) if selection_root else self.candidate_root
        self.trade_date = trade_date
        self.universe_id = universe_id
        self.evidence_reader = evidence_reader
        self._snapshot: CanonicalComparisonSnapshot | None = None
        self._paths: tuple[Path, ...] = ()
        self._fingerprints: tuple[_Fingerprint, ...] = ()

    def _unavailable(self, reason: UnavailableReason) -> CanonicalComparisonResult:
        return CanonicalComparisonResult(status="unavailable", unavailable_reason=reason)

    def _partition_descriptor(self, manifest: dict[str, Any], trade: date) -> tuple[str, str, int]:
        relative = manifest.get("trade_date_partition_relative_path") or manifest.get(
            "partition_relative_path"
        )
        if not relative and isinstance(manifest.get("partitions"), dict):
            entry = manifest["partitions"].get(trade.isoformat())
            if isinstance(entry, dict):
                relative = entry.get("relative_path")
        digest = manifest.get("trade_date_partition_sha256") or manifest.get("partition_sha256")
        row_count = manifest.get("trade_date_row_count") or manifest.get("partition_row_count")
        if (
            not isinstance(relative, str)
            or not relative
            or not isinstance(digest, str)
            or not isinstance(row_count, int)
        ):
            raise ValueError("canonical partition descriptor unavailable")
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("canonical partition path unavailable")
        return relative, digest, row_count

    def _validate_partition(self, path: Path, trade: date, row_count: int) -> None:
        if path.suffix.lower() != ".parquet":
            raise ValueError("canonical partition format unavailable")
        try:
            import duckdb

            connection = duckdb.connect(":memory:")
            try:
                rows = connection.execute(
                    "SELECT trade_date, symbol, open, high, low, close, volume, amount "
                    "FROM read_parquet(?)",
                    [str(path)],
                ).fetchall()
            finally:
                connection.close()
        except Exception as exc:
            raise ValueError("canonical partition unavailable") from exc
        if len(rows) != row_count or any(
            str(row[0])[:10] != trade.isoformat()
            or not str(row[1]).lower().startswith(("sh.", "sz."))
            or any(value is None for value in row[2:])
            for row in rows
        ):
            raise ValueError("canonical partition semantics mismatch")

    def _read_evidence(self, evidence_id: str) -> Any:
        return (self.evidence_reader or EvidenceReader(self.evidence_root)).read(evidence_id)

    def read(self) -> CanonicalComparisonResult:
        if self._snapshot is not None:
            return self.verify()
        try:
            for root in (
                self.dataset_root,
                self.candidate_root,
                self.evidence_root,
                self.selection_root,
            ):
                _assert_no_symlink_chain(root)
            if self.selection_root == self.candidate_root:
                _assert_exact_json_tree(
                    self.candidate_root,
                    {"candidate.json", "gate.json", "normalized.json", "selection.json"},
                )
            else:
                _assert_exact_json_tree(
                    self.candidate_root, {"candidate.json", "gate.json", "normalized.json"}
                )
                _assert_exact_json_tree(self.selection_root, {"selection.json"})
            manifest_raw, manifest_fp = _read_verified(self.dataset_root / "manifest.json")
            manifest = _json(manifest_raw)
            if not isinstance(manifest, dict) or not manifest.get("generation"):
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            candidate_path = self.candidate_root / "candidate.json"
            gate_path = self.candidate_root / "gate.json"
            normalized_path = self.candidate_root / "normalized.json"
            selection_path = self.selection_root / "selection.json"
            candidate_raw, candidate_fp = _read_verified(candidate_path)
            gate_raw, gate_fp = _read_verified(gate_path)
            normalized_raw, normalized_fp = _read_verified(normalized_path)
            selection_raw, selection_fp = _read_verified(selection_path)
            candidate = CandidateManifest.model_validate(_json(candidate_raw))
            gate = CandidateGateReport.model_validate(_json(gate_raw))
            selection = SessionSelection.model_validate(_json(selection_raw))
            if (
                candidate.provider_id.value != "baostock"
                or candidate.status != "accepted"
                or selection.selected_provider_id.value != "baostock"
            ):
                return self._unavailable(UnavailableReason.SELECTION_BINDING_INVALID)
            if (
                selection.selected_candidate_id != candidate.candidate_id
                or selection.trade_date != candidate.trade_date
                or selection.universe_id != candidate.universe_id
                or selection.candidate_manifest_sha256 != candidate.manifest_sha256
                or selection.evidence_sha256 != candidate.evidence_sha256
                or selection.gate_report_sha256 != gate.aggregate_sha256
            ):
                return self._unavailable(UnavailableReason.SELECTION_BINDING_INVALID)
            if (
                gate.candidate_id != candidate.candidate_id
                or gate.verdict != "pass"
                or candidate.gate_report_sha256 != gate.aggregate_sha256
                or candidate.normalized_object_relative_path != "normalized.json"
                or candidate.gate_report_relative_path != "gate.json"
            ):
                return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
            if candidate.normalized_object_sha256 != normalized_fp.sha256:
                return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
            normalized = _json(normalized_raw)
            rows = (
                normalized.get("rows", normalized) if isinstance(normalized, dict) else normalized
            )
            if not isinstance(rows, list) or len(rows) != candidate.row_count:
                return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
            for row in rows:
                bar = DailyBar.model_validate(row)
                if (
                    bar.source != "baostock"
                    or bar.trade_date != candidate.trade_date
                    or not bar.symbol.startswith(("sh.", "sz."))
                ):
                    return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
            if self.trade_date is not None and candidate.trade_date != self.trade_date:
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            if self.universe_id is not None and candidate.universe_id != self.universe_id:
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            if (
                manifest.get("manifest_sha256")
                and manifest["manifest_sha256"] != manifest_fp.sha256
            ):
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            manifest_date = manifest.get("trade_date") or manifest.get("as_of")
            if (
                manifest_date is not None
                and str(manifest_date)[:10] != candidate.trade_date.isoformat()
            ):
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            relative, partition_sha, partition_count = self._partition_descriptor(
                manifest, candidate.trade_date
            )
            partition_path = self.dataset_root / relative
            _, partition_fp = _read_verified(partition_path)
            if partition_fp.sha256 != partition_sha:
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            if partition_count != candidate.row_count:
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            self._validate_partition(partition_path, candidate.trade_date, partition_count)
            evidence = self._read_evidence(candidate.evidence_id)
            evidence_manifest = evidence.manifest
            if (
                evidence_manifest.provider_id.value != "baostock"
                or evidence_manifest.evidence_id != candidate.evidence_id
                or evidence_manifest.manifest_sha256 != candidate.evidence_sha256
                or evidence_manifest.trade_date != candidate.trade_date
                or evidence_manifest.universe_id != candidate.universe_id
            ):
                return self._unavailable(UnavailableReason.EVIDENCE_INCOMPLETE)
            lineage = {
                "provider_id": candidate.provider_id.value,
                "universe_id": candidate.universe_id,
                "evidence_id": candidate.evidence_id,
                "evidence_sha256": candidate.evidence_sha256,
                "candidate_id": candidate.candidate_id,
                "candidate_manifest_sha256": candidate.manifest_sha256,
                "gate_report_sha256": gate.aggregate_sha256,
                "adapter_version": candidate.adapter_version,
                "source_schema_version": candidate.source_schema_version,
            }
            if (
                manifest.get("publication_lineage") is not None
                and manifest["publication_lineage"] != lineage
            ):
                return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)
            snapshot = CanonicalComparisonSnapshot(
                snapshot_id=_sha(
                    {
                        "manifest": manifest_fp.sha256,
                        "partition": partition_fp.sha256,
                        "candidate": candidate.manifest_sha256,
                    }
                )[:32],
                dataset_manifest_generation=str(manifest["generation"]),
                dataset_manifest_sha256=manifest_fp.sha256,
                trade_date_partition_sha256=partition_fp.sha256,
                trade_date_row_count=partition_count,
                r2f2_publication_lineage=lineage,
                selection_id=selection.selection_id,
                selection_sha256=selection.selection_sha256,
                candidate_sha256=candidate.manifest_sha256,
                evidence_sha256=candidate.evidence_sha256,
                gate_sha256=gate.aggregate_sha256,
                factor_sha256=candidate.factor_resolution_sha256,
                normalized_sha256=candidate.normalized_object_sha256,
                trade_date=candidate.trade_date,
                universe_id=candidate.universe_id,
                version_vector={
                    "adapter_version": candidate.adapter_version,
                    "source_schema_version": candidate.source_schema_version,
                },
            )
            self._paths = (
                self.dataset_root / "manifest.json",
                partition_path,
                candidate_path,
                gate_path,
                selection_path,
                normalized_path,
            )
            self._fingerprints = tuple(_read_verified(path)[1] for path in self._paths)
            self._snapshot = snapshot
            return CanonicalComparisonResult(status="ready", snapshot=snapshot)
        except Exception:
            return self._unavailable(UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED)

    def verify(self) -> CanonicalComparisonResult:
        if self._snapshot is None:
            return self.read()
        try:
            current = tuple(_read_verified(path)[1] for path in self._paths)
        except Exception:
            return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
        if current != self._fingerprints:
            return self._unavailable(UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED)
        return CanonicalComparisonResult(status="ready", snapshot=self._snapshot)


class PublishedCanonicalComparison:
    def __init__(self, reader: CanonicalCandidateReader, snapshot: CanonicalComparisonSnapshot):
        self.reader = reader
        self.snapshot = snapshot
        self.closed = False
        self.verify_before_reconcile = False
        self.verify_after_reconcile = False

    @classmethod
    def open(
        cls, reader: CanonicalCandidateReader
    ) -> PublishedCanonicalComparison | CanonicalComparisonResult:
        result = reader.read()
        if result.status != "ready" or result.snapshot is None:
            return result
        return cls(reader, result.snapshot)

    def verify(self) -> CanonicalComparisonResult:
        if self.closed:
            return CanonicalComparisonResult(
                status="unavailable",
                unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
            )
        result = self.reader.verify()
        if result.status == "ready":
            if not self.verify_before_reconcile:
                self.verify_before_reconcile = True
            else:
                self.verify_after_reconcile = True
        return result

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> PublishedCanonicalComparison:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
