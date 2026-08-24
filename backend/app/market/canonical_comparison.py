"""Read-only binding of a shadow comparison to one published canonical snapshot."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .candidates import CandidateManifest, SessionSelection


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
    version_vector: dict[str, str] = {}
    snapshot_sha256: str = "0" * 64

    def _preimage(self) -> dict[str, Any]:
        values = self.model_dump(mode="json")
        values.pop("snapshot_sha256", None)
        return values

    def model_post_init(self, __context: Any) -> None:
        expected = _sha(self._preimage())
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
    inode: int
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


def _read(path: Path) -> tuple[bytes, _Fingerprint]:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise FileNotFoundError from exc
    try:
        before = os.fstat(fd)
        if not os.path.isfile(path) or before.st_nlink != 1 or before.st_mode & 0o022:
            raise OSError
        payload = os.read(fd, before.st_size + 1)
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or len(payload) != before.st_size:
            raise OSError
        return payload, _Fingerprint(
            path=str(path),
            sha256=_sha(payload),
            size=before.st_size,
            inode=before.st_ino,
            mtime_ns=before.st_mtime_ns,
        )
    finally:
        os.close(fd)


class CanonicalCandidateReader:
    """Capture and verify canonical descriptors without creating or mutating anything."""

    def __init__(
        self,
        dataset_root: Path | str,
        candidate_root: Path | str | None = None,
        evidence_root: Path | str | None = None,
        selection_root: Path | str | None = None,
        *,
        trade_date: date | None = None,
        universe_id: str | None = None,
    ):
        self.dataset_root = Path(dataset_root)
        self.candidate_root = (
            Path(candidate_root) if candidate_root is not None else self.dataset_root
        )
        self.evidence_root = Path(evidence_root) if evidence_root is not None else self.dataset_root
        self.selection_root = (
            Path(selection_root) if selection_root is not None else self.dataset_root
        )
        self.trade_date = trade_date
        self.universe_id = universe_id
        self._fingerprints: tuple[_Fingerprint, ...] = ()
        self._bound_paths: tuple[Path, ...] = ()

    def _candidate_bundle(self, candidate_id: str) -> Path:
        direct = self.candidate_root / "bundles" / candidate_id
        return direct if direct.is_dir() else self.candidate_root / candidate_id

    def _capture(self) -> tuple[_Fingerprint, ...]:
        paths = self._bound_paths or (
            self.dataset_root / "manifest.json",
            self.candidate_root / "candidate.json",
            self.candidate_root / "gate.json",
            self.selection_root / "selection.json",
            self.candidate_root / "normalized.json",
        )
        result = []
        for path in paths:
            _, fingerprint = _read(path)
            result.append(fingerprint)
        return tuple(result)

    def read(self) -> CanonicalComparisonResult:
        try:
            manifest_raw, manifest_fp = _read(self.dataset_root / "manifest.json")
            manifest = json.loads(manifest_raw)
            if not isinstance(manifest, dict) or not manifest.get("generation"):
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
                )
            candidate_path = self.candidate_root / "candidate.json"
            if not candidate_path.exists():
                # Canonical roots commonly store candidate files below a selection-named bundle.
                selection_hint = self.selection_root / "selection.json"
                if selection_hint.exists():
                    selection_hint_data = json.loads(selection_hint.read_bytes())
                    candidate_path = (
                        self._candidate_bundle(selection_hint_data.get("selected_candidate_id", ""))
                        / "candidate.json"
                    )
            candidate_raw, candidate_fp = _read(candidate_path)
            candidate = json.loads(candidate_raw)
            bundle = candidate_path.parent
            gate_raw, gate_fp = _read(bundle / "gate.json")
            normalized_raw, normalized_fp = _read(bundle / "normalized.json")
            selection_path = self.selection_root / "selection.json"
            if not selection_path.exists():
                selection_path = bundle / "selection.json"
            selection_raw, selection_fp = _read(selection_path)
            selection = json.loads(selection_raw)
            json.loads(gate_raw)
            json.loads(normalized_raw)
            if candidate.get("status") != "accepted" or selection.get(
                "selected_candidate_id"
            ) != candidate.get("candidate_id"):
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.SELECTION_BINDING_INVALID,
                )
            candidate_model = CandidateManifest.model_validate(candidate)
            selection_model = SessionSelection.model_validate(selection)
            if (
                candidate.get("gate_report_sha256")
                and candidate.get("gate_report_sha256") != gate_fp.sha256
            ):
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED,
                )
            if (
                candidate.get("normalized_object_sha256")
                and candidate.get("normalized_object_sha256") != normalized_fp.sha256
            ):
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED,
                )
            evidence_sha = str(candidate.get("evidence_sha256", ""))
            trade = date.fromisoformat(str(candidate["trade_date"]))
            if self.trade_date is not None and trade != self.trade_date:
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
                )
            if self.universe_id is not None and candidate.get("universe_id") != self.universe_id:
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
                )
            manifest_trade_date = manifest.get("trade_date") or manifest.get("as_of")
            if (
                manifest_trade_date is not None
                and str(manifest_trade_date)[:10] != trade.isoformat()
            ):
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
                )
            declared_manifest_sha = manifest.get("manifest_sha256")
            if declared_manifest_sha and declared_manifest_sha != manifest_fp.sha256:
                return CanonicalComparisonResult(
                    status="unavailable",
                    unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
                )
            lineage = {
                key: candidate.get(key)
                for key in (
                    "provider_id",
                    "universe_id",
                    "evidence_id",
                    "evidence_sha256",
                    "candidate_id",
                    "manifest_sha256",
                    "gate_report_sha256",
                    "adapter_version",
                    "source_schema_version",
                )
            }
            snapshot = CanonicalComparisonSnapshot(
                snapshot_id=_sha(
                    {
                        "manifest": manifest_fp.sha256,
                        "candidate": candidate_fp.sha256,
                        "selection": selection_fp.sha256,
                    }
                )[:32],
                dataset_manifest_generation=str(manifest["generation"]),
                dataset_manifest_sha256=manifest_fp.sha256,
                trade_date_partition_sha256=str(
                    manifest.get("trade_date_partition_sha256")
                    or manifest.get("partition_sha256")
                    or _sha(normalized_raw)
                ),
                trade_date_row_count=int(
                    manifest.get("trade_date_row_count")
                    or manifest.get("partition_row_count")
                    or candidate.get("row_count", 0)
                ),
                r2f2_publication_lineage=lineage,
                selection_id=str(selection.get("selection_id", "")),
                selection_sha256=selection_model.selection_sha256,
                candidate_sha256=candidate_model.manifest_sha256,
                evidence_sha256=evidence_sha,
                gate_sha256=gate_fp.sha256,
                factor_sha256=str(candidate.get("factor_resolution_sha256", "")),
                normalized_sha256=candidate_model.normalized_object_sha256,
                trade_date=trade,
                universe_id=str(candidate.get("universe_id", "")),
                version_vector={
                    "adapter_version": str(candidate.get("adapter_version", "")),
                    "source_schema_version": str(candidate.get("source_schema_version", "")),
                },
            )
            self._bound_paths = (
                self.dataset_root / "manifest.json",
                candidate_path,
                bundle / "gate.json",
                selection_path,
                bundle / "normalized.json",
            )
            self._fingerprints = tuple(_read(path)[1] for path in self._bound_paths)
            return CanonicalComparisonResult(status="ready", snapshot=snapshot)
        except (OSError, FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return CanonicalComparisonResult(
                status="unavailable",
                unavailable_reason=UnavailableReason.CANONICAL_DESCRIPTOR_CHANGED,
            )

    def verify(self) -> CanonicalComparisonResult:
        result = self.read()
        if result.status != "ready":
            return result
        try:
            fresh = self._capture()
        except (OSError, FileNotFoundError):
            return CanonicalComparisonResult(
                status="unavailable",
                unavailable_reason=UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED,
            )
        if self._fingerprints and fresh != self._fingerprints:
            return CanonicalComparisonResult(
                status="unavailable",
                unavailable_reason=UnavailableReason.CANDIDATE_DESCRIPTOR_CHANGED,
            )
        return result


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
        capability = cls(reader, result.snapshot)
        capability.verify()
        return capability

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
        self.verify()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
