"""Complete shadow candidate bundles, isolated from canonical candidate storage."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import date
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .shadow_evidence import ShadowEvidenceReader
from .shadow_normalize import ShadowNormalizedCandidate


class ShadowCandidateUnavailable(RuntimeError):
    """Candidate bundle is missing, incomplete, or not bound to evidence."""


def _json(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _sha(value: bytes | Any) -> str:
    return hashlib.sha256(value if isinstance(value, bytes) else _json(value)).hexdigest()


class ShadowQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    quality_report_id: str
    candidate_id: str
    trade_date: date
    universe_id: str
    gate_version: str = "r2f3-v1"
    ordered_gate_outcomes: tuple[tuple[str, str], ...]
    expected_symbol_count: int = Field(ge=0)
    loaded_symbol_count: int = Field(ge=0)
    suspended_count: int = Field(ge=0)
    failed_symbols: tuple[str, ...] = ()
    verdict: str
    input_evidence_sha256: str
    report_sha256: str = "0" * 64

    @model_validator(mode="after")
    def bind_report_hash(self) -> ShadowQualityReport:
        values = self.model_dump(mode="json")
        values.pop("report_sha256", None)
        expected = _sha(values)
        if self.report_sha256 == "0" * 64:
            object.__setattr__(self, "report_sha256", expected)
        elif self.report_sha256 != expected:
            raise ValueError("shadow quality report hash mismatch")
        return self


class ShadowCandidateManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    provider_id: str
    trade_date: date
    universe_id: str
    evidence_id: str
    evidence_sha256: str
    normalized_object_ref: str
    normalized_object_sha256: str
    quality_report_ref: str
    quality_report_sha256: str
    source_schema_version: str = "r2f3-v1"
    adapter_version: str = "r2f3-v1"
    row_count: int = Field(ge=0)
    expected_symbol_count: int = Field(ge=0)
    bundle_commit_sha256: str
    status: str = "accepted"
    manifest_sha256: str = "0" * 64

    @model_validator(mode="after")
    def bind_manifest_hash(self) -> ShadowCandidateManifest:
        values = self.model_dump(mode="json")
        values.pop("manifest_sha256", None)
        expected = _sha(values)
        if self.manifest_sha256 == "0" * 64:
            object.__setattr__(self, "manifest_sha256", expected)
        elif self.manifest_sha256 != expected:
            raise ValueError("shadow candidate manifest hash mismatch")
        return self


class ShadowCandidateBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest: ShadowCandidateManifest
    quality_report: ShadowQualityReport
    candidate: ShadowNormalizedCandidate


def _candidate_id(candidate: ShadowNormalizedCandidate, evidence_id: str) -> str:
    return _sha({"normalized": candidate.normalized_sha256, "evidence": evidence_id})[:32]


class ShadowCandidateStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def _paths(self) -> tuple[Path, Path]:
        bundles = self.root / "bundles"
        bundles.mkdir(parents=True, exist_ok=True)
        return bundles, self.root / "staging"

    def publish(
        self,
        candidate: ShadowNormalizedCandidate,
        *,
        evidence_reader: ShadowEvidenceReader,
        evidence_id: str,
        quality_report: ShadowQualityReport | None = None,
        candidate_id: str | None = None,
        adapter_version: str = "r2f3-v1",
        source_schema_version: str = "r2f3-v1",
    ) -> ShadowCandidateManifest:
        try:
            evidence = evidence_reader.read(evidence_id)
        except Exception as exc:
            raise ShadowCandidateUnavailable("shadow evidence unavailable") from exc
        if (
            evidence.evidence_id != evidence_id
            or evidence.manifest_sha256 == ""
            or candidate.trade_date is None
        ):
            raise ShadowCandidateUnavailable("shadow evidence unavailable")
        cid = candidate_id or _candidate_id(candidate, evidence_id)
        expected = set(candidate.expected_symbols)
        observed = {row.symbol for row in candidate.rows}
        failed = tuple(sorted(expected - observed))
        verdict = "pass" if candidate.complete and not failed else "fail"
        quality = quality_report or ShadowQualityReport(
            quality_report_id=f"quality-{cid}",
            candidate_id=cid,
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            ordered_gate_outcomes=(
                ("coverage", verdict),
                ("self_quality", "pass" if candidate.quality_status == "ready" else "fail"),
            ),
            expected_symbol_count=len(expected),
            loaded_symbol_count=len(observed),
            suspended_count=sum(row.suspension for row in candidate.rows),
            failed_symbols=failed,
            verdict=verdict,
            input_evidence_sha256=evidence.manifest_sha256,
        )
        if (
            quality.candidate_id != cid
            or quality.input_evidence_sha256 != evidence.manifest_sha256
            or quality.verdict != "pass"
        ):
            raise ShadowCandidateUnavailable("shadow quality gate failed")
        normalized_bytes = _json(candidate.model_dump(mode="json"))
        quality_bytes = _json(quality.model_dump(mode="json"))
        bundle_rel = f"bundles/{cid}"
        manifest = ShadowCandidateManifest(
            candidate_id=cid,
            provider_id=candidate.provider_id,
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            evidence_id=evidence_id,
            evidence_sha256=evidence.manifest_sha256,
            normalized_object_ref=f"{bundle_rel}/normalized.json",
            normalized_object_sha256=_sha(normalized_bytes),
            quality_report_ref=f"{bundle_rel}/quality.json",
            quality_report_sha256=_sha(quality_bytes),
            source_schema_version=source_schema_version,
            adapter_version=adapter_version,
            row_count=len(candidate.rows),
            expected_symbol_count=len(expected),
            bundle_commit_sha256="0" * 64,
        )
        commit_preimage = {
            "manifest": manifest.model_dump(mode="json"),
            "normalized_sha256": _sha(normalized_bytes),
            "quality_sha256": _sha(quality_bytes),
        }
        commit = _sha(commit_preimage)
        manifest = manifest.model_copy(update={"bundle_commit_sha256": commit})
        # Rebind the manifest digest after the commit field is fixed.
        manifest = ShadowCandidateManifest.model_validate(
            {**manifest.model_dump(mode="json"), "manifest_sha256": "0" * 64}
        )
        bundles, staging_root = self._paths()
        staging_root.mkdir(parents=True, exist_ok=True)
        staging = staging_root / secrets.token_hex(12)
        staging.mkdir()
        try:
            (staging / "normalized.json").write_bytes(normalized_bytes)
            (staging / "quality.json").write_bytes(quality_bytes)
            (staging / "candidate.json").write_bytes(_json(manifest.model_dump(mode="json")))
            (staging / "COMMIT").write_bytes(f"COMMIT\n{commit}\n".encode())
            for path in staging.iterdir():
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            destination = bundles / cid
            if destination.exists():
                existing = ShadowCandidateReader(self.root).read(cid)
                if existing.manifest.manifest_sha256 != manifest.manifest_sha256:
                    raise ShadowCandidateUnavailable("shadow candidate identity collision")
                return existing.manifest
            os.rename(staging, destination)
        finally:
            if staging.exists():
                for path in staging.iterdir():
                    path.unlink()
                staging.rmdir()
        return manifest

    write = publish


class ShadowCandidateReader:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def read(self, candidate_id: str) -> ShadowCandidateBundle:
        bundle = self.root / "bundles" / candidate_id
        try:
            if not bundle.is_dir() or {item.name for item in bundle.iterdir()} != {
                "normalized.json",
                "quality.json",
                "candidate.json",
                "COMMIT",
            }:
                raise ShadowCandidateUnavailable("shadow candidate unavailable")
            manifest_raw = (bundle / "candidate.json").read_bytes()
            quality_raw = (bundle / "quality.json").read_bytes()
            normalized_raw = (bundle / "normalized.json").read_bytes()
            commit = (bundle / "COMMIT").read_text()
            manifest = ShadowCandidateManifest.model_validate(json.loads(manifest_raw))
            quality = ShadowQualityReport.model_validate(json.loads(quality_raw))
            candidate = ShadowNormalizedCandidate.model_validate(json.loads(normalized_raw))
            if (
                manifest.candidate_id != candidate_id
                or manifest.normalized_object_sha256 != _sha(normalized_raw)
                or manifest.quality_report_sha256 != _sha(quality_raw)
                or manifest.evidence_id == ""
                or quality.candidate_id != candidate_id
                or commit != f"COMMIT\n{manifest.bundle_commit_sha256}\n"
            ):
                raise ShadowCandidateUnavailable("shadow candidate descriptor mismatch")
            return ShadowCandidateBundle(
                manifest=manifest, quality_report=quality, candidate=candidate
            )
        except ShadowCandidateUnavailable:
            raise
        except Exception as exc:
            raise ShadowCandidateUnavailable("shadow candidate unavailable") from exc

    read_bundle = read
