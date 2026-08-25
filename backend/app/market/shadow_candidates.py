"""Complete shadow candidate bundles, isolated from canonical candidate storage."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from datetime import date
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .providers.registry import ShadowRegistry
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


def _read_private(path: Path, *, limit: int = 64 * 1024 * 1024) -> bytes:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ShadowCandidateUnavailable("shadow candidate unavailable") from exc
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size > limit
        ):
            raise ShadowCandidateUnavailable("shadow candidate unavailable")
        payload = os.read(fd, limit + 1)
        after = os.fstat(fd)
        if len(payload) != before.st_size or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_ctime_ns,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_ctime_ns,
            after.st_mtime_ns,
        ):
            raise ShadowCandidateUnavailable("shadow candidate unavailable")
        return payload
    finally:
        os.close(fd)


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _assert_no_symlink_chain(path: Path) -> None:
    """Reject a path whose existing directory chain can be redirected."""
    if not path.is_absolute():
        raise ShadowCandidateUnavailable("shadow candidate root unavailable")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ShadowCandidateUnavailable("shadow candidate root unavailable") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ShadowCandidateUnavailable("shadow candidate root unavailable")


def _rename_noclobber(source: Path, destination: Path) -> None:
    """Atomically publish a directory without replacing an existing bundle."""
    if destination.exists():
        raise FileExistsError(destination)
    # macOS exposes renameatx_np(RENAME_EXCL), which closes the check/rename race.
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        renameatx_np = libc.renameatx_np
        renameatx_np.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameatx_np.restype = ctypes.c_int
        parent_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        source_parent_fd = os.open(source.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            result = renameatx_np(
                source_parent_fd,
                source.name.encode(),
                parent_fd,
                destination.name.encode(),
                0x00000004,
            )
            if result != 0:
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error), destination)
            return
        finally:
            os.close(source_parent_fd)
            os.close(parent_fd)
    except AttributeError:
        # Non-Darwin fallback: the precondition remains no-clobber for supported CI.
        if destination.exists():
            raise FileExistsError(destination) from None
        os.rename(source, destination)


class ShadowQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    quality_report_id: str
    candidate_id: str
    trade_date: date
    universe_id: str
    gate_version: Literal["r2f3-v1"] = "r2f3-v1"
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
        if self.quality_report_id != f"quality-{self.candidate_id}":
            raise ValueError("shadow quality report identity mismatch")
        if tuple(name for name, _ in self.ordered_gate_outcomes) != (
            "coverage",
            "self_quality",
        ):
            raise ValueError("shadow quality gate order mismatch")
        if any(verdict not in {"pass", "fail"} for _, verdict in self.ordered_gate_outcomes):
            raise ValueError("shadow quality gate verdict unavailable")
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
    canonical_manifest_generation: str = Field(min_length=1)
    canonical_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_schema_version: Literal["r2f3-v1"] = "r2f3-v1"
    adapter_version: Literal["r2f3-v1"] = "r2f3-v1"
    row_count: int = Field(ge=0)
    expected_symbol_count: int = Field(ge=0)
    bundle_commit_sha256: str
    status: Literal["accepted"] = "accepted"
    reconciliation_id: str | None = None
    reconciliation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    reconciliation_status: Literal["ready", "material_mismatch", "unavailable"] | None = None
    reconciliation_compared_counts: dict[str, int] | None = None
    reconciliation_mismatch_counts: dict[str, int] | None = None
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
    return _sha(
        {
            "domain": "stock-eva/r2f3/shadow-candidate/v1",
            "normalized_sha256": candidate.normalized_sha256,
            "evidence_id": evidence_id,
            "provider_id": candidate.provider_id,
            "trade_date": candidate.trade_date.isoformat(),
            "universe_id": candidate.universe_id,
        }
    )[:32]


def _registry_binding(
    registry: ShadowRegistry,
    candidate: ShadowNormalizedCandidate,
    evidence_id: str,
    evidence_sha256: str,
) -> dict[str, Any]:
    if type(registry) is not ShadowRegistry:
        raise ShadowCandidateUnavailable("shadow registry unavailable")
    if not all((candidate.job_id, candidate.window_id, candidate.session_id)):
        raise ShadowCandidateUnavailable("shadow candidate identity unavailable")
    try:
        with registry._lock(shared=True):
            connection = registry._connection_for_read()
            try:
                job = connection.execute(
                    "SELECT job_id,provider_id,window_id,trade_date,universe_id,"
                    "canonical_manifest_generation,canonical_manifest_sha256,"
                    "version_vector_sha256,run_status FROM shadow_job "
                    "WHERE job_id=? AND provider_id=? AND window_id=?",
                    (candidate.job_id, candidate.provider_id, candidate.window_id),
                ).fetchone()
                evidence = connection.execute(
                    "SELECT evidence_id,job_id,provider_id,window_id,session_id,"
                    "evidence_sha256,attached_session_report_id FROM shadow_evidence_ref "
                    "WHERE evidence_id=? AND job_id=? AND provider_id=? AND window_id=? "
                    "AND session_id=?",
                    (
                        evidence_id,
                        candidate.job_id,
                        candidate.provider_id,
                        candidate.window_id,
                        candidate.session_id,
                    ),
                ).fetchone()
            finally:
                if not registry._memory:
                    connection.close()
    except Exception as exc:
        raise ShadowCandidateUnavailable("shadow registry unavailable") from exc
    if job is None or evidence is None:
        raise ShadowCandidateUnavailable("shadow registry binding unavailable")
    if job[8] != "pending_normalization" or evidence[6] is not None:
        raise ShadowCandidateUnavailable("shadow job is not evidence-ready")
    if (
        job[1] != candidate.provider_id
        or job[2] != candidate.window_id
        or str(job[3])[:10] != candidate.trade_date.isoformat()
        or job[4] != candidate.universe_id
        or candidate.version_vector_sha256 != job[7]
        or evidence[5] != evidence_sha256
    ):
        raise ShadowCandidateUnavailable("shadow registry binding mismatch")
    return {
        "job_id": job[0],
        "provider_id": job[1],
        "window_id": job[2],
        "trade_date": job[3],
        "universe_id": job[4],
        "canonical_manifest_generation": job[5],
        "canonical_manifest_sha256": job[6],
        "version_vector_sha256": job[7],
        "evidence_id": evidence[0],
        "session_id": evidence[4],
        "evidence_sha256": evidence[5],
    }


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
        reconciliation_report: Any | None = None,
        registry: ShadowRegistry,
    ) -> ShadowCandidateManifest:
        if not self.root.is_absolute():
            raise ShadowCandidateUnavailable("shadow candidate root unavailable")
        _assert_no_symlink_chain(self.root)
        if (
            type(evidence_reader) is not ShadowEvidenceReader
            or type(registry) is not ShadowRegistry
        ):
            raise ShadowCandidateUnavailable("shadow evidence or registry unavailable")
        try:
            evidence, descriptor = evidence_reader.read_descriptor(evidence_id)
        except Exception as exc:
            raise ShadowCandidateUnavailable("shadow evidence unavailable") from exc
        evidence_id_value = getattr(evidence, "evidence_id", None) or getattr(
            getattr(evidence, "manifest", None), "evidence_id", None
        )
        evidence_sha_value = getattr(evidence, "manifest_sha256", None) or getattr(
            getattr(evidence, "manifest", None), "manifest_sha256", None
        )
        if (
            evidence_id_value != evidence_id
            or not evidence_sha_value
            or candidate.trade_date is None
        ):
            raise ShadowCandidateUnavailable("shadow evidence unavailable")
        if not getattr(evidence, "evidence_ready", False):
            raise ShadowCandidateUnavailable("shadow evidence is not ready")
        evidence_manifest = getattr(evidence, "manifest", None)
        evidence_provider = descriptor.get("provider_id") or getattr(
            evidence_manifest, "provider_id", ""
        )
        evidence_provider = getattr(evidence_provider, "value", evidence_provider)
        evidence_date = descriptor.get("trade_date") or getattr(
            evidence_manifest, "trade_date", None
        )
        evidence_universe = descriptor.get("universe_id") or getattr(
            evidence_manifest, "universe_id", ""
        )
        if evidence_provider and evidence_provider != candidate.provider_id:
            raise ShadowCandidateUnavailable("shadow evidence provider mismatch")
        if evidence_date and str(evidence_date)[:10] != candidate.trade_date.isoformat():
            raise ShadowCandidateUnavailable("shadow evidence date mismatch")
        if evidence_universe and evidence_universe != candidate.universe_id:
            raise ShadowCandidateUnavailable("shadow evidence universe mismatch")
        plan_requests = descriptor.get("plan_requests", ())

        def same_date(value: Any) -> bool:
            raw = str(value)
            normalized = raw if "-" in raw else f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            return normalized[:10] == candidate.trade_date.isoformat()

        if plan_requests and any(
            not same_date(item.get("trade_date", "")) for item in plan_requests
        ):
            raise ShadowCandidateUnavailable("shadow evidence request date mismatch")
        registry_binding = _registry_binding(registry, candidate, evidence_id, evidence_sha_value)
        for field in ("job_id", "window_id", "session_id"):
            if descriptor.get(field) != getattr(candidate, field):
                raise ShadowCandidateUnavailable("shadow evidence identity mismatch")
        if (
            descriptor.get("version_vector_sha256", candidate.version_vector_sha256)
            != candidate.version_vector_sha256
        ):
            raise ShadowCandidateUnavailable("shadow evidence version mismatch")
        computed_candidate_id = _candidate_id(candidate, evidence_id)
        if candidate_id is not None and candidate_id != computed_candidate_id:
            raise ShadowCandidateUnavailable("shadow candidate identity mismatch")
        cid = computed_candidate_id
        expected = set(candidate.expected_symbols)
        observed = {row.symbol for row in candidate.rows}
        legal_missing = set(candidate.suspended_symbols) | set(candidate.not_listed_symbols)
        failed = tuple(sorted(expected - observed - legal_missing))
        derived_complete = (
            candidate.complete
            and observed | set(candidate.suspended_symbols) | set(candidate.not_listed_symbols)
            == expected
            and candidate.quality_status == "ready"
            and bool(
                candidate.factor_semantics
                and candidate.factor_anchor
                and candidate.factor_direction
            )
            and all(
                row.factor is not None and row.factor_previous is not None for row in candidate.rows
            )
            and set(candidate.required_index_symbols) == set(candidate.index_symbols)
            and all(row.provider_id == candidate.provider_id for row in candidate.rows)
        )
        expected_quality = ShadowQualityReport(
            quality_report_id=f"quality-{cid}",
            candidate_id=cid,
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            ordered_gate_outcomes=(
                ("coverage", "pass" if derived_complete and not failed else "fail"),
                ("self_quality", "pass" if candidate.quality_status == "ready" else "fail"),
            ),
            expected_symbol_count=len(expected),
            loaded_symbol_count=len(observed),
            suspended_count=sum(row.suspension for row in candidate.rows)
            + len(candidate.suspended_symbols),
            failed_symbols=failed,
            verdict="pass" if derived_complete and not failed else "fail",
            input_evidence_sha256=evidence_sha_value,
        )
        if quality_report is not None and quality_report.model_dump(
            mode="json"
        ) != expected_quality.model_dump(mode="json"):
            raise ShadowCandidateUnavailable("shadow quality report mismatch")
        quality = expected_quality
        if quality.verdict != "pass":
            raise ShadowCandidateUnavailable("shadow quality gate failed")
        reconciliation_values: dict[str, Any] = {}
        if reconciliation_report is not None:
            from .shadow_reconciliation import ShadowReconciliationReport

            if (
                type(reconciliation_report) is not ShadowReconciliationReport
                or getattr(reconciliation_report, "status", None) != "ready"
                or getattr(reconciliation_report, "trade_date", None) != candidate.trade_date
                or getattr(reconciliation_report, "universe_id", None) != candidate.universe_id
                or not getattr(reconciliation_report, "report_sha256", None)
            ):
                raise ShadowCandidateUnavailable("shadow reconciliation is not ready")
            reconciliation_values = {
                "reconciliation_id": reconciliation_report.reconciliation_id,
                "reconciliation_sha256": reconciliation_report.report_sha256,
                "reconciliation_status": reconciliation_report.status,
                "reconciliation_compared_counts": dict(reconciliation_report.compared_counts),
                "reconciliation_mismatch_counts": dict(reconciliation_report.mismatch_counts),
            }
        normalized_bytes = _json(candidate.model_dump(mode="json"))
        quality_bytes = _json(quality.model_dump(mode="json"))
        bundle_rel = f"bundles/{cid}"
        manifest = ShadowCandidateManifest(
            candidate_id=cid,
            provider_id=candidate.provider_id,
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            evidence_id=evidence_id,
            evidence_sha256=evidence_sha_value,
            normalized_object_ref=f"{bundle_rel}/normalized.json",
            normalized_object_sha256=_sha(normalized_bytes),
            quality_report_ref=f"{bundle_rel}/quality.json",
            quality_report_sha256=_sha(quality_bytes),
            canonical_manifest_generation=registry_binding["canonical_manifest_generation"],
            canonical_manifest_sha256=registry_binding["canonical_manifest_sha256"],
            row_count=len(candidate.rows),
            expected_symbol_count=len(expected),
            bundle_commit_sha256="0" * 64,
            **reconciliation_values,
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
            for path in (
                staging / "normalized.json",
                staging / "quality.json",
                staging / "candidate.json",
            ):
                _fsync_file(path)
            (staging / "COMMIT").write_bytes(f"COMMIT\n{commit}\n".encode())
            _fsync_file(staging / "COMMIT")
            dir_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            destination = bundles / cid
            if destination.exists():
                existing = ShadowCandidateReader(
                    self.root, evidence_reader=evidence_reader, registry=registry
                ).read(cid)
                if existing.manifest.manifest_sha256 != manifest.manifest_sha256:
                    raise ShadowCandidateUnavailable("shadow candidate identity collision")
                return existing.manifest
            _rename_noclobber(staging, destination)
            _fsync_dir(bundles)
            verified = ShadowCandidateReader(
                self.root, evidence_reader=evidence_reader, registry=registry
            ).read(cid)
            if verified.manifest.manifest_sha256 != manifest.manifest_sha256:
                raise ShadowCandidateUnavailable("shadow candidate readback mismatch")
        finally:
            if staging.exists():
                for path in staging.iterdir():
                    path.unlink()
                staging.rmdir()
        return manifest

    write = publish

    def attach(
        self,
        *,
        candidate_reader: ShadowCandidateReader,
        candidate_id: str,
        registry: ShadowRegistry,
    ) -> ShadowCandidateManifest:
        return ShadowCandidateAttachmentService().attach(
            candidate_reader=candidate_reader,
            candidate_id=candidate_id,
            registry=registry,
        )


class ShadowCandidateAttachmentService:
    """Attach one published candidate manifest through the registry CAS.

    Bundle publication and this control-plane attachment are deliberately
    separate.  The service never manufactures a candidate identity: it reads
    the committed bundle and uses ``manifest_sha256`` as the frozen registry
    candidate hash.
    """

    def attach(
        self,
        *,
        candidate_reader: ShadowCandidateReader,
        candidate_id: str,
        registry: ShadowRegistry,
    ) -> ShadowCandidateManifest:
        if (
            type(candidate_reader) is not ShadowCandidateReader
            or type(registry) is not ShadowRegistry
        ):
            raise ShadowCandidateUnavailable("shadow candidate attachment unavailable")
        try:
            bundle = candidate_reader.read(candidate_id)
        except Exception as exc:
            raise ShadowCandidateUnavailable("shadow candidate bundle unavailable") from exc
        manifest = bundle.manifest
        candidate = bundle.candidate
        if (
            manifest.candidate_id != candidate_id
            or manifest.reconciliation_status != "ready"
            or not manifest.reconciliation_sha256
            or manifest.manifest_sha256 == "0" * 64
            or candidate.provider_id != manifest.provider_id
            or candidate.trade_date != manifest.trade_date
            or candidate.universe_id != manifest.universe_id
        ):
            raise ShadowCandidateUnavailable("shadow candidate reconciliation unavailable")

        def save(connection):
            job = connection.execute(
                "SELECT job_id,provider_id,window_id,trade_date,universe_id,run_status "
                "FROM shadow_job WHERE job_id=? AND provider_id=? AND window_id=?",
                (candidate.job_id, manifest.provider_id, candidate.window_id),
            ).fetchone()
            evidence = connection.execute(
                "SELECT evidence_id,job_id,provider_id,window_id,session_id,evidence_sha256,"
                "attached_session_report_id FROM shadow_evidence_ref WHERE evidence_id=?",
                (manifest.evidence_id,),
            ).fetchone()
            if job is None or evidence is None:
                raise ShadowCandidateUnavailable("shadow candidate registry binding unavailable")
            if (
                job[0] != candidate.job_id
                or job[1] != manifest.provider_id
                or job[2] != candidate.window_id
                or str(job[3])[:10] != manifest.trade_date.isoformat()
                or job[4] != manifest.universe_id
                or job[5] != "pending_normalization"
                or evidence[1:5]
                != (
                    candidate.job_id,
                    manifest.provider_id,
                    candidate.window_id,
                    candidate.session_id,
                )
                or evidence[5] != manifest.evidence_sha256
                or evidence[6] is not None
            ):
                raise ShadowCandidateUnavailable("shadow candidate registry binding unavailable")
            try:
                connection.execute(
                    "INSERT INTO shadow_candidate_ref "
                    "(candidate_id,evidence_id,job_id,provider_id,window_id,session_id,"
                    "candidate_ref,candidate_sha256,quality_report_ref,quality_report_sha256) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        manifest.candidate_id,
                        manifest.evidence_id,
                        candidate.job_id,
                        manifest.provider_id,
                        candidate.window_id,
                        candidate.session_id,
                        manifest.normalized_object_ref,
                        manifest.manifest_sha256,
                        manifest.quality_report_ref,
                        manifest.quality_report_sha256,
                    ),
                )
            except Exception as exc:
                raise ShadowCandidateUnavailable("shadow candidate attachment conflict") from exc
            return manifest

        return registry._with_transaction(save)


def attach_candidate(
    *, candidate_reader: ShadowCandidateReader, candidate_id: str, registry: ShadowRegistry
) -> ShadowCandidateManifest:
    return ShadowCandidateAttachmentService().attach(
        candidate_reader=candidate_reader, candidate_id=candidate_id, registry=registry
    )


class ShadowCandidateReader:
    def __init__(
        self,
        root: Path | str,
        *,
        evidence_reader: ShadowEvidenceReader,
        registry: ShadowRegistry,
    ):
        self.root = Path(root)
        if (
            type(evidence_reader) is not ShadowEvidenceReader
            or type(registry) is not ShadowRegistry
        ):
            raise ShadowCandidateUnavailable("shadow evidence or registry unavailable")
        self.evidence_reader = evidence_reader
        self.registry = registry

    def read(self, candidate_id: str) -> ShadowCandidateBundle:
        bundle = self.root / "bundles" / candidate_id
        try:
            _assert_no_symlink_chain(self.root)
            _assert_no_symlink_chain(bundle)
            if not bundle.is_dir() or {item.name for item in bundle.iterdir()} != {
                "normalized.json",
                "quality.json",
                "candidate.json",
                "COMMIT",
            }:
                raise ShadowCandidateUnavailable("shadow candidate unavailable")
            if any(item.is_symlink() for item in bundle.iterdir()):
                raise ShadowCandidateUnavailable("shadow candidate unavailable")
            manifest_raw = _read_private(bundle / "candidate.json")
            quality_raw = _read_private(bundle / "quality.json")
            normalized_raw = _read_private(bundle / "normalized.json")
            commit = _read_private(bundle / "COMMIT").decode("utf-8")
            manifest = ShadowCandidateManifest.model_validate(json.loads(manifest_raw))
            quality = ShadowQualityReport.model_validate(json.loads(quality_raw))
            candidate = ShadowNormalizedCandidate.model_validate(json.loads(normalized_raw))
            expected_symbols = set(candidate.expected_symbols)
            observed_symbols = {row.symbol for row in candidate.rows}
            legal_missing = set(candidate.suspended_symbols) | set(candidate.not_listed_symbols)
            failed_symbols = tuple(sorted(expected_symbols - observed_symbols - legal_missing))
            derived_complete = (
                candidate.complete
                and observed_symbols | legal_missing == expected_symbols
                and candidate.quality_status == "ready"
                and bool(
                    candidate.factor_semantics
                    and candidate.factor_anchor
                    and candidate.factor_direction
                )
                and all(
                    row.factor is not None and row.factor_previous is not None
                    for row in candidate.rows
                )
                and all(row.provider_id == candidate.provider_id for row in candidate.rows)
            )
            expected_verdict = "pass" if derived_complete and not failed_symbols else "fail"
            if (
                manifest.candidate_id != candidate_id
                or _candidate_id(candidate, manifest.evidence_id) != candidate_id
                or manifest.normalized_object_sha256 != _sha(normalized_raw)
                or manifest.quality_report_sha256 != _sha(quality_raw)
                or manifest.evidence_id == ""
                or quality.candidate_id != candidate_id
                or manifest.row_count != len(candidate.rows)
                or manifest.expected_symbol_count != len(candidate.expected_symbols)
                or quality.expected_symbol_count != len(candidate.expected_symbols)
                or quality.loaded_symbol_count != len(candidate.rows)
                or quality.suspended_count
                != sum(row.suspension for row in candidate.rows) + len(candidate.suspended_symbols)
                or quality.failed_symbols != failed_symbols
                or quality.ordered_gate_outcomes
                != (
                    ("coverage", "pass" if derived_complete and not failed_symbols else "fail"),
                    ("self_quality", "pass" if candidate.quality_status == "ready" else "fail"),
                )
                or quality.input_evidence_sha256 != manifest.evidence_sha256
                or quality.verdict != expected_verdict
                or commit != f"COMMIT\n{manifest.bundle_commit_sha256}\n"
            ):
                raise ShadowCandidateUnavailable("shadow candidate descriptor mismatch")
            if (
                manifest.provider_id != candidate.provider_id
                or manifest.trade_date != candidate.trade_date
                or manifest.universe_id != candidate.universe_id
            ):
                raise ShadowCandidateUnavailable("shadow candidate identity mismatch")
            try:
                evidence, descriptor = self.evidence_reader.read_descriptor(manifest.evidence_id)
                evidence_sha_value = getattr(evidence, "manifest_sha256", None) or getattr(
                    getattr(evidence, "manifest", None), "manifest_sha256", None
                )
                if (
                    evidence_sha_value != manifest.evidence_sha256
                    or not getattr(evidence, "evidence_ready", False)
                    or descriptor.get("provider_id") != manifest.provider_id
                    or (
                        descriptor.get("trade_date") is not None
                        and str(descriptor["trade_date"])[:10] != manifest.trade_date.isoformat()
                    )
                    or (
                        descriptor.get("universe_id") is not None
                        and descriptor["universe_id"] != manifest.universe_id
                    )
                    or descriptor.get("job_id") != candidate.job_id
                    or descriptor.get("window_id") != candidate.window_id
                    or descriptor.get("session_id") != candidate.session_id
                ):
                    raise ShadowCandidateUnavailable("shadow evidence binding mismatch")
                registry_binding = _registry_binding(
                    self.registry, candidate, manifest.evidence_id, manifest.evidence_sha256
                )
                if (
                    manifest.canonical_manifest_generation
                    != registry_binding["canonical_manifest_generation"]
                    or manifest.canonical_manifest_sha256
                    != registry_binding["canonical_manifest_sha256"]
                ):
                    raise ShadowCandidateUnavailable("shadow registry generation mismatch")
            except ShadowCandidateUnavailable:
                raise
            except Exception as exc:
                raise ShadowCandidateUnavailable("shadow evidence unavailable") from exc
            return ShadowCandidateBundle(
                manifest=manifest, quality_report=quality, candidate=candidate
            )
        except ShadowCandidateUnavailable:
            raise
        except Exception as exc:
            raise ShadowCandidateUnavailable("shadow candidate unavailable") from exc

    read_bundle = read
