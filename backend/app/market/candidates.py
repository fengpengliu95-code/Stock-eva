"""Immutable R2-F2 candidate gate and single-session selection contracts."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from ctypes import CDLL, c_char_p, c_int, c_uint
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from backend.app.market.evidence import (
    EvidenceError,
    PublishedEvidence,
    _factor_value_semantic_hash,
)
from backend.app.market.models import DailyBar
from backend.app.market.providers.base import (
    BoundedText,
    ProviderEndpoint,
    ProviderId,
    SafeFailureClass,
    SafeIdentifier,
    SafeRelativePath,
    SafeSha256,
)


def _canonical(value: Any) -> bytes:
    value = _json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")


def _json_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, StrEnum):
        return value.value
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class _Immutable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class SelectionReason(StrEnum):
    PRIMARY_READY = "primary_ready"
    QUALIFIED_FALLBACK = "qualified_fallback"


class GateName(StrEnum):
    TRANSPORT_COMPLETE = "transport_complete"
    SCHEMA = "schema"
    DATE = "date"
    UNIVERSE = "universe"
    COVERAGE = "coverage"
    SEMANTIC = "semantic"
    FACTOR = "factor"
    SUSPENSION = "suspension"
    EVIDENCE_HASH = "evidence_hash"
    DETERMINISM = "determinism"


R2F2_GATE_ORDER: tuple[GateName, ...] = (
    GateName.TRANSPORT_COMPLETE,
    GateName.SCHEMA,
    GateName.DATE,
    GateName.UNIVERSE,
    GateName.COVERAGE,
    GateName.SEMANTIC,
    GateName.FACTOR,
    GateName.SUSPENSION,
    GateName.EVIDENCE_HASH,
    GateName.DETERMINISM,
)

# This table is intentionally descriptive only.  The persisted report contains
# the ordered outcome tuple, never this legacy/public description.
R2F2_GATE_EVIDENCE: tuple[tuple[GateName, str, str], ...] = (
    (
        R2F2_GATE_ORDER[0],
        "F0.1 TransportObservationProjection",
        "BaoStockProvider._read / provider transport observation collector",
    ),
    (
        R2F2_GATE_ORDER[1],
        "RawEndpointBatch",
        "providers.base exact discriminated endpoint/schema validator",
    ),
    (
        R2F2_GATE_ORDER[2],
        "validate_raw_date_binding(request, batch) + publication identity",
        "R2-F2 adapter/evidence requested-date binding followed by existing publication "
        "requested_date/session identity gate",
    ),
    (
        R2F2_GATE_ORDER[3],
        "_publication_issues",
        "backend.app.market.automation._publication_issues expected/required symbols and indexes",
    ),
    (
        R2F2_GATE_ORDER[4],
        "run_publication_refresh",
        "backend.app.market.automation.run_publication_refresh coverage == 1",
    ),
    (
        R2F2_GATE_ORDER[5],
        "normalize_baostock_rows + publication_bar_quality_issue",
        "existing normalized DailyBar and MarketStore quality contract",
    ),
    (
        R2F2_GATE_ORDER[6],
        "_main_board_factor_snapshot + _publication_issues",
        "AdjustmentFactorCache resolution and missing_adjust_factor gate",
    ),
    (
        R2F2_GATE_ORDER[7],
        "publication_bar_quality_issue",
        "MarketStore.publication_bar_quality_issue suspended placeholder/index rules",
    ),
    (
        R2F2_GATE_ORDER[8],
        "EvidenceReader",
        "descriptor-bound schema, row-count and SHA-256 readback",
    ),
    (
        R2F2_GATE_ORDER[9],
        "EvidenceStore.replay",
        "frozen normalization clock and byte/semantic replay comparison",
    ),
)


class GateOutcome(_Immutable):
    gate_name: GateName
    gate_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    verdict: Literal["pass", "fail"]
    bounded_metrics: tuple[tuple[str, int | float | bool | BoundedText | None], ...] = ()
    failure_class: SafeFailureClass | None = None
    referenced_hashes: tuple[SafeSha256, ...] = ()


class CandidateGateReport(_Immutable):
    gate_report_id: SafeIdentifier
    candidate_id: SafeIdentifier
    gate_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    outcomes: tuple[GateOutcome, ...]
    verdict: Literal["pass", "fail"]
    aggregate_sha256: SafeSha256
    created_at: datetime

    @model_validator(mode="after")
    def validate_aggregate(self) -> CandidateGateReport:
        if self.created_at.utcoffset() is None:
            raise ValueError("gate report timestamp must be timezone-aware")
        names = tuple(item.gate_name for item in self.outcomes)
        if names != R2F2_GATE_ORDER:
            raise ValueError("gate report must contain the exact ordered gate set")
        if any(item.gate_version != self.gate_version for item in self.outcomes):
            raise ValueError("gate outcome version mismatch")
        expected_verdict = (
            "pass" if all(item.verdict == "pass" for item in self.outcomes) else "fail"
        )
        if self.verdict != expected_verdict:
            raise ValueError("gate report verdict mismatch")
        values = self.model_dump(mode="json")
        values.pop("aggregate_sha256", None)
        if self.aggregate_sha256 != _digest(values):
            raise ValueError("gate aggregate hash mismatch")
        return self


class CandidateManifest(_Immutable):
    candidate_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    provider_id: ProviderId
    evidence_id: SafeIdentifier
    evidence_sha256: SafeSha256
    normalized_object_relative_path: SafeRelativePath
    normalized_object_sha256: SafeSha256
    gate_report_relative_path: SafeRelativePath
    gate_report_sha256: SafeSha256
    factor_resolution_sha256: SafeSha256
    adapter_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    source_schema_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
    row_count: int = Field(ge=0, le=10_000_000)
    required_symbol_count: int = Field(ge=1, le=10_000_000)
    status: Literal["accepted", "rejected"]
    manifest_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_identity(self) -> CandidateManifest:
        values = self.model_dump(mode="json")
        values.pop("manifest_sha256", None)
        if self.manifest_sha256 != _digest(values):
            raise ValueError("candidate manifest hash mismatch")
        return self


class SessionSelection(_Immutable):
    selection_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    selected_candidate_id: SafeIdentifier
    selected_provider_id: ProviderId
    reason: SelectionReason
    fallback_from: ProviderId | None
    evidence_sha256: SafeSha256
    candidate_manifest_sha256: SafeSha256
    gate_report_sha256: SafeSha256
    selected_at: datetime
    selection_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_selection(self) -> SessionSelection:
        if self.selected_at.utcoffset() is None:
            raise ValueError("selection timestamp must be timezone-aware")
        if self.reason is not SelectionReason.PRIMARY_READY:
            raise ValueError("qualified fallback is reserved and rejected")
        if self.fallback_from is not None:
            raise ValueError("fallback provider is not allowed")
        values = self.model_dump(mode="json")
        values.pop("selection_sha256", None)
        if self.selection_sha256 != _digest(values):
            raise ValueError("selection hash mismatch")
        return self


def _open_root_path(path: Path) -> int:
    """Open every ancestor without following a symlink."""
    absolute = Path(os.path.abspath(path))
    descriptor = os.open(os.sep, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        for component in absolute.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


class PublishedSelection(_Immutable):
    """A short-lived capability proving an immutable selection bundle readback."""

    selection: SessionSelection
    root_path: str
    bundle_relative_path: SafeRelativePath
    selection_sha256: SafeSha256
    _root_fd: int | None = PrivateAttr(default=None)
    _root_identity: tuple[int, int] | None = PrivateAttr(default=None)
    _selection_bytes: bytes = PrivateAttr(default=b"")

    @classmethod
    def bind(
        cls,
        selection: SessionSelection,
        *,
        root_path: Path,
        bundle_relative_path: str,
        root_fd: int,
        root_identity: tuple[int, int],
        selection_bytes: bytes,
    ) -> PublishedSelection:
        value = cls(
            selection=selection,
            root_path=str(root_path),
            bundle_relative_path=bundle_relative_path,
            selection_sha256=hashlib.sha256(selection_bytes).hexdigest(),
        )
        object.__setattr__(value, "_root_fd", os.dup(root_fd))
        object.__setattr__(value, "_root_identity", root_identity)
        object.__setattr__(value, "_selection_bytes", bytes(selection_bytes))
        return value

    def verify(self) -> PublishedSelection:
        root_fd = self._root_fd
        identity = self._root_identity
        if root_fd is None or identity is None:
            raise EvidenceError("selection capability is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        current = os.fstat(root_fd)
        if (current.st_dev, current.st_ino) != identity:
            raise EvidenceError("candidate root changed", "EVIDENCE_HASH_MISMATCH")
        fresh = _open_root_path(Path(self.root_path))
        try:
            configured = os.fstat(fresh)
            if (configured.st_dev, configured.st_ino) != identity:
                raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH")
        finally:
            os.close(fresh)
        expected = _canonical(self.selection.model_dump(mode="json"))
        if (
            expected != self._selection_bytes
            or hashlib.sha256(expected).hexdigest() != self.selection_sha256
        ):
            raise EvidenceError("selection capability bytes mismatch", "EVIDENCE_HASH_MISMATCH")
        from backend.app.market.evidence import open_evidence_relative

        descriptor = open_evidence_relative(root_fd, f"{self.bundle_relative_path}/selection.json")
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size != len(self._selection_bytes):
                raise EvidenceError("selection bundle is invalid", "EVIDENCE_HASH_MISMATCH")
            payload = os.read(descriptor, info.st_size + 1)
            after = os.fstat(descriptor)
            if (after.st_dev, after.st_ino, after.st_size, after.st_ctime_ns) != (
                info.st_dev,
                info.st_ino,
                info.st_size,
                info.st_ctime_ns,
            ):
                raise EvidenceError(
                    "selection bundle changed during read", "EVIDENCE_HASH_MISMATCH"
                )
        finally:
            os.close(descriptor)
        if payload != self._selection_bytes:
            raise EvidenceError("selection bundle changed", "EVIDENCE_HASH_MISMATCH")
        return self

    def close(self) -> None:
        descriptor = self._root_fd
        if descriptor is not None:
            object.__setattr__(self, "_root_fd", None)
            os.close(descriptor)

    def __enter__(self) -> PublishedSelection:
        return self.verify()

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


class PublishedCandidateRejection(_Immutable):
    """Typed result for a durable rejected candidate audit bundle."""

    status: Literal["rejected"] = "rejected"
    candidate: CandidateManifest
    gate_report: CandidateGateReport
    root_path: str
    bundle_relative_path: SafeRelativePath
    _root_fd: int | None = PrivateAttr(default=None)
    _root_identity: tuple[int, int] | None = PrivateAttr(default=None)

    @classmethod
    def bind(
        cls,
        candidate: CandidateManifest,
        gate_report: CandidateGateReport,
        *,
        root_path: Path,
        bundle_relative_path: str,
        root_fd: int,
        root_identity: tuple[int, int],
    ) -> PublishedCandidateRejection:
        value = cls(
            candidate=candidate,
            gate_report=gate_report,
            root_path=str(root_path),
            bundle_relative_path=bundle_relative_path,
        )
        object.__setattr__(value, "_root_fd", os.dup(root_fd))
        object.__setattr__(value, "_root_identity", root_identity)
        return value

    def verify(self) -> PublishedCandidateRejection:
        if self.candidate.status != "rejected" or self.gate_report.verdict != "fail":
            raise EvidenceError("rejection audit status is invalid", "EVIDENCE_MANIFEST_INVALID")
        root_fd = self._root_fd
        identity = self._root_identity
        if root_fd is None or identity is None:
            raise EvidenceError("rejection audit is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        current = os.fstat(root_fd)
        if (current.st_dev, current.st_ino) != identity:
            raise EvidenceError("candidate root changed", "EVIDENCE_HASH_MISMATCH")
        fresh = _open_root_path(Path(self.root_path))
        try:
            configured = os.fstat(fresh)
            if (configured.st_dev, configured.st_ino) != identity:
                raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH")
        finally:
            os.close(fresh)
        from backend.app.market.evidence import open_evidence_relative

        expected = {
            "gate.json": _canonical(self.gate_report.model_dump(mode="json")),
            "candidate.json": _canonical(self.candidate.model_dump(mode="json")),
        }
        for name, payload in expected.items():
            try:
                CandidateStore(Path(self.root_path))._assert_root(root_fd, identity)
            except OSError as exc:
                raise EvidenceError(
                    "rejection audit root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
                ) from exc
            descriptor = open_evidence_relative(root_fd, f"{self.bundle_relative_path}/{name}")
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode):
                    raise EvidenceError(
                        "rejection audit file is not regular", "EVIDENCE_UNSAFE_PATH"
                    )
                data = os.read(descriptor, info.st_size + 1)
                after = os.fstat(descriptor)
                if (after.st_dev, after.st_ino, after.st_size, after.st_ctime_ns) != (
                    info.st_dev,
                    info.st_ino,
                    info.st_size,
                    info.st_ctime_ns,
                ):
                    raise EvidenceError(
                        "rejection audit changed during read", "EVIDENCE_HASH_MISMATCH"
                    )
            finally:
                os.close(descriptor)
            if data != payload:
                raise EvidenceError("rejection audit changed", "EVIDENCE_HASH_MISMATCH")
            try:
                CandidateStore(Path(self.root_path))._assert_root(root_fd, identity)
            except OSError as exc:
                raise EvidenceError(
                    "rejection audit root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
                ) from exc
        try:
            CandidateStore(Path(self.root_path))._assert_root(root_fd, identity)
        except OSError as exc:
            raise EvidenceError(
                "rejection audit root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
            ) from exc
        normalized = CandidateStore._readback(
            root_fd, f"{self.bundle_relative_path}/normalized.json"
        )
        try:
            CandidateStore(Path(self.root_path))._assert_root(root_fd, identity)
        except OSError as exc:
            raise EvidenceError(
                "rejection audit root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
            ) from exc
        if hashlib.sha256(normalized).hexdigest() != self.candidate.normalized_object_sha256:
            raise EvidenceError("rejection normalized object changed", "EVIDENCE_HASH_MISMATCH")
        return self

    def close(self) -> None:
        descriptor = self._root_fd
        if descriptor is not None:
            object.__setattr__(self, "_root_fd", None)
            os.close(descriptor)

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


class CandidateStore:
    """Small immutable JSON projection store under the evidence root."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    @contextmanager
    def _bound_root(self):
        try:
            root_fd = _open_root_path(self.root)
        except OSError as exc:
            raise EvidenceError(
                "candidate root is unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
            ) from exc
        identity = os.fstat(root_fd)
        expected = (identity.st_dev, identity.st_ino)
        try:
            self._assert_root(root_fd, expected)
            yield root_fd, expected
        finally:
            os.close(root_fd)

    def _assert_root(self, root_fd: int, expected: tuple[int, int]) -> None:
        current = os.fstat(root_fd)
        if (current.st_dev, current.st_ino) != expected:
            raise EvidenceError("candidate root changed during publish", "EVIDENCE_HASH_MISMATCH")
        try:
            fresh = _open_root_path(self.root)
        except OSError as exc:
            raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH") from exc
        try:
            configured = os.fstat(fresh)
            if (configured.st_dev, configured.st_ino) != expected:
                raise EvidenceError("candidate root was replaced", "EVIDENCE_HASH_MISMATCH")
        finally:
            os.close(fresh)

    @staticmethod
    def _parts(relative: str) -> tuple[str, ...]:
        parts = tuple(relative.split("/"))
        if not parts or any(not part or part in {".", ".."} for part in parts):
            raise EvidenceError("unsafe candidate path", "EVIDENCE_UNSAFE_PATH")
        return parts

    @staticmethod
    def _mkdir_open(parent: int, component: str) -> int:
        try:
            return os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except FileNotFoundError:
            try:
                os.mkdir(component, 0o700, dir_fd=parent)
                return os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent,
                )
            except OSError as exc:
                raise EvidenceError(
                    "candidate artifact directory is unsafe", "EVIDENCE_UNSAFE_PATH"
                ) from exc
        except OSError as exc:
            raise EvidenceError(
                "candidate artifact directory is unsafe", "EVIDENCE_UNSAFE_PATH"
            ) from exc

    def _write_once(
        self,
        relative: str,
        payload: bytes,
        *,
        root_fd: int,
        root_identity: tuple[int, int],
    ) -> Path:
        self._assert_root(root_fd, root_identity)
        parts = self._parts(relative)
        parent = os.dup(root_fd)
        created: tuple[int, str] | None = None
        try:
            for component in parts[:-1]:
                child = self._mkdir_open(parent, component)
                os.close(parent)
                parent = child
            name = parts[-1]
            try:
                existing = os.open(
                    name,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent,
                )
            except FileNotFoundError:
                existing = None
            if existing is not None:
                try:
                    data = os.read(existing, len(payload) + 1)
                    if data != payload:
                        raise ValueError("immutable candidate artifact collision")
                finally:
                    os.close(existing)
            else:
                staging = self._mkdir_open(root_fd, "staging")
                try:
                    temp_name = f"candidate-{os.getpid()}-{id(payload)}.partial"
                    temp = os.open(
                        temp_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                        0o600,
                        dir_fd=staging,
                    )
                    created = (staging, temp_name)
                    try:
                        os.write(temp, payload)
                        os.fsync(temp)
                    finally:
                        os.close(temp)
                    try:
                        os.link(
                            temp_name,
                            name,
                            src_dir_fd=staging,
                            dst_dir_fd=parent,
                            follow_symlinks=False,
                        )
                        os.fsync(parent)
                    except FileExistsError:
                        existing = os.open(
                            name,
                            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=parent,
                        )
                        try:
                            if os.read(existing, len(payload) + 1) != payload:
                                raise ValueError("immutable candidate artifact collision")
                        finally:
                            os.close(existing)
                    os.unlink(temp_name, dir_fd=staging)
                    created = None
                finally:
                    os.close(staging)
            self._assert_root(root_fd, root_identity)
            path = self.root / relative
            readback = self._readback(root_fd, relative)
            if readback != payload:
                raise EvidenceError(
                    "candidate artifact readback mismatch", "EVIDENCE_HASH_MISMATCH"
                )
            return path
        except BaseException:
            if created is not None:
                try:
                    os.unlink(created[1], dir_fd=created[0])
                except OSError:
                    pass
            raise
        finally:
            os.close(parent)

    @staticmethod
    def _write_staged(parent: int, name: str, payload: bytes) -> None:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent,
        )
        try:
            written = os.write(descriptor, payload)
            if written != len(payload):
                raise EvidenceError("candidate artifact write was short", "EVIDENCE_HASH_MISMATCH")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            if os.read(descriptor, len(payload) + 1) != payload:
                raise EvidenceError(
                    "candidate artifact readback mismatch", "EVIDENCE_HASH_MISMATCH"
                )
        finally:
            os.close(descriptor)

    @staticmethod
    def _rename_exclusive(
        source_parent: int,
        source: str,
        destination_parent: int,
        destination: str,
    ) -> None:
        try:
            libc = CDLL(None, use_errno=True)
            renameatx_np = libc.renameatx_np
        except AttributeError as exc:
            raise EvidenceError(
                "atomic bundle publication is unavailable", "EVIDENCE_WRITE_FAILED"
            ) from exc
        renameatx_np.argtypes = [c_int, c_char_p, c_int, c_char_p, c_uint]
        renameatx_np.restype = c_int
        if (
            renameatx_np(
                source_parent,
                source.encode(),
                destination_parent,
                destination.encode(),
                0x00000004,
            )
            != 0
        ):
            error = OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
            if error.errno == getattr(os, "EEXIST", 17):
                raise FileExistsError(destination)
            raise error

    @staticmethod
    def _remove_owned_stage(parent: int, name: str) -> None:
        try:
            stage = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except OSError:
            return
        try:
            for child in ("normalized.json", "gate.json", "candidate.json", "selection.json"):
                try:
                    os.unlink(child, dir_fd=stage)
                except FileNotFoundError:
                    pass
            os.rmdir(name, dir_fd=parent)
        finally:
            os.close(stage)

    @staticmethod
    def _remove_owned_bundle(
        parent: int,
        name: str,
        expected: tuple[int, int],
    ) -> None:
        try:
            bundle = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except OSError:
            return
        try:
            identity = os.fstat(bundle)
            if (identity.st_dev, identity.st_ino) != expected:
                return
            for child in ("normalized.json", "gate.json", "candidate.json", "selection.json"):
                try:
                    os.unlink(child, dir_fd=bundle)
                except FileNotFoundError:
                    pass
            os.fsync(bundle)
            os.rmdir(name, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(bundle)

    def _quarantine_replaced_bundle(
        self,
        candidate_id: str,
        expected_root: tuple[int, int],
    ) -> None:
        try:
            root_fd = _open_root_path(self.root)
        except OSError:
            return
        try:
            current = os.fstat(root_fd)
            if (current.st_dev, current.st_ino) == expected_root:
                return
            bundles = self._mkdir_open(root_fd, "bundles")
            audit = self._mkdir_open(root_fd, "orphan-audit")
            try:
                try:
                    bundle = os.open(
                        candidate_id,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                        dir_fd=bundles,
                    )
                except OSError:
                    return
                else:
                    os.close(bundle)
                name = f"rejected-bundle-{candidate_id}-{uuid4().hex}"
                os.rename(candidate_id, name, src_dir_fd=bundles, dst_dir_fd=audit)
                os.fsync(bundles)
                os.fsync(audit)
            finally:
                os.close(audit)
                os.close(bundles)
        finally:
            os.close(root_fd)

    @staticmethod
    def _readback(root_fd: int, relative: str) -> bytes:
        from backend.app.market.evidence import open_evidence_relative

        descriptor = open_evidence_relative(root_fd, relative)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise EvidenceError("candidate artifact is not regular", "EVIDENCE_UNSAFE_PATH")
            data = os.read(descriptor, info.st_size + 1)
            after = os.fstat(descriptor)
            if (after.st_dev, after.st_ino, after.st_size, after.st_ctime_ns) != (
                info.st_dev,
                info.st_ino,
                info.st_size,
                info.st_ctime_ns,
            ):
                raise EvidenceError(
                    "candidate artifact changed during read", "EVIDENCE_HASH_MISMATCH"
                )
            return data
        finally:
            os.close(descriptor)

    @staticmethod
    def _payload(model: BaseModel) -> bytes:
        return _canonical(model.model_dump(mode="json"))

    def publish_gate_report(self, report: CandidateGateReport) -> Path:
        with self._bound_root() as (fd, identity):
            return self._write_once(
                f"gates/{report.gate_report_id}.json",
                self._payload(report),
                root_fd=fd,
                root_identity=identity,
            )

    def publish_candidate(self, candidate: CandidateManifest) -> Path:
        with self._bound_root() as (fd, identity):
            return self._write_once(
                f"candidates/{candidate.candidate_id}.json",
                self._payload(candidate),
                root_fd=fd,
                root_identity=identity,
            )

    def publish_chain(
        self,
        *,
        report: CandidateGateReport,
        candidate: CandidateManifest,
        selection: SessionSelection | None,
        evidence: PublishedEvidence,
        normalized_payload: bytes,
    ) -> PublishedSelection | PublishedCandidateRejection:
        """Publish one immutable candidate bundle and return its verification capability."""
        validate_candidate_lineage(candidate, evidence, report)
        if not normalized_payload:
            raise ValueError("normalized candidate object is mandatory")
        normalized_sha = hashlib.sha256(normalized_payload).hexdigest()
        if normalized_sha != candidate.normalized_object_sha256:
            raise ValueError("normalized candidate object hash mismatch")
        try:
            decoded = json.loads(normalized_payload.decode("utf-8"))
            if not isinstance(decoded, list) or (not decoded and candidate.status == "accepted"):
                raise ValueError("normalized candidate object must contain rows")
            rows = tuple(DailyBar.model_validate(item) for item in decoded)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError("normalized candidate object is invalid") from exc
        _validate_approved_plan(evidence)
        durable_symbols, _durable_roles = _durable_universe(evidence)
        if (
            len(rows) != candidate.row_count
            or len(durable_symbols) != candidate.required_symbol_count
        ):
            raise ValueError("normalized candidate object cardinality mismatch")
        if any(row.trade_date != candidate.trade_date for row in rows):
            raise ValueError("normalized candidate object date mismatch")
        bundle = f"bundles/{candidate.candidate_id}"
        if candidate.normalized_object_relative_path != f"{bundle}/normalized.json":
            raise ValueError("normalized candidate path is not descriptor-bound")
        if candidate.gate_report_relative_path != f"{bundle}/gate.json":
            raise ValueError("gate report path is not descriptor-bound")
        if candidate.source_schema_version != "daily_astock.v1":
            raise ValueError("normalized candidate schema is unsupported")
        recomputed = evaluate_candidate_gates(
            candidate_id=candidate.candidate_id,
            trade_date=candidate.trade_date,
            required_symbols=durable_symbols,
            rows=rows,
            evidence=evidence,
            factor_resolution=evidence.manifest.factor_resolution,
            created_at=report.created_at,
            gate_version=report.gate_version,
        )
        if report.model_dump(mode="json") != recomputed.model_dump(mode="json"):
            raise ValueError("caller gate report does not match durable readback facts")
        expected_status = "accepted" if recomputed.verdict == "pass" else "rejected"
        if candidate.status != expected_status:
            raise ValueError("candidate status does not match recomputed gates")
        if candidate.status == "accepted":
            if selection is None:
                raise ValueError("accepted candidate requires a selection")
            if selection.selected_candidate_id != candidate.candidate_id:
                raise ValueError("selection candidate lineage mismatch")
            if selection.candidate_manifest_sha256 != candidate.manifest_sha256:
                raise ValueError("selection candidate hash mismatch")
            if selection.gate_report_sha256 != report.aggregate_sha256:
                raise ValueError("selection gate hash mismatch")
            if selection.evidence_sha256 != candidate.evidence_sha256:
                raise ValueError("selection evidence hash mismatch")
        with self._bound_root() as (fd, identity):
            bundle_payloads = {
                "normalized.json": normalized_payload,
                "gate.json": self._payload(report),
                "candidate.json": self._payload(candidate),
            }
            if candidate.status == "accepted":
                bundle_payloads["selection.json"] = self._payload(selection)
            staging = self._mkdir_open(fd, "staging")
            bundles = self._mkdir_open(fd, "bundles")
            stage_name = f"candidate-bundle-{uuid4().hex}"
            stage = None
            created_bundle = False
            completed = False
            bundle_identity = None
            try:
                os.mkdir(stage_name, 0o700, dir_fd=staging)
                stage = os.open(
                    stage_name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=staging,
                )
                for name, payload in bundle_payloads.items():
                    self._write_staged(stage, name, payload)
                os.fsync(stage)
                os.fsync(staging)
                try:
                    self._rename_exclusive(staging, stage_name, bundles, candidate.candidate_id)
                    created_bundle = True
                    bundle_fd = os.open(
                        candidate.candidate_id,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                        dir_fd=bundles,
                    )
                    try:
                        metadata = os.fstat(bundle_fd)
                        bundle_identity = (metadata.st_dev, metadata.st_ino)
                    finally:
                        os.close(bundle_fd)
                except FileExistsError:
                    existing = {
                        name: self._readback(fd, f"{bundle}/{name}") for name in bundle_payloads
                    }
                    if any(existing[name] != payload for name, payload in bundle_payloads.items()):
                        raise ValueError("immutable candidate bundle collision") from None
                self._assert_root(fd, identity)
                if candidate.status != "accepted":
                    rejection = PublishedCandidateRejection.bind(
                        candidate,
                        report,
                        root_path=self.root,
                        bundle_relative_path=bundle,
                        root_fd=fd,
                        root_identity=identity,
                    )
                    rejection.verify()
                    completed = True
                    return rejection
                selection_bytes = bundle_payloads["selection.json"]
                assert selection is not None
                capability = PublishedSelection.bind(
                    selection,
                    root_path=self.root,
                    bundle_relative_path=bundle,
                    root_fd=fd,
                    root_identity=identity,
                    selection_bytes=selection_bytes,
                )
                capability.verify()
                completed = True
                return capability
            except BaseException:
                if created_bundle and bundle_identity is not None and not completed:
                    self._remove_owned_bundle(bundles, candidate.candidate_id, bundle_identity)
                    self._quarantine_replaced_bundle(candidate.candidate_id, identity)
                raise
            finally:
                if stage is not None:
                    os.close(stage)
                if not created_bundle:
                    self._remove_owned_stage(staging, stage_name)
                os.close(bundles)
                os.close(staging)


def build_gate_report(
    *,
    candidate_id: str,
    outcomes: tuple[GateOutcome, ...],
    created_at: datetime,
    gate_version: str = "r2f2-gates.v1",
) -> CandidateGateReport:
    verdict = "pass" if all(item.verdict == "pass" for item in outcomes) else "fail"
    probe = CandidateGateReport.model_construct(
        gate_report_id=f"gate-{_digest((candidate_id, outcomes))[:24]}",
        candidate_id=candidate_id,
        gate_version=gate_version,
        outcomes=outcomes,
        verdict=verdict,
        aggregate_sha256="0" * 64,
        created_at=created_at,
    )
    values = probe.model_dump(mode="json")
    values["aggregate_sha256"] = _digest(
        {key: value for key, value in values.items() if key != "aggregate_sha256"}
    )
    return CandidateGateReport.model_validate(values)


def build_candidate_manifest(**values: Any) -> CandidateManifest:
    values = dict(values)
    if "provider_id" in values:
        values["provider_id"] = ProviderId(values["provider_id"])
    values.setdefault("candidate_id", f"candidate-{_digest(values)[:24]}")
    values.setdefault("status", "accepted")
    probe = CandidateManifest.model_construct(**values, manifest_sha256="0" * 64)
    normalized = probe.model_dump(mode="json")
    normalized.pop("manifest_sha256", None)
    normalized["manifest_sha256"] = _digest(normalized)
    return CandidateManifest.model_validate(normalized)


def _as_candidate(value: CandidateManifest | dict[str, Any]) -> CandidateManifest:
    return (
        value if isinstance(value, CandidateManifest) else CandidateManifest.model_validate(value)
    )


def _validate_factor_resolution(evidence: PublishedEvidence) -> None:
    """Recheck factor snapshot identity at the canonical publication seam."""
    manifest = evidence.manifest
    if evidence.reader_identity != _digest(manifest.model_dump(mode="json")):
        raise ValueError("published evidence reader identity mismatch")
    bindings = tuple(manifest.factor_resolution)
    expected_resolution_hash = (
        _digest([item.model_dump(mode="json") for item in bindings]) if bindings else _digest([])
    )
    if manifest.factor_resolution_sha256 != expected_resolution_hash:
        raise ValueError("factor resolution aggregate hash mismatch")
    snapshot = manifest.factor_cache_snapshot
    factor_rows: dict[str, dict[str, Any]] = {}
    if snapshot is not None:
        if snapshot.reader_identity != _digest(snapshot.manifest.model_dump(mode="json")):
            raise ValueError("factor snapshot reader identity mismatch")
    for binding in bindings:
        if binding.selected_kind == "factor_cache_snapshot":
            if snapshot is None or binding.cache is None or binding.live is not None:
                raise ValueError("factor binding source is not mutually exclusive")
            if (
                binding.cache.cache_object_id != snapshot.manifest.object_id
                or binding.cache.cache_object_sha256 != snapshot.manifest.object_sha256
                or binding.cache.record_key != f"{binding.symbol}.{binding.trade_date}"
            ):
                raise ValueError("factor binding is not descriptor-bound")
        else:
            if binding.live is None or binding.cache is not None:
                raise ValueError("factor live/cache source is not mutually exclusive")
            if binding.live.row_key != f"{binding.symbol}.{binding.trade_date}":
                raise ValueError("factor live binding key mismatch")
        if binding.plan_ordinal >= manifest.logical_request_plan.request_count:
            raise ValueError("factor binding plan ordinal is unbound")
        logical = manifest.logical_request_plan.requests[binding.plan_ordinal]
        expected_endpoint = (
            ProviderEndpoint.DAILY_FACTOR
            if binding.selected_kind == "factor_cache_snapshot"
            else binding.live.endpoint
        )
        if (
            logical.endpoint is not expected_endpoint
            or binding.symbol not in logical.symbols
            or binding.trade_date != manifest.trade_date
        ):
            raise ValueError("factor binding is outside the logical request")
    if bindings:
        try:
            for page in evidence.read_rows():
                for row in page.get("factor_rows", ()):
                    fields = tuple(page.get("factor_fields", ()))
                    item = dict(zip(fields, row, strict=True))
                    if "code" in item:
                        factor_rows[f"{item['code']}.{item['dividOperateDate']}"] = {
                            "symbol": item["code"],
                            "trade_date": item["dividOperateDate"],
                            "back_adjust_factor": item["backAdjustFactor"],
                        }
        except (EvidenceError, TypeError, ValueError) as exc:
            raise ValueError("factor snapshot readback failed") from exc
        if len(factor_rows) != len(bindings):
            raise ValueError("factor snapshot cardinality mismatch")
        for binding in bindings:
            key = f"{binding.symbol}.{binding.trade_date}"
            row = factor_rows.get(key)
            if row is None or binding.selected_value_semantic_hash != _factor_value_semantic_hash(
                row
            ):
                raise ValueError("factor selected value identity mismatch")


def validate_candidate_lineage(
    candidate: CandidateManifest,
    evidence: PublishedEvidence,
    gate_report: CandidateGateReport,
) -> None:
    """Validate the complete candidate -> evidence/gate identity join."""
    if not isinstance(evidence, PublishedEvidence):
        raise ValueError("candidate lineage requires bound PublishedEvidence")
    try:
        evidence.read_rows()
    except EvidenceError as exc:
        raise ValueError("published evidence readback failed") from exc
    _validate_factor_resolution(evidence)
    manifest = evidence.manifest
    expected = {
        "evidence_id": manifest.evidence_id,
        "evidence_sha256": manifest.manifest_sha256,
        "trade_date": manifest.trade_date,
        "universe_id": manifest.universe_id,
        "provider_id": manifest.provider_id,
        "adapter_version": manifest.adapter_version,
        "factor_resolution_sha256": manifest.factor_resolution_sha256,
    }
    for field, value in expected.items():
        if getattr(candidate, field) != value:
            raise ValueError(f"candidate {field} lineage mismatch")
    if gate_report.candidate_id != candidate.candidate_id:
        raise ValueError("candidate gate report identity mismatch")
    if gate_report.aggregate_sha256 != candidate.gate_report_sha256:
        raise ValueError("candidate gate report hash mismatch")
    if any(manifest.manifest_sha256 not in item.referenced_hashes for item in gate_report.outcomes):
        raise ValueError("candidate gate report is not bound to published evidence")
    if candidate.status == "accepted" and gate_report.verdict != "pass":
        raise ValueError("accepted candidate requires a passing gate report")


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(name, default)
    return getattr(row, name, default)


def _durable_universe(
    evidence: PublishedEvidence,
    evidence_pages: tuple[dict[str, Any], ...] | None = None,
) -> tuple[tuple[str, ...], dict[str, str]]:
    """Rebuild the session universe from the held raw pages.

    The logical plan is only an expectation.  Publication must also prove that
    the provider's all-stock and session pages agree with that expectation and
    with the factor sidecar.  This deliberately consumes ``evidence_pages``
    supplied by the already-bound reader, so an evaluator cannot reopen a
    mutable root or substitute a second tree between these checks.
    """
    _validate_approved_plan(evidence)
    pages = evidence_pages if evidence_pages is not None else evidence.read_rows()
    raw_descriptors = tuple(
        item for item in evidence.manifest.objects if item.object_kind.value == "raw_endpoint_page"
    )
    if len(pages) != len(raw_descriptors):
        raise ValueError("durable universe raw page cardinality mismatch")

    by_endpoint: dict[ProviderEndpoint, list[dict[str, Any]]] = {}
    for descriptor, page in zip(raw_descriptors, pages, strict=True):
        if tuple(page.get("fields", ())) != tuple(descriptor.fields):
            raise ValueError("durable universe descriptor fields mismatch")
        fields = tuple(page.get("fields", ()))
        decoded: list[dict[str, Any]] = []
        for raw_row in page.get("rows", ()):
            if len(raw_row) != len(fields):
                raise ValueError("durable universe raw row shape mismatch")
            decoded.append(dict(zip(fields, raw_row, strict=True)))
        if descriptor.endpoint is None:
            raise ValueError("durable universe raw endpoint is unbound")
        by_endpoint.setdefault(descriptor.endpoint, []).extend(decoded)

    def symbol(value: Any) -> str:
        value = str(value or "").strip().lower()
        if len(value) != 9 or value[:3] not in {"sh.", "sz."} or not value[3:].isdigit():
            raise ValueError("durable universe contains an invalid symbol")
        return value

    def unique_codes(rows: list[dict[str, Any]]) -> tuple[str, ...]:
        values = tuple(symbol(row.get("code")) for row in rows)
        if len(values) != len(set(values)):
            raise ValueError("durable universe contains duplicate symbols")
        return values

    all_stock = unique_codes(by_endpoint.get(ProviderEndpoint.ALL_STOCK, []))
    requests = evidence.manifest.logical_request_plan.requests
    plan_stock = tuple(requests[1].symbols)
    if tuple(all_stock) != plan_stock:
        raise ValueError("provider all-stock universe does not match logical plan")

    daily_rows = by_endpoint.get(ProviderEndpoint.DAILY_ASTOCK, [])
    daily_stock = unique_codes(daily_rows)
    if tuple(daily_stock) != plan_stock:
        raise ValueError("daily stock universe does not match all-stock universe")
    if any(str(row.get("date")) != evidence.manifest.trade_date.isoformat() for row in daily_rows):
        raise ValueError("daily stock row date does not match session")

    factor_rows = by_endpoint.get(ProviderEndpoint.DAILY_FACTOR, [])
    factor_symbols = unique_codes(factor_rows)
    if tuple(factor_symbols) != plan_stock:
        raise ValueError("daily factor universe does not match stock universe")
    if any(
        str(row.get("dividOperateDate")) != evidence.manifest.trade_date.isoformat()
        for row in factor_rows
    ):
        raise ValueError("daily factor row date does not match session")
    resolved_symbols = tuple(symbol(item.symbol) for item in evidence.manifest.factor_resolution)
    if len(resolved_symbols) != len(set(resolved_symbols)) or tuple(resolved_symbols) != plan_stock:
        raise ValueError("factor resolution universe does not match stock universe")

    index_rows = by_endpoint.get(ProviderEndpoint.INDEX_HISTORY, [])
    index_symbols = unique_codes(index_rows)
    if index_symbols != ("sh.000001", "sz.399001"):
        raise ValueError("index universe is not the approved pair")
    if any(str(row.get("date")) != evidence.manifest.trade_date.isoformat() for row in index_rows):
        raise ValueError("index row date does not match session")

    symbols = {item: "stock" for item in plan_stock}
    symbols.update({item: "index" for item in index_symbols})
    return tuple(sorted(symbols)), symbols


def _validate_approved_plan(evidence: PublishedEvidence) -> None:
    requests = evidence.manifest.logical_request_plan.requests
    if len(requests) != 5:
        raise ValueError("logical request plan must contain exactly five requests")
    expected = (
        (ProviderEndpoint.ALL_STOCK, "universe", "stock", "all_stock.market.v1", ()),
        (ProviderEndpoint.DAILY_ASTOCK, "daily_stock", "stock", "daily_astock.v1", None),
        (ProviderEndpoint.DAILY_FACTOR, "daily_factor", "stock", "daily_factor.v1", None),
        (
            ProviderEndpoint.INDEX_HISTORY,
            "index_history",
            "index",
            "index_history.session.v1",
            ("sh.000001",),
        ),
        (
            ProviderEndpoint.INDEX_HISTORY,
            "index_history",
            "index",
            "index_history.session.v1",
            ("sz.399001",),
        ),
    )
    stock_symbols = requests[1].symbols
    if not stock_symbols:
        raise ValueError("logical request plan daily stock symbols are empty")
    for ordinal, (request, shape) in enumerate(zip(requests, expected, strict=True)):
        endpoint, request_role, role, schema, symbols = shape
        if request.plan_ordinal != ordinal or request.endpoint is not endpoint:
            raise ValueError("logical request plan ordinal or endpoint is invalid")
        if request.schema_variant != schema:
            raise ValueError("logical request plan schema is invalid")
        if request.request_role.value != request_role:
            raise ValueError("logical request plan request role is invalid")
        if request.instrument_role is None or request.instrument_role.value != role:
            raise ValueError("logical request plan role is invalid")
        if (
            request.start_date != request.end_date
            or request.start_date != evidence.manifest.trade_date
        ):
            raise ValueError("logical request plan date is invalid")
        if symbols is not None and request.symbols != symbols:
            raise ValueError("logical request plan symbol cardinality is invalid")
    if requests[0].symbols or requests[1].instrument_role.value != "stock":
        raise ValueError("logical request plan universe shape is invalid")
    if requests[2].symbols != stock_symbols:
        raise ValueError("logical request plan factor symbols do not match daily stock")
    if len(stock_symbols) != len(set(stock_symbols)):
        raise ValueError("logical request plan has duplicate stock symbols")


def evaluate_candidate_gates(
    *,
    candidate_id: str,
    trade_date: date,
    required_symbols: tuple[str, ...],
    rows: tuple[Any, ...],
    evidence: PublishedEvidence,
    factor_resolution: tuple[Any, ...] | None = None,
    created_at: datetime,
    gate_version: str = "r2f2-gates.v1",
) -> CandidateGateReport:
    """Evaluate the ten gates from typed/readback inputs in their fixed order."""
    if not isinstance(evidence, PublishedEvidence):
        raise ValueError("candidate gates require bound PublishedEvidence")
    try:
        evidence_pages = evidence.read_rows()
        reader = evidence._reader
        if reader is None or reader._held_root_fd is None or reader._held_root_identity is None:
            raise EvidenceError("published evidence has no reader", "EVIDENCE_MANIFEST_INVALID")
        reader._assert_root_identity(reader._held_root_fd, reader._held_root_identity)
    except EvidenceError as exc:
        raise ValueError("candidate evidence readback failed") from exc
    if (
        factor_resolution is not None
        and tuple(factor_resolution) != evidence.manifest.factor_resolution
    ):
        raise ValueError("candidate factor resolution is not the published snapshot")
    published_resolution = evidence.manifest.factor_resolution
    symbols = tuple(_row_value(row, "symbol", _row_value(row, "code")) for row in rows)
    unique_symbols = set(symbols)
    durable_symbols, durable_roles = _durable_universe(evidence, evidence_pages)
    date_ok = all(
        _row_value(row, "trade_date", _row_value(row, "date")) == trade_date for row in rows
    )
    input_universe_ok = tuple(sorted(set(required_symbols))) == durable_symbols
    coverage_ok = len(rows) == len(durable_symbols) and unique_symbols == set(durable_symbols)
    semantic_ok = bool(rows)
    suspension_ok = bool(rows)
    for row in rows:
        security_type = _row_value(row, "security_type", "stock")
        expected_role = durable_roles.get(_row_value(row, "symbol"))
        if expected_role is None or expected_role != security_type:
            semantic_ok = False
        suspended = bool(_row_value(row, "is_suspended", False))
        if not suspended:
            if _row_value(row, "adjust_factor") is None and security_type == "stock":
                semantic_ok = False
        elif security_type != "stock":
            suspension_ok = False
        elif any(float(_row_value(row, field, 0) or 0) != 0 for field in ("volume", "amount")):
            suspension_ok = False
    evidence_manifest = evidence.manifest
    evidence_ok = bool(evidence_manifest.manifest_sha256) and bool(evidence.reader_identity)
    completions = tuple(evidence_manifest.request_completions)
    transport_ok = bool(evidence_ok and completions) and all(
        str(getattr(getattr(item, "final_outcome", None), "value", "")) == "success"
        for item in completions
    )
    factor_keys = {
        (getattr(item, "symbol", None), getattr(item, "trade_date", None))
        for item in published_resolution
    }
    factor_ok = all(
        (_row_value(row, "symbol"), trade_date) in factor_keys
        for row in rows
        if _row_value(row, "security_type", "stock") == "stock"
        and not bool(_row_value(row, "is_suspended", False))
    )
    raw_descriptors = tuple(
        item for item in evidence.manifest.objects if item.object_kind.value == "raw_endpoint_page"
    )
    descriptor_fields_ok = len(evidence_pages) == len(raw_descriptors) and all(
        tuple(page.get("fields", ())) == tuple(descriptor.fields)
        for page, descriptor in zip(evidence_pages, raw_descriptors, strict=True)
    )
    replay_ok = bool(evidence_pages) and all(
        evidence_manifest.normalization_clock_utc == descriptor.normalization_clock_utc
        for descriptor in evidence.manifest.objects
        if descriptor.object_kind.value == "raw_endpoint_page"
    )
    checks = (
        transport_ok,
        descriptor_fields_ok,
        date_ok,
        input_universe_ok and unique_symbols == set(durable_symbols),
        coverage_ok,
        semantic_ok,
        factor_ok,
        suspension_ok,
        evidence_ok and replay_ok,
        replay_ok,
    )
    outcomes = tuple(
        GateOutcome(
            gate_name=name,
            gate_version=gate_version,
            verdict="pass" if passed else "fail",
            bounded_metrics=(("rows", len(rows)), ("required_symbols", len(durable_symbols))),
            failure_class=None if passed else "semantic",
            referenced_hashes=tuple(
                item for item in (evidence_manifest.manifest_sha256,) if item is not None
            ),
        )
        for name, passed in zip(R2F2_GATE_ORDER, checks, strict=True)
    )
    return build_gate_report(
        candidate_id=candidate_id,
        outcomes=outcomes,
        created_at=created_at,
        gate_version=gate_version,
    )


def select_primary_candidate(
    *,
    trade_date: date,
    universe_id: str,
    candidates: tuple[CandidateManifest | dict[str, Any], ...],
    selected_at: datetime,
    reason: SelectionReason = SelectionReason.PRIMARY_READY,
    gate_report: CandidateGateReport,
    evidence: PublishedEvidence,
) -> SessionSelection:
    if reason is not SelectionReason.PRIMARY_READY:
        raise ValueError("qualified fallback is reserved and rejected")
    if len(candidates) != 1:
        raise ValueError("selection requires exactly one complete candidate")
    candidate = _as_candidate(candidates[0])
    if candidate.status != "accepted":
        raise ValueError("selection requires an accepted candidate")
    if candidate.provider_id is not ProviderId.BAOSTOCK:
        raise ValueError("selection provider must be baostock")
    if candidate.trade_date != trade_date or candidate.universe_id != universe_id:
        raise ValueError("selection scope does not match candidate")
    _validate_approved_plan(evidence)
    _durable_universe(evidence)
    validate_candidate_lineage(candidate, evidence, gate_report)
    values = {
        "selection_id": f"selection-{_digest(candidate.model_dump(mode='json'))[:24]}",
        "trade_date": trade_date,
        "universe_id": universe_id,
        "selected_candidate_id": candidate.candidate_id,
        "selected_provider_id": candidate.provider_id,
        "reason": reason,
        "fallback_from": None,
        "evidence_sha256": candidate.evidence_sha256,
        "candidate_manifest_sha256": candidate.manifest_sha256,
        "gate_report_sha256": candidate.gate_report_sha256,
        "selected_at": selected_at,
    }
    probe = SessionSelection.model_construct(**values, selection_sha256="0" * 64)
    normalized = probe.model_dump(mode="json")
    normalized["selection_sha256"] = _digest(
        {key: value for key, value in normalized.items() if key != "selection_sha256"}
    )
    return SessionSelection.model_validate(normalized)


__all__ = [
    "CandidateGateReport",
    "CandidateManifest",
    "CandidateStore",
    "GateName",
    "GateOutcome",
    "R2F2_GATE_EVIDENCE",
    "R2F2_GATE_ORDER",
    "PublishedSelection",
    "PublishedCandidateRejection",
    "SelectionReason",
    "SessionSelection",
    "build_candidate_manifest",
    "build_gate_report",
    "evaluate_candidate_gates",
    "select_primary_candidate",
    "validate_candidate_lineage",
]
