"""Immutable, local derived snapshots for market-regime results."""

import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.regime.models import MarketRegimeResult

CaptureMode = Literal["after_close", "post_hoc_backfill"]
_SCHEMA = """
CREATE TABLE IF NOT EXISTS regime_snapshots (
    as_of TEXT NOT NULL,
    formula_version TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (as_of, formula_version)
)
"""


class RegimeSnapshotConflict(RuntimeError):
    """A snapshot key already exists with different immutable content."""


class RegimeSnapshotUnavailable(RuntimeError):
    """The dedicated snapshot store cannot be safely read."""


class MarketRegimeSnapshot(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    snapshot_id: str = Field(pattern=r"^regime-snapshot-[0-9a-f]{24}$")
    as_of: date
    formula_version: str
    result_id: str
    data_as_of: date | None
    capture_mode: CaptureMode
    evidence_cutoff_at: datetime
    recorded_at: datetime
    dataset_generation: str
    dataset_identity_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result: MarketRegimeResult

    @model_validator(mode="after")
    def validate_boundaries(self) -> "MarketRegimeSnapshot":
        if self.result.as_of != self.as_of:
            raise ValueError("snapshot result as_of must match snapshot as_of")
        if self.result.formula_version != self.formula_version:
            raise ValueError("snapshot formula version must match result")
        if self.result.result_id != self.result_id:
            raise ValueError("snapshot result id must match result")
        if self.result.data_as_of != self.data_as_of:
            raise ValueError("snapshot data_as_of must match result")
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("snapshot data_as_of must not exceed as_of")
        if self.evidence_cutoff_at.tzinfo is None or self.recorded_at.tzinfo is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        return self


class SnapshotCaptureRequest(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    result: MarketRegimeResult
    capture_mode: CaptureMode
    evidence_cutoff_at: datetime
    dataset_generation: str = Field(min_length=1)
    dataset_identity: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_evidence_cutoff(self) -> "SnapshotCaptureRequest":
        if self.evidence_cutoff_at.tzinfo is None:
            raise ValueError("evidence cutoff must be timezone-aware")
        return self


def _canonical(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(payload: object) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class RegimeSnapshotStore:
    """Writer-explicit store whose reader never creates local state."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def capture(
        self, request: SnapshotCaptureRequest
    ) -> tuple[MarketRegimeSnapshot, bool]:
        existing = self.read_exact(
            request.result.as_of,
            request.result.formula_version,
        )
        if existing is not None:
            proposed = self._build(request, recorded_at=existing.recorded_at)
            if proposed.content_hash != existing.content_hash:
                raise RegimeSnapshotConflict("immutable regime snapshot conflict")
            return existing, False

        snapshot = self._build(request, recorded_at=datetime.now(UTC))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self.path, timeout=0, isolation_level=None)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(_SCHEMA)
            try:
                connection.execute(
                    """
                    INSERT INTO regime_snapshots
                    (as_of, formula_version, snapshot_id, content_hash, payload)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.as_of.isoformat(),
                        snapshot.formula_version,
                        snapshot.snapshot_id,
                        snapshot.content_hash,
                        snapshot.model_dump_json(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                connection.execute("ROLLBACK")
                existing = self.read_exact(request.result.as_of, request.result.formula_version)
                if existing is not None and self._build(
                    request, recorded_at=existing.recorded_at
                ).content_hash == existing.content_hash:
                    return existing, False
                raise RegimeSnapshotConflict("immutable regime snapshot conflict") from exc
            connection.execute("COMMIT")
            return snapshot, True
        except RegimeSnapshotConflict:
            raise
        except (sqlite3.Error, OSError, ValueError) as exc:
            if connection is not None and connection.in_transaction:
                connection.execute("ROLLBACK")
            raise RegimeSnapshotUnavailable("regime snapshot store unavailable") from exc
        finally:
            if connection is not None:
                connection.close()

    def read_exact(
        self, as_of: date, formula_version: str
    ) -> MarketRegimeSnapshot | None:
        if not self.path.is_file():
            return None
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                f"file:{self.path}?mode=ro",
                uri=True,
                timeout=0,
            )
            row = connection.execute(
                """
                SELECT payload, content_hash
                FROM regime_snapshots
                WHERE as_of = ? AND formula_version = ?
                """,
                (as_of.isoformat(), formula_version),
            ).fetchone()
            if row is None:
                return None
            payload, content_hash = row
            snapshot = MarketRegimeSnapshot.model_validate_json(payload)
            if (
                snapshot.content_hash != content_hash
                or self._content_hash(snapshot) != content_hash
            ):
                raise RegimeSnapshotUnavailable("regime snapshot content hash mismatch")
            return snapshot
        except RegimeSnapshotUnavailable:
            raise
        except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
            raise RegimeSnapshotUnavailable("regime snapshot store unavailable") from exc
        finally:
            if connection is not None:
                connection.close()

    def _build(
        self,
        request: SnapshotCaptureRequest,
        *,
        recorded_at: datetime,
    ) -> MarketRegimeSnapshot:
        result = request.result
        identity_hash = hashlib.sha256(request.dataset_identity.encode("utf-8")).hexdigest()
        semantic = {
            "as_of": result.as_of.isoformat(),
            "formula_version": result.formula_version,
            "result_id": result.result_id,
            "data_as_of": (
                None if result.data_as_of is None else result.data_as_of.isoformat()
            ),
            "capture_mode": request.capture_mode,
            "evidence_cutoff_at": _utc(request.evidence_cutoff_at).isoformat(),
            "dataset_generation": request.dataset_generation,
            "dataset_identity_hash": identity_hash,
            "result": result.model_dump(mode="json"),
        }
        snapshot_id = f"regime-snapshot-{_hash(semantic)[:24]}"
        payload = {
            "snapshot_id": snapshot_id,
            **semantic,
            "recorded_at": _utc(recorded_at).isoformat(),
        }
        snapshot = MarketRegimeSnapshot(
            **payload,
            content_hash="0" * 64,
        )
        return snapshot.model_copy(update={"content_hash": self._content_hash(snapshot)})

    @staticmethod
    def _content_hash(snapshot: MarketRegimeSnapshot) -> str:
        return _hash(snapshot.model_dump(mode="json", exclude={"content_hash"}))
