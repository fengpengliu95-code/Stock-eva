"""Immutable, local derived snapshots for market-regime results."""

import hashlib
import json
import re
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.models import RefreshResult
from backend.app.regime.models import MarketRegimeResult
from backend.app.regime.service import MarketRegimeService
from backend.app.regime.store import MarketReadUnavailable, MarketRegimeStore
from backend.app.storage.dataset import PublishedReadSnapshot

CaptureMode = Literal["after_close", "post_hoc_backfill"]
DATASET_GENERATION_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS regime_snapshots (
    snapshot_id TEXT NOT NULL PRIMARY KEY,
    as_of TEXT NOT NULL,
    formula_version TEXT NOT NULL,
    result_id TEXT NOT NULL,
    data_as_of TEXT,
    capture_mode TEXT NOT NULL,
    evidence_cutoff_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    dataset_generation TEXT NOT NULL,
    dataset_identity_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    result_payload TEXT NOT NULL,
    UNIQUE (as_of, formula_version)
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
    dataset_generation: str = Field(pattern=DATASET_GENERATION_PATTERN)
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
        _validate_result_dates(self.result, self.as_of)
        if self.evidence_cutoff_at.tzinfo is None or self.recorded_at.tzinfo is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        return self


class SnapshotCaptureRequest(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    result: MarketRegimeResult
    capture_mode: CaptureMode
    evidence_cutoff_at: datetime
    dataset_generation: str = Field(pattern=DATASET_GENERATION_PATTERN)
    dataset_identity: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_evidence_cutoff(self) -> "SnapshotCaptureRequest":
        if self.evidence_cutoff_at.tzinfo is None:
            raise ValueError("evidence cutoff must be timezone-aware")
        _validate_result_dates(self.result, self.result.as_of)
        return self


class SnapshotBackfillPlan(BaseModel):
    start: date
    end: date
    selected_dates: tuple[date, ...]
    dataset_generation: str = Field(pattern=DATASET_GENERATION_PATTERN)


class SnapshotBackfillOutcome(SnapshotBackfillPlan):
    inserted: int = 0
    idempotent: int = 0
    conflicts: int = 0
    errors: int = 0


def _canonical(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(payload: object) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _validate_result_dates(result: MarketRegimeResult, as_of: date) -> None:
    lineages = list(result.source_lineage)
    evidence = [*result.supporting_evidence, *result.contrary_evidence]
    for component in result.component_scores:
        lineages.extend(component.source_lineage)
        evidence.extend(component.supporting_evidence)
        evidence.extend(component.contrary_evidence)
    if any(item.earliest_input_date > as_of or item.latest_input_date > as_of for item in lineages):
        raise ValueError("snapshot result lineage must not exceed as_of")
    if any(item.as_of > as_of for item in evidence):
        raise ValueError("snapshot result evidence must not exceed as_of")


class RegimeSnapshotStore:
    """Writer-explicit store whose reader never creates local state."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def capture(self, request: SnapshotCaptureRequest) -> tuple[MarketRegimeSnapshot, bool]:
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
                    (snapshot_id, as_of, formula_version, result_id, data_as_of, capture_mode,
                     evidence_cutoff_at, recorded_at, dataset_generation, dataset_identity_hash,
                     content_hash, result_payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(self._stored_fields(snapshot).values()),
                )
            except sqlite3.IntegrityError as exc:
                connection.execute("ROLLBACK")
                existing = self.read_exact(request.result.as_of, request.result.formula_version)
                if (
                    existing is not None
                    and self._build(request, recorded_at=existing.recorded_at).content_hash
                    == existing.content_hash
                ):
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

    def read_exact(self, as_of: date, formula_version: str) -> MarketRegimeSnapshot | None:
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
                SELECT snapshot_id, as_of, formula_version, result_id, data_as_of, capture_mode,
                       evidence_cutoff_at, recorded_at, dataset_generation, dataset_identity_hash,
                       content_hash, result_payload
                FROM regime_snapshots
                WHERE as_of = ? AND formula_version = ?
                """,
                (as_of.isoformat(), formula_version),
            ).fetchone()
            if row is None:
                return None
            (
                snapshot_id,
                stored_as_of,
                stored_formula_version,
                result_id,
                data_as_of,
                capture_mode,
                evidence_cutoff_at,
                recorded_at,
                dataset_generation,
                dataset_identity_hash,
                content_hash,
                result_payload,
            ) = row
            snapshot = MarketRegimeSnapshot.model_validate(
                {
                    "snapshot_id": snapshot_id,
                    "as_of": stored_as_of,
                    "formula_version": stored_formula_version,
                    "result_id": result_id,
                    "data_as_of": data_as_of,
                    "capture_mode": capture_mode,
                    "evidence_cutoff_at": evidence_cutoff_at,
                    "recorded_at": recorded_at,
                    "dataset_generation": dataset_generation,
                    "dataset_identity_hash": dataset_identity_hash,
                    "content_hash": content_hash,
                    "result": json.loads(result_payload),
                }
            )
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
            "data_as_of": (None if result.data_as_of is None else result.data_as_of.isoformat()),
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
        fields = RegimeSnapshotStore._stored_fields(snapshot)
        fields.pop("content_hash")
        return _hash(fields)

    @staticmethod
    def _stored_fields(snapshot: MarketRegimeSnapshot) -> dict[str, str | None]:
        return {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of.isoformat(),
            "formula_version": snapshot.formula_version,
            "result_id": snapshot.result_id,
            "data_as_of": (
                None if snapshot.data_as_of is None else snapshot.data_as_of.isoformat()
            ),
            "capture_mode": snapshot.capture_mode,
            "evidence_cutoff_at": snapshot.evidence_cutoff_at.isoformat(),
            "recorded_at": snapshot.recorded_at.isoformat(),
            "dataset_generation": snapshot.dataset_generation,
            "dataset_identity_hash": snapshot.dataset_identity_hash,
            "content_hash": snapshot.content_hash,
            "result_payload": _canonical(snapshot.result.model_dump(mode="json")),
        }


class RegimeSnapshotCaptureService:
    """Capture backfills from one strict immutable market publication."""

    def __init__(
        self,
        *,
        regime_store: MarketRegimeStore,
        snapshot_store: RegimeSnapshotStore,
        service: MarketRegimeService | None = None,
    ) -> None:
        self.regime_store = regime_store
        self.snapshot_store = snapshot_store
        self.service = service or MarketRegimeService()

    def plan(self, start: date, end: date) -> SnapshotBackfillPlan:
        snapshot = self._capture_publication()
        return self._plan(snapshot, start, end)

    def capture_range(
        self,
        start: date,
        end: date,
        *,
        evidence_cutoff_at: datetime,
    ) -> SnapshotBackfillOutcome:
        snapshot = self._capture_publication()
        plan = self._plan(snapshot, start, end)
        inserted = idempotent = conflicts = errors = 0
        for as_of in plan.selected_dates:
            try:
                result = self.service.evaluate(self.regime_store.read_bound(as_of, snapshot))
                _, created = self.snapshot_store.capture(
                    SnapshotCaptureRequest(
                        result=result,
                        capture_mode="post_hoc_backfill",
                        evidence_cutoff_at=evidence_cutoff_at,
                        dataset_generation=snapshot.generation,
                        dataset_identity=snapshot.identity,
                    )
                )
                if created:
                    inserted += 1
                else:
                    idempotent += 1
            except RegimeSnapshotConflict:
                conflicts += 1
            except (MarketReadUnavailable, RegimeSnapshotUnavailable, ValueError):
                errors += 1
        return SnapshotBackfillOutcome(
            **plan.model_dump(),
            inserted=inserted,
            idempotent=idempotent,
            conflicts=conflicts,
            errors=errors,
        )

    def capture_after_close(self, result: RefreshResult) -> MarketRegimeSnapshot:
        if result.status != "ready":
            raise RegimeSnapshotUnavailable("published date is unavailable")
        snapshot = self._capture_publication()
        if result.requested_date not in snapshot.trade_dates:
            raise RegimeSnapshotUnavailable("published date is unavailable")
        regime_result = self.service.evaluate(
            self.regime_store.read_bound(result.requested_date, snapshot)
        )
        captured, _ = self.snapshot_store.capture(
            SnapshotCaptureRequest(
                result=regime_result,
                capture_mode="after_close",
                evidence_cutoff_at=result.completed_at,
                dataset_generation=snapshot.generation,
                dataset_identity=snapshot.identity,
            )
        )
        return captured

    def _capture_publication(self) -> PublishedReadSnapshot:
        capture = getattr(self.regime_store.reader, "cache_snapshot", None)
        if not callable(capture):
            raise RegimeSnapshotUnavailable("verified publication is unavailable")
        try:
            snapshot = capture()
        except RuntimeError as exc:
            raise RegimeSnapshotUnavailable("verified publication is unavailable") from exc
        if not isinstance(snapshot, PublishedReadSnapshot):
            raise RegimeSnapshotUnavailable("verified publication is unavailable")
        if (
            not isinstance(snapshot.generation, str)
            or re.fullmatch(DATASET_GENERATION_PATTERN, snapshot.generation) is None
            or not snapshot.trade_dates
        ):
            raise RegimeSnapshotUnavailable("verified publication is unavailable")
        if tuple(sorted(set(snapshot.trade_dates))) != snapshot.trade_dates:
            raise RegimeSnapshotUnavailable("verified publication is unavailable")
        return snapshot

    @staticmethod
    def _plan(
        snapshot: PublishedReadSnapshot,
        start: date,
        end: date,
    ) -> SnapshotBackfillPlan:
        if end < start:
            raise ValueError("snapshot date range is inverted")
        dates = tuple(item for item in snapshot.trade_dates if start <= item <= end)
        if not dates:
            raise ValueError("snapshot date range has no verified dates")
        return SnapshotBackfillPlan(
            start=start,
            end=end,
            selected_dates=dates,
            dataset_generation=snapshot.generation,
        )
