from datetime import date, datetime
from pathlib import Path

import duckdb

from backend.app.config import Settings
from backend.app.fund_flow.models import (
    MARKET_UPSTREAM_SCOPE_IDENTITY,
    EvidenceScope,
    FundFlowEvidenceSnapshot,
)
from backend.app.market.supplement_ingestion import (
    PINNED_PROVIDER_VERSION,
    SupplementalDatasetError,
    SupplementalStore,
    _fund_flow_from_row,
    _sha256,
    supplemental_dataset_root,
)


class FundFlowEvidenceStore:
    """SELECT-only adapter over the existing immutable supplemental manifest."""

    def __init__(
        self,
        *,
        supplemental_store: SupplementalStore,
        enabled: bool,
    ) -> None:
        self.supplemental_store = supplemental_store
        self.enabled = enabled

    @classmethod
    def from_settings(cls, settings: Settings) -> "FundFlowEvidenceStore":
        return cls(
            supplemental_store=SupplementalStore(
                supplemental_dataset_root(settings),
                settings.local_control_dir / settings.supplemental_audit_database_name,
                staging_root=settings.local_staging_dir / "supplemental",
                lock_path=settings.local_lock_dir / "supplemental-ingestion.lock",
            ),
            enabled=settings.akshare_supplemental_enabled,
        )

    def read(
        self,
        *,
        as_of: date,
        scope: EvidenceScope,
        scope_id: str,
    ) -> FundFlowEvidenceSnapshot:
        if not self.enabled:
            return self._empty_snapshot(
                source_status="not_configured",
                as_of=as_of,
                scope=scope,
                scope_id=scope_id,
                quality_issues=["akshare_supplemental_disabled"],
            )

        try:
            manifest = self.supplemental_store._manifest(allow_missing=True)
            files = manifest["files"]
            if not files:
                return self._empty_snapshot(
                    source_status="empty",
                    as_of=as_of,
                    scope=scope,
                    scope_id=scope_id,
                    quality_issues=["supplemental_not_published"],
                )
            kind = "market_fund_flow" if scope == "market" else "sector_fund_flow"
            upstream_scope_identity = (
                MARKET_UPSTREAM_SCOPE_IDENTITY if scope == "market" else scope_id
            )
            entries = [item for item in files if item["dataset_kind"] == kind]
            if not entries:
                return self._empty_snapshot(
                    source_status="empty",
                    as_of=as_of,
                    scope=scope,
                    scope_id=scope_id,
                    quality_issues=["fund_flow_scope_not_published"],
                )
            if len(entries) != 1:
                raise SupplementalDatasetError(
                    "supplemental manifest has ambiguous fund-flow objects"
                )
            entry = entries[0]
            path = self.supplemental_store.root / str(entry["path"])
            if not path.is_file() or _sha256(path) != entry["sha256"]:
                raise SupplementalDatasetError("published supplemental object is invalid")
            self.supplemental_store._validate_parquet(
                path,
                kind,
                int(entry["row_count"]),
                as_of=date.fromisoformat(str(entry["as_of"])),
            )
            rows, future_point_count = self._query(
                path=path,
                kind=kind,
                source_scope="market" if scope == "market" else "industry",
                upstream_scope_identity=upstream_scope_identity,
                as_of=as_of,
            )
            points = [_fund_flow_from_row(row) for row in rows]
            published_at = self._published_at(manifest)
            publication_dates = {date.fromisoformat(str(item["as_of"])) for item in files}
            issues = ["supplemental_dataset_dates_differ"] if len(publication_dates) > 1 else []
            if not points:
                return FundFlowEvidenceSnapshot(
                    source_status="empty",
                    as_of=as_of,
                    scope=scope,
                    scope_id=scope_id,
                    upstream_scope_identity=upstream_scope_identity,
                    publication_as_of=date.fromisoformat(str(entry["as_of"])),
                    published_at=published_at,
                    provider_contract=entry["provider_contract"],
                    object_sha256=str(entry["sha256"]),
                    future_point_count=future_point_count,
                    quality_issues=sorted(
                        {
                            *issues,
                            "fund_flow_scope_has_no_points_at_or_before_as_of",
                        }
                    ),
                )
            if published_at is None:
                issues.append("publication_timestamp_unavailable")
            return FundFlowEvidenceSnapshot(
                source_status="ready",
                as_of=as_of,
                scope=scope,
                scope_id=scope_id,
                upstream_scope_identity=upstream_scope_identity,
                publication_as_of=date.fromisoformat(str(entry["as_of"])),
                published_at=published_at,
                provider_contract=entry["provider_contract"],
                object_sha256=str(entry["sha256"]),
                points=points,
                future_point_count=future_point_count,
                quality_issues=sorted(set(issues)),
            )
        except (OSError, ValueError, duckdb.Error, SupplementalDatasetError):
            return self._empty_snapshot(
                source_status="error",
                as_of=as_of,
                scope=scope,
                scope_id=scope_id,
                quality_issues=["supplemental_published_snapshot_unavailable"],
            )

    @staticmethod
    def _published_at(manifest: dict[str, object]) -> datetime | None:
        raw = manifest.get("published_at")
        if not isinstance(raw, str):
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    @staticmethod
    def _query(
        *,
        path: Path,
        kind: str,
        source_scope: str,
        upstream_scope_identity: str,
        as_of: date,
    ) -> tuple[list[dict[str, object]], int]:
        connection = duckdb.connect(":memory:")
        try:
            rows = connection.execute(
                """
                SELECT *
                FROM read_parquet(?, hive_partitioning = false)
                WHERE dataset_kind = ?
                  AND scope = ?
                  AND scope_name = ?
                  AND trade_date <= ?
                ORDER BY trade_date, observed_at
                """,
                [str(path), kind, source_scope, upstream_scope_identity, as_of],
            ).fetchall()
            columns = [item[0] for item in connection.description]
            future_point_count = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM read_parquet(?, hive_partitioning = false)
                    WHERE dataset_kind = ?
                      AND scope = ?
                      AND scope_name = ?
                      AND trade_date > ?
                    """,
                    [str(path), kind, source_scope, upstream_scope_identity, as_of],
                ).fetchone()[0]
            )
        finally:
            connection.close()
        return [dict(zip(columns, row, strict=True)) for row in rows], future_point_count

    @staticmethod
    def _empty_snapshot(
        *,
        source_status: str,
        as_of: date,
        scope: EvidenceScope,
        scope_id: str,
        quality_issues: list[str],
    ) -> FundFlowEvidenceSnapshot:
        return FundFlowEvidenceSnapshot(
            source_status=source_status,
            as_of=as_of,
            scope=scope,
            scope_id=scope_id,
            upstream_scope_identity=(
                MARKET_UPSTREAM_SCOPE_IDENTITY if scope == "market" else scope_id
            ),
            provider_contract=PINNED_PROVIDER_VERSION,
            quality_issues=quality_issues,
        )
