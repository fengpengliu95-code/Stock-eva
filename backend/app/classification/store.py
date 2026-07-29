import hashlib
import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import TypeVar

import duckdb
from pydantic import BaseModel

from backend.app.classification.models import (
    BOARD_DERIVATION_VERSION,
    CLASSIFICATION_SCHEMA_VERSION,
    ClassificationSnapshot,
    CoverageAudit,
    GenerationSummary,
    IndexComponentRecord,
    MarketScope,
    SectorMembershipRecord,
    SecurityMasterRecord,
)

Record = TypeVar(
    "Record",
    SecurityMasterRecord,
    IndexComponentRecord,
    SectorMembershipRecord,
)


class ClassificationConflictError(RuntimeError):
    """A source snapshot is internally inconsistent and must not publish."""


def eligibility_reason(row: SecurityMasterRecord, as_of: date) -> str | None:
    if row.security_type != "stock":
        return "non_stock"
    if row.exchange not in {"sh", "sz"}:
        return "unsupported_exchange"
    if row.board not in {"main", "chinext", "star"}:
        return "unsupported_board"
    if row.list_date is not None and as_of < row.list_date:
        return "not_listed_yet"
    if row.delist_date is not None and as_of >= row.delist_date:
        return "delisted"
    return None


def actionability_reasons(row: SecurityMasterRecord) -> list[str]:
    reasons = []
    if not row.is_tradable:
        reasons.append("suspended")
    if row.price_available is False:
        reasons.append("no_price")
    elif row.price_available is None:
        reasons.append("price_not_audited")
    return reasons


def market_scope(rows: list[SecurityMasterRecord]) -> MarketScope:
    market_names = {"sh": "SSE", "sz": "SZSE", "bj": "BSE"}
    covered_markets = sorted(
        {market_names.get(row.exchange, row.exchange.upper()) for row in rows}
    )
    covered_boards = sorted({row.board for row in rows})
    price_values = [
        row.price_available
        for row in rows
        if row.security_type == "stock"
        and row.exchange in {"sh", "sz"}
        and row.board in {"main", "chinext", "star"}
    ]
    price_coverage_status = (
        "not_audited"
        if any(value is None for value in price_values)
        else "partial"
        if any(value is False for value in price_values)
        else "verified"
    )
    return MarketScope(
        covered_markets=covered_markets,
        covered_boards=covered_boards,
        eligible_markets=["SSE", "SZSE"],
        eligible_boards=["chinext", "main", "star"],
        excluded_markets=["BSE"],
        excluded_security_types=["b_share", "bond", "etf", "fund", "index", "other"],
        price_coverage_status=price_coverage_status,
        derivation_version=BOARD_DERIVATION_VERSION,
    )


class ClassificationStore:
    def __init__(self, path: Path, *, temp_directory: Path | None = None) -> None:
        self.path = path
        self.temp_directory = temp_directory or path.parent / ".classification-duckdb-tmp"
        self.query_count = 0

    def _connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.path))
        connection.execute("SET temp_directory = ?", [str(self.temp_directory)])
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS classification_generations (
                sequence BIGINT PRIMARY KEY,
                generation_id VARCHAR UNIQUE NOT NULL,
                schema_version VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                source_version VARCHAR NOT NULL,
                source_snapshot_date DATE NOT NULL,
                source_date_semantics VARCHAR NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL,
                row_counts JSON NOT NULL,
                coverage_audits JSON NOT NULL,
                market_scope JSON NOT NULL
            )
            """
        )
        connection.execute(
            """
            ALTER TABLE classification_generations
            ADD COLUMN IF NOT EXISTS source_date_semantics VARCHAR
            DEFAULT 'source_observed'
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS classification_ready_pointer (
                singleton INTEGER PRIMARY KEY,
                sequence BIGINT NOT NULL,
                generation_id VARCHAR NOT NULL,
                CHECK (singleton = 1)
            )
            """
        )
        for table, extra in (
            ("security_master_history", ""),
            ("index_component_history", "index_id VARCHAR,"),
            (
                "sector_membership_history",
                "taxonomy_id VARCHAR, sector_id VARCHAR,",
            ),
        ):
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    generation_sequence BIGINT NOT NULL,
                    generation_id VARCHAR NOT NULL,
                    record_id VARCHAR NOT NULL,
                    security_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    {extra}
                    source VARCHAR NOT NULL,
                    source_version VARCHAR NOT NULL,
                    source_snapshot_date DATE NOT NULL,
                    effective_from DATE,
                    effective_to DATE,
                    observed_at TIMESTAMPTZ NOT NULL,
                    lineage_hash VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL,
                    payload JSON NOT NULL,
                    PRIMARY KEY (generation_id, record_id)
                )
                """
            )
        return connection

    def _execute(self, connection, sql: str, parameters: list[object] | None = None):
        self.query_count += 1
        return connection.execute(sql, parameters or [])

    @staticmethod
    def _content_hash(record: BaseModel) -> str:
        payload = record.model_dump(mode="json")
        payload.pop("observed_at", None)
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()

    @classmethod
    def _deduplicate(
        cls,
        rows: list[Record],
        *,
        key,
        label: str,
    ) -> list[Record]:
        result: dict[tuple, Record] = {}
        for row in rows:
            natural_key = key(row)
            current = result.get(natural_key)
            if current is None:
                result[natural_key] = row
                continue
            if (
                current.lineage_hash != row.lineage_hash
                or cls._content_hash(current) != cls._content_hash(row)
            ):
                raise ClassificationConflictError(f"conflicting {label} in source snapshot")
        return sorted(result.values(), key=lambda row: row.record_id)

    def _normalized(self, snapshot: ClassificationSnapshot) -> ClassificationSnapshot:
        securities = self._deduplicate(
            snapshot.securities,
            key=lambda row: (row.source, row.source_snapshot_date, row.security_id),
            label="security master",
        )
        components = self._deduplicate(
            snapshot.index_components,
            key=lambda row: (
                row.source,
                row.source_snapshot_date,
                row.index_id,
                row.security_id,
            ),
            label="index component",
        )
        memberships = self._deduplicate(
            snapshot.sector_memberships,
            key=lambda row: (
                row.source,
                row.source_snapshot_date,
                row.taxonomy_id,
                row.security_id,
            ),
            label="sector membership",
        )
        security_ids = {row.security_id for row in securities}
        unknown = sorted(
            {
                row.security_id
                for row in [*components, *memberships]
                if row.security_id not in security_ids
            }
        )
        if unknown:
            raise ClassificationConflictError(
                f"unknown security referenced by classification: {unknown[0]}"
            )
        return snapshot.model_copy(
            update={
                "securities": securities,
                "index_components": components,
                "sector_memberships": memberships,
            }
        )

    @staticmethod
    def _generation_id(snapshot: ClassificationSnapshot) -> str:
        payload = {
            "schema_version": CLASSIFICATION_SCHEMA_VERSION,
            "source": snapshot.source,
            "source_version": snapshot.source_version,
            "source_snapshot_date": snapshot.source_snapshot_date.isoformat(),
            "source_date_semantics": snapshot.source_date_semantics,
            "lineages": sorted(
                row.lineage_hash
                for row in [
                    *snapshot.securities,
                    *snapshot.index_components,
                    *snapshot.sector_memberships,
                ]
            ),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"classification-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"

    @staticmethod
    def _coverage_audits(
        snapshot: ClassificationSnapshot,
        generation_id: str,
    ) -> list[CoverageAudit]:
        reasons = {
            row.security_id: eligibility_reason(row, snapshot.source_snapshot_date)
            for row in snapshot.securities
        }
        eligible = {
            row.security_id: row.symbol
            for row in snapshot.securities
            if reasons[row.security_id] is None
        }
        actionability = Counter(
            reason
            for row in snapshot.securities
            if row.security_id in eligible
            for reason in actionability_reasons(row)
        )
        taxonomy_ids = sorted(
            set(snapshot.declared_taxonomies)
            | {row.taxonomy_id for row in snapshot.sector_memberships}
        )
        audits = []
        for taxonomy_id in taxonomy_ids:
            mapped_ids = {
                row.security_id
                for row in snapshot.sector_memberships
                if row.taxonomy_id == taxonomy_id
            }
            unmapped = sorted(
                symbol for security_id, symbol in eligible.items() if security_id not in mapped_ids
            )
            mapped_count = len(eligible) - len(unmapped)
            ratio = mapped_count / len(eligible) if eligible else None
            issues = []
            status = "ready"
            if ratio is None:
                status = "degraded"
                issues.append("no_eligible_securities")
            elif ratio < 0.95:
                status = "degraded"
                issues.append("classification_coverage_below_target")
            audits.append(
                CoverageAudit(
                    status=status,
                    as_of=snapshot.source_snapshot_date,
                    generation_id=generation_id,
                    taxonomy_id=taxonomy_id,
                    eligible_count=len(eligible),
                    mapped_count=mapped_count,
                    coverage_ratio=ratio,
                    unmapped_symbols=unmapped,
                    exclusion_reasons=dict(
                        sorted(Counter(reason for reason in reasons.values() if reason).items())
                    ),
                    actionability_reasons=dict(sorted(actionability.items())),
                    source_lineage=[f"{snapshot.source}@{snapshot.source_version}"],
                    quality_issues=issues,
                )
            )
        return audits

    def _assert_no_published_conflicts(
        self,
        connection,
        table: str,
        rows: list[Record],
        *,
        group_columns: tuple[str, ...],
        label: str,
    ) -> None:
        if not rows:
            return
        pointer = self._ready_pointer(connection)
        if pointer is None:
            return
        source = rows[0].source
        snapshots = sorted({row.source_snapshot_date for row in rows})
        placeholders = ", ".join("?" for _ in snapshots)
        columns = ", ".join([*group_columns, "security_id", "lineage_hash", "content_hash"])
        existing = self._execute(
            connection,
            f"""
            SELECT {columns}
            FROM {table}
            WHERE generation_sequence <= ?
              AND source = ?
              AND source_snapshot_date IN ({placeholders})
            """,
            [pointer[0], source, *snapshots],
        ).fetchall()
        existing_map = {
            (*row[: len(group_columns)], row[len(group_columns)]): (
                row[-2],
                row[-1],
            )
            for row in existing
        }
        for row in rows:
            natural = (
                *(getattr(row, column) for column in group_columns),
                row.security_id,
            )
            prior = existing_map.get(natural)
            if prior is not None and prior != (
                row.lineage_hash,
                self._content_hash(row),
            ):
                raise ClassificationConflictError(f"conflicting {label} in source snapshot")

    def publish(self, incoming: ClassificationSnapshot) -> GenerationSummary:
        snapshot = self._normalized(incoming)
        generation_id = self._generation_id(snapshot)
        connection = self._connect()
        try:
            existing = self._generation_by_id(connection, generation_id)
            if existing is not None:
                return existing
            self._assert_no_published_conflicts(
                connection,
                "security_master_history",
                snapshot.securities,
                group_columns=("source_snapshot_date",),
                label="security master",
            )
            self._assert_no_published_conflicts(
                connection,
                "index_component_history",
                snapshot.index_components,
                group_columns=("source_snapshot_date", "index_id"),
                label="index component",
            )
            self._assert_no_published_conflicts(
                connection,
                "sector_membership_history",
                snapshot.sector_memberships,
                group_columns=("source_snapshot_date", "taxonomy_id"),
                label="sector membership",
            )
            next_sequence = int(
                connection.execute(
                    "SELECT coalesce(max(sequence), 0) + 1 FROM classification_generations"
                ).fetchone()[0]
            )
            row_counts = {
                "security_master_history": len(snapshot.securities),
                "index_component_history": len(snapshot.index_components),
                "sector_membership_history": len(snapshot.sector_memberships),
            }
            audits = self._coverage_audits(snapshot, generation_id)
            scope = market_scope(snapshot.securities)
            connection.begin()
            try:
                connection.execute(
                    """
                    INSERT INTO classification_generations (
                        sequence,
                        generation_id,
                        schema_version,
                        source,
                        source_version,
                        source_snapshot_date,
                        source_date_semantics,
                        observed_at,
                        row_counts,
                        coverage_audits,
                        market_scope
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        next_sequence,
                        generation_id,
                        CLASSIFICATION_SCHEMA_VERSION,
                        snapshot.source,
                        snapshot.source_version,
                        snapshot.source_snapshot_date,
                        snapshot.source_date_semantics,
                        snapshot.observed_at,
                        json.dumps(row_counts, ensure_ascii=False),
                        json.dumps(
                            [item.model_dump(mode="json") for item in audits],
                            ensure_ascii=False,
                        ),
                        scope.model_dump_json(),
                    ],
                )
                self._insert_security_rows(
                    connection, next_sequence, generation_id, snapshot.securities
                )
                self._insert_index_rows(
                    connection, next_sequence, generation_id, snapshot.index_components
                )
                self._insert_sector_rows(
                    connection, next_sequence, generation_id, snapshot.sector_memberships
                )
                connection.execute("DELETE FROM classification_ready_pointer")
                connection.execute(
                    "INSERT INTO classification_ready_pointer VALUES (1, ?, ?)",
                    [next_sequence, generation_id],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return GenerationSummary(
                generation_id=generation_id,
                sequence=next_sequence,
                source=snapshot.source,
                source_version=snapshot.source_version,
                source_snapshot_date=snapshot.source_snapshot_date,
                source_date_semantics=snapshot.source_date_semantics,
                observed_at=snapshot.observed_at,
                row_counts=row_counts,
                coverage_audits=audits,
                market_scope=scope,
            )
        finally:
            connection.close()

    def _insert_security_rows(self, connection, sequence, generation_id, rows) -> None:
        for row in rows:
            connection.execute(
                """
                INSERT INTO security_master_history VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    sequence,
                    generation_id,
                    row.record_id,
                    row.security_id,
                    row.symbol,
                    row.source,
                    row.source_version,
                    row.source_snapshot_date,
                    row.effective_from,
                    row.effective_to,
                    row.observed_at,
                    row.lineage_hash,
                    self._content_hash(row),
                    row.model_dump_json(),
                ],
            )

    def _insert_index_rows(self, connection, sequence, generation_id, rows) -> None:
        for row in rows:
            connection.execute(
                """
                INSERT INTO index_component_history VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    sequence,
                    generation_id,
                    row.record_id,
                    row.security_id,
                    row.symbol,
                    row.index_id,
                    row.source,
                    row.source_version,
                    row.source_snapshot_date,
                    row.effective_from,
                    row.effective_to,
                    row.observed_at,
                    row.lineage_hash,
                    self._content_hash(row),
                    row.model_dump_json(),
                ],
            )

    def _insert_sector_rows(self, connection, sequence, generation_id, rows) -> None:
        for row in rows:
            connection.execute(
                """
                INSERT INTO sector_membership_history VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    sequence,
                    generation_id,
                    row.record_id,
                    row.security_id,
                    row.symbol,
                    row.taxonomy_id,
                    row.sector_id,
                    row.source,
                    row.source_version,
                    row.source_snapshot_date,
                    row.effective_from,
                    row.effective_to,
                    row.observed_at,
                    row.lineage_hash,
                    self._content_hash(row),
                    row.model_dump_json(),
                ],
            )

    @staticmethod
    def _ready_pointer(connection) -> tuple[int, str] | None:
        row = connection.execute(
            "SELECT sequence, generation_id FROM classification_ready_pointer WHERE singleton = 1"
        ).fetchone()
        return (int(row[0]), row[1]) if row is not None else None

    def ready_generation(self) -> str | None:
        connection = self._connect()
        try:
            pointer = self._ready_pointer(connection)
            return pointer[1] if pointer else None
        finally:
            connection.close()

    def generation_count(self) -> int:
        connection = self._connect()
        try:
            return int(
                connection.execute("SELECT count(*) FROM classification_generations").fetchone()[0]
            )
        finally:
            connection.close()

    def _generation_by_id(self, connection, generation_id: str) -> GenerationSummary | None:
        row = connection.execute(
            """
            SELECT sequence, generation_id, source, source_version, source_snapshot_date,
                   source_date_semantics, observed_at, row_counts, coverage_audits,
                   market_scope
            FROM classification_generations WHERE generation_id = ?
            """,
            [generation_id],
        ).fetchone()
        return self._generation_from_row(row) if row is not None else None

    @staticmethod
    def _generation_from_row(row) -> GenerationSummary:
        return GenerationSummary(
            sequence=row[0],
            generation_id=row[1],
            source=row[2],
            source_version=row[3],
            source_snapshot_date=row[4],
            source_date_semantics=row[5],
            observed_at=row[6],
            row_counts=json.loads(row[7]),
            coverage_audits=[CoverageAudit.model_validate(item) for item in json.loads(row[8])],
            market_scope=MarketScope.model_validate_json(row[9]),
        )

    def generation_at(self, as_of: date) -> GenerationSummary | None:
        connection = self._connect()
        try:
            pointer = self._ready_pointer(connection)
            if pointer is None:
                return None
            row = self._execute(
                connection,
                """
                SELECT sequence, generation_id, source, source_version, source_snapshot_date,
                       source_date_semantics, observed_at, row_counts, coverage_audits,
                       market_scope
                FROM classification_generations
                WHERE sequence <= ?
                  AND source_snapshot_date <= ?
                  AND CAST(observed_at AS DATE) <= ?
                ORDER BY source_snapshot_date DESC, sequence DESC
                LIMIT 1
                """,
                [pointer[0], as_of, as_of],
            ).fetchone()
            return self._generation_from_row(row) if row is not None else None
        finally:
            connection.close()

    def _records_at(
        self,
        table: str,
        model,
        as_of: date,
        *,
        filters: dict[str, str] | None = None,
        apply_effective_window: bool = True,
    ) -> tuple[list, str | None, date | None]:
        connection = self._connect()
        try:
            pointer = self._ready_pointer(connection)
            if pointer is None:
                return [], None, None
            generation = self._execute(
                connection,
                """
                SELECT sequence, generation_id
                FROM classification_generations
                WHERE sequence <= ?
                  AND source_snapshot_date <= ?
                  AND CAST(observed_at AS DATE) <= ?
                ORDER BY source_snapshot_date DESC, sequence DESC
                LIMIT 1
                """,
                [pointer[0], as_of, as_of],
            ).fetchone()
            if generation is None:
                return [], None, None
            generation_sequence = int(generation[0])
            generation_id = generation[1]
            conditions = [
                "generation_sequence = ?",
                "generation_id = ?",
                "source_snapshot_date <= ?",
                "CAST(observed_at AS DATE) <= ?",
            ]
            parameters: list[object] = [
                generation_sequence,
                generation_id,
                as_of,
                as_of,
            ]
            for column, value in (filters or {}).items():
                conditions.append(f"{column} = ?")
                parameters.append(value)
            if apply_effective_window:
                conditions.extend(
                    [
                        "(effective_from IS NULL OR effective_from <= ?)",
                        "(effective_to IS NULL OR effective_to >= ?)",
                    ]
                )
                parameters.extend([as_of, as_of])
            where = " AND ".join(conditions)
            snapshot_row = self._execute(
                connection,
                f"""
                SELECT source_snapshot_date
                FROM {table}
                WHERE {where}
                ORDER BY source_snapshot_date DESC, generation_sequence DESC
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            if snapshot_row is None:
                return [], None, None
            selected_date = snapshot_row[0]
            rows = self._execute(
                connection,
                f"""
                SELECT payload, generation_id, effective_to
                FROM {table}
                WHERE {where}
                  AND source_snapshot_date = ?
                ORDER BY symbol
                """,
                [*parameters, selected_date],
            ).fetchall()
            if not rows:
                return [], None, selected_date
            return (
                [model.model_validate_json(row[0]) for row in rows],
                generation_id,
                selected_date,
            )
        finally:
            connection.close()

    def securities_at(
        self,
        as_of: date,
    ) -> tuple[list[SecurityMasterRecord], str | None, date | None]:
        return self._records_at(
            "security_master_history",
            SecurityMasterRecord,
            as_of,
            apply_effective_window=False,
        )

    def index_components_at(
        self,
        index_id: str,
        as_of: date,
    ) -> tuple[list[IndexComponentRecord], str | None, date | None]:
        return self._records_at(
            "index_component_history",
            IndexComponentRecord,
            as_of,
            filters={"index_id": index_id},
        )

    def sector_memberships_at(
        self,
        taxonomy_id: str,
        as_of: date,
    ) -> tuple[list[SectorMembershipRecord], str | None, date | None]:
        return self._records_at(
            "sector_membership_history",
            SectorMembershipRecord,
            as_of,
            filters={"taxonomy_id": taxonomy_id},
        )
