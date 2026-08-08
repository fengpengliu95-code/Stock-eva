import hashlib
import json
from datetime import UTC, date, datetime
from importlib.metadata import version
from time import monotonic
from typing import Any

from backend.app.classification.failures import (
    ClassificationFailure,
    ClassificationProviderError,
)
from backend.app.classification.models import (
    INDEX_CATALOG,
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    IndexComponentRecord,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.market.baostock import (
    BaoStockError,
    BaoStockProvider,
    _OperationDeadlineExceeded,
    _OperationDeadlineUnavailable,
)
from backend.app.security_identity import derive_security_identity


class _ClassificationSchemaError(ValueError):
    pass


class _ClassificationDataQualityError(ValueError):
    pass


def _date(value: str) -> date | None:
    return date.fromisoformat(value) if value else None


def _hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _row_dicts(fields: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    return [dict(zip(fields, row, strict=True)) for row in rows]


def _required_rows(
    fields: list[str],
    rows: list[list[str]],
    *,
    label: str,
    required_fields: set[str],
) -> list[dict[str, str]]:
    missing = sorted(required_fields - set(fields))
    if missing:
        raise _ClassificationSchemaError(f"{label} missing required fields: {', '.join(missing)}")
    if not rows:
        raise _ClassificationDataQualityError(f"{label} returned no rows")
    try:
        return _row_dicts(fields, rows)
    except ValueError as exc:
        raise _ClassificationSchemaError(f"{label} row shape is invalid") from exc


def _looks_like_target_a_share(symbol: str) -> bool:
    identity = derive_security_identity(symbol, "1")
    return identity.exchange in {"sh", "sz"} and identity.board in {"main", "chinext", "star"}


def _single_snapshot_date(
    rows: list[dict[str, str]],
    *,
    label: str,
    requested_as_of: date,
) -> date | None:
    dates = {_date(row["updateDate"]) for row in rows}
    dates.discard(None)
    if len(dates) > 1:
        raise _ClassificationDataQualityError(f"mixed {label} snapshot dates")
    snapshot_date = next(iter(dates), None)
    if snapshot_date is not None and snapshot_date > requested_as_of:
        raise _ClassificationDataQualityError(f"{label} snapshot date is after requested as_of")
    return snapshot_date


class BaoStockClassificationProvider:
    """Bounded metadata adapter over the repository's BaoStock session/retry wrapper."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        clock=lambda: datetime.now(UTC),
        min_request_interval_seconds: float | None = None,
        socket_timeout_seconds: float = 30.0,
        monotonic_fn=monotonic,
    ) -> None:
        self.clock = clock
        self._monotonic = monotonic_fn
        self.source_version = version("baostock")
        self.last_request_count = 0
        self.session = BaoStockProvider(
            client=client,
            min_request_interval_seconds=min_request_interval_seconds,
            socket_timeout_seconds=socket_timeout_seconds,
        )

    def _error(
        self,
        *,
        stage: str,
        failure_class: str,
        started_at: float,
        initial_request_count: int,
    ) -> ClassificationProviderError:
        return ClassificationProviderError(
            ClassificationFailure(
                failure_stage=stage,
                failure_class=failure_class,
                elapsed_seconds=round(max(0.0, self._monotonic() - started_at), 3),
                provider_request_count=(
                    self.session._provider_request_count - initial_request_count
                ),
                configured_timeout_seconds=self.session.socket_timeout_seconds,
                configured_max_attempts=self.session.max_attempts,
            )
        )

    @staticmethod
    def _transport_failure_class(exc: Exception) -> str:
        if isinstance(exc, _OperationDeadlineExceeded):
            return "deadline"
        if isinstance(exc, _OperationDeadlineUnavailable):
            return "internal"
        return (
            "transport" if isinstance(exc, (BaoStockError, TimeoutError, OSError)) else "internal"
        )

    def _network_stage(
        self,
        stage: str,
        operation,
        *,
        started_at: float,
        initial_request_count: int,
    ):
        try:
            return operation()
        except Exception as exc:
            raise self._error(
                stage=stage,
                failure_class=self._transport_failure_class(exc),
                started_at=started_at,
                initial_request_count=initial_request_count,
            ) from exc

    def _validation_stage(
        self,
        stage: str,
        operation,
        *,
        started_at: float,
        initial_request_count: int,
    ):
        try:
            return operation()
        except Exception as exc:
            if isinstance(exc, _ClassificationDataQualityError):
                failure_class = "data_quality"
            elif isinstance(exc, (_ClassificationSchemaError, KeyError, TypeError, ValueError)):
                failure_class = "schema"
            else:
                failure_class = "internal"
            raise self._error(
                stage=stage,
                failure_class=failure_class,
                started_at=started_at,
                initial_request_count=initial_request_count,
            ) from exc

    @staticmethod
    def _usable_basics(
        all_records: list[dict[str, str]],
        basic_records: list[dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        basics = {row["code"]: row for row in basic_records}
        for row in all_records:
            if not _looks_like_target_a_share(row["code"]):
                continue
            basic = basics.get(row["code"])
            if basic is None or basic["type"] != "1" or basic["status"] not in {"0", "1"}:
                raise _ClassificationDataQualityError("target A-share has no usable basic metadata")
        return basics

    def fetch(self, as_of: date) -> ClassificationSnapshot:
        initial_request_count = self.session._provider_request_count
        started_at = self._monotonic()
        owns_session = False

        def network(stage: str, operation):
            return self._network_stage(
                stage,
                operation,
                started_at=started_at,
                initial_request_count=initial_request_count,
            )

        def validate(stage: str, operation):
            return self._validation_stage(
                stage,
                operation,
                started_at=started_at,
                initial_request_count=initial_request_count,
            )

        def read(stage: str, operation):
            return network(stage, lambda: self.session._read(operation))

        def required(stage: str, payload, *, label: str, fields: set[str]):
            result_fields, rows = payload
            return validate(
                stage,
                lambda: _required_rows(
                    result_fields,
                    rows,
                    label=label,
                    required_fields=fields,
                ),
            )

        try:
            network("login", self.session._login)
            owns_session = True
            observed_at = validate("validation", self.clock)
            all_records = required(
                "security_universe",
                read(
                    "security_universe",
                    lambda: self.session.client.query_all_stock(day=as_of.isoformat()),
                ),
                label="query_all_stock",
                fields={"code", "tradeStatus", "code_name"},
            )
            basic_records = required(
                "security_basic",
                read("security_basic", lambda: self.session.client.query_stock_basic()),
                label="query_stock_basic",
                fields={"code", "code_name", "ipoDate", "outDate", "type", "status"},
            )
            basics = validate(
                "validation",
                lambda: self._usable_basics(all_records, basic_records),
            )
            securities = validate(
                "validation",
                lambda: [
                    self._security_record(row, basics.get(row["code"]), as_of, observed_at)
                    for row in all_records
                ],
            )
            security_ids = {row.security_id for row in securities}
            industry_records = required(
                "industry",
                read(
                    "industry",
                    lambda: self.session.client.query_stock_industry(date=as_of.isoformat()),
                ),
                label="query_stock_industry",
                fields={
                    "updateDate",
                    "code",
                    "code_name",
                    "industry",
                    "industryClassification",
                },
            )
            validate(
                "industry",
                lambda: _single_snapshot_date(
                    industry_records,
                    label="industry",
                    requested_as_of=as_of,
                ),
            )
            memberships = validate(
                "industry",
                lambda: [
                    self._sector_record(row, observed_at)
                    for row in industry_records
                    if f"baostock:{row['code']}" in security_ids
                ],
            )
            index_operations = {
                "hs300": lambda: self.session.client.query_hs300_stocks(date=as_of.isoformat()),
                "sz50": lambda: self.session.client.query_sz50_stocks(date=as_of.isoformat()),
                "csi500": lambda: self.session.client.query_zz500_stocks(date=as_of.isoformat()),
            }
            components: list[IndexComponentRecord] = []
            for index_id, operation in index_operations.items():
                records = required(
                    index_id,
                    read(index_id, operation),
                    label=f"{index_id} component",
                    fields={"updateDate", "code", "code_name"},
                )
                validate(
                    index_id,
                    lambda records=records, index_id=index_id: _single_snapshot_date(
                        records,
                        label=f"{index_id} component",
                        requested_as_of=as_of,
                    ),
                )

                def validated_components(
                    records=records,
                    index_id=index_id,
                ) -> list[IndexComponentRecord]:
                    result = []
                    for row in records:
                        if f"baostock:{row['code']}" not in security_ids:
                            raise _ClassificationDataQualityError(
                                f"{index_id} references unknown security"
                            )
                        result.append(self._index_record(index_id, row, observed_at))
                    return result

                components.extend(validate(index_id, validated_components))
            return validate(
                "validation",
                lambda: ClassificationSnapshot(
                    source="baostock",
                    source_version=self.source_version,
                    source_snapshot_date=as_of,
                    source_date_semantics="requested_unverified",
                    observed_at=observed_at,
                    securities=securities,
                    index_components=components,
                    sector_memberships=memberships,
                ),
            )
        finally:
            if owns_session:
                self.session._logout()
            self.last_request_count = self.session._provider_request_count - initial_request_count

    def _security_record(
        self,
        row: dict[str, str],
        basic: dict[str, str] | None,
        snapshot_date: date,
        observed_at: datetime,
    ) -> SecurityMasterRecord:
        source_type = basic["type"] if basic is not None else ""
        identity = derive_security_identity(row["code"], source_type)
        list_date = _date(basic["ipoDate"]) if basic is not None else None
        delist_date = _date(basic["outDate"]) if basic is not None else None
        raw = {
            "code": row["code"],
            "code_name": row["code_name"],
            "tradeStatus": row["tradeStatus"],
            "ipoDate": basic["ipoDate"] if basic is not None else "",
            "outDate": basic["outDate"] if basic is not None else "",
            "type": source_type,
            "status": basic["status"] if basic is not None else "",
            "board_derivation_version": identity.derivation_version,
            "price_available": None,
        }
        lineage = _hash(raw)
        symbol = row["code"]
        return SecurityMasterRecord(
            record_id=f"security:{snapshot_date}:{symbol}",
            security_id=f"baostock:{symbol}",
            symbol=symbol,
            name=row["code_name"],
            exchange=identity.exchange,
            board=identity.board,
            security_type=identity.security_type,
            list_date=list_date,
            delist_date=delist_date,
            is_tradable=(row["tradeStatus"] == "1"),
            price_available=None,
            listing_status=raw["status"],
            daily_trade_status=row["tradeStatus"],
            source="baostock",
            source_version=self.source_version,
            source_snapshot_date=snapshot_date,
            source_date_semantics="requested_unverified",
            effective_from=list_date,
            effective_to=delist_date,
            observed_at=observed_at,
            source_record_id=f"query_all_stock:{snapshot_date}:{symbol}",
            lineage_hash=lineage,
        )

    def _sector_record(
        self,
        row: dict[str, str],
        observed_at: datetime,
    ) -> SectorMembershipRecord:
        snapshot_date = _date(row["updateDate"])
        if snapshot_date is None:
            raise _ClassificationDataQualityError("industry row has no updateDate")
        raw = {
            "updateDate": row["updateDate"],
            "code": row["code"],
            "industry": row["industry"],
            "industryClassification": row["industryClassification"],
        }
        sector_digest = _hash(
            {
                "taxonomy": row["industryClassification"],
                "industry": row["industry"],
            }
        )[:16]
        sector_id = f"baostock-industry-{sector_digest}"
        return SectorMembershipRecord(
            record_id=(f"sector:{TAXONOMY_BAOSTOCK_INDUSTRY}:{snapshot_date}:{row['code']}"),
            taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
            taxonomy_name="BaoStock industryClassification",
            sector_id=sector_id,
            sector_name=row["industry"],
            security_id=f"baostock:{row['code']}",
            symbol=row["code"],
            raw_industry=row["industry"],
            raw_classification=row["industryClassification"],
            source="baostock",
            source_version=self.source_version,
            source_snapshot_date=snapshot_date,
            source_date_semantics="source_observed",
            effective_from=snapshot_date,
            effective_to=None,
            observed_at=observed_at,
            source_record_id=f"query_stock_industry:{snapshot_date}:{row['code']}",
            lineage_hash=_hash(raw),
        )

    def _index_record(
        self,
        index_id: str,
        row: dict[str, str],
        observed_at: datetime,
    ) -> IndexComponentRecord:
        snapshot_date = _date(row["updateDate"])
        if snapshot_date is None:
            raise _ClassificationDataQualityError(f"{index_id} row has no updateDate")
        raw = {
            "index_id": index_id,
            "updateDate": row["updateDate"],
            "code": row["code"],
            "code_name": row["code_name"],
        }
        return IndexComponentRecord(
            record_id=f"index:{index_id}:{snapshot_date}:{row['code']}",
            index_id=index_id,
            index_name=INDEX_CATALOG[index_id].name,
            security_id=f"baostock:{row['code']}",
            symbol=row["code"],
            source="baostock",
            source_version=self.source_version,
            source_snapshot_date=snapshot_date,
            source_date_semantics="source_observed",
            effective_from=snapshot_date,
            effective_to=None,
            observed_at=observed_at,
            source_record_id=f"{INDEX_CATALOG[index_id].component_source}:{snapshot_date}:{row['code']}",
            lineage_hash=_hash(raw),
        )
