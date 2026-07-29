import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import version
from typing import Any

from backend.app.classification.models import (
    INDEX_CATALOG,
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    IndexComponentRecord,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.market.baostock import BaoStockError, BaoStockProvider


class ClassificationProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class SecurityIdentity:
    exchange: str
    board: str
    security_type: str
    derivation_version: str = "cn-symbol-prefix-v1"


def derive_security_identity(symbol: str, source_type: str) -> SecurityIdentity:
    exchange = symbol.split(".", 1)[0].lower()
    if source_type == "2":
        return SecurityIdentity(exchange=exchange, board="index", security_type="index")
    if source_type != "1":
        return SecurityIdentity(exchange=exchange, board="other", security_type="other")
    code = symbol.split(".", 1)[1] if "." in symbol else symbol
    if exchange == "bj":
        board = "bse"
    elif exchange == "sh" and code.startswith(("688", "689")):
        board = "star"
    elif exchange == "sz" and code.startswith(("300", "301")):
        board = "chinext"
    elif exchange == "sh" and code.startswith("900"):
        board = "b_share"
    elif exchange == "sz" and code.startswith("200"):
        board = "b_share"
    elif exchange == "sh" and code.startswith(("600", "601", "603", "605")):
        board = "main"
    elif exchange == "sz" and code.startswith(("000", "001", "002", "003")):
        board = "main"
    else:
        board = "other"
    return SecurityIdentity(exchange=exchange, board=board, security_type="stock")


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
        raise ClassificationProviderError(
            f"{label} missing required fields: {', '.join(missing)}"
        )
    if not rows:
        raise ClassificationProviderError(f"{label} returned no rows")
    try:
        return _row_dicts(fields, rows)
    except ValueError as exc:
        raise ClassificationProviderError(f"{label} row shape is invalid") from exc


def _looks_like_target_a_share(symbol: str) -> bool:
    identity = derive_security_identity(symbol, "1")
    return (
        identity.exchange in {"sh", "sz"}
        and identity.board in {"main", "chinext", "star"}
    )


def _single_snapshot_date(
    rows: list[dict[str, str]],
    *,
    label: str,
    requested_as_of: date,
) -> date | None:
    dates = {_date(row["updateDate"]) for row in rows}
    dates.discard(None)
    if len(dates) > 1:
        raise ClassificationProviderError(f"mixed {label} snapshot dates")
    snapshot_date = next(iter(dates), None)
    if snapshot_date is not None and snapshot_date > requested_as_of:
        raise ClassificationProviderError(
            f"{label} snapshot date is after requested as_of"
        )
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
    ) -> None:
        self.clock = clock
        self.source_version = version("baostock")
        self.last_request_count = 0
        self.session = BaoStockProvider(
            client=client,
            min_request_interval_seconds=min_request_interval_seconds,
            socket_timeout_seconds=socket_timeout_seconds,
        )

    def fetch(self, as_of: date) -> ClassificationSnapshot:
        initial_request_count = self.session._provider_request_count
        owns_session = False
        try:
            self.session._login()
            owns_session = True
            observed_at = self.clock()
            all_fields, all_rows = self.session._read(
                lambda: self.session.client.query_all_stock(day=as_of.isoformat())
            )
            basic_fields, basic_rows = self.session._read(
                lambda: self.session.client.query_stock_basic()
            )
            industry_fields, industry_rows = self.session._read(
                lambda: self.session.client.query_stock_industry(date=as_of.isoformat())
            )
            index_payloads = {
                "hs300": self.session._read(
                    lambda: self.session.client.query_hs300_stocks(date=as_of.isoformat())
                ),
                "sz50": self.session._read(
                    lambda: self.session.client.query_sz50_stocks(date=as_of.isoformat())
                ),
                "csi500": self.session._read(
                    lambda: self.session.client.query_zz500_stocks(date=as_of.isoformat())
                ),
            }
        except (BaoStockError, KeyError, ValueError) as exc:
            raise ClassificationProviderError("BaoStock classification metadata failed") from exc
        finally:
            if owns_session:
                self.session._logout()
            self.last_request_count = (
                self.session._provider_request_count - initial_request_count
            )

        all_records = _required_rows(
            all_fields,
            all_rows,
            label="query_all_stock",
            required_fields={"code", "tradeStatus", "code_name"},
        )
        basic_records = _required_rows(
            basic_fields,
            basic_rows,
            label="query_stock_basic",
            required_fields={
                "code",
                "code_name",
                "ipoDate",
                "outDate",
                "type",
                "status",
            },
        )
        industry_records = _required_rows(
            industry_fields,
            industry_rows,
            label="query_stock_industry",
            required_fields={
                "updateDate",
                "code",
                "code_name",
                "industry",
                "industryClassification",
            },
        )
        index_records = {
            index_id: _required_rows(
                fields,
                rows,
                label=f"{index_id} component",
                required_fields={"updateDate", "code", "code_name"},
            )
            for index_id, (fields, rows) in index_payloads.items()
        }
        basics = {row["code"]: row for row in basic_records}
        for row in all_records:
            if not _looks_like_target_a_share(row["code"]):
                continue
            basic = basics.get(row["code"])
            if (
                basic is None
                or basic["type"] != "1"
                or basic["status"] not in {"0", "1"}
            ):
                raise ClassificationProviderError(
                    "target A-share has no usable basic metadata: "
                    f"{row['code']}"
                )
        securities = [
            self._security_record(row, basics.get(row["code"]), as_of, observed_at)
            for row in all_records
        ]
        security_ids = {row.security_id for row in securities}

        _single_snapshot_date(
            industry_records,
            label="industry",
            requested_as_of=as_of,
        )
        memberships = [
            self._sector_record(row, observed_at)
            for row in industry_records
            if f"baostock:{row['code']}" in security_ids
        ]
        components: list[IndexComponentRecord] = []
        for index_id, records in index_records.items():
            _single_snapshot_date(
                records,
                label=f"{index_id} component",
                requested_as_of=as_of,
            )
            for row in records:
                if f"baostock:{row['code']}" not in security_ids:
                    raise ClassificationProviderError(
                        f"{index_id} references unknown security"
                    )
                components.append(self._index_record(index_id, row, observed_at))
        return ClassificationSnapshot(
            source="baostock",
            source_version=self.source_version,
            source_snapshot_date=as_of,
            source_date_semantics="requested_unverified",
            observed_at=observed_at,
            securities=securities,
            index_components=components,
            sector_memberships=memberships,
        )

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
            is_tradable=(
                row["tradeStatus"] == "1"
            ),
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
            raise ClassificationProviderError("industry row has no updateDate")
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
            record_id=(
                f"sector:{TAXONOMY_BAOSTOCK_INDUSTRY}:"
                f"{snapshot_date}:{row['code']}"
            ),
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
            raise ClassificationProviderError(f"{index_id} row has no updateDate")
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
