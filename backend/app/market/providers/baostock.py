"""Typed BaoStock compatibility adapter at the existing transport boundary."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from hashlib import sha256

from backend.app.market.baostock import DAILY_FIELDS, _is_main_board, capture_registry
from backend.app.market.baostock import BaoStockProvider as IncumbentBaoStockProvider
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.models import DailyBar
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.provider_transport import (
    ProtocolStage,
    TransportObservation,
    TransportOutcome,
)
from backend.app.market.provider_transport import ProviderEndpoint as TransportEndpoint

from .base import (
    AdjustFactorRow,
    AllStockRow,
    AttemptCompletion,
    DailyAStockRow,
    DailyFactorRow,
    EndpointContractSummary,
    ExpectedLogicalRequest,
    IndexHistoryRangeRow,
    IndexHistorySessionRow,
    InstrumentRole,
    ProviderId,
    ProviderRawBatch,
    ProviderRequest,
    RawEndpointBatch,
    RequestCompletion,
    SafeVersion,
    TradeDatesRow,
    TransportLineageRef,
    TransportObservationAggregate,
    TransportObservationProjection,
    endpoint_contract_for,
    validate_raw_date_binding,
)

_DAILY_WIRE_FIELDS = tuple(
    DAILY_FIELDS.split(",")[:-1] + ["peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST"]
)
_DAILY_FACTOR_WIRE_FIELDS = (
    "code",
    "dividOperateDate",
    "foreAdjustFactor",
    "backAdjustFactor",
    "adjustFacto",
)

_RAW_BATCH_VALIDATION_CODES = {
    "raw endpoint batches must be ordered by plan and page": "BATCH_ORDER",
    "raw endpoint summaries do not bind source batches": "SUMMARY_BINDING",
    "source batch pages do not bind completion": "PAGE_COMPLETION_BINDING",
    "successful completion row count mismatch": "ROW_COUNT_MISMATCH",
    "transport lineage does not exactly match successful pages": "LINEAGE_PAGE_MISMATCH",
    "raw endpoint batches do not exactly match successful pages": "BATCH_PAGE_MISMATCH",
    "successful COMPLETE projections do not exactly match pages": "PROJECTION_PAGE_MISMATCH",
    "successful page lineage joins are not exact": "LINEAGE_JOIN_MISMATCH",
    "completion hash mismatch": "COMPLETION_HASH_MISMATCH",
}


def _raw_batch_validation_code(error: Exception) -> str:
    rendered = str(error)
    for message, code in _RAW_BATCH_VALIDATION_CODES.items():
        if message in rendered:
            return code
    return "UNKNOWN_RAW_BATCH_VALIDATION"


class BaoStockProviderAdapter:
    provider_id = ProviderId.BAOSTOCK
    adapter_version: SafeVersion = "r2f2.v1"
    endpoint_contract_version: SafeVersion = "r2f2-endpoints.v1"

    def __init__(self, client: object | None = None, **kwargs: object) -> None:
        self._incumbent = IncumbentBaoStockProvider(client=client, **kwargs)
        self.client = self._incumbent.client
        self.factor_cache = self._incumbent.factor_cache
        self.max_attempts = self._incumbent.max_attempts

    def fetch(self, trade_date, symbols: Sequence[str] | None = None):
        return self._incumbent.fetch(trade_date, symbols)

    def fetch_range(self, start_date, end_date, *, symbols: Sequence[str]):
        return self._incumbent.fetch_range(start_date, end_date, symbols=symbols)

    def trading_dates(self, start_date, end_date):
        return self._incumbent.trading_dates(start_date, end_date)

    def refresh_operation(self, refresh_id: str):
        """Delegate the incumbent transport scope without altering its identity."""
        return self._incumbent.refresh_operation(refresh_id)

    def inspect_main_board(self, trade_date):
        return self._incumbent.inspect_main_board(trade_date)

    def ensure_factor_snapshot(self, symbols: Sequence[str], trade_date: date) -> None:
        """Advance the incumbent factor stream until an exact target snapshot exists."""
        requested = tuple(sorted(set(str(symbol).lower() for symbol in symbols)))
        if not requested:
            raise ValueError("factor snapshot requires symbols")
        cache = self.factor_cache

        def covered() -> bool:
            rows = cache.exact_snapshot_records(requested, trade_date)
            return tuple(str(row["symbol"]) for row in rows) == requested

        if covered():
            return
        self._incumbent._login(TransportEndpoint.DAILY_FACTOR)
        try:
            self._incumbent._main_board_factor_snapshot(trade_date, requested)
        finally:
            self._incumbent._logout()
        if not covered():
            raise ValueError("factor snapshot does not exactly cover requested symbols")

    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch:
        started_at = datetime.now(UTC)
        endpoint_batches: list[RawEndpointBatch] = []
        completions: list[RequestCompletion] = []
        lineages: list[TransportLineageRef] = []
        projections: list[TransportObservationProjection] = []
        summaries: list[EndpointContractSummary] = []
        raw_observations: list[TransportObservation] = []
        with transport_observation_sink(raw_observations.append):
            with self._incumbent.refresh_operation(request.refresh_id):
                with capture_registry(0) as login_entries:
                    self._incumbent._login(TransportEndpoint.TRADE_DATES)
                login_count = len(raw_observations)
                try:
                    for logical in request.logical_request_plan.requests:
                        pages, completion, page_lineages, page_projections, summary = (
                            self._fetch_logical(logical, raw_observations)
                        )
                        endpoint_batches.extend(pages)
                        completions.append(completion)
                        lineages.extend(page_lineages)
                        projections.extend(page_projections)
                        summaries.append(summary)
                finally:
                    self._incumbent._logout()
        login_projections = tuple(
            self._projection(item, login_entries, default_kind="login_audit")
            for item in raw_observations[:login_count]
        )
        completed_at = datetime.now(UTC)
        completion_hash = sha256(
            json.dumps(
                [item.model_dump(mode="json") for item in completions],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        try:
            batch = ProviderRawBatch(
                provider_id=ProviderId.BAOSTOCK,
                request=request,
                adapter_version=self.adapter_version,
                endpoint_contract_version=self.endpoint_contract_version,
                started_at=started_at,
                completed_at=completed_at,
                normalization_clock_utc=completed_at,
                logical_request_plan=request.logical_request_plan,
                request_plan_hash=request.logical_request_plan.request_plan_hash,
                endpoint_batches=tuple(endpoint_batches),
                request_completions=tuple(completions),
                completion_hash=completion_hash,
                transport_lineage=tuple(lineages),
                transport_observations=TransportObservationAggregate.from_observations(
                    login_projections + tuple(projections)
                ),
                endpoint_summaries=tuple(summaries),
            )
        except (TypeError, ValueError) as error:
            logging.getLogger(__name__).error(
                "baostock_raw_batch_validation_failed code=%s",
                _raw_batch_validation_code(error),
            )
            raise
        for endpoint_batch in batch.endpoint_batches:
            validate_raw_date_binding(request, endpoint_batch)
        return batch

    def _fetch_logical(self, logical: ExpectedLogicalRequest, raw_observations):
        endpoint = TransportEndpoint(logical.endpoint.value)
        contract = endpoint_contract_for(
            logical.endpoint, logical.instrument_role, logical.schema_variant
        )
        captured_pages: list[tuple[int, int, str, str, list[str], list[list[str]]]] = []
        terminals: list[int] = []
        observation_start = len(raw_observations)
        with capture_registry(logical.plan_ordinal) as scope_entries:
            fields, _rows = self._incumbent._read(
                endpoint,
                lambda: self._query(endpoint, logical),
                page_capture=lambda attempt, page, request_id, session_id, page_fields, page_rows: (
                    captured_pages.append(
                        (attempt, page, request_id, session_id, page_fields, page_rows)
                    )
                ),
                pagination_terminal=lambda: terminals.append(1),
                capture_all_operation_outcomes=True,
            )
        fields, captured_pages = self._project_known_wire_schema(
            endpoint, contract.fields, fields, captured_pages
        )
        if endpoint is TransportEndpoint.ALL_STOCK:
            captured_pages = self._filter_active_main_board_rows(fields, captured_pages)
        if endpoint is TransportEndpoint.DAILY_FACTOR:
            captured_pages = self._filter_requested_factor_rows(
                fields, captured_pages, logical.symbols
            )
        local = tuple(
            item for item in raw_observations[observation_start:] if item.endpoint is endpoint
        )
        if tuple(fields) != contract.fields or any(
            tuple(page[4]) != contract.fields for page in captured_pages
        ):
            raise ValueError("source schema does not match endpoint contract")
        local_projections = tuple(
            self._projection(item, scope_entries, default_kind="page") for item in local
        )
        operations = tuple(
            item
            for item, projection in zip(local, local_projections, strict=True)
            if item.protocol_stage is ProtocolStage.OPERATION
            and projection.lineage_kind == "query_root"
        )
        if not operations:
            raise RuntimeError("BaoStock logical request has no operation observation")
        for attempt in {item.attempt for item in operations}:
            if sum(item.attempt == attempt for item in operations) != 1:
                raise ValueError("BaoStock logical request has duplicate query-root operation")
        root_ids: dict[int, str] = {}
        for item in operations:
            entries = [
                entry
                for entry in scope_entries
                if entry["request_id"] == item.request_id
                and entry["provider_session_id"] == item.provider_session_id
                and entry["attempt"] == item.attempt
                and entry["page"] == item.page
                and entry["lineage_kind"] == "query_root"
            ]
            if len(entries) == 1 and item.outcome is not TransportOutcome.SUCCESS:
                root_ids[item.attempt] = item.request_id
            elif len(entries) == 1 and item.outcome is TransportOutcome.SUCCESS:
                root_ids.setdefault(item.attempt, item.request_id)
        if set(root_ids) != {item.attempt for item in operations}:
            raise RuntimeError("BaoStock logical request has no authoritative query root")
        projection_by_identity = {
            (item.request_id, item.attempt, item.page, item.protocol_stage): projection
            for item, projection in zip(local, local_projections, strict=True)
        }
        successful_ops = tuple(
            item for item in operations if item.outcome is TransportOutcome.SUCCESS
        )
        successful_attempt = max((item.attempt for item in successful_ops), default=0)
        if not successful_ops:
            raise RuntimeError("BaoStock logical request has no successful operation observation")
        successful_pages = tuple(page for page in captured_pages if page[0] == successful_attempt)

        def page_lineage(page):
            attempt, page_number, page_id, session_id, _, _ = page
            matches = [
                (item, projection)
                for item, projection in zip(local, local_projections, strict=True)
                if item.request_id == page_id
                and item.provider_session_id == session_id
                and item.attempt == attempt
                and item.page == page_number
                and item.protocol_stage is ProtocolStage.COMPLETE
                and item.outcome is TransportOutcome.SUCCESS
                and item.end_marker_seen
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    "successful page requires exactly one marked COMPLETE observation"
                )
            item, projection = matches[0]
            return TransportLineageRef(
                refresh_id=item.refresh_id,
                provider_session_id=item.provider_session_id,
                root_request_id=root_ids[attempt],
                page_request_id=page_id,
                endpoint=logical.endpoint,
                plan_ordinal=logical.plan_ordinal,
                attempt=attempt,
                page=page_number,
                protocol_stage=ProtocolStage.COMPLETE,
                observation_digest=projection.observation_digest,
            )

        page_lineages = tuple(page_lineage(page) for page in successful_pages)
        endpoint_batches = tuple(
            RawEndpointBatch(
                endpoint=logical.endpoint,
                schema_variant=logical.schema_variant,
                request_role=logical.request_role,
                instrument_role=logical.instrument_role,
                plan_ordinal=logical.plan_ordinal,
                shard_id=logical.shard_id,
                lineage=lineage,
                rows=tuple(self._typed_row(logical, page[4], row) for row in page[5]),
                row_count=len(page[5]),
                source_schema=contract.schema_variant,
                fields=contract.fields,
                units=contract.units,
                date_semantics=contract.date_semantics,
                pagination_policy=contract.pagination_policy,
                provider_row_order_digest=self._typed_row_digest(logical, page[4], page[5]),
            )
            for page, lineage in zip(successful_pages, page_lineages, strict=True)
        )
        attempts: list[AttemptCompletion] = []
        for attempt_number in sorted({item.attempt for item in operations}):
            attempt_ops = tuple(item for item in operations if item.attempt == attempt_number)
            root = next(item for item in attempt_ops if item.request_id == root_ids[attempt_number])
            pages = tuple(sorted({page[1] for page in captured_pages if page[0] == attempt_number}))
            page_ids = tuple(
                (page[1], page[2]) for page in captured_pages if page[0] == attempt_number
            )
            root_projection = projection_by_identity[
                (root.request_id, root.attempt, root.page, root.protocol_stage)
            ]
            is_success = root.outcome is TransportOutcome.SUCCESS
            attempts.append(
                AttemptCompletion(
                    plan_ordinal=logical.plan_ordinal,
                    provider_session_id=root.provider_session_id,
                    attempt=attempt_number,
                    root_request_id=root.request_id,
                    operation_observation_digest=root_projection.observation_digest,
                    observed_pages=pages,
                    page_request_ids=page_ids,
                    observed_page_count=len(pages),
                    pagination_terminal_count=(
                        len(terminals) if is_success and attempt_number == successful_attempt else 0
                    ),
                    terminal=is_success,
                    outcome=root.outcome,
                )
            )
        final_root = next(item for item in successful_ops if item.attempt == successful_attempt)
        completion = RequestCompletion(
            plan_ordinal=logical.plan_ordinal,
            attempts=tuple(attempts),
            successful_attempt=successful_attempt,
            successful_root_request_id=final_root.request_id,
            final_outcome=TransportOutcome.SUCCESS,
            row_count=sum(page.row_count for page in endpoint_batches),
        )
        summary = EndpointContractSummary(
            endpoint=contract.endpoint,
            request_role=contract.request_role,
            instrument_role=contract.instrument_role,
            schema_variant=contract.schema_variant,
            source_schema=contract.schema_variant,
            fields=contract.fields,
            units=contract.units,
            date_semantics=contract.date_semantics,
            pagination_policy=contract.pagination_policy,
            batch_count=len(endpoint_batches),
            row_count=sum(page.row_count for page in endpoint_batches),
        )
        return endpoint_batches, completion, page_lineages, local_projections, summary

    @staticmethod
    def _filter_active_main_board_rows(fields, captured_pages):
        if tuple(fields) != ("code", "tradeStatus", "code_name"):
            raise ValueError("source schema does not match endpoint contract")
        code_position = fields.index("code")
        status_position = fields.index("tradeStatus")
        filtered_pages = []
        for attempt, page, request_id, session_id, page_fields, rows in captured_pages:
            if any(len(row) != len(fields) for row in rows):
                raise ValueError("source schema does not match endpoint contract")
            filtered_pages.append(
                (
                    attempt,
                    page,
                    request_id,
                    session_id,
                    page_fields,
                    [
                        row
                        for row in sorted(rows, key=lambda item: item[code_position])
                        if _is_main_board(row[code_position]) and row[status_position] == "1"
                    ],
                )
            )
        return filtered_pages

    @staticmethod
    def _project_known_wire_schema(endpoint, contract_fields, fields, captured_pages):
        if endpoint is TransportEndpoint.DAILY_FACTOR and tuple(fields) == (
            _DAILY_FACTOR_WIRE_FIELDS
        ):
            if any(tuple(page[4]) != _DAILY_FACTOR_WIRE_FIELDS for page in captured_pages):
                raise ValueError("source schema does not match endpoint contract")
            return list(contract_fields), [
                (
                    attempt,
                    page,
                    request_id,
                    session_id,
                    list(contract_fields),
                    rows,
                )
                for attempt, page, request_id, session_id, _page_fields, rows in captured_pages
            ]
        if endpoint is not TransportEndpoint.DAILY_ASTOCK or tuple(fields) != _DAILY_WIRE_FIELDS:
            return fields, captured_pages
        if any(tuple(page[4]) != _DAILY_WIRE_FIELDS for page in captured_pages):
            raise ValueError("source schema does not match endpoint contract")
        positions = tuple(_DAILY_WIRE_FIELDS.index(field) for field in contract_fields)
        projected_pages = [
            (
                attempt,
                page,
                request_id,
                session_id,
                list(contract_fields),
                [[row[position] for position in positions] for row in rows],
            )
            for attempt, page, request_id, session_id, _page_fields, rows in captured_pages
        ]
        return list(contract_fields), projected_pages

    @staticmethod
    def _filter_requested_factor_rows(fields, captured_pages, requested_symbols):
        if "code" not in fields or "dividOperateDate" not in fields:
            raise ValueError("source schema does not match endpoint contract")
        code_position = fields.index("code")
        date_position = fields.index("dividOperateDate")
        expected = set(requested_symbols)
        filtered_pages = []
        for attempt, page, request_id, session_id, page_fields, rows in captured_pages:
            if any(len(row) != len(fields) for row in rows):
                raise ValueError("source schema does not match endpoint contract")
            filtered_pages.append(
                (
                    attempt,
                    page,
                    request_id,
                    session_id,
                    page_fields,
                    sorted(
                        (row for row in rows if row[code_position] in expected),
                        key=lambda row: (row[date_position], row[code_position]),
                    ),
                )
            )
        return filtered_pages

    @staticmethod
    def _projection(item, entries, *, default_kind):
        matches = [
            entry
            for entry in entries
            if entry["request_id"] == item.request_id
            and entry["provider_session_id"] == item.provider_session_id
            and entry["attempt"] == item.attempt
            and entry["page"] == item.page
        ]
        if len(matches) != 1:
            raise RuntimeError("transport observation has no unique capture registry scope")
        entry = matches[0]
        kind = entry["lineage_kind"]
        return TransportObservationProjection.from_observation_with_lineage(
            item,
            plan_ordinal=int(entry["plan_ordinal"]),
            lineage_kind=kind if kind in {"login_audit", "query_root", "page"} else default_kind,
        )

    @staticmethod
    def _typed_row_digest(logical, fields, rows):
        typed = [BaoStockProviderAdapter._typed_row(logical, fields, row) for row in rows]
        return sha256(
            json.dumps(
                [item.model_dump(mode="json") for item in typed],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def _query(self, endpoint, logical):
        iso_date = logical.start_date.isoformat()
        if endpoint is TransportEndpoint.TRADE_DATES:
            return self.client.query_trade_dates(
                start_date=iso_date, end_date=logical.end_date.isoformat()
            )
        if endpoint is TransportEndpoint.ALL_STOCK:
            return self.client.query_all_stock(day=iso_date)
        if endpoint is TransportEndpoint.DAILY_ASTOCK:
            return self.client.query_daily_history_k_AStock(date=iso_date)
        if endpoint is TransportEndpoint.DAILY_FACTOR:
            return self.client.query_daily_adjust_factor(date=iso_date)
        if endpoint is TransportEndpoint.ADJUST_FACTOR:
            return self.client.query_adjust_factor(
                logical.symbols[0], start_date="1990-01-01", end_date=iso_date
            )
        if endpoint is TransportEndpoint.INDEX_HISTORY:
            return self.client.query_history_k_data_plus(
                logical.symbols[0],
                DAILY_FIELDS,
                start_date=iso_date,
                end_date=logical.end_date.isoformat(),
                frequency="d",
                adjustflag="3",
            )
        raise ValueError("unsupported BaoStock endpoint")

    @staticmethod
    def _typed_row(logical, fields, row):
        values = dict(zip(fields, row, strict=True))
        stock_daily_shaped = logical.schema_variant == "daily_astock.v1" or (
            logical.schema_variant in {"index_history.session.v1", "index_history.range.v1"}
            and logical.instrument_role is InstrumentRole.STOCK
        )
        if stock_daily_shaped and values.get("tradestatus") != "1":
            for field in ("volume", "amount", "turn", "pctChg"):
                if values.get(field) == "":
                    values[field] = None
        values.update(
            endpoint=logical.endpoint,
            schema_variant=logical.schema_variant,
            request_role=logical.request_role,
            instrument_role=logical.instrument_role,
        )
        row_types = {
            "trade_dates.v1": TradeDatesRow,
            "all_stock.market.v1": AllStockRow,
            "daily_astock.v1": DailyAStockRow,
            "daily_factor.v1": DailyFactorRow,
            "adjust_factor.session.v1": AdjustFactorRow,
            "index_history.session.v1": IndexHistorySessionRow,
            "index_history.range.v1": IndexHistoryRangeRow,
        }
        try:
            return row_types[logical.schema_variant].model_validate(values)
        except KeyError:
            raise ValueError("unknown BaoStock row schema") from None

    def normalize(
        self, evidence: object, *, normalization_clock_utc: datetime
    ) -> tuple[DailyBar, ...]:
        # Runtime import keeps the provider contract independent of Task 8's
        # storage module while rejecting mutable/raw duck-typed objects.
        from backend.app.market.evidence import PublishedEvidence

        if not isinstance(evidence, PublishedEvidence):
            raise TypeError("normalization requires a published evidence reader")
        bars: list[DailyBar] = []
        for item in tuple(evidence.read_rows()):
            fields = tuple(item["fields"])
            if "date" not in fields or "adjustflag" not in fields:
                continue
            bars.extend(
                normalize_baostock_rows(
                    fields=fields,
                    rows=item["rows"],
                    factor_fields=item.get("factor_fields", ()),
                    factor_rows=item.get("factor_rows", ()),
                    ingested_at=normalization_clock_utc,
                )
            )
        return tuple(bars)


BaoStockDailyBarAdapter = BaoStockProviderAdapter
