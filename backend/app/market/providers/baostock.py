"""Typed BaoStock compatibility adapter at the existing transport boundary."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256

from backend.app.market.baostock import DAILY_FIELDS
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


class BaoStockProviderAdapter:
    provider_id = ProviderId.BAOSTOCK
    adapter_version: SafeVersion = "r2f2.v1"
    endpoint_contract_version: SafeVersion = "r2f2-endpoints.v1"

    def __init__(self, client: object | None = None, **kwargs: object) -> None:
        self._incumbent = IncumbentBaoStockProvider(client=client, **kwargs)
        self.client = self._incumbent.client
        self.max_attempts = self._incumbent.max_attempts

    def fetch(self, trade_date, symbols: Sequence[str] | None = None):
        return self._incumbent.fetch(trade_date, symbols)

    def fetch_range(self, start_date, end_date, *, symbols: Sequence[str]):
        return self._incumbent.fetch_range(start_date, end_date, symbols=symbols)

    def trading_dates(self, start_date, end_date):
        return self._incumbent.trading_dates(start_date, end_date)

    def inspect_main_board(self, trade_date):
        return self._incumbent.inspect_main_board(trade_date)

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
            TransportObservationProjection.from_observation_with_lineage(
                item, plan_ordinal=0, lineage_kind="login_audit"
            )
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
        local = tuple(
            item for item in raw_observations[observation_start:] if item.endpoint is endpoint
        )
        if tuple(fields) != contract.fields or any(
            tuple(page[4]) != contract.fields for page in captured_pages
        ):
            raise ValueError("source schema does not match endpoint contract")
        operations = tuple(item for item in local if item.protocol_stage is ProtocolStage.OPERATION)
        if not operations:
            raise RuntimeError("BaoStock logical request has no operation observation")
        root_ids = {
            attempt: next(page[2] for page in captured_pages if page[0] == attempt and page[1] == 1)
            for attempt in {item.attempt for item in operations}
        }
        local_projections = tuple(
            TransportObservationProjection.from_observation_with_lineage(
                item,
                plan_ordinal=logical.plan_ordinal,
                lineage_kind="query_root"
                if item.request_id == root_ids.get(item.attempt)
                else "page",
            )
            for item in local
        )
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
                provider_row_order_digest=sha256(
                    json.dumps(page[5], ensure_ascii=False, separators=(",", ":")).encode()
                ).hexdigest(),
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
        if not hasattr(evidence, "read_rows"):
            raise TypeError("normalization requires a published evidence reader")
        bars: list[DailyBar] = []
        for item in tuple(evidence.read_rows()):
            bars.extend(
                normalize_baostock_rows(
                    fields=item["fields"],
                    rows=item["rows"],
                    factor_fields=item.get("factor_fields", ()),
                    factor_rows=item.get("factor_rows", ()),
                    ingested_at=normalization_clock_utc,
                )
            )
        return tuple(bars)


BaoStockDailyBarAdapter = BaoStockProviderAdapter
