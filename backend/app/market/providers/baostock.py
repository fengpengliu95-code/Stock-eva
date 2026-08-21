"""BaoStock compatibility adapter.

The incumbent ``market.baostock.BaoStockProvider`` remains the transport and
normalization owner.  This adapter adds the typed provider boundary without
changing its retry, timeout, circuit or pagination behavior.
"""

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
from backend.app.market.provider_transport import (
    ProviderEndpoint as TransportEndpoint,
)

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
        """Preserve the incumbent caller contract during migration."""
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
        observations: list[TransportObservationProjection] = []
        summaries: list[EndpointContractSummary] = []

        raw_observations: list[TransportObservation] = []
        with transport_observation_sink(raw_observations.append):
            with self._incumbent.refresh_operation(request.refresh_id):
                self._incumbent._login(
                    TransportEndpoint.TRADE_DATES,
                    provider_session_id=request.provider_session_id,
                )
                try:
                    for logical in request.request_plan.requests:
                        (
                            batch,
                            completion,
                            logical_lineages,
                            logical_observations,
                            summary,
                        ) = self._fetch_logical(request, logical, raw_observations)
                        endpoint_batches.extend(batch)
                        completions.append(completion)
                        lineages.extend(logical_lineages)
                        observations.extend(logical_observations)
                        summaries.append(summary)
                finally:
                    self._incumbent._logout()

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
            logical_request_plan=request.request_plan,
            request_plan_hash=request.request_plan.request_plan_hash,
            endpoint_batches=tuple(endpoint_batches),
            request_completions=tuple(completions),
            completion_hash=completion_hash,
            transport_lineage=tuple(lineages),
            transport_observations=TransportObservationAggregate.from_observations(observations),
            endpoint_summaries=tuple(summaries),
        )
        for endpoint_batch in batch.endpoint_batches:
            validate_raw_date_binding(request, endpoint_batch)
        return batch

    def _fetch_logical(
        self,
        request: ProviderRequest,
        logical: ExpectedLogicalRequest,
        raw_observations: list[TransportObservation],
    ) -> tuple[
        tuple[RawEndpointBatch, ...],
        RequestCompletion,
        tuple[TransportLineageRef, ...],
        tuple[TransportObservationProjection, ...],
        EndpointContractSummary,
    ]:
        endpoint = TransportEndpoint(logical.endpoint.value)
        contract = endpoint_contract_for(
            logical.endpoint, logical.instrument_role, logical.schema_variant
        )
        captured_pages: list[tuple[int, int, str, str, list[str], list[list[str]]]] = []
        observation_start = len(raw_observations)

        def operation():
            return self._query(endpoint, logical)

        fields, rows = self._incumbent._read(
            endpoint,
            operation,
            page_capture=lambda attempt, page, request_id, session_id, page_fields, page_rows: (
                captured_pages.append(
                    (attempt, page, request_id, session_id, page_fields, page_rows)
                )
            ),
            provider_session_id=request.provider_session_id,
            capture_all_operation_outcomes=True,
        )
        if tuple(fields) != contract.fields or any(
            tuple(page_fields) != contract.fields for _, _, _, _, page_fields, _ in captured_pages
        ):
            raise ValueError("source schema does not match endpoint contract")
        logical_observations = tuple(
            item for item in raw_observations[observation_start:] if item.endpoint is endpoint
        )
        if not logical_observations:
            raise RuntimeError("BaoStock transport observation was not captured")
        operation_observations = tuple(
            item for item in logical_observations if item.protocol_stage is ProtocolStage.OPERATION
        )
        successful = tuple(
            item for item in operation_observations if item.outcome is TransportOutcome.SUCCESS
        )
        if not successful:
            raise RuntimeError("BaoStock logical request has no successful observation")
        successful_attempt = max(item.attempt for item in successful)
        successful_pages = tuple(item for item in captured_pages if item[0] == successful_attempt)
        if not successful_pages:
            raise RuntimeError("BaoStock successful request has no captured source page")
        successful_fields = successful_pages[0][4]
        successful_rows = [row for *_, page_rows in successful_pages for row in page_rows]
        if successful_fields != fields or successful_rows != rows:
            raise RuntimeError("BaoStock source capture disagrees with normalized read result")
        projections = tuple(
            TransportObservationProjection.from_observation(item) for item in logical_observations
        )
        lineages = tuple(
            TransportLineageRef(
                refresh_id=item.refresh_id,
                provider_session_id=item.provider_session_id,
                request_id=item.request_id,
                endpoint=logical.endpoint,
                plan_ordinal=logical.plan_ordinal,
                attempt=item.attempt,
                page=item.page,
                observation_digest=projection.observation_digest,
            )
            for item, projection in zip(logical_observations, projections, strict=True)
        )

        def page_lineage(captured: tuple[int, int, str, str, list[str], list[list[str]]]):
            attempt, page, request_id, session_id, _, _ = captured
            candidates = [
                (item, lineage)
                for item, lineage in zip(logical_observations, lineages, strict=True)
                if item.request_id == request_id
                and item.provider_session_id == session_id
                and item.attempt == attempt
                and item.page == page
                and item.protocol_stage
                in {ProtocolStage.OPERATION, ProtocolStage.COMPLETE, ProtocolStage.PAGINATION}
            ]
            if not candidates:
                raise RuntimeError("BaoStock source page has no transport observation")
            operation_candidates = [
                lineage
                for item, lineage in candidates
                if item.protocol_stage is ProtocolStage.OPERATION
            ]
            if len(operation_candidates) > 1:
                raise RuntimeError("BaoStock source page has duplicate operation observations")
            if operation_candidates:
                return operation_candidates[0]
            if len(candidates) != 1:
                raise RuntimeError("BaoStock source page has ambiguous terminal observations")
            return candidates[0][1]

        endpoint_batches = tuple(
            RawEndpointBatch(
                endpoint=logical.endpoint,
                schema_variant=logical.schema_variant,
                request_role=logical.request_role,
                instrument_role=logical.instrument_role,
                plan_ordinal=logical.plan_ordinal,
                shard_id=logical.shard_id,
                lineage=page_lineage(captured),
                rows=tuple(self._typed_row(logical, fields, row) for row in page_rows),
                row_count=len(page_rows),
                source_schema=contract.schema_variant,
                fields=contract.fields,
                units=contract.units,
                date_semantics=contract.date_semantics,
                pagination_policy=contract.pagination_policy,
                provider_row_order_digest=sha256(
                    json.dumps(page_rows, ensure_ascii=False, separators=(",", ":")).encode()
                ).hexdigest(),
            )
            for captured in successful_pages
            for page_rows in (captured[5],)
        )
        attempts: list[AttemptCompletion] = []
        for attempt_number in sorted({item.attempt for item in operation_observations}):
            attempt_observations = tuple(
                item for item in operation_observations if item.attempt == attempt_number
            )
            attempt_success = any(
                item.outcome is TransportOutcome.SUCCESS for item in attempt_observations
            )
            attempt_pages = tuple(
                sorted(
                    {
                        page
                        for attempt, page, _, _, _, _ in captured_pages
                        if attempt == attempt_number
                    }
                )
            )
            attempt_observation = attempt_observations[-1]
            attempts.append(
                AttemptCompletion(
                    plan_ordinal=logical.plan_ordinal,
                    attempt=attempt_number,
                    request_id=attempt_observation.request_id,
                    observed_pages=attempt_pages,
                    observed_page_count=len(attempt_pages),
                    terminal=attempt_success,
                    outcome=(attempt_observation.outcome),
                )
            )
        final_observation = max(
            successful,
            key=lambda item: (item.attempt, item.page, item.observed_at),
        )
        completion = RequestCompletion(
            plan_ordinal=logical.plan_ordinal,
            attempts=tuple(attempts),
            successful_attempt=final_observation.attempt,
            successful_request_id=final_observation.request_id,
            final_outcome=TransportOutcome.SUCCESS,
            row_count=sum(item.row_count for item in endpoint_batches),
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
            row_count=sum(item.row_count for item in endpoint_batches),
        )
        return endpoint_batches, completion, lineages, projections, summary

    def _query(self, endpoint: TransportEndpoint, logical: ExpectedLogicalRequest):
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
    def _typed_row(logical: ExpectedLogicalRequest, fields, row):
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
        row_type = row_types.get(logical.schema_variant)
        if row_type is None:
            raise ValueError("unknown BaoStock row schema")
        return row_type.model_validate(values)

    def normalize(
        self, evidence: object, *, normalization_clock_utc: datetime
    ) -> tuple[DailyBar, ...]:
        if not hasattr(evidence, "read_rows"):
            raise TypeError("normalization requires a published evidence reader")
        payload = evidence.read_rows()
        if not isinstance(payload, tuple):
            payload = tuple(payload)
        # Evidence readers expose the incumbent normalizer's already validated
        # source arguments.  The adapter never accepts a live SDK result here.
        bars: list[DailyBar] = []
        for item in payload:
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


# A descriptive alias keeps the protocol name discoverable without admitting
# another provider implementation.
BaoStockDailyBarAdapter = BaoStockProviderAdapter
