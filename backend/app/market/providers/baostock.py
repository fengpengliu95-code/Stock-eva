"""BaoStock compatibility adapter.

The incumbent ``market.baostock.BaoStockProvider`` remains the transport and
normalization owner.  This adapter adds the typed provider boundary without
changing its retry, timeout, circuit or pagination behavior.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from backend.app.market.baostock import BaoStockProvider as IncumbentBaoStockProvider
from backend.app.market.models import DailyBar

from .base import ProviderId, ProviderRawBatch, ProviderRequest, SafeVersion


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
        """Capture a complete typed batch through an injected raw client.

        The incumbent SDK has no stable public raw-result object.  During the
        transition a transport-aware client may provide ``fetch_raw``; an
        ordinary BaoStock client must continue using the legacy ``fetch`` API
        until evidence publication owns this capture orchestration.
        """
        raw_fetch = getattr(type(self.client), "fetch_raw", None)
        if not callable(raw_fetch):
            raise NotImplementedError("raw capture requires the evidence-aware client boundary")
        result = raw_fetch(self.client, request)
        if not isinstance(result, ProviderRawBatch):
            raise TypeError("raw provider client returned an invalid ProviderRawBatch")
        return result

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
        from backend.app.market.normalize import normalize_baostock_rows

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
