"""Static provider contracts for the market ingestion boundary."""

from .baostock import BaoStockDailyBarAdapter, BaoStockProviderAdapter
from .base import (
    ENDPOINT_CONTRACTS,
    DailyBarProvider,
    EndpointContract,
    ExpectedLogicalRequest,
    ExpectedLogicalRequestPlan,
    InstrumentRole,
    PaginationPolicy,
    ProviderEndpoint,
    ProviderId,
    ProviderRawBatch,
    ProviderRequest,
    RequestCompletion,
    RequestRole,
    SafeRelativePath,
    SafeVersion,
    validate_raw_date_binding,
    validate_safe_relative_path,
)

__all__ = [
    "BaoStockDailyBarAdapter",
    "BaoStockProviderAdapter",
    "DailyBarProvider",
    "ENDPOINT_CONTRACTS",
    "EndpointContract",
    "ExpectedLogicalRequest",
    "ExpectedLogicalRequestPlan",
    "InstrumentRole",
    "PaginationPolicy",
    "ProviderEndpoint",
    "ProviderId",
    "ProviderRawBatch",
    "ProviderRequest",
    "RequestCompletion",
    "RequestRole",
    "SafeRelativePath",
    "SafeVersion",
    "validate_raw_date_binding",
    "validate_safe_relative_path",
]
