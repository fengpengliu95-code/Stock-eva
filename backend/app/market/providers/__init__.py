"""Static provider contracts for the market ingestion boundary."""

from .baostock import BaoStockProviderAdapter
from .base import ProviderId, ProviderRequest

__all__ = ["BaoStockProviderAdapter", "ProviderId", "ProviderRequest"]
