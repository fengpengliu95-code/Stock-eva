import pytest

from backend.app.storage.dataset import NasMarketStore


@pytest.fixture(autouse=True)
def explicit_legacy_lineage_for_pre_batch5_dataset_cases(request, monkeypatch):
    """Make existing legacy fixtures explicit without weakening Batch5 anchors."""

    if request.module.__name__.endswith("test_replication_integration_batch5"):
        return
    original_save_refresh = NasMarketStore.save_refresh
    original_upsert_bars = NasMarketStore.upsert_bars

    def save_refresh(store, bars, result, **kwargs):
        if kwargs.get("publish", result.status == "ready") and (
            "lineage_input" not in kwargs and "publication_lineage" not in kwargs
        ):
            kwargs["lineage_input"] = {"mode": "legacy"}
        return original_save_refresh(store, bars, result, **kwargs)

    def upsert_bars(store, bars, **kwargs):
        kwargs.setdefault("lineage_input", {"mode": "legacy"})
        return original_upsert_bars(store, bars, **kwargs)

    monkeypatch.setattr(NasMarketStore, "save_refresh", save_refresh)
    monkeypatch.setattr(NasMarketStore, "upsert_bars", upsert_bars)
