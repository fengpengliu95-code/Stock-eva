import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market import supplement_cli
from backend.app.market.akshare_supplemental import (
    AKShareSupplementalProvider,
    SupplementalDataError,
    SupplementalSourceUnavailableError,
)
from backend.app.market.supplement_ingestion import (
    SupplementalDatasetInitializer,
    SupplementalIngestionRequest,
    SupplementalIngestionService,
    SupplementalStore,
)
from backend.app.storage.preflight import MountInfo

OBSERVED_AT = datetime(2026, 7, 24, 18, 30, tzinfo=UTC)


class FakeAKShareClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def stock_industry_clf_hist_sw(self):
        self.calls.append("stock_industry_clf_hist_sw")
        return [
            {
                "symbol": "600000",
                "start_date": date(2021, 12, 13),
                "industry_code": "801780",
                "industry_name": "银行",
                "update_time": date(2024, 1, 2),
            }
        ]

    def stock_market_fund_flow(self):
        self.calls.append("stock_market_fund_flow")
        return [
            {
                "日期": date(2026, 7, 24),
                "上证-收盘价": 3520.0,
                "主力净流入-净额": 2_000_000_000,
                "主力净流入-净占比": 2.0,
                "超大单净流入-净额": 1_200_000_000,
                "超大单净流入-净占比": 1.2,
                "大单净流入-净额": 800_000_000,
                "大单净流入-净占比": 0.8,
                "中单净流入-净额": -500_000_000,
                "中单净流入-净占比": -0.5,
                "小单净流入-净额": -1_500_000_000,
                "小单净流入-净占比": -1.5,
            }
        ]

    def stock_sector_fund_flow_hist(self, *, symbol: str):
        self.calls.append(f"stock_sector_fund_flow_hist:{symbol}")
        return [
            {
                "日期": date(2026, 7, 24),
                "主力净流入-净额": 300_000_000,
                "主力净流入-净占比": 3.0,
                "超大单净流入-净额": 200_000_000,
                "超大单净流入-净占比": 2.0,
                "大单净流入-净额": 100_000_000,
                "大单净流入-净占比": 1.0,
                "中单净流入-净额": -50_000_000,
                "中单净流入-净占比": -0.5,
                "小单净流入-净额": -250_000_000,
                "小单净流入-净占比": -2.5,
            }
        ]


def provider(client: FakeAKShareClient) -> AKShareSupplementalProvider:
    return AKShareSupplementalProvider(
        client=client,
        clock=lambda: OBSERVED_AT,
    )


def request() -> SupplementalIngestionRequest:
    return SupplementalIngestionRequest(
        through_date=date(2026, 7, 24),
        include_market_flow=True,
        include_classification=True,
        sector_names=["银行"],
        canary=True,
    )


def initialized_store(tmp_path: Path) -> SupplementalStore:
    root = tmp_path / "dataset"
    SupplementalDatasetInitializer(root, require_smb=False).initialize()
    return SupplementalStore(root, tmp_path / "audit.sqlite3")


class FakeMountInspector:
    def __init__(self, mount: MountInfo | None) -> None:
        self.mount = mount

    def find_mount(self, _path: Path) -> MountInfo | None:
        return self.mount


def test_explicit_initializer_requires_smb_and_is_idempotent(tmp_path: Path) -> None:
    share = tmp_path / "Stock"
    share.mkdir()
    root = share / "stock-eva-supplemental"
    initializer = SupplementalDatasetInitializer(
        root,
        require_smb=True,
        mount_inspector=FakeMountInspector(
            MountInfo(
                mount_point=share,
                filesystem_type="smbfs",
                options=("nodev",),
            )
        ),
    )

    assert initializer.initialize() == root
    first_manifest = (root / "manifest.json").read_bytes()
    assert initializer.initialize() == root

    assert (root / ".stock-eva-supplemental.json").is_file()
    assert (root / "manifest.json").read_bytes() == first_manifest


def test_missing_nas_mount_cli_initialization_performs_zero_nas_writes(
    tmp_path: Path,
    capsys,
) -> None:
    unmounted = tmp_path / "not-mounted"
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=unmounted / "stock-eva-market",
        supplemental_data_dir=unmounted / "stock-eva-supplemental",
    )

    exit_code = supplement_cli.run(["--initialize"], settings=settings)

    assert exit_code == 4
    assert json.loads(capsys.readouterr().out)["error_code"] == (
        "SUPPLEMENTAL_INITIALIZATION_FAILED"
    )
    assert unmounted.exists() is False


def test_execute_refuses_uninitialized_local_root_before_provider_load(
    tmp_path: Path,
    capsys,
) -> None:
    settings = api_settings(tmp_path, enabled=True)

    exit_code = supplement_cli.run(
        [
            "--through-date",
            "2026-07-24",
            "--market-flow",
            "--canary",
            "--execute",
        ],
        settings=settings,
    )

    assert exit_code == 4
    assert json.loads(capsys.readouterr().out)["error_code"] == ("SUPPLEMENTAL_PUBLISH_FAILED")
    assert settings.supplemental_data_dir.exists() is False
    assert settings.local_control_dir.exists() is False


def test_plan_is_network_free_and_execute_requires_explicit_canary(tmp_path: Path) -> None:
    client = FakeAKShareClient()
    service = SupplementalIngestionService(
        initialized_store(tmp_path),
        provider(client),
    )

    plan = service.plan(request())

    assert plan["mode"] == "dry_run"
    assert plan["datasets"] == [
        "industry_classification",
        "market_fund_flow",
        "sector_fund_flow",
    ]
    assert client.calls == []
    with pytest.raises(ValueError, match="canary"):
        service.execute(request().model_copy(update={"canary": False}))
    assert client.calls == []


def test_execute_publishes_validated_immutable_parquet_and_is_idempotent(
    tmp_path: Path,
) -> None:
    client = FakeAKShareClient()
    store = initialized_store(tmp_path)
    service = SupplementalIngestionService(store, provider(client))

    first = service.execute(request())
    second = service.execute(request())

    assert first.status == "ready"
    assert second == first
    assert client.calls == [
        "stock_industry_clf_hist_sw",
        "stock_market_fund_flow",
        "stock_sector_fund_flow_hist:银行",
    ]
    manifest = json.loads((tmp_path / "dataset" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["dataset"] == "stock-eva-supplemental"
    assert {item["dataset_kind"] for item in manifest["files"]} == {
        "industry_classification",
        "market_fund_flow",
        "sector_fund_flow",
    }
    assert all("_staging" not in item["path"] for item in manifest["files"])
    assert all(len(item["sha256"]) == 64 for item in manifest["files"])
    paths = [tmp_path / "dataset" / item["path"] for item in manifest["files"]]
    assert all(path.is_file() for path in paths)

    connection = duckdb.connect(":memory:")
    try:
        columns = {
            row[0]
            for row in connection.execute(
                """
                DESCRIBE SELECT * FROM read_parquet(
                    ?, hive_partitioning = false
                )
                """,
                [[str(path) for path in paths]],
            ).fetchall()
        }
    finally:
        connection.close()
    assert {"source", "upstream", "endpoint", "observed_at", "trade_date"} <= columns
    assert not {"open", "high", "low", "close", "volume", "amount"} & columns
    assert {item.status for item in store.list_runs()} == {"ready"}


class MissingDateClient(FakeAKShareClient):
    def stock_market_fund_flow(self):
        self.calls.append("stock_market_fund_flow")
        return [{"日期": None, "主力净流入-净额": 1}]


class UnavailableClient(FakeAKShareClient):
    def stock_market_fund_flow(self):
        self.calls.append("stock_market_fund_flow")
        raise ConnectionError("upstream detail must not enter audit")


def test_provider_failure_keeps_manifest_and_uses_safe_unavailable_code(
    tmp_path: Path,
) -> None:
    store = initialized_store(tmp_path)
    service = SupplementalIngestionService(
        store,
        provider(UnavailableClient()),
    )
    market_only = request().model_copy(
        update={
            "include_classification": False,
            "sector_names": [],
        }
    )
    before = (store.root / "manifest.json").read_bytes()

    with pytest.raises(SupplementalSourceUnavailableError):
        service.execute(market_only)

    assert (store.root / "manifest.json").read_bytes() == before
    latest = store.list_runs(limit=1)[0]
    assert latest.status == "error"
    assert latest.error_code == "SUPPLEMENTAL_SOURCE_UNAVAILABLE"
    assert "upstream detail" not in latest.model_dump_json()


def test_failed_batch_keeps_previous_manifest_and_records_safe_error(
    tmp_path: Path,
) -> None:
    store = initialized_store(tmp_path)
    good = SupplementalIngestionService(store, provider(FakeAKShareClient()))
    good.execute(request())
    before = (tmp_path / "dataset" / "manifest.json").read_bytes()

    bad_request = request().model_copy(update={"through_date": date(2026, 7, 25)})
    bad = SupplementalIngestionService(store, provider(MissingDateClient()))
    with pytest.raises(SupplementalDataError, match="explicit source date"):
        bad.execute(bad_request)

    assert (tmp_path / "dataset" / "manifest.json").read_bytes() == before
    latest = store.list_runs(limit=1)[0]
    assert latest.status == "error"
    assert latest.error_code == "SUPPLEMENTAL_SOURCE_INVALID"
    assert "_staging" not in latest.error_code


def api_settings(tmp_path: Path, *, enabled: bool) -> Settings:
    return Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        supplemental_data_dir=tmp_path / "supplemental",
        akshare_supplemental_enabled=enabled,
    )


@pytest.mark.anyio
async def test_get_reads_only_published_snapshot_without_loading_provider(
    tmp_path: Path,
) -> None:
    settings = api_settings(tmp_path, enabled=True)
    store = SupplementalStore(
        settings.supplemental_data_dir,
        settings.local_control_dir / settings.supplemental_audit_database_name,
    )
    SupplementalDatasetInitializer(
        settings.supplemental_data_dir,
        require_smb=False,
    ).initialize()
    SupplementalIngestionService(
        store,
        provider(FakeAKShareClient()),
    ).execute(request())
    app.dependency_overrides[get_settings] = lambda: settings
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/api/v1/market/supplemental")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["as_of"] == "2026-07-24"
    assert payload["market_flow"]["reported_main_net_inflow"] == 2_000_000_000
    assert payload["sector_flows"][0]["scope_name"] == "银行"
    assert payload["industry_classifications"][0]["source_symbol"] == "600000"
    assert payload["quality_issues"] == []
