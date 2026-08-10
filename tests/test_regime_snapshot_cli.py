import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import backend.app.cli as cli
from backend.app.cli import build_parser
from backend.app.config import Settings
from backend.app.market.models import DailyBar
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore


def test_snapshot_cli_defaults_to_read_only_plan() -> None:
    args = build_parser().parse_args(
        [
            "market-regime-snapshots",
            "--start",
            "2026-07-01",
            "--end",
            "2026-07-31",
        ]
    )

    assert args.command == "market-regime-snapshots"
    assert args.start == date(2026, 7, 1)
    assert args.end == date(2026, 7, 31)
    assert args.execute is False


def test_snapshot_cli_rejects_inverted_range_before_market_reader(
    tmp_path, monkeypatch, capsys
) -> None:
    calls = 0
    settings = Settings(local_control_dir=tmp_path / "control")

    def unexpected_reader(_settings):
        nonlocal calls
        calls += 1
        raise AssertionError("invalid range reached market reader")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "market_regime_store_from_settings", unexpected_reader)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-regime-snapshots",
            "--start",
            "2026-07-31",
            "--end",
            "2026-07-01",
        ],
    )

    assert cli.main() == 1
    assert calls == 0
    assert (
        json.loads(capsys.readouterr().out)["error_code"] == "regime_snapshot_capture_unavailable"
    )
    assert not settings.local_control_dir.exists()


def test_snapshot_cli_suppresses_constructor_noise_and_secrets(
    tmp_path, monkeypatch, capsys
) -> None:
    settings = Settings(local_control_dir=tmp_path / "control")

    def noisy_reader(_settings):
        print("/private/dataset super-secret")
        raise cli.MarketReadUnavailable("market_storage_unavailable")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "market_regime_store_from_settings", noisy_reader)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-regime-snapshots",
            "--start",
            "2026-07-01",
            "--end",
            "2026-07-31",
        ],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "/private" not in captured.out
    assert "super-secret" not in captured.out
    assert json.loads(captured.out)["error_code"] == "regime_snapshot_capture_unavailable"


def test_snapshot_cli_sanitizes_unexpected_noisy_constructor_failure(
    tmp_path, monkeypatch, capsys
) -> None:
    settings = Settings(local_control_dir=tmp_path / "control")

    def noisy_reader(_settings):
        print("/private/query unexpected-secret")
        raise RuntimeError("unexpected /private/query unexpected-secret")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "market_regime_store_from_settings", noisy_reader)
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "market-regime-snapshots", "--start", "2026-07-01", "--end", "2026-07-31"],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "unexpected-secret" not in captured.out
    assert "/private" not in captured.out
    assert json.loads(captured.out)["error_code"] == "regime_snapshot_capture_unavailable"


def _published_settings(tmp_path: Path) -> tuple[Settings, tuple[date, ...]]:
    dates = (date(2026, 7, 22), date(2026, 7, 23))
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()
    (dataset_root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (dataset_root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        local_control_dir=tmp_path / "runtime" / "control",
        market_data_dir=tmp_path / "runtime" / "market",
        local_staging_dir=tmp_path / "runtime" / "staging",
        local_lock_dir=tmp_path / "runtime" / "locks",
        local_temp_dir=tmp_path / "runtime" / "temp",
        user_data_dir=tmp_path / "runtime" / "user",
        local_market_dataset_root=dataset_root,
        nas_market_dataset_root=None,
    )
    bars = [
        DailyBar(
            trade_date=item,
            symbol="sh.600000",
            security_type="stock",
            exchange="sh",
            board="main",
            open=10,
            high=10,
            low=10,
            close=10,
            preclose=10,
            volume=1,
            amount=1,
            turnover_rate=1,
            pct_change=0,
            adjust_factor=1,
            is_trading=True,
            is_suspended=False,
            is_st=False,
            source_record_id=f"fixture-{item}",
            ingested_at=datetime(2026, 7, 24, tzinfo=UTC),
            quality_status="ready",
        )
        for item in dates
    ]
    NasMarketStore(
        MarketStore(settings.market_data_dir / settings.market_database_name),
        dataset_root,
        settings.local_staging_dir,
    ).upsert_bars(bars)
    return settings, dates


def _snapshot_tree(root: Path) -> dict[str, bytes]:
    if not root.exists():
        return {}
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_snapshot_cli_dry_run_and_repeat_execute_are_exact_and_idempotent(
    tmp_path, monkeypatch, capsys
) -> None:
    settings, dates = _published_settings(tmp_path)
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    arguments = [
        "stock-eva",
        "market-regime-snapshots",
        "--start",
        dates[0].isoformat(),
        "--end",
        dates[-1].isoformat(),
    ]
    before = _snapshot_tree(settings.local_control_dir)
    monkeypatch.setattr(sys, "argv", arguments)

    assert cli.main() == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["status"] == "dry-run"
    assert dry_run["selected_dates"] == [item.isoformat() for item in dates]
    assert dry_run["writes_snapshot_data"] is False
    assert _snapshot_tree(settings.local_control_dir) == before

    monkeypatch.setattr(sys, "argv", [*arguments, "--execute"])
    assert cli.main() == 0
    first = json.loads(capsys.readouterr().out)
    database = settings.local_control_dir / settings.regime_snapshot_database_name
    first_bytes = database.read_bytes()
    first_inventory = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in database.parent.glob(f"{database.name}*")
    }
    assert first["inserted"] == len(dates)
    assert first["writes_snapshot_data"] is True

    assert cli.main() == 0
    second = json.loads(capsys.readouterr().out)
    assert second["inserted"] == 0
    assert second["idempotent"] == len(dates)
    assert second["writes_snapshot_data"] is False
    assert database.read_bytes() == first_bytes
    assert {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in database.parent.glob(f"{database.name}*")
    } == first_inventory


def test_snapshot_cli_rejects_outside_manifest_without_snapshot_store(
    tmp_path, monkeypatch, capsys
) -> None:
    settings, _dates = _published_settings(tmp_path)
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-regime-snapshots",
            "--start",
            "2026-07-01",
            "--end",
            "2026-07-02",
        ],
    )

    assert cli.main() == 1
    assert (
        json.loads(capsys.readouterr().out)["error_code"] == "regime_snapshot_capture_unavailable"
    )
    assert not (settings.local_control_dir / settings.regime_snapshot_database_name).exists()
