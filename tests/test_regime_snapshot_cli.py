import json
import sys
from datetime import date

import backend.app.cli as cli
from backend.app.cli import build_parser
from backend.app.config import Settings


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
        json.loads(capsys.readouterr().out)["error_code"]
        == "regime_snapshot_capture_unavailable"
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
