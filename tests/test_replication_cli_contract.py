"""Offline contract probes for the R2-F4.3 replication CLI surface."""

import json
from pathlib import Path

from backend.app import cli
from backend.app.config import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        local_market_dataset_root=tmp_path / "canonical",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        replication_enabled=False,
        replication_drain_enabled=False,
    )


def test_market_replicate_defaults_to_typed_zero_write_dry_run(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "get_settings", lambda: _settings(tmp_path))
    monkeypatch.setattr(
        "sys.argv",
        ["stock-eva", "market-replicate", "--destination", "/tmp/r2f4-3-cli-contract", "--json"],
    )
    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry_run"
    assert payload["writes"] is False
    assert payload["provider_requests"] == 0


def test_replication_cli_rejects_lexical_paths_before_any_write(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "get_settings", lambda: _settings(tmp_path))
    monkeypatch.setattr(
        "sys.argv", ["stock-eva", "market-replicate", "--destination", "${UNRESOLVED}"]
    )
    assert cli.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "provider_requests": 0,
        "reason_code": "INVALID_PATH",
        "status": "error",
        "writes": False,
    }


def test_replication_init_requires_exact_ack_and_dry_run_is_zero_write(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(cli, "get_settings", lambda: _settings(tmp_path))
    destination = tmp_path / "new-archive"
    monkeypatch.setattr(
        "sys.argv", ["stock-eva", "market-replication-init", "--destination", str(destination)]
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "dry_run"
    assert not destination.exists()

    monkeypatch.setattr(
        "sys.argv",
        [
            "stock-eva",
            "market-replication-init",
            "--destination",
            str(destination),
            "--execute",
        ],
    )
    assert cli.main() == 2
    assert not destination.exists()
