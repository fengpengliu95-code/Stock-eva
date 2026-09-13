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
        market_data_dir=tmp_path / "market-data",
        user_data_dir=tmp_path / "user-data",
        provider_evidence_root=tmp_path / "provider-evidence",
        provider_shadow_root=tmp_path / "provider-shadow",
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


def test_replication_paths_reject_every_configured_mutable_root_alias(
    monkeypatch, tmp_path, capsys
):
    settings = _settings(tmp_path)
    roots = (
        settings.local_market_dataset_root,
        settings.local_control_dir,
        settings.local_staging_dir,
        settings.local_lock_dir,
        settings.local_temp_dir,
        settings.market_data_dir,
        settings.user_data_dir,
        settings.provider_evidence_root,
        settings.provider_shadow_root,
    )
    for index, root in enumerate(roots):
        root.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(cli, "get_settings", lambda settings=settings: settings)
        monkeypatch.setattr(
            "sys.argv",
            ["stock-eva", "market-replicate", "--destination", str(root / f"child-{index}")],
        )
        assert cli.main() == 2
        assert json.loads(capsys.readouterr().out)["reason_code"] == "INVALID_PATH"

    alias = tmp_path / "canonical-alias"
    alias.symlink_to(settings.local_market_dataset_root, target_is_directory=True)
    monkeypatch.setattr("sys.argv", ["stock-eva", "market-replicate", "--destination", str(alias)])
    assert cli.main() == 2
    assert json.loads(capsys.readouterr().out)["reason_code"] == "INVALID_PATH"


def test_ec38_all_mutable_root_symlink_ancestor_descendant_dry_runs_are_zero_write(
    monkeypatch, tmp_path, capsys
):
    """EC-38: every path-guard root rejects aliases and overlap before writes."""
    settings = _settings(tmp_path).model_copy(
        update={
            "nas_market_dataset_root": tmp_path / "nas-dataset",
            "supplemental_data_dir": tmp_path / "supplemental",
            "replication_destination_root": tmp_path / "destination",
        }
    )
    roots = tuple(
        root
        for root in (
            settings.local_market_dataset_root,
            settings.nas_market_dataset_root,
            settings.local_control_dir,
            settings.local_staging_dir,
            settings.local_lock_dir,
            settings.local_temp_dir,
            settings.market_data_dir,
            settings.user_data_dir,
            settings.supplemental_data_dir,
            settings.provider_evidence_root,
            settings.provider_shadow_root,
            settings.replication_destination_root,
        )
        if root is not None
    )
    for root in roots:
        root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cli, "get_settings", lambda: settings)

    def fingerprint() -> tuple:
        return tuple(
            (
                str(item.relative_to(tmp_path)),
                "symlink" if item.is_symlink() else "dir" if item.is_dir() else "file",
                item.lstat().st_ino,
                item.lstat().st_size,
                item.lstat().st_mtime_ns,
                None if item.is_dir() or item.is_symlink() else item.read_bytes(),
            )
            for item in sorted(tmp_path.rglob("*"))
        )

    def run_rejected(candidate: Path, before: tuple) -> None:
        for command in ("market-replicate", "market-replication-init"):
            monkeypatch.setattr(
                "sys.argv", ["stock-eva", command, "--destination", str(candidate), "--json"]
            )
            assert cli.main() == 2
            payload = json.loads(capsys.readouterr().out)
            assert payload == {
                "provider_requests": 0,
                "reason_code": "INVALID_PATH",
                "status": "error",
                "writes": False,
            }
            assert not candidate.exists()
            assert fingerprint() == before

    for index, root in enumerate(roots):
        direct = root / f"direct-{index}"
        before = fingerprint()
        run_rejected(direct, before)

        alias = tmp_path / f"alias-{index}"
        alias.symlink_to(root, target_is_directory=True)
        descendant = alias / "descendant"
        before = fingerprint()
        run_rejected(descendant, before)

        parent_alias = tmp_path / f"parent-alias-{index}"
        parent_alias.symlink_to(root.parent, target_is_directory=True)
        ancestor = parent_alias / root.name / "ancestor-child"
        before = fingerprint()
        run_rejected(ancestor, before)


def test_operation_day_is_typed_in_dry_run_and_status_json_is_sanitized(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(cli, "get_settings", lambda: _settings(tmp_path))
    monkeypatch.setattr(
        "sys.argv",
        [
            "stock-eva",
            "market-replicate",
            "--destination",
            "/tmp/r2f4-3-operation-day",
            "--operation-day",
            "2026-09-14",
            "--json",
        ],
    )
    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry_run"
    assert payload["operation_day"] == "2026-09-14"

    monkeypatch.setattr("sys.argv", ["stock-eva", "market-replication-status", "--json"])
    assert cli.main() == 0
    status = json.loads(capsys.readouterr().out)
    assert status["mode"] == "status"
    assert status["provider_requests"] == 0
    assert status["paths_exposed"] is False


def test_market_replicate_execute_calls_the_injected_worker_with_operation_day(
    monkeypatch, tmp_path, capsys
):
    destination = tmp_path / "archive"
    settings = _settings(tmp_path).model_copy(
        update={
            "replication_enabled": True,
            "replication_drain_enabled": True,
            "replication_destination_root": destination,
        }
    )
    calls = []

    class Worker:
        def run_once(self, *, operation_day=None):
            calls.append(operation_day)
            return {"status": "idle", "writes": False, "provider_requests": 0}

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "build_replication_drain_worker", lambda settings, layout: Worker())
    monkeypatch.setattr(
        "sys.argv",
        [
            "stock-eva",
            "market-replicate",
            "--destination",
            str(destination),
            "--operation-day",
            "2026-09-14",
            "--execute",
            "--json",
        ],
    )
    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "idle"
    assert payload["mode"] == "execute"
    assert payload["operation_day"] == "2026-09-14"
    assert calls == ["2026-09-14"]
    assert not destination.exists()
