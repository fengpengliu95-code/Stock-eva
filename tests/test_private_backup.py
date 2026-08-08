import hashlib
import json
import sqlite3
import stat
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.app.cli as cli
from backend.app.user.backup import (
    PrivateBackupError,
    PrivateBackupService,
    PrivateBackupSetService,
)
from backend.app.user.backup_cli import main as backup_main


def create_source(path: Path, value: str = "committed") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("CREATE TABLE private_data (value TEXT NOT NULL)")
        connection.execute("INSERT INTO private_data VALUES (?)", [value])
        connection.commit()
    finally:
        connection.close()


def read_value(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        return connection.execute("SELECT value FROM private_data").fetchone()[0]
    finally:
        connection.close()


def test_backup_uses_sqlite_snapshot_and_passes_integrity_check(tmp_path: Path) -> None:
    source = tmp_path / "user" / "stock_eva_user.sqlite3"
    create_source(source)
    service = PrivateBackupService(source, tmp_path / "backups")

    outcome = service.run(datetime(2026, 7, 24, 20, tzinfo=UTC))

    assert outcome.status == "completed"
    assert outcome.integrity_check == "ok"
    assert outcome.daily_backup.exists()
    assert outcome.weekly_backup.exists()
    assert read_value(outcome.daily_backup) == "committed"
    assert read_value(outcome.weekly_backup) == "committed"
    assert list((tmp_path / "backups").glob("*.partial*")) == []


def test_same_day_and_week_replace_named_snapshot_without_unbounded_growth(
    tmp_path: Path,
) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)
    backup_root = tmp_path / "backups"
    service = PrivateBackupService(source, backup_root)

    first = service.run(datetime(2026, 7, 24, 1, tzinfo=UTC))
    connection = sqlite3.connect(source)
    try:
        connection.execute("UPDATE private_data SET value = 'newer'")
        connection.commit()
    finally:
        connection.close()
    second = service.run(datetime(2026, 7, 24, 22, tzinfo=UTC))

    assert first.daily_backup == second.daily_backup
    assert first.weekly_backup == second.weekly_backup
    assert read_value(second.daily_backup) == "newer"
    assert len(list(backup_root.glob("*-daily-*.sqlite3"))) == 1
    assert len(list(backup_root.glob("*-weekly-*.sqlite3"))) == 1


def test_retention_keeps_latest_seven_daily_and_four_weekly(tmp_path: Path) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)
    backup_root = tmp_path / "backups"
    service = PrivateBackupService(source, backup_root)
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)

    for offset in range(70):
        service.run(start + timedelta(days=offset))

    daily = sorted(backup_root.glob("*-daily-*.sqlite3"))
    weekly = sorted(backup_root.glob("*-weekly-*.sqlite3"))
    assert len(daily) == 7
    assert len(weekly) == 4
    assert daily[-1].name.endswith("2026-03-11.sqlite3")
    assert weekly[-1].name.endswith("2026-W11.sqlite3")


def test_failed_integrity_check_does_not_replace_existing_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)
    backup_root = tmp_path / "backups"
    service = PrivateBackupService(source, backup_root)
    now = datetime(2026, 7, 24, 12, tzinfo=UTC)
    existing = service.run(now)
    original_bytes = existing.daily_backup.read_bytes()
    monkeypatch.setattr(service, "_integrity_check", lambda _path: "corrupt")

    with pytest.raises(PrivateBackupError, match="integrity"):
        service.run(now)

    assert existing.daily_backup.read_bytes() == original_bytes
    assert list(backup_root.glob("*.partial*")) == []


@pytest.mark.parametrize(
    "backup_root",
    [
        Path("/Volumes/Stock/private-backups"),
        Path("/Network/Servers/nas/private-backups"),
        Path("/net/nas/private-backups"),
    ],
)
def test_backup_rejects_network_volume_targets(
    tmp_path: Path,
    backup_root: Path,
) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)

    with pytest.raises(PrivateBackupError, match="local filesystem"):
        PrivateBackupService(source, backup_root)


def test_backup_rejects_private_database_on_network_volume(tmp_path: Path) -> None:
    with pytest.raises(PrivateBackupError, match="private database"):
        PrivateBackupService(
            Path("/Volumes/Stock/stock_eva_user.sqlite3"),
            tmp_path / "backups",
        )


def test_backup_cli_prints_safe_machine_readable_result(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)
    backup_root = tmp_path / "backups"

    exit_code = backup_main(
        [
            "--source",
            str(source),
            "--backup-root",
            str(backup_root),
            "--at",
            "2026-07-24T20:00:00+00:00",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert '"status": "completed"' in output
    assert str(source) not in output
    assert str(backup_root) not in output


def test_root_cli_backup_is_independent_of_market_storage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    user_dir = tmp_path / "user"
    source = user_dir / "stock_eva_user.sqlite3"
    portfolio = user_dir / "stock_eva_portfolio.sqlite3"
    create_source(source)
    create_source(portfolio, value="portfolio")
    backup_root = tmp_path / "backups"
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(
            user_data_dir=user_dir,
            user_database_name=source.name,
            portfolio_database_name=portfolio.name,
        ),
    )
    monkeypatch.setattr(
        cli.StoragePreflight,
        "inspect",
        lambda _self: (_ for _ in ()).throw(
            AssertionError("private backup must not inspect market storage")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "backup-private-data",
            "--backup-root",
            str(backup_root),
        ],
    )

    assert cli.main() == 0
    output = capsys.readouterr().out
    assert '"status": "completed"' in output
    assert '"database_status": {"portfolio": "backed_up", "user": "backed_up"}' in output
    assert len(list(backup_root.glob("stock-eva-private-daily-*.bundle"))) == 1
    assert len(list(backup_root.glob("stock-eva-private-weekly-*.bundle"))) == 1


def test_two_database_backup_is_one_manifest_bound_atomic_bundle(tmp_path: Path) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user, value="user-state")
    create_source(portfolio, value="portfolio-state")
    service = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        tmp_path / "backups",
    )

    outcome = service.run(datetime(2026, 7, 30, 4, tzinfo=timezone(timedelta(hours=8))))

    daily = PrivateBackupSetService.verify_bundle(outcome.daily_bundle)
    weekly = PrivateBackupSetService.verify_bundle(outcome.weekly_bundle)
    assert daily == weekly
    assert daily["bundle_id"] == outcome.bundle_id
    assert outcome.database_status == {"portfolio": "backed_up", "user": "backed_up"}
    assert outcome.completed_at == datetime(2026, 7, 29, 20, tzinfo=UTC)
    entries = {entry["role"]: entry for entry in daily["databases"]}
    assert stat.S_IMODE((tmp_path / "backups").stat().st_mode) == 0o700
    assert stat.S_IMODE(outcome.daily_bundle.stat().st_mode) == 0o700
    assert stat.S_IMODE(outcome.weekly_bundle.stat().st_mode) == 0o700
    assert stat.S_IMODE((outcome.daily_bundle / "manifest.json").stat().st_mode) == 0o600
    for role, value in (("user", "user-state"), ("portfolio", "portfolio-state")):
        snapshot = outcome.daily_bundle / entries[role]["snapshot_file"]
        assert read_value(snapshot) == value
        assert stat.S_IMODE(snapshot.stat().st_mode) == 0o600
        assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == entries[role]["sha256"]
        assert entries[role]["integrity_check"] == "ok"
    assert json.loads((outcome.daily_bundle / "manifest.json").read_text())["bundle_id"] == (
        outcome.bundle_id
    )
    assert list((tmp_path / "backups").glob(".stock-eva-private-set-*")) == []


def test_uninitialized_portfolio_is_explicit_in_bundle_not_fabricated(
    tmp_path: Path,
) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)

    outcome = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        tmp_path / "backups",
    ).run(datetime(2026, 7, 29, 20, tzinfo=UTC))

    manifest = PrivateBackupSetService.verify_bundle(outcome.daily_bundle)
    entries = {entry["role"]: entry for entry in manifest["databases"]}
    assert entries["user"]["status"] == "backed_up"
    assert outcome.status == "partial"
    assert manifest["completeness_status"] == "partial"
    assert entries["portfolio"] == {
        "role": "portfolio",
        "source_name": "stock_eva_portfolio.sqlite3",
        "status": "not_initialized",
        "snapshot_file": None,
        "sha256": None,
        "integrity_check": None,
    }
    assert portfolio.exists() is False


def test_backup_set_rejects_roles_resolving_to_the_same_database(tmp_path: Path) -> None:
    source = tmp_path / "user.sqlite3"
    create_source(source)

    with pytest.raises(ValueError, match="sources must be distinct"):
        PrivateBackupSetService(
            {"user": source, "portfolio": source.parent / "." / source.name},
            tmp_path / "backups",
        )


def test_backup_set_rejects_non_regular_optional_source(tmp_path: Path) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)
    portfolio.mkdir()

    with pytest.raises(PrivateBackupError, match="regular file"):
        PrivateBackupSetService(
            {"user": user, "portfolio": portfolio},
            tmp_path / "backups",
        ).run(datetime(2026, 7, 29, 20, tzinfo=UTC))


@pytest.mark.parametrize(
    "mutation",
    [
        "traversal",
        "duplicate_role",
        "extra_field",
        "invalid_completed_at",
        "non_utc_completed_at",
    ],
)
def test_backup_bundle_verifier_rejects_hostile_manifest_shapes(
    tmp_path: Path,
    mutation: str,
) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)
    create_source(portfolio)
    outcome = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        tmp_path / "backups",
    ).run(datetime(2026, 7, 29, 20, tzinfo=UTC))
    manifest_path = outcome.daily_bundle / "manifest.json"
    document = json.loads(manifest_path.read_text())
    if mutation == "traversal":
        document["databases"][0]["snapshot_file"] = "../outside.sqlite3"
    elif mutation == "duplicate_role":
        document["databases"][1]["role"] = document["databases"][0]["role"]
    elif mutation == "invalid_completed_at":
        document["completed_at"] = "not-a-timestamp"
    elif mutation == "non_utc_completed_at":
        document["completed_at"] = "2026-07-30T04:00:00+08:00"
    else:
        document["databases"][0]["unexpected"] = "value"
    core = {key: value for key, value in document.items() if key != "bundle_id"}
    document["bundle_id"] = PrivateBackupSetService._bundle_id(core)
    manifest_path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))

    with pytest.raises(PrivateBackupError, match="backup"):
        PrivateBackupSetService.verify_bundle(outcome.daily_bundle)


def test_backup_bundle_verifier_rejects_symlinked_bundle_root(tmp_path: Path) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)
    create_source(portfolio)
    outcome = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        tmp_path / "backups",
    ).run(datetime(2026, 7, 29, 20, tzinfo=UTC))
    alias = tmp_path / "bundle-alias"
    alias.symlink_to(outcome.daily_bundle, target_is_directory=True)

    with pytest.raises(PrivateBackupError, match="permissions"):
        PrivateBackupSetService.verify_bundle(alias)


def test_second_database_failure_preserves_prior_complete_bundles(
    tmp_path: Path,
) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)
    create_source(portfolio)
    backup_root = tmp_path / "backups"
    service = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        backup_root,
    )
    existing = service.run(datetime(2026, 7, 29, 20, tzinfo=UTC))
    prior_manifest = (existing.daily_bundle / "manifest.json").read_bytes()
    portfolio.write_bytes(b"not-sqlite")

    with pytest.raises(PrivateBackupError, match="backup set"):
        service.run(datetime(2026, 7, 30, 20, tzinfo=UTC))

    assert (existing.daily_bundle / "manifest.json").read_bytes() == prior_manifest
    assert PrivateBackupSetService.verify_bundle(existing.daily_bundle)["bundle_id"] == (
        existing.bundle_id
    )
    assert len(list(backup_root.glob("stock-eva-private-daily-*.bundle"))) == 1
    assert len(list(backup_root.glob("stock-eva-private-weekly-*.bundle"))) == 1
    assert list(backup_root.glob(".stock-eva-private-set-*")) == []


def test_backup_set_retention_is_applied_to_complete_bundles(tmp_path: Path) -> None:
    user = tmp_path / "user" / "stock_eva_user.sqlite3"
    portfolio = tmp_path / "user" / "stock_eva_portfolio.sqlite3"
    create_source(user)
    create_source(portfolio)
    backup_root = tmp_path / "backups"
    service = PrivateBackupSetService(
        {"user": user, "portfolio": portfolio},
        backup_root,
        keep_daily=3,
        keep_weekly=2,
    )

    for offset in range(10):
        service.run(datetime(2026, 1, 1, 12, tzinfo=UTC) + timedelta(days=offset))

    daily = sorted(backup_root.glob("stock-eva-private-daily-*.bundle"))
    weekly = sorted(backup_root.glob("stock-eva-private-weekly-*.bundle"))
    assert len(daily) == 3
    assert len(weekly) == 2
    assert all(PrivateBackupSetService.verify_bundle(path) for path in [*daily, *weekly])
