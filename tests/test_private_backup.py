import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.app.cli as cli
from backend.app.user.backup import PrivateBackupError, PrivateBackupService
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
    create_source(source)
    backup_root = tmp_path / "backups"
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(
            user_data_dir=user_dir,
            user_database_name=source.name,
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
    assert len(list(backup_root.glob("*-daily-*.sqlite3"))) == 1
