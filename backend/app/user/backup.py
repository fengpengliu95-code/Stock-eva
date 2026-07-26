"""Consistent, local-only snapshots of Stock EVA private SQLite data."""

import os
import shutil
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel


class PrivateBackupError(RuntimeError):
    pass


class PrivateBackupOutcome(BaseModel):
    status: str
    daily_backup: Path
    weekly_backup: Path
    integrity_check: str
    completed_at: datetime


class PrivateBackupService:
    """Create bounded daily/weekly SQLite snapshots on a local filesystem."""

    NETWORK_ROOTS = (Path("/Volumes"), Path("/Network"), Path("/net"))

    def __init__(
        self,
        source: Path,
        backup_root: Path,
        *,
        keep_daily: int = 7,
        keep_weekly: int = 4,
    ) -> None:
        if keep_daily < 1 or keep_weekly < 1:
            raise ValueError("backup retention counts must be positive")
        resolved_root = backup_root.expanduser().resolve()
        if any(resolved_root.is_relative_to(root) for root in self.NETWORK_ROOTS):
            raise PrivateBackupError("private backups require a local filesystem")
        resolved_source = source.expanduser().resolve()
        if any(resolved_source.is_relative_to(root) for root in self.NETWORK_ROOTS):
            raise PrivateBackupError("private database must remain on a local filesystem")
        self.source = resolved_source
        self.backup_root = resolved_root
        self.keep_daily = keep_daily
        self.keep_weekly = keep_weekly

    def run(self, now: datetime) -> PrivateBackupOutcome:
        if not self.source.is_file():
            raise PrivateBackupError("private SQLite source is unavailable")
        self.backup_root.mkdir(parents=True, exist_ok=True)
        prefix = self.source.stem
        daily = self.backup_root / f"{prefix}-daily-{now.date().isoformat()}.sqlite3"
        iso_year, iso_week, _ = now.date().isocalendar()
        weekly = self.backup_root / f"{prefix}-weekly-{iso_year}-W{iso_week:02d}.sqlite3"
        daily_stage = self._temporary_path()
        weekly_stage = self._temporary_path()
        try:
            self._backup_to(daily_stage)
            if self._integrity_check(daily_stage) != "ok":
                raise PrivateBackupError("daily backup integrity check failed")
            shutil.copyfile(daily_stage, weekly_stage)
            if self._integrity_check(weekly_stage) != "ok":
                raise PrivateBackupError("weekly backup integrity check failed")
            os.replace(daily_stage, daily)
            os.replace(weekly_stage, weekly)
            self._prune(f"{prefix}-daily-*.sqlite3", self.keep_daily)
            self._prune(f"{prefix}-weekly-*.sqlite3", self.keep_weekly)
        except PrivateBackupError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise PrivateBackupError("private SQLite backup failed") from exc
        finally:
            daily_stage.unlink(missing_ok=True)
            weekly_stage.unlink(missing_ok=True)
        return PrivateBackupOutcome(
            status="completed",
            daily_backup=daily,
            weekly_backup=weekly,
            integrity_check="ok",
            completed_at=now,
        )

    def _temporary_path(self) -> Path:
        descriptor, name = tempfile.mkstemp(
            prefix=".stock-eva-backup-",
            suffix=".partial",
            dir=self.backup_root,
        )
        os.close(descriptor)
        return Path(name)

    def _backup_to(self, target: Path) -> None:
        source_uri = f"{self.source.as_uri()}?mode=ro"
        source = sqlite3.connect(source_uri, uri=True, timeout=5)
        destination = sqlite3.connect(target, timeout=5)
        try:
            source.backup(destination)
        finally:
            destination.close()
            source.close()

    @staticmethod
    def _integrity_check(path: Path) -> str:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            row = connection.execute("PRAGMA integrity_check").fetchone()
        finally:
            connection.close()
        return str(row[0]) if row else "missing"

    def _prune(self, pattern: str, keep: int) -> None:
        snapshots = sorted(self.backup_root.glob(pattern))
        for path in snapshots[:-keep]:
            path.unlink()
