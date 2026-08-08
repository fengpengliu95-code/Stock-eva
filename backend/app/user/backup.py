"""Consistent, local-only snapshots of Stock EVA private SQLite data."""

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
from datetime import UTC, datetime
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


class PrivateBackupSetOutcome(BaseModel):
    status: str
    bundle_id: str
    daily_bundle: Path
    weekly_bundle: Path
    database_status: dict[str, str]
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
        os.chmod(self.backup_root, 0o700)
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
            self._remove_temporary_artifacts(daily_stage)
            self._remove_temporary_artifacts(weekly_stage)
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

    @staticmethod
    def _remove_temporary_artifacts(path: Path) -> None:
        path.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            path.with_name(f"{path.name}{suffix}").unlink(missing_ok=True)

    def _backup_to(self, target: Path) -> None:
        source_uri = f"{self.source.as_uri()}?mode=ro"
        source = sqlite3.connect(source_uri, uri=True, timeout=5)
        destination = sqlite3.connect(target, timeout=5)
        try:
            source.backup(destination)
            mode = destination.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
            if str(mode).lower() != "delete":
                raise PrivateBackupError("backup snapshot requires rollback journal mode")
            destination.commit()
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


class PrivateBackupSetService:
    """Atomically publish one manifest-bound backup set for both private databases."""

    SCHEMA_VERSION = 1
    EXPECTED_ROLES = frozenset({"user", "portfolio"})
    NETWORK_ROOTS = PrivateBackupService.NETWORK_ROOTS

    def __init__(
        self,
        sources: dict[str, Path],
        backup_root: Path,
        *,
        required_roles: frozenset[str] = frozenset({"user"}),
        keep_daily: int = 7,
        keep_weekly: int = 4,
    ) -> None:
        if keep_daily < 1 or keep_weekly < 1:
            raise ValueError("backup retention counts must be positive")
        if set(sources) != self.EXPECTED_ROLES:
            raise ValueError("backup set requires user and portfolio roles")
        if not required_roles <= set(sources):
            raise ValueError("required backup roles must have configured sources")
        if any(not role for role in sources):
            raise ValueError("backup roles must be unique and non-empty")
        resolved_root = backup_root.expanduser().resolve()
        if self._is_network_path(resolved_root):
            raise PrivateBackupError("private backups require a local filesystem")
        resolved_sources = {role: source.expanduser().resolve() for role, source in sources.items()}
        if len(set(resolved_sources.values())) != len(resolved_sources):
            raise ValueError("private database sources must be distinct")
        if any(self._is_network_path(source) for source in resolved_sources.values()):
            raise PrivateBackupError("private database must remain on a local filesystem")
        self.sources = resolved_sources
        self.required_roles = required_roles
        self.backup_root = resolved_root
        self.keep_daily = keep_daily
        self.keep_weekly = keep_weekly

    @classmethod
    def _is_network_path(cls, path: Path) -> bool:
        return any(path.is_relative_to(root) for root in cls.NETWORK_ROOTS)

    def run(self, now: datetime) -> PrivateBackupSetOutcome:
        invalid_optional = [
            role
            for role, source in sorted(self.sources.items())
            if source.exists() and not source.is_file()
        ]
        if invalid_optional:
            raise PrivateBackupError("configured private SQLite source is not a regular file")
        missing_required = [
            role for role in sorted(self.required_roles) if not self.sources[role].is_file()
        ]
        if missing_required:
            raise PrivateBackupError("required private SQLite source is unavailable")
        self.backup_root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.backup_root, 0o700)
        stage = Path(
            tempfile.mkdtemp(
                prefix=".stock-eva-private-set-",
                dir=self.backup_root,
            )
        )
        weekly_stage = stage.with_name(f"{stage.name}-weekly")
        published: list[Path] = []
        try:
            core = self._build_stage(stage, now)
            bundle_id = self._bundle_id(core)
            manifest = {**core, "bundle_id": bundle_id}
            self._write_manifest(stage / "manifest.json", manifest)
            self.verify_bundle(stage)
            shutil.copytree(stage, weekly_stage)
            self.verify_bundle(weekly_stage)

            normalized_now = (
                now.replace(tzinfo=UTC) if now.utcoffset() is None else now.astimezone(UTC)
            )
            day = normalized_now.date().isoformat()
            iso_year, iso_week, _ = normalized_now.date().isocalendar()
            stamp = normalized_now.strftime("%Y%m%dT%H%M%S%fZ")
            short_id = bundle_id[:12]
            daily = self.backup_root / (f"stock-eva-private-daily-{day}-{stamp}-{short_id}.bundle")
            weekly = self.backup_root / (
                f"stock-eva-private-weekly-{iso_year}-W{iso_week:02d}-{stamp}-{short_id}.bundle"
            )
            for source_stage, target in ((stage, daily), (weekly_stage, weekly)):
                if target.exists():
                    existing = self.verify_bundle(target)
                    if existing["bundle_id"] != bundle_id:
                        raise PrivateBackupError("backup bundle identity collision")
                    shutil.rmtree(source_stage)
                    continue
                os.replace(source_stage, target)
                published.append(target)
            self._prune("stock-eva-private-daily-*.bundle", self.keep_daily)
            self._prune("stock-eva-private-weekly-*.bundle", self.keep_weekly)
        except PrivateBackupError:
            for path in published:
                shutil.rmtree(path, ignore_errors=True)
            raise
        except (OSError, sqlite3.Error, ValueError, TypeError, json.JSONDecodeError) as exc:
            for path in published:
                shutil.rmtree(path, ignore_errors=True)
            raise PrivateBackupError("private SQLite backup set failed") from exc
        finally:
            shutil.rmtree(stage, ignore_errors=True)
            shutil.rmtree(weekly_stage, ignore_errors=True)
        return PrivateBackupSetOutcome(
            status=(
                "completed"
                if all(entry["status"] == "backed_up" for entry in core["databases"])
                else "partial"
            ),
            bundle_id=bundle_id,
            daily_bundle=daily,
            weekly_bundle=weekly,
            database_status={entry["role"]: entry["status"] for entry in core["databases"]},
            integrity_check="ok",
            completed_at=datetime.fromisoformat(str(core["completed_at"])),
        )

    def _build_stage(self, stage: Path, now: datetime) -> dict[str, object]:
        entries = []
        for role, source in sorted(self.sources.items()):
            if not source.is_file():
                entries.append(
                    {
                        "role": role,
                        "source_name": source.name,
                        "status": "not_initialized",
                        "snapshot_file": None,
                        "sha256": None,
                        "integrity_check": None,
                    }
                )
                continue
            target = stage / f"{role}.sqlite3"
            self._backup_to(source, target)
            os.chmod(target, 0o600)
            integrity = PrivateBackupService._integrity_check(target)
            if integrity != "ok":
                raise PrivateBackupError(f"{role} backup integrity check failed")
            entries.append(
                {
                    "role": role,
                    "source_name": source.name,
                    "status": "backed_up",
                    "snapshot_file": target.name,
                    "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                    "integrity_check": integrity,
                }
            )
        completed_at = now.replace(tzinfo=UTC) if now.utcoffset() is None else now.astimezone(UTC)
        return {
            "schema_version": self.SCHEMA_VERSION,
            "completed_at": completed_at.isoformat(),
            "completeness_status": (
                "complete"
                if all(entry["status"] == "backed_up" for entry in entries)
                else "partial"
            ),
            "databases": entries,
        }

    @staticmethod
    def _backup_to(source_path: Path, target: Path) -> None:
        source = sqlite3.connect(
            f"{source_path.as_uri()}?mode=ro",
            uri=True,
            timeout=5,
        )
        destination = sqlite3.connect(target, timeout=5)
        try:
            source.backup(destination)
            mode = destination.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
            if str(mode).lower() != "delete":
                raise PrivateBackupError("backup snapshot requires rollback journal mode")
            destination.commit()
        finally:
            destination.close()
            source.close()

    @staticmethod
    def _bundle_id(core: dict[str, object]) -> str:
        encoded = json.dumps(core, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _write_manifest(path: Path, manifest: dict[str, object]) -> None:
        with path.open("x", encoding="utf-8") as output:
            json.dump(manifest, output, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            output.flush()
            os.fsync(output.fileno())
        os.chmod(path, 0o600)

    @classmethod
    def verify_bundle(cls, bundle: Path) -> dict[str, object]:
        manifest_path = bundle / "manifest.json"
        try:
            if (
                not bundle.is_dir()
                or bundle.is_symlink()
                or stat.S_IMODE(bundle.stat().st_mode) != 0o700
            ):
                raise PrivateBackupError("backup bundle directory permissions are invalid")
            if (
                not manifest_path.is_file()
                or manifest_path.is_symlink()
                or stat.S_IMODE(manifest_path.stat().st_mode) != 0o600
            ):
                raise PrivateBackupError("backup manifest permissions are invalid")
            document = json.loads(manifest_path.read_text(encoding="utf-8"))
            if set(document) != {
                "schema_version",
                "completed_at",
                "completeness_status",
                "databases",
                "bundle_id",
            }:
                raise PrivateBackupError("backup bundle manifest fields are invalid")
            bundle_id = document["bundle_id"]
            if not isinstance(bundle_id, str) or len(bundle_id) != 64:
                raise PrivateBackupError("backup bundle id is invalid")
            manifest = {key: value for key, value in document.items() if key != "bundle_id"}
            if manifest.get("schema_version") != cls.SCHEMA_VERSION:
                raise PrivateBackupError("backup bundle schema is unsupported")
            if bundle_id != cls._bundle_id(manifest):
                raise PrivateBackupError("backup bundle manifest identity mismatch")
            completed_at = datetime.fromisoformat(manifest["completed_at"])
            if completed_at.utcoffset() is None or completed_at.utcoffset().total_seconds() != 0:
                raise PrivateBackupError("backup bundle completed_at must be UTC")
            entries = manifest["databases"]
            if not isinstance(entries, list):
                raise PrivateBackupError("backup bundle databases are invalid")
            roles = [entry.get("role") for entry in entries if isinstance(entry, dict)]
            if len(roles) != len(entries) or set(roles) != cls.EXPECTED_ROLES:
                raise PrivateBackupError("backup bundle roles are invalid")
            if len(roles) != len(set(roles)):
                raise PrivateBackupError("backup bundle roles are duplicated")
            expected_files = {"manifest.json"}
            snapshot_files: set[str] = set()
            for entry in entries:
                if set(entry) != {
                    "role",
                    "source_name",
                    "status",
                    "snapshot_file",
                    "sha256",
                    "integrity_check",
                }:
                    raise PrivateBackupError("backup database entry fields are invalid")
                source_name = entry["source_name"]
                if (
                    not isinstance(source_name, str)
                    or Path(source_name).name != source_name
                    or not source_name.endswith(".sqlite3")
                ):
                    raise PrivateBackupError("backup database source name is invalid")
                if entry["status"] == "not_initialized":
                    if any(
                        entry[field] is not None
                        for field in ("snapshot_file", "sha256", "integrity_check")
                    ):
                        raise PrivateBackupError("uninitialized backup entry has evidence")
                    continue
                if entry["status"] != "backed_up":
                    raise PrivateBackupError("backup database status is invalid")
                snapshot_file = entry["snapshot_file"]
                if (
                    not isinstance(snapshot_file, str)
                    or Path(snapshot_file).name != snapshot_file
                    or snapshot_file != f"{entry['role']}.sqlite3"
                    or snapshot_file in snapshot_files
                ):
                    raise PrivateBackupError("backup snapshot filename is invalid")
                snapshot_files.add(snapshot_file)
                sha256 = entry["sha256"]
                if (
                    not isinstance(sha256, str)
                    or len(sha256) != 64
                    or any(character not in "0123456789abcdef" for character in sha256)
                    or entry["integrity_check"] != "ok"
                ):
                    raise PrivateBackupError("backup database evidence is invalid")
                snapshot = bundle / snapshot_file
                if not snapshot.is_file() or snapshot.is_symlink():
                    raise PrivateBackupError("backup snapshot is not a regular file")
                if stat.S_IMODE(snapshot.stat().st_mode) != 0o600:
                    raise PrivateBackupError("backup snapshot permissions are invalid")
                expected_files.add(snapshot.name)
                if hashlib.sha256(snapshot.read_bytes()).hexdigest() != sha256:
                    raise PrivateBackupError("backup bundle database hash mismatch")
                if PrivateBackupService._integrity_check(snapshot) != "ok":
                    raise PrivateBackupError("backup bundle database integrity failed")
            expected_completeness = (
                "complete"
                if all(entry["status"] == "backed_up" for entry in entries)
                else "partial"
            )
            if manifest["completeness_status"] != expected_completeness:
                raise PrivateBackupError("backup bundle completeness is invalid")
            if {path.name for path in bundle.iterdir()} != expected_files:
                raise PrivateBackupError("backup bundle contains unexpected files")
            return {**manifest, "bundle_id": bundle_id}
        except PrivateBackupError:
            raise
        except (
            OSError,
            sqlite3.Error,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise PrivateBackupError("backup bundle is invalid") from exc

    def _prune(self, pattern: str, keep: int) -> None:
        bundles = sorted(self.backup_root.glob(pattern))
        for bundle in bundles[:-keep]:
            shutil.rmtree(bundle)
