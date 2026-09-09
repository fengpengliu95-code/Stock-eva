import os
import stat
from dataclasses import dataclass
from pathlib import Path

from backend.app.config import Settings
from backend.app.storage.models import LocalStoragePaths

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW


def _physical_path(path: Path) -> Path:
    absolute = Path(os.path.normpath(path))
    if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
        return Path("/private/var", *absolute.parts[2:])
    return absolute


def _ensure_directory_chain(path: Path) -> None:
    """Create a local writer directory only through no-follow dirfds."""
    if not path.is_absolute():
        raise ValueError("replication writer directory must be absolute")
    normalized = _physical_path(path)
    descriptors: list[int] = []
    try:
        current = os.open(Path(normalized.anchor), _DIRECTORY_FLAGS)
        descriptors.append(current)
        for component in normalized.parts[1:]:
            created = False
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=current)
            except FileNotFoundError:
                os.mkdir(component, mode=0o700, dir_fd=current)
                created = True
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=current)
            if created:
                os.fsync(child)
                os.fsync(current)
            descriptors.append(child)
            current = child
    except OSError as exc:
        raise ValueError("replication writer directory ancestor is unsafe") from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(frozen=True)
class StorageLayout:
    settings: Settings

    @property
    def local_paths(self) -> LocalStoragePaths:
        return LocalStoragePaths(
            control=self.settings.local_control_dir,
            staging=self.settings.local_staging_dir,
            locks=self.settings.local_lock_dir,
            temporary=self.settings.local_temp_dir,
            market_database=(self.settings.market_data_dir / self.settings.market_database_name),
            user_database=(self.settings.user_data_dir / self.settings.user_database_name),
            portfolio_database=(
                self.settings.user_data_dir / self.settings.portfolio_database_name
            ),
            factor_cache_database=(
                self.settings.local_control_dir / self.settings.factor_cache_database_name
            ),
            provider_health_database=(
                self.settings.local_control_dir / self.settings.provider_health_database_name
            ),
            universe_contract_database=(
                self.settings.local_control_dir / self.settings.universe_contract_database_name
            ),
        )

    @property
    def market_refresh_lock(self) -> Path:
        return self.settings.local_lock_dir / "market-refresh.lock"

    @property
    def duckdb_temporary(self) -> Path:
        return self.settings.local_temp_dir / "duckdb"

    @property
    def regime_snapshot_database(self) -> Path:
        return self.settings.local_control_dir / self.settings.regime_snapshot_database_name

    @property
    def calendar_generation_database(self) -> Path:
        return self.settings.local_control_dir / self.settings.calendar_generation_database_name

    @property
    def provider_health_database(self) -> Path:
        return self.local_paths.provider_health_database

    @property
    def universe_contract_database(self) -> Path:
        return self.local_paths.universe_contract_database

    @property
    def provider_registry_database(self) -> Path:
        return self.settings.local_control_dir / self.settings.provider_registry_database_name

    @property
    def provider_registry_lock(self) -> Path:
        return self.provider_registry_database.with_name(
            self.provider_registry_database.name + ".lock"
        )

    @property
    def daily_bar_shadow_database(self) -> Path:
        return self.settings.local_control_dir / self.settings.daily_bar_shadow_database_name

    @property
    def daily_bar_shadow_lock(self) -> Path:
        return self.daily_bar_shadow_database.with_name(
            self.daily_bar_shadow_database.name + ".lock"
        )

    @property
    def replication_database(self) -> Path:
        # Compatibility name only: the sidecar authority is a directory of
        # immutable generations, never a mutable ``replication.sqlite3`` file.
        return self.replication_sidecar_root

    @property
    def replication_sidecar_root(self) -> Path:
        """Immutable replication sidecar generation root.

        ``replication_database`` remains a legacy configuration property for
        parsing compatibility only; no sidecar writer is allowed to use that
        mutable pathname.
        """
        return self.settings.local_control_dir / "replication-sidecar"

    @property
    def replication_journal_root(self) -> Path:
        return self.settings.local_control_dir / self.settings.replication_journal_root_name

    @property
    def replication_lock(self) -> Path:
        return self.settings.local_lock_dir / self.settings.replication_lock_name

    @property
    def replication_source_instance(self) -> Path | None:
        if self.settings.local_market_dataset_root is None:
            return None
        return (
            self.settings.local_market_dataset_root
            / "_replication"
            / self.settings.replication_source_instance_name
        )

    @property
    def replication_source_commits(self) -> Path | None:
        if self.settings.local_market_dataset_root is None:
            return None
        return self.settings.local_market_dataset_root / "_replication" / "source-commits"

    def ensure_replication_writer_dirs(self) -> None:
        """Create only local replication parents for an explicit writer operation."""
        for path in (
            self.settings.local_control_dir,
            self.replication_sidecar_root,
            self.replication_journal_root,
            self.settings.local_lock_dir,
        ):
            _ensure_directory_chain(path)

    @property
    def daily_bar_shadow_evidence_root(self) -> Path:
        return self.provider_shadow_root / "daily-evidence"

    @property
    def daily_bar_shadow_candidate_root(self) -> Path:
        return self.provider_shadow_root / "daily-candidates"

    def validate_daily_bar_shadow_layout(
        self, *, canonical_roots: tuple[Path, ...] = ()
    ) -> tuple[Path, Path, Path]:
        """Validate Daily sidecar paths without creating or resolving user links."""
        self.validate_provider_shadow_root(canonical_roots=canonical_roots)
        database = self.daily_bar_shadow_database
        evidence = self.daily_bar_shadow_evidence_root
        candidates = self.daily_bar_shadow_candidate_root
        health_database = self.settings.local_control_dir / getattr(
            self.settings, "provider_health_database_name", "provider_health.sqlite3"
        )
        if (
            not database.parent.is_absolute()
            or not evidence.is_absolute()
            or not candidates.is_absolute()
            or database in {self.provider_registry_database, health_database}
            or evidence == candidates
        ):
            raise ValueError("Daily Bar shadow layout is not isolated")
        return database, evidence, candidates

    @property
    def provider_shadow_root(self) -> Path:
        return self.settings.provider_shadow_root

    @property
    def provider_shadow_staging(self) -> Path:
        return self.provider_shadow_root / "staging"

    @property
    def provider_shadow_bundles(self) -> Path:
        return self.provider_shadow_root / "bundles"

    def provider_shadow_attempt_staging(self, nonce: str) -> Path:
        if not nonce or "/" in nonce or "\\" in nonce or nonce in {".", ".."}:
            raise ValueError("shadow staging nonce must be a basename")
        return self.provider_shadow_staging / nonce

    def provider_shadow_bundle(self, evidence_id: str) -> Path:
        if (
            not evidence_id
            or "/" in evidence_id
            or "\\" in evidence_id
            or evidence_id in {".", ".."}
        ):
            raise ValueError("shadow evidence ID must be a basename")
        return self.provider_shadow_bundles / evidence_id

    def validate_provider_shadow_root(self, *, canonical_roots: tuple[Path, ...] = ()) -> Path:
        """Validate without creating anything, following no symlinks in any ancestor."""
        from backend.app.market.providers.registry import RegistryUnavailable

        root = self.provider_shadow_root
        if not root.is_absolute():
            raise RegistryUnavailable("shadow root unavailable")

        def physical(path: Path) -> Path:
            absolute = Path(os.path.abspath(path))
            if absolute.parts[1:2] == ("tmp",) and Path("/tmp").is_symlink():
                return Path("/private/tmp", *absolute.parts[2:])
            if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
                return Path("/private/var", *absolute.parts[2:])
            return absolute

        canonical = tuple(canonical_roots) + (
            self.provider_evidence_root,
            self.settings.local_control_dir,
            self.provider_registry_database.parent,
        )
        root_resolved = physical(root)
        for candidate in canonical:
            candidate_resolved = physical(candidate)
            if root_resolved == candidate_resolved:
                raise RegistryUnavailable("shadow root overlaps canonical root")
            try:
                root_resolved.relative_to(candidate_resolved)
                raise RegistryUnavailable("shadow root overlaps canonical root")
            except ValueError:
                pass
            try:
                candidate_resolved.relative_to(root_resolved)
                raise RegistryUnavailable("shadow root overlaps canonical root")
            except ValueError:
                pass
        current = Path(root_resolved.anchor)
        for component in root_resolved.parts[1:]:
            current /= component
            try:
                info = os.lstat(current)
            except FileNotFoundError:
                raise RegistryUnavailable("shadow root unavailable") from None
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RegistryUnavailable("shadow root unavailable")
        if root_resolved.stat().st_mode & 0o002:
            raise RegistryUnavailable("shadow root permissions unavailable")
        return root

    @property
    def provider_evidence_root(self) -> Path:
        return self.settings.provider_evidence_root

    @property
    def provider_evidence_objects(self) -> Path:
        return self.provider_evidence_root / "objects"

    @property
    def provider_evidence_manifests(self) -> Path:
        return self.provider_evidence_root / "manifests"

    @property
    def provider_evidence_staging(self) -> Path:
        return self.provider_evidence_root / "staging"

    def ensure_local_runtime_dirs(self) -> None:
        paths = self.local_paths
        for path in (
            paths.control,
            paths.staging,
            paths.locks,
            paths.temporary,
            paths.market_database.parent,
            paths.user_database.parent,
            self.duckdb_temporary,
        ):
            path.mkdir(parents=True, exist_ok=True)
