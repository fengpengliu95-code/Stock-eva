from dataclasses import dataclass
from pathlib import Path

from backend.app.config import Settings
from backend.app.storage.models import LocalStoragePaths


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
        )

    @property
    def market_refresh_lock(self) -> Path:
        return self.settings.local_lock_dir / "market-refresh.lock"

    @property
    def duckdb_temporary(self) -> Path:
        return self.settings.local_temp_dir / "duckdb"

    @property
    def regime_snapshot_database(self) -> Path:
        return (
            self.settings.local_control_dir
            / self.settings.regime_snapshot_database_name
        )

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
