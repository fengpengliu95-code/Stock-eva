"""Central construction boundary for dataset-backed market stores."""

from backend.app.config import Settings
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import configured_market_dataset_root
from backend.app.storage.replication import (
    ReplicationOutboxService,
    SourceInstanceStore,
    domain_sha256,
)


def build_nas_market_store(
    settings: Settings,
    *,
    read_only: bool = False,
    layout: StorageLayout | None = None,
) -> NasMarketStore | None:
    """Build every NAS/local-dataset store through one replication-aware seam."""
    layout = layout or StorageLayout(settings)
    root = configured_market_dataset_root(settings)
    if root is None:
        return None
    control = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
        read_only=read_only,
    )
    service = None
    if settings.replication_enabled:
        source_path = root / "_replication" / "source-instance.json"
        try:
            if source_path.exists():
                source = SourceInstanceStore(source_path).read(canonical_root_path=root)
            elif read_only:
                source = None
            else:
                connection = control._connect()
                try:
                    rows = connection.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='main' ORDER BY table_name"
                    ).fetchall()
                finally:
                    connection.close()
                schema_digest = domain_sha256(
                    "stock-eva/r2f4.3/canonical-control-schema/v1",
                    [str(row[0]) for row in rows],
                )
                source = SourceInstanceStore(source_path).create(
                    canonical_root_path=root,
                    canonical_schema_digest=schema_digest,
                )
            if source is not None:
                service = ReplicationOutboxService(
                    layout.replication_sidecar_root,
                    enabled=True,
                    source_instance_id=source.source_instance_id,
                    source_instance_sha256=source.source_instance_sha256,
                    journal_root=layout.replication_journal_root,
                )
        except Exception:
            service = None
    return NasMarketStore(
        control,
        root,
        layout.local_paths.staging,
        replication_enabled=settings.replication_enabled,
        replication_service=service,
    )


def read_dataset_lineage_mode(
    settings: Settings, *, layout: StorageLayout | None = None
) -> str:
    """Read the incumbent manifest mode without initializing writer state."""
    store = build_nas_market_store(settings, read_only=True, layout=layout)
    if store is None:
        raise ValueError("dataset store is not configured")
    manifest = store._manifest()
    return store.coordinator._existing_lineage_mode(manifest)
