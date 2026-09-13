"""Central construction boundary for dataset-backed market stores."""

from backend.app.config import Settings
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore, canonical_control_schema_digest
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import configured_market_dataset_root
from backend.app.storage.replication import (
    DESTINATION_DESCRIPTOR_NAME,
    DestinationArchiveWriter,
    DestinationCommitVerifier,
    DestinationDescriptor,
    JournalRecord,
    LineageResolver,
    ReplicationDrainWorker,
    ReplicationOutboxService,
    RetainedEvidenceLineageReader,
    SourceInstanceStore,
)


class ReplicationDrainConfigurationError(RuntimeError):
    """Typed failure for missing/invalid persisted local destination setup."""


def build_replication_drain_worker(
    settings: Settings, *, layout: StorageLayout | None = None
) -> ReplicationDrainWorker | None:
    """Construct one bounded drain worker only after both gates and descriptor proof."""
    if not (settings.replication_enabled and settings.replication_drain_enabled):
        return None
    if settings.replication_destination_root is None:
        raise ReplicationDrainConfigurationError("replication destination binding is unavailable")
    layout = layout or StorageLayout(settings)
    source_root = configured_market_dataset_root(settings)
    if source_root is None:
        raise ReplicationDrainConfigurationError("replication source dataset is unavailable")
    descriptor_path = settings.replication_destination_root / DESTINATION_DESCRIPTOR_NAME
    try:
        descriptor_record = DestinationDescriptor.read(descriptor_path)
        descriptor = DestinationArchiveWriter(
            descriptor_record,
            source_root=source_root,
            writer_host_id=descriptor_record.single_writer_host_id,
        )
        source_instance = SourceInstanceStore(
            source_root / "_replication" / "source-instance.json"
        ).read(canonical_root_path=source_root)
        outbox = ReplicationOutboxService(
            layout.replication_sidecar_root,
            enabled=True,
            source_instance_id=source_instance.source_instance_id,
            source_instance_sha256=source_instance.source_instance_sha256,
            destination_verifier=DestinationCommitVerifier(descriptor_record),
        )
        journals = {
            record.checkpoint_id: record.checkpoint_projection
            for record in (
                JournalRecord.read(path)
                for path in sorted(layout.replication_journal_root.glob("*.json"))
            )
        }
    except Exception as exc:
        raise ReplicationDrainConfigurationError(
            "replication destination binding is invalid"
        ) from exc
    return ReplicationDrainWorker(
        outbox,
        descriptor,
        replication_enabled=True,
        replication_drain_enabled=True,
        checkpoint_reader=journals.get,
    )


def build_nas_market_store(
    settings: Settings,
    *,
    read_only: bool = False,
    layout: StorageLayout | None = None,
    lineage_resolver: LineageResolver | None = None,
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
                # Persist the descriptor-stable canonical schema contract,
                # never a table-name-only hash.
                schema_digest = canonical_control_schema_digest()
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
    resolver = lineage_resolver or LineageResolver(
        RetainedEvidenceLineageReader(settings.provider_evidence_root)
    )
    return NasMarketStore(
        control,
        root,
        layout.local_paths.staging,
        replication_enabled=settings.replication_enabled,
        replication_service=service,
        lineage_resolver=resolver,
    )


def read_dataset_lineage_mode(settings: Settings, *, layout: StorageLayout | None = None) -> str:
    """Read the incumbent manifest mode without initializing writer state."""
    store = build_nas_market_store(settings, read_only=True, layout=layout)
    if store is None:
        raise ValueError("dataset store is not configured")
    manifest = store._manifest()
    return store.coordinator._existing_lineage_mode(manifest)
