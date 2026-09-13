from typing import Annotated

from fastapi import APIRouter, Depends

from backend.app.config import Settings, get_settings
from backend.app.storage.models import StorageReadiness
from backend.app.storage.preflight import StoragePreflight
from backend.app.storage.replication import ReplicationStatusResponse, ReplicationStatusService

router = APIRouter(prefix="/storage", tags=["storage"])


def get_storage_readiness(
    settings: Annotated[Settings, Depends(get_settings)],
) -> StorageReadiness:
    return StoragePreflight(settings).inspect()


@router.get("/readiness", response_model=StorageReadiness)
def storage_readiness(
    readiness: Annotated[StorageReadiness, Depends(get_storage_readiness)],
) -> StorageReadiness:
    return readiness


@router.get("/replication", response_model=ReplicationStatusResponse)
def replication_status(
    settings: Annotated[Settings, Depends(get_settings)],
) -> ReplicationStatusResponse:
    """Read-only, sanitized local replication projection."""
    return ReplicationStatusService(settings).read()
