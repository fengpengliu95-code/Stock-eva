from typing import Annotated

from fastapi import APIRouter, Depends

from backend.app.config import Settings, get_settings
from backend.app.storage.models import StorageReadiness
from backend.app.storage.preflight import StoragePreflight

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
