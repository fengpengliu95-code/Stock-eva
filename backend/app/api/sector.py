from datetime import date, datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException

from backend.app.classification.store import ClassificationStore
from backend.app.config import Settings, get_settings
from backend.app.regime.store import (
    MarketReadUnavailable,
    PublishedDatasetMarketReader,
    PublishedSnapshotDuckDbMarketReader,
)
from backend.app.sector.models import (
    LeaderRankingResponse,
    SectorRotationResponse,
)
from backend.app.sector.service import (
    ClassificationStorageUnavailableError,
    SectorRotationService,
    UnknownTaxonomyError,
)
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import configured_market_dataset_root

router = APIRouter(prefix="/analysis", tags=["analysis"])


def get_sector_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _reject_future_as_of(as_of: date, today: date) -> None:
    if as_of > today:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "future_as_of",
                "as_of": as_of.isoformat(),
                "today": today.isoformat(),
                "timezone": "Asia/Shanghai",
            },
        )


def get_sector_rotation_service(
    settings: Annotated[Settings, Depends(get_settings)],
) -> SectorRotationService:
    layout = StorageLayout(settings)
    dataset_root = configured_market_dataset_root(settings)
    if dataset_root is None:
        reader = PublishedSnapshotDuckDbMarketReader(layout.local_paths.market_database)
    else:
        reader = PublishedDatasetMarketReader(
            control_path=layout.local_paths.market_database,
            control_temp=layout.duckdb_temporary,
            dataset_root=dataset_root,
            staging_root=layout.local_paths.staging,
        )
    classification = ClassificationStore(
        settings.market_data_dir / settings.classification_database_name,
        temp_directory=settings.local_temp_dir / "classification-duckdb",
    )
    return SectorRotationService(classification, reader)


def _unknown_taxonomy(exc: UnknownTaxonomyError) -> HTTPException:
    taxonomy_id = str(exc)
    return HTTPException(
        status_code=404,
        detail={"code": "unknown_taxonomy", "taxonomy_id": taxonomy_id},
    )


def _analysis_storage_unavailable(code: str) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "code": code,
            "storage_status": "unavailable",
        },
    )


@router.get(
    "/sector-rotation",
    response_model=SectorRotationResponse,
)
def sector_rotation(
    as_of: date,
    taxonomy_id: str,
    service: Annotated[
        SectorRotationService,
        Depends(get_sector_rotation_service),
    ],
    today: Annotated[date, Depends(get_sector_today)],
) -> SectorRotationResponse:
    _reject_future_as_of(as_of, today)
    try:
        return service.rotation(as_of, taxonomy_id)
    except UnknownTaxonomyError as exc:
        raise _unknown_taxonomy(exc) from exc
    except ClassificationStorageUnavailableError as exc:
        raise _analysis_storage_unavailable("classification_storage_unavailable") from exc
    except MarketReadUnavailable as exc:
        raise _analysis_storage_unavailable(exc.code) from exc


@router.get(
    "/sectors/{sector_id}/leaders",
    response_model=LeaderRankingResponse,
)
def sector_leaders(
    sector_id: str,
    as_of: date,
    taxonomy_id: str,
    service: Annotated[
        SectorRotationService,
        Depends(get_sector_rotation_service),
    ],
    today: Annotated[date, Depends(get_sector_today)],
) -> LeaderRankingResponse:
    _reject_future_as_of(as_of, today)
    try:
        return service.leaders(as_of, taxonomy_id, sector_id)
    except UnknownTaxonomyError as exc:
        raise _unknown_taxonomy(exc) from exc
    except ClassificationStorageUnavailableError as exc:
        raise _analysis_storage_unavailable("classification_storage_unavailable") from exc
    except MarketReadUnavailable as exc:
        raise _analysis_storage_unavailable(exc.code) from exc
