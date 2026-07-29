from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.classification.models import (
    CoverageAudit,
    IndexComponentsResponse,
    SectorMembersResponse,
    SectorResponse,
    SecurityResponse,
)
from backend.app.classification.service import (
    ClassificationService,
    UnknownIndexError,
    UnknownTaxonomyError,
)
from backend.app.classification.store import ClassificationStore
from backend.app.config import Settings, get_settings

router = APIRouter(prefix="/classification", tags=["classification"])


def get_classification_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> ClassificationStore:
    return ClassificationStore(
        settings.market_data_dir / settings.classification_database_name,
        temp_directory=settings.local_temp_dir / "classification-duckdb",
    )


@router.get("/securities", response_model=SecurityResponse)
def classification_securities(
    as_of: date,
    store: Annotated[ClassificationStore, Depends(get_classification_store)],
    symbol: Annotated[str | None, Query(pattern=r"^(?:sh|sz|bj)\.\d{6}$")] = None,
) -> SecurityResponse:
    return ClassificationService(store).securities(as_of, symbol=symbol)


@router.get(
    "/indexes/{index_id}/components",
    response_model=IndexComponentsResponse,
)
def classification_index_components(
    index_id: str,
    as_of: date,
    store: Annotated[ClassificationStore, Depends(get_classification_store)],
) -> IndexComponentsResponse:
    try:
        return ClassificationService(store).index_components(index_id, as_of)
    except UnknownIndexError as exc:
        raise HTTPException(
            status_code=404,
            detail={"code": "unknown_index", "index_id": index_id},
        ) from exc


@router.get(
    "/taxonomies/{taxonomy_id}/sectors",
    response_model=SectorResponse,
)
def classification_sectors(
    taxonomy_id: str,
    as_of: date,
    store: Annotated[ClassificationStore, Depends(get_classification_store)],
) -> SectorResponse:
    try:
        return ClassificationService(store).sectors(taxonomy_id, as_of)
    except UnknownTaxonomyError as exc:
        raise HTTPException(
            status_code=404,
            detail={"code": "unknown_taxonomy", "taxonomy_id": taxonomy_id},
        ) from exc


@router.get(
    "/taxonomies/{taxonomy_id}/sectors/{sector_id}/members",
    response_model=SectorMembersResponse,
)
def classification_sector_members(
    taxonomy_id: str,
    sector_id: str,
    as_of: date,
    store: Annotated[ClassificationStore, Depends(get_classification_store)],
) -> SectorMembersResponse:
    try:
        return ClassificationService(store).sector_members(
            taxonomy_id,
            sector_id,
            as_of,
        )
    except UnknownTaxonomyError as exc:
        raise HTTPException(
            status_code=404,
            detail={"code": "unknown_taxonomy", "taxonomy_id": taxonomy_id},
        ) from exc


@router.get("/coverage", response_model=CoverageAudit)
def classification_coverage(
    as_of: date,
    taxonomy_id: str,
    store: Annotated[ClassificationStore, Depends(get_classification_store)],
) -> CoverageAudit:
    try:
        return ClassificationService(store).coverage(taxonomy_id, as_of)
    except UnknownTaxonomyError as exc:
        raise HTTPException(
            status_code=404,
            detail={"code": "unknown_taxonomy", "taxonomy_id": taxonomy_id},
        ) from exc
