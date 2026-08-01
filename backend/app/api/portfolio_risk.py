from datetime import date, datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from backend.app.classification.store import ClassificationStore
from backend.app.config import Settings, get_settings
from backend.app.portfolio.ledger import (
    PortfolioLedger,
    PortfolioLedgerConflict,
    PortfolioLedgerUnavailable,
)
from backend.app.portfolio.models import (
    PortfolioDailySnapshot,
    PortfolioDailySnapshotCreate,
    PortfolioDailySnapshotUpdate,
    PortfolioRiskResult,
    PortfolioSnapshotWriteResult,
)
from backend.app.portfolio.risk import (
    DeterministicRegimeReader,
    PortfolioRiskDependencyUnavailable,
    PortfolioRiskService,
)
from backend.app.regime.store import (
    MarketReadUnavailable,
    MarketRegimeStore,
    PublishedDatasetMarketReader,
    PublishedSnapshotDuckDbMarketReader,
)
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import configured_market_dataset_root

portfolio_router = APIRouter(prefix="/portfolio", tags=["portfolio"])
analysis_router = APIRouter(prefix="/analysis", tags=["analysis"])


def get_portfolio_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _reject_future(as_of: date, today: date) -> None:
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


def get_portfolio_ledger(
    settings: Annotated[Settings, Depends(get_settings)],
) -> PortfolioLedger:
    return PortfolioLedger(StorageLayout(settings).local_paths.user_database)


def get_portfolio_risk_service(
    settings: Annotated[Settings, Depends(get_settings)],
) -> PortfolioRiskService:
    layout = StorageLayout(settings)
    dataset_root = configured_market_dataset_root(settings)
    if dataset_root is None:
        market_reader = PublishedSnapshotDuckDbMarketReader(layout.local_paths.market_database)
    else:
        market_reader = PublishedDatasetMarketReader(
            control_path=layout.local_paths.market_database,
            control_temp=layout.duckdb_temporary,
            dataset_root=dataset_root,
            staging_root=layout.local_paths.staging,
        )
    classification = ClassificationStore(
        settings.market_data_dir / settings.classification_database_name,
        temp_directory=settings.local_temp_dir / "classification-duckdb",
    )
    return PortfolioRiskService(
        PortfolioLedger(layout.local_paths.user_database),
        market_reader,
        classification,
        DeterministicRegimeReader(MarketRegimeStore(market_reader)),
    )


def _ledger_error(exc: PortfolioLedgerUnavailable) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "code": "portfolio_ledger_unavailable",
            "storage_status": "unavailable",
        },
    )


@portfolio_router.post(
    "/daily-snapshots",
    response_model=PortfolioSnapshotWriteResult,
    responses={
        409: {"description": "Snapshot revision conflict"},
        503: {"description": "Private ledger unavailable"},
    },
)
def create_daily_snapshot(
    data: PortfolioDailySnapshotCreate,
    response: Response,
    ledger: Annotated[PortfolioLedger, Depends(get_portfolio_ledger)],
    today: Annotated[date, Depends(get_portfolio_today)],
) -> PortfolioSnapshotWriteResult:
    """Create an immutable manual daily snapshot or return its idempotent result."""
    _reject_future(data.as_of, today)
    try:
        result = ledger.create(data)
    except PortfolioLedgerConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "portfolio_revision_conflict", "reason": str(exc)},
        ) from exc
    except PortfolioLedgerUnavailable as exc:
        raise _ledger_error(exc) from exc
    response.status_code = (
        status.HTTP_201_CREATED if result.write_status == "created" else status.HTTP_200_OK
    )
    return result


@portfolio_router.put(
    "/daily-snapshots/{snapshot_date}",
    response_model=PortfolioSnapshotWriteResult,
    responses={
        409: {"description": "Snapshot revision conflict"},
        503: {"description": "Private ledger unavailable"},
    },
)
def revise_daily_snapshot(
    snapshot_date: date,
    data: PortfolioDailySnapshotUpdate,
    ledger: Annotated[PortfolioLedger, Depends(get_portfolio_ledger)],
    today: Annotated[date, Depends(get_portfolio_today)],
) -> PortfolioSnapshotWriteResult:
    """Append a revision while retaining every earlier revision."""
    _reject_future(snapshot_date, today)
    if data.as_of != snapshot_date:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "snapshot_date_mismatch",
                "path_date": snapshot_date.isoformat(),
                "body_date": data.as_of.isoformat(),
            },
        )
    try:
        return ledger.revise(data)
    except PortfolioLedgerConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "portfolio_revision_conflict", "reason": str(exc)},
        ) from exc
    except PortfolioLedgerUnavailable as exc:
        raise _ledger_error(exc) from exc


@portfolio_router.get(
    "/daily-snapshots",
    response_model=PortfolioDailySnapshot | None,
    responses={503: {"description": "Private ledger unavailable"}},
)
def read_daily_snapshot(
    as_of: date,
    revision: Annotated[int | None, Query(ge=1)] = None,
    ledger: Annotated[PortfolioLedger, Depends(get_portfolio_ledger)] = None,
    today: Annotated[date, Depends(get_portfolio_today)] = None,
) -> PortfolioDailySnapshot | None:
    """Read a point-in-time snapshot without initializing private storage."""
    _reject_future(as_of, today)
    try:
        return (
            ledger.read_revision(as_of, revision)
            if revision is not None
            else ledger.read_selected(as_of)
        )
    except PortfolioLedgerUnavailable as exc:
        raise _ledger_error(exc) from exc


@analysis_router.get(
    "/portfolio-risk",
    response_model=PortfolioRiskResult,
    responses={503: {"description": "Required local published storage unavailable"}},
)
def portfolio_risk(
    as_of: date,
    service: Annotated[
        PortfolioRiskService,
        Depends(get_portfolio_risk_service),
    ],
    today: Annotated[date, Depends(get_portfolio_today)],
) -> PortfolioRiskResult:
    """Evaluate deterministic local research risk from evidence visible at as_of."""
    _reject_future(as_of, today)
    try:
        return service.evaluate(as_of)
    except PortfolioLedgerUnavailable as exc:
        raise _ledger_error(exc) from exc
    except MarketReadUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": exc.code, "storage_status": "unavailable"},
        ) from exc
    except PortfolioRiskDependencyUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": exc.code, "storage_status": "unavailable"},
        ) from exc
