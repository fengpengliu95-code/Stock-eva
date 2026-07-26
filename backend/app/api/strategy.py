from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.app.api.market import get_market_store
from backend.app.config import Settings, get_settings
from backend.app.market.store import MarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.strategy.dsl import RuleValidationError, validate_rule
from backend.app.strategy.models import (
    StrategyRecord,
    StrategyRunRecord,
    StrategyVersionRecord,
)
from backend.app.strategy.run import StrategyRunService
from backend.app.strategy.store import (
    StrategyConflictError,
    StrategyNotFoundError,
    StrategyStore,
)

router = APIRouter(prefix="/strategies", tags=["strategies"])
run_router = APIRouter(prefix="/strategy-runs", tags=["strategies"])


class RulePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    rule: dict[str, object]


class StrategyCreate(RulePayload):
    name: str = Field(min_length=1, max_length=120)


class StrategyVersionCreate(RulePayload):
    expected_current_version: int = Field(ge=1)


class StrategyRunCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(ge=1)
    symbols: list[str] = Field(min_length=1, max_length=20)
    as_of_date: date

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            symbol = value.strip().lower()
            parts = symbol.split(".")
            if (
                len(parts) != 2
                or parts[0] not in {"sh", "sz"}
                or not (len(parts[1]) == 6 and parts[1].isdigit())
            ):
                raise ValueError("symbols must look like sh.600000 or sz.000001")
            normalized.append(symbol)
        return normalized


class StrategyMarketScanCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(ge=1)


def get_strategy_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> StrategyStore:
    return StrategyStore(StorageLayout(settings).local_paths.user_database)


def _validated(rule: dict[str, object]):
    try:
        return validate_rule(rule)
    except RuleValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _store_error(exc: Exception) -> HTTPException:
    if isinstance(exc, StrategyNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, StrategyConflictError):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=422, detail=str(exc))


@router.post("/validate")
def validate_strategy(payload: RulePayload) -> dict[str, object]:
    rule = _validated(payload.rule)
    return {
        "status": "valid",
        "rule_hash": rule.rule_hash,
        "depth": rule.depth,
        "predicate_count": rule.predicate_count,
        "canonical_rule": rule.ast,
    }


@router.post("", status_code=status.HTTP_201_CREATED)
def create_strategy(
    payload: StrategyCreate,
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> dict[str, StrategyRecord | StrategyVersionRecord]:
    try:
        strategy, version = store.create_strategy(payload.name, _validated(payload.rule))
    except (StrategyConflictError, ValueError) as exc:
        raise _store_error(exc) from exc
    return {"strategy": strategy, "version": version}


@router.get("", response_model=list[StrategyRecord])
def list_strategies(
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> list[StrategyRecord]:
    return store.list_strategies()


@router.get(
    "/{strategy_id}/versions/{version}",
    response_model=StrategyVersionRecord,
)
def get_strategy_version(
    strategy_id: str,
    version: int,
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> StrategyVersionRecord:
    try:
        return store.get_version(strategy_id, version)
    except StrategyNotFoundError as exc:
        raise _store_error(exc) from exc


@router.post(
    "/{strategy_id}/versions",
    response_model=StrategyVersionRecord,
    status_code=status.HTTP_201_CREATED,
)
def add_strategy_version(
    strategy_id: str,
    payload: StrategyVersionCreate,
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> StrategyVersionRecord:
    try:
        return store.add_version(
            strategy_id,
            expected_current_version=payload.expected_current_version,
            rule=_validated(payload.rule),
        )
    except (StrategyNotFoundError, StrategyConflictError) as exc:
        raise _store_error(exc) from exc


@router.post(
    "/{strategy_id}/runs",
    response_model=StrategyRunRecord,
    status_code=status.HTTP_201_CREATED,
)
def run_strategy(
    strategy_id: str,
    payload: StrategyRunCreate,
    strategy_store: Annotated[StrategyStore, Depends(get_strategy_store)],
    market_store: Annotated[MarketStore, Depends(get_market_store)],
) -> StrategyRunRecord:
    try:
        return StrategyRunService(strategy_store, market_store).run(
            strategy_id,
            version=payload.version,
            symbols=payload.symbols,
            as_of_date=payload.as_of_date,
        )
    except (StrategyNotFoundError, ValueError) as exc:
        raise _store_error(exc) from exc


@router.post(
    "/{strategy_id}/runs/all-main-board",
    response_model=StrategyRunRecord,
    status_code=status.HTTP_201_CREATED,
)
def run_all_main_board_strategy(
    strategy_id: str,
    payload: StrategyMarketScanCreate,
    strategy_store: Annotated[StrategyStore, Depends(get_strategy_store)],
    market_store: Annotated[MarketStore, Depends(get_market_store)],
) -> StrategyRunRecord:
    try:
        return StrategyRunService(strategy_store, market_store).run_all_main_board(
            strategy_id,
            version=payload.version,
        )
    except (StrategyNotFoundError, ValueError) as exc:
        raise _store_error(exc) from exc


@router.get("/{strategy_id}/runs", response_model=list[StrategyRunRecord])
def list_strategy_runs(
    strategy_id: str,
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> list[StrategyRunRecord]:
    try:
        return store.list_runs(strategy_id)
    except StrategyNotFoundError as exc:
        raise _store_error(exc) from exc


@run_router.get("/{run_id}", response_model=StrategyRunRecord)
def get_strategy_run(
    run_id: str,
    store: Annotated[StrategyStore, Depends(get_strategy_store)],
) -> StrategyRunRecord:
    try:
        return store.get_run(run_id)
    except StrategyNotFoundError as exc:
        raise _store_error(exc) from exc
