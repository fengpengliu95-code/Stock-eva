from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from backend.app.alert.models import (
    AlertEvaluationResponse,
    AlertEventListResponse,
    AlertEventRecord,
    AlertRuleRecord,
)
from backend.app.alert.service import AlertService
from backend.app.alert.store import (
    AlertConflictError,
    AlertNotFoundError,
    AlertStore,
)
from backend.app.api.market import get_market_store
from backend.app.api.strategy import get_strategy_store
from backend.app.api.user import get_user_store
from backend.app.config import Settings, get_settings
from backend.app.market.store import MarketStore
from backend.app.strategy.store import StrategyNotFoundError, StrategyStore
from backend.app.user.store import NotFoundError, UserStore

router = APIRouter(prefix="/alerts", tags=["alerts"])


class AlertRuleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    watchlist_id: str
    strategy_id: str
    strategy_version: int = Field(ge=1)


class AlertEvaluationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    signal_date: date


def get_alert_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> AlertStore:
    return AlertStore(settings.user_data_dir / settings.user_database_name)


def get_alert_service(
    alert_store: Annotated[AlertStore, Depends(get_alert_store)],
    user_store: Annotated[UserStore, Depends(get_user_store)],
    strategy_store: Annotated[StrategyStore, Depends(get_strategy_store)],
    market_store: Annotated[MarketStore, Depends(get_market_store)],
) -> AlertService:
    return AlertService(alert_store, user_store, strategy_store, market_store)


def _alert_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (AlertNotFoundError, NotFoundError, StrategyNotFoundError)):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, AlertConflictError):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=422, detail=str(exc))


@router.post(
    "/rules",
    response_model=AlertRuleRecord,
    status_code=status.HTTP_201_CREATED,
)
def create_alert_rule(
    payload: AlertRuleCreate,
    service: Annotated[AlertService, Depends(get_alert_service)],
) -> AlertRuleRecord:
    try:
        return service.create_rule(**payload.model_dump())
    except Exception as exc:
        raise _alert_error(exc) from exc


@router.get("/rules", response_model=list[AlertRuleRecord])
def list_alert_rules(
    store: Annotated[AlertStore, Depends(get_alert_store)],
) -> list[AlertRuleRecord]:
    return store.list_rules()


@router.post(
    "/rules/{rule_id}/evaluate",
    response_model=AlertEvaluationResponse,
)
def evaluate_alert_rule(
    rule_id: str,
    payload: AlertEvaluationCreate,
    service: Annotated[AlertService, Depends(get_alert_service)],
) -> AlertEvaluationResponse:
    try:
        return service.evaluate(rule_id, signal_date=payload.signal_date)
    except Exception as exc:
        raise _alert_error(exc) from exc


@router.get("/events", response_model=AlertEventListResponse)
def list_alert_events(
    service: Annotated[AlertService, Depends(get_alert_service)],
) -> AlertEventListResponse:
    return service.list_events()


def _manual_transition(
    event_id: str,
    *,
    to_state: str,
    reason: str,
    store: AlertStore,
) -> AlertEventRecord:
    try:
        return store.transition(
            event_id,
            to_state=to_state,
            actor="user",
            reason=reason,
        )
    except Exception as exc:
        raise _alert_error(exc) from exc


@router.post(
    "/events/{event_id}/acknowledge",
    response_model=AlertEventRecord,
)
def acknowledge_alert(
    event_id: str,
    store: Annotated[AlertStore, Depends(get_alert_store)],
) -> AlertEventRecord:
    return _manual_transition(
        event_id,
        to_state="acknowledged",
        reason="user_acknowledged",
        store=store,
    )


@router.post(
    "/events/{event_id}/suppress",
    response_model=AlertEventRecord,
)
def suppress_alert(
    event_id: str,
    store: Annotated[AlertStore, Depends(get_alert_store)],
) -> AlertEventRecord:
    return _manual_transition(
        event_id,
        to_state="suppressed",
        reason="user_suppressed",
        store=store,
    )


@router.post(
    "/events/{event_id}/restore",
    response_model=AlertEventRecord,
)
def restore_alert(
    event_id: str,
    store: Annotated[AlertStore, Depends(get_alert_store)],
) -> AlertEventRecord:
    return _manual_transition(
        event_id,
        to_state="eligible",
        reason="user_restored_for_re_evaluation",
        store=store,
    )
