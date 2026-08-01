import re
from datetime import date, datetime
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.config import Settings, get_settings
from backend.app.fund_flow.models import FundFlowEvidenceResult
from backend.app.fund_flow.service import FundFlowEvidenceService
from backend.app.fund_flow.store import FundFlowEvidenceStore
from backend.app.market.calendar import TradingCalendar, get_trading_calendar

router = APIRouter(prefix="/analysis", tags=["analysis"])
_SECTOR_SCOPE_ID = re.compile(r"^[0-9A-Za-z_\-\u4e00-\u9fff·（）()＋+]{1,64}$")


def get_fund_flow_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def get_fund_flow_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> FundFlowEvidenceStore:
    return FundFlowEvidenceStore.from_settings(settings)


def _scope_id(scope: str, value: str | None) -> str:
    if scope == "market":
        if value not in {None, "all_a_share"}:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "invalid_scope_id",
                    "reason": "market scope_id must be all_a_share or omitted",
                },
            )
        return "all_a_share"
    if value is None or value != value.strip() or not _SECTOR_SCOPE_ID.fullmatch(value):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_scope_id",
                "reason": "sector scope_id is required and must be a safe published name",
            },
        )
    return value


@router.get(
    "/fund-flow-evidence",
    response_model=FundFlowEvidenceResult,
)
def fund_flow_evidence(
    as_of: date,
    scope: Annotated[Literal["market", "sector"], Query()] = "market",
    scope_id: Annotated[str | None, Query(max_length=64)] = None,
    store: Annotated[FundFlowEvidenceStore, Depends(get_fund_flow_store)] = None,
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)] = None,
    today: Annotated[date, Depends(get_fund_flow_today)] = None,
) -> FundFlowEvidenceResult:
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
    normalized_scope_id = _scope_id(scope, scope_id)
    snapshot = store.read(
        as_of=as_of,
        scope=scope,
        scope_id=normalized_scope_id,
    )
    return FundFlowEvidenceService(calendar).evaluate(snapshot)
