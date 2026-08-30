from collections.abc import Callable
from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.api.storage import get_storage_readiness
from backend.app.config import Settings, get_settings
from backend.app.market.automation import get_market_clock
from backend.app.market.calendar import SHANGHAI, TradingCalendar, get_trading_calendar
from backend.app.market.calendar_sync import (
    CalendarSyncPolicy,
    CalendarSyncStore,
    CalendarSyncStoreReadError,
)
from backend.app.market.continuity import (
    ContinuityInventory,
    ContinuityStatusSummary,
    build_continuity_status_summary,
)
from backend.app.market.daily_shadow_models import DAILY_SHADOW_PROFILE
from backend.app.market.daily_shadow_registry import DailyShadowRegistryReader
from backend.app.market.failover import (
    FailoverReadinessV1,
    build_capability_snapshot,
    build_readiness,
)
from backend.app.market.models import MarketDataStatus, MarketSummary, PriceSeriesPoint
from backend.app.market.provider_health import SQLiteProviderHealthStore
from backend.app.market.providers.registry import RegistryUnavailable, ShadowRegistry
from backend.app.market.providers.shadow_contracts import AdmissionState, ShadowProviderId
from backend.app.market.series import DataQualityError, PriceSeriesService
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore
from backend.app.market.supplement_ingestion import (
    SupplementalStore,
    supplemental_dataset_root,
)
from backend.app.market.supplemental import (
    SupplementalMarketResponse,
    supplemental_capabilities,
)
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.storage.models import StorageReadiness
from backend.app.storage.preflight import StoragePreflight, configured_market_dataset_root

router = APIRouter(prefix="/market", tags=["market"])


class UnavailableReason(StrEnum):
    REGISTRY_MISSING = "registry_missing"
    REGISTRY_SCHEMA_INVALID = "registry_schema_invalid"
    SHADOW_ROOT_UNAVAILABLE = "shadow_root_unavailable"
    JOB_STORE_UNAVAILABLE = "job_store_unavailable"
    CANONICAL_DESCRIPTOR_CHANGED = "canonical_descriptor_changed"
    CANDIDATE_DESCRIPTOR_CHANGED = "candidate_descriptor_changed"
    SELECTION_BINDING_INVALID = "selection_binding_invalid"
    EVIDENCE_INCOMPLETE = "evidence_incomplete"
    VERSION_VECTOR_DRIFT = "version_vector_drift"
    CALENDAR_SNAPSHOT_INVALID = "calendar_snapshot_invalid"


class ShadowProviderStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready", "unavailable"]
    provider: ShadowProviderId | None = None
    state: AdmissionState | None = None
    unavailable_reason: UnavailableReason | None = None
    report_id: str | None = None


class DailyBarShadowStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready", "unavailable"]
    provider: Literal["tickflow"] = "tickflow"
    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    state: Literal["PENDING", "OBSERVING", "SHADOW_QUALIFIED", "RESET", "UNAVAILABLE"]
    epoch_id: str | None = None
    consecutive_sessions: int = 0
    required_sessions: Literal[20] = 20
    first_trade_date: date | None = None
    last_trade_date: date | None = None
    last_outcome: (
        Literal[
            "SUCCESS",
            "FAILURE",
            "MISMATCH",
            "UNAVAILABLE",
            "SKIPPED_CIRCUIT_OPEN",
        ]
        | None
    ) = None
    observation_mode: Literal["HISTORICAL_SHADOW"] = "HISTORICAL_SHADOW"
    descriptor_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    circuit_state: Literal["CLOSED", "OPEN", "HALF_OPEN"] | None = None
    units_state: Literal["UNKNOWN"] = "UNKNOWN"
    suspension_semantics_state: Literal["UNKNOWN"] = "UNKNOWN"
    factor_evidence_state: Literal["UNQUALIFIED"] = "UNQUALIFIED"
    daily_bar_qualified: bool = False
    adjustment_factor_qualified: Literal[False] = False
    publication_eligible: Literal[False] = False
    failover_enabled: Literal[False] = False
    unavailable_reason: Literal["CONTROL_STATE_UNAVAILABLE"] | None = None

    @model_validator(mode="after")
    def validate_daily_status(self) -> "DailyBarShadowStatusResponse":
        qualified = self.state == "SHADOW_QUALIFIED"
        if self.daily_bar_qualified != qualified:
            raise ValueError("Daily Bar qualification state is inconsistent")
        if self.status == "unavailable":
            if (
                self.state != "UNAVAILABLE"
                or self.unavailable_reason is None
                or self.descriptor_sha256 is not None
                or self.version_vector_sha256 is not None
            ):
                raise ValueError("Daily Bar unavailable state is inconsistent")
        elif (
            self.state == "UNAVAILABLE"
            or self.unavailable_reason is not None
            or self.descriptor_sha256 is None
            or self.circuit_state is None
        ):
            raise ValueError("Daily Bar ready state is incomplete")
        return self


@router.get("/provider-status", response_model=ShadowProviderStatusResponse)
def market_provider_status(
    settings: Annotated[Settings, Depends(get_settings)],
    provider_id: ShadowProviderId | None = None,
) -> ShadowProviderStatusResponse:
    """Read-only shadow status; this path never initializes or migrates registry state."""
    if provider_id is None:
        return ShadowProviderStatusResponse(
            status="unavailable", unavailable_reason=UnavailableReason.REGISTRY_MISSING
        )
    layout = StorageLayout(settings)
    try:
        layout.validate_provider_shadow_root(
            canonical_roots=tuple(
                root
                for root in (
                    settings.local_market_dataset_root,
                    settings.nas_market_dataset_root,
                )
                if root is not None
            )
        )
    except RegistryUnavailable:
        return ShadowProviderStatusResponse(
            status="unavailable",
            provider=provider_id,
            unavailable_reason=UnavailableReason.SHADOW_ROOT_UNAVAILABLE,
        )
    path = layout.provider_registry_database
    if not path.is_file():
        return ShadowProviderStatusResponse(
            status="unavailable",
            provider=provider_id,
            unavailable_reason=UnavailableReason.REGISTRY_MISSING,
        )
    try:
        record = ShadowRegistry(path).read_status(provider_id.value)
    except RegistryUnavailable:
        return ShadowProviderStatusResponse(
            status="unavailable",
            provider=provider_id,
            unavailable_reason=UnavailableReason.REGISTRY_SCHEMA_INVALID,
        )
    return ShadowProviderStatusResponse(
        status="ready", provider=record.provider_id, state=record.admission_state
    )


@router.get(
    "/provider-daily-bar-shadow",
    response_model=DailyBarShadowStatusResponse,
)
def market_provider_daily_bar_shadow(
    settings: Annotated[Settings, Depends(get_settings)],
    provider: Literal["tickflow"] = "tickflow",
) -> DailyBarShadowStatusResponse:
    """Read the isolated Daily capability sidecar without creating or migrating it."""
    layout = StorageLayout(settings)
    try:
        layout.validate_daily_bar_shadow_layout(
            canonical_roots=tuple(
                root
                for root in (
                    settings.local_market_dataset_root,
                    settings.nas_market_dataset_root,
                    settings.daily_bar_shadow_calendar_root,
                )
                if root is not None
            )
        )
        status = DailyShadowRegistryReader(
            layout.daily_bar_shadow_database,
            evidence_root=layout.daily_bar_shadow_evidence_root,
            candidate_root=layout.daily_bar_shadow_candidate_root,
        ).read()
    except Exception:
        status = None
    if status is None or status.status != "READY":
        return DailyBarShadowStatusResponse(
            status="unavailable",
            provider=provider,
            state="UNAVAILABLE",
            unavailable_reason="CONTROL_STATE_UNAVAILABLE",
        )
    window = status.window
    state = window.state if window is not None else "PENDING"
    qualified = state == "SHADOW_QUALIFIED"
    return DailyBarShadowStatusResponse(
        status="ready",
        provider=provider,
        state=state,
        epoch_id=window.epoch_id if window is not None else None,
        consecutive_sessions=window.consecutive_sessions if window is not None else 0,
        first_trade_date=window.first_trade_date if window is not None else None,
        last_trade_date=window.last_trade_date if window is not None else None,
        last_outcome=status.last_outcome,
        descriptor_sha256=status.descriptor_sha256,
        version_vector_sha256=status.version_vector_sha256,
        circuit_state=status.circuit_state,
        daily_bar_qualified=qualified,
    )


def read_market_failover_readiness(settings: Settings) -> FailoverReadinessV1:
    """Read the advisory failover shield without provider or canonical access."""
    priority = tuple(settings.market_provider_priority)
    # Validate the complete allowlisted configuration before touching any sidecar path.
    try:
        build_readiness(
            configured_auto_failover_enabled=settings.market_auto_failover_enabled,
            provider_priority=priority,
            control_state_available=False,
        )
    except Exception as exc:
        raise ValueError("invalid failover configuration") from exc

    kwargs = {
        "configured_auto_failover_enabled": settings.market_auto_failover_enabled,
        "provider_priority": priority,
    }
    if priority == ("baostock",):
        return build_readiness(**kwargs)
    if priority[1] != "tickflow":
        return build_readiness(**kwargs, control_state_available=False)

    try:
        layout = StorageLayout(settings)
        layout.validate_daily_bar_shadow_layout()
        status = DailyShadowRegistryReader(
            layout.daily_bar_shadow_database,
            evidence_root=layout.daily_bar_shadow_evidence_root,
            candidate_root=layout.daily_bar_shadow_candidate_root,
            verify_external=True,
        ).read()
        secondary = build_capability_snapshot(status)
        return build_readiness(**kwargs, secondary=secondary)
    except Exception:
        return build_readiness(**kwargs, control_state_available=False)


@router.get("/failover-readiness", response_model=FailoverReadinessV1)
def market_failover_readiness(
    settings: Annotated[Settings, Depends(get_settings)],
) -> FailoverReadinessV1:
    try:
        return read_market_failover_readiness(settings)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_failover_configuration"},
        ) from exc


def get_calendar_sync_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> CalendarSyncStore:
    return CalendarSyncStore(
        settings.local_control_dir / settings.calendar_sync_database_name,
        initialize=False,
    )


def _read_provider_health_snapshot(settings: Settings):
    """Read circuit evidence without initializing or reaping persisted probe leases."""
    layout = StorageLayout(settings)
    if not layout.provider_health_database.is_file():
        return None
    try:
        return SQLiteProviderHealthStore(
            layout.provider_health_database,
            failure_threshold=settings.provider_circuit_failure_threshold,
            cooldown_seconds=settings.provider_circuit_cooldown_seconds,
            probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
        ).provider_health_snapshot()
    except Exception:
        return None


def get_market_store(
    settings: Annotated[Settings, Depends(get_settings)],
    readiness: Annotated[StorageReadiness, Depends(get_storage_readiness)],
) -> MarketStore | NasMarketStore:
    if not readiness.market_data_available:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "market_storage_unavailable",
                "storage_status": readiness.status,
                "reason_code": readiness.reason_code,
            },
        )
    layout = StorageLayout(settings)
    control = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
        read_only=True,
    )
    if readiness.mode in {"nas", "local_dataset"}:
        dataset_root = configured_market_dataset_root(settings)
        assert dataset_root is not None
        store = NasMarketStore(
            control,
            dataset_root,
            layout.local_paths.staging,
        )
        try:
            store.ensure_readiness()
        except (DatasetError, OSError) as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "market_storage_unavailable",
                    "storage_status": "unavailable",
                    "reason_code": "nas_dataset_read_failed",
                },
            ) from exc
        return store
    return control


@router.get("/supplemental", response_model=SupplementalMarketResponse)
def market_supplemental(
    settings: Annotated[Settings, Depends(get_settings)],
) -> SupplementalMarketResponse:
    if (
        configured_market_dataset_root(settings) is not None
        and not StoragePreflight(settings).inspect().market_data_available
    ):
        return SupplementalMarketResponse(
            status="error",
            provider="akshare",
            as_of=None,
            market_flow=None,
            sector_flows=[],
            industry_classifications=[],
            capabilities=supplemental_capabilities(),
            quality_issues=["market_storage_unavailable"],
        )
    return SupplementalStore(
        supplemental_dataset_root(settings),
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental-ingestion.lock",
    ).read_response(enabled=settings.akshare_supplemental_enabled)


@router.get("/summary", response_model=MarketSummary)
def market_summary(
    request: Request,
    store: Annotated[MarketStore, Depends(get_market_store)],
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)],
    clock: Annotated[Callable[[], datetime], Depends(get_market_clock)],
) -> MarketSummary:
    if request.query_params:
        raise HTTPException(
            status_code=422,
            detail="market summary does not accept client freshness parameters",
        )
    expected_session = calendar.latest_expected_session(clock())
    return MarketSummaryService(store).latest(expected_session=expected_session)


@router.get("/status", response_model=MarketDataStatus)
def market_status(
    store: Annotated[MarketStore, Depends(get_market_store)],
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)],
    clock: Annotated[Callable[[], datetime], Depends(get_market_clock)],
    settings: Annotated[Settings, Depends(get_settings)],
    calendar_sync: Annotated[
        CalendarSyncStore,
        Depends(get_calendar_sync_store),
    ],
) -> MarketDataStatus:
    now = clock().astimezone(SHANGHAI)
    expected = calendar.latest_expected_session(now)
    published = store.published_refresh()
    scheduler = store.scheduler_state()
    try:
        calendar_maintenance = calendar_sync.state()
    except CalendarSyncStoreReadError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "calendar_control_read_failed",
                "storage_status": "unavailable",
            },
        ) from exc
    calendar_decision = CalendarSyncPolicy().decide(
        now,
        state=calendar_maintenance,
    )
    calendar_status = (
        "conflict"
        if calendar_maintenance.conflict_detected
        else scheduler.calendar_status
        if scheduler is not None and scheduler.calendar_status != "confirmed"
        else ("confirmed" if calendar.session_status(now.date()) != "unknown" else "unavailable")
    )
    refresh_enabled = settings.auto_refresh_enabled or settings.scheduled_refresh_enabled
    refresh_state = (
        scheduler.refresh_state
        if refresh_enabled and scheduler is not None
        else "idle"
        if refresh_enabled
        else "disabled"
    )
    phase = calendar.market_phase(now)
    if phase == "after_close_waiting":
        if refresh_state == "running":
            phase = "refreshing"
        elif (
            expected is not None and published is not None and published.requested_date >= expected
        ):
            phase = "complete"
        elif refresh_state in {"delayed", "error"}:
            phase = "delayed"
    if isinstance(store, NasMarketStore):
        continuity_scanner = ContinuityInventory(
            calendar=calendar,
            inventory_reader=store,
            inventory_mode="immutable_dataset",
            calendar_conflict=lambda: calendar_maintenance.conflict_detected,
        )
        continuity_queue = store.control.repair_queue_snapshot()
        provider_health = _read_provider_health_snapshot(settings)
    else:
        # Mutable local DuckDB history is deliberately not an immutable publication
        # authority for continuity.
        continuity_scanner = None
        continuity_queue = None
        provider_health = None
    continuity = build_continuity_status_summary(
        scanner=continuity_scanner,
        configured_start=settings.market_continuity_start_date,
        latest_completed_session=expected,
        queue_snapshot=continuity_queue,
        repair_enabled=settings.market_repair_enabled,
        provider_health=provider_health,
        freshness_state=scheduler,
        now=now,
    )
    try:
        continuity = ContinuityStatusSummary.model_validate(
            continuity.model_dump(mode="python", round_trip=True)
        )
    except Exception:
        # A bypassed internal model must degrade only the additive continuity fields.  Keep
        # the pre-R2-F1 base status path and its existing HTTP failure behavior intact.
        continuity = ContinuityStatusSummary(
            continuity_start_date=(
                settings.market_continuity_start_date
                if isinstance(settings.market_continuity_start_date, date)
                else None
            ),
            repair_execution_enabled=bool(settings.market_repair_enabled),
            continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
        )
    status = MarketDataStatus(
        market_phase=phase,
        calendar_status=calendar_status,
        latest_expected_session=expected,
        published_as_of=published.requested_date if published else None,
        refresh_state=refresh_state,
        last_success_at=(
            scheduler.last_success_at
            if scheduler is not None and scheduler.last_success_at is not None
            else published.completed_at
            if published
            else None
        ),
        next_retry_at=scheduler.next_retry_at if scheduler else None,
        calendar_last_sync_at=calendar_maintenance.last_attempt_at,
        calendar_last_success_at=calendar_maintenance.last_success_at,
        calendar_conflict_detected=calendar_maintenance.conflict_detected,
        calendar_conflict_at=calendar_maintenance.conflict_at,
        calendar_next_sync_at=(calendar_decision.next_sync_at or calendar_maintenance.next_sync_at),
        calendar_sources=[item.model_dump() for item in calendar.sources_for(now.year)],
        **continuity.model_dump(mode="python"),
    )
    return MarketDataStatus.model_validate(status.model_dump(mode="python", round_trip=True))


@router.get("/history/dates", response_model=list[date])
def market_history_dates(
    store: Annotated[MarketStore, Depends(get_market_store)],
) -> list[date]:
    return store.available_dates(source="baostock")


@router.get("/history/{symbol}", response_model=list[PriceSeriesPoint])
def market_history(
    symbol: str,
    start: date,
    end: date,
    store: Annotated[MarketStore, Depends(get_market_store)],
    adjustment: Literal["none", "qfq"] = "qfq",
) -> list[PriceSeriesPoint]:
    if start > end:
        raise HTTPException(status_code=422, detail="start must be on or before end")
    try:
        return PriceSeriesService(store).read(
            symbol,
            start=start,
            end=end,
            adjustment=adjustment,
        )
    except DataQualityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
