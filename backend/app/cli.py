import argparse
import json
import os
import re
import signal
import stat
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from backend.app.api.market import read_market_failover_readiness
from backend.app.classification.failures import (
    ClassificationFailure,
    ClassificationFailureError,
)
from backend.app.classification.provider import (
    BaoStockClassificationProvider,
    make_baostock_classification_provider,
)
from backend.app.classification.store import ClassificationStore
from backend.app.classification.sync import run_classification_sync
from backend.app.config import (
    CalendarRuntimeSettings,
    Settings,
    get_calendar_runtime_settings,
    get_market_failover_runtime_settings,
    get_settings,
    get_tickflow_free_runtime_settings,
)
from backend.app.market.automation import (
    AutomationOutcome,
    BaoStockProbeRunner,
    MarketAutomationService,
    RefreshAlreadyRunning,
    RefreshRunLock,
    canonical_refresh_callback,
    collect_required_symbols,
    get_market_clock,
    make_universe_post_success_hook,
    run_publication_refresh,
)
from backend.app.market.backfill import (
    BackfillService,
    minimum_effective_days,
    resolve_effective_window,
)
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.calendar import build_live_trading_calendar, get_trading_calendar
from backend.app.market.calendar_generation import (
    CalendarGenerationError,
    CalendarGenerationStore,
    CalendarStoreUnavailable,
    load_source,
)
from backend.app.market.calendar_maintenance import CalendarMaintenanceService
from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore
from backend.app.market.calendar_runtime import build_calendar_generation_status
from backend.app.market.calendar_sync import (
    CalendarSyncResult,
    CalendarSyncService,
    CalendarSyncStore,
    read_calendar_conflict,
)
from backend.app.market.candidates import CandidateStore
from backend.app.market.continuity import (
    ContinuityEnqueueService,
    ContinuityInventory,
    ContinuityRepairExecutor,
    ContinuityScanResult,
    ContinuityUnavailable,
    RepairClaimCoordinator,
    RepairQueueSnapshot,
    RepairRetryPolicy,
)
from backend.app.market.evidence import replay_cli_payload
from backend.app.market.factor_cache import AdjustmentFactorCache
from backend.app.market.failover import canonical_json_bytes
from backend.app.market.failures import (
    MarketFailure,
    MarketFailureError,
    legacy_failure_quality_issues,
    market_failure_from_exception,
    public_failure_message,
)
from backend.app.market.full_history import FullMarketHistoryService
from backend.app.market.provider_canary import (
    ProviderCanaryError,
    confirmation_required_report,
    plan_provider_canary,
    run_provider_canary,
)
from backend.app.market.provider_health import (
    InMemoryProviderHealthStore,
    ProviderHealth,
    SQLiteProviderHealthStore,
)
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.providers.registry import RegistryUnavailable, ShadowRegistry
from backend.app.market.providers.shadow_contracts import ShadowProviderId
from backend.app.market.refresh import MarketRefreshService
from backend.app.market.store import MarketStore
from backend.app.market.universe import UniverseSidecarStore
from backend.app.market.universe_status import (
    UniverseStatusControlError,
    UniverseStatusDateError,
    build_universe_status,
    parse_universe_trade_date,
)
from backend.app.orchestration.adapters import build_after_close_pipeline
from backend.app.regime.acceptance import (
    MINIMUM_RELEASE_ONE_SESSIONS,
    ReleaseOneAcceptanceService,
)
from backend.app.regime.snapshots import (
    RegimeSnapshotCaptureService,
    RegimeSnapshotStore,
    RegimeSnapshotUnavailable,
)
from backend.app.regime.store import MarketReadUnavailable, market_regime_store_from_settings
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.factory import (
    build_nas_market_store,
    build_replication_drain_worker,
    read_dataset_lineage_mode,
)
from backend.app.storage.initialize import (
    DatasetInitializationError,
    EmptyDatasetInitializer,
)
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import StoragePreflight, configured_market_dataset_root
from backend.app.storage.replication import (
    DESTINATION_INIT_ACK,
    ReplicationStatusService,
    RestoreService,
    initialize_destination,
)
from backend.app.strategy.store import StrategyStore
from backend.app.user.backup import PrivateBackupError, PrivateBackupSetService
from backend.app.user.store import UserStore


class _CliArgumentError(ValueError):
    """An allowlisted parse failure that carries no user input."""


class _BackfillLineageInputInvalid(ValueError):
    pass


class _BackfillLineageInputUnavailable(OSError):
    pass


class _SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise _CliArgumentError("invalid CLI arguments")


def _read_backfill_lineage_file(
    path: Path, expected_dates: tuple[date, ...]
) -> tuple[tuple[date, dict[str, object]], ...]:
    """Read ordered lineage JSON without following symlinks or duplicate keys."""
    if not path.is_absolute() or path == Path("/"):
        raise _BackfillLineageInputInvalid("invalid lineage input")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | nofollow)
    except OSError as exc:
        raise _BackfillLineageInputUnavailable("lineage input unavailable") from exc
    try:
        stat_result = os.fstat(descriptor)
        if not stat.S_ISREG(stat_result.st_mode) or stat_result.st_size > 1024 * 1024:
            raise _BackfillLineageInputInvalid("invalid lineage input")
        payload = os.read(descriptor, stat_result.st_size + 1)
    finally:
        os.close(descriptor)
    if len(payload) > 1024 * 1024:
        raise _BackfillLineageInputInvalid("invalid lineage input")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise _BackfillLineageInputInvalid("invalid lineage input")
            result[key] = value
        return result

    try:
        root = json.loads(payload.decode("utf-8"), object_pairs_hook=pairs)
        if (
            set(root) != {"schema", "entries"}
            or root["schema"] != "stock-eva/r2f4.3/backfill-lineage/v1"
        ):
            raise ValueError
        parsed = []
        for item in root["entries"]:
            if set(item) != {"trade_date", "lineage_input"}:
                raise ValueError
            parsed.append((date.fromisoformat(item["trade_date"]), item["lineage_input"]))
    except Exception as exc:
        raise _BackfillLineageInputInvalid("invalid lineage input") from exc
    expected = tuple(expected_dates)
    if tuple(item[0] for item in parsed) != expected:
        raise _BackfillLineageInputInvalid("invalid lineage input")
    return tuple(parsed)


def _provider_health_payload(health: ProviderHealth) -> dict[str, object]:
    return {
        "state": health.state.value,
        "blocking_endpoints": [endpoint.value for endpoint in health.blocking_endpoints],
        "probe_endpoints": [endpoint.value for endpoint in health.probe_endpoints],
        "cooldown_until": {
            item.endpoint.value: item.cooldown_until.isoformat()
            for item in health.endpoints
            if item.cooldown_until is not None
        },
    }


def _automation_outcome_payload(outcome: AutomationOutcome) -> dict[str, object]:
    """Serialize only the stable, typed automation contract for CLI consumers."""
    # ``model_copy``/``model_construct`` can bypass Pydantic validators.  Revalidate the
    # complete outcome before reading any field so contradictory internal state cannot reach
    # the legacy or repair serializers.
    try:
        validated = AutomationOutcome.model_validate(
            outcome.model_dump(mode="python", warnings="none")
        )
    except ValidationError:
        # Pydantic's normal validation detail includes ``input_value`` snippets.  Those
        # snippets may contain private paths, request keys, or provider text injected into a
        # bypassed model, so never expose the original exception to CLI callers.
        try:
            AutomationOutcome.model_validate({"decision": None, "state": None, "result": None})
        except ValidationError as sanitized:
            raise sanitized from None
        raise RuntimeError("automation outcome validation unexpectedly succeeded") from None
    payload: dict[str, object] = {
        "decision": validated.decision.model_dump(mode="json"),
        "state": validated.state.model_dump(mode="json"),
        # ``result`` is a legacy freshness field and intentionally remains present as null.
        "result": (
            validated.result.model_dump(mode="json") if validated.result is not None else None
        ),
    }
    repair = validated.continuity_result
    if repair is not None:
        payload["continuity_result"] = repair.public_payload()
    return payload


def _probe_timeout_value(value: str) -> int:
    seconds = int(value)
    if not 1 <= seconds <= 60:
        raise argparse.ArgumentTypeError("probe timeout must be between 1 and 60 seconds")
    return seconds


def _socket_timeout_value(value: str) -> float:
    seconds = float(value)
    if not 1 <= seconds <= 120:
        raise argparse.ArgumentTypeError("socket timeout must be between 1 and 120 seconds")
    return seconds


def _provider_canary_symbol(value: str) -> str:
    if re.fullmatch(r"(?:sh|sz)\.\d{6}", value) is None:
        raise argparse.ArgumentTypeError("provider canary symbol is invalid")
    return value


def _shadow_provider_symbol(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}", value) is None:
        raise argparse.ArgumentTypeError("shadow provider symbol is invalid")
    return value


def _external_authorization_id(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
        raise argparse.ArgumentTypeError("external authorization ID is invalid")
    return value


def _evidence_id(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
        raise argparse.ArgumentTypeError("evidence ID is invalid")
    return value


def _sha256_argument(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise argparse.ArgumentTypeError("candidate SHA-256 is invalid")
    return value


def _classification_failure_payload(
    *,
    as_of: date,
    failure: ClassificationFailure,
) -> dict[str, object]:
    return {
        "status": "error",
        "as_of": as_of.isoformat(),
        "quality_issues": ["classification_sync_failed"],
        "writes_classification_data": False,
        **failure.model_dump(),
    }


def _market_failure_payload(failure: MarketFailure) -> dict[str, object]:
    return {
        "status": "error",
        "quality_issues": legacy_failure_quality_issues(failure),
        "error_message": public_failure_message(failure),
        **failure.model_dump(),
    }


def _unexpected_classification_failure(
    *,
    started_at: float,
    provider: object | None,
    configured_timeout_seconds: float | None,
) -> ClassificationFailure:
    session = getattr(provider, "session", None)
    return ClassificationFailure(
        failure_stage="validation",
        failure_class="internal",
        elapsed_seconds=round(max(0.0, perf_counter() - started_at), 3),
        provider_request_count=max(0, getattr(provider, "last_request_count", 0)),
        configured_timeout_seconds=getattr(
            session,
            "socket_timeout_seconds",
            configured_timeout_seconds,
        ),
        configured_max_attempts=getattr(session, "max_attempts", None),
    )


class _DiscardWriter:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        pass


class _ProbeTimeoutInterrupt(BaseException):
    pass


@contextmanager
def _probe_timeout(seconds: int):
    def raise_timeout(_signum, _frame):
        raise _ProbeTimeoutInterrupt

    previous = signal.signal(signal.SIGALRM, raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        try:
            yield
        except _ProbeTimeoutInterrupt as exc:
            raise TimeoutError("metadata probe timed out") from exc
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def build_parser() -> argparse.ArgumentParser:
    parser = _SanitizedArgumentParser(description="Stock EVA after-close data tasks")
    subparsers = parser.add_subparsers(dest="command", required=True)
    refresh = subparsers.add_parser(
        "refresh",
        help="refresh one completed trading date",
        description=(
            "Manual operator override for one completed trading date. This command is not "
            "provider-health-gated; audit the circuit state before deployment use."
        ),
    )
    refresh.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    target = refresh.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--all-main-board",
        action="store_true",
        help="load Shanghai/Shenzhen main-board stocks and the two summary indexes",
    )
    target.add_argument(
        "--symbols",
        nargs="+",
        help="controlled symbol slice, for example sh.600000 sh.000001",
    )
    refresh.add_argument(
        "--dry-run",
        action="store_true",
        help="print the request plan without calling BaoStock",
    )
    refresh.add_argument(
        "--execute-all-main-board",
        action="store_true",
        help="required confirmation for a real all-main-board refresh",
    )
    refresh.add_argument(
        "--inspect-universe",
        action="store_true",
        help="dry-run metadata probe: calendar and main-board universe only",
    )
    refresh.add_argument(
        "--probe-timeout-seconds",
        type=_probe_timeout_value,
        default=30,
        help="hard timeout for the optional metadata probe (1-60 seconds)",
    )
    refresh.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    backfill = subparsers.add_parser(
        "backfill",
        help="plan or execute a resumable explicit-symbol historical backfill",
    )
    backfill.add_argument("--symbols", nargs="+", required=True)
    backfill.add_argument("--start", type=date.fromisoformat)
    backfill.add_argument("--end", required=True, type=date.fromisoformat)
    backfill.add_argument("--effective-days", type=int)
    backfill.add_argument("--strategy-id")
    backfill.add_argument("--strategy-version", type=int)
    backfill.add_argument("--symbol-batch-size", type=int, default=5)
    backfill.add_argument("--date-batch-size", type=int, default=60)
    backfill.add_argument("--max-batches", type=int, default=20)
    backfill.add_argument("--min-request-interval", type=float, default=0.5)
    backfill.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    backfill.add_argument(
        "--execute",
        action="store_true",
        help="perform network requests; omitted means dry-run",
    )
    backfill.add_argument(
        "--lineage-input",
        type=Path,
        help="local ordered lineage JSON for dataset-backed backfill",
    )
    replicate = subparsers.add_parser(
        "market-replicate",
        help="plan or explicitly execute local-to-NAS immutable replication",
    )
    replicate.add_argument("--destination", type=Path)
    replicate.add_argument("--operation-day", type=date.fromisoformat)
    replicate.add_argument("--execute", action="store_true")
    replicate.add_argument("--json", action="store_true")
    restore = subparsers.add_parser(
        "market-restore",
        help="plan or explicitly execute a verified NAS restore to a temporary root",
    )
    restore.add_argument("--source", required=True, type=Path)
    restore.add_argument("--destination", required=True, type=Path)
    restore.add_argument("--execute", action="store_true")
    restore.add_argument("--operation-day", type=date.fromisoformat)
    restore.add_argument("--json", action="store_true")
    init = subparsers.add_parser(
        "market-replication-init",
        help="plan or explicitly initialize a new empty local replication destination",
    )
    init.add_argument("--destination", required=True, type=Path)
    init.add_argument("--execute", action="store_true")
    init.add_argument("--acknowledge")
    init.add_argument("--json", action="store_true")
    subparsers.add_parser(
        "market-replication-status",
        help="read-only local replication status",
    ).add_argument("--json", action="store_true")
    full_history = subparsers.add_parser(
        "full-market-backfill",
        help="plan or execute resumable full-main-board history into NAS",
    )
    full_history.add_argument("--start", required=True, type=date.fromisoformat)
    full_history.add_argument("--end", required=True, type=date.fromisoformat)
    full_history.add_argument("--max-sessions", type=int, default=3000)
    full_history.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    full_history.add_argument(
        "--execute-all-main-board-history",
        action="store_true",
        help="required confirmation for real full-market historical publication",
    )
    subparsers.add_parser(
        "refresh-runs",
        help="list auditable daily refresh run records",
    )
    subparsers.add_parser(
        "market-schema-migrate",
        help=argparse.SUPPRESS,
    )
    automation_once = subparsers.add_parser(
        "auto-refresh-once",
        help="evaluate the backend schedule once without a client-provided date",
    )
    automation_once.add_argument(
        "--execute",
        action="store_true",
        help="perform a due refresh; omitted means a network-free plan",
    )
    automation_once.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    provider_canary = subparsers.add_parser(
        "provider-canary",
        help=(
            "plan or run six independent endpoint probes; reports actual requests and "
            "before/after snapshot equality as zero-write evidence, not an OS proof"
        ),
        description=(
            "Run six independent endpoint probes and report actual requests. Zero-write "
            "evidence is before/after snapshot equality plus a static no-write path, not "
            "an OS proof."
        ),
    )
    provider_canary.add_argument(
        "--date",
        required=True,
        type=date.fromisoformat,
        dest="trade_date",
    )
    provider_canary.add_argument(
        "--stock-symbol",
        required=True,
        type=_provider_canary_symbol,
    )
    replay = subparsers.add_parser(
        "market-provider-replay",
        help="replay one local, immutable provider evidence manifest",
    )
    replay.add_argument("--evidence-id", required=True, type=_evidence_id)
    replay.add_argument("--compare-candidate-sha", type=_sha256_argument)
    replay.add_argument("--compare-semantic-sha", type=_sha256_argument)
    replay.add_argument("--compare-adapter-version")
    provider_canary.add_argument(
        "--index-symbol",
        required=True,
        type=_provider_canary_symbol,
    )
    provider_canary.add_argument("--control-root", required=True, type=Path)
    provider_canary.add_argument("--data-root", required=True, type=Path)
    provider_canary.add_argument(
        "--execute",
        action="store_true",
        help="perform provider requests only with the separate acknowledgement",
    )
    provider_canary.add_argument(
        "--acknowledge-provider-requests",
        action="store_true",
        help="acknowledge six independent endpoint probes and their reported actual requests",
    )
    provider_status = subparsers.add_parser(
        "market-provider-status",
        help="read-only status of the isolated shadow provider registry",
    )
    provider_status.add_argument(
        "--provider",
        choices=tuple(item.value for item in ShadowProviderId),
    )
    subparsers.add_parser(
        "market-failover-readiness",
        help="read-only advisory status of the failover capability shield",
    )
    daily_shadow = subparsers.add_parser(
        "market-provider-daily-shadow",
        help="plan or execute one credentialless TickFlow Free Daily shadow session",
    )
    daily_shadow.add_argument("--provider", required=True, choices=("tickflow",))
    daily_shadow.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    daily_shadow.add_argument("--execute", action="store_true")
    daily_shadow.add_argument("--external-authorization-id", type=_external_authorization_id)
    daily_shadow.add_argument(
        "--acknowledge-provider-requests",
        action="store_true",
        help="acknowledge one bounded whole-session Free Daily provider invocation",
    )
    shadow_canary = subparsers.add_parser(
        "market-provider-canary",
        help="plan an isolated TickFlow or Tushare shadow canary",
    )
    shadow_canary.add_argument("--provider", required=True, choices=("tickflow", "tushare"))
    shadow_canary.add_argument(
        "--capability",
        choices=("free-daily", "authenticated"),
        help="TickFlow capability; omitted defaults to credentialless free-daily",
    )
    shadow_canary.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    shadow_canary.add_argument("--symbols", nargs="*", type=_shadow_provider_symbol, default=())
    shadow_canary.add_argument("--execute", action="store_true")
    shadow_canary.add_argument(
        "--external-authorization-id",
        type=_external_authorization_id,
    )
    shadow_canary.add_argument(
        "--acknowledge-provider-requests",
        action="store_true",
        help="acknowledge the four bounded TickFlow discovery requests",
    )
    shadow_run = subparsers.add_parser(
        "market-provider-shadow",
        help="plan or execute one isolated bounded provider shadow window",
    )
    shadow_run.add_argument("--provider", required=True, choices=("tickflow", "tushare"))
    shadow_run.add_argument("--start", required=True, type=date.fromisoformat)
    shadow_run.add_argument("--end", required=True, type=date.fromisoformat)
    shadow_run.add_argument("--execute", action="store_true")
    shadow_run.add_argument("--external-authorization-id", type=_external_authorization_id)
    shadow_promote = subparsers.add_parser(
        "market-provider-promote",
        help="plan or explicitly review a shadow promotion (failover remains disabled)",
    )
    shadow_promote.add_argument("--provider", required=True, choices=("tickflow", "tushare"))
    shadow_promote.add_argument("--window-id", required=True, type=_evidence_id)
    shadow_promote.add_argument("--review-id", required=True, type=_external_authorization_id)
    shadow_promote.add_argument("--execute", action="store_true")
    calendar_sync = subparsers.add_parser(
        "calendar-sync",
        help="plan or execute versioned BaoStock checks of the official calendar",
    )
    calendar_sync.add_argument(
        "--mode",
        choices=("auto", "full", "light"),
        default="auto",
        help="auto follows monthly/startup/daily policy",
    )
    calendar_sync.add_argument("--start", type=date.fromisoformat)
    calendar_sync.add_argument("--end", type=date.fromisoformat)
    calendar_sync.add_argument(
        "--startup",
        action="store_true",
        help="evaluate the startup light-check slot when mode is auto",
    )
    calendar_sync.add_argument(
        "--execute",
        action="store_true",
        help="perform the BaoStock observation and persist audit state",
    )
    calendar_sync.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    subparsers.add_parser(
        "calendar-generation-status",
        help="read-only status of the promoted calendar runtime",
    )
    calendar_stage = subparsers.add_parser(
        "calendar-generation-stage",
        help="validate and optionally stage one reviewed calendar source package",
    )
    calendar_stage.add_argument("--source", required=True, type=Path)
    calendar_stage.add_argument("--execute", action="store_true")
    calendar_maintenance = subparsers.add_parser(
        "calendar-maintenance",
        help="plan or execute one bounded promoted-calendar maintenance slot",
    )
    calendar_maintenance.add_argument("--year", type=int, dest="target_year")
    calendar_maintenance.add_argument("--execute", action="store_true")
    storage_init = subparsers.add_parser(
        "storage-init",
        help="initialize a new empty NAS market dataset",
    )
    storage_init.add_argument(
        "--initialize-empty-nas-dataset",
        action="store_true",
        help="required acknowledgement; refuses an existing path or share root",
    )
    export = subparsers.add_parser("export", help="export one stored date to Parquet")
    export.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    classification_sync = subparsers.add_parser(
        "classification-sync",
        help="plan or execute a point-in-time security/index/sector metadata sync",
    )
    classification_sync.add_argument(
        "--as-of",
        required=True,
        type=date.fromisoformat,
        dest="as_of",
    )
    classification_sync.add_argument(
        "--execute",
        action="store_true",
        help="perform bounded BaoStock metadata reads and publish; omitted means dry-run",
    )
    classification_sync.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    private_backup = subparsers.add_parser(
        "backup-private-data",
        help="create a consistent local SQLite snapshot with bounded retention",
    )
    private_backup.add_argument(
        "--backup-root",
        type=Path,
        default=(Path.home() / "Library/Application Support/Stock EVA/backups"),
    )
    private_backup.add_argument("--keep-daily", type=int, default=7)
    private_backup.add_argument("--keep-weekly", type=int, default=4)
    regime_snapshots = subparsers.add_parser(
        "market-regime-snapshots",
        help="plan or explicitly capture immutable market-regime snapshots",
    )
    regime_snapshots.add_argument("--start", required=True, type=date.fromisoformat)
    regime_snapshots.add_argument("--end", required=True, type=date.fromisoformat)
    regime_snapshots.add_argument(
        "--execute",
        action="store_true",
        help="persist selected verified manifest dates; omitted means read-only plan",
    )
    release_one_audit = subparsers.add_parser(
        "release-one-audit",
        help="read-only verification of persisted Release 1 replay evidence",
    )
    release_one_audit.add_argument(
        "--sessions",
        type=int,
        default=MINIMUM_RELEASE_ONE_SESSIONS,
    )
    continuity = subparsers.add_parser(
        "market-continuity",
        help="plan or enqueue verified immutable continuity gaps",
    )
    continuity.add_argument("--start", type=date.fromisoformat)
    continuity.add_argument("--end", type=date.fromisoformat)
    continuity.add_argument(
        "--execute",
        action="store_true",
        help="persist only verified continuity repair jobs; never runs a provider repair",
    )
    market_universe = subparsers.add_parser(
        "market-universe",
        help="read-only status of the persisted exact-session universe sidecar",
    )
    market_universe.add_argument(
        "--date",
        required=True,
        dest="trade_date",
        help="exact point-in-time date in YYYY-MM-DD format",
    )
    return parser


def _continuity_cli_payload(
    *,
    status: str,
    effective_start: date | None = None,
    effective_end: date | None = None,
    missing_sessions: tuple[date, ...] = (),
    reason_code: str | None = None,
    writes_control_state: bool = False,
    created_count: int = 0,
    existing_count: int = 0,
    created_job_ids: tuple[str, ...] = (),
    execute: bool = False,
) -> dict[str, object]:
    """Return the stable, path/provider-free operator payload."""
    payload: dict[str, object] = {
        "status": status,
        "effective_start": effective_start.isoformat() if effective_start else None,
        "effective_end": effective_end.isoformat() if effective_end else None,
        "missing_session_count": len(missing_sessions),
        "oldest_missing_session": (missing_sessions[0].isoformat() if missing_sessions else None),
        "writes_control_state": writes_control_state,
        "provider_requests": 0,
        "writes_parquet": False,
        "writes_manifest": False,
        "writes_pointer": False,
        "reason_code": reason_code,
    }
    if execute:
        payload.update(
            {
                "created_count": created_count,
                "existing_count": existing_count,
                "created_job_ids": list(created_job_ids),
            }
        )
    else:
        payload["execute_requires"] = "--execute"
    return payload


def _calendar_runtime_now() -> datetime:
    return datetime.now(UTC)


def _market_universe_command(args: argparse.Namespace) -> int:
    """Project the strict sidecar status; this path never initializes runtime directories."""
    try:
        requested = parse_universe_trade_date(args.trade_date)
    except UniverseStatusDateError:
        print(
            json.dumps(
                {
                    "code": "universe_status_invalid",
                    "reason_code": "PIT_VISIBILITY_INVALID",
                    "provider_requests": 0,
                    "writes": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    try:
        settings = get_settings()
        status = build_universe_status(
            requested,
            UniverseSidecarStore(StorageLayout(settings).universe_contract_database),
        )
    except UniverseStatusDateError:
        print(
            json.dumps(
                {
                    "code": "universe_status_invalid",
                    "reason_code": "PIT_VISIBILITY_INVALID",
                    "provider_requests": 0,
                    "writes": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    except UniverseStatusControlError:
        print(
            json.dumps(
                {"code": "universe_unavailable", "reason_code": "CONTROL_STATE_UNAVAILABLE"},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 3
    except Exception:
        print(
            json.dumps(
                {"code": "universe_unavailable", "reason_code": "CONTROL_STATE_UNAVAILABLE"},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 3
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0 if status["status"] == "ready" else 1


def _calendar_generation_status_command(_args: argparse.Namespace) -> int:
    try:
        settings = get_calendar_runtime_settings()
    except (ValidationError, ValueError):
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "error_code": "INVALID_CALENDAR_RUNTIME_CONFIGURATION",
                    "provider_requests": 0,
                    "canonical_writes": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    status = build_calendar_generation_status(settings, now=_calendar_runtime_now())
    print(json.dumps(status.model_dump(mode="json"), ensure_ascii=False, sort_keys=True))
    return 0 if status.status in {"disabled", "ready"} else 1


def _calendar_stage_payload(
    *,
    outcome: str,
    source_sha256: str | None = None,
    admission: str | None = None,
    target_year: int | None = None,
    writes_calendar_state: bool = False,
    execute: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": "completed" if execute else "planned",
        "outcome": outcome,
        "source_sha256": source_sha256,
        "target_year": target_year,
        "admission": admission,
        "policy": "reviewed_source_admission",
        "network_requests": 0,
        "official_requests": 0,
        "machine_requests": 0,
        "writes_calendar_state": writes_calendar_state,
        "canonical_writes": False,
    }
    if not execute:
        payload["execute_requires"] = "--execute"
    return payload


def _calendar_generation_stage_command(args: argparse.Namespace) -> int:
    # Source admission happens before settings/path construction so malformed input cannot
    # initialize a calendar database or its parent directory.
    try:
        source = load_source(args.source)
    except (CalendarGenerationError, OSError, TypeError, ValueError):
        payload = _calendar_stage_payload(outcome="SOURCE_INVALID", execute=args.execute)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 2
    try:
        settings = get_calendar_runtime_settings()
    except (ValidationError, ValueError):
        payload = _calendar_stage_payload(
            outcome="CONTROL_STATE_UNAVAILABLE", target_year=source.year, execute=args.execute
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 2
    now = _calendar_runtime_now()
    store = CalendarGenerationStore(
        settings.local_control_dir / settings.calendar_generation_database_name
    )
    try:
        planned = store.plan_stage(source, now)
    except CalendarGenerationError:
        payload = _calendar_stage_payload(
            outcome="SOURCE_INVALID", target_year=source.year, execute=args.execute
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 2
    if not args.execute:
        payload = _calendar_stage_payload(
            outcome=planned.outcome,
            source_sha256=planned.source_sha256,
            admission=planned.admission,
            target_year=source.year,
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    try:
        result = store.stage_execute(source, now)
    except CalendarGenerationError:
        payload = _calendar_stage_payload(
            outcome="SOURCE_INVALID", target_year=source.year, execute=True
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 2
    except (CalendarStoreUnavailable, OSError):
        payload = _calendar_stage_payload(
            outcome="CONTROL_STATE_UNAVAILABLE", target_year=source.year, execute=True
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 1
    wrote = result.outcome == "STAGED" or (
        result.outcome == "SOURCE_CONFLICT" and result.admission == "quarantined"
    )
    payload = _calendar_stage_payload(
        outcome=result.outcome,
        source_sha256=result.source_sha256,
        admission=result.admission,
        target_year=source.year,
        writes_calendar_state=wrote,
        execute=True,
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return (
        0
        if result.outcome in {"STAGED", "ALREADY_STAGED"} and result.admission != "quarantined"
        else 1
    )


def _calendar_maintenance_payload(result: object, *, execute: bool) -> dict[str, object]:
    values = {
        "status": "completed" if execute else "planned",
        "outcome": result.outcome,
        "target_year": result.target_year,
        "source_sha256": result.source_sha256,
        "generation_sha256": getattr(result, "generation_sha256", None),
        "policy": result.next_year_status,
        "network_requests": result.network_requests,
        "official_requests": result.official_requests,
        "machine_requests": result.machine_requests,
        "writes_calendar_state": result.writes_calendar_state,
        "canonical_writes": False,
    }
    if not execute:
        values["execute_requires"] = "--execute"
    return values


def _calendar_maintenance_command(args: argparse.Namespace) -> int:
    try:
        settings = get_calendar_runtime_settings()
    except (ValidationError, ValueError):
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "outcome": "CONTROL_STATE_UNAVAILABLE",
                    "error_code": "INVALID_CALENDAR_RUNTIME_CONFIGURATION",
                    "network_requests": 0,
                    "official_requests": 0,
                    "machine_requests": 0,
                    "writes_calendar_state": False,
                    "canonical_writes": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    now = _calendar_runtime_now()
    store = CalendarGenerationStore(
        settings.local_control_dir / settings.calendar_generation_database_name
    )
    maintenance_clock = _calendar_runtime_now if args.execute else (lambda: now)
    health = CalendarMaintenanceHealthStore(
        settings.local_control_dir / settings.provider_health_database_name,
        failure_threshold=settings.provider_circuit_failure_threshold,
        cooldown_seconds=settings.provider_circuit_cooldown_seconds,
        probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
        clock=maintenance_clock,
    )
    service = CalendarMaintenanceService(
        store,
        health,
        clock=maintenance_clock,
        socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
        min_request_interval_seconds=settings.auto_refresh_min_request_interval_seconds,
    )
    try:
        if args.execute:
            result = service.execute(target_year=args.target_year)
        else:
            result = service.plan(now=now, target_year=args.target_year)
    except CalendarGenerationError:
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "outcome": "CONTROL_STATE_UNAVAILABLE",
                    "error_code": "INVALID_CALENDAR_MAINTENANCE_INPUT",
                    "network_requests": 0,
                    "official_requests": 0,
                    "machine_requests": 0,
                    "writes_calendar_state": False,
                    "canonical_writes": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    payload = _calendar_maintenance_payload(result, execute=args.execute)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    success = {
        "STAGED",
        "SOURCE_PENDING",
        "ALREADY_ATTEMPTED",
        "NOT_DUE",
        "PROMOTED",
    }
    return 0 if result.outcome in success else 1


def _calendar_runtime_service(settings: Settings) -> CalendarMaintenanceService:
    """Build the existing-control runtime worker from the resolved Settings projection."""
    runtime = CalendarRuntimeSettings.from_settings(settings)
    return CalendarMaintenanceService(
        CalendarGenerationStore(
            runtime.local_control_dir / runtime.calendar_generation_database_name
        ),
        CalendarMaintenanceHealthStore(
            runtime.local_control_dir / runtime.provider_health_database_name,
            failure_threshold=runtime.provider_circuit_failure_threshold,
            cooldown_seconds=runtime.provider_circuit_cooldown_seconds,
            probe_lease_seconds=runtime.provider_circuit_probe_lease_seconds,
        ),
        socket_timeout_seconds=runtime.baostock_socket_timeout_seconds,
        min_request_interval_seconds=runtime.auto_refresh_min_request_interval_seconds,
    )


def _market_continuity_command(args: argparse.Namespace) -> int:
    """Plan or enqueue continuity before any general runtime initialization."""
    try:
        settings = get_settings()
        layout = StorageLayout(settings)
        readiness = StoragePreflight(settings).inspect()
        dataset_root = configured_market_dataset_root(settings)
        if (
            not readiness.market_data_available
            or readiness.mode not in {"nas", "local_dataset"}
            or dataset_root is None
        ):
            payload = _continuity_cli_payload(
                status="unavailable",
                reason_code="MANIFEST_INVENTORY_UNAVAILABLE",
                execute=args.execute,
            )
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1

        calendar = get_trading_calendar()
        calendar_snapshot = calendar.snapshot()
        calendar_sync_path = layout.local_paths.control / settings.calendar_sync_database_name
        read_control = MarketStore(
            layout.local_paths.market_database,
            temp_directory=layout.duckdb_temporary,
            read_only=True,
        )
        immutable_store = build_nas_market_store(settings, read_only=True, layout=layout)
        if immutable_store is None:
            raise DatasetError("dataset store is unavailable")
        scanner = ContinuityInventory(
            calendar=calendar_snapshot,
            inventory_reader=immutable_store,
            inventory_mode="immutable_dataset",
            # Re-read the local sync state on every scan.  The enqueue service invokes a
            # second scan inside the shared lock; a conflict introduced between scans must
            # fail closed rather than use a stale preflight value.
            calendar_conflict=lambda: read_calendar_conflict(calendar_sync_path),
        )
        now = get_market_clock()()
        latest = calendar_snapshot.latest_expected_session(now)
        scan = scanner.scan(
            configured_start=settings.market_continuity_start_date,
            latest_completed_session=latest,
            requested_start=args.start,
            requested_end=args.end,
        )
        try:
            if isinstance(scan, ContinuityUnavailable):
                scan = ContinuityUnavailable.model_validate(
                    scan.model_dump(mode="python", round_trip=True)
                )
            else:
                scan = ContinuityScanResult.model_validate(
                    scan.model_dump(mode="python", round_trip=True)
                )
        except Exception:
            payload = _continuity_cli_payload(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
                execute=args.execute,
            )
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
        if isinstance(scan, ContinuityUnavailable):
            payload = _continuity_cli_payload(
                status=(
                    "error" if scan.reason_code == "CONTINUITY_RANGE_INVALID" else "unavailable"
                ),
                reason_code=scan.reason_code,
                execute=args.execute,
            )
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
        if not args.execute:
            # Planning is strictly read-only.  The immutable scan alone is not
            # sufficient to claim the controller is usable: SELECT-only queue
            # evidence must also be present and valid.  Do not initialize the
            # database or any runtime path on this branch.
            try:
                queue = RepairQueueSnapshot.model_validate(
                    read_control.repair_queue_snapshot().model_dump(mode="python", round_trip=True)
                )
            except Exception:
                queue = None
            if queue is None or queue.status != "ready":
                payload = _continuity_cli_payload(
                    status="unavailable",
                    effective_start=scan.effective_start,
                    effective_end=scan.effective_end,
                    missing_sessions=scan.missing_sessions,
                    reason_code="CONTROL_STATE_UNAVAILABLE",
                )
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
                return 1
            payload = _continuity_cli_payload(
                status=scan.status,
                effective_start=scan.effective_start,
                effective_end=scan.effective_end,
                missing_sessions=scan.missing_sessions,
            )
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0

        # The first strict scan above is the preflight.  The store factory is deliberately
        # lazy: the shared lock is acquired and the evidence is revalidated before it creates
        # any broad runtime directory or opens a writer-owned control database.
        writer: MarketStore | None = None

        def create_writer() -> MarketStore:
            nonlocal writer
            layout.ensure_local_runtime_dirs()
            writer = MarketStore(
                layout.local_paths.market_database,
                temp_directory=layout.duckdb_temporary,
            )
            return writer

        def initialize_writer_schema() -> None:
            if writer is None:
                raise RuntimeError("continuity writer was not created")
            writer.initialize_all_writer_schema(
                staging_directory=layout.market_refresh_lock.parent,
            )

        result = ContinuityEnqueueService(
            scanner=scanner,
            store_factory=create_writer,
            lock_path=layout.market_refresh_lock,
            universe_id="all-main-board",
            clock=get_market_clock(),
            schema_initializer=initialize_writer_schema,
        ).execute(
            configured_start=settings.market_continuity_start_date,
            latest_completed_session=latest,
            requested_start=args.start,
            requested_end=args.end,
            completed_scan=scan,
        )
        fresh = result.fresh_scan
        if result.status == "current":
            output_status = "current"
        elif result.status == "enqueued":
            output_status = "gaps"
        elif result.status == "already_running":
            output_status = "unavailable"
        else:
            output_status = "unavailable"
        payload = _continuity_cli_payload(
            status=output_status,
            effective_start=fresh.effective_start if fresh else scan.effective_start,
            effective_end=fresh.effective_end if fresh else scan.effective_end,
            missing_sessions=fresh.missing_sessions if fresh else scan.missing_sessions,
            reason_code=result.reason_code,
            writes_control_state=result.writes_control_state,
            created_count=result.created_count,
            existing_count=result.existing_count,
            created_job_ids=tuple(item.job_id for item in result.created_jobs),
            execute=True,
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if result.status in {"current", "enqueued"} else 1
    except Exception:
        payload = _continuity_cli_payload(
            status="unavailable",
            reason_code="CONTROL_STATE_UNAVAILABLE",
            execute=getattr(args, "execute", False),
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 1


def _daily_shadow_error(
    *,
    trade_date: date,
    error_code: str,
    return_code: int = 1,
) -> int:
    print(
        json.dumps(
            {
                "status": "unavailable" if return_code == 1 else "error",
                "provider": "tickflow",
                "profile": "TICKFLOW_FREE_DAILY_BAR_OHLC_V1",
                "trade_date": trade_date.isoformat(),
                "error_code": error_code,
                "provider_requests": 0,
                "writes": False,
                "canonical_writes": False,
                "publication_enabled": False,
                "failover_enabled": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return return_code


def _market_provider_daily_shadow_command(args: argparse.Namespace) -> int:
    if args.execute and not args.external_authorization_id:
        return _daily_shadow_error(
            trade_date=args.trade_date,
            error_code="external_authorization_required",
            return_code=2,
        )
    if args.execute and not args.acknowledge_provider_requests:
        return _daily_shadow_error(
            trade_date=args.trade_date,
            error_code="provider_requests_acknowledgement_required",
            return_code=2,
        )
    try:
        from backend.app.market.daily_shadow_canonical import (
            DailyCanonicalReader,
            PublishedDailyCanonicalProjection,
        )
        from backend.app.market.daily_shadow_registry import (
            DailyShadowRegistry,
            DailyShadowRegistryReader,
        )
        from backend.app.market.providers.tickflow_daily_shadow import (
            TickFlowFreeDailyShadowAdapter,
        )

        settings = get_tickflow_free_runtime_settings()
        if args.execute and (
            not settings.provider_shadow_enabled or not settings.provider_shadow_execute_enabled
        ):
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="daily_shadow_execution_disabled",
                return_code=2,
            )
        canonical_root = settings.local_market_dataset_root or settings.nas_market_dataset_root
        if canonical_root is None:
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="canonical_unavailable",
            )
        layout = StorageLayout(settings)
        layout.validate_daily_bar_shadow_layout(
            canonical_roots=tuple(
                root
                for root in (
                    canonical_root,
                    settings.daily_bar_shadow_calendar_root if args.execute else None,
                )
                if root is not None
            )
        )
        control = DailyShadowRegistryReader(
            layout.daily_bar_shadow_database,
            evidence_root=layout.daily_bar_shadow_evidence_root,
            candidate_root=layout.daily_bar_shadow_candidate_root,
        ).read()
        if (
            control.status != "READY"
            or control.descriptor_sha256 is None
            or control.terms_evidence_sha256 is None
        ):
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="control_state_unavailable",
            )
        opened = PublishedDailyCanonicalProjection.open(
            DailyCanonicalReader(canonical_root, trade_date=args.trade_date)
        )
        if type(opened) is not PublishedDailyCanonicalProjection:
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="canonical_unavailable",
            )
        try:
            canonical_snapshot = opened.snapshot
            plan = TickFlowFreeDailyShadowAdapter().plan(canonical_snapshot)
        finally:
            opened.close()
        base = {
            "provider": "tickflow",
            "profile": plan.profile,
            "trade_date": args.trade_date.isoformat(),
            "request_count": plan.request_count,
            "expected_symbols": plan.symbol_count,
            "request_plan_sha256": plan.request_plan_sha256,
            "canonical_snapshot_sha256": plan.canonical_snapshot_sha256,
            "canonical_universe_sha256": plan.canonical_universe_sha256,
            "canonical_symbol_set_sha256": plan.canonical_symbol_set_sha256,
            "symbol_mapping_sha256": plan.symbol_mapping_sha256,
            "descriptor_sha256": control.descriptor_sha256,
            "terms_evidence_sha256": control.terms_evidence_sha256,
            "publication_enabled": False,
            "failover_enabled": False,
        }
        if not args.execute:
            print(
                json.dumps(
                    {
                        "status": "planned",
                        **base,
                        "provider_requests": 0,
                        "writes": False,
                        "canonical_writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        calendar_root = settings.daily_bar_shadow_calendar_root
        if calendar_root is None:
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="calendar_unavailable",
            )
        from backend.app.market.daily_shadow_candidates import DailyCandidateStore
        from backend.app.market.daily_shadow_models import (
            DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        )
        from backend.app.market.daily_shadow_worker import (
            DailyHalfOpenProbeResult,
            DailyShadowWorker,
        )
        from backend.app.market.providers.http import build_tickflow_free_client
        from backend.app.market.providers.tickflow_daily_shadow import (
            TickFlowFreeDailyShadowFetcher,
        )
        from backend.app.market.shadow_calendar import ConfirmedCalendarReader
        from backend.app.market.shadow_evidence import (
            ShadowEvidenceReader,
            ShadowEvidenceStore,
        )

        calendar_start = (
            control.expected_dates[0]
            if control.window is not None
            and control.window.state != "RESET"
            and control.expected_dates
            else args.trade_date
        )
        calendar_reader = ConfirmedCalendarReader(
            calendar_root,
            provider_id="tickflow",
            window_id=f"daily-window-{calendar_start.isoformat()}",
            universe_id="daily-canonical-universe-policy-v1",
            universe_sha256=DAILY_CANONICAL_UNIVERSE_POLICY_SHA256,
        )
        confirmed = None
        for span in range(19, 65):
            candidate = calendar_reader.read(
                calendar_start,
                calendar_start + timedelta(days=span),
                captured_at=f"reviewed-{control.descriptor_sha256}",
                snapshot_id=f"daily-calendar-{calendar_start.isoformat()}",
            )
            if len(candidate.confirmed_next_sessions) == 20:
                confirmed = candidate
                break
            if len(candidate.confirmed_next_sessions) > 20:
                break
        if (
            confirmed is None
            or confirmed.confirmed_next_sessions[0] != calendar_start
            or args.trade_date not in confirmed.confirmed_next_sessions
        ):
            return _daily_shadow_error(
                trade_date=args.trade_date,
                error_code="calendar_unavailable",
            )

        def canonical_factory(trade_date):
            return PublishedDailyCanonicalProjection.open(
                DailyCanonicalReader(canonical_root, trade_date=trade_date)
            )

        def fixed_five_probe(probe_date):
            probe_canonical = None
            try:
                opened_probe = canonical_factory(probe_date)
                if type(opened_probe) is not PublishedDailyCanonicalProjection:
                    return DailyHalfOpenProbeResult(
                        outcome="FAILURE",
                        provider_requests=0,
                        observed_symbols=0,
                        failure_class="canonical_unavailable",
                    )
                probe_canonical = opened_probe
                probe_plan = TickFlowFreeDailyShadowAdapter().fixed_five_probe_plan(
                    probe_canonical.snapshot
                )
                probe_fetch = TickFlowFreeDailyShadowFetcher.execute(
                    transport_factory=build_tickflow_free_client,
                    plan=probe_plan,
                )
                if probe_fetch.status != "ready" or len(probe_fetch.rows) != 5:
                    return DailyHalfOpenProbeResult(
                        outcome="FAILURE",
                        provider_requests=probe_fetch.request_count,
                        observed_symbols=0,
                        failure_class=probe_fetch.failure_class or "provider_failure",
                    )
                return DailyHalfOpenProbeResult(
                    outcome="SUCCESS",
                    provider_requests=probe_fetch.request_count,
                    observed_symbols=5,
                )
            except Exception:
                return DailyHalfOpenProbeResult(
                    outcome="FAILURE",
                    provider_requests=0,
                    observed_symbols=0,
                    failure_class="probe_unavailable",
                )
            finally:
                if probe_canonical is not None:
                    probe_canonical.close()

        worker = DailyShadowWorker(
            registry=DailyShadowRegistry(layout.daily_bar_shadow_database),
            canonical_factory=canonical_factory,
            confirmed_calendar=confirmed,
            fetcher=lambda request_plan: TickFlowFreeDailyShadowFetcher.execute(
                transport_factory=build_tickflow_free_client,
                plan=request_plan,
            ),
            evidence_store=ShadowEvidenceStore(layout.daily_bar_shadow_evidence_root),
            evidence_reader=ShadowEvidenceReader(layout.daily_bar_shadow_evidence_root),
            candidate_store=DailyCandidateStore(layout.daily_bar_shadow_candidate_root),
            terms_evidence_sha256=control.terms_evidence_sha256,
            owner=args.external_authorization_id,
            probe_runner=fixed_five_probe,
        )
        result = worker.run_one(args.trade_date)
        payload = {
            "status": "completed" if result.outcome == "SUCCESS" else "unavailable",
            **base,
            **result.model_dump(mode="json"),
            "writes": result.outcome
            not in {
                "SKIPPED_CIRCUIT_OPEN",
                "SKIPPED_HALF_OPEN",
                "BUSY",
                "ALREADY_TERMINAL",
                "RESET",
            },
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return (
            0 if result.outcome in {"SUCCESS", "ALREADY_TERMINAL", "HALF_OPEN_PROBE_SUCCESS"} else 1
        )
    except Exception:
        return _daily_shadow_error(
            trade_date=args.trade_date,
            error_code="daily_shadow_unavailable",
        )


def _replication_cli_path(
    value: Path,
    *,
    label: str,
    protected_roots: tuple[Path, ...] = (),
) -> Path:
    """Validate replication paths lexically before any operation-specific I/O."""
    raw = os.fspath(value)
    if not raw or "${" in raw or "$" in raw or "%" in raw:
        raise _CliArgumentError(f"{label} path is unresolved")
    candidate = Path(raw)
    if not candidate.is_absolute() or candidate == Path("/"):
        raise _CliArgumentError(f"{label} path must be an absolute non-root path")
    home = Path.home()
    if candidate == home or home in candidate.parents:
        raise _CliArgumentError(f"{label} path overlaps the user home")
    left = Path(os.path.realpath(candidate))
    for protected in protected_roots:
        right = Path(os.path.realpath(protected))
        if left == right or left in right.parents or right in left.parents:
            raise _CliArgumentError(f"{label} path overlaps local storage")
    return candidate


def _replication_protected_roots(settings: Settings) -> tuple[Path, ...]:
    return tuple(
        path
        for path in (
            settings.local_market_dataset_root,
            settings.nas_market_dataset_root,
            settings.local_control_dir,
            settings.local_staging_dir,
            settings.local_lock_dir,
            settings.local_temp_dir,
            settings.market_data_dir,
            settings.user_data_dir,
            settings.supplemental_data_dir,
            settings.provider_evidence_root,
            settings.provider_shadow_root,
            settings.replication_destination_root,
        )
        if path is not None
    )


def _replication_cli_error(error: Exception) -> int:
    print(
        json.dumps(
            {
                "status": "error",
                "reason_code": "INVALID_PATH",
                "writes": False,
                "provider_requests": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 2


def _operation_day_visible(published_at: str, operation_day: date) -> bool:
    try:
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        if published.tzinfo is None:
            return False
        cutoff = datetime.fromisoformat(
            f"{operation_day.isoformat()}T23:59:59.999999+08:00"
        ).astimezone(UTC)
        return published.astimezone(UTC) <= cutoff
    except (TypeError, ValueError):
        return False


def main() -> int:
    try:
        args = build_parser().parse_args()
    except _CliArgumentError:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_code": "invalid_cli_arguments",
                    "writes_data": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    if args.command == "calendar-sync":
        if (args.start is None) != (args.end is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "--start and --end must be used together",
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        if args.start is not None and args.end is not None and args.start > args.end:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "INVALID_CALENDAR_SYNC_RANGE",
                        "network_requests": 0,
                        "writes_calendar_state": False,
                        "canonical_writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
    if args.command == "calendar-generation-status":
        return _calendar_generation_status_command(args)
    if args.command == "calendar-generation-stage":
        return _calendar_generation_stage_command(args)
    if args.command == "calendar-maintenance":
        return _calendar_maintenance_command(args)
    if args.command == "market-continuity":
        return _market_continuity_command(args)
    if args.command == "market-universe":
        return _market_universe_command(args)
    if args.command == "market-provider-daily-shadow":
        return _market_provider_daily_shadow_command(args)
    if args.command == "market-failover-readiness":
        try:
            readiness = read_market_failover_readiness(get_market_failover_runtime_settings())
        except (ValidationError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "unavailable",
                        "error_code": "INVALID_FAILOVER_CONFIGURATION",
                        "provider_requests": 0,
                        "secondary_requests": 0,
                        "canonical_writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        print(canonical_json_bytes(readiness.model_dump(mode="json")).decode("utf-8"), end="")
        return 0 if readiness.status == "ready" else 1
    if args.command == "market-provider-status":
        settings = get_settings()
        provider = args.provider
        if provider is None:
            payload = {
                "status": "unavailable",
                "provider": None,
                "state": None,
                "unavailable_reason": "registry_missing",
                "writes": False,
                "provider_requests": 0,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
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
            payload = {
                "status": "unavailable",
                "provider": provider,
                "state": None,
                "unavailable_reason": "shadow_root_unavailable",
                "writes": False,
                "provider_requests": 0,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
        path = layout.provider_registry_database
        try:
            record = ShadowRegistry(path).read_status(provider)
            payload = {
                "status": "ready",
                "provider": record.provider_id.value,
                "state": record.admission_state.value,
                "unavailable_reason": None,
                "writes": False,
                "provider_requests": 0,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0
        except RegistryUnavailable:
            payload = {
                "status": "unavailable",
                "provider": provider,
                "state": None,
                "unavailable_reason": "registry_missing"
                if not path.exists()
                else "registry_schema_invalid",
                "writes": False,
                "provider_requests": 0,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
    if args.command == "market-provider-canary":
        from backend.app.market.providers.tickflow import (
            TickFlowAdapter,
            TickFlowCanaryRunner,
            TickFlowFreeAdapter,
            TickFlowFreeCanaryRunner,
        )
        from backend.app.market.providers.tushare import TushareAdapter

        symbols = tuple(args.symbols)
        tickflow_mode = None
        if args.provider == "tickflow":
            tickflow_mode = (
                "AUTHENTICATED_DISCOVERY"
                if args.capability == "authenticated"
                else "FREE_DAILY_DISCOVERY"
            )
        if args.provider == "tickflow" and tickflow_mode == "FREE_DAILY_DISCOVERY":
            adapter = TickFlowFreeAdapter()
        elif args.provider == "tickflow":
            adapter = TickFlowAdapter(plan_only=True)
        else:
            adapter = TushareAdapter(plan_only=True)
        if not args.execute:
            if tickflow_mode == "FREE_DAILY_DISCOVERY" and symbols:
                payload = {
                    "status": "error",
                    "provider": args.provider,
                    "provider_mode": tickflow_mode,
                    "error_code": "fixed_free_sample_required",
                    "provider_requests": 0,
                    "writes": False,
                }
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
                return 2
            plan = adapter.plan(args.trade_date, symbols=symbols)
            payload = {
                "status": "planned",
                "provider": args.provider,
                "trade_date": args.trade_date.isoformat(),
                "request_count": plan.request_count,
                "endpoints": [item.endpoint for item in plan.requests],
                "provider_requests": 0,
                "writes": False,
                "writes_evidence": False,
                "writes_canonical": False,
            }
            if tickflow_mode is not None:
                payload["provider_mode"] = tickflow_mode
            fixed_symbols = getattr(plan, "fixed_symbols", None)
            if fixed_symbols is not None:
                payload["fixed_symbols"] = list(fixed_symbols)
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0
        if not args.external_authorization_id:
            payload = {
                "status": "error",
                "provider": args.provider,
                "error_code": "external_authorization_required",
                "provider_requests": 0,
                "writes": False,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 2
        if not args.acknowledge_provider_requests:
            payload = {
                "status": "error",
                "provider": args.provider,
                "error_code": "provider_requests_acknowledgement_required",
                "provider_requests": 0,
                "writes": False,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 2
        if args.provider == "tushare":
            # Tushare's official transport is not proven HTTPS; keep it blocked before any
            # registry, credential, client or filesystem operation.
            payload = {
                "status": "blocked",
                "provider": args.provider,
                "error_code": "provider_transport_unproven",
                "provider_requests": 0,
                "writes": False,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
        postclient_boundary = False
        try:
            from backend.app.market.providers.http import (
                build_tickflow_client,
                build_tickflow_free_client,
            )

            settings = (
                get_tickflow_free_runtime_settings()
                if tickflow_mode == "FREE_DAILY_DISCOVERY"
                else get_settings()
            )
            layout = StorageLayout(settings)
            if tickflow_mode == "FREE_DAILY_DISCOVERY":
                runner = TickFlowFreeCanaryRunner(
                    settings=settings,
                    layout=layout,
                    registry=ShadowRegistry(layout.provider_registry_database),
                    client_factory=build_tickflow_free_client,
                )
            else:
                runner = TickFlowCanaryRunner(
                    settings=settings,
                    layout=layout,
                    registry=ShadowRegistry(layout.provider_registry_database),
                    client_factory=build_tickflow_client,
                )
            # Only failures raised after entering the runner can carry a provider
            # request count. Construction and all earlier gates are zero-request.
            postclient_boundary = True
            if tickflow_mode == "FREE_DAILY_DISCOVERY":
                if symbols:
                    raise PermissionError("fixed Free sample is required")
                result = runner.execute(
                    args.trade_date,
                    external_authorization_id=args.external_authorization_id,
                    acknowledge_provider_requests=args.acknowledge_provider_requests,
                )
            else:
                result = runner.execute(
                    args.trade_date,
                    symbols=symbols,
                    external_authorization_id=args.external_authorization_id,
                    acknowledge_provider_requests=args.acknowledge_provider_requests,
                )
            writes = getattr(result, "writes", result.writes_evidence)
            payload = {
                "status": result.status,
                "provider": result.provider,
                "trade_date": result.trade_date.isoformat(),
                "request_count": result.request_count,
                "endpoints": list(result.endpoints),
                "provider_requests": result.request_count,
                "writes": writes,
                "writes_evidence": result.writes_evidence,
                "writes_canonical": result.writes_canonical,
            }
            if tickflow_mode is not None:
                payload["provider_mode"] = tickflow_mode
            if hasattr(result, "units_status"):
                payload["units_status"] = result.units_status
            if hasattr(result, "capabilities"):
                payload["capabilities"] = result.capabilities.model_dump(mode="json")
                payload["daily_bar_qualified"] = result.daily_bar_qualified
                payload["adjustment_factor_qualified"] = result.adjustment_factor_qualified
                payload["starts_shadow"] = result.starts_shadow
                payload["fixed_symbols"] = list(result.fixed_symbols)
            if getattr(result, "failure_class", None) is not None:
                payload["failure_class"] = result.failure_class
            if getattr(result, "endpoint", None) is not None:
                payload["endpoint"] = result.endpoint
            print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0 if result.status == "discovered" else 1
        except Exception as exc:
            from backend.app.market.providers.http import (
                safe_public_endpoint,
                safe_public_failure_class,
            )

            try:
                provider_requests = getattr(exc, "request_count", 0) if postclient_boundary else 0
            except Exception:
                provider_requests = 0
            provider_requests = (
                provider_requests
                if type(provider_requests) is int and 0 <= provider_requests <= 4
                else 0
            )
            try:
                failure_class = safe_public_failure_class(getattr(exc, "failure_class", None))
            except Exception:
                failure_class = None
            try:
                endpoint = safe_public_endpoint(getattr(exc, "endpoint", None))
            except Exception:
                endpoint = None
            payload = {
                "status": "unavailable",
                "provider": args.provider,
                "error_code": "canary_execution_blocked",
                "provider_requests": provider_requests,
                "writes": False,
            }
            if failure_class is not None:
                payload["failure_class"] = failure_class
            if endpoint is not None:
                payload["endpoint"] = endpoint
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
    if args.command in {"market-provider-shadow", "market-provider-promote"}:
        # Task13 is an offline gate: plan is read-only and execute is blocked until an
        # external authorization record is supplied to a separately reviewed runner.
        if args.command == "market-provider-shadow":
            if args.execute and args.external_authorization_id:
                try:
                    from backend.app.market.shadow_scheduler import build_shadow_scheduler

                    settings = get_settings()
                    layout = StorageLayout(settings)
                    canonical_root = settings.local_market_dataset_root or (
                        settings.provider_shadow_root / "canonical"
                    )
                    build_shadow_scheduler(
                        registry=ShadowRegistry(layout.provider_registry_database),
                        canonical_root=canonical_root,
                        shadow_root=layout.provider_shadow_root,
                        shadow_start_date=args.start,
                        provider_id=args.provider,
                        window_id=f"shadow-{args.provider}-{args.start.isoformat()}-{args.end.isoformat()}",
                    )
                except Exception:
                    # Construction is fail-closed and deliberately does not start a worker;
                    # the external authorization gate remains the authority boundary.
                    pass
            payload = {
                "status": "planned" if not args.execute else "blocked",
                "provider": args.provider,
                "start": args.start.isoformat(),
                "end": args.end.isoformat(),
                "provider_requests": 0,
                "writes": False,
                "writes_canonical": False,
                "failover_enabled": False,
                "error_code": None if not args.execute else "external_authorization_required",
            }
        else:
            payload = {
                "status": "planned" if not args.execute else "blocked",
                "provider": args.provider,
                "window_id": args.window_id,
                "review_id": args.review_id,
                "provider_requests": 0,
                "writes": False,
                "failover_enabled": False,
                "error_code": None if not args.execute else "external_authorization_required",
            }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if not args.execute else 2
    if args.command == "market-provider-replay":
        try:
            payload = replay_cli_payload(
                get_settings().provider_evidence_root,
                args.evidence_id,
                args.compare_candidate_sha,
                args.compare_semantic_sha,
                args.compare_adapter_version,
            )
        except Exception:
            payload = {
                "status": "unavailable",
                "evidence_id": args.evidence_id,
                "candidate_sha256": None,
                "semantic_hash": None,
                "byte_match": None,
                "semantic_match": None,
                "normalization_clock_utc": None,
                "row_count": 0,
                "trade_date": None,
                "failure_class": "EVIDENCE_ROOT_UNAVAILABLE",
            }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if payload.get("status") == "ready" else 1
    if args.command == "provider-canary":
        report_arguments = {
            "trade_date": args.trade_date,
            "stock_symbol": args.stock_symbol,
            "index_symbol": args.index_symbol,
            "control_root": args.control_root,
            "data_root": args.data_root,
        }
        try:
            if args.execute != args.acknowledge_provider_requests:
                report = confirmation_required_report(**report_arguments)
                exit_code = 2
            elif not args.execute:
                report = plan_provider_canary(**report_arguments)
                exit_code = 0
            else:
                # The probe path never reads factors. Supplying a sentinel prevents the
                # provider's normal in-memory SQLite factor-cache initialization.
                report = run_provider_canary(
                    **report_arguments,
                    provider_factory=lambda *, max_attempts: BaoStockProvider(
                        max_attempts=max_attempts,
                        factor_cache=object(),
                    ),
                )
                exit_code = 0 if report.status == "ready" else 1
            payload = report.model_dump(mode="json")
        except ProviderCanaryError as error:
            payload = {
                "status": "error",
                "error_code": error.error_code,
                "writes_database": False,
                "writes_parquet": False,
                "writes_manifest": False,
                "writes_pointer": False,
                "refresh_triggered": False,
            }
            exit_code = 1
        except Exception:
            payload = {
                "status": "error",
                "error_code": "PROVIDER_ENDPOINT_FAILED",
                "writes_database": False,
                "writes_parquet": False,
                "writes_manifest": False,
                "writes_pointer": False,
                "refresh_triggered": False,
            }
            exit_code = 1
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return exit_code
    if args.command == "market-schema-migrate":
        try:
            settings = get_settings()
            layout = StorageLayout(settings)
            store = MarketStore(
                layout.local_paths.market_database,
                temp_directory=layout.duckdb_temporary,
            )
            with RefreshRunLock(layout.market_refresh_lock):
                store.initialize_all_writer_schema(
                    staging_directory=layout.market_refresh_lock.parent,
                )
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "market_control_schema_migration_busy",
                        "writes_market_control_schema": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "market_control_schema_migration_failed",
                        "writes_market_control_schema": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "status": "ready",
                    "migration": "market_control_schema",
                    "writes_market_control_schema": True,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "classification-sync":
        started_at = perf_counter()
        configured_timeout_seconds = args.socket_timeout_seconds
        provider = None
        try:
            settings = get_settings()
            configured_timeout_seconds = (
                args.socket_timeout_seconds or settings.baostock_socket_timeout_seconds
            )
            store = ClassificationStore(
                settings.market_data_dir / settings.classification_database_name,
                temp_directory=settings.local_temp_dir / "classification-duckdb",
            )
            discard = _DiscardWriter()
            # Provider construction/execution is single-threaded; redirects are process-global.
            with redirect_stdout(discard), redirect_stderr(discard):
                provider = (
                    BaoStockClassificationProvider(
                        min_request_interval_seconds=(
                            settings.auto_refresh_min_request_interval_seconds
                        ),
                        socket_timeout_seconds=configured_timeout_seconds,
                    )
                    if args.execute
                    else None
                )
                result = run_classification_sync(
                    as_of=args.as_of,
                    execute=args.execute,
                    store=store,
                    provider=provider,
                )
        except ClassificationFailureError as exc:
            print(
                json.dumps(
                    _classification_failure_payload(
                        as_of=args.as_of,
                        failure=exc.failure,
                    ),
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    _classification_failure_payload(
                        as_of=args.as_of,
                        failure=_unexpected_classification_failure(
                            started_at=started_at,
                            provider=provider,
                            configured_timeout_seconds=configured_timeout_seconds,
                        ),
                    ),
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status in {"dry-run", "ready"} else 1
    if args.command == "release-one-audit":
        if args.sessions < MINIMUM_RELEASE_ONE_SESSIONS:
            print(
                json.dumps(
                    {
                        "status": "not_ready",
                        "error_code": "release_one_audit_unavailable",
                        "writes_snapshot_data": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        try:
            discard = _DiscardWriter()
            with redirect_stdout(discard), redirect_stderr(discard):
                settings = get_settings()
                layout = StorageLayout(settings)
                report = ReleaseOneAcceptanceService(
                    regime_store=market_regime_store_from_settings(settings),
                    snapshot_store=RegimeSnapshotStore(layout.regime_snapshot_database),
                    classification_store=ClassificationStore(
                        settings.market_data_dir / settings.classification_database_name,
                        temp_directory=settings.local_temp_dir / "classification-duckdb",
                    ),
                ).run(
                    sessions=args.sessions,
                    audit_cutoff_at=datetime.now(UTC),
                )
            payload = {
                **report.model_dump(mode="json"),
                "writes_snapshot_data": False,
            }
            exit_code = 0 if report.status == "ready" else 1
        except Exception:
            payload = {
                "status": "not_ready",
                "error_code": "release_one_audit_unavailable",
                "writes_snapshot_data": False,
            }
            exit_code = 1
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return exit_code
    settings = get_settings()
    prevalidated_lineage_pairs = None
    local_effective_dates = None
    configured_backfill_root = None
    if args.command == "backfill":
        # Dataset-root discovery is market/backfill-only.  Private backups and
        # unrelated commands must not probe NAS/local market configuration.
        configured_backfill_root = configured_market_dataset_root(settings)
    # Effective-day planning is a local calendar decision.  Resolve it before
    # constructing BaoStockProvider so an unavailable calendar is a strict
    # zero-provider/zero-write result.
    if (
        args.command == "backfill"
        and args.effective_days is not None
        and configured_backfill_root is not None
    ):
        if args.effective_days < 1:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "SOURCE_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        calendar_start = args.end - timedelta(days=args.effective_days * 3)
        local_confirmed = (
            get_trading_calendar().snapshot().confirmed_open_sessions(calendar_start, args.end)
        )
        if local_confirmed is None or len(local_confirmed) < args.effective_days:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "SOURCE_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        local_effective_dates = list(local_confirmed[-args.effective_days :])
    # A local lineage file is a preflight input, not a provider-side hint.  Do
    # this validation before StoragePreflight, pointer reconciliation, or
    # provider construction so malformed/forged input is a strict zero-write
    # and zero-request rejection.
    if args.command == "backfill" and args.lineage_input is not None:
        if configured_backfill_root is None:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_NOT_SUPPORTED_FOR_PLAIN_STORE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        if args.start is None and local_effective_dates is None:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_REQUIRES_EXPLICIT_START",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        confirmed = (
            tuple(local_effective_dates)
            if local_effective_dates is not None
            else get_trading_calendar().snapshot().confirmed_open_sessions(args.start, args.end)
        )
        if confirmed is None:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "SOURCE_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        try:
            prevalidated_lineage_pairs = _read_backfill_lineage_file(args.lineage_input, confirmed)
        except _BackfillLineageInputInvalid:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_INVALID",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        except (OSError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
    elif args.command == "backfill" and configured_backfill_root is not None:
        try:
            incumbent_mode = read_dataset_lineage_mode(settings)
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "CONTROL_STATE_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        if incumbent_mode != "legacy":
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_REQUIRED_FOR_DATASET_STORE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        if args.start is not None or local_effective_dates is not None:
            confirmed = (
                get_trading_calendar()
                .snapshot()
                .confirmed_open_sessions(args.start or local_effective_dates[0], args.end)
            )
            if confirmed is None:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "error_code": "SOURCE_UNAVAILABLE",
                            "provider_requests": 0,
                            "writes": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            prevalidated_lineage_pairs = tuple(
                (trade_date, {"mode": "legacy"}) for trade_date in confirmed
            )
    if args.command == "market-replication-status":
        try:
            result = ReplicationStatusService(settings).read()
            print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, sort_keys=True))
            return 0 if result.status in {"ready", "disabled"} else 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "unavailable",
                        "reason_code": "REPLICATION_STATE_UNAVAILABLE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
    if args.command == "market-replication-init":
        try:
            destination = _replication_cli_path(
                args.destination,
                label="destination",
                protected_roots=_replication_protected_roots(settings),
            )
        except _CliArgumentError as exc:
            return _replication_cli_error(exc)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan",
                        "status": "dry_run",
                        "reason_code": "NONE",
                        "writes": False,
                        "provider_requests": 0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if args.acknowledge != DESTINATION_INIT_ACK:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": "ACKNOWLEDGEMENT_REQUIRED",
                        "writes": False,
                        "provider_requests": 0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        try:
            descriptor = initialize_destination(
                destination,
                acknowledgement=DESTINATION_INIT_ACK,
                local_root=settings.local_market_dataset_root,
            )
        except Exception:
            return _replication_cli_error(RuntimeError("destination initialization failed"))
        print(
            json.dumps(
                {
                    "status": "ready",
                    "reason_code": "NONE",
                    "descriptor_sha256": descriptor.descriptor_sha256,
                    "writes": True,
                    "provider_requests": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.command in {"market-replicate", "market-restore"}:
        layout = StorageLayout(settings)
        if args.command == "market-restore":
            try:
                _replication_cli_path(
                    args.source,
                    label="source",
                    protected_roots=_replication_protected_roots(settings),
                )
                _replication_cli_path(
                    args.destination,
                    label="destination",
                    protected_roots=_replication_protected_roots(settings),
                )
            except _CliArgumentError as exc:
                return _replication_cli_error(exc)
            service = RestoreService(
                args.source,
                canonical_roots=tuple(
                    root
                    for root in (
                        settings.local_market_dataset_root,
                        settings.nas_market_dataset_root,
                    )
                    if root is not None
                ),
                audit_root=(
                    (Path.cwd() / layout.local_paths.control).resolve() / "restore-audit"
                    if args.execute
                    else None
                ),
            )
            result = (
                service.execute(args.destination)
                if args.execute
                else service.plan(args.destination)
            )
            payload = result.model_dump(mode="json")
            payload.update(
                {
                    "provider_requests": 0,
                    "writes": result.effects.writes,
                    "operation_day": (
                        args.operation_day.isoformat() if args.operation_day is not None else None
                    ),
                }
            )
            if args.operation_day is not None:
                try:
                    snapshot = service._snapshot(None)
                    visible = _operation_day_visible(
                        snapshot.record.source_published_at,
                        args.operation_day,
                    )
                except Exception:
                    visible = False
                if not visible:
                    payload.update({"status": "unavailable", "reason_code": "SOURCE_UNAVAILABLE"})
                    return_code = 1
                else:
                    return_code = 0 if result.status in {"dry_run", "ready"} else 1
            else:
                return_code = 0 if result.status in {"dry_run", "ready"} else 1
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return return_code

        try:
            destination = args.destination or settings.replication_destination_root
            if destination is None:
                raise _CliArgumentError("destination is required")
            destination = _replication_cli_path(
                destination,
                label="destination",
                # The configured destination is the one mutable root this
                # command is authorized to operate on; all other configured
                # roots remain protected from overlap.
                protected_roots=tuple(
                    root for root in _replication_protected_roots(settings) if root != destination
                ),
            )
        except _CliArgumentError as exc:
            return _replication_cli_error(exc)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "plan",
                        "status": "dry_run",
                        "reason_code": "NONE",
                        "provider_requests": 0,
                        "writes": False,
                        "destination_writes": False,
                        "operation_day": (
                            args.operation_day.isoformat()
                            if args.operation_day is not None
                            else None
                        ),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if not (settings.replication_enabled and settings.replication_drain_enabled):
            print(
                json.dumps(
                    {
                        "status": "disabled",
                        "writes": False,
                        "provider_requests": 0,
                        "operation_day": (
                            args.operation_day.isoformat()
                            if args.operation_day is not None
                            else None
                        ),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if destination != settings.replication_destination_root:
            settings = settings.model_copy(update={"replication_destination_root": destination})
            layout = StorageLayout(settings)
        source_root = configured_market_dataset_root(settings)
        if source_root is None:
            payload = {
                "status": "unavailable",
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "provider_requests": 0,
                "writes": False,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 1
        try:
            worker = build_replication_drain_worker(settings, layout=layout)
            if worker is None:
                payload = {"status": "disabled", "writes": False, "provider_requests": 0}
            else:
                payload = worker.run_once(
                    operation_day=(
                        args.operation_day.isoformat() if args.operation_day is not None else None
                    )
                )
        except Exception:
            payload = {
                "status": "unavailable",
                "reason_code": "SOURCE_CHECKPOINT_UNAVAILABLE",
                "provider_requests": 0,
                "writes": False,
            }
        payload.setdefault("mode", "execute" if args.execute else "plan")
        payload.setdefault(
            "operation_day",
            args.operation_day.isoformat() if args.operation_day is not None else None,
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if payload.get("status") in {"idle", "replicated", "already_replicated"} else 1
    if args.command == "market-regime-snapshots":
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        if args.end < args.start or args.start > today or args.end > today:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "regime_snapshot_capture_unavailable",
                        "writes_snapshot_data": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        try:
            discard = _DiscardWriter()
            with redirect_stdout(discard), redirect_stderr(discard):
                service = RegimeSnapshotCaptureService(
                    regime_store=market_regime_store_from_settings(settings),
                    snapshot_store=RegimeSnapshotStore(
                        StorageLayout(settings).regime_snapshot_database
                    ),
                )
                if args.execute:
                    outcome = service.capture_range(
                        args.start,
                        args.end,
                        evidence_cutoff_at=datetime.combine(args.end, time.max, UTC),
                    )
                    payload = {
                        "status": (
                            "ready" if not outcome.errors and not outcome.conflicts else "error"
                        ),
                        **outcome.model_dump(mode="json"),
                        "writes_snapshot_data": outcome.inserted > 0,
                    }
                    exit_code = 0 if payload["status"] == "ready" else 1
                else:
                    plan = service.plan(args.start, args.end)
                    payload = {
                        "status": "dry-run",
                        **plan.model_dump(mode="json"),
                        "writes_snapshot_data": False,
                    }
                    exit_code = 0
        except (MarketReadUnavailable, RegimeSnapshotUnavailable, ValueError):
            payload = {
                "status": "error",
                "error_code": "regime_snapshot_capture_unavailable",
                "writes_snapshot_data": False,
            }
            exit_code = 1
        except Exception:
            payload = {
                "status": "error",
                "error_code": "regime_snapshot_capture_unavailable",
                "writes_snapshot_data": False,
            }
            exit_code = 1
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return exit_code
    if args.command == "backup-private-data":
        try:
            outcome = PrivateBackupSetService(
                {
                    "user": settings.user_data_dir / settings.user_database_name,
                    "portfolio": (settings.user_data_dir / settings.portfolio_database_name),
                },
                args.backup_root,
                keep_daily=args.keep_daily,
                keep_weekly=args.keep_weekly,
            ).run(datetime.now(UTC))
        except (PrivateBackupError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "private_backup_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "status": outcome.status,
                    "integrity_check": outcome.integrity_check,
                    "completed_at": outcome.completed_at.isoformat(),
                    "bundle_id": outcome.bundle_id,
                    "daily_bundle": outcome.daily_bundle.name,
                    "weekly_bundle": outcome.weekly_bundle.name,
                    "database_status": outcome.database_status,
                    "writes_market_data": False,
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "storage-init":
        if not args.initialize_empty_nas_dataset:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": "explicit_initialization_required",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            root = EmptyDatasetInitializer(settings).initialize()
        except DatasetInitializationError as error:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": str(error),
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        readiness = StoragePreflight(settings).inspect()
        print(
            json.dumps(
                {
                    "status": "ready",
                    "dataset_root": str(root),
                    "storage": readiness.model_dump(mode="json"),
                    "writes_market_data": True,
                },
                ensure_ascii=False,
            )
        )
        return 0
    readiness = StoragePreflight(settings).inspect()
    if (
        settings.nas_market_dataset_root is not None
        and not readiness.market_data_available
        and args.command
        in {
            "refresh",
            "backfill",
            "full-market-backfill",
            "auto-refresh-once",
            "export",
        }
    ):
        print(
            json.dumps(
                {
                    "status": "error",
                    "quality_issues": ["market_storage_unavailable"],
                    "storage_status": readiness.status,
                    "reason_code": readiness.reason_code,
                    "writes_market_data": False,
                },
                ensure_ascii=False,
            )
        )
        return 1
    layout = StorageLayout(settings)
    if getattr(args, "execute", False) or args.command not in {
        "calendar-sync",
        "auto-refresh-once",
    }:
        layout.ensure_local_runtime_dirs()
    socket_timeout_seconds = (
        getattr(args, "socket_timeout_seconds", None) or settings.baostock_socket_timeout_seconds
    )
    health_options = {
        "failure_threshold": settings.provider_circuit_failure_threshold,
        "cooldown_seconds": settings.provider_circuit_cooldown_seconds,
        "probe_lease_seconds": settings.provider_circuit_probe_lease_seconds,
    }
    now = get_market_clock()()
    if args.command == "auto-refresh-once" and not args.execute:
        # Planning is deliberately before writer/NAS/provider construction.  The local control
        # reader never initializes a missing DuckDB or mutates the NAS pointer.
        try:
            if layout.provider_health_database.exists():
                health_store = SQLiteProviderHealthStore(
                    layout.provider_health_database,
                    **health_options,
                )
                provider_health = health_store.provider_health_snapshot()
            else:
                provider_health = InMemoryProviderHealthStore(
                    **health_options
                ).provider_health_snapshot()
            read_store = MarketStore(
                layout.local_paths.market_database,
                temp_directory=layout.duckdb_temporary,
                read_only=True,
            )
            published = read_store.published_refresh()
            decision = MarketAutomationService(
                read_store,
                object(),
                get_trading_calendar(),
                required_symbols=set,
            ).policy.decide(
                now,
                published_as_of=(published.requested_date if published is not None else None),
                state=read_store.scheduler_state(),
            )
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "PROVIDER_HEALTH_UNAVAILABLE",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "action": decision.action,
                    "target_session": (
                        decision.target_session.isoformat()
                        if decision.target_session is not None
                        else None
                    ),
                    "refresh_state": decision.refresh_state,
                    "next_run_at": (
                        decision.next_run_at.isoformat()
                        if decision.next_run_at is not None
                        else None
                    ),
                    "provider_health": _provider_health_payload(provider_health),
                    "writes_market_data": False,
                    "execute_requires": "--execute",
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "calendar-sync":
        now = get_market_clock()()
        runtime_result = None
        if args.execute and settings.calendar_runtime_enabled:
            # The existing calendar-sync job owns one optional runtime slot.  Run it before
            # planning legacy reconciliation so a newly promoted authority gets a fresh plan.
            runtime_result = _calendar_runtime_service(settings).execute()
        runtime_calendar = build_live_trading_calendar(
            CalendarRuntimeSettings.from_settings(settings)
        )
        calendar_snapshot = runtime_calendar.snapshot()
        if settings.calendar_runtime_enabled and calendar_snapshot.status != "confirmed":
            print(
                json.dumps(
                    {
                        "status": "unavailable",
                        "outcome": "CONTROL_STATE_UNAVAILABLE",
                        "network_requests": 0,
                        "writes_calendar_state": False,
                        "canonical_writes": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        calendar_store = CalendarSyncStore(
            layout.local_paths.control / settings.calendar_sync_database_name,
            initialize=False,
        )
        planning_service = CalendarSyncService(
            calendar_store,
            calendar_snapshot,
            None,
        )
        if args.execute:
            planning_service.initialize_for_execution()
        plan = planning_service.plan(
            now=now,
            mode=args.mode,
            startup=args.startup,
            start_date=args.start,
            end_date=args.end,
        )
        if not args.execute:
            decision = planning_service.policy.decide(
                now,
                state=calendar_store.state(),
                startup=args.startup,
            )
            print(
                json.dumps(
                    {
                        "status": "dry-run",
                        "action": plan.mode if plan is not None else "none",
                        "plan": (plan.model_dump(mode="json") if plan is not None else None),
                        "next_sync_at": (
                            decision.next_sync_at.isoformat()
                            if decision.next_sync_at is not None
                            else None
                        ),
                        "network_requests": 0,
                        "writes_calendar_state": False,
                        "execute_requires": "--execute",
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        if plan is None:
            print(
                json.dumps(
                    {
                        "status": "not-due",
                        "writes_calendar_state": False,
                        **(
                            {
                                "calendar_maintenance": _calendar_maintenance_payload(
                                    runtime_result, execute=True
                                )
                            }
                            if runtime_result is not None
                            else {}
                        ),
                    },
                    ensure_ascii=False,
                )
            )
            return (
                0
                if runtime_result is None
                or runtime_result.outcome
                in {
                    "STAGED",
                    "SOURCE_PENDING",
                    "ALREADY_ATTEMPTED",
                    "NOT_DUE",
                    "PROMOTED",
                }
                else 1
            )
        try:
            calendar_health_store = SQLiteProviderHealthStore(
                layout.provider_health_database,
                failure_threshold=settings.provider_circuit_failure_threshold,
                cooldown_seconds=settings.provider_circuit_cooldown_seconds,
                probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
            )
            calendar_health_store.initialize()
            provider = BaoStockProvider(
                min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
                socket_timeout_seconds=socket_timeout_seconds,
            )
            with RefreshRunLock(layout.market_refresh_lock):
                result = CalendarSyncService(
                    calendar_store,
                    runtime_calendar,
                    provider,
                    health_store=calendar_health_store,
                ).execute(plan)
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["calendar_sync_failed"],
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            public_result = CalendarSyncResult.model_validate(result.model_dump(mode="python"))
        except (ValidationError, TypeError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "INVALID_CALENDAR_SYNC_RESULT",
                        # Validation occurs after execute; request accounting cannot be
                        # proven from an invalid result and must not be reported as zero.
                        "network_requests": None,
                        "writes_calendar_state": False,
                        "canonical_writes": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    **public_result.model_dump(mode="json"),
                    "writes_calendar_state": (
                        public_result.status != "skipped_circuit_open"
                        and public_result.failure_code is None
                    ),
                    **(
                        {
                            "calendar_maintenance": _calendar_maintenance_payload(
                                runtime_result, execute=True
                            )
                        }
                        if runtime_result is not None
                        else {}
                    ),
                },
                ensure_ascii=False,
            )
        )
        runtime_ok = runtime_result is None or runtime_result.outcome in {
            "STAGED",
            "SOURCE_PENDING",
            "ALREADY_ATTEMPTED",
            "NOT_DUE",
            "PROMOTED",
        }
        return 0 if result.status in {"ready", "observed_only"} and runtime_ok else 1
    control_store = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )
    configured_store = build_nas_market_store(settings, layout=layout)
    store = (
        configured_store
        if readiness.mode in {"nas", "local_dataset"} and configured_store is not None
        else control_store
    )
    if isinstance(store, NasMarketStore):
        try:
            store.validate_readiness()
            store.reconcile_control_pointer()
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["market_storage_unavailable"],
                        "reason_code": "nas_dataset_read_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
    if args.command == "full-market-backfill":
        if not isinstance(store, NasMarketStore):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["nas_market_dataset_required"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        provider = BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            factor_cache_path=str(layout.local_paths.factor_cache_database),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        service = FullMarketHistoryService(store, provider)
        if not args.execute_all_main_board_history:
            try:
                plan = service.plan(
                    start_date=args.start,
                    end_date=args.end,
                    max_sessions=args.max_sessions,
                )
            except Exception:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["calendar_or_plan_error"],
                            "writes_market_data": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            payload = plan.model_dump(mode="json")
            payload.update(
                {
                    "status": "dry-run",
                    "scope": "all-main-board-history",
                    "writes_market_data": False,
                    "network_note": ("one BaoStock calendar query was used to build this plan"),
                    "execute_requires": "--execute-all-main-board-history",
                }
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                plan = service.plan(
                    start_date=args.start,
                    end_date=args.end,
                    max_sessions=args.max_sessions,
                )
                result = service.execute(plan)
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["nas_dataset_operation_failed"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["history_execution_failed"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status == "ready" else 1
    if args.command == "export":
        try:
            output = store.export_date(args.trade_date, layout.local_paths.staging / "exports")
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["market_storage_unavailable"],
                        "reason_code": "nas_dataset_read_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps({"status": "ready", "path": str(output)}, ensure_ascii=False))
        return 0
    if args.command == "refresh-runs":
        print(
            json.dumps(
                [item.model_dump(mode="json") for item in store.list_refreshes()],
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "auto-refresh-once":
        calendar = get_trading_calendar()
        try:
            health_store = SQLiteProviderHealthStore(
                layout.provider_health_database,
                **health_options,
            )
            health_store.initialize()
            provider = BaoStockProvider(
                min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
                factor_cache_path=str(layout.local_paths.factor_cache_database),
                socket_timeout_seconds=socket_timeout_seconds,
            )
            canonical_refresh = None
            if hasattr(provider, "client") and hasattr(provider, "factor_cache"):
                canonical_adapter = BaoStockProviderAdapter(
                    client=provider.client,
                    factor_cache=provider.factor_cache,
                    min_request_interval_seconds=settings.auto_refresh_min_request_interval_seconds,
                    socket_timeout_seconds=socket_timeout_seconds,
                )
                canonical_refresh = canonical_refresh_callback(
                    store,
                    canonical_adapter,
                    evidence_root=layout.provider_evidence_root,
                    lock_path=layout.market_refresh_lock,
                    factor_cache=provider.factor_cache,
                )
            user_store = UserStore(layout.local_paths.user_database)
            continuity = None
            coordinator = None
            repair_executor = None
            if (
                settings.market_repair_enabled
                and settings.market_continuity_start_date is not None
                and isinstance(store, NasMarketStore)
            ):
                queue_store = store.control
                calendar_sync_path = (
                    layout.local_paths.control / settings.calendar_sync_database_name
                )
                continuity = ContinuityInventory(
                    calendar=calendar,
                    inventory_reader=store,
                    inventory_mode="immutable_dataset",
                    calendar_conflict=lambda: read_calendar_conflict(calendar_sync_path),
                )
                coordinator = RepairClaimCoordinator(
                    store=queue_store,
                    health_store=health_store,
                    lock_path=layout.market_refresh_lock,
                    owner="auto-refresh-repair",
                    lease_seconds=settings.market_repair_lease_seconds,
                )
                repair_executor = ContinuityRepairExecutor(
                    coordinator=coordinator,
                    queue_store=queue_store,
                    canonical_store=store,
                    provider=provider,
                    inventory_reader=store,
                    required_symbols=lambda: collect_required_symbols(user_store),
                    clock=get_market_clock(),
                    retry_policy=RepairRetryPolicy(
                        max_attempts=settings.market_repair_max_attempts,
                        base_seconds=settings.market_repair_retry_base_seconds,
                    ),
                    scanner=continuity,
                    configured_start=settings.market_continuity_start_date,
                    queue_schema_initializer=queue_store.initialize_continuity_schema,
                    health_store=health_store,
                    post_publish=build_after_close_pipeline(
                        layout.local_paths.user_database,
                        store,
                        settings=settings,
                    ),
                )
            service = MarketAutomationService(
                store,
                provider,
                calendar,
                required_symbols=lambda: collect_required_symbols(user_store),
                lock_path=layout.market_refresh_lock,
                post_publish=build_after_close_pipeline(
                    layout.local_paths.user_database,
                    store,
                    settings=settings,
                ),
                health_store=health_store,
                probe_runner=BaoStockProbeRunner(
                    lambda *, max_attempts: BaoStockProvider(
                        max_attempts=max_attempts,
                        min_request_interval_seconds=(
                            settings.auto_refresh_min_request_interval_seconds
                        ),
                        socket_timeout_seconds=socket_timeout_seconds,
                    )
                ),
                continuity=coordinator,
                repair_enabled=(repair_executor is not None),
                repair_executor=repair_executor,
                canonical_refresh=canonical_refresh,
                universe_post_success_hook=make_universe_post_success_hook(
                    environment=settings.environment,
                    mode=settings.market_universe_mode,
                    enabled=settings.market_universe_maintenance_enabled,
                    interval_seconds=settings.market_universe_maintenance_interval_seconds,
                    sidecar=UniverseSidecarStore(layout.universe_contract_database),
                    calendar_store=CalendarGenerationStore(layout.calendar_generation_database),
                    classification_store=ClassificationStore(
                        settings.market_data_dir / settings.classification_database_name,
                        temp_directory=settings.local_temp_dir / "classification-duckdb",
                    ),
                    user_store=user_store,
                    lock_path=layout.market_refresh_lock,
                    provider_factory=make_baostock_classification_provider,
                    evidence_root=layout.provider_evidence_root,
                    context_validator=CandidateStore(
                        layout.provider_evidence_root
                    ).verify_publication_context,
                ),
                market_universe_maintenance_enabled=settings.market_universe_maintenance_enabled,
                market_universe_mode=settings.market_universe_mode,
                environment=settings.environment,
                universe_context_validator=CandidateStore(
                    layout.provider_evidence_root
                ).verify_publication_context,
            )
            outcome = service.run_due_once(now)
            if settings.replication_enabled and settings.replication_drain_enabled:
                try:
                    worker = build_replication_drain_worker(settings, layout=layout)
                    if worker is not None:
                        worker.run_once()
                except Exception:
                    # Drain failures are typed/observational and must never
                    # rewrite the canonical automation result.
                    pass
            provider_health = health_store.provider_health()
        except Exception as error:
            failure = market_failure_from_exception(error, stage="fetch")
            print(
                json.dumps(
                    _market_failure_payload(failure),
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            automation_payload = _automation_outcome_payload(outcome)
        except ValidationError:
            # A contradictory internal outcome is an unavailable automation result.  Keep
            # this boundary allowlisted: Pydantic's details may contain private paths,
            # request keys, or provider text from a bypassed model.
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": "AUTOMATION_RESULT_UNAVAILABLE",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    **automation_payload,
                    "provider_health": _provider_health_payload(provider_health),
                },
                ensure_ascii=False,
            )
        )
        if outcome.state.error_code == "PROVIDER_PROBE_SUCCEEDED":
            return 0
        return 1 if outcome.state.refresh_state in {"retry_wait", "delayed", "error"} else 0
    if args.command == "backfill":
        if (args.start is None) == (args.effective_days is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "provide exactly one of --start or --effective-days",
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        if (args.strategy_id is None) != (args.strategy_version is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "--strategy-id and --strategy-version must be used together",
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        lineage_pairs = prevalidated_lineage_pairs
        if args.lineage_input is not None and not isinstance(store, NasMarketStore):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "BACKFILL_LINEAGE_INPUT_NOT_SUPPORTED_FOR_PLAIN_STORE",
                        "provider_requests": 0,
                        "writes": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2
        if args.lineage_input is not None and args.start is not None and lineage_pairs is None:
            confirmed = (
                get_trading_calendar().snapshot().confirmed_open_sessions(args.start, args.end)
            )
            if confirmed is None:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "error_code": "SOURCE_UNAVAILABLE",
                            "provider_requests": 0,
                            "writes": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            try:
                lineage_pairs = _read_backfill_lineage_file(args.lineage_input, confirmed)
            except _BackfillLineageInputInvalid:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "error_code": "BACKFILL_LINEAGE_INPUT_INVALID",
                            "provider_requests": 0,
                            "writes": False,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                return 2
            except (OSError, ValueError):
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "error_code": "BACKFILL_LINEAGE_INPUT_UNAVAILABLE",
                            "provider_requests": 0,
                            "writes": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 2
        provider = BaoStockProvider(
            min_request_interval_seconds=max(args.min_request_interval, 0.2),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        selected_dates = None
        start_date = args.start
        if args.effective_days is not None:
            if local_effective_dates is not None:
                selected_dates = local_effective_dates
                start_date = selected_dates[0]
            else:
                try:
                    selected_dates = resolve_effective_window(
                        provider,
                        end_date=args.end,
                        effective_days=args.effective_days,
                    )
                except Exception:
                    print(
                        json.dumps(
                            {"status": "error", "quality_issues": ["calendar_or_plan_error"]},
                            ensure_ascii=False,
                        )
                    )
                    return 1
                start_date = selected_dates[0]
        service = BackfillService(store, provider)
        try:
            if lineage_pairs is not None:
                plan = service.plan_dataset(
                    start_date=start_date,
                    end_date=args.end,
                    symbols=args.symbols,
                    symbol_batch_size=args.symbol_batch_size,
                    date_batch_size=args.date_batch_size,
                    max_batches=args.max_batches,
                    trading_dates=selected_dates or [item[0] for item in lineage_pairs],
                    lineage_pairs=[
                        {"trade_date": item[0], "lineage_input": item[1]} for item in lineage_pairs
                    ],
                )
            else:
                plan = service.plan(
                    start_date=start_date,
                    end_date=args.end,
                    symbols=args.symbols,
                    symbol_batch_size=args.symbol_batch_size,
                    date_batch_size=args.date_batch_size,
                    max_batches=args.max_batches,
                    trading_dates=selected_dates,
                )
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["calendar_or_plan_error"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        required_history = None
        if args.strategy_id is not None:
            strategy_store = StrategyStore(layout.local_paths.user_database)
            version = strategy_store.get_version(
                args.strategy_id,
                args.strategy_version,
            )
            required_history = minimum_effective_days(version.rule_ast)
            if len(plan.trading_dates) < required_history:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "message": (
                                f"strategy requires {required_history} effective days; "
                                f"plan has {len(plan.trading_dates)}"
                            ),
                        },
                        ensure_ascii=False,
                    )
                )
                return 2
        if not args.execute:
            payload = plan.model_dump(mode="json")
            payload.update(
                {
                    "status": "dry-run",
                    "network_note": "one BaoStock calendar query was used to build this plan",
                    "strategy_minimum_effective_days": required_history,
                }
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        result = service.execute(
            plan,
            min_request_interval_seconds=args.min_request_interval,
        )
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status in {"ready", "partial"} else 1
    if args.all_main_board and (args.dry_run or not args.execute_all_main_board):
        inspection = None
        elapsed = None
        if args.inspect_universe:
            calendar = get_trading_calendar()
            if calendar.session_status(args.trade_date) != "open":
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["official_calendar_not_confirmed_open"],
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            provider = BaoStockProvider(
                min_request_interval_seconds=0.5,
                socket_timeout_seconds=socket_timeout_seconds,
            )
            started = perf_counter()
            try:
                with _probe_timeout(args.probe_timeout_seconds):
                    inspection = provider.inspect_main_board(args.trade_date)
            except Exception:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["universe_inspection_failed"],
                            "probe_timeout_seconds": args.probe_timeout_seconds,
                            "writes_market_data": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            elapsed = round(perf_counter() - started, 3)
        base_estimated_requests = (
            inspection.metadata_provider_requests + 5 if inspection is not None else 7
        )
        factor_cache_path = layout.local_paths.factor_cache_database
        cached_factor_count = (
            (
                len(
                    AdjustmentFactorCache(factor_cache_path).exact_snapshots(
                        inspection.main_board_symbols,
                        args.trade_date,
                    )
                )
                if factor_cache_path.exists()
                else 0
            )
            if inspection is not None
            else None
        )
        factor_bootstrap_remaining = (
            inspection.main_board_count - cached_factor_count
            if inspection is not None and cached_factor_count is not None
            else None
        )
        estimated_requests = (
            base_estimated_requests + factor_bootstrap_remaining
            if factor_bootstrap_remaining is not None
            else base_estimated_requests
        )
        estimated_seconds_lower_bound = (
            round(
                elapsed / inspection.metadata_provider_requests * base_estimated_requests
                + (factor_bootstrap_remaining or 0) * 0.5,
                1,
            )
            if inspection is not None
            and elapsed is not None
            and inspection.metadata_provider_requests
            else None
        )
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "scope": "all-main-board",
                    "trade_date": args.trade_date.isoformat(),
                    "batch_cap": 1,
                    "request_batches": 1,
                    "requested_count": (
                        inspection.total_expected_count if inspection is not None else None
                    ),
                    "main_board_count": (
                        inspection.main_board_count if inspection is not None else None
                    ),
                    "shanghai_count": (
                        inspection.shanghai_count if inspection is not None else None
                    ),
                    "shenzhen_count": (
                        inspection.shenzhen_count if inspection is not None else None
                    ),
                    "metadata_provider_requests": (
                        inspection.metadata_provider_requests if inspection is not None else 0
                    ),
                    "metadata_probe_elapsed_seconds": elapsed,
                    "cached_factor_count": cached_factor_count,
                    "factor_bootstrap_remaining": factor_bootstrap_remaining,
                    "estimated_provider_requests": estimated_requests,
                    "provider_requests_with_retries_upper_bound": (estimated_requests * 2),
                    "min_request_interval_seconds": 0.5,
                    "minimum_throttle_elapsed_seconds": round(
                        (estimated_requests - 1) * 0.5,
                        1,
                    ),
                    "estimated_execution_seconds_lower_bound": (estimated_seconds_lower_bound),
                    "recommended_resource_window_seconds": (
                        3600 if factor_bootstrap_remaining else 900
                    ),
                    "writes_market_data": False,
                    "execute_requires": "--execute-all-main-board",
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.dry_run:
        symbols = sorted(set(args.symbols))
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "scope": "explicit-symbols",
                    "trade_date": args.trade_date.isoformat(),
                    "symbols": symbols,
                    "requested_count": len(symbols),
                    "batch_cap": 1,
                    "estimated_provider_requests_upper_bound": 1 + 2 * len(symbols),
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.all_main_board:
        calendar = get_trading_calendar()
        if calendar.session_status(args.trade_date) != "open":
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["official_calendar_not_confirmed_open"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                provider = BaoStockProvider(
                    min_request_interval_seconds=(
                        settings.auto_refresh_min_request_interval_seconds
                    ),
                    factor_cache_path=str(layout.local_paths.factor_cache_database),
                    socket_timeout_seconds=socket_timeout_seconds,
                )
                if provider.trading_dates(args.trade_date, args.trade_date) != [args.trade_date]:
                    raise MarketFailureError(
                        MarketFailure(
                            failure_stage="validate",
                            failure_class="calendar",
                            retryable=False,
                        )
                    )
                user_store = UserStore(layout.local_paths.user_database)
                result = run_publication_refresh(
                    store,
                    provider,
                    trade_date=args.trade_date,
                    required_symbols=collect_required_symbols(user_store),
                    lineage_input={"mode": "legacy"},
                )
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception as error:
            failure = market_failure_from_exception(error, stage="validate")
            print(
                json.dumps(
                    _market_failure_payload(failure),
                    ensure_ascii=False,
                )
            )
            return 1
    else:
        try:
            result = MarketRefreshService(
                store,
                BaoStockProvider(socket_timeout_seconds=socket_timeout_seconds),
                lineage_input={"mode": "legacy"} if isinstance(store, NasMarketStore) else None,
            ).refresh(args.trade_date, symbols=args.symbols)
        except Exception as error:
            failure = market_failure_from_exception(error, stage="fetch")
            print(
                json.dumps(
                    _market_failure_payload(failure),
                    ensure_ascii=False,
                )
            )
            return 1
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
    return 0 if result.status in {"ready", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
