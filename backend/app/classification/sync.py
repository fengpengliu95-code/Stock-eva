from datetime import date
from time import monotonic
from typing import Protocol

import duckdb

from backend.app.classification.failures import (
    ClassificationFailure,
    ClassificationFailureError,
    ClassificationSyncError,
)
from backend.app.classification.models import (
    ClassificationSnapshot,
    ClassificationSyncResult,
)
from backend.app.classification.store import (
    ClassificationConflictError,
    ClassificationStore,
)


class ClassificationProvider(Protocol):
    def fetch(self, as_of: date) -> ClassificationSnapshot: ...


def _provider_diagnostics(
    provider: ClassificationProvider | None,
) -> tuple[int, float | None, int | None]:
    try:
        session = getattr(provider, "session", None)
        request_count = max(0, getattr(provider, "last_request_count", 0))
        timeout = getattr(session, "socket_timeout_seconds", None)
        max_attempts = getattr(session, "max_attempts", None)
    except Exception:
        return 0, None, None
    return request_count, timeout, max_attempts


def _sync_error(
    *,
    stage: str,
    failure_class: str,
    started_at: float,
    provider: ClassificationProvider | None,
) -> ClassificationSyncError:
    request_count, timeout, max_attempts = _provider_diagnostics(provider)
    return ClassificationSyncError(
        ClassificationFailure(
            failure_stage=stage,
            failure_class=failure_class,
            elapsed_seconds=round(max(0.0, monotonic() - started_at), 3),
            provider_request_count=request_count,
            configured_timeout_seconds=timeout,
            configured_max_attempts=max_attempts,
        )
    )


def _fetch_outcome(
    provider: ClassificationProvider,
    as_of: date,
) -> tuple[ClassificationSnapshot | None, ClassificationFailure | None, bool]:
    try:
        return provider.fetch(as_of), None, False
    except ClassificationFailureError as error:
        failure = error.failure
        return None, failure, False
    except Exception:
        return None, None, True


def _publish_outcome(
    store: ClassificationStore,
    snapshot: ClassificationSnapshot,
):
    try:
        return store.publish(snapshot), None
    except ClassificationConflictError:
        return None, "conflict"
    except (duckdb.Error, OSError):
        return None, "storage"
    except Exception:
        return None, "internal"


def run_classification_sync(
    *,
    as_of: date,
    execute: bool,
    store: ClassificationStore,
    provider: ClassificationProvider | None,
) -> ClassificationSyncResult:
    started_at = monotonic()
    if not execute:
        return ClassificationSyncResult(
            status="dry-run",
            as_of=as_of,
            network_requests=0,
            writes_classification_data=False,
            new_generation=False,
            execute_requires="--execute",
            generation=None,
        )
    if as_of > date.today():
        raise _sync_error(
            stage="validation",
            failure_class="data_quality",
            started_at=started_at,
            provider=provider,
        )
    if provider is None:
        raise _sync_error(
            stage="validation",
            failure_class="internal",
            started_at=started_at,
            provider=provider,
        )
    snapshot, structured_failure, unexpected_failure = _fetch_outcome(provider, as_of)
    if structured_failure is not None:
        raise ClassificationSyncError(structured_failure)
    if unexpected_failure or snapshot is None:
        raise _sync_error(
            stage="validation",
            failure_class="internal",
            started_at=started_at,
            provider=provider,
        )
    outcome, publication_failure = _publish_outcome(store, snapshot)
    if publication_failure is not None or outcome is None:
        raise _sync_error(
            stage="publication",
            failure_class=publication_failure or "internal",
            started_at=started_at,
            provider=provider,
        )
    generation = outcome.generation
    issues = sorted(
        {issue for audit in generation.coverage_audits for issue in audit.quality_issues}
    )
    return ClassificationSyncResult(
        status="degraded" if issues else "ready",
        as_of=as_of,
        network_requests=getattr(provider, "last_request_count", 6),
        writes_classification_data=outcome.inserted,
        new_generation=outcome.inserted,
        execute_requires=None,
        generation=generation,
        quality_issues=issues,
    )
