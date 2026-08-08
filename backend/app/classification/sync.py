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


def _sync_error(
    *,
    stage: str,
    failure_class: str,
    started_at: float,
    provider: ClassificationProvider | None,
) -> ClassificationSyncError:
    session = getattr(provider, "session", None)
    return ClassificationSyncError(
        ClassificationFailure(
            failure_stage=stage,
            failure_class=failure_class,
            elapsed_seconds=round(max(0.0, monotonic() - started_at), 3),
            provider_request_count=max(0, getattr(provider, "last_request_count", 0)),
            configured_timeout_seconds=getattr(session, "socket_timeout_seconds", None),
            configured_max_attempts=getattr(session, "max_attempts", None),
        )
    )


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
    try:
        snapshot = provider.fetch(as_of)
    except ClassificationFailureError:
        raise
    except Exception as exc:
        raise _sync_error(
            stage="validation",
            failure_class="internal",
            started_at=started_at,
            provider=provider,
        ) from exc
    try:
        outcome = store.publish(snapshot)
    except ClassificationConflictError as exc:
        raise _sync_error(
            stage="publication",
            failure_class="conflict",
            started_at=started_at,
            provider=provider,
        ) from exc
    except (duckdb.Error, OSError) as exc:
        raise _sync_error(
            stage="publication",
            failure_class="storage",
            started_at=started_at,
            provider=provider,
        ) from exc
    except Exception as exc:
        raise _sync_error(
            stage="publication",
            failure_class="internal",
            started_at=started_at,
            provider=provider,
        ) from exc
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
