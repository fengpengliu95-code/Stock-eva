from datetime import date
from typing import Protocol

from backend.app.classification.models import (
    ClassificationSnapshot,
    ClassificationSyncResult,
)
from backend.app.classification.store import ClassificationStore


class ClassificationProvider(Protocol):
    def fetch(self, as_of: date) -> ClassificationSnapshot: ...


def run_classification_sync(
    *,
    as_of: date,
    execute: bool,
    store: ClassificationStore,
    provider: ClassificationProvider | None,
) -> ClassificationSyncResult:
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
        raise ValueError("classification execute rejects future as_of")
    if provider is None:
        raise ValueError("classification execute requires a provider")
    snapshot = provider.fetch(as_of)
    outcome = store.publish(snapshot)
    generation = outcome.generation
    issues = sorted(
        {
            issue
            for audit in generation.coverage_audits
            for issue in audit.quality_issues
        }
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
