"""Task13 scheduler and automation boundary behavior."""

from datetime import UTC, datetime

import pytest

from backend.app.market.providers.registry import ShadowRegistry
from backend.app.market.shadow_jobs import ShadowHandoff, ShadowJobStore
from backend.app.market.shadow_scheduler import ShadowScheduler
from tests.test_market_provider_registry import _record, _sha, _terms


def test_shadow_handoff_offer_is_zero_wait_and_callback_free():
    called = []
    handoff = ShadowHandoff(enqueue_outcome=called.append)
    assert handoff.offer("outcome", wait_budget=0, nonblocking=True)
    assert called == []


def test_shadow_handoff_rejects_blocking_contract():
    handoff = ShadowHandoff()
    try:
        handoff.offer("outcome", wait_budget=1, nonblocking=True)
    except ValueError:
        pass
    else:
        raise AssertionError("blocking handoff was accepted")


def test_shadow_handoff_drops_when_bounded_queue_is_full():
    handoff = ShadowHandoff(maxsize=1)
    assert handoff.offer("first", wait_budget=0, nonblocking=True)
    assert not handoff.offer("second", wait_budget=0, nonblocking=True)


def test_scheduler_is_independent_worker_surface():
    scheduler = ShadowScheduler.__new__(ShadowScheduler)
    scheduler.canonical_scanner = None
    scheduler.job_store = None
    assert scheduler.scan(limit=1) == 0


@pytest.mark.parametrize(
    ("worker_result", "expected_status"),
    [
        ("completed", "pending"),
        ("success", "pending"),
        ("failure", "failed"),
        ("unavailable", "unavailable"),
    ],
)
def test_scheduler_releases_sqlite_lease_on_every_worker_exit(
    tmp_path, worker_result, expected_status
):
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    state = registry.transition("tickflow", "canary", expected_state_version=1)
    registry.transition("tickflow", "shadow", expected_state_version=state.state_version)
    window = registry.ensure_window("tickflow", _sha("vector"), "calendar-v1", _sha("calendar"))
    store = ShadowJobStore(registry, tmp_path / "shadow")
    store.enqueue(
        job_id="job-1",
        provider_id="tickflow",
        window_id=window.window_id,
        trade_date="2026-01-02",
        universe_id="u1",
        canonical_manifest_generation="g1",
        canonical_manifest_sha256=_sha("manifest"),
        version_vector_sha256=_sha("vector"),
    )
    scheduler = ShadowScheduler(
        store,
        worker=lambda *_args, **_kwargs: {"status": worker_result},
        owner="worker-1",
        deadline_seconds=1,
    )
    result = scheduler.run_once(now=datetime(2026, 1, 2, tzinfo=UTC))
    assert result.status == worker_result
    assert store.get("job-1").run_status == expected_status
    assert store.get("job-1").lease_owner is None


def test_scheduler_worker_exception_releases_sqlite_lease(tmp_path):
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    state = registry.transition("tickflow", "canary", expected_state_version=1)
    registry.transition("tickflow", "shadow", expected_state_version=state.state_version)
    window = registry.ensure_window("tickflow", _sha("vector"), "calendar-v1", _sha("calendar"))
    store = ShadowJobStore(registry, tmp_path / "shadow")
    store.enqueue(
        job_id="job-error",
        provider_id="tickflow",
        window_id=window.window_id,
        trade_date="2026-01-02",
        universe_id="u1",
        canonical_manifest_generation="g1",
        canonical_manifest_sha256=_sha("manifest"),
        version_vector_sha256=_sha("vector"),
    )

    def crash(*_args, **_kwargs):
        raise RuntimeError("must be sanitized")

    result = ShadowScheduler(store, worker=crash, owner="worker-error").run_once()
    assert result.status == "failed"
    assert store.get("job-error").run_status == "failed"
    assert store.get("job-error").lease_owner is None
