"""Task13 scheduler and automation boundary behavior."""

import json
from datetime import UTC, datetime

import pytest

from backend.app.market.providers.registry import ShadowRegistry
from backend.app.market.shadow_jobs import (
    CanonicalOutcomeScanner,
    ShadowBundlePublisher,
    ShadowHandoff,
    ShadowJobStore,
    ShadowOutcomeReporter,
)
from backend.app.market.shadow_scheduler import ShadowScheduler, build_shadow_scheduler
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
    with pytest.raises(TypeError):
        ShadowScheduler(None)


def test_scheduler_idle_publishes_run_level_outcome(tmp_path):
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    root = tmp_path / "shadow"
    scheduler = ShadowScheduler(
        ShadowJobStore(registry, root),
        canonical_scanner=CanonicalOutcomeScanner(
            tmp_path / "canonical",
            shadow_start_date=datetime(2026, 1, 1).date(),
            provider_id="tickflow",
            window_id="tickflow-window-1",
        ),
        outcome_reporter=ShadowOutcomeReporter(ShadowBundlePublisher(root)),
    )
    assert scheduler.run_once().status == "idle"
    report = root / "bundles" / "outcome-run-none-idle" / "report.json"
    assert json.loads(report.read_text()) == {
        "job_id": None,
        "reason_code": None,
        "report_version": 1,
        "state_version": None,
        "status": "idle",
    }


def test_production_shadow_factory_wires_scanner_publisher_writer_and_scheduler(tmp_path):
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    scheduler = build_shadow_scheduler(
        registry=registry,
        canonical_root=tmp_path / "canonical",
        shadow_root=tmp_path / "shadow",
        shadow_start_date=datetime(2026, 1, 1).date(),
        provider_id="tickflow",
        window_id="tickflow-window-1",
    )
    assert type(scheduler).__name__ == "ShadowScheduler"
    assert type(scheduler.canonical_scanner).__name__ == "CanonicalOutcomeScanner"
    assert type(scheduler.outcome_reporter).__name__ == "ShadowOutcomeReporter"
    assert type(scheduler.terminal_writer).__name__ == "ShadowTerminalWriter"
    assert not (tmp_path / "shadow").exists()
    assert scheduler.run_once().status == "idle"
    assert (tmp_path / "shadow" / "bundles" / "outcome-run-none-idle").is_dir()


@pytest.mark.parametrize(
    ("worker_result", "expected_status"),
    [
        ("completed", "failed"),
        ("success", "failed"),
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
        canonical_scanner=CanonicalOutcomeScanner(
            tmp_path / "canonical",
            shadow_start_date=datetime(2026, 1, 1).date(),
            provider_id="tickflow",
            window_id=window.window_id,
        ),
        outcome_reporter=ShadowOutcomeReporter(ShadowBundlePublisher(tmp_path / "shadow")),
    )
    result = scheduler.run_once(now=datetime(2026, 1, 2, tzinfo=UTC))
    assert result.status == (
        worker_result if worker_result in {"failure", "unavailable"} else expected_status
    )
    assert store.get("job-1").run_status == expected_status
    assert store.get("job-1").lease_owner is None
    outcome_reports = list((tmp_path / "shadow" / "bundles").glob("*/report.json"))
    assert outcome_reports
    assert any(json.loads(path.read_text())["job_id"] == "job-1" for path in outcome_reports)


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

    result = ShadowScheduler(
        store,
        worker=crash,
        owner="worker-error",
        canonical_scanner=CanonicalOutcomeScanner(
            tmp_path / "canonical",
            shadow_start_date=datetime(2026, 1, 1).date(),
            provider_id="tickflow",
            window_id=window.window_id,
        ),
        outcome_reporter=ShadowOutcomeReporter(ShadowBundlePublisher(tmp_path / "shadow")),
    ).run_once()
    assert result.status == "failed"
    assert store.get("job-error").run_status == "failed"
    assert store.get("job-error").lease_owner is None
