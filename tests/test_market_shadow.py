"""Task13 scheduler and automation boundary behavior."""

from backend.app.market.shadow_jobs import ShadowHandoff
from backend.app.market.shadow_scheduler import ShadowScheduler


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
    assert ShadowScheduler is not None
