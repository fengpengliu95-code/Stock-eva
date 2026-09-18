from datetime import date, timedelta

import pytest

from backend.app.market.secondary_qualification import (
    CanonicalQualificationVersionVectorV1,
    OfflineQualificationHarness,
    QualificationFetchResultV1,
    QualificationSessionRequestV1,
    RawDailyBarV1,
    SessionQualificationObservationV1,
    evaluate_qualification_window,
)


def _vector(suffix: str = "1") -> CanonicalQualificationVersionVectorV1:
    return CanonicalQualificationVersionVectorV1(
        adapter_sha256=suffix * 64,
        endpoint_sha256="2" * 64,
        schema_sha256="3" * 64,
        normalizer_sha256="4" * 64,
        reconciliation_sha256="5" * 64,
        calendar_sha256="6" * 64,
        universe_sha256="7" * 64,
        terms_sha256="8" * 64,
        quota_sha256="9" * 64,
    )


def _request() -> QualificationSessionRequestV1:
    return QualificationSessionRequestV1(
        provider_id="tushare",
        session_date=date(2026, 9, 17),
        expected_symbols=("sh.600000", "sz.000001"),
        required_indexes=("sh.000001",),
        previous_adjust_factors={"sh.600000": 1.0, "sz.000001": 1.1},
        version_vector=_vector(),
        max_attempts=1,
    )


def _result(**changes: object) -> QualificationFetchResultV1:
    values: dict[str, object] = {
        "provider_id": "tushare",
        "session_date": date(2026, 9, 17),
        "bars": (
            RawDailyBarV1(
                symbol="sh.600000", source_provider="tushare", volume_lots=10, amount_thousand_cny=2
            ),
        ),
        "suspended_symbols": ("sz.000001",),
        "session_universe": ("sh.600000", "sz.000001"),
        "index_symbols": ("sh.000001",),
        "adjust_factors": {"sh.600000": 1.02, "sz.000001": 1.1},
        "end_marker_seen": True,
        "pagination_complete": True,
    }
    values.update(changes)
    return QualificationFetchResultV1(**values)


class _FakeProvider:
    def __init__(self, result: QualificationFetchResultV1 | BaseException) -> None:
        self.result = result
        self.calls = 0

    def fetch_session(self, request: QualificationSessionRequestV1) -> QualificationFetchResultV1:
        self.calls += 1
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def test_complete_session_qualifies_once_and_converts_units_exactly_once() -> None:
    provider = _FakeProvider(_result())

    outcome = OfflineQualificationHarness(provider).run(_request())

    assert outcome.status == "qualified"
    assert outcome.reason_code is None
    assert outcome.provider_requests == 1
    assert outcome.attempts == 1
    assert outcome.normalized_volume_shares == 1000
    assert outcome.normalized_amount_cny == 2000
    assert outcome.writes is False
    assert provider.calls == 1


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        (TimeoutError(), "PROVIDER_TIMEOUT"),
        (_result(end_marker_seen=False), "INCOMPLETE_RESPONSE"),
        (_result(pagination_complete=False), "INCOMPLETE_RESPONSE"),
        (_result(bars=()), "MISSING_SUSPENSION_EVIDENCE"),
        (_result(bars=(_result().bars[0], _result().bars[0])), "DUPLICATE_SYMBOL"),
        (_result(adjust_factors={"sh.600000": 1.02}), "MISSING_ADJUSTMENT_FACTOR"),
        (_result(session_universe=("sh.600000",)), "UNIVERSE_MISMATCH"),
        (_result(index_symbols=()), "REQUIRED_INDEX_MISSING"),
        (
            _result(
                bars=(
                    RawDailyBarV1(
                        symbol="sh.600000",
                        source_provider="tickflow",
                        volume_lots=10,
                        amount_thousand_cny=2,
                    ),
                )
            ),
            "MIXED_PROVIDER_SOURCE",
        ),
    ],
)
def test_session_failures_are_closed_and_never_retried(
    result: QualificationFetchResultV1 | BaseException, reason: str
) -> None:
    provider = _FakeProvider(result)

    outcome = OfflineQualificationHarness(provider).run(_request())

    assert outcome.status == "rejected"
    assert outcome.reason_code == reason
    assert outcome.provider_requests == 1
    assert outcome.attempts == 1
    assert outcome.writes is False
    assert provider.calls == 1


def test_window_requires_twenty_sessions_and_one_frozen_vector() -> None:
    vector = _vector()
    sessions = tuple(date(2026, 8, 21) + timedelta(days=offset) for offset in range(20))
    observations = tuple(
        SessionQualificationObservationV1(
            session_date=session,
            status="qualified",
            candidate_sha256=f"{offset:064x}",
            evidence_sha256="a" * 64,
            gate_sha256="b" * 64,
            selection_sha256="c" * 64,
            version_vector_sha256=vector.version_vector_sha256,
        )
        for offset, session in enumerate(sessions)
    )

    result = evaluate_qualification_window(observations, expected_sessions=sessions)

    assert result.status == "qualified"
    assert result.consecutive_sessions == 20
    assert result.canonical_failover_state == "UNQUALIFIED"
    assert result.writes is False

    drifted = observations[:-1] + (
        observations[-1].model_copy(update={"version_vector_sha256": "f" * 64}),
    )
    assert (
        evaluate_qualification_window(drifted, expected_sessions=sessions).reason_code
        == "VERSION_VECTOR_DRIFT"
    )
    assert (
        evaluate_qualification_window(observations[:-1], expected_sessions=sessions).reason_code
        == "WINDOW_INCOMPLETE"
    )
    assert (
        evaluate_qualification_window(observations, expected_sessions=sessions[::-1]).reason_code
        == "SESSION_ORDER_INVALID"
    )
