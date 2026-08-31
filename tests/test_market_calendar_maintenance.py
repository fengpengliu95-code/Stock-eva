import gzip
from contextlib import contextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace

import httpx
import pytest
from pydantic import BaseModel

from backend.app.market.baostock import BaoStockError
from backend.app.market.calendar_generation import (
    CalendarGenerationStore,
    CalendarSourceBundleV1,
    body_sha256,
    build_schedule,
    build_source,
)
from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore
from backend.app.market.provider_health import SQLiteProviderHealthStore
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportOutcome,
)
from tests.test_market_calendar_generation import _valid_source

NOW = datetime(2026, 12, 22, 1, tzinfo=UTC)
BODIES = (b"sse body", b"szse body")


def fetch(source, factory):
    from backend.app.market.calendar_maintenance import fetch_official_calendars

    return fetch_official_calendars(source, now=NOW, client_factory=factory)


def factory_for(handler, configurations=None, clients=None):
    configurations = [] if configurations is None else configurations
    clients = [] if clients is None else clients

    def factory(**kwargs):
        configurations.append(kwargs)
        client = httpx.Client(transport=httpx.MockTransport(handler), **kwargs)
        clients.append(client)
        return client

    return factory


def test_exact_dual_requests_use_fresh_credentialless_clients_and_decoded_bytes():
    configurations = []
    requests = []

    def handler(request):
        requests.append(request)
        body = BODIES[len(requests) - 1]
        return httpx.Response(
            200,
            content=gzip.compress(body),
            headers={"Content-Encoding": "gzip", "Set-Cookie": "secret=private; Domain=.com.cn"},
        )

    result = fetch(_valid_source(), factory_for(handler, configurations))

    assert result.failure is None
    assert result.official_requests == 2
    assert result.bodies == BODIES
    assert [str(request.url) for request in requests] == [
        schedule.official_url for schedule in _valid_source().schedules
    ]
    assert len(configurations) == 2
    for configuration in configurations:
        assert configuration["trust_env"] is False
        assert configuration["verify"] is True
        assert configuration["follow_redirects"] is False
        assert configuration["auth"] is None
        timeout = configuration["timeout"]
        assert (timeout.connect, timeout.read, timeout.write, timeout.pool) == (5, 30, 5, 5)
    for request in requests:
        assert request.method == "GET"
        assert request.headers["accept-encoding"] == "gzip, deflate"
        assert not {"authorization", "proxy-authorization", "cookie"}.intersection(request.headers)
    assert "sse body" not in repr(result)


@pytest.mark.parametrize("status", [301, 302, 401, 403, 429, 500])
def test_http_failures_are_one_actual_request_and_drop_partial_result(status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            status,
            content=b"private upstream response",
            headers={"Location": "https://www.sse.com.cn/another"},
        )

    result = fetch(_valid_source(), factory_for(handler))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == len(requests) == 1
    assert result.bodies is None
    assert "private upstream" not in repr(result)


def test_second_hash_failure_drops_first_verified_body_and_counts_two():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=BODIES[0] if len(requests) == 1 else b"changed")

    result = fetch(_valid_source(), factory_for(handler))

    assert result.failure == "OFFICIAL_HASH_MISMATCH"
    assert result.official_requests == 2
    assert result.bodies is None


def test_first_hash_failure_stops_before_second_request():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=b"wrong")

    result = fetch(_valid_source(), factory_for(handler))

    assert result.failure == "OFFICIAL_HASH_MISMATCH"
    assert result.official_requests == len(requests) == 1


def test_oversized_stream_stops_reading_and_closes_response():
    visited = []
    closed = []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            for chunk in (b"x" * 1_048_576, b"x", b"must not read this"):
                visited.append(len(chunk))
                yield chunk

        def close(self):
            closed.append(True)

    result = fetch(_valid_source(), factory_for(lambda _: httpx.Response(200, stream=Stream())))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None
    assert visited == [1_048_576, 1]
    assert closed == [True]


def test_exact_one_mib_body_is_accepted():
    body = b"x" * 1_048_576
    source = _source_with_bodies(body, BODIES[1])

    def handler(request):
        content = body if request.url.host == "www.sse.com.cn" else BODIES[1]
        return httpx.Response(200, content=content)

    result = fetch(source, factory_for(handler))

    assert result.failure is None
    assert result.bodies == (body, BODIES[1])


def test_one_byte_over_one_mib_body_is_unavailable():
    body = b"x" * (1_048_576 + 1)
    source = _source_with_bodies(body, BODIES[1])

    result = fetch(source, factory_for(lambda _: httpx.Response(200, content=body)))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None


def test_midstream_http_error_is_sanitized_and_closes_response():
    closed = []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial"
            raise httpx.ReadError("private body and URL")

        def close(self):
            closed.append(True)

    result = fetch(_valid_source(), factory_for(lambda _: httpx.Response(200, stream=Stream())))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None
    assert closed == [True]
    assert "private body" not in repr(result)


def test_network_timeout_is_sanitized_and_not_retried():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("private raw URL and payload", request=request)

    result = fetch(_valid_source(), factory_for(handler))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == len(calls) == 1
    assert "private raw" not in repr(result)


def test_client_construction_failure_does_not_count_http_request():
    result = fetch(_valid_source(), lambda **_: _raise_os_error())

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 0
    assert result.bodies is None


def test_second_client_construction_failure_counts_first_request_only():
    calls = []

    def factory(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise OSError("private local setup detail")
        transport = httpx.MockTransport(lambda _: httpx.Response(200, content=BODIES[0]))
        return httpx.Client(transport=transport)

    result = fetch(_valid_source(), factory)

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert len(calls) == 2


@pytest.mark.parametrize("error_type", [TypeError, ValueError, RuntimeError])
def test_client_programming_error_is_not_classified_as_upstream_failure(error_type):
    failure = error_type("programming defect")

    def factory(**kwargs):
        raise failure

    with pytest.raises(error_type) as captured:
        fetch(_valid_source(), factory)
    assert captured.value is failure


def test_cleanup_error_does_not_mask_primary_programmer_failure():
    failure = TypeError("primary programmer failure")

    class FailedCloseClient(httpx.Client):
        def close(self):
            super().close()
            raise OSError("private close failure")

    def handler(request):
        raise failure

    def factory(**kwargs):
        return FailedCloseClient(transport=httpx.MockTransport(handler), **kwargs)

    with pytest.raises(TypeError) as captured:
        fetch(_valid_source(), factory)
    assert captured.value is failure


def test_invalid_source_hash_is_rejected_before_client_construction():
    source = _valid_source()
    object.__setattr__(source, "source_sha256", "0" * 64)
    called = []

    result = fetch(source, lambda **_: called.append(True))

    assert result.failure == "SOURCE_INVALID"
    assert result.official_requests == 0
    assert called == []


def test_model_construct_source_hash_is_rejected_before_client_construction():
    values = _valid_source().model_dump(mode="python")
    values["source_sha256"] = "0" * 64
    source = BaseModel.model_construct.__func__(CalendarSourceBundleV1, **values)
    called = []

    result = fetch(source, lambda **_: called.append(True))

    assert result.failure == "SOURCE_INVALID"
    assert result.official_requests == 0
    assert called == []


def test_invalid_source_url_is_rejected_before_client_construction():
    source = _valid_source()
    schedule = source.schedules[0]
    tampered = schedule.model_copy()
    object.__setattr__(tampered, "official_url", "http://example.test/calendar")
    object.__setattr__(source, "schedules", (tampered, source.schedules[1]))
    called = []

    result = fetch(source, lambda **_: called.append(True))

    assert result.failure == "SOURCE_INVALID"
    assert result.official_requests == 0
    assert called == []


def test_conflicting_valid_schedules_are_rejected_before_client_construction():
    source = _valid_source()
    first_values = source.schedules[0].model_dump(mode="python")
    first_values["closed_dates"] = (date(2027, 1, 1), date(2027, 2, 1))
    first = build_schedule(**first_values)
    source_values = source.model_dump(mode="python")
    source_values["schedules"] = (first, source.schedules[1])
    source = build_source(**source_values)
    called = []

    result = fetch(source, lambda **_: called.append(True))

    assert result.failure == "SOURCE_CONFLICT"
    assert result.official_requests == 0
    assert called == []


def test_default_factory_is_lazy_and_invalid_source_never_constructs_client(monkeypatch):
    import backend.app.market.calendar_maintenance as maintenance

    source = _valid_source()
    object.__setattr__(source, "source_sha256", "0" * 64)
    called = []
    monkeypatch.setattr(maintenance.httpx, "Client", lambda **_: called.append(True))

    result = maintenance.fetch_official_calendars(source, now=NOW)

    assert result.failure == "SOURCE_INVALID"
    assert called == []


def test_client_callback_cannot_change_pinned_source_url():
    source = _valid_source()
    original_urls = tuple(schedule.official_url for schedule in source.schedules)
    requests = []

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=BODIES[0] if len(requests) == 1 else BODIES[1])

    def factory(**kwargs):
        object.__setattr__(source.schedules[0], "official_url", "https://foreign.example/secret")
        return httpx.Client(transport=httpx.MockTransport(handler), **kwargs)

    result = fetch(source, factory)

    assert result.failure is None
    assert requests == list(original_urls)


def test_client_callback_cannot_change_pinned_body_hash_contract():
    source = _valid_source()

    def factory(**kwargs):
        object.__setattr__(source.schedules[0], "body_sha256", "0" * 64)

        def handler(request):
            content = BODIES[0] if request.url.host == "www.sse.com.cn" else BODIES[1]
            return httpx.Response(200, content=content)

        return httpx.Client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    result = fetch(source, factory)

    assert result.failure is None
    assert result.bodies == BODIES


def test_empty_body_is_unavailable():
    result = fetch(_valid_source(), factory_for(lambda _: httpx.Response(200, content=b"")))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1


def test_malformed_gzip_body_is_unavailable_and_closed():
    consumed = []
    closed = []
    clients = []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            consumed.append(True)
            yield b"not-gzip"

        def close(self):
            closed.append(True)

    def handler(_):
        return httpx.Response(200, stream=Stream(), headers={"Content-Encoding": "gzip"})

    result = fetch(_valid_source(), factory_for(handler, clients=clients))

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None
    assert consumed == [True]
    assert closed == [True]
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize("content_encoding", ["unverified-encoding", "gzip, br", "gzip,,deflate"])
def test_unsupported_or_malformed_content_encoding_is_unavailable(content_encoding):
    visited = []
    closed = []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            visited.append(True)
            yield BODIES[0]

        def close(self):
            closed.append(True)

    result = fetch(
        _valid_source(),
        factory_for(
            lambda _: httpx.Response(
                200, stream=Stream(), headers={"Content-Encoding": content_encoding}
            )
        ),
    )

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None
    assert visited == []
    assert closed == [True]


def test_client_close_transport_error_does_not_publish_success():
    class Client(httpx.Client):
        def close(self):
            super().close()
            raise OSError("private close detail")

    result = fetch(
        _valid_source(),
        lambda **kwargs: Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=BODIES[0])),
            **kwargs,
        ),
    )

    assert result.failure == "OFFICIAL_UNAVAILABLE"
    assert result.official_requests == 1
    assert result.bodies is None


def test_response_and_clients_are_closed_after_success():
    clients = []
    closed = []
    calls = []

    class Stream(httpx.SyncByteStream):
        def __init__(self, body):
            self.body = body

        def __iter__(self):
            yield self.body

        def close(self):
            closed.append(self.body)

    def handler(request):
        body = BODIES[len(calls)]
        calls.append(request)
        return httpx.Response(200, stream=Stream(body))

    result = fetch(_valid_source(), factory_for(handler, clients=clients))

    assert result.failure is None
    assert closed == list(BODIES)
    assert all(client.is_closed for client in clients)


def _raise_os_error():
    raise OSError("private local setup detail")


def _source_with_bodies(first: bytes, second: bytes) -> CalendarSourceBundleV1:
    source = _valid_source()
    schedules = tuple(
        build_schedule(
            **{
                **schedule.model_dump(mode="python"),
                "body_sha256": body_sha256(body),
            }
        )
        for schedule, body in zip(source.schedules, (first, second), strict=True)
    )
    values = source.model_dump(mode="python")
    values["schedules"] = schedules
    return build_source(**values)


def _worker_fixture(tmp_path):
    source = _valid_source()
    store = CalendarGenerationStore(tmp_path / "calendar.sqlite3")
    assert store.stage_execute(source, NOW).outcome == "STAGED"
    health_path = tmp_path / "health.sqlite3"
    SQLiteProviderHealthStore(health_path).initialize()
    return store, CalendarMaintenanceHealthStore(health_path), source


def _official_factory(status=200):
    calls = []

    def factory(**kwargs):
        calls.append(kwargs)
        index = len(calls) - 1
        body = BODIES[index]
        return httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, content=body if status == 200 else b"error")
            ),
            **kwargs,
        )

    factory.calls = calls
    return factory


def test_worker_early_official_failure_spends_slot_without_provider(tmp_path):
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    store, health, _ = _worker_fixture(tmp_path)
    providers = []
    result = CalendarMaintenanceService(
        store,
        health,
        http_client_factory=_official_factory(status=503),
        provider_factory=lambda **kwargs: providers.append(kwargs),
        clock=lambda: NOW,
    ).execute()

    assert result.outcome == "OFFICIAL_UNAVAILABLE"
    assert (result.official_requests, result.machine_requests) == (1, 0)
    assert providers == []
    assert store.read_control().attempts[-1].outcome == "OFFICIAL_UNAVAILABLE"


def test_worker_provider_construction_failure_counts_zero_machine_requests(tmp_path):
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    store, health, _ = _worker_fixture(tmp_path)

    def provider_factory(**kwargs):
        raise OSError("synthetic provider setup failure")

    result = CalendarMaintenanceService(
        store,
        health,
        http_client_factory=_official_factory(),
        provider_factory=provider_factory,
        clock=lambda: NOW,
    ).execute()

    assert result.outcome == "MACHINE_UNAVAILABLE"
    assert (result.official_requests, result.machine_requests) == (2, 0)


def test_worker_machine_failure_without_audit_is_control_unavailable_and_counts_request(tmp_path):
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    store, health, _ = _worker_fixture(tmp_path)

    class FailingProvider:
        @contextmanager
        def refresh_operation(self, run_id):
            yield

        def calendar_days(self, start, end):
            raise BaoStockError("synthetic machine failure")

    result = CalendarMaintenanceService(
        store,
        health,
        http_client_factory=_official_factory(),
        provider_factory=lambda **kwargs: FailingProvider(),
        clock=lambda: NOW,
    ).execute()

    assert result.outcome == "CONTROL_STATE_UNAVAILABLE"
    assert (result.official_requests, result.machine_requests) == (2, 1)


def test_worker_finish_failure_is_control_unavailable_after_reserved_slot(tmp_path):
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    class FinishFails(CalendarGenerationStore):
        def finish_attempt(self, *args, **kwargs):
            return False

    source = _valid_source()
    store = FinishFails(tmp_path / "calendar.sqlite3")
    assert store.stage_execute(source, NOW).outcome == "STAGED"
    health_path = tmp_path / "health.sqlite3"
    SQLiteProviderHealthStore(health_path).initialize()
    health = CalendarMaintenanceHealthStore(health_path)
    result = CalendarMaintenanceService(
        store,
        health,
        http_client_factory=_official_factory(status=503),
        clock=lambda: NOW,
    ).execute()

    assert result.outcome == "CONTROL_STATE_UNAVAILABLE"
    assert result.writes_calendar_state is True


def test_worker_transport_failure_preserves_normalized_error():
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    recorded = []

    class Health:
        def list_observations(self):
            return [
                SimpleNamespace(
                    refresh_id="refresh-1",
                    provider_session_id="session-1",
                    protocol_stage=ProtocolStage.OPERATION,
                    endpoint=ProviderEndpoint.TRADE_DATES,
                    attempt=1,
                    outcome=TransportOutcome.ERROR,
                    normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                )
            ]

        def record_terminal_failure(self, refresh_id, endpoint, error, *, observed_at):
            recorded.append((refresh_id, endpoint, error))

    service = CalendarMaintenanceService(object(), Health())
    assert service._record_machine_failure("refresh-1", NOW) is True
    assert recorded == [
        ("refresh-1", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.RECV_TIMEOUT)
    ]


def test_worker_semantic_failure_after_success_uses_protocol_error():
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    recorded = []

    class Health:
        def list_observations(self):
            return [
                SimpleNamespace(
                    refresh_id="refresh-2",
                    provider_session_id="session-2",
                    protocol_stage=ProtocolStage.OPERATION,
                    endpoint=ProviderEndpoint.TRADE_DATES,
                    attempt=1,
                    outcome=TransportOutcome.SUCCESS,
                    normalized_error=None,
                )
            ]

        def record_terminal_failure(self, refresh_id, endpoint, error, *, observed_at):
            recorded.append(error)

    service = CalendarMaintenanceService(object(), Health())
    assert service._record_machine_failure("refresh-2", NOW) is True
    assert recorded == [NormalizedTransportError.PROTOCOL_ERROR]


@pytest.mark.parametrize(
    "observations",
    [
        [],
        [
            SimpleNamespace(
                refresh_id="refresh-3",
                provider_session_id="session-3",
                protocol_stage=ProtocolStage.RECEIVE,
                endpoint=ProviderEndpoint.TRADE_DATES,
                attempt=1,
                outcome=TransportOutcome.ERROR,
                normalized_error=NormalizedTransportError.EOF,
            )
        ],
    ],
)
def test_worker_missing_operation_audit_never_guesses_failure(observations):
    from backend.app.market.calendar_maintenance import CalendarMaintenanceService

    class Health:
        def list_observations(self):
            return observations

        def record_terminal_failure(self, *args, **kwargs):
            raise AssertionError("uncertain audit must not mutate circuit state")

    service = CalendarMaintenanceService(object(), Health())
    assert service._record_machine_failure("refresh-3", NOW) is False
