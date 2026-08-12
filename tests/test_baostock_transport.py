import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
    TransportScopeError,
    current_request_context,
    normalize_provider_code,
    provider_session_scope,
    refresh_scope,
    request_scope,
)


def test_provider_endpoints_are_an_exact_allowlist() -> None:
    assert {endpoint.value for endpoint in ProviderEndpoint} == {
        "trade_dates",
        "all_stock",
        "daily_astock",
        "daily_factor",
        "adjust_factor",
        "index_history",
    }

    with pytest.raises(ValueError):
        ProviderEndpoint("raw_socket")


def test_nested_scopes_own_stable_refresh_session_and_request_ids() -> None:
    with refresh_scope("refresh-123") as refresh:
        assert refresh.refresh_id == "refresh-123"
        with provider_session_scope() as session:
            assert session.refresh_id == refresh.refresh_id
            with request_scope(ProviderEndpoint.ALL_STOCK, attempt=1, page=1) as request:
                assert request.refresh_id == refresh.refresh_id
                assert request.provider_session_id == session.provider_session_id
                assert request.request_id
                assert current_request_context() == request
            with request_scope(ProviderEndpoint.ALL_STOCK, attempt=2, page=1) as retry:
                assert retry.provider_session_id == session.provider_session_id
                assert retry.request_id != request.request_id
        with provider_session_scope() as next_session:
            assert next_session.provider_session_id != session.provider_session_id

    with pytest.raises(TransportScopeError, match="request scope"):
        current_request_context()


def test_session_and_request_scopes_require_their_parent_scope() -> None:
    with pytest.raises(TransportScopeError, match="refresh scope"):
        with provider_session_scope():
            pass

    with refresh_scope():
        with pytest.raises(TransportScopeError, match="provider session scope"):
            with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                pass


def test_transport_observation_is_frozen_typed_and_contains_only_safe_fields() -> None:
    observation = TransportObservation(
        refresh_id="refresh-123",
        provider_session_id="session-456",
        request_id="request-789",
        endpoint=ProviderEndpoint.DAILY_ASTOCK,
        attempt=2,
        page=1,
        protocol_stage=ProtocolStage.RECEIVE,
        elapsed_ms=30_001,
        recv_calls=4,
        response_bytes=8192,
        end_marker_seen=False,
        provider_code="10002008",
        normalized_error=NormalizedTransportError.RECV_TIMEOUT,
        outcome=TransportOutcome.ERROR,
        observed_at=datetime(2026, 8, 12, 11, 20, tzinfo=UTC),
    )

    payload = observation.model_dump(mode="json")
    assert set(payload) == {
        "refresh_id",
        "provider_session_id",
        "request_id",
        "provider_id",
        "endpoint",
        "attempt",
        "page",
        "protocol_stage",
        "elapsed_ms",
        "recv_calls",
        "response_bytes",
        "end_marker_seen",
        "provider_code",
        "normalized_error",
        "outcome",
        "observed_at",
    }
    assert payload["provider_id"] == "baostock"
    assert "message" not in json.dumps(payload)
    with pytest.raises(ValidationError):
        observation.recv_calls = 5


@pytest.mark.parametrize(
    "forbidden",
    [
        {"provider_message": "token=secret https://provider.invalid/private"},
        {"exception_text": "SELECT * FROM private /Users/secret"},
        {"payload": "raw-provider-response"},
    ],
)
def test_transport_observation_rejects_forbidden_raw_fields(forbidden: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        TransportObservation(
            refresh_id="refresh-123",
            provider_session_id="session-456",
            request_id="request-789",
            endpoint=ProviderEndpoint.TRADE_DATES,
            attempt=1,
            page=1,
            protocol_stage=ProtocolStage.PROVIDER_STATUS,
            elapsed_ms=1,
            recv_calls=1,
            response_bytes=21,
            end_marker_seen=True,
            provider_code="vendor-new-code",
            normalized_error=NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR,
            outcome=TransportOutcome.ERROR,
            observed_at=datetime.now(UTC),
            **forbidden,
        )


@pytest.mark.parametrize(
    ("provider_code", "expected"),
    [
        ("0", None),
        ("429", NormalizedTransportError.RATE_LIMIT),
        ("10001005", NormalizedTransportError.RATE_LIMIT),
        ("10002001", NormalizedTransportError.CONNECT_ERROR),
        ("10002002", NormalizedTransportError.CONNECT_ERROR),
        ("10002003", NormalizedTransportError.CONNECT_ERROR),
        ("10002004", NormalizedTransportError.EOF),
        ("10002005", NormalizedTransportError.SEND_ERROR),
        ("10002006", NormalizedTransportError.SEND_ERROR),
        ("10002007", NormalizedTransportError.PROTOCOL_ERROR),
        ("10002008", NormalizedTransportError.RECV_TIMEOUT),
        ("10004001", NormalizedTransportError.PROTOCOL_ERROR),
        ("10004002", NormalizedTransportError.BAD_COMPRESSION),
        ("10004004", NormalizedTransportError.PROTOCOL_ERROR),
        ("10004019", NormalizedTransportError.PROTOCOL_ERROR),
        ("10004020", NormalizedTransportError.PROTOCOL_ERROR),
    ],
)
def test_provider_codes_map_without_provider_message_text(
    provider_code: str,
    expected: NormalizedTransportError | None,
) -> None:
    assert normalize_provider_code(provider_code, provider_message="ignored") == expected


def test_unknown_provider_code_never_guesses_from_message_text() -> None:
    code = "vendor-new-code"

    assert normalize_provider_code(code, provider_message="too many requests") == (
        NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR
    )
    assert normalize_provider_code(code, provider_message="authentication failed") == (
        NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR
    )


class PartialSendSocket:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.sent = b""
        self.send_calls = 0
        self.sendall_calls = 0

    def send(self, payload: bytes) -> int:
        self.send_calls += 1
        accepted = max(1, len(payload) // 2)
        self.sent += payload[:accepted]
        return accepted

    def sendall(self, payload: bytes) -> None:
        self.sendall_calls += 1
        self.sent += payload

    def recv(self, _size: int) -> bytes:
        response, self.response = self.response, b""
        return response


def test_upstream_send_msg_accepts_partial_send_while_patch_must_use_sendall() -> None:
    import baostock.common.contants as constants
    import baostock.common.context as context
    import baostock.data.messageheader as messageheader
    import baostock.util.socketutil as socketutil

    response = messageheader.to_message_header("01", 2).encode() + b"ok" + b"<![CDATA[]]>\n"
    connection = PartialSendSocket(response)
    previous = getattr(context, "default_socket", None)
    context.default_socket = connection
    try:
        assert socketutil.send_msg("request") == response.decode()
    finally:
        context.default_socket = previous

    outbound = b"request\n"
    assert connection.send_calls == 1
    assert connection.sendall_calls == 0
    assert connection.sent != outbound
    assert connection.sent == outbound[: len(outbound) // 2]
    assert constants.BAOSTOCK_CLIENT_VERSION == "00.9.30"

    patched_connection = PartialSendSocket(b"")
    patched_connection.sendall(outbound)
    assert patched_connection.sent == outbound
    assert patched_connection.sendall_calls == 1
