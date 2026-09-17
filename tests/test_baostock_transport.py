import json
import threading
import zlib
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

import backend.app.market.provider_transport as provider_transport
from backend.app.market.baostock import (
    BaoStockProvider,
    _OperationDeadlineExceeded,
)
from backend.app.market.baostock_vendor import (
    EXPECTED_BAOSTOCK_VERSION,
    EXPECTED_SOCKETUTIL_SHA256,
    BaoStockTransportPatchError,
    checked_connect,
    checked_send_msg,
    install_baostock_transport_patch,
    socketutil_source_sha256,
    transport_observation_sink,
)
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
    import importlib.util

    import baostock.common.contants as constants
    import baostock.common.context as context
    import baostock.data.messageheader as messageheader
    import baostock.util.socketutil as socketutil

    # Earlier characterization tests intentionally install the production
    # transport patch globally. Load the pinned SDK source under an isolated
    # module name so this assertion always exercises pristine upstream
    # ``send()``, without restoring or uninstalling the checked patch.
    source_path = socketutil.__file__
    assert source_path is not None
    spec = importlib.util.spec_from_file_location(
        "_stock_eva_pristine_baostock_socketutil", source_path
    )
    assert spec is not None and spec.loader is not None
    pristine_socketutil = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pristine_socketutil)

    response = messageheader.to_message_header("01", 2).encode() + b"ok" + b"<![CDATA[]]>\n"
    connection = PartialSendSocket(response)
    previous = getattr(context, "default_socket", None)
    context.default_socket = connection
    try:
        assert pristine_socketutil.send_msg("request") == response.decode()
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


PROTOCOL_MARKER = b"<![CDATA[]]>\n"


def _frame(body: bytes, *, message_type: str = "34") -> bytes:
    import baostock.data.messageheader as messageheader

    header = messageheader.to_message_header(message_type, len(body)).encode()
    return header + body + PROTOCOL_MARKER


def _frame_with_marker_in_declared_length(body: bytes, *, message_type: str = "34") -> bytes:
    import baostock.data.messageheader as messageheader

    header = messageheader.to_message_header(
        message_type, len(body) + len(PROTOCOL_MARKER)
    ).encode()
    return header + body + PROTOCOL_MARKER


def _frame_with_crc_trailer(body: bytes, *, corrupt: bool = False) -> bytes:
    import baostock.data.messageheader as messageheader

    header = messageheader.to_message_header("34", len(body)).encode()
    crc = zlib.crc32(header + body) + (1 if corrupt else 0)
    return header + body + b"\1" + str(crc).encode() + PROTOCOL_MARKER


class FramedSocket:
    def __init__(
        self,
        chunks: list[bytes | BaseException],
        *,
        send_error: BaseException | None = None,
        connect_error: BaseException | None = None,
    ) -> None:
        self.chunks = iter(chunks)
        self.send_error = send_error
        self.connect_error = connect_error
        self.sent = b""
        self.sendall_calls = 0
        self.recv_calls = 0
        self.connect_calls = 0
        self.closed = False
        self.timeout = None

    def connect(self, _address: tuple[str, int]) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    def send(self, _payload: bytes) -> int:
        raise AssertionError("the pinned patch must not call send")

    def sendall(self, payload: bytes) -> None:
        self.sendall_calls += 1
        if self.send_error is not None:
            raise self.send_error
        self.sent += payload

    def recv(self, _size: int) -> bytes:
        self.recv_calls += 1
        chunk = next(self.chunks, b"")
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    def settimeout(self, seconds: float) -> None:
        self.timeout = seconds

    def close(self) -> None:
        self.closed = True


def _invoke_checked_send(
    connection: FramedSocket,
) -> tuple[str | None, BaoStockTransportPatchError | None, list[TransportObservation]]:
    import baostock.common.context as context

    observations: list[TransportObservation] = []
    result: str | None = None
    error: BaoStockTransportPatchError | None = None
    previous = getattr(context, "default_socket", None)
    context.default_socket = connection
    try:
        with (
            refresh_scope("refresh-safe"),
            provider_session_scope("session-safe"),
            request_scope(ProviderEndpoint.TRADE_DATES, attempt=1),
            transport_observation_sink(observations.append),
        ):
            try:
                result = checked_send_msg("request-token-secret")
            except BaoStockTransportPatchError as exc:
                error = exc
    finally:
        context.default_socket = previous
    return result, error, observations


def _assert_sanitized_terminal(
    error: BaoStockTransportPatchError,
    observations: list[TransportObservation],
    *,
    normalized_error: NormalizedTransportError,
    protocol_stage: ProtocolStage,
    recv_calls: int,
    response_bytes: int,
    end_marker_seen: bool,
) -> None:
    assert error.normalized_error == normalized_error
    assert error.protocol_stage == protocol_stage
    assert error.recv_calls == recv_calls
    assert error.response_bytes == response_bytes
    assert error.end_marker_seen is end_marker_seen
    assert len(observations) == 1
    observation = observations[0]
    assert observation.normalized_error == normalized_error
    assert observation.protocol_stage == protocol_stage
    assert observation.recv_calls == recv_calls
    assert observation.response_bytes == response_bytes
    assert observation.end_marker_seen is end_marker_seen
    serialized = observation.model_dump_json() + str(error)
    for forbidden in (
        "request-token-secret",
        "private-token",
        "https://provider.invalid/private",
        "/Users/private/provider.log",
        "synthetic raw exception",
    ):
        assert forbidden not in serialized


def test_checked_send_uses_sendall_and_emits_socket_counters() -> None:
    response = _frame(b"0\1ok")
    connection = FramedSocket([response[:7], response[7:]])

    result, error, observations = _invoke_checked_send(connection)

    assert error is None
    assert result == response.decode()
    assert connection.sendall_calls == 1
    assert connection.sent == b"request-token-secret\n"
    assert len(observations) == 1
    observation = observations[0]
    assert observation.protocol_stage == ProtocolStage.COMPLETE
    assert observation.outcome == TransportOutcome.SUCCESS
    assert observation.provider_code == "0"
    assert observation.recv_calls == 2
    assert observation.response_bytes == len(response)
    assert observation.end_marker_seen is True


@pytest.mark.parametrize("message_type", ["34", "96"])
def test_checked_send_accepts_exact_marker_inclusive_length_convention(
    message_type: str,
) -> None:
    body = b"0\1ok" if message_type == "34" else zlib.compress(b"0\1ok\n")
    response = _frame_with_marker_in_declared_length(body, message_type=message_type)

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result is not None
    assert observations[0].provider_code == "0"
    assert observations[0].response_bytes == len(response)


def test_checked_send_rejects_unrecognized_declared_length_difference() -> None:
    body = b"0\1ok"
    import baostock.data.messageheader as messageheader

    header = messageheader.to_message_header("34", len(body) + 1).encode()
    response = header + body + PROTOCOL_MARKER

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
        protocol_stage=ProtocolStage.FRAME,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )


def test_checked_send_accepts_exact_valid_crc_trailer() -> None:
    response = _frame_with_crc_trailer(b"0\1ok")

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result is not None
    assert result[21:] == "0\1ok" + PROTOCOL_MARKER.decode()
    assert observations[0].provider_code == "0"
    assert observations[0].response_bytes == len(response)


def test_checked_send_rejects_corrupt_crc_trailer() -> None:
    response = _frame_with_crc_trailer(b"0\1ok", corrupt=True)

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
        protocol_stage=ProtocolStage.FRAME,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )


def test_checked_send_accepts_compatible_server_patch_version() -> None:
    response = _frame(b"0\1ok")
    response = b"00.9.31" + response[7:]

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result == response.decode()
    assert observations[0].provider_code == "0"
    assert observations[0].outcome == TransportOutcome.SUCCESS


def test_checked_send_accepts_compatible_server_minor_version() -> None:
    response = _frame(b"0\1ok")
    response = b"00.8.80" + response[7:]

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result == response.decode()
    assert observations[0].provider_code == "0"
    assert observations[0].outcome == TransportOutcome.SUCCESS


def test_checked_send_rejects_incompatible_server_protocol_family() -> None:
    response = _frame(b"0\1ok")
    response = b"01.0.00" + response[7:]

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
        protocol_stage=ProtocolStage.FRAME,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )


def test_checked_send_classifies_provider_status_without_persisting_message() -> None:
    response = _frame(
        b"10001005\1private-token https://provider.invalid/private synthetic raw exception"
    )

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result == response.decode()
    assert len(observations) == 1
    observation = observations[0]
    assert observation.protocol_stage == ProtocolStage.PROVIDER_STATUS
    assert observation.outcome == TransportOutcome.ERROR
    assert observation.provider_code == "10001005"
    assert observation.normalized_error == NormalizedTransportError.RATE_LIMIT
    serialized = observation.model_dump_json()
    assert "private-token" not in serialized
    assert "https://provider.invalid/private" not in serialized
    assert "synthetic raw exception" not in serialized


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(_frame(b""), id="empty-noncompressed-body"),
        pytest.param(
            _frame(
                zlib.compress(b"unsafe code\1private-token https://provider.invalid/private"),
                message_type="96",
            ),
            id="unsafe-compressed-provider-code",
        ),
    ],
)
def test_checked_send_rejects_missing_or_unsafe_provider_code(response: bytes) -> None:
    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
        protocol_stage=ProtocolStage.PROVIDER_STATUS,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )
    assert observations[0].provider_code is None


def test_checked_send_retains_safe_unknown_provider_code_without_message_guessing() -> None:
    response = _frame(b"vendor-new-code\1private-token https://provider.invalid/private rate limit")

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result == response.decode()
    assert len(observations) == 1
    observation = observations[0]
    assert observation.protocol_stage == ProtocolStage.PROVIDER_STATUS
    assert observation.outcome == TransportOutcome.ERROR
    assert observation.provider_code == "vendor-new-code"
    assert observation.normalized_error == (
        NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR
    )
    serialized = observation.model_dump_json()
    assert "private-token" not in serialized
    assert "https://provider.invalid/private" not in serialized
    assert "rate limit" not in serialized


def test_checked_connect_converts_error_before_exposing_raw_exception(monkeypatch) -> None:
    import backend.app.market.baostock_vendor as vendor

    connection = FramedSocket(
        [],
        connect_error=OSError(
            "synthetic raw exception private-token https://provider.invalid/private"
        ),
    )
    observations: list[TransportObservation] = []
    monkeypatch.setattr(vendor.socket, "socket", lambda *_args: connection)

    with (
        refresh_scope("refresh-safe"),
        provider_session_scope("session-safe"),
        request_scope(ProviderEndpoint.TRADE_DATES, attempt=1),
        transport_observation_sink(observations.append),
        pytest.raises(BaoStockTransportPatchError) as caught,
    ):
        checked_connect(object())

    _assert_sanitized_terminal(
        caught.value,
        observations,
        normalized_error=NormalizedTransportError.CONNECT_ERROR,
        protocol_stage=ProtocolStage.CONNECT,
        recv_calls=0,
        response_bytes=0,
        end_marker_seen=False,
    )
    assert connection.connect_calls == 1
    assert connection.closed is True


def test_checked_send_converts_send_error_without_reading() -> None:
    connection = FramedSocket(
        [],
        send_error=OSError("synthetic raw exception /Users/private/provider.log private-token"),
    )

    result, error, observations = _invoke_checked_send(connection)

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.SEND_ERROR,
        protocol_stage=ProtocolStage.SEND,
        recv_calls=0,
        response_bytes=0,
        end_marker_seen=False,
    )


@pytest.mark.parametrize(
    ("chunks", "expected_error", "expected_stage", "recv_calls", "response_bytes", "marker"),
    [
        (
            [TimeoutError("synthetic raw exception private-token")],
            NormalizedTransportError.RECV_TIMEOUT,
            ProtocolStage.RECEIVE,
            1,
            0,
            False,
        ),
        (
            [b""],
            NormalizedTransportError.EOF,
            ProtocolStage.RECEIVE,
            1,
            0,
            False,
        ),
        (
            [b"bad" + PROTOCOL_MARKER],
            NormalizedTransportError.SHORT_HEADER,
            ProtocolStage.FRAME,
            1,
            len(b"bad" + PROTOCOL_MARKER),
            True,
        ),
        (
            [_frame(b"0\1ok")[: -len(PROTOCOL_MARKER)] + b"invalid-marker"],
            NormalizedTransportError.PROTOCOL_ERROR,
            ProtocolStage.FRAME,
            1,
            len(_frame(b"0\1ok")[: -len(PROTOCOL_MARKER)] + b"invalid-marker"),
            False,
        ),
        (
            [b"00.9.30\1AA\1notdigits!" + PROTOCOL_MARKER],
            NormalizedTransportError.PROTOCOL_ERROR,
            ProtocolStage.FRAME,
            1,
            len(b"00.9.30\1AA\1notdigits!" + PROTOCOL_MARKER),
            True,
        ),
    ],
)
def test_checked_receive_fails_closed_with_exact_socket_counters(
    chunks: list[bytes | BaseException],
    expected_error: NormalizedTransportError,
    expected_stage: ProtocolStage,
    recv_calls: int,
    response_bytes: int,
    marker: bool,
) -> None:
    result, error, observations = _invoke_checked_send(FramedSocket(chunks))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=expected_error,
        protocol_stage=expected_stage,
        recv_calls=recv_calls,
        response_bytes=response_bytes,
        end_marker_seen=marker,
    )


def test_checked_receive_rejects_bad_compression_with_raw_byte_counters() -> None:
    response = _frame(b"not-zlib", message_type="96")

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.BAD_COMPRESSION,
        protocol_stage=ProtocolStage.DECOMPRESS,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )


def test_checked_receive_rejects_trailing_compressed_payload_bytes() -> None:
    compressed = zlib.compress(b"0\1ok\n") + b"private-token"
    response = _frame(compressed, message_type="96")

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert result is None
    assert error is not None
    _assert_sanitized_terminal(
        error,
        observations,
        normalized_error=NormalizedTransportError.BAD_COMPRESSION,
        protocol_stage=ProtocolStage.DECOMPRESS,
        recv_calls=1,
        response_bytes=len(response),
        end_marker_seen=True,
    )


def test_checked_receive_returns_decompressed_body_only_after_complete_frame() -> None:
    body = b"0\1ok\n"
    compressed = zlib.compress(body)
    response = _frame(compressed, message_type="96")

    result, error, observations = _invoke_checked_send(FramedSocket([response]))

    assert error is None
    assert result == response[:21].decode() + body.decode()
    assert observations[0].recv_calls == 1
    assert observations[0].response_bytes == len(response)
    assert observations[0].end_marker_seen is True


@pytest.mark.parametrize(
    ("version", "source_sha"),
    [
        ("0.9.4", EXPECTED_SOCKETUTIL_SHA256),
        (EXPECTED_BAOSTOCK_VERSION, "0" * 64),
    ],
)
def test_patch_refuses_wrong_version_or_source_before_replacing_sdk_calls(
    monkeypatch,
    version: str,
    source_sha: str,
) -> None:
    import baostock.util.socketutil as socketutil

    import backend.app.market.baostock_vendor as vendor

    original_connect = socketutil.SocketUtil.connect
    original_send_msg = socketutil.send_msg
    monkeypatch.setattr(vendor, "installed_baostock_version", lambda: version)
    monkeypatch.setattr(vendor, "socketutil_source_sha256", lambda: source_sha)

    with pytest.raises(BaoStockTransportPatchError, match="pinned source verification failed"):
        install_baostock_transport_patch()

    assert socketutil.SocketUtil.connect is original_connect
    assert socketutil.send_msg is original_send_msg


def test_patch_sanitizes_source_verification_read_failure(monkeypatch) -> None:
    import backend.app.market.baostock_vendor as vendor

    monkeypatch.setattr(
        vendor,
        "socketutil_source_sha256",
        lambda: (_ for _ in ()).throw(
            OSError("synthetic raw exception /Users/private/provider.log private-token")
        ),
    )

    with pytest.raises(BaoStockTransportPatchError) as caught:
        install_baostock_transport_patch()

    assert str(caught.value) == "BaoStock pinned source verification failed"


def test_local_baostock_source_matches_the_pinned_patch_contract() -> None:
    assert EXPECTED_BAOSTOCK_VERSION == "0.9.3"
    assert EXPECTED_SOCKETUTIL_SHA256 == (
        "248591168ad087fb9c91b64e8c909608082528ecbecf25541dcbce0fe9cfcd25"
    )
    assert socketutil_source_sha256() == EXPECTED_SOCKETUTIL_SHA256


def test_patch_installation_is_idempotent_under_concurrency() -> None:
    import baostock.util.socketutil as socketutil

    errors: list[BaseException] = []

    def install() -> None:
        try:
            install_baostock_transport_patch()
        except BaseException as exc:
            errors.append(exc)

    workers = [threading.Thread(target=install) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(1)

    assert errors == []
    assert all(worker.is_alive() is False for worker in workers)
    assert socketutil.SocketUtil.connect is checked_connect
    assert socketutil.send_msg is checked_send_msg


@pytest.mark.parametrize(
    "deadline_stage",
    ["login-connect", "request-send", "request-recv"],
)
def test_provider_deadline_emits_exactly_one_terminal_observation_and_keeps_legacy_error(
    monkeypatch,
    deadline_stage: str,
) -> None:
    import baostock.common.context as context

    import backend.app.market.baostock_vendor as vendor

    interrupt_type = getattr(provider_transport, "OperationDeadlineInterrupt", None)
    assert interrupt_type is not None
    observations = []
    previous = getattr(context, "default_socket", None)
    context.default_socket = None

    class Result:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]

        def __init__(self) -> None:
            self.rows = iter([["2026-07-23", "1"]])
            self.current = None

        def next(self) -> bool:
            self.current = next(self.rows, None)
            return self.current is not None

        def get_row_data(self):
            return self.current

    if deadline_stage == "login-connect":
        connection = FramedSocket([], connect_error=interrupt_type())
        monkeypatch.setattr(vendor.socket, "socket", lambda *_args: connection)

        class Client:
            def __init__(self) -> None:
                self.context = context

            def login(self):
                checked_connect(object())
                return Result()

        expected_stage = ProtocolStage.CONNECT
        expected_error = NormalizedTransportError.CONNECT_ERROR
    else:
        connection = (
            FramedSocket([], send_error=interrupt_type())
            if deadline_stage == "request-send"
            else FramedSocket([interrupt_type()])
        )

        class Client:
            def __init__(self) -> None:
                self.context = context

            def login(self):
                context.default_socket = connection
                return Result()

            def query_trade_dates(self, **_kwargs):
                checked_send_msg("private-token-deadline-request")
                return Result()

        expected_stage = (
            ProtocolStage.SEND if deadline_stage == "request-send" else ProtocolStage.RECEIVE
        )
        expected_error = (
            NormalizedTransportError.SEND_ERROR
            if deadline_stage == "request-send"
            else NormalizedTransportError.RECV_TIMEOUT
        )

    provider = BaoStockProvider(
        client=Client(),
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    try:
        with (
            transport_observation_sink(observations.append),
            pytest.raises(_OperationDeadlineExceeded) as caught,
        ):
            provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23))
    finally:
        context.default_socket = previous

    assert caught.value.failure is not None
    assert caught.value.failure.failure_class == "transport_timeout"
    socket_terminals = [
        item for item in observations if item.protocol_stage != ProtocolStage.OPERATION
    ]
    operation_terminals = [
        item for item in observations if item.protocol_stage == ProtocolStage.OPERATION
    ]
    assert len(socket_terminals) == 1
    observation = socket_terminals[0]
    assert observation.protocol_stage == expected_stage
    assert observation.normalized_error == expected_error
    assert len(operation_terminals) == 1
    operation = operation_terminals[0]
    assert operation.normalized_error == NormalizedTransportError.RECV_TIMEOUT
    assert operation.refresh_id == observation.refresh_id
    assert operation.endpoint == observation.endpoint
    assert operation.request_id == observation.request_id
    assert operation.provider_session_id == observation.provider_session_id
    serialized = "".join(item.model_dump_json() for item in observations) + str(caught.value)
    assert "private-token-deadline-request" not in serialized
    assert "raw exception" not in serialized
