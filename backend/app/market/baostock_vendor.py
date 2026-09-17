import hashlib
import importlib.metadata
import re
import socket
import threading
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from backend.app.market.failures import MarketFailure
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    OperationDeadlineInterrupt,
    ProtocolStage,
    TransportObservation,
    TransportOutcome,
    TransportScopeError,
    current_request_context,
    normalize_provider_code,
)

EXPECTED_BAOSTOCK_VERSION = "0.9.3"
EXPECTED_SOCKETUTIL_SHA256 = "248591168ad087fb9c91b64e8c909608082528ecbecf25541dcbce0fe9cfcd25"
PROTOCOL_MARKER = b"<![CDATA[]]>\n"
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_RECV_CALLS = 16_384

ObservationSink = Callable[[TransportObservation], None]
_observation_sink: ContextVar[ObservationSink | None] = ContextVar(
    "baostock_transport_observation_sink",
    default=None,
)
_patch_lock = threading.Lock()
_safe_provider_code = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
_compatible_protocol_version = re.compile(r"^00\.\d\.\d{2}$")


class BaoStockTransportPatchError(OSError):
    def __init__(
        self,
        message: str,
        *,
        normalized_error: NormalizedTransportError,
        protocol_stage: ProtocolStage,
        recv_calls: int = 0,
        response_bytes: int = 0,
        end_marker_seen: bool = False,
        failure: MarketFailure | None = None,
    ) -> None:
        self.normalized_error = normalized_error
        self.protocol_stage = protocol_stage
        self.recv_calls = recv_calls
        self.response_bytes = response_bytes
        self.end_marker_seen = end_marker_seen
        self.failure = failure or _market_failure(normalized_error)
        super().__init__(message)


def _market_failure(normalized_error: NormalizedTransportError) -> MarketFailure:
    failure_class = (
        "transport_timeout"
        if normalized_error == NormalizedTransportError.RECV_TIMEOUT
        else "transport_connect"
    )
    return MarketFailure(
        failure_stage="fetch",
        failure_class=failure_class,
        retryable=True,
    )


@contextmanager
def transport_observation_sink(sink: ObservationSink) -> Iterator[None]:
    token = _observation_sink.set(sink)
    try:
        yield
    finally:
        _observation_sink.reset(token)


def emit_terminal_observation(
    *,
    started_at: float,
    protocol_stage: ProtocolStage,
    recv_calls: int,
    response_bytes: int,
    end_marker_seen: bool,
    provider_code: str | None,
    normalized_error: NormalizedTransportError | None,
) -> None:
    sink = _observation_sink.get()
    if sink is None:
        return
    try:
        request = current_request_context()
    except TransportScopeError:
        return
    sink(
        TransportObservation(
            refresh_id=request.refresh_id,
            provider_session_id=request.provider_session_id,
            request_id=request.request_id,
            endpoint=request.endpoint,
            attempt=request.attempt,
            page=request.page,
            protocol_stage=protocol_stage,
            elapsed_ms=max(0, int((monotonic() - started_at) * 1000)),
            recv_calls=recv_calls,
            response_bytes=response_bytes,
            end_marker_seen=end_marker_seen,
            provider_code=provider_code,
            normalized_error=normalized_error,
            outcome=(
                TransportOutcome.ERROR if normalized_error is not None else TransportOutcome.SUCCESS
            ),
            observed_at=datetime.now(UTC),
        )
    )


def _raise_transport_error(
    message: str,
    *,
    started_at: float,
    normalized_error: NormalizedTransportError,
    protocol_stage: ProtocolStage,
    recv_calls: int = 0,
    response_bytes: int = 0,
    end_marker_seen: bool = False,
    failure: MarketFailure | None = None,
) -> None:
    emit_terminal_observation(
        started_at=started_at,
        protocol_stage=protocol_stage,
        recv_calls=recv_calls,
        response_bytes=response_bytes,
        end_marker_seen=end_marker_seen,
        provider_code=None,
        normalized_error=normalized_error,
    )
    raise BaoStockTransportPatchError(
        message,
        normalized_error=normalized_error,
        protocol_stage=protocol_stage,
        recv_calls=recv_calls,
        response_bytes=response_bytes,
        end_marker_seen=end_marker_seen,
        failure=failure,
    ) from None


def checked_connect(_socket_util: Any) -> None:
    import baostock.common.contants as constants
    import baostock.common.context as context

    started_at = monotonic()
    connection = None
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect((constants.BAOSTOCK_SERVER_IP, constants.BAOSTOCK_SERVER_PORT))
    except OperationDeadlineInterrupt:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        emit_terminal_observation(
            started_at=started_at,
            protocol_stage=ProtocolStage.CONNECT,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=NormalizedTransportError.CONNECT_ERROR,
        )
        raise
    except Exception as exc:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        failure = MarketFailure(
            failure_stage="fetch",
            failure_class="dns" if isinstance(exc, socket.gaierror) else "transport_connect",
            retryable=True,
        )
        _raise_transport_error(
            "BaoStock transport connect failed",
            started_at=started_at,
            normalized_error=NormalizedTransportError.CONNECT_ERROR,
            protocol_stage=ProtocolStage.CONNECT,
            failure=failure,
        )
    context.default_socket = connection
    emit_terminal_observation(
        started_at=started_at,
        protocol_stage=ProtocolStage.COMPLETE,
        recv_calls=0,
        response_bytes=0,
        end_marker_seen=False,
        provider_code=None,
        normalized_error=None,
    )


def _parse_header(header: bytes) -> tuple[str, int]:
    import baostock.common.contants as constants

    try:
        parts = header.decode("ascii").split(constants.MESSAGE_SPLIT)
    except UnicodeDecodeError:
        raise ValueError("invalid BaoStock frame header") from None
    if (
        len(parts) != 3
        or _compatible_protocol_version.fullmatch(parts[0]) is None
        or len(parts[1]) != 2
        or not parts[1].isalnum()
        or len(parts[2]) != constants.MESSAGE_HEADER_BODYLENGTH
        or not parts[2].isdigit()
    ):
        raise ValueError("invalid BaoStock frame header")
    return parts[1], int(parts[2])


def _provider_code(body: bytes) -> str | None:
    import baostock.common.contants as constants

    try:
        code = body.decode("utf-8").split(constants.MESSAGE_SPLIT, 1)[0]
    except UnicodeDecodeError:
        return None
    return code if _safe_provider_code.fullmatch(code) else None


def _decompress_body(raw_body: bytes) -> bytes:
    decompressor = zlib.decompressobj()
    body = decompressor.decompress(raw_body, MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES or decompressor.unconsumed_tail:
        raise zlib.error("decompressed BaoStock response exceeds the bound")
    body += decompressor.flush(MAX_RESPONSE_BYTES - len(body) + 1)
    if len(body) > MAX_RESPONSE_BYTES or not decompressor.eof or decompressor.unused_data:
        raise zlib.error("invalid compressed BaoStock response")
    return body


def checked_send_msg(message: str) -> str:
    import baostock.common.contants as constants
    import baostock.common.context as context

    started_at = monotonic()
    connection = getattr(context, "default_socket", None)
    if connection is None:
        _raise_transport_error(
            "BaoStock transport session is unavailable",
            started_at=started_at,
            normalized_error=NormalizedTransportError.CONNECT_ERROR,
            protocol_stage=ProtocolStage.CONNECT,
        )
    try:
        connection.sendall((message + "\n").encode("utf-8"))
    except OperationDeadlineInterrupt:
        emit_terminal_observation(
            started_at=started_at,
            protocol_stage=ProtocolStage.SEND,
            recv_calls=0,
            response_bytes=0,
            end_marker_seen=False,
            provider_code=None,
            normalized_error=NormalizedTransportError.SEND_ERROR,
        )
        raise
    except Exception:
        _raise_transport_error(
            "BaoStock transport send failed",
            started_at=started_at,
            normalized_error=NormalizedTransportError.SEND_ERROR,
            protocol_stage=ProtocolStage.SEND,
        )

    received = bytearray()
    recv_calls = 0
    message_type: str | None = None
    body_length: int | None = None
    raw_body: bytes | None = None
    while True:
        if recv_calls >= MAX_RECV_CALLS:
            _raise_transport_error(
                "BaoStock transport receive bound exceeded",
                started_at=started_at,
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.RECEIVE,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=received.endswith(PROTOCOL_MARKER),
            )
        recv_calls += 1
        try:
            chunk = connection.recv(8192)
        except OperationDeadlineInterrupt:
            emit_terminal_observation(
                started_at=started_at,
                protocol_stage=ProtocolStage.RECEIVE,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=received.endswith(PROTOCOL_MARKER),
                provider_code=None,
                normalized_error=NormalizedTransportError.RECV_TIMEOUT,
            )
            raise
        except TimeoutError:
            _raise_transport_error(
                "BaoStock transport receive timed out",
                started_at=started_at,
                normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                protocol_stage=ProtocolStage.RECEIVE,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=received.endswith(PROTOCOL_MARKER),
            )
        except ConnectionResetError:
            chunk = b""
        except OSError:
            _raise_transport_error(
                "BaoStock transport receive failed",
                started_at=started_at,
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.RECEIVE,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=received.endswith(PROTOCOL_MARKER),
            )

        if chunk == b"":
            _raise_transport_error(
                "BaoStock transport response ended before a complete frame",
                started_at=started_at,
                normalized_error=NormalizedTransportError.EOF,
                protocol_stage=ProtocolStage.RECEIVE,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=received.endswith(PROTOCOL_MARKER),
            )

        received.extend(chunk)
        end_marker_seen = received.endswith(PROTOCOL_MARKER)
        if len(received) > MAX_RESPONSE_BYTES:
            _raise_transport_error(
                "BaoStock transport response bound exceeded",
                started_at=started_at,
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.FRAME,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=end_marker_seen,
            )

        if len(received) < constants.MESSAGE_HEADER_LENGTH:
            if end_marker_seen:
                _raise_transport_error(
                    "BaoStock transport response header is incomplete",
                    started_at=started_at,
                    normalized_error=NormalizedTransportError.SHORT_HEADER,
                    protocol_stage=ProtocolStage.FRAME,
                    recv_calls=recv_calls,
                    response_bytes=len(received),
                    end_marker_seen=True,
                )
            continue

        if body_length is None:
            try:
                message_type, body_length = _parse_header(
                    bytes(received[: constants.MESSAGE_HEADER_LENGTH])
                )
            except ValueError:
                _raise_transport_error(
                    "BaoStock transport response frame is invalid",
                    started_at=started_at,
                    normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                    protocol_stage=ProtocolStage.FRAME,
                    recv_calls=recv_calls,
                    response_bytes=len(received),
                    end_marker_seen=end_marker_seen,
                )
            if body_length > MAX_RESPONSE_BYTES - constants.MESSAGE_HEADER_LENGTH - len(
                PROTOCOL_MARKER
            ):
                _raise_transport_error(
                    "BaoStock transport response frame is invalid",
                    started_at=started_at,
                    normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                    protocol_stage=ProtocolStage.FRAME,
                    recv_calls=recv_calls,
                    response_bytes=len(received),
                    end_marker_seen=end_marker_seen,
                )

        if not end_marker_seen:
            if len(received) > constants.MESSAGE_HEADER_LENGTH + body_length + len(PROTOCOL_MARKER):
                _raise_transport_error(
                    "BaoStock transport response marker is invalid",
                    started_at=started_at,
                    normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                    protocol_stage=ProtocolStage.FRAME,
                    recv_calls=recv_calls,
                    response_bytes=len(received),
                    end_marker_seen=False,
                )
            continue
        wire_body = bytes(received[constants.MESSAGE_HEADER_LENGTH : -len(PROTOCOL_MARKER)])
        if body_length in {len(wire_body), len(wire_body) + len(PROTOCOL_MARKER)}:
            raw_body = wire_body
        elif message_type not in constants.COMPRESSED_MESSAGE_TYPE_TUPLE:
            declared_body = wire_body[:body_length]
            crc_trailer = wire_body[body_length:]
            expected_crc = zlib.crc32(
                bytes(received[: constants.MESSAGE_HEADER_LENGTH]) + declared_body
            )
            if crc_trailer == b"\1" + str(expected_crc).encode("ascii"):
                raw_body = declared_body
        if raw_body is None:
            _raise_transport_error(
                "BaoStock transport response frame length is invalid",
                started_at=started_at,
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.FRAME,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=end_marker_seen,
            )
        break

    body_start = constants.MESSAGE_HEADER_LENGTH
    assert raw_body is not None
    if message_type in constants.COMPRESSED_MESSAGE_TYPE_TUPLE:
        try:
            body = _decompress_body(raw_body)
            decoded_body = body.decode("utf-8")
        except (UnicodeDecodeError, zlib.error):
            _raise_transport_error(
                "BaoStock transport response decompression failed",
                started_at=started_at,
                normalized_error=NormalizedTransportError.BAD_COMPRESSION,
                protocol_stage=ProtocolStage.DECOMPRESS,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=True,
            )
        result = bytes(received[:body_start]).decode("ascii") + decoded_body
        provider_code = _provider_code(body)
    else:
        try:
            result = (
                bytes(received[:body_start]).decode("ascii")
                + raw_body.decode("utf-8")
                + PROTOCOL_MARKER.decode("ascii")
            )
        except UnicodeDecodeError:
            _raise_transport_error(
                "BaoStock transport response encoding is invalid",
                started_at=started_at,
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.FRAME,
                recv_calls=recv_calls,
                response_bytes=len(received),
                end_marker_seen=True,
            )
        provider_code = _provider_code(raw_body)

    if provider_code is None:
        _raise_transport_error(
            "BaoStock provider status is invalid",
            started_at=started_at,
            normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
            protocol_stage=ProtocolStage.PROVIDER_STATUS,
            recv_calls=recv_calls,
            response_bytes=len(received),
            end_marker_seen=True,
        )
    normalized_error = normalize_provider_code(provider_code) if provider_code is not None else None
    emit_terminal_observation(
        started_at=started_at,
        protocol_stage=(
            ProtocolStage.PROVIDER_STATUS
            if normalized_error is not None
            else ProtocolStage.COMPLETE
        ),
        recv_calls=recv_calls,
        response_bytes=len(received),
        end_marker_seen=True,
        provider_code=provider_code,
        normalized_error=normalized_error,
    )
    return result


def installed_baostock_version() -> str:
    return importlib.metadata.version("baostock")


def socketutil_source_sha256() -> str:
    import baostock.util.socketutil as socketutil

    return hashlib.sha256(Path(socketutil.__file__).read_bytes()).hexdigest()


def install_baostock_transport_patch() -> None:
    import baostock.util.socketutil as socketutil

    with _patch_lock:
        try:
            verified = (
                installed_baostock_version() == EXPECTED_BAOSTOCK_VERSION
                and socketutil_source_sha256() == EXPECTED_SOCKETUTIL_SHA256
            )
        except Exception:
            verified = False
        if not verified:
            raise BaoStockTransportPatchError(
                "BaoStock pinned source verification failed",
                normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                protocol_stage=ProtocolStage.FRAME,
                failure=MarketFailure(
                    failure_stage="fetch",
                    failure_class="internal",
                    retryable=False,
                ),
            )
        if (
            socketutil.SocketUtil.connect is checked_connect
            and socketutil.send_msg is checked_send_msg
        ):
            return
        # Installing checked-send first ensures no concurrent caller can observe
        # a newly connected socket with the upstream partial-send implementation.
        socketutil.send_msg = checked_send_msg
        socketutil.SocketUtil.connect = checked_connect
