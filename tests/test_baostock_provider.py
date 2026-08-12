import json
import signal
import socket
import threading
import time
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.market.baostock import (
    BaoStockError,
    BaoStockProvider,
    BaoStockSessionStateError,
)
from backend.app.market.provider_transport import (
    ProviderEndpoint,
    current_request_context,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "baostock_daily.json"


class FakeResult:
    def __init__(self, fields, rows, error_code="0", error_msg="") -> None:
        self.fields = fields
        self.rows = iter(rows)
        self.error_code = error_code
        self.error_msg = error_msg
        self.current = None

    def next(self) -> bool:
        self.current = next(self.rows, None)
        return self.current is not None

    def get_row_data(self):
        return self.current


class FakeBaoStock:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.logged_out = False

    def login(self):
        return FakeResult([], [])

    def logout(self):
        self.logged_out = True

    def query_history_k_data_plus(self, code, fields, **kwargs):
        names = fields.split(",")
        rows = [
            row
            for row in self.payload["daily_rows"]
            if row[self.payload["daily_fields"].index("code")] == code
        ]
        positions = [self.payload["daily_fields"].index(name) for name in names]
        return FakeResult(names, [[row[position] for position in positions] for row in rows])

    def query_adjust_factor(self, code, **kwargs):
        rows = [row for row in self.payload["factor_rows"] if row[0] == code]
        return FakeResult(self.payload["factor_fields"], rows)

    def query_daily_history_k_AStock(self, **kwargs):
        return FakeResult(self.payload["daily_fields"], self.payload["daily_rows"])

    def query_daily_adjust_factor(self, **kwargs):
        target = kwargs["date"]
        date_index = self.payload["factor_fields"].index("dividOperateDate")
        rows = [row for row in self.payload["factor_rows"] if row[date_index] == target]
        return FakeResult(self.payload["factor_fields"], rows)

    def query_trade_dates(self, **kwargs):
        return FakeResult(
            ["calendar_date", "is_trading_day"],
            [["2026-07-23", "1"]],
        )

    def query_all_stock(self, **kwargs):
        code_index = self.payload["daily_fields"].index("code")
        return FakeResult(
            ["code", "tradeStatus", "code_name"],
            [[row[code_index], "1", row[code_index]] for row in self.payload["daily_rows"]],
        )


def test_provider_logs_out_and_returns_canonical_explicit_slice() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = FakeBaoStock(payload)
    provider = BaoStockProvider(client=client)

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000", "sh.000001"])

    assert [bar.symbol for bar in batch.bars] == ["sh.600000", "sh.000001"]
    assert batch.failed_symbols == []
    assert batch.bars[0].adjust_factor == 0.8
    assert client.logged_out is True


def test_provider_bulk_slice_filters_main_board_and_adds_indexes() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    provider = BaoStockProvider(client=FakeBaoStock(payload))

    batch = provider.fetch(date(2026, 7, 23))

    assert len([bar for bar in batch.bars if bar.security_type == "stock"]) == 3
    assert [bar.symbol for bar in batch.bars if bar.security_type == "index"] == [
        "sh.000001",
        "sz.399001",
    ]


def test_universe_inspection_reads_metadata_without_fetching_market_rows() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class MetadataOnlyClient(FakeBaoStock):
        def query_daily_history_k_AStock(self, **kwargs):
            raise AssertionError("universe inspection must not fetch daily rows")

        def query_daily_adjust_factor(self, **kwargs):
            raise AssertionError("universe inspection must not fetch factors")

        def query_history_k_data_plus(self, *args, **kwargs):
            raise AssertionError("universe inspection must not fetch indexes")

    provider = BaoStockProvider(client=MetadataOnlyClient(payload))

    inspection = provider.inspect_main_board(date(2026, 7, 23))

    assert inspection.main_board_count == 3
    assert inspection.shanghai_count == 2
    assert inspection.shenzhen_count == 1
    assert inspection.total_expected_count == 5
    assert inspection.metadata_provider_requests == 2
    assert inspection.main_board_symbols == (
        "sh.600000",
        "sh.600001",
        "sz.000001",
    )


def test_provider_applies_minimum_interval_to_every_operation() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    clock = [0.0]
    sleeps: list[float] = []

    def monotonic() -> float:
        return clock[0]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    provider = BaoStockProvider(
        client=FakeBaoStock(payload),
        min_request_interval_seconds=0.5,
        monotonic_fn=monotonic,
        sleep_fn=sleeper,
    )

    provider.inspect_main_board(date(2026, 7, 23))

    assert sleeps == [0.5]


class FakeSocket:
    def __init__(self) -> None:
        self.timeout = None
        self.shutdown_calls = 0
        self.closed = False

    def settimeout(self, seconds) -> None:
        self.timeout = seconds

    def shutdown(self, _how) -> None:
        self.shutdown_calls += 1

    def close(self) -> None:
        self.closed = True


class ActiveSessionClient(FakeBaoStock):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.context = SimpleNamespace(default_socket=FakeSocket())
        self.login_calls = 0
        self.query_calls = 0
        self.logout_calls = 0

    def login(self):
        self.login_calls += 1
        raise TimeoutError("must not replace an active session")

    def query_trade_dates(self, **kwargs):
        self.query_calls += 1
        return super().query_trade_dates(**kwargs)

    def logout(self):
        self.logout_calls += 1


@pytest.mark.parametrize("entrypoint", ["login", "fetch"])
def test_active_session_entry_fails_without_touching_existing_session(
    entrypoint: str,
) -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = ActiveSessionClient(payload)
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )
    provider._session_usable = True
    original_socket = client.context.default_socket

    with pytest.raises(BaoStockSessionStateError, match="session is already active"):
        if entrypoint == "login":
            provider._login()
        else:
            provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert client.login_calls == 0
    assert client.query_calls == 0
    assert client.logout_calls == 0
    assert original_socket.shutdown_calls == 0
    assert original_socket.closed is False
    assert client.context.default_socket is original_socket
    assert provider._session_usable is True
    assert provider.client is client


@pytest.mark.parametrize(
    "raised",
    [
        RuntimeError("synthetic login failure"),
        BaoStockError("synthetic BaoStock login failure"),
    ],
)
def test_generic_login_failure_discards_only_the_new_half_initialized_session(
    raised: Exception,
) -> None:
    class HalfInitializedLoginClient:
        def __init__(self) -> None:
            self.context = SimpleNamespace(default_socket=None)
            self.socket = FakeSocket()
            self.login_calls = 0

        def login(self):
            self.login_calls += 1
            self.context.default_socket = self.socket
            raise raised

    client = HalfInitializedLoginClient()
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    with pytest.raises(type(raised)) as captured:
        provider._login()

    assert captured.value is raised
    assert client.login_calls == 1
    assert client.socket.shutdown_calls == 1
    assert client.socket.closed is True
    assert client.context.default_socket is None
    assert provider._session_usable is False


@pytest.mark.parametrize(
    "raised",
    [
        RuntimeError("synthetic timeout configuration failure"),
        BaoStockError("synthetic BaoStock timeout configuration failure"),
    ],
)
def test_generic_timeout_configuration_failure_discards_the_new_session(
    raised: Exception,
) -> None:
    class FailingTimeoutSocket(FakeSocket):
        def settimeout(self, seconds) -> None:
            self.timeout = seconds
            raise raised

    class SuccessfulLoginClient:
        def __init__(self) -> None:
            self.context = SimpleNamespace(default_socket=None)
            self.socket = FailingTimeoutSocket()
            self.login_calls = 0

        def login(self):
            self.login_calls += 1
            self.context.default_socket = self.socket
            return FakeResult([], [])

    client = SuccessfulLoginClient()
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    with pytest.raises(type(raised)) as captured:
        provider._login()

    assert captured.value is raised
    assert client.login_calls == 1
    assert client.socket.shutdown_calls == 1
    assert client.socket.closed is True
    assert client.context.default_socket is None
    assert provider._session_usable is False


class CloseReleasedSocket(FakeSocket):
    def __init__(self) -> None:
        super().__init__()
        self.recv_started = threading.Event()
        self.released = threading.Event()

    def recv(self, _size) -> bytes:
        self.recv_started.set()
        self.released.wait()
        raise OSError("synthetic connection closed")

    def shutdown(self, how) -> None:
        super().shutdown(how)
        self.released.set()

    def close(self) -> None:
        super().close()
        self.released.set()


class BlockingPaginationResult:
    fields = ["code"]
    error_code = "0"
    error_msg = ""
    data = [["sh.600000"]]
    per_page_count = 1
    cur_page_num = "1"

    def __init__(self, connection: CloseReleasedSocket, continued: list[str]) -> None:
        self.connection = connection
        self.continued = continued
        self.cur_row_num = 0

    def next(self) -> bool:
        if self.cur_row_num == 0:
            return True
        self.connection.recv(8192)
        self.continued.append("pagination continued after close")
        return False

    def get_row_data(self) -> list[str]:
        self.cur_row_num += 1
        return ["sh.600000"]


class DeadlineBlockingClient:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.context = SimpleNamespace(default_socket=None)
        self.sockets: list[CloseReleasedSocket] = []
        self.query_calls = 0
        self.continued: list[str] = []

    def _new_socket(self) -> CloseReleasedSocket:
        connection = CloseReleasedSocket()
        self.sockets.append(connection)
        self.context.default_socket = connection
        return connection

    def login(self):
        connection = self._new_socket()
        if self.mode == "login":
            connection.recv(8192)
            self.continued.append("login continued after close")
        return FakeResult([], [])

    def logout(self):
        return FakeResult([], [])

    def blocking_query(self):
        self.query_calls += 1
        connection = self.context.default_socket
        if self.mode == "query":
            connection.recv(8192)
            self.continued.append("query continued after close")
        return BlockingPaginationResult(connection, self.continued)

    def release_all(self) -> None:
        for connection in self.sockets:
            connection.close()


class NoOpCloseSocket(FakeSocket):
    def shutdown(self, _how) -> None:
        pass

    def close(self) -> None:
        pass


class NeverReleasedRequestClient:
    def __init__(self) -> None:
        self.context = SimpleNamespace(default_socket=None)
        self.release = threading.Event()
        self.query_calls = 0
        self.continued: list[str] = []

    def login(self):
        self.context.default_socket = NoOpCloseSocket()
        return FakeResult([], [])

    def blocking_query(self):
        self.query_calls += 1
        self.release.wait()
        self.continued.append("request continued after deadline")
        return FakeResult(["code"], [["sh.600000"]])


def assert_fails_within_deadline(
    operation,
    client: DeadlineBlockingClient,
    *,
    expected_seconds: float,
) -> None:
    started_at = time.monotonic()
    with pytest.raises(BaoStockError):
        operation()
    assert time.monotonic() - started_at <= expected_seconds
    assert all(connection.closed for connection in client.sockets)
    assert client.continued == []
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("stock-eva-baostock-operation-") and thread.is_alive()
    ]


@pytest.mark.parametrize("mode", ["login", "query", "pagination"])
def test_wall_clock_deadline_closes_blocking_baostock_operation(mode: str) -> None:
    timeout = 0.02
    client = DeadlineBlockingClient(mode)
    provider = BaoStockProvider(
        client=client,
        max_attempts=1,
        min_request_interval_seconds=0,
        socket_timeout_seconds=timeout,
    )

    if mode == "login":

        def operation():
            return provider._login()
    else:
        provider._login()

        def operation():
            return provider._read(ProviderEndpoint.INDEX_HISTORY, client.blocking_query)

    assert_fails_within_deadline(
        operation,
        client,
        expected_seconds=timeout + 0.12,
    )


def test_retry_attempts_have_a_derived_total_wall_clock_bound() -> None:
    timeout = 0.02
    client = DeadlineBlockingClient("query")
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=timeout,
    )
    provider._login()

    assert_fails_within_deadline(
        lambda: provider._read(ProviderEndpoint.INDEX_HISTORY, client.blocking_query),
        client,
        expected_seconds=(provider.max_attempts * timeout) + 0.15,
    )
    assert client.query_calls == provider.max_attempts


def test_deadline_never_leaves_uncancellable_request_workers() -> None:
    client = NeverReleasedRequestClient()
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=0.02,
    )
    provider._login()
    existing_threads = {thread.ident for thread in threading.enumerate()}

    try:
        started_at = time.monotonic()
        with pytest.raises(BaoStockError, match="wall-clock deadline exceeded"):
            provider._read(ProviderEndpoint.INDEX_HISTORY, client.blocking_query)
        assert (
            time.monotonic() - started_at
            <= (provider.max_attempts * provider.socket_timeout_seconds) + 0.1
        )

        leaked_workers = [
            thread
            for thread in threading.enumerate()
            if thread.ident not in existing_threads
            and thread.name.startswith("stock-eva-baostock-operation-request")
            and thread.is_alive()
        ]
        assert leaked_workers == []
        assert client.query_calls == provider.max_attempts
        assert client.continued == []
        assert client.context.default_socket is None
    finally:
        client.release.set()
        for thread in threading.enumerate():
            if thread.ident not in existing_threads and thread.name.startswith(
                "stock-eva-baostock-operation-request"
            ):
                thread.join(0.2)


def test_non_main_thread_fails_closed_before_network_operation() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        socket_timeout_seconds=0.02,
    )
    operation_calls: list[str] = []
    errors: list[BaseException] = []

    def call_from_worker() -> None:
        try:
            provider._run_with_deadline(
                lambda: operation_calls.append("called"),
                operation_name="request",
            )
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=call_from_worker)
    caller.start()
    caller.join(0.2)

    assert caller.is_alive() is False
    assert operation_calls == []
    assert len(errors) == 1
    assert isinstance(errors[0], BaoStockError)
    assert "main thread" in str(errors[0])


def test_deadline_interrupt_escapes_broad_exception_handler() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        socket_timeout_seconds=0.02,
    )
    swallowed: list[str] = []
    unwound: list[str] = []

    def operation() -> None:
        try:
            threading.Event().wait()
        except Exception:
            swallowed.append("deadline")
        finally:
            unwound.append("operation")

    with pytest.raises(BaoStockError, match="wall-clock deadline exceeded"):
        provider._run_with_deadline(operation, operation_name="request")

    assert swallowed == []
    assert unwound == ["operation"]


def test_deadline_restores_existing_signal_handler_and_timer() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        socket_timeout_seconds=0.02,
    )
    original_handler = signal.getsignal(signal.SIGALRM)
    original_delay, original_interval = signal.getitimer(signal.ITIMER_REAL)

    def existing_handler(_signum, _frame) -> None:
        raise AssertionError("existing timer should not fire")

    signal.signal(signal.SIGALRM, existing_handler)
    signal.setitimer(signal.ITIMER_REAL, 0.5)
    try:
        assert provider._run_with_deadline(lambda: "ok", operation_name="request") == "ok"
        restored_delay, restored_interval = signal.getitimer(signal.ITIMER_REAL)
        assert signal.getsignal(signal.SIGALRM) is existing_handler
        assert 0 < restored_delay <= 0.5
        assert restored_interval == 0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, original_delay, original_interval)


def test_earlier_existing_signal_timer_is_not_extended() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        socket_timeout_seconds=0.2,
    )
    original_handler = signal.getsignal(signal.SIGALRM)
    original_delay, original_interval = signal.getitimer(signal.ITIMER_REAL)

    class ExistingDeadline(BaseException):
        pass

    def existing_handler(_signum, _frame) -> None:
        raise ExistingDeadline

    signal.signal(signal.SIGALRM, existing_handler)
    signal.setitimer(signal.ITIMER_REAL, 0.02)
    started_at = time.monotonic()
    try:
        with pytest.raises(ExistingDeadline):
            provider._run_with_deadline(
                lambda: threading.Event().wait(),
                operation_name="request",
            )
        assert time.monotonic() - started_at < provider.socket_timeout_seconds
        assert signal.getsignal(signal.SIGALRM) is existing_handler
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, original_delay, original_interval)


def test_returning_existing_timer_handler_still_fails_closed() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        socket_timeout_seconds=0.2,
    )
    original_handler = signal.getsignal(signal.SIGALRM)
    original_delay, original_interval = signal.getitimer(signal.ITIMER_REAL)

    def returning_handler(_signum, _frame) -> None:
        pass

    signal.signal(signal.SIGALRM, returning_handler)
    signal.setitimer(signal.ITIMER_REAL, 0.02)
    try:
        with pytest.raises(BaoStockError, match="pre-existing timer expired"):
            provider._run_with_deadline(
                lambda: threading.Event().wait(),
                operation_name="request",
            )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, original_delay, original_interval)


class EofSocket(FakeSocket):
    def recv(self, _size) -> bytes:
        return b""


class EofThenRecoverClient(FakeBaoStock):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.context = SimpleNamespace(default_socket=None)
        self.sockets: list[EofSocket] = []
        self.login_calls = 0
        self.history_calls = 0

    def login(self):
        self.login_calls += 1
        connection = EofSocket()
        self.sockets.append(connection)
        self.context.default_socket = connection
        return FakeResult([], [])

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.history_calls += 1
        if self.history_calls == 1:
            self.context.default_socket.recv(8192)
        return super().query_history_k_data_plus(code, fields, **kwargs)


def test_peer_eof_is_converted_to_retryable_transport_failure() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = EofThenRecoverClient(payload)
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=11,
    )

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert batch.failed_symbols == []
    assert client.login_calls == 2
    assert client.history_calls == 2
    assert [item.timeout for item in client.sockets] == [11, 11]
    assert all(item.closed for item in client.sockets)


class RecoveringSocketClient(FakeBaoStock):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.context = SimpleNamespace(default_socket=None)
        self.sockets: list[FakeSocket] = []
        self.login_calls = 0
        self.history_calls: dict[str, int] = {}
        self.fail_once = {"sh.600000"}

    def login(self):
        self.login_calls += 1
        connection = FakeSocket()
        self.sockets.append(connection)
        self.context.default_socket = connection
        return FakeResult([], [])

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.history_calls[code] = self.history_calls.get(code, 0) + 1
        if code in self.fail_once:
            self.fail_once.remove(code)
            raise TimeoutError("synthetic socket timeout")
        return super().query_history_k_data_plus(code, fields, **kwargs)


def test_timeout_closes_socket_relogs_and_retries_with_same_bound() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = RecoveringSocketClient(payload)
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=17,
    )

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert batch.failed_symbols == []
    assert client.history_calls == {"sh.600000": 2}
    assert client.login_calls == 2
    assert [item.timeout for item in client.sockets] == [17, 17]
    assert all(item.closed for item in client.sockets)
    assert client.sockets[0].shutdown_calls == 1


class ContinueAfterFailureClient(RecoveringSocketClient):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.fail_once.clear()
        self.logout_calls = 0

    def logout(self):
        self.logout_calls += 1
        return FakeResult([], [])

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.history_calls[code] = self.history_calls.get(code, 0) + 1
        if code == "sh.600000":
            raise OSError("synthetic broken connection")
        return FakeBaoStock.query_history_k_data_plus(self, code, fields, **kwargs)


def test_exhausted_symbol_is_missing_and_next_symbol_uses_fresh_session() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = ContinueAfterFailureClient(payload)
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=9,
    )

    batch = provider.fetch(
        date(2026, 7, 23),
        symbols=["sh.600000", "sz.000001"],
    )

    assert client.history_calls == {"sh.600000": 2, "sz.000001": 1}
    assert client.login_calls == 3
    assert batch.failed_symbols == ["sh.600000"]
    assert [bar.symbol for bar in batch.bars] == ["sz.000001"]
    assert all(item.timeout == 9 for item in client.sockets)
    assert all(item.closed for item in client.sockets)
    assert client.logout_calls == 1


class InterruptingClient(RecoveringSocketClient):
    interrupt_type = KeyboardInterrupt

    def query_history_k_data_plus(self, code, fields, **kwargs):
        raise self.interrupt_type


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_process_interrupt_is_not_converted_into_a_retry(interrupt_type) -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = InterruptingClient(payload)
    client.interrupt_type = interrupt_type
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    with pytest.raises(interrupt_type):
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert client.login_calls == 1


class LoginRecoveryClient(FakeBaoStock):
    def __init__(self, payload, outcomes) -> None:
        super().__init__(payload)
        self.context = SimpleNamespace(default_socket=None)
        self.outcomes = iter(outcomes)
        self.login_calls = 0
        self.login_default_timeouts = []
        self.sockets: list[FakeSocket] = []

    def login(self):
        self.login_calls += 1
        self.login_default_timeouts.append(socket.getdefaulttimeout())
        connection = FakeSocket()
        self.sockets.append(connection)
        self.context.default_socket = connection
        outcome = next(self.outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def test_initial_login_connect_and_recv_timeout_are_bounded_and_retried(
    monkeypatch,
) -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = LoginRecoveryClient(
        payload,
        [TimeoutError("synthetic login recv timeout"), FakeResult([], [])],
    )
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
        socket_timeout_seconds=13,
    )
    monkeypatch.setattr(provider, "_uses_real_baostock_socket", lambda: True)
    previous = socket.getdefaulttimeout()

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert batch.failed_symbols == []
    assert client.login_calls == 2
    assert client.login_default_timeouts == [13, 13]
    assert socket.getdefaulttimeout() == previous
    assert all(item.closed for item in client.sockets)


def test_provider_error_during_initial_login_is_retried_with_bound() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = LoginRecoveryClient(
        payload,
        [
            FakeResult([], [], error_code="100", error_msg="source unavailable"),
            FakeResult([], []),
        ],
    )
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert batch.failed_symbols == []
    assert client.login_calls == 2
    assert client.sockets[0].closed is True


def test_login_retries_stop_at_configured_attempt_limit() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = LoginRecoveryClient(
        payload,
        [TimeoutError("one"), TimeoutError("two"), FakeResult([], [])],
    )
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    with pytest.raises(Exception, match="login transport failed"):
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert client.login_calls == 2
    assert all(item.closed for item in client.sockets)


class ProgrammingErrorClient(RecoveringSocketClient):
    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.history_calls[code] = self.history_calls.get(code, 0) + 1
        raise AssertionError("provider contract bug")


def test_programming_error_fails_fast_without_retry_or_relogin() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = ProgrammingErrorClient(payload)
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    with pytest.raises(AssertionError, match="provider contract bug"):
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert client.history_calls == {"sh.600000": 1}
    assert client.login_calls == 1


def test_provider_installs_pinned_transport_only_for_real_client(monkeypatch) -> None:
    import backend.app.market.baostock as provider_module

    installed: list[str] = []
    monkeypatch.setattr(
        provider_module,
        "install_baostock_transport_patch",
        lambda: installed.append("installed"),
    )

    BaoStockProvider(client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())))
    assert installed == []

    BaoStockProvider()
    assert installed == ["installed"]


class ScopedBaoStock(FakeBaoStock):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.scopes = []

    def _capture(self, operation: str) -> None:
        self.scopes.append((operation, current_request_context()))

    def login(self):
        self._capture("login")
        return super().login()

    def query_trade_dates(self, **kwargs):
        self._capture("query_trade_dates")
        return super().query_trade_dates(**kwargs)

    def query_all_stock(self, **kwargs):
        self._capture("query_all_stock")
        return super().query_all_stock(**kwargs)

    def query_daily_history_k_AStock(self, **kwargs):
        self._capture("query_daily_history_k_AStock")
        return super().query_daily_history_k_AStock(**kwargs)

    def query_daily_adjust_factor(self, **kwargs):
        self._capture("query_daily_adjust_factor")
        return super().query_daily_adjust_factor(**kwargs)

    def query_adjust_factor(self, code, **kwargs):
        self._capture("query_adjust_factor")
        return super().query_adjust_factor(code, **kwargs)

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self._capture("query_history_k_data_plus")
        return super().query_history_k_data_plus(code, fields, **kwargs)


def test_public_fetch_tags_all_six_endpoints_with_one_refresh_and_session() -> None:
    client = ScopedBaoStock(json.loads(FIXTURE_PATH.read_text()))
    provider = BaoStockProvider(client=client, min_request_interval_seconds=0)

    provider.fetch(date(2026, 7, 23))

    endpoint_by_operation = {
        "query_trade_dates": ProviderEndpoint.TRADE_DATES,
        "query_all_stock": ProviderEndpoint.ALL_STOCK,
        "query_daily_history_k_AStock": ProviderEndpoint.DAILY_ASTOCK,
        "query_daily_adjust_factor": ProviderEndpoint.DAILY_FACTOR,
        "query_adjust_factor": ProviderEndpoint.ADJUST_FACTOR,
        "query_history_k_data_plus": ProviderEndpoint.INDEX_HISTORY,
    }
    query_scopes = [item for item in client.scopes if item[0] != "login"]
    assert {operation for operation, _context in query_scopes} == set(endpoint_by_operation)
    assert all(
        context.endpoint == endpoint_by_operation[operation] for operation, context in query_scopes
    )
    assert len({context.refresh_id for _operation, context in client.scopes}) == 1
    assert len({context.provider_session_id for _operation, context in client.scopes}) == 1
    assert len({context.request_id for _operation, context in query_scopes}) == len(query_scopes)
    assert {context.attempt for _operation, context in query_scopes} == {1}
    assert {context.page for _operation, context in query_scopes} == {1}
    login_context = next(context for operation, context in client.scopes if operation == "login")
    assert login_context.endpoint == ProviderEndpoint.TRADE_DATES
    assert (login_context.attempt, login_context.page) == (1, 1)
    assert login_context.request_id not in {
        context.request_id for _operation, context in query_scopes
    }


def test_each_public_operation_owns_one_fresh_refresh_scope() -> None:
    client = ScopedBaoStock(json.loads(FIXTURE_PATH.read_text()))
    provider = BaoStockProvider(client=client, min_request_interval_seconds=0)
    operation_scopes = []

    for operation in (
        lambda: provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23)),
        lambda: provider.inspect_main_board(date(2026, 7, 23)),
        lambda: provider.fetch_range(
            date(2026, 7, 23),
            date(2026, 7, 23),
            symbols=["sh.600000"],
        ),
    ):
        start = len(client.scopes)
        operation()
        current = [context for _name, context in client.scopes[start:]]
        assert len({context.refresh_id for context in current}) == 1
        assert len({context.provider_session_id for context in current}) == 1
        operation_scopes.append(current)

    assert len({scopes[0].refresh_id for scopes in operation_scopes}) == 3
    assert len({scopes[0].provider_session_id for scopes in operation_scopes}) == 3


class ScopedRetryClient(RecoveringSocketClient):
    def __init__(self, payload) -> None:
        super().__init__(payload)
        self.login_scopes = []
        self.query_scopes = []

    def login(self):
        self.login_scopes.append(current_request_context())
        return super().login()

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.query_scopes.append(current_request_context())
        return super().query_history_k_data_plus(code, fields, **kwargs)


def test_retry_keeps_refresh_id_but_rotates_session_and_request_ids() -> None:
    client = ScopedRetryClient(json.loads(FIXTURE_PATH.read_text()))
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    history_scopes = [
        context
        for context in client.query_scopes
        if context.endpoint == ProviderEndpoint.INDEX_HISTORY
    ]
    assert [context.attempt for context in history_scopes] == [1, 2]
    assert len({context.refresh_id for context in client.login_scopes + history_scopes}) == 1
    assert len({context.provider_session_id for context in client.login_scopes}) == 2
    assert history_scopes[0].provider_session_id != history_scopes[1].provider_session_id
    assert history_scopes[0].request_id != history_scopes[1].request_id
    assert [context.endpoint for context in client.login_scopes] == [
        ProviderEndpoint.TRADE_DATES,
        ProviderEndpoint.INDEX_HISTORY,
    ]


class ProtocolPageResult:
    fields = ["code"]
    error_code = "0"
    error_msg = ""
    per_page_count = 2000

    def __init__(
        self,
        pages: list[list[list[str]]],
        *,
        page_numbers: list[str] | None = None,
        fail_on_transition: bool = False,
    ) -> None:
        self.pages = pages
        self.page_numbers = page_numbers or [str(index + 1) for index in range(len(pages))]
        self.fail_on_transition = fail_on_transition
        self.page_index = 0
        self.data = pages[0]
        self.cur_page_num = self.page_numbers[0]
        self.cur_row_num = 0
        self.transition_scopes = []

    def next(self) -> bool:
        if self.cur_row_num < len(self.data):
            return True
        if len(self.data) < self.per_page_count:
            return False
        self.transition_scopes.append(current_request_context())
        if self.fail_on_transition:
            raise TimeoutError("private-token page two transport failure")
        self.page_index += 1
        if self.page_index >= len(self.pages):
            return False
        self.data = self.pages[self.page_index]
        self.cur_page_num = self.page_numbers[self.page_index]
        self.cur_row_num = 0
        return bool(self.data)

    def get_row_data(self) -> list[str]:
        row = self.data[self.cur_row_num]
        self.cur_row_num += 1
        return row


def _full_page(prefix: str = "row") -> list[list[str]]:
    return [[f"{prefix}-{index:04d}"] for index in range(2000)]


def test_pagination_advances_page_scope_and_request_id() -> None:
    client = ScopedBaoStock(json.loads(FIXTURE_PATH.read_text()))
    provider = BaoStockProvider(client=client, max_attempts=1, min_request_interval_seconds=0)
    result = ProtocolPageResult([_full_page(), [["last-row"]]])
    initial_scopes = []

    def operation():
        initial_scopes.append(current_request_context())
        return result

    fields, rows = provider._read(ProviderEndpoint.ALL_STOCK, operation)

    assert fields == ["code"]
    assert len(rows) == 2001
    assert len(initial_scopes) == 1
    assert len(result.transition_scopes) == 1
    first = initial_scopes[0]
    second = result.transition_scopes[0]
    assert (first.attempt, first.page) == (1, 1)
    assert (second.attempt, second.page) == (1, 2)
    assert second.refresh_id == first.refresh_id
    assert second.provider_session_id == first.provider_session_id
    assert second.request_id != first.request_id


def test_pagination_accepts_three_advancing_unique_pages() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    result = ProtocolPageResult([_full_page("first"), _full_page("second"), [["last-row"]]])

    _fields, rows = provider._read(ProviderEndpoint.ALL_STOCK, lambda: result)

    assert len(rows) == 4001
    assert [context.page for context in result.transition_scopes] == [2, 3]


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(
            ProtocolPageResult([_full_page(), []]),
            id="exact-full-page-followed-by-empty-page",
        ),
        pytest.param(
            ProtocolPageResult([_full_page()]),
            id="exact-full-page-with-missing-next-page",
        ),
        pytest.param(
            ProtocolPageResult([_full_page(), _full_page("next")], page_numbers=["1", "1"]),
            id="stalled-page-number",
        ),
        pytest.param(
            ProtocolPageResult([_full_page(), _full_page()], page_numbers=["1", "2"]),
            id="repeated-page-data",
        ),
        pytest.param(
            ProtocolPageResult([_full_page(), [["last-row"]]], page_numbers=["invalid", "2"]),
            id="abnormal-page-number",
        ),
    ],
)
def test_pagination_protocol_gaps_fail_the_complete_read(result: ProtocolPageResult) -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError, match="pagination protocol"):
        provider._read(ProviderEndpoint.ALL_STOCK, lambda: result)


def test_page_two_transport_failure_never_returns_first_page_rows() -> None:
    provider = BaoStockProvider(
        client=FakeBaoStock(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    result = ProtocolPageResult([_full_page()], fail_on_transition=True)

    with pytest.raises(BaoStockError) as exc_info:
        provider._read(ProviderEndpoint.ALL_STOCK, lambda: result)

    assert len(result.transition_scopes) == 1
    assert result.transition_scopes[0].page == 2
    assert "private-token" not in str(exc_info.value)


def test_exact_full_universe_page_cannot_be_returned_as_partial_candidate() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class PrematureUniverseClient(FakeBaoStock):
        def query_all_stock(self, **kwargs):
            return ProtocolPageResult([_full_page("sh.601"), []])

    provider = BaoStockProvider(
        client=PrematureUniverseClient(payload),
        max_attempts=1,
        min_request_interval_seconds=0,
    )

    with pytest.raises(BaoStockError, match="pagination protocol"):
        provider.fetch(date(2026, 7, 23))


def test_duplicate_universe_symbol_fails_the_complete_candidate() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class DuplicateUniverseClient(FakeBaoStock):
        def query_all_stock(self, **kwargs):
            result = super().query_all_stock(**kwargs)
            rows = list(result.rows)
            return FakeResult(result.fields, rows + [rows[0]])

    provider = BaoStockProvider(client=DuplicateUniverseClient(payload))

    with pytest.raises(BaoStockError, match="duplicate symbols"):
        provider.fetch(date(2026, 7, 23))


def test_duplicate_daily_symbol_fails_the_complete_candidate() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class DuplicateDailyClient(FakeBaoStock):
        def query_all_stock(self, **kwargs):
            result = super().query_all_stock(**kwargs)
            rows = list(result.rows)
            unique = list({row[0]: row for row in rows}.values())
            return FakeResult(result.fields, unique)

        def query_daily_history_k_AStock(self, **kwargs):
            rows = self.payload["daily_rows"]
            return FakeResult(self.payload["daily_fields"], rows + [rows[0]])

    provider = BaoStockProvider(client=DuplicateDailyClient(payload))

    with pytest.raises(BaoStockError, match="duplicate symbols"):
        provider.fetch(date(2026, 7, 23))


@pytest.mark.parametrize("divergence", ["missing", "unexpected"])
def test_all_stock_daily_universe_divergence_fails_candidate(divergence: str) -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class DivergentUniverseClient(FakeBaoStock):
        def query_all_stock(self, **kwargs):
            result = super().query_all_stock(**kwargs)
            rows = list(result.rows)
            if divergence == "missing":
                rows.append(["sh.600099", "1", "sh.600099"])
            else:
                rows = [row for row in rows if row[0] != "sh.600000"]
            return FakeResult(result.fields, rows)

    provider = BaoStockProvider(client=DivergentUniverseClient(payload))

    with pytest.raises(BaoStockError, match="universe divergence"):
        provider.fetch(date(2026, 7, 23))


def test_duplicate_explicit_symbol_rows_fail_instead_of_returning_partial_batch() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class DuplicateExplicitClient(FakeBaoStock):
        def query_history_k_data_plus(self, code, fields, **kwargs):
            result = super().query_history_k_data_plus(code, fields, **kwargs)
            rows = list(result.rows)
            return FakeResult(result.fields, rows + rows)

    provider = BaoStockProvider(client=DuplicateExplicitClient(payload))

    with pytest.raises(BaoStockError, match="duplicate symbols"):
        provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])
