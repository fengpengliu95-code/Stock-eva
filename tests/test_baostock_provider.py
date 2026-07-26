import json
import socket
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.market.baostock import BaoStockProvider

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
    assert batch.bars[0].adjust_factor == 1.25
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
