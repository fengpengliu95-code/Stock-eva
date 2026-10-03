"""Portable subprocess boundary for scheduled BaoStock work.

Only an explicit command and validated settings cross the spawn boundary.  Runtime
services, BaoStock clients, sockets, locks, and database handles are constructed in
the child process instead of being inherited from a multi-threaded ASGI process.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import multiprocessing
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

from backend.app.blocking import run_blocking_drained
from backend.app.config import Settings

ProviderCommandKind = Literal["market", "calendar"]
TestScenario = Literal["success", "timeout", "crash", "unserializable_failure"]


@dataclass(frozen=True)
class ProviderProcessTestConfig:
    """Explicit local-only synthetic transport configuration used by regression tests."""

    scenario: TestScenario
    marker: Path
    release: Path | None = None
    socket_timeout_seconds: float = 0.1


@dataclass(frozen=True)
class ProviderProcessCommand:
    kind: ProviderCommandKind
    settings: dict[str, object]
    requested_at: datetime | None = None
    startup: bool = False
    test: ProviderProcessTestConfig | None = None


@dataclass(frozen=True)
class ProviderProcessEnvelope:
    status: Literal["ok", "busy", "failure"]
    exit_code: int | None = None
    failure_type: str | None = None
    failure_message: str | None = None


class ProviderProcessError(RuntimeError):
    """A scheduled provider child failed or violated its IPC contract."""

    def __init__(self, failure_type: str, message: str) -> None:
        self.failure_type = failure_type
        super().__init__(message)


class _SyntheticResult:
    error_code = "0"
    error_msg = "success"
    fields = ["calendar_date", "is_trading_day"]

    def __init__(self) -> None:
        self._rows = iter([["2026-07-23", "1"]])

    def next(self) -> bool:
        try:
            self._current = next(self._rows)
        except StopIteration:
            return False
        return True

    def get_row_data(self):
        return self._current


class _SyntheticSocketClient:
    def __init__(self, config: ProviderProcessTestConfig) -> None:
        self.config = config
        self.context = SimpleNamespace(default_socket=None)

    def login(self):
        return SimpleNamespace(error_code="0", error_msg="success")

    def logout(self):
        return None

    def query_trade_dates(self, **_kwargs):
        if self.config.scenario == "unserializable_failure":
            self.config.marker.write_text("in-flight")
            error = RuntimeError("synthetic unpickleable provider failure")
            error.callback = lambda: None
            raise error
        if self.config.scenario == "timeout":
            left, right = socket.socketpair()
            self.context.default_socket = left
            self.config.marker.write_text("in-flight")

            def release() -> None:
                if self.config.release is not None:
                    while not self.config.release.exists():
                        time.sleep(0.005)
                    with contextlib.suppress(OSError):
                        right.sendall(b"released")

            watcher = threading.Thread(target=release, daemon=True)
            watcher.start()
            try:
                left.recv(16)
            finally:
                left.close()
                right.close()
                watcher.join(timeout=0.2)
        return _SyntheticResult()


def _run_synthetic(command: ProviderProcessCommand) -> int:
    from backend.app.market.automation import RefreshRunLock
    from backend.app.market.baostock import BaoStockProvider
    from backend.app.storage.layout import StorageLayout

    config = command.test
    if config is None:
        raise RuntimeError("synthetic provider configuration is missing")
    if config.scenario == "crash":
        config.marker.write_text("in-flight")
        os._exit(86)
    settings = Settings.model_validate(command.settings)
    layout = StorageLayout(settings)
    layout.ensure_local_runtime_dirs()
    with RefreshRunLock(layout.market_refresh_lock):
        provider = BaoStockProvider(
            client=_SyntheticSocketClient(config),
            max_attempts=1,
            min_request_interval_seconds=0,
            socket_timeout_seconds=config.socket_timeout_seconds,
        )
        dates = provider.trading_dates(date(2026, 7, 23), date(2026, 7, 23))
    if dates != [date(2026, 7, 23)]:
        raise RuntimeError("synthetic provider returned unexpected dates")
    config.marker.with_name(f"{config.marker.name}-{command.kind}").write_text("complete")
    return 0


def _run_cli(command: ProviderProcessCommand) -> tuple[int, dict[str, object]]:
    from backend.app import cli

    settings = Settings.model_validate(command.settings)
    if command.kind == "market":
        # Replication remains in the parent scheduler's drained thread boundary.
        settings = settings.model_copy(update={"replication_drain_enabled": False})
    cli.get_settings = lambda: settings
    argv = ["stock-eva"]
    if command.kind == "market":
        argv.extend(("auto-refresh-once", "--execute"))
    else:
        argv.extend(("calendar-sync", "--execute"))
        if command.startup:
            argv.append("--startup")
    previous = sys.argv
    sys.argv = argv
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
            exit_code = cli.main()
        try:
            payload = json.loads(output.getvalue().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            payload = {}
        return exit_code, payload if isinstance(payload, dict) else {}
    finally:
        sys.argv = previous


def _provider_process_entry(send, command: ProviderProcessCommand) -> None:
    """Module-level spawn target; never receives callables or runtime objects."""
    try:
        if command.test is not None:
            exit_code, payload = _run_synthetic(command), {}
        else:
            exit_code, payload = _run_cli(command)
        if exit_code == 0:
            envelope = ProviderProcessEnvelope(status="ok", exit_code=exit_code)
        elif "refresh_already_running" in payload.get("quality_issues", []):
            envelope = ProviderProcessEnvelope(status="busy", exit_code=exit_code)
        else:
            quality_issues = payload.get("quality_issues", [])
            fallback_type = (
                str(quality_issues[0])
                if isinstance(quality_issues, list) and quality_issues
                else "CommandExit"
            )
            envelope = ProviderProcessEnvelope(
                status="failure",
                exit_code=exit_code,
                failure_type=str(
                    payload.get("error_code") or payload.get("reason_code") or fallback_type
                )[:128],
                failure_message="scheduled provider command failed",
            )
    except BaseException as error:
        envelope = (
            ProviderProcessEnvelope(status="busy")
            if type(error).__qualname__ == "RefreshAlreadyRunning"
            else ProviderProcessEnvelope(
                status="failure",
                failure_type=f"{type(error).__module__}.{type(error).__qualname__}",
                failure_message=str(error)[:512],
            )
        )
    try:
        send.send(envelope)
    finally:
        send.close()


async def run_provider_process(
    command: ProviderProcessCommand,
    *,
    cancel_grace_seconds: float = 0.25,
) -> ProviderProcessEnvelope:
    """Spawn one provider command and reap it on success, failure, crash, or cancellation."""
    if cancel_grace_seconds < 0:
        raise ValueError("cancel grace must not be negative")
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_provider_process_entry,
        args=(send, command),
        name=f"stock-eva-provider-{command.kind}",
    )
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    reader_registered = False
    started = False

    def connection_ready() -> None:
        if not ready.done():
            ready.set_result(None)

    def reap() -> None:
        """Synchronously guarantee a bounded reap even while the task is cancelled."""
        if process.pid is None:
            return
        process.join(timeout=cancel_grace_seconds)
        if process.is_alive():
            process.terminate()
            process.join(timeout=cancel_grace_seconds)
        if process.is_alive():
            process.kill()
            process.join()

    try:
        process.start()
        started = True
        send.close()
        loop.add_reader(receive.fileno(), connection_ready)
        reader_registered = True
        await ready
        try:
            envelope = receive.recv()
        except EOFError as error:
            process.join(timeout=cancel_grace_seconds)
            raise ProviderProcessError(
                "ChildEOF",
                f"provider child exited without a result (exit code {process.exitcode})",
            ) from error
        if not isinstance(envelope, ProviderProcessEnvelope):
            raise ProviderProcessError(
                "InvalidEnvelope", "provider child returned invalid IPC data"
            )
        if envelope.status == "failure":
            raise ProviderProcessError(
                envelope.failure_type or "ProviderFailure",
                envelope.failure_message or "scheduled provider command failed",
            )
        return envelope
    finally:
        if reader_registered:
            loop.remove_reader(receive.fileno())
        receive.close()
        if not started:
            send.close()
        if started or process.pid is not None:
            reap()


@dataclass
class ProviderProcessRunner:
    """Adapter from existing scheduler callbacks to explicit provider commands."""

    kind: ProviderCommandKind
    settings: Settings
    test: ProviderProcessTestConfig | None = None
    startup: bool = True

    async def __call__(self, function, *args, **kwargs):
        name = getattr(function, "__name__", "")
        if self.kind == "market" and name != "run_due_once":
            return await run_blocking_drained(function, *args, **kwargs)
        if self.kind == "calendar" and name == "initialize_for_execution":
            return await run_blocking_drained(function, *args, **kwargs)
        envelope = await run_provider_process(
            ProviderProcessCommand(
                kind=self.kind,
                settings=self.settings.model_dump(mode="python"),
                requested_at=next((item for item in args if isinstance(item, datetime)), None),
                startup=self.startup,
                test=self.test,
            )
        )
        if envelope.status == "busy":
            return None
        self.startup = False
        if self.kind == "calendar" and name == "execute":
            return envelope
        if self.kind == "calendar":
            # The CLI command performed runtime maintenance and legacy calendar sync as
            # one lock-owned unit.  Suppress duplicate in-process planning this iteration.
            return SimpleNamespace(outcome="PROVIDER_PROCESS_COMPLETE")
        return envelope
