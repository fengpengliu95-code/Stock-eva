"""Cancellation-safe adapters for bounded blocking operations."""

from __future__ import annotations

import asyncio
import multiprocessing
import pickle
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


class BlockingProcessError(RuntimeError):
    """A blocking child failed with an exception that could not cross IPC safely."""


@dataclass(frozen=True)
class _ChildFailure:
    module: str
    name: str
    message: str
    traceback: str


def _process_entry(connection, function, args, kwargs) -> None:
    """Run one inherited callable in the child process's main thread."""
    try:
        try:
            payload: tuple[str, Any] = ("result", function(*args, **kwargs))
            pickle.dumps(payload)
        except BaseException as error:
            try:
                pickle.dumps(error)
                payload = ("exception", error)
            except Exception:
                payload = (
                    "failure",
                    _ChildFailure(
                        module=type(error).__module__,
                        name=type(error).__qualname__,
                        message=str(error),
                        traceback="".join(traceback.format_exception(error)),
                    ),
                )
        connection.send(payload)
    finally:
        connection.close()


async def run_blocking_drained[**P, R](
    function: Callable[P, R], *args: P.args, **kwargs: P.kwargs
) -> R:
    """Do not return cancellation until an already-started worker has finished."""
    worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        try:
            await worker
        except Exception:
            pass
        raise


async def run_blocking_process[**P, R](
    function: Callable[P, R],
    *args: P.args,
    cancel_grace_seconds: float = 0.25,
    **kwargs: P.kwargs,
) -> R:
    """Run a blocking operation in a bounded fork child and reap it on every exit.

    The child is created synchronously by the event-loop thread, but all user code runs
    after the fork.  This leaves the ASGI loop responsive while making the operation's
    surviving thread the child main thread, as required by POSIX interval timers.
    """
    if cancel_grace_seconds < 0:
        raise ValueError("cancel grace must not be negative")
    try:
        context = multiprocessing.get_context("fork")
    except ValueError as error:
        raise RuntimeError("blocking process execution requires POSIX fork support") from error
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_process_entry,
        args=(send, function, args, kwargs),
        name="stock-eva-blocking-operation",
    )
    process.start()
    send.close()
    loop = asyncio.get_running_loop()
    ready = loop.create_future()

    def connection_ready() -> None:
        if not ready.done():
            ready.set_result(None)

    loop.add_reader(receive.fileno(), connection_ready)
    try:
        await ready
        try:
            kind, payload = receive.recv()
        except EOFError as error:
            raise BlockingProcessError(
                f"blocking child exited without a result (exit code {process.exitcode})"
            ) from error
        if kind == "result":
            return payload
        if kind == "exception":
            raise payload
        if kind == "failure" and isinstance(payload, _ChildFailure):
            raise BlockingProcessError(
                f"{payload.module}.{payload.name}: {payload.message}\n{payload.traceback}"
            )
        raise BlockingProcessError("blocking child returned an invalid response")
    except asyncio.CancelledError:
        if process.is_alive() and cancel_grace_seconds:
            await asyncio.to_thread(process.join, cancel_grace_seconds)
        if process.is_alive():
            process.terminate()
        raise
    finally:
        loop.remove_reader(receive.fileno())
        receive.close()
        if process.is_alive():
            process.terminate()
        await asyncio.to_thread(process.join, cancel_grace_seconds)
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join)
