"""Cancellation-safe adapters for bounded blocking operations."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from enum import StrEnum


class BlockingOperation(StrEnum):
    MARKET_REFRESH = "market_refresh"
    REPLICATION_DRAIN = "replication_drain"
    CALENDAR_INITIALIZE = "calendar_initialize"
    CALENDAR_MAINTENANCE = "calendar_maintenance"
    CALENDAR_SYNC = "calendar_sync"


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


async def run_blocking_operation[**P, R](
    _operation: BlockingOperation,
    function: Callable[P, R],
    *args: P.args,
    scheduled_at=None,
    **kwargs: P.kwargs,
) -> R:
    """Default explicit-operation adapter retains the drained thread behavior."""
    return await run_blocking_drained(function, *args, **kwargs)
