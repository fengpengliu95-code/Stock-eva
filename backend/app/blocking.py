"""Cancellation-safe adapters for bounded blocking operations."""

from __future__ import annotations

import asyncio
from collections.abc import Callable


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
