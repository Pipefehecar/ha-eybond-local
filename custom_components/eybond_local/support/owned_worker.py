"""Own a blocking worker through cancellation, without pretending it stopped."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any, Callable


async def async_run_owned_worker(callable_: Callable[..., Any], **kwargs: Any) -> Any:
    """Drain the exact worker, then propagate cancellation or its normal result.

    Cancelling to_thread cannot cancel the filesystem/HTTP operation underneath.
    If cancellation was requested, even a subsequent worker failure must not
    turn cancellation into an ordinary recoverable error in the caller.
    """
    task = asyncio.create_task(asyncio.to_thread(callable_, **kwargs))
    return await async_finish_owned_task(task)


async def async_finish_owned_task(task: asyncio.Task[Any]) -> Any:
    """Join an already-owned task through repeated waiter cancellation.

    Intended for mandatory finalization as well as non-cancellable workers;
    callers must explicitly cancel long-lived pumps before joining them.
    """

    pending_cancel: asyncio.CancelledError | None = None
    while not task.done():
        try:
            # wait() does not cancel the owned task when its waiter is cancelled.
            # Unlike repeatedly shielding the worker itself, this leaves one
            # result consumer: Python 3.14's shield otherwise reports a late
            # worker error from an abandoned shield as an uncaught exception.
            await asyncio.wait((task,))
        except asyncio.CancelledError as exc:
            if pending_cancel is None:
                pending_cancel = exc
    if pending_cancel is not None:
        with suppress(Exception, asyncio.CancelledError):
            task.result()
        raise pending_cancel
    return task.result()
