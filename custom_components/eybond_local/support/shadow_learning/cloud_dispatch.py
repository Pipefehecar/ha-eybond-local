"""Cancellation-safe cloud-action dispatch for shadow learning."""

from __future__ import annotations

from typing import Any, Callable

from ..owned_worker import async_run_owned_worker


async def async_dispatch_cloud_action(
    callable_: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    """Keep one blocking cloud action owned until its terminal outcome.

    Cancelling ``asyncio.to_thread`` does not stop its underlying HTTP request.
    Repeated cancellation is therefore deferred until that exact worker has
    completed, keeping the fail-closed shadow route active in the meantime.
    """

    return await async_run_owned_worker(callable_, **kwargs)


async def async_confirm_owned_action_not_sent(callable_, action_kwargs) -> bool:
    """Provider boundary: only exact clients with an owned HTTP resource qualify."""
    from ...cloud_http_session import CloudHttpSession
    from ...smartess_cloud import fetch_signed_action
    from ...dessmonitor_cloud import send_device_control

    http = action_kwargs.get("http_session")
    if type(http) is not CloudHttpSession or callable_ not in (fetch_signed_action, send_device_control):
        return False
    return await async_run_owned_worker(http.close_and_confirm_no_request)


__all__ = ["async_dispatch_cloud_action", "async_confirm_owned_action_not_sent"]
