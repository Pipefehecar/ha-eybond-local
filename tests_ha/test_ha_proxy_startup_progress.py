"""Bounded proxy startup diagnostics without a live collector or cloud."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from custom_components.eybond_local.runtime.coordinator import cloud_tools


def _startup(monkeypatch, statuses):
    clock = [0.0]
    samples = iter(statuses)
    current = {}

    def inspect(_path):
        nonlocal current
        current = next(samples, current)
        return current

    async def sleep(seconds):
        clock[0] += seconds

    async def executor(func):
        return func()

    monkeypatch.setattr(cloud_tools, "inspect_proxy_capture_start_status", inspect)
    monkeypatch.setattr(
        cloud_tools, "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: clock[0]),
            sleep=sleep,
        ),
    )
    owner = SimpleNamespace(
        hass=SimpleNamespace(async_add_executor_job=executor),
        config_entry=SimpleNamespace(entry_id="synthetic-proxy-entry"),
        _proxy_capture_process_running=lambda: True,
    )
    return owner, clock


async def _wait(owner):
    await cloud_tools.CoordinatorCloudToolsMixin._async_wait_for_proxy_capture_reconnect(
        owner, Path("unused.jsonl"),
    )


async def test_ready_transition_reports_states_without_changing_deadline(monkeypatch, caplog):
    owner, clock = _startup(monkeypatch, [
        {},
        {"connected": True, "identity_verified": True},
        {"connected": True, "identity_verified": True, "operational_activity": True},
    ])
    with caplog.at_level(logging.DEBUG, logger=cloud_tools.__name__):
        await _wait(owner)
    assert clock[0] == 2.0
    assert "connected=False identity_verified=False operational_activity=False" in caplog.text
    assert "connected=True identity_verified=True operational_activity=True" in caplog.text
    assert "timed out" not in caplog.text


@pytest.mark.parametrize("verified", [False, True])
async def test_timeout_retains_reason_and_logs_bounded_progress(monkeypatch, caplog, verified):
    owner, clock = _startup(monkeypatch, [
        {"connected": True, "identity_verified": verified},
    ])
    reason = "cloud_session_not_ready" if verified else "collector_identity_timeout"
    with caplog.at_level(logging.DEBUG, logger=cloud_tools.__name__):
        with pytest.raises(TimeoutError, match=f"proxy_capture_{reason}"):
            await _wait(owner)
    assert clock[0] == 75.0
    messages = [record.getMessage() for record in caplog.records if record.name == cloud_tools.__name__]
    assert len(messages) == 6  # t=0/15/30/45/60 plus final warning, not every second.
    assert "timed out" in messages[-1]
    assert f"identity_verified={verified}" in messages[-1]
    assert "operational_activity=False upstream_error=False" in messages[-1]


@pytest.mark.parametrize(("status", "reason"), [
    ({"upstream_error": "synthetic-connect-error"}, "proxy_capture_upstream_connect_failed"),
    ({"identity_mismatch": True}, "proxy_capture_collector_identity_mismatch"),
])
async def test_terminal_errors_still_fail_without_waiting(monkeypatch, status, reason):
    owner, clock = _startup(monkeypatch, [status])
    with pytest.raises(RuntimeError, match=reason):
        await _wait(owner)
    assert clock[0] == 0.0


async def test_cancellation_is_not_reported_as_a_timeout(monkeypatch, caplog):
    owner, _clock = _startup(monkeypatch, [{}])

    async def cancelled(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(cloud_tools.asyncio, "sleep", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await _wait(owner)
    assert "timed out" not in caplog.text
