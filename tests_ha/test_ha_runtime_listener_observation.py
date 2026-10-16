"""Configured callback ports remain in the domain inventory for entry lifetime."""

import asyncio
import socket

import pytest

from test_ha_setup_unload import _collector_entry
from custom_components.eybond_local.passive_discovery import get_passive_callback_discovery


@pytest.fixture
def real_runtime_listener_observation():
    """Opt into real collector sockets even with the fake payload runtime."""


@pytest.mark.parametrize("setup_failure", [False, True])
async def test_custom_port_observation_lifetime(
    hass, fake_runtime, socket_enabled, real_runtime_listener_observation, monkeypatch, setup_failure,
):
    from conftest import FakeRuntimeManager

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    entry = _collector_entry(hass)
    hass.config_entries.async_update_entry(entry, options={**entry.options, "tcp_port": port})
    if setup_failure:
        async def fail_start(self):
            raise RuntimeError("synthetic_start_failure")
        monkeypatch.setattr(FakeRuntimeManager, "async_start", fail_start)

    assert await hass.config_entries.async_setup(entry.entry_id) is not setup_failure
    discovery = get_passive_callback_discovery(hass)
    assert discovery is not None
    if setup_failure:
        assert not discovery._ensured_listeners
        return

    coordinator = entry.runtime_data
    observation = coordinator._runtime_listener_observation
    await coordinator._async_observe_runtime_listener()
    assert coordinator._runtime_listener_observation == observation
    assert len(discovery._ensured_listeners) == 1
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        from custom_components.eybond_local.collector.protocol import build_collector_request
        writer.write(build_collector_request(
            1, b"E5000099990001", devcode=1, collector_addr=1, fcode=1,
        ))
        await writer.drain()
        async with asyncio.timeout(3):
            while not discovery.iter_observed_sessions():
                await asyncio.sleep(.01)
        assert {s["listener_port"] for s in discovery.iter_observed_sessions()} == {port}
        assert await hass.config_entries.async_unload(entry.entry_id)
        assert not discovery._ensured_listeners
        assert not discovery.iter_observed_sessions()
        await coordinator._async_release_runtime_listener_observation()
    finally:
        writer.close()
        await writer.wait_closed()
        await coordinator.async_shutdown()
