"""Issue #62: first-install callback, edit/retry and occupied listener."""

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.data_entry_flow import FlowResultType

from custom_components.eybond_local.connection import callback_identity as identity
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.passive_discovery import get_callback_session_registry
from test_ha_remote_scan import _advanced_setup


def settings(port):
    return {
        "server_ip": "127.0.0.1", "collector_ip": "127.0.0.1", "driver_hint": "auto",
        "connection_strategy": "callback_on_demand",
        "advanced_connection": {
            "tcp_port": port, "udp_port": 58899,
            "advertised_server_ip": "203.0.113.10", "advertised_tcp_port": "18899",
            "discovery_target": "192.0.2.255", "discovery_interval": 3,
            "heartbeat_interval": 60,
        },
    }


async def manual_flow(hass):
    flow_id, flow = await _advanced_setup(hass)
    flow._local_ip = "127.0.0.1"
    flow._interface_options = [{"ip": "127.0.0.1", "name": "lo", "label": "loopback", "network": "127.0.0.0/8", "broadcast": "127.255.255.255"}]
    result = await hass.config_entries.flow.async_configure(flow_id, {"next_step_id": "manual"})
    assert result["step_id"] == "manual"
    return flow_id, flow


async def test_edit_settings_replaces_closed_attempt_and_uses_new_route(hass):
    flow_id, flow = await manual_flow(hass)
    transaction = AsyncMock(return_value=identity.CallbackIdentityOutcome(result="callback_timeout"))
    try:
        with patch("custom_components.eybond_local.connection.admission_transaction.async_run_callback_identity_transaction", transaction):
            await hass.config_entries.flow.async_configure(flow_id, settings(8899))
            previous = flow._callback_continuation
            result = await hass.config_entries.flow.async_configure(flow_id, {"next_step_id": "manual_edit_settings"})
            assert result["step_id"] == "manual"
            result = await hass.config_entries.flow.async_configure(flow_id, settings(18899))
        assert transaction.await_count == 2
        assert flow._callback_continuation is not previous
        request = transaction.call_args.args[1]
        assert request.tcp_port == 18899
        assert request.advertised_server_ip == "203.0.113.10"
        assert not request.expected_pn
        assert result["type"] is not FlowResultType.CREATE_ENTRY
    finally:
        hass.config_entries.flow.async_abort(flow_id)


async def test_first_install_manual_callback_binds_selected_port_and_sends_advertised_payload(
    hass, socket_enabled, monkeypatch,
):
    assert get_callback_session_registry(hass) is None
    flow_id, flow = await manual_flow(hass)
    packets = []
    class Receiver(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport
        def datagram_received(self, data, addr):
            packets.append(data)
            self.transport.sendto(b"rsp>server=1;", addr)
    udp, _ = await asyncio.get_running_loop().create_datagram_endpoint(Receiver, local_addr=("127.0.0.1", 0))
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    values = settings(port)
    values["advanced_connection"]["udp_port"] = udp.get_extra_info("sockname")[1]
    monkeypatch.setattr(identity, "DEFAULT_ONBOARDING_TIMEOUT_POLICY", replace(
        identity.DEFAULT_ONBOARDING_TIMEOUT_POLICY, callback_identity_session_wait=0.05, discovery_timeout=0.1,
    ))
    bound = []
    send = identity._ProductionTriggerSender.async_send
    async def inspect_then_send(self, request):
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        bound.append(port)
        writer.close()
        await writer.wait_closed()
        await send(self, request)
    monkeypatch.setattr(identity._ProductionTriggerSender, "async_send", inspect_then_send)
    try:
        result = await hass.config_entries.flow.async_configure(flow_id, values)
        assert bound == [port]
        assert packets == [b"set>server=203.0.113.10:18899;"]
        assert get_callback_session_registry(hass) is not None
        assert result["type"] is not FlowResultType.CREATE_ENTRY
        assert not flow._callback_continuation.certified_pn
    finally:
        udp.close()
        hass.config_entries.flow.async_abort(flow_id)


async def test_busy_listener_does_not_trigger_collector_and_retry_on_free_port_works(
    hass, socket_enabled, monkeypatch,
):
    from custom_components.eybond_local.passive_discovery import async_start_passive_callback_discovery
    from custom_components.eybond_local.connection.callback_ledger import get_callback_trigger_ledger
    await async_start_passive_callback_discovery(hass)
    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen()
    busy_port = held.getsockname()[1]
    sent = []
    class Sender:
        async def async_send(self, request):
            sent.append(request.tcp_port)
            get_callback_trigger_ledger().record(target=request.target_ip, source="test")
    request = identity.CallbackIdentityRequest(
        server_ip="127.0.0.1", tcp_port=busy_port, udp_port=58899,
        target_ip="192.0.2.7", session_wait_timeout=0.01,
    )
    try:
        result = await identity.async_run_callback_identity_transaction(hass, request, sender=Sender())
        assert result.result == "callback_listener_unavailable"
        assert sent == []
    finally:
        held.close()
    # Same port is now available: a failed acquisition must not poison its cache.
    result = await identity.async_run_callback_identity_transaction(hass, request, sender=Sender())
    assert sent == [busy_port]
    assert result.result == "callback_timeout"


@pytest.mark.timeout(15)
async def test_busy_default_port_does_not_prevent_identity_on_alternate_port(
    hass, socket_enabled, monkeypatch, no_passive_discovery_listeners,
):
    """Real passive service, TCP identity and UDP callback after a bind failure.

    Substitute isolated loopback ports for the usual 8899/18899 roles; never
    bind the workstation's live collector endpoints during a test.
    """
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "tests" / "helpers"))
    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario
    from custom_components.eybond_local import passive_discovery as passive

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen()
    busy_port = held.getsockname()[1]
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        alternate_port = reserve.getsockname()[1]
    monkeypatch.setattr(passive, "_LISTENER_HOST", "127.0.0.1")
    monkeypatch.setattr(passive, "DEFAULT_TCP_PORT", busy_port)
    monkeypatch.setattr(passive, "LEGACY_BINARY_COLLECTOR_SERVER_PORT", busy_port)
    monkeypatch.setattr(passive, "DEFAULT_COLLECTOR_SERVER_PORT", alternate_port)
    pn = "V001020SYN62344022"
    collector = FakeCollectorService(
        listen_ip="127.0.0.1", udp_port=0, heartbeat_interval=1,
        connect_timeout=2, tcp_bind_ip="127.0.0.1", udp_reply="",
        scenario=resolve_scenario(preset="collector_only", profile=CollectorProfile(pn=pn)),
    )
    real_start, real_stop = no_passive_discovery_listeners
    monkeypatch.setattr(passive.PassiveCallbackDiscovery, "async_start", real_start)
    monkeypatch.setattr(passive.PassiveCallbackDiscovery, "async_stop", real_stop)
    service = None
    flow_id = None
    try:
        await collector.start()
        flow_id, flow = await manual_flow(hass)
        values = settings(busy_port)
        values["advanced_connection"].update(
            udp_port=collector._udp_transport.get_extra_info("sockname")[1],
            advertised_server_ip="127.0.0.1", advertised_tcp_port=str(busy_port),
        )
        result = await hass.config_entries.flow.async_configure(flow_id, values)
        assert flow._manual_result.last_error == "callback_listener_unavailable"
        service = passive.get_passive_callback_discovery(hass)
        assert busy_port not in service._listeners
        assert alternate_port in service._listeners
        await hass.config_entries.flow.async_configure(
            flow_id, {"next_step_id": "manual_edit_settings"},
        )
        values["advanced_connection"].update(
            tcp_port=alternate_port, advertised_tcp_port=str(alternate_port),
        )
        result = await hass.config_entries.flow.async_configure(flow_id, values)
        assert result["step_id"] == "manual_recovery_confirm"
        assert flow._callback_continuation.certified_pn == pn
        assert any(
            item["listener_port"] == alternate_port
            for item in service.iter_observed_sessions()
        )
        assert not hass.config_entries.async_entries(DOMAIN)
    finally:
        if flow_id is not None:
            hass.config_entries.flow.async_abort(flow_id)
        await collector.stop()
        if service is not None:
            await real_stop(service)
        held.close()


async def test_first_install_setup_failure_is_actionable_without_udp(hass):
    flow_id, flow = await manual_flow(hass)
    try:
        with patch("homeassistant.setup.async_setup_component", new=AsyncMock(return_value=False)), patch.object(
            identity._ProductionTriggerSender, "async_send", new=AsyncMock(),
        ) as send:
            result = await hass.config_entries.flow.async_configure(flow_id, settings(18899))
        assert result["step_id"] == "manual_confirm"
        assert "manual_edit_settings" in result["menu_options"]
        assert flow._manual_result.last_error == "callback_listener_unavailable"
        assert "could not start" in result["description_placeholders"]["probe_summary"]
        send.assert_not_awaited()
        assert not hass.config_entries.async_entries(DOMAIN)
    finally:
        hass.config_entries.flow.async_abort(flow_id)
