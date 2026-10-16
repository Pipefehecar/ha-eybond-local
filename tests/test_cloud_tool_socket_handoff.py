"""A cloud tool drains owned physical siblings, not just the selected facade."""

from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from custom_components.eybond_local.connection.session_registry import CallbackSessionRegistry
from custom_components.eybond_local.collector.transport.shared_framed import SharedEybondTransport
from custom_components.eybond_local.runtime.link.connection import LinkConnectionMixin


PN = "V001020SYN62344022"
OTHER = "V000405SYN94677058"


class CloudToolSocketHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, owner_changes=False):
        def record(sid, pn=PN, source="fc2_parameter_2", port=8899):
            return dict(session_id=sid, collector_pn=pn, peer_ip="192.0.2.10",
                        listener_port=port, protocol_shape="eybond_framed",
                        collector_identity_source=source, state="routed_framed")

        records = [record("primary"), record("callback"), record("foreign", OTHER),
                   record("weak", source="framed_heartbeat"), record("other-port", port=18899)]
        registry = CallbackSessionRegistry(sessions_source=lambda: records)
        registry.claim_identity("entry", PN)
        registry.claim_identity("foreign-entry", OTHER)
        connections = {}
        closed = []

        async def close(sid):
            closed.append(sid)
            next(r for r in records if r["session_id"] == sid)["state"] = "closed_disconnected"
            if sid == "primary":
                records.append(record("new-arrival"))
                connections["new-arrival"] = SimpleNamespace(disconnect=AsyncMock())
                if owner_changes:
                    registry.release("entry")
                    registry.claim_identity("replacement", PN)

        for r in records:
            sid = r["session_id"]
            async def disconnect(sid=sid):
                await close(sid)
            connections[sid] = SimpleNamespace(disconnect=AsyncMock(side_effect=disconnect))

        transport = SharedEybondTransport(host="0.0.0.0", port=8899,
            request_timeout=1, heartbeat_interval=30, collector_ip="192.0.2.10", collector_pn=PN)
        transport._listener = SimpleNamespace(
            payload_connection_for_session=connections.get,
            at_connection_for_session=lambda sid: None,
        )
        link = LinkConnectionMixin()
        link._route_lease = SimpleNamespace(entry_id="entry", state="running")
        link._callback_ownership_registry = registry
        link._callback_entry_id = "entry"
        link._domain_ownership_active = lambda: True
        link._payload_transports = lambda: (transport,)
        link._disconnect_all_transports = AsyncMock()
        link._collector_ip = "192.0.2.10"
        link.collector_info = SimpleNamespace(remote_ip="192.0.2.10")
        await link.async_disconnect_collector_connections(reason="shadow_learning_protected_retry")
        link._disconnect_all_transports.assert_not_awaited()
        for sid in ("foreign", "weak", "new-arrival", "other-port"):
            connections[sid].disconnect.assert_not_awaited()
        return closed

    async def test_primary_and_callback_close_but_foreign_weak_and_new_arrival_do_not(self):
        self.assertEqual(await self.exercise(), ["primary", "callback"])

    async def test_authority_is_rechecked_after_first_disconnect_await(self):
        self.assertEqual(await self.exercise(owner_changes=True), ["primary"])
