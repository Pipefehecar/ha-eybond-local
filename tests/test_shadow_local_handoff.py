"""Loopback proof of same-listener transport handback, not cloud settlement.

The temporary handler is synthetic: this qualifies socket ownership and local
payload resumption, not permanent endpoint readback, NAT or the arming wizard.
"""

import asyncio
import socket
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.protocol import HEADER_SIZE, build_collector_request, decode_header
from custom_components.eybond_local.collector.transport import (
    SharedCollectorAtTransport, SharedEybondTransport, SharedProxyCaptureRoute,
    _CollectorConnection, _LISTENERS,
)
from custom_components.eybond_local.link_models import EybondLinkRoute


PN = "E50000200000000001"


class ShadowLocalHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, wire):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        kwargs = dict(host="127.0.0.1", port=port, request_timeout=1.0,
                      collector_ip="127.0.0.1", collector_pn=PN, collector_session_protocol=wire)
        transport = (
            SharedCollectorAtTransport(**kwargs) if wire == "at_text"
            else SharedEybondTransport(**kwargs, heartbeat_interval=60)
        )
        hello = (
            f"AT+DTUPN:{PN}\r\n".encode() if wire == "at_text"
            else build_collector_request(1, PN.encode(), devcode=2376, collector_addr=1, fcode=1)
        )
        claimed = asyncio.Event()

        async def handler(reader, writer):
            try:
                self.assertEqual(await reader.readexactly(len(hello)), hello)
                claimed.set()
                self.assertEqual(await reader.read(), b"")
            finally:
                writer.close()
                await writer.wait_closed()

        route = SharedProxyCaptureRoute(
            host="127.0.0.1", port=port, collector_ip="127.0.0.1", collector_pn=PN,
            expected_session_protocol=wire, handler=handler,
        )
        clients = []
        tasks = []

        async def connect(*, local):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            clients.append(writer)
            writer.write(hello)
            await writer.drain()
            if local:
                async def respond():
                    try:
                        while True:
                            head = await reader.readexactly(2)
                            if head == b"AT":
                                command = head + await reader.readuntil(b"\n")
                                self.assertEqual(command, b"AT+WFSS?\r\n")
                                writer.write(b"AT+WFSS:-55\r\n")
                            else:
                                header = decode_header(head + await reader.readexactly(HEADER_SIZE - 2))
                                await reader.readexactly(header.payload_len)
                                writer.write(build_collector_request(
                                    header.tid, b"payload-ok" if header.fcode == 4 else PN.encode(),
                                    devcode=header.devcode, collector_addr=header.devaddr, fcode=header.fcode,
                                ))
                            await writer.drain()
                    except (asyncio.IncompleteReadError, ConnectionError):
                        pass
                tasks.append(asyncio.create_task(respond()))
            return reader, writer

        async def read_local():
            self.assertTrue(await transport.wait_until_connected(1))
            self.assertEqual(transport.collector_info.collector_pn, PN)
            if wire == "at_text":
                reply = await transport.async_query("WFSS")
                self.assertEqual(reply.value, "-55")
            self.assertEqual(await transport.async_send_payload(
                b"read-only-query", route=EybondLinkRoute(devcode=2376, collector_addr=1),
            ), b"payload-ok")

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=AsyncMock()):
            await transport.start()
            listener = transport._listener
            try:
                await connect(local=True)
                await read_local()
                await route.start()
                self.assertIs(route._listener, listener)
                await transport.disconnect()
                cloud_reader, _writer = await connect(local=False)
                await asyncio.wait_for(claimed.wait(), 2)
                self.assertFalse(transport.connected)
                await route.stop()
                self.assertEqual(await asyncio.wait_for(cloud_reader.read(), 1), b"")
                self.assertIs(_LISTENERS[("127.0.0.1", port)], listener)
                self.assertEqual(listener._exclusive_routes, {})
                await connect(local=True)
                await read_local()
            finally:
                await route.stop()
                for writer in clients:
                    writer.close()
                    await writer.wait_closed()
                await transport.stop()
                await asyncio.gather(*tasks)
            self.assertNotIn(("127.0.0.1", port), _LISTENERS)

    async def test_framed_local_payload_resumes_after_temporary_route_stop(self):
        await self.exercise("eybond_framed")

    async def test_at_local_management_and_payload_resume_after_temporary_route_stop(self):
        await self.exercise("at_text")
