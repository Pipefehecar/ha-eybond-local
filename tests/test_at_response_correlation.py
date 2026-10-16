"""Unsolicited or late AT metadata must not acknowledge another command."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.transport import _CollectorAtConnection, _CollectorConnection


class Writer:
    def __init__(self):
        self.sent = asyncio.Event()
        self.buffer = bytearray()
        self.closed = False

    def write(self, data):
        self.buffer.extend(data)
        self.sent.set()

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    def get_extra_info(self, name):
        return ("127.0.0.1", 12345) if name == "peername" else None

    async def wait_closed(self):
        pass


class AtResponseCorrelationTests(unittest.IsolatedAsyncioTestCase):
    async def test_framed_metadata_matches_parameter_on_both_wires(self):
        from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header

        for framed in (False, True):
            for function in (2, 3):
                with self.subTest(framed=framed, function=function):
                    c = (_CollectorConnection(heartbeat_interval=60, write_timeout=.5)
                         if framed else _CollectorAtConnection(write_timeout=.5))
                    reader, writer = asyncio.StreamReader(), Writer()
                    with patch.object(_CollectorConnection, "_heartbeat_loop", new=AsyncMock()):
                        run = asyncio.create_task(c.run(reader, writer))
                        action = None
                        try:
                            self.assertTrue(await c.wait_until_connected(.2))
                            send = c.async_send_collector if framed else c._async_send_mixed_frame
                            action = asyncio.create_task(send(
                                fcode=function, payload=b"\x15", devcode=1, collector_addr=1,
                                request_timeout=1,
                            ))
                            await asyncio.wait_for(writer.sent.wait(), 1)
                            tid = decode_header(writer.buffer[:8]).tid
                            stale = b"\x00\x02E50000200000000001"
                            reader.feed_data(build_collector_request(tid, stale, devcode=1, collector_addr=1, fcode=function))
                            # A heartbeat after the stale reply is a parser barrier.
                            reader.feed_data(build_collector_request(999, b"E5000020000000", devcode=1, collector_addr=1, fcode=1))
                            async with asyncio.timeout(1):
                                while c.collector_info.heartbeat_devcode is None:
                                    await asyncio.sleep(0)
                            self.assertFalse(action.done())
                            expected = b"\x00\x15cloud.invalid,18899,TCP"
                            reader.feed_data(build_collector_request(tid, expected, devcode=1, collector_addr=1, fcode=function))
                            self.assertEqual((await action)[1], expected)
                            pending = c._pending_parameter if framed else c._pending_framed_parameter
                            self.assertFalse(pending)
                        finally:
                            if action is not None and not action.done():
                                action.cancel()
                                await asyncio.gather(action, return_exceptions=True)
                            await c.disconnect()
                            await asyncio.gather(run, return_exceptions=True)

    async def exercise(self, *, framed, write=False, cancel_prior=False):
        connection = (
            _CollectorConnection(heartbeat_interval=60, write_timeout=.5) if framed
            else _CollectorAtConnection(write_timeout=.5)
        )
        reader, writer = asyncio.StreamReader(), Writer()
        with patch.object(_CollectorConnection, "_heartbeat_loop", new=AsyncMock()):
            running = asyncio.create_task(connection.run(reader, writer))
            action = None
            try:
                self.assertTrue(await connection.wait_until_connected(.2))
                if cancel_prior:
                    prior = asyncio.create_task(connection.async_query("FWVER", request_timeout=1))
                    await asyncio.wait_for(writer.sent.wait(), 1)
                    prior.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await prior
                    self.assertEqual(connection._pending_at_command, "")
                    writer.sent.clear()
                action = asyncio.create_task(
                    connection.async_write("cldsrvhost1", "cloud.invalid,18899,TCP", request_timeout=1)
                    if write else connection.async_query("cldsrvhost1", request_timeout=1)
                )
                await asyncio.wait_for(writer.sent.wait(), 1)
                reader.feed_data(b"AT+DTUPN:E50000200000000001\r\nAT+FWVER:old-response\r\n")
                # Let the real read loop consume both unsolicited lines.
                async with asyncio.timeout(1):
                    while connection.collector_info.collector_pn != "E50000200000000001":
                        await asyncio.sleep(0)
                self.assertFalse(action.done())
                reader.feed_data(b"AT+CLDSRVHOST1:cloud.invalid,18899,TCP\r\n")
                result = await action
                self.assertEqual(result.command, "CLDSRVHOST1")
                self.assertEqual(result.value, "cloud.invalid,18899,TCP")
                self.assertEqual(connection._pending_at_command, "")
            finally:
                if action is not None and not action.done():
                    action.cancel()
                    await asyncio.gather(action, return_exceptions=True)
                await connection.disconnect()
                await asyncio.gather(running, return_exceptions=True)

    async def test_queries_and_writes_on_both_wires_match_command(self):
        for framed in (False, True):
            for write in (False, True):
                with self.subTest(framed=framed, write=write):
                    await self.exercise(framed=framed, write=write)

    async def test_cancelled_command_cannot_complete_a_different_next_command(self):
        for framed in (False, True):
            with self.subTest(framed=framed):
                await self.exercise(framed=framed, cancel_prior=True)
