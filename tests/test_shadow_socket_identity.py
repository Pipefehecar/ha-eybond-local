"""Fresh socket identity and cancellation, with no cloud/device side effects."""

import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.support.shadow_learning.identity import async_verify_shadow_socket
from custom_components.eybond_local.support.shadow_learning.proxy import (
    InProcessFailClosedShadowProxyHandler, route_status_has_verified_identity,
)
from tests.test_shadow_learning_proxy import _seed

PN = "E50000200000000001"


def reply(*, pn=PN, tid=1, fc=2):
    return build_collector_request(tid, (b"\x00\x02" if fc == 2 else b"") + pn.encode(),
                                   devcode=1, collector_addr=1, fcode=fc)


class Writer:
    def __init__(self, reader, response):
        self.reader, self.response, self.sent = reader, response, []

    def write(self, value):
        self.sent.append(value)
        self.response(self.reader, len(self.sent))

    async def drain(self):
        await asyncio.sleep(0)


class ShadowSocketIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_challenge_preserves_coalesced_non_challenge_traffic(self):
        reader = asyncio.StreamReader()
        first = reply(tid=0x0D0A, fc=1)
        self.assertTrue(first.startswith(b"\r\n"))
        tail = b"AT+WFSS:-55\r\n"

        def respond(reader, _number):
            reader.feed_data(first + reply()[:5])
            asyncio.get_running_loop().call_soon(reader.feed_data, reply()[5:] + tail)

        writer = Writer(reader, respond)
        source, prelude = await async_verify_shadow_socket(
            reader, writer, collector_pn=PN, session_protocol="eybond_framed",
        )
        self.assertEqual(source, "fc2_parameter_2")
        self.assertEqual(prelude, first + tail)
        self.assertEqual(len(writer.sent), 1)

    async def test_wrong_tid_or_short_heartbeat_does_not_verify(self):
        for data in (reply(tid=77), reply(pn=PN[:14], fc=1), b"AT+WFSS:-50\r\n"):
            with self.subTest(data=data):
                reader = asyncio.StreamReader()
                writer = Writer(reader, lambda reader, _: reader.feed_data(data))
                with self.assertRaisesRegex(TimeoutError, "identity_timeout"):
                    await async_verify_shadow_socket(reader, writer, collector_pn=PN,
                                                     session_protocol="eybond_framed", timeout=0.01)
                self.assertEqual(len(writer.sent), 2)

    async def test_fc1_fallback_uses_normal_identity_probe(self):
        reader = asyncio.StreamReader()
        writer = Writer(reader, lambda reader, n: reader.feed_data(reply(fc=1)) if n == 2 else None)
        source, prelude = await async_verify_shadow_socket(
            reader, writer, collector_pn=PN, session_protocol="eybond_framed", timeout=0.01,
        )
        self.assertEqual((source, prelude), ("fc1_identity_challenge", b""))

    async def test_at_fragmented_empty_line_prefix_and_exact_pn(self):
        reader = asyncio.StreamReader()

        def respond(reader, _number):
            reader.feed_data(b"\r")
            asyncio.get_running_loop().call_soon(reader.feed_data, f"\nAT+DTUPN:{PN}\r\n".encode())

        writer = Writer(reader, respond)
        self.assertEqual(await async_verify_shadow_socket(
            reader, writer, collector_pn=PN, session_protocol="at_text",
        ), ("at_dtupn", b""))
        self.assertEqual(writer.sent, [b"AT+DTUPN?\r\n"])

    async def test_at_identity_text_inside_binary_payload_is_not_an_at_response(self):
        reader = asyncio.StreamReader()
        frame = build_collector_request(7, f"AT+DTUPN:{PN}\r\n".encode(),
                                        devcode=1, collector_addr=1, fcode=4)
        writer = Writer(reader, lambda reader, _: reader.feed_data(frame))
        with self.assertRaisesRegex(TimeoutError, "identity_timeout"):
            await async_verify_shadow_socket(reader, writer, collector_pn=PN,
                                             session_protocol="at_text", timeout=0.01)

    async def test_foreign_identity_and_prefix_are_not_the_requested_full_pn(self):
        for pn in ("V000405SYN94677058", PN[:14]):
            reader = asyncio.StreamReader()
            writer = Writer(reader, lambda reader, _: reader.feed_data(reply(pn=pn)))
            with self.assertRaisesRegex(RuntimeError, "identity_mismatch"):
                await async_verify_shadow_socket(reader, writer, collector_pn=PN,
                                                 session_protocol="eybond_framed")

    async def test_no_wire_guess_and_bounded_prelude(self):
        reader = asyncio.StreamReader()
        writer = Writer(reader, lambda reader, _: reader.feed_data(b"AT+WFSS:-55\r\n" * 6000))
        with self.assertRaisesRegex(RuntimeError, "context_invalid"):
            await async_verify_shadow_socket(reader, writer, collector_pn=PN, session_protocol="")
        self.assertFalse(writer.sent)
        with self.assertRaisesRegex(RuntimeError, "prelude_too_large"):
            await async_verify_shadow_socket(reader, writer, collector_pn=PN, session_protocol="at_text")

    async def test_disconnected_or_cancelled_probe_does_not_succeed(self):
        reader = asyncio.StreamReader()
        writer = Writer(reader, lambda reader, _: reader.feed_eof())
        with self.assertRaisesRegex(RuntimeError, "identity_disconnected"):
            await async_verify_shadow_socket(reader, writer, collector_pn=PN, session_protocol="at_text")
        reader = asyncio.StreamReader()
        writer = Writer(reader, lambda *args: None)
        task = asyncio.create_task(async_verify_shadow_socket(
            reader, writer, collector_pn=PN, session_protocol="at_text",
        ))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class ShadowIdentityRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_cancellation_drains_child_pump_before_closing_trace(self):
        with tempfile.TemporaryDirectory() as tmp:
            pump_started, pump_cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cloud_done = asyncio.Event()

            async def cloud(reader, writer):
                try:
                    writer.write(b"AT+CLDSRVHOST1?\r\n")
                    await writer.drain()
                    await reader.read()
                finally:
                    writer.close()
                    await writer.wait_closed()
                    cloud_done.set()

            upstream = await asyncio.start_server(cloud, "127.0.0.1", 0)
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1", upstream_port=upstream.sockets[0].getsockname()[1],
                seed=replace(_seed(), collector_pn=PN), output_path=Path(tmp) / "trace.jsonl",
                expected_session_protocol="at_text",
            )

            async def slow_response(*args, **kwargs):
                pump_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    pump_cancelled.set()
                    await release.wait()
                    # The trace still belongs to this pump, even after the
                    # parent/stop task has been cancelled repeatedly.
                    self.assertFalse(handler._output_handle.closed)
                    await handler._append_event("last_pump_event", "test", {})

            handler.backend._handle_at_line = slow_response
            await handler.start()
            proxy = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
            stop = None
            try:
                await reader.readline()
                writer.write(f"AT+DTUPN:{PN}\r\n".encode())
                await writer.drain()
                await asyncio.wait_for(pump_started.wait(), 1)
                stop = asyncio.create_task(handler.stop())
                await asyncio.wait_for(pump_cancelled.wait(), 1)
                for _ in range(2):
                    stop.cancel()
                    await asyncio.sleep(0)
                self.assertFalse(stop.done())
                self.assertIsNotNone(handler._output_handle)
                self.assertFalse(route_status_has_verified_identity(handler.status(), PN))
            finally:
                release.set()
                if stop is not None:
                    try:
                        await stop
                    except asyncio.CancelledError:
                        pass
                await handler.stop()
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(cloud_done.wait(), 1)
                proxy.close()
                upstream.close()
                await proxy.wait_closed()
                await upstream.wait_closed()
            self.assertFalse(handler._tasks)
            self.assertIn("last_pump_event", (Path(tmp) / "trace.jsonl").read_text())

    async def test_real_handler_proves_identity_before_cloud_and_drains_before_trace_close(self):
        for wire in ("at_text", "eybond_framed"):
            with self.subTest(wire=wire), tempfile.TemporaryDirectory() as tmp:
                cloud_connected = asyncio.Event()
                cloud_done = asyncio.Event()

                async def cloud(reader, writer):
                    cloud_connected.set()
                    try:
                        await reader.read()
                    finally:
                        writer.close()
                        await writer.wait_closed()
                        cloud_done.set()

                upstream = await asyncio.start_server(cloud, "127.0.0.1", 0)
                handler = InProcessFailClosedShadowProxyHandler(
                    upstream_host="127.0.0.1", upstream_port=upstream.sockets[0].getsockname()[1],
                    seed=replace(_seed(), collector_pn=PN), output_path=Path(tmp) / "trace.jsonl",
                    expected_session_protocol=wire,
                )
                await handler.start()
                proxy = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
                port = proxy.sockets[0].getsockname()[1]
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                second_writer = None
                try:
                    query = await asyncio.wait_for(reader.read(100), 1)
                    self.assertFalse(cloud_connected.is_set())
                    self.assertFalse(route_status_has_verified_identity(handler.status(), PN))
                    writer.write(f"AT+DTUPN:{PN}\r\n".encode() if wire == "at_text" else reply())
                    await writer.drain()
                    await asyncio.wait_for(cloud_connected.wait(), 1)
                    writer.write(b"AT+WFSS:-50\r\n")
                    for _ in range(100):
                        if route_status_has_verified_identity(handler.status(), PN):
                            break
                        await asyncio.sleep(0.001)
                    self.assertTrue(route_status_has_verified_identity(handler.status(), PN))
                    self.assertTrue(query)
                    second_reader, second_writer = await asyncio.open_connection("127.0.0.1", port)
                    self.assertEqual(await asyncio.wait_for(second_reader.read(), 1), b"")
                    self.assertTrue(route_status_has_verified_identity(handler.status(), PN))
                    await handler.stop()
                    self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
                    await asyncio.wait_for(cloud_done.wait(), 1)
                    self.assertFalse(handler.status()["collector_identity_verified"])
                    self.assertFalse(handler._tasks)
                    self.assertIsNone(handler._output_handle)
                finally:
                    await handler.stop()
                    for client in (writer, second_writer):
                        if client is not None:
                            client.close()
                            await client.wait_closed()
                    proxy.close()
                    upstream.close()
                    await proxy.wait_closed()
                    await upstream.wait_closed()

    async def test_identity_failure_never_opens_cloud_and_next_connection_starts_unverified(self):
        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="cloud.invalid", upstream_port=18899,
                seed=replace(_seed(), collector_pn=PN), output_path=Path(tmp) / "trace.jsonl",
                expected_session_protocol="at_text",
            )
            await handler.start()
            server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            # Save the real connector before patching the proxy's upstream call.
            connect = asyncio.open_connection
            try:
                with patch("asyncio.open_connection", side_effect=AssertionError("upstream before identity")):
                    for _ in range(2):
                        reader, writer = await connect("127.0.0.1", port)
                        self.assertEqual(await reader.readline(), b"AT+DTUPN?\r\n")
                        writer.write(b"AT+DTUPN:V000405SYN94677058\r\n")
                        await writer.drain()
                        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
                        writer.close()
                        await writer.wait_closed()
                        for _ in range(100):
                            if not handler._tasks:
                                break
                            await asyncio.sleep(0.001)
                        self.assertFalse(handler.status()["collector_identity_verified"])
                        self.assertFalse(handler.status()["route_protocol_activity"])
            finally:
                await handler.stop()
                server.close()
                await server.wait_closed()
