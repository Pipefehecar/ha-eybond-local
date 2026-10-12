"""Deterministic proxy shutdown races; no sockets or trace files are opened."""

from __future__ import annotations

import asyncio
import argparse
import io
import json
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.collector_cloud_proxy import (
    FrameExtractor,
    JsonLineWriter,
    _handle_passive_client,
    _pipe,
    _run,
)
from custom_components.eybond_local.support.proxy_capture.session import (
    InProcessProxyCaptureHandler,
)


class _Output(io.StringIO):
    def __init__(self, *, gate: str = "", fail: str = "") -> None:
        super().__init__()
        self.loop = asyncio.get_running_loop()
        self.gate = gate
        self.fail = fail
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self.events: list[str] = []
        self.late_writes: list[str] = []
        self.close_count = 0

    def _wait(self, kind: str) -> None:
        if kind == self.gate:
            self.loop.call_soon_threadsafe(self.entered.set)
            if not self.release.wait(5):
                raise AssertionError("executor gate was not released")

    def write(self, line: str) -> int:
        kind = json.loads(line)["kind"]
        self._wait(kind)
        if self.closed:
            self.late_writes.append(kind)
        if kind == self.fail:
            raise OSError("trace write failed")
        result = super().write(line)
        self.events.append(kind)
        return result

    def close(self) -> None:
        self._wait("close")
        self.close_count += 1
        self.events.append("close")
        super().close()


class _Socket:
    def __init__(self, reader: asyncio.StreamReader) -> None:
        self.reader = reader
        self.forwarded = asyncio.Event()
        self.closed = False

    def get_extra_info(self, _name: str):
        return ("192.0.2.1", 1)

    def write(self, _data: bytes) -> None:
        self.forwarded.set()

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True
        self.reader.feed_eof()

    async def wait_closed(self) -> None:
        pass


class ProxyShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.outputs: list[_Output] = []

    async def asyncTearDown(self) -> None:
        # Release workers even when an assertion fails, keeping failures bounded.
        for output in self.outputs:
            output.release.set()
        pending = tuple(
            task for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), 2)

    def output(self, **kwargs) -> _Output:
        output = _Output(**kwargs)
        self.outputs.append(output)
        return output

    async def ready(self, event: asyncio.Event) -> None:
        await asyncio.wait_for(event.wait(), 2)

    async def handler(self, output: _Output, *, restore: bool = True):
        client_reader, upstream_reader = asyncio.StreamReader(), asyncio.StreamReader()
        client, upstream = _Socket(client_reader), _Socket(upstream_reader)

        async def open_output(_path):
            return output

        async def close_output(stream):
            await asyncio.to_thread(stream.close)

        async def connect(*_args):
            return upstream_reader, upstream

        handler = InProcessProxyCaptureHandler(
            upstream_host="unused.invalid", upstream_port=1,
            output_path=Path("unused.jsonl"),
            masked_endpoint="unused.invalid,1,TCP" if restore else "",
            restore_trigger_path=Path("/nonexistent-proxy-shutdown-trigger") if restore else None,
            async_open_output=open_output, async_close_output=close_output,
        )
        with patch("asyncio.open_connection", connect):
            await handler.start()
        parent = asyncio.create_task(handler.handle_client(client_reader, client))
        return handler, parent, client, upstream

    def pipes(self):
        return tuple(t for t in asyncio.all_tasks() if t.get_coro().__qualname__ == "_pipe")

    async def test_stop_restore_wait_drains_tails_before_disconnect_and_close(self) -> None:
        output = self.output(gate="tail")
        handler, parent, client, upstream = await self.handler(output)
        client.reader.feed_data(b"\x01")
        upstream.reader.feed_data(b"\x02")
        await self.ready(client.forwarded)
        await self.ready(upstream.forwarded)
        pipes = self.pipes()
        self.assertEqual(len(pipes), 2)
        stop = asyncio.create_task(handler.stop())
        await self.ready(output.entered)
        second_stop = asyncio.create_task(handler.stop())
        for _ in range(2):
            stop.cancel()
            parent.cancel()
            await asyncio.sleep(0)
        self.assertFalse(stop.done())
        self.assertFalse(output.closed)
        self.assertNotIn("disconnect", output.events)
        output.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(stop, 2)
        await asyncio.wait_for(second_stop, 2)
        await handler.stop()
        self.assertTrue(parent.done())
        self.assertTrue(all(t.done() for t in pipes))
        self.assertTrue(client.closed and upstream.closed)
        self.assertEqual(output.events.count("tail"), 2)
        self.assertEqual(output.events.count("pipe_ended"), 2)
        self.assertEqual(output.events[-2:], ["disconnect", "close"])
        self.assertEqual(output.close_count, 1)
        self.assertEqual(output.late_writes, [])

    async def test_stop_waits_for_cancelled_executor_write_in_both_branches(self) -> None:
        for restore in (False, True):
            with self.subTest(restore=restore):
                output = self.output(gate="chunk")
                handler, parent, client, upstream = await self.handler(output, restore=restore)
                client.reader.feed_data(b"\x01")
                await self.ready(output.entered)
                pipes = self.pipes()
                stop = asyncio.create_task(handler.stop())
                for _ in range(3):
                    await asyncio.sleep(0)
                    parent.cancel()
                self.assertFalse(stop.done())
                self.assertFalse(output.closed)
                output.release.set()
                await asyncio.wait_for(stop, 2)
                self.assertTrue(parent.done())
                self.assertTrue(all(t.done() for t in pipes))
                self.assertTrue(client.closed and upstream.closed)
                self.assertEqual(output.events.count("chunk"), 1)
                self.assertEqual(output.events[-1], "close")
                self.assertEqual(output.late_writes, [])

    async def test_writer_repeated_cancellation_preserves_serialization_with_close(self) -> None:
        output = self.output(gate="chunk")
        writer = JsonLineWriter(output)
        write = asyncio.create_task(writer.write({"kind": "chunk"}))
        await self.ready(output.entered)
        close = asyncio.create_task(writer.close())
        await asyncio.sleep(0)  # Enter close before cancelling its caller.
        for _ in range(3):
            write.cancel()
            close.cancel()
            await asyncio.sleep(0)
        self.assertFalse(write.done())
        self.assertFalse(close.done())
        self.assertFalse(output.closed)
        output.release.set()
        for task in (write, close):
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        await writer.close()
        self.assertEqual(output.events, ["chunk", "close"])
        with self.assertRaisesRegex(RuntimeError, "proxy_trace_closed"):
            await writer.write({"kind": "late"})
        self.assertEqual(output.late_writes, [])

    async def test_stop_waits_for_close_worker_despite_repeated_cancellation(self) -> None:
        output = self.output(gate="close")
        handler, parent, client, _upstream = await self.handler(output)
        client.reader.feed_eof()
        await asyncio.wait_for(parent, 2)
        stop = asyncio.create_task(handler.stop())
        await self.ready(output.entered)
        for _ in range(3):
            stop.cancel()
            await asyncio.sleep(0)
        self.assertFalse(stop.done())
        self.assertFalse(output.closed)
        output.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(stop, 2)
        await handler.stop()
        self.assertEqual(output.close_count, 1)

    async def test_connection_trace_failures_still_drain_pipes_and_close_sockets(self) -> None:
        for kind in ("connect", "chunk", "pipe_ended", "tail", "disconnect"):
            with self.subTest(kind=kind):
                output = self.output(fail=kind)
                handler, parent, client, upstream = await self.handler(output)
                client.reader.feed_data(b"\x01")
                client.reader.feed_eof()
                with self.assertRaisesRegex(OSError, "trace write failed"):
                    await asyncio.wait_for(parent, 2)
                await asyncio.wait_for(handler.stop(), 2)
                self.assertEqual(self.pipes(), ())
                self.assertTrue(client.closed and upstream.closed)
                self.assertTrue(output.closed)
                self.assertEqual(output.close_count, 1)

    async def test_pipe_trace_failure_cannot_skip_socket_close(self) -> None:
        output = self.output(fail="pipe_ended")
        reader = asyncio.StreamReader()
        reader.feed_eof()
        socket = _Socket(reader)
        with self.assertRaises(OSError):
            await _pipe(reader, socket, direction="test", remote="test",
                        frame_writer=JsonLineWriter(output))
        self.assertTrue(socket.closed)

    async def test_passive_tail_failure_cannot_skip_socket_close(self) -> None:
        for kind in ("passive_connect", "tail", "passive_disconnect"):
            with self.subTest(kind=kind):
                output = self.output(fail=kind)
                reader = asyncio.StreamReader()
                reader.feed_data(b"\x01")
                reader.feed_eof()
                socket = _Socket(reader)
                with self.assertRaises(OSError):
                    await _handle_passive_client(reader, socket, frame_writer=JsonLineWriter(output))
                self.assertTrue(socket.closed)

    async def test_cli_shutdown_joins_passive_client_before_closing_trace(self) -> None:
        output = self.output(gate="tail")
        reader = asyncio.StreamReader()
        reader.feed_data(b"\x01")
        socket = _Socket(reader)
        fed = asyncio.Event()
        original_feed = FrameExtractor.feed

        async def feed(extractor, data):
            await original_feed(extractor, data)
            fed.set()

        class Server:
            sockets = ()
            closed = False

            def close(self):
                self.closed = True

            async def wait_closed(self):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.close()

            async def serve_forever(self):
                await asyncio.Event().wait()

        server = Server()

        async def start_server(callback, *_args):
            callback(reader, socket)
            return server

        args = argparse.Namespace(
            output="unused.jsonl", restore_endpoint="", no_upstream=True,
            announce_collector=False, listen_host="unused", listen_port=1, duration=0,
        )
        with (
            patch.object(Path, "mkdir"),
            patch.object(Path, "open", return_value=output),
            patch("asyncio.start_server", start_server),
            patch.object(FrameExtractor, "feed", feed),
        ):
            run = asyncio.create_task(_run(args))
            await self.ready(fed)
            run.cancel()
            await self.ready(output.entered)
            run.cancel()
            await asyncio.sleep(0)
            self.assertFalse(run.done())
            self.assertFalse(output.closed)
            output.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(run, 2)
        self.assertTrue(socket.closed and server.closed)
        self.assertEqual(output.events[-3:], ["tail", "passive_disconnect", "close"])
        self.assertEqual(output.late_writes, [])


if __name__ == "__main__":
    unittest.main()
