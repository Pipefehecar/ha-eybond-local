"""Ordinary (non-Bridge) learning owns HTTPS until its worker is drained."""
import asyncio
from contextlib import ExitStack
import threading
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from custom_components.eybond_local import cloud_http_session as http
from custom_components.eybond_local import dessmonitor_cloud as dess
from custom_components.eybond_local import smartess_cloud as smart
from custom_components.eybond_local.support import cloud_control_discovery as scd
from custom_components.eybond_local.support import dessmonitor_active as dcd
from custom_components.eybond_local.support.shadow_learning import ShadowWriteObservation
from test_cloud_http_session import reply

PN = "E50000200000000001"


class OwnershipTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.resources = []
        self.observations = []
        self.entered, self.release = threading.Event(), threading.Event()
        self.block = False
        self.failure = None
        self.metadata_failure = False
        self.ready = True
        self.pause_observation = False
        self.observing = asyncio.Event()

    async def run_provider(self, source):
        module = scd if source == "smartess" else dcd
        identity = dess.DessMonitorDeviceIdentity(PN, "SN1", 2304, 1)
        def resource(base):
            session = http.CloudHttpSession(base)
            self.resources.append(session)
            return session

        async def executor(fn):
            return fn()

        async def route():
            pass

        async def observed(cursor, _timeout):
            if self.pause_observation:
                self.observing.set()
                await asyncio.Event().wait()
            return tuple(self.observations[cursor:])

        with ExitStack() as stack:
            stack.enter_context(patch.object(module, "CloudHttpSession", side_effect=resource))
            if source == "smartess":
                bundle = {"request": {"params": identity.to_record()}, "responses": {
                    "device_settings": {"dat": {"field": [{"id": "mode", "name": "Mode",
                        "item": [{"key": "0", "val": "Off"}, {"key": "1", "val": "On"}]}]}}}}
                stack.enter_context(patch.object(scd, "fetch_control_discovery_bundle_for_collector",
                    return_value=bundle, side_effect=TimeoutError() if self.metadata_failure else None))
                stack.enter_context(patch.object(scd, "login_for_control_discovery",
                    return_value=(None, smart.SessionCredentials(token="token", secret="secret"))))
                operation = scd.SmartEssActiveCorrelationOperation()
            else:
                bundle = dess.DessMonitorEvidenceBundle(identity=identity, telemetry_fields=(),
                    chart_fields=(), key_parameters=(), control_fields=(dess.DessMonitorControlField(
                        field_id="mode", title="Mode", choices=(("0", "Off"), ("1", "On"))),))
                stack.enter_context(patch.object(dcd, "fetch_read_only_evidence_for_session",
                    return_value=bundle, side_effect=TimeoutError() if self.metadata_failure else None))
                stack.enter_context(patch.object(dcd, "login_with_password",
                    return_value=(None, dess.DessMonitorSession(token="token", secret="secret"))))
                operation = dcd.DessMonitorActiveCorrelationOperation()
            return await operation.async_correlate(executor=executor, collector_pn=PN,
                username="u", password="p", fallback_identity=None, max_fields=0,
                progress=lambda *_: None, adopt_identity=lambda _: None,
                start_shadow_route=route, on_learning=lambda: None,
                orchestrator_callbacks={"observation_cursor": lambda: len(self.observations),
                    "current_observations_since": lambda c: () if self.pause_observation else tuple(self.observations[c:]),
                    "wait_for_observations_since": observed, "is_session_ready": lambda: self.ready})

    def connection(self, *_args, **_kwargs):
        connection = Mock()
        response = reply()
        current = {}
        def request(_method, target, **_kwargs):
            current.update(parse_qs(urlsplit(target).query))
        def read(_limit):
            self.entered.set()
            if self.block and not self.release.wait(3):
                raise TimeoutError("test worker was not released")
            if self.failure:
                raise self.failure
            self.observations.append(ShadowWriteObservation(register=300, values=(1,),
                function_code=6, devcode=2304, devaddr=1, raw_payload_hex="0001"))
            return b'{"err":0,"desc":"ok","dat":{}}'
        connection.request.side_effect = request
        response.read.side_effect = read
        connection.getresponse.return_value = response
        return connection

    async def test_both_real_clients_reuse_and_close_owned_resource(self):
        for source in ("smartess", "dessmonitor"):
            with self.subTest(source=source), patch.object(http, "HTTPSConnection", side_effect=self.connection) as factory, \
                    patch.object(smart, "urlopen") as old_smart, patch.object(dess, "urlopen") as old_dess:
                result = await self.run_provider(source)
                self.assertEqual(result.result["captured_not_applied_count"], 2)
                factory.assert_called_once()
                self.assertTrue(self.resources[-1]._closed)
                old_smart.assert_not_called()
                old_dess.assert_not_called()
        self.assertIsNot(self.resources[0], self.resources[1])

    async def test_uncertain_error_aborts_without_second_command(self):
        for source in ("smartess", "dessmonitor"):
            self.failure = TimeoutError()
            connection = self.connection()
            with self.subTest(source=source), patch.object(http, "HTTPSConnection", return_value=connection):
                with self.assertRaises((smart.SmartEssCloudError, dess.DessMonitorCloudError)):
                    await self.run_provider(source)
                connection.request.assert_called_once()
                connection.close.assert_called_once()
                self.assertTrue(self.resources[-1]._closed)

    async def test_repeated_cancel_drains_worker_before_owner_closes(self):
        for source in ("smartess", "dessmonitor"):
            self.entered.clear()
            self.release.clear()
            self.block = True
            connection = self.connection()
            with self.subTest(source=source), patch.object(http, "HTTPSConnection", return_value=connection):
                task = asyncio.create_task(self.run_provider(source))
                try:
                    self.assertTrue(await asyncio.to_thread(self.entered.wait, 2))
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(.01)
                        self.assertFalse(task.done())
                        self.assertFalse(self.resources[-1]._closed)
                        connection.close.assert_not_called()
                finally:
                    self.release.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 2)
                self.assertTrue(self.resources[-1]._closed)
                connection.close.assert_called_once()
                connection.request.assert_called_once()

    async def test_metadata_failure_never_creates_control_http_resource(self):
        self.metadata_failure = True
        for source in ("smartess", "dessmonitor"):
            with self.assertRaises(TimeoutError):
                await self.run_provider(source)
        self.assertEqual(self.resources, [])

    async def test_cancel_between_request_and_next_probe_closes_idle_socket(self):
        for source in ("smartess", "dessmonitor"):
            self.pause_observation = True
            self.observing.clear()
            connection = self.connection()
            with patch.object(http, "HTTPSConnection", return_value=connection):
                task = asyncio.create_task(self.run_provider(source))
                try:
                    await asyncio.wait_for(self.observing.wait(), 2)
                    connection.close.assert_not_called()
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                self.assertTrue(self.resources[-1]._closed)
                connection.request.assert_called_once()
                connection.close.assert_called_once()

    async def test_fresh_run_after_failure_owns_a_different_resource(self):
        for source in ("smartess", "dessmonitor"):
            with patch.object(http, "HTTPSConnection", side_effect=self.connection) as factory:
                self.failure = TimeoutError()
                with self.assertRaises((smart.SmartEssCloudError, dess.DessMonitorCloudError)):
                    await self.run_provider(source)
                failed = self.resources[-1]
                self.failure = None
                outcome = await self.run_provider(source)
                self.assertEqual(outcome.result["captured_not_applied_count"], 2)
                self.assertEqual(factory.call_count, 2)
                self.assertIsNot(failed, self.resources[-1])
                self.assertTrue(failed._closed)
                self.assertTrue(self.resources[-1]._closed)

    async def test_lost_route_closes_unused_resource_without_network(self):
        self.ready = False
        for source in ("smartess", "dessmonitor"):
            with patch.object(http, "HTTPSConnection") as factory:
                await self.run_provider(source)
                factory.assert_not_called()
                self.assertTrue(self.resources[-1]._closed)
