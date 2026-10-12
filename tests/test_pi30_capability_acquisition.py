"""PI30 current metadata acquisition is independent of discovery's deadline."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers import pi30, pi30_controls
from custom_components.eybond_local.drivers.pi30_controls import (
    async_acquire_charge_current_controls,
    charge_current_acquisition_diagnostics,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, WriteCapability
from custom_components.eybond_local.payload.pi30 import Pi30Error


# Public test response shapes, with no customer identity or archive contents.
TARGET = ProbeTarget(0x0994, 1, 0)
QPIRI = "220.0 14.5 220.0 60.0 13.9 3200 3000 24.0 25.3 24.0 28.7 27.0 2 10 050 1 0 2 6 01 0 0 00.0 0 1"
REPLIES = {
    "QPI": "PI30", "QID": "NAK", "QSID": "NAK", "QMN": "NAK",
    "QFLAG": "EabxzDjkuvy", "QPIRI": QPIRI,
    "QMCHGCR": "010 020 030 040 050 060 070 080 090 100 110",
    "QMUCHGCR": "002 010 020 030 040 050 060 070 080",
}
CURRENT_KEYS = {"max_charging_current", "max_ac_charging_current"}


class _Transport(FixtureTransport):
    def __init__(self):
        super().__init__(registers=None, probe_target=TARGET, command_responses={
            (TARGET.devcode, 1, key): value for key, value in REPLIES.items()
        })
        self.commands = []

    async def async_send_payload(self, payload, *, route):
        self.commands.append(payload[:-3].decode("ascii"))
        return await super().async_send_payload(payload, route=route)


def _saved_binding() -> DetectedInverter:
    # A restored binding can have no cached current readbacks at all.
    driver = pi30.Pi30Driver()
    return DetectedInverter(
        driver_key="pi30", protocol_family="pi30", model_name="PI30 test",
        serial_number="", probe_target=TARGET, profile_name=driver.profile_name,
        register_schema_name=driver.register_schema_name,
    )


def _session(replies=None):
    replies = dict(REPLIES if replies is None else replies)

    async def request(command):
        result = replies[command]
        if isinstance(result, Exception):
            raise result
        return result

    return SimpleNamespace(request=AsyncMock(side_effect=request))


class Pi30CapabilityAcquisitionTests(unittest.IsolatedAsyncioTestCase):
    async def test_exhausted_probe_defers_to_runtime_without_repeating_queries(self):
        transport = _Transport()
        driver = pi30.Pi30Driver()
        clock = iter((0.0, 11.902))
        with patch.object(pi30, "time", SimpleNamespace(monotonic=lambda: next(clock))):
            inverter = await driver.async_probe(transport, TARGET)
        self.assertIsNotNone(inverter)
        self.assertFalse(CURRENT_KEYS & {cap.key for cap in inverter.capabilities})
        self.assertNotIn("QMCHGCR", transport.commands)
        self.assertNotIn("QMUCHGCR", transport.commands)
        transport.commands.clear()
        state = {}
        with patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
            side_effect=lambda *args, **kwargs: ({"operating_mode_code": "B"}, frozenset(), {}),
        )):
            result = await driver.async_read_values(transport, inverter, runtime_state=state)
            for _ in range(4):
                await driver.async_read_values(transport, inverter, runtime_state=state)
        self.assertEqual(transport.commands, ["QPIRI", "QMCHGCR", "QMUCHGCR"])
        self.assertEqual(result.values["max_charging_current"], "50 A")
        self.assertEqual(result.values["max_ac_charging_current"], "10 A")
        self.assertEqual(result.diagnostics["pi30_charge_current_acquisition_status"], "complete")
        for cap in inverter.capabilities:
            if cap.key in CURRENT_KEYS:
                self.assertFalse(cap.tested)
                self.assertTrue(cap.requires_confirm)
                self.assertEqual(cap.provenance, "doc_backed")

    async def test_saved_binding_uses_fresh_readback_not_stale_details(self):
        inverter = _saved_binding()
        inverter.details.update(max_charging_current=120, max_ac_charging_current="80 A")
        session = _session()
        values = await async_acquire_charge_current_controls(session, inverter, runtime_state={})
        self.assertEqual(values, {"max_charging_current": "50 A", "max_ac_charging_current": "10 A"})
        self.assertEqual([c.args[0] for c in session.request.await_args_list],
                         ["QPIRI", "QMCHGCR", "QMUCHGCR"])
        self.assertEqual(inverter.details["max_charging_current"], "50 A")

    async def test_no_metadata_queries_when_required_telemetry_failed(self):
        for values in ({}, {"alarm_active": False}, {"energy_total": 123}):
            with self.subTest(values=values):
                transport, state = _Transport(), {}
                with patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
                    return_value=(dict(values), frozenset(), {}),
                )):
                    await pi30.Pi30Driver().async_read_values(
                        transport, _saved_binding(), runtime_state=state,
                    )
                self.assertEqual(transport.commands, [])
                self.assertEqual(charge_current_acquisition_diagnostics(state), {})

    async def test_existing_model_controls_require_no_additional_commands(self):
        inverter = _saved_binding()
        inverter.capabilities = tuple(WriteCapability(
            key=key, register=0, value_kind="enum", tested=True, note="model control",
        ) for key in sorted(CURRENT_KEYS))
        original = inverter.capabilities
        session = _session()
        self.assertEqual(await async_acquire_charge_current_controls(session, inverter, runtime_state={}), {})
        self.assertIs(inverter.capabilities, original)
        session.request.assert_not_awaited()

    async def test_failures_are_bounded_per_session_and_reset_allows_retry(self):
        inverter, state = _saved_binding(), {}
        session = _session({**REPLIES, "QMCHGCR": Pi30Error("NAK"), "QMUCHGCR": "010 010"})
        for _ in range(6):
            self.assertEqual(await async_acquire_charge_current_controls(session, inverter, runtime_state=state), {})
        self.assertEqual(session.request.await_count, 9)
        self.assertEqual(inverter.capabilities, ())
        self.assertEqual(charge_current_acquisition_diagnostics(state), {
            "pi30_charge_current_acquisition_attempts": 3,
            "pi30_charge_current_acquisition_status": "exhausted",
        })
        state.clear()
        self.assertEqual(len(await async_acquire_charge_current_controls(_session(), inverter, runtime_state=state)), 2)

    async def test_failed_fresh_readback_cannot_use_cached_current_to_add_controls(self):
        inverter, state = _saved_binding(), {}
        inverter.details.update(max_charging_current=50, max_ac_charging_current=10)
        session = _session({**REPLIES, "QPIRI": "malformed"})
        self.assertEqual(await async_acquire_charge_current_controls(session, inverter, runtime_state=state), {})
        self.assertEqual([c.args[0] for c in session.request.await_args_list], ["QPIRI"])
        self.assertEqual(inverter.capabilities, ())

    async def test_slow_first_list_cannot_starve_second_after_slow_qpiri(self):
        calls, timeouts = [], []
        clock = [0.0]
        real_loop = asyncio.get_running_loop()

        async def request(command):
            calls.append(command)
            if command == "QPIRI":
                clock[0] += 1.49
                return QPIRI
            if command == "QMCHGCR":
                raise TimeoutError
            return REPLIES[command]

        async def wait_for(operation, *, timeout):
            timeouts.append(timeout)
            try:
                return await operation
            except TimeoutError:
                clock[0] += timeout  # Deterministic full first-list timeout.
                raise

        inverter, state = _saved_binding(), {}
        # Only the acquisition clock is replaced, not the event loop itself.
        with patch.object(pi30_controls, "asyncio", SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: clock[0]),
            wait_for=wait_for,
        )):
            values = await async_acquire_charge_current_controls(
                SimpleNamespace(request=request), inverter, runtime_state=state,
            )
        self.assertIs(asyncio.get_running_loop(), real_loop)
        self.assertEqual(calls, ["QPIRI", "QMCHGCR", "QMUCHGCR"])
        self.assertGreater(timeouts[-1], 0.7)
        self.assertEqual(values, {"max_ac_charging_current": "10 A"})
        calls.clear()
        session = _session()
        await async_acquire_charge_current_controls(session, inverter, runtime_state=state)
        self.assertEqual([c.args[0] for c in session.request.await_args_list], ["QPIRI", "QMCHGCR"])

    async def test_stale_session_binding_or_capabilities_cannot_publish(self):
        for change in ("session", "binding", "capabilities"):
            with self.subTest(change=change):
                inverter, state, calls = _saved_binding(), {}, []

                async def request(command):
                    calls.append(command)
                    if command == "QMCHGCR":
                        if change == "session":
                            state.clear()
                        elif change == "binding":
                            inverter.probe_target = ProbeTarget(0x0994, 2, 0)
                        else:
                            inverter.capabilities = (WriteCapability(
                                key="replacement", register=0, value_kind="bool", note="replacement",
                            ),)
                    return REPLIES[command]

                self.assertEqual(await async_acquire_charge_current_controls(
                    SimpleNamespace(request=request), inverter, runtime_state=state,
                ), {})
                self.assertEqual(calls, ["QPIRI", "QMCHGCR"])
                self.assertFalse(CURRENT_KEYS & {cap.key for cap in inverter.capabilities})
                self.assertEqual(inverter.details, {})

    async def test_cancellation_does_not_publish_partial_result_and_can_retry(self):
        inverter, state, started = _saved_binding(), {}, asyncio.Event()

        async def request(command):
            if command == "QMUCHGCR":
                started.set()
                await asyncio.Event().wait()
            return REPLIES[command]

        task = asyncio.create_task(async_acquire_charge_current_controls(
            SimpleNamespace(request=request), inverter, runtime_state=state,
        ))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(inverter.capabilities, ())
        self.assertEqual(inverter.details, {})
        self.assertEqual(len(await async_acquire_charge_current_controls(_session(), inverter, runtime_state=state)), 2)

    async def test_completed_acquisition_does_not_reinstate_invalidated_choices(self):
        inverter, state = _saved_binding(), {}
        await async_acquire_charge_current_controls(_session(), inverter, runtime_state=state)
        inverter = replace(inverter, capabilities=())
        session = _session()
        self.assertEqual(await async_acquire_charge_current_controls(session, inverter, runtime_state=state), {})
        self.assertEqual(inverter.capabilities, ())
        session.request.assert_not_awaited()

    async def test_absent_session_state_does_not_start_unbounded_acquisition(self):
        session = _session()
        self.assertEqual(await async_acquire_charge_current_controls(session, _saved_binding(), runtime_state=None), {})
        session.request.assert_not_awaited()

    async def test_real_timeout_bounds_failed_readback_and_preserves_binding(self):
        inverter, state = _saved_binding(), {}
        original = inverter.capabilities

        async def request(_command):
            await asyncio.Event().wait()

        with patch.object(pi30_controls, "_ACQUISITION_TIMEOUT", 0.02):
            result = await asyncio.wait_for(async_acquire_charge_current_controls(
                SimpleNamespace(request=request), inverter, runtime_state=state,
            ), timeout=0.5)
        self.assertEqual(result, {})
        self.assertIs(inverter.capabilities, original)
        self.assertEqual(inverter.details, {})
        self.assertEqual(charge_current_acquisition_diagnostics(state)[
            "pi30_charge_current_acquisition_attempts"
        ], 1)

    async def test_metadata_parse_errors_and_timeouts_have_exact_per_query_outcomes(self):
        for reply, outcome in (("bad", "error"), (Pi30Error("nak"), "error"),
                               (Pi30Error("request_timeout"), "timeout"), (TimeoutError(), "timeout")):
            with self.subTest(reply=repr(reply)):
                timings = []
                result = await async_acquire_charge_current_controls(
                    _session({**REPLIES, "QMCHGCR": reply}), _saved_binding(),
                    runtime_state={}, command_timings=timings,
                )
                self.assertEqual([(command, status) for command, _ms, status in timings],
                                 [("QPIRI", "ok"), ("QMCHGCR", outcome), ("QMUCHGCR", "ok")])
                self.assertTrue(all(ms >= 0 for _command, ms, _status in timings))
                self.assertEqual(result, {"max_ac_charging_current": "10 A"})

    async def test_combined_timeline_includes_metadata_cost_without_polluting_telemetry(self):
        class SlowMetadata(_Transport):
            async def async_send_payload(self, payload, *, route):
                if payload[:-3] == b"QPIRI":
                    await asyncio.sleep(0.02)
                return await super().async_send_payload(payload, route=route)

        transport, inverter, state = SlowMetadata(), _saved_binding(), {}
        # QMOD is a real fresh required reply; other missing telemetry queries
        # fail without manufacturing readings. The metadata phase follows it.
        transport._command_responses[(TARGET.devcode, 1, "QMOD")] = "B"
        for command in ("QPIGS", "QPIWS", "Q1", "QET"):
            transport._command_responses[(TARGET.devcode, 1, command)] = "NAK"
        result = await pi30.Pi30Driver().async_read_values(transport, inverter, runtime_state=state)
        diag = result.diagnostics
        telemetry = diag["pi30_poll_commands"].split(", ")
        self.assertEqual(diag["pi30_poll_scope"], "telemetry")
        self.assertEqual(diag["pi30_metadata_commands"], "QPIRI, QMCHGCR, QMUCHGCR")
        self.assertEqual(diag["pi30_metadata_status"], "attempted")
        self.assertEqual(diag["pi30_read_commands"].split(", "), transport.commands)
        self.assertEqual(transport.commands, telemetry + ["QPIRI", "QMCHGCR", "QMUCHGCR"])
        self.assertEqual([s.split("=")[0] for s in diag["pi30_read_command_durations_ms"].split(", ")],
                         transport.commands)
        self.assertGreaterEqual(diag["pi30_metadata_total_ms"], 15)
        self.assertGreaterEqual(diag["pi30_read_total_ms"] - diag["pi30_poll_total_ms"], 15)
        self.assertEqual(diag["pi30_poll_attempted"], len(telemetry))
        self.assertNotIn("QPIRI", diag["pi30_command_cumulative"])
        transport.commands.clear()
        second = await pi30.Pi30Driver().async_read_values(transport, inverter, runtime_state=state)
        self.assertEqual(second.diagnostics["pi30_metadata_commands"], "")
        self.assertEqual(second.diagnostics["pi30_metadata_command_durations_ms"], "")
        self.assertEqual(second.diagnostics["pi30_metadata_status"], "skipped")
        self.assertEqual(second.diagnostics["pi30_read_commands"].split(", "), transport.commands)


if __name__ == "__main__":
    unittest.main()
