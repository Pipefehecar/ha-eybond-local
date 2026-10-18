"""PI30 ACKs must be followed by fresh configuration evidence, not cached labels."""
import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers import pi30
from custom_components.eybond_local.drivers.pi30_controls import async_acquire_charge_current_controls
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import RuntimeSnapshot
from custom_components.eybond_local.payload.pi30 import Pi30Error
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from test_pi30_capability_acquisition import _saved_binding, _session, QPIRI


class _Hub(HubManagementMixin):
    def __init__(self, inverter):
        self._inverter = inverter
        self._driver = pi30.Pi30Driver()
        self._runtime_read_state = {}
        self._pi30_flag_command_case = "profile"
        self._link_manager = SimpleNamespace(transport=None)
        self._write_blockers = {}
        self._async_ensure_connected = AsyncMock()
        self.values = dict(inverter.details)

    async def async_refresh(self):
        result = await self._driver.async_read_values(
            None, self._inverter, runtime_state=self._runtime_read_state,
        )
        self.values.update(result.values)
        for key in result.removed_keys:
            self.values.pop(key, None)
        return RuntimeSnapshot(connected=True, inverter=self._inverter, values=dict(self.values))


class Pi30SettingReadbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_lowercase_option_reaches_write_and_fresh_readback(self):
        self.hub._pi30_flag_command_case = "lower"
        self.inverter.details["buzzer_enabled"] = False
        self.hub.values["buzzer_enabled"] = False
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "PEa" else "EaDbjkuvxyz"
        # A reconnect can clear the driver read cache; the entry preference is
        # still supplied at the next write, without changing the default map.
        self.hub._runtime_read_state.clear()
        result = await self.hub.async_write_capability("buzzer_enabled", True)
        self.assertIs(result, True)
        self.assertIs(self.hub.values["buzzer_enabled"], True)
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list], ["PEa", "QFLAG"])
        self.assertIs(self.inverter.details["buzzer_enabled"], False)

    async def test_lowercase_nak_is_not_retried_with_uppercase(self):
        self.hub._pi30_flag_command_case = "lower"
        self.session.request.side_effect = Pi30Error("nak")
        with self.assertRaises(Pi30Error):
            await self.hub.async_write_capability("buzzer_enabled", False)
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list], ["PDa"])

    async def test_explicit_lowercase_ack_does_not_bypass_readback(self):
        self.hub._pi30_flag_command_case = "lower"
        self.hub.values["lcd_backlight_enabled"] = True
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "PDx" else "EaxD"
        with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
            await self.hub.async_write_capability("lcd_backlight_enabled", False)
        self.assertIs(self.hub.values["lcd_backlight_enabled"], True)
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list], ["PDx", "QFLAG", "QFLAG"])

    async def asyncSetUp(self):
        self.inverter = _saved_binding()
        self.inverter.capabilities = load_driver_profile("pi30_ascii.json").capabilities
        await async_acquire_charge_current_controls(_session(), self.inverter, runtime_state={})
        self.hub = _Hub(self.inverter)
        self.session = _session({"MUCHGC020": "ACK", "QPIRI": QPIRI})
        self.session_patch = patch.object(pi30.Pi30Driver, "_session", return_value=self.session)
        self.session_patch.start()
        self.addCleanup(self.session_patch.stop)
        self.poll_patch = patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
            return_value=({"operating_mode_code": "L"}, frozenset(), {})))
        self.poll_patch.start()
        self.addCleanup(self.poll_patch.stop)

    async def test_fresh_changed_readback_confirms_without_label_in_numeric_telemetry(self):
        response = QPIRI.split()
        response[13] = "20"
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "MUCHGC020" else " ".join(response)
        result = await self.hub.async_write_capability("max_ac_charging_current", "20 A")
        self.assertEqual(result, "20 A")
        self.assertEqual(self.hub.values["max_ac_charging_current"], 20)
        self.assertEqual(self.inverter.details["max_ac_charging_current"], 10)
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list], ["MUCHGC020", "QPIRI"])
        # At most one more confirmation read; normal polling is not expanded forever.
        await self.hub.async_refresh()
        await self.hub.async_refresh()
        self.assertEqual(self.session.request.await_count, 3)

    async def test_ack_old_value_is_not_success_and_write_is_not_repeated(self):
        with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
            await self.hub.async_write_capability("max_ac_charging_current", "20 A")
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list],
                         ["MUCHGC020", "QPIRI", "QPIRI"])
        self.assertEqual(self.hub.values["max_ac_charging_current"], 10)

    async def test_read_failure_invalidates_even_cached_requested_value(self):
        self.hub.values["max_ac_charging_current"] = 20
        async def request(command):
            if command == "MUCHGC020":
                return "ACK"
            raise Pi30Error("request_timeout")
        self.session.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
            await self.hub.async_write_capability("max_ac_charging_current", "20 A")
        self.assertNotIn("max_ac_charging_current", self.hub.values)

    async def test_qflag_readback_after_accepted_boolean_write(self):
        self.session = _session({"PDX": "ACK", "QFLAG": "EaDx"})
        with patch.object(pi30.Pi30Driver, "_session", return_value=self.session):
            self.assertIs(await self.hub.async_write_capability("lcd_backlight_enabled", False), False)
        self.assertIs(self.hub.values["lcd_backlight_enabled"], False)
        self.assertEqual([c.args[0] for c in self.session.request.await_args_list], ["PDX", "QFLAG"])

    async def test_nak_never_schedules_readback_or_changes_cached_value(self):
        self.session.request.side_effect = Pi30Error("nak")
        with self.assertRaises(Pi30Error):
            await self.hub._driver.async_write_capability(
                None, self.inverter, "max_ac_charging_current", "20 A", runtime_state=self.hub._runtime_read_state)
        self.assertNotIn(pi30._SETTING_READBACK_KEY, self.hub._runtime_read_state)
        self.assertEqual(self.inverter.details["max_ac_charging_current"], 10)

    async def test_max_lowercase_flag_is_confirmed_only_by_fresh_qflag(self):
        self.inverter.capabilities = load_driver_profile("pi30_ascii/models/pi30_max.json").capabilities
        for readback, confirmed in (("EaDx", True), ("EaxD", False)):
            with self.subTest(readback=readback):
                self.hub._runtime_read_state.clear()
                self.hub.values["lcd_backlight_enabled"] = True
                session = _session({"PDx": "ACK", "QFLAG": readback})
                with patch.object(pi30.Pi30Driver, "_session", return_value=session):
                    if confirmed:
                        self.assertIs(await self.hub.async_write_capability("lcd_backlight_enabled", False), False)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
                            await self.hub.async_write_capability("lcd_backlight_enabled", False)
                commands = [c.args[0] for c in session.request.await_args_list]
                self.assertEqual(commands, ["PDx", "QFLAG"] if confirmed else ["PDx", "QFLAG", "QFLAG"])
                self.assertIs(self.hub.values["lcd_backlight_enabled"], not confirmed)

    async def test_max_nak_never_retries_uppercase_or_updates_cached_flag(self):
        self.inverter.capabilities = load_driver_profile("pi30_ascii/models/pi30_max.json").capabilities
        self.inverter.details["buzzer_enabled"] = True
        session = _session({"PDa": Pi30Error("nak")})
        with patch.object(pi30.Pi30Driver, "_session", return_value=session):
            with self.assertRaises(Pi30Error):
                await self.hub._driver.async_write_capability(
                    None, self.inverter, "buzzer_enabled", False,
                    runtime_state=self.hub._runtime_read_state,
                )
        self.assertEqual([c.args[0] for c in session.request.await_args_list], ["PDa"])
        self.assertNotIn(pi30._SETTING_READBACK_KEY, self.hub._runtime_read_state)
        self.assertIs(self.inverter.details["buzzer_enabled"], True)

    async def test_reset_rebind_and_cancellation_cannot_publish_pending_read(self):
        for mode in ("reset", "rebind", "cancel"):
            with self.subTest(mode=mode):
                cap = self.inverter.get_capability("max_ac_charging_current")
                state = {pi30._SETTING_READBACK_KEY: pi30._PendingSettingReadback(self.inverter, cap, "QPIRI", 1)}
                async def request(command):
                    if mode == "reset":
                        state.clear()
                    elif mode == "rebind":
                        self.inverter.capabilities = tuple(c for c in self.inverter.capabilities if c != cap)
                    else:
                        raise asyncio.CancelledError
                    return QPIRI
                session = SimpleNamespace(request=request)
                if mode == "cancel":
                    with self.assertRaises(asyncio.CancelledError):
                        await pi30._async_read_pending_setting(session, self.inverter, state)
                    self.assertEqual(state, {})
                else:
                    self.assertEqual(await pi30._async_read_pending_setting(session, self.inverter, state),
                                     ({}, frozenset(), {}))
                if mode == "rebind":
                    self.inverter.capabilities += (cap,)


if __name__ == "__main__":
    unittest.main()
