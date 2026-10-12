"""Unknown battery setpoint limits are not guessed from a shared register map."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.metadata import profile_loader
from custom_components.eybond_local.models import WriteCapability


KEYS = (
    "battery_bulk_voltage", "battery_float_voltage", "battery_under_voltage",
    "battery_under_voltage_off_grid", "battery_redischarge_voltage",
    "battery_overvoltage_protection_voltage", "battery_equalization_voltage",
)


class NumericCapabilityBoundsTests(unittest.TestCase):
    def test_family_and_previous_workarounds_have_no_guessed_limits(self):
        for name in (
            "protocols/classic_smg_rs232_v1.json", "protocols/communication_protocol_1.json",
            "protocols/communication_protocol_11.json", "models/smg_variant_4200.json",
            "models/anenji_4200_protocol_1.json", "models/aninerel_anl_4200t_24l_w_pro.json",
            "models/smg_6200.json",
        ):
            profile = profile_loader.load_driver_profile(f"modbus_smg/{name}")
            for key in KEYS:
                with self.subTest(profile=name, key=key):
                    cap = profile.get_capability(key)
                    self.assertIsNone(cap.minimum)
                    self.assertIsNone(cap.maximum)
                    self.assertEqual((cap.number_minimum, cap.number_maximum), (0, 6553.5))
                    for value in (26.8, 27.9, 56.2, 70.1):
                        self.assertEqual(encode_capability_words(cap, value), [round(value * 10)])

    def test_existing_external_overlay_inherits_without_regeneration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles" / "overlay.json"
            path.parent.mkdir()
            path.write_text(json.dumps({
                "extends": "modbus_smg/models/smg_variant_4200.json",
                "profile_key": "test_overlay", "capabilities": [],
            }))
            profile_loader.set_external_profile_roots((path.parent,))
            try:
                profile = profile_loader.load_driver_profile("overlay.json")
                cap = profile.get_capability("battery_bulk_voltage")
                self.assertEqual(cap.register, 324)
                self.assertIsNone(cap.minimum)
                self.assertEqual(encode_capability_words(cap, 27.9), [279])
            finally:
                profile_loader.set_external_profile_roots(())
                profile_loader.load_driver_profile.cache_clear()

    def test_explicit_profile_limits_are_still_enforced(self):
        base = profile_loader.load_driver_profile("modbus_smg/models/smg_variant_4200.json")
        cap = replace(base.get_capability("battery_bulk_voltage"), minimum=400, maximum=650)
        self.assertEqual((cap.number_minimum, cap.number_maximum), (40, 65))
        self.assertEqual(encode_capability_words(cap, 56.2), [562])
        for value in (27.9, 65.1):
            with self.assertRaises(ValueError): encode_capability_words(cap, value)

    def test_encoding_bounds_remain_without_profile_limits(self):
        for kind, divisor, multiplier, mask, ceiling in (
            ("scaled_u16", 10, None, None, 6553.5),
            ("scaled_u16", None, 10, None, 655350),
            ("u16", None, None, None, 65535),
            ("u32", None, None, None, 4294967295),
            ("u16", None, None, 0x00F0, 15),
        ):
            cap = WriteCapability(key="test", register=1, value_kind=kind, note="",
                divisor=divisor, multiplier=multiplier, bitmask=mask,
                word_count=2 if kind == "u32" else 1,
                combine="u32_high_first" if kind == "u32" else "u16")
            with self.subTest(kind=kind, multiplier=multiplier, mask=mask):
                self.assertEqual(cap.number_maximum, ceiling)
                encode_capability_words(cap, ceiling)
                for invalid in (-1, ceiling + cap.native_step):
                    with self.assertRaises(ValueError): encode_capability_words(cap, invalid)

    def test_nonfinite_scaled_values_fail_before_io(self):
        cap = WriteCapability(key="test", register=1, value_kind="scaled_u16", note="", divisor=10)
        for invalid in (float('nan'), float('inf'), -float('inf'), 1e308, -0.01):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                encode_capability_words(cap, invalid)


if __name__ == "__main__": unittest.main()
