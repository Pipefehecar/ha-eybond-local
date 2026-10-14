"""Reviewed promotions preserve device selections, keys and wire contracts."""
from dataclasses import replace
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from custom_components.eybond_local.metadata.learned_control_aliases import retain_builtin_controls
from custom_components.eybond_local.metadata.profile_loader import _parse_capability, load_driver_profile
from custom_components.eybond_local.models import WriteCapability
from custom_components.eybond_local.schema import capability_write_exposure_allowed

PROFILE = "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json"


def pair():
    builtin = WriteCapability(
        key="canonical", register=60000, value_kind="u16", note="Synthetic",
        write_function=16, unit="%", learned_control_aliases=("learned_example_60000",),
    )
    learned = replace(builtin, key="learned_example_60000", write_function=None,
                      experimental=True, metadata_scope="device", learned_control_aliases=())
    return builtin, learned


class LearnedControlAliasTests(unittest.TestCase):
    def test_exact_alias_keeps_original_learned_object_and_value_key(self):
        builtin, learned = pair()
        self.assertEqual(retain_builtin_controls((builtin,), (learned,), driver_key="modbus_smg"), ())
        self.assertEqual(learned.value_key, "learned_example_60000")
        self.assertIsNone(learned.write_function)
        self.assertFalse(learned.tested)

    def test_register_alone_never_deduplicates(self):
        builtin, learned = pair()
        for driver, new, old in (
            ("pi30", builtin, learned),
            ("modbus_smg", replace(builtin, learned_control_aliases=()), learned),
            ("modbus_smg", builtin, replace(learned, key="learned_unrelated")),
            ("modbus_smg", builtin, replace(learned, experimental=False)),
        ):
            with self.subTest(driver=driver, key=old.key):
                self.assertEqual(retain_builtin_controls((new,), (old,), driver_key=driver), (new,))

    def test_different_contracts_are_not_aliases(self):
        builtin, learned = pair()
        changes = (
            dict(register=60001), dict(write_function=6), dict(word_count=2),
            dict(value_kind="scaled_u16", divisor=10), dict(multiplier=10),
            dict(minimum=5), dict(maximum=80), dict(unit="A"), dict(step=2),
            dict(bitmask=1), dict(command="QX"), dict(read_key="other"),
            dict(poll_readback=False), dict(requires_confirm=True),
            dict(reboot_required=True), dict(unsafe_while_running=True),
            dict(depends_on=("other",)), dict(combine="u16_le"),
        )
        for change in changes:
            with self.subTest(change=change):
                self.assertEqual(retain_builtin_controls(
                    (builtin,), (replace(learned, **change),), driver_key="modbus_smg"), (builtin,))

    def test_ambiguous_aliases_and_multiple_matches_do_not_hide_builtin(self):
        builtin, learned = pair()
        other = replace(builtin, key="another")
        both = (builtin, other)
        self.assertEqual(retain_builtin_controls(both, (learned,), driver_key="modbus_smg"), both)
        self.assertEqual(retain_builtin_controls((builtin,), (learned, learned), driver_key="modbus_smg"), (builtin,))

    def test_repeated_merge_and_no_overlay_preserve_controls(self):
        builtin, learned = pair()
        self.assertEqual(retain_builtin_controls((builtin,), (), driver_key="modbus_smg"), (builtin,))
        self.assertEqual(retain_builtin_controls((learned,), (learned,), driver_key="modbus_smg"), (learned,))

    def test_alias_parser_rejects_malformed_metadata(self):
        raw = dict(key="synthetic", register=60000, value_kind="u16")
        for aliases in (None, "learned_x", ["wrong"], ["learned_"], ["learned_x", "learned_x"], [1], [{}]):
            with self.subTest(aliases=aliases), self.assertRaises(ValueError):
                _parse_capability(dict(raw, learned_control_aliases=aliases), {}, {}, {})

    def test_exact_model_promotes_five_unverified_scalar_controls_only(self):
        profile = load_driver_profile(PROFILE)
        promoted = [cap for cap in profile.capabilities if cap.learned_control_aliases]
        self.assertEqual({cap.register for cap in promoted}, {341, 342, 343, 344, 351})
        for cap in promoted:
            self.assertEqual(cap.write_function, 16)
            self.assertFalse(cap.tested)
            self.assertIsNone(cap.minimum)
            self.assertIsNone(cap.maximum)
            for mode, expected in (("read_only", False), ("auto", False), ("full", True)):
                self.assertEqual(capability_write_exposure_allowed(cap, control_mode=mode, detection_confidence="high"), expected)
        self.assertEqual(profile.get_capability("op1_offgrid_soc_protection").register, 343)
        self.assertEqual(profile.get_capability("battery_low_cutoff_soc").register, 344)

    def test_explicit_exclusion_cannot_be_bypassed_by_new_builtin(self):
        builtin, learned = pair()
        merged = retain_builtin_controls((builtin,), (learned,), driver_key="modbus_smg") + (learned,)
        for mode in ("auto", "full"):
            self.assertFalse(any(capability_write_exposure_allowed(
                cap, control_mode=mode, detection_confidence="high", device_scoped_overlay_active=True,
                selected_control_keys=frozenset(),
            ) for cap in merged))
            self.assertTrue(capability_write_exposure_allowed(
                learned, control_mode=mode, detection_confidence="high", device_scoped_overlay_active=True,
                selected_control_keys=frozenset({learned.key}),
            ))
