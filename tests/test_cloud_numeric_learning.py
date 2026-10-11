"""Protected numeric discovery is independent of model, voltage and provider."""
import copy
import unittest
from types import SimpleNamespace

from custom_components.eybond_local.cloud_numeric_learning import (
    classify_numeric_encoding, numeric_learning_candidates,
)
from custom_components.eybond_local.smartess_cloud import build_learn_settings_plan
from custom_components.eybond_local.dessmonitor_cloud import DessMonitorControlField
from custom_components.eybond_local.support.shadow_learning.dessmonitor_orchestrator import build_dessmonitor_learning_plan
from custom_components.eybond_local.support.shadow_learning.orchestrator import summarize_shadow_learning_attempts
from custom_components.eybond_local.support.shadow_learning.overlay_generator import _build_learned_capabilities
from custom_components.eybond_local.support.shadow_learning.review_model import unpromoted_control_rows, unpromoted_controls_markdown


def samples(probes, divisor=1):
    return [dict(requested_value=p["value"], numeric_evidence=p["numeric_evidence"],
                 observation={"values": [int(float(p["value"]) * divisor)]}) for p in probes]


class NumericLearningTests(unittest.TestCase):
    def test_no_range_is_not_a_blocker_or_a_fabricated_soc_limit(self):
        probes = numeric_learning_candidates(unit="%")
        self.assertEqual([p["value"] for p in probes], ["1", "2"])
        classification = classify_numeric_encoding(samples(probes))
        self.assertEqual(classification["value_kind"], "u16")
        self.assertNotIn("maximum", classification)
        self.assertNotIn("minimum", classification)

    def test_device_ranges_do_not_leak_between_voltage_families(self):
        for hint, bounds in (("20.0~30.0V", (200, 300)), ("40.0~60.0V", (400, 600))):
            with self.subTest(hint=hint):
                result = classify_numeric_encoding(samples(numeric_learning_candidates(hint=hint, unit="V"), 10))
                self.assertEqual((result["minimum"], result["maximum"]), bounds)
                self.assertEqual(result["divisor"], 10)

    def test_current_at_upper_boundary_samples_inward(self):
        probes = numeric_learning_candidates(hint="0 to 100", current="100")
        self.assertEqual([p["value"] for p in probes], ["100", "99"])

    def test_zero_and_one_are_numbers_not_a_switch(self):
        result = classify_numeric_encoding(samples(numeric_learning_candidates(hint="0~100")))
        self.assertEqual(result["value_kind"], "u16")

    def test_invalid_or_conflicting_hints_do_not_supply_limits(self):
        for hint in ("100~0", "20~30 or 40~60", "NaN~Infinity"):
            probes = numeric_learning_candidates(hint=hint)
            self.assertNotIn("minimum", probes[0]["numeric_evidence"])

    def test_insufficient_nonfinite_signed_offset_and_clamped_evidence_rejected(self):
        base = samples(numeric_learning_candidates())
        cases = [base[:1], [base[0], base[0]]]
        for raw in (0, 3, 65536, True):
            bad = copy.deepcopy(base)
            bad[1]["observation"]["values"] = [raw]
            cases.append(bad)
        for requested in ("NaN", "Infinity", "-2"):
            bad = copy.deepcopy(base)
            bad[1]["requested_value"] = requested
            cases.append(bad)
        for case in cases:
            self.assertIn("rejection_reason", classify_numeric_encoding(case))

    def test_single_point_range_is_evidence_not_a_number_control(self):
        self.assertEqual(classify_numeric_encoding(samples(numeric_learning_candidates(hint="5~5")))["rejection_reason"], "numeric_needs_distinct_samples")

    def test_smartess_and_dessmonitor_preserve_exact_field_id_and_numeric_metadata(self):
        field = dict(id="bat_eybond_read_83", name="Synthetic SOC threshold", unit="%")
        smartess = build_learn_settings_plan({"field": [field]}, field_ids=(), include_numeric=True)
        dess = build_dessmonitor_learning_plan((DessMonitorControlField(field_id=field["id"], title=field["name"], unit="%"),))
        for plan in (smartess, dess):
            self.assertEqual([p["value"] for p in plan], ["1", "2"])
            self.assertTrue(all(p["field_id"] == field["id"] for p in plan))
            attempts = []
            for i, item in enumerate(plan):
                attempts.append(dict(item, sequence_index=i, requested_value=item["value"], field_name=field["name"],
                                     observation=dict(register=1234, function_code=6, values=[i+1], unit=1)))
            correlation = summarize_shadow_learning_attempts(attempts=attempts)
            caps, summary = _build_learned_capabilities(source_profile=SimpleNamespace(capabilities=()), correlation=correlation, session_manifest={})
            self.assertEqual(len(caps), 1, summary)
            self.assertEqual(caps[0]["register"], 1234)  # Captured address, NOT cloud ID suffix.
            self.assertEqual(caps[0]["value_kind"], "u16")
            self.assertEqual(caps[0]["write_function"], 6)
            self.assertFalse(caps[0]["tested"])
            self.assertFalse(caps[0]["enabled_default"])

    def test_inconclusive_encoding_is_visible_not_silently_omitted(self):
        state = {"overlay": {"manifest": {"skipped_controls": [dict(field_id="soc", field_name="SOC threshold", reason="numeric_needs_distinct_samples")]}}}
        rows = unpromoted_control_rows(state)
        rendered = unpromoted_controls_markdown(rows, lambda key, fallback: fallback)
        self.assertIn("SOC threshold", rendered)
        self.assertIn("Not enough distinct", rendered)
