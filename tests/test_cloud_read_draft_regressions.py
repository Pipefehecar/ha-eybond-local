"""Source attribution and exact native units through the inactive draft writer."""
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from test_cloud_local_history_correlation import _cloud_history, _history_collection, _local_series, FULL_PN
from custom_components.eybond_local.metadata.local_metadata import local_register_schemas_root
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema, set_external_register_schema_roots
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.register_decode import decode_block
from custom_components.eybond_local.support.cloud_local_history_correlation import build_cloud_local_history_review
from custom_components.eybond_local.support.cloud_local_history_representability import (
    build_cloud_local_history_representability_review, build_local_register_overlay_context,
)
from custom_components.eybond_local.support.cloud_local_history_draft import CloudLocalReadDraftPlan, build_cloud_local_read_draft_plan
from custom_components.eybond_local.support.cloud_local_history_draft_writer import generate_inactive_cloud_local_read_schema_draft, _native_transform

SCHEMA = "modbus_smg/models/smg_6200.json"


def plan_for(source="smartess", *, divisor=1000, unit="kW", negative=False, provider="smartess"):
    raw = (230, 240, 250, 260, 270)
    signed = tuple(-value if negative else value for value in raw)
    cloud = _cloud_history(values=tuple(str(Decimal(value) / divisor) for value in signed),
        title="Battery Power", series_key="battery_power", unit=unit, source_id=source,
        source_action="queryDeviceKeyParameterOneDay", field_kind="key_parameter")
    cloud = replace(cloud, provider_id=provider)
    local = _local_series(primary=tuple(value & 0xffff for value in signed), driver_key="modbus_smg", start=65000)
    review = build_cloud_local_history_review(_history_collection(cloud), local)
    schema = load_register_schema(SCHEMA)
    context = build_local_register_overlay_context(collector_pn=FULL_PN, driver_key=schema.driver_key,
        probe_target=ProbeTarget(2376, 1, 1), register_schema_name=SCHEMA, register_schema=schema)
    return build_cloud_local_read_draft_plan(build_cloud_local_history_representability_review(review, context))


class CloudReadDraftRegressionTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        set_external_register_schema_roots((local_register_schemas_root(self.root),))
        self.addCleanup(set_external_register_schema_roots, ())

    def write(self, plan):
        self.assertEqual(plan.item_count, 1)
        return generate_inactive_cloud_local_read_schema_draft(config_dir=self.root, source_schema_name=SCHEMA, plan=plan)

    def test_source_and_provider_survive_plan_file_names_and_manifest_without_activation(self):
        for source, label in (("smartess", "SmartESS"), ("dessmonitor", "DESSMonitor")):
            with self.subTest(source=source):
                plan = plan_for(source)
                record = plan.to_record()
                self.assertEqual(record["source_id"], source)
                self.assertEqual(record["provider_id"], "smartess")
                self.assertEqual(CloudLocalReadDraftPlan.from_record(record, representability=plan.representability), plan)
                for key in ("source_id", "provider_id"):
                    self.assertIsNone(CloudLocalReadDraftPlan.from_record(
                        {**record, key: "foreign"}, representability=plan.representability))
                artifact = self.write(plan)
                raw = json.loads(artifact.schema_path.read_text())
                self.assertEqual(artifact.manifest["source_id"], source)
                self.assertEqual(artifact.manifest["provider_id"], "smartess")
                self.assertIn(f"learned/{source}_review/", artifact.schema_name)
                self.assertIn(label, raw["title"])
                self.assertTrue(raw["measurement_descriptions"][0]["key"].startswith(f"learned_read_{source}_"))
                self.assertNotIn("shadow_learning_overlay", raw)
                self.assertFalse(artifact.manifest["activation_allowed"])
                self.assertFalse(artifact.manifest["local_mapping_proven"])
                self.assertFalse(raw["measurement_descriptions"][0]["enabled_default"])
        first = self.write(plan_for(provider="smartess"))
        second = self.write(plan_for(provider="another-provider"))
        self.assertNotEqual(first.evidence_sha256, second.evidence_sha256)

    def test_kw_correlation_is_emitted_and_really_decoded_as_watts(self):
        for divisor in (1, 10, 100, 1000):
            for negative in (False, True):
                with self.subTest(divisor=divisor, negative=negative):
                    plan = plan_for(divisor=divisor, negative=negative)
                    original = plan.to_record()
                    artifact = self.write(plan)
                    schema = load_register_schema(artifact.schema_name)
                    row, = artifact.manifest["generated_reads"]
                    spec, = (s for s in schema.spec_set(row["spec_set"]) if s.key == row["key"])
                    word = (-230 if negative else 230) & 0xffff
                    actual = decode_block(65000, [word], (spec,))[spec.key]
                    expected = (-230 if negative else 230) * (1000 // divisor)
                    self.assertEqual(actual, expected)
                    measurement, = (m for m in schema.measurement_descriptions if m.key == spec.key)
                    self.assertEqual(measurement.unit, "W")
                    self.assertEqual(row["divisor"], 1)
                    self.assertEqual(plan.to_record(), original)  # Cloud kW proof is not rewritten.
                    self.assertEqual(artifact.manifest["plan"]["items"][0]["candidate"]["divisor"], divisor)
                    if divisor == 1000 and not negative:
                        self.assertEqual(plan.representability.review.history_collection.series[0].points[0].value, "0.23")
                        self.assertEqual(actual, 230)  # 0.230 kW -> 230 W, not 0.23 W.

    def test_native_watts_are_not_rescaled_and_other_conflicts_are_not_guessed(self):
        plan = plan_for(divisor=1, unit="W")
        artifact = self.write(plan)
        schema = load_register_schema(artifact.schema_name)
        row, = artifact.manifest["generated_reads"]
        spec, = (s for s in schema.spec_set(row["spec_set"]) if s.key == row["key"])
        self.assertEqual(decode_block(65000, [230], (spec,))[spec.key], 230)
        item, = plan_for().items
        for unit in ("MW", "Wh", "V"):
            with self.assertRaisesRegex(ValueError, "unit_transform_unproven"):
                _native_transform(replace(item, semantic=replace(item.semantic, observed_unit=unit)))
        with self.assertRaisesRegex(ValueError, "unit_conflict_incomplete"):
            replace(item.semantic, observed_unit="")


if __name__ == "__main__":
    unittest.main()
