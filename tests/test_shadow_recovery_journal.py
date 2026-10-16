"""Recovery persistence cannot silently lose obligations or invent success."""

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from custom_components.eybond_local.support.shadow_learning import session, session_journal


class RecoveryJournalTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = session.build_shadow_learning_session_state(
            entry_id="entry-1", route_owner_id="shadow_learning:study-1",
            collector_pn="E5000020000000", trace_path="trace.jsonl",
            original_endpoint="iot.eybond.com,18899,TCP",
            proxy_endpoint="192.0.2.1,8899,TCP",
            upstream_endpoint="iot.eybond.com,18899,TCP", restore_required=True,
            started_at="2026-06-05T12:00:00+00:00", updated_at="2026-06-05T12:00:00+00:00",
            status="restore_failed",
        )
        session.save_shadow_learning_session_state(config_dir=self.root, state=self.state)

    def inspect(self, **kwargs):
        return session.inspect_shadow_learning_recovery(
            self.root, entry_id=kwargs.get("entry_id", self.state.entry_id),
            collector_pn=kwargs.get("collector_pn", self.state.collector_pn),
        )

    def test_verified_finalization_is_historical_and_owner_scoped(self):
        session.clear_shadow_learning_session_state(
            self.root, expected_state=self.state, outcome="endpoint_verified",
        )
        facts = self.inspect()
        self.assertEqual(facts["shadow_learning_recovery_disk_status"], "missing")
        self.assertEqual(facts["shadow_learning_recovery_last_finalization_outcome"], "endpoint_verified")
        self.assertNotIn("last_finalization", str(self.inspect(entry_id="another-entry")))
        for secret in (self.state.collector_pn, self.state.original_endpoint, self.state.route_owner_id):
            self.assertNotIn(secret, str(facts))

    def test_receipt_failure_keeps_recovery_obligation(self):
        with patch.object(session, "atomic_write_record", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                session.clear_shadow_learning_session_state(
                    self.root, expected_state=self.state, outcome="endpoint_verified",
                )
        self.assertEqual(session.load_shadow_learning_session_state(self.root), self.state)
        self.assertNotIn("last_finalization", str(self.inspect()))

    def test_failed_atomic_save_keeps_previous_complete_record(self):
        for operation in ("replace", "fsync"):
            with self.subTest(operation=operation):
                with patch.object(session_journal.os, operation, side_effect=OSError("disk full")):
                    with self.assertRaises(OSError):
                        session.save_shadow_learning_session_state(
                            config_dir=self.root, state=replace(self.state, status="restoring"),
                        )
                self.assertEqual(session.load_shadow_learning_session_state(self.root), self.state)
                self.assertEqual(list(session.shadow_learning_session_root(self.root).glob(".*.json.*")), [])

    def test_stale_finalizer_cannot_clear_newer_record(self):
        newer = replace(self.state, status="restoring")
        session.save_shadow_learning_session_state(config_dir=self.root, state=newer)
        with self.assertRaisesRegex(RuntimeError, "record_changed"):
            session.clear_shadow_learning_session_state(
                self.root, expected_state=self.state, outcome="endpoint_verified",
            )
        self.assertEqual(session.load_shadow_learning_session_state(self.root), newer)
        self.assertNotIn("last_finalization", str(self.inspect()))

    def test_unfinished_owner_cannot_be_overwritten(self):
        for updates in ({"entry_id": "entry-2"}, {"route_owner_id": "shadow_learning:study-2"},
                        {"collector_pn": "E5000099990002"}):
            with self.subTest(updates=updates), self.assertRaisesRegex(RuntimeError, "record_owned"):
                session.save_shadow_learning_session_state(config_dir=self.root, state=replace(self.state, **updates))
        self.assertEqual(session.load_shadow_learning_session_state(self.root), self.state)

    def test_corrupt_record_is_not_overwritten(self):
        session.active_shadow_learning_state_path(self.root).write_text("{broken")
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            session.save_shadow_learning_session_state(config_dir=self.root, state=self.state)

    def test_new_protection_intent_roundtrips_but_malformed_is_legacy(self):
        import json
        state = replace(self.state, protection_version=1, cloud_source="valuecloud", cloud_commands_possible=True)
        session.save_shadow_learning_session_state(config_dir=self.root, state=state)
        self.assertEqual(session.load_shadow_learning_session_state(self.root), state)
        path = session.active_shadow_learning_state_path(self.root)
        record = json.loads(path.read_text())
        record["cloud_commands_possible"] = "false"
        path.write_text(json.dumps(record))
        loaded = session.load_shadow_learning_session_state(self.root)
        self.assertEqual(loaded.protection_version, 0)
        self.assertTrue(loaded.cloud_commands_possible)

    def test_durable_command_intent_cannot_regress(self):
        session.save_shadow_learning_session_state(
            config_dir=self.root, state=replace(self.state, cloud_commands_possible=True),
        )
        with self.assertRaisesRegex(RuntimeError, "intent_regression"):
            session.save_shadow_learning_session_state(config_dir=self.root, state=self.state)

    def test_discard_and_unmutated_start_are_not_claimed_as_restoration(self):
        for outcome in ("explicit_discard", "start_not_mutated"):
            with self.subTest(outcome=outcome):
                session.save_shadow_learning_session_state(config_dir=self.root, state=self.state)
                session.clear_shadow_learning_session_state(self.root, expected_state=self.state, outcome=outcome)
                self.assertEqual(self.inspect()["shadow_learning_recovery_last_finalization_outcome"], outcome)

    def test_new_pending_state_keeps_last_finalization_separate(self):
        session.clear_shadow_learning_session_state(self.root, expected_state=self.state, outcome="endpoint_verified")
        session.save_shadow_learning_session_state(config_dir=self.root, state=self.state)
        facts = self.inspect()
        self.assertTrue(facts["shadow_learning_recovery_pending"])
        self.assertEqual(facts["shadow_learning_recovery_disk_status"], "current_entry")
        self.assertEqual(facts["shadow_learning_recovery_last_finalization_outcome"], "endpoint_verified")
