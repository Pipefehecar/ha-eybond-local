"""Reviewed legacy receipts never mutate endpoints or silently discard intent."""
from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from tools.review_learning_no_request import record_review
from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
from custom_components.eybond_local.support.shadow_learning.session import (
    build_shadow_learning_session_state, load_shadow_learning_session_state,
    save_shadow_learning_session_state,
)


class ReviewTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.hold = CloudLearningHold(entry_id="entry", study_id="study", collector_pn="E50000200000000001",
                                      source="smartess", armed_at="2026-10-16T10:00:00Z")
        self.store = CloudLearningProtectionStore(self.root)
        self.store.create(hold=self.hold)
        self.state = build_shadow_learning_session_state(
            entry_id="entry", route_owner_id="shadow_learning:study", collector_pn=self.hold.collector_pn,
            trace_path="synthetic.jsonl", original_endpoint="cloud.invalid,18899,TCP",
            proxy_endpoint="192.0.2.1,8899,TCP", upstream_endpoint="cloud.invalid,18899,TCP",
            restore_required=True, started_at=self.hold.armed_at, updated_at=self.hold.armed_at,
            status="restore_failed", protection_version=1, cloud_source="smartess", cloud_commands_possible=True,
        )
        save_shadow_learning_session_state(config_dir=self.root, state=self.state)
        self.evidence = self.root / "reviewed.log"
        self.evidence.write_text("synthetic full run reviewed by maintainer")

    def review(self, **changes):
        kwargs = dict(config_dir=self.root, collector_pn=self.hold.collector_pn,
                      study_id=self.hold.study_id, evidence_file=self.evidence, confirmed=True)
        kwargs.update(changes)
        return record_review(**kwargs)

    def test_receipt_retains_original_hold_and_journal_until_normal_recovery(self):
        digest = self.review()
        self.assertEqual(digest, hashlib.sha256(self.evidence.read_bytes()).hexdigest())
        self.store.require_no_request_receipt(self.hold)
        self.assertEqual(self.store.load(self.hold.collector_pn), self.hold)
        self.assertEqual(load_shadow_learning_session_state(self.root), self.state)

    def test_requires_explicit_review_and_exact_study(self):
        for changes in ({"confirmed": False}, {"confirmed": 1}, {"study_id": "other"},
                        {"collector_pn": "V000405SYN94677058"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.review(**changes)
        self.assertFalse(self.store._receipt_path(self.hold).exists())

    def test_running_or_legacy_session_is_not_reviewable(self):
        for changes in ({"status": "learning"}, {"status": "ready"}, {"protection_version": 0}):
            save_shadow_learning_session_state(config_dir=self.root, state=replace(self.state, **changes))
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.review()
        self.assertFalse(self.store._receipt_path(self.hold).exists())

    def test_empty_evidence_is_not_a_review(self):
        self.evidence.write_text("")
        with self.assertRaises(ValueError):
            self.review()
        self.assertFalse(self.store._receipt_path(self.hold).exists())
