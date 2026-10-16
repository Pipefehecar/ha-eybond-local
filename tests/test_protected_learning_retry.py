"""Protected retry never retires a hold or releases endpoint ownership."""

from dataclasses import replace, asdict
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.connection.collector_endpoint_operation import (
    CollectorEndpointOperationAuthority, OPERATION_SHADOW_LEARNING, OPERATION_PROXY_CAPTURE,
)
from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
from custom_components.eybond_local.connection.protected_learning_route import ProtectedLearningRoutePermit
from custom_components.eybond_local.support.shadow_learning.protection_store import CloudLearningProtectionStore
from custom_components.eybond_local.support.shadow_learning import session


class RetryJournalTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.hold = CloudLearningHold("entry", "original", "V001020SYN62344022", "smartess", "2026-10-16T12:00:00+00:00")
        self.store = CloudLearningProtectionStore(self.root)
        self.store.create(hold=self.hold)
        self.previous = session.build_shadow_learning_session_state(
            entry_id="entry", route_owner_id="shadow_learning:original", collector_pn=self.hold.collector_pn,
            trace_path="original.jsonl", original_endpoint="cloud.invalid,18899,TCP",
            proxy_endpoint="192.0.2.1,8899,TCP", upstream_endpoint="cloud.invalid,18899,TCP",
            restore_required=True, started_at=self.hold.armed_at, updated_at=self.hold.armed_at,
            status="restore_failed", protection_version=1, cloud_source="smartess", cloud_commands_possible=True,
        )
        session.save_shadow_learning_session_state(config_dir=self.root, state=self.previous)
        self.next = replace(self.previous, route_owner_id="shadow_learning:retry", trace_path="retry.jsonl",
                            original_endpoint=self.previous.proxy_endpoint, restore_required=False,
                            cloud_source="dessmonitor", status="preflight")

    def rollover(self, **kwargs):
        return session.replace_held_shadow_learning_session(config_dir=self.root, previous=self.previous,
            state=kwargs.get("state", self.next), hold=self.hold)

    def test_archive_before_replacement_and_hold_unchanged(self):
        self.rollover()
        self.assertEqual(session.load_shadow_learning_session_state(self.root), self.next)
        archives = list((session.shadow_learning_session_root(self.root)/"retained_runs").glob("*.json"))
        self.assertEqual(len(archives), 1)
        self.assertEqual(json.loads(archives[0].read_text()), asdict(self.previous))
        self.assertEqual(self.store.load(self.hold.collector_pn), self.hold)
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            session.save_shadow_learning_session_state(config_dir=self.root, state=self.previous)

    def test_invalid_retry_cannot_overwrite(self):
        for updates in ({"cloud_commands_possible": False}, {"collector_pn": "V000405SYN94677058"},
                        {"original_endpoint": self.previous.original_endpoint}, {"restore_required": True},
                        {"route_owner_id": self.previous.route_owner_id}, {"upstream_endpoint": "other.invalid,8899,TCP"}):
            with self.subTest(updates=updates), self.assertRaisesRegex(RuntimeError, "retry_record_mismatch"):
                self.rollover(state=replace(self.next, **updates))
            self.assertEqual(session.load_shadow_learning_session_state(self.root), self.previous)

    def test_archive_failure_retains_old_journal_and_hold(self):
        with patch.object(session, "atomic_write_record", side_effect=OSError("full")):
            with self.assertRaises(OSError):
                self.rollover()
        self.assertEqual(session.load_shadow_learning_session_state(self.root), self.previous)
        self.assertEqual(self.store.load(self.hold.collector_pn), self.hold)

    def test_atomic_shadow_handoff_rejects_stale_and_foreign_tokens(self):
        authority = CollectorEndpointOperationAuthority()
        old = authority.acquire("entry", OPERATION_SHADOW_LEARNING, owner_ref="old").token
        new = authority.handoff_shadow_learning(old, "new")
        self.assertFalse(authority.owns(old))
        self.assertTrue(authority.owns(new))
        self.assertFalse(authority.release("entry", old))
        with self.assertRaises(RuntimeError):
            authority.handoff_shadow_learning(old, "other")
        self.assertFalse(authority.acquire("entry", OPERATION_PROXY_CAPTURE).acquired)


class RoutePermitTests(unittest.IsolatedAsyncioTestCase):
    async def test_hub_accepts_only_validated_single_use_retry_and_keeps_other_routes_fenced(self):
        from custom_components.eybond_local.runtime.hub.lifecycle import HubLifecycleMixin

        hub = HubLifecycleMixin()
        hub._collector_mutation_guard = AsyncMock(side_effect=RuntimeError("held"))
        start = AsyncMock()
        hub._link_manager = SimpleNamespace(async_start_shadow_learning_route=start)
        validate = AsyncMock()
        permit = ProtectedLearningRoutePermit("entry", "owner", "pn", 8899, "cloud.invalid", 18899, validate)
        args = dict(entry_id="entry", owner_id="owner", collector_pn="pn", listen_port=8899,
                    upstream_host="cloud.invalid", upstream_port=18899,
                    collector_ip="192.0.2.2", output_path=Path("unused.jsonl"), seed=None)
        with self.assertRaisesRegex(RuntimeError, "held"):
            await hub.async_start_shadow_learning_route(**args)
        with self.assertRaisesRegex(RuntimeError, "permit_invalid"):
            await hub.async_start_shadow_learning_route(**args, protected_retry_permit=SimpleNamespace(consume=AsyncMock()))
        start.assert_not_awaited()
        await hub.async_start_shadow_learning_route(**args, protected_retry_permit=permit)
        validate.assert_awaited_once()
        start.assert_awaited_once_with(**args, expected_session_protocol="")
        with self.assertRaisesRegex(RuntimeError, "route_mismatch"):
            await hub.async_start_shadow_learning_route(**args, protected_retry_permit=permit)
        with self.assertRaisesRegex(RuntimeError, "held"):
            await hub.async_start_proxy_capture_route(**{k:v for k,v in args.items() if k != "seed"})
        start.assert_awaited_once()

    async def test_permit_is_single_use_and_exact_route(self):
        validate = AsyncMock()
        permit = ProtectedLearningRoutePermit("entry", "owner", "pn", 8899, "cloud.invalid", 18899, validate)
        args = dict(entry_id="entry", owner_id="owner", collector_pn="pn", listen_port=8899,
                    upstream_host="cloud.invalid", upstream_port=18899)
        with self.assertRaisesRegex(RuntimeError, "route_mismatch"):
            await permit.consume(**dict(args, owner_id="stale"))
        validate.assert_not_awaited()
        await permit.consume(**args)
        validate.assert_awaited_once()
        with self.assertRaisesRegex(RuntimeError, "route_mismatch"):
            await permit.consume(**args)


class RetryEvidenceTests(unittest.TestCase):
    def test_only_consistent_distinct_samples_can_build_retry_drafts(self):
        from custom_components.eybond_local.support.shadow_learning.retry_evidence import qualify_retry_evidence
        def item(field, value, register=300):
            return dict(field_id=field, requested_value=str(value), observation=dict(register=register, values=[value]))
        source = {"correlation": {"matched": [item("good", 0), item("good", 1), item("single", 1),
                    item("mixed", 0), item("mixed", 1, 301)]}}
        qualified = qualify_retry_evidence(source)["correlation"]
        self.assertEqual([x["field_id"] for x in qualified["matched"]], ["good", "good"])
        self.assertEqual(qualified["retry_quarantined_count"], 3)
        self.assertEqual(len(source["correlation"]["matched"]), 5)
        for change in ({"correlation": {**source["correlation"], "unmatched_writes": [{}]}},
                       {**source, "results": [{"field_id": "good", "observation_count": 2}]}):
            self.assertEqual(qualify_retry_evidence(change)["correlation"]["matched"], [])
