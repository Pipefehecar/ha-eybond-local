"""Durable intent tests, not qualification of a live cloud-write guard."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.connection.cloud_learning_protection import (
    CloudLearningHold,
    CloudLearningProtectionRequired,
)
from custom_components.eybond_local.support.shadow_learning.protection_store import (
    CloudLearningMutationFence,
    CloudLearningProtectionStore,
)
from custom_components.eybond_local.support.shadow_learning.cloud_dispatch import (
    async_dispatch_cloud_action,
)
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.runtime.hub.lifecycle import HubLifecycleMixin

PN = "V001020SYN62344022"
OTHER_PN = "V000405SYN94677058"


def _hold(**changes):
    return CloudLearningHold(**{
        "entry_id": "entry_1", "study_id": "study_1", "collector_pn": PN,
        "source": "smartess", "armed_at": "2026-10-15T12:00:00+00:00", **changes,
    })


class MutationFenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = CloudLearningProtectionStore(self.root)
        self.fence = CloudLearningMutationFence(self.root, PN)

    async def test_rechecks_absence_and_blocks_after_reload_and_readd(self):
        await self.fence.require_unheld()
        self.store.create(hold=_hold(entry_id="deleted_entry"))
        for fence in (self.fence, CloudLearningMutationFence(self.root, PN)):
            with self.assertRaisesRegex(CloudLearningProtectionRequired, "hold_pending"):
                await fence.require_unheld()
        await CloudLearningMutationFence(self.root, OTHER_PN).require_unheld()

    async def test_seen_hold_or_corruption_cannot_be_cleared_by_file_loss(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                self.store.create(hold=_hold())
                path = self.store._path(PN)
                if corrupt:
                    path.write_text("{broken", encoding="utf-8")
                fence = CloudLearningMutationFence(self.root, PN)
                reason = await fence.hold_reason()
                self.assertIn(reason, {"cloud_learning_hold_pending", "cloud_learning_hold_invalid"})
                path.unlink()
                self.assertEqual(await fence.hold_reason(), reason)

    async def test_invalid_identity_is_not_an_absent_hold(self):
        fence = CloudLearningMutationFence(self.root, "")
        self.assertEqual(await fence.hold_reason(), "cloud_learning_hold_identity_invalid")

    async def test_exact_no_request_receipt_retires_hold_without_erasing_evidence(self):
        hold = _hold()
        self.store.create(hold=hold)
        self.assertEqual(await self.fence.hold_reason(), "cloud_learning_hold_pending")
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.retire_no_request(hold)
        self.store.record_no_request(hold)
        self.store.record_no_request(hold)  # idempotent durable receipt
        with self.assertRaises(CloudLearningProtectionRequired):
            await self.fence.accept_no_request_retirement(hold)  # active hold remains
        self.store.retire_no_request(hold)
        self.assertEqual(await self.fence.hold_reason(), "cloud_learning_hold_pending")
        await self.fence.accept_no_request_retirement(hold)
        await self.fence.require_unheld()
        self.assertIsNone(self.store.load(PN))
        self.assertTrue(self.store._receipt_path(hold).exists())
        archive = self.store._root / "retired_holds" / self.store._receipt_path(hold).name
        self.assertEqual(CloudLearningHold.from_record(json.loads(archive.read_text())), hold)
        next_hold = _hold(study_id="next")
        self.store.create(hold=next_hold)
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.retire_no_request(hold)
        self.assertEqual(self.store.load(PN), next_hold)

    async def test_receipt_corruption_and_foreign_identity_do_not_release(self):
        hold = _hold()
        self.store.create(hold=hold)
        self.store.record_no_request(hold)
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.retire_no_request(_hold(study_id="foreign"))
        self.store._receipt_path(hold).write_text("{broken")
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.retire_no_request(hold)
        self.assertEqual(self.store.load(PN), hold)

    async def test_old_hold_check_cannot_reinstate_retired_generation(self):
        hold = _hold()
        self.store.create(hold=hold)
        started, release = threading.Event(), threading.Event()
        original = self.store.require_no_hold
        def delayed(**kwargs):
            started.set()
            release.wait(5)
            raise CloudLearningProtectionRequired("cloud_learning_hold_pending")
        with patch.object(self.fence._store, "require_no_hold", side_effect=delayed):
            task = asyncio.create_task(self.fence.require_unheld())
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            self.store.record_no_request(hold)
            self.store.retire_no_request(hold)
            # The release's fresh absence read must use the real disk, while
            # the previous read is still returning its old generation.
            self.fence._store.require_no_hold.side_effect = original
            await self.fence.accept_no_request_retirement(hold)
            release.set()
            await task
        await self.fence.require_unheld()

    async def test_inflight_absence_cannot_overwrite_concurrent_hold(self):
        started, release = threading.Event(), threading.Event()
        def absent_read(**_kwargs):
            started.set()
            release.wait(5)
        with patch.object(self.fence._store, "require_no_hold", side_effect=absent_read):
            task = asyncio.create_task(self.fence.require_unheld())
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                self.fence._blocked_reason = "cloud_learning_hold_pending"
            finally:
                release.set()
            with self.assertRaises(CloudLearningProtectionRequired):
                await task

    async def test_runtime_endpoint_apply_and_reboot_never_reach_adapter(self):
        self.store.create(hold=_hold())
        hub = HubManagementMixin()
        hub._link_manager = SimpleNamespace(owned_session_identity=(PN, "socket1"), owned_session_generation=1)
        hub._collector_mutation_guard = self.fence.require_unheld
        calls = []
        async def operation():
            calls.append(True)
            return "read-value"
        for name in ("write_endpoint", "apply", "reboot", "write_wifi", "write_uart"):
            with self.subTest(name=name), self.assertRaises(CloudLearningProtectionRequired):
                await hub._run_management_operation(name, operation)
            self.assertEqual(hub._last_management_operation["status"], "error")
        self.assertEqual(calls, [])
        self.assertEqual(await hub._run_management_operation("read_endpoint_state", operation), "read-value")
        self.assertEqual(calls, [True])

    async def test_runtime_rechecks_session_after_disk_await(self):
        from custom_components.eybond_local.collector.management import CollectorManagementTransportError
        hub = HubManagementMixin()
        hub._link_manager = SimpleNamespace(owned_session_identity=(PN, "socket1"), owned_session_generation=1)
        async def guard():
            await self.fence.require_unheld()
            hub._link_manager.owned_session_identity = (PN, "socket2")
        hub._collector_mutation_guard = guard
        async def operation():
            self.fail("Must not write through a changed session after disk IO")
        with self.assertRaises(CollectorManagementTransportError):
            await hub._run_management_operation("write_endpoint", operation)

    async def test_held_runtime_cannot_start_an_upstream_proxy_or_learning_route(self):
        self.store.create(hold=_hold())
        hub = HubLifecycleMixin()
        hub._collector_mutation_guard = self.fence.require_unheld
        # No link manager: any IO delegation before the fence would fail.
        params = dict(collector_ip="192.0.2.2", listen_port=18899,
                      upstream_host="cloud.invalid", upstream_port=18899,
                      output_path=self.root / "never-created.jsonl")
        with self.assertRaises(CloudLearningProtectionRequired):
            await hub.async_start_proxy_capture_route(**params)
        with self.assertRaises(CloudLearningProtectionRequired):
            await hub.async_start_shadow_learning_route(**params, seed=None)
        self.assertFalse(params["output_path"].exists())

    def test_runtime_route_ownership_requires_exact_live_entry_and_study(self):
        hub = HubLifecycleMixin()
        lease = SimpleNamespace(entry_id="entry", owner_id="study", mode="shadow_learning", state="running")
        hub._link_manager = SimpleNamespace(route_lease=lease)
        self.assertTrue(hub.shadow_learning_route_owned_by(entry_id="entry", owner_id="study"))
        for field, value in (("entry_id", "foreign"), ("owner_id", "new_study"),
                             ("mode", "proxy_capture"), ("state", "stopping")):
            prior = getattr(lease, field)
            setattr(lease, field, value)
            self.assertFalse(hub.shadow_learning_route_owned_by(entry_id="entry", owner_id="study"))
            setattr(lease, field, prior)
        for invalid in (None, "", " study", False):
            self.assertFalse(hub.shadow_learning_route_owned_by(entry_id="entry", owner_id=invalid))
        hub._link_manager.route_lease = None
        self.assertFalse(hub.shadow_learning_route_owned_by(entry_id="entry", owner_id="study"))


class HoldModelTests(unittest.TestCase):
    def test_strict_roundtrip_and_no_sensitive_repr(self):
        for source in ("smartess", "dessmonitor", "valuecloud"):
            hold = _hold(source=source)
            self.assertEqual(CloudLearningHold.from_record(hold.to_record()), hold)
            self.assertNotIn(PN, repr(hold))
            self.assertNotIn(hold.study_id, repr(hold))
            self.assertNotIn(hold.entry_id, repr(hold))
            self.assertEqual(set(hold.to_record()), {
                "schema_version", "entry_id", "study_id", "collector_pn", "source", "armed_at",
            })

    def test_invalid_constructor_fields_are_not_coerced(self):
        for key, values in {
            "entry_id": (None, True, "", "../entry", " entry", "a" * 97),
            "study_id": (None, 1, "", "study/1", "study\n"),
            "collector_pn": (None, 1, "short", PN + " ", "../private"),
            "source": (None, 1, "unknown", " smartess"),
            "armed_at": (None, 1, "", "yesterday", "2026-10-15T12:00:00", " 2026-10-15T12:00:00Z",
                         "2026-10-15\n12:00:00Z", "2026-10-15T12:00:00.1234567Z", "2026-99-99T12:00:00Z"),
        }.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    _hold(**{key: value})

    def test_unknown_schema_extra_fields_and_bool_version_fail_closed(self):
        record = _hold().to_record()
        bad = [None, [], {}, {**record, "password": "secret"}]
        bad += [{**record, "schema_version": value} for value in (True, "1", 1.0, 2)]
        bad += [{k: v for k, v in record.items() if k != field} for field in record]
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                CloudLearningHold.from_record(raw)


class HoldStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = CloudLearningProtectionStore(self.root)

    def test_durable_roundtrip_across_store_restart_and_private_permissions(self):
        hold = _hold()
        self.store.create(hold=hold)
        restarted = CloudLearningProtectionStore(self.root)
        self.assertEqual(restarted.load(PN), hold)
        restarted.require_exact_hold(hold)
        path = self.store._path(PN)
        self.assertNotIn(PN, str(path))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        with self.assertRaisesRegex(CloudLearningProtectionRequired, "pending"):
            restarted.require_no_hold(PN)

    def test_no_time_based_clear_and_no_entry_readd_bypass(self):
        hold = _hold(armed_at="2000-01-01T00:00:00+00:00")
        self.store.create(hold=hold)
        for other in (hold, replace(hold, entry_id="new_entry"), replace(hold, study_id="new_study")):
            with self.assertRaisesRegex(CloudLearningProtectionRequired, "exists"):
                self.store.create(hold=other)
        self.assertEqual(self.store.load(PN), hold)
        with self.assertRaisesRegex(CloudLearningProtectionRequired, "pending"):
            self.store.require_no_hold(PN)

    def test_different_collectors_are_independent(self):
        self.store.create(hold=_hold())
        self.assertIsNone(self.store.load(OTHER_PN))
        self.store.require_no_hold(OTHER_PN)
        other = _hold(collector_pn=OTHER_PN, study_id="other")
        self.store.create(hold=other)
        self.assertEqual(self.store.load(OTHER_PN), other)

    def test_corrupt_foreign_oversized_duplicate_or_unknown_record_blocks(self):
        path = self.store._path(PN)
        path.parent.mkdir(parents=True)
        foreign = _hold(collector_pn=OTHER_PN).to_record()
        raw = json.dumps(_hold().to_record())
        samples = (b"", b"{", b"null", b"\xff", b" " * 4097,
                   json.dumps(foreign).encode(),
                   raw.replace('"schema_version": 1', '"schema_version": 9').encode(),
                   raw.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1').encode())
        for payload in samples:
            with self.subTest(payload=payload[:50]):
                path.write_bytes(payload)
                with self.assertRaisesRegex(CloudLearningProtectionRequired, "invalid"):
                    self.store.require_no_hold(PN)
                with self.assertRaisesRegex(CloudLearningProtectionRequired, "exists"):
                    self.store.create(hold=_hold())
                self.assertEqual(path.read_bytes(), payload)

    def test_unreadable_record_is_not_absence(self):
        with patch("os.open", side_effect=PermissionError):
            with self.assertRaisesRegex(CloudLearningProtectionRequired, "unreadable"):
                self.store.require_no_hold(PN)

    def test_symlink_and_nonregular_file_block_without_following(self):
        path = self.store._path(PN)
        path.parent.mkdir(parents=True)
        path.symlink_to(self.root / "missing_target")
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.require_no_hold(PN)
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(CloudLearningProtectionRequired):
            self.store.require_no_hold(PN)

    def test_cached_owner_cannot_ignore_disappeared_or_replaced_record(self):
        hold = _hold()
        self.store.create(hold=hold)
        self.store._path(PN).unlink()
        with self.assertRaisesRegex(CloudLearningProtectionRequired, "changed"):
            self.store.require_exact_hold(hold)
        self.store.create(hold=replace(hold, study_id="foreign_study"))
        with self.assertRaisesRegex(CloudLearningProtectionRequired, "changed"):
            self.store.require_exact_hold(hold)

    def test_concurrent_owners_cannot_overwrite_one_another(self):
        gate = threading.Barrier(2)

        def arm(study):
            gate.wait(timeout=5)
            try:
                self.store.create(hold=_hold(study_id=study))
            except CloudLearningProtectionRequired:
                return False
            return True

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(arm, ("study_a", "study_b")))
        self.assertEqual(sorted(results), [False, True])
        self.assertIn(self.store.load(PN).study_id, {"study_a", "study_b"})

    def test_fsync_failure_never_grants_dispatch_permission(self):
        real_fsync = os.fsync
        # parent links, record, final containing directory are all required.
        for fail_at in range(1, 5):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as root:
                store = CloudLearningProtectionStore(Path(root))
                syncs = 0
                dispatched = []

                def sync(fd):
                    nonlocal syncs
                    syncs += 1
                    if syncs == fail_at:
                        raise OSError("private filesystem details")
                    real_fsync(fd)

                with patch("os.fsync", side_effect=sync):
                    with self.assertRaisesRegex(CloudLearningProtectionRequired, "persist_failed"):
                        store.create(hold=_hold())
                        dispatched.append(True)
                self.assertEqual(dispatched, [])
                if fail_at >= 3:
                    # A failed barrier is never repaired by deleting its hold.
                    self.assertTrue(store._path(PN).exists())
                    with self.assertRaises(CloudLearningProtectionRequired):
                        store.require_no_hold(PN)


class HoldCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def _cancel_worker(self, *, persist: bool, fail: bool):
        started, finish, completed = threading.Event(), threading.Event(), threading.Event()
        calls = []
        with tempfile.TemporaryDirectory() as root:
            store = CloudLearningProtectionStore(Path(root))
            create = store.create

            def worker(**kwargs):
                started.set()
                if not finish.wait(timeout=5):
                    raise TimeoutError("test_worker_not_released")
                try:
                    if persist:
                        create(**kwargs)
                    if fail:
                        raise OSError("private worker failure")
                    return "ok"
                finally:
                    completed.set()

            async def run():
                if persist:
                    await store.async_create(hold=_hold())
                else:
                    await async_dispatch_cloud_action(worker)
                calls.append("next_action")

            with patch.object(store, "create", side_effect=worker):
                task = asyncio.create_task(run())
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 3))
                    task.cancel()
                    await asyncio.sleep(0.01)
                    task.cancel()  # repeated cancellation must still drain
                    await asyncio.sleep(0.01)
                    self.assertFalse(task.done())
                    finish.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, timeout=3)
                    self.assertTrue(completed.is_set())
                    self.assertEqual(calls, [])
                    if persist:
                        self.assertEqual(store.load(PN), _hold())
                finally:
                    finish.set()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_cancelled_persistence_drains_and_never_dispatches(self):
        await self._cancel_worker(persist=True, fail=False)

    async def test_cancelled_persistence_with_late_error_preserves_cancellation(self):
        await self._cancel_worker(persist=True, fail=True)

    async def test_cancelled_http_with_late_error_preserves_cancellation(self):
        await self._cancel_worker(persist=False, fail=True)

    async def test_http_result_and_uncancelled_exception_are_unchanged(self):
        self.assertEqual(await async_dispatch_cloud_action(lambda: "response"), "response")
        with self.assertRaisesRegex(OSError, "test_error"):
            await async_dispatch_cloud_action(lambda: (_ for _ in ()).throw(OSError("test_error")))


if __name__ == "__main__":
    unittest.main()
