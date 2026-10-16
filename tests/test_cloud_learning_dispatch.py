"""Every async provider must honor the caller's owned dispatch boundary."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningProtectionRequired
from custom_components.eybond_local.support.shadow_learning.orchestrator import async_orchestrate_shadow_learning_settings
from custom_components.eybond_local.support.shadow_learning.dessmonitor_orchestrator import async_orchestrate_dessmonitor_shadow_learning
from custom_components.eybond_local.support.shadow_learning.valuecloud_orchestrator import async_orchestrate_valuecloud_shadow_learning
from custom_components.eybond_local.smartess_cloud import SessionCredentials
from custom_components.eybond_local.dessmonitor_cloud import DessMonitorControlField, DessMonitorDeviceIdentity, DessMonitorSession
from custom_components.eybond_local.valuecloud_cloud import ValueCloudSession

PN = "E50000200000000001"


class ProviderDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_async_providers_refuse_before_http_when_dispatch_owner_refuses(self):
        identity = dict(pn=PN, sn="synthetic", devcode=2304, devaddr=1)
        for provider in ("smartess", "dessmonitor", "valuecloud", "valuecloud_legacy"):
            with self.subTest(provider=provider):
                attempts = []

                def action(**kwargs):
                    self.fail("An unowned HTTP action escaped the dispatch boundary")

                async def refuse(callable_, **kwargs):
                    self.assertIs(callable_, action)
                    attempts.append(kwargs)
                    raise CloudLearningProtectionRequired("cloud_learning_hold_pending")

                common = dict(
                    confirm_cloud_write=True, shadow_session_state="ready",
                    is_session_ready=lambda: True, dispatch_action=refuse,
                    continue_on_error=True,
                )
                if provider == "smartess":
                    coroutine = async_orchestrate_shadow_learning_settings(
                        settings_dat={"field": [{"id": "mode", "name": "Mode", "item": [
                            {"key": "0", "val": "A"}, {"key": "1", "val": "B"},
                        ]}]}, session=SessionCredentials(token="token", secret="secret"),
                        **identity, dry_run=False, field_ids=[], include_numeric=False,
                        fetch_action=action, **common,
                    )
                elif provider == "dessmonitor":
                    coroutine = async_orchestrate_dessmonitor_shadow_learning(
                        control_fields=(DessMonitorControlField(
                            field_id="mode", title="Mode", choices=(("0", "A"), ("1", "B")),
                        ),), session=DessMonitorSession(token="token", secret="secret"),
                        identity=DessMonitorDeviceIdentity(**identity), action=action, **common,
                    )
                else:
                    coroutine = async_orchestrate_valuecloud_shadow_learning(
                        batch_control={"groups": [{"controlItemId": 10, "parameters": [{
                            "id": "mode", "detailsId": 20, "order": 1,
                            "name": "Mode", "readwrite": "RW", "item": {"0": "A", "1": "B"},
                        }]}]} if provider == "valuecloud" else None,
                        control_strategy={"fields": [{"id": "mode", "name": "Mode", "readwrite": "RW", "datatype": 3, "item": [
                            {"key": "0", "val": "A"}, {"key": "1", "val": "B"},
                        ]}]} if provider == "valuecloud_legacy" else None,
                        session=ValueCloudSession(token="token", secret="secret"), **identity,
                        dry_run=False, field_ids=[], setup_action=action,
                        legacy_setup_action=action, **common,
                    )
                with self.assertRaises(CloudLearningProtectionRequired):
                    await coroutine
                self.assertEqual(len(attempts), 1)
