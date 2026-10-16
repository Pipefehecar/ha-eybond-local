"""Cloud identity and endpoint recovery through the real HA options lifecycle."""

from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from homeassistant.data_entry_flow import FlowResultType

from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot
from custom_components.eybond_local.support.shadow_learning.session import (
    active_shadow_learning_state_path,
    build_shadow_learning_session_state,
)
from test_ha_config_flow import collector_entry
from synthetic import SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


def local_endpoint():
    return f"{SYNTHETIC_SERVER_IP},18899,TCP"


async def test_support_recovery_diagnostics_does_not_repair_or_trust_cache(
    hass, collector_entry, fake_runtime,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    path = active_shadow_learning_state_path(Path(hass.config.config_dir))
    try:
        coordinator._cached_shadow_learning_session_state = None
        coordinator._shadow_learning_session_state_loaded = True
        missing = await coordinator._async_shadow_learning_recovery_diagnostics()
        assert missing["shadow_learning_recovery_disk_status"] == "missing"
        assert missing["shadow_learning_recovery_cache_loaded"] is True
        await hass.async_add_executor_job(path.parent.mkdir, 511, True, True)
        await hass.async_add_executor_job(path.write_text, "{broken")
        broken = await coordinator._async_shadow_learning_recovery_diagnostics()
        assert broken["shadow_learning_recovery_disk_status"] == "invalid_json"
        assert coordinator._cached_shadow_learning_session_state is None
        assert coordinator._shadow_learning_session_state_loaded is True
        payload = coordinator._build_support_bundle_payload(shadow_learning_recovery=broken)
        assert payload["runtime"]["metadata"]["shadow_learning_recovery_disk_status"] == "invalid_json"
        assert await hass.async_add_executor_job(path.read_text) == "{broken"
    finally:
        await hass.async_add_executor_job(path.unlink, True)
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("family,host,provider,source", [
    ("valuecloud_at", "iot.eybond.com", "valuecloud", "valuecloud"),
    ("smartvalue_at", "m2m.eybond.com", "smartvalue", ""),
    ("smartess_at", "dtu_ess.eybond.com", "smartess", "smartess"),
])
async def test_local_redirect_does_not_change_cloud_api_picker(
    hass, collector_entry, fake_runtime, family, host, provider, source,
):
    hass.config_entries.async_update_entry(collector_entry, data={
        **collector_entry.data,
        "collector_cloud_family": "smartess_at",
        "control_mode": "full",
        "collector_original_server_endpoint": f"{host},18899,TCP",
        "collector_original_server_endpoint_profile_key": family,
        "collector_original_server_endpoint_source": "runtime_observed",
    })
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    coordinator.data = RuntimeSnapshot(
        collector=CollectorInfo(
            collector_pn=SYNTHETIC_COLLECTOR_PN,
            collector_server_endpoint=local_endpoint(),
            collector_cloud_family="smartess_at",
            collector_cloud_family_source="explicit_endpoint_port",
            collector_cloud_family_confidence="medium",
        ),
        values={
            "collector_server_endpoint": local_endpoint(),
            "collector_cloud_family": "smartess_at",
            "collector_cloud_family_source": "explicit_endpoint_port",
            "collector_cloud_family_confidence": "medium",
        },
    )
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    try:
        flow = hass.config_entries.options._progress[first["flow_id"]]
        assert coordinator.collector_cloud_family == family
        assert coordinator.cloud_evidence_provider == provider
        assert coordinator.collector_session_protocol == ""
        assert await coordinator.async_shadow_learning_start_blocker() == ""
        if source:
            result = await flow.async_step_shadow_learning({"learning_method": "active_correlation"})
            # Providers with a single source advance directly to consent.
            assert flow._control_discovery_default_learning_source(coordinator, "active_correlation") == source
            assert source in {item.source_id for item in flow._control_discovery_learning_sources(coordinator)}
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] in {"shadow_learning_source", "shadow_learning_consent"}
        else:
            # A preserved family without a registered active engine must not
            # acquire another provider's credentials/controls through its port.
            assert not any(method.method_id == "active_correlation" for method in flow._control_discovery_learning_methods(coordinator))
        await coordinator._async_remember_runtime_identity(coordinator.data)
        assert collector_entry.data["collector_cloud_family"] == family
        snapshot = await coordinator._async_prepare_runtime_snapshot_profile(coordinator.data)
        assert snapshot.collector.collector_cloud_family == family
        assert snapshot.values["collector_cloud_family"] == family
        assert snapshot.collector.collector_cloud_family_source == "endpoint_host"
        assert snapshot.values["collector_cloud_family_source"] == "endpoint_host"
        assert snapshot.collector.collector_cloud_family_confidence == "high"
        assert snapshot.values["collector_cloud_family_confidence"] == "high"
        assert coordinator.collector_session_protocol == ""
    finally:
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("status", ["restoring", "restore_failed"])
@pytest.mark.parametrize("entry_step", [
    "cloud_tools", "shadow_learning", "shadow_learning_source",
    "shadow_learning_consent", "shadow_learning_credentials", "shadow_learning_result",
])
async def test_pending_restore_blocks_learning_and_allows_explicit_recovery(
    hass, collector_entry, fake_runtime, monkeypatch, status, entry_step,
):
    hass.config_entries.async_update_entry(collector_entry, data={
        **collector_entry.data,
        "collector_cloud_family": "valuecloud_at", "control_mode": "full",
        "collector_original_server_endpoint": "iot.eybond.com,18899,TCP",
        "collector_original_server_endpoint_profile_key": "valuecloud_at",
    })
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    runtime = coordinator._runtime
    original = "iot.eybond.com,18899,TCP"
    state = build_shadow_learning_session_state(
        entry_id=collector_entry.entry_id,
        route_owner_id=f"shadow_learning:{collector_entry.entry_id}_recovery",
        collector_pn=SYNTHETIC_COLLECTOR_PN, trace_path="",
        original_endpoint=original, proxy_endpoint=local_endpoint(),
        upstream_endpoint=original, restore_required=True,
        started_at="2026-10-05T08:00:00+00:00",
        updated_at="2026-10-05T08:01:00+00:00", status=status,
        # This is known pre-command recovery, not an uncertain legacy run.
        protection_version=1, cloud_source="valuecloud", cloud_commands_possible=False,
    )
    await coordinator._async_save_shadow_learning_session_state(state)
    # Model a coordinator which has to load its recovery record from disk.
    coordinator._cached_shadow_learning_session_state = None
    coordinator._shadow_learning_session_state_loaded = False
    # Even a matching cached endpoint is not live recovery evidence.
    coordinator.data = replace(coordinator.data, values={"collector_server_endpoint": original})
    capture = AsyncMock()
    monkeypatch.setattr(runtime, "async_capture_support_evidence", capture)
    write = AsyncMock(side_effect=TimeoutError())
    monkeypatch.setattr(runtime, "async_set_collector_server_endpoint", write)
    read = AsyncMock(return_value={"current_endpoint": local_endpoint(), "reboot_required": "0"})
    monkeypatch.setattr(runtime, "async_get_collector_server_endpoint_state", read)
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    try:
        assert await coordinator.async_shadow_learning_start_blocker() == "shadow_learning_restore_pending"
        with pytest.raises(RuntimeError, match="^shadow_learning_restore_pending$"):
            await coordinator.async_start_shadow_learning()
        write.assert_not_awaited()
        capture.assert_not_awaited()
        flow = hass.config_entries.options._progress[first["flow_id"]]
        # Cold coordinator/deep-linked or stale wizard step: durable recovery
        # must win over an old consent and must not depend on snapshot values.
        coordinator._cached_shadow_learning_session_state = None
        coordinator._shadow_learning_session_state_loaded = False
        flow._shadow_learning_state.update(wizard_consent=True, wizard_credentials={"username": "synthetic"})
        result = await getattr(flow, f"async_step_{entry_step}")()
        assert result["step_id"] == "shadow_learning_result"
        assert "wizard_credentials" not in flow._shadow_learning_state
        assert "wizard_consent" not in flow._shadow_learning_state
        write.assert_not_awaited()
        capture.assert_not_awaited()
        actions = result["data_schema"].schema["result_action"].config["options"]
        assert [item["value"] for item in actions] == ["restore_connection", "create_support_package", "done"]
        result = await flow.async_step_shadow_learning_result({"result_action": "restore_connection"})
        assert result["errors"] == {}
        assert coordinator.shadow_learning_restore_pending
        assert coordinator._cached_shadow_learning_session_state.status == "restore_failed"
        assert "could not confirm" in result["description_placeholders"]["control_discovery_hint"]
        # The device has since restored its original endpoint. Fresh live reads
        # must finalize the old obligation without another write/apply.
        read.return_value = {"current_endpoint": original, "reboot_required": "0"}
        result = await flow.async_step_shadow_learning_result({"result_action": "restore_connection"})
        assert result["errors"] == {}
        assert not coordinator.shadow_learning_restore_pending
        assert coordinator._cached_shadow_learning_session_state is None
        assert await coordinator.async_shadow_learning_start_blocker() == ""
        assert "was restored" in result["description_placeholders"]["control_discovery_hint"]
        assert all(call.args == (original,) for call in write.await_args_list)
        write.assert_awaited_once()
        capture.assert_not_awaited()
    finally:
        # Leave the genuine persisted transaction finalized even on assertion failure.
        write.side_effect = None
        write.return_value = {"readback_endpoint": original, "apply_performed": True}
        read.return_value = {"current_endpoint": original, "reboot_required": "0"}
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)
