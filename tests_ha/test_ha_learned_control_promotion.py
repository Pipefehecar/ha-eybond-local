"""An existing learned entity survives builtin promotion and HA reload."""
from dataclasses import replace
from functools import lru_cache
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.metadata import profile_loader
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import decode_write_request
from custom_components.eybond_local.support.shadow_learning.overlay_generator import generate_shadow_learning_overlay_drafts
from custom_components.eybond_local.support.shadow_learning.review_model import build_activation_selection
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP

NAME = "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json"


@pytest.mark.parametrize("mode", ["auto", "full", "read_only"])
@pytest.mark.parametrize("with_overlay", [False, True])
async def test_existing_selection_and_entity_id_survive_promotion(hass, fake_runtime, monkeypatch, tmp_path, mode, with_overlay):
    from conftest import FakeRuntimeManager

    hass.config.config_dir = str(tmp_path)
    profile = load_driver_profile(NAME)
    previous = replace(profile, capabilities=tuple(c for c in profile.capabilities if not c.learned_control_aliases))
    target = ProbeTarget(1, 255, 1)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Aninerel ANL-4200T-24L-W-PRO", serial_number="",
        probe_target=target, variant_key="aninerel_anl_4200t_24l_w_pro",
        profile_name=NAME, register_schema_name=NAME,
        capabilities=previous.capabilities, capability_groups=profile.groups,
    )
    collector = CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN)
    registers = dict.fromkeys(range(700), 0)
    registers.update({341: 31, 342: 60, 343: 20, 351: 160})
    permitted = False
    frames = []

    class Transport(FixtureTransport):
        async def async_send_payload(self, payload, *, route):
            if payload[1] != 3:
                assert permitted, "Lifecycle must never write"
                frames.append(bytes(payload))
            return await super().async_send_payload(payload, route=route)

    transport = Transport(registers=registers, command_responses=None, probe_target=target)
    driver = SmgModbusDriver()
    upgraded = False
    original_raw_loader = profile_loader._load_raw_profile

    @lru_cache(maxsize=None)
    def versioned_raw_loader(path):
        raw = original_raw_loader(path)
        if not upgraded and path.name == Path(NAME).name:
            # Startup restoration also loads the builtin map, before the first
            # runtime snapshot. Simulate the old catalog there as well.
            return {**raw, "capabilities": [
                item for item in raw.get("capabilities", [])
                if not item.get("learned_control_aliases")
            ]}
        return raw

    monkeypatch.setattr(profile_loader, "_load_raw_profile", versioned_raw_loader)
    profile_loader.clear_profile_loader_cache()
    original_raw_loader.cache_clear()

    async def refresh(self, *, poll_interval=None):
        binding = replace(inverter, capabilities=profile.capabilities if upgraded else previous.capabilities)
        if self.overlay_applier:
            binding = self.overlay_applier(binding, collector)
        self.binding = binding
        result = await driver.async_read_values(transport, binding)
        return RuntimeSnapshot(
            connected=True, collector=collector, inverter=binding,
            values=result.diagnostics,
            telemetry=fold_driver_telemetry(TypedTelemetryFrame.empty(), driver_key=driver.key,
                                           values=result.values, replace=True),
        )

    async def write(self, key, value):
        return await driver.async_write_capability(transport, self.binding, key, value)

    monkeypatch.setattr(FakeRuntimeManager, "set_inverter_overlay_applier", lambda self, fn: setattr(self, "overlay_applier", fn), raising=False)
    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)
    entry = MockConfigEntry(domain=DOMAIN, version=5, unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}", data={
        "connection_type": "eybond", "connection_mode": "known_ip",
        "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
        "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
        "driver_hint": "auto", "control_mode": mode, "connection_strategy": "callback_on_demand",
        "endpoint_control_policy": "external", "proxy_enabled": False,
        "detected_driver": driver.key, "detected_model": inverter.model_name,
        "detected_serial": "", "detection_confidence": "high",
    }, options={"poll_interval": 30, "poll_mode": "auto"})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    try:
        await hass.async_block_till_done()

        def generate():
            matched = []
            for register, field, title in ((341, "bat_eybond_read_83", "Low DC Protection SOC In AC Mode"),
                                           (342, "bat_eybond_read_84", "Low DC Recovery SOC In AC Mode")):
                for value in (1, 2):
                    matched.append(dict(field_id=field, field_name=title, requested_value=str(value),
                                        value_source="current", numeric_evidence={"kind": "number", "unit": "%"},
                                        observation={"register": register, "function_code": 16,
                                                     "values": [value], "unit": 1, "devcode": 1, "devaddr": 255}))
            # Generate as the old builtin map did, before these controls existed.
            with patch("custom_components.eybond_local.support.shadow_learning.overlay_generator.load_driver_profile", return_value=previous):
                result = generate_shadow_learning_overlay_drafts(
                    config_dir=Path(hass.config.config_dir), source_profile_name=NAME, source_schema_name=NAME,
                    session_manifest={"session_id": "synthetic-promotion", "collector_pn": SYNTHETIC_COLLECTOR_PN,
                                      "protocol_adapter_key": "modbus_rtu", "write_response_mode": "exception"},
                    correlation={"matched": matched},
                )
            # Reproduce the archived legacy numeric contract, not today's
            # generator's stricter confirmation flag. Missing function means
            # the SMG driver's default FC16, not FC06.
            raw = json.loads(result.profile_path.read_text())
            for capability in raw["capabilities"]:
                capability["requires_confirm"] = False
                capability.pop("write_function", None)
            result.profile_path.write_text(json.dumps(raw), encoding="utf-8")
            return result

        draft = await hass.async_add_executor_job(generate)
        assert draft.generated_capability_count == 2
        key, excluded = "learned_bat_eybond_read_83_341", "learned_bat_eybond_read_84_342"
        selection = build_activation_selection(review_model=draft.manifest["review_model"],
            selections={"controls": {key: {"enabled": True}, excluded: {"enabled": False}}})
        if with_overlay:
            await entry.runtime_data.async_activate_device_scoped_overlay(
                profile_name=draft.manifest["output"]["profile_name"],
                register_schema_name=draft.manifest["output"]["schema_name"], selection=selection,
            )
        await hass.async_block_till_done()
        registry = er.async_get(hass)
        unique = f"{entry.entry_id}_number_{key}"
        entity_id = registry.async_get_entity_id("number", DOMAIN, unique)
        if with_overlay and mode != "read_only":
            assert entity_id is not None
            entity_id = registry.async_update_entity(entity_id, new_entity_id="number.my_existing_soc", name="My SOC").entity_id
        else:
            assert entity_id is None
        for builtin_key in ("low_dc_protection_soc_ac_mode", "low_dc_recovery_soc_ac_mode"):
            assert registry.async_get_entity_id("number", DOMAIN, f"{entry.entry_id}_number_{builtin_key}") is None
        upgraded = True
        profile_loader.clear_profile_loader_cache()
        original_raw_loader.cache_clear()
        for _ in range(2):
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()
            await entry.runtime_data.async_refresh()
            await hass.async_block_till_done()
            assert registry.async_get_entity_id("number", DOMAIN, unique) == entity_id
            for capability in profile.capabilities:
                if not capability.learned_control_aliases:
                    continue
                builtin_entity = registry.async_get_entity_id(
                    "number", DOMAIN, f"{entry.entry_id}_number_{capability.key}",
                )
                visible = mode == "full" and not (with_overlay and capability.register in (341, 342))
                assert bool(builtin_entity) == visible
                if builtin_entity:
                    assert float(hass.states.get(builtin_entity).state) == registers[capability.register]
            assert registry.async_get_entity_id("number", DOMAIN, f"{entry.entry_id}_number_{excluded}") is None
            if entity_id:
                assert registry.async_get(entity_id).name == "My SOC"
                assert float(hass.states.get(entity_id).state) == 31
            assert frames == []
        write_entity = entity_id or registry.async_get_entity_id(
            "number", DOMAIN, f"{entry.entry_id}_number_low_dc_protection_soc_ac_mode",
        )
        if write_entity:
            permitted = True
            await hass.services.async_call("number", "set_value", {"entity_id": write_entity, "value": 32}, blocking=True)
            assert float(hass.states.get(write_entity).state) == 32
            assert len(frames) == 1
            request = decode_write_request(frames[0])
            assert request.function_code == 16 and request.address == 341 and request.values == (32,)
        if mode == "full":
            # Register 351 is outside the base config read block. Its dedicated
            # readback and the real SMG encoder must work without a schema fork.
            current_entity = registry.async_get_entity_id(
                "number", DOMAIN, f"{entry.entry_id}_number_max_discharge_current_protection",
            )
            permitted = True
            await hass.services.async_call("number", "set_value", {"entity_id": current_entity, "value": 161}, blocking=True)
            assert float(hass.states.get(current_entity).state) == 161
            assert len(frames) == 2
            request = decode_write_request(frames[-1])
            assert request.function_code == 16 and request.address == 351 and request.values == (161,)
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        profile_loader.clear_profile_loader_cache()
        original_raw_loader.cache_clear()
