"""Real HA number validation must not impose a guessed battery class."""
from dataclasses import replace
import logging
from unittest.mock import AsyncMock

import pytest
from homeassistant.components.number import DATA_COMPONENT
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.number import EybondCapabilityNumber


@pytest.mark.parametrize('key,value', [('battery_bulk_voltage', 27.9), ('battery_float_voltage', 26.8)])
@pytest.mark.parametrize('explicit_limits', [False, True])
async def test_number_service_unknown_and_explicit_bounds(hass, key, value, explicit_limits):
    profile = load_driver_profile('modbus_smg/models/smg_variant_4200.json')
    cap = profile.get_capability(key)
    if explicit_limits:
        cap = replace(cap, minimum=400, maximum=650)
    inverter = DetectedInverter(driver_key='modbus_smg', protocol_family='modbus_smg',
        model_name='Synthetic inverter', serial_number='', probe_target=ProbeTarget(1, 255, 1),
        capabilities=(cap,))
    entry = MockConfigEntry(domain='eybond_local')
    entry.add_to_hass(hass)
    coordinator = DataUpdateCoordinator(hass, logging.getLogger(__name__), name='bounds', config_entry=entry)
    coordinator.capability_enabled_by_default = lambda capability: True
    coordinator.inverter_device_info = lambda: None
    coordinator.async_write_capability = AsyncMock(
        side_effect=lambda key, value: encode_capability_words(cap, value))
    coordinator.async_set_updated_data(RuntimeSnapshot(connected=True, inverter=inverter,
        values={key: value, 'battery_connected': True, 'charging_inactive': True}))
    assert await async_setup_component(hass, 'number', {})
    component = hass.data[DATA_COMPONENT]
    entity = EybondCapabilityNumber(coordinator, cap)
    entity.entity_id = f'number.{key}'
    await component.async_add_entities([entity])
    state = hass.states.get(entity.entity_id)
    assert state is not None
    assert state.attributes['min'] == (40 if explicit_limits else 0)
    assert state.attributes['max'] == (65 if explicit_limits else 6553.5)
    assert state.attributes['minimum_source'] == ('profile' if explicit_limits else 'encoding')
    try:
        if explicit_limits:
            with pytest.raises(HomeAssistantError):
                await hass.services.async_call('number', 'set_value',
                    {'entity_id': entity.entity_id, 'value': value}, blocking=True)
            coordinator.async_write_capability.assert_not_awaited()
        else:
            await hass.services.async_call('number', 'set_value',
                {'entity_id': entity.entity_id, 'value': value}, blocking=True)
            coordinator.async_write_capability.assert_awaited_once_with(key, value)
    finally:
        await entity.async_remove()
