"""Device-advertised PI30 charge-current controls, not a retail model guess.

PI30 Communication Protocol 2015-09-24, sections 2.16/2.17 and 3.3/3.5,
defines selectable-value queries and the corresponding three-digit writes.
Only missing controls are enriched after positive protocol identification.
Existing model-specific controls (including their tested status) are preserved.
"""

from __future__ import annotations

import asyncio
from asyncio import CancelledError
from collections.abc import Callable
from dataclasses import dataclass
import re

from ..models import CapabilityChoice, DetectedInverter, WriteCapability
from ..payload.pi30 import Pi30Error, parse_qpiri


_CONTROLS = (
    ("max_charging_current", "QMCHGCR", "MCHGC", "Max Total Charge Current"),
    ("max_ac_charging_current", "QMUCHGCR", "MUCHGC", "Max Utility Charge Current"),
)

_ACQUISITION_STATE = "pi30_charge_current_acquisition"
_ACQUISITION_TIMEOUT = 3.0
_ACQUISITION_MAX_ATTEMPTS = 3


async def _request_parsed(
    session, command: str, parser: Callable, *, timeout: float,
    command_timings: list[tuple[str, int, str]] | None,
):
    """Measure request AND parse; an invalid payload is never a success."""

    loop = asyncio.get_running_loop()
    started = loop.time()
    outcome = "error"
    try:
        value = parser(await asyncio.wait_for(session.request(command), timeout=timeout))
        outcome = "ok"
        return value
    except TimeoutError:
        outcome = "timeout"
        raise
    except Pi30Error as exc:
        # Pi30Session normalizes a transport timeout into this protocol error.
        outcome = "timeout" if str(exc) == "request_timeout" else "error"
        raise
    except CancelledError:
        outcome = "cancelled"
        raise
    finally:
        if command_timings is not None:
            command_timings.append((command, int(round((loop.time() - started) * 1000)), outcome))


@dataclass(slots=True)
class _Acquisition:
    binding: tuple
    attempts: int = 0
    complete: bool = False
    in_progress: bool = False


def _binding(inverter: DetectedInverter) -> tuple:
    return (
        inverter.driver_key, inverter.protocol_family, inverter.variant_key,
        inverter.serial_number, inverter.probe_target,
        inverter.profile_name, inverter.register_schema_name,
    )


def charge_current_acquisition_diagnostics(runtime_state: dict | None) -> dict:
    state = (runtime_state or {}).get(_ACQUISITION_STATE)
    if not isinstance(state, _Acquisition):
        return {}
    return {
        "pi30_charge_current_acquisition_attempts": state.attempts,
        "pi30_charge_current_acquisition_status": (
            "complete" if state.complete else
            "exhausted" if state.attempts >= _ACQUISITION_MAX_ATTEMPTS else "pending"
        ),
    }


async def async_acquire_charge_current_controls(
    session, inverter: DetectedInverter, *, runtime_state: dict | None,
    command_timings: list[tuple[str, int, str]] | None = None,
) -> dict:
    """Finish optional metadata acquisition on the bound runtime session.

    Detection is allowed to run out of optional budget. Saved bindings also
    arrive without advertised choices. Neither case is a negative capability
    verdict: use up to three separate, bounded acquisition windows, then stop
    for this session. The hub clears runtime_state on session changes. Never
    persist attempts or infer current readback from cached identity details.
    """

    if runtime_state is None or inverter.driver_key != "pi30":
        return {}
    binding = _binding(inverter)
    state = runtime_state.get(_ACQUISITION_STATE)
    if not isinstance(state, _Acquisition) or state.binding != binding:
        state = _Acquisition(binding)
        runtime_state[_ACQUISITION_STATE] = state
    if state.complete or state.in_progress or state.attempts >= _ACQUISITION_MAX_ATTEMPTS:
        return {}
    capabilities = inverter.capabilities
    existing = {cap.key for cap in capabilities}
    if all(key in existing for key, *_ in _CONTROLS):
        state.complete = True
        return {}

    state.attempts += 1
    state.in_progress = True
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _ACQUISITION_TIMEOUT

    def is_current() -> bool:
        return (
            runtime_state.get(_ACQUISITION_STATE) is state
            and _binding(inverter) == binding
            and inverter.capabilities is capabilities
        )

    try:
        try:
            current = await _request_parsed(
                session, "QPIRI", parse_qpiri, timeout=min(1.5, _ACQUISITION_TIMEOUT),
                command_timings=command_timings,
            )
        except Exception:  # Optional metadata failure; cancellation propagates.
            return {}
        if not is_current():
            return {}
        enriched = await async_enrich_charge_current_controls(
            session, current, capabilities, timeout=max(0.0, deadline - loop.time()),
            is_current=is_current,
            command_timings=command_timings,
        )
        # No stale completion may publish across a reset/rebind or overwrite a
        # concurrently changed capability set. There is no await after this gate.
        if not is_current():
            return {}
        values = {
            cap.value_key: cap.enum_value_map[current[cap.value_key]]
            for cap in enriched if cap.key not in existing
        }
        inverter.capabilities = enriched
        inverter.details.update(values)
        state.complete = all(key in {cap.key for cap in enriched} for key, *_ in _CONTROLS)
        return values
    finally:
        state.in_progress = False


def parse_charge_current_choices(payload: str) -> tuple[int, ...]:
    """Accept a bounded list of distinct three-digit positive ampere values."""

    if not isinstance(payload, str) or not re.fullmatch(r"[0-9]{3}(?: [0-9]{3}){0,63}", payload):
        raise Pi30Error("invalid_charge_current_choices")
    values = tuple(int(field) for field in payload.split(" "))
    if 0 in values or len(set(values)) != len(values):
        raise Pi30Error("invalid_charge_current_choices")
    return values


async def async_enrich_charge_current_controls(
    session, values: dict, capabilities: tuple[WriteCapability, ...], *, timeout: float,
    is_current: Callable[[], bool] | None = None,
    command_timings: list[tuple[str, int, str]] | None = None,
) -> tuple[WriteCapability, ...]:
    """Read at most two missing lists within a shared optional time budget.

    A timeout/NAK/malformed list never invalidates an already detected inverter.
    Used by best-effort detection and finite post-binding acquisition, not the
    recurring telemetry command set. Writes remain untested/Full-Control-only
    and accept only the device's advertised values.
    """

    result = list(capabilities)
    existing = {cap.key for cap in capabilities}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, timeout)
    missing = [
        (index, control) for index, control in enumerate(_CONTROLS)
        if control[0] not in existing
        and type(values.get(control[0])) is int and values[control[0]] > 0
    ]
    for position, (index, (key, query, command, title)) in enumerate(missing):
        if is_current is not None and not is_current():
            break
        current = values[key]
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            # Reserve a fair share for every missing list: a slow/unsupported
            # total-current query must not starve the utility-current query.
            choices = await _request_parsed(
                session, query, parse_charge_current_choices,
                timeout=min(1.5, remaining / (len(missing) - position)),
                command_timings=command_timings,
            )
        except Exception:  # Optional query/parser failure; cancellation is BaseException.
            continue
        if current not in choices:
            continue
        result.append(WriteCapability(
            key=key, register=0, value_kind="enum", command=command, command_width=3,
            note=f"PI30 {query} advertised these values on this device; local writes are untested.",
            tested=False, provenance="doc_backed", support_tier="conditional",
            title=title, group="battery", order=260 + 10 * index, unit="A",
            enabled_default=True, requires_confirm=True,
            change_summary="Limits battery charging current to a value advertised by this inverter.",
            choices=tuple(CapabilityChoice(value=value, label=f"{value} A", order=order)
                          for order, value in enumerate(choices)),
        ))
    return tuple(result)
