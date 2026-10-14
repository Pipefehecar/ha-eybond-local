"""Keep activated entity keys when reviewed controls become builtin.

Only an explicit alias AND an identical supported write/readback contract can
shadow a new builtin. Never infer equivalence from a register alone. The old
capability, its selection/exposure rules and its value key remain untouched.
"""

from __future__ import annotations

from ..models import WriteCapability


def _same_contract(builtin: WriteCapability, learned: WriteCapability) -> bool:
    # Initially qualify only scalar SMG controls. Enums, commands, actions and
    # bitfields need their own reviewed migration semantics.
    if (
        builtin.value_kind not in ("u16", "scaled_u16")
        or builtin.register < 0
        or builtin.word_count != 1
        or builtin.command
        or builtin.command_map
        or builtin.bitmask is not None
        or learned.command
        or learned.command_map
        or learned.bitmask is not None
    ):
        return False
    fields = (
        "register", "value_kind", "word_count", "combine", "divisor", "multiplier",
        "minimum", "maximum", "unit", "action_value", "enum_map", "choices",
        "command_width", "command_precision", "read_key", "poll_readback",
        "requires_confirm", "reboot_required", "unsafe_while_running",
        "safe_operating_modes", "visible_if", "editable_if", "depends_on",
        "affects", "exclusive_with",
    )
    return (
        all(getattr(builtin, field) == getattr(learned, field) for field in fields)
        and (builtin.write_function or 16) == (learned.write_function or 16)
        and builtin.native_step == learned.native_step
    )


def retain_builtin_controls(
    capabilities: tuple[WriteCapability, ...],
    overlay: tuple[WriteCapability, ...],
    *,
    driver_key: str,
) -> tuple[WriteCapability, ...]:
    """Hide only reviewed equivalents; caller appends unchanged learned controls.

    Unselected learned controls still shadow their builtin equivalent: selecting
    Full Control must not bypass the user's saved exclusion after an update. The
    ordinary overlay selection policy decides whether that learned key is exposed.
    Ambiguous aliases or unsupported drivers retain the existing builtin instead.
    """
    if driver_key != "modbus_smg":
        return capabilities
    owners: dict[str, int] = {}
    for capability in capabilities:
        if type(capability) is WriteCapability and not capability.is_device_scoped_experimental:
            for alias in capability.learned_control_aliases:
                owners[alias] = owners.get(alias, 0) + 1
    retained = []
    for capability in capabilities:
        matches = []
        if type(capability) is WriteCapability and not capability.is_device_scoped_experimental:
            matches = [
                candidate for candidate in overlay
                if type(candidate) is WriteCapability
                and candidate.is_device_scoped_experimental
                and candidate.key in capability.learned_control_aliases
                and owners.get(candidate.key) == 1
                and _same_contract(capability, candidate)
            ]
        if len(matches) != 1:
            retained.append(capability)
    return tuple(retained)
