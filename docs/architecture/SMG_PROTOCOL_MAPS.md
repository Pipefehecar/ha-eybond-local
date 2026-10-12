# SMG protocol maps and generic controls

Register 184 is a runtime layout identifier, not a monotonically increasing
document revision. The integration selects a map from observed identity and
then validates its readable configuration. It does not choose a newer map
because a lower-numbered map partially responded.

## Evidence and scope

| Runtime number | Maintained basis | Generic control policy |
| --- | --- | --- |
| 1 | Classic SMG RS232 map; the later ANENJI RS232 document explicitly names number 1 | 30 untested basic capabilities |
| 2 | GM6200 Communication Protocol 2, July 2024, explicitly specifies register 184 = `0x02` | 54 untested capabilities, seven explicitly blocked |
| 3, 4, 5, 6 | Communication Protocol No. 3-10, with a separate layout for each of these four numbers | Existing document-backed, untested protocol matrices |
| 11 | Classic documented base corroborated by known layout-11 model maps and captures | 30 untested compatible capabilities, no guessed OP2 extensions |
| 7, 8, 9, 10 and other unknown numbers | No verified map in the maintained corpus | No inferred generic map or writes |

The title **Communication Protocol No. 3-10** does not establish support for
every number from 3 through 10. Its register-184 tables describe 3, 4, 5 and 6.
Similarly, the November 2021 **SMG RS232 V1.0.1** document marks register 184 as
invalid data; the document's V1 label cannot establish an inverter's identity.

Public primary-document copies used here:

- [Classic SMG RS232 V1.0.1](https://github.com/syssi/esphome-smg-ii/blob/main/docs/SMG-RS232%2BCommunication%2BProtocol%2BV1.0.1.pdf)
- [GM6200 Communication Protocol 2](https://github.com/syssi/esphome-smg-ii/blob/main/docs/GM6200%2BCommunication%2BProtocol%2B2_CN.html)

The corresponding source records are `documentation_classic_smg_rs232_v1`
and `documentation_gm6200_protocol_2` under `catalog/inverter_models/sources/`.
The December 2023 ANENJI document and the No. 3-10 PDF are retained in the
maintainer's local protocol corpus; they are not republished here.

A dedicated, trustworthy protocol-11 specification was not found in the
September 2026 research. Protocol-11 compatibility is therefore explicitly
limited, not presented as complete vendor documentation. The sanitized
`tests/fixtures/smg_protocol_11_7904.json` replay covers the unknown 6200 W
fingerprint from issue #41. A read capture does not verify writes.

## Numeric limits and shared battery maps

The seven shared battery-voltage templates intentionally have no `minimum` or
`maximum`: register compatibility does not establish a battery voltage class or
valid setpoint interval. The former 48 V template bounds and broad 0–70 V
workarounds are not specifications and must not be inherited as such. Explicit
profile or device-scoped learned limits continue to apply; this policy does not
infer limits from live battery voltage or divide a 48 V interval by two.

`WriteCapability.number_minimum/number_maximum` supply HA's required numeric
bounds from an explicit profile limit, or otherwise from the unsigned field's
encoding and scale. Schema attributes identify each side as `profile` or
`encoding`. These bounds are not a claim about safe battery settings. The Modbus
write codec also rejects values outside the encoded field, even if the profile
omits limits, so an unbounded u32 write cannot wrap and a scaled u16 cannot
overflow. The existing confirmation, access and readback policy is unchanged.

## Independent branches, not sequential upgrades

Protocol profiles 1, 2 and 11 independently extend the documented classic
control base. Protocol 2 overrides different enums and adds only its own
documented registers. Protocol 11 restricts charge-priority writes to the
shared values 1, 2 and 3; values 0 and 4 require an exact or learned profile.
Protocols 3–6 use their existing separate shared map and per-number overlays.

These differences prevent a single cumulative inheritance chain:

| Register | Classic / compatible base | Protocol 2 | Protocol 3–6 or exact-model extension |
| --- | --- | --- | --- |
| 301 | Classic output-source choices | SUB, SBU, SUF and ZEC; value 0 is not defined | Output settings live in the 600+ block |
| 344 | Not a generic protocol-1/11 write | PV grid-feed power limit, watts | Exact OP2 6200 uses this address for output-2 cutoff SOC |
| 345–348 | Not generic protocol-1/11 controls | Four secondary-priority HHMM schedule fields | Protocol 3–6 schedules use 603/604 and 679/680 |
| 434–439 | Not generic protocol-1/11 controls | Three date words and three time words | Protocol 3–6 clock uses 696–701 |

Protocol 2 also defines signed CT power at 235–236 and PV energy at 443–445.
Its schema does not decode reserved register 316 as the classic optional dry
contact field. Its enum tables replace the applicable mode sets instead of
merging in unsupported classic values. Known OP2 model maps remain separate.

## Detection and write authority

Exact model descriptors win over protocol fallbacks. Their tested flags,
extra registers and existing profiles do not change when a generic protocol
profile is added. An unknown device is named **SMG Protocol N (Unverified
Variant)** rather than assigned a commercial identity from a similar device.

The catalog's `when_layout_codes` condition uses the existing identity probe;
the selected schema then checks readable output voltage and frequency. This
change adds no transport negotiation or extra protocol-discovery probes.
Protocol 2 does have its own documented runtime read ranges.

All generic capabilities are `tested=false`, `doc_backed`, and conditional
unless explicitly blocked. `doc_backed` describes the command map, not proof
that an unknown inverter implements every command. Auto remains monitoring-only;
Full Control is an explicit opt-in. Existing entity defaults still apply:
advanced entities require individual activation, and user-disabled entities
stay disabled after reload. Merely detecting a protocol, selecting Full Control
or loading entities sends no writes.

Protocol 2 keeps warning-mask changes, grid-feed limit, anti-islanding changes,
forced equalization, energy/record clears and factory reset blocked on an
unverified model. Device-specific policy is required to expose these operations.
Readback confirmation does not convert a capability to tested automatically.

When a model needs additional controls, keep them in an evidence-backed exact
model or local learned overlay. Do not broaden the generic branch solely
because one inverter implements an extra register.

## Regression coverage

`test_smg_compatible_protocols.py` covers generic 1/2/11 policy, the issue-41
replay, protocol-2 address/enum isolation, normal polling after HHMM and clock
writes, exact-model precedence, and rejection of undocumented protocol numbers.
All simulated writes stay inside the fixture transport.

`test_ha_smg_protocol_controls.py` exercises the real Home Assistant platforms,
Auto/Full Control exposure, explicit advanced-entity activation and preservation
of user-disabled entities across reloads, including an upgrade from the old
read-only family snapshot. The affected-test selector also includes the protocol
replay tests for SMG profile, schema, runtime-catalog and driver changes.
The existing classic and protocol-3–6
regression suites protect model-specific behavior and tested-capability counts.
