"""Qualify the exact shadow socket before opening its cloud upstream.

Reuse the normal read-only identity wire, only on an explicitly known dialect.
Seed metadata, a peer address and an unsolicited heartbeat are not a challenge
response. This is socket identity, NOT permanent-local recovery or settlement.
"""

from __future__ import annotations

import asyncio

from ...collector.identity_probe import (
    build_identity_probe_request,
    identity_probe_kinds,
    parse_identity_probe_response,
)
from ...collector_identity import validated_collector_pn
from ..cloud_session_wire import consume_cloud_message


async def async_verify_shadow_socket(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    collector_pn: str,
    session_protocol: str,
    timeout: float = 5.0,
) -> tuple[str, bytes]:
    """Return strong source and untouched non-challenge traffic for this socket.

    No cloud socket exists yet, so probe TIDs cannot collide with cloud requests.
    The bounded prelude survives TCP fragmentation/coalescing; the successful
    challenge response itself is consumed locally, never replayed to the cloud.
    """

    kinds = identity_probe_kinds(session_protocol)
    if not kinds or not validated_collector_pn(collector_pn):
        raise RuntimeError("shadow_socket_identity_context_invalid")
    pending = bytearray()
    prelude = bytearray()
    received = 0
    for kind in kinds:
        request = build_identity_probe_request(session_protocol, probe_kind=kind)
        if request is None:
            raise RuntimeError("shadow_socket_identity_context_invalid")
        try:
            async with asyncio.timeout(timeout):
                writer.write(request.payload)
                await writer.drain()
                while True:
                    # Do not strip an arbitrary CRLF: those bytes can also be
                    # a framed transaction id. Require the complete AT marker.
                    if session_protocol == "at_text" and pending.startswith(b"\r\nAT+"):
                        del pending[:2]
                    message = consume_cloud_message(pending)
                    if message is not None:
                        wire, payload = message
                        expected_wire = "at" if session_protocol == "at_text" else "frame"
                        pn, source = (
                            parse_identity_probe_response(request, payload)
                            if wire == expected_wire else ("", "")
                        )
                        if pn:
                            if pn != collector_pn:
                                raise RuntimeError("shadow_socket_identity_mismatch")
                            return source, bytes(prelude + pending)
                        prelude.extend(payload)
                        continue
                    chunk = await reader.read(4096)
                    if not chunk:
                        raise RuntimeError("shadow_socket_identity_disconnected")
                    received += len(chunk)
                    if received > 65536:
                        raise RuntimeError("shadow_socket_identity_prelude_too_large")
                    pending.extend(chunk)
        except TimeoutError:
            continue
    raise TimeoutError("shadow_socket_identity_timeout")
