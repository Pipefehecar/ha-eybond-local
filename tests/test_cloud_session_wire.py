"""Mixed-stream framing: raw PI30 CRCs must not become EyeBond headers."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.payload.pi30 import build_request, parse_response
from custom_components.eybond_local.support.cloud_session_wire import consume_cloud_message


class CloudSessionWirePi30Tests(unittest.TestCase):
    def test_exact_qpigs_reproduction_and_common_commands(self):
        self.assertEqual(build_request("QPIGS"), b"QPIGS\xb7\xa9\r")
        for command in ("QPI", "QPIGS", "QPIRI", "QMOD", "QID", "POP02", "MCHGC030"):
            with self.subTest(command=command):
                frame = build_request(command)
                buffer = bytearray(frame)
                self.assertEqual(consume_cloud_message(buffer, allow_pi30=True), ("pi30", frame))
                self.assertEqual(buffer, b"")

    def test_response_binary_and_escaped_crc_use_existing_payload_rules(self):
        # Search synthetic bodies for each CRC byte escaping rule, not a
        # second implementation of the CRC. At least one byte is escaped.
        from custom_components.eybond_local.payload.pi30 import crc16_xmodem

        for special in (0x28, 0x0D, 0x0A):
            body = next(f"({i}" for i in range(10000)
                        if special in crc16_xmodem(f"({i}".encode()).to_bytes(2, "big"))
            frame = build_request(body)
            self.assertEqual(parse_response(frame), body[1:])
            buffer = bytearray(frame)
            self.assertEqual(consume_cloud_message(buffer, allow_pi30=True), ("pi30", frame))
            self.assertEqual(buffer, b"")

    def test_bad_binary_request_crc_is_not_recognized(self):
        for frame in (b"QPIGS\xb7\xa8\r", b"QPIGS\x00\x00\r"):
            for end in range(1, len(frame) + 1):
                with self.subTest(frame=frame, end=end):
                    buffer = bytearray(frame[:end])
                    self.assertIsNone(consume_cloud_message(buffer, allow_pi30=True))
                    self.assertEqual(buffer, frame[:end])

    def test_bad_binary_response_crc_is_not_recognized(self):
        frame = b"(PI30\x00\x00\r"
        buffer = bytearray(frame)
        self.assertIsNone(consume_cloud_message(buffer, allow_pi30=True))
        self.assertEqual(buffer, frame)

    def test_bounded_frame_and_no_scanning_or_separator_stripping(self):
        boundary = build_request("Q" + "0" * 508)
        self.assertEqual(len(boundary), 512)
        buffer = bytearray(boundary)
        self.assertEqual(consume_cloud_message(buffer, allow_pi30=True), ("pi30", boundary))
        for frame in (build_request("Q" + "0" * 509),
                      b"QPIGS\xb7\xa8\r" + build_request("QPIGS"),
                      b"\r\n" + build_request("QPIGS")):
            buffer = bytearray(frame)
            self.assertIsNone(consume_cloud_message(buffer, allow_pi30=True))
            self.assertEqual(buffer, frame)

    def test_header_collisions_preserve_full_eybond_frames_at_every_split(self):
        for tid in (0x5150, 0x5003, 0x4D10, 0x2801, 0x0D0A):
            frame = build_collector_request(tid, build_request("QPIGS"),
                devcode=0x4947, collector_addr=1, fcode=4)
            for split in range(1, len(frame)):
                with self.subTest(tid=tid, split=split):
                    buffer = bytearray(frame[:split])
                    self.assertIsNone(consume_cloud_message(buffer, allow_pi30=True))
                    self.assertEqual(buffer, frame[:split])
                    buffer.extend(frame[split:])
                    self.assertEqual(consume_cloud_message(buffer, allow_pi30=True), ("frame", frame))
                    self.assertEqual(buffer, b"")

    def test_mixed_coalesced_and_fragmented_stream_preserves_all_bytes(self):
        expected = [
            ("at", b"AT+DTUPN?\r\n"),
            ("pi30", build_request("QPIGS")),
            ("pi30", build_request("(230.0 50.0 " + "0 " * 80)),
            ("frame", build_collector_request(0x5150, b"\x01\x02\r\n",
                devcode=2376, collector_addr=1, fcode=4)),
            ("modbus", bytes.fromhex("0103006400034414")),
            ("ascii", b"GPDAT0\r"),
            ("ascii", b"(183.1 027.6 00.43\r"),
            ("ascii", b"#data\r"),
            ("at", b"AT+WFSS?\r\n"),
            ("pi30", build_request("QMOD")),
        ]
        stream = b"".join(frame for _, frame in expected)
        deliveries = [[stream], [bytes((b,)) for b in stream]]
        deliveries.extend([stream[:split], stream[split:]] for split in range(1, len(stream)))
        for chunks in deliveries:
            with self.subTest(lengths=[len(chunk) for chunk in chunks]):
                buffer = bytearray()
                messages = []
                for chunk in chunks:
                    buffer.extend(chunk)
                    while (message := consume_cloud_message(buffer, allow_pi30=True)) is not None:
                        messages.append(message)
                self.assertEqual(messages, expected)
                self.assertEqual(buffer, b"")

    def test_legacy_ascii_is_preserved_not_mislabeled_as_crc_verified_pi30(self):
        # Printable damaged CRCs are indistinguishable from generic no-CRC
        # text; preserve legacy framing but never label them verified PI30.
        for frame in (b"Q1\r", b"MP\r", b"POP\r", b"QPIGSaa\r", b"(PI30aa\r",
                      b"GPDAT0\r", b"(183.1 027.6\r"):
            for opt_in in (False, True):
                with self.subTest(frame=frame, opt_in=opt_in):
                    buffer = bytearray(frame)
                    self.assertEqual(consume_cloud_message(buffer, allow_pi30=opt_in), ("ascii", frame))
                    self.assertEqual(buffer, b"")

    def test_default_shared_parser_does_not_enable_raw_pi30_carrier(self):
        for command in ("QPIGS", "POP02", "MCHGC030"):
            with self.subTest(command=command):
                frame = build_request(command)
                buffer = bytearray(frame)
                self.assertIsNone(consume_cloud_message(buffer))
                self.assertEqual(buffer, frame)
