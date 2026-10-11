"""No vendor IO: reuse, bounded bodies and at-most-once semantic writes."""
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection, HTTPSConnection, IncompleteRead, RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.error import HTTPError, URLError

from custom_components.eybond_local import cloud_http_session as http

BASE = "https://example.invalid/public/"


def reply(body=b'{"err":0,"dat":{}}', *, status=200, remaining=0, will_close=False):
    value = MagicMock(status=status, length=remaining, will_close=will_close)
    value.__enter__.return_value = value
    value.read.return_value = body
    return value


class SessionTests(unittest.TestCase):
    def get(self, session, **kwargs):
        return session.get(BASE + "?token=synthetic-secret&action=control", headers={}, timeout=2, **kwargs)

    def test_construction_has_no_io_reuses_connection_and_closes_idempotently(self):
        with patch.object(http, "HTTPSConnection") as factory:
            factory.return_value.getresponse.side_effect = [reply(), reply()]
            session = http.CloudHttpSession(BASE)
            factory.assert_not_called()
            self.get(session)
            self.get(session)
            factory.assert_called_once_with("example.invalid", 443, timeout=2)
            self.assertEqual(factory.return_value.request.call_count, 2)
            self.assertEqual(factory.return_value.sock.settimeout.call_count, 2)
            session.close()
            session.close()
            factory.return_value.close.assert_called_once()
            with self.assertRaisesRegex(ValueError, "closed"):
                self.get(session)

    def test_tls_default_verifies_certificate_and_hostname(self):
        connection = HTTPSConnection("example.invalid")
        self.assertTrue(connection._context.check_hostname)
        self.assertEqual(connection._context.verify_mode, ssl.CERT_REQUIRED)
        connection.close()

    def test_explicit_connect_precedes_request_and_is_not_repeated_on_reuse(self):
        with patch.object(http, "HTTPSConnection") as factory:
            connection = factory.return_value
            connection.sock = None
            connection.connect.side_effect = lambda: setattr(connection, "sock", Mock())
            connection.getresponse.return_value = reply()
            session = http.CloudHttpSession(BASE)
            self.get(session)
            self.get(session)
            connection.connect.assert_called_once_with()
            calls = [call[0] for call in connection.mock_calls]
            self.assertLess(calls.index("connect"), calls.index("request"))
            self.assertEqual(connection.request.call_count, 2)
            session.close()

    def test_failure_phase_is_sanitized_and_never_retries(self):
        for where, phase in (("connect", "connect"), ("request", "request"),
                             ("getresponse", "response_headers"), ("read", "response_body")):
            with self.subTest(phase=phase), patch.object(http, "HTTPSConnection") as factory:
                connection = factory.return_value
                response = reply()
                connection.getresponse.return_value = response
                if where == "connect":
                    connection.sock = None
                target = response if where == "read" else connection
                getattr(target, where).side_effect = TimeoutError("signed-url synthetic-secret")
                session = http.CloudHttpSession(BASE)
                with self.assertLogs(http.__name__, "WARNING") as captured:
                    with self.assertRaises(URLError):
                        self.get(session)
                message = " ".join(captured.output)
                self.assertIn("phase=" + phase, message)
                self.assertIn("reason=timeout", message)
                self.assertNotIn("synthetic-secret", message)
                self.assertNotIn("signed-url", message)
                self.assertNotIn("example.invalid", message)
                self.assertEqual(len(captured.records), 1)
                self.assertIsNone(captured.records[0].exc_info)
                self.assertEqual(connection.request.call_count, 0 if where == "connect" else 1)
                with self.assertRaisesRegex(ValueError, "closed"):
                    self.get(session)
                connection.close.assert_called_once()

    def test_failure_reasons_are_closed_not_exception_text(self):
        for error, reason in ((http.gaierror("secret"), "dns"),
                              (ssl.SSLError("secret"), "tls"),
                              (BrokenPipeError("secret"), "network"),
                              (RuntimeError("secret"), "unexpected")):
            self.assertEqual(http._failure_reason(error), reason)

    def test_origin_path_and_limits_rejected_before_request_without_secret_in_error(self):
        for url in ("http://example.invalid/public/", "https://other.invalid/public/",
                    "https://example.invalid:444/public/", "https://example.invalid/other/",
                    "https://user:synthetic-secret@example.invalid/public/", BASE + "#synthetic-secret",
                    BASE + "?a=\r\nsynthetic-secret"):
            with self.subTest(url=url), patch.object(http, "HTTPSConnection") as factory:
                session = http.CloudHttpSession(BASE)
                with self.assertRaises(ValueError) as caught:
                    session.get(url, headers={}, timeout=2)
                self.assertNotIn("synthetic-secret", str(caught.exception))
                factory.assert_not_called()
        for timeout in (0, -1, True, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                http.CloudHttpSession(BASE).get(BASE, headers={}, timeout=timeout)
        for limit in (0, True, 4 * 1024 * 1024 + 1):
            with self.assertRaises(ValueError):
                self.get(http.CloudHttpSession(BASE), max_bytes=limit)

    def test_redirect_and_http_failure_never_follow_or_retry(self):
        for status in (301, 302, 307, 308, 401, 429, 500):
            with self.subTest(status=status), patch.object(http, "HTTPSConnection") as factory:
                factory.return_value.getresponse.return_value = reply(status=status)
                session = http.CloudHttpSession(BASE)
                with self.assertRaises(HTTPError) as caught:
                    self.get(session)
                self.assertEqual(caught.exception.code, status)
                self.assertEqual(caught.exception.url, "")
                factory.return_value.getresponse.return_value.read.assert_not_called()
                with self.assertRaisesRegex(ValueError, "closed"):
                    self.get(session)
                self.assertEqual(factory.return_value.request.call_count, 1)
                factory.return_value.close.assert_called_once()

    def test_connect_partial_send_response_failure_and_stale_socket_are_terminal(self):
        for where, error in (("request", TimeoutError()), ("request", BrokenPipeError()),
                             ("getresponse", RemoteDisconnected()), ("getresponse", ssl.SSLError())):
            with self.subTest(where=where, error=error), patch.object(http, "HTTPSConnection") as factory:
                getattr(factory.return_value, where).side_effect = error
                session = http.CloudHttpSession(BASE)
                with self.assertRaises((URLError, ssl.SSLError)):
                    self.get(session)
                with self.assertRaisesRegex(ValueError, "closed"):
                    self.get(session)
                self.assertEqual(factory.return_value.request.call_count, 1)
                factory.return_value.close.assert_called_once()

    def test_bounded_or_truncated_response_is_not_reusable(self):
        for response, error in ((reply(b"123456"), ValueError), (reply(b"1", remaining=8), IncompleteRead)):
            with patch.object(http, "HTTPSConnection") as factory:
                factory.return_value.getresponse.return_value = response
                session = http.CloudHttpSession(BASE)
                with self.assertRaises(error):
                    self.get(session, max_bytes=5)
                response.read.assert_called_once_with(6)
                factory.return_value.close.assert_called_once()

    def test_clean_server_close_allows_next_distinct_request_not_a_replay(self):
        with patch.object(http, "HTTPSConnection") as factory:
            connections = [Mock(getresponse=Mock(return_value=reply(will_close=True))),
                           Mock(getresponse=Mock(return_value=reply()))]
            factory.side_effect = connections
            session = http.CloudHttpSession(BASE)
            self.get(session)
            self.get(session)
            self.assertEqual(factory.call_count, 2)
            for connection in connections:
                connection.request.assert_called_once()
            session.close()

    def test_busy_close_or_parallel_request_cannot_interrupt_owned_worker(self):
        entered, release = threading.Event(), threading.Event()
        def read(_):
            entered.set()
            if not release.wait(3):
                raise TimeoutError()
            return b'{}'
        with patch.object(http, "HTTPSConnection") as factory, ThreadPoolExecutor() as pool:
            response = reply()
            response.read.side_effect = read
            factory.return_value.getresponse.return_value = response
            session = http.CloudHttpSession(BASE)
            worker = pool.submit(self.get, session)
            try:
                self.assertTrue(entered.wait(2))
                with self.assertRaisesRegex(RuntimeError, "busy"):
                    session.close()
                with self.assertRaisesRegex(RuntimeError, "busy"):
                    self.get(session)
                factory.return_value.close.assert_not_called()
            finally:
                release.set()
            self.assertEqual(worker.result(2), b'{}')
            session.close()

    def test_operations_do_not_share_connections_or_failure_state(self):
        with patch.object(http, "HTTPSConnection") as factory:
            a = Mock(getresponse=Mock(return_value=reply()))
            b = Mock(getresponse=Mock(return_value=reply()))
            factory.side_effect = [a, b]
            first, second = http.CloudHttpSession(BASE), http.CloudHttpSession(BASE)
            self.get(first)
            self.get(second)
            first.close()
            b.close.assert_not_called()
            self.get(second)
            self.assertEqual(factory.call_count, 2)
            second.close()

    def test_real_http_response_framing_reuses_socket_and_detects_short_body(self):
        # Replace TLS only for this loopback test; production TLS defaults are
        # checked separately. Exercise real http.client framing/connection reuse.
        seen = []
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *args):
                pass
            def do_GET(self):
                seen.append(self.client_address[1])
                data = b'{}'
                self.send_response(200)
                self.send_header("Content-Length", "9" if len(seen) == 3 else str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()
                if len(seen) == 3:
                    self.close_connection = True
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        session = http.CloudHttpSession(BASE)
        try:
            with patch.object(http, "HTTPSConnection", side_effect=lambda *a, **k:
                              HTTPConnection("127.0.0.1", server.server_port, timeout=k['timeout'])):
                self.assertEqual(self.get(session), b'{}')
                self.assertEqual(self.get(session), b'{}')
                with self.assertRaises(IncompleteRead):
                    self.get(session)
                self.assertEqual(len(seen), 3)
                self.assertEqual(len(set(seen)), 1)
        finally:
            session.close()
            server.shutdown()
            server.server_close()
            thread.join(2)
