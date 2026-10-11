"""Operation-owned HTTPS reuse; never retry a semantic cloud write.

No credentials, redirects, global pool or provider fallback. The owner drains
its blocking worker before closing this resource. Construction performs no IO.
"""
from __future__ import annotations

from http.client import HTTPSConnection, IncompleteRead
from io import BytesIO
import logging
from math import isfinite
from socket import gaierror
from ssl import SSLError
from threading import Lock
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

_LOGGER = logging.getLogger(__name__)


def _failure_reason(exc: BaseException) -> str:
    """Closed diagnostics only: never stringify provider/socket exceptions."""
    for kind, reason in ((TimeoutError, "timeout"), (gaierror, "dns"),
                         (SSLError, "tls"), (HTTPError, "http_status"),
                         (IncompleteRead, "incomplete_response"), (OSError, "network"),
                         (ValueError, "invalid_response")):
        if isinstance(exc, kind):
            return reason
    return "unexpected"


class CloudHttpSession:
    """A single origin/path, serialized requests, terminal failure/close."""

    def __init__(self, base_url: str):
        parsed = self._parse(base_url)
        if parsed.query:
            raise ValueError("cloud_http_origin_invalid")
        self._origin = (parsed.hostname, parsed.port or 443)
        self._path = parsed.path or "/"
        self._connection = None
        self._closed = False
        self._lock = Lock()

    @staticmethod
    def _parse(url):
        if type(url) is not str or any(ord(c) <= 32 or ord(c) >= 127 for c in url):
            raise ValueError("cloud_http_url_invalid")
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise ValueError("cloud_http_origin_invalid")
        return parsed

    def get(self, url: str, *, headers: dict[str, str], timeout: float,
            max_bytes: int = 2 * 1024 * 1024) -> bytes:
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cloud_http_busy")
        phase, reused, started = "validation", False, monotonic()
        try:
            if self._closed:
                raise ValueError("cloud_http_closed")
            parsed = self._parse(url)
            if ((parsed.hostname, parsed.port or 443) != self._origin
                    or (parsed.path or "/") != self._path):
                raise ValueError("cloud_http_origin_mismatch")
            if (type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
                    or type(max_bytes) is not int or not 0 < max_bytes <= 4 * 1024 * 1024):
                raise ValueError("cloud_http_limits_invalid")
            if self._connection is None:
                # Default context validates certificates and hostname. This
                # constructor and all TLS/HTTP IO execute in the owned worker.
                self._connection = HTTPSConnection(*self._origin, timeout=timeout)
            connection = self._connection
            connection.timeout = timeout
            reused = connection.sock is not None
            if not reused:
                # DNS/TCP/TLS finish before http.client can send the signed
                # request. A connect timeout alone cannot identify which of
                # those three sub-stages failed. Never infer queue settlement.
                phase = "connect"
                connection.connect()
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
            target = self._path + ("?" + parsed.query if parsed.query else "")
            phase = "request"
            connection.request("GET", target, headers=headers)
            phase = "response_headers"
            response = connection.getresponse()
            with response:
                # Do not follow redirects or retain a signed URL/error body.
                if not 200 <= response.status < 300:
                    raise HTTPError("", response.status, "cloud_http_status", None, BytesIO())
                phase = "response_body"
                payload = response.read(max_bytes + 1)
                if len(payload) > max_bytes:
                    raise ValueError("cloud_http_response_too_large")
                if response.length not in (None, 0):
                    raise IncompleteRead(b"", response.length)
                if response.will_close:
                    connection.close()
                    self._connection = None
            return payload
        except BaseException as exc:
            if phase != "validation":
                _LOGGER.warning(
                    "Cloud HTTPS request failed phase=%s reason=%s reused=%s elapsed_ms=%s; no automatic retry",
                    phase, _failure_reason(exc), reused, round((monotonic() - started) * 1000),
                )
            self._close_locked()
            if isinstance(exc, OSError) and not isinstance(exc, URLError):
                raise URLError(exc) from exc
            raise
        finally:
            self._lock.release()

    def _close_locked(self):
        self._closed = True
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()

    def close(self) -> None:
        """Owner-only cleanup AFTER draining its worker; never race request IO."""
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cloud_http_busy")
        try:
            self._close_locked()
        finally:
            self._lock.release()
