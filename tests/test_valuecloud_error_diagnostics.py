"""Safe ValueCloud failures: distinguish causes without retaining HTTP content."""
from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from custom_components.eybond_local import valuecloud_cloud as cloud
from custom_components.eybond_local.support.cloud_api_adapters import ValueCloudApiAdapter


class ValueCloudErrorDiagnosticsTests(unittest.TestCase):
    def test_transport_errors_are_classified_without_raw_detail(self):
        secret = "private-body-token-password"
        for original, reason, classification, status in (
            (HTTPError('https://private.invalid', 502, secret, {}, io.BytesIO(secret.encode())),
             'http_error', 'unavailable', 502),
            (HTTPError('https://private.invalid', 401, secret, {}, io.BytesIO()),
             'http_error', 'unexpected', 401),
            (HTTPError('https://private.invalid', 403, secret, {}, io.BytesIO()),
             'http_error', 'unexpected', 403),
            (HTTPError('https://private.invalid', 429, secret, {}, io.BytesIO()),
             'http_error', 'rate_limited', 429),
            (URLError(secret), 'network', 'network', None),
            (URLError(TimeoutError(secret)), 'timeout', 'timeout', None),
            (TimeoutError(secret), 'timeout', 'timeout', None),
            (ConnectionResetError(secret), 'network', 'network', None),
        ):
            with self.subTest(reason=reason, status=status), patch.object(
                cloud, 'urlopen', side_effect=original,
            ) as request:
                with self.assertRaises(cloud.ValueCloudError) as raised:
                    cloud._http_json(method='POST', path=cloud.VALUECLOUD_BATCH_SETUP_PATH)
                error = raised.exception
                diagnostics = error.safe_diagnostics()
                self.assertEqual(diagnostics['provider_reason'], reason)
                self.assertEqual(diagnostics['request_stage'], 'control')
                self.assertEqual(diagnostics.get('http_status'), status)
                self.assertEqual(ValueCloudApiAdapter().classify_error(error), classification)
                self.assertNotIn(secret, str(error) + json.dumps(diagnostics))
                self.assertTrue(error.__suppress_context__)
                request.assert_called_once()
                if isinstance(original, HTTPError):
                    self.assertTrue(original.closed)

    def test_invalid_responses_do_not_export_body(self):
        for body, reason in ((b'secret-html-page', 'invalid_json'), (b'[]', 'invalid_envelope')):
            response = MagicMock()
            response.__enter__.return_value = response
            response.read.return_value = body
            response.headers = {}
            with self.subTest(reason=reason), patch.object(cloud, 'urlopen', return_value=response):
                with self.assertRaises(cloud.ValueCloudError) as raised:
                    cloud._http_json(method='POST', path=cloud.LOGIN_PATH)
            self.assertEqual(raised.exception.safe_diagnostics(), {
                'request_stage': 'login', 'provider_reason': reason,
            })
            self.assertNotIn('secret-html-page', str(raised.exception))
            self.assertEqual(ValueCloudApiAdapter().classify_error(raised.exception), 'unavailable')

    def test_unrecognized_fields_and_exception_text_are_not_exported(self):
        error = cloud.ValueCloudError('secret', stage='secret', reason_code='secret', http_status=True)
        error.delivery_sequence = True
        error.delivery_total = 2
        self.assertEqual(error.safe_diagnostics(), {
            'request_stage': 'unknown', 'provider_reason': 'unknown',
        })
        self.assertEqual(ValueCloudApiAdapter().classify_error(error), 'unexpected')
        self.assertEqual(ValueCloudApiAdapter().classify_error(RuntimeError('secret')), '')

    def test_unknown_login_rejection_does_not_blame_password(self):
        envelope = cloud.ValueCloudEnvelope(16, 'secret', 'secret', False, {}, {}, {})
        with patch.object(cloud, '_http_json', return_value=envelope):
            with self.assertRaises(cloud.ValueCloudError) as raised:
                cloud.login_with_password(username='account', password='password')
        self.assertEqual(raised.exception.safe_diagnostics(), {
            'request_stage': 'login', 'provider_reason': 'rejected',
        })
        self.assertEqual(ValueCloudApiAdapter().classify_error(raised.exception), 'unexpected')
        self.assertNotIn('secret', str(raised.exception))

    def test_http_auth_denial_is_not_confused_with_later_control_denial(self):
        for status in (401, 403):
            self.assertEqual(ValueCloudApiAdapter().classify_error(cloud.ValueCloudError(
                'http_error', stage='login', reason_code='http_error', http_status=status,
            )), 'auth_failed')
            self.assertEqual(ValueCloudApiAdapter().classify_error(cloud.ValueCloudError(
                'http_error', stage='control', reason_code='http_error', http_status=status,
            )), 'unexpected')


if __name__ == '__main__':
    unittest.main()
