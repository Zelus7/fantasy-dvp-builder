import contextlib
import io
import unittest
from unittest.mock import Mock, patch

import requests

from pipeline.transport import fetch_pipeline_config, _retry_delay
from pipeline.build_datasets import upload


def response(status=200, body=None, headers=None):
    result = Mock(status_code=status, headers=headers or {}, ok=200 <= status < 400)
    result.json.return_value = {'leagues': []} if body is None else body
    return result


class ConfigTransportTests(unittest.TestCase):
    def setUp(self):
        self.log = io.StringIO()
        stdout = contextlib.redirect_stdout(self.log)
        stdout.__enter__()
        self.addCleanup(stdout.__exit__, None, None, None)
        self.get = patch('pipeline.transport.requests.get').start()
        self.sleep = patch('pipeline.transport.time.sleep').start()
        self.addCleanup(patch.stopall)

    def fetch(self):
        return fetch_pipeline_config('https://fixture.invalid', 'sensitive-fixture-token')

    def test_first_try_success(self):
        result = response()
        self.get.return_value = result
        self.assertEqual(self.fetch(), {'leagues': []})
        self.assertEqual(self.get.call_count, 1)
        self.sleep.assert_not_called()
        self.assertFalse(self.get.call_args.kwargs['allow_redirects'])
        result.close.assert_called_once()

    def test_transient_500_then_success(self):
        failed = response(500, {'error': {'code': 'PIPELINE_CONFIG_FAILED', 'details': {'stage': 'list-leagues', 'category': 'storage', 'requestId': '12345678-1234-1234-1234-123456789abc'}}})
        self.get.side_effect = [failed, response()]
        self.fetch()
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_called_once()
        self.assertIn('list-leagues', self.log.getvalue())
        self.assertIn('12345678-1234-1234-1234-123456789abc', self.log.getvalue())
        failed.close.assert_called_once()

    def test_temporary_statuses_are_bounded(self):
        for status in (408, 429, 500, 502, 503, 504, 522):
            with self.subTest(status=status):
                self.get.reset_mock(side_effect=True)
                self.sleep.reset_mock()
                self.get.return_value = response(status)
                with self.assertRaises(RuntimeError):
                    self.fetch()
                self.assertEqual(self.get.call_count, 3)
                self.assertEqual(self.sleep.call_count, 2)

    def test_credential_and_validation_failures_are_not_retried(self):
        cases = [(401, {}), (403, {}), (422, {}), (502, {'error': {'code': 'ESPN_AUTH_EXPIRED'}}),
                 (500, {'error': {'details': {'category': 'credentials'}}})]
        for status, body in cases:
            self.get.reset_mock()
            self.get.return_value = response(status, body)
            with self.assertRaises(RuntimeError):
                self.fetch()
            self.assertEqual(self.get.call_count, 1)
        self.sleep.assert_not_called()

    def test_timeout_and_connection_reset_recover(self):
        self.get.side_effect = [requests.Timeout('sensitive-fixture-token'), requests.ConnectionError('secret-url'), response()]
        self.fetch()
        self.assertEqual(self.sleep.call_count, 2)
        self.assertNotIn('sensitive-fixture-token', self.log.getvalue())
        self.assertNotIn('secret-url', self.log.getvalue())

    def test_tls_failures_are_not_retried(self):
        self.get.side_effect = requests.exceptions.SSLError('sensitive-fixture-token')
        with self.assertRaisesRegex(RuntimeError, 'TLS_ERROR'):
            self.fetch()
        self.assertEqual(self.get.call_count, 1)

    def test_redirect_is_not_followed_or_retried(self):
        self.get.return_value = response(302, headers={'Location': 'https://untrusted.invalid'})
        with self.assertRaises(RuntimeError):
            self.fetch()
        self.assertEqual(self.get.call_count, 1)

    def test_invalid_success_payload_is_not_retried(self):
        for body in ([], {'leagues': None}, {'notLeagues': []}):
            self.get.return_value = response(body=body)
            with self.assertRaisesRegex(RuntimeError, 'INVALID_CONFIG_RESPONSE'):
                self.fetch()
        self.sleep.assert_not_called()

    def test_html_error_recovers_without_leaking_body(self):
        failed = response(503)
        failed.json.side_effect = ValueError('sensitive-fixture-token')
        self.get.side_effect = [failed, response()]
        self.fetch()
        self.assertNotIn('sensitive-fixture-token', self.log.getvalue())

    def test_malformed_error_and_secrets_do_not_leak(self):
        for body in ({'error': 'sensitive-fixture-token'}, {'error': {'details': []}},
                     {'error': {'message': 'sensitive-fixture-token', 'details': {'requestId': 'sensitive-fixture-token'}}}):
            self.get.return_value = response(400, body)
            with self.assertRaises(RuntimeError) as caught:
                self.fetch()
            self.assertNotIn('sensitive-fixture-token', str(caught.exception))
        self.assertNotIn('sensitive-fixture-token', self.log.getvalue())

    def test_retry_after_is_honored_and_capped(self):
        self.assertEqual(_retry_delay(response(headers={'Retry-After': '12'}), 1), 12)
        self.assertEqual(_retry_delay(response(headers={'Retry-After': '9999'}), 1), 30)
        self.assertLess(_retry_delay(response(headers={'Retry-After': 'bogus'}), 1), 3)
        self.assertLess(_retry_delay(response(headers={'Retry-After': 'NaN'}), 1), 3)

    def test_upload_failure_is_not_replayed_or_logged_verbatim(self):
        with patch('pipeline.build_datasets.requests.post', return_value=response(500, {'error': {'message': 'sensitive-fixture-token'}})) as post:
            with self.assertRaises(RuntimeError) as caught:
                upload('https://fixture.invalid', 'synthetic-token', '/api/internal/pipeline/dvp', {'rows': []})
            self.assertNotIn('sensitive-fixture-token', str(caught.exception))
            post.assert_called_once()


if __name__ == '__main__':
    unittest.main()
