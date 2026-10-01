"""Bounded recovery for the read-only pipeline configuration request.

Uploads and live verification (which records snapshots) are not automatically
replayed. Diagnostics never include credentials, URLs or raw response bodies.
"""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import random
import re
import time

import requests

RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 530}
STOP_CODES = {'ESPN_AUTH_EXPIRED', 'ESPN_NOT_CONNECTED', 'CREDENTIAL_KEY_MISSING', 'PIPELINE_CONFIG_INVALID'}
MAX_ATTEMPTS = 3


def _field(value, pattern, limit=80):
    return value if isinstance(value, str) and len(value) <= limit and re.fullmatch(pattern, value) else None


def response_diagnostic(response):
    """Only allow known diagnostic fields, never the server's free-text message."""
    result = {'status': response.status_code}
    try:
        body = response.json()
    except ValueError:
        body = {}
    error = body.get('error', {}) if isinstance(body, dict) else {}
    error = error if isinstance(error, dict) else {}
    details = error.get('details', {})
    details = details if isinstance(details, dict) else {}
    fields = {
        'code': _field(error.get('code'), r'[A-Z][A-Z0-9_]*'),
        'requestId': _field(details.get('requestId') or (body.get('requestId') if isinstance(body, dict) else None), r'[a-f0-9-]{36}'),
        'cfRay': _field(response.headers.get('cf-ray'), r'[a-fA-F0-9]{16,32}(?:-[A-Z]{3})?'),
        'stage': details.get('stage') if details.get('stage') in {'list-leagues', 'league-bundle', 'validate-config'} else None,
        'category': details.get('category') if details.get('category') in {'storage', 'credentials', 'espn', 'application'} else None,
    }
    result.update({key: value for key, value in fields.items() if value is not None})
    return result


def _retry_delay(response, attempt):
    fallback = min(2 ** attempt, 8) + random.uniform(0, 0.5)
    if response is None:
        return fallback
    value = response.headers.get('Retry-After', '')
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return fallback
    # Bound wall time even if a proxy returns an excessive or invalid delay.
    return min(max(seconds, 0), 30) if seconds == seconds else fallback


def fetch_pipeline_config(base, token):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = None
        diagnostic = {'event': 'pipeline_config_request', 'attempt': attempt, 'maxAttempts': MAX_ATTEMPTS}
        retryable = False
        try:
            response = requests.get(f"{base.rstrip('/')}/api/internal/pipeline/config",
                                    headers={'Authorization': f'Bearer {token}'}, timeout=(10, 30),
                                    allow_redirects=False)
            diagnostic.update(response_diagnostic(response))
            if response.status_code == 200:
                try:
                    config = response.json()
                except ValueError:
                    config = None
                if isinstance(config, dict) and isinstance(config.get('leagues'), list):
                    print(json.dumps({**diagnostic, 'outcome': 'success'}), flush=True)
                    return config
                diagnostic['code'] = 'INVALID_CONFIG_RESPONSE'
            else:
                retryable = (response.status_code in RETRY_STATUSES
                             and diagnostic.get('code') not in STOP_CODES
                             and diagnostic.get('category') != 'credentials')
        except requests.exceptions.SSLError:
            diagnostic['code'] = 'TLS_ERROR'
        except (requests.Timeout, requests.ConnectionError):
            diagnostic['code'] = 'NETWORK_UNAVAILABLE'
            retryable = True
        except requests.RequestException:
            diagnostic['code'] = 'REQUEST_FAILED'
        finally:
            if response is not None:
                response.close()
        retry = retryable and attempt < MAX_ATTEMPTS
        delay = _retry_delay(response, attempt) if retry else 0
        print(json.dumps({**diagnostic, 'outcome': 'retry' if retry else 'failed',
                          'retryInSeconds': round(delay, 2)}), flush=True)
        if not retry:
            raise RuntimeError('Pipeline configuration failed: ' + json.dumps(diagnostic)) from None
        time.sleep(delay)
