from __future__ import annotations

from http.client import IncompleteRead
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.client import MPClient, Response, RetryableTransportError
from src.config import EndpointConfig, RetryConfig


class RetryingClient(MPClient):
    def __init__(self) -> None:
        super().__init__(
            endpoint=EndpointConfig(
                base_url="https://api.metropublisher.com",
                instance_id="123",
                api_key="key",
                api_secret="secret",
                requests_per_second=0,
            ),
            retry=RetryConfig(
                retry_count=2,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                backoff_jitter_seconds=0,
            ),
            limiter=type("NoopLimiter", (), {"acquire": lambda self: None})(),
            auth_provider="https://go.vanguardistas.net",
            access_token="token",
        )
        self.send_attempts = 0

    def _send(self, method, url, params, json_body, data, headers):
        self.send_attempts += 1
        if self.send_attempts == 1:
            raise RetryableTransportError("GET https://example.com/file.bin failed: incomplete read (13 bytes received)")
        return Response(status_code=200, content=b"done", headers={})


class ClientRetryTest(TestCase):
    def test_download_retries_on_incomplete_read(self) -> None:
        client = RetryingClient()
        with patch("src.client.time.sleep", return_value=None):
            content = client.download("https://example.com/file.bin")
        self.assertEqual(content, b"done")
        self.assertEqual(client.send_attempts, 2)
