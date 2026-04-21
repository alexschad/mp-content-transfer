from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.rate_limit import RateLimiter


class RateLimiterTest(TestCase):
    def test_rate_limiter_sleeps_when_needed(self) -> None:
        limiter = RateLimiter(requests_per_second=2.0)
        with patch("src.rate_limit.time.monotonic", side_effect=[0.0, 0.1, 0.5]), patch(
            "src.rate_limit.time.sleep"
        ) as sleep_mock:
            limiter.acquire()
            limiter.acquire()
        sleep_mock.assert_called_once()
