from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time


@dataclass
class RateLimiter:
    requests_per_second: float
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _next_allowed: float = 0.0

    def acquire(self) -> None:
        if self.requests_per_second <= 0:
            return
        spacing = 1.0 / self.requests_per_second
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = max(now, self._next_allowed) + spacing
