"""RPC request rate limiting service."""

import time
from threading import Lock
from typing import Callable, TypeVar

import structlog
from prometheus_client import Counter

log = structlog.get_logger(__name__)

# Metric for throttle delays
throttle_delay_seconds = Counter(
    "deft_rpc_throttle_delay_seconds_total",
    "Total seconds spent waiting due to rate limiting",
)

T = TypeVar("T")


class RPCRateLimiter:
    """Token bucket rate limiter for RPC requests."""

    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self.lock = Lock()

    def acquire(self) -> None:
        """Block until a request slot is available."""
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_request_time

            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                log.debug("rate_limit_throttle", sleep_seconds=sleep_time)
                time.sleep(sleep_time)
                throttle_delay_seconds.inc(sleep_time)  # Track delay

            self.last_request_time = time.time()

    def execute(self, func: Callable[[], T]) -> T:
        """Execute a function with rate limiting."""
        self.acquire()
        return func()


# Global rate limiter instance (initialized in tasks.py)
_rate_limiter: RPCRateLimiter | None = None


def get_rate_limiter(requests_per_second: float) -> RPCRateLimiter:
    """Get or create the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RPCRateLimiter(requests_per_second)
    return _rate_limiter
