"""Tests for RPC rate limiter."""

import time

import pytest

from deft_indexer.services.rate_limiter import RPCRateLimiter


def test_rate_limiter_throttles_requests():
    """Test rate limiter enforces request spacing."""
    limiter = RPCRateLimiter(requests_per_second=10.0)

    start = time.time()
    limiter.acquire()
    limiter.acquire()
    elapsed = time.time() - start

    # Should take at least 0.1 seconds (1/10 req/sec)
    assert elapsed >= 0.09  # Allow small margin


def test_rate_limiter_execute():
    """Test rate limiter execute wrapper."""
    limiter = RPCRateLimiter(requests_per_second=10.0)

    result = limiter.execute(lambda: "success")
    assert result == "success"


def test_rate_limiter_concurrent_access():
    """Test rate limiter is thread-safe."""
    from concurrent.futures import ThreadPoolExecutor

    limiter = RPCRateLimiter(requests_per_second=5.0)
    results = []

    def make_request():
        limiter.acquire()
        results.append(time.time())

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(make_request) for _ in range(3)]
        for future in futures:
            future.result()

    # Requests should be spaced at least 0.2 seconds apart
    assert len(results) == 3
    assert results[1] - results[0] >= 0.19
    assert results[2] - results[1] >= 0.19
