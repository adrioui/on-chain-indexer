"""API rate limiting using Redis."""

import time
from typing import Annotated

import redis
import structlog
from fastapi import HTTPException, Request, status
from prometheus_client import Counter

from ..config import get_settings

log = structlog.get_logger(__name__)
settings = get_settings()

# Redis client for rate limiting
_redis_client: redis.Redis | None = None

# Metrics
api_rate_limit_hits_total = Counter(
    "deft_api_rate_limit_hits_total",
    "Total API rate limit rejections",
    labelnames=("route", "ip"),
)

api_requests_total = Counter(
    "deft_api_requests_total",
    "Total API requests by route",
    labelnames=("route", "status"),
)


def get_redis_client() -> redis.Redis:
    """Get or create Redis client."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


def check_rate_limit(
    ip: str, route: str, max_requests: int = 10, window_seconds: int = 60
) -> tuple[bool, dict]:
    """
    Check if IP is within rate limit for route.

    Returns (allowed, info_dict) where info_dict contains:
    - current_count: Current request count
    - limit: Maximum allowed requests
    - window: Time window in seconds
    - retry_after: Seconds until limit resets (if exceeded)
    """
    client = get_redis_client()
    current_window = int(time.time()) // window_seconds
    key = f"rate_limit:{route}:{ip}:{current_window}"

    try:
        count = client.incr(key)
        if count == 1:
            client.expire(key, window_seconds)

        info = {
            "current_count": count,
            "limit": max_requests,
            "window": window_seconds,
            "retry_after": window_seconds if count > max_requests else 0,
        }

        return count <= max_requests, info

    except Exception as e:
        log.error("rate_limit_check_failed", ip=ip, route=route, error=str(e))
        # Fail open on Redis errors
        return True, {"error": str(e)}


def rate_limit_middleware(
    request: Request, route: str, max_requests: int = 10, window_seconds: int = 60
) -> None:
    """
    Rate limiting middleware for FastAPI routes.

    Raises HTTPException 429 if limit exceeded.
    """
    from ..config import get_settings

    settings = get_settings()

    # Extract real IP from X-Forwarded-For only if behind trusted proxy
    forwarded_for = request.headers.get("x-forwarded-for")
    if settings.trust_proxy and forwarded_for:
        # Take first IP (client), ignore proxy chain
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = request.client.host if request.client else "unknown"

    allowed, info = check_rate_limit(client_ip, route, max_requests, window_seconds)

    if not allowed:
        api_rate_limit_hits_total.labels(route=route, ip=client_ip).inc()
        api_requests_total.labels(route=route, status="rate_limited").inc()
        log.warning(
            "rate_limit_exceeded",
            ip=client_ip,
            route=route,
            count=info["current_count"],
            limit=info["limit"],
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Try again in {info['retry_after']} seconds.",
            headers={"Retry-After": str(info["retry_after"])},
        )

    api_requests_total.labels(route=route, status="ok").inc()
