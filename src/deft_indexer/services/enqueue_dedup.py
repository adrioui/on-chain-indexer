"""Redis-based enqueue deduplication to prevent duplicate tasks."""

import redis
import structlog

from ..config import get_settings

log = structlog.get_logger(__name__)
settings = get_settings()

_redis_client: redis.Redis | None = None


def get_redis_client() -> redis.Redis:
    """Get or create Redis client."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


def enqueue_with_dedup(
    contract_id: int, from_block: int, to_block: int | None = None
) -> bool:
    """
    Enqueue indexing task with deduplication.

    Uses Redis SETNX with dynamic TTL to prevent duplicate tasks for same range.
    TTL = max(60s, estimated_duration * 2) to handle long-running tasks.
    Returns True if task was enqueued, False if already queued.
    """
    from ..tasks import index_contract_events

    client = get_redis_client()
    to_block_str = str(to_block) if to_block is not None else "head"
    key = f"q:{contract_id}:{from_block}-{to_block_str}"

    try:
        # Calculate dynamic TTL based on range size
        # Assume ~1000 blocks/minute processing rate
        range_size = (to_block - from_block) if to_block is not None else 1000
        estimated_duration = max(60, int(range_size / 1000 * 60) * 2)  # 2x buffer

        # Try to acquire lock
        acquired = client.set(key, "1", nx=True, ex=estimated_duration)

        if acquired:
            index_contract_events.delay(
                contract_id, from_block=from_block, to_block=to_block
            )
            log.debug(
                "task_enqueued_with_dedup",
                contract_id=contract_id,
                from_block=from_block,
                to_block=to_block,
                ttl=estimated_duration,
            )
            return True
        else:
            log.debug(
                "task_already_queued",
                contract_id=contract_id,
                from_block=from_block,
                to_block=to_block,
            )
            return False

    except Exception as e:
        log.error("dedup_check_failed", contract_id=contract_id, error=str(e))
        # Fail open - enqueue anyway
        index_contract_events.delay(
            contract_id, from_block=from_block, to_block=to_block
        )
        return True


def clear_dedup_key(
    contract_id: int, from_block: int, to_block: int | None = None
) -> None:
    """Clear dedup key when task starts processing."""
    client = get_redis_client()
    to_block_str = str(to_block) if to_block is not None else "head"
    key = f"q:{contract_id}:{from_block}-{to_block_str}"

    try:
        client.delete(key)
        log.debug(
            "dedup_key_cleared",
            contract_id=contract_id,
            from_block=from_block,
            to_block=to_block,
        )
    except Exception as e:
        log.warning("dedup_key_clear_failed", contract_id=contract_id, error=str(e))
