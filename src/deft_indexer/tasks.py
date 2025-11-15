from __future__ import annotations

import json
import time
from typing import Iterable

import structlog
from hexbytes import HexBytes
from prometheus_client import Counter, Gauge, Histogram
from requests.exceptions import HTTPError
from sqlalchemy import select
from sqlalchemy.orm import Session
from web3 import Web3
from web3._utils.events import get_event_data
from web3.exceptions import Web3RPCError

from .celery_app import celery_app
from .config import get_settings
from .db import SessionLocal, init_db
from .logging import configure_logging
from .models import Checkpoint, Contract, EventRecord
from .publisher import kafka_producer
from .services.rate_limiter import get_rate_limiter

configure_logging()
log = structlog.get_logger(__name__)
settings = get_settings()
init_db()

events_processed = Counter(
    "deft_events_indexed_total", "Total events indexed", labelnames=("network",)
)
checkpoint_gauge = Gauge(
    "deft_indexer_checkpoint_block",
    "Latest processed block per contract",
    labelnames=("contract",),
)
rpc_requests_total = Counter(
    "deft_rpc_requests_total",
    "Total RPC requests made",
    labelnames=("method", "status"),
)
rpc_request_duration_seconds = Histogram(
    "deft_rpc_request_duration_seconds",
    "RPC request latency in seconds",
    labelnames=("method",),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
rpc_rate_limit_hits_total = Counter(
    "deft_rpc_rate_limit_hits_total",
    "Total 429 rate limit errors encountered",
    labelnames=("network",),
)
rpc_log_window_shrinks_total = Counter(
    "deft_rpc_log_window_shrinks_total",
    "Total fetch attempts that required block window shrinking",
)
reorg_detections_total = Counter(
    "deft_reorg_detections_total",
    "Total chain reorganizations detected",
    labelnames=("chain_id",),
)
index_lag_blocks = Gauge(
    "deft_index_lag_blocks",
    "Blocks behind chain head (head - checkpoint.last_block)",
    labelnames=("contract",),
)
canonical_block_checks_total = Counter(
    "deft_canonical_block_checks_total",
    "Total canonical block verification runs",
    labelnames=("chain_id", "status"),
)
chunk_size_current = Gauge(
    "deft_chunk_size_current", "Current adaptive chunk size", labelnames=("contract",)
)
logs_per_chunk_histogram = Histogram(
    "deft_logs_per_chunk",
    "Histogram of logs fetched per chunk",
    labelnames=("contract",),
    buckets=(10, 50, 100, 500, 1000, 5000, 10000),
)
contracts_enqueued_total = Counter(
    "deft_contracts_enqueued_total",
    "Total contract indexing tasks enqueued by beat scheduler",
)
contracts_skipped_total = Counter(
    "deft_contracts_skipped_total",
    "Total contracts skipped (up-to-date) by beat scheduler",
)


def _store_canonical_blocks(
    session: Session, chain_id: int, blocks: dict[int, str]
) -> None:
    """Store canonical block hashes for reorg detection."""
    from .models import CanonicalBlock

    for number, block_hash in blocks.items():
        canonical = CanonicalBlock(chain_id=chain_id, number=number, hash=block_hash)
        session.merge(canonical)


def _get_event_abis(contract_abi: str) -> list[dict]:
    parsed = json.loads(contract_abi)
    return [item for item in parsed if item.get("type") == "event"]


def _current_checkpoint(session: Session, contract_id: int) -> int | None:
    result = (
        session.execute(select(Checkpoint).where(Checkpoint.contract_id == contract_id))
        .scalars()
        .first()
    )
    return result.last_block if result else None


def _update_checkpoint(session: Session, contract_id: int, block_number: int) -> None:
    existing = (
        session.execute(select(Checkpoint).where(Checkpoint.contract_id == contract_id))
        .scalars()
        .first()
    )
    if existing:
        existing.last_block = block_number
    else:
        session.add(Checkpoint(contract_id=contract_id, last_block=block_number))


def _persist_events(
    session: Session, contract: Contract, events: Iterable[dict]
) -> None:
    for event in events:
        record = EventRecord(
            contract_id=contract.id,
            event_name=event["event"],
            block_number=event["blockNumber"],
            log_index=event["logIndex"],
            transaction_hash=event["transactionHash"].hex(),
            parsed_data=dict(event["args"]),
            raw_data={
                "transactionHash": event["transactionHash"].hex(),
                "address": event["address"],
                "topics": [
                    topic.hex() if isinstance(topic, bytes) else topic
                    for topic in event["raw_topics"]
                ],
                "data": event["raw_data"],
            },
        )
        session.merge(record)


def _emit_events(contract: Contract, events: Iterable[dict]) -> None:
    with kafka_producer() as producer:
        for event in events:
            payload = {
                "contract": contract.address,
                "network": contract.network,
                "event": event["event"],
                "block": event["blockNumber"],
                "log_index": event["logIndex"],
                "transaction_hash": event["transactionHash"].hex(),
                "parsed": dict(event["args"]),
                "raw": {
                    "transactionHash": event["transactionHash"].hex(),
                    "address": event["address"],
                    "topics": [
                        topic.hex() if isinstance(topic, bytes) else topic
                        for topic in event["raw_topics"]
                    ],
                    "data": event["raw_data"],
                },
            }
            producer.send(
                settings.kafka_topic,
                key=contract.address.encode(),
                value=json.dumps(payload).encode(),
            )


def _fetch_logs_for_range(
    web3: Web3,
    address: str,
    start_block: int,
    end_block: int,
    rate_limiter,
    min_span: int = 1,
) -> list[dict]:
    """
    Fetch logs with automatic block window shrinking on -32005 errors.

    Handles Infura's 10k log limit by recursively halving the block range
    when the provider returns too many results.
    """
    current_start = start_block
    current_end = end_block

    while True:
        try:
            logs = rate_limiter.execute(
                lambda: web3.eth.get_logs(
                    {
                        "fromBlock": current_start,
                        "toBlock": current_end,
                        "address": address,
                    }
                )
            )
            return logs
        except Web3RPCError as e:
            error_data = e.args[0] if e.args else {}
            error_code = (
                error_data.get("code") if isinstance(error_data, dict) else None
            )

            # Handle -32005: query returned more than 10000 results
            if error_code == -32005:
                span = current_end - current_start + 1

                # Check if provider suggests a range
                if isinstance(error_data, dict) and "data" in error_data:
                    data = error_data["data"]
                    if isinstance(data, dict) and "from" in data and "to" in data:
                        current_start = data["from"]
                        current_end = data["to"]
                        log.info(
                            "window_shrink_provider_suggestion",
                            original_span=span,
                            new_start=current_start,
                            new_end=current_end,
                        )
                        rpc_log_window_shrinks_total.inc()
                        continue

                # Halve the window
                if span <= min_span:
                    log.error(
                        "window_shrink_failed",
                        start=current_start,
                        end=current_end,
                        span=span,
                    )
                    raise ValueError(
                        f"Cannot shrink window below minimum span {min_span}: "
                        f"blocks {current_start}-{current_end} still exceed log limit"
                    )

                new_span = max(span // 2, min_span)
                current_end = current_start + new_span - 1
                log.info(
                    "window_shrink_automatic",
                    original_span=span,
                    new_span=new_span,
                    new_start=current_start,
                    new_end=current_end,
                )
                rpc_log_window_shrinks_total.inc()
            else:
                # Re-raise other RPC errors
                raise


def _decode_log_with_cache(raw_log: dict, abi_map: dict[str, dict], web3: Web3) -> dict:
    """
    Decode a raw log using cached ABI definitions and get_event_data.

    Returns a dict with both parsed data and raw topics/data as hex strings.
    """
    # Get topic0 (event signature)
    topic0 = raw_log["topics"][0]
    if isinstance(topic0, HexBytes):
        hex_str = topic0.hex()
        topic0_hex = hex_str if hex_str.startswith("0x") else "0x" + hex_str
    elif isinstance(topic0, bytes):
        topic0_hex = "0x" + topic0.hex()
    else:
        topic0_hex = topic0 if topic0.startswith("0x") else "0x" + topic0

    # Look up ABI definition
    if topic0_hex not in abi_map:
        raise ValueError(f"Unknown event signature: {topic0_hex}")

    event_abi = abi_map[topic0_hex]

    # Decode using get_event_data
    decoded_event = get_event_data(web3.codec, event_abi, raw_log)

    # Rehydrate into a plain dict to avoid mutating Web3 AttributeDict instances
    decoded: dict = {key: decoded_event[key] for key in decoded_event}
    if "args" in decoded_event:
        decoded["args"] = dict(decoded_event["args"])

    # Convert HexBytes to hex strings for raw data
    raw_topics = []
    for topic in raw_log["topics"]:
        if isinstance(topic, HexBytes):
            hex_str = topic.hex()
            raw_topics.append(hex_str if hex_str.startswith("0x") else "0x" + hex_str)
        elif isinstance(topic, bytes):
            raw_topics.append("0x" + topic.hex())
        else:
            raw_topics.append(topic if topic.startswith("0x") else "0x" + topic)

    if isinstance(raw_log["data"], HexBytes):
        hex_str = raw_log["data"].hex()
        raw_data = hex_str if hex_str.startswith("0x") else "0x" + hex_str
    elif isinstance(raw_log["data"], bytes):
        raw_data = "0x" + raw_log["data"].hex()
    else:
        raw_data = raw_log["data"]

    # Add raw fields to decoded copy without mutating original AttributeDict
    return {**decoded, "raw_topics": raw_topics, "raw_data": raw_data}


@celery_app.task(
    bind=True,
    autoretry_for=(HTTPError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_kwargs={"max_retries": 5},
)
def index_contract_events(
    self, contract_id: int, from_block: int | None = None, to_block: int | None = None
) -> dict:
    # Clear dedup key immediately when task starts
    from .services.enqueue_dedup import clear_dedup_key

    clear_dedup_key(contract_id, from_block or 0, to_block)

    session = SessionLocal()
    try:
        contract = (
            session.execute(
                select(Contract).where(
                    Contract.id == contract_id, Contract.active.is_(True)
                )
            )
            .scalars()
            .first()
        )
        if not contract:
            log.warning("contract_not_found", contract_id=contract_id)
            return {"status": "missing"}

        web3 = Web3(Web3.HTTPProvider(settings.rpc_url))
        rate_limiter = get_rate_limiter(settings.rpc_requests_per_second)

        # Apply finality buffer to head
        latest_block = web3.eth.block_number
        safe_head = max(0, latest_block - settings.finality_blocks)

        checkpoint = _current_checkpoint(session, contract.id)

        # Track actual indexing lag (how far behind head we are)
        if checkpoint:
            index_lag_blocks.labels(contract=contract.address).set(
                latest_block - checkpoint
            )

        # Determine start block with overlap
        if from_block is not None:
            start = from_block
        elif checkpoint is None:
            start = contract.start_block
        else:
            # Add overlap to heal potential gaps
            start = max(contract.start_block, checkpoint - settings.poll_overlap_blocks)

        # Cap end at safe_head
        end = to_block if to_block is not None else safe_head
        if end > safe_head:
            end = safe_head

        if start > end:
            log.debug("no_safe_blocks", start=start, end=end, safe_head=safe_head)
            return {
                "status": "up_to_date",
                "latest_block": latest_block,
                "safe_head": safe_head,
            }

        processed = 0
        event_abis = _get_event_abis(contract.abi)
        checksum_address = Web3.to_checksum_address(contract.address)
        contract_interface = web3.eth.contract(
            address=checksum_address, abi=json.loads(contract.abi)
        )

        # Initialize adaptive chunk controller
        from .services.chunk_controller import ChunkController

        controller = ChunkController(
            size=settings.initial_chunk_size,
            min_size=settings.min_chunk_size,
            max_size=settings.max_chunk_size,
        )
        chunk_size_current.labels(contract=contract.address).set(controller.size)

        # Build ABI topic signature cache: {topic0_hex: event_abi}
        abi_map: dict[str, dict] = {}
        for event_abi in event_abis:
            # Construct event signature: "EventName(type1,type2,...)"
            param_types = ",".join(
                input_item["type"] for input_item in event_abi.get("inputs", [])
            )
            event_signature = f"{event_abi['name']}({param_types})"
            # Compute keccak256 hash of signature
            topic0 = web3.keccak(text=event_signature)
            # Normalize to hex string with 0x prefix
            if isinstance(topic0, HexBytes):
                hex_str = topic0.hex()
                topic0_hex = hex_str if hex_str.startswith("0x") else "0x" + hex_str
            else:
                topic0_hex = topic0 if topic0.startswith("0x") else "0x" + topic0
            abi_map[topic0_hex] = event_abi

        current_block = start
        while current_block <= end:
            chunk_end = min(current_block + controller.size - 1, end)

            # Track canonical hashes for this chunk (sample at chunk_end)
            canonical_hashes: dict[int, str] = {}
            try:
                block = rate_limiter.execute(lambda: web3.eth.get_block(chunk_end))
                block_hash = (
                    block["hash"].hex()
                    if isinstance(block["hash"], bytes)
                    else block["hash"]
                )
                if not block_hash.startswith("0x"):
                    block_hash = "0x" + block_hash
                canonical_hashes[chunk_end] = block_hash
            except Exception as e:
                log.warning("block_hash_fetch_failed", block=chunk_end, error=str(e))

            # Fetch raw logs - controller handles all sizing decisions
            start_time = time.time()
            raw_logs = []
            try:
                raw_logs = rate_limiter.execute(
                    lambda: web3.eth.get_logs(
                        {
                            "address": checksum_address,
                            "fromBlock": current_block,
                            "toBlock": chunk_end,
                        }
                    )
                )
                duration = time.time() - start_time

                # Update controller based on success
                controller.on_success(duration, len(raw_logs))
                chunk_size_current.labels(contract=contract.address).set(
                    controller.size
                )
                logs_per_chunk_histogram.labels(contract=contract.address).observe(
                    len(raw_logs)
                )

                rpc_requests_total.labels(method="eth_getLogs", status="success").inc()
                rpc_request_duration_seconds.labels(method="eth_getLogs").observe(
                    duration
                )

            except Web3RPCError as e:
                # Extract error info and let controller decide next size
                error_data = e.args[0] if e.args else {}
                error_code = (
                    error_data.get("code") if isinstance(error_data, dict) else None
                )

                if error_code == -32005:
                    # Query returned too many results
                    at_min = controller.size == controller.min_size
                    new_size, backoff = controller.on_error("-32005", at_minimum=at_min)
                    chunk_size_current.labels(contract=contract.address).set(new_size)
                    rpc_log_window_shrinks_total.labels(network=contract.network).inc()
                    log.warning(
                        "log_query_overflow",
                        current=current_block,
                        end=chunk_end,
                        backoff=backoff,
                    )
                    # Backoff to prevent hot loop at min_chunk_size
                    if backoff > 0:
                        time.sleep(backoff)
                    # Skip this range, will retry with smaller size on next loop
                    continue
                else:
                    new_size, _ = controller.on_error("rpc_error")
                    chunk_size_current.labels(contract=contract.address).set(new_size)
                    raise

            except HTTPError as e:
                if e.response.status_code == 429:
                    new_size, _ = controller.on_error("rate_limit")
                    chunk_size_current.labels(contract=contract.address).set(new_size)
                    rpc_requests_total.labels(
                        method="eth_getLogs", status="rate_limited"
                    ).inc()
                    rpc_rate_limit_hits_total.labels(network=contract.network).inc()
                else:
                    new_size, _ = controller.on_error("http_error")
                    chunk_size_current.labels(contract=contract.address).set(new_size)
                    rpc_requests_total.labels(
                        method="eth_getLogs", status="error"
                    ).inc()
                raise

            except TimeoutError:
                new_size, _ = controller.on_error("timeout")
                chunk_size_current.labels(contract=contract.address).set(new_size)
                raise

            # Decode each raw log using cached ABI map
            chunk_events: list[dict] = []
            for raw_log in raw_logs:
                try:
                    decoded = _decode_log_with_cache(raw_log, abi_map, web3)
                    chunk_events.append(decoded)
                except Exception as e:
                    log.warning("decode_failed", raw_log=raw_log, error=str(e))
                    continue

            if chunk_events:
                _persist_events(session, contract, chunk_events)
                _emit_events(contract, chunk_events)
                processed += len(chunk_events)
                events_processed.labels(network=contract.network).inc(len(chunk_events))

            # Store canonical blocks
            _store_canonical_blocks(session, contract.chain_id, canonical_hashes)

            _update_checkpoint(session, contract.id, chunk_end)
            session.commit()
            checkpoint_gauge.labels(contract=contract.address).set(chunk_end)

            current_block = chunk_end + 1

        log.info(
            "index_completed",
            contract_id=contract_id,
            processed=processed,
            start=start,
            end=end,
        )
        return {
            "status": "ok",
            "start": start,
            "end": end,
            "processed": processed,
            "contract_id": contract.id,
        }
    except HTTPError as e:
        session.rollback()
        if e.response.status_code == 429:
            log.warning(
                "rate_limit_exceeded",
                contract_id=contract_id,
                retry_delay=settings.rpc_rate_limit_retry_delay,
            )
            # Retry with longer delay for rate limits
            raise self.retry(exc=e, countdown=settings.rpc_rate_limit_retry_delay)
        log.exception("index_failed", contract_id=contract_id)
        raise
    except Exception:
        session.rollback()
        log.exception("index_failed", contract_id=contract_id)
        raise
    finally:
        session.close()


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True)
def verify_recent_canonical(self, chain_id: int = 1) -> dict:
    """
    Verify canonical block hashes within finality window.

    Detects chain reorgs by comparing stored hashes with current RPC state.
    Rolls back affected events and checkpoints, re-enqueues indexing from fork point.
    """
    session = SessionLocal()
    try:
        from sqlalchemy import delete as sql_delete

        from .models import CanonicalBlock, EventRecord

        web3 = Web3(Web3.HTTPProvider(settings.rpc_url))
        rate_limiter = get_rate_limiter(settings.rpc_requests_per_second)

        head = web3.eth.block_number
        safe_head = max(0, head - settings.finality_blocks)
        window_start = max(0, head - settings.finality_blocks)

        # Fetch current chain state (store as 0x-prefixed hex)
        rpc_hashes: dict[int, str] = {}
        for block_num in range(window_start, head + 1):
            try:
                block = rate_limiter.execute(lambda n=block_num: web3.eth.get_block(n))
                # Ensure 0x prefix for consistent format
                block_hash = (
                    block["hash"].hex()
                    if isinstance(block["hash"], bytes)
                    else block["hash"]
                )
                if not block_hash.startswith("0x"):
                    block_hash = "0x" + block_hash
                rpc_hashes[block_num] = block_hash
            except Exception as e:
                log.warning(
                    "reorg_check_block_fetch_failed", block=block_num, error=str(e)
                )
                continue

        # Fetch stored canonical hashes
        stored_blocks = (
            session.execute(
                select(CanonicalBlock).where(
                    CanonicalBlock.chain_id == chain_id,
                    CanonicalBlock.number >= window_start,
                    CanonicalBlock.number <= head,
                )
            )
            .scalars()
            .all()
        )

        stored_hashes = {block.number: block.hash for block in stored_blocks}

        # Detect mismatches
        mismatches = [
            num
            for num, rpc_hash in rpc_hashes.items()
            if num in stored_hashes and stored_hashes[num] != rpc_hash
        ]

        # Update canonical records with latest hashes
        for num, rpc_hash in rpc_hashes.items():
            session.merge(CanonicalBlock(chain_id=chain_id, number=num, hash=rpc_hash))

        if not mismatches:
            canonical_block_checks_total.labels(chain_id=chain_id, status="ok").inc()
            session.commit()
            return {"status": "ok", "window_start": window_start, "head": head}

        # Reorg detected - find fork point
        fork_at = min(mismatches)
        log.warning("reorg_detected", fork_at=fork_at, mismatches=len(mismatches))

        reorg_detections_total.labels(chain_id=chain_id).inc()
        canonical_block_checks_total.labels(chain_id=chain_id, status="reorg").inc()

        # First, identify affected contracts
        affected_contract_ids = (
            session.execute(
                select(EventRecord.contract_id)
                .where(
                    EventRecord.chain_id == chain_id,
                    EventRecord.block_number >= fork_at,
                )
                .distinct()
            )
            .scalars()
            .all()
        )

        # Single scoped delete for all affected events
        delete_result = session.execute(
            sql_delete(EventRecord).where(
                EventRecord.chain_id == chain_id,
                EventRecord.block_number >= fork_at,
                EventRecord.contract_id.in_(affected_contract_ids),
            )
        )
        deleted_count = delete_result.rowcount

        checkpoints = (
            session.execute(
                select(Checkpoint).where(
                    Checkpoint.contract_id.in_(affected_contract_ids)
                )
            )
            .scalars()
            .all()
        )

        contracts_to_reindex = []
        for cp in checkpoints:
            if cp.last_block and cp.last_block >= fork_at:
                cp.last_block = max(fork_at - 1, 0)
                if cp.last_finalized and cp.last_finalized >= fork_at:
                    cp.last_finalized = max(fork_at - 1, 0)
                contracts_to_reindex.append(cp.contract_id)

        session.commit()

        # Re-enqueue affected contracts with dedup
        from .services.enqueue_dedup import enqueue_with_dedup

        for contract_id in contracts_to_reindex:
            enqueue_with_dedup(contract_id, from_block=fork_at, to_block=safe_head)

        return {
            "status": "reorg",
            "fork_at": fork_at,
            "deleted_events": deleted_count,
            "reindexed_contracts": len(contracts_to_reindex),
        }

    except Exception:
        session.rollback()
        log.exception("reorg_check_failed", chain_id=chain_id)
        raise
    finally:
        session.close()


@celery_app.task
def enqueue_active_contracts() -> dict:
    """
    Enqueue indexing tasks for active contracts near chain head.

    Applies finality buffer and overlap to heal gaps without duplicate work.
    """
    session = SessionLocal()
    try:
        web3 = Web3(Web3.HTTPProvider(settings.rpc_url))
        head = web3.eth.block_number
        safe_head = max(0, head - settings.finality_blocks)

        contracts = (
            session.execute(select(Contract).where(Contract.active.is_(True)))
            .scalars()
            .all()
        )

        scheduled = 0
        skipped = 0

        from .services.enqueue_dedup import enqueue_with_dedup

        for contract in contracts:
            checkpoint = _current_checkpoint(session, contract.id)

            # Determine start block with overlap
            if checkpoint is None:
                start = contract.start_block
            else:
                # Add overlap to heal potential gaps
                start = max(
                    contract.start_block, checkpoint - settings.poll_overlap_blocks
                )

            # Ensure overlap doesn't push start above safe_head
            if start > safe_head:
                skipped += 1
                contracts_skipped_total.inc()
                log.debug(
                    "contract_up_to_date",
                    contract_id=contract.id,
                    checkpoint=checkpoint,
                    safe_head=safe_head,
                )
                continue

            # Enqueue with dedup to prevent duplicate tasks
            enqueued = enqueue_with_dedup(
                contract.id, from_block=start, to_block=safe_head
            )

            if enqueued:
                scheduled += 1
                contracts_enqueued_total.inc()
                log.debug(
                    "contract_enqueued",
                    contract_id=contract.id,
                    start=start,
                    safe_head=safe_head,
                )
            else:
                skipped += 1
                contracts_skipped_total.inc()
                log.debug("contract_already_queued", contract_id=contract.id)

        return {
            "scheduled": scheduled,
            "skipped": skipped,
            "head": head,
            "safe_head": safe_head,
        }

    finally:
        session.close()
