# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**deft-indexer** is a blockchain event indexer that registers smart contracts via a FastAPI control plane, streams events through Celery workers, persists data in PostgreSQL, and fans out real-time payloads to Kafka/Redpanda. The entire stack runs containerized with Docker Compose.

## Development Commands

### Setup
```bash
# Install dependencies (including dev tools)
python3 -m pip install -e .[dev]

# Run with Docker Compose (builds and starts all services)
docker compose -f docker-compose.yml up --build

# Run tests
pytest

# Run specific test
pytest tests/test_api.py::test_register_contract
```

### Testing
```bash
# Run full test suite
pytest

# Run with coverage report
pytest --cov=src/deft_indexer --cov-report=term-missing

# Run specific test module
pytest tests/test_api_endpoints.py -v

# Run tests matching pattern
pytest -k "error" -v
```

**Test Structure**: Tests are organized into focused modules:
- `test_api_endpoints.py`: Happy-path endpoint tests
- `test_api_errors.py`: Error scenario validation
- `test_abi_resolution.py`: ABI service unit tests
- `test_tasks.py`: Celery task logic with Web3 mocking
- `test_models.py`: Database constraint and relationship tests
- `test_rate_limiter.py`: RPC rate limiter unit tests

All tests use SQLite (`test.db`) and mock external dependencies (Web3, httpx, Celery, Kafka). The `reset_database` fixture ensures clean state between tests. See `tests/README.md` for detailed testing documentation.

### Important Endpoints
- API: http://localhost:8080
- API Docs: http://localhost:8080/docs
- Prometheus Metrics: http://localhost:8080/metrics (API-side) and http://localhost:9000/metrics (worker-side)
- Kafka (external): localhost:29092
- PostgreSQL: localhost:5432

**Note**: Metrics are exposed at two endpoints:
- **:8080/metrics** - FastAPI process metrics (API requests, rate limiting)
- **:9000/metrics** - Celery worker metrics (RPC calls, chunk sizing, indexing throughput)

Both endpoints share the same Prometheus registry, so you can scrape either or both depending on your needs. In a multi-worker setup, each worker will attempt to bind to :9000 - the first one wins and exposes all workers' metrics via the shared registry.

### Configuration
Key settings in `.env`:
- `RPC_URL`: Blockchain RPC endpoint (required, no default)
- `DATABASE_URL`: PostgreSQL connection string
- `CHUNK_SIZE`: Blocks per indexing batch (default: 2500)
- `BATCH_CONCURRENCY`: Celery worker concurrency (default: 4)
- `RPC_REQUESTS_PER_SECOND`: Max RPC requests/sec to avoid rate limiting (default: 10.0)
- `RPC_RATE_LIMIT_RETRY_DELAY`: Seconds to wait after 429 rate limit error (default: 60)
- `METRICS_PORT`: Prometheus metrics server port for Celery workers (default: 9000)

## Architecture

### Core Components

**1. FastAPI Control Plane** (`src/deft_indexer/api/main.py`)
- `POST /watch`: Registers contracts, validates on-chain code, resolves ABI, starts indexing
- `GET /contracts`: Lists all registered contracts
- `GET /contracts/{id}/events`: Retrieves indexed events
- `POST /contracts/{id}/reindex`: Triggers reindexing for a specific block range
- `GET /metrics`: Prometheus metrics endpoint

**2. Celery Task System** (`src/deft_indexer/tasks.py`)
- `index_contract_events`: Main indexing task that chunks through block ranges
- `enqueue_active_contracts`: Scheduled by Beat every 30s to poll all active contracts
- Auto-retry with exponential backoff on RPC failures (max 5 retries)
- Processes events in chunks, updates checkpoints atomically per chunk

**3. Data Models** (`src/deft_indexer/models.py`)
- **Contract**: Stores address, network, ABI, start_block, active flag
- **Checkpoint**: Tracks last successfully indexed block per contract
- **EventRecord**: Stores both parsed (structured) and raw (topics/data) event payloads
- Unique constraint on `(contract_id, block_number, transaction_hash)` prevents duplicates

**4. ABI Resolution** (`src/deft_indexer/services/abi.py`)
- Supports inline JSON ABI or Etherscan API URLs
- Validates ABI structure (must be JSON array)
- Raises `ABIResolutionError` for invalid formats

**5. Kafka Publisher** (`src/deft_indexer/publisher.py`)
- Context manager for idempotent Kafka producer
- Uses contract address as message key for partitioning
- Configured with `acks=all` and `enable_idempotence=True`

**6. RPC Rate Limiter** (`src/deft_indexer/services/rate_limiter.py`)
- Token bucket rate limiter to prevent RPC provider throttling
- Configurable requests-per-second threshold
- Thread-safe with lock-based synchronization
- Tracks throttle delays via Prometheus metrics

### Data Flow

1. Client calls `POST /watch` with contract address, network, start_block, and ABI
2. API validates contract exists on-chain via `web3.eth.get_code`
3. ABI is resolved (inline JSON or fetched from Etherscan)
4. Contract record created in PostgreSQL with `active=True`
5. `index_contract_events.delay()` queues first indexing task
6. Celery Beat continuously schedules active contracts every 30s
7. Worker fetches logs via `eth_getLogs` in chunks (default 2500 blocks), rate-limited per `RPC_REQUESTS_PER_SECOND`
8. If RPC returns `-32005` (too many logs), block window is automatically halved and retried
9. Events are parsed using contract ABI, then:
   - Inserted/merged into `events` table
   - Published to Kafka topic `deft.events`
   - Checkpoint updated to chunk end block
10. Process repeats until worker catches up to chain head

### Key Architectural Patterns

**Checkpointing**: Each contract has a `Checkpoint` record tracking `last_block`. Workers resume from `checkpoint + 1` after restarts or failures. Checkpoints update after each chunk commit, ensuring no block ranges are skipped or duplicated.

**Idempotent Event Storage**: `EventRecord` uses SQLAlchemy `session.merge()` with a unique index on `(contract_id, block_number, transaction_hash)`. This prevents duplicate events if reindexing overlaps.

**Kafka Ordering**: Messages use contract address as key, ensuring all events for a contract land in the same partition and maintain ordering.

**Dual Payload Format**: Events store both `parsed_data` (decoded via ABI) and `raw_data` (topics + hex data). This supports consumers needing structured data or requiring custom decoding.

**Automatic Block Window Shrinking**: When RPC providers return error `-32005` (query exceeds 10k log limit), the indexer automatically halves the block range and retries. This continues recursively until the range is small enough or hits a minimum span threshold. Providers may suggest specific ranges in error responses, which are honored when available.

**RPC Rate Limiting**: Token bucket limiter enforces configurable requests-per-second threshold to prevent 429 rate limit errors. Workers sleep between requests as needed, with delays tracked in metrics. On 429 errors, tasks retry with exponential backoff starting at `RPC_RATE_LIMIT_RETRY_DELAY` seconds.

**Prometheus Metrics**:
- `deft_events_indexed_total`: Counter of processed events (labeled by network)
- `deft_indexer_checkpoint_block`: Gauge showing latest block per contract
- `deft_rpc_requests_total`: Counter of RPC requests by method and status (success/error/rate_limited)
- `deft_rpc_request_duration_seconds`: Gauge tracking RPC request latency by method
- `deft_rpc_rate_limit_hits_total`: Counter of 429 rate limit errors by network
- `deft_rpc_log_window_shrinks_total`: Counter of block window shrink operations
- `deft_rpc_throttle_delay_seconds_total`: Counter of total seconds spent waiting due to rate limiting

### Database Schema Notes

- `contracts.active`: Boolean flag to pause/resume indexing without deleting records
- `checkpoints.last_block`: BigInteger supporting chain block heights
- `events.log_index`: Integer to handle multiple events in same transaction
- Cascade deletes: Removing a contract deletes all checkpoints and events

### Celery Configuration (`src/deft_indexer/celery_app.py`)

- `task_acks_late=True`: Requeue on worker crash
- `worker_prefetch_multiplier=1`: One task per worker at a time (prevents backlog pileup)
- `task_time_limit=300`: 5-minute timeout per task
- `beat_schedule`: Polls active contracts every 30 seconds

### Testing Strategy

Tests use `sqlite:///test.db` and mock Web3/Celery dependencies. The `reset_database` fixture ensures clean state per test. Key mocks:
- `get_web3()`: Returns fake Web3 with stubbed `is_address()` and `eth.get_code()`
- `resolve_abi()`: Returns empty ABI array
- `index_contract_events.delay()`: Records call arguments without executing

## Common Development Scenarios

**Adding a New Endpoint**: Update `src/deft_indexer/api/main.py`, ensure session dependencies use `Depends(get_session)`, follow existing response model patterns.

**Modifying Database Schema**: Edit `src/deft_indexer/models.py`, then either restart containers (migrations auto-run on startup) or manually run `Base.metadata.create_all(bind=engine)`.

**Adjusting Indexing Logic**: Edit `src/deft_indexer/tasks.py`. Key functions: `_persist_events()` for storage, `_emit_events()` for Kafka, `_update_checkpoint()` for progress tracking, `_fetch_logs_for_range()` for window shrinking logic, `_decode_log_with_cache()` for ABI-based event parsing.

**Running Without Containers**: Set `DATABASE_URL`, `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS` to local services, then:
```bash
# Terminal 1: Start API
uvicorn deft_indexer.api.main:app --host 0.0.0.0 --port 8080

# Terminal 2: Start Celery worker
celery -A deft_indexer.celery_app.celery_app worker --loglevel=info

# Terminal 3: Start Celery beat
celery -A deft_indexer.celery_app.celery_app beat --loglevel=info
```

## Important Notes

- **RPC Rate Limits**: Adjust `CHUNK_SIZE` based on RPC provider limits (Infura free tier: ~100k requests/day). Configure `RPC_REQUESTS_PER_SECOND` to match your provider's rate limit (e.g., 1.0 for free tiers, 10.0+ for paid plans)
- **Automatic Window Shrinking**: Indexer handles `-32005` errors (log limit exceeded) by automatically reducing block ranges. No manual intervention needed for high-activity contracts
- **Rate Limit Recovery**: On 429 errors, tasks automatically retry after `RPC_RATE_LIMIT_RETRY_DELAY` seconds with exponential backoff (max 5 retries)
- **Reindexing**: Use `POST /contracts/{id}/reindex` with `from_block`/`to_block` to backfill or repair data
- **Event Deduplication**: Safe to reindex overlapping ranges; database constraints prevent duplicates. You may see `UniqueViolation` errors in logs when dedup/enqueue overlaps touch the same block range - this is expected behavior and prevents actual duplicate events in the database
- **Network Support**: Currently single-network per deployment. For multi-network, deploy separate instances with different `RPC_URL` and databases
- **SQLite for Tests**: Tests use SQLite with `test.db` file. Production requires PostgreSQL for concurrent writes
