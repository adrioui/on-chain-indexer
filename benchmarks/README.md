# Benchmark Suite

## Prerequisites

- Docker Compose stack running (`docker compose up`)
- k6 installed (`brew install k6` or see https://k6.io/docs/getting-started/installation/)
- Python 3.13+ with web3.py (`pip install web3 requests`)
- Anvil for local testing (`curl -L https://foundry.paradigm.xyz | bash && foundryup`)

## API Load Tests

### Watch Endpoint
Tests POST /watch under concurrent load:

```bash
k6 run benchmarks/bench_api_watch.js
```

**SLO**: p95 < 200ms, error rate < 1%, no 5xx errors

### List Endpoint
Tests GET /contracts under concurrent load:

```bash
k6 run benchmarks/bench_api_list.js
```

**SLO**: p95 < 100ms, error rate < 1%

### Custom API URL
```bash
API_URL=http://production-api:8080 k6 run benchmarks/bench_api_watch.js
```

## Throughput Benchmark

Tests end-to-end indexing throughput with Anvil local chain.

### Setup

1. Start Anvil:
```bash
anvil --host 0.0.0.0 --block-time 1
```

2. Start deft-indexer stack:
```bash
docker compose up
```

### Run Benchmark

```bash
python3 benchmarks/bench_throughput.py
```

**SLO**: ≥10 events/sec end-to-end throughput

### Customize

Edit constants in `benchmarks/bench_throughput.py`:
- `TARGET_EVENTS`: Number of events to generate (default: 1000)
- `TARGET_THROUGHPUT`: SLO threshold (default: 10 events/sec)
- `ANVIL_RPC`: Anvil endpoint
- `API_BASE`: deft-indexer API endpoint

## Interpreting Results

### k6 Output
- **http_req_duration p(95)**: 95th percentile latency
- **errors rate**: Percentage of failed checks
- **http_reqs**: Total requests/sec

### Throughput Benchmark
- **Throughput**: Events indexed per second
- **SLO MET/MISSED**: Pass/fail against target

### Prometheus Metrics
Check metrics during benchmarks:

```bash
curl localhost:8080/metrics | grep -E '(deft_events_indexed_total|deft_chunk_size_current|deft_rpc_requests_total)'
```

## Continuous Monitoring

Run benchmarks on every release:

```bash
make benchmark
```

Include results in release notes.
