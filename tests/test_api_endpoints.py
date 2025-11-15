import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from web3 import Web3

os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path("test.db").absolute()))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from deft_indexer.api.main import app
from deft_indexer.db import Base, engine, get_session
from deft_indexer.models import Contract, EventRecord


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client(monkeypatch):
    class FakeWeb3:
        def is_address(self, address):
            return True

        def is_checksum_address(self, address):
            return True

        class Eth:
            block_number = 1000000  # Mock current block number

            @staticmethod
            def get_code(_):
                return b"deadbeef"

        eth = Eth()

    async def fake_resolve(*_args, **_kwargs):
        return "[]"

    recorded = {}

    def fake_delay(contract_id: int, from_block=None, to_block=None):
        recorded["contract_id"] = contract_id
        recorded["from_block"] = from_block
        recorded["to_block"] = to_block
        return None

    def fake_enqueue_with_dedup(contract_id: int, from_block: int, to_block=None):
        recorded["contract_id"] = contract_id
        recorded["from_block"] = from_block
        recorded["to_block"] = to_block
        return True

    # Mock Redis for rate limiting - always allow
    class FakeRedis:
        def incr(self, key):
            return 1

        def expire(self, key, seconds):
            return True

    def fake_get_redis_client():
        return FakeRedis()

    monkeypatch.setattr("deft_indexer.api.main.get_web3", lambda: FakeWeb3())
    monkeypatch.setattr("deft_indexer.api.main.resolve_abi", fake_resolve)
    monkeypatch.setattr("deft_indexer.api.main.index_contract_events.delay", fake_delay)
    monkeypatch.setattr(
        "deft_indexer.services.enqueue_dedup.enqueue_with_dedup",
        fake_enqueue_with_dedup,
    )
    monkeypatch.setattr(
        "deft_indexer.services.api_rate_limiter.get_redis_client", fake_get_redis_client
    )

    with TestClient(app) as test_client:
        yield test_client, recorded


@pytest.fixture
def sample_contract(client):
    """Create a sample contract for tests needing existing data."""
    test_client, _ = client
    session = next(get_session())
    contract = Contract(
        address="0x1111111111111111111111111111111111111111",
        network="ethereum",
        abi="[]",
        start_block=100,
        active=True,
    )
    session.add(contract)
    session.commit()
    session.refresh(contract)
    yield contract
    session.close()


def test_health_endpoint(client):
    """Test GET /health returns ok status."""
    test_client, _ = client
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_register_contract_success(client):
    """Test POST /watch with valid payload creates contract and enqueues task."""
    test_client, recorded = client
    payload = {
        "address": "0x0000000000000000000000000000000000000000",
        "network": "ethereum",
        "start_block": 100,
        "abi_json": "[]",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 201
    body = response.json()
    assert body["address"] == payload["address"]
    assert body["network"] == "ethereum"
    assert body["start_block"] == 100
    assert body["active"] is True
    assert recorded["from_block"] == 100


def test_register_contract_accepts_lowercase_address(client):
    test_client, recorded = client
    payload = {
        "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "network": "ethereum",
        "start_block": 10861674,
        "abi_json": "[]",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 201
    body = response.json()
    expected_address = Web3.to_checksum_address(payload["address"])
    assert body["address"] == expected_address
    assert recorded["from_block"] == payload["start_block"]


def test_list_contracts_empty(client):
    """Test GET /contracts returns empty list when no contracts exist."""
    test_client, _ = client
    response = test_client.get("/contracts")
    assert response.status_code == 200
    assert response.json() == []


def test_list_contracts_with_data(client, sample_contract):
    """Test GET /contracts returns registered contracts."""
    test_client, _ = client
    response = test_client.get("/contracts")
    assert response.status_code == 200
    contracts = response.json()
    assert len(contracts) == 1
    assert contracts[0]["address"] == sample_contract.address
    assert contracts[0]["network"] == "ethereum"


def test_list_events_empty(client, sample_contract):
    """Test GET /contracts/{id}/events returns empty list when no events exist."""
    test_client, _ = client
    response = test_client.get(f"/contracts/{sample_contract.id}/events")
    assert response.status_code == 200
    assert response.json() == []


def test_list_events_with_data(client, sample_contract):
    """Test GET /contracts/{id}/events returns indexed events."""
    test_client, _ = client
    session = next(get_session())
    event = EventRecord(
        contract_id=sample_contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,
        transaction_hash="0xabc123",
        parsed_data={"from": "0x0", "to": "0x1", "value": 100},
        raw_data={"topics": [], "data": "0x"},
    )
    session.add(event)
    session.commit()
    session.close()

    response = test_client.get(f"/contracts/{sample_contract.id}/events")
    assert response.status_code == 200
    events = response.json()
    assert len(events) == 1
    assert events[0]["event_name"] == "Transfer"
    assert events[0]["block_number"] == 200
    assert events[0]["parsed_data"]["value"] == 100


def test_reindex_contract_success(client, sample_contract):
    """Test POST /contracts/{id}/reindex queues reindexing task."""
    test_client, recorded = client
    payload = {"from_block": 150, "to_block": 200}
    response = test_client.post(
        f"/contracts/{sample_contract.id}/reindex", json=payload
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "enqueued"
    assert body["contract_id"] == sample_contract.id
    assert body["from_block"] == 150
    assert body["to_block"] == 200
    assert "safe_head" in body
    assert recorded["contract_id"] == sample_contract.id
    assert recorded["from_block"] == 150
    assert recorded["to_block"] == 200


def test_reindex_contract_without_to_block(client, sample_contract):
    """Test POST /contracts/{id}/reindex with only from_block."""
    test_client, recorded = client
    payload = {"from_block": 150}
    response = test_client.post(
        f"/contracts/{sample_contract.id}/reindex", json=payload
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "enqueued"
    assert body["contract_id"] == sample_contract.id
    assert body["from_block"] == 150
    # When to_block is not specified, it defaults to safe_head
    assert body["to_block"] == body["safe_head"]
    assert "safe_head" in body
    assert recorded["from_block"] == 150
    # to_block is set to safe_head in the endpoint
    assert recorded["to_block"] is not None


def test_metrics_endpoint(client):
    """Test GET /metrics returns Prometheus metrics."""
    test_client, _ = client
    response = test_client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert b"deft_events_indexed_total" in response.content
