import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path("test.db").absolute()))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from deft_indexer.api.main import app
from deft_indexer.db import Base, engine, get_session
from deft_indexer.models import Contract
from deft_indexer.services.abi import ABIResolutionError


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client(monkeypatch):
    class FakeWeb3:
        def __init__(self, address_valid=True, code_exists=True):
            self.address_valid = address_valid
            self.code_exists = code_exists

        def is_address(self, address):
            return self.address_valid

        def is_checksum_address(self, address):
            return self.address_valid

        class Eth:
            block_number = 1000000  # Mock current block number

            def __init__(self, code_exists):
                self.code_exists = code_exists

            def get_code(self, _):
                return b"deadbeef" if self.code_exists else b""

        @property
        def eth(self):
            return FakeWeb3.Eth(self.code_exists)

    fake_web3 = FakeWeb3()

    async def fake_resolve(*_args, **_kwargs):
        return "[]"

    def fake_delay(*_args, **_kwargs):
        return None

    def fake_enqueue_with_dedup(*_args, **_kwargs):
        return True

    # Mock Redis for rate limiting - always allow
    class FakeRedis:
        def incr(self, key):
            return 1

        def expire(self, key, seconds):
            return True

    def fake_get_redis_client():
        return FakeRedis()

    monkeypatch.setattr("deft_indexer.api.main.get_web3", lambda: fake_web3)
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
        yield test_client, fake_web3


def test_register_contract_invalid_address(client):
    """Test POST /watch with invalid address returns 400."""
    test_client, fake_web3 = client
    fake_web3.address_valid = False

    payload = {
        "address": "0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",  # 42 chars but invalid hex
        "network": "ethereum",
        "start_block": 100,
        "abi_json": "[]",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 400
    assert "Invalid contract address" in response.json()["detail"]


def test_register_contract_no_code_on_chain(client):
    """Test POST /watch with address having no code returns 404."""
    test_client, fake_web3 = client
    fake_web3.code_exists = False

    payload = {
        "address": "0x0000000000000000000000000000000000000000",
        "network": "ethereum",
        "start_block": 100,
        "abi_json": "[]",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 404
    assert "Contract code not found on chain" in response.json()["detail"]


def test_register_contract_duplicate(client):
    """Test POST /watch with already registered address returns 409."""
    test_client, _ = client

    payload = {
        "address": "0x1111111111111111111111111111111111111111",
        "network": "ethereum",
        "start_block": 100,
        "abi_json": "[]",
    }

    # First registration succeeds
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 201

    # Second registration fails with 409
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 409
    assert "Contract already registered" in response.json()["detail"]


def test_register_contract_abi_resolution_failure(client, monkeypatch):
    """Test POST /watch with ABI resolution error returns 400."""
    test_client, _ = client

    async def failing_resolve(*_args, **_kwargs):
        raise ABIResolutionError("Invalid ABI format")

    monkeypatch.setattr("deft_indexer.api.main.resolve_abi", failing_resolve)

    payload = {
        "address": "0x2222222222222222222222222222222222222222",
        "network": "ethereum",
        "start_block": 100,
        "abi_json": "not_json",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 400
    assert "Invalid ABI format" in response.json()["detail"]


def test_register_contract_missing_abi_sources(client, monkeypatch):
    """Test POST /watch without abi_json or etherscan_url returns 400."""
    test_client, _ = client

    async def failing_resolve(*_args, **_kwargs):
        raise ABIResolutionError("Either abi_json or etherscan_url is required")

    monkeypatch.setattr("deft_indexer.api.main.resolve_abi", failing_resolve)

    payload = {
        "address": "0x3333333333333333333333333333333333333333",
        "network": "ethereum",
        "start_block": 100,
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 400
    assert "required" in response.json()["detail"].lower()


def test_register_contract_invalid_start_block(client):
    """Test POST /watch with negative start_block returns 422."""
    test_client, _ = client

    payload = {
        "address": "0x4444444444444444444444444444444444444444",
        "network": "ethereum",
        "start_block": -1,
        "abi_json": "[]",
    }
    response = test_client.post("/watch", json=payload)
    assert response.status_code == 422


def test_list_events_contract_not_found(client):
    """Test GET /contracts/{id}/events with non-existent contract returns empty list."""
    test_client, _ = client
    response = test_client.get("/contracts/99999/events")
    assert response.status_code == 200
    assert response.json() == []


def test_reindex_contract_not_found(client):
    """Test POST /contracts/{id}/reindex with non-existent contract returns 404."""
    test_client, _ = client
    payload = {"from_block": 100}
    response = test_client.post("/contracts/99999/reindex", json=payload)
    assert response.status_code == 404
    assert "Contract not found" in response.json()["detail"]


def test_reindex_invalid_block_range(client):
    """Test POST /contracts/{id}/reindex with invalid from_block returns 422."""
    test_client, _ = client
    session = next(get_session())
    contract = Contract(
        address="0x5555555555555555555555555555555555555555",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()
    contract_id = contract.id
    session.close()

    payload = {"from_block": -10}
    response = test_client.post(f"/contracts/{contract_id}/reindex", json=payload)
    assert response.status_code == 422
