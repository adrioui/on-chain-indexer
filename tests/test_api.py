"""
Legacy test module - tests have been reorganized into focused modules.

API tests are now organized as follows:
- test_api_endpoints.py - Happy-path tests for all API endpoints
- test_api_errors.py - Error scenario tests (400, 404, 409, 422)
- test_abi_resolution.py - ABI resolution service tests
- test_tasks.py - Celery task logic tests
- test_models.py - Database model constraint tests

This file is kept for backward compatibility.
"""

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path("test.db").absolute()))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from deft_indexer.api.main import app
from deft_indexer.db import Base, engine


@pytest.fixture(autouse=True)
def reset_database():
    """Reset database schema between tests."""
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client(monkeypatch):
    """Provide TestClient with mocked external dependencies."""

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
        return True  # Always return True (enqueued successfully)

    monkeypatch.setattr("deft_indexer.api.main.get_web3", lambda: FakeWeb3())
    monkeypatch.setattr("deft_indexer.api.main.resolve_abi", fake_resolve)
    monkeypatch.setattr("deft_indexer.api.main.index_contract_events.delay", fake_delay)
    monkeypatch.setattr(
        "deft_indexer.services.enqueue_dedup.enqueue_with_dedup",
        fake_enqueue_with_dedup,
    )

    with TestClient(app) as test_client:
        yield test_client, recorded


def test_register_contract(client):
    """Original test kept for backward compatibility - see test_api_endpoints.py for comprehensive tests."""
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
    assert recorded["from_block"] == payload["start_block"]
