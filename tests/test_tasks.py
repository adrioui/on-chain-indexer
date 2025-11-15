import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from hexbytes import HexBytes

os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path("test.db").absolute()))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("RPC_URL", "http://localhost:8545")

from deft_indexer.db import Base, engine, get_session
from deft_indexer.models import Checkpoint, Contract, EventRecord
from deft_indexer.tasks import (
    _current_checkpoint,
    _get_event_abis,
    _update_checkpoint,
    enqueue_active_contracts,
    index_contract_events,
)


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_contract():
    """Create a sample active contract."""
    session = next(get_session())
    contract = Contract(
        address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        network="ethereum",
        abi='[{"type": "event", "name": "Transfer", "inputs": []}]',
        start_block=100,
        active=True,
    )
    session.add(contract)
    session.commit()
    session.refresh(contract)
    contract_id = contract.id
    session.close()
    return contract_id


@pytest.fixture
def mock_web3():
    """Mock Web3 provider for task tests."""
    mock = MagicMock()
    mock.eth.block_number = 1000
    return mock


def test_get_event_abis():
    """Test _get_event_abis extracts event definitions from ABI."""
    abi = '[{"type": "event", "name": "Transfer"}, {"type": "function", "name": "transfer"}]'
    events = _get_event_abis(abi)
    assert len(events) == 1
    assert events[0]["name"] == "Transfer"


def test_get_event_abis_empty():
    """Test _get_event_abis with ABI having no events."""
    abi = '[{"type": "function", "name": "transfer"}]'
    events = _get_event_abis(abi)
    assert events == []


def test_current_checkpoint_none():
    """Test _current_checkpoint returns None when no checkpoint exists."""
    session = next(get_session())
    result = _current_checkpoint(session, contract_id=999)
    assert result is None
    session.close()


def test_current_checkpoint_exists(sample_contract):
    """Test _current_checkpoint returns last_block when checkpoint exists."""
    session = next(get_session())
    checkpoint = Checkpoint(contract_id=sample_contract, last_block=500)
    session.add(checkpoint)
    session.commit()

    result = _current_checkpoint(session, sample_contract)
    assert result == 500
    session.close()


def test_update_checkpoint_creates_new(sample_contract):
    """Test _update_checkpoint creates checkpoint when none exists."""
    session = next(get_session())
    _update_checkpoint(session, sample_contract, 250)
    session.commit()

    checkpoint = _current_checkpoint(session, sample_contract)
    assert checkpoint == 250
    session.close()


def test_update_checkpoint_updates_existing(sample_contract):
    """Test _update_checkpoint updates existing checkpoint."""
    session = next(get_session())
    checkpoint = Checkpoint(contract_id=sample_contract, last_block=200)
    session.add(checkpoint)
    session.commit()

    _update_checkpoint(session, sample_contract, 300)
    session.commit()

    updated = _current_checkpoint(session, sample_contract)
    assert updated == 300
    session.close()


@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_contract_not_found(mock_kafka, mock_web3_class):
    """Test index_contract_events returns missing status for non-existent contract."""
    result = index_contract_events(contract_id=99999)
    assert result["status"] == "missing"


@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_inactive_contract(
    mock_kafka, mock_web3_class, sample_contract
):
    """Test index_contract_events skips inactive contracts."""
    session = next(get_session())
    contract = session.query(Contract).filter_by(id=sample_contract).first()
    contract.active = False
    session.commit()
    session.close()

    result = index_contract_events(contract_id=sample_contract)
    assert result["status"] == "missing"


@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_up_to_date(mock_kafka, mock_web3_class, sample_contract):
    """Test index_contract_events returns up_to_date when checkpoint ahead of chain."""
    mock_web3 = MagicMock()
    mock_web3.eth.block_number = 150
    mock_web3_class.return_value = mock_web3

    session = next(get_session())
    checkpoint = Checkpoint(contract_id=sample_contract, last_block=200)
    session.add(checkpoint)
    session.commit()
    session.close()

    result = index_contract_events(contract_id=sample_contract)
    assert result["status"] == "up_to_date"
    assert result["latest_block"] == 150


@patch("deft_indexer.tasks.get_event_data")
@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_processes_logs(
    mock_kafka, mock_web3_class, mock_get_event_data, sample_contract
):
    """Test index_contract_events processes logs and updates checkpoint."""
    # Setup mock Web3
    mock_web3 = MagicMock()
    mock_web3.eth.block_number = 300

    # Mock get_block to return block data with hash for canonical tracking
    mock_web3.eth.get_block.return_value = {
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "number": 200,
    }

    # Mock Web3.to_checksum_address
    mock_web3_class.to_checksum_address.return_value = (
        "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
    )
    mock_web3_class.return_value = mock_web3

    # Mock keccak for event signature hashing (Transfer() with no params)
    # This matches the sample_contract ABI which has empty inputs
    mock_web3.keccak.return_value = HexBytes(
        "0x406dade31f7ae4b5dbc276258c28dde5ae6d5c2773c5745802c493a2360e55e0"
    )

    # Mock transactionHash as object with hex() method
    mock_tx_hash = MagicMock()
    mock_tx_hash.hex.return_value = "0xabc123"

    # Mock raw log from eth.get_logs
    # Using Transfer() signature hash (matches empty inputs array)
    raw_log = {
        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics": [
            "0x406dade31f7ae4b5dbc276258c28dde5ae6d5c2773c5745802c493a2360e55e0"
        ],
        "data": "0x",
        "blockNumber": 150,
        "transactionHash": "0xabc123",
        "logIndex": 0,
    }
    mock_web3.eth.get_logs.return_value = [raw_log]

    # Mock decoded event from get_event_data
    mock_decoded = {
        "event": "Transfer",
        "blockNumber": 150,
        "logIndex": 0,
        "transactionHash": mock_tx_hash,
        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "args": {"from": "0x0", "to": "0x1", "value": 100},
    }
    mock_get_event_data.return_value = mock_decoded

    # Mock contract interface
    mock_contract = MagicMock()

    # Mock event instance for ABI map building
    mock_event_instance = MagicMock()
    mock_event_instance.return_value.topics = [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    ]

    # Mock event handler
    mock_event_handler = MagicMock()
    mock_event_handler.return_value = mock_event_instance.return_value
    mock_contract.events.__getitem__.return_value = mock_event_handler

    # Mock codec for get_event_data
    mock_web3.codec = MagicMock()

    mock_web3.eth.contract.return_value = mock_contract

    # Mock Kafka producer
    mock_producer = MagicMock()
    mock_kafka.return_value.__enter__.return_value = mock_producer

    result = index_contract_events(
        contract_id=sample_contract, from_block=100, to_block=200
    )

    assert result["status"] == "ok"
    assert result["processed"] == 1
    assert result["start"] == 100
    assert result["end"] == 200

    # Verify checkpoint updated
    session = next(get_session())
    checkpoint = _current_checkpoint(session, sample_contract)
    assert checkpoint == 200

    # Verify event persisted
    events = session.query(EventRecord).filter_by(contract_id=sample_contract).all()
    assert len(events) == 1
    assert events[0].event_name == "Transfer"
    assert events[0].block_number == 150
    # Verify raw topics are preserved
    assert "topics" in events[0].raw_data
    session.close()


@patch("deft_indexer.tasks.index_contract_events")
@patch("deft_indexer.services.enqueue_dedup.get_redis_client")
@patch("deft_indexer.tasks.Web3")
def test_enqueue_active_contracts(mock_web3_class, mock_redis, mock_index_task):
    """Test enqueue_active_contracts schedules tasks for active contracts."""
    # Mock Web3 for finality checks
    mock_web3 = MagicMock()
    mock_web3.eth.block_number = 1000
    mock_web3_class.return_value = mock_web3

    # Mock Redis to allow dedup logic to succeed
    mock_redis_client = MagicMock()
    mock_redis_client.set.return_value = True
    mock_redis.return_value = mock_redis_client

    session = next(get_session())

    # Create active and inactive contracts
    active1 = Contract(
        address="0x1111111111111111111111111111111111111111",
        network="ethereum",
        abi="[]",
        start_block=100,
        active=True,
    )
    active2 = Contract(
        address="0x2222222222222222222222222222222222222222",
        network="ethereum",
        abi="[]",
        start_block=200,
        active=True,
    )
    inactive = Contract(
        address="0x3333333333333333333333333333333333333333",
        network="ethereum",
        abi="[]",
        start_block=300,
        active=False,
    )

    session.add_all([active1, active2, inactive])
    session.commit()
    session.close()

    result = enqueue_active_contracts()

    assert result["scheduled"] == 2
    assert mock_index_task.delay.call_count == 2


@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_no_checkpoint_uses_start_block(
    mock_kafka, mock_web3_class, sample_contract
):
    """Test index_contract_events uses contract start_block when no checkpoint exists."""
    mock_web3 = MagicMock()
    mock_web3.eth.block_number = 500
    mock_web3_class.to_checksum_address.return_value = (
        "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
    )
    mock_web3_class.return_value = mock_web3

    # Mock get_block to return block data with hash for canonical tracking
    mock_web3.eth.get_block.return_value = {
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "number": 436,  # safe_head = 500 - 64
    }

    # Mock eth.get_logs to return empty list
    mock_web3.eth.get_logs.return_value = []

    mock_contract = MagicMock()
    mock_web3.eth.contract.return_value = mock_contract

    mock_producer = MagicMock()
    mock_kafka.return_value.__enter__.return_value = mock_producer

    result = index_contract_events(contract_id=sample_contract)

    assert result["status"] == "ok"
    assert result["start"] == 100  # Should use contract.start_block


@patch("deft_indexer.tasks.Web3")
@patch("deft_indexer.tasks.kafka_producer")
def test_index_contract_events_handles_429_with_custom_retry(
    mock_kafka, mock_web3_class, sample_contract
):
    """Test that 429 errors trigger custom retry with configured delay."""
    from unittest.mock import Mock

    from requests.exceptions import HTTPError

    mock_response = Mock()
    mock_response.status_code = 429
    http_error = HTTPError(response=mock_response)

    mock_web3 = MagicMock()
    mock_web3.eth.block_number = 300
    mock_web3.eth.get_logs.side_effect = http_error
    mock_web3_class.return_value = mock_web3
    mock_web3_class.to_checksum_address.return_value = (
        "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
    )

    # Mock the task's retry method
    with patch.object(index_contract_events, "retry") as mock_retry:
        mock_retry.side_effect = Exception("Retry called")

        with pytest.raises(Exception, match="Retry called"):
            index_contract_events(contract_id=sample_contract)

        # Verify retry was called with correct countdown
        mock_retry.assert_called_once()
        call_kwargs = mock_retry.call_args.kwargs
        assert call_kwargs["countdown"] == 60  # RPC_RATE_LIMIT_RETRY_DELAY


def test_fetch_logs_shrinks_window():
    """Test _fetch_logs_for_range shrinks window on -32005 errors."""
    from web3.exceptions import Web3RPCError

    from deft_indexer.tasks import _fetch_logs_for_range

    mock_web3 = MagicMock()
    mock_rate_limiter = MagicMock()

    # First call raises -32005, second succeeds with smaller window
    error = Web3RPCError(
        {"code": -32005, "message": "query returned more than 10000 results"}
    )
    mock_rate_limiter.execute.side_effect = [
        error,
        [{"blockNumber": 120, "topics": [b"\x00"], "data": "0x"}],
    ]

    logs = _fetch_logs_for_range(
        mock_web3, "0xabc", 100, 200, mock_rate_limiter, min_span=1
    )

    assert len(logs) == 1
    assert mock_rate_limiter.execute.call_count == 2


def test_decode_log_uses_get_event_data():
    """Test _decode_log_with_cache uses get_event_data for decoding."""
    from deft_indexer.tasks import _decode_log_with_cache

    mock_web3 = MagicMock()

    # Mock the codec and get_event_data
    with patch("deft_indexer.tasks.get_event_data") as mock_get_event_data:
        mock_event_data = {
            "event": "Transfer",
            "args": {"from": "0x0", "to": "0x1", "value": 100},
            "transactionHash": HexBytes("0x01"),
            "address": "0xabc",
            "blockNumber": 100,
            "logIndex": 0,
        }
        mock_get_event_data.return_value = mock_event_data

        abi_map = {
            "0xdeadbeef": {
                "type": "event",
                "name": "Transfer",
                "inputs": [],
            }
        }

        raw_log = {
            "topics": [HexBytes("0xdeadbeef")],
            "data": HexBytes("0x00"),
            "address": "0xabc",
            "blockNumber": 100,
            "logIndex": 0,
        }

        decoded = _decode_log_with_cache(raw_log, abi_map, mock_web3)

        # Verify get_event_data was called
        mock_get_event_data.assert_called_once()

        # Verify raw fields were added
        assert decoded["raw_topics"] == ["0xdeadbeef"]
        assert decoded["raw_data"] == "0x00"
