import os
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path("test.db").absolute()))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from deft_indexer.db import Base, engine, get_session
from deft_indexer.models import Checkpoint, Contract, EventRecord


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def test_contract_unique_address():
    """Test Contract.address has unique constraint."""
    session = next(get_session())

    contract1 = Contract(
        address="0x1111111111111111111111111111111111111111",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract1)
    session.commit()

    contract2 = Contract(
        address="0x1111111111111111111111111111111111111111",
        network="ethereum",
        abi="[]",
        start_block=200,
    )
    session.add(contract2)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()
    session.close()


def test_contract_defaults():
    """Test Contract model default values."""
    session = next(get_session())

    contract = Contract(
        address="0x2222222222222222222222222222222222222222", abi="[]", start_block=100
    )
    session.add(contract)
    session.commit()
    session.refresh(contract)

    assert contract.network == "ethereum"
    assert contract.active is True
    assert contract.created_at is not None

    session.close()


def test_contract_cascade_delete_checkpoints():
    """Test deleting Contract cascades to Checkpoints."""
    session = next(get_session())

    contract = Contract(
        address="0x3333333333333333333333333333333333333333",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    checkpoint = Checkpoint(contract_id=contract.id, last_block=500)
    session.add(checkpoint)
    session.commit()

    # Verify checkpoint exists
    assert session.query(Checkpoint).filter_by(contract_id=contract.id).count() == 1

    # Delete contract
    session.delete(contract)
    session.commit()

    # Verify checkpoint was deleted
    assert session.query(Checkpoint).filter_by(contract_id=contract.id).count() == 0

    session.close()


def test_contract_cascade_delete_events():
    """Test deleting Contract cascades to EventRecords."""
    session = next(get_session())

    contract = Contract(
        address="0x4444444444444444444444444444444444444444",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    event = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,
        transaction_hash="0xabc",
        parsed_data={},
        raw_data={},
    )
    session.add(event)
    session.commit()

    # Verify event exists
    assert session.query(EventRecord).filter_by(contract_id=contract.id).count() == 1

    # Delete contract
    session.delete(contract)
    session.commit()

    # Verify event was deleted
    assert session.query(EventRecord).filter_by(contract_id=contract.id).count() == 0

    session.close()


def test_checkpoint_unique_contract_block():
    """Test Checkpoint has unique constraint on contract_id (one checkpoint per contract)."""
    session = next(get_session())

    contract = Contract(
        address="0x5555555555555555555555555555555555555555",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    checkpoint1 = Checkpoint(contract_id=contract.id, last_block=500)
    session.add(checkpoint1)
    session.commit()

    # Trying to add a second checkpoint for the same contract should fail
    checkpoint2 = Checkpoint(contract_id=contract.id, last_block=600)
    session.add(checkpoint2)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()
    session.close()


def test_event_record_unique_contract_block_tx():
    """Test EventRecord has unique index on (chain_id, contract_id, block_number, transaction_hash, log_index)."""
    session = next(get_session())

    contract = Contract(
        address="0x6666666666666666666666666666666666666666",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    # Different log_index in same tx should succeed
    event1 = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,
        transaction_hash="0xabc123",
        parsed_data={},
        raw_data={},
    )
    session.add(event1)
    session.commit()

    event2 = EventRecord(
        contract_id=contract.id,
        event_name="Approval",
        block_number=200,
        log_index=1,
        transaction_hash="0xabc123",
        parsed_data={},
        raw_data={},
    )
    session.add(event2)
    session.commit()  # Should succeed - different log_index

    # Same log_index should fail
    event3 = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,  # Duplicate log_index
        transaction_hash="0xabc123",
        parsed_data={},
        raw_data={},
    )
    session.add(event3)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()
    session.close()


def test_event_record_same_tx_different_log_index():
    """Test EventRecords with same tx hash but different blocks are allowed."""
    session = next(get_session())

    contract = Contract(
        address="0x7777777777777777777777777777777777777777",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    event1 = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,
        transaction_hash="0xdef456",
        parsed_data={},
        raw_data={},
    )
    event2 = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=201,
        log_index=0,
        transaction_hash="0xdef456",
        parsed_data={},
        raw_data={},
    )
    session.add_all([event1, event2])
    session.commit()

    # Verify both events exist
    events = session.query(EventRecord).filter_by(contract_id=contract.id).all()
    assert len(events) == 2

    session.close()


def test_contract_relationship_to_checkpoints():
    """Test Contract.checkpoints relationship loads correctly (one checkpoint per contract)."""
    session = next(get_session())

    contract = Contract(
        address="0x8888888888888888888888888888888888888888",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    # Only one checkpoint per contract
    checkpoint = Checkpoint(contract_id=contract.id, last_block=300)
    session.add(checkpoint)
    session.commit()

    # Refresh to load relationship
    session.refresh(contract)
    assert len(contract.checkpoints) == 1
    assert contract.checkpoints[0].last_block == 300

    session.close()


def test_contract_relationship_to_events():
    """Test Contract.events relationship loads correctly."""
    session = next(get_session())

    contract = Contract(
        address="0x9999999999999999999999999999999999999999",
        network="ethereum",
        abi="[]",
        start_block=100,
    )
    session.add(contract)
    session.commit()

    event1 = EventRecord(
        contract_id=contract.id,
        event_name="Transfer",
        block_number=200,
        log_index=0,
        transaction_hash="0x1",
        parsed_data={},
        raw_data={},
    )
    event2 = EventRecord(
        contract_id=contract.id,
        event_name="Approval",
        block_number=201,
        log_index=0,
        transaction_hash="0x2",
        parsed_data={},
        raw_data={},
    )
    session.add_all([event1, event2])
    session.commit()

    # Refresh to load relationship
    session.refresh(contract)
    assert len(contract.events) == 2

    session.close()
