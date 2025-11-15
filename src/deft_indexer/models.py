from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class CanonicalBlock(Base):
    """Tracks canonical block hashes for reorg detection."""

    __tablename__ = "canonical_blocks"
    __table_args__ = (PrimaryKeyConstraint("chain_id", "number"),)

    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    hash: Mapped[str] = mapped_column(String(66), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )


class Contract(Base):
    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(42), unique=True, nullable=False)
    network: Mapped[str] = mapped_column(String(32), nullable=False, default="ethereum")
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    abi: Mapped[str] = mapped_column(Text, nullable=False)
    start_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    checkpoints: Mapped[list["Checkpoint"]] = relationship(
        "Checkpoint", back_populates="contract", cascade="all, delete-orphan"
    )
    events: Mapped[list["EventRecord"]] = relationship(
        "EventRecord", back_populates="contract", cascade="all, delete-orphan"
    )


class Checkpoint(Base):
    __tablename__ = "checkpoints"
    __table_args__ = (
        Index("ix_checkpoint_contract_block", "contract_id", "last_block"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    last_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_finalized: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, default=None
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    contract: Mapped[Contract] = relationship("Contract", back_populates="checkpoints")


class EventRecord(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index(
            "ix_event_identity",
            "chain_id",
            "contract_id",
            "block_number",
            "transaction_hash",
            "log_index",
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False
    )
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    event_name: Mapped[str] = mapped_column(String(128), nullable=False)
    block_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    log_index: Mapped[int] = mapped_column(Integer, nullable=False)
    transaction_hash: Mapped[str] = mapped_column(String(66), nullable=False)
    parsed_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    contract: Mapped[Contract] = relationship("Contract", back_populates="events")
