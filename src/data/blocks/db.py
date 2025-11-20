"""Database models for blocks."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


class BlockCheckpoints(Base):
    """Block backfill checkpoints - tracks all successfully processed dates."""

    __tablename__ = "blocks_checkpoints"

    date: Mapped[str] = mapped_column(
        String(10), primary_key=True
    )  # Format: YYYY-MM-DD
    block_count: Mapped[int] = mapped_column(
        Integer, nullable=False
    )  # Number of blocks processed for this date


class BlockDB(Base):
    """Ethereum block database model."""

    __tablename__ = "blocks"

    number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    hash: Mapped[str] = mapped_column(String(66), unique=True, index=True)
    parent_hash: Mapped[str] = mapped_column(String(66), index=True)
    nonce: Mapped[str] = mapped_column(String(18))
    sha3_uncles: Mapped[str] = mapped_column(String(66))
    transactions_root: Mapped[str] = mapped_column(String(66))
    state_root: Mapped[str] = mapped_column(String(66))
    receipts_root: Mapped[str] = mapped_column(String(66))
    miner: Mapped[str] = mapped_column(String(42), index=True)
    size: Mapped[int] = mapped_column(Integer)
    extra_data: Mapped[str] = mapped_column(String)
    gas_limit: Mapped[int] = mapped_column(BigInteger)
    gas_used: Mapped[int] = mapped_column(BigInteger)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    transaction_count: Mapped[int] = mapped_column(Integer)
    base_fee_per_gas: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
