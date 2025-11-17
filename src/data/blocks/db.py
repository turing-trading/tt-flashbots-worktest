"""Database models for blocks."""

from sqlalchemy import BigInteger, Column, DateTime, Integer, Numeric, String

from src.helpers.db import Base


class BlockCheckpoints(Base):
    """Block backfill checkpoints - tracks all successfully processed dates."""

    __tablename__ = "blocks_checkpoints"

    date = Column(String(10), primary_key=True)  # Format: YYYY-MM-DD
    block_count = Column(
        Integer, nullable=False
    )  # Number of blocks processed for this date


class BlockDB(Base):
    """Ethereum block database model."""

    __tablename__ = "blocks"

    number = Column(BigInteger, primary_key=True, index=True)
    hash = Column(String(66), unique=True, index=True)
    parent_hash = Column(String(66), index=True)
    nonce = Column(String(18))
    sha3_uncles = Column(String(66))
    transactions_root = Column(String(66))
    state_root = Column(String(66))
    receipts_root = Column(String(66))
    miner = Column(String(42), index=True)
    size = Column(Integer)
    extra_data = Column(String)
    gas_limit = Column(BigInteger)
    gas_used = Column(BigInteger)
    timestamp = Column(DateTime, index=True)
    transaction_count = Column(Integer)
    base_fee_per_gas = Column(Numeric, nullable=True)
