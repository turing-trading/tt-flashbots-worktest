"""Database models for miner balance data."""

from sqlalchemy import BigInteger, Column, Index, Numeric, String

from src.helpers.db import Base


class ProposerBalancesDB(Base):
    """Miner balance increase per block."""

    __tablename__ = "proposers_balance"

    block_number = Column(BigInteger, primary_key=True, index=True)
    miner = Column(String(42), nullable=False, index=True)
    balance_before = Column(Numeric, nullable=False)  # Wei at block N-1
    balance_after = Column(Numeric, nullable=False)  # Wei at block N
    balance_increase = Column(Numeric, nullable=False)  # Wei increase (can be negative)

    __table_args__ = (Index("idx_proposer_block", "miner", "block_number"),)
