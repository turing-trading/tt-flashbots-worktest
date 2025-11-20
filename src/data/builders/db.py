"""Database models for miner balance data."""

from sqlalchemy import BigInteger, Column, Index, Numeric, String

from src.helpers.db import Base


class BuilderBalancesDB(Base):
    """Miner balance increase per block."""

    __tablename__ = "builders_balance"

    block_number = Column(BigInteger, primary_key=True, index=True)
    miner = Column(String(42), nullable=False, index=True)
    balance_before = Column(Numeric, nullable=False)  # Wei at block N-1
    balance_after = Column(Numeric, nullable=False)  # Wei at block N
    balance_increase = Column(Numeric, nullable=False)  # Wei increase (can be negative)

    __table_args__ = (Index("idx_builder_block", "miner", "block_number"),)


class ExtraBuilderBalanceDB(Base):
    """Balance increase for known builder addresses per block."""

    __tablename__ = "extra_builder_balance"

    block_number = Column(BigInteger, primary_key=True, index=True)
    builder_address = Column(String(42), primary_key=True, index=True)
    miner = Column(String(42), nullable=False, index=True)
    balance_before = Column(Numeric, nullable=False)  # Wei at block N-1
    balance_after = Column(Numeric, nullable=False)  # Wei at block N
    balance_increase = Column(Numeric, nullable=False)  # Wei increase (can be negative)

    __table_args__ = (
        Index("idx_builder_block", "builder_address", "block_number"),
        Index("idx_builder_miner", "miner", "block_number"),
    )
