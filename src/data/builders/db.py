"""Database models for miner balance data."""

from decimal import Decimal

from sqlalchemy import BigInteger, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


class BuilderBalancesDB(Base):
    """Miner balance increase per block."""

    __tablename__ = "builder_balance"

    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    miner: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    balance_before: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei at block N-1
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei at block N
    balance_increase: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei increase (can be negative)

    __table_args__ = (Index("idx_builder_block", "miner", "block_number"),)


class ExtraBuilderBalanceDB(Base):
    """Balance increase for known builder addresses per block."""

    __tablename__ = "extra_builder_balance"

    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    builder_address: Mapped[str] = mapped_column(
        String(42), primary_key=True, index=True
    )
    miner: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    balance_before: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei at block N-1
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei at block N
    balance_increase: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False
    )  # Wei increase (can be negative)

    __table_args__ = (
        Index("idx_builder_block", "builder_address", "block_number"),
        Index("idx_builder_miner", "miner", "block_number"),
    )
