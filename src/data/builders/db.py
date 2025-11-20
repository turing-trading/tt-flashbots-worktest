"""Database models for miner balance data."""

from sqlalchemy import BigInteger, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base
from src.helpers.db_mixins import BalanceFieldsMixin


class BuilderBalancesDB(Base, BalanceFieldsMixin):
    """Miner balance increase per block."""

    __tablename__ = "builder_balance"

    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    miner: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    # balance_before, balance_after, balance_increase inherited from BalanceFieldsMixin

    __table_args__ = (Index("idx_builder_block", "miner", "block_number"),)


class ExtraBuilderBalanceDB(Base, BalanceFieldsMixin):
    """Balance increase for known builder addresses per block."""

    __tablename__ = "extra_builder_balance"

    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    builder_address: Mapped[str] = mapped_column(
        String(42), primary_key=True, index=True
    )
    miner: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    # balance_before, balance_after, balance_increase inherited from BalanceFieldsMixin

    __table_args__ = (
        Index("idx_extra_builder_block", "builder_address", "block_number"),
        Index("idx_extra_builder_miner", "miner", "block_number"),
    )
