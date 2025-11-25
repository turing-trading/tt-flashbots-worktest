"""Database models for PBS analysis."""

from datetime import datetime

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


class AnalysisPBSDB(Base):
    """PBS analysis database model with proposer_name and precomputed columns."""

    __tablename__ = "analysis_pbs"

    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True)
    block_timestamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, index=True
    )
    builder_balance_increase: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    proposer_subsidy: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    total_value: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0, index=True
    )
    # builder_balance_increase + proposer_subsidy + relay_fee
    # + builder_extra_transfers
    is_block_vanilla: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, index=True
    )  # True if no relays
    n_relays: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, index=True
    )  # Count of relays
    relays: Mapped[list[str] | None] = mapped_column(ARRAY(String(255)), nullable=True)
    builder_name: Mapped[str] = mapped_column(
        String, nullable=False, default="unknown", index=True
    )
    slot: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, index=True
    )  # Beacon chain slot number (null for vanilla blocks)
    builder_extra_transfers: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )  # Sum of positive balance increases for known builder addresses (ETH)
    relay_fee: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )  # Relay fee from ultrasound_adjustments (ETH, only for Ultrasound relay)
    # New fields
    proposer_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )  # Entity name from proposer_mapping
    builder_profit: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )  # total_value - proposer_subsidy - relay_fee (ETH)
    pct_proposer_share: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )  # proposer_subsidy / total_value * 100 (NULL if total_value <= 0)
    pct_builder_share: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )  # builder_profit / total_value * 100 (NULL if total_value <= 0)
    pct_relay_fee: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )  # relay_fee / total_value * 100 (NULL if total_value <= 0)


# Backward compatibility alias for V3
AnalysisPBSV3DB = AnalysisPBSDB
