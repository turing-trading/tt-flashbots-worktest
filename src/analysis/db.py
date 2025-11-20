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


class AnalysisPBSV3DB(Base):
    """PBS analysis database model V3 with slot, extra transfers, and relay fees."""

    __tablename__ = "analysis_pbs_v3"

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
