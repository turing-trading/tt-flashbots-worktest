"""Database models for PBS analysis."""

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
)

from src.helpers.db import Base


class AnalysisPBSDB(Base):
    """PBS analysis database model."""

    __tablename__ = "analysis_pbs"

    block_number = Column(BigInteger, primary_key=True, index=True)
    block_timestamp = Column(DateTime, nullable=False, index=True)
    builder_balance_increase = Column(Float, nullable=True)  # ETH (converted from Wei)
    relays = Column(ARRAY(String(255)), nullable=True)
    proposer_subsidy = Column(Float, nullable=True)  # ETH (converted from Wei)
    builder_name = Column(String, nullable=True, index=True)


class AnalysisPBSV2DB(Base):
    """PBS analysis database model V2 with computed fields and non-nullable values."""

    __tablename__ = "analysis_pbs_v2"

    block_number = Column(BigInteger, primary_key=True, index=True)
    block_timestamp = Column(DateTime, nullable=False, index=True)
    builder_balance_increase = Column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    proposer_subsidy = Column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    total_value = Column(
        Float, nullable=False, default=0.0, index=True
    )  # builder_balance_increase + proposer_subsidy + relay_fee + builder_extra_transfers
    is_block_vanilla = Column(
        Boolean, nullable=False, default=False, index=True
    )  # True if no relays
    n_relays = Column(Integer, nullable=False, default=0, index=True)  # Count of relays
    relays = Column(ARRAY(String(255)), nullable=True)
    builder_name = Column(
        String, nullable=False, default="unknown", index=True
    )


class AnalysisPBSV3DB(Base):
    """PBS analysis database model V3 with slot, extra transfers, and relay fees."""

    __tablename__ = "analysis_pbs_v3"

    block_number = Column(BigInteger, primary_key=True, index=True)
    block_timestamp = Column(DateTime, nullable=False, index=True)
    builder_balance_increase = Column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    proposer_subsidy = Column(
        Float, nullable=False, default=0.0
    )  # ETH (converted from Wei)
    total_value = Column(
        Float, nullable=False, default=0.0, index=True
    )  # builder_balance_increase + proposer_subsidy + relay_fee + builder_extra_transfers
    is_block_vanilla = Column(
        Boolean, nullable=False, default=False, index=True
    )  # True if no relays
    n_relays = Column(Integer, nullable=False, default=0, index=True)  # Count of relays
    relays = Column(ARRAY(String(255)), nullable=True)
    builder_name = Column(
        String, nullable=False, default="unknown", index=True
    )
    slot = Column(
        BigInteger, nullable=True, index=True
    )  # Beacon chain slot number (null for vanilla blocks)
    builder_extra_transfers = Column(
        Float, nullable=False, default=0.0
    )  # Sum of positive balance increases for known builder addresses (ETH)
    relay_fee = Column(
        Float, nullable=True
    )  # Relay fee from ultrasound_adjustments (ETH, only for Ultrasound relay)
