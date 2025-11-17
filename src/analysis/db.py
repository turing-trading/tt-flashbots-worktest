"""Database models for PBS analysis."""

from sqlalchemy import ARRAY, BigInteger, Column, DateTime, Float, String

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
