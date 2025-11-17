"""Database models for PBS analysis."""

from sqlalchemy import ARRAY, BigInteger, Column, DateTime, Numeric, String

from src.helpers.db import Base


class AnalysisPBSDB(Base):
    """PBS analysis database model."""

    __tablename__ = "analysis_pbs"

    block_number = Column(BigInteger, primary_key=True, index=True)
    block_timestamp = Column(DateTime, nullable=False, index=True)
    builder_balance_increase = Column(Numeric, nullable=True)
    relays = Column(ARRAY(String(255)), nullable=True)
    proposer_subsidy = Column(Numeric, nullable=True)
