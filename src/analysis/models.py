"""Pydantic models for PBS analysis data."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class AnalysisPBS(BaseModel):
    """PBS analysis data model."""

    block_number: int = Field(..., description="Block number from blocks table")
    block_timestamp: datetime = Field(
        ..., description="Block timestamp from blocks table"
    )
    builder_balance_increase: Decimal | None = Field(
        None, description="Builder balance increase from proposers_balances table"
    )
    relays: list[str] | None = Field(
        None, description="List of relay names from relays_payloads table"
    )
    proposer_subsidy: Decimal | None = Field(
        None, description="Proposer subsidy (value) from relays_payloads table"
    )

    class Config:
        """Pydantic config."""

        from_attributes = True
