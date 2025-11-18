"""Pydantic models for PBS analysis data."""

from datetime import datetime

from pydantic import BaseModel, Field


class AnalysisPBS(BaseModel):
    """PBS analysis data model."""

    block_number: int = Field(..., description="Block number from blocks table")
    block_timestamp: datetime = Field(
        ..., description="Block timestamp from blocks table"
    )
    builder_balance_increase: float | None = Field(
        None,
        description="Builder balance increase in ETH from proposers_balances table",
    )
    relays: list[str] | None = Field(
        None, description="List of relay names from relays_payloads table"
    )
    proposer_subsidy: float | None = Field(
        None, description="Proposer subsidy in ETH (value) from relays_payloads table"
    )
    builder_name: str | None = Field(
        None, description="Builder name from builders_identifiers table"
    )

    class Config:
        """Pydantic config."""

        from_attributes = True
