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


class AnalysisPBSV2(BaseModel):
    """PBS analysis data model V2 with computed fields and non-nullable values."""

    block_number: int = Field(..., description="Block number from blocks table")
    block_timestamp: datetime = Field(
        ..., description="Block timestamp from blocks table"
    )
    builder_balance_increase: float = Field(
        default=0.0,
        description="Builder balance increase in ETH from proposers_balances table",
    )
    proposer_subsidy: float = Field(
        default=0.0,
        description="Proposer subsidy in ETH (value) from relays_payloads table",
    )
    total_value: float = Field(
        default=0.0,
        description="Total MEV value (builder_balance_increase + proposer_subsidy)",
    )
    is_block_vanilla: bool = Field(
        default=False,
        description="True if block is vanilla (no relays), False if MEV-Boost",
    )
    n_relays: int = Field(
        default=0, description="Number of relays used for this block"
    )
    relays: list[str] | None = Field(
        None, description="List of relay names from relays_payloads table"
    )
    builder_name: str = Field(
        default="unknown",
        description="Builder name from builders_identifiers table",
    )

    class Config:
        """Pydantic config."""

        from_attributes = True
