"""Pydantic models for PBS analysis data."""

# Pydantic needs this at runtime to validate the datetime field
from datetime import datetime

from pydantic import BaseModel, Field


class AnalysisPBSV3(BaseModel):
    """PBS analysis data model V3.

    Includes additional fields for slot, extra transfers, and relay fees.
    """

    block_number: int = Field(..., description="Block number from blocks table")
    block_timestamp: datetime = Field(
        ..., description="Block timestamp from blocks table"
    )
    builder_balance_increase: float = Field(
        default=0.0,
        description="Builder balance increase in ETH from builder_balances table",
    )
    proposer_subsidy: float = Field(
        default=0.0,
        description="Proposer subsidy in ETH (value) from relays_payloads table",
    )
    total_value: float = Field(
        default=0.0,
        description=(
            "Total MEV value (builder_balance_increase + proposer_subsidy "
            "+ relay_fee + builder_extra_transfers)"
        ),
    )
    is_block_vanilla: bool = Field(
        default=False,
        description="True if block is vanilla (no relays), False if MEV-Boost",
    )
    n_relays: int = Field(default=0, description="Number of relays used for this block")
    relays: list[str] | None = Field(
        None, description="List of relay names from relays_payloads table"
    )
    builder_name: str = Field(
        default="unknown",
        description="Builder name from builders_identifiers table",
    )
    slot: int | None = Field(
        None,
        description=(
            "Beacon chain slot number from relays_payloads table "
            "(null for vanilla blocks)"
        ),
    )
    builder_extra_transfers: float = Field(
        default=0.0,
        description=(
            "Sum of positive balance increases for known builder addresses "
            "in ETH from extra_builder_balance table"
        ),
    )
    relay_fee: float | None = Field(
        None,
        description=(
            "Relay fee in ETH from ultrasound_adjustments table "
            "(only for Ultrasound relay)"
        ),
    )

    class Config:
        """Pydantic config."""

        from_attributes = True
