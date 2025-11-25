"""Pydantic models for PBS analysis data."""

# Pydantic needs this at runtime to validate the datetime field
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AnalysisPBS(BaseModel):
    """PBS analysis data model.

    Includes proposer name from proposer_mapping and precomputed percentage columns.
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
        default=None, description="List of relay names from relays_payloads table"
    )
    builder_name: str = Field(
        default="unknown",
        description="Builder name from builders_identifiers table",
    )
    slot: int | None = Field(
        default=None,
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
        default=None,
        description=(
            "Relay fee in ETH from ultrasound_adjustments table "
            "(only for Ultrasound relay)"
        ),
    )
    # New fields
    proposer_name: str | None = Field(
        default=None,
        description="Proposer entity name from proposer_mapping table",
    )
    builder_profit: float = Field(
        default=0.0,
        description="Builder profit in ETH (total - proposer - relay_fee)",
    )
    pct_proposer_share: float | None = Field(
        default=None,
        description="Proposer share as percentage of total_value",
    )
    pct_builder_share: float | None = Field(
        default=None,
        description="Builder share as percentage of total_value",
    )
    pct_relay_fee: float | None = Field(
        default=None,
        description="Relay fee as percentage of total_value",
    )

    model_config = ConfigDict(from_attributes=True)


# Backward compatibility alias for V3
AnalysisPBSV3 = AnalysisPBS
