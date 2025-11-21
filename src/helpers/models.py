"""Common Pydantic models for data structures used across the application."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class BlockHeader(BaseModel):
    """Block header received from newHeads WebSocket subscription."""

    number: str = Field(..., description="Block number as hex string")
    hash: str = Field(..., description="Block hash")
    parent_hash: str = Field(..., description="Parent block hash", alias="parentHash")
    miner: str = Field(..., description="Miner/validator address")
    timestamp: str = Field(
        ..., description="Block timestamp as hex string", alias="timestamp"
    )
    extra_data: str | None = Field(
        default=None, description="Extra data field", alias="extraData"
    )
    gas_limit: str | None = Field(
        default=None, description="Gas limit as hex string", alias="gasLimit"
    )
    gas_used: str | None = Field(
        default=None, description="Gas used as hex string", alias="gasUsed"
    )
    base_fee_per_gas: str | None = Field(
        default=None,
        description="Base fee per gas as hex string",
        alias="baseFeePerGas",
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class AggregatedBlockData(BaseModel):
    """Aggregated block data from multiple tables for analysis."""

    block_number: int
    block_timestamp: datetime
    builder_balance_increase: float
    proposer_subsidy: float
    total_value: float
    is_block_vanilla: bool
    n_relays: int
    relays: list[str] | None
    builder_name: str
    slot: int | None
    builder_extra_transfers: float
    relay_fee: float | None


class AdjustmentResponse(BaseModel):
    """Response from Ultrasound relay adjustment API."""

    adjusted_block_hash: str | None = None
    adjusted_value: str | None = None  # Wei as string
    block_number: int | None = None
    builder_pubkey: str | None = None
    delta: str | None = None  # Wei as string
    submitted_block_hash: str | None = None
    submitted_received_at: str | None = None
    submitted_value: str | None = None  # Wei as string


__all__ = [
    "AdjustmentResponse",
    "AggregatedBlockData",
    "BlockHeader",
]
