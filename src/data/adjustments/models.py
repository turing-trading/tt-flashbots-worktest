"""Pydantic models for Ultrasound relay adjustments."""

from pydantic import BaseModel


class UltrasoundAdjustment(BaseModel):
    """Ultrasound relay adjustment - Pydantic model."""

    # From our query
    slot: int

    # From API response
    adjusted_block_hash: str
    adjusted_value: int  # Wei value as integer
    block_number: int
    builder_pubkey: str
    delta: int  # Adjustment delta in Wei as integer
    submitted_block_hash: str
    submitted_received_at: str  # ISO timestamp string
    submitted_value: int  # Original Wei value as integer
