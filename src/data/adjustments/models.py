"""Pydantic models for Ultrasound relay adjustments."""

from datetime import datetime

from pydantic import BaseModel


class UltrasoundAdjustment(BaseModel):
    """Ultrasound relay adjustment - Pydantic model."""

    # Primary key
    slot: int

    # From API response (all optional for cases with no adjustment)
    adjusted_block_hash: str | None = None
    adjusted_value: int | None = None  # Wei value as integer
    block_number: int | None = None
    builder_pubkey: str | None = None
    delta: int | None = None  # Adjustment delta in Wei as integer
    submitted_block_hash: str | None = None
    submitted_received_at: str | None = None  # ISO timestamp string
    submitted_value: int | None = None  # Original Wei value as integer

    # Metadata
    fetched_at: datetime
    has_adjustment: bool = True
