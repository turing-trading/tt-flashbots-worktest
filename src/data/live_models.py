"""Pydantic models for live data processing intermediate results."""

from pydantic import BaseModel, Field


class BuilderBalanceData(BaseModel):
    """Builder balance increase data from live processing."""

    balance_increase: int = Field(..., description="Balance increase in wei")


class RelayData(BaseModel):
    """Relay payload data from live processing."""

    relay: str = Field(..., description="Relay name")
    value: int = Field(..., description="Payload value in wei")
    slot: int | None = Field(default=None, description="Beacon chain slot number")
    proposer_fee_recipient: str | None = Field(
        default=None, description="Proposer fee recipient address"
    )


class ExtraBuilderBalanceData(BaseModel):
    """Extra builder balance data from live processing."""

    builder_address: str = Field(..., description="Builder address")
    balance_increase: int = Field(..., description="Balance increase in wei")


class AdjustmentData(BaseModel):
    """Ultrasound adjustment data from live processing."""

    delta: int = Field(..., description="Relay fee (delta) in wei")


__all__ = [
    "AdjustmentData",
    "BuilderBalanceData",
    "ExtraBuilderBalanceData",
    "RelayData",
]
