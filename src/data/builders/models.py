"""Pydantic models for builder identifiers."""

from pydantic import BaseModel, Field


class BuilderIdentifier(BaseModel):
    """Builder identifier data model."""

    builder_pubkey: str = Field(..., description="Builder public key from relays_payloads")
    builder_name: str = Field(..., description="Parsed builder name from extra_data")

    class Config:
        """Pydantic config."""

        from_attributes = True
