"""Pydantic models for builder identifiers."""

from pydantic import BaseModel, Field


class BuilderIdentifier(BaseModel):
    """Builder identifier data model."""

    extra_data: str = Field(..., description="Unique extra_data from blocks")
    builder_name: str = Field(..., description="Parsed builder name from extra_data")

    class Config:
        """Pydantic config."""

        from_attributes = True
