"""Pydantic models for proposer data."""

from pydantic import BaseModel


class ProposerMapping(BaseModel):
    """Proposer fee recipient to label mapping."""

    proposer_fee_recipient: str
    label: str
    lido_node_operator: str | None = None
