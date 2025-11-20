"""Models for the project."""

from pydantic import BaseModel


class RelaysPayloadsPayload(BaseModel):
    """Signed validator registration payload."""

    cursor: int
    limit: int


class RelaysPayloads(BaseModel):
    """Signed validator registration."""

    slot: int
    parent_hash: str
    block_hash: str
    builder_pubkey: str
    proposer_pubkey: str
    proposer_fee_recipient: str
    gas_limit: int
    gas_used: int
    value: int
    block_number: int
    num_tx: int


class RelaysPayloadsCheckpoints(BaseModel):
    """Relay payloads checkpoints."""

    relay: str
    from_slot: int
    to_slot: int
