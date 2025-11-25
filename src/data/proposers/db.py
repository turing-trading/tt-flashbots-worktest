"""Database models for proposer data."""

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


class ProposerMappingDB(Base):
    """Proposer fee recipient to label mapping database model."""

    __tablename__ = "proposer_mapping"

    proposer_fee_recipient: Mapped[str] = mapped_column(
        String(42), primary_key=True, index=True
    )
    label: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    lido_node_operator: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )
