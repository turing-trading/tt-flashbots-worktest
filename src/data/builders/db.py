"""Database models for builder identifiers."""

from sqlalchemy import BigInteger, Column, Integer, String

from src.helpers.db import Base


class BuilderIdentifiersCheckpoints(Base):
    """Checkpoints for builder identifiers backfill."""

    __tablename__ = "builders_identifiers_checkpoints"

    id = Column(Integer, primary_key=True, default=1)
    from_block = Column(BigInteger, nullable=False, default=0)
    to_block = Column(BigInteger, nullable=False, default=0)


class BuilderIdentifiersDB(Base):
    """Builder identifiers extracted from block extra_data."""

    __tablename__ = "builders_identifiers"

    extra_data = Column(String, primary_key=True, index=True)
    builder_name = Column(String, nullable=False)
