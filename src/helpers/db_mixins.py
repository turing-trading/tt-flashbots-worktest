"""Database model mixins for common field patterns."""

from decimal import Decimal

from sqlalchemy import Numeric
from sqlalchemy.orm import Mapped, mapped_column


class BalanceFieldsMixin:
    """Mixin for balance tracking fields.

    Provides three common fields used across builder balance models:
    - balance_before: Balance at block N-1 (in Wei)
    - balance_after: Balance at block N (in Wei)
    - balance_increase: Change in balance (can be negative)

    Example:
        ```python
        from src.helpers.db import Base
        from src.helpers.db_mixins import BalanceFieldsMixin

        class MyBalanceModel(Base, BalanceFieldsMixin):
            __tablename__ = "my_balance_table"
            block_number: Mapped[int] = mapped_column(BigInteger, primary_key=True)
            # balance_before, balance_after, balance_increase inherited from mixin
        ```
    """

    balance_before: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False, doc="Wei at block N-1"
    )
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False, doc="Wei at block N"
    )
    balance_increase: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False, doc="Wei increase (can be negative)"
    )


__all__ = ["BalanceFieldsMixin"]
