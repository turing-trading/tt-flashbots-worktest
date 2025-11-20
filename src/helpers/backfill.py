"""Base classes and utilities for backfill operations."""

from abc import ABC, abstractmethod

from rich.console import Console

from src.helpers.db import create_tables


class BackfillBase(ABC):
    """Abstract base class for backfill operations.

    Provides common functionality for all backfill classes including:
    - Console initialization for progress display
    - Table creation
    - Batch size configuration

    Subclasses must implement:
    - run(): Main backfill orchestration logic
    """

    def __init__(self, batch_size: int) -> None:
        """Initialize backfill with common configuration.

        Args:
            batch_size: Number of items to process per batch
        """
        self.batch_size = batch_size
        self.console = Console()

    async def create_tables(self) -> None:
        """Create database tables if they don't exist.

        Calls the centralized create_tables helper to ensure all
        required database tables are created before backfill starts.
        """
        await create_tables()

    @abstractmethod
    async def run(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Run the backfill process.

        This method must be implemented by subclasses to define
        their specific backfill logic and orchestration.
        """
        ...


__all__ = ["BackfillBase"]
