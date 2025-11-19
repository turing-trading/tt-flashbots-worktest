"""Pytest configuration and shared fixtures for data integrity tests."""

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.helpers.db import AsyncSessionLocal


# Apply 10-second timeout to all integration tests
def pytest_collection_modifyitems(config, items):
    """Add timeout marker to integration tests."""
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(pytest.mark.timeout(10))


@pytest_asyncio.fixture
async def async_session() -> AsyncSession:
    """Provide async database session for tests.

    Note: This uses the same database connection as the application.
    For production tests, consider using a separate test database.

    Yields:
        AsyncSession: Database session for testing
    """
    try:
        async with AsyncSessionLocal() as session:
            # Quick connectivity check
            await session.execute(text("SELECT 1"))
            yield session
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.fixture
def tolerance() -> float:
    """Provide tolerance for floating point comparisons.

    Returns:
        float: Maximum acceptable difference for float equality
    """
    return 0.0001


@pytest.fixture
def max_violations() -> int:
    """Maximum number of violations to report in test failures.

    Returns:
        int: Limit for error reporting to avoid overwhelming output
    """
    return 10


@pytest.fixture
def sample_size() -> int:
    """Sample size for quick integration tests.

    Returns:
        int: Number of records to sample for fast testing
    """
    return 100
