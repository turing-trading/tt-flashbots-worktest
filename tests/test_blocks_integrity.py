"""Data integrity tests for blocks table."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockDB


@pytest.mark.asyncio
async def test_block_continuity(async_session: AsyncSession, max_violations: int):
    """Test that there are no gaps in block numbers.

    Verifies that all blocks between min and max block numbers exist
    in the database without any missing entries.
    """
    # Get min and max block numbers
    stmt = select(func.min(BlockDB.number), func.max(BlockDB.number))
    result = await async_session.execute(stmt)
    min_block, max_block = result.one()

    if min_block is None or max_block is None:
        pytest.skip("No blocks in database")

    # Count actual blocks
    stmt = select(func.count()).select_from(BlockDB)
    result = await async_session.execute(stmt)
    actual_count = result.scalar()

    # Expected count (inclusive range)
    expected_count = max_block - min_block + 1

    assert (
        actual_count == expected_count
    ), f"Found block gaps: expected {expected_count} blocks, got {actual_count} (range: {min_block}-{max_block})"


@pytest.mark.asyncio
async def test_block_hash_uniqueness(async_session: AsyncSession, max_violations: int):
    """Test that all block hashes are unique.

    Each block should have a unique hash. Duplicate hashes would indicate
    data corruption or incorrect processing.
    """
    stmt = (
        select(BlockDB.hash, func.count().label("count"))
        .group_by(BlockDB.hash)
        .having(func.count() > 1)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate block hashes: {duplicates}"


@pytest.mark.asyncio
async def test_parent_hash_chain(async_session: AsyncSession, max_violations: int):
    """Test that parent hashes form a valid chain.

    Each block's parent_hash should match the hash of the previous block,
    forming an unbroken chain (except for the first block in the database).
    """
    # Get blocks with their predecessors to check parent hash chain
    stmt = text("""
        SELECT
            b1.number as block_number,
            b1.parent_hash,
            b2.hash as prev_hash
        FROM blocks b1
        LEFT JOIN blocks b2 ON b2.number = b1.number - 1
        WHERE b2.number IS NOT NULL
        AND b1.parent_hash != b2.hash
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    violations = result.fetchall()

    assert (
        len(violations) == 0
    ), f"Found {len(violations)} parent hash chain violations: {violations[:10]}"


@pytest.mark.asyncio
async def test_timestamp_monotonicity(async_session: AsyncSession, max_violations: int):
    """Test that block timestamps are monotonically non-decreasing.

    Block timestamps should generally increase or stay the same as block
    numbers increase. Timestamps going backward would indicate data issues.
    """
    stmt = text("""
        SELECT
            b1.number as block_number,
            b1.timestamp as current_timestamp,
            b2.timestamp as previous_timestamp
        FROM blocks b1
        JOIN blocks b2 ON b2.number = b1.number - 1
        WHERE b1.timestamp < b2.timestamp
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    violations = result.fetchall()

    assert (
        len(violations) == 0
    ), f"Found {len(violations)} timestamp ordering violations (timestamps going backward): {violations[:10]}"


@pytest.mark.asyncio
async def test_required_fields_not_null(async_session: AsyncSession):
    """Test that required fields are never NULL.

    Core block fields like number, hash, timestamp should never be NULL.
    """
    # Check critical fields are not NULL
    stmt = select(func.count()).select_from(BlockDB).where(BlockDB.number.is_(None))
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found blocks with NULL number"

    stmt = select(func.count()).select_from(BlockDB).where(BlockDB.hash.is_(None))
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found blocks with NULL hash"

    stmt = select(func.count()).select_from(BlockDB).where(BlockDB.timestamp.is_(None))
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found blocks with NULL timestamp"

    stmt = select(func.count()).select_from(BlockDB).where(BlockDB.miner.is_(None))
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found blocks with NULL miner"


@pytest.mark.asyncio
async def test_block_hash_format(async_session: AsyncSession, max_violations: int):
    """Test that block hashes have correct format.

    Block hashes should be 66 characters (0x + 64 hex characters).
    """
    stmt = text("""
        SELECT number, hash, LENGTH(hash) as hash_length
        FROM blocks
        WHERE LENGTH(hash) != 66 OR NOT hash LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} blocks with invalid hash format: {invalid[:10]}"


@pytest.mark.asyncio
async def test_miner_address_format(async_session: AsyncSession, max_violations: int):
    """Test that miner addresses have correct format.

    Miner addresses should be 42 characters (0x + 40 hex characters).
    """
    stmt = text("""
        SELECT number, miner, LENGTH(miner) as address_length
        FROM blocks
        WHERE LENGTH(miner) != 42 OR NOT miner LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} blocks with invalid miner address format: {invalid[:10]}"


@pytest.mark.asyncio
async def test_gas_values_consistency(async_session: AsyncSession, max_violations: int):
    """Test that gas_used is never greater than gas_limit.

    The amount of gas used in a block cannot exceed the gas limit.
    """
    stmt = (
        select(BlockDB.number, BlockDB.gas_used, BlockDB.gas_limit)
        .where(BlockDB.gas_used > BlockDB.gas_limit)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    violations = result.fetchall()

    assert (
        len(violations) == 0
    ), f"Found {len(violations)} blocks where gas_used > gas_limit: {violations[:10]}"


@pytest.mark.asyncio
async def test_transaction_count_non_negative(async_session: AsyncSession):
    """Test that transaction counts are non-negative.

    Transaction count should be >= 0 for all blocks.
    """
    stmt = (
        select(func.count())
        .select_from(BlockDB)
        .where(BlockDB.transaction_count < 0)
    )
    result = await async_session.execute(stmt)
    count = result.scalar()

    assert count == 0, f"Found {count} blocks with negative transaction count"


@pytest.mark.asyncio
@pytest.mark.slow
async def test_no_duplicate_numbers(async_session: AsyncSession):
    """Test that block numbers are unique (primary key constraint).

    This should be enforced by the database, but we verify it explicitly.
    """
    stmt = (
        select(BlockDB.number, func.count().label("count"))
        .group_by(BlockDB.number)
        .having(func.count() > 1)
    )
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert len(duplicates) == 0, f"Found duplicate block numbers: {duplicates}"
