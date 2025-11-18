"""Data integrity tests for proposers_balance table."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.proposers.db import ProposerBalancesDB


@pytest.mark.asyncio
async def test_balance_foreign_key_integrity(
    async_session: AsyncSession, max_violations: int
):
    """Test that all proposer balance records reference valid blocks.

    Every entry in proposers_balance should have a corresponding block
    in the blocks table.
    """
    stmt = text("""
        SELECT pb.block_number
        FROM proposers_balance pb
        LEFT JOIN blocks b ON pb.block_number = b.number
        WHERE b.number IS NULL
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    orphaned = result.fetchall()

    assert len(orphaned) == 0, (
        f"Found {len(orphaned)} orphaned proposer balance records (no matching block): {orphaned[:10]}"
    )


@pytest.mark.asyncio
async def test_miner_address_consistency(
    async_session: AsyncSession, max_violations: int
):
    """Test that miner addresses match between blocks and proposer_balance.

    The miner field should be consistent between the two tables for the
    same block_number.
    """
    stmt = text("""
        SELECT
            pb.block_number,
            b.miner as block_miner,
            pb.miner as balance_miner
        FROM proposers_balance pb
        JOIN blocks b ON pb.block_number = b.number
        WHERE b.miner != pb.miner
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    mismatches = result.fetchall()

    assert len(mismatches) == 0, (
        f"Found {len(mismatches)} miner address mismatches between blocks and proposer_balance: {mismatches[:10]}"
    )


@pytest.mark.asyncio
async def test_balance_calculation(async_session: AsyncSession, max_violations: int):
    """Test that balance_increase = balance_after - balance_before.

    This fundamental calculation should be exact for all records.
    """
    stmt = (
        select(
            ProposerBalancesDB.block_number,
            ProposerBalancesDB.balance_before,
            ProposerBalancesDB.balance_after,
            ProposerBalancesDB.balance_increase,
        ).limit(10000)  # Sample for performance
    )

    result = await async_session.execute(stmt)

    violations = []
    for row in result:
        expected = row.balance_after - row.balance_before
        actual = row.balance_increase
        if expected != actual:
            violations.append((row.block_number, expected, actual, expected - actual))
            if len(violations) >= max_violations:
                break

    assert len(violations) == 0, (
        f"Found {len(violations)} balance calculation errors: {violations[:10]}"
    )


@pytest.mark.asyncio
async def test_wei_value_ranges(async_session: AsyncSession, max_violations: int):
    """Test that Wei values are within realistic ranges.

    Very large values (> 100k ETH) are suspicious and should be flagged.
    Balances should also be non-negative.
    """
    max_reasonable_wei = 100_000 * 10**18  # 100k ETH in Wei

    # Check balance_before is non-negative
    stmt = (
        select(ProposerBalancesDB.block_number, ProposerBalancesDB.balance_before)
        .where(ProposerBalancesDB.balance_before < 0)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    negative_before = result.fetchall()

    assert len(negative_before) == 0, (
        f"Found {len(negative_before)} negative balance_before values: {negative_before[:10]}"
    )

    # Check balance_after is non-negative
    stmt = (
        select(ProposerBalancesDB.block_number, ProposerBalancesDB.balance_after)
        .where(ProposerBalancesDB.balance_after < 0)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    negative_after = result.fetchall()

    assert len(negative_after) == 0, (
        f"Found {len(negative_after)} negative balance_after values: {negative_after[:10]}"
    )

    # Check for excessively large balance_increase (can be negative due to gas costs)
    stmt = (
        select(ProposerBalancesDB.block_number, ProposerBalancesDB.balance_increase)
        .where(ProposerBalancesDB.balance_increase > max_reasonable_wei)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    excessive = result.fetchall()

    assert len(excessive) == 0, (
        f"Found {len(excessive)} excessive balance increases (> 100k ETH): {excessive[:10]}"
    )


@pytest.mark.asyncio
async def test_no_orphaned_balance_records(async_session: AsyncSession):
    """Test that every proposer balance has a corresponding block.

    This is the inverse of the foreign key test, checking for completeness.
    """
    stmt = text("""
        SELECT COUNT(*) as orphaned_count
        FROM proposers_balance pb
        LEFT JOIN blocks b ON pb.block_number = b.number
        WHERE b.number IS NULL
    """)
    result = await async_session.execute(stmt)
    count = result.scalar()

    assert count == 0, f"Found {count} orphaned proposer balance records"


@pytest.mark.asyncio
async def test_balance_record_uniqueness(async_session: AsyncSession):
    """Test that each block has at most one balance record.

    block_number is the primary key and should be unique.
    """
    stmt = (
        select(ProposerBalancesDB.block_number, func.count().label("count"))
        .group_by(ProposerBalancesDB.block_number)
        .having(func.count() > 1)
    )
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert len(duplicates) == 0, (
        f"Found {len(duplicates)} duplicate balance records for same block: {duplicates}"
    )


@pytest.mark.asyncio
async def test_miner_address_format(async_session: AsyncSession, max_violations: int):
    """Test that miner addresses have correct Ethereum address format.

    Addresses should be 42 characters (0x + 40 hex characters).
    """
    stmt = text("""
        SELECT block_number, miner, LENGTH(miner) as address_length
        FROM proposers_balance
        WHERE LENGTH(miner) != 42 OR NOT miner LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert len(invalid) == 0, (
        f"Found {len(invalid)} invalid miner address formats: {invalid[:10]}"
    )


@pytest.mark.asyncio
@pytest.mark.slow
async def test_negative_balance_increases_reasonable(
    async_session: AsyncSession, max_violations: int
):
    """Test that negative balance increases are reasonable.

    Proposers can lose ETH due to gas costs, but the losses should be
    relatively small (< 1 ETH typically).
    """
    max_reasonable_loss = -1 * 10**18  # -1 ETH in Wei

    stmt = (
        select(ProposerBalancesDB.block_number, ProposerBalancesDB.balance_increase)
        .where(ProposerBalancesDB.balance_increase < max_reasonable_loss)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    excessive_losses = result.fetchall()

    # This is a warning-level test - large losses are possible but suspicious
    if len(excessive_losses) > 0:
        print(
            f"WARNING: Found {len(excessive_losses)} blocks with large proposer losses (< -1 ETH): {excessive_losses[:10]}"
        )
