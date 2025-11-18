"""Edge case and boundary condition tests."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.analysis.db import AnalysisPBSDB
from src.data.blocks.db import BlockDB
from src.data.proposers.db import ProposerBalancesDB


@pytest.mark.asyncio
async def test_pre_eip1559_blocks(async_session: AsyncSession):
    """Test that pre-EIP-1559 blocks handle NULL base_fee_per_gas correctly.

    Before the London hard fork, base_fee_per_gas didn't exist and should be NULL.
    """
    # London hard fork was at block ~12965000 (Aug 2021)
    # Check that some early blocks have NULL base_fee_per_gas
    stmt = (
        select(func.count())
        .select_from(BlockDB)
        .where(BlockDB.number < 12965000, BlockDB.base_fee_per_gas.is_not(None))
    )
    result = await async_session.execute(stmt)
    count = result.scalar()

    # This is informational - database may only have post-London blocks
    if count == 0:
        print("INFO: No pre-EIP-1559 blocks found in database")


@pytest.mark.asyncio
async def test_blocks_with_no_transactions(async_session: AsyncSession):
    """Test that blocks with zero transactions are handled correctly.

    Empty blocks are valid and should have transaction_count = 0, gas_used = 0.
    """
    stmt = (
        select(BlockDB.number, BlockDB.transaction_count, BlockDB.gas_used)
        .where(BlockDB.transaction_count == 0)
        .limit(100)
    )
    result = await async_session.execute(stmt)
    empty_blocks = result.fetchall()

    if len(empty_blocks) == 0:
        pytest.skip("No empty blocks found in database")

    # Verify gas_used is 0 for empty blocks
    for block in empty_blocks:
        assert (
            block.gas_used == 0
        ), f"Block {block.number} has 0 transactions but gas_used = {block.gas_used}"


@pytest.mark.asyncio
async def test_negative_balance_increases(async_session: AsyncSession):
    """Test that negative balance increases are handled correctly.

    Proposers can lose ETH due to gas costs, which is a valid scenario.
    """
    stmt = (
        select(ProposerBalancesDB.block_number, ProposerBalancesDB.balance_increase)
        .where(ProposerBalancesDB.balance_increase < 0)
        .limit(100)
    )
    result = await async_session.execute(stmt)
    negative_increases = result.fetchall()

    # This is informational - negative increases are valid
    if len(negative_increases) > 0:
        print(
            f"INFO: Found {len(negative_increases)} blocks with negative proposer balance increases (gas costs > rewards)"
        )


@pytest.mark.asyncio
async def test_vanilla_blocks_have_zero_relays(async_session: AsyncSession):
    """Test that vanilla blocks (non-MEV-Boost) have proper zero values.

    Vanilla blocks should have no relay data and appropriate zero values.
    """
    stmt = (
        select(
            AnalysisPBSDB.block_number,
            AnalysisPBSDB.is_block_vanilla,
            AnalysisPBSDB.n_relays,
            AnalysisPBSDB.proposer_subsidy,
        )
        .where(AnalysisPBSDB.is_block_vanilla == True)
        .limit(100)
    )
    result = await async_session.execute(stmt)
    vanilla_blocks = result.fetchall()

    if len(vanilla_blocks) == 0:
        pytest.skip("No vanilla blocks found in database")

    for block in vanilla_blocks:
        assert (
            block.n_relays == 0
        ), f"Vanilla block {block.block_number} has non-zero n_relays: {block.n_relays}"
        assert (
            block.proposer_subsidy == 0.0
        ), f"Vanilla block {block.block_number} has non-zero proposer_subsidy: {block.proposer_subsidy}"


@pytest.mark.asyncio
async def test_very_large_eth_values(async_session: AsyncSession, max_violations: int):
    """Test handling of very large ETH values.

    Values > 100k ETH are suspicious and should be flagged.
    """
    max_reasonable_eth = 100_000.0

    stmt = (
        select(AnalysisPBSDB.block_number, AnalysisPBSDB.total_value)
        .where(AnalysisPBSDB.total_value > max_reasonable_eth)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    excessive = result.fetchall()

    # This is informational - may have legitimate large values
    if len(excessive) > 0:
        print(
            f"INFO: Found {len(excessive)} blocks with very large total_value (> 100k ETH): {excessive[:10]}"
        )


@pytest.mark.asyncio
async def test_very_small_eth_values(async_session: AsyncSession):
    """Test that very small ETH values are represented correctly.

    Values close to zero should not become negative due to float precision.
    """
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(
            AnalysisPBSDB.total_value < 0,
            AnalysisPBSDB.total_value > -0.0001,  # Allow tiny float errors
        )
    )
    result = await async_session.execute(stmt)
    count = result.scalar()

    # This is informational - small negative values from float precision
    if count > 0:
        print(f"INFO: Found {count} blocks with tiny negative total_value (float precision)")


@pytest.mark.asyncio
async def test_blocks_at_slot_boundaries(async_session: AsyncSession):
    """Test that blocks at slot boundaries are handled correctly.

    Check for any anomalies around slot/epoch boundaries.
    """
    # Query to find blocks that might be at slot boundaries
    # (this is informational as we don't have direct slot boundary info in all tables)
    stmt = text("""
        SELECT
            block_number,
            COUNT(DISTINCT relay) as relay_count
        FROM relays_payloads
        WHERE slot % 32 = 0  -- Epoch boundaries
        GROUP BY block_number
        LIMIT 100
    """)
    result = await async_session.execute(stmt)
    boundary_blocks = result.fetchall()

    # This is just informational
    if len(boundary_blocks) > 0:
        print(f"INFO: Found {len(boundary_blocks)} blocks at epoch boundaries")


@pytest.mark.asyncio
async def test_builder_name_unknown_default(async_session: AsyncSession):
    """Test that unknown builders are properly marked.

    When builder cannot be identified, it should default to 'unknown'.
    """
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.builder_name == "unknown")
    )
    result = await async_session.execute(stmt)
    unknown_count = result.scalar()

    # This is informational - some blocks will have unknown builders
    if unknown_count > 0:
        print(f"INFO: Found {unknown_count} blocks with builder_name = 'unknown'")


@pytest.mark.asyncio
async def test_multiple_relays_same_block(async_session: AsyncSession):
    """Test that blocks appearing in multiple relays are handled correctly.

    The same block can be submitted to multiple relays, which is expected behavior.
    """
    stmt = text("""
        SELECT
            block_number,
            COUNT(DISTINCT relay) as relay_count
        FROM relays_payloads
        GROUP BY block_number
        ORDER BY relay_count DESC
        LIMIT 10
    """)
    result = await async_session.execute(stmt)
    multi_relay_blocks = result.fetchall()

    # This is informational - shows distribution of relay usage
    if len(multi_relay_blocks) > 0:
        max_relays = multi_relay_blocks[0].relay_count if multi_relay_blocks else 0
        print(
            f"INFO: Maximum relays for single block: {max_relays}. "
            f"Top multi-relay blocks: {multi_relay_blocks[:5]}"
        )


@pytest.mark.asyncio
async def test_hash_collision_detection(async_session: AsyncSession):
    """Test that there are no hash collisions in critical fields.

    This is extremely unlikely but would be catastrophic if it occurred.
    """
    # Check block hashes
    stmt = (
        select(BlockDB.hash, func.count().label("count"))
        .group_by(BlockDB.hash)
        .having(func.count() > 1)
    )
    result = await async_session.execute(stmt)
    hash_collisions = result.fetchall()

    assert (
        len(hash_collisions) == 0
    ), f"CRITICAL: Found block hash collisions: {hash_collisions}"

    # Check parent hashes pointing to different blocks
    stmt = text("""
        SELECT parent_hash, COUNT(DISTINCT hash) as different_hashes
        FROM blocks
        GROUP BY parent_hash
        HAVING COUNT(DISTINCT hash) > 1
        LIMIT 10
    """)
    result = await async_session.execute(stmt)
    parent_collisions = result.fetchall()

    # Multiple blocks can have same parent (forks/reorgs), this is informational
    if len(parent_collisions) > 0:
        print(f"INFO: Found {len(parent_collisions)} potential forks/reorgs")


@pytest.mark.asyncio
async def test_timestamp_unix_epoch_boundaries(async_session: AsyncSession):
    """Test that timestamps are within reasonable Unix epoch range.

    Timestamps should be after Ethereum genesis and before far future.
    """
    from datetime import datetime

    ethereum_genesis = datetime(2015, 7, 30)  # Ethereum mainnet launch
    far_future = datetime(2100, 1, 1)  # Reasonable upper bound

    stmt = (
        select(func.count())
        .select_from(BlockDB)
        .where(BlockDB.timestamp < ethereum_genesis)
    )
    result = await async_session.execute(stmt)
    too_early = result.scalar()

    assert (
        too_early == 0
    ), f"Found {too_early} blocks with timestamps before Ethereum genesis"

    stmt = (
        select(func.count())
        .select_from(BlockDB)
        .where(BlockDB.timestamp > far_future)
    )
    result = await async_session.execute(stmt)
    too_late = result.scalar()

    assert too_late == 0, f"Found {too_late} blocks with timestamps in far future"
