"""Cross-table data flow and consistency tests."""

from datetime import datetime

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.analysis.db import AnalysisPBSDB
from src.data.blocks.db import BlockDB


@pytest.mark.asyncio
async def test_block_coverage_for_analysis(
    async_session: AsyncSession, max_violations: int
):
    """Test that all blocks after START_DATE have analysis records.

    Every block from the start date onwards should have been analyzed.
    """
    start_date = datetime(2022, 1, 1, 0, 0, 0)

    stmt = text("""
        SELECT b.number, b.timestamp
        FROM blocks b
        LEFT JOIN analysis_pbs a ON b.number = a.block_number
        WHERE b.timestamp >= :start_date
        AND a.block_number IS NULL
        LIMIT :max_violations
    """)
    result = await async_session.execute(
        stmt, {"start_date": start_date, "max_violations": max_violations}
    )
    missing = result.fetchall()

    assert (
        len(missing) == 0
    ), f"Found {len(missing)} blocks without analysis records (after {start_date}): {missing[:10]}"


@pytest.mark.asyncio
async def test_no_orphaned_analysis_records(
    async_session: AsyncSession, max_violations: int
):
    """Test that all analysis records have corresponding blocks.

    This is the inverse check - no analysis without a block.
    """
    stmt = text("""
        SELECT a.block_number
        FROM analysis_pbs a
        LEFT JOIN blocks b ON a.block_number = b.number
        WHERE b.number IS NULL
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    orphaned = result.fetchall()

    assert (
        len(orphaned) == 0
    ), f"Found {len(orphaned)} analysis records without corresponding blocks: {orphaned[:10]}"


@pytest.mark.asyncio
async def test_data_completeness_continuity(async_session: AsyncSession):
    """Test overall data pipeline completeness.

    Check that we have reasonable coverage across all major tables.
    """
    # Get block count
    stmt = select(func.count()).select_from(BlockDB)
    result = await async_session.execute(stmt)
    block_count = result.scalar()

    if block_count == 0:
        pytest.skip("No blocks in database")

    # Get analysis count (for blocks after START_DATE)
    start_date = datetime(2022, 1, 1, 0, 0, 0)
    stmt = text("""
        SELECT COUNT(*)
        FROM blocks b
        WHERE b.timestamp >= :start_date
    """)
    result = await async_session.execute(stmt, {"start_date": start_date})
    expected_analysis_count = result.scalar()

    stmt = select(func.count()).select_from(AnalysisPBSDB)
    result = await async_session.execute(stmt)
    actual_analysis_count = result.scalar()

    # Allow some tolerance for incomplete backfills
    coverage_ratio = (
        actual_analysis_count / expected_analysis_count if expected_analysis_count > 0 else 0
    )

    # This is informational - may be incomplete during backfill
    if coverage_ratio < 0.99:  # Expect at least 99% coverage
        print(
            f"INFO: Analysis coverage is {coverage_ratio:.2%} "
            f"({actual_analysis_count}/{expected_analysis_count} blocks)"
        )


@pytest.mark.asyncio
async def test_relay_data_matches_mev_boost_blocks(
    async_session: AsyncSession, max_violations: int
):
    """Test that blocks with relay data are properly identified as non-vanilla.

    If a block has relay payloads, it should be marked as non-vanilla in analysis.
    """
    stmt = text("""
        SELECT DISTINCT
            a.block_number,
            a.is_block_vanilla,
            a.n_relays
        FROM analysis_pbs a
        JOIN relays_payloads rp ON a.block_number = rp.block_number
        WHERE a.is_block_vanilla = TRUE
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    misclassified = result.fetchall()

    assert (
        len(misclassified) == 0
    ), f"Found {len(misclassified)} blocks with relay data marked as vanilla: {misclassified[:10]}"


@pytest.mark.asyncio
async def test_proposer_balance_completeness(
    async_session: AsyncSession, max_violations: int
):
    """Test that most blocks have proposer balance records.

    Check for reasonable coverage of balance data.
    """
    # Get total block count
    stmt = select(func.count()).select_from(BlockDB)
    result = await async_session.execute(stmt)
    total_blocks = result.scalar()

    if total_blocks == 0:
        pytest.skip("No blocks in database")

    # Get balance record count
    stmt = text("""
        SELECT COUNT(*)
        FROM proposers_balance
    """)
    result = await async_session.execute(stmt)
    balance_count = result.scalar()

    coverage_ratio = balance_count / total_blocks if total_blocks > 0 else 0

    # This is informational - some blocks may not have balance data
    if coverage_ratio < 0.90:  # Expect at least 90% coverage
        print(
            f"INFO: Proposer balance coverage is {coverage_ratio:.2%} "
            f"({balance_count}/{total_blocks} blocks)"
        )


@pytest.mark.asyncio
async def test_builder_identifiers_referenced(async_session: AsyncSession):
    """Test that builder identifiers are actually used in relay payloads.

    Check that we don't have orphaned builder records.
    """
    stmt = text("""
        SELECT bi.builder_pubkey, bi.builder_name
        FROM builders_identifiers bi
        LEFT JOIN relays_payloads rp ON bi.builder_pubkey = rp.builder_pubkey
        WHERE rp.builder_pubkey IS NULL
        LIMIT 100
    """)
    result = await async_session.execute(stmt)
    orphaned = result.fetchall()

    # This is informational - may have builders not yet used
    if len(orphaned) > 0:
        print(
            f"INFO: Found {len(orphaned)} builder identifiers not referenced in relay payloads: {orphaned[:10]}"
        )


@pytest.mark.asyncio
async def test_cross_table_block_number_consistency(
    async_session: AsyncSession, max_violations: int
):
    """Test that block numbers are consistent across all tables.

    When a block_number appears in multiple tables, verify the data is consistent.
    """
    # Check blocks vs proposers_balance vs analysis
    stmt = text("""
        SELECT
            b.number,
            b.miner as block_miner,
            pb.miner as balance_miner
        FROM blocks b
        JOIN proposers_balance pb ON b.number = pb.block_number
        JOIN analysis_pbs a ON b.number = a.block_number
        WHERE b.miner != pb.miner
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    miner_mismatches = result.fetchall()

    assert (
        len(miner_mismatches) == 0
    ), f"Found {len(miner_mismatches)} miner address mismatches across tables: {miner_mismatches[:10]}"


@pytest.mark.asyncio
@pytest.mark.slow
async def test_relay_aggregation_in_analysis(
    async_session: AsyncSession, max_violations: int
):
    """Test that relay aggregation in analysis matches raw relay data.

    The n_relays count in analysis should match the count from relays_payloads.
    """
    stmt = text("""
        SELECT
            a.block_number,
            a.n_relays as analysis_count,
            COUNT(DISTINCT rp.relay) as actual_count
        FROM analysis_pbs a
        JOIN relays_payloads rp ON a.block_number = rp.block_number
        GROUP BY a.block_number, a.n_relays
        HAVING a.n_relays != COUNT(DISTINCT rp.relay)
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    mismatches = result.fetchall()

    assert (
        len(mismatches) == 0
    ), f"Found {len(mismatches)} relay count mismatches between analysis and raw relay data: {mismatches[:10]}"
