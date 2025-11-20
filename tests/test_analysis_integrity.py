"""Data integrity tests for analysis_pbs_v2 table."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.analysis.db import AnalysisPBSV3DB as AnalysisPBSDB

# Mark all tests in this module as integration tests (require database)
pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_total_value_calculation(
    async_session: AsyncSession, max_violations: int, tolerance: float, sample_size: int
):
    """Test that total_value = builder_balance_increase + proposer_subsidy.

    This fundamental calculation should be exact (allowing for small
    floating point errors).
    """
    stmt = (
        select(
            AnalysisPBSDB.block_number,
            AnalysisPBSDB.builder_balance_increase,
            AnalysisPBSDB.proposer_subsidy,
            AnalysisPBSDB.total_value,
        ).limit(sample_size)  # Sample for performance
    )

    result = await async_session.execute(stmt)

    violations = []
    for row in result:
        expected = (row.builder_balance_increase or 0.0) + (row.proposer_subsidy or 0.0)
        actual = row.total_value or 0.0
        if abs(expected - actual) > tolerance:
            violations.append((row.block_number, expected, actual, expected - actual))
            if len(violations) >= max_violations:
                break

    assert len(violations) == 0, (
        f"Found {len(violations)} total_value calculation errors: {violations[:10]}"
    )


@pytest.mark.asyncio
async def test_vanilla_block_classification(
    async_session: AsyncSession, max_violations: int, sample_size: int
):
    """Test that vanilla block flags are consistent with relay data.

    Vanilla blocks should have n_relays = 0 and relays = NULL.
    Non-vanilla blocks should have n_relays > 0 and relays NOT NULL.
    """
    stmt = (
        select(
            AnalysisPBSDB.block_number,
            AnalysisPBSDB.is_block_vanilla,
            AnalysisPBSDB.n_relays,
            AnalysisPBSDB.relays,
        ).limit(sample_size)  # Sample for performance
    )

    result = await async_session.execute(stmt)

    violations = []
    for row in result:
        if row.is_block_vanilla:
            # Vanilla blocks should have n_relays = 0 and relays = NULL
            if row.n_relays != 0 or row.relays is not None:
                violations.append(
                    (row.block_number, "vanilla", row.n_relays, row.relays)
                )
        else:
            # Non-vanilla blocks should have n_relays > 0 and relays NOT NULL
            if row.n_relays == 0 or row.relays is None:
                violations.append(
                    (row.block_number, "non-vanilla", row.n_relays, row.relays)
                )

        if len(violations) >= max_violations:
            break

    assert len(violations) == 0, (
        f"Found {len(violations)} vanilla block classification violations: {violations[:10]}"
    )


@pytest.mark.asyncio
async def test_relay_count_consistency(
    async_session: AsyncSession, max_violations: int
):
    """Test that n_relays matches length of relays array.

    The relay count should accurately reflect the array length.
    """
    stmt = text("""
        SELECT
            block_number,
            n_relays,
            ARRAY_LENGTH(relays, 1) as actual_relay_count
        FROM analysis_pbs
        WHERE relays IS NOT NULL
        AND n_relays != COALESCE(ARRAY_LENGTH(relays, 1), 0)
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    mismatches = result.fetchall()

    assert len(mismatches) == 0, (
        f"Found {len(mismatches)} relay count mismatches: {mismatches[:10]}"
    )


@pytest.mark.asyncio
async def test_non_nullable_defaults(async_session: AsyncSession):
    """Test that non-nullable fields never contain NULL.

    Critical fields should have default values rather than NULL.
    """
    # Check builder_balance_increase
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.builder_balance_increase.is_(None))
    )
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found NULL builder_balance_increase"

    # Check proposer_subsidy
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.proposer_subsidy.is_(None))
    )
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found NULL proposer_subsidy"

    # Check total_value
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.total_value.is_(None))
    )
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found NULL total_value"

    # Check builder_name (should never be NULL or empty)
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(
            (AnalysisPBSDB.builder_name.is_(None)) | (AnalysisPBSDB.builder_name == "")
        )
    )
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found NULL or empty builder_name"

    # Check is_block_vanilla
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.is_block_vanilla.is_(None))
    )
    result = await async_session.execute(stmt)
    assert result.scalar() == 0, "Found NULL is_block_vanilla"


@pytest.mark.asyncio
async def test_timestamp_consistency(async_session: AsyncSession, max_violations: int):
    """Test that analysis timestamps match block timestamps.

    The block_timestamp should be consistent between tables.
    """
    stmt = text("""
        SELECT
            b.number,
            b.timestamp as block_timestamp,
            a.block_timestamp as analysis_timestamp
        FROM blocks b
        JOIN analysis_pbs a ON b.number = a.block_number
        WHERE b.timestamp != a.block_timestamp
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    mismatches = result.fetchall()

    assert len(mismatches) == 0, (
        f"Found {len(mismatches)} timestamp mismatches between blocks and analysis: {mismatches[:10]}"
    )


@pytest.mark.asyncio
async def test_analysis_references_valid_block(
    async_session: AsyncSession, max_violations: int
):
    """Test that all analysis records reference valid blocks.

    Foreign key integrity check.
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

    assert len(orphaned) == 0, (
        f"Found {len(orphaned)} orphaned analysis records (no matching block): {orphaned[:10]}"
    )


@pytest.mark.asyncio
async def test_n_relays_range(async_session: AsyncSession, max_violations: int):
    """Test that n_relays is within expected range.

    Should be >= 0 and typically <= 10 (current max number of relays).
    """
    # Check non-negative
    stmt = (
        select(AnalysisPBSDB.block_number, AnalysisPBSDB.n_relays)
        .where(AnalysisPBSDB.n_relays < 0)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    negative = result.fetchall()

    assert len(negative) == 0, (
        f"Found {len(negative)} negative n_relays values: {negative[:10]}"
    )

    # Check reasonable upper bound
    stmt = (
        select(AnalysisPBSDB.block_number, AnalysisPBSDB.n_relays)
        .where(AnalysisPBSDB.n_relays > 20)  # Very generous upper bound
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    excessive = result.fetchall()

    # This is informational - may legitimately have many relays in future
    if len(excessive) > 0:
        print(
            f"INFO: Found {len(excessive)} blocks with unexpectedly high relay counts (> 20): {excessive[:10]}"
        )


@pytest.mark.asyncio
async def test_value_fields_non_negative(async_session: AsyncSession):
    """Test that value fields are non-negative where expected.

    Total value and proposer subsidy should be non-negative.
    Builder balance increase can be negative (losses from gas costs).
    """
    # Check total_value is non-negative
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.total_value < 0)
    )
    result = await async_session.execute(stmt)
    count = result.scalar()
    assert count == 0, f"Found {count} negative total_value entries"

    # Check proposer_subsidy is non-negative
    stmt = (
        select(func.count())
        .select_from(AnalysisPBSDB)
        .where(AnalysisPBSDB.proposer_subsidy < 0)
    )
    result = await async_session.execute(stmt)
    count = result.scalar()
    assert count == 0, f"Found {count} negative proposer_subsidy entries"


@pytest.mark.asyncio
async def test_analysis_record_uniqueness(async_session: AsyncSession):
    """Test that each block has at most one analysis record.

    block_number is the primary key and should be unique.
    """
    stmt = (
        select(AnalysisPBSDB.block_number, func.count().label("count"))
        .group_by(AnalysisPBSDB.block_number)
        .having(func.count() > 1)
    )
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert len(duplicates) == 0, (
        f"Found {len(duplicates)} duplicate analysis records for same block: {duplicates}"
    )
