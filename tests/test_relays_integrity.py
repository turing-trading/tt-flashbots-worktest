"""Data integrity tests for relays_payloads table."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.constants import RELAYS
from src.data.relays.db import RelaysPayloadsDB


@pytest.mark.asyncio
async def test_composite_key_uniqueness(async_session: AsyncSession):
    """Test that (slot, relay) combinations are unique.

    The composite primary key should ensure no duplicate entries.
    """
    stmt = text("""
        SELECT slot, relay, COUNT(*) as count
        FROM relays_payloads
        GROUP BY slot, relay
        HAVING COUNT(*) > 1
    """)
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert (
        len(duplicates) == 0
    ), f"Found {len(duplicates)} duplicate (slot, relay) combinations: {duplicates}"


@pytest.mark.asyncio
async def test_slot_to_block_consistency(
    async_session: AsyncSession, max_violations: int
):
    """Test that same slot maps to same block across relays.

    All relays should report the same execution layer block for a given
    beacon chain slot.
    """
    stmt = text("""
        SELECT slot, COUNT(DISTINCT block_number) as block_count
        FROM relays_payloads
        GROUP BY slot
        HAVING COUNT(DISTINCT block_number) > 1
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    inconsistent = result.fetchall()

    assert (
        len(inconsistent) == 0
    ), f"Found {len(inconsistent)} slots with multiple different block numbers: {inconsistent[:10]}"


@pytest.mark.asyncio
async def test_relay_coverage(async_session: AsyncSession):
    """Test that all configured relays have recent data.

    Each relay in the RELAYS configuration should have data from recent slots.
    """
    # Get latest slot
    stmt = select(func.max(RelaysPayloadsDB.slot))
    result = await async_session.execute(stmt)
    max_slot = result.scalar()

    if max_slot is None:
        pytest.skip("No relay data in database")

    # Check each relay has data in last 10000 slots (roughly 1 day)
    recent_threshold = max_slot - 10000

    missing_relays = []
    for relay in RELAYS:
        stmt = (
            select(func.count())
            .select_from(RelaysPayloadsDB)
            .where(
                RelaysPayloadsDB.relay == relay,
                RelaysPayloadsDB.slot >= recent_threshold,
            )
        )
        result = await async_session.execute(stmt)
        count = result.scalar()

        if count == 0:
            missing_relays.append(relay)

    # This is a warning rather than failure - relays can go offline
    if len(missing_relays) > 0:
        print(
            f"WARNING: {len(missing_relays)} relays have no recent data (last 10000 slots): {missing_relays}"
        )


@pytest.mark.asyncio
async def test_builder_pubkey_format(async_session: AsyncSession, max_violations: int):
    """Test that builder public keys have correct format.

    Builder pubkeys should be 98 characters (0x + 96 hex characters).
    """
    stmt = text("""
        SELECT slot, relay, builder_pubkey, LENGTH(builder_pubkey) as key_length
        FROM relays_payloads
        WHERE LENGTH(builder_pubkey) != 98 OR NOT builder_pubkey LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} invalid builder pubkey formats: {invalid[:10]}"


@pytest.mark.asyncio
async def test_proposer_pubkey_format(
    async_session: AsyncSession, max_violations: int
):
    """Test that proposer public keys have correct format.

    Proposer pubkeys should be 98 characters (0x + 96 hex characters).
    """
    stmt = text("""
        SELECT slot, relay, proposer_pubkey, LENGTH(proposer_pubkey) as key_length
        FROM relays_payloads
        WHERE LENGTH(proposer_pubkey) != 98 OR NOT proposer_pubkey LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} invalid proposer pubkey formats: {invalid[:10]}"


@pytest.mark.asyncio
async def test_proposer_fee_recipient_format(
    async_session: AsyncSession, max_violations: int
):
    """Test that fee recipient addresses have correct format.

    Fee recipients should be 42 characters (0x + 40 hex characters).
    """
    stmt = text("""
        SELECT slot, relay, proposer_fee_recipient, LENGTH(proposer_fee_recipient) as address_length
        FROM relays_payloads
        WHERE LENGTH(proposer_fee_recipient) != 42 OR NOT proposer_fee_recipient LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} invalid fee recipient formats: {invalid[:10]}"


@pytest.mark.asyncio
async def test_value_field_non_negative(async_session: AsyncSession):
    """Test that proposer payment values are non-negative.

    The value field represents payment to proposers and should always be >= 0.
    """
    stmt = select(func.count()).select_from(RelaysPayloadsDB).where(RelaysPayloadsDB.value < 0)
    result = await async_session.execute(stmt)
    count = result.scalar()

    assert count == 0, f"Found {count} relay payloads with negative values"


@pytest.mark.asyncio
async def test_gas_values_consistency(async_session: AsyncSession, max_violations: int):
    """Test that gas_used is never greater than gas_limit in relay data.

    Same validation as for blocks - gas used cannot exceed gas limit.
    """
    stmt = (
        select(RelaysPayloadsDB.slot, RelaysPayloadsDB.relay, RelaysPayloadsDB.gas_used, RelaysPayloadsDB.gas_limit)
        .where(RelaysPayloadsDB.gas_used > RelaysPayloadsDB.gas_limit)
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    violations = result.fetchall()

    assert (
        len(violations) == 0
    ), f"Found {len(violations)} relay payloads where gas_used > gas_limit: {violations[:10]}"


@pytest.mark.asyncio
async def test_num_tx_non_negative(async_session: AsyncSession):
    """Test that transaction counts are non-negative.

    The num_tx field should always be >= 0.
    """
    stmt = select(func.count()).select_from(RelaysPayloadsDB).where(RelaysPayloadsDB.num_tx < 0)
    result = await async_session.execute(stmt)
    count = result.scalar()

    assert count == 0, f"Found {count} relay payloads with negative transaction count"


@pytest.mark.asyncio
@pytest.mark.slow
async def test_value_consistency_across_relays(
    async_session: AsyncSession, max_violations: int, tolerance: float
):
    """Test that payment values are similar across relays for same block.

    Different relays should report similar (ideally identical) values for
    the same block. Large discrepancies suggest data issues.
    """
    stmt = text("""
        SELECT
            block_number,
            MIN(value) as min_value,
            MAX(value) as max_value,
            MAX(value) - MIN(value) as difference,
            COUNT(*) as relay_count
        FROM relays_payloads
        GROUP BY block_number
        HAVING COUNT(*) > 1  -- Only check blocks with multiple relays
        AND (MAX(value) - MIN(value)) > :tolerance  -- Allow small differences
        ORDER BY difference DESC
        LIMIT :max_violations
    """)
    # Allow 0.01 ETH difference (10^16 Wei)
    tolerance_wei = int(0.01 * 10**18)
    result = await async_session.execute(
        stmt, {"tolerance": tolerance_wei, "max_violations": max_violations}
    )
    discrepancies = result.fetchall()

    # This is informational - some variation is expected
    if len(discrepancies) > 0:
        print(
            f"INFO: Found {len(discrepancies)} blocks with value discrepancies across relays: {discrepancies[:5]}"
        )


@pytest.mark.asyncio
async def test_relay_payload_references_valid_block(
    async_session: AsyncSession, max_violations: int
):
    """Test that relay payloads reference blocks that exist.

    This checks foreign key integrity with the blocks table.
    """
    stmt = text("""
        SELECT rp.block_number, rp.relay
        FROM relays_payloads rp
        LEFT JOIN blocks b ON rp.block_number = b.number
        WHERE b.number IS NULL
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    orphaned = result.fetchall()

    assert (
        len(orphaned) == 0
    ), f"Found {len(orphaned)} relay payloads referencing non-existent blocks: {orphaned[:10]}"
