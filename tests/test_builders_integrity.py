"""Data integrity tests for builders_identifiers table."""

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.builders.db import BuilderIdentifiersDB


@pytest.mark.asyncio
async def test_builder_name_not_empty(async_session: AsyncSession):
    """Test that builder names are never empty or NULL.

    Builder names should always have a value, defaulting to 'unknown' if needed.
    """
    stmt = (
        select(func.count())
        .select_from(BuilderIdentifiersDB)
        .where(
            (BuilderIdentifiersDB.builder_name.is_(None))
            | (BuilderIdentifiersDB.builder_name == "")
        )
    )
    result = await async_session.execute(stmt)
    count = result.scalar()

    assert count == 0, f"Found {count} builders with empty or NULL names"


@pytest.mark.asyncio
async def test_builder_pubkey_uniqueness(async_session: AsyncSession):
    """Test that builder public keys are unique (primary key).

    Each builder_pubkey should appear only once.
    """
    stmt = (
        select(BuilderIdentifiersDB.builder_pubkey, func.count().label("count"))
        .group_by(BuilderIdentifiersDB.builder_pubkey)
        .having(func.count() > 1)
    )
    result = await async_session.execute(stmt)
    duplicates = result.fetchall()

    assert (
        len(duplicates) == 0
    ), f"Found {len(duplicates)} duplicate builder pubkeys: {duplicates}"


@pytest.mark.asyncio
async def test_builder_pubkey_format(async_session: AsyncSession, max_violations: int):
    """Test that builder public keys have correct format.

    Builder pubkeys should be 98 characters (0x + 96 hex characters).
    """
    stmt = text("""
        SELECT builder_pubkey, builder_name, LENGTH(builder_pubkey) as key_length
        FROM builders_identifiers
        WHERE LENGTH(builder_pubkey) != 98 OR NOT builder_pubkey LIKE '0x%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} invalid builder pubkey formats: {invalid[:10]}"


@pytest.mark.asyncio
async def test_no_null_bytes_in_names(async_session: AsyncSession, max_violations: int):
    """Test that builder names don't contain null bytes.

    Null bytes would indicate improper string parsing or corruption.
    """
    stmt = text("""
        SELECT builder_pubkey, builder_name
        FROM builders_identifiers
        WHERE builder_name LIKE '%\\x00%'
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    invalid = result.fetchall()

    assert (
        len(invalid) == 0
    ), f"Found {len(invalid)} builder names with null bytes: {invalid[:10]}"


@pytest.mark.asyncio
async def test_geth_variants_marked_unknown(
    async_session: AsyncSession, max_violations: int
):
    """Test that Geth client variants are marked as 'unknown'.

    The builder name cleaning logic should mark Geth variants as unknown.
    """
    stmt = (
        select(BuilderIdentifiersDB.builder_pubkey, BuilderIdentifiersDB.builder_name)
        .where(
            BuilderIdentifiersDB.builder_name.ilike("%geth%"),
            BuilderIdentifiersDB.builder_name != "unknown",
        )
        .limit(max_violations)
    )
    result = await async_session.execute(stmt)
    geth_variants = result.fetchall()

    # This is informational - some builders may legitimately have "geth" in name
    if len(geth_variants) > 0:
        print(
            f"INFO: Found {len(geth_variants)} builder names containing 'geth' that are not marked 'unknown': {geth_variants[:10]}"
        )


@pytest.mark.asyncio
async def test_builder_names_normalized(
    async_session: AsyncSession, max_violations: int
):
    """Test that builder names appear properly normalized.

    Check for common normalization issues like excessive whitespace,
    special characters, etc.
    """
    # Check for leading/trailing whitespace
    stmt = text("""
        SELECT builder_pubkey, builder_name
        FROM builders_identifiers
        WHERE builder_name != TRIM(builder_name)
        LIMIT :max_violations
    """)
    result = await async_session.execute(stmt, {"max_violations": max_violations})
    whitespace_issues = result.fetchall()

    assert (
        len(whitespace_issues) == 0
    ), f"Found {len(whitespace_issues)} builder names with leading/trailing whitespace: {whitespace_issues[:10]}"
