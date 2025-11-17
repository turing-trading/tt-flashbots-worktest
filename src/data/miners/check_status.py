"""Quick diagnostic script to check miners backfill status."""

from asyncio import run

from sqlalchemy import text

from src.helpers.db import AsyncSessionLocal


async def check_status():
    """Check the status of blocks and miners_balance tables."""
    async with AsyncSessionLocal() as session:
        # Check blocks table
        result = await session.execute(text("SELECT COUNT(*) FROM blocks"))
        blocks_count = result.scalar()
        print(f"Blocks table: {blocks_count:,} rows")

        # Check min/max block numbers in blocks
        result = await session.execute(
            text("SELECT MIN(number), MAX(number) FROM blocks")
        )
        row = result.fetchone()
        if row:
            min_block, max_block = row
        else:
            min_block, max_block = None, None
        print(f"Block range: {min_block:,} to {max_block:,}")

        # Check miners_balance table
        result = await session.execute(text("SELECT COUNT(*) FROM miners_balance"))
        balance_count = result.scalar()
        print(f"Miners balance table: {balance_count:,} rows")

        # Check missing blocks count
        result = await session.execute(
            text(
                """
            SELECT COUNT(*)
            FROM blocks b
            LEFT JOIN miners_balance mb ON b.number = mb.block_number
            WHERE mb.block_number IS NULL AND b.number > 0
        """
            )
        )
        missing_count = result.scalar()
        print(f"Missing blocks: {missing_count:,}")

        # Show sample of missing blocks
        result = await session.execute(
            text(
                """
            SELECT b.number, b.miner
            FROM blocks b
            LEFT JOIN miners_balance mb ON b.number = mb.block_number
            WHERE mb.block_number IS NULL AND b.number > 0
            ORDER BY b.number DESC
            LIMIT 10
        """
            )
        )
        missing_sample = result.fetchall()
        print("\nSample of missing blocks (most recent):")
        for block_num, miner in missing_sample:
            print(f"  Block {block_num}: {miner}")


if __name__ == "__main__":
    run(check_status())
