"""Detect potentially missed MEV blocks by analyzing vanilla blocks.

This script identifies blocks that appear to be MEV blocks but are missing
from relays_payloads, potentially due to gaps in relay data collection.

It checks vanilla blocks (not in relays_payloads) for:
1. Known MEV builder addresses (coinbase/miner)
2. Known MEV-related extra_data patterns
"""

import json
from asyncio import run
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger

# Known MEV-related extra_data patterns (hex encoded)
MEV_EXTRA_DATA_PATTERNS = [
    "flashbots",
    "builder0x69",
    "beaverbuild",
    "rsync",
    "titan",
    "penguin",
    "bloxroute",
    "manifold",
    "eth-builder",
]


class MissedMEVDetector:
    """Detect potentially missed MEV blocks."""

    def __init__(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        output_file: str = "missed_mev_blocks.json",
    ):
        """Initialize detector.

        Args:
            start_date: Start date for analysis (default: 2023-01-01)
            end_date: End date for analysis (default: now)
            output_file: Path to save JSON output (default: missed_mev_blocks.json)
        """
        self.start_date = start_date or datetime(2023, 1, 1)
        self.end_date = end_date or datetime.now()
        self.output_file = output_file
        self.logger = get_logger("missed_mev_detector", log_level="INFO")
        self.console = Console()
        self.known_mev_builders: set[str] = set()

    async def _get_known_mev_builders(self, session: AsyncSession) -> set[str]:
        """Get known MEV builder addresses by finding miners of blocks with relay payloads.

        This dynamically builds the list of MEV builders by querying blocks that have
        corresponding entries in relays_payloads.

        Args:
            session: Database session

        Returns:
            Set of known MEV builder miner addresses (lowercase)
        """
        self.logger.info("Building list of known MEV builders from database...")

        stmt = (
            select(func.distinct(BlockDB.miner))
            .select_from(BlockDB)
            .join(
                RelaysPayloadsDB,
                BlockDB.number == RelaysPayloadsDB.block_number,
            )
            .where(BlockDB.miner.isnot(None))
        )

        result = await session.execute(stmt)
        miners = result.fetchall()

        # Convert to set of lowercase addresses
        mev_builders = {miner[0].lower() for miner in miners if miner[0]}

        self.logger.info(f"Found {len(mev_builders)} unique MEV builder addresses")
        return mev_builders

    async def _get_vanilla_blocks_with_mev_characteristics(
        self, session: AsyncSession
    ) -> list[dict]:
        """Find vanilla blocks that have MEV characteristics.

        Uses keyset pagination (cursor-based) instead of OFFSET for optimal performance.
        Each query is O(1) instead of O(n) as with OFFSET-based pagination.

        Returns:
            List of blocks with MEV characteristics
        """
        self.logger.info("Querying vanilla blocks with MEV characteristics...")

        batch_size = 50_000
        missed_mev_blocks = []
        total_vanilla_blocks = 0
        last_block_number = None  # Cursor for keyset pagination
        batch_num = 0

        while True:
            # Query vanilla blocks using LEFT JOIN - blocks without relay payloads
            # Uses keyset pagination: WHERE block_number < cursor instead of OFFSET
            stmt = (
                select(
                    BlockDB.number.label("block_number"),
                    BlockDB.timestamp.label("timestamp"),
                    BlockDB.miner.label("miner"),
                    BlockDB.extra_data.label("extra_data"),
                )
                .select_from(BlockDB)
                .outerjoin(
                    RelaysPayloadsDB,
                    BlockDB.number == RelaysPayloadsDB.block_number,
                )
                .where(
                    and_(
                        BlockDB.timestamp >= self.start_date,
                        BlockDB.timestamp <= self.end_date,
                        RelaysPayloadsDB.block_number.is_(None),  # No relay payload
                    )
                )
            )

            # Add keyset cursor condition (after first batch)
            if last_block_number is not None:
                stmt = stmt.where(BlockDB.number < last_block_number)

            stmt = stmt.order_by(BlockDB.number.desc()).limit(batch_size)

            result = await session.execute(stmt)
            vanilla_blocks = result.fetchall()

            if not vanilla_blocks:
                break

            total_vanilla_blocks += len(vanilla_blocks)
            batch_num += 1

            # Track last block number for cursor
            last_block_number = min(block.block_number for block in vanilla_blocks)

            self.logger.info(
                f"Processing batch {batch_num}: {len(vanilla_blocks)} vanilla blocks "
                f"(cursor at block {last_block_number:,})"
            )

            # Filter for MEV characteristics in this batch
            for block in vanilla_blocks:
                is_mev = False
                mev_indicators = []

                # Check miner address against known MEV builders
                if block.miner and block.miner.lower() in self.known_mev_builders:
                    is_mev = True
                    mev_indicators.append(f"Known builder: {block.miner}")

                # Check extra_data for MEV patterns
                if block.extra_data:
                    try:
                        # Decode hex extra_data to string
                        extra_data_hex = block.extra_data
                        if extra_data_hex.startswith("0x"):
                            extra_data_hex = extra_data_hex[2:]
                        extra_data_str = bytes.fromhex(extra_data_hex).decode(
                            "utf-8", errors="ignore"
                        ).lower()

                        for pattern in MEV_EXTRA_DATA_PATTERNS:
                            if pattern.lower() in extra_data_str:
                                is_mev = True
                                mev_indicators.append(f"Extra data contains: {pattern}")
                                break
                    except Exception:
                        pass

                if is_mev:
                    missed_mev_blocks.append(
                        {
                            "block_number": block.block_number,
                            "timestamp": block.timestamp,
                            "miner": block.miner,
                            "extra_data": block.extra_data,
                            "indicators": mev_indicators,
                        }
                    )

            # Cursor automatically moves via last_block_number tracking

        self.logger.info(
            f"Processed {total_vanilla_blocks} vanilla blocks in {batch_num} batches, "
            f"found {len(missed_mev_blocks)} potentially missed MEV blocks"
        )
        return missed_mev_blocks

    async def _get_vanilla_block_stats(self, session: AsyncSession) -> dict:
        """Get statistics about vanilla vs relay blocks.

        Returns:
            Dictionary with block statistics
        """
        self.logger.info("Calculating block statistics...")

        # Total blocks in range
        total_blocks_stmt = (
            select(func.count())
            .select_from(BlockDB)
            .where(
                and_(
                    BlockDB.timestamp >= self.start_date,
                    BlockDB.timestamp <= self.end_date,
                )
            )
        )
        total_result = await session.execute(total_blocks_stmt)
        total_blocks = total_result.scalar_one()

        # Blocks in relays_payloads
        relay_blocks_stmt = (
            select(func.count(func.distinct(RelaysPayloadsDB.block_number)))
            .select_from(RelaysPayloadsDB)
            .join(BlockDB, RelaysPayloadsDB.block_number == BlockDB.number)
            .where(
                and_(
                    BlockDB.timestamp >= self.start_date,
                    BlockDB.timestamp <= self.end_date,
                )
            )
        )
        relay_result = await session.execute(relay_blocks_stmt)
        relay_blocks = relay_result.scalar_one()

        # Calculate vanilla blocks
        vanilla_blocks = total_blocks - relay_blocks

        return {
            "total_blocks": total_blocks,
            "relay_blocks": relay_blocks,
            "vanilla_blocks": vanilla_blocks,
            "relay_pct": (relay_blocks / total_blocks * 100) if total_blocks > 0 else 0,
            "vanilla_pct": (vanilla_blocks / total_blocks * 100) if total_blocks > 0 else 0,
        }

    async def _group_by_miner(self, missed_blocks: list[dict]) -> dict[str, list]:
        """Group missed blocks by miner address.

        Args:
            missed_blocks: List of missed MEV blocks

        Returns:
            Dictionary mapping miner address to list of blocks
        """
        by_miner: dict[str, list] = {}
        for block in missed_blocks:
            miner = block["miner"]
            if miner not in by_miner:
                by_miner[miner] = []
            by_miner[miner].append(block)
        return by_miner

    def _display_stats_table(self, stats: dict) -> None:
        """Display block statistics in a table.

        Args:
            stats: Statistics dictionary
        """
        table = Table(title="Block Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="yellow")
        table.add_column("Percentage", justify="right", style="green")

        table.add_row(
            "Total Blocks",
            f"{stats['total_blocks']:,}",
            "100.0%",
        )
        table.add_row(
            "Relay Blocks (MEV)",
            f"{stats['relay_blocks']:,}",
            f"{stats['relay_pct']:.1f}%",
        )
        table.add_row(
            "Vanilla Blocks",
            f"{stats['vanilla_blocks']:,}",
            f"{stats['vanilla_pct']:.1f}%",
        )

        self.console.print(table)

    def _display_missed_mev_table(self, missed_blocks: list[dict]) -> None:
        """Display missed MEV blocks in a table.

        Args:
            missed_blocks: List of missed MEV blocks
        """
        if not missed_blocks:
            self.console.print("\n[green]✓ No missed MEV blocks detected[/green]")
            return

        table = Table(title="Potentially Missed MEV Blocks")
        table.add_column("Block", style="cyan")
        table.add_column("Timestamp", style="magenta")
        table.add_column("Miner", style="yellow")
        table.add_column("MEV Indicators", style="red")

        # Show first 50 blocks
        for block in missed_blocks[:50]:
            table.add_row(
                str(block["block_number"]),
                block["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                block["miner"][:20] + "..." if len(block["miner"]) > 20 else block["miner"],
                ", ".join(block["indicators"][:2]),  # Show first 2 indicators
            )

        self.console.print(table)

        if len(missed_blocks) > 50:
            self.console.print(
                f"\n[yellow]Showing first 50 of {len(missed_blocks)} total[/yellow]"
            )

    def _display_miner_summary(self, by_miner: dict[str, list]) -> None:
        """Display summary grouped by miner.

        Args:
            by_miner: Dictionary mapping miner to list of blocks
        """
        table = Table(title="Missed MEV Blocks by Miner")
        table.add_column("Miner", style="cyan")
        table.add_column("Count", justify="right", style="yellow")
        table.add_column("Slot Range", style="magenta")

        # Sort by count descending
        sorted_miners = sorted(
            by_miner.items(), key=lambda x: len(x[1]), reverse=True
        )

        for miner, blocks in sorted_miners:
            count = len(blocks)
            min_block = min(b["block_number"] for b in blocks)
            max_block = max(b["block_number"] for b in blocks)

            table.add_row(
                miner[:20] + "..." if len(miner) > 20 else miner,
                f"{count:,}",
                f"{min_block:,} - {max_block:,}",
            )

        self.console.print(table)

    def _save_to_json(self, missed_blocks: list[dict]) -> None:
        """Save missed block numbers to JSON file.

        Args:
            missed_blocks: List of missed MEV blocks
        """
        output_path = Path(self.output_file)

        # Extract just the block numbers, sorted
        block_numbers = sorted([block["block_number"] for block in missed_blocks])

        # Create output data
        output_data = {
            "date_range": {
                "start": self.start_date.isoformat(),
                "end": self.end_date.isoformat(),
            },
            "total_missed_blocks": len(block_numbers),
            "block_numbers": block_numbers,
            "details": [
                {
                    "block_number": block["block_number"],
                    "timestamp": block["timestamp"].isoformat(),
                    "miner": block["miner"],
                    "indicators": block["indicators"],
                }
                for block in missed_blocks
            ],
        }

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2)

        self.console.print(f"\n[green]✓ Saved results to {output_path}[/green]")

    async def analyze(self) -> dict:
        """Run the analysis and return results.

        Returns:
            Dictionary with analysis results
        """
        self.console.print("[bold blue]Missed MEV Block Detector[/bold blue]\n")
        self.console.print(f"[cyan]Date range: {self.start_date} to {self.end_date}[/cyan]\n")

        async with AsyncSessionLocal() as session:
            # Build list of known MEV builders from database
            self.known_mev_builders = await self._get_known_mev_builders(session)
            self.console.print(
                f"[cyan]Identified {len(self.known_mev_builders)} unique MEV builder addresses[/cyan]\n"
            )

            # Get statistics
            stats = await self._get_vanilla_block_stats(session)
            self._display_stats_table(stats)

            # Find missed MEV blocks
            missed_blocks = await self._get_vanilla_blocks_with_mev_characteristics(
                session
            )

            if missed_blocks:
                self.console.print(
                    f"\n[bold red]⚠ Found {len(missed_blocks)} potentially missed MEV blocks[/bold red]\n"
                )

                # Display detailed table
                self._display_missed_mev_table(missed_blocks)

                # Group by miner
                by_miner = await self._group_by_miner(missed_blocks)
                self.console.print()
                self._display_miner_summary(by_miner)

                # Save to JSON
                self._save_to_json(missed_blocks)

                # Show recommendations
                self.console.print("\n[bold yellow]Recommendations:[/bold yellow]")
                self.console.print("1. Review the slot ranges above for potential relay data gaps")
                self.console.print("2. Run retry_gaps.py to backfill missing relay data")
                self.console.print("3. Check if these builders are properly configured in relays")
                self.console.print(f"4. Review {self.output_file} for detailed block list")
            else:
                self.console.print("\n[green]✓ No missed MEV blocks detected[/green]")
                # Still save empty result to JSON
                self._save_to_json([])

            return {
                "stats": stats,
                "missed_blocks": missed_blocks,
                "total_missed": len(missed_blocks),
            }


async def main():
    """Main entry point."""
    import sys

    # Parse command line arguments
    start_date = None
    end_date = None

    if len(sys.argv) > 1:
        start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    if len(sys.argv) > 2:
        end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")

    detector = MissedMEVDetector(start_date=start_date, end_date=end_date)
    await detector.analyze()


if __name__ == "__main__":
    run(main())
