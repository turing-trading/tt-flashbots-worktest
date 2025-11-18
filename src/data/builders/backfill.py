"""Backfill builder identifiers from block extra_data."""

import binascii
from asyncio import run

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderIdentifiersCheckpoints, BuilderIdentifiersDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger


class BackfillBuilderIdentifiers:
    """Backfill builder identifiers from block extra_data."""

    def __init__(self, batch_size: int = 100_000):
        """Initialize backfill.

        Args:
            batch_size: Number of blocks to process per batch
        """
        self.batch_size = batch_size
        self.logger = get_logger("backfill_builder_identifiers", log_level="INFO")
        self.console = Console()

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def _parse_builder_name(self, extra_data: str) -> str:
        """Parse builder name from extra_data hex string.

        Args:
            extra_data: Hex string of extra_data from block

        Returns:
            Parsed builder name (ASCII string) or 'unknown' if unparseable
        """
        if not extra_data:
            return "unknown"

        # Remove '0x' prefix if present
        hex_str = extra_data[2:] if extra_data.startswith("0x") else extra_data

        try:
            # Convert hex to bytes
            bytes_data = binascii.unhexlify(hex_str)
            # Decode as UTF-8, strip null bytes
            builder_name = bytes_data.decode("utf-8", errors="ignore").strip("\x00")

            # Clean up the builder name
            builder_name = self._clean_builder_name(builder_name)

            # Return cleaned string or 'unknown' if empty
            return builder_name if builder_name else "unknown"
        except Exception:
            # If parsing fails, return 'unknown'
            return "unknown"

    def _clean_builder_name(self, name: str) -> str:
        """Clean builder name by removing emojis and extracting domain/pool names.

        Args:
            name: Raw builder name string

        Returns:
            Cleaned builder name
        """
        import re

        # Remove emojis and other non-ASCII characters (keep only printable ASCII)
        # This removes characters like ✨, 🚀, etc.
        cleaned = "".join(c for c in name if ord(c) < 128 and c.isprintable())

        # Strip whitespace
        cleaned = cleaned.strip()

        # First, try to extract content from parentheses (e.g., "Quasar (quasar.win)" -> "quasar.win")
        paren_match = re.search(r"\(([^)]+)\)", cleaned)
        if paren_match:
            cleaned = paren_match.group(1)

        # Extract domain/pool names from slash-separated patterns like "EU2/pool.binance.com/"
        # Take the part after the last "/" that contains a domain or name
        if "/" in cleaned:
            # Split by "/" and get the last non-empty part
            parts = [p for p in cleaned.split("/") if p]
            if parts:
                # Take the last part which should be the domain/pool name
                cleaned = parts[-1]

        # For domain-like strings (containing dots), extract just the domain part
        # This handles "poolin.com!c" -> "poolin.com", "BTC.com#" -> "BTC.com", "poolin.comHN" -> "poolin.com"
        if "." in cleaned:
            # Match common TLDs precisely - the pattern stops at the TLD
            # No \b needed because we just want to match up to and including the TLD
            tld_pattern = r"([a-zA-Z0-9]+(?:[._-][a-zA-Z0-9]+)*\.(?:com|net|org|io|win|xyz|eth|pool|info|co|uk|de|fr|cn|jp))"
            domain_match = re.match(tld_pattern, cleaned)
            if domain_match:
                cleaned = domain_match.group(1)

        # Remove trailing and leading special characters (but keep dots, hyphens, underscores in the middle)
        cleaned = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9.]+$", "", cleaned)

        # Remove trailing numbers and mixed alphanumeric suffixes (e.g., "speth22" -> "speth", "speth03e" -> "speth")
        cleaned = re.sub(r"[0-9]+[a-z0-9]*$", "", cleaned)

        # Final cleanup: strip any remaining whitespace
        cleaned = cleaned.strip()

        # If the result is a single character or empty, return "unknown"
        if len(cleaned) <= 1:
            return "unknown"

        return cleaned

    async def _get_checkpoint(self, session: AsyncSession) -> tuple[int, int] | None:
        """Get the latest checkpoint.

        Returns:
            Tuple of (from_block, to_block) or None if no checkpoint exists
        """
        stmt = select(BuilderIdentifiersCheckpoints).where(
            BuilderIdentifiersCheckpoints.id == 1
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()

        if checkpoint:
            # Type ignore: SQLAlchemy Column objects are runtime values
            return (int(checkpoint.from_block), int(checkpoint.to_block))  # type: ignore
        return None

    async def _update_checkpoint(
        self, session: AsyncSession, from_block: int, to_block: int
    ) -> None:
        """Update or create checkpoint.

        Args:
            session: Database session
            from_block: Starting block number
            to_block: Ending block number
        """
        stmt = pg_insert(BuilderIdentifiersCheckpoints).values(
            id=1, from_block=from_block, to_block=to_block
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "from_block": stmt.excluded.from_block,
                "to_block": stmt.excluded.to_block,
            },
        )

        await session.execute(stmt)
        await session.commit()

    async def _get_block_range(self, session: AsyncSession) -> tuple[int, int]:
        """Get the min and max block numbers from relays_payloads table.

        Returns:
            Tuple of (min_block, max_block)
        """
        stmt = select(
            func.min(RelaysPayloadsDB.block_number),
            func.max(RelaysPayloadsDB.block_number),
        )
        result = await session.execute(stmt)
        min_block, max_block = result.one()
        return (min_block or 0, max_block or 0)

    async def _process_batch(
        self,
        session: AsyncSession,
        start_block: int,
        end_block: int,
    ) -> int:
        """Process a batch of blocks and extract builder identifiers.

        Args:
            session: Database session
            start_block: Starting block number (inclusive)
            end_block: Ending block number (inclusive)

        Returns:
            Number of unique builder identifiers found
        """
        # Join relays_payloads with blocks to get builder_pubkey and extra_data
        stmt = (
            select(
                RelaysPayloadsDB.builder_pubkey,
                BlockDB.extra_data,
            )
            .select_from(RelaysPayloadsDB)
            .join(BlockDB, RelaysPayloadsDB.block_number == BlockDB.number)
            .where(RelaysPayloadsDB.block_number >= start_block)
            .where(RelaysPayloadsDB.block_number <= end_block)
            .where(RelaysPayloadsDB.builder_pubkey.isnot(None))
            .where(BlockDB.extra_data.isnot(None))
            .where(BlockDB.extra_data != "")
            .distinct()
        )

        result = await session.execute(stmt)
        rows = result.fetchall()

        if not rows:
            return 0

        # Parse builder names and prepare data
        # Use a dict to deduplicate by builder_pubkey (keep the first occurrence)
        identifiers_dict = {}
        for builder_pubkey, extra_data in rows:
            # Skip if we've already processed this builder_pubkey
            if builder_pubkey in identifiers_dict:
                continue

            # Replace null bytes in extra_data (PostgreSQL doesn't support them in text fields)
            cleaned_extra_data = extra_data.replace("\x00", "")

            # Skip if empty after cleaning
            if not cleaned_extra_data or cleaned_extra_data == "0x":
                continue

            # Parse builder name from cleaned data and also strip null bytes
            builder_name = self._parse_builder_name(cleaned_extra_data).replace(
                "\x00", ""
            )

            # Skip if builder_name is empty after cleaning
            if not builder_name:
                continue

            identifiers_dict[builder_pubkey] = builder_name

        # Convert dict to list of dicts for insertion
        identifiers_data = [
            {"builder_pubkey": pubkey, "builder_name": name}
            for pubkey, name in identifiers_dict.items()
        ]

        # Upsert into database
        if identifiers_data:
            stmt = pg_insert(BuilderIdentifiersDB).values(identifiers_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["builder_pubkey"],
                set_={"builder_name": stmt.excluded.builder_name},
            )
            await session.execute(stmt)
            await session.commit()

        return len(identifiers_data)

    async def run(self) -> None:
        """Run the backfill process."""
        self.logger.info("Creating tables if they don't exist...")
        await self.create_tables()

        async with AsyncSessionLocal() as session:
            # Get block range from blocks table
            min_block, max_block = await self._get_block_range(session)
            self.logger.info(f"Blocks range: {min_block:,} to {max_block:,}")

            # Get checkpoint
            checkpoint = await self._get_checkpoint(session)

            if checkpoint is None:
                # No checkpoint, start from beginning
                from_block = min_block
                to_block = min_block
            else:
                from_block, to_block = checkpoint

            # Determine what needs to be processed
            if to_block >= max_block:
                self.console.print(
                    "[yellow]All blocks processed - builders_identifiers is up to date[/yellow]"
                )
                return

            # Calculate total blocks to process
            total_blocks = max_block - to_block

            self.console.print("[bold blue]Backfilling Builder Identifiers[/bold blue]")
            self.console.print(
                f"[cyan]Processing blocks {to_block + 1:,} to {max_block:,}[/cyan]"
            )
            self.console.print(f"[cyan]Total blocks: {total_blocks:,}[/cyan]\n")

            # Create progress display
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                console=self.console,
            )

            total_identifiers = 0
            current_block = to_block + 1

            with progress:
                task_id = progress.add_task("Processing blocks", total=total_blocks)

                while current_block <= max_block:
                    batch_end = min(current_block + self.batch_size - 1, max_block)

                    # Process this batch
                    identifiers_found = await self._process_batch(
                        session, current_block, batch_end
                    )
                    total_identifiers += identifiers_found

                    # Update checkpoint
                    await self._update_checkpoint(session, from_block, batch_end)

                    # Update progress
                    blocks_processed = batch_end - current_block + 1
                    progress.update(
                        task_id,
                        advance=blocks_processed,
                        description=f"Processing blocks (found {total_identifiers:,} builders)",
                    )

                    current_block = batch_end + 1

            self.console.print(
                f"\n[bold green]✓ Backfill completed[/bold green] - "
                f"Found {total_identifiers:,} unique builder identifiers"
            )


if __name__ == "__main__":
    backfill = BackfillBuilderIdentifiers(batch_size=100_000)
    run(backfill.run())
