"""Backfill Ethereum block data from AWS Public Blockchain Dataset."""

import io
from asyncio import run
from datetime import datetime, timedelta

import httpx
import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockCheckpoints, BlockDB
from src.data.blocks.models import Block
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger


class BackfillBlocks:
    """Backfill Ethereum blocks from AWS S3."""

    def __init__(
        self,
        start_date: str = "2015-07-30",
        end_date: str | None = None,
        batch_size: int = 1000,
    ):
        """Initialize backfill.

        Args:
            start_date: Start date in YYYY-MM-DD format (default: genesis)
            end_date: End date in YYYY-MM-DD format (default: today)
            batch_size: Number of blocks to insert per batch
        """
        self.base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if end_date
            else datetime.now().date()
        )
        self.batch_size = batch_size
        self.logger = get_logger("backfill_blocks", log_level="INFO")
        self.console = Console()

    async def _get_completed_dates(self, session: AsyncSession) -> set[str]:
        """Get all dates that have been successfully processed.

        Returns:
            Set of date strings in YYYY-MM-DD format
        """
        stmt = select(BlockCheckpoints.date)
        result = await session.execute(stmt)
        completed_dates = {row[0] for row in result.fetchall()}
        return completed_dates

    async def _add_checkpoint(
        self, session: AsyncSession, date: str, block_count: int
    ) -> None:
        """Add a checkpoint for a successfully processed date.

        Args:
            session: Database session
            date: Date string in YYYY-MM-DD format
            block_count: Number of blocks processed for this date
        """
        # Use upsert to handle re-processing of dates
        stmt = pg_insert(BlockCheckpoints).values(date=date, block_count=block_count)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={"block_count": stmt.excluded.block_count},
        )
        await session.execute(stmt)
        await session.commit()

    async def _fetch_parquet(
        self, client: httpx.AsyncClient, date: str
    ) -> pd.DataFrame | None:
        """Fetch parquet file for a given date from S3.

        Args:
            client: HTTP client
            date: Date string in YYYY-MM-DD format

        Returns:
            DataFrame with block data or None if not found
        """
        # Construct S3 URL for the date partition
        # Format: v1.0/eth/blocks/date=YYYY-MM-DD/
        s3_key = f"v1.0/eth/blocks/date={date}/"

        try:
            # First, list objects in this partition to get the parquet filename
            list_url = f"{self.base_url}/?prefix={s3_key}"
            response = await client.get(list_url, timeout=30.0)
            response.raise_for_status()

            # Parse XML response to extract Key
            import xml.etree.ElementTree as ET

            root = ET.fromstring(response.text)
            namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            keys = root.findall(".//s3:Key", namespace)

            if not keys:
                self.logger.warning(f"No data found for date {date}")
                return None

            # Get the first (and usually only) parquet file
            parquet_key = keys[0].text
            if not parquet_key:
                return None

            # Download the parquet file
            parquet_url = f"{self.base_url}/{parquet_key}"
            self.logger.debug(f"Fetching {parquet_url}")

            response = await client.get(parquet_url, timeout=60.0)
            response.raise_for_status()

            # Read parquet from bytes
            df = pd.read_parquet(io.BytesIO(response.content))
            self.logger.debug(f"Fetched {len(df)} blocks for date {date}")

            return df

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"No data available for date {date}")
            else:
                self.logger.error(f"HTTP {e.response.status_code} for date {date}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching data for date {date}: {e}")
            return None

    def _dataframe_to_blocks(self, df: pd.DataFrame) -> list[Block]:
        """Convert DataFrame to list of Block Pydantic models."""
        blocks = []
        for _, row in df.iterrows():
            # Handle timestamp conversion
            timestamp_val = row["timestamp"]
            if isinstance(timestamp_val, pd.Timestamp):
                timestamp = timestamp_val.to_pydatetime()
            elif hasattr(timestamp_val, "to_pydatetime"):
                timestamp = timestamp_val.to_pydatetime()  # type: ignore[union-attr]
            else:
                timestamp = pd.Timestamp(timestamp_val).to_pydatetime()  # type: ignore[arg-type]

            # Handle base_fee_per_gas which might be NaN
            base_fee_val = row["base_fee_per_gas"]
            try:
                base_fee_per_gas = (
                    None
                    if pd.isna(base_fee_val)  # type: ignore[arg-type]
                    else float(base_fee_val)
                )
            except (TypeError, ValueError):
                base_fee_per_gas = None

            block = Block(
                number=int(row["number"]),
                hash=str(row["hash"]),
                parent_hash=str(row["parent_hash"]),
                nonce=str(row["nonce"]),
                sha3_uncles=str(row["sha3_uncles"]),
                transactions_root=str(row["transactions_root"]),
                state_root=str(row["state_root"]),
                receipts_root=str(row["receipts_root"]),
                miner=str(row["miner"]),
                size=int(row["size"]),
                extra_data=str(row["extra_data"]),
                gas_limit=int(row["gas_limit"]),
                gas_used=int(row["gas_used"]),
                timestamp=timestamp,
                transaction_count=int(row["transaction_count"]),
                base_fee_per_gas=base_fee_per_gas,
            )
            blocks.append(block)
        return blocks

    async def _store_blocks(self, session: AsyncSession, blocks: list[Block]) -> None:
        """Store blocks in the database using batch upsert."""
        if not blocks:
            return

        # Process in batches
        for i in range(0, len(blocks), self.batch_size):
            batch = blocks[i : i + self.batch_size]
            values = [block.model_dump() for block in batch]

            # Upsert using ON CONFLICT
            stmt = pg_insert(BlockDB).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["number"],
                set_={
                    BlockDB.hash: stmt.excluded.hash,
                    BlockDB.parent_hash: stmt.excluded.parent_hash,
                    BlockDB.nonce: stmt.excluded.nonce,
                    BlockDB.sha3_uncles: stmt.excluded.sha3_uncles,
                    BlockDB.transactions_root: stmt.excluded.transactions_root,
                    BlockDB.state_root: stmt.excluded.state_root,
                    BlockDB.receipts_root: stmt.excluded.receipts_root,
                    BlockDB.miner: stmt.excluded.miner,
                    BlockDB.size: stmt.excluded.size,
                    BlockDB.extra_data: stmt.excluded.extra_data,
                    BlockDB.gas_limit: stmt.excluded.gas_limit,
                    BlockDB.gas_used: stmt.excluded.gas_used,
                    BlockDB.timestamp: stmt.excluded.timestamp,
                    BlockDB.transaction_count: stmt.excluded.transaction_count,
                    BlockDB.base_fee_per_gas: stmt.excluded.base_fee_per_gas,
                },
            )

            await session.execute(stmt)

        await session.commit()

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _backfill_date(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        date: str,
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Backfill data for a single date.

        Returns:
            Number of blocks processed
        """
        df = await self._fetch_parquet(client, date)

        if df is None or len(df) == 0:
            progress.update(
                task_id, advance=1, description=f"{date} [yellow]⊘[/yellow] no data"
            )
            # Still add checkpoint with 0 blocks to mark as processed
            await self._add_checkpoint(session, date, 0)
            return 0

        # Convert to Pydantic models
        blocks = self._dataframe_to_blocks(df)

        # Store in database
        await self._store_blocks(session, blocks)

        # Add checkpoint for this date
        await self._add_checkpoint(session, date, len(blocks))

        progress.update(
            task_id,
            advance=1,
            description=f"{date} [green]✓[/green] {len(blocks):,} blocks",
        )

        return len(blocks)

    async def run(self) -> None:
        """Run the backfill process."""
        await self.create_tables()

        # Generate all dates in range
        all_dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            all_dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Get already completed dates
        async with AsyncSessionLocal() as session:
            completed_dates = await self._get_completed_dates(session)

        # Filter out completed dates
        dates_to_process = [date for date in all_dates if date not in completed_dates]

        if not dates_to_process:
            self.console.print(
                "[yellow]No new dates to process - all dates in range already completed[/yellow]"
            )
            return

        total_days = len(dates_to_process)
        completed_count = len(completed_dates)

        self.console.print(
            "[bold blue]Backfilling Ethereum blocks from AWS S3[/bold blue]"
        )
        self.console.print(
            f"[cyan]Date range: {self.start_date} to {self.end_date}[/cyan]"
        )
        self.console.print(f"[cyan]Total dates in range: {len(all_dates):,}[/cyan]")
        self.console.print(f"[cyan]Already completed: {completed_count:,}[/cyan]")
        self.console.print(f"[cyan]To process: {total_days:,}[/cyan]\n")

        # Create progress display
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
            expand=True,
        )

        total_blocks = 0

        with progress:
            task_id = progress.add_task("Processing dates", total=total_days)

            async with httpx.AsyncClient() as client:
                async with AsyncSessionLocal() as session:
                    for date_str in dates_to_process[::-1]:
                        blocks_count = await self._backfill_date(
                            client, session, date_str, progress, task_id
                        )
                        total_blocks += blocks_count

        # self.console.print(
        #     f"\n[bold green]✓ Backfill completed[/bold green] - "
        #     f"Processed {total_blocks:,} blocks across {total_days:,} dates"
        # )


if __name__ == "__main__":
    # Example: Backfill from genesis to today
    backfill = BackfillBlocks(
        start_date="2022-01-01",
        end_date=None,  # Until today
        batch_size=1000,
    )
    run(backfill.run())
