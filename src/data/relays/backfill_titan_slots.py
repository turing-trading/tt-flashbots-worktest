"""Backfill specific date range for ALL relays using bidarchive CSV data.

This script downloads historical bid data from bidarchive.relayscan.io,
keeps max value per slot for each relay, and inserts into database.

Target date range:
- From: 2025-10-15 16:27:26
- To: 2025-11-02 23:13:12
"""

import zipfile
from asyncio import run
from datetime import datetime, timedelta
from io import BytesIO

import httpx
import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger

# Beacon chain constants
BEACON_GENESIS_TIMESTAMP = 1606824023  # December 1, 2020, 12:00:23 UTC
SECONDS_PER_SLOT = 12


def _datetime_to_slot(dt: datetime) -> int:
    """Convert datetime to slot number."""
    timestamp = int(dt.timestamp())
    return (timestamp - BEACON_GENESIS_TIMESTAMP) // SECONDS_PER_SLOT


# Calculate slot ranges from date range
# From: 2025-10-15 16:27:26 To: 2025-11-02 23:13:12

FROM_DATE = datetime(2025, 10, 10, 20, 28, 28)
TO_DATE = datetime(2025, 11, 7, 13, 30, 57)
FROM_SLOT = _datetime_to_slot(FROM_DATE)
TO_SLOT = _datetime_to_slot(TO_DATE)
ESTIMATED_SLOTS = TO_SLOT - FROM_SLOT + 1

# Specific slot ranges to backfill
SLOT_RANGES = [
    {"from_slot": FROM_SLOT, "to_slot": TO_SLOT, "estimated": ESTIMATED_SLOTS},
]


class AllRelaysBackfill:
    """Backfill specific slot ranges for all relays using CSV archives."""

    def __init__(self):
        """Initialize backfill."""
        self.logger = get_logger("all_relays_backfill", log_level="INFO")
        self.console = Console()

    def _slot_to_datetime(self, slot: int) -> datetime:
        """Convert slot number to datetime.

        Args:
            slot: Slot number

        Returns:
            Datetime corresponding to the slot
        """
        timestamp = BEACON_GENESIS_TIMESTAMP + (slot * SECONDS_PER_SLOT)
        return datetime.fromtimestamp(timestamp)

    def _get_date_range(self, from_slot: int, to_slot: int) -> list[datetime]:
        """Get list of dates covering the slot range.

        Args:
            from_slot: Start slot
            to_slot: End slot

        Returns:
            List of dates (one per day)
        """
        start_date = self._slot_to_datetime(from_slot).date()
        end_date = self._slot_to_datetime(to_slot).date()

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        return dates

    async def _download_csv_archive(
        self, client: httpx.AsyncClient, date: datetime
    ) -> bytes | None:
        """Download CSV archive for a specific date.

        Args:
            client: HTTP client
            date: Date to download

        Returns:
            Zip file bytes or None if not found
        """
        # URL pattern: https://bidarchive.relayscan.io/ethereum/mainnet/2025-06/2025-06-06_top.csv.zip
        year_month = date.strftime("%Y-%m")
        date_str = date.strftime("%Y-%m-%d")
        url = f"https://bidarchive.relayscan.io/ethereum/mainnet/{year_month}/{date_str}_top.csv.zip"

        try:
            response = await client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.debug(f"No archive found for {date_str}")
                return None
            self.logger.warning(f"HTTP error downloading {date_str}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error downloading archive for {date_str}: {e}")
            return None

    def _parse_csv_from_zip(self, zip_bytes: bytes) -> pd.DataFrame:
        """Parse CSV data from zip file using pandas.

        Args:
            zip_bytes: Zip file bytes

        Returns:
            DataFrame with CSV data
        """
        try:
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
                # Get first CSV file in archive
                csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                if not csv_files:
                    self.logger.warning("No CSV file found in archive")
                    return pd.DataFrame()

                with zf.open(csv_files[0]) as csv_file:
                    # Read CSV directly with pandas, disable low_memory to avoid dtype warnings
                    df = pd.read_csv(csv_file, low_memory=False)
                    return df

        except Exception as e:
            self.logger.error(f"Error parsing CSV from zip: {e}")
            return pd.DataFrame()

    def _filter_and_aggregate(
        self, df: pd.DataFrame, from_slot: int, to_slot: int
    ) -> list[tuple[RelaysPayloads, str]]:
        """Filter and aggregate DataFrame, convert to Pydantic models.

        Args:
            df: DataFrame with CSV data
            from_slot: Start slot
            to_slot: End slot

        Returns:
            List of RelaysPayloads Pydantic models
        """
        if df.empty:
            return []

        # Convert numeric columns to proper types
        df["slot"] = pd.to_numeric(df["slot"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce")

        # Drop rows with NaN in critical columns
        df = df.dropna(subset=["slot", "value", "block_number"])  # type: ignore[assignment]

        # Filter for slot range
        df = df[(df["slot"] >= from_slot) & (df["slot"] <= to_slot)].copy()  # type: ignore[assignment]

        # Filter out zero/null values
        df = df[df["value"] > 0]  # type: ignore[assignment]

        # Filter out zero/null block numbers
        df = df[df["block_number"] > 0]  # type: ignore[assignment]

        # Filter out missing relay names
        df = df[df["relay"].notna()]  # type: ignore[assignment]

        # Rename relay.ultrasound.money to relay-analytics.ultrasound.money (new canonical name)
        df.loc[df["relay"] == "relay.ultrasound.money", "relay"] = (
            "relay-analytics.ultrasound.money"
        )

        # Filter for titanrelay.xyz and bloxroute.max-profit.blxrbdn.com only
        df = df[
            (df["relay"].str.contains("titan", case=False, na=False))
            # | (df["relay"] == "bloxroute.max-profit.blxrbdn.com")
        ]  # type: ignore[assignment]

        # Filter out missing proposer_fee_recipient
        # df = df[df["proposer_fee_recipient"].notna()]  # type: ignore[assignment]

        if df.empty:
            return []

        # Deduplicate by (relay, slot) keeping max value to avoid primary key conflicts
        df = df.loc[df.groupby(["relay", "slot"])["value"].idxmax()]  # type: ignore[assignment]

        # Convert to Pydantic models with relay info
        payloads = []
        for _, row in df.iterrows():
            try:
                payload = RelaysPayloads(
                    slot=int(row["slot"]),
                    parent_hash=str(row.get("parent_hash", "")),
                    block_hash=str(row.get("block_hash", "")),
                    builder_pubkey=str(row.get("builder_pubkey", "")),
                    proposer_pubkey=str(row.get("proposer_pubkey", "")),
                    proposer_fee_recipient=str(row.get("proposer_fee_recipient", "")),
                    gas_limit=0,  # Not available in bidarchive CSV
                    gas_used=0,  # Not available in bidarchive CSV
                    value=int(row["value"]),
                    block_number=int(row["block_number"]),
                    num_tx=0,  # Not available in bidarchive CSV
                )
                # Store relay separately as tuple
                payloads.append((payload, str(row["relay"])))
            except (ValueError, KeyError) as e:
                self.logger.debug(f"Skipping invalid row: {e}")
                continue

        return payloads

    async def _store_payloads(
        self, session: AsyncSession, payloads: list[tuple[RelaysPayloads, str]]
    ) -> int:
        """Store Pydantic payloads in database with batching.

        Args:
            session: Database session
            payloads: List of (RelaysPayloads, relay_name) tuples

        Returns:
            Number of payloads stored
        """
        if not payloads:
            return 0

        # Convert Pydantic models to dicts and add relay
        values = []
        for payload, relay in payloads:
            payload_dict = payload.model_dump()
            payload_dict["relay"] = relay
            values.append(payload_dict)

        if not values:
            return 0

        # Batch inserts to avoid PostgreSQL parameter limit (65535)
        # Each row has ~12 fields, so batch size of 5000 = ~60000 parameters
        batch_size = 5000
        total_stored = 0

        for i in range(0, len(values), batch_size):
            batch = values[i : i + batch_size]

            stmt = pg_insert(RelaysPayloadsDB).values(batch)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=["slot", "relay"],
                set_={
                    RelaysPayloadsDB.parent_hash: excluded.parent_hash,
                    RelaysPayloadsDB.block_hash: excluded.block_hash,
                    RelaysPayloadsDB.builder_pubkey: excluded.builder_pubkey,
                    RelaysPayloadsDB.proposer_pubkey: excluded.proposer_pubkey,
                    RelaysPayloadsDB.proposer_fee_recipient: excluded.proposer_fee_recipient,
                    RelaysPayloadsDB.gas_limit: excluded.gas_limit,
                    RelaysPayloadsDB.gas_used: excluded.gas_used,
                    RelaysPayloadsDB.value: excluded.value,
                    RelaysPayloadsDB.block_number: excluded.block_number,
                    RelaysPayloadsDB.num_tx: excluded.num_tx,
                },
                where=excluded.value
                > RelaysPayloadsDB.value,  # Only update if new value is greater
            )

            await session.execute(stmt)
            await session.commit()
            total_stored += len(batch)

        return total_stored

    async def _backfill_range(
        self, session: AsyncSession, client: httpx.AsyncClient, slot_range: dict
    ) -> dict:
        """Backfill a single slot range using CSV archives.

        Args:
            session: Database session
            client: HTTP client
            slot_range: Dictionary with from_slot, to_slot, estimated

        Returns:
            Dictionary with results
        """
        from_slot = slot_range["from_slot"]
        to_slot = slot_range["to_slot"]
        estimated = slot_range["estimated"]

        self.console.print(
            f"\n[bold blue]Backfilling slots {from_slot:,} → {to_slot:,}[/bold blue]"
        )

        # Get date range covering the slot range
        dates = self._get_date_range(from_slot, to_slot)
        self.console.print(
            f"[cyan]Downloading CSV archives for {len(dates)} dates[/cyan]\n"
        )

        total_fetched = 0

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
        )

        with progress:
            task_id = progress.add_task(
                "Processing CSV archives",
                total=len(dates),
            )

            for date in dates:
                # Download CSV archive
                zip_bytes = await self._download_csv_archive(client, date)

                if zip_bytes:
                    # Parse CSV from zip
                    df = self._parse_csv_from_zip(zip_bytes)

                    # Filter and aggregate (keeps max value per block for this CSV)
                    payloads = self._filter_and_aggregate(df, from_slot, to_slot)

                    # Upsert after each CSV
                    if payloads:
                        stored = await self._store_payloads(session, payloads)
                        total_fetched += stored
                        self.logger.info(
                            f"Date {date}: found {len(payloads)} relay payloads, stored {stored}"
                        )
                    else:
                        self.logger.info(f"Date {date}: found 0 relay payloads")

                progress.update(task_id, advance=1)

            if total_fetched > 0:
                self.console.print(
                    f"[green]Stored {total_fetched} total relay payloads[/green]"
                )

        return {
            "from_slot": from_slot,
            "to_slot": to_slot,
            "estimated": estimated,
            "fetched": total_fetched,
            "dates_processed": len(dates),
            "coverage_pct": (total_fetched / estimated * 100) if estimated > 0 else 0,
        }

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(self) -> None:
        """Run backfill for all slot ranges."""
        self.console.print("[bold blue]All Relays Backfill[/bold blue]")
        self.console.print(f"[cyan]Ranges: {len(SLOT_RANGES)}[/cyan]\n")

        await self.create_tables()

        results = []

        async with AsyncSessionLocal() as session:
            async with httpx.AsyncClient(timeout=120.0) as client:
                for slot_range in SLOT_RANGES:
                    result = await self._backfill_range(session, client, slot_range)
                    results.append(result)

        # Display summary
        self.console.print("\n[bold green]Backfill Complete[/bold green]\n")

        from rich.table import Table

        table = Table(title="Results")
        table.add_column("Slot Range", style="cyan")
        table.add_column("Estimated", justify="right", style="yellow")
        table.add_column("Fetched", justify="right", style="green")
        table.add_column("Coverage", justify="right", style="magenta")
        table.add_column("Dates", justify="right", style="blue")

        total_estimated = 0
        total_fetched = 0

        for result in results:
            table.add_row(
                f"{result['from_slot']:,}-{result['to_slot']:,}",
                f"{result['estimated']:,}",
                f"{result['fetched']:,}",
                f"{result['coverage_pct']:.1f}%",
                f"{result['dates_processed']:,}",
            )
            total_estimated += result["estimated"]
            total_fetched += result["fetched"]

        self.console.print(table)

        overall_coverage = (
            (total_fetched / total_estimated * 100) if total_estimated > 0 else 0
        )
        self.console.print(
            f"\n[bold]Overall: {total_fetched:,}/{total_estimated:,} "
            f"({overall_coverage:.1f}% coverage)[/bold]"
        )


async def main():
    """Main entry point."""
    backfill = AllRelaysBackfill()
    await backfill.run()


if __name__ == "__main__":
    run(main())
