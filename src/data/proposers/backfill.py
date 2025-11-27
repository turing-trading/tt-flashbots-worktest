"""Backfill proposer mapping data from OpenEthData parquet file."""

from typing import TYPE_CHECKING, BinaryIO, Protocol, cast

from asyncio import run

import pandas as pd

import httpx
from rich.console import Console

from src.data.proposers.db import ProposerMappingDB
from src.data.proposers.models import ProposerMapping
from src.helpers.backfill import BackfillBase
from src.helpers.db import AsyncSessionLocal, upsert_models
from src.helpers.progress import create_standard_progress


if TYPE_CHECKING:
    from pandas import DataFrame


class _ReadParquetProtocol(Protocol):
    """Protocol for pandas.read_parquet with properly typed signature."""

    def __call__(self, source: BinaryIO) -> DataFrame: ...


_READ_PARQUET_ATTR: str = "read_parquet"


def _read_parquet(source: BinaryIO) -> pd.DataFrame:
    """Typed wrapper for pandas.read_parquet.

    Uses getattr with a variable attribute name to avoid:
    - pyright reportUnknownMemberType (pandas-stubs has Unknown in filters param)
    - ruff B009 (disallows getattr with constant attribute)
    """
    read_func = cast("_ReadParquetProtocol", getattr(pd, _READ_PARQUET_ATTR))
    return read_func(source)


PARQUET_URL = "https://storage.googleapis.com/public_eth_data/openethdata/validator_data.parquet.gzip"


class BackfillProposerMapping(BackfillBase):
    """Backfill proposer fee recipient to label mapping."""

    def __init__(self) -> None:
        """Initialize backfill."""
        super().__init__(batch_size=1000)
        self.console = Console()

    async def _download_parquet(self) -> pd.DataFrame:
        """Download and parse the validator parquet file."""
        self.console.print("[cyan]Downloading validator parquet file...[/cyan]")

        async with httpx.AsyncClient() as client:
            response = await client.get(
                PARQUET_URL, timeout=120.0, follow_redirects=True
            )
            response.raise_for_status()

        # Read parquet from bytes
        import io

        df = _read_parquet(io.BytesIO(response.content))
        self.console.print(f"[green]Downloaded {len(df):,} validator records[/green]")
        return df

    async def _get_fee_recipients_for_pubkeys(
        self, pubkeys: list[str]
    ) -> dict[str, str]:
        """Query database to get fee recipients for pubkeys from relays_payloads."""
        async with AsyncSessionLocal() as session:
            # Use raw SQL for efficiency with large IN clause
            from sqlalchemy import text

            # Process in chunks to avoid huge IN clauses
            chunk_size = 10000
            fee_recipients: dict[str, str] = {}

            for i in range(0, len(pubkeys), chunk_size):
                chunk = pubkeys[i : i + chunk_size]
                placeholders = ", ".join([f":p{j}" for j in range(len(chunk))])
                query = text(
                    f"""
                    SELECT DISTINCT proposer_pubkey, proposer_fee_recipient
                    FROM relays_payloads
                    WHERE proposer_pubkey IN ({placeholders})
                    """  # noqa: S608 - placeholders are parameterized
                )
                params = {f"p{j}": pk for j, pk in enumerate(chunk)}
                result = await session.execute(query, params)
                for row in result:
                    fee_recipients[row[0]] = row[1]

            return fee_recipients

    async def run(self) -> None:
        """Run the backfill process."""
        self.console.print("[bold blue]Backfilling proposer mapping...[/bold blue]\n")

        # Create table if it doesn't exist
        await self.create_tables()

        # Download parquet
        df = await self._download_parquet()

        # Filter to rows with labels
        df_labeled = df[df["label"].notna() & (df["label"] != "")]
        self.console.print(
            f"[cyan]Found {len(df_labeled):,} validators with labels[/cyan]"
        )

        # Get unique pubkeys
        pubkeys = df_labeled["pubkey"].unique().tolist()
        self.console.print(
            f"[cyan]Looking up fee recipients for {len(pubkeys):,} pubkeys...[/cyan]"
        )

        # Get fee recipients from database
        fee_recipients = await self._get_fee_recipients_for_pubkeys(pubkeys)
        self.console.print(
            f"[green]Found fee recipients for {len(fee_recipients):,} pubkeys[/green]"
        )

        # Create mapping: fee_recipient -> (label, lido_node_operator)
        # Group by pubkey first, then map to fee_recipient
        mapping: dict[str, tuple[str, str | None]] = {}

        progress = create_standard_progress(console=self.console)
        with progress:
            task = progress.add_task("Processing validators", total=len(df_labeled))

            for _, row in df_labeled.iterrows():
                pubkey = row["pubkey"]
                label = row["label"]
                lido_op = (
                    row["lido_node_operator"]
                    if pd.notna(row["lido_node_operator"])
                    else None
                )

                if pubkey in fee_recipients:
                    fee_recipient = fee_recipients[pubkey]
                    # Normalize fee_recipient to lowercase for consistency
                    fee_recipient_lower = fee_recipient.lower()

                    # Keep the first label we find for each fee_recipient
                    # (or update if this one has more info like lido_node_operator)
                    if fee_recipient_lower not in mapping:
                        mapping[fee_recipient_lower] = (label, lido_op)
                    elif lido_op and not mapping[fee_recipient_lower][1]:
                        # Update if we have lido_node_operator info and existing doesn't
                        mapping[fee_recipient_lower] = (label, lido_op)

                progress.advance(task)

        self.console.print(
            f"\n[green]Created {len(mapping):,} unique fee_recipient -> label "
            "mappings[/green]"
        )

        # Convert to ProposerMapping models
        models = [
            ProposerMapping(
                proposer_fee_recipient=fee_recipient,
                label=label,
                lido_node_operator=lido_op,
            )
            for fee_recipient, (label, lido_op) in mapping.items()
        ]

        # Upsert to database in batches
        self.console.print("[cyan]Upserting to database...[/cyan]")
        for i in range(0, len(models), self.batch_size):
            batch = models[i : i + self.batch_size]
            await upsert_models(
                db_model_class=ProposerMappingDB,
                pydantic_models=batch,
            )

        self.console.print(
            f"[bold green]✓ Backfill complete: {len(models):,} "
            "mappings stored[/bold green]"
        )


if __name__ == "__main__":
    backfill = BackfillProposerMapping()
    run(backfill.run())
