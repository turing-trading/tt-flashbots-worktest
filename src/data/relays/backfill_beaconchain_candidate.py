"""Backfill relays_payloads for potential MEV blocks classified as vanilla.

Finds blocks where the miner has used relays before but the specific block
is not in relays_payloads, then fetches relay data from beaconcha.in API.
"""

import os

import asyncio

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

import httpx
from rich.console import Console

from src.helpers.db import AsyncSessionLocal
from src.helpers.progress import create_standard_progress


# beaconcha.in relay tag to canonical relay name mapping
RELAY_TAG_MAPPING = {
    "relayooor-relay": "relayooor.wtf",
    "bloxroute-regulated-relay": "bloxroute.regulated.blxrbdn.com",
    "bloxroute-max-profit-relay": "bloxroute.max-profit.blxrbdn.com",
    "bloxroute-ethical-relay": "bloxroute.ethical.blxrbdn.com",
    "flashbots-relay": "boost-relay.flashbots.net",
    "ultrasound-relay": "relay-analytics.ultrasound.money",
    "agnostic-relay": "agnostic-relay.net",
    "aestus-relay": "aestus.live",
    "titan-relay": "titanrelay.xyz",
    "manifold-relay": "mainnet.manifoldfinance.com",
    "eden-relay": "relay.edennetwork.io",
    "blocknative-relay": "builder-relay-mainnet.blocknative.com",
}

# API configuration
BEACONCHAIN_API = "https://beaconcha.in/api/v1/execution/block"
BEACONCHAIN_API_KEY = os.getenv("BEACONCHAIN_API_KEY")
API_BATCH_SIZE = 100  # max blocks per API request (up to 100)
RATE_LIMIT_DELAY = 0.1  # seconds between batch requests
DB_BATCH_SIZE = 100  # commit every N payloads
MAX_RETRIES = 3  # max retries for DB operations
RETRY_DELAY = 2  # seconds between retries

console = Console()


async def get_candidate_blocks() -> list[int]:
    """Get potential MEV blocks classified as vanilla.

    Finds blocks where the miner has used relays before but the specific
    block is not in relays_payloads.

    Raises:
        OperationalError: If the database connection fails.
    """
    console.print("[cyan]Step 1: Finding miners that have used relays...[/cyan]")

    for attempt in range(MAX_RETRIES):
        try:
            async with AsyncSessionLocal() as session:
                # Get miners that have used relays
                result = await session.execute(
                    text("""
                    SELECT DISTINCT b.miner
                    FROM blocks b
                    INNER JOIN relays_payloads rp ON b.number = rp.block_number
                """)
                )
                relay_miners = [row[0] for row in result.fetchall()]
                console.print(
                    f"  Found {len(relay_miners):,} miners that have used relays"
                )

                console.print("[cyan]Step 2: Finding candidate blocks...[/cyan]")

                # Find blocks with these miners but not in relays_payloads
                result = await session.execute(
                    text("""
                    SELECT b.number
                    FROM blocks b
                    LEFT JOIN relays_payloads rp ON b.number = rp.block_number
                    WHERE b.miner = ANY(:miners)
                      AND rp.block_number IS NULL
                    ORDER BY b.number
                """),
                    {"miners": relay_miners},
                )
                return [row[0] for row in result.fetchall()]
        except OperationalError:
            if attempt < MAX_RETRIES - 1:
                console.print(
                    "[yellow]DB connection error, retrying "
                    f"({attempt + 1}/{MAX_RETRIES})...[/yellow]"
                )
                await asyncio.sleep(RETRY_DELAY)
            else:
                raise
    return []


async def fetch_blocks_batch(
    client: httpx.AsyncClient, block_numbers: list[int]
) -> list[dict]:
    """Fetch multiple blocks from beaconcha.in API (up to 100 at once)."""
    if not block_numbers:
        return []

    # Create comma-separated block numbers
    blocks_param = ",".join(str(b) for b in block_numbers)
    url = f"{BEACONCHAIN_API}/{blocks_param}"
    headers = {"apikey": BEACONCHAIN_API_KEY}

    try:
        response = await client.get(url, headers=headers, timeout=60)  # type: ignore[reportUnknownReturnType]
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "OK" and data.get("data"):
            return data["data"]
    except Exception as e:
        console.print(
            f"[red]Error fetching batch of {len(block_numbers)} blocks: {e}[/red]"
        )
    return []


def map_relay_tag(tag: str | None) -> str | None:
    """Map beaconcha.in relay tag to canonical relay name."""
    if not tag:
        return None
    # Try direct mapping
    if tag in RELAY_TAG_MAPPING:
        return RELAY_TAG_MAPPING[tag]
    # Try to infer from tag name
    tag_lower = tag.lower()
    for key, value in RELAY_TAG_MAPPING.items():
        if key.replace("-relay", "") in tag_lower:
            return value
    console.print(f"[yellow]Warning: Unknown relay tag: {tag}[/yellow]")
    return None


def extract_relay_payload(block_data: dict) -> dict | None:
    """Extract relay payload data from block data."""
    relay_info = block_data.get("relay")
    if not relay_info:
        return None  # Vanilla block (no MEV-Boost)

    relay_tag = relay_info.get("tag")
    relay_name = map_relay_tag(relay_tag)
    if not relay_name:
        return None

    pos_consensus = block_data.get("posConsensus", {})

    return {
        "slot": pos_consensus.get("slot"),
        "relay": relay_name,
        "parent_hash": block_data.get("parentHash"),
        "block_hash": block_data.get("blockHash"),
        "builder_pubkey": relay_info.get("builderPubkey"),
        "proposer_pubkey": None,  # Not available from beaconcha.in
        "proposer_fee_recipient": relay_info.get("producerFeeRecipient").lower()
        if relay_info.get("producerFeeRecipient")
        else None,
        "gas_limit": block_data.get("gasLimit"),
        "gas_used": block_data.get("gasUsed"),
        "value": block_data.get("producerReward"),  # MEV reward in wei
        "block_number": block_data.get("blockNumber"),
        "num_tx": block_data.get("txCount"),
    }


async def upsert_batch(payloads: list[dict]) -> int:
    """Upsert a batch of relay payloads to the database with retry logic."""
    if not payloads:
        return 0

    stmt = text("""
        INSERT INTO relays_payloads (
            slot, relay, parent_hash, block_hash, builder_pubkey,
            proposer_pubkey, proposer_fee_recipient, gas_limit,
            gas_used, value, block_number, num_tx
        ) VALUES (
            :slot, :relay, :parent_hash, :block_hash, :builder_pubkey,
            :proposer_pubkey, :proposer_fee_recipient, :gas_limit,
            :gas_used, :value, :block_number, :num_tx
        )
        ON CONFLICT (slot, relay) DO NOTHING
    """)

    for attempt in range(MAX_RETRIES):
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(stmt, payloads)
                await session.commit()
            return len(payloads)
        except OperationalError as e:
            if attempt < MAX_RETRIES - 1:
                console.print(
                    "[yellow]DB connection error during upsert, retrying "
                    f"({attempt + 1}/{MAX_RETRIES})...[/yellow]"
                )
                await asyncio.sleep(RETRY_DELAY)
            else:
                console.print(
                    f"[red]Failed to upsert after {MAX_RETRIES} attempts: {e}[/red]"
                )
                return 0

    return 0


async def main() -> None:
    """Main backfill function for candidate MEV vanilla blocks."""
    console.print("[bold blue]Beaconcha.in Candidate MEV Vanilla Backfill[/bold blue]")
    console.print(
        "Finding blocks with relay-using miners but missing from relays_payloads\n"
    )

    # Get candidate blocks
    candidate_blocks = await get_candidate_blocks()
    total_blocks = len(candidate_blocks)

    if total_blocks == 0:
        console.print("[green]No candidate blocks found. Done![/green]")
        return

    min_block = min(candidate_blocks)
    max_block = max(candidate_blocks)
    console.print(f"\nFound [bold]{total_blocks:,}[/bold] candidate blocks")
    console.print(f"Block range: {min_block:,} to {max_block:,}")
    console.print(f"API batch size: {API_BATCH_SIZE} blocks per request\n")

    inserted = 0
    confirmed_vanilla = 0
    errors = 0
    db_batch: list[dict] = []

    # Calculate total API requests needed
    total_api_requests = (total_blocks + API_BATCH_SIZE - 1) // API_BATCH_SIZE

    progress = create_standard_progress(console, expand=True)

    with progress:
        task_id = progress.add_task("Fetching blocks", total=total_blocks)

        async with httpx.AsyncClient() as client:
            for i in range(0, total_blocks, API_BATCH_SIZE):
                # Get batch of block numbers
                batch_blocks = candidate_blocks[i : i + API_BATCH_SIZE]
                batch_num = i // API_BATCH_SIZE + 1

                # Fetch batch from API
                blocks_data = await fetch_blocks_batch(client, batch_blocks)

                if blocks_data:
                    # Process each block in the response
                    for block_data in blocks_data:
                        payload = extract_relay_payload(block_data)
                        if payload and payload["slot"]:
                            db_batch.append(payload)
                        else:
                            confirmed_vanilla += 1

                    # Count blocks that weren't returned (errors)
                    errors += len(batch_blocks) - len(blocks_data)
                else:
                    errors += len(batch_blocks)

                # Commit to DB when batch is large enough
                if len(db_batch) >= DB_BATCH_SIZE:
                    inserted += await upsert_batch(db_batch)
                    db_batch = []

                # Update progress
                blocks_processed = min(i + API_BATCH_SIZE, total_blocks)
                last_block = batch_blocks[-1] if batch_blocks else 0
                progress.update(
                    task_id,
                    completed=blocks_processed,
                    description=(
                        f"Batch {batch_num}/{total_api_requests} |"
                        f" Block {last_block:,} (found: {inserted:,},"
                        f" vanilla: {confirmed_vanilla:,}, err: {errors})"
                    ),
                )

                # Rate limiting between API requests
                await asyncio.sleep(RATE_LIMIT_DELAY)

        # Final DB batch
        if db_batch:
            inserted += await upsert_batch(db_batch)

    console.print("\n[bold green]Done![/bold green]")
    console.print(f"Total processed: {total_blocks:,}")
    console.print(f"MEV blocks found & inserted: {inserted:,}")
    console.print(f"Confirmed vanilla: {confirmed_vanilla:,}")
    console.print(f"Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
