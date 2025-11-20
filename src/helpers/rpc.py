"""Ethereum JSON-RPC client utilities."""

import operator

from typing import TYPE_CHECKING, Any

import asyncio

from src.helpers.parsers import parse_hex_int


if TYPE_CHECKING:
    import httpx


class RPCClient:
    """Ethereum JSON-RPC client with batching support."""

    def __init__(self, rpc_url: str, timeout: float = 30.0) -> None:
        """Initialize RPC client.

        Args:
            rpc_url: Ethereum JSON-RPC endpoint URL
            timeout: Default timeout for requests in seconds

        Raises:
            ValueError: If rpc_url is empty or None
        """
        if not rpc_url:
            msg = "RPC URL cannot be empty"
            raise ValueError(msg)

        self.rpc_url = rpc_url
        self.timeout = timeout

    async def call(
        self,
        client: httpx.AsyncClient,
        method: str,
        params: list[Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Make a single JSON-RPC call.

        Args:
            client: HTTP client instance
            method: RPC method name (e.g., "eth_blockNumber")
            params: Method parameters list
            timeout: Optional timeout override

        Returns:
            RPC result value

        Raises:
            httpx.HTTPError: If the HTTP request fails
            ValueError: If the RPC response contains an error
        """
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
            "id": 1,
        }

        response = await client.post(
            self.rpc_url, json=payload, timeout=timeout or self.timeout
        )
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            msg = f"RPC error: {result['error']}"
            raise ValueError(msg)

        return result.get("result")

    async def batch_call(
        self,
        client: httpx.AsyncClient,
        requests: list[tuple[str, list[Any]]],
        *,
        timeout: float | None = None,
    ) -> list[Any]:
        """Make multiple JSON-RPC calls in a single batch request.

        Args:
            client: HTTP client instance
            requests: List of (method, params) tuples
            timeout: Optional timeout override

        Returns:
            List of results in the same order as requests

        Raises:
            httpx.HTTPError: If the HTTP request fails
        """
        batch_payload: list[dict[str, Any]] = []
        for idx, (method, params) in enumerate(requests):
            batch_payload.append({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": idx,
            })

        response = await client.post(
            self.rpc_url, json=batch_payload, timeout=timeout or self.timeout
        )
        response.raise_for_status()
        results = response.json()

        # Sort by ID to match request order
        sorted_results = sorted(results, key=operator.itemgetter("id"))

        # Extract result values (return None for errors)
        return [r.get("result") for r in sorted_results]

    async def get_balance(
        self,
        client: httpx.AsyncClient,
        address: str,
        block_number: int | str = "latest",
    ) -> int:
        """Get ETH balance for an address at a specific block.

        Args:
            client: HTTP client instance
            address: Ethereum address
            block_number: Block number (int) or "latest"

        Returns:
            Balance in wei
        """
        block_param = (
            hex(block_number) if isinstance(block_number, int) else block_number
        )
        result = await self.call(client, "eth_getBalance", [address, block_param])
        return parse_hex_int(result) if result else 0

    async def batch_get_balances(
        self,
        client: httpx.AsyncClient,
        requests: list[tuple[str, int]],
    ) -> dict[tuple[str, int], int]:
        """Batch multiple eth_getBalance calls into a single JSON-RPC request.

        Args:
            client: HTTP client instance
            requests: List of (address, block_number) tuples

        Returns:
            Dict mapping (address, block_number) to balance in wei

        Example:
            ```python
            rpc = RPCClient(rpc_url)
            async with httpx.AsyncClient() as client:
                balances = await rpc.batch_get_balances(client, [
                    ("0x123...", 1000),
                    ("0x456...", 1000),
                ])
                # balances = {("0x123...", 1000): 5000000000000000000, ...}
            ```
        """
        # Build batch requests
        batch_requests = [
            ("eth_getBalance", [address, hex(block_number)])
            for address, block_number in requests
        ]

        try:
            results = await self.batch_call(client, batch_requests, timeout=60.0)

            # Map results back to (address, block_number)
            balance_map: dict[tuple[str, int], int] = {}
            for idx, result in enumerate(results):
                address, block_number = requests[idx]
                if result:
                    balance = int(result, 16)
                    balance_map[address, block_number] = balance
                else:
                    balance_map[address, block_number] = 0

            return balance_map

        except Exception:
            # Return zeros for all requests on error
            return dict.fromkeys(requests, 0)

    async def get_balance_change(
        self,
        client: httpx.AsyncClient,
        address: str,
        block_number: int,
    ) -> tuple[int, int, int]:
        """Get balance before, after, and change for a block.

        Args:
            client: HTTP client instance
            address: Ethereum address
            block_number: Block number

        Returns:
            Tuple of (balance_before, balance_after, balance_change) in wei

        Example:
            ```python
            rpc = RPCClient(rpc_url)
            async with httpx.AsyncClient() as client:
                before, after, change = await rpc.get_balance_change(
                    client, "0x123...", 1000
                )
            ```
        """
        results = await self.batch_call(
            client,
            [
                ("eth_getBalance", [address, hex(block_number - 1)]),
                ("eth_getBalance", [address, hex(block_number)]),
            ],
        )

        balance_before = parse_hex_int(results[0]) if results[0] else 0
        balance_after = parse_hex_int(results[1]) if results[1] else 0
        balance_change = balance_after - balance_before

        return balance_before, balance_after, balance_change

    async def get_block_number(self, client: httpx.AsyncClient) -> int:
        """Get the latest block number.

        Args:
            client: HTTP client instance

        Returns:
            Latest block number
        """
        result = await self.call(client, "eth_blockNumber", [])
        return parse_hex_int(result) if result else 0


async def batch_get_balance_changes(
    rpc_client: RPCClient,
    client: httpx.AsyncClient,
    addresses_and_blocks: list[tuple[str, int]],
    batch_size: int = 10,
    parallel_batches: int = 5,
) -> dict[tuple[str, int], tuple[int, int, int]]:
    """Batch multiple balance change requests with parallel execution.

    For each (address, block_number) pair, fetches balance at block N-1 and N,
    then calculates the change.

    Args:
        rpc_client: RPC client instance
        client: HTTP client instance
        addresses_and_blocks: List of (address, block_number) tuples
        batch_size: Number of balance requests per batch
        parallel_batches: Number of batches to execute in parallel

    Returns:
        Dict mapping (address, block_number) to (before, after, change) tuples

    Example:
        ```python
        rpc = RPCClient(rpc_url)
        async with httpx.AsyncClient() as client:
            changes = await batch_get_balance_changes(
                rpc, client,
                [("0x123...", 1000), ("0x456...", 1001)],
                batch_size=10,
                parallel_batches=5,
            )
            # changes = {
            #     ("0x123...", 1000): (before, after, change),
            #     ("0x456...", 1001): (before, after, change),
            # }
        ```
    """
    # Build requests for all balances we need (before and after for each block)
    balance_requests: list[tuple[str, int]] = []
    for address, block_number in addresses_and_blocks:
        balance_requests.extend([
            (address, block_number - 1),  # Balance before
            (address, block_number),  # Balance after
        ])

    # Process balance requests in parallel batches
    all_balances: dict[tuple[str, int], int] = {}

    # Split into batches
    batches = [
        balance_requests[i : i + batch_size]
        for i in range(0, len(balance_requests), batch_size)
    ]

    # Process batches in parallel chunks
    for i in range(0, len(batches), parallel_batches):
        parallel_chunk = batches[i : i + parallel_batches]

        # Execute multiple batch requests in parallel
        results = await asyncio.gather(*[
            rpc_client.batch_get_balances(client, batch) for batch in parallel_chunk
        ])

        # Merge results
        for balances in results:
            all_balances.update(balances)

    # Calculate changes
    changes: dict[tuple[str, int], tuple[int, int, int]] = {}
    for address, block_number in addresses_and_blocks:
        balance_before = all_balances.get((address, block_number - 1), 0)
        balance_after = all_balances.get((address, block_number), 0)
        balance_change = balance_after - balance_before
        changes[address, block_number] = (balance_before, balance_after, balance_change)

    return changes


__all__ = [
    "RPCClient",
    "batch_get_balance_changes",
]
