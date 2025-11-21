"""Tests for standalone RPC helper functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.helpers.rpc import RPCClient, batch_get_balance_changes


class TestBatchGetBalanceChanges:
    """Tests for batch_get_balance_changes function."""

    @pytest.mark.asyncio
    async def test_batch_get_balance_changes(self) -> None:
        """Test batch getting balance changes."""
        import httpx

        rpc_client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        # Mock responses for batch requests
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"jsonrpc": "2.0", "id": 0, "result": "0xde0b6b3a7640000"},  # 1 ETH before
            {"jsonrpc": "2.0", "id": 1, "result": "0x1bc16d674ec80000"},  # 2 ETH after
            {
                "jsonrpc": "2.0",
                "id": 2,
                "result": "0x2b5e3af16b1880000",
            },  # 3 ETH before
            {"jsonrpc": "2.0", "id": 3, "result": "0x3782dace9d900000"},  # 4 ETH after
        ]
        mock_http_client.post.return_value = mock_response

        addresses_and_blocks = [
            ("0xaddress1", 1000),
            ("0xaddress2", 1001),
        ]

        results = await batch_get_balance_changes(
            rpc_client,
            mock_http_client,
            addresses_and_blocks,
            batch_size=10,
            parallel_batches=5,
        )

        assert len(results) == 2
        assert ("0xaddress1", 1000) in results
        assert ("0xaddress2", 1001) in results

        # Check balance changes are calculated
        before1, after1, change1 = results["0xaddress1", 1000]
        assert before1 == 1000000000000000000
        assert after1 == 2000000000000000000
        assert change1 == 1000000000000000000

    @pytest.mark.asyncio
    async def test_batch_get_balance_changes_empty(self) -> None:
        """Test batch get balance changes with empty list."""
        import httpx

        rpc_client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        results = await batch_get_balance_changes(
            rpc_client,
            mock_http_client,
            [],
            batch_size=10,
            parallel_batches=5,
        )

        assert results == {}
        mock_http_client.post.assert_not_called()
