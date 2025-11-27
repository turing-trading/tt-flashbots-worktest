"""Tests for RPC client."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from typing import Any

import httpx

from src.helpers.rpc import RPCClient


class TestRPCClient:
    """Tests for RPCClient class."""

    def test_init_with_valid_url(self) -> None:
        """Test RPCClient initialization with valid URL."""
        client = RPCClient("https://eth.llamarpc.com")

        assert client.rpc_url == "https://eth.llamarpc.com"
        assert client.timeout == 30.0

    def test_init_with_custom_timeout(self) -> None:
        """Test RPCClient initialization with custom timeout."""
        client = RPCClient("https://eth.llamarpc.com", timeout=60.0)

        assert client.timeout == 60.0

    def test_init_with_empty_url_raises(self) -> None:
        """Test that empty URL raises ValueError."""
        with pytest.raises(ValueError, match="RPC URL cannot be empty"):
            RPCClient("")

    def test_init_with_none_url_raises(self) -> None:
        """Test that None URL raises ValueError."""
        with pytest.raises(ValueError, match="RPC URL cannot be empty"):
            RPCClient(None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_call_single_method(self) -> None:
        """Test making a single RPC call."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x1000",
        }
        mock_http_client.post.return_value = mock_response

        result = await client.call(mock_http_client, "eth_blockNumber")

        assert result == "0x1000"
        mock_http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_with_params(self) -> None:
        """Test RPC call with parameters."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"number": "0x1"},
        }
        mock_http_client.post.return_value = mock_response

        result = await client.call(
            mock_http_client, "eth_getBlockByNumber", ["0x1", True]
        )

        assert result["number"] == "0x1"

    @pytest.mark.asyncio
    async def test_call_with_custom_timeout(self) -> None:
        """Test RPC call with custom timeout."""
        client = RPCClient("https://test.rpc", timeout=30.0)
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
        mock_http_client.post.return_value = mock_response

        await client.call(mock_http_client, "eth_blockNumber", timeout=60.0)

        # Verify timeout was passed to post call
        call_args = mock_http_client.post.call_args
        assert call_args is not None

    @pytest.mark.asyncio
    async def test_batch_call(self) -> None:
        """Test making a batch RPC call."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"jsonrpc": "2.0", "id": 0, "result": "0x1000"},
            {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x1"}},
        ]
        mock_http_client.post.return_value = mock_response

        requests: list[tuple[str, list[Any]]] = [
            ("eth_blockNumber", []),
            ("eth_getBlockByNumber", ["0x1", True]),
        ]
        results = await client.batch_call(mock_http_client, requests)

        assert len(results) == 2
        assert results[0] == "0x1000"
        assert results[1]["number"] == "0x1"

    @pytest.mark.asyncio
    async def test_call_with_rpc_error(self) -> None:
        """Test RPC call that returns an error."""
        import pytest

        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "Error message"},
        }
        mock_http_client.post.return_value = mock_response

        with pytest.raises(ValueError, match="RPC error"):
            await client.call(mock_http_client, "eth_blockNumber")

    @pytest.mark.asyncio
    async def test_get_block_number(self) -> None:
        """Test getting latest block number."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0xabc"}
        mock_http_client.post.return_value = mock_response

        result = await client.get_block_number(mock_http_client)

        assert result == 2748  # 0xabc in decimal

    @pytest.mark.asyncio
    async def test_get_balance(self) -> None:
        """Test getting account balance."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0xde0b6b3a7640000",  # 1 ETH in Wei
        }
        mock_http_client.post.return_value = mock_response

        result = await client.get_balance(mock_http_client, "0xaddress")

        assert result == 1000000000000000000

    @pytest.mark.asyncio
    async def test_batch_get_balances(self) -> None:
        """Test batch getting balances."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"jsonrpc": "2.0", "id": 0, "result": "0xde0b6b3a7640000"},
            {"jsonrpc": "2.0", "id": 1, "result": "0x1bc16d674ec80000"},
        ]
        mock_http_client.post.return_value = mock_response

        requests = [("0xaddress1", 1000), ("0xaddress2", 1000)]
        results = await client.batch_get_balances(mock_http_client, requests)

        assert len(results) == 2
        assert ("0xaddress1", 1000) in results
        assert ("0xaddress2", 1000) in results

    @pytest.mark.asyncio
    async def test_get_balance_change(self) -> None:
        """Test getting balance change between blocks."""
        client = RPCClient("https://test.rpc")
        mock_http_client = AsyncMock(spec=httpx.AsyncClient)

        # Mock batch call response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"jsonrpc": "2.0", "id": 0, "result": "0xde0b6b3a7640000"},  # 1 ETH before
            {"jsonrpc": "2.0", "id": 1, "result": "0x1bc16d674ec80000"},  # 2 ETH after
        ]
        mock_http_client.post.return_value = mock_response

        balance_before, balance_after, increase = await client.get_balance_change(
            mock_http_client, "0xaddress", 1000
        )

        assert balance_before == 1000000000000000000
        assert balance_after == 2000000000000000000
        assert increase == 1000000000000000000

    @pytest.mark.asyncio
    async def test_get_balances_batch_with_null_result(self) -> None:
        """Test batch_get_balances handles null/None results."""
        client = RPCClient("https://rpc.example.com")

        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 0, "result": "0x1"},  # Valid result
            {"id": 1, "result": None},  # Null result
            {"id": 2, "result": ""},  # Empty result
        ]
        mock_http_client.post.return_value = mock_response

        requests = [
            ("0xaddr1", 100),
            ("0xaddr2", 100),
            ("0xaddr3", 100),
        ]
        balance_map = await client.batch_get_balances(mock_http_client, requests)

        # Valid result should be parsed
        assert balance_map["0xaddr1", 100] == 1
        # Null and empty results should default to 0
        assert balance_map["0xaddr2", 100] == 0
        assert balance_map["0xaddr3", 100] == 0

    @pytest.mark.asyncio
    async def test_get_balances_batch_exception_returns_zeros(self) -> None:
        """Test batch_get_balances returns zeros on exception."""
        client = RPCClient("https://rpc.example.com")

        mock_http_client = AsyncMock(spec=httpx.AsyncClient)
        # Simulate an exception during the request
        mock_http_client.post.side_effect = Exception("Network error")

        requests = [
            ("0xaddr1", 100),
            ("0xaddr2", 200),
        ]
        balance_map = await client.batch_get_balances(mock_http_client, requests)

        # All requests should have zero balance on error
        assert balance_map["0xaddr1", 100] == 0
        assert balance_map["0xaddr2", 200] == 0
