"""Tests for RPC models."""

import pytest

from pydantic import ValidationError

from src.helpers.rpc_models import (
    EthBlockNumberRequest,
    EthGetBlockByNumberRequest,
    JsonRpcRequest,
)


def test_json_rpc_request() -> None:
    """Test JsonRpcRequest model."""
    request = JsonRpcRequest(method="test_method", params=[1, "two"], id=1)
    assert request.jsonrpc == "2.0"
    assert request.method == "test_method"
    assert request.params == [1, "two"]
    assert request.id == 1


def test_json_rpc_request_string_id() -> None:
    """Test JsonRpcRequest with string ID."""
    request = JsonRpcRequest(method="test_method", id="abc123")
    assert request.id == "abc123"


def test_json_rpc_request_default_params() -> None:
    """Test JsonRpcRequest with default params."""
    request = JsonRpcRequest(method="test_method", id=1)
    assert request.params == []


def test_json_rpc_request_validation() -> None:
    """Test JsonRpcRequest validation."""
    with pytest.raises(ValidationError):
        JsonRpcRequest(id=1)  # type: ignore[call-arg]


def test_eth_block_number_request() -> None:
    """Test EthBlockNumberRequest model."""
    request = EthBlockNumberRequest(id=1)
    assert request.jsonrpc == "2.0"
    assert request.method == "eth_blockNumber"
    assert request.params == []
    assert request.id == 1


def test_eth_block_number_request_frozen_method() -> None:
    """Test EthBlockNumberRequest method is frozen."""
    request = EthBlockNumberRequest(id=1)
    # The method field is frozen and should not be changeable
    with pytest.raises(ValidationError):
        request.method = "different_method"


def test_eth_get_block_by_number_request() -> None:
    """Test EthGetBlockByNumberRequest model."""
    request = EthGetBlockByNumberRequest(params=["0x1", True], id=2)
    assert request.jsonrpc == "2.0"
    assert request.method == "eth_getBlockByNumber"
    assert request.params == ["0x1", True]
    assert request.id == 2


def test_eth_get_block_by_number_request_frozen_method() -> None:
    """Test EthGetBlockByNumberRequest method is frozen."""
    request = EthGetBlockByNumberRequest(params=["0x1", True], id=1)
    # The method field is frozen and should not be changeable
    with pytest.raises(ValidationError):
        request.method = "different_method"


def test_json_rpc_request_serialization() -> None:
    """Test JsonRpcRequest serialization."""
    request = JsonRpcRequest(method="test_method", params=[1, "two"], id=1)
    data = request.model_dump()
    assert data == {
        "jsonrpc": "2.0",
        "method": "test_method",
        "params": [1, "two"],
        "id": 1,
    }


def test_eth_block_number_request_serialization() -> None:
    """Test EthBlockNumberRequest serialization."""
    request = EthBlockNumberRequest(id=123)
    data = request.model_dump()
    assert data == {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 123,
    }
