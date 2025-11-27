"""Pydantic models for JSON-RPC requests and responses."""

from typing import Any

from pydantic import BaseModel, Field


class JsonRpcRequest(BaseModel):
    """JSON-RPC 2.0 request model."""

    jsonrpc: str = Field(default="2.0", description="JSON-RPC version")
    method: str = Field(..., description="Method name to call")
    params: list[Any] = Field(
        default_factory=list, description="Method parameters"
    )
    id: int | str = Field(..., description="Request ID")


class EthBlockNumberRequest(JsonRpcRequest):
    """JSON-RPC request for eth_blockNumber."""

    method: str = Field(default="eth_blockNumber", frozen=True)
    params: list[Any] = Field(default_factory=list, frozen=True)


class EthGetBlockByNumberRequest(JsonRpcRequest):
    """JSON-RPC request for eth_getBlockByNumber."""

    method: str = Field(default="eth_getBlockByNumber", frozen=True)


__all__ = [
    "EthBlockNumberRequest",
    "EthGetBlockByNumberRequest",
    "JsonRpcRequest",
]
