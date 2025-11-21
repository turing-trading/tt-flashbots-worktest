"""Pydantic models for JSON-RPC requests and responses."""

from pydantic import BaseModel, Field

from src.helpers.http_models import JsonValue  # noqa: TC001


class JsonRpcRequest(BaseModel):
    """JSON-RPC 2.0 request model."""

    jsonrpc: str = Field(default="2.0", description="JSON-RPC version")
    method: str = Field(..., description="Method name to call")
    params: list[JsonValue] = Field(
        default_factory=list, description="Method parameters"
    )
    id: int | str = Field(..., description="Request ID")


class EthBlockNumberRequest(JsonRpcRequest):
    """JSON-RPC request for eth_blockNumber."""

    method: str = Field(default="eth_blockNumber", frozen=True)
    params: list[JsonValue] = Field(default_factory=list, frozen=True)


class EthGetBlockByNumberRequest(JsonRpcRequest):
    """JSON-RPC request for eth_getBlockByNumber."""

    method: str = Field(default="eth_getBlockByNumber", frozen=True)


__all__ = [
    "EthBlockNumberRequest",
    "EthGetBlockByNumberRequest",
    "JsonRpcRequest",
]

# Rebuild models to ensure JsonValue recursive type is fully resolved
JsonRpcRequest.model_rebuild()
EthBlockNumberRequest.model_rebuild()
EthGetBlockByNumberRequest.model_rebuild()
