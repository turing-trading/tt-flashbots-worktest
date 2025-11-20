"""Tests for configuration and environment variable helpers."""

import os
from typing import TYPE_CHECKING

import pytest

from src.helpers.config import (
    get_eth_rpc_url,
    get_eth_ws_url,
    get_optional_env,
    get_required_env,
    get_required_url,
)


if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def clean_env() -> "Generator[None, None, None]":
    """Clean environment variables before and after test."""
    # Save current env
    saved_env = {
        "TEST_KEY": os.environ.get("TEST_KEY"),
        "TEST_URL": os.environ.get("TEST_URL"),
        "ETH_RPC_URL": os.environ.get("ETH_RPC_URL"),
        "ETH_WS_URL": os.environ.get("ETH_WS_URL"),
    }

    # Clear test keys
    for key in saved_env:
        if key in os.environ:
            del os.environ[key]

    yield

    # Restore env
    for key, value in saved_env.items():
        if value is not None:
            os.environ[key] = value
        elif key in os.environ:
            del os.environ[key]


class TestGetRequiredEnv:
    """Tests for get_required_env function."""

    def test_returns_env_value_when_set(self, clean_env: None) -> None:
        """Test that get_required_env returns value when set."""
        os.environ["TEST_KEY"] = "test_value"
        assert get_required_env("TEST_KEY") == "test_value"

    def test_raises_when_not_set(self, clean_env: None) -> None:
        """Test that get_required_env raises ValueError when not set."""
        with pytest.raises(ValueError, match="TEST_KEY environment variable is not set"):
            get_required_env("TEST_KEY")

    def test_raises_when_empty_string(self, clean_env: None) -> None:
        """Test that get_required_env raises ValueError when empty."""
        os.environ["TEST_KEY"] = ""
        with pytest.raises(ValueError, match="TEST_KEY environment variable is not set"):
            get_required_env("TEST_KEY")


class TestGetOptionalEnv:
    """Tests for get_optional_env function."""

    def test_returns_env_value_when_set(self, clean_env: None) -> None:
        """Test that get_optional_env returns value when set."""
        os.environ["TEST_KEY"] = "test_value"
        assert get_optional_env("TEST_KEY") == "test_value"

    def test_returns_none_when_not_set(self, clean_env: None) -> None:
        """Test that get_optional_env returns None when not set."""
        assert get_optional_env("TEST_KEY") is None

    def test_returns_default_when_not_set(self, clean_env: None) -> None:
        """Test that get_optional_env returns default when not set."""
        assert get_optional_env("TEST_KEY", "default") == "default"

    def test_returns_env_value_over_default(self, clean_env: None) -> None:
        """Test that get_optional_env prefers env value over default."""
        os.environ["TEST_KEY"] = "env_value"
        assert get_optional_env("TEST_KEY", "default") == "env_value"


class TestGetRequiredUrl:
    """Tests for get_required_url function."""

    def test_returns_url_parameter_when_provided(self, clean_env: None) -> None:
        """Test that get_required_url returns URL parameter when provided."""
        url = "https://example.com"
        assert get_required_url("TEST_URL", url=url) == url

    def test_returns_env_value_when_url_not_provided(self, clean_env: None) -> None:
        """Test that get_required_url returns env value when URL not provided."""
        os.environ["TEST_URL"] = "https://from-env.com"
        assert get_required_url("TEST_URL") == "https://from-env.com"

    def test_url_parameter_takes_precedence(self, clean_env: None) -> None:
        """Test that URL parameter takes precedence over env var."""
        os.environ["TEST_URL"] = "https://from-env.com"
        url = "https://from-param.com"
        assert get_required_url("TEST_URL", url=url) == url

    def test_raises_when_url_not_provided_and_env_not_set(self, clean_env: None) -> None:
        """Test that get_required_url raises when no URL and no env var."""
        with pytest.raises(
            ValueError, match="TEST_URL must be provided or set in TEST_URL"
        ):
            get_required_url("TEST_URL")

    def test_custom_description_in_error(self, clean_env: None) -> None:
        """Test that custom description appears in error message."""
        with pytest.raises(
            ValueError, match="Custom API URL must be provided or set in TEST_URL"
        ):
            get_required_url("TEST_URL", description="Custom API URL")

    def test_empty_env_value_raises(self, clean_env: None) -> None:
        """Test that empty env value is treated as not set."""
        os.environ["TEST_URL"] = ""
        with pytest.raises(ValueError, match="must be provided"):
            get_required_url("TEST_URL")


class TestGetEthRpcUrl:
    """Tests for get_eth_rpc_url function."""

    def test_returns_rpc_url_parameter(self, clean_env: None) -> None:
        """Test that get_eth_rpc_url returns parameter when provided."""
        url = "https://eth.llamarpc.com"
        assert get_eth_rpc_url(url) == url

    def test_returns_env_value(self, clean_env: None) -> None:
        """Test that get_eth_rpc_url returns env value."""
        os.environ["ETH_RPC_URL"] = "https://mainnet.infura.io/v3/test"
        assert get_eth_rpc_url() == "https://mainnet.infura.io/v3/test"

    def test_raises_with_correct_message(self, clean_env: None) -> None:
        """Test that error message mentions Ethereum RPC URL."""
        with pytest.raises(
            ValueError, match="Ethereum RPC URL must be provided or set in ETH_RPC_URL"
        ):
            get_eth_rpc_url()


class TestGetEthWsUrl:
    """Tests for get_eth_ws_url function."""

    def test_returns_ws_url_parameter(self, clean_env: None) -> None:
        """Test that get_eth_ws_url returns parameter when provided."""
        url = "wss://eth-mainnet.g.alchemy.com/v2/test"
        assert get_eth_ws_url(url) == url

    def test_returns_env_value(self, clean_env: None) -> None:
        """Test that get_eth_ws_url returns env value."""
        os.environ["ETH_WS_URL"] = "wss://mainnet.infura.io/ws/v3/test"
        assert get_eth_ws_url() == "wss://mainnet.infura.io/ws/v3/test"

    def test_raises_with_correct_message(self, clean_env: None) -> None:
        """Test that error message mentions Ethereum WebSocket URL."""
        with pytest.raises(
            ValueError,
            match="Ethereum WebSocket URL must be provided or set in ETH_WS_URL",
        ):
            get_eth_ws_url()
