"""Tests for configuration and environment variable helpers."""

import os

import pytest

from typing import TYPE_CHECKING

from src.helpers.config import (
    get_eth_rpc_url,
    get_eth_ws_url,
    get_grafana_api_key,
    get_grafana_url,
    get_optional_env,
    get_required_env,
    get_required_url,
)


if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def clean_env() -> Generator[None]:
    """Clean environment variables before and after test."""
    # Save current env
    saved_env = {
        "TEST_KEY": os.environ.get("TEST_KEY"),
        "TEST_URL": os.environ.get("TEST_URL"),
        "ETH_RPC_URL": os.environ.get("ETH_RPC_URL"),
        "ETH_WS_URL": os.environ.get("ETH_WS_URL"),
        "GRAFANA_API_KEY": os.environ.get("GRAFANA_API_KEY"),
        "GRAFANA_URL": os.environ.get("GRAFANA_URL"),
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


@pytest.mark.usefixtures("clean_env")
class TestGetRequiredEnv:
    """Tests for get_required_env function."""

    def test_returns_env_value_when_set(self) -> None:
        """Test that get_required_env returns value when set."""
        os.environ["TEST_KEY"] = "test_value"
        assert get_required_env("TEST_KEY") == "test_value"

    def test_raises_when_not_set(self) -> None:
        """Test that get_required_env raises ValueError when not set."""
        with pytest.raises(
            ValueError, match="TEST_KEY environment variable is not set"
        ):
            get_required_env("TEST_KEY")

    def test_raises_when_empty_string(self) -> None:
        """Test that get_required_env raises ValueError when empty."""
        os.environ["TEST_KEY"] = ""
        with pytest.raises(
            ValueError, match="TEST_KEY environment variable is not set"
        ):
            get_required_env("TEST_KEY")


@pytest.mark.usefixtures("clean_env")
class TestGetOptionalEnv:
    """Tests for get_optional_env function."""

    def test_returns_env_value_when_set(self) -> None:
        """Test that get_optional_env returns value when set."""
        os.environ["TEST_KEY"] = "test_value"
        assert get_optional_env("TEST_KEY") == "test_value"

    def test_returns_none_when_not_set(self) -> None:
        """Test that get_optional_env returns None when not set."""
        assert get_optional_env("TEST_KEY") is None

    def test_returns_default_when_not_set(self) -> None:
        """Test that get_optional_env returns default when not set."""
        assert get_optional_env("TEST_KEY", "default") == "default"

    def test_returns_env_value_over_default(self) -> None:
        """Test that get_optional_env prefers env value over default."""
        os.environ["TEST_KEY"] = "env_value"
        assert get_optional_env("TEST_KEY", "default") == "env_value"


@pytest.mark.usefixtures("clean_env")
class TestGetRequiredUrl:
    """Tests for get_required_url function."""

    def test_returns_url_parameter_when_provided(self) -> None:
        """Test that get_required_url returns URL parameter when provided."""
        url = "https://example.com"
        assert get_required_url("TEST_URL", url=url) == url

    def test_returns_env_value_when_url_not_provided(self) -> None:
        """Test that get_required_url returns env value when URL not provided."""
        os.environ["TEST_URL"] = "https://from-env.com"
        assert get_required_url("TEST_URL") == "https://from-env.com"

    def test_url_parameter_takes_precedence(self) -> None:
        """Test that URL parameter takes precedence over env var."""
        os.environ["TEST_URL"] = "https://from-env.com"
        url = "https://from-param.com"
        assert get_required_url("TEST_URL", url=url) == url

    def test_raises_when_url_not_provided_and_env_not_set(self) -> None:
        """Test that get_required_url raises when no URL and no env var."""
        with pytest.raises(
            ValueError, match="TEST_URL must be provided or set in TEST_URL"
        ):
            get_required_url("TEST_URL")

    def test_custom_description_in_error(self) -> None:
        """Test that custom description appears in error message."""
        with pytest.raises(
            ValueError, match="Custom API URL must be provided or set in TEST_URL"
        ):
            get_required_url("TEST_URL", description="Custom API URL")

    def test_empty_env_value_raises(self) -> None:
        """Test that empty env value is treated as not set."""
        os.environ["TEST_URL"] = ""
        with pytest.raises(ValueError, match="must be provided"):
            get_required_url("TEST_URL")


@pytest.mark.usefixtures("clean_env")
class TestGetEthRpcUrl:
    """Tests for get_eth_rpc_url function."""

    def test_returns_rpc_url_parameter(self) -> None:
        """Test that get_eth_rpc_url returns parameter when provided."""
        url = "https://eth.llamarpc.com"
        assert get_eth_rpc_url(url) == url

    def test_returns_env_value(self) -> None:
        """Test that get_eth_rpc_url returns env value."""
        os.environ["ETH_RPC_URL"] = "https://mainnet.infura.io/v3/test"
        assert get_eth_rpc_url() == "https://mainnet.infura.io/v3/test"

    def test_raises_with_correct_message(self) -> None:
        """Test that error message mentions Ethereum RPC URL."""
        with pytest.raises(
            ValueError, match="Ethereum RPC URL must be provided or set in ETH_RPC_URL"
        ):
            get_eth_rpc_url()


@pytest.mark.usefixtures("clean_env")
class TestGetEthWsUrl:
    """Tests for get_eth_ws_url function."""

    def test_returns_ws_url_parameter(self) -> None:
        """Test that get_eth_ws_url returns parameter when provided."""
        url = "wss://eth-mainnet.g.alchemy.com/v2/test"
        assert get_eth_ws_url(url) == url

    def test_returns_env_value(self) -> None:
        """Test that get_eth_ws_url returns env value."""
        os.environ["ETH_WS_URL"] = "wss://mainnet.infura.io/ws/v3/test"
        assert get_eth_ws_url() == "wss://mainnet.infura.io/ws/v3/test"

    def test_raises_with_correct_message(self) -> None:
        """Test that error message mentions Ethereum WebSocket URL."""
        with pytest.raises(
            ValueError,
            match="Ethereum WebSocket URL must be provided or set in ETH_WS_URL",
        ):
            get_eth_ws_url()


@pytest.mark.usefixtures("clean_env")
class TestGetGrafanaApiKey:
    """Tests for get_grafana_api_key function."""

    def test_returns_api_key_parameter(self) -> None:
        """Test that get_grafana_api_key returns parameter when provided."""
        api_key = "glsa_test123"
        assert get_grafana_api_key(api_key) == api_key

    def test_returns_env_value(self) -> None:
        """Test that get_grafana_api_key returns env value."""
        os.environ["GRAFANA_API_KEY"] = "glsa_from_env"
        assert get_grafana_api_key() == "glsa_from_env"

    def test_parameter_takes_precedence(self) -> None:
        """Test that parameter takes precedence over env var."""
        os.environ["GRAFANA_API_KEY"] = "glsa_from_env"
        assert get_grafana_api_key("glsa_from_param") == "glsa_from_param"

    def test_raises_when_not_provided_and_env_not_set(self) -> None:
        """Test that get_grafana_api_key raises when no key and no env var."""
        with pytest.raises(
            ValueError,
            match="Grafana API key must be provided or set in GRAFANA_API_KEY",
        ):
            get_grafana_api_key()

    def test_raises_when_env_empty(self) -> None:
        """Test that empty env value is treated as not set."""
        os.environ["GRAFANA_API_KEY"] = ""
        with pytest.raises(
            ValueError,
            match="Grafana API key must be provided or set in GRAFANA_API_KEY",
        ):
            get_grafana_api_key()


@pytest.mark.usefixtures("clean_env")
class TestGetGrafanaUrl:
    """Tests for get_grafana_url function."""

    def test_returns_url_parameter(self) -> None:
        """Test that get_grafana_url returns parameter when provided."""
        url = "https://grafana.example.com"
        assert get_grafana_url(url) == url

    def test_returns_env_value(self) -> None:
        """Test that get_grafana_url returns env value."""
        os.environ["GRAFANA_URL"] = "https://grafana.from-env.com"
        assert get_grafana_url() == "https://grafana.from-env.com"

    def test_raises_with_correct_message(self) -> None:
        """Test that error message mentions Grafana URL."""
        with pytest.raises(
            ValueError, match="Grafana URL must be provided or set in GRAFANA_URL"
        ):
            get_grafana_url()
