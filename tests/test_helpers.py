"""Unit tests for helper functions."""

import pytest

from src.helpers.parsers import parse_hex_int, parse_hex_timestamp, wei_to_eth


class TestParsers:
    """Test parsing utility functions."""

    def test_parse_hex_int_valid(self):
        """Test parsing valid hex integers."""
        assert parse_hex_int("0x10") == 16
        assert parse_hex_int("0x0") == 0
        assert parse_hex_int("0xff") == 255
        assert parse_hex_int("0xFF") == 255
        assert parse_hex_int("0x1234abcd") == 305441741

    def test_parse_hex_int_none(self):
        """Test parsing None returns default value."""
        assert parse_hex_int(None) == 0
        assert parse_hex_int(None, default=42) == 42

    def test_parse_hex_int_empty_string(self):
        """Test parsing empty string raises ValueError."""
        with pytest.raises(ValueError):
            parse_hex_int("")

    def test_parse_hex_timestamp_valid(self):
        """Test parsing valid hex timestamps."""
        from datetime import datetime

        # Test timestamp: 2023-01-01 00:00:00 (in local time)
        # The result depends on local timezone
        hex_ts = hex(1672531200)
        result = parse_hex_timestamp(hex_ts)
        assert isinstance(result, datetime)
        # Just verify it's a datetime object with year 2023
        # (actual hour may vary by timezone)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1

    def test_parse_hex_timestamp_none(self):
        """Test parsing None raises TypeError."""
        with pytest.raises(TypeError):
            parse_hex_timestamp(None)

    def test_wei_to_eth_valid(self):
        """Test Wei to ETH conversion."""
        # 1 ETH = 10^18 Wei
        assert wei_to_eth(1_000_000_000_000_000_000) == 1.0
        assert wei_to_eth(500_000_000_000_000_000) == 0.5
        assert wei_to_eth(0) == 0.0
        assert wei_to_eth(1) == 1e-18

    def test_wei_to_eth_none(self):
        """Test None returns None."""
        assert wei_to_eth(None) is None

    def test_wei_to_eth_precision(self):
        """Test Wei to ETH maintains precision."""
        # Test with a precise value
        wei_value = 1_234_567_890_123_456_789
        eth_value = wei_to_eth(wei_value)
        assert abs(eth_value - 1.234567890123456789) < 1e-18


class TestConstants:
    """Test application constants."""

    def test_builder_name_cleaning(self):
        """Test builder name cleaning logic."""
        from src.analysis.builder_name import clean_builder_name

        # Test None handling
        assert clean_builder_name(None) == "unknown"

        # Test geth variants
        assert clean_builder_name("geth") == "unknown"
        assert clean_builder_name("Geth-builder") == "unknown"

        # Test passthrough for normal names
        assert clean_builder_name("flashbots") == "flashbots"

    def test_relay_constants_exist(self):
        """Test that relay constants are defined."""
        from src.data.relays.constants import RELAYS

        assert isinstance(RELAYS, list)
        assert len(RELAYS) > 0
        assert all(isinstance(relay, str) for relay in RELAYS)
