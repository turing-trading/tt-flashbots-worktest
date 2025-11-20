"""Unit tests for helper functions."""

import pytest

from src.helpers.parsers import parse_hex_int, parse_hex_timestamp, wei_to_eth


class TestParsers:
    """Test parsing utility functions."""

    def test_parse_hex_int_valid(self) -> None:
        """Test parsing valid hex integers."""
        assert parse_hex_int("0x10") == 16
        assert parse_hex_int("0x0") == 0
        assert parse_hex_int("0xff") == 255
        assert parse_hex_int("0xFF") == 255
        assert parse_hex_int("0x1234abcd") == 305441741

    def test_parse_hex_int_none(self) -> None:
        """Test parsing None returns default value."""
        assert parse_hex_int(None) == 0
        assert parse_hex_int(None, default=42) == 42

    def test_parse_hex_int_empty_string(self) -> None:
        """Test parsing empty string raises ValueError."""
        with pytest.raises(ValueError):
            parse_hex_int("")

    def test_parse_hex_timestamp_valid(self) -> None:
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

    def test_wei_to_eth_valid(self) -> None:
        """Test Wei to ETH conversion."""
        # 1 ETH = 10^18 Wei
        assert wei_to_eth(1_000_000_000_000_000_000) == 1.0
        assert wei_to_eth(500_000_000_000_000_000) == 0.5
        assert wei_to_eth(0) == 0.0
        assert wei_to_eth(1) == 1e-18

    def test_wei_to_eth_none(self) -> None:
        """Test None returns None."""
        assert wei_to_eth(None) is None


class TestConstants:
    """Test application constants."""

    def test_builder_name_cleaning(self) -> None:
        """Test builder name cleaning logic."""
        from src.analysis.builder_name import clean_builder_name

        # Test None handling
        assert clean_builder_name(None) == "unknown"

        # Test geth variants
        assert clean_builder_name("geth") == "unknown"
        assert clean_builder_name("Geth-builder") == "unknown"

        # Test passthrough for normal names
        assert clean_builder_name("flashbots") == "flashbots"

    def test_relay_constants_exist(self) -> None:
        """Test that relay constants are defined."""
        from src.data.relays.constants import RELAYS

        assert isinstance(RELAYS, list)
        assert len(RELAYS) > 0
        assert all(isinstance(relay, str) for relay in RELAYS)


class TestBuilderNameParsing:
    """Test builder name parsing from extra_data."""

    def test_parse_builder_name_from_extra_data_all_cases(self) -> None:
        """Test parsing builder names from all extra_data test cases."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        # Comprehensive test cases covering all extra_data patterns
        test_cases = [
            # Known builders with clear identifiers
            ("0x546974616e2028746974616e6275696c6465722e78797a29", "Titan"),
            (
                "0x4275696c6465724e65742028466c617368626f747329",
                "BuilderNet (Flashbots)",
            ),
            ("0xe29ca82051756173617220287175617361722e77696e2920e29ca8", "Quasar"),
            ("0x4275696c6465724e6574202842656176657229", "BuilderNet (Beaver)"),
            (
                "0x4275696c6465724e657420284e65746865726d696e6429",
                "BuilderNet (Nethermind)",
            ),
            ("0x6265617665726275696c642e6f7267", "BuilderNet (Beaver)"),
            ("0x407273796e636275696c646572", "Rsync"),
            ("0x4275696c6465722b207777772e627463732e636f6d2f6275696c646572", "BTCS"),
            # Geth variants (should return unknown)
            ("0xd883010f0b846765746888676f312e32342e32856c696e7578", "unknown"),
            ("0xd883011004846765746888676f312e32342e39856c696e7578", "unknown"),
            ("0xd883011005846765746888676f312e32352e31856c696e7578", "unknown"),
            ("0xd883011003846765746888676f312e32352e30856c696e7578", "unknown"),
            ("0xd883011004846765746888676f312e32352e31856c696e7578", "unknown"),
            ("0xd883011001846765746888676f312e32342e34856c696e7578", "unknown"),
            ("0xd883010f0a846765746888676f312e32342e32856c696e7578", "unknown"),
            ("0xd883011005846765746888676f312e32342e39856c696e7578", "unknown"),
            ("0xd883011007846765746888676f312e32352e31856c696e7578", "unknown"),
            ("0xd883011004846765746888676f312e32342e37856c696e7578", "unknown"),
            ("0xd883011002846765746888676f312e32342e34856c696e7578", "unknown"),
            ("0xd883010f0b846765746888676f312e32332e39856c696e7578", "unknown"),
            ("0xd883011005846765746888676f312e32342e30856c696e7578", "unknown"),
            ("0xd883011003846765746888676f312e32332e39856c696e7578", "unknown"),
            ("0xd883011000846765746888676f312e32342e34856c696e7578", "unknown"),
            ("0xd883011007846765746888676f312e32342e39856c696e7578", "unknown"),
            ("0xd883011003846765746888676f312e32342e36856c696e7578", "unknown"),
            ("0xd883011007846765746888676f312e32352e33856c696e7578", "unknown"),
            # Empty extra_data
            ("0x", "unknown"),
            # Nethermind variants
            ("0x4e65746865726d696e642076312e33322e32", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33352e30", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33322e34", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33342e30", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e64", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33352e32", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33332e31", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33342e31", "BuilderNet (Nethermind)"),
            ("0x4e65746865726d696e642076312e33322e33", "BuilderNet (Nethermind)"),
            # Other known builders
            ("0x696f6275696c6465722e78797a", "IO Builder"),
            ("0x626f625468654275696c6465722e78797a", "Bob The Builder"),
            ("0x457572656b612028657572656b616275696c6465722e78797a29", "Eureka"),
            (
                "0x4269746765742868747470733a2f2f7777772e6269746765742e636f6d2f29",
                "Bitget",
            ),
            ("0x4275696c6465722b20627463732e636f6d207c206574686761732e636f6d", "BTCS"),
            ("0x7270632e747572626f6275696c6465722e78797a", "Turbo"),
            # Besu variants
            ("0x626573752032352e382e30", "besu"),
            ("0x626573752032352e392e30", "besu"),
            ("0x626573752032352e342e31", "besu"),
            ("0x626573752032352e31312e30", "besu"),
            ("0x626573752032352e31302d646576656c6f702d37633362633932", "besu"),
            ("0x626573752032352e392d646576656c6f702d65383836306432", "besu"),
            # Reth variants
            ("0x726574682f76312e382e322f6c696e7578", "linux"),
            ("0x726574682f76312e372e302f6c696e7578", "linux"),
        ]

        for extra_data, expected in test_cases:
            result = parse_builder_name_from_extra_data(extra_data)
            assert result == expected, (
                f"Failed for {extra_data}: expected '{expected}', got '{result}'"
            )

    def test_parse_builder_name_none_input(self) -> None:
        """Test parsing with None input returns 'unknown'."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        assert parse_builder_name_from_extra_data(None) == "unknown"

    def test_parse_builder_name_invalid_hex(self) -> None:
        """Test parsing with invalid hex returns 'unknown'."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        # Invalid hex string
        assert parse_builder_name_from_extra_data("0xZZZZ") == "unknown"

        # Odd-length hex string (invalid)
        assert parse_builder_name_from_extra_data("0x123") == "unknown"

    def test_parse_builder_name_builder_mapping(self) -> None:
        """Test that builder name mapping is applied correctly."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        # Titan should map to canonical "Titan"
        assert (
            parse_builder_name_from_extra_data(
                "0x546974616e2028746974616e6275696c6465722e78797a29"
            )
            == "Titan"
        )

        # Beaverbuild.org should map to "BuilderNet (Beaver)"
        assert (
            parse_builder_name_from_extra_data("0x6265617665726275696c642e6f7267")
            == "BuilderNet (Beaver)"
        )

        # Rsync builder variants should map to "Rsync"
        assert (
            parse_builder_name_from_extra_data("0x407273796e636275696c646572")
            == "Rsync"
        )

        # Bob The Builder variants
        assert (
            parse_builder_name_from_extra_data("0x626f625468654275696c6465722e78797a")
            == "Bob The Builder"
        )

    def test_parse_builder_name_emoji_handling(self) -> None:
        """Test that emojis are properly stripped from builder names."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        # Quasar with sparkle emojis should be cleaned to "Quasar"
        assert (
            parse_builder_name_from_extra_data(
                "0xe29ca82051756173617220287175617361722e77696e2920e29ca8"
            )
            == "Quasar"
        )

    def test_parse_builder_name_version_removal(self) -> None:
        """Test that version numbers are properly removed."""
        from src.analysis.builder_name import parse_builder_name_from_extra_data

        # All Nethermind versions should map to "BuilderNet (Nethermind)"
        nethermind_versions = [
            "0x4e65746865726d696e642076312e33322e32",
            "0x4e65746865726d696e642076312e33352e30",
            "0x4e65746865726d696e642076312e33342e30",
            "0x4e65746865726d696e64",  # No version
        ]

        for version_hex in nethermind_versions:
            result = parse_builder_name_from_extra_data(version_hex)
            assert result == "BuilderNet (Nethermind)", (
                f"Failed for {version_hex}: expected 'BuilderNet (Nethermind)', "
                f"got '{result}'"
            )
