"""Tests for helper Pydantic models."""

from datetime import datetime, UTC

import pytest
from pydantic import ValidationError

from src.helpers.models import AdjustmentResponse, AggregatedBlockData, BlockHeader


class TestBlockHeader:
    """Tests for BlockHeader model."""

    def test_create_valid_block_header(self) -> None:
        """Test creating a valid block header."""
        header = BlockHeader(
            number="0x1",
            hash="0xhash",
            parentHash="0xparent",
            miner="0xminer",
            timestamp="0x64a1b2c3",
        )

        assert header.number == "0x1"
        assert header.hash == "0xhash"
        assert header.parent_hash == "0xparent"
        assert header.miner == "0xminer"
        assert header.timestamp == "0x64a1b2c3"

    def test_block_header_with_optional_fields(self) -> None:
        """Test block header with optional fields."""
        header = BlockHeader(
            number="0x1",
            hash="0xhash",
            parentHash="0xparent",
            miner="0xminer",
            timestamp="0x64a1b2c3",
            extraData="0xdata",
            gasLimit="0x1000000",
            gasUsed="0x500000",
            baseFeePerGas="0x3b9aca00",
        )

        assert header.extra_data == "0xdata"
        assert header.gas_limit == "0x1000000"
        assert header.gas_used == "0x500000"
        assert header.base_fee_per_gas == "0x3b9aca00"

    def test_block_header_alias_mapping(self) -> None:
        """Test that aliases map correctly to field names."""
        header = BlockHeader(
            number="0x1",
            hash="0xhash",
            parentHash="0xparent",
            miner="0xminer",
            timestamp="0x64a1b2c3",
        )

        # Should be accessible via snake_case after alias mapping
        assert hasattr(header, "parent_hash")
        assert header.parent_hash == "0xparent"

    def test_block_header_allows_extra_fields(self) -> None:
        """Test that block header allows extra fields from websocket."""
        header = BlockHeader(
            number="0x1",
            hash="0xhash",
            parentHash="0xparent",
            miner="0xminer",
            timestamp="0x64a1b2c3",
            unknown_field="should_be_allowed",  # type: ignore[call-arg]
        )

        assert header.number == "0x1"
        # Extra field should be allowed due to Config extra="allow"


class TestAggregatedBlockData:
    """Tests for AggregatedBlockData model."""

    def test_create_valid_aggregated_data(self) -> None:
        """Test creating valid aggregated block data."""
        data = AggregatedBlockData(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=1.5,
            proposer_subsidy=0.05,
            total_value=1.55,
            is_block_vanilla=False,
            n_relays=3,
            relays=["flashbots", "bloxroute", "eden"],
            builder_name="flashbots",
            slot=12345,
            builder_extra_transfers=0.0,
            relay_fee=0.02,
        )

        assert data.block_number == 1000
        assert data.builder_balance_increase == 1.5
        assert data.proposer_subsidy == 0.05
        assert data.total_value == 1.55
        assert data.is_block_vanilla is False
        assert data.n_relays == 3
        assert len(data.relays) == 3  # type: ignore[arg-type]
        assert data.builder_name == "flashbots"

    def test_aggregated_data_with_null_optional_fields(self) -> None:
        """Test aggregated data with null optional fields."""
        data = AggregatedBlockData(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=1.0,
            proposer_subsidy=0.0,
            total_value=1.0,
            is_block_vanilla=True,
            n_relays=0,
            relays=None,
            builder_name="unknown",
            slot=None,
            builder_extra_transfers=0.0,
            relay_fee=None,
        )

        assert data.relays is None
        assert data.slot is None
        assert data.relay_fee is None

    def test_aggregated_data_vanilla_block(self) -> None:
        """Test aggregated data for vanilla block."""
        data = AggregatedBlockData(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=2.0,
            proposer_subsidy=0.0,
            total_value=2.0,
            is_block_vanilla=True,
            n_relays=0,
            relays=None,
            builder_name="vanilla",
            slot=None,
            builder_extra_transfers=0.0,
            relay_fee=None,
        )

        assert data.is_block_vanilla is True
        assert data.n_relays == 0
        assert data.relays is None


class TestAdjustmentResponse:
    """Tests for AdjustmentResponse model."""

    def test_create_valid_adjustment_response(self) -> None:
        """Test creating valid adjustment response."""
        response = AdjustmentResponse(
            adjusted_block_hash="0xadjusted",
            adjusted_value="1500000000000000000",
            block_number=1000,
            builder_pubkey="0xbuilder",
            delta="500000000000000000",
            submitted_block_hash="0xsubmitted",
            submitted_received_at="2023-01-01T00:00:00Z",
            submitted_value="1000000000000000000",
        )

        assert response.adjusted_block_hash == "0xadjusted"
        assert response.adjusted_value == "1500000000000000000"
        assert response.block_number == 1000
        assert response.builder_pubkey == "0xbuilder"
        assert response.delta == "500000000000000000"

    def test_adjustment_response_all_optional(self) -> None:
        """Test adjustment response with all fields optional."""
        response = AdjustmentResponse()

        assert response.adjusted_block_hash is None
        assert response.adjusted_value is None
        assert response.block_number is None
        assert response.builder_pubkey is None
        assert response.delta is None
        assert response.submitted_block_hash is None
        assert response.submitted_received_at is None
        assert response.submitted_value is None

    def test_adjustment_response_partial_fields(self) -> None:
        """Test adjustment response with partial fields."""
        response = AdjustmentResponse(
            block_number=1000,
            builder_pubkey="0xbuilder",
            delta="100000000000000000",
        )

        assert response.block_number == 1000
        assert response.builder_pubkey == "0xbuilder"
        assert response.delta == "100000000000000000"
        assert response.adjusted_block_hash is None
        assert response.adjusted_value is None

    def test_adjustment_response_value_as_string(self) -> None:
        """Test that values are stored as strings (Wei)."""
        response = AdjustmentResponse(
            adjusted_value="1234567890123456789",
            submitted_value="9876543210987654321",
            delta="111111111111111111",
        )

        # Values should be strings, not parsed as integers
        assert isinstance(response.adjusted_value, str)
        assert isinstance(response.submitted_value, str)
        assert isinstance(response.delta, str)
