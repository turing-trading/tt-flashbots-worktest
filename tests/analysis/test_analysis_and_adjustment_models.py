"""Tests for analysis and adjustment models."""

from datetime import datetime, UTC

from src.analysis.models import AnalysisPBSV3
from src.data.adjustments.models import UltrasoundAdjustment


class TestAnalysisPBSV3:
    """Tests for AnalysisPBSV3 model."""

    def test_create_with_required_fields_only(self) -> None:
        """Test creating model with only required fields."""
        analysis = AnalysisPBSV3(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
        )

        assert analysis.block_number == 1000
        assert analysis.builder_balance_increase == 0.0
        assert analysis.proposer_subsidy == 0.0
        assert analysis.total_value == 0.0
        assert analysis.is_block_vanilla is False
        assert analysis.n_relays == 0
        assert analysis.relays is None
        assert analysis.builder_name == "unknown"
        assert analysis.slot is None
        assert analysis.builder_extra_transfers == 0.0
        assert analysis.relay_fee is None

    def test_create_with_all_fields(self) -> None:
        """Test creating model with all fields."""
        analysis = AnalysisPBSV3(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=1.5,
            proposer_subsidy=0.05,
            total_value=1.57,
            is_block_vanilla=False,
            n_relays=3,
            relays=["flashbots", "bloxroute", "eden"],
            builder_name="flashbots",
            slot=12345,
            builder_extra_transfers=0.02,
            relay_fee=0.01,
        )

        assert analysis.builder_balance_increase == 1.5
        assert analysis.proposer_subsidy == 0.05
        assert analysis.total_value == 1.57
        assert analysis.is_block_vanilla is False
        assert analysis.n_relays == 3
        assert len(analysis.relays) == 3  # type: ignore[arg-type]
        assert analysis.builder_name == "flashbots"
        assert analysis.slot == 12345
        assert analysis.builder_extra_transfers == 0.02
        assert analysis.relay_fee == 0.01

    def test_vanilla_block(self) -> None:
        """Test model for vanilla block."""
        analysis = AnalysisPBSV3(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=2.0,
            is_block_vanilla=True,
            n_relays=0,
            builder_name="vanilla",
        )

        assert analysis.is_block_vanilla is True
        assert analysis.n_relays == 0
        assert analysis.slot is None
        assert analysis.relays is None

    def test_mev_boost_block(self) -> None:
        """Test model for MEV-Boost block."""
        analysis = AnalysisPBSV3(
            block_number=1000,
            block_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
            builder_balance_increase=1.0,
            proposer_subsidy=0.1,
            total_value=1.1,
            is_block_vanilla=False,
            n_relays=2,
            relays=["flashbots", "bloxroute"],
            builder_name="flashbots",
            slot=12345,
        )

        assert analysis.is_block_vanilla is False
        assert analysis.n_relays == 2
        assert analysis.slot == 12345
        assert len(analysis.relays) == 2  # type: ignore[arg-type]


class TestUltrasoundAdjustment:
    """Tests for UltrasoundAdjustment model."""

    def test_create_valid_adjustment(self) -> None:
        """Test creating a valid adjustment."""
        adjustment = UltrasoundAdjustment(
            slot=12345,
            adjusted_block_hash="0xadjusted",
            adjusted_value=1500000000000000000,
            block_number=1000,
            builder_pubkey="0xbuilder",
            delta=500000000000000000,
            submitted_block_hash="0xsubmitted",
            submitted_received_at="2023-01-01T00:00:00Z",
            submitted_value=1000000000000000000,
        )

        assert adjustment.slot == 12345
        assert adjustment.adjusted_block_hash == "0xadjusted"
        assert adjustment.adjusted_value == 1500000000000000000
        assert adjustment.block_number == 1000
        assert adjustment.builder_pubkey == "0xbuilder"
        assert adjustment.delta == 500000000000000000
        assert adjustment.submitted_block_hash == "0xsubmitted"
        assert adjustment.submitted_received_at == "2023-01-01T00:00:00Z"
        assert adjustment.submitted_value == 1000000000000000000

    def test_adjustment_values_are_integers(self) -> None:
        """Test that values are stored as integers (Wei)."""
        adjustment = UltrasoundAdjustment(
            slot=100,
            adjusted_block_hash="0xhash",
            adjusted_value=2000000000000000000,
            block_number=100,
            builder_pubkey="0xbuilder",
            delta=1000000000000000000,
            submitted_block_hash="0xhash",
            submitted_received_at="2023-01-01T00:00:00Z",
            submitted_value=1000000000000000000,
        )

        assert isinstance(adjustment.adjusted_value, int)
        assert isinstance(adjustment.delta, int)
        assert isinstance(adjustment.submitted_value, int)

    def test_positive_delta(self) -> None:
        """Test adjustment with positive delta."""
        adjustment = UltrasoundAdjustment(
            slot=100,
            adjusted_block_hash="0xhash",
            adjusted_value=2000000000000000000,
            block_number=100,
            builder_pubkey="0xbuilder",
            delta=500000000000000000,  # Positive delta
            submitted_block_hash="0xhash",
            submitted_received_at="2023-01-01T00:00:00Z",
            submitted_value=1500000000000000000,
        )

        assert adjustment.delta > 0
        assert adjustment.adjusted_value > adjustment.submitted_value

    def test_negative_delta(self) -> None:
        """Test adjustment with negative delta."""
        adjustment = UltrasoundAdjustment(
            slot=100,
            adjusted_block_hash="0xhash",
            adjusted_value=1000000000000000000,
            block_number=100,
            builder_pubkey="0xbuilder",
            delta=-500000000000000000,  # Negative delta
            submitted_block_hash="0xhash",
            submitted_received_at="2023-01-01T00:00:00Z",
            submitted_value=1500000000000000000,
        )

        assert adjustment.delta < 0
        assert adjustment.adjusted_value < adjustment.submitted_value
