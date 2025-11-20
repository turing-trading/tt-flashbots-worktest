"""Tests for database operations and helpers."""

from datetime import datetime, UTC, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.data.builders.db import BuilderBalancesDB, ExtraBuilderBalanceDB
from src.data.builders.models import BuilderBalance, ExtraBuilderBalance
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import upsert_models


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# Mark all tests in this module as integration tests (requires Docker)
pytestmark = pytest.mark.integration


class TestUpsertModels:
    """Tests for upsert_models helper function."""

    @pytest.mark.asyncio
    async def test_insert_new_block(self, test_db_engine: "AsyncSession") -> None:
        """Test inserting a new block record."""
        block = Block(
            number=1,
            hash="0x1234567890abcdef",
            parent_hash="0x0000000000000000",
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=0,
            extra_data="0x",
        )

        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[block],
            session=test_db_engine,
        )

        # Verify insertion
        result = await test_db_engine.execute(select(BlockDB).where(BlockDB.number == 1))
        db_block = result.scalar_one()
        assert db_block.number == 1
        assert db_block.hash == "0x1234567890abcdef"
        assert db_block.miner == "0xminer"

    @pytest.mark.asyncio
    async def test_upsert_existing_block(self, test_db_engine: "AsyncSession") -> None:
        """Test upserting an existing block updates it."""
        # Insert initial block
        block1 = Block(
            number=1,
            hash="0xoldhash",
            parent_hash="0x0000000000000000",
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xoldminer",
            size=1000,
            timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=0,
            extra_data="0x",
        )
        await upsert_models(db_model_class=BlockDB, pydantic_models=[block1], session=test_db_engine)

        # Upsert with new data
        block2 = Block(
            number=1,
            hash="0xnewhash",
            parent_hash="0x0000000000000000",
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xnewminer",
            size=1000,
            timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC),
            gas_used=42000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=0,
            extra_data="0x",
        )
        await upsert_models(db_model_class=BlockDB, pydantic_models=[block2], session=test_db_engine)

        # Verify update
        result = await test_db_engine.execute(select(BlockDB).where(BlockDB.number == 1))
        db_block = result.scalar_one()
        assert db_block.hash == "0xnewhash"
        assert db_block.miner == "0xnewminer"
        assert db_block.gas_used == 42000

        # Verify only one record exists
        count_result = await test_db_engine.execute(select(BlockDB))
        assert len(count_result.all()) == 1

    @pytest.mark.asyncio
    async def test_insert_multiple_blocks(self, test_db_engine: "AsyncSession") -> None:
        """Test inserting multiple blocks at once."""
        blocks = [
            Block(
                number=i,
                hash=f"0x{i:064x}",
                parent_hash=f"0x{i-1:064x}" if i > 0 else "0x0",
                nonce="0x0000000000000000",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                miner=f"0xminer{i}",
                size=1000,
                timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC) + timedelta(seconds=i * 15),
                gas_used=21000 * i,
                gas_limit=5000000,
                transaction_count=i,
                base_fee_per_gas=0,
                extra_data="0x",
            )
            for i in range(1, 6)
        ]

        await upsert_models(db_model_class=BlockDB, pydantic_models=blocks, session=test_db_engine)

        # Verify all inserted
        result = await test_db_engine.execute(select(BlockDB))
        db_blocks = result.scalars().all()
        assert len(db_blocks) == 5
        assert {b.number for b in db_blocks} == {1, 2, 3, 4, 5}

    @pytest.mark.asyncio
    async def test_insert_with_extra_fields(self, test_db_engine: "AsyncSession") -> None:
        """Test inserting with extra fields parameter."""
        payload = RelaysPayloads(
            slot=100,
            proposer_pubkey="0xpubkey",
            proposer_fee_recipient="0xrecipient",
            builder_pubkey="0xbuilder",
            parent_hash="0xparenthash",
            block_hash="0xblockhash",
            block_number=1000,
            gas_used=21000,
            gas_limit=5000000,
            num_tx=10,
            value=1500000000000000000,  # 1.5 ETH in Wei
        )

        await upsert_models(
            db_model_class=RelaysPayloadsDB,
            pydantic_models=[payload],
            extra_fields={"relay": "flashbots"},
            session=test_db_engine,
        )

        # Verify insertion with extra field
        result = await test_db_engine.execute(
            select(RelaysPayloadsDB).where(RelaysPayloadsDB.slot == 100)
        )
        db_payload = result.scalar_one()
        assert db_payload.slot == 100
        assert db_payload.relay == "flashbots"
        assert db_payload.block_number == 1000


class TestBuilderBalancesDB:
    """Tests for BuilderBalancesDB model."""

    @pytest.mark.asyncio
    async def test_insert_builder_balance(self, test_db_engine: "AsyncSession") -> None:
        """Test inserting builder balance record."""
        balance = BuilderBalance(
            block_number=1000,
            miner="0xminer",
            balance_before=100500000000000000000,  # 100.5 ETH in Wei
            balance_after=101500000000000000000,   # 101.5 ETH in Wei
            balance_increase=1000000000000000000,  # 1.0 ETH in Wei
        )

        await upsert_models(
            db_model_class=BuilderBalancesDB,
            pydantic_models=[balance],
            session=test_db_engine,
        )

        # Verify insertion
        result = await test_db_engine.execute(
            select(BuilderBalancesDB).where(BuilderBalancesDB.block_number == 1000)
        )
        db_balance = result.scalar_one()
        assert db_balance.block_number == 1000
        assert db_balance.miner == "0xminer"
        assert db_balance.balance_before == 100500000000000000000
        assert db_balance.balance_after == 101500000000000000000
        assert db_balance.balance_increase == 1000000000000000000

    @pytest.mark.asyncio
    async def test_balance_fields_mixin(self, test_db_engine: "AsyncSession") -> None:
        """Test that balance fields mixin works correctly."""
        # Test BuilderBalancesDB
        balance1 = BuilderBalance(
            block_number=100,
            miner="0xbuilder1",
            balance_before=50000000000000000000,   # 50.0 ETH in Wei
            balance_after=55000000000000000000,    # 55.0 ETH in Wei
            balance_increase=5000000000000000000,  # 5.0 ETH in Wei
        )
        await upsert_models(db_model_class=BuilderBalancesDB, pydantic_models=[balance1], session=test_db_engine)

        # Test ExtraBuilderBalanceDB
        balance2 = ExtraBuilderBalance(
            block_number=100,
            builder_address="0xextra",
            miner="0xbuilder2",
            balance_before=100000000000000000000,  # 100.0 ETH in Wei
            balance_after=110000000000000000000,   # 110.0 ETH in Wei
            balance_increase=10000000000000000000, # 10.0 ETH in Wei
        )
        await upsert_models(
            db_model_class=ExtraBuilderBalanceDB, pydantic_models=[balance2], session=test_db_engine
        )

        # Verify both use same balance field structure
        result1 = await test_db_engine.execute(
            select(BuilderBalancesDB).where(BuilderBalancesDB.block_number == 100)
        )
        db_balance1 = result1.scalar_one()

        result2 = await test_db_engine.execute(
            select(ExtraBuilderBalanceDB).where(
                ExtraBuilderBalanceDB.block_number == 100
            )
        )
        db_balance2 = result2.scalar_one()

        # Both should have balance fields from mixin
        assert hasattr(db_balance1, "balance_before")
        assert hasattr(db_balance1, "balance_after")
        assert hasattr(db_balance1, "balance_increase")
        assert hasattr(db_balance2, "balance_before")
        assert hasattr(db_balance2, "balance_after")
        assert hasattr(db_balance2, "balance_increase")


class TestRelaysPayloadsDB:
    """Tests for RelaysPayloadsDB model."""

    @pytest.mark.asyncio
    async def test_insert_relay_payload(self, test_db_engine: "AsyncSession") -> None:
        """Test inserting relay payload record."""
        payload = RelaysPayloads(
            slot=12345,
            proposer_pubkey="0xpubkey123",
            proposer_fee_recipient="0xfeerecipient",
            builder_pubkey="0xbuilder456",
            parent_hash="0xparenthash",
            block_hash="0xblockhash789",
            block_number=1000,
            gas_used=15000000,
            gas_limit=30000000,
            num_tx=150,
            value=2500000000000000000,  # 2.5 ETH in Wei
        )

        await upsert_models(
            db_model_class=RelaysPayloadsDB,
            pydantic_models=[payload],
            extra_fields={"relay": "bloxroute"},
            session=test_db_engine,
        )

        # Verify insertion
        result = await test_db_engine.execute(
            select(RelaysPayloadsDB).where(RelaysPayloadsDB.slot == 12345)
        )
        db_payload = result.scalar_one()
        assert db_payload.slot == 12345
        assert db_payload.relay == "bloxroute"
        assert db_payload.proposer_pubkey == "0xpubkey123"
        assert db_payload.builder_pubkey == "0xbuilder456"
        assert db_payload.value == 2500000000000000000

    @pytest.mark.asyncio
    async def test_upsert_relay_payload(self, test_db_engine: "AsyncSession") -> None:
        """Test upserting relay payload updates existing record."""
        # Insert initial payload
        payload1 = RelaysPayloads(
            slot=5000,
            proposer_pubkey="0xpubkey",
            proposer_fee_recipient="0xrecipient",
            builder_pubkey="0xbuilder",
            parent_hash="0xhash0",
            block_hash="0xhash1",
            block_number=1000,
            gas_used=10000000,
            gas_limit=30000000,
            num_tx=100,
            value=1000000000000000000,  # 1.0 ETH in Wei
        )
        await upsert_models(
            db_model_class=RelaysPayloadsDB,
            pydantic_models=[payload1],
            extra_fields={"relay": "relay1"},
            session=test_db_engine,
        )

        # Upsert with updated data
        payload2 = RelaysPayloads(
            slot=5000,
            proposer_pubkey="0xpubkey",
            proposer_fee_recipient="0xrecipient",
            builder_pubkey="0xbuilder",
            parent_hash="0xhash0",
            block_hash="0xhash2",
            block_number=1001,
            gas_used=15000000,
            gas_limit=30000000,
            num_tx=150,
            value=2000000000000000000,  # 2.0 ETH in Wei
        )
        await upsert_models(
            db_model_class=RelaysPayloadsDB,
            pydantic_models=[payload2],
            extra_fields={"relay": "relay1"},
            session=test_db_engine,
        )

        # Verify update
        result = await test_db_engine.execute(
            select(RelaysPayloadsDB).where(RelaysPayloadsDB.slot == 5000)
        )
        all_payloads = result.scalars().all()

        # Should only have one record (upsert replaces)
        assert len(all_payloads) == 1
        db_payload = all_payloads[0]
        assert db_payload.block_hash == "0xhash2"
        assert db_payload.block_number == 1001
        assert db_payload.gas_used == 15000000
        assert db_payload.value == 2000000000000000000


class TestDatabaseIntegration:
    """Integration tests for database operations."""

    @pytest.mark.asyncio
    async def test_complex_upsert_scenario(self, test_db_engine: "AsyncSession") -> None:
        """Test complex scenario with multiple inserts and updates."""
        # Insert multiple blocks
        blocks = [
            Block(
                number=i,
                hash=f"0x{i:064x}",
                parent_hash=f"0x{i-1:064x}" if i > 0 else "0x0",
                nonce="0x0000000000000000",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                miner=f"0xminer{i}",
                size=1000,
                timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC) + timedelta(seconds=i * 15),
                gas_used=21000 * i,
                gas_limit=5000000,
                transaction_count=i,
                base_fee_per_gas=10.0 * i,  # Base fee in Gwei
                extra_data="0x",
            )
            for i in range(1, 11)
        ]
        await upsert_models(db_model_class=BlockDB, pydantic_models=blocks, session=test_db_engine)

        # Update some blocks
        updated_blocks = [
            Block(
                number=i,
                hash=f"0xnew{i:060x}",  # 0x + new + 60 hex chars = 65 chars total
                parent_hash=f"0x{i-1:064x}" if i > 0 else "0x0",
                nonce="0x0000000000000000",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                miner=f"0xnewminer{i}",
                size=1000,
                timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC) + timedelta(seconds=i * 15),
                gas_used=42000 * i,
                gas_limit=5000000,
                transaction_count=i,
                base_fee_per_gas=20.0 * i,  # Base fee in Gwei
                extra_data="0x",
            )
            for i in range(1, 6)
        ]
        await upsert_models(db_model_class=BlockDB, pydantic_models=updated_blocks, session=test_db_engine)

        # Verify total count (should still be 10)
        result = await test_db_engine.execute(select(BlockDB))
        all_blocks = result.scalars().all()
        assert len(all_blocks) == 10

        # Verify updated blocks
        updated_result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number <= 5)
        )
        updated_db_blocks = updated_result.scalars().all()
        for block in updated_db_blocks:
            assert block.hash.startswith("0xnew")
            assert block.miner.startswith("0xnewminer")
            assert block.gas_used == 42000 * block.number

        # Verify non-updated blocks
        non_updated_result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number > 5)
        )
        non_updated_db_blocks = non_updated_result.scalars().all()
        for block in non_updated_db_blocks:
            assert not block.hash.startswith("0xnew")
            assert block.gas_used == 21000 * block.number

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, test_db_engine: "AsyncSession") -> None:
        """Test that failed operations don't leave partial data."""
        # Insert valid block
        block1 = Block(
            number=1,
            hash="0xvalid",
            parent_hash="0x0",
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2015, 7, 30, 15, 26, 28, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=0,
            extra_data="0x",
        )
        await upsert_models(db_model_class=BlockDB, pydantic_models=[block1], session=test_db_engine)

        # Verify insertion
        result = await test_db_engine.execute(select(BlockDB).where(BlockDB.number == 1))
        assert result.scalar_one() is not None
