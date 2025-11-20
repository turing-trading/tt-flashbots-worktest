"""Tests for database helper functions."""

import os
from datetime import datetime, UTC

import pytest
from sqlalchemy import select

from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.helpers.db import get_database_url, upsert_model, upsert_models


class TestGetDatabaseUrl:
    """Tests for get_database_url function."""

    def test_get_database_url_default_behavior(self) -> None:
        """Test get_database_url default behavior."""
        # Save current env
        saved_vars = {
            "POSTGRE_HOST": os.environ.get("POSTGRE_HOST"),
            "POSTGRE_PORT": os.environ.get("POSTGRE_PORT"),
            "POSTGRE_USER": os.environ.get("POSTGRE_USER"),
            "POSTGRE_PASSWORD": os.environ.get("POSTGRE_PASSWORD"),
            "POSTGRE_DB": os.environ.get("POSTGRE_DB"),
        }

        try:
            # Set test environment variables
            os.environ["POSTGRE_HOST"] = "localhost"
            os.environ["POSTGRE_PORT"] = "5432"
            os.environ["POSTGRE_USER"] = "test_user"
            os.environ["POSTGRE_PASSWORD"] = "test_password"
            os.environ["POSTGRE_DB"] = "test_db"

            url = get_database_url()

            assert url is not None
            assert "postgresql+psycopg" in url
            assert "test_user:test_password" in url
            assert "localhost:5432" in url
            assert "test_db" in url
        finally:
            # Restore env
            for key, value in saved_vars.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]

    def test_get_database_url_missing_host(self) -> None:
        """Test get_database_url raises when POSTGRE_HOST is missing."""
        saved_host = os.environ.get("POSTGRE_HOST")

        try:
            if "POSTGRE_HOST" in os.environ:
                del os.environ["POSTGRE_HOST"]

            with pytest.raises(ValueError, match="POSTGRE_HOST is not set"):
                get_database_url()
        finally:
            if saved_host:
                os.environ["POSTGRE_HOST"] = saved_host

    def test_get_database_url_missing_user(self) -> None:
        """Test get_database_url raises when POSTGRE_USER is missing."""
        saved_vars = {
            "POSTGRE_HOST": os.environ.get("POSTGRE_HOST"),
            "POSTGRE_USER": os.environ.get("POSTGRE_USER"),
        }

        try:
            os.environ["POSTGRE_HOST"] = "localhost"
            if "POSTGRE_USER" in os.environ:
                del os.environ["POSTGRE_USER"]

            with pytest.raises(ValueError, match="POSTGRE_USER is not set"):
                get_database_url()
        finally:
            for key, value in saved_vars.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]

    def test_get_database_url_missing_password(self) -> None:
        """Test get_database_url raises when POSTGRE_PASSWORD is missing."""
        saved_vars = {
            "POSTGRE_HOST": os.environ.get("POSTGRE_HOST"),
            "POSTGRE_USER": os.environ.get("POSTGRE_USER"),
            "POSTGRE_PASSWORD": os.environ.get("POSTGRE_PASSWORD"),
        }

        try:
            os.environ["POSTGRE_HOST"] = "localhost"
            os.environ["POSTGRE_USER"] = "test"
            if "POSTGRE_PASSWORD" in os.environ:
                del os.environ["POSTGRE_PASSWORD"]

            with pytest.raises(ValueError, match="POSTGRE_PASSWORD is not set"):
                get_database_url()
        finally:
            for key, value in saved_vars.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]

    def test_get_database_url_missing_db(self) -> None:
        """Test get_database_url raises when POSTGRE_DB is missing."""
        saved_vars = {
            "POSTGRE_HOST": os.environ.get("POSTGRE_HOST"),
            "POSTGRE_USER": os.environ.get("POSTGRE_USER"),
            "POSTGRE_PASSWORD": os.environ.get("POSTGRE_PASSWORD"),
            "POSTGRE_DB": os.environ.get("POSTGRE_DB"),
        }

        try:
            os.environ["POSTGRE_HOST"] = "localhost"
            os.environ["POSTGRE_USER"] = "test"
            os.environ["POSTGRE_PASSWORD"] = "pass"
            if "POSTGRE_DB" in os.environ:
                del os.environ["POSTGRE_DB"]

            with pytest.raises(ValueError, match="POSTGRE_DB is not set"):
                get_database_url()
        finally:
            for key, value in saved_vars.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]


class TestUpsertModel:
    """Tests for upsert_model singular function."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_model_single_with_session(self, test_db_engine) -> None:
        """Test upserting a single model using upsert_model with session."""
        block = Block(
            number=999,
            hash="0x" + "f" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xsingleminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Use upsert_models with session to test the session path
        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[block],
            session=test_db_engine,
        )

        # Verify insertion
        result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number == 999)
        )
        db_block = result.scalar_one()
        assert db_block.number == 999
        assert db_block.miner == "0xsingleminer"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_model_multiple_with_session(self, test_db_engine) -> None:
        """Test upserting multiple models with session."""
        blocks = [
            Block(
                number=1001,
                hash="0x" + "a" * 64,
                parent_hash="0x" + "0" * 64,
                nonce="0x0000000000000000",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                miner="0xminer1",
                size=1000,
                timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                gas_used=21000,
                gas_limit=5000000,
                transaction_count=1,
                base_fee_per_gas=10.0,
                extra_data="0x",
            ),
            Block(
                number=1002,
                hash="0x" + "b" * 64,
                parent_hash="0x" + "a" * 64,
                nonce="0x0000000000000000",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                miner="0xminer2",
                size=1000,
                timestamp=datetime(2024, 1, 1, 1, 0, 0, tzinfo=UTC),
                gas_used=21000,
                gas_limit=5000000,
                transaction_count=1,
                base_fee_per_gas=10.0,
                extra_data="0x",
            ),
        ]

        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=blocks,
            session=test_db_engine,
        )

        # Verify both blocks were inserted
        result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number.in_([1001, 1002]))
        )
        db_blocks = result.scalars().all()
        assert len(db_blocks) == 2
        assert db_blocks[0].number in [1001, 1002]
        assert db_blocks[1].number in [1001, 1002]

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_model_update_existing(self, test_db_engine) -> None:
        """Test that upsert updates existing records."""
        block = Block(
            number=3000,
            hash="0x" + "c" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xoriginalminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Insert initial block
        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[block],
            session=test_db_engine,
        )

        # Update with new miner
        block.miner = "0xupdatedminer"
        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[block],
            session=test_db_engine,
        )

        # Verify update
        result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number == 3000)
        )
        db_block = result.scalar_one()
        assert db_block.miner == "0xupdatedminer"


class TestUpsertModelsEmptyList:
    """Tests for upsert_models with empty list."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_models_empty_list(self, test_db_engine) -> None:
        """Test upsert_models with empty list (early return)."""
        # Should not raise, just return early
        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[],
            session=test_db_engine,
        )


class TestUpsertModelWrapper:
    """Tests for upsert_model wrapper function."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_model_wrapper_with_mocked_session(
        self, test_db_engine, monkeypatch
    ) -> None:
        """Test upsert_model wrapper calls upsert_models correctly."""
        from unittest.mock import AsyncMock, MagicMock
        from src.helpers.db import upsert_model, AsyncSessionLocal

        # Create a mock session that behaves like the test session
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__.return_value = test_db_engine
        mock_session_factory.return_value.__aexit__ = AsyncMock()

        # Patch AsyncSessionLocal to use our test database
        monkeypatch.setattr("src.helpers.db.AsyncSessionLocal", mock_session_factory)

        block = Block(
            number=4000,
            hash="0x" + "d" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xwrapperminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call upsert_model without session parameter (production path)
        await upsert_model(
            db_model_class=BlockDB,
            pydantic_model=block,
        )

        # Verify the block was inserted
        result = await test_db_engine.execute(
            select(BlockDB).where(BlockDB.number == 4000)
        )
        db_block = result.scalar_one()
        assert db_block.miner == "0xwrapperminer"


class TestUpsertModelsErrorHandling:
    """Tests for upsert_models error handling."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_models_rollback_on_error(self, monkeypatch) -> None:
        """Test that upsert_models rolls back on error in production path."""
        from unittest.mock import AsyncMock, MagicMock, call
        from src.helpers.db import upsert_models

        # Create a mock session that raises an error during execute
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=Exception("Database error"))
        mock_session.rollback = AsyncMock()
        mock_session.commit = AsyncMock()

        # Create a proper async context manager mock
        class MockSessionContext:
            async def __aenter__(self):
                return mock_session

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        mock_session_factory = MagicMock(return_value=MockSessionContext())

        # Patch AsyncSessionLocal to return our mock
        monkeypatch.setattr("src.helpers.db.AsyncSessionLocal", mock_session_factory)

        block = Block(
            number=5000,
            hash="0x" + "e" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xerrorminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call upsert_models without session (production path), should raise
        with pytest.raises(Exception, match="Database error"):
            await upsert_models(
                db_model_class=BlockDB,
                pydantic_models=[block],
            )

        # Verify rollback was called
        assert mock_session.rollback.called


class TestCreateTables:
    """Tests for create_tables function."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_create_tables(self, test_db_engine, monkeypatch) -> None:
        """Test create_tables function creates database tables."""
        from src.helpers.db import create_tables, async_engine

        # Patch the async_engine to use our test engine
        monkeypatch.setattr("src.helpers.db.async_engine", test_db_engine.bind)

        # Call create_tables - should not raise
        await create_tables()

        # Verify tables exist by querying one
        result = await test_db_engine.execute(select(BlockDB).limit(1))
        # If we get here without error, tables were created successfully


class TestPerformUpsertErrorHandling:
    """Tests for _perform_upsert error handling."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_perform_upsert_invalid_model_class(self, test_db_engine, monkeypatch) -> None:
        """Test _perform_upsert handles uninspectable model by checking mapper."""
        from unittest.mock import MagicMock
        from src.helpers.db import upsert_models
        import sqlalchemy

        # Mock inspect to return None (simulating uninspectable model)
        original_inspect = sqlalchemy.inspect

        def mock_inspect(obj, raiseerr=True):
            # Return None for our test case to trigger lines 136-137
            if isinstance(obj, type) and obj.__name__ == "BlockDB":
                return None
            return original_inspect(obj, raiseerr)

        monkeypatch.setattr("src.helpers.db.inspect", mock_inspect)

        block = Block(
            number=6000,
            hash="0x" + "f" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xinvalidminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Try to upsert with mocked inspect returning None - should raise ValueError
        with pytest.raises(ValueError, match="Cannot inspect"):
            await upsert_models(
                db_model_class=BlockDB,
                pydantic_models=[block],
                session=test_db_engine,
            )
