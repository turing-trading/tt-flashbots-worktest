"""Tests for database helper functions."""

from datetime import UTC, datetime
import os

import pytest

from typing import TYPE_CHECKING, Any, Never

from sqlalchemy import select


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.helpers.db import get_database_url, upsert_model, upsert_models


class TestUpsertModelUnit:
    """Unit tests for upsert_model function with mocking."""

    @pytest.mark.asyncio
    async def test_upsert_model_calls_upsert_models(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that upsert_model calls upsert_models correctly."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock, MagicMock

        from src.data.blocks.models import Block

        # Mock AsyncSessionLocal and session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()

        class MockSessionContext:
            async def __aenter__(self) -> Any:
                return mock_session

            async def __aexit__(
                self, exc_type: object, exc_val: object, exc_tb: object
            ) -> None:
                return None

        mock_session_factory = MagicMock(return_value=MockSessionContext())
        monkeypatch.setattr("src.helpers.db.AsyncSessionLocal", mock_session_factory)

        # Mock _perform_upsert to avoid actual database operations
        perform_upsert_called = False

        async def mock_perform_upsert(*args: object, **kwargs: object) -> None:
            nonlocal perform_upsert_called
            perform_upsert_called = True

        monkeypatch.setattr("src.helpers.db._perform_upsert", mock_perform_upsert)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call upsert_model - this covers line 101
        from src.data.blocks.db import BlockDB

        await upsert_model(db_model_class=BlockDB, pydantic_model=block)

        # Verify _perform_upsert was called
        assert perform_upsert_called


class TestUpsertModelsUnit:
    """Unit tests for upsert_models function with mocking."""

    @pytest.mark.asyncio
    async def test_upsert_models_production_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test upsert_models production path without session parameter."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock, MagicMock

        from sqlalchemy import inspect as sa_inspect

        from src.data.blocks.db import BlockDB
        from src.data.blocks.models import Block
        from src.helpers.db import upsert_models

        # Mock AsyncSessionLocal and session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()

        class MockSessionContext:
            async def __aenter__(self) -> Any:
                return mock_session

            async def __aexit__(
                self, exc_type: object, exc_val: object, exc_tb: object
            ) -> None:
                return None

        mock_session_factory = MagicMock(return_value=MockSessionContext())
        monkeypatch.setattr("src.helpers.db.AsyncSessionLocal", mock_session_factory)

        # Mock inspect for _perform_upsert
        class MockColumn:  # noqa: B903
            def __init__(self, name: str) -> None:
                self.name = name

        class MockMapper:
            primary_key = [MockColumn("number"), MockColumn("hash")]

        def mock_inspect(obj: object) -> Any:
            if obj == BlockDB:
                return MockMapper()
            return sa_inspect(obj)

        monkeypatch.setattr("src.helpers.db.inspect", mock_inspect)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call without session parameter - covers lines 190-200
        await upsert_models(db_model_class=BlockDB, pydantic_models=[block])

        # Verify commit was called (line 200)
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_upsert_models_with_provided_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test upsert_models with provided session parameter."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock

        from sqlalchemy import inspect as sa_inspect

        from src.data.blocks.db import BlockDB
        from src.data.blocks.models import Block
        from src.helpers.db import upsert_models

        # Mock session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        # Mock inspect
        class MockColumn:  # noqa: B903
            def __init__(self, name: str) -> None:
                self.name = name

        class MockMapper:
            primary_key = [MockColumn("number"), MockColumn("hash")]

        def mock_inspect(obj: object) -> Any:
            if obj == BlockDB:
                return MockMapper()
            return sa_inspect(obj)

        monkeypatch.setattr("src.helpers.db.inspect", mock_inspect)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call WITH session parameter - covers line 192
        await upsert_models(
            db_model_class=BlockDB, pydantic_models=[block], session=mock_session
        )

        # Verify execute was called
        assert mock_session.execute.called

    @pytest.mark.asyncio
    async def test_upsert_models_rollback_on_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test upsert_models rolls back on error."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock, MagicMock

        from src.data.blocks.db import BlockDB
        from src.data.blocks.models import Block
        from src.helpers.db import upsert_models

        # Mock session that raises error
        mock_session = AsyncMock()
        mock_session.rollback = AsyncMock()

        class MockSessionContext:
            async def __aenter__(self) -> Any:
                return mock_session

            async def __aexit__(
                self, exc_type: object, exc_val: object, exc_tb: object
            ) -> None:
                return None

        mock_session_factory = MagicMock(return_value=MockSessionContext())
        monkeypatch.setattr("src.helpers.db.AsyncSessionLocal", mock_session_factory)

        # Mock _perform_upsert to raise an error
        async def mock_perform_upsert(*args: object, **kwargs: object) -> Never:
            msg = "Test error"
            raise RuntimeError(msg)

        monkeypatch.setattr("src.helpers.db._perform_upsert", mock_perform_upsert)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call should raise and rollback should be called - covers lines 196-198
        with pytest.raises(RuntimeError, match="Test error"):
            await upsert_models(db_model_class=BlockDB, pydantic_models=[block])

        # Verify rollback was called
        assert mock_session.rollback.called


class TestPerformUpsertUnit:
    """Unit tests for _perform_upsert function with mocking."""

    @pytest.mark.asyncio
    async def test_perform_upsert_logic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test _perform_upsert internal logic with mocked database."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock

        from sqlalchemy import inspect as sa_inspect

        from src.data.blocks.db import BlockDB
        from src.data.blocks.models import Block
        from src.helpers.db import _perform_upsert  # noqa: PLC2701

        # Mock session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        # Mock inspect to return a proper mapper
        class MockColumn:  # noqa: B903
            def __init__(self, name: str) -> None:
                self.name = name

        class MockMapper:
            primary_key = [MockColumn("number"), MockColumn("hash")]

        def mock_inspect(obj: object) -> Any:
            if obj == BlockDB:
                return MockMapper()
            return sa_inspect(obj)

        monkeypatch.setattr("src.helpers.db.inspect", mock_inspect)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call _perform_upsert - covers lines 126-161
        await _perform_upsert(
            db_model_class=BlockDB,
            pydantic_models=[block],
            extra_fields=None,
            session=mock_session,
        )

        # Verify execute was called
        assert mock_session.execute.called

    @pytest.mark.asyncio
    async def test_perform_upsert_with_empty_list(self) -> None:
        """Test _perform_upsert early return with empty list."""
        from unittest.mock import AsyncMock

        from src.data.blocks.db import BlockDB
        from src.helpers.db import _perform_upsert  # noqa: PLC2701

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        # Call with empty list - should return early (line 142)
        await _perform_upsert(
            db_model_class=BlockDB,
            pydantic_models=[],
            extra_fields=None,
            session=mock_session,
        )

        # Execute should NOT be called
        assert not mock_session.execute.called

    @pytest.mark.asyncio
    async def test_perform_upsert_invalid_model_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test _perform_upsert raises ValueError for uninspectable model."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock

        from src.data.blocks.models import Block
        from src.helpers.db import _perform_upsert  # noqa: PLC2701

        # Mock inspect to return None
        monkeypatch.setattr("src.helpers.db.inspect", lambda _: None)

        mock_session = AsyncMock()

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Should raise ValueError - covers lines 136-137
        with pytest.raises(ValueError, match="Cannot inspect"):
            await _perform_upsert(
                db_model_class=object,  # type: ignore[arg-type]
                pydantic_models=[block],
                extra_fields=None,
                session=mock_session,
            )

    @pytest.mark.asyncio
    async def test_perform_upsert_with_extra_fields(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test _perform_upsert with extra_fields parameter."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock

        from sqlalchemy import inspect as sa_inspect

        from src.data.blocks.db import BlockDB
        from src.data.blocks.models import Block
        from src.helpers.db import _perform_upsert  # noqa: PLC2701

        # Mock session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        # Mock inspect to return proper mapper
        class MockColumn:  # noqa: B903
            def __init__(self, name: str) -> None:
                self.name = name

        class MockMapper:
            primary_key = [MockColumn("number"), MockColumn("hash")]

        def mock_inspect(obj: object) -> Any:
            if obj == BlockDB:
                return MockMapper()
            return sa_inspect(obj)

        monkeypatch.setattr("src.helpers.db.inspect", mock_inspect)

        block = Block(
            number=1,
            hash="0x" + "a" * 64,
            parent_hash="0x" + "0" * 64,
            nonce="0x0000000000000000",
            sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            state_root="0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
            receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            miner="0xminer",
            size=1000,
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            gas_used=21000,
            gas_limit=5000000,
            transaction_count=1,
            base_fee_per_gas=10.0,
            extra_data="0x",
        )

        # Call _perform_upsert with extra_fields - covers lines 129-131
        await _perform_upsert(
            db_model_class=BlockDB,
            pydantic_models=[block],
            extra_fields={"miner": "0xoverridden"},  # Use a real Block field
            session=mock_session,
        )

        # Verify execute was called
        assert mock_session.execute.called


class TestCreateTablesUnit:
    """Unit tests for create_tables function with mocking."""

    @pytest.mark.asyncio
    async def test_create_tables_calls_create_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test create_tables calls Base.metadata.create_all."""
        from unittest.mock import MagicMock

        from src.helpers.db import create_tables

        # Track if run_sync was called (line 218)
        run_sync_called = False

        class MockConnection:
            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(
                self, exc_type: object, exc_val: object, exc_tb: object
            ) -> None:
                return None

            async def run_sync(self, _func: object) -> None:
                nonlocal run_sync_called
                run_sync_called = True
                # Don't actually call _func to avoid needing real database

        # Mock async_engine
        mock_engine = MagicMock()
        mock_engine.begin = MagicMock(return_value=MockConnection())
        monkeypatch.setattr("src.helpers.db.async_engine", mock_engine)

        # Call create_tables - covers lines 217-218
        await create_tables()

        # Verify run_sync was called (line 218)
        assert run_sync_called


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
    async def test_upsert_model_single_with_session(
        self, test_db_engine: AsyncSession
    ) -> None:
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
    async def test_upsert_model_multiple_with_session(
        self, test_db_engine: AsyncSession
    ) -> None:
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
        assert db_blocks[0].number in {1001, 1002}
        assert db_blocks[1].number in {1001, 1002}

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_upsert_model_update_existing(
        self, test_db_engine: AsyncSession
    ) -> None:
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
    async def test_upsert_models_empty_list(self, test_db_engine: AsyncSession) -> None:
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
        self, test_db_engine: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test upsert_model wrapper calls upsert_models correctly."""
        from unittest.mock import AsyncMock, MagicMock

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
    async def test_upsert_models_rollback_on_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that upsert_models rolls back on error in production path."""
        from unittest.mock import AsyncMock, MagicMock

        from src.helpers.db import upsert_models

        # Create a mock session that raises an error during execute
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=Exception("Database error"))
        mock_session.rollback = AsyncMock()
        mock_session.commit = AsyncMock()

        # Create a proper async context manager mock
        class MockSessionContext:
            async def __aenter__(self) -> Any:
                return mock_session

            async def __aexit__(
                self, exc_type: object, exc_val: object, exc_tb: object
            ) -> None:
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
    async def test_create_tables(
        self, test_db_engine: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test create_tables function creates database tables."""
        from src.helpers.db import create_tables

        # Patch the async_engine to use our test engine
        monkeypatch.setattr("src.helpers.db.async_engine", test_db_engine.bind)

        # Call create_tables - should not raise
        await create_tables()

        # Verify tables exist by querying one
        await test_db_engine.execute(select(BlockDB).limit(1))
        # If we get here without error, tables were created successfully


class TestPerformUpsertErrorHandling:
    """Tests for _perform_upsert error handling."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_perform_upsert_invalid_model_class(
        self, test_db_engine: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test _perform_upsert handles uninspectable model by checking mapper."""
        import sqlalchemy

        from src.helpers.db import upsert_models

        # Mock inspect to return None (simulating uninspectable model)
        original_inspect = sqlalchemy.inspect

        def mock_inspect(obj: object, raiseerr: bool = True) -> Any:
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
