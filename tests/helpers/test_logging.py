"""Tests for logging configuration."""

import pytest

import logging

from src.helpers.logging import get_logger


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_returns_logger(self) -> None:
        """Test that get_logger returns a logger instance."""
        logger = get_logger("test_module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_different_names(self) -> None:
        """Test getting loggers with different names."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        assert logger1.name == "module1"
        assert logger2.name == "module2"
        assert logger1 is not logger2

    def test_get_logger_same_name_returns_same_instance(self) -> None:
        """Test that getting logger with same name returns same instance."""
        logger1 = get_logger("test_same")
        logger2 = get_logger("test_same")

        assert logger1 is logger2

    def test_get_logger_with_debug_level(self) -> None:
        """Test get_logger with DEBUG level."""
        logger = get_logger("test_debug", log_level="DEBUG")

        assert logger.level == logging.DEBUG

    def test_get_logger_with_info_level(self) -> None:
        """Test get_logger with INFO level."""
        logger = get_logger("test_info", log_level="INFO")

        assert logger.level == logging.INFO

    def test_get_logger_with_warning_level(self) -> None:
        """Test get_logger with WARNING level."""
        logger = get_logger("test_warning", log_level="WARNING")

        assert logger.level == logging.WARNING

    def test_get_logger_with_error_level(self) -> None:
        """Test get_logger with ERROR level."""
        logger = get_logger("test_error", log_level="ERROR")

        assert logger.level == logging.ERROR

    def test_get_logger_with_critical_level(self) -> None:
        """Test get_logger with CRITICAL level."""
        logger = get_logger("test_critical", log_level="CRITICAL")

        assert logger.level == logging.CRITICAL

    def test_get_logger_default_level(self) -> None:
        """Test get_logger with default level."""
        logger = get_logger("test_default")

        # Default should be INFO
        assert logger.level == logging.INFO

    def test_get_logger_invalid_level_raises(self) -> None:
        """Test that invalid log level raises ValueError."""
        with pytest.raises(ValueError, match="Invalid log level"):
            get_logger("test_invalid", log_level="INVALID")

    def test_get_logger_invalid_handler_raises(self) -> None:
        """Test that invalid handler raises ValueError."""
        with pytest.raises(ValueError, match="Invalid handler"):
            get_logger("test_invalid_handler", log_handler="invalid")

    def test_get_logger_stdout_handler(self) -> None:
        """Test get_logger with stdout handler."""
        logger = get_logger("test_stdout", log_handler="stdout")

        assert logger is not None
        assert len(logger.handlers) > 0

    def test_get_logger_with_color(self) -> None:
        """Test get_logger with color enabled."""
        logger = get_logger("test_color", log_color=True)

        assert logger is not None
        assert len(logger.handlers) > 0

    def test_get_logger_without_color(self) -> None:
        """Test get_logger with color disabled."""
        logger = get_logger("test_no_color", log_color=False)

        assert logger is not None
        assert len(logger.handlers) > 0

    def test_logger_can_log_messages(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that configured logger can log messages."""
        logger = get_logger("test_log_messages", log_level="DEBUG")

        with caplog.at_level(logging.DEBUG, logger="test_log_messages"):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

        assert any("Debug message" in record.message for record in caplog.records)
        assert any("Info message" in record.message for record in caplog.records)
        assert any("Warning message" in record.message for record in caplog.records)
        assert any("Error message" in record.message for record in caplog.records)


class TestLoggingIntegration:
    """Integration tests for logging functionality."""

    def test_multiple_loggers_independent(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that multiple loggers work independently."""
        logger1 = get_logger("integration_module1")
        logger2 = get_logger("integration_module2")

        with caplog.at_level(logging.INFO):
            logger1.info("Message from module1")
            logger2.info("Message from module2")

        assert any(
            "Message from module1" in record.message for record in caplog.records
        )
        assert any(
            "Message from module2" in record.message for record in caplog.records
        )

    def test_logger_name_hierarchy(self) -> None:
        """Test logger name hierarchy."""
        parent_logger = get_logger("hierarchy_parent")
        child_logger = get_logger("hierarchy_parent.child")

        assert parent_logger.name == "hierarchy_parent"
        assert child_logger.name == "hierarchy_parent.child"

    def test_logger_caching(self) -> None:
        """Test that loggers are cached correctly."""
        # First call creates and caches logger
        logger1 = get_logger("cached_logger", log_level="DEBUG")

        # Second call should return cached logger
        logger2 = get_logger("cached_logger", log_level="INFO")

        # Should be the same instance
        assert logger1 is logger2
        # And should have the original level (DEBUG)
        assert logger1.level == logging.DEBUG
