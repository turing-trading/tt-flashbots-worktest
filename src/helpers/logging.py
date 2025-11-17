"""Logger module."""

import logging
import sys

import colorlog

loggers: dict[str, logging.Logger] = {}


def get_logger(
    name: str,
    log_handler: str = "stdout",
    log_level: str = "INFO",
    log_color: bool = False,
) -> logging.Logger:
    """Get logger.

    Args:
        name: The name of the logger.
        log_handler: The log handler type ('stdout').
        log_level: The logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        log_color: Whether to use colored output.

    Returns:
        logging.Logger: Configured logger instance.

    Raises:
        ValueError: If invalid handler or log level is provided.
    """
    if name in loggers:
        return loggers[name]

    logger = logging.getLogger(name) if not log_color else colorlog.getLogger(name)

    if log_handler == "stdout" and not log_color:
        handler = logging.StreamHandler(sys.stdout)
    elif log_handler == "stdout" and log_color:
        handler = colorlog.StreamHandler(sys.stdout)
    else:
        err_msg = f"Invalid handler: {log_handler}"
        raise ValueError(err_msg)

    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if log_level not in log_levels:
        err_msg = f"Invalid log level: {log_level}"
        raise ValueError(err_msg)

    level = log_levels[log_level]

    logger.setLevel(level)
    handler.setLevel(level)

    if not log_color:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
    else:
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s %(asctime)s - %(name)s - %(levelname)s - %(message)s",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    loggers[name] = logger
    return logger
