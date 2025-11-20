"""Common configuration constants used across the application."""

# Batch Size Constants
DEFAULT_BATCH_SIZE = 1000
"""Default number of items to process per batch"""

SMALL_BATCH_SIZE = 100
"""Small batch size for memory-intensive operations"""

LARGE_BATCH_SIZE = 10_000
"""Large batch size for simple operations"""

RPC_BATCH_SIZE = 10
"""Default number of RPC calls per batch request"""

DB_BATCH_SIZE = 100
"""Default number of records to insert per database batch"""

# HTTP and Network Constants
DEFAULT_TIMEOUT = 30.0
"""Default HTTP request timeout in seconds"""

EXTENDED_TIMEOUT = 60.0
"""Extended timeout for slow endpoints"""

CONNECTION_TIMEOUT = 3.0
"""Timeout for establishing connections"""

# Retry Configuration
MAX_RETRIES = 5
"""Default maximum number of retry attempts"""

RETRY_BASE_DELAY = 1.0
"""Base delay for exponential backoff in seconds"""

RETRY_MAX_DELAY = 60.0
"""Maximum delay between retries in seconds"""

# Concurrency Limits
DEFAULT_PARALLEL_BATCHES = 5
"""Default number of batch requests to run in parallel"""

HIGH_PARALLEL_BATCHES = 30
"""High concurrency for fast operations"""

VERY_HIGH_PARALLEL_BATCHES = 100
"""Very high concurrency for simple RPC calls"""

# Database Limits
POSTGRES_PARAM_LIMIT = 65_535
"""PostgreSQL's parameter limit for prepared statements"""

# Block and Slot Constants
SLOT_JUMP_SIZE = 50_000
"""Number of slots to jump when searching for data (~1.7 days)"""

GENESIS_DATE = "2015-07-30"
"""Ethereum genesis block date"""

# HTTP Connection Pooling
MAX_KEEPALIVE_CONNECTIONS = 5
"""Maximum number of keepalive connections in pool"""

MAX_CONNECTIONS = 10
"""Maximum total number of connections"""


__all__ = [
    "CONNECTION_TIMEOUT",
    "DB_BATCH_SIZE",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_PARALLEL_BATCHES",
    "DEFAULT_TIMEOUT",
    "EXTENDED_TIMEOUT",
    "GENESIS_DATE",
    "HIGH_PARALLEL_BATCHES",
    "LARGE_BATCH_SIZE",
    "MAX_CONNECTIONS",
    "MAX_KEEPALIVE_CONNECTIONS",
    "MAX_RETRIES",
    "POSTGRES_PARAM_LIMIT",
    "RETRY_BASE_DELAY",
    "RETRY_MAX_DELAY",
    "RPC_BATCH_SIZE",
    "SLOT_JUMP_SIZE",
    "SMALL_BATCH_SIZE",
    "VERY_HIGH_PARALLEL_BATCHES",
]
