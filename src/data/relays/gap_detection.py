"""Helper functions for detecting and analyzing gaps in relay data."""

from datetime import date, datetime, timedelta
from typing import Any

# Beacon Chain constants
BEACON_GENESIS_TIMESTAMP = 1606824023  # December 1, 2020, 12:00:23 UTC
SECONDS_PER_SLOT = 12
SLOTS_PER_DAY = 7200  # (24 * 60 * 60) / 12


def timestamp_to_slot(timestamp: datetime) -> int:
    """Convert a datetime to approximate beacon chain slot number.

    Args:
        timestamp: Datetime to convert

    Returns:
        int: Approximate slot number

    Example:
        >>> from datetime import datetime
        >>> timestamp_to_slot(datetime(2020, 12, 1, 12, 0, 23))
        0
        >>> timestamp_to_slot(datetime(2020, 12, 2, 12, 0, 23))
        7200
    """
    unix_timestamp = int(timestamp.timestamp())
    seconds_since_genesis = unix_timestamp - BEACON_GENESIS_TIMESTAMP
    return max(0, seconds_since_genesis // SECONDS_PER_SLOT)


def slot_to_timestamp(slot: int) -> datetime:
    """Convert a beacon chain slot number to approximate datetime.

    Args:
        slot: Slot number

    Returns:
        datetime: Approximate datetime

    Example:
        >>> slot_to_timestamp(0)
        datetime.datetime(2020, 12, 1, 12, 0, 23)
        >>> slot_to_timestamp(7200)
        datetime.datetime(2020, 12, 2, 12, 0, 23)
    """
    seconds_since_genesis = slot * SECONDS_PER_SLOT
    unix_timestamp = BEACON_GENESIS_TIMESTAMP + seconds_since_genesis
    return datetime.fromtimestamp(unix_timestamp)


def date_to_slot_range(date_input: date | datetime) -> tuple[int, int]:
    """Convert a date to the slot range for that day.

    Args:
        date_input: Date or datetime (time component ignored for datetime)

    Returns:
        tuple[int, int]: (start_slot, end_slot) for the day

    Example:
        >>> from datetime import datetime
        >>> date_to_slot_range(datetime(2020, 12, 1))
        (0, 7199)
    """
    # Convert to datetime if it's a date object
    if isinstance(date_input, datetime):
        date_dt = date_input
    else:
        # It's a date object
        date_dt = datetime.combine(date_input, datetime.min.time())

    # Start of day
    start_of_day = date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    start_slot = timestamp_to_slot(start_of_day)

    # End of day
    end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
    end_slot = timestamp_to_slot(end_of_day)

    return (start_slot, end_slot)


def detect_outliers(
    value: float, mean: float, stddev: float, threshold_pct: float = 0.5
) -> bool:
    """Detect if a value is an outlier based on statistical thresholds.

    A value is considered an outlier if:
    1. It's less than threshold_pct of the mean (default 50%)
    2. It's more than 2 standard deviations below the mean

    Args:
        value: Value to check
        mean: Mean of the distribution
        stddev: Standard deviation of the distribution
        threshold_pct: Percentage of mean threshold (default 0.5 = 50%)

    Returns:
        bool: True if value is an outlier

    Example:
        >>> detect_outliers(10, 100, 20)  # 10 is <50% of 100
        True
        >>> detect_outliers(80, 100, 20)  # 80 is within normal range
        False
        >>> detect_outliers(50, 100, 20)  # 50 is <2 stddev below mean (100-2*20=60)
        True
    """
    # Check if below percentage threshold
    if value < (mean * threshold_pct):
        return True

    # Check if more than 2 standard deviations below mean
    if stddev > 0 and value < (mean - 2 * stddev):
        return True

    return False


def consolidate_gaps(
    gaps: list[dict[str, Any]], max_gap_slots: int = 7200
) -> list[dict[str, Any]]:
    """Consolidate adjacent or nearby gap ranges for the same relay.

    Merges gaps that are within max_gap_slots of each other to reduce
    the number of API calls needed.

    Args:
        gaps: List of gap dictionaries with relay, from_slot, to_slot
        max_gap_slots: Maximum slot distance to consider gaps adjacent (default 7200 = 1 day)

    Returns:
        list[dict]: Consolidated list of gaps

    Example:
        >>> gaps = [
        ...     {"relay": "r1", "from_slot": 1000, "to_slot": 2000},
        ...     {"relay": "r1", "from_slot": 2001, "to_slot": 3000},
        ...     {"relay": "r2", "from_slot": 1000, "to_slot": 2000},
        ... ]
        >>> consolidate_gaps(gaps, max_gap_slots=100)
        [
            {"relay": "r1", "from_slot": 1000, "to_slot": 3000},
            {"relay": "r2", "from_slot": 1000, "to_slot": 2000},
        ]
    """
    if not gaps:
        return []

    # Group gaps by relay
    gaps_by_relay: dict[str, list[dict[str, Any]]] = {}
    for gap in gaps:
        relay = gap["relay"]
        if relay not in gaps_by_relay:
            gaps_by_relay[relay] = []
        gaps_by_relay[relay].append(gap)

    # Consolidate gaps for each relay
    consolidated = []
    for _, relay_gaps in gaps_by_relay.items():
        # Sort by from_slot
        sorted_gaps = sorted(relay_gaps, key=lambda g: g["from_slot"])

        # Merge adjacent gaps
        current_gap = sorted_gaps[0].copy()
        for next_gap in sorted_gaps[1:]:
            # Check if gaps are adjacent or overlapping
            gap_distance = next_gap["from_slot"] - current_gap["to_slot"]
            if gap_distance <= max_gap_slots:
                # Merge gaps
                current_gap["to_slot"] = max(
                    current_gap["to_slot"], next_gap["to_slot"]
                )
                # Combine metadata if present
                if "dates" in current_gap and "dates" in next_gap:
                    current_gap["dates"].extend(next_gap["dates"])
            else:
                # Save current gap and start new one
                consolidated.append(current_gap)
                current_gap = next_gap.copy()

        # Add the last gap
        consolidated.append(current_gap)

    return consolidated


def estimate_missing_blocks(from_slot: int, to_slot: int) -> int:
    """Estimate the number of missing blocks in a slot range.

    Assumes one block per slot (not accounting for missed slots).

    Args:
        from_slot: Start slot (inclusive)
        to_slot: End slot (inclusive)

    Returns:
        int: Estimated number of missing blocks

    Example:
        >>> estimate_missing_blocks(1000, 2000)
        1001
        >>> estimate_missing_blocks(0, 7199)
        7200
    """
    return max(0, to_slot - from_slot + 1)


def format_gap_summary(gaps: list[dict[str, Any]]) -> str:
    """Format a human-readable summary of gaps.

    Args:
        gaps: List of gap dictionaries

    Returns:
        str: Formatted summary

    Example:
        >>> gaps = [
        ...     {"relay": "titanrelay.xyz", "from_slot": 1000, "to_slot": 8199, "dates": ["2020-12-01"]},
        ... ]
        >>> print(format_gap_summary(gaps))
        titanrelay.xyz: 1 gap(s), ~7,200 blocks
          - Slots 1000-8199 (1 day(s))
    """
    if not gaps:
        return "No gaps detected"

    # Group by relay
    gaps_by_relay: dict[str, list[dict[str, Any]]] = {}
    for gap in gaps:
        relay = gap["relay"]
        if relay not in gaps_by_relay:
            gaps_by_relay[relay] = []
        gaps_by_relay[relay].append(gap)

    # Format summary
    lines = []
    for relay, relay_gaps in sorted(gaps_by_relay.items()):
        total_blocks = sum(
            estimate_missing_blocks(g["from_slot"], g["to_slot"]) for g in relay_gaps
        )
        lines.append(f"{relay}: {len(relay_gaps)} gap(s), ~{total_blocks:,} blocks")

        for gap in relay_gaps:
            from_slot = gap["from_slot"]
            to_slot = gap["to_slot"]
            blocks = estimate_missing_blocks(from_slot, to_slot)

            dates_info = ""
            if "dates" in gap and gap["dates"]:
                dates_info = f" ({len(gap['dates'])} day(s))"

            lines.append(
                f"  - Slots {from_slot}-{to_slot} (~{blocks:,} blocks){dates_info}"
            )

    return "\n".join(lines)
