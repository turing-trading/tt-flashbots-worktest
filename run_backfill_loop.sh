#!/bin/bash

# Continuous backfill runner
# Runs the relay backfill every 30 seconds, waiting for each run to complete
# Continues even if a run fails

echo "Starting continuous relay backfill (30 second intervals)"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting backfill..."

    # Run the backfill and capture exit code
    poetry run python src/data/relays/backfill.py
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Backfill completed successfully"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ Backfill failed with exit code $exit_code"
    fi

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 30 seconds before next run..."
    echo ""
    sleep 30
done
