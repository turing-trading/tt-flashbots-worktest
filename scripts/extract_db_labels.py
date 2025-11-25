#!/usr/bin/env python3
"""Extract proposer_fee_recipient and labels from database."""

import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

OUTPUT_CSV = Path("/Users/thomas/Downloads/proposer_labels.csv")


def main():
    conn = psycopg.connect(
        host=os.getenv("POSTGRE_HOST"),
        port=os.getenv("POSTGRE_PORT", "5432"),
        user=os.getenv("POSTGRE_USER"),
        password=os.getenv("POSTGRE_PASSWORD"),
        dbname=os.getenv("POSTGRE_DB"),
    )

    cur = conn.cursor()

    # Get aggregated labels per fee_recipient
    query = """
    WITH fee_recipient_labels AS (
        SELECT
            rp.proposer_fee_recipient,
            v.label,
            v.lido_node_operator,
            COUNT(*) as cnt
        FROM relays_payloads rp
        JOIN validators v ON rp.proposer_pubkey = v.pubkey
        WHERE v.label IS NOT NULL AND v.label <> ''
        GROUP BY rp.proposer_fee_recipient, v.label, v.lido_node_operator
    )
    SELECT
        proposer_fee_recipient,
        -- Get the most common label for this fee_recipient
        (SELECT label FROM fee_recipient_labels f2
         WHERE f2.proposer_fee_recipient = f1.proposer_fee_recipient
         ORDER BY cnt DESC LIMIT 1) as label,
        (SELECT lido_node_operator FROM fee_recipient_labels f2
         WHERE f2.proposer_fee_recipient = f1.proposer_fee_recipient
         AND f2.lido_node_operator IS NOT NULL AND f2.lido_node_operator <> ''
         ORDER BY cnt DESC LIMIT 1) as lido_node_operator,
        SUM(cnt) as total_blocks
    FROM fee_recipient_labels f1
    GROUP BY proposer_fee_recipient
    ORDER BY total_blocks DESC
    LIMIT 100
    """

    cur.execute(query)
    rows = cur.fetchall()

    print(f"Found {len(rows)} unique fee recipients with labels:\n")

    # Display results
    for row in rows[:30]:
        fee_recipient = row[0][:10] + "..." + row[0][-6:] if row[0] else "None"
        label = row[1] or ""
        lido_op = row[2] or ""
        blocks = row[3]
        print(f"{fee_recipient}  |  {label:20}  |  {lido_op:20}  |  {blocks:>8} blocks")

    if len(rows) > 30:
        print(f"... and {len(rows) - 30} more")

    # Write to CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proposer_fee_recipient", "label", "lido_node_operator", "total_blocks"])
        for row in rows:
            writer.writerow(row)

    print(f"\nWrote {len(rows)} rows to {OUTPUT_CSV}")

    conn.close()


if __name__ == "__main__":
    main()
