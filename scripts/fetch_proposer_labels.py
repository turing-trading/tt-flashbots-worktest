#!/usr/bin/env python3
"""Fetch blockchain labels for proposer fee recipients."""

import csv
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BLOCKCHAIN_LABEL_API_KEY")
BASE_URL = "https://www.blockchainlabels.com/api/v1/labels/address/ethereum"
INPUT_CSV = Path("/Users/thomas/Downloads/proposer_fee_recipients.csv")
OUTPUT_CSV = Path("/Users/thomas/Downloads/proposer_labels.csv")
MAX_ROWS = 500
RATE_LIMIT = 5  # requests per second


def fetch_label(address: str) -> list | None:
    """Fetch labels for a single address."""
    url = f"{BASE_URL}/{address}"
    headers = {"X-API-KEY": API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # API returns {"error": null, "data": {"_id": "...", "labels": [...]}}
            if data.get("data") and data["data"].get("labels"):
                return data["data"]["labels"]
            return None
        elif response.status_code == 404:
            return None
        else:
            print(f"Error {response.status_code} for {address}: {response.text}")
            return None
    except requests.RequestException as e:
        print(f"Request failed for {address}: {e}")
        return None


def main():
    if not API_KEY:
        raise ValueError("BLOCKCHAIN_LABEL_API_KEY not found in .env")

    # Read input CSV and get unique addresses (case-insensitive)
    addresses = []
    seen = set()
    with open(INPUT_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= MAX_ROWS:
                break
            addr = row["proposer_fee_recipient"]
            addr_lower = addr.lower()
            if addr_lower not in seen:
                seen.add(addr_lower)
                addresses.append(addr)

    print(f"Found {len(addresses)} unique addresses from first {MAX_ROWS} rows")

    # Fetch labels with rate limiting
    results = []
    request_times: list[float] = []

    for i, addr in enumerate(addresses):
        # Rate limiting: ensure max 5 requests/second
        now = time.time()
        request_times = [t for t in request_times if now - t < 1.0]
        if len(request_times) >= RATE_LIMIT:
            sleep_time = 1.0 - (now - request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

        request_times.append(time.time())

        labels = fetch_label(addr)
        if labels:
            for item in labels:
                results.append({
                    "address": addr,
                    "label": item.get("label", ""),
                    "label_type": item.get("type", ""),
                })
        else:
            results.append({
                "address": addr,
                "label": "",
                "label_type": "",
            })

        if (i + 1) % 50 == 0:
            print(f"Processed {i + 1}/{len(addresses)} addresses")

    # Write output CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        fieldnames = ["address", "label", "label_type"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Wrote {len(results)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
