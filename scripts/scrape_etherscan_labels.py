#!/usr/bin/env python3
"""Scrape entity labels from Etherscan for proposer fee recipients."""

import csv
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

INPUT_CSV = Path("/Users/thomas/Downloads/proposer_fee_recipients.csv")
OUTPUT_CSV = Path("/Users/thomas/Downloads/proposer_labels.csv")
MAX_ROWS = 100
RATE_LIMIT_DELAY = 1.5  # seconds between requests to avoid rate limiting

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def is_generic_label(label: str) -> bool:
    """Check if a label is generic (not a real entity name)."""
    if not label:
        return True
    label_lower = label.lower()
    # Generic patterns to filter out
    generic_patterns = [
        r"^fee recipient",
        r"^proposer fee recipient",
        r"^address[:\s]",
        r"^0x[a-f0-9]",
        r"^mev builder[:\s]*0x",
        r"^\d+\s*(eth|gwei|wei)",
    ]
    for pattern in generic_patterns:
        if re.match(pattern, label_lower):
            return True
    # Also filter if label is just an address snippet
    if re.search(r"0x[a-f0-9]{3,}\.\.\.[a-f0-9]{3,}", label_lower):
        return True
    return False


def extract_entity_name(label: str) -> str:
    """Extract just the entity name from a label like 'Lido: Execution Layer Rewards Vault'."""
    if not label:
        return ""
    # For labels with colon, take the part before the colon as the entity
    if ":" in label:
        entity = label.split(":")[0].strip()
        # But keep full label if entity part is generic
        if entity.lower() in ["fee recipient", "mev builder", "address"]:
            return ""
        return entity
    return label


def extract_label_from_html(html: str) -> tuple[str | None, str | None]:
    """Extract the entity label from Etherscan HTML. Returns (entity, full_label)."""
    soup = BeautifulSoup(html, "html.parser")

    # Method 1: Look for the title tag which often contains the label
    title = soup.find("title")
    if title:
        title_text = title.get_text()
        # Pattern: "Label Name | Address 0x... | Etherscan"
        match = re.match(r"^([^|]+)\s*\|", title_text)
        if match:
            full_label = match.group(1).strip()
            # Clean up - remove address suffix if present
            full_label = re.sub(r"\s*\(0x[a-fA-F0-9]+\)\s*$", "", full_label)
            full_label = re.sub(r"\s*0x[a-fA-F0-9]+\s*$", "", full_label).strip()

            if full_label and full_label not in ["Ethereum", "Etherscan"]:
                if not is_generic_label(full_label):
                    entity = extract_entity_name(full_label)
                    return entity, full_label

    # Method 2: Look for entity names in HTML (tooltips, links, etc.)
    # Pattern: "EntityName: Something" in raw HTML
    entity_patterns = [
        # Matches "ether.fi: Deployer" style labels in HTML attributes
        r"'>([A-Za-z][A-Za-z0-9_.]+):\s*(?:Deployer|Fee|Staking|Execution|Rewards|Vault)",
        r"title='([A-Za-z][A-Za-z0-9_.]+):\s*(?:Deployer|Fee|Staking|Execution|Rewards|Vault)",
    ]
    for pattern in entity_patterns:
        match = re.search(pattern, html)
        if match:
            entity = match.group(1).strip()
            # Filter out generic matches
            if entity and len(entity) > 2 and entity.lower() not in [
                "fee", "mev", "address", "contract", "beacon"
            ]:
                return entity, f"Contract created by {entity}"

    return None, None


def scrape_etherscan_label(address: str) -> tuple[str | None, str | None, str | None]:
    """Scrape Etherscan for the label of an address. Returns (entity, full_label, error)."""
    url = f"https://etherscan.io/address/{address}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            entity, full_label = extract_label_from_html(response.text)
            return entity, full_label, None
        else:
            return None, None, f"HTTP {response.status_code}"
    except requests.RequestException as e:
        return None, None, str(e)


def main():
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

    print(f"Scraping labels for {len(addresses)} unique addresses from first {MAX_ROWS} rows")

    # Scrape labels
    results = []
    for i, addr in enumerate(addresses):
        print(f"[{i + 1}/{len(addresses)}] Scraping {addr[:10]}...{addr[-6:]}", end=" ")

        entity, full_label, error = scrape_etherscan_label(addr)

        if entity:
            print(f"-> {entity} ({full_label})")
            results.append({
                "proposer_fee_recipient": addr,
                "entity": entity,
                "full_label": full_label or "",
            })
        elif error:
            print(f"-> Error: {error}")
            results.append({
                "proposer_fee_recipient": addr,
                "entity": "",
                "full_label": "",
            })
        else:
            print("-> No entity label")
            results.append({
                "proposer_fee_recipient": addr,
                "entity": "",
                "full_label": "",
            })

        # Rate limiting
        if i < len(addresses) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    # Write output CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        fieldnames = ["proposer_fee_recipient", "entity", "full_label"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Summary
    labeled = sum(1 for r in results if r["entity"])
    print(f"\nWrote {len(results)} rows to {OUTPUT_CSV}")
    print(f"Found entity labels for {labeled}/{len(results)} addresses")


if __name__ == "__main__":
    main()
