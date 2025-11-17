"""Constants for the project."""

import os

RELAYS = [
    "relay-analytics.ultrasound.money",  # Redirect from relay.ultrasound.money
    "bloxroute.max-profit.blxrbdn.com",
    "bloxroute.regulated.blxrbdn.com",
    # "titanrelay.xyz",
    "agnostic-relay.net",
    "aestus.live",
    "boost-relay.flashbots.net",
    "relay.ethgas.com",
    "relay.btcs.com",
    "relay.wenmerge.com",
    "mainnet-relay.securerpc.com",
]


ENDPOINTS = {
    "proposer_payload_delivered": "/relay/v1/data/bidtraces/proposer_payload_delivered",
}

LIMITS = {
    "proposer_payload_delivered": 200,
}

# Per-relay limits (some relays have lower maximum limits)
RELAY_LIMITS = {
    "bloxroute.max-profit.blxrbdn.com": 100,
    "bloxroute.regulated.blxrbdn.com": 100,
}

BEACON_ENDPOINT = os.getenv(
    "BEACON_ENDPOINT", "https://ethereum-beacon-api.publicnode.com"
)
