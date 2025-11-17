"""Constants for the project."""

import os

RELAYS = [
    "relay.ultrasound.money",
    "bloxroute.max-profit.blxrbdn.com",
    "bloxroute.regulated.blxrbdn.com",
    "titanrelay.xyz",
    "agnostic-relay.net",
    "aestus.live",
    "boost-relay.flashbots.net",
]


ENDPOINTS = {
    "proposer_payload_delivered": "/relay/v1/data/bidtraces/proposer_payload_delivered",
}

LIMITS = {
    "proposer_payload_delivered": 200,
}

BEACON_ENDPOINT = os.getenv(
    "BEACON_ENDPOINT", "https://ethereum-beacon-api.publicnode.com"
)
