# MEV-Boost Relay & Builder Market Share Dashboard

### Technical Analysis Summary

This repository presents an analytical dashboard exploring MEV-Boost relay activity, builder market concentration, proposer/builder profitability, and block value distributions across Ethereum blocks within the analyzed date range.

**Date range:** 2025-10-30 → 2025-11-19
**Total blocks analyzed:** **142,878**

---

## 📊 Overview

The dashboard computes:

* Relay market share
* Builder market share
* Profit split between proposers and builders
* Block total value distribution
* Negative-value block analysis
* Builder-level overbidding and proposer revenue share

All results are derived using MEV-Boost payloads, enriched block traces, and on-chain balance differences.

---

## ✅ Key Findings

* MEV-Boost adoption remains high at **~93%**.
* Proposers capture **89%** of captured value, builders capture **8%** and relays **2%**.
* Builder market is **heavily centralized**, with Titan producing nearly half of all MEV-Boost blocks.
* Relay distribution shows **Ultrasound**, **Bloxroute Max Profit**, and **Titan** as dominant payload sources.
* MEV-Boost blocks show **0% negative total values**, whereas vanilla blocks show **1.59% negative total values**.
* Handling Ultrasound bid adjustments improves profit accuracy (no longer a caveat).

---

## 📑 Summary Tables

### **MEV-Boost Adoption**

| Metric          | Value   |
| --------------- | ------- |
| Total Blocks    | 142,878 |
| MEV-Boost Share | ~93%    |
| Vanilla Share   | ~7%     |

---

## **Relay Market Share**

| Relay                | Share (%) |
| -------------------- | --------- |
| Ultrasound           | **30.3%** |
| Bloxroute Max Profit | **20.8%** |
| Titan                | **20.2%** |
| Bloxroute Regulated  | **12.6%** |
| Aestus               | **5.28%** |
| Agnostic             | **4.80%** |
| Flashbots            | **4.29%** |
| EthGas               | **1.74%** |
| BTCS                 | **0.04%** |

---

## **Builder Market Share**

| Builder                 | Share (%) |
| ----------------------- | --------- |
| Titan                   | **49.2%** |
| BuilderNet (Flashbots)  | **16.3%** |
| Quasar                  | **11.0%** |
| BuilderNet (Beaver)     | **9.86%** |
| BuilderNet (Nethermind) | **7.05%** |
| BTCS                    | **3.14%** |
| Rsync                   | **2.32%** |
| Others                  | **0.57%** |
| Bob The Builder         | **0.26%** |
| IO Builder              | **0.21%** |

---

## **Profit Distribution**

| Party     | Profit (ETH) | Share (%) |
| --------- | ------------ | --------- |
| Proposers | **4516 ETH** | **85.0%** |
| Builders  | **1508 ETH** | **15.0%** |

---

## **Average Total Value per Block**

| Block Type | Avg Total Value |
| ---------- | --------------- |
| mev_boost  | **0.0453 ETH**  |
| vanilla    | **0.00590 ETH** |

---

## **Negative Total Value Occurrence**

| Block Type | Negative Rate |
| ---------- | ------------- |
| mev_boost  | **0%**        |
| vanilla    | **1.59%**     |


## **Proposer Profit Share by Builder**

| Builder                 | Proposer Profit % |
| ----------------------- | ----------------- |
| Quasar                  | **92.7%**         |
| Titan                   | **82.3%**         |
| BuilderNet (Flashbots)  | **81.3%**         |
| BuilderNet (Beaver)     | **77.9%**         |
| BuilderNet (Nethermind) | **77.1%**         |

---

## **Builder Overbid Behavior**

| Builder                 | Overbid % |
| ----------------------- | --------- |
| Quasar                  | **13.9%** |
| BuilderNet (Flashbots)  | **12.9%** |
| Titan                   | **9.81%** |
| BuilderNet (Nethermind) | **9.54%** |
| BuilderNet (Beaver)     | **7.53%** |

---

## ⚠️ Data Limitations & Interpretation Notes (Updated)

### ❗ Missing Relay Data → Misclassified Vanilla Blocks

Some blocks are **incorrectly classified as vanilla** when relay data is missing.
This causes two issues:

1. **Proposer subsidy is not captured**
   – MEV-Boost blocks usually include a direct payment to the proposer.
   – When relay data is missing, this subsidy disappears from attribution.

2. **Negative total value** appears even though the block was profitable
   – MEV-Boost blocks rarely have negative total value.
   – Misclassification artificially introduces negative values into the dataset.

**This is the primary caveat affecting total value and profit accuracy.**

### ✔ Ultrasound Bid Adjustment Handled

Ultrasound's second-best-bid mechanism has been compensated for in the pipeline,
so its adjusted payouts **are correctly accounted for**.

### ✔ Builder Transfer Bias

Some builders route funds between internal addresses.
This affects measured on-chain profit, though not total value.

---

## 🔧 Future Improvements

### **1. Reconstruct Proposer Subsidy On-Chain Without Relay Data**

Relay payloads are the authoritative source of builder → proposer payments,
but in their absence we can still recover the subsidy by:

* Scanning intra-block transfers
* Identifying transfers from the builder payment address to the proposer
* Reconstructing the missing proposer payment directly from on-chain data

This would:

* Eliminate dependency on relay availability
* Restore correct total value and profit attribution
* Prevent false negative-value blocks

### **2. Cross-check block classification**

Flag as MEV-Boost when a builder payment is detected, even without relay metadata.

---

## 🧩 Conclusions

* MEV-Boost adoption is near-universal in the sampled period.
* Builders remain highly centralized, with Titan dominating the market.
* Proposers capture the majority of revenue due to bidding competition.
* Relay market shares align with expected ecosystem behavior.
* Negative-value vanilla blocks are mostly artifacts of missing relay metadata.
* Future reconstruction of proposer subsidy from direct transfers will resolve these issues fully.

