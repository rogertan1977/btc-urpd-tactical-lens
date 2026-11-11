# ⚡ Delta URPD Agent: Hardware-Democratized On-Chain Microstructure Monitor

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.9+](https://img.shields.io/badge/Python-3.9%2B-brightgreen.svg)](https://www.python.org/)
[![Architecture: Zero--Bloat](https://img.shields.io/badge/Architecture-Anti--Bloat-red.svg)]()
[![Hardware: Haswell--Ready](https://img.shields.io/badge/Hardware-i7--4770%20Compatible-orange.svg)]()

> **"Price action can deceive; the migration of coin creation prices does not."**  
> A zero-cost, ultra-lightweight quantitative monitor that captures Bitcoin UTXO Realized Price Distribution (URPD) deltas ($\Delta \text{URPD}$) to unmask institutional iceberg absorption and retail capitulation in real-time.

---

## 🏛️ The "Hardware Democratization" Ethos

Modern quantitative tools are plagued by architectural bloat. Standard setups often consume **4GB+ RAM inside massive Docker images**, loading heavy dataframes and machine learning dependencies just to align and subtract two 300-element arrays.

**We reject this paradigm.**

This agent is built to run on decade-old consumer hardware (such as an **Intel Core i7-4770 with 8GB DDR3 RAM** or a **$4/mo single-core VPS**):
- **Zero Heavy Scientific Stacks**: Absolutely no `pandas`, `numpy`, or `scipy`.
- **Pure Standard Library Math**: Array alignment and delta computations are executed via native Python hash maps, set-unions, and `itertools` in **$< 0.1 \text{ ms}$**.
- **O(1) Dynamic Memory Model**: All data transfer objects employ `__slots__` to suppress dynamic dictionary overhead. Total process footprint remains **$< 20\text{MB}$ RSS**.
- **Embedded SQLite WAL Engine**: Zero-daemon persistence with zero-lock concurrent reads and microscopic disk growth ($\approx 6\text{MB/year}$).
- **Single-Shot Ephemeral Lifespan**: Executes on a cron schedule, evaluates the chain, alerts Telegram, releases all resources, and cleanly terminates.

---

## 📈 The Alpha: Static vs. Delta URPD

Retail traders view URPD as a static chart showing historical support and resistance. **Smart money tracks the Delta ($\Delta \text{V}$):**

$$\Delta V(P) = V_T(P) - V_{T-1}(P)$$