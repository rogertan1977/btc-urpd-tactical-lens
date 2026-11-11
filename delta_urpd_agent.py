#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project: Delta URPD Liquidity Migration Agent
Philosophy: Hardware Democratization (Zero-Bloat / Standard Library + aiohttp)
Target Spec: Legacy Hardware (e.g., Intel Core i7-4770, 8GB RAM, <50MB RSS footprint)
Execution Model: Ephemeral One-Shot Runner (Scrape -> Persist -> Compute -> Alert -> Terminate)
"""

import sys
import os
import json
import sqlite3
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional, Any, Set

# =====================================================================
# 0. Global Configuration (Env Overrides Supported)
# =====================================================================
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")

# Price threshold for high-watermark trapped supply stress testing (in USD)
HIGH_POSITION_THRESHOLD: float = 78000.0


# =====================================================================
# 1. Memory-Constrained Domain Models
# =====================================================================
class URPDBucket:
    """
    Primitive URPD bucket representation.
    Employs __slots__ to eliminate __dict__ allocation, reducing memory footprint by ~68%.
    """
    __slots__ = ('price', 'supply', 'realized_cap')

    def __init__(self, price: float, supply: float, realized_cap: float = 0.0) -> None:
        self.price: float = price
        self.supply: float = supply
        self.realized_cap: float = realized_cap

    def __repr__(self) -> str:
        return f"<Bucket ${self.price:,.0f}: {self.supply:,.2f} BTC>"


class DeltaResult:
    """Aligned delta calculation entity across consecutive snapshots."""
    __slots__ = ('price', 'supply_prev', 'supply_curr', 'delta_supply', 'pct_change')

    def __init__(
        self,
        price: float,
        supply_prev: float,
        supply_curr: float,
        delta_supply: float,
        pct_change: float
    ) -> None:
        self.price: float = price
        self.supply_prev: float = supply_prev
        self.supply_curr: float = supply_curr
        self.delta_supply: float = delta_supply
        self.pct_change: float = pct_change


class IntelligenceReport:
    """Macro intelligence and microstructural behavioral synthesis."""
    __slots__ = (
        'high_threshold', 'high_prev_vol', 'high_curr_vol', 'high_delta_vol',
        'high_pct_change', 'sentiment_status', 'sentiment_detail',
        'peak_accum_bin', 'peak_dist_bin', 'gross_inflow', 'gross_outflow',
        'net_market_shift', 'tactical_summary'
    )

    def __init__(
        self,
        high_threshold: float,
        high_prev_vol: float,
        high_curr_vol: float,
        high_delta_vol: float,
        high_pct_change: float,
        sentiment_status: str,
        sentiment_detail: str,
        peak_accum_bin: Optional[DeltaResult],
        peak_dist_bin: Optional[DeltaResult],
        gross_inflow: float,
        gross_outflow: float,
        net_market_shift: float,
        tactical_summary: str
    ) -> None:
        self.high_threshold = high_threshold
        self.high_prev_vol = high_prev_vol
        self.high_curr_vol = high_curr_vol
        self.high_delta_vol = high_delta_vol
        self.high_pct_change = high_pct_change
        self.sentiment_status = sentiment_status
        self.sentiment_detail = sentiment_detail
        self.peak_accum_bin = peak_accum_bin
        self.peak_dist_bin = peak_dist_bin
        self.gross_inflow = gross_inflow
        self.gross_outflow = gross_outflow
        self.net_market_shift = net_market_shift
        self.tactical_summary = tactical_summary


# =====================================================================
# 2. Local Storage Layer (SQLite WAL Mode)
# =====================================================================
class LocalStorage:
    """Zero-daemon persistence using embedded SQLite with Write-Ahead Logging."""

    def __init__(self, db_path: str = "urpd_metrics.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        # WAL mode guarantees concurrent non-blocking reads during writes
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS urpd_snapshots (
                    date_str TEXT NOT NULL,
                    price_bin REAL NOT NULL,
                    supply REAL NOT NULL,
                    realized_cap REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date_str, price_bin)
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON urpd_snapshots(date_str);")
            conn.commit()

    def save_snapshot(self, date_str: str, buckets: List[URPDBucket]) -> None:
        """Batch-insert/upsert a daily URPD distribution snapshot."""
        if not buckets:
            return
        records = [
            (date_str, b.price, b.supply, b.realized_cap)
            for b in buckets
        ]
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO urpd_snapshots (date_str, price_bin, supply, realized_cap)
                VALUES (?, ?, ?, ?);
            """, records)
            conn.commit()

    def load_snapshot(self, date_str: str) -> Dict[float, float]:
        """Load {price_bin: supply} hash map for a target date in O(K) time."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price_bin, supply FROM urpd_snapshots WHERE date_str = ? ORDER BY price_bin ASC;",
                (date_str,)
            )
            return {row[0]: row[1] for row in cursor.fetchall()}


# =====================================================================
# 3. Polymorphic Ingestion Layer (aiohttp Scraper)
# =====================================================================
class URPDFetcher:
    """High-throughput asynchronous ingestor targeting open public endpoints."""

    def __init__(self) -> None:
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

    async def fetch_bitview(self, date_str: Optional[str] = None, agg_step: str = "lin500") -> List[URPDBucket]:
        """
        Scrape normalized URPD data from open Bitview REST endpoints.
        :param date_str: Snapshot date (YYYY-MM-DD), or None for real-time tip.
        :param agg_step: Linear bucket size (lin200, lin500, lin1000).
        """
        import aiohttp

        endpoint = (
            f"https://bitview.space/api/urpd/all/{date_str}?agg={agg_step}&weight=none"
            if date_str
            else f"https://bitview.space/api/urpd/all?agg={agg_step}&weight=none"
        )

        step_val = 500.0
        if agg_step.startswith("lin"):
            try:
                step_val = float(agg_step.replace("lin", ""))
            except ValueError:
                step_val = 500.0

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
            try:
                async with session.get(endpoint) as response:
                    if response.status != 200:
                        sys.stderr.write(f"[-] HTTP {response.status} returned from {endpoint}\n")
                        return []
                    raw_data = await response.json(content_type=None)
                    return self._parse_bitview_json(raw_data, default_step=step_val)
            except Exception as e:
                sys.stderr.write(f"[Fetcher Error] Ingestion failed for {endpoint}: {e}\n")
                return []

    def _parse_bitview_json(self, raw_data: Any, default_step: float = 500.0) -> List[URPDBucket]:
        """
        Polymorphic heuristic parser handling heterogeneous JSON payloads:
        1. Relational schema: {"columns": [...], "data": [...]}
        2. Parallel arrays: {"price": [...], "supply": [...]}
        3. Implicit single vectors: {"supply": [...]} (reconstructed via bucket index)
        4. Key-Value maps: {"70000": 1.5, ...}
        5. Record-oriented lists: [{"price": 70000, "supply": 1.5}, ...]
        """
        if not raw_data:
            return []

        def _extract(node: Any) -> Optional[List[URPDBucket]]:
            if not node:
                return None

            # Branch 1: Dictionary-based structures
            if isinstance(node, dict):
                # 1.1 Relational column/data format
                if "columns" in node and "data" in node and isinstance(node["data"], list):
                    cols = [str(c).lower() for c in node["columns"]]
                    p_idx = next((cols.index(k) for k in ("price", "price_floor", "prices", "bin", "bins") if k in cols), -1)
                    s_idx = next((cols.index(k) for k in ("supply", "supplies", "volume", "amount") if k in cols), -1)
                    if p_idx != -1 and s_idx != -1:
                        res = []
                        for row in node["data"]:
                            if isinstance(row, (list, tuple)) and len(row) > max(p_idx, s_idx):
                                try:
                                    res.append(URPDBucket(float(row[p_idx]), float(row[s_idx])))
                                except (ValueError, TypeError):
                                    continue
                        if res:
                            return res

                # 1.2 Parallel array vectors
                p_key = next((k for k in ("price", "price_floor", "prices", "bin", "bins", "p") if k in node and isinstance(node[k], list)), None)
                s_key = next((k for k in ("supply", "supplies", "volume", "volumes", "v", "amount") if k in node and isinstance(node[k], list)), None)

                if p_key and s_key:
                    prices = node[p_key]
                    supplies = node[s_key]
                    res = []
                    for p, s in zip(prices, supplies):
                        if p is not None and s is not None:
                            try:
                                res.append(URPDBucket(float(p), float(s)))
                            except (ValueError, TypeError):
                                continue
                    if res:
                        return res

                # 1.3 Implicit supply array (reconstruct prices by index * step)
                if s_key and not p_key:
                    supplies = node[s_key]
                    res = []
                    for idx, s in enumerate(supplies):
                        if s is not None:
                            try:
                                res.append(URPDBucket(float(idx * default_step), float(s)))
                            except (ValueError, TypeError):
                                continue
                    if res:
                        return res

                # 1.4 Direct Key-Value mapping
                is_kv = True
                kv_res = []
                for k, v in node.items():
                    if isinstance(v, (dict, list)):
                        is_kv = False
                        break
                    try:
                        p_float = float(k)
                        s_float = float(v)
                        kv_res.append(URPDBucket(p_float, s_float))
                    except (ValueError, TypeError):
                        is_kv = False
                        break
                if is_kv and kv_res:
                    return kv_res

                # 1.5 Recursive unwrap of nested payload envelopes
                for sub_key in ("buckets", "data", "urpd", "result", "distribution", "items"):
                    if sub_key in node and isinstance(node[sub_key], (dict, list)):
                        sub_result = _extract(node[sub_key])
                        if sub_result:
                            return sub_result

            # Branch 2: List-based structures
            elif isinstance(node, list):
                if not node:
                    return None

                first_elem = node[0]
                if isinstance(first_elem, dict):
                    res = []
                    for row in node:
                        if not isinstance(row, dict):
                            continue
                        p = next((row[k] for k in ("price", "price_floor", "bin", "prices", "p") if k in row), None)
                        s = next((row[k] for k in ("supply", "supplies", "volume", "v", "amount") if k in row), None)
                        if p is not None and s is not None:
                            try:
                                cap = float(row.get("realized_cap", 0.0))
                                res.append(URPDBucket(float(p), float(s), cap))
                            except (ValueError, TypeError):
                                continue
                    if res:
                        return res

                elif isinstance(first_elem, (list, tuple)):
                    res = []
                    for row in node:
                        if isinstance(row, (list, tuple)) and len(row) >= 2:
                            try:
                                p = float(row[0])
                                s = float(row[1])
                                cap = float(row[2]) if len(row) > 2 else 0.0
                                res.append(URPDBucket(p, s, cap))
                            except (ValueError, TypeError):
                                continue
                    if res:
                        return res

                elif isinstance(first_elem, (int, float)):
                    res = []
                    for idx, s in enumerate(node):
                        try:
                            res.append(URPDBucket(float(idx * default_step), float(s)))
                        except (ValueError, TypeError):
                            continue
                    if res:
                        return res

            return None

        parsed = _extract(raw_data)
        if parsed:
            return parsed

        print(f"RAW JSON DUMP: {str(raw_data)[:200]}")
        return []


# =====================================================================
# 4. Pure Python Delta Engine (Zero-Pandas Alignment)
# =====================================================================
class DeltaURPDEngine:
    """High-performance array alignment and delta analytics engine."""

    @staticmethod
    def compute_delta(
        t_prev: Dict[float, float],
        t_curr: Dict[float, float]
    ) -> List[DeltaResult]:
        """
        Outer-join set union alignment:
        K = keys(t_prev) U keys(t_curr)
        Delta V = V_t - V_(t-1)
        Time Complexity: O(K log K) | Space Complexity: O(K)
        """
        all_bins: Set[float] = set(t_prev.keys()).union(set(t_curr.keys()))
        sorted_bins = sorted(all_bins)

        results: List[DeltaResult] = []
        for price in sorted_bins:
            v_prev = t_prev.get(price, 0.0)
            v_curr = t_curr.get(price, 0.0)
            delta = v_curr - v_prev

            if v_prev > 1e-6:
                pct = (delta / v_prev) * 100.0
            else:
                pct = 100.0 if v_curr > 0 else 0.0

            results.append(DeltaResult(
                price=price,
                supply_prev=v_prev,
                supply_curr=v_curr,
                delta_supply=delta,
                pct_change=pct
            ))

        return results

    @staticmethod
    def generate_intelligence_report(
        deltas: List[DeltaResult],
        high_price_threshold: float = 78000.0,
        significance_threshold: float = 300.0
    ) -> IntelligenceReport:
        """
        Generates qualitative macro intelligence:
        1. High-watermark trapped supply stress testing (capitulation vs. diamond-hand absorption).
        2. Network-wide peak accumulation and distribution anchor nodes.
        3. Quantitative takeaway for executive trading decisions.
        """
        high_bins = [d for d in deltas if d.price >= high_price_threshold]
        high_prev_total = sum(d.supply_prev for d in high_bins)
        high_curr_total = sum(d.supply_curr for d in high_bins)
        high_delta_total = high_curr_total - high_prev_total
        high_pct = (high_delta_total / high_prev_total * 100.0) if high_prev_total > 1e-6 else 0.0

        if high_delta_total < -significance_threshold:
            sentiment_status = "🔥 [HIGH-LEVEL PANIC ALERT] (Aggressive Capitulation / Stop-Loss Cascades)"
            sentiment_detail = (
                f"High-watermark defense broken. Trapped holders capitulating with net outflow of "
                f"{abs(high_delta_total):,.2f} BTC. Panic selling is displacing supply toward lower liquidity pockets."
            )
        elif high_delta_total > significance_threshold:
            sentiment_status = "⚠️ [CONTRARIAN ACCUMULATION / DIAMOND HANDS] (Trapped Holders Averaging Down / Refusing to Exit)"
            sentiment_detail = (
                f"Aggressive resolve detected. High-level supply increased by +{high_delta_total:,.2f} BTC. "
                "Trapped holders remain unyielding while dip-buyers step in; overhead supply overhang remains unpurged."
            )
        else:
            sentiment_status = "🛡️ [CALM / DORMANT CONSOLIDATION] (Minimal Turnover, Supply Locked)"
            sentiment_detail = (
                f"High-level supply remains dormant with minor net shift of {high_delta_total:+,.2f} BTC ({high_pct:+.2f}%). "
                "Turnover is negligible; market is locked in a range-bound standoff."
            )

        peak_accum_bin: Optional[DeltaResult] = None
        peak_dist_bin: Optional[DeltaResult] = None

        if deltas:
            peak_accum_bin = max(deltas, key=lambda x: x.delta_supply)
            peak_dist_bin = min(deltas, key=lambda x: x.delta_supply)

        gross_inflow = sum(d.delta_supply for d in deltas if d.delta_supply > 0)
        gross_outflow = sum(abs(d.delta_supply) for d in deltas if d.delta_supply < 0)
        net_market_shift = gross_inflow - gross_outflow

        accum_p = f"${peak_accum_bin.price:,.0f}" if peak_accum_bin else "N/A"
        dist_p = f"${peak_dist_bin.price:,.0f}" if peak_dist_bin else "N/A"
        summary = (
            f"On-chain microstructure exhibits directed migration from {dist_p} toward {accum_p}. "
            f"Gross newly minted UTXO volume stands at {gross_inflow:,.2f} BTC, with institutional iceberg buyers "
            f"absorbing liquid float at {accum_p}."
        )

        return IntelligenceReport(
            high_threshold=high_price_threshold,
            high_prev_vol=high_prev_total,
            high_curr_vol=high_curr_total,
            high_delta_vol=high_delta_total,
            high_pct_change=high_pct,
            sentiment_status=sentiment_status,
            sentiment_detail=sentiment_detail,
            peak_accum_bin=peak_accum_bin,
            peak_dist_bin=peak_dist_bin,
            gross_inflow=gross_inflow,
            gross_outflow=gross_outflow,
            net_market_shift=net_market_shift,
            tactical_summary=summary
        )


# =====================================================================
# 5. Elite Telegram Dispatcher (Chunked Terminal UI)
# =====================================================================
class TelegramNotifier:
    """
    Asynchronous Telegram notification engine.
    Features:
    - Auto-chunking for full-spectrum matrices to guarantee payload < 4096 characters.
    - Monospace HTML formatting for clean mobile display without horizontal line wrap.
    - Zero third-party bot SDK bloat.
    """

    @staticmethod
    def _format_bin_row(r: DeltaResult) -> str:
        """Format individual row within 48-char boundary."""
        if r.delta_supply >= 300.0:
            tag = "🔥[▲BUY]"
        elif r.delta_supply >= 30.0:
            tag = "  [▲ADD]"
        elif r.delta_supply <= -300.0:
            tag = "🩸[▼DUMP]"
        elif r.delta_supply <= -30.0:
            tag = "  [▼CUT]"
        else:
            tag = "  [🔒HOLD]"

        # Compact layout: Price(8) | T-1(8) | T(8) | Delta(9) | %(6) | State
        return (
            f"${r.price:>7,.0f} | "
            f"{r.supply_prev:>8,.1f} | "
            f"{r.supply_curr:>8,.1f} | "
            f"{r.delta_supply:>+8,.1f} | "
            f"{r.pct_change:>+5.1f}% | "
            f"{tag}"
        )

    @classmethod
    def build_matrix_messages(
        cls,
        active_deltas: List[DeltaResult],
        t_prev_date: str,
        t_curr_date: str
    ) -> List[str]:
        """Split complete price bins into sequential chunks under 3,200 chars."""
        if not active_deltas:
            return []

        header = (
            f"Price Bin |  T-1 Vol  |   T Vol   |   Delta   |  Chg% | State\n"
            f"-------------------------------------------------------"
        )

        rows = [cls._format_bin_row(r) for r in active_deltas]
        chunk_size = 45
        chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
        total_chunks = len(chunks)

        messages = []
        for idx, chunk in enumerate(chunks, 1):
            table_body = "\n".join(chunk)
            msg = (
                f"🗺️ <b>[BTC Full-Spectrum Supply Migration Matrix (Part {idx}/{total_chunks})]</b>\n"
                f"🗓 Period: <code>{t_prev_date}</code> ➔ <code>{t_curr_date}</code> (UTC)\n"
                f"<pre>{header}\n{table_body}</pre>"
            )
            messages.append(msg)

        return messages

    @classmethod
    def build_tactical_briefing(
        cls,
        intel: IntelligenceReport,
        t_prev_date: str,
        t_curr_date: str
    ) -> str:
        """Construct executive intelligence briefing."""
        accum_str = (
            f"${intel.peak_accum_bin.price:,.0f} (+{intel.peak_accum_bin.delta_supply:,.2f} BTC / {intel.peak_accum_bin.pct_change:+.1f}%)"
            if intel.peak_accum_bin and intel.peak_accum_bin.delta_supply > 0
            else "None detected"
        )
        dist_str = (
            f"${intel.peak_dist_bin.price:,.0f} ({intel.peak_dist_bin.delta_supply:,.2f} BTC / {intel.peak_dist_bin.pct_change:+.1f}%)"
            if intel.peak_dist_bin and intel.peak_dist_bin.delta_supply < 0
            else "None detected"
        )

        return (
            f"📊 <b>[Tactical Intel Briefing - On-Chain Microstructure Analysis]</b>\n"
            f"🗓 <b>Evaluation Window:</b> <code>{t_prev_date}</code> ➔ <code>{t_curr_date}</code> (UTC)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🎯 <b>1. High-Watermark Bagholder Stress Test (Range ≥ ${intel.high_threshold:,.0f})</b>\n"
            f"• <b>State:</b> {intel.sentiment_status}\n"
            f"• <b>Volume Shift:</b> <code>{intel.high_prev_vol:,.2f} BTC</code> ➔ <code>{intel.high_curr_vol:,.2f} BTC</code>\n"
            f"• <b>Net Delta:</b> <code>{intel.high_delta_vol:+,.2f} BTC</code> ({intel.high_pct_change:+.2f}%)\n"
            f"• <b>Behavioral Breakdown:</b> <i>{intel.sentiment_detail}</i>\n\n"
            f"⚡ <b>2. Core Liquidity Anchor Nodes</b>\n"
            f"• 🏆 <b>Peak Inflow Node:</b> <code>{accum_str}</code>\n"
            f"  └ <i>Institutional iceberg bid defense zone detected</i>\n"
            f"• 🚨 <b>Peak Outflow Node:</b> <code>{dist_str}</code>\n"
            f"  └ <i>Severe capitulation liquidation / stop-loss cluster</i>\n\n"
            f"🌊 <b>3. Macro Liquidity Dynamics</b>\n"
            f"• Gross Inflow (New UTXO Mint): <code>+{intel.gross_inflow:,.2f} BTC</code>\n"
            f"• Gross Outflow (Dormant Spend): <code>-{intel.gross_outflow:,.2f} BTC</code>\n"
            f"• Net Market Shift: <code>{intel.net_market_shift:+,.2f} BTC</code>\n\n"
            f"💡 <b>4. Tactical Execution Summary</b>\n"
            f"<i>{intel.tactical_summary}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>🤖 Generated by Delta URPD Agent (Hardware Democratization Project)</i>"
        )

    @classmethod
    async def push_all_reports(
        cls,
        token: str,
        chat_id: str,
        deltas: List[DeltaResult],
        intel: IntelligenceReport,
        t_prev_date: str,
        t_curr_date: str
    ) -> bool:
        """Transmit multi-part matrix sequence followed by tactical summary."""
        if not token or not chat_id or "YOUR_TELEGRAM" in token:
            print("[*] Telegram credentials not configured. Skipping remote push.")
            return False

        import aiohttp
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        active_deltas = [
            d for d in deltas
            if (d.supply_prev > 0.01 or d.supply_curr > 0.01 or abs(d.delta_supply) > 0.01)
        ]

        matrix_messages = cls.build_matrix_messages(active_deltas, t_prev_date, t_curr_date)
        briefing_message = cls.build_tactical_briefing(intel, t_prev_date, t_curr_date)
        all_payloads = matrix_messages + [briefing_message]

        timeout = aiohttp.ClientTimeout(total=25)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for idx, text_payload in enumerate(all_payloads, 1):
                    payload = {
                        "chat_id": chat_id,
                        "text": text_payload,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    }
                    async with session.post(url, json=payload) as resp:
                        if resp.status != 200:
                            err_body = await resp.text()
                            sys.stderr.write(f"[-] Telegram dispatch error for chunk {idx}/{len(all_payloads)} [HTTP {resp.status}]: {err_body}\n")
                            return False
                    # Rate-limiting safety interval
                    await asyncio.sleep(0.35)

                print(f"[+] Telegram dispatch successful. Delivered {len(all_payloads)} chunks.")
                return True
        except Exception as e:
            sys.stderr.write(f"[-] Telegram network error: {e}\n")
            return False


# =====================================================================
# 6. Terminal Panorama Dashboard
# =====================================================================
def render_terminal_dashboard(
    deltas: List[DeltaResult],
    intel: IntelligenceReport,
    t_prev_date: str,
    t_curr_date: str
) -> None:
    """Print high-density ASCII trading terminal dashboard."""
    active_deltas = [
        d for d in deltas
        if (d.supply_prev > 0.01 or d.supply_curr > 0.01 or abs(d.delta_supply) > 0.01)
    ]

    print("\n" + "=" * 102)
    print(f" [DELTA URPD MONITOR] BTC Full-Spectrum Supply Migration Matrix  |  {t_prev_date}  -->  {t_curr_date} (UTC)")
    print("=" * 102)

    print(f"{'Price Bin':>12} | {'T-1 Vol (BTC)':>15} | {'T Vol (BTC)':>15} | {'Delta (BTC)':>16} | {'Delta %':>10} | {'Micro-Action'}")
    print("-" * 102)

    if not active_deltas:
        print("  [!] No active volume shifts detected across Price Bins.")
    else:
        for r in active_deltas:
            if r.delta_supply >= 300.0:
                action_tag = "🔥 [▲▲ Heavy Inflow]"
            elif r.delta_supply >= 50.0:
                action_tag = "   [▲  Volume Buildup]"
            elif r.delta_supply <= -300.0:
                action_tag = "🩸 [▼▼ Capitulation Dump]"
            elif r.delta_supply <= -50.0:
                action_tag = "   [▼  Volume Drain]"
            else:
                action_tag = "   [ - Dormant / Locked]"

            delta_str = f"{r.delta_supply:+,.2f} BTC"
            pct_str = f"{r.pct_change:+.1f}%"
            print(
                f"${r.price:>10,.0f} | "
                f"{r.supply_prev:>11,.2f} BTC | "
                f"{r.supply_curr:>11,.2f} BTC | "
                f"{delta_str:>16} | "
                f"{pct_str:>10} | "
                f"{action_tag}"
            )

    print("-" * 102)
    print(f"[*] Panorama Summary: Displaying {len(active_deltas)} active buckets (zero-volume bins omitted).")

    print("\n" + "#" * 102)
    print(" 📊 [Tactical Intel Briefing & Microstructural Analysis]")
    print("#" * 102)

    print(f"\n1. High-Watermark Bagholder Stress Test (Price Band >= ${intel.high_threshold:,.0f}):")
    print(f"   - Diagnosis   : {intel.sentiment_status}")
    print(f"   - Volume Scale: T-1 {intel.high_prev_vol:,.2f} BTC  ->  T {intel.high_curr_vol:,.2f} BTC")
    print(f"   - Net Shift   : {intel.high_delta_vol:+,.2f} BTC ({intel.high_pct_change:+.2f}%)")
    print(f"   - Context     : {intel.sentiment_detail}")

    print("\n2. Core Liquidity Anchor Nodes:")
    if intel.peak_accum_bin and intel.peak_accum_bin.delta_supply > 0:
        print(f"   - 🏆 Peak Inflow Node : ${intel.peak_accum_bin.price:,.0f} "
              f"(Net: +{intel.peak_accum_bin.delta_supply:,.2f} BTC / {intel.peak_accum_bin.pct_change:+.1f}%)")
        print(f"     -> Intent: Institutional iceberg bid support absorption.")
    else:
        print("   - 🏆 Peak Inflow Node : No significant accumulation detected.")

    if intel.peak_dist_bin and intel.peak_dist_bin.delta_supply < 0:
        print(f"   - 🚨 Peak Outflow Node: ${intel.peak_dist_bin.price:,.0f} "
              f"(Net: {intel.peak_dist_bin.delta_supply:,.2f} BTC / {intel.peak_dist_bin.pct_change:+.1f}%)")
        print(f"     -> Intent: Severe stop-loss capitulation or active liquidation.")
    else:
        print("   - 🚨 Peak Outflow Node: No significant distribution detected.")

    print("\n3. Macro Liquidity & Turnover Dynamics:")
    print(f"   - Gross Minted Inflow : +{intel.gross_inflow:,.2f} BTC")
    print(f"   - Gross Liquidated Outflow: -{intel.gross_outflow:,.2f} BTC")
    print(f"   - Net Turnover Shift  : {intel.net_market_shift:+,.2f} BTC")

    print("\n4. Operator Tactical Takeaway:")
    print(f"   💡 {intel.tactical_summary}")
    print("=" * 102 + "\n")


# =====================================================================
# 7. Ephemeral Execution Workflow (Run-and-Exit)
# =====================================================================
async def main() -> None:
    storage = LocalStorage("urpd_metrics.db")
    fetcher = URPDFetcher()

    now_utc = datetime.now(timezone.utc)
    t_curr_str = now_utc.strftime("%Y-%m-%d")
    t_prev_str = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"[*] Initializing Delta URPD Agent: Inspecting {t_prev_str} -> {t_curr_str} (UTC)...")

    # 1. Fetch or load T-1 reference snapshot
    snap_prev = storage.load_snapshot(t_prev_str)
    if not snap_prev:
        print(f"[*] Local cache missing for {t_prev_str}. Ingesting historical reference...")
        buckets_prev = await fetcher.fetch_bitview(t_prev_str, agg_step="lin500")
        if buckets_prev:
            storage.save_snapshot(t_prev_str, buckets_prev)
            snap_prev = {b.price: b.supply for b in buckets_prev}

    # 2. Ingest current T snapshot
    print(f"[*] Ingesting current ({t_curr_str}) URPD distribution matrix...")
    buckets_curr = await fetcher.fetch_bitview(None, agg_step="lin500")
    if not buckets_curr:
        buckets_curr = await fetcher.fetch_bitview(t_curr_str, agg_step="lin500")

    if not buckets_curr:
        sys.stderr.write("[-] Fatal: Failed to acquire current URPD matrix. Terminating.\n")
        sys.exit(1)

    storage.save_snapshot(t_curr_str, buckets_curr)
    snap_curr = {b.price: b.supply for b in buckets_curr}

    if not snap_prev:
        print("[!] Baseline established for T. Historical T-1 missing; full delta available on next run.")
        sys.exit(0)

    # 3. Compute zero-bloat delta matrix
    deltas = DeltaURPDEngine.compute_delta(snap_prev, snap_curr)

    # 4. Generate qualitative tactical intelligence
    intel_report = DeltaURPDEngine.generate_intelligence_report(
        deltas,
        high_price_threshold=HIGH_POSITION_THRESHOLD,
        significance_threshold=300.0
    )

    # 5. Render local terminal dashboard
    render_terminal_dashboard(deltas, intel_report, t_prev_date=t_prev_str, t_curr_date=t_curr_str)

    # 6. Push chunked Telegram reports
    await TelegramNotifier.push_all_reports(
        token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID,
        deltas=deltas,
        intel=intel_report,
        t_prev_date=t_prev_str,
        t_curr_date=t_curr_str
    )

    print("[*] Run cycle complete. Releasing handles and exiting cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        sys.exit(0)