#!/usr/bin/env python3
"""
swing_GPT.py — Full‑universe swing‑trading bot (single file)

What’s inside
- Polygon full‑market scan (one call) + 20‑day ADV cache via Grouped Daily (≈22 sessions)
- Filters: price bounds, ADV20, absolute gap%, volume‑spike vs ADV
- Benzinga: News (incl. channels: Guidance, Price Target, Analyst Ratings), Calendar Ratings, Earnings (JSON forced)
- GPT‑5 nano decision layer (strict JSON)
- Sizing rules: ≤5% notional, ≤1% risk (stock baseline)
- Alpaca paper bracket order (dry‑run by default)
- Telegram alerts (optional)
- Pretty console tables (Rich) + CSV export

Install
  pip install requests python-dotenv rich pydantic pandas numpy openai==1.*
  # Optional: pip install alpaca-trade-api

.env (needed)
  POLYGON_API_KEY=...
  BENZINGA_API_KEY=...
  OPENAI_API_KEY=...
  OPENAI_MODEL=gpt-5-nano     # falls back to gpt-4.1-mini
  ALPACA_KEY_ID=...
  ALPACA_SECRET_KEY=...
  ALPACA_PAPER_BASE=https://paper-api.alpaca.markets
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...
  # Optional default equity if you don't pass --equity
  ACCOUNT_EQUITY=25000

Usage examples
  python swing_GPT.py --equity 25000 --finalists 150 --send 40 --csv --telegram
  python swing_GPT.py --equity 50000 --rth --interval 120 --finalists 200 --send 60
  python swing_GPT.py --tickers IONS,TSLA   # uses env ACCOUNT_EQUITY or defaults to 25000
"""
from __future__ import annotations
import os, sys, math, time, json, re, html, argparse, csv, gzip
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich import box

# ------------------------- Setup -------------------------
load_dotenv()
console = Console()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
BENZINGA_API_KEY = os.getenv("BENZINGA_API_KEY")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-5-nano")
ALPACA_KEY_ID    = os.getenv("ALPACA_KEY_ID")
ALPACA_SECRET_KEY= os.getenv("ALPACA_SECRET_KEY")
ALPACA_PAPER_BASE= os.getenv("ALPACA_PAPER_BASE", "https://paper-api.alpaca.markets")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
ACCOUNT_EQUITY_ENV = os.getenv("ACCOUNT_EQUITY")

SESSION = requests.Session()
SESSION.headers.update({"Accept-Encoding": "gzip"})

DUBAI_OFFSET = 4  # UTC+4

# ------------------------- Helpers -------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def as_dubai(dt: datetime) -> str:
    return (dt.astimezone(timezone.utc) + timedelta(hours=DUBAI_OFFSET)).strftime("%Y-%m-%d %H:%M:%S +04")

def strip_html(s: Optional[str]) -> str:
    if not s: return ""
    s = html.unescape(s)
    return re.sub(r"<[^>]+>", "", s).strip()

# TA helpers for finalists
def ema(series: pd.Series, span: int) -> float:
    if series.empty: return float("nan")
    return float(series.ewm(span=span, adjust=False).mean().iloc[-1])

def rsi(series: pd.Series, period: int = 14) -> float:
    if len(series) < period + 1: return float("nan")
    delta = series.diff()
    up = delta.clip(lower=0).rolling(period).mean()
    down = (-delta.clip(upper=0)).rolling(period).mean()
    rs = up / down.replace(0, np.nan)
    return float(100 - (100 / (1 + rs)).iloc[-1])

def atr_from_hlc(df_daily: pd.DataFrame, period: int = 14) -> float:
    if len(df_daily) < period + 1: return float("nan")
    high = df_daily["h"]; low = df_daily["l"]; close = df_daily["c"]
    prev_close = close.shift(1)
    tr = pd.concat([(high-low), (high-prev_close).abs(), (low-prev_close).abs()], axis=1).max(axis=1)
    return float(tr.rolling(period).mean().iloc[-1])

# ------------------------- Polygon client -------------------------
class Polygon:
    BASE = "https://api.polygon.io"
    def __init__(self, key: str):
        if not key: raise ValueError("POLYGON_API_KEY missing")
        self.key = key
    def _get(self, path: str, params: dict | None = None) -> dict:
        p = dict(params or {}); p["apiKey"] = self.key
        url = f"{self.BASE}{path}"
        r = SESSION.get(url, params=p, timeout=60)
        if r.headers.get("Content-Encoding") == "gzip":
            r._content = gzip.decompress(r.content)
        console.print(f"[dim]Polygon → {r.url}[/dim]")
        r.raise_for_status()
        return r.json()
    def full_market_snapshot(self) -> dict:
        return self._get("/v2/snapshot/locale/us/markets/stocks/tickers")
    def grouped_daily(self, date_str: str) -> dict:
        return self._get(f"/v2/aggs/grouped/locale/us/market/stocks/{date_str}")
    def daily_aggs(self, ticker: str, days: int = 220) -> pd.DataFrame:
        end = now_utc().strftime("%Y-%m-%d")
        start = (now_utc() - timedelta(days=days*2)).strftime("%Y-%m-%d")
        data = self._get(f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}", {"adjusted":"true","sort":"asc","limit": days*3})
        rows = data.get("results") or []
        if not rows: return pd.DataFrame(columns=["t","o","h","l","c","v"])
        df = pd.DataFrame(rows)
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        for col in ["o","h","l","c","v"]:
            if col not in df.columns: df[col] = np.nan
        return df[["t","o","h","l","c","v"]].tail(days)

# ------------------------- ADV20 cache -------------------------
def trading_days(n=45):
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n, 0, -1)]

def build_or_load_adv20(poly: Polygon, cache_dir: str = "cache") -> pd.DataFrame:
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d")
    cache_file = Path(cache_dir) / f"adv20_{stamp}.parquet"
    if cache_file.exists():
        return pd.read_parquet(cache_file)

    vols: dict[str, list] = {}
    seen_sessions = 0
    for d in trading_days(60):  # generous window to get ≥22 sessions
        data = poly.grouped_daily(d)
        res = data.get("results") or []
        if not res:
            continue
        seen_sessions += 1
        for row in res:
            t = row.get("T"); v = row.get("v") or 0
            if t:
                vols.setdefault(t, []).append(v)
        if seen_sessions >= 22:
            break
    adv = {t: (float(np.mean(vs[-20:])) if len(vs) >= 1 else 0.0) for t, vs in vols.items()}
    df = pd.DataFrame({"ticker": list(adv.keys()), "adv20": list(adv.values())})
    df.to_parquet(cache_file, index=False)
    return df

# ------------------------- Benzinga client -------------------------
class Benzinga:
    BASE = "https://api.benzinga.com"
    HEADERS = {"accept": "application/json"}
    def __init__(self, token: str):
        if not token: raise ValueError("BENZINGA_API_KEY missing")
        self.token = token
    def _get(self, path: str, params: dict | None = None):
        p = dict(params or {}); p["token"] = self.token
        url = f"{self.BASE}{path}"
        r = SESSION.get(url, params=p, headers=self.HEADERS, timeout=30)
        console.print(f"[dim]Benzinga → {r.url}[/dim]")
        r.raise_for_status()
        return r.json()
    def news(self, ticker: str, page_size: int = 6, channels: Optional[str] = None) -> list[dict]:
        params = {"tickers": ticker, "pageSize": page_size, "displayOutput": "full"}
        if channels: params["channels"] = channels
        data = self._get("/api/v2/news", params)
        return data if isinstance(data, list) else []
    def ratings(self, ticker: str, page_size: int = 6) -> dict:
        fields = "date,time,ticker,analyst_name,rating_current,rating_prior,pt_current,pt_prior,url"
        return self._get("/api/v2.1/calendar/ratings", {"parameters[tickers]": ticker, "pagesize": page_size, "fields": fields})
    def earnings(self, ticker: str, date_from: str, date_to: str, page_size: int = 20) -> dict:
        return self._get("/api/v2.1/calendar/earnings", {
            "parameters[tickers]": ticker,
            "parameters[date_from]": date_from,
            "parameters[date_to]": date_to,
            "pagesize": page_size
        })

# ------------------------- Data model -------------------------
@dataclass
class Metrics:
    ticker: str
    price: float
    prev_close: float
    adv20: int
    volume_today: int
    vwap_day: Optional[float]
    rsi14: Optional[float] = None
    atr14: Optional[float] = None
    ema20: Optional[float] = None
    ema50: Optional[float] = None
    sma200: Optional[float] = None
    day_low: Optional[float] = None
    day_high: Optional[float] = None
    @property
    def gap_pct(self) -> float:
        if not self.prev_close or math.isnan(self.prev_close): return 0.0
        return (self.price - self.prev_close) / self.prev_close * 100.0
    @property
    def vol_spike_x(self) -> float:
        return self.volume_today / max(1, self.adv20)

# ------------------------- OpenAI (GPT-5 nano) -------------------------
def call_gpt5_decision(packet: dict, account: dict, context: dict) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    SYSTEM = (
        "You are GPT-5, a portfolio research strategist. Return STRICT JSON per schema. "
        "Use fundamentals, technicals, and current sentiment/news to decide LONG/SHORT/PASS and define a swing plan. "
        "Obey risk: ≤5% notional, ≤1% loss at stop. Cite 3–6 sources."
    )
    USER_TMPL = f"""
DATA_PACKET = {json.dumps(packet, separators=(',',':'))}
ACCOUNT = {json.dumps(account, separators=(',',':'))}
CONTEXT = {json.dumps(context, separators=(',',':'))}
TASKS:
1) Research the ticker. Analyze fundamentals + technicals from DATA_PACKET.
2) Perform web sentiment scan (Twitter/Reddit/news) to identify catalysts and tone.
3) Decide: LONG / SHORT / PASS. Provide confidence 0–1.
4) Define swing strategy: entry_range [l,u], stop_loss, take_profits [..], r_multiple, max_hold_days.
5) Choose vehicle: STOCK or OPTIONS (exact contract(s) or spread).
6) Position sizing: obey ≤5% notional and ≤1% risk. Include computed shares/contracts.
7) Provide 3–6 short source citations.
OUTPUT: JSON ONLY.
""".strip()
    models_try = [OPENAI_MODEL, "gpt-4.1-mini"]
    last_err = None
    for m in models_try:
        try:
            resp = client.chat.completions.create(
                model=m,
                response_format={"type": "json_object"},
                messages=[{"role":"system","content":SYSTEM},{"role":"user","content":USER_TMPL}],
                temperature=0.2,
            )
            content = resp.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"OpenAI call failed: {last_err}")

# ------------------------- Alpaca (minimal REST) -------------------------
def place_bracket_order(symbol: str, side: str, qty: int, limit_px: float, stop_px: float, tp_px: float, live: bool=False):
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY):
        console.print("[yellow]Alpaca keys missing; skipping order.[/yellow]")
        return
    order = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side.lower(),
        "type": "limit",
        "time_in_force": "day",
        "limit_price": str(round(limit_px, 2)),
        "order_class": "bracket",
        "take_profit": {"limit_price": str(round(tp_px, 2))},
        "stop_loss": {"stop_price": str(round(stop_px, 2))}
    }
    if not live:
        console.print(f"[cyan]DRY-RUN Alpaca order[/cyan]: {json.dumps(order)}"); return
    url = f"{ALPACA_PAPER_BASE}/v2/orders"
    r = SESSION.post(url, json=order, headers={"APCA-API-KEY-ID":ALPACA_KEY_ID, "APCA-API-SECRET-KEY":ALPACA_SECRET_KEY}, timeout=30)
    console.print(f"[dim]Alpaca → {url}[/dim]")
    if r.status_code >= 300:
        console.print(f"[red]Alpaca error {r.status_code}[/red]: {r.text}")
    else:
        console.print(f"[green]Alpaca OK[/green]: {r.json()}")

# ------------------------- Telegram -------------------------
def tg_send(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = SESSION.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        if r.status_code >= 300:
            console.print(f"[red]Telegram error[/red]: {r.text}")
    except Exception as e:
        console.print(f"[yellow]Telegram send failed:[/yellow] {e}")

# ------------------------- Sizing -------------------------
def stock_qty(entry_mid: float, stop: float, equity: float) -> int:
    stop_dist = abs(entry_mid - stop)
    if stop_dist <= 0: return 0
    max_notional = 0.05 * equity
    max_risk = 0.01 * equity
    shares_by_risk = math.floor(max_risk / stop_dist)
    shares_by_notional = math.floor(max_notional / max(1e-6, entry_mid))
    return max(0, min(shares_by_risk, shares_by_notional))

# ------------------------- Scanner (full universe) -------------------------
def compute_candidates_full_universe(poly: Polygon,
                                     price_min=0.10, price_max=2000.0,
                                     adv_min=300_000,
                                     gap_abs_min_pct=7.0,
                                     vol_spike_min_x=2.5,
                                     limit: Optional[int]=None) -> List[dict]:
    snap = poly.full_market_snapshot()
    tickers = snap.get("tickers") or []
    if not tickers: return []

    adv20_df = build_or_load_adv20(poly)
    adv_map = dict(zip(adv20_df["ticker"].values, adv20_df["adv20"].values))

    cands: List[dict] = []
    for row in tickers:
        tkr = row.get("ticker")
        day = row.get("day") or {}
        last_p = (row.get("lastTrade") or {}).get("p") or day.get("c")
        prev_c = (row.get("prevDay") or {}).get("c")
        if not tkr or last_p is None or prev_c is None:
            continue
        if not (price_min <= last_p <= price_max):
            continue
        adv20 = adv_map.get(tkr, 0.0)
        if adv20 < adv_min:
            continue
        gap_pct = (last_p - prev_c) / max(prev_c, 1e-6) * 100.0
        if abs(gap_pct) < gap_abs_min_pct:
            continue
        vol_today = day.get("v") or 0
        vol_spike_x = vol_today / max(1, adv20)
        if vol_spike_x < vol_spike_min_x and abs(gap_pct) < 40.0:
            continue
        cands.append({
            "ticker": tkr,
            "price": float(last_p),
            "prev_close": float(prev_c),
            "gap_pct": round(gap_pct, 2),
            "day_range": [day.get("l"), day.get("h")],
            "volume": int(vol_today),
            "adv20": int(adv20),
            "vol_spike_x": round(vol_spike_x, 2),
            "vwap_day": day.get("vw"),
        })
        if limit and len(cands) >= limit:
            break
    cands.sort(key=lambda x: (abs(x["gap_pct"]) >= 40.0, x["vol_spike_x"], abs(x["gap_pct"])), reverse=True)
    return cands

# ------------------------- Enrichment (finalists) -------------------------
def enrich_finalist_metrics(poly: Polygon, row: dict) -> Metrics:
    t = row["ticker"]
    df = poly.daily_aggs(t, days=220)
    ema20v = ema(df["c"], 20) if not df.empty else float("nan")
    ema50v = ema(df["c"], 50) if not df.empty else float("nan")
    sma200v = float(df["c"].rolling(200).mean().iloc[-1]) if len(df) >= 200 else float("nan")
    rsi14v = rsi(df["c"], 14) if not df.empty else float("nan")
    atr14v = atr_from_hlc(df.rename(columns={"o":"o","h":"h","l":"l","c":"c"}), 14) if not df.empty else float("nan")
    day_low = row.get("day_range", [None, None])[0]
    day_high = row.get("day_range", [None, None])[1]
    return Metrics(
        ticker=t,
        price=row["price"],
        prev_close=row["prev_close"],
        adv20=row["adv20"],
        volume_today=row["volume"],
        vwap_day=row.get("vwap_day"),
        rsi14=rsi14v,
        atr14=atr14v,
        ema20=ema20v,
        ema50=ema50v,
        sma200=sma200v,
        day_low=day_low,
        day_high=day_high,
    )

# ------------------------- Benzinga bundle -------------------------
def benzinga_bundle(bz: Benzinga, ticker: str) -> dict:
    date_from = (now_utc() - timedelta(days=90)).strftime("%Y-%m-%d")
    date_to   = (now_utc() + timedelta(days=90)).strftime("%Y-%m-%d")
    try:
        news_all = bz.news(ticker, page_size=6)
        news_pt  = bz.news(ticker, page_size=6, channels="Price Target")
        news_ard = bz.news(ticker, page_size=6, channels="Analyst Ratings")
        news_guid= bz.news(ticker, page_size=6, channels="Guidance")
        ratings  = bz.ratings(ticker, page_size=6)
        earnings = bz.earnings(ticker, date_from=date_from, date_to=date_to, page_size=12)
    except Exception as e:
        console.print(f"[yellow]{ticker} Benzinga warn:[/yellow] {e}")
        news_all, news_pt, news_ard, news_guid, ratings, earnings = [], [], [], [], {}, {}
    bundle = {
        "news": [
            {"created": n.get("created"), "title": strip_html(n.get("title")), "channels": [c.get("name","") for c in n.get("channels",[])], "url": n.get("url")}
            for n in (news_all or [])
        ],
        "price_target_news": [{"created": n.get("created"), "title": strip_html(n.get("title")), "url": n.get("url")} for n in (news_pt or [])],
        "analyst_ratings_news": [{"created": n.get("created"), "title": strip_html(n.get("title")), "url": n.get("url")} for n in (news_ard or [])],
        "guidance_news": [{"created": n.get("created"), "title": strip_html(n.get("title")), "url": n.get("url")} for n in (news_guid or [])],
        "calendar_ratings": ratings.get("ratings", []),
        "calendar_earnings": earnings.get("earnings", []),
    }
    return bundle

# ------------------------- Packets → GPT -------------------------
def packet_from_metrics(m: Metrics, bz_bundle: dict) -> dict:
    return {
        "asof": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ticker": m.ticker,
        "price": m.price,
        "prev_close": m.prev_close,
        "gap_pct": round(m.gap_pct, 2),
        "day_range": [m.day_low, m.day_high],
        "volume": m.volume_today,
        "adv20": m.adv20,
        "vol_spike_x": round(m.vol_spike_x, 2),
        "vwap": m.vwap_day,
        "premarket_vwap": None,
        "rsi14": m.rsi14,
        "atr14": m.atr14,
        "ema20": m.ema20,
        "ema50": m.ema50,
        "sma200": m.sma200,
        "options": {"ivr": None, "chain_summary": {}, "notable_strikes": []},
        "fundamentals": {},
        "notes": f"mega gap flag={'true' if abs(m.gap_pct) >= 40 else 'false'}; "
                 f"{'above' if (m.vwap_day and m.price and m.price>m.vwap_day) else 'below' if (m.vwap_day and m.price) else 'no_vwap'} vwap; "
                 f"RSI={(m.rsi14 if m.rsi14 is not None else float('nan')):.1f}",
        "benzinga": bz_bundle,
    }

# ------------------------- Pretty printing -------------------------
def print_candidates_table(rows: List[dict], title: str = "Candidates"):
    table = Table(title=title, box=box.SIMPLE_HEAVY)
    table.add_column("#", justify="right")
    table.add_column("Ticker", style="bold")
    table.add_column("Price", justify="right")
    table.add_column("Gap%", justify="right")
    table.add_column("Vol xADV", justify="right")
    table.add_column("VWAP dev", justify="right")
    for i, r in enumerate(rows, 1):
        vwap_dev = ""
        if r.get("vwap_day") and r.get("price"):
            vwap_dev = f"{r['price'] - r['vwap_day']:+.2f}"
        table.add_row(str(i), r["ticker"], f"{r['price']:.2f}", f"{r['gap_pct']:.1f}", f"{r['vol_spike_x']:.2f}", vwap_dev)
    console.print(table)

def print_decisions_table(decisions: List[dict]):
    table = Table(title="Trade Decisions", box=box.SIMPLE_HEAVY, show_lines=False)
    table.add_column("Ticker", style="bold")
    table.add_column("Signal")
    table.add_column("Entry Range")
    table.add_column("Stop")
    table.add_column("TPs")
    table.add_column("R:R")
    table.add_column("Thesis")
    for d in decisions:
        tps = ", ".join([f"{tp:.2f}" for tp in d.get("take_profits", [])][:4])
        entry = d.get("entry_range", [])
        entry_s = f"{entry[0]:.2f}–{entry[1]:.2f}" if len(entry)==2 else "—"
        rr = f"1:{d.get('r_multiple', 0):.1f}"
        thesis = (d.get("thesis","") or "")[:120]
        table.add_row(
            d.get("ticker",""),
            d.get("signal",""),
            entry_s,
            f"{d.get('stop_loss',0):.2f}",
            tps or "—",
            rr,
            thesis
        )
    console.print(table)

# ------------------------- CSV export -------------------------
def export_csv(rows: List[dict], path: str):
    if not rows: return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader(); w.writerows(rows)
    console.print(f"[green]Saved CSV:[/green] {path}")

# ------------------------- Runner -------------------------
def run_scan_and_decide(universe_tickers: Optional[List[str]], equity: float, finalists: int, send_n: int,
                        csv_out: bool, telegram: bool, execute_live: bool, interval: Optional[int]=None,
                        loop: bool=False):
    # Friendly env/key checks
    if not POLYGON_API_KEY: console.print("[red]POLYGON_API_KEY missing[/red]"); sys.exit(1)
    if not BENZINGA_API_KEY: console.print("[red]BENZINGA_API_KEY missing[/red]"); sys.exit(1)
    if not OPENAI_API_KEY: console.print("[red]OPENAI_API_KEY missing[/red]"); sys.exit(1)

    poly = Polygon(POLYGON_API_KEY)
    bz = Benzinga(BENZINGA_API_KEY)

    def pass_once():
        console.rule("[bold]FULL-UNIVERSE SCAN[/bold]")
        # Universe selection
        if universe_tickers:
            snap = poly.full_market_snapshot().get("tickers", [])
            snap_map = {r.get("ticker"): r for r in snap}
            rows = []
            for t in universe_tickers:
                r = snap_map.get(t.upper())
                if not r: continue
                day = r.get("day") or {}
                last_p = (r.get("lastTrade") or {}).get("p") or day.get("c")
                prev_c = (r.get("prevDay") or {}).get("c")
                if last_p is None or prev_c is None: continue
                rows.append({
                    "ticker": t.upper(),
                    "price": float(last_p),
                    "prev_close": float(prev_c),
                    "gap_pct": (float(last_p) - float(prev_c))/max(float(prev_c),1e-6)*100.0,
                    "day_range": [day.get("l"), day.get("h")],
                    "volume": int(day.get("v") or 0),
                    "adv20": 0,
                    "vol_spike_x": 0.0,
                    "vwap_day": day.get("vw"),
                })
            adv20_df = build_or_load_adv20(poly)
            adv_map = dict(zip(adv20_df["ticker"].values, adv20_df["adv20"].values))
            cand_rows = []
            for r in rows:
                r["adv20"] = int(adv_map.get(r["ticker"], 0))
                r["vol_spike_x"] = r["volume"] / max(1, r["adv20"]) if r["adv20"] else 0.0
                if not (0.10 <= r["price"] <= 2000): continue
                if r["adv20"] < 300_000: continue
                if abs(r["gap_pct"]) < 7.0: continue
                if r["vol_spike_x"] < 2.5 and abs(r["gap_pct"]) < 40.0: continue
                cand_rows.append({**r, "gap_pct": round(r["gap_pct"],2), "vol_spike_x": round(r["vol_spike_x"],2)})
            cand_rows.sort(key=lambda x: (abs(x["gap_pct"])>=40.0, x["vol_spike_x"], abs(x["gap_pct"])), reverse=True)
            candidates = cand_rows
        else:
            candidates = compute_candidates_full_universe(poly)

        if not candidates:
            console.print("[yellow]No candidates passed filters.[/yellow]")
            return
        top_rows = candidates[:finalists]
        print_candidates_table(top_rows, title=f"Candidates (Top {len(top_rows)})")
        if csv_out:
            export_csv(top_rows, f"candidates_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")

        # Enrich finalists → metrics + Benzinga → GPT decisions for top send_n
        decisions: List[dict] = []
        send_rows = top_rows[:send_n]
        for r in send_rows:
            try:
                m = enrich_finalist_metrics(poly, r)
                bz_bundle = benzinga_bundle(bz, r["ticker"])
                packet = packet_from_metrics(m, bz_bundle)
                account = {"equity": equity, "open_risk_pct": 0.0, "max_positions": 8}
                context = {
                    "bias_notes": "Mega-gap fade rule for ≥40% up gaps; include penny stocks < $1.50 but exclude ADV20<300k.",
                    "upcoming_events": ["earnings", "FOMC", "CPI"],
                    "borrowing": {"shortable": True, "ssr": False}
                }
                decision = call_gpt5_decision(packet, account, context)
            except Exception as e:
                console.print(f"[red]{r['ticker']} enrich/AI failed:[/red] {e}")
                continue

            decisions.append(decision)
            if telegram:
                tps = ", ".join([f"{tp:.2f}" for tp in decision.get("take_profits", [])])
                msg = (
                    f"🎯 {decision.get('ticker')} {decision.get('signal')}\n"
                    f"Entry: {decision.get('entry_range')} | Stop: {decision.get('stop_loss')} | TP: [{tps or '—'}]\n"
                    f"Vehicle: {decision.get('vehicle')} | R:R≈ {decision.get('r_multiple')}\n"
                    f"Thesis: {str(decision.get('thesis',''))[:300]}"
                )
                tg_send(msg)

            # Optional execution (paper)
            try:
                if execute_live and decision.get("vehicle") == "STOCK" and decision.get("signal") in ("LONG","SHORT"):
                    entry = decision.get("entry_range") or [m.price, m.price]
                    entry_mid = float(sum(entry)/2.0)
                    stop = float(decision.get("stop_loss") or entry_mid*0.95)
                    tps = decision.get("take_profits") or [entry_mid*1.03]
                    tp = float(tps[0])
                    qty = decision.get("position_size", {}).get("shares")
                    if not qty:
                        qty = stock_qty(entry_mid, stop, equity)
                    side = "buy" if decision["signal"] == "LONG" else "sell"
                    if qty and qty > 0:
                        place_bracket_order(m.ticker, side, qty, entry_mid, stop, tp, live=False)  # flip to True for real paper order
            except Exception as e:
                console.print(f"[yellow]Execution warn {r['ticker']}:[/yellow] {e}")

        console.rule("[bold]DECISIONS[/bold]")
        print_decisions_table(decisions)
        if csv_out and decisions:
            export_csv(decisions, f"decisions_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")

    # one‑shot or loop
    pass_once()
    if not loop:
        return
    if not interval: interval = 120
    console.print(f"[bold]RTH loop every {interval}s[/bold] — Ctrl+C to stop")
    try:
        while True:
            pass_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[cyan]Stopped.[/cyan]")

# ------------------------- CLI -------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Swing GPT full‑universe bot")
    ap.add_argument("--equity", type=float, help="Account equity for sizing (USD). If omitted, uses env ACCOUNT_EQUITY or 25000.")
    ap.add_argument("--tickers", type=str, default="", help="Comma‑separated subset (optional), e.g., IONS,TSLA")
    ap.add_argument("--finalists", type=int, default=150, help="Top N candidates to enrich (default 150)")
    ap.add_argument("--send", type=int, default=40, help="Top M finalists to send to GPT (default 40)")
    ap.add_argument("--csv", action="store_true", help="Export CSV of candidates and decisions")
    ap.add_argument("--telegram", action="store_true", help="Send Telegram alerts for decisions")
    ap.add_argument("--execute", action="store_true", help="Place bracket orders on Alpaca (DRY‑RUN unless you edit live flag)")
    ap.add_argument("--rth", action="store_true", help="Run continuous RTH loop")
    ap.add_argument("--interval", type=int, default=120, help="Loop interval seconds (default 120)")
    return ap.parse_args()

# ------------------------- Main -------------------------
def main():
    args = parse_args()
    # Resolve equity (arg → env → default)
    src = "default"
    equity = 25000.0
    if args.equity is not None:
        equity = float(args.equity); src = "--equity"
    elif ACCOUNT_EQUITY_ENV:
        try:
            equity = float(ACCOUNT_EQUITY_ENV); src = "env ACCOUNT_EQUITY"
        except ValueError:
            pass
    console.print(f"[yellow]Using equity = ${equity:,.2f} (source: {src})[/yellow]")

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()] or None
    run_scan_and_decide(
        universe_tickers=tickers,
        equity=equity,
        finalists=args.finalists,
        send_n=args.send,
        csv_out=args.csv,
        telegram=args.telegram,
        execute_live=args.execute,
        interval=args.interval,
        loop=args.rth,
    )

if __name__ == "__main__":
    main()

