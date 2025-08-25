# alpaca_options.py — FULL swing bot (stocks-only execution, options-as-signal)
# ---------------------------------------------------------------------------
# What’s inside (v2 — observability & Polygon options-first)
# - Heavy, structured logging with decision traces (why each ticker was kept/filtered)
# - Env toggles for DEBUG, dry-run, scan size, and entry style (market vs protected limit)
# - Polygon V3 Options usage (nearest expiry, ATM call/put mids, expected move, OI/Skew)
# - GPT is supportive (for catalyst memo), not a blocker for targets
# - Immediate research on start + daily schedule; real entry window 09:35–09:55 ET
# - ATR initial stop, trailing stop, time exit
# - Telegram polling optional; raw HTTP send only (no 409 conflicts on Render)
# - Uses your env names: ALPACA_UNHOLY_KEY/SECRET, Stocks_GPT_TELEGRAM_BOT_TOKEN
# ---------------------------------------------------------------------------

import os
import sys
import json
import time
import math
import uuid
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # py<3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

import numpy as np
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

# ---- Vendors
from openai import OpenAI
from polygon import RESTClient as PolygonREST
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    LimitOrderRequest,
    MarketOrderRequest,
    StopOrderRequest,
    ClosePositionRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce

# Telegram (commands are optional)
try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
    TELEGRAM_IMPORTED = True
except Exception:
    TELEGRAM_IMPORTED = False

# =============================================================================
# Settings & ENV
# =============================================================================
@dataclass
class Settings:
    # Universe filters
    MIN_PRICE: float = 2.0
    MAX_PRICE: float = 400.0
    MIN_AVG_DOLLAR_VOL: float = 2_000_000
    MIN_MARKET_CAP: float = 100_000_000
    MAX_MARKET_CAP: float = 20_000_000_000
    MAX_UNIVERSE: int = 400

    # Holding horizon
    HOLD_MIN_DAYS: int = 10
    HOLD_MAX_DAYS: int = 60

    # Scoring weights
    W_FUND: float = 0.35
    W_EVENT: float = 0.20
    W_OPTIONS: float = 0.25   # ↑ emphasize options signals since you pay for it
    W_ATTENTION: float = 0.05
    W_TECH: float = 0.15

    MIN_SCORE: float = 60.0   # slightly relaxed

    # Risk
    RISK_PER_TRADE_BP: int = 100  # 1% per ATR
    MAX_POSITION_PCT: float = 0.08
    INIT_STOP_ATR_MULT: float = 1.25
    TRAIL_ATR_MULT_BEFORE_15: float = 1.5
    TRAIL_ATR_MULT_AFTER_15: float = 1.0

    # Schedules (ET)
    DAILY_RESEARCH_ET: Tuple[int, int] = (16, 30)
    ENTRY_WINDOW_ET: Tuple[int, int] = (9, 35)
    ENTRY_WINDOW_ET_END: Tuple[int, int] = (9, 55)

    # Scanning
    SCAN_LIMIT: int = int(os.getenv("SCAN_LIMIT", "200"))  # how many symbols to scan in detail

    # Behavior toggles
    PLACE_ORDERS: bool = os.getenv("PLACE_ORDERS", "true").lower() == "true"
    ENTRY_STYLE: str = os.getenv("ENTRY_STYLE", "protected_limit")  # protected_limit|market
    USE_POLYGON_OPTIONS: bool = os.getenv("USE_POLYGON_OPTIONS", "true").lower() == "true"
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"  # log everything, skip orders
    FORCE_TEST_SYMBOL: Optional[str] = os.getenv("FORCE_TEST_SYMBOL")  # e.g., AAPL to always include


SETTINGS = Settings()
ET = ZoneInfo("America/New_York")

# Env
ALPACA_KEY = os.getenv("ALPACA_UNHOLY_KEY")
ALPACA_SECRET = os.getenv("ALPACA_UNHOLY_SECRET_KEY")
ALPACA_IS_PAPER = os.getenv("ALPACA_PAPER_TRADING", "true").lower() == "true"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano")

TELEGRAM_TOKEN = os.getenv("Stocks_GPT_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_USER_ID")
TELEGRAM_POLLING = os.getenv("TELEGRAM_POLLING", "false").lower() == "true"  # default false on Render

# Logging config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("swing_investor_bot.log", encoding="utf-8")],
)
log = logging.getLogger("swingbot")

# Clients
alpaca = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_IS_PAPER)
poly = PolygonREST(POLYGON_API_KEY)
openai = OpenAI(api_key=OPENAI_API_KEY)

# Global State
PORTFOLIO: Dict[str, Dict[str, Any]] = {}
WATCHLIST: List[str] = []
CANDIDATES: Dict[str, Dict[str, Any]] = {}
PAUSED: bool = False

# =============================================================================
# Utility & Telemetry
# =============================================================================

def now_et() -> datetime:
    return datetime.now(ET)


def is_between(now: datetime, start: Tuple[int, int], end: Tuple[int, int]) -> bool:
    t = now.time()
    return dtime(*start) <= t <= dtime(*end)


def send_telegram(msg: str):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


def jlog(event: str, **kwargs):
    """Structured JSON log to make Render logs useful for step-by-step tracing."""
    payload = {"event": event, "ts": now_et().isoformat(), **kwargs}
    log.info(json.dumps(payload, default=str))


# ---- HTTP helper for Polygon v3 endpoints (options)
POLY_V3 = "https://api.polygon.io"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def poly_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = params.copy() if params else {}
    p["apiKey"] = POLYGON_API_KEY
    url = POLY_V3 + path
    t0 = time.time()
    r = requests.get(url, params=p, timeout=8)
    dt = int((time.time() - t0)*1000)
    jlog("polygon_http", path=path, status=r.status_code, ms=dt, params={k:v for k,v in p.items() if k!='apiKey'})
    r.raise_for_status()
    return r.json()


# =============================================================================
# Polygon Data Fetchers
# =============================================================================
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def list_active_snapshots(limit=400) -> List[str]:
    try:
        try:
            snaps = poly.list_snapshot_tickers(market_type="stocks")
        except AttributeError:
            snaps = poly.get_snapshot_all_tickers()
    except Exception as e:
        log.error(f"snapshot fetch error: {e}")
        return []

    rows = []
    for s in snaps:
        try:
            tkr = getattr(s, "ticker", None) or s.get("ticker")
            day = getattr(s, "day", None)
            v = getattr(day, "v", None)
            last_trade = getattr(getattr(s, "ticker", None), "last_trade", None)
            price = getattr(last_trade, "price", None)
            if not tkr or not price or not v:
                continue
            rows.append((tkr, float(price), int(v)))
        except Exception:
            continue

    if not rows:
        return []
    df = pd.DataFrame(rows, columns=["symbol", "price", "vol"])  # type: ignore
    df["dollar_vol"] = df["price"] * df["vol"]
    df = df[(df["price"].between(SETTINGS.MIN_PRICE, SETTINGS.MAX_PRICE)) & (df["dollar_vol"] >= SETTINGS.MIN_AVG_DOLLAR_VOL)]
    df = df.sort_values("dollar_vol", ascending=False).head(limit)
    return df["symbol"].tolist()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_aggs(ticker: str, timespan="day", window=90) -> pd.DataFrame:
    end = now_et().date()
    start = end - timedelta(days=365)
    try:
        aggs = poly.get_aggs(ticker, 1, timespan, str(start), str(end))
    except Exception as e:
        jlog("get_aggs_error", ticker=ticker, err=str(e))
        return pd.DataFrame([])
    if not aggs:
        return pd.DataFrame([])
    df = pd.DataFrame([{ "t": a.timestamp, "o": a.open, "h": a.high, "l": a.low, "c": a.close, "v": a.volume } for a in aggs ])
    return df.tail(window)


def atr(df: pd.DataFrame, period=14) -> Optional[float]:
    if df.empty or len(df) < period + 1:
        return None
    h, l, c = df["h"].values, df["l"].values, df["c"].values
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    return float(pd.Series(tr).rolling(period).mean().dropna().iloc[-1])


def momentum_20d(df: pd.DataFrame) -> Optional[float]:
    if df.empty or len(df) < 20:
        return None
    return float((df["c"].iloc[-1] / df["c"].iloc[-20] - 1.0) * 100.0)


def get_fundamentals_snapshot(ticker: str) -> Dict[str, Any]:
    out = {"rev_yoy": None, "eps_yoy": None, "margin_delta": None, "mcap": None}
    try:
        fin = poly.get_financials(ticker=ticker, limit=4)
        if fin:
            rows = []
            for f in fin:
                inc = getattr(f, "income_statement", None) or {}
                rows.append({
                    "rev": float(getattr(inc, "revenues", 0) or 0),
                    "eps": float(getattr(inc, "eps_basic", 0) or 0),
                    "gp": float(getattr(inc, "gross_profit", 0) or 0),
                    "cor": float(getattr(inc, "cost_of_revenue", 0) or 0),
                })
            if len(rows) >= 2:
                r0, r1 = rows[0]["rev"], rows[1]["rev"]
                e0, e1 = rows[0]["eps"], rows[1]["eps"]
                rev_y = ((r0 - r1) / abs(r1)) * 100 if r1 else None
                eps_y = ((e0 - e1) / abs(e1)) * 100 if e1 else None
                md = (rows[0]["gp"] - rows[0]["cor"]) - (rows[1]["gp"] - rows[1]["cor"]) if all(x is not None for x in [rows[0]["gp"], rows[0]["cor"], rows[1]["gp"], rows[1]["cor"]]) else None
                out.update({"rev_yoy": rev_y, "eps_yoy": eps_y, "margin_delta": md})
        snap = poly.get_snapshot_ticker(ticker=ticker, market_type="stocks")
        out["mcap"] = getattr(getattr(snap, "ticker", None), "market_cap", None)
    except Exception as e:
        jlog("fundamentals_error", ticker=ticker, err=str(e))
    return out


# ---- Options enrichment using Polygon v3 (contracts + quotes/snapshots)
def get_options_signals(ticker: str) -> Dict[str, Any]:
    out = {"iv_pct": None, "oi_total": None, "exp_move_pct": None, "skew_call_over_put": None,
           "atm_strike": None, "expiry": None, "call_id": None, "put_id": None}
    try:
        # Spot
        snap = poly.get_snapshot_ticker(ticker=ticker, market_type="stocks")
        spot_obj = getattr(getattr(snap, "ticker", None), "last_trade", None)
        spot = float(getattr(spot_obj, "price", 0.0) or 0.0)
        if not spot:
            return out

        if not SETTINGS.USE_POLYGON_OPTIONS:
            return out

        # 1) Find nearest expiry via reference contracts
        today = now_et().date().isoformat()
        j = poly_get("/v3/reference/options/contracts", {
            "underlying_ticker": ticker,
            "limit": 500,
            "expiration_date.gte": today,
            "order": "asc",
            "sort": "expiration_date",
        })
        results = j.get("results", [])
        if not results:
            return out
        expiries = []
        for r in results:
            exp = r.get("expiration_date")
            if exp and exp not in expiries:
                expiries.append(exp)
        expiry = expiries[0]
        out["expiry"] = expiry
        # 2) Pick ATM strike (closest to spot) among this expiry
        same_exp = [r for r in results if r.get("expiration_date") == expiry]
        strikes = sorted({float(r.get("strike_price")) for r in same_exp if r.get("strike_price") is not None})
        if not strikes:
            return out
        atm = min(strikes, key=lambda k: abs(k - spot))
        out["atm_strike"] = atm
        atm_calls = [r for r in same_exp if r.get("contract_type") == "call" and abs(float(r.get("strike_price", 0)) - atm) < 1e-9]
        atm_puts  = [r for r in same_exp if r.get("contract_type") == "put"  and abs(float(r.get("strike_price", 0)) - atm) < 1e-9]
        if not atm_calls or not atm_puts:
            return out
        call_id = atm_calls[0].get("contract_id")
        put_id  = atm_puts[0].get("contract_id")
        out.update({"call_id": call_id, "put_id": put_id})

        # 3) Get quotes (mid as proxy) — try quotes endpoint, fallback to snapshot
        def mid_from_quote(q: Dict[str, Any]) -> Optional[float]:
            a = q.get("ask_price") or q.get("P") or q.get("ask")
            b = q.get("bid_price") or q.get("p") or q.get("bid")
            if a and b:
                try:
                    return (float(a) + float(b)) / 2.0
                except Exception:
                    return None
            return None

        call_mid = None; put_mid = None; total_oi = None; skew = None
        try:
            qc = poly_get(f"/v3/quotes/options/{call_id}/last")
            qp = poly_get(f"/v3/quotes/options/{put_id}/last")
            call_mid = mid_from_quote(qc.get("results", {}) or {})
            put_mid  = mid_from_quote(qp.get("results", {}) or {})
        except Exception as e:
            jlog("options_quotes_error", ticker=ticker, err=str(e))
            # fallback to snapshots
            sc = poly_get(f"/v3/snapshot/options/{ticker}/{call_id}")
            sp = poly_get(f"/v3/snapshot/options/{ticker}/{put_id}")
            call_mid = mid_from_quote((sc.get("results", {}) or {}).get("latest_quote", {}))
            put_mid  = mid_from_quote((sp.get("results", {}) or {}).get("latest_quote", {}))
        # Open interest if present on metadata
        try:
            # some contracts return oi on reference or snapshot
            oi_c = atm_calls[0].get("open_interest")
            oi_p = atm_puts[0].get("open_interest")
            if oi_c is None or oi_p is None:
                sc2 = poly_get(f"/v3/snapshot/options/{ticker}/{call_id}")
                sp2 = poly_get(f"/v3/snapshot/options/{ticker}/{put_id}")
                oi_c = oi_c or ((sc2.get("results", {}) or {}).get("open_interest"))
                oi_p = oi_p or ((sp2.get("results", {}) or {}).get("open_interest"))
            if oi_c is not None and oi_p is not None:
                total_oi = float(oi_c) + float(oi_p)
                skew = round(float(oi_c) / (float(oi_p) + 1e-6), 2)
        except Exception:
            pass

        if call_mid and put_mid and spot:
            out["exp_move_pct"] = round(((call_mid + put_mid) / spot) * 100.0, 2)
        out["oi_total"] = total_oi
        out["skew_call_over_put"] = skew
        # IV percentile proxy via realized vol (fallback)
        hist = get_aggs(ticker, "day", 252)
        if not hist.empty:
            rv = hist["c"].pct_change().rolling(30).std().dropna() * math.sqrt(252)
            if len(rv) > 20:
                cur = rv.iloc[-1]
                iv_pct = float((rv <= cur).mean() * 100)
                out["iv_pct"] = round(iv_pct, 1)
        jlog("options_enriched", ticker=ticker, **{k:v for k,v in out.items() if v is not None})
    except Exception as e:
        jlog("options_enrich_error", ticker=ticker, err=str(e))
    return out


# =============================================================================
# GPT (supportive, non-blocking)
# =============================================================================

def gpt_web_research_memo(ticker: str, facts: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""
Be a concise sell-side analyst for 2–12 week swing trades. Ticker: {ticker}
Summarize fresh catalysts (IR/EDGAR/news), narratives on Reddit/X, and return JSON only:
{{"thesis":"...","links":["..."],"confidence":55}}
Facts: {json.dumps(facts)[:1500]}
"""
    try:
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": "Return compact JSON only."},{"role":"user","content":prompt}],
            temperature=0.2,
        )
        txt = resp.choices[0].message.content or "{}"
        data = json.loads(txt)
        return {"thesis": data.get("thesis"), "links": data.get("links", []), "confidence": int(data.get("confidence", 55))}
    except Exception as e:
        jlog("gpt_memo_error", ticker=ticker, err=str(e))
        return {"thesis": None, "links": [], "confidence": 55}


# =============================================================================
# Scoring & Risk
# =============================================================================

def score_breakdown(f: Dict[str, Any], opt: Dict[str, Any], mom20: Optional[float]) -> Dict[str,float]:
    s_fund = 0.0
    if f.get("rev_yoy") is not None: s_fund += np.clip(f["rev_yoy"]/20, -1, 1)*50
    if f.get("eps_yoy") is not None: s_fund += np.clip(f["eps_yoy"]/20, -1, 1)*50
    if f.get("margin_delta") and f["margin_delta"]>0: s_fund += 5
    s_fund = float(np.clip(s_fund, 0, 100))

    s_event = 50.0  # neutral; memo nudges via confidence later

    s_opt = 0.0
    if opt.get("iv_pct") is not None: s_opt += (opt["iv_pct"]-50)/50*40
    if opt.get("oi_total") and opt["oi_total"]>10_000: s_opt += 10
    if opt.get("skew_call_over_put") and opt["skew_call_over_put"]>1.25: s_opt += 10
    if opt.get("exp_move_pct") and opt["exp_move_pct"]>4: s_opt += min(10, (opt["exp_move_pct"]-4))
    s_opt = float(np.clip(s_opt, 0, 100))

    s_attn = 50.0

    s_tech = 0.0
    if mom20 is not None: s_tech += np.clip(mom20/20, -1, 1)*60
    s_tech = float(np.clip(s_tech, 0, 100))

    total = (Settings.W_FUND*s_fund + Settings.W_EVENT*s_event + Settings.W_OPTIONS*s_opt + Settings.W_ATTENTION*s_attn + Settings.W_TECH*s_tech)*100
    return {"fund":s_fund,"event":s_event,"opt":s_opt,"attn":s_attn,"tech":s_tech,"total":float(np.clip(total,0,100))}


def position_size_from_atr(equity: float, atr_val: float, price: float) -> int:
    if not atr_val or atr_val <= 0 or price <= 0:
        return 0
    per_name_risk = (Settings.RISK_PER_TRADE_BP/10000.0)*equity
    shares = int(per_name_risk/max(1e-6, atr_val))
    cap_shares = int((Settings.MAX_POSITION_PCT*equity)/price)
    return max(0, min(shares, cap_shares))


def compute_initial_stop(entry: float, atr_val: float) -> float:
    return round(max(0.01, entry - Settings.INIT_STOP_ATR_MULT*atr_val), 2)


def compute_trailing_stop(highest_close: float, atr_val: float, profit_ratio: float) -> float:
    mult = Settings.TRAIL_ATR_MULT_AFTER_15 if profit_ratio>=0.15 else Settings.TRAIL_ATR_MULT_BEFORE_15
    return round(highest_close - mult*atr_val, 2)


def account_equity() -> float:
    try:
        acct = alpaca.get_account()
        return float(acct.equity)
    except Exception as e:
        jlog("equity_error", err=str(e))
        return 0.0


def place_entry(symbol: str, qty: int, ref_price: float) -> Optional[str]:
    if SETTINGS.DRY_RUN or not SETTINGS.PLACE_ORDERS:
        jlog("order_skipped", symbol=symbol, qty=qty, reason="DRY_RUN or PLACE_ORDERS=false")
        return "DRY_RUN"
    try:
        if SETTINGS.ENTRY_STYLE == "market":
            od = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
        else:
            limit_price = round(ref_price*1.003, 2)
            od = LimitOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY, limit_price=limit_price, time_in_force=TimeInForce.DAY, extended_hours=False)
        odr = alpaca.submit_order(order_data=od)
        return getattr(odr, "id", None) or "OK"
    except Exception as e:
        jlog("order_error", symbol=symbol, err=str(e))
        return None


def place_stop(symbol: str, qty: int, stop_price: float) -> Optional[str]:
    if SETTINGS.DRY_RUN or not SETTINGS.PLACE_ORDERS:
        return "DRY_RUN"
    try:
        od = StopOrderRequest(symbol=symbol, qty=qty, side=OrderSide.SELL, stop_price=stop_price, time_in_force=TimeInForce.GTC)
        s = alpaca.submit_order(order_data=od)
        return getattr(s, "id", None) or "OK"
    except Exception as e:
        jlog("stop_error", symbol=symbol, err=str(e))
        return None


# =============================================================================
# Research + Selection
# =============================================================================
async def daily_research_and_selection():
    global WATCHLIST, CANDIDATES
    run_id = str(uuid.uuid4())[:8]
    jlog("research_start", run_id=run_id)
    try:
        universe = list_active_snapshots()
        if not universe:
            universe = ["AAPL","MSFT","NVDA","TSLA","META","AMZN","AMD","GOOGL","NFLX","CRM"]
            jlog("universe_fallback", run_id=run_id, size=len(universe))
        WATCHLIST = universe[:Settings.MAX_UNIVERSE]
        jlog("watchlist_ready", run_id=run_id, size=len(WATCHLIST))

        picked: Dict[str, Dict[str, Any]] = {}
        scanned = 0
        reject_counts = {"nodata":0,"mom":0,"mcap":0,"score":0,"target":0}

        for sym in WATCHLIST[:SETTINGS.SCAN_LIMIT]:
            scanned += 1
            df = get_aggs(sym, "day", 90)
            if df.empty:
                reject_counts["nodata"] += 1
                jlog("rej_nodata", run_id=run_id, sym=sym)
                continue
            mom20 = momentum_20d(df)
            if mom20 is None or mom20 < -5:
                reject_counts["mom"] += 1
                jlog("rej_mom", run_id=run_id, sym=sym, mom20=mom20)
                continue
            a = atr(df, 14)
            price = float(df["c"].iloc[-1])
            f = get_fundamentals_snapshot(sym)
            if f.get("mcap") and not (Settings.MIN_MARKET_CAP <= f["mcap"] <= Settings.MAX_MARKET_CAP):
                reject_counts["mcap"] += 1
                jlog("rej_mcap", run_id=run_id, sym=sym, mcap=f.get("mcap"))
                continue
            opt = get_options_signals(sym)
            sb = score_breakdown(f, opt, mom20)
            total = sb["total"]
            if total < Settings.MIN_SCORE:
                reject_counts["score"] += 1
                jlog("rej_score", run_id=run_id, sym=sym, score=total, breakdown=sb)
                continue

            # Target & time (non-blocking): use options exp. move or 2*ATR as fallback
            exp_move = opt.get("exp_move_pct") or 6.0
            tgt_default = round(price * (1 + max(0.08, exp_move*0.6/100)), 2)
            memo = gpt_web_research_memo(sym, {"price":price,"atr":a,"mom20":mom20,**f,**{k:v for k,v in opt.items() if v is not None}})
            target = None
            try:
                # if your GPT memo returns a numeric target, prefer it; else default
                target = float(memo.get("target_price"))  # may not exist
            except Exception:
                target = None
            if not target or target <= price*1.03:
                target = tgt_default
            time_weeks = int(memo.get("time_weeks", 8)) if isinstance(memo, dict) else 8

            picked[sym] = {
                "price": price,
                "atr": a or 1.0,
                "facts": {"mom20":mom20, **f, **opt},
                "target": target,
                "time_weeks": time_weeks,
                "confidence": int(memo.get("confidence", 55)) if isinstance(memo, dict) else 55,
                "score_breakdown": sb,
            }
            jlog("kept", run_id=run_id, sym=sym, price=price, atr=a, exp_move=opt.get("exp_move_pct"), target=target, score=total, sb=sb)

        if SETTINGS.FORCE_TEST_SYMBOL and SETTINGS.FORCE_TEST_SYMBOL not in picked:
            sym = SETTINGS.FORCE_TEST_SYMBOL
            df = get_aggs(sym, "day", 90)
            if not df.empty:
                a = atr(df,14) or 1.0
                price = float(df["c"].iloc[-1])
                picked[sym] = {"price":price, "atr":a, "facts":{}, "target": round(price*1.08,2), "time_weeks":6, "confidence":80, "score_breakdown":{}}
                jlog("force_added", run_id=run_id, sym=sym)

        CANDIDATES = picked
        jlog("research_done", run_id=run_id, scanned=scanned, kept=len(CANDIDATES), rejects=reject_counts)
        if not CANDIDATES:
            send_telegram("No swing candidates passed filters today.")
        else:
            summary = [f"{k} tgt {v['target']} ({v['time_weeks']}w) score ~{int(v['score_breakdown'].get('total',0))}" for k,v in list(CANDIDATES.items())[:10]]
            send_telegram(f"📋 Candidates:\n{chr(10).join(summary)}")
    except Exception as e:
        jlog("research_error", run_id=run_id, err=str(e))
        send_telegram(f"Research error: {e}")


# =============================================================================
# Entry & Monitoring
# =============================================================================
async def entry_window_task():
    await asyncio.sleep(1)
    while True:
        n = now_et()
        if n.weekday() >= 5:
            await asyncio.sleep(300); continue
        if is_between(n, Settings.ENTRY_WINDOW_ET, Settings.ENTRY_WINDOW_ET_END):
            if PAUSED or not CANDIDATES:
                await asyncio.sleep(30); continue
            ranked = sorted(CANDIDATES.items(), key=lambda kv: (kv[1]["confidence"], kv[1]["target"]/max(1e-6, kv[1]["atr"])), reverse=True)[:3]
            eq = account_equity()
            jlog("entry_window_open", candidates=len(CANDIDATES), trying=len(ranked), equity=eq)
            for sym, data in ranked:
                qty = position_size_from_atr(eq, data["atr"], data["price"])
                if qty <= 0:
                    jlog("skip_qty_zero", sym=sym, price=data["price"], atr=data["atr"], equity=eq)
                    continue
                order_id = place_entry(sym, qty, data["price"])
                if order_id:
                    send_telegram(f"➡️ BUY {qty} {sym} @ {Settings.ENTRY_STYLE} (ref {data['price']:.2f}) | tgt {data['target']} | stop {compute_initial_stop(data['price'], data['atr'])}")
                    PORTFOLIO[sym] = {
                        "qty": qty,
                        "avg_price": data["price"],
                        "stop_id": None,
                        "target": data["target"],
                        "target_date": (now_et() + timedelta(weeks=data["time_weeks"])).date(),
                        "opened_at": now_et(),
                        "atr": data["atr"],
                        "highest_close": data["price"],
                    }
                    stp = compute_initial_stop(data["price"], data["atr"])  # place after entry
                    sid = place_stop(sym, qty, stp)
                    if sid:
                        PORTFOLIO[sym]["stop_id"] = sid
                    jlog("entry_order", sym=sym, qty=qty, order_id=order_id, stop=sid, target=data["target"]) 
            await asyncio.sleep(120)
        else:
            await asyncio.sleep(20)


async def monitor_positions_task():
    while True:
        if not PORTFOLIO:
            await asyncio.sleep(60); continue
        try:
            for sym, pos in list(PORTFOLIO.items()):
                df = get_aggs(sym, "day", 10)
                if df.empty: continue
                last_close = float(df["c"].iloc[-1])
                pos["highest_close"] = max(pos.get("highest_close", last_close), last_close)
                pr = (last_close - pos["avg_price"]) / pos["avg_price"]
                trail = compute_trailing_stop(pos["highest_close"], pos["atr"], pr)
                if last_close <= trail:
                    try:
                        if not SETTINGS.DRY_RUN and SETTINGS.PLACE_ORDERS:
                            close_position_percent(sym, 1.0)
                        send_telegram(f"🛡️ Trailing stop hit: {sym} ~{last_close:.2f}")
                        PORTFOLIO.pop(sym, None)
                    except Exception as e:
                        jlog("trail_close_err", sym=sym, err=str(e))
                if now_et().date() >= pos["target_date"]:
                    try:
                        if not SETTINGS.DRY_RUN and SETTINGS.PLACE_ORDERS:
                            close_position_percent(sym, 1.0)
                        send_telegram(f"⏱ Time exit reached: {sym}")
                        PORTFOLIO.pop(sym, None)
                    except Exception as e:
                        jlog("time_exit_err", sym=sym, err=str(e))
        except Exception as e:
            jlog("monitor_err", err=str(e))
        await asyncio.sleep(300)


# =============================================================================
# Telegram Commands (only if polling ON)
# =============================================================================
if TELEGRAM_IMPORTED:
    async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            acct = alpaca.get_account(); eq = float(acct.equity); cash = float(acct.cash)
        except Exception:
            eq, cash = 0.0, 0.0
        await update.message.reply_text(json.dumps({
            "equity": eq, "cash": cash, "paused": PAUSED,
            "candidates": len(CANDIDATES), "watchlist": len(WATCHLIST), "positions": len(PORTFOLIO)
        }, indent=2))

    async def cmd_research(update: Update, context: ContextTypes.DEFAULT_TYPE):
        sym = (context.args[0] if context.args else "AAPL").upper()
        df = get_aggs(sym, "day", 90)
        if df.empty:
            await update.message.reply_text("No data."); return
        a = atr(df, 14) or 1.0
        price = float(df["c"].iloc[-1])
        f = get_fundamentals_snapshot(sym)
        o = get_options_signals(sym)
        sb = score_breakdown(f,o,momentum_20d(df))
        await update.message.reply_text(json.dumps({"facts":{**f,**o,"price":price,"atr":a}, "score":sb}, indent=2))


# =============================================================================
# Scheduling & Main
# =============================================================================
async def schedule_daily_research():
    while True:
        n = now_et()
        hh, mm = Settings.DAILY_RESEARCH_ET
        run_at = n.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if n > run_at: run_at += timedelta(days=1)
        await asyncio.sleep(max(1, int((run_at-n).total_seconds())))
        await daily_research_and_selection()


async def main():
    jlog("boot", paper=ALPACA_IS_PAPER, log_level=LOG_LEVEL, place_orders=SETTINGS.PLACE_ORDERS, entry_style=SETTINGS.ENTRY_STYLE, scan_limit=SETTINGS.SCAN_LIMIT)

    # Sync existing positions (best-effort)
    try:
        for p in alpaca.get_all_positions():
            if getattr(p, "side", "long") != "long":
                continue
            PORTFOLIO[p.symbol] = {
                "qty": int(float(p.qty)),
                "avg_price": float(p.avg_entry_price),
                "stop_id": None,
                "target": float(p.avg_entry_price) * 1.15,
                "target_date": (now_et() + timedelta(weeks=8)).date(),
                "opened_at": now_et(),
                "atr": 1.0,
                "highest_close": float(p.avg_entry_price),
            }
    except Exception as e:
        jlog("sync_positions_err", err=str(e))

    await daily_research_and_selection()

    if TELEGRAM_POLLING and TELEGRAM_TOKEN and TELEGRAM_IMPORTED:
        try:
            app = Application.builder().token(TELEGRAM_TOKEN).build()
            app.add_handler(CommandHandler("status", cmd_status))
            app.add_handler(CommandHandler("research", cmd_research))
            await app.initialize(); await app.start(); asyncio.create_task(app.updater.start_polling())
            jlog("telegram_polling_started")
        except Exception as e:
            jlog("telegram_poll_err", err=str(e))

    await asyncio.gather(
        schedule_daily_research(),
        entry_window_task(),
        monitor_positions_task(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        send_telegram("⏹️ Bot stopped.")

