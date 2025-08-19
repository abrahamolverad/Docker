# swing_investor_bot.py
# Research-driven SWING bot: stocks-only execution, options-as-signal.
# Live chat via Telegram; GPT-5 nano performs web search + writes thesis memos.

import os, sys, asyncio, logging, math, time, json
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

# --- Vendors
from openai import OpenAI
from polygon import RESTClient as PolygonREST
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest, StopOrderRequest, GetOrdersRequest, ClosePositionRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.common.exceptions import APIError

# Telegram (bot commands)
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =========================
# Settings
# =========================
@dataclass
class Settings:
    # Strategy horizon
    HOLD_MIN_DAYS: int = 10
    HOLD_MAX_DAYS: int = 60

    # Universe & filters
    MIN_PRICE: float = 2.0
    MAX_PRICE: float = 400.0
    MIN_AVG_DOLLAR_VOL: float = 2_000_000
    MIN_MARKET_CAP: float = 100_000_000  # 100M
    MAX_MARKET_CAP: float = 20_000_000_000  # 20B (focus on SMID)
    MAX_UNIVERSE: int = 400

    # Scoring weights
    W_FUND: float = 0.35
    W_EVENT: float = 0.25
    W_OPTIONS: float = 0.15
    W_ATTENTION: float = 0.10
    W_TECH: float = 0.15

    # Entry threshold
    MIN_SCORE: float = 70.0

    # Risk
    RISK_PER_TRADE_BP: int = 100         # 1.00% of equity per ATR
    MAX_POSITION_PCT: float = 0.08       # 8% cap per name
    SECTOR_CAP_PCT: float = 0.25         # 25% cap per sector (placeholder if you map sectors)
    INIT_STOP_ATR_MULT: float = 1.25
    TRAIL_ATR_MULT_BEFORE_15: float = 1.5
    TRAIL_ATR_MULT_AFTER_15: float = 1.0

    # Rebalance timing (ET)
    DAILY_RESEARCH_ET: Tuple[int, int] = (16, 30)  # 4:30pm ET
    ENTRY_WINDOW_ET: Tuple[int, int] = (9, 35)     # Enter a few minutes after open

    # Concurrency
    CONCURRENCY: int = 6

SETTINGS = Settings()

# =========================
# Env & Logging
# =========================
load_dotenv()
ET = ZoneInfo("America/New_York")

ALPACA_KEY = os.getenv("ALPACA_UNHOLY_KEY")
ALPACA_SECRET = os.getenv("ALPACA_UNHOLY_SECRET_KEY")
ALPACA_IS_PAPER = os.getenv("ALPACA_PAPER_TRADING", "true").lower() == "true"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano")

TELEGRAM_TOKEN = os.getenv("Stocks_GPT_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_USER_ID")

if not all([ALPACA_KEY, ALPACA_SECRET, POLYGON_API_KEY, OPENAI_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
    missing = [k for k,v in {
        "ALPACA_UNHOLY_KEY":ALPACA_KEY, "ALPACA_UNHOLY_SECRET_KEY":ALPACA_SECRET,
        "POLYGON_API_KEY":POLYGON_API_KEY, "OPENAI_API_KEY":OPENAI_API_KEY,
        "Stocks_GPT_TELEGRAM_BOT_TOKEN":TELEGRAM_TOKEN, "TELEGRAM_USER_ID":TELEGRAM_CHAT_ID
    }.items() if not v]
    print(f"Missing env vars: {missing}")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("swing_investor_bot.log", encoding="utf-8")]
)
log = logging.getLogger("swingbot")

# =========================
# Clients
# =========================
alpaca = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_IS_PAPER)
poly = PolygonREST(POLYGON_API_KEY)
openai = OpenAI(api_key=OPENAI_API_KEY)

# =========================
# State
# =========================
PORTFOLIO: Dict[str, Dict[str, Any]] = {}      # symbol -> {qty, avg_price, stop_id, thesis, target, target_date, opened_at}
WATCHLIST: List[str] = []
CANDIDATES: Dict[str, Dict[str, Any]] = {}
PAUSED: bool = False

# =========================
# Helpers
# =========================
def now_et() -> datetime:
    return datetime.now(ET)

def is_between(now: datetime, hhmm_from: Tuple[int,int], hhmm_to: Tuple[int,int]) -> bool:
    t = now.time()
    start = dtime(*hhmm_from)
    end = dtime(*hhmm_to)
    return start <= t <= end

def send_telegram(msg: str):
    try:
        import requests
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

# =========================
# Polygon fetchers
# =========================
def list_active_snapshots(limit=SETTINGS.MAX_UNIVERSE):
    # Use list_snapshot_tickers (or fallback) to get liquid active names
    try:
        snaps = poly.list_snapshot_tickers(market_type="stocks")
    except AttributeError:
        snaps = poly.get_snapshot_all_tickers()  # older client, still works

    rows = []
    for s in snaps:
        try:
            tkr = getattr(s, "ticker", None) or s.get("ticker")
            last = getattr(getattr(s, "ticker", None), "last_trade", None)
            price = getattr(last, "price", None)
            v = getattr(getattr(s, "day", None), "v", None)
            if not tkr or not price or not v: continue
            rows.append((tkr, float(price), int(v)))
        except Exception:
            continue

    # Filter by price & approx dollar vol
    df = pd.DataFrame(rows, columns=["symbol","price","vol"])
    df["dollar_vol"] = df["price"]*df["vol"]
    df = df[(df["price"].between(SETTINGS.MIN_PRICE, SETTINGS.MAX_PRICE)) &
            (df["dollar_vol"] >= SETTINGS.MIN_AVG_DOLLAR_VOL)]
    df = df.sort_values("dollar_vol", ascending=False).head(limit)
    return df["symbol"].tolist()

def get_aggs(ticker: str, timespan="day", window=60) -> pd.DataFrame:
    end = now_et().date()
    start = end - timedelta(days=365)
    aggs = poly.get_aggs(ticker, 1, timespan, str(start), str(end))
    if not aggs: return pd.DataFrame([])
    df = pd.DataFrame([{"t":a.timestamp, "o":a.open, "h":a.high, "l":a.low, "c":a.close, "v":a.volume} for a in aggs])
    df = df.tail(window)
    return df

def atr(df: pd.DataFrame, period=14) -> Optional[float]:
    if df.empty or len(df) < period+1: return None
    high, low, close = df["h"].values, df["l"].values, df["c"].values
    tr = np.maximum(high[1:] - low[1:], np.maximum(abs(high[1:] - close[:-1]), abs(low[1:] - close[:-1])))
    return float(pd.Series(tr).rolling(period).mean().dropna().iloc[-1])

def momentum_20d(df: pd.DataFrame) -> Optional[float]:
    if df.empty or len(df) < 20: return None
    return (df["c"].iloc[-1]/df["c"].iloc[-20] - 1.0) * 100.0

def get_fundamentals_snapshot(ticker:str) -> Dict[str,Any]:
    out = {"rev_yoy":None, "eps_yoy":None, "margin_delta":None, "mcap":None}
    try:
        # Polygon has multiple fundamentals endpoints; we use vX/reference/financials via client shortcut
        # (python client exposes as REST paths; keeping it light and robust)
        fin = poly.get_financials(ticker=ticker, limit=4)
        if not fin: return out
        rows = []
        for f in fin:
            rows.append({
                "period": getattr(f, "fiscal_period", None) or getattr(f, "period", None),
                "rev": float(getattr(getattr(f, "income_statement", None) or (), "revenues", 0) or 0),
                "eps": float(getattr(getattr(f, "income_statement", None) or (), "eps_basic", 0) or 0),
                "gm":  float(getattr(getattr(f, "income_statement", None) or (), "gross_profit", 0) or 0),
                "rev_cost": float(getattr(getattr(f,"income_statement", None) or (), "cost_of_revenue", 0) or 0)
            })
        if len(rows)>=2:
            rev_yoy = (rows[0]["rev"] - rows[1]["rev"])/abs(rows[1]["rev"]) if rows[1]["rev"] else None
            eps_yoy = (rows[0]["eps"] - rows[1]["eps"])/abs(rows[1]["eps"]) if rows[1]["eps"] else None
            gm0 = rows[0]["gm"]; rc0 = rows[0]["rev_cost"]; gm1 = rows[1]["gm"]; rc1 = rows[1]["rev_cost"]
            margin_delta = ((gm0 - rc0) - (gm1 - rc1)) if all(x is not None for x in [gm0,rc0,gm1,rc1]) else None
            out.update({"rev_yoy": rev_yoy*100 if rev_yoy is not None else None,
                        "eps_yoy": eps_yoy*100 if eps_yoy is not None else None,
                        "margin_delta": margin_delta})
        # Market cap via snapshot (approx)
        snap = poly.get_snapshot_ticker(ticker=ticker, market_type="stocks")
        out["mcap"] = getattr(getattr(snap, "ticker", None), "market_cap", None)
    except Exception as e:
        log.debug(f"fundamentals fail {ticker}: {e}")
    return out

def get_options_signals(ticker:str) -> Dict[str,Any]:
    """Compute a minimal options signal set: IV percentile, call/put OI concentration, expected move."""
    out = {"iv_pct":None, "oi_spike":None, "exp_move_pct":None, "skew_call_over_put":None}
    try:
        # nearest monthly expiry chain
        # polygon client: get_options_contracts, get_option_chain, get_snapshot_option...
        # To keep robust across client versions, call REST path via requests if needed; here we use the client’s convenience:
        chains = poly.get_option_chain(underlying_symbol=ticker, expires="next")  # pseudo helper; some clients expose similar
        if not chains: return out
        # Filter ATM strikes
        spot = getattr(getattr(poly.get_snapshot_ticker(ticker=ticker, market_type="stocks"), "ticker", None), "last_trade", None)
        spot = float(getattr(spot, "price", None) or 0)
        if spot <= 0: return out
        # Find closest strike
        strikes = sorted({float(getattr(c, "strike_price", 0) or 0) for c in chains if hasattr(c, "strike_price")})
        if not strikes: return out
        strike = min(strikes, key=lambda k: abs(k-spot))
        calls = [c for c in chains if getattr(c,"contract_type", "").upper()=="CALL" and abs(float(getattr(c,"strike_price",0))-strike)<1e-6]
        puts  = [p for p in chains if getattr(p,"contract_type","").upper()=="PUT" and abs(float(getattr(p,"strike_price",0))-strike)<1e-6]
        def mid(x): 
            a = float(getattr(x,"ask_price",0) or 0); b=float(getattr(x,"bid_price",0) or 0); 
            return (a+b)/2 if a and b else None
        call_mid = max([mid(c) for c in calls if mid(c)], default=None)
        put_mid  = max([mid(p) for p in puts if mid(p)], default=None)
        if call_mid and put_mid and spot:
            exp_move = (call_mid + put_mid) / spot
            out["exp_move_pct"] = round(exp_move*100, 2)
        # OI spikes & skew
        call_oi = sum([float(getattr(c,"open_interest",0) or 0) for c in calls])
        put_oi  = sum([float(getattr(p,"open_interest",0) or 0) for p in puts])
        out["skew_call_over_put"] = round(call_oi/(put_oi+1e-6), 2)
        # IV percentile (rough): compare current ATM IV to 1Y range from snapshots
        ivs = [float(getattr(c,"implied_volatility",0) or 0) for c in chains if getattr(c,"contract_type","").upper()=="CALL"]
        if ivs:
            cur_iv = np.nanmedian(ivs)
            # Pull historical daily IV proxies via options snapshots (simplified). If unavailable, approximate with 30d realized vol.
            hist = get_aggs(ticker, "day", 252)
            if not hist.empty:
                rv = hist["c"].pct_change().rolling(30).std().dropna()*np.sqrt(252)
                if len(rv)>20:
                    iv_pct = float((rv <= cur_iv).mean()*100)
                    out["iv_pct"] = round(iv_pct, 1)
        # OI spike heuristic
        out["oi_spike"] = True if (call_oi + put_oi) > 10_000 else False
    except Exception as e:
        log.debug(f"options signal fail {ticker}: {e}")
    return out

# =========================
# GPT Research
# =========================
def gpt_web_research_memo(ticker:str, facts:Dict[str,Any]) -> Dict[str,Any]:
    """Ask GPT-5 to browse and produce a thesis memo with target/stop/time."""
    user_prompt = f"""
You are an equity analyst for 2–12 week swing trades.
Ticker: {ticker}

[Bot facts]
Price: {facts.get('price')}
ATR(14d): {facts.get('atr')}
20d Momentum %: {facts.get('mom20')}
Fundamentals: rev_yoy={facts.get('rev_yoy')}%, eps_yoy={facts.get('eps_yoy')}%, margin_delta={facts.get('margin_delta')}, mcap={facts.get('mcap')}
Options signals: iv_pct={facts.get('iv_pct')}, exp_move%={facts.get('exp_move_pct')}, skew_call_over_put={facts.get('skew_call_over_put')}, oi_spike={facts.get('oi_spike')}

Your tasks:
1) Use web search to find the **most recent catalysts** (IR press releases, SEC/EDGAR, reputable news) and **summarize** them with links.
2) Assess Reddit/X chatter at a high level (signals/key narratives) with links if notable.
3) Provide a **swing thesis memo** with: Bull case, Bear case, Why now, Risks.
4) Output **Target Price**, **Time Window** (weeks), **Initial Stop** (structure-based), and **Confidence 0–100**.
Return a compact JSON with keys: thesis, target_price, time_weeks, stop_note, confidence, links[].
"""
    try:
        # Prefer Responses API with web_search tool
        resp = openai.responses.create(
            model=OPENAI_MODEL,
            input=user_prompt,
            tools=[{"type":"web_search"}],
        )
        content = resp.output_text  # the model should emit JSON or text with JSON
    except Exception:
        # Fallback to chat completion (no browsing)
        comp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":"Return concise JSON only."},
                      {"role":"user","content":user_prompt}],
            temperature=0.2
        )
        content = comp.choices[0].message.content

    # Safe JSON parse
    try:
        data = json.loads(content)
    except Exception:
        data = {"thesis": content[:2000], "target_price": None, "time_weeks": 6, "stop_note":"VWAP/ATR stop", "confidence": 55, "links":[]}
    return data

# =========================
# Scoring
# =========================
def score_candidate(f, opt, mom20):
    s_fund = 0
    if f.get("rev_yoy") is not None: s_fund += np.clip(f["rev_yoy"]/20, -1, 1)*50
    if f.get("eps_yoy") is not None: s_fund += np.clip(f["eps_yoy"]/20, -1, 1)*50
    # margin delta just adds small bonus if positive
    if f.get("margin_delta") and f["margin_delta"]>0: s_fund += 5
    s_fund = np.clip(s_fund, 0, 100)

    s_event = 50  # neutral baseline, GPT will tilt up/down post-memo
    s_opt = 0
    if opt.get("iv_pct") is not None:
        s_opt += (opt["iv_pct"]-50)/50*40  # >50th pct → positive attention
    if opt.get("oi_spike"): s_opt += 10
    if opt.get("skew_call_over_put") and opt["skew_call_over_put"]>1.25: s_opt += 10
    s_opt = float(np.clip(s_opt, 0, 100))

    s_attn = 50  # placeholder; GPT research may adjust decision
    s_tech = 0
    if mom20 is not None: s_tech += np.clip(mom20/20, -1, 1)*60
    s_tech = float(np.clip(s_tech, 0, 100))

    total = (SETTINGS.W_FUND*s_fund + SETTINGS.W_EVENT*s_event +
             SETTINGS.W_OPTIONS*s_opt + SETTINGS.W_ATTENTION*s_attn +
             SETTINGS.W_TECH*s_tech)*100  # weights sum to 1.0
    return float(np.clip(total, 0, 100))

# =========================
# Risk & Execution
# =========================
def position_size_from_atr(portfolio_equity: float, atr_val: float, price: float) -> int:
    if not atr_val or atr_val<=0 or price<=0: return 0
    per_name_risk = (SETTINGS.RISK_PER_TRADE_BP/10000.0) * portfolio_equity
    shares = int(per_name_risk / atr_val)
    # cap by max position %
    max_shares_by_limit = int((SETTINGS.MAX_POSITION_PCT*portfolio_equity)/price)
    return max(0, min(shares, max_shares_by_limit))

def compute_initial_stop(entry: float, avwap: Optional[float], atr_val: float) -> float:
    base = entry - SETTINGS.INIT_STOP_ATR_MULT*atr_val
    if avwap: base = min(base, avwap - 0.25*atr_val)
    return round(max(0.01, base), 2)

def compute_trailing_stop(highest_close: float, atr_val: float, profit_ratio: float) -> float:
    mult = SETTINGS.TRAIL_ATR_MULT_AFTER_15 if profit_ratio>=0.15 else SETTINGS.TRAIL_ATR_MULT_BEFORE_15
    return round(highest_close - mult*atr_val, 2)

def account_equity() -> float:
    acct = alpaca.get_account()
    return float(acct.equity)

def place_entry_order(symbol:str, qty:int, limit_price:float):
    od = LimitOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY,
                           limit_price=limit_price, time_in_force=TimeInForce.DAY, extended_hours=False)
    return alpaca.submit_order(order_data=od)

def place_stop_order(symbol:str, qty:int, stop_price:float):
    od = StopOrderRequest(symbol=symbol, qty=qty, side=OrderSide.SELL,
                          stop_price=stop_price, time_in_force=TimeInForce.GTC)
    return alpaca.submit_order(order_data=od)

def close_position_percent(symbol:str, pct:float=1.0):
    req = ClosePositionRequest(symbol_or_asset_id=symbol, percentage=str(pct))
    return alpaca.close_position(close=req)

# =========================
# Research + Selection + Entry
# =========================
async def daily_research_and_selection():
    """Run after close: form watchlist, ask GPT to write memos, stash targets, schedule entries."""
    global WATCHLIST, CANDIDATES
    try:
        send_telegram("🧠 Running daily research…")
        universe = list_active_snapshots()
        WATCHLIST = universe[:SETTINGS.MAX_UNIVERSE]

        picked: Dict[str, Dict[str,Any]] = {}
        for sym in WATCHLIST[:60]:  # keep first pass tight for speed
            df = get_aggs(sym, "day", 90)
            if df.empty: continue
            mom20 = momentum_20d(df)
            if mom20 is None or mom20 < -10:  # avoid near-term downtrends
                continue

            a = atr(df, 14)
            price = float(df["c"].iloc[-1])

            f = get_fundamentals_snapshot(sym)
            if f.get("mcap") and not (SETTINGS.MIN_MARKET_CAP <= f["mcap"] <= SETTINGS.MAX_MARKET_CAP):
                continue

            opt = get_options_signals(sym)
            base_score = score_candidate(f, opt, mom20)
            if base_score < SETTINGS.MIN_SCORE:
                continue

            facts = {"price":price, "atr":a, "mom20":mom20, **f, **opt}
            memo = gpt_web_research_memo(sym, facts)
            # Light post-filter: require a target higher than price
            tpx = memo.get("target_price")
            target_ok = False
            try:
                if tpx: target_ok = float(tpx) > price*1.05
            except Exception: pass
            if not target_ok: 
                continue

            picked[sym] = {
                "price":price, "atr":a, "facts":facts, "memo":memo,
                "target": float(memo.get("target_price") or 0),
                "time_weeks": int(memo.get("time_weeks") or 8),
                "confidence": int(memo.get("confidence") or 55)
            }

        CANDIDATES = picked
        if not CANDIDATES:
            send_telegram("No swing candidates passed filters today.")
        else:
            lines = [f"*{k}* → tgt ${v['target']:.2f} in ~{v['time_weeks']}w (conf {v['confidence']}), ATR {v['facts']['atr']:.2f}" for k,v in CANDIDATES.items()]
            send_telegram("📋 *Candidates:*\n" + "\n".join(lines[:15]))
    except Exception as e:
        log.error(f"daily_research_and_selection failed: {e}", exc_info=True)
        send_telegram(f"Research error: {e}")

async def entry_window_task():
    """Enter top candidates near the open with sizing and initial stops."""
    await asyncio.sleep(1)
    while True:
        n = now_et()
        if n.weekday() >= 5:  # weekend
            await asyncio.sleep(300); continue

        if is_between(n, SETTINGS.ENTRY_WINDOW_ET, SETTINGS.ENTRY_WINDOW_ET):
            if PAUSED or not CANDIDATES:
                await asyncio.sleep(60); continue
            # choose top 3 by confidence then target/atr reward
            ranked = sorted(CANDIDATES.items(), key=lambda kv:(kv[1]["confidence"], kv[1]["target"]/max(1e-6, kv[1]["facts"]["atr"])), reverse=True)[:3]
            eq = account_equity()
            for sym, data in ranked:
                try:
                    qty = position_size_from_atr(eq, data["facts"]["atr"], data["price"])
                    if qty <= 0: 
                        continue
                    entry = round(data["price"], 2)
                    order = place_entry_order(sym, qty, entry)
                    send_telegram(f"➡️ Entry submitted: BUY {qty} {sym} @ ${entry}")
                    # place stop after fill check (simplified: place immediately)
                    stop_price = compute_initial_stop(entry, None, data["facts"]["atr"])
                    st = place_stop_order(sym, qty, stop_price)
                    PORTFOLIO[sym] = {
                        "qty": qty, "avg_price": entry, "stop_id": st.id if hasattr(st,'id') else None,
                        "target": data["target"], "target_date": (now_et()+timedelta(weeks=data["time_weeks"])).date(),
                        "opened_at": now_et(), "thesis": data["memo"], "atr": data["facts"]["atr"], "highest_close": entry
                    }
                except Exception as e:
                    log.error(f"entry for {sym} failed: {e}")
                    send_telegram(f"Entry failed {sym}: {e}")
            await asyncio.sleep(120)
        else:
            await asyncio.sleep(20)

async def monitor_positions_task():
    """End-of-day trailing stop update and periodic status."""
    while True:
        if not PORTFOLIO:
            await asyncio.sleep(60); continue
        try:
            for sym, pos in list(PORTFOLIO.items()):
                # refresh last close
                df = get_aggs(sym, "day", 10)
                if df.empty: continue
                last_close = float(df["c"].iloc[-1])
                pos["highest_close"] = max(pos.get("highest_close", last_close), last_close)
                profit_ratio = (last_close - pos["avg_price"])/pos["avg_price"]
                trail = compute_trailing_stop(pos["highest_close"], pos["atr"], profit_ratio)
                # If price < trail → close
                if last_close <= trail:
                    try:
                        close_position_percent(sym, 1.0)
                        send_telegram(f"🛡️ Trailing stop hit: {sym} @ ~${last_close:.2f}")
                        PORTFOLIO.pop(sym, None)
                    except Exception as e:
                        log.error(f"trail close {sym} failed: {e}")
                # time exit
                if now_et().date() >= pos["target_date"]:
                    try:
                        close_position_percent(sym, 1.0)
                        send_telegram(f"⏱ Time exit reached: {sym}")
                        PORTFOLIO.pop(sym, None)
                    except Exception as e:
                        log.error(f"time exit {sym} failed: {e}")
        except Exception as e:
            log.error(f"monitor error: {e}")
        await asyncio.sleep(300)

# =========================
# Telegram commands
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Swing Investor Bot online. Use /status, /portfolio, /research TICKER, /rebalance, /pause, /resume, /close TICKER")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    acct = alpaca.get_account()
    text = (f"*Status*\n"
            f"Equity: ${float(acct.equity):,.2f}\n"
            f"Cash:   ${float(acct.cash):,.2f}\n"
            f"Paused: {'Yes' if PAUSED else 'No'}\n"
            f"Candidates: {len(CANDIDATES)} | Watchlist: {len(WATCHLIST)} | Positions: {len(PORTFOLIO)}")
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not PORTFOLIO:
        await update.message.reply_text("No open swing positions.")
        return
    lines=[]
    for s,p in PORTFOLIO.items():
        lines.append(f"*{s}* qty {p['qty']} @ ${p['avg_price']:.2f} → tgt ${p['target']:.2f} by {p['target_date']}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_research(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sym = context.args[0].upper()
    except Exception:
        await update.message.reply_text("Usage: /research TICKER")
        return
    df = get_aggs(sym, "day", 90)
    if df.empty:
        await update.message.reply_text("No data.")
        return
    a = atr(df, 14); price = float(df['c'].iloc[-1]); mom = momentum_20d(df)
    f = get_fundamentals_snapshot(sym); o = get_options_signals(sym)
    facts={"price":price,"atr":a,"mom20":mom, **f, **o}
    memo = gpt_web_research_memo(sym, facts)
    text = f"*{sym} research*\nTarget: {memo.get('target_price')} | {memo.get('time_weeks')}w | Conf {memo.get('confidence')}\n{memo.get('thesis','')[:1500]}"
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sym = context.args[0].upper()
        if sym not in PORTFOLIO:
            await update.message.reply_text("Not in portfolio.")
            return
        close_position_percent(sym, 1.0)
        PORTFOLIO.pop(sym, None)
        await update.message.reply_text(f"Closed {sym}.")
    except Exception as e:
        await update.message.reply_text(f"Close error: {e}")

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global PAUSED; PAUSED=True
    await update.message.reply_text("Trading paused.")

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global PAUSED; PAUSED=False
    await update.message.reply_text("Trading resumed.")

async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await daily_research_and_selection()
    await update.message.reply_text("Rebalanced candidates from scratch.")

# =========================
# Schedulers
# =========================
async def schedule_daily_research():
    """Run at the configured ET time once per day."""
    while True:
        n = now_et()
        hh, mm = SETTINGS.DAILY_RESEARCH_ET
        run_at = n.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if n > run_at:
            run_at = run_at + timedelta(days=1)
        wait = (run_at - n).total_seconds()
        await asyncio.sleep(wait)
        await daily_research_and_selection()

# =========================
# Main
# =========================
async def main():
    send_telegram("🤖 Swing Investor Bot starting…")

    # Bring in any open positions to state (optional; simple sync)
    try:
        open_positions = alpaca.get_all_positions()
        for p in open_positions:
            if p.side != "long":
                continue
            PORTFOLIO[p.symbol] = {
                "qty": int(float(p.qty)),
                "avg_price": float(p.avg_entry_price),
                "stop_id": None,
                "target": float(p.avg_entry_price)*1.15,  # placeholder
                "target_date": (now_et()+timedelta(weeks=8)).date(),
                "opened_at": now_et(),
                "thesis": {"thesis":"synced"},
                "atr": 1.0,
                "highest_close": float(p.avg_entry_price)
            }
    except Exception as e:
        log.warning(f"sync positions skipped: {e}")

    # Telegram bot
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("research", cmd_research))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("rebalance", cmd_rebalance))
    await app.initialize(); await app.start()
    asyncio.create_task(app.updater.start_polling())

    # Background loops
    tasks = [
        schedule_daily_research(),
        entry_window_task(),
        monitor_positions_task(),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        send_telegram("⏹️ Bot stopped.")
