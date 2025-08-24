# ==============================================================================
# --- AUTONOMOUS TRADING ALGORITHM (Short_King.py v4.5) ---
#
# v4.5 UPDATE:
# - SHORTABLE-ONLY: Vetting logic now only requires the `shortable` flag,
#   removing the `easy_to_borrow` check to allow trading HTB stocks.
# - SPREAD FILTER ACTIVATED: The MAX_SPREAD_PERCENT setting is now actively
#   used in the vetting process to discard stocks with wide bid-ask spreads.
#
# v4.4 PATCH:
# - TERMINOLOGY: Changed logging references from "ETB" (Easy-to-Borrow) to
#   the more general "shortable" for clarity in vetting drop reasons.
# ==============================================================================

import asyncio
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, time as dtime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from enum import Enum
import json
import time
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
from functools import partial
from types import SimpleNamespace

# --- Vendor Client Imports ---
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, StopOrderRequest, ClosePositionRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.trading.models import TradeUpdate, Position, Order
from alpaca.trading.stream import TradingStream
from alpaca.common.exceptions import APIError
from polygon import RESTClient
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- AI & Data Science Imports ---
import numpy as np
from openai import OpenAI
import requests

# ==============================================================================
# --- STEP 1: CONFIGURATION & SETTINGS ---
# ==============================================================================

@dataclass
class Settings:
    """Centralized configuration for all trading parameters and filters."""
    SIMULATION_MODE: bool = False
    SIMULATED_ET_HOUR: int = 8

    MIN_PRICE: float = 0.1
    MAX_PRICE: float = 1000.0
    MIN_AVG_DOLLAR_VOLUME: float = 100_000
    MIN_PM_VOLUME: int = 10000
    MIN_NOTIONAL_VALUE: float = 5_000.0
    MAX_SHARES_OUTSTANDING: int = 100_000_000
    MIN_ATR_PERCENT: float = 3.0
    MAX_SPREAD_PERCENT: float = 1.5
    MIN_GAP_PERCENT: float = 30.0

    TRADE_NOTIONAL_VALUE: float = 1000.0
    MAX_POSITIONS: int = int(os.getenv("MAX_POSITIONS", 20))
    STOP_LOSS_PERCENT: float = 10.0

    CONCURRENT_TASKS: int = 5
    MAX_QUEUE_SIZE: int = 2000
    TELEGRAM_HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL", 900))
    GAPPER_CONSISTENCY_CHECK_SECONDS: int = 200

SETTINGS = Settings()

# --- Environment & Logging Setup ---
load_dotenv()
APCA_API_KEY_ID = os.getenv("ALPACA_SCALPINGSNIPER_KEY")
APCA_API_SECRET_KEY = os.getenv("ALPACA_SCALPINGSNIPER_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("Slcaper_Stock_TELEGRAM_BOT_TOKEN")
ALPACA_IS_PAPER = os.getenv("ALPACA_PAPER_TRADING", "true").lower() == "true"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_USER_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano")

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
log_file = 'Short_King.log'
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Global State, Clients, and Caches ---
trading_client = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=ALPACA_IS_PAPER)
trading_stream = TradingStream(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=ALPACA_IS_PAPER)
polygon_rest_client = RESTClient(POLYGON_API_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

POTENTIAL_GAPPERS: Dict[str, Any] = {}
PENDING_ORDERS: Dict[str, Dict[str, Any]] = {}
OPEN_POSITIONS: Dict[str, Dict[str, Any]] = {}
TRADED_SYMBOLS_TODAY: set[str] = set()
ANALYZED_SYMBOLS_TODAY: set[str] = set()
REALIZED_PNL_TODAY: float = 0.0
TRADE_SIGNAL_QUEUE = asyncio.Queue()
API_SEMAPHORE = asyncio.Semaphore(SETTINGS.CONCURRENT_TASKS)
trading_enabled: bool = True
pyramiding_enabled: bool = True

SECONDARY_WATCHLIST: Dict[str, Dict[str, any]] = {}
second_wave_triggered_today: bool = False

# ==============================================================================
# --- UTILITY & API WRAPPER FUNCTIONS ---
# ==============================================================================
ET = ZoneInfo("America/New_York")
_POLY_MARKET = "stocks"

def check_environment_variables():
    """Checks for all required environment variables at startup."""
    required_vars = {
        "Alpaca Key ID": APCA_API_KEY_ID,
        "Alpaca Secret Key": APCA_API_SECRET_KEY,
        "Polygon API Key": POLYGON_API_KEY,
        "Telegram Bot Token": TELEGRAM_BOT_TOKEN,
        "Telegram Chat ID": TELEGRAM_CHAT_ID,
        "OpenAI API Key": OPENAI_API_KEY,
    }
    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        logging.critical(f"FATAL: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    if not openai_client: logging.warning("OpenAI client not configured. AI analysis will be disabled.")

    logging.info("All required environment variables are present.")

def get_current_et_time() -> dtime:
    if SETTINGS.SIMULATION_MODE:
        return dtime(hour=SETTINGS.SIMULATED_ET_HOUR)
    return datetime.now(ET).time()

def is_trading_day() -> bool:
    """Returns True if the current day is a weekday (Monday-Friday)."""
    return datetime.now(ET).weekday() < 5

def is_premarket() -> bool:
    now_et = get_current_et_time()
    return dtime(4, 0) <= now_et < dtime(9, 30)

def is_rth() -> bool:
    now_et = get_current_et_time()
    return dtime(9, 30) <= now_et < dtime(16, 0)

def calc_position_size(price: float) -> int:
    if price <= 0: return 0
    return int(SETTINGS.TRADE_NOTIONAL_VALUE / price)

def send_telegram_alert(message: str):
    """Sends a Telegram alert and logs the attempt."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram Token or Chat ID not set. Cannot send alert.")
        return

    logging.info(f"Attempting to send Telegram alert: '{message[:50]}...'")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info("Telegram alert sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Telegram API request failed: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while sending Telegram alert: {e}")

def _mk_snapshot_from_gapper(gapper):
    """Convert any Polygon row or cached object into a standard SimpleNamespace."""
    if isinstance(gapper, SimpleNamespace) and hasattr(gapper, "pct"):
        return gapper
    if isinstance(gapper, dict):
        day = gapper.get("day", {})
        return SimpleNamespace(
            ticker=gapper.get("ticker") or gapper.get("T"),
            pct=gapper.get("todaysChangePerc") or gapper.get("todays_change_percent") or gapper.get("P"),
            volume=gapper.get("day", {}).get("v") if gapper.get("day") else gapper.get("volume"),
            last=gapper.get("day", {}).get("c") if gapper.get("day") else gapper.get("last"),
        )
    return SimpleNamespace(
        ticker=getattr(gapper, "ticker", None),
        pct=getattr(gapper, "todays_change_percent", None) or getattr(gapper, "pct", None),
        volume=getattr(getattr(gapper, "day", None) or (), "v", None),
        last=getattr(getattr(gapper, "day", None) or (), "c", None),
    )

async def get_last_quote_for_symbol(symbol: str):
    """Asynchronous wrapper for fetching a quote."""
    client = RESTClient(POLYGON_API_KEY)
    try:
        func = partial(client.get_last_quote, ticker=symbol)
        return await asyncio.to_thread(func)
    finally:
        if hasattr(client, "close"):
            client.close()

async def get_snapshot_for_symbol(symbol: str):
    """Asynchronous wrapper for fetching a full snapshot."""
    client = RESTClient(POLYGON_API_KEY)
    try:
        func = partial(client.get_snapshot_ticker, ticker=symbol, market_type=_POLY_MARKET)
        return await asyncio.to_thread(func)
    finally:
        if hasattr(client, "close"):
            client.close()

# --- Polygon pagination helper: get ALL snapshot tickers ---
def fetch_all_snapshot_tickers(limit: int = 1000) -> list[dict]:
    """Fetch US stock snapshots from Polygon with pagination (cursor)."""
    import requests
    base = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"limit": limit, "apiKey": POLYGON_API_KEY}
    out = []
    url = base
    while True:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        out.extend(data.get("tickers", []))
        next_url = data.get("next_url")
        if not next_url:
            break
        url = next_url
        params = {"apiKey": POLYGON_API_KEY}
    return out

async def liquidate_position(symbol: str, reason: str):
    """Closes a specific position with a market order."""
    logging.warning(f"LIQUIDATING {symbol} due to: {reason}")
    try:
        if symbol in OPEN_POSITIONS:
            stop_id = OPEN_POSITIONS[symbol].get("stop_id")
            if stop_id:
                try:
                    await asyncio.to_thread(trading_client.cancel_order_by_id, order_id=stop_id)
                except APIError as e:
                    logging.error(f"Could not cancel stop order for {symbol}: {e}")

            close_request = ClosePositionRequest(symbol_or_asset_id=symbol, percentage="1.0")
            await asyncio.to_thread(trading_client.close_position, close_request)
            send_telegram_alert(f"🚨 *LIQUIDATED:* Position ${symbol} closed. Reason: {reason}.")
            OPEN_POSITIONS.pop(symbol, None)
    except APIError as e:
        logging.error(f"Failed to liquidate {symbol}: {e}")

async def liquidate_all_positions():
    """Closes all open positions as a safety measure."""
    logging.warning("Initiating liquidation of all open positions.")
    try:
        await asyncio.to_thread(trading_client.close_all_positions, cancel_orders=True)
        send_telegram_alert("🚨 *LIQUIDATION:* All positions have been covered.")
        OPEN_POSITIONS.clear()
    except Exception as e:
        logging.error(f"Failed during liquidation: {e}")

# ==============================================================================
# --- ADVANCED INSIGHTS & AI ANALYSIS FUNCTIONS ---
# ==============================================================================
async def get_52_week_high_low(symbol: str) -> (Optional[float], Optional[float]):
    """Fetches the 52-week high and low for a symbol from Polygon."""
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)

        aggs = await asyncio.to_thread(
            polygon_rest_client.get_aggs,
            symbol, 1, 'day', start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        )
        if not aggs: return "N/A", "N/A"

        high_52_week = max(agg.high for agg in aggs)
        low_52_week = min(agg.low for agg in aggs)
        return high_52_week, low_52_week
    except Exception as e:
        logging.error(f"Could not fetch 52-week data for {symbol}: {e}")
        return "N/A", "N/A"

async def get_rsi(symbol: str, period: int = 14, timeframe: str = 'minute', limit: int = 100) -> Optional[float]:
    """Fetches price data and calculates the RSI."""
    try:
        end_date = datetime.now(ET).date()
        start_date = end_date - timedelta(days=5)

        aggs = await asyncio.to_thread(
            polygon_rest_client.get_aggs,
            symbol, 1, timeframe, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), limit=limit
        )
        if len(aggs) < period + 1: return None

        closes = np.array([agg.close for agg in aggs])
        deltas = np.diff(closes)
        seed = deltas[:period]

        up = seed[seed >= 0].sum()/period
        down = -seed[seed < 0].sum()/period

        if down == 0: return 100.0
        rs = up / down
        rsi = 100.0 - (100.0 / (1.0 + rs))

        return rsi
    except Exception as e:
        logging.error(f"Could not calculate RSI for {symbol}: {e}")
        return None

async def get_vwap(symbol: str) -> Optional[float]:
    """Fetches the current day's VWAP from a Polygon snapshot."""
    try:
        snapshot = await get_snapshot_for_symbol(symbol)
        if snapshot and hasattr(snapshot, 'day') and hasattr(snapshot.day, 'vwap') and snapshot.day.vwap:
            return snapshot.day.vwap
        return None
    except Exception as e:
        logging.error(f"Could not fetch VWAP for {symbol}: {e}")
        return None

async def get_market_context() -> str:
    """Fetches data for SPY and QQQ to determine overall market sentiment."""
    try:
        spy_snapshot, qqq_snapshot = await asyncio.gather(
            get_snapshot_for_symbol("SPY"),
            get_snapshot_for_symbol("QQQ")
        )

        if not hasattr(spy_snapshot, 'ticker') or not hasattr(qqq_snapshot, 'ticker'):
            logging.error("Could not get valid snapshot for SPY or QQQ.")
            return "Unknown"

        spy_change = spy_snapshot.ticker.todays_change_perc
        qqq_change = qqq_snapshot.ticker.todays_change_perc

        avg_change = (spy_change + qqq_change) / 2
        if avg_change > 0.75: return f"Strongly Bullish (SPY {spy_change:.2f}%, QQQ {qqq_change:.2f}%)"
        if avg_change > 0.2: return f"Bullish (SPY {spy_change:.2f}%, QQQ {qqq_change:.2f}%)"
        if avg_change < -0.75: return f"Strongly Bearish (SPY {spy_change:.2f}%, QQQ {qqq_change:.2f}%)"
        if avg_change < -0.2: return f"Bearish (SPY {spy_change:.2f}%, QQQ {qqq_change:.2f}%)"
        return f"Mixed/Flat (SPY {spy_change:.2f}%, QQQ {qqq_change:.2f}%)"
    except Exception as e:
        logging.error(f"Could not fetch market context: {e}")
        return "Unknown"

async def generate_openai_analysis(symbol: str, initial_gap_percent: Optional[float]):
    """Gathers data, prompts OpenAI to perform research, and sends the analysis."""
    if not openai_client: return

    logging.info(f"Generating full quantitative AI analysis for {symbol}...")
    try:
        results = await asyncio.gather(
            get_52_week_high_low(symbol),
            get_rsi(symbol),
            get_vwap(symbol),
            get_market_context(),
            get_snapshot_for_symbol(symbol),
            return_exceptions=True
        )

        high_52, low_52 = results[0] if not isinstance(results[0], Exception) else ("N/A", "N/A")
        rsi = results[1] if not isinstance(results[1], Exception) else None
        vwap = results[2] if not isinstance(results[2], Exception) else None
        market_context = results[3] if not isinstance(results[3], Exception) else "Unknown"
        current_snapshot = results[4] if not isinstance(results[4], Exception) else None

        current_price = "N/A"
        if current_snapshot and hasattr(current_snapshot, 'ticker') and hasattr(current_snapshot.ticker, 'last_trade') and current_snapshot.ticker.last_trade:
            current_price = current_snapshot.ticker.last_trade.price

        rsi_text = f"{rsi:.1f} ('Overbought')" if isinstance(rsi, float) and rsi > 70 else (f"{rsi:.1f}" if isinstance(rsi, float) else "N/A")
        vwap_text = f"${vwap:.2f}" if isinstance(vwap, float) else "N/A"
        price_vs_vwap = "N/A"
        if isinstance(current_price, float) and isinstance(vwap, float):
            price_vs_vwap = 'above' if current_price > vwap else 'below'

        prompt = (
            f"**[Data Provided by Bot]**\n"
            f"- Ticker: ${symbol}\n"
            f"- Current Price: ${current_price}\n"
            f"- Pre-Market Gap: +{initial_gap_percent:.1f}%\n"
            f"- 52-Week Range: ${low_52} - ${high_52}\n"
            f"- RSI (1-minute): {rsi_text}\n"
            f"- Price vs. VWAP: Trading {price_vs_vwap} VWAP ({vwap_text})\n"
            f"- Broader Market Context: {market_context}\n\n"
            f"**[Your Task as a Quantitative Analyst]**\n"
            f"1.  **Web Research:** Search the web for:\n"
            f"    a) The latest significant news/catalysts for ${symbol} today.\n"
            f"    b) Recent social sentiment from Reddit and Twitter/X.\n"
            f"    c) The most recently reported Short Interest % of float.\n"
            f"2.  **Synthesize Analysis:** Based on BOTH the provided data and your web research, provide the following:\n"
            f"    a) **Bull Case:** What are the primary arguments for the stock going even higher?\n"
            f"    b) **Bear Case (Short Thesis):** What are the primary arguments for shorting this stock?\n"
            f"    c) **Actionable Signal:** What specific technical event (e.g., a break below VWAP) would confirm a high-probability short entry?\n"
            f"    d) **Risk Analysis & Profit Target:** What is the main risk to this short trade (e.g., short squeeze)? Suggest a theoretical profit target based on a historical support level or other indicator.\n\n"
            f"Format the response clearly and concisely."
        )

        completion = await asyncio.to_thread(
            openai_client.chat.completions.create, model=OPENAI_MODEL, messages=[{"role": "user", "content": prompt}]
        )
        analysis_text = completion.choices[0].message.content

        full_message = f"🤖 *Quantitative AI Analysis for ${symbol}*\n\n{analysis_text}"
        send_telegram_alert(full_message)

    except Exception as e:
        logging.error(f"Failed to generate OpenAI analysis for {symbol}: {e}", exc_info=True)
        send_telegram_alert(f"🤖 Failed to generate full AI analysis for ${symbol}.")

# ==============================================================================
# --- TELEGRAM BOT COMMANDS ---
# ==============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = f"*Short_King.py v4.5* is running."
    await update.message.reply_text(text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    unrealized_pl = sum(float(p['position_obj'].unrealized_pl) for p in OPEN_POSITIONS.values() if 'position_obj' in p)
    text = (f"*Open Positions:* {len(OPEN_POSITIONS)} / {SETTINGS.MAX_POSITIONS}\n"
            f"*Watchlist Size:* {len(POTENTIAL_GAPPERS) // 2}\n"
            f"*2nd Wave Watchlist:* {len(SECONDARY_WATCHLIST)}\n"
            f"*Trading Enabled:* {'✅' if trading_enabled else '⏸️'}\n"
            f"*Pyramiding Enabled:* {'✅' if pyramiding_enabled else '❌'}\n"
            f"*Unrealized P/L:* `${unrealized_pl:+.2f}`\n"
            f"*Realized P/L Today:* `${REALIZED_PNL_TODAY:+.2f}`")
    await update.message.reply_text(text, parse_mode='Markdown')

async def positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not OPEN_POSITIONS:
        await update.message.reply_text("No open short positions.")
        return
    message = "*--- Open Short Positions ---*\n"
    for symbol, data in OPEN_POSITIONS.items():
        pnl = float(data['position_obj'].unrealized_pl)
        initial_gap_str = f"{data.get('initial_gap_percent', 'N/A'):.1f}%" if data.get('initial_gap_percent') else "N/A"
        message += (f"`${symbol}` | State: *{data['state'].upper()}*\n"
                    f"   Qty: {data['position_obj'].qty} @ ${float(data['position_obj'].avg_entry_price):.2f}\n"
                    f"   Initial Gap: {initial_gap_str} | P/L: `${pnl:+.2f}`\n")
    await update.message.reply_text(message, parse_mode='Markdown')

async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    unrealized_pl = sum(float(p['position_obj'].unrealized_pl) for p in OPEN_POSITIONS.values() if 'position_obj' in p)
    message = (f"*--- P&L Summary ---*\n"
               f"💰 *Realized P/L (Today):* `${REALIZED_PNL_TODAY:+.2f}`\n"
               f"📈 *Unrealized P/L (Open):* `${unrealized_pl:+.2f}`")
    await update.message.reply_text(message, parse_mode='Markdown')

async def refresh_insights_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not OPEN_POSITIONS:
        await update.message.reply_text("No open positions to refresh.")
        return

    await update.message.reply_text(f"🔥 Refreshing insights for {len(OPEN_POSITIONS)} open position(s)...")
    for symbol, data in OPEN_POSITIONS.items():
        initial_gap = data.get('initial_gap_percent')
        if not initial_gap:
            try:
                snapshot = await get_snapshot_for_symbol(symbol)
                if snapshot and hasattr(snapshot, 'ticker'):
                    initial_gap = snapshot.ticker.todays_change_perc
                else:
                    logging.warning(f"Invalid snapshot data received for {symbol} during refresh.")
                    initial_gap = 0.0
            except Exception as e:
                logging.error(f"Could not fetch snapshot for {symbol} during refresh: {e}")
                initial_gap = 0.0
        asyncio.create_task(generate_openai_analysis(symbol, initial_gap))

async def liquidate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        symbol = context.args[0].upper()
        if symbol in OPEN_POSITIONS:
            await update.message.reply_text(f"🚨 Liquidating position in ${symbol} now...")
            await liquidate_position(symbol, "Manual liquidation via Telegram command.")
        else:
            await update.message.reply_text(f"No open position found for ${symbol}.")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /liquidate [SYMBOL]")

async def close_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not OPEN_POSITIONS:
        await update.message.reply_text("No open positions to close.")
        return
    await update.message.reply_text(f"🚨🚨 Liquidating ALL {len(OPEN_POSITIONS)} open position(s) now...")
    await liquidate_all_positions()

async def pause_trading_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global trading_enabled
    trading_enabled = False
    await update.message.reply_text("⏸️ Trading has been PAUSED. The bot will not open new positions.")

async def resume_trading_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global trading_enabled
    trading_enabled = True
    await update.message.reply_text("▶️ Trading has been RESUMED. The bot can now open new positions.")

async def show_watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = "*--- Primary Watchlist ---*\n"
    if POTENTIAL_GAPPERS:
        symbols = [s for s in POTENTIAL_GAPPERS.keys() if not s.endswith('_data')]
        message += f"`{', '.join(symbols)}`\n\n"
    else:
        message += "Empty.\n\n"

    message += "*--- Second Wave Watchlist ---*\n"
    if SECONDARY_WATCHLIST:
        symbols = [f"{s} ({d['initial_gap']:.1f}%)" for s, d in SECONDARY_WATCHLIST.items()]
        message += f"`{', '.join(symbols)}`"
    else:
        message += "Empty."
    await update.message.reply_text(message, parse_mode='Markdown')

async def set_max_positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_max = int(context.args[0])
        if new_max >= 0:
            SETTINGS.MAX_POSITIONS = new_max
            await update.message.reply_text(f"⚙️ Max positions set to {new_max}.")
        else:
            await update.message.reply_text("Please provide a non-negative number.")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /set_max_positions [number]")

async def toggle_pyramiding_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global pyramiding_enabled
    pyramiding_enabled = not pyramiding_enabled
    status = "ENABLED" if pyramiding_enabled else "DISABLED"
    await update.message.reply_text(f"⚙️ Pyramiding is now {status}.")

async def market_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_premarket():
        status = "PRE-MARKET"
    elif is_rth():
        status = "OPEN"
    else:
        status = "CLOSED"
    await update.message.reply_text(f"📈 Market Status: *{status}*", parse_mode='Markdown')

# ==============================================================================
# --- CORE LOGIC TASKS ---
# ==============================================================================
async def premarket_gapper_scanner_task():
    """Scan Polygon every 5 min for %-gappers and populate POTENTIAL_GAPPERS."""
    while True:
        if not is_trading_day():
            await asyncio.sleep(3600)
            continue
        if not is_premarket():
            await asyncio.sleep(60)
            continue
        try:
            logging.info("Scanning for new potential short targets…")
            raw = await asyncio.to_thread(fetch_all_snapshot_tickers)
            total_fetched = len(raw)

            snaps = [_mk_snapshot_from_gapper(r) for r in raw]
            gainers = sorted([s for s in snaps if s.pct is not None], key=lambda s: s.pct, reverse=True)[:2000]

            reasons = defaultdict(int)
            filtered = []
            for snap in gainers:
                if snap.pct is None or snap.pct < SETTINGS.MIN_GAP_PERCENT:
                    reasons["gap_below_threshold"] += 1
                    continue
                if getattr(snap, "volume", 0) and snap.volume < SETTINGS.MIN_PM_VOLUME:
                    reasons["low_volume"] += 1
                    continue
                filtered.append(snap)

            logging.info(f"Fetched ~{total_fetched} snapshots -> gainers={len(gainers)} kept={len(filtered)} "
                         f"dropped={len(gainers)-len(filtered)} | reasons={dict(reasons)}")

            for snap in filtered:
                sym = snap.ticker
                if (
                    sym not in POTENTIAL_GAPPERS
                    and sym not in TRADED_SYMBOLS_TODAY
                ):
                    logging.info(f"Gapper {sym} at {snap.pct:.1f}% → adding to watchlist.")
                    POTENTIAL_GAPPERS[sym] = datetime.now(timezone.utc)
                    POTENTIAL_GAPPERS[sym + "_data"] = snap

                    price_str = f"${snap.last:.2f}" if snap.last is not None else "N/A"
                    send_telegram_alert(f"👀 *New Short Candidate:* `${sym}`\n"
                                        f"Gap: *{snap.pct:.1f}%* | Price: {price_str}")

        except Exception as e:
            logging.error(f"Scanner error: {e}", exc_info=True)
        await asyncio.sleep(300)

async def consistency_and_vetting_task():
    """Check cached gappers for persistence and enqueue vetted symbols."""
    SLEEP = 60
    while True:
        if not is_trading_day():
            await asyncio.sleep(3600)
            continue
        if not POTENTIAL_GAPPERS:
            await asyncio.sleep(SLEEP)
            continue
        try:
            logging.info(f"Running consistency check on {len(POTENTIAL_GAPPERS)} watchlist items…")
            now_utc = datetime.now(timezone.utc)
            remove: list[str] = []
            for symbol, first_seen in list(POTENTIAL_GAPPERS.items()):
                if symbol.endswith("_data"): continue
                if symbol in TRADED_SYMBOLS_TODAY:
                    remove.append(symbol)
                    continue

                snap = POTENTIAL_GAPPERS.get(f"{symbol}_data")
                if not snap:
                    logging.info(f"{symbol}: no snapshot data → drop")
                    remove.append(symbol)
                    continue

                pct = float(snap.pct or 0)
                if pct < SETTINGS.MIN_GAP_PERCENT:
                    logging.info(f"{symbol} gap faded to {pct:.1f}% → drop")
                    remove.append(symbol)
                    continue

                await TRADE_SIGNAL_QUEUE.put({"symbol": symbol, "reason": "Gapper Consistency", "initial_gap_percent": pct})
                logging.info(f"{symbol} passed consistency → VETTING_QUEUE")
                remove.append(symbol)

            for sym in remove:
                POTENTIAL_GAPPERS.pop(sym, None)
                POTENTIAL_GAPPERS.pop(f"{sym}_data", None)
        except Exception as exc:
            logging.error(f"Consistency checker crashed: {exc}", exc_info=True)
        await asyncio.sleep(SLEEP)

async def candidate_vetting_and_execution_task():
    """Vets and executes trades from the signal queue (Primary Wave)."""
    while True:
        event = await TRADE_SIGNAL_QUEUE.get()
        symbol = event['symbol']
        initial_gap_percent = event.get('initial_gap_percent')

        if symbol in TRADED_SYMBOLS_TODAY:
            TRADE_SIGNAL_QUEUE.task_done()
            continue

        async with API_SEMAPHORE:
            logging.info(f"--- VETTING (1st Wave) {symbol} FOR SHORT ---")
            drop = defaultdict(int)
            try:
                TRADED_SYMBOLS_TODAY.add(symbol)
                snap_data = POTENTIAL_GAPPERS.get(f"{symbol}_data")

                asset = await asyncio.to_thread(trading_client.get_asset, symbol)
                if not asset.tradable:
                    drop["not_tradable"] += 1; continue

                if not asset.shortable:
                    drop["not_shortable"] += 1
                    price_to_store = snap_data.last if snap_data and snap_data.last is not None else 0.0
                    SECONDARY_WATCHLIST[symbol] = {'price': price_to_store, 'initial_gap': initial_gap_percent}
                    send_telegram_alert(f"➡️ `${symbol}` moved to 2nd Wave Watchlist (Gap: {initial_gap_percent:.1f}%)")
                    continue

                if hasattr(asset, 'shares_outstanding') and asset.shares_outstanding and \
                   asset.shares_outstanding > SETTINGS.MAX_SHARES_OUTSTANDING:
                    drop["too_many_shares_outstanding"] += 1; continue

                quote = await get_last_quote_for_symbol(symbol)
                
                ask = getattr(quote, "ask_price", None)
                bid = getattr(quote, "bid_price", None)
                if ask and bid and ask > 0 and bid > 0:
                    mid = (ask + bid) / 2
                    spread_pct = (ask - bid) / mid * 100
                    if spread_pct > SETTINGS.MAX_SPREAD_PERCENT:
                        drop["spread_too_wide"] += 1
                        continue

                entry_price = quote.bid_price
                if not (entry_price and entry_price > 0):
                    lp = getattr(snap_data, "last", None)
                    if lp and lp > 0:
                        entry_price = lp
                    else:
                        drop["invalid_quote"] += 1; continue

                if len(OPEN_POSITIONS) < SETTINGS.MAX_POSITIONS and trading_enabled:
                    logging.info(f"✅ VETTING PASSED (1st Wave): Executing SHORT trade for {symbol}.")
                    await execute_trade(symbol, OrderSide.SELL, entry_price, initial_gap_percent)
            except Exception as e:
                logging.error(f"Error during 1st Wave vetting for {symbol}: {e}", exc_info=True)
            finally:
                if drop: logging.info(f"Vetting drop-off for {symbol}: {dict(drop)}")
                TRADE_SIGNAL_QUEUE.task_done()

async def execute_trade(symbol: str, side: OrderSide, price: float, initial_gap_percent: Optional[float] = None):
    """Submits a limit order for entry."""
    qty = calc_position_size(price)
    if qty == 0: return

    order_request = LimitOrderRequest(
        symbol=symbol, qty=qty, side=side, limit_price=price,
        time_in_force=TimeInForce.DAY, extended_hours=True
    )
    log_prefix = "PYRAMID ENTRY" if symbol in OPEN_POSITIONS else "ENTRY"
    logging.info(f"{log_prefix} {side.name} {qty} {symbol} | Type: LIMIT @ {price} (Extended Hours)")
    try:
        trade_order = await asyncio.to_thread(trading_client.submit_order, order_data=order_request)
        if symbol not in OPEN_POSITIONS:
            PENDING_ORDERS[trade_order.id] = {"symbol": symbol, "initial_gap_percent": initial_gap_percent}
        alert_prefix = " pyramiding on" if symbol in OPEN_POSITIONS else ""
        send_telegram_alert(f"➡️ *Short Order Submitted{alert_prefix}:* {side.value} {qty} ${symbol} @ ${price:.2f}")
    except Exception as e:
        logging.error(f"Failed to submit short entry order for {symbol}: {e}")

async def process_filled_order(order: Order):
    """Processes a filled entry order, creating or updating the position state."""
    logging.info(f"Entry fill received: {order.side} {order.qty} of {order.symbol} @ ${order.filled_avg_price}")
    try:
        position = await asyncio.to_thread(trading_client.get_open_position, order.symbol)

        is_new_position = order.symbol not in OPEN_POSITIONS

        if is_new_position:
            pending_data = PENDING_ORDERS.get(order.id, {})
            initial_gap_percent = pending_data.get('initial_gap_percent')

            OPEN_POSITIONS[order.symbol] = {
                "position_obj": position, "stop_id": None, "state": "monitoring",
                "opened_at": datetime.now(timezone.utc), "initial_gap_percent": initial_gap_percent,
                "last_pyramid_check": datetime.now(timezone.utc)
            }
            send_telegram_alert(f"✅ *Short Position Opened:* {order.side} {abs(float(order.filled_qty))} ${order.symbol}\n"
                                f"Avg Price: ${order.filled_avg_price}")
        else:
            OPEN_POSITIONS[order.symbol]['position_obj'] = position
            logging.info(f"Position {order.symbol} updated with new fill. New Qty: {position.qty}")

        if is_new_position and order.symbol not in ANALYZED_SYMBOLS_TODAY:
            initial_gap = OPEN_POSITIONS[order.symbol].get('initial_gap_percent', 0.0)
            asyncio.create_task(generate_openai_analysis(order.symbol, initial_gap))
            ANALYZED_SYMBOLS_TODAY.add(order.symbol)

    except Exception as e:
        logging.error(f"CRITICAL: FAILED TO PROCESS FILL for {order.symbol}: {e}. LIQUIDATING.")
        await liquidate_position(order.symbol, "Fill processing failure")

async def handle_trade_updates(trade: TradeUpdate):
    """Handles trade fills from the stream."""
    global REALIZED_PNL_TODAY
    try:
        if trade.event in ('fill', 'partial_fill'):
            order = trade.order

            if order.id in PENDING_ORDERS:
                await process_filled_order(order)
                if order.status in ('filled', 'canceled', 'expired'):
                    PENDING_ORDERS.pop(order.id, None)
                    logging.info(f"Entry order {order.id} for {order.symbol} is final, removed from pending.")

            elif order.symbol in OPEN_POSITIONS:
                if order.side == OrderSide.BUY:
                    pos_data = OPEN_POSITIONS.get(order.symbol)
                    if not pos_data: return

                    if order.status == 'filled':
                        entry_price = float(pos_data['position_obj'].avg_entry_price)
                        exit_price = float(order.filled_avg_price)
                        qty = abs(float(order.filled_qty))
                        realized_pl = qty * (entry_price - exit_price)

                        REALIZED_PNL_TODAY += realized_pl
                        logging.info(f"TRADE_RESULT {order.symbol} P/L=${realized_pl:.2f}")
                        send_telegram_alert(f"💰 *Position Covered:* ${order.symbol}\n*P/L:* `${realized_pl:+.2f}`")
                        OPEN_POSITIONS.pop(order.symbol, None)
    except Exception as e:
        logging.error(f"Trade update failure: {e}", exc_info=True)

async def manage_open_positions_task():
    """Manages stops for short positions. Uses a virtual stop pre-market and places a real GTC stop at market open."""
    while True:
        if not is_trading_day():
            await asyncio.sleep(3600)
            continue
        await asyncio.sleep(10)
        if not OPEN_POSITIONS: continue

        for symbol, data in list(OPEN_POSITIONS.items()):
            try:
                pos = await asyncio.to_thread(trading_client.get_open_position, symbol)
                data['position_obj'] = pos

                stop_loss_price = float(pos.avg_entry_price) * (1 + SETTINGS.STOP_LOSS_PERCENT / 100.0)

                if not data.get('stop_id'):
                    if is_premarket():
                        data['state'] = 'pm_virtual_stop'
                        if float(pos.current_price) >= stop_loss_price:
                            await liquidate_position(symbol, f"Virtual PM Stop Loss hit at ${float(pos.current_price):.2f}")
                            continue
                    elif is_rth():
                        logging.info(f"Market is open. Placing real GTC stop for {symbol}.")
                        stop_req = StopOrderRequest(
                            symbol=symbol, qty=abs(float(pos.qty)), side=OrderSide.BUY,
                            time_in_force=TimeInForce.GTC, stop_price=round(stop_loss_price, 2)
                        )
                        stop_order = await asyncio.to_thread(trading_client.submit_order, order_data=stop_req)
                        data['stop_id'] = stop_order.id
                        data['state'] = 'active_short'
                        logging.info(f"Placed GTC buy-stop for {symbol} @ {stop_loss_price:.2f}. State -> ACTIVE_SHORT.")
                        send_telegram_alert(f"🛡️ *GTC Stop Placed* for ${symbol} @ ${stop_loss_price:.2f}")

            except APIError as e:
                if "position not found" in str(e):
                    logging.warning(f"Position {symbol} seems to be closed. Removing from local state.")
                    OPEN_POSITIONS.pop(symbol, None)
                else:
                    logging.error(f"API Error managing {symbol}: {e}")
            except Exception as e:
                logging.error(f"Error managing {symbol}: {e}", exc_info=True)


async def run_trading_stream():
    """Run Alpaca TradingStream inside the current asyncio loop."""
    logging.info("run_trading_stream consumer started.")
    try:
        await trading_stream._run_forever()
    except Exception as exc:
        logging.error(f"Trading stream terminated: {exc}")
        raise

# ==============================================================================
# --- SECONDARY LOGIC TASKS ---
# ==============================================================================
async def second_wave_revetting_task():
    """
    At 10:00 AM ET, re-evaluates non-shortable candidates from pre-market.
    """
    global second_wave_triggered_today
    while True:
        await asyncio.sleep(30)
        if not is_trading_day():
            await asyncio.sleep(3600)
            continue

        now_et = get_current_et_time()
        if is_rth() and now_et >= dtime(10, 0) and not second_wave_triggered_today:
            logging.warning("--- INITIATING SECOND WAVE RE-VETTING ---")
            second_wave_triggered_today = True

            if not SECONDARY_WATCHLIST:
                logging.info("Second wave watchlist is empty. No action needed.")
                continue

            send_telegram_alert(f"🌊 *Second Wave Started!* Re-vetting {len(SECONDARY_WATCHLIST)} candidates for shortability.")

            for symbol, data in list(SECONDARY_WATCHLIST.items()):
                async with API_SEMAPHORE:
                    if symbol in OPEN_POSITIONS: continue

                    logging.info(f"--- VETTING (2nd Wave) {symbol} FOR SHORT ---")
                    try:
                        initial_gap = data['initial_gap']
                        high_price = data['price']

                        snapshot = await get_snapshot_for_symbol(symbol)
                        if not snapshot or not hasattr(snapshot, 'ticker'):
                            logging.warning(f"Could not get valid snapshot for {symbol} during 2nd wave.")
                            continue
                        current_gap = snapshot.ticker.todays_change_perc

                        if current_gap < initial_gap:
                            logging.info(f"FILTERED (2nd Wave): {symbol} gap faded to {current_gap:.1f}%. Initial was {initial_gap:.1f}%.")
                            continue

                        asset = await asyncio.to_thread(trading_client.get_asset, symbol)

                        if asset.shortable:
                            logging.info(f"✅ VETTING PASSED (2nd Wave): {symbol} is now shortable!")

                            price_to_use = high_price if high_price and high_price > 0 else snapshot.ticker.last_trade.price
                            if len(OPEN_POSITIONS) < SETTINGS.MAX_POSITIONS and trading_enabled:
                                await execute_trade(symbol, OrderSide.SELL, price_to_use, current_gap)
                        else:
                            logging.info(f"FILTERED (2nd Wave): {symbol} is still not shortable.")

                    except Exception as e:
                        logging.error(f"Error during 2nd Wave vetting for {symbol}: {e}")

            SECONDARY_WATCHLIST.clear()
            logging.warning("--- SECOND WAVE RE-VETTING COMPLETE ---")

async def pyramid_positions_task():
    """Hourly check to add to positions that have gapped up further."""
    while True:
        await asyncio.sleep(60 * 15)
        if not is_trading_day() or not is_rth() or not OPEN_POSITIONS or not pyramiding_enabled:
            continue

        logging.info("Running hourly pyramid check on open positions...")
        now_utc = datetime.now(timezone.utc)

        for symbol, data in list(OPEN_POSITIONS.items()):
            last_check = data.get('last_pyramid_check', now_utc - timedelta(hours=2))
            if (now_utc - last_check).total_seconds() < 3600:
                continue

            initial_gap = data.get('initial_gap_percent')
            if not initial_gap:
                data['last_pyramid_check'] = now_utc
                continue

            logging.info(f"Checking pyramid condition for {symbol} (Initial Gap: {initial_gap:.1f}%)")
            try:
                snapshot = await get_snapshot_for_symbol(symbol)
                if not snapshot or not hasattr(snapshot, 'ticker'):
                    logging.warning(f"Could not get valid snapshot for {symbol} during pyramid check.")
                    data['last_pyramid_check'] = now_utc
                    continue
                
                current_gap = snapshot.ticker.todays_change_perc

                if current_gap >= initial_gap + 5:
                    logging.warning(f"PYRAMID CONDITION MET for {symbol}! Current Gap: {current_gap:.1f}% > Initial: {initial_gap:.1f}% + 5.")
                    send_telegram_alert(f" pyramiding on `${symbol}`. Gap increased to {current_gap:.1f}%.")

                    await execute_trade(symbol, OrderSide.SELL, snapshot.ticker.last_trade.price, initial_gap)
                else:
                    logging.info(f"Pyramid condition not met for {symbol}. Current Gap: {current_gap:.1f}%")

                data['last_pyramid_check'] = now_utc

            except Exception as e:
                logging.error(f"Error during pyramid check for {symbol}: {e}")
                data['last_pyramid_check'] = now_utc

# ==============================================================================
# --- BACKGROUND & MAIN TASKS ---
# ==============================================================================
async def run_heartbeat():
    """Logs the bot's status periodically."""
    while True:
        await asyncio.sleep(60)
        sim_status = f" (SIMULATING {SETTINGS.SIMULATED_ET_HOUR}:00 ET)" if SETTINGS.SIMULATION_MODE else ""
        watchlist_count = len(POTENTIAL_GAPPERS) // 2
        logging.info(f"Heartbeat: Vetting Queue={TRADE_SIGNAL_QUEUE.qsize()}, Watchlist={watchlist_count}, 2ndWave={len(SECONDARY_WATCHLIST)}, Open Shorts={len(OPEN_POSITIONS)}{sim_status}")

async def telegram_heartbeat():
    """Sends a periodic 'I'm alive' message to Telegram."""
    while True:
        await asyncio.sleep(SETTINGS.TELEGRAM_HEARTBEAT_INTERVAL)
        try:
            open_pos_count = len(OPEN_POSITIONS)
            sim_status = f" (Simulating {SETTINGS.SIMULATED_ET_HOUR}:00 ET)" if SETTINGS.SIMULATION_MODE else ""
            send_telegram_alert(f"⏳ Heartbeat OK – {open_pos_count} open short positions.{sim_status}")
        except Exception as e:
            logging.error(f"Telegram heartbeat failed: {e}")

async def daily_reset_task():
    """Resets daily flags and lists."""
    global trading_enabled, pyramiding_enabled, second_wave_triggered_today
    while True:
        now_et = datetime.now(ET)
        if now_et.hour == 1 and now_et.minute == 0 and not SETTINGS.SIMULATION_MODE:
            logging.info("--- Performing daily reset ---")
            send_telegram_alert("☀️ *Good Morning!* Performing daily reset for Short_King.")
            trading_enabled = True
            pyramiding_enabled = True
            second_wave_triggered_today = False
            TRADED_SYMBOLS_TODAY.clear()
            POTENTIAL_GAPPERS.clear()
            SECONDARY_WATCHLIST.clear()
            ANALYZED_SYMBOLS_TODAY.clear()
            REALIZED_PNL_TODAY = 0.0
            logging.info("Daily flags, watchlists, and PnL reset.")
            await asyncio.sleep(3600)
        await asyncio.sleep(60)

# async def end_of_day_close_task():
#     """(DISABLED) Closes all positions at 16:25 ET as a safety net."""
#     ...

async def sync_positions_on_startup():
    """Queries Alpaca for existing positions and syncs the internal state."""
    logging.info("Attempting to sync with existing Alpaca positions...")
    try:
        existing_positions = await asyncio.to_thread(trading_client.get_all_positions)
        if not existing_positions:
            logging.info("No existing positions found in Alpaca account.")
            return

        open_orders = await asyncio.to_thread(trading_client.get_orders, GetOrdersRequest(status=QueryOrderStatus.OPEN))
        orders_by_symbol = {o.symbol: o for o in open_orders}
        logging.info(f"Found {len(open_orders)} open orders to sync.")

        logging.info(f"Found {len(existing_positions)} existing positions. Syncing short positions now...")
        for pos in existing_positions:
            if pos.side == 'short':
                stop_order_id = None
                if pos.symbol in orders_by_symbol:
                    order = orders_by_symbol[pos.symbol]
                    if order.side == OrderSide.BUY and order.order_type == 'stop' and float(order.qty) == abs(float(pos.qty)):
                        stop_order_id = order.id
                        logging.info(f"Found existing stop order {order.id} for synced position {pos.symbol}.")

                logging.warning(f"Synced existing short position: {pos.qty} of {pos.symbol} @ ${pos.avg_entry_price}. State set to 'monitoring'.")
                OPEN_POSITIONS[pos.symbol] = {
                    "position_obj": pos, "stop_id": stop_order_id, "state": "monitoring",
                    "opened_at": datetime.now(timezone.utc), "initial_gap_percent": None,
                    "last_pyramid_check": datetime.now(timezone.utc)
                }
                TRADED_SYMBOLS_TODAY.add(pos.symbol)

                if pos.symbol not in ANALYZED_SYMBOLS_TODAY:
                    try:
                        snapshot = await get_snapshot_for_symbol(pos.symbol)
                        current_gap = 0.0
                        if snapshot and hasattr(snapshot, 'ticker'):
                            current_gap = snapshot.ticker.todays_change_perc
                        else:
                            logging.error(f"Error getting snapshot for AI analysis on sync {pos.symbol}")
                        
                        asyncio.create_task(generate_openai_analysis(pos.symbol, current_gap))
                        ANALYZED_SYMBOLS_TODAY.add(pos.symbol)
                    except Exception as e:
                        logging.error(f"Error getting snapshot for AI analysis on sync {pos.symbol}: {e}")

        logging.info(f"Sync complete. Internal state now tracking {len(OPEN_POSITIONS)} short positions.")
    except Exception as e:
        logging.error(f"Failed to sync existing positions: {e}", exc_info=True)


async def main():
    """The main entry point for the bot."""
    check_environment_variables()
    logging.info(f"--- Initializing Short_King.py v4.5 ---")

    try:
        account = await asyncio.to_thread(trading_client.get_account)
        logging.info(f"Account equity: ${float(account.equity):,.2f}.")
    except Exception as e:
        logging.critical(f"Failed to initialize Alpaca client: {e}", exc_info=True)
        sys.exit(1)

    await sync_positions_on_startup()

    send_telegram_alert(f"🤖 *Short King Bot Initializing...* Version 4.5. Now tracking {len(OPEN_POSITIONS)} existing positions.")

    TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() in ("1","true","yes")
    if TELEGRAM_ENABLED and TELEGRAM_BOT_TOKEN:
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception as _e:
            logging.warning(f"delete_webhook warning: {_e}")
        
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("status", status_command))
        app.add_handler(CommandHandler("openpositions", positions_command))
        app.add_handler(CommandHandler("pnl", pnl_command))
        app.add_handler(CommandHandler("refresh_insights", refresh_insights_command))
        app.add_handler(CommandHandler("liquidate", liquidate_command))
        app.add_handler(CommandHandler("close_all", close_all_command))
        app.add_handler(CommandHandler("pause_trading", pause_trading_command))
        app.add_handler(CommandHandler("resume_trading", resume_trading_command))
        app.add_handler(CommandHandler("show_watchlist", show_watchlist_command))
        app.add_handler(CommandHandler("set_max_positions", set_max_positions_command))
        app.add_handler(CommandHandler("toggle_pyramiding", toggle_pyramiding_command))
        app.add_handler(CommandHandler("market_status", market_status_command))
        
        await app.start()
        try:
            asyncio.create_task(app.updater.start_polling(drop_pending_updates=True))
        except Exception as e:
            logging.error(f"Telegram polling failed: {e}. Disabling Telegram integration.")

    trading_stream.subscribe_trade_updates(handle_trade_updates)

    tasks = [
        run_trading_stream(),
        premarket_gapper_scanner_task(),
        consistency_and_vetting_task(),
        candidate_vetting_and_execution_task(),
        manage_open_positions_task(),
        second_wave_revetting_task(),
        pyramid_positions_task(),
        run_heartbeat(),
        telegram_heartbeat(),
        daily_reset_task(),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
    finally:
        logging.info("--- TRADING BOT SHUT DOWN ---")
        send_telegram_alert("⏹️ *Short King bot shutdown complete.*")