# ==============================================================================
# --- SHORT KING: UNHINGED RISK MANAGEMENT EDITION (LIVE) ---
#
# MODIFIED VERSION (V6):
# - BASE: Strictly follows SHORTKING_DEC25.PY structure (1400+ line equiv).
# - RISK: Global 10% Hard Stop (PM & RTH) -> ACTIVE.
# - RISK: 15% Ratchet System -> DEACTIVATED (Parked for future use).
# - MEMORY: MongoDB restored for High/Low/Avg tracking.
# - STRATEGY: Opening Bell (9:30-9:35) sniper logic based on PM behavior.
# - EXECUTION: Forced Liquidation (Internal Trigger) for PM & RTH.
# - UPDATE V5: Added "Trend Meter" (e.g., 30% Positive / 70% Negative) to PnL.
# - UPDATE V5: Startup state recovery prevents re-entry.
# ==============================================================================

import asyncio
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, time as dtime, timezone, timedelta, date
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Literal
from collections import defaultdict
import re
import statistics
import math # Import math for ceil
from types import SimpleNamespace
import pytz
import pandas as pd
import numpy as np

# --- Vendor Client Imports ---
import requests
from dotenv import load_dotenv
from dataclasses import dataclass, field # Import field
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest, StopLimitOrderRequest, StopOrderRequest,
    ClosePositionRequest, GetOrdersRequest, ReplaceOrderRequest, GetPortfolioHistoryRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus, AssetStatus, OrderType
from alpaca.trading.models import TradeUpdate, Position, Order
from alpaca.trading.stream import TradingStream
from alpaca.common.exceptions import APIError
from polygon import RESTClient as PolygonRESTClient
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

# --- Database Imports (RESTORED) ---
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import openai

# ==============================================================================
# --- REMOVED RATCHET ENGINE & RISK MATRIX CONFIGURATION ---
# ==============================================================================
# RatchetSettings dataclass and RATCHET_CONFIG object removed.

NY_TZ_STR: str = "America/New_York"
NY_TZ = pytz.timezone(NY_TZ_STR)
SIMPLE_STOP_LOSS_PCT: float = 0.20 # Legacy 20% stop loss (Overridden by Risk Manager)
POST_CANCEL_SLEEP: float = 0.75
POST_PLACE_SLEEP: float = 0.2
CANCEL_RETRIES: int = 12


# Enum for market phases
class MarketPhase:
    PRE_MARKET = "PRE_MARKET"
    OPENING_BELL = "OPENING_BELL"
    REGULAR_HOURS = "REGULAR_HOURS"
    CLOSED = "CLOSED"

# ==============================================================================
# --- GENERAL CONFIGURATION & SETTINGS (MODIFIED) ---
# ==============================================================================
@dataclass
class Settings:
    """Centralized configuration for all trading parameters and filters."""
    # === MODIFICATION: Gap threshold lowered to 15% ===
    MIN_GAP_PERCENT: float = 15.0
    # === REMOVED: ADVANCED_VETTING_GAP_THRESHOLD ===
    MIN_PM_VOLUME: int = 10000
    MAX_SPREAD_PERCENT: float = 2.0
    # === MODIFICATION: Trade value lowered to 1000.0 ===
    TRADE_NOTIONAL_VALUE: float = 1000.0
    MAX_POSITIONS: int = 10 # V10 Change: Increased from 5 to 10
    CONCURRENT_TASKS: int = 10
    TELEGRAM_HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL", 600))
    TELEGRAM_PNL_UPDATE_INTERVAL: int = 5
    OPENAI_MODEL: str = "gpt-4-turbo"
    BLACKLISTED_TICKERS: Set[str] = field(default_factory=lambda: {"DMF"})

    # --- RISK MANAGEMENT CONSTANTS (UNHINGED) ---
    MAX_LOSS_PCT: float = 0.10          # 10% Hard Cap (PM & RTH)
    RATCHET_TRIGGER_PCT: float = 0.15   # 15% Profit triggers the ratchet
    RATCHET_TRAILING_STEP: float = 0.01 # 1% Trailing step
    OPENING_BELL_DURATION: int = 5      # Minutes (9:30-9:35)

SETTINGS = Settings()

# --- Environment & Logging Setup ---
load_dotenv()
# Alpaca (UPDATED FOR LIVE KEYS)
APCA_API_KEY_ID = os.getenv("ALPACA_R")
APCA_API_SECRET_KEY = os.getenv("ALPACA_SR")
ALPACA_IS_PAPER = False # <--- SWITCHED TO LIVE TRADING

# Polygon
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("Stocks_GPT_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_USER_ID")
# Database
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") # RESTORED

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
log_file = 'Short_King_Simple_PM.log' # Renamed log file
file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
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
# API Clients
trading_client = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=ALPACA_IS_PAPER)
trading_stream = TradingStream(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=ALPACA_IS_PAPER)
polygon_client = PolygonRESTClient(POLYGON_API_KEY)
openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
telegram_request = HTTPXRequest(connect_timeout=60.0, read_timeout=60.0)
telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN, request=telegram_request) if TELEGRAM_BOT_TOKEN else None

# Database (RESTORED)
if not MONGO_URI:
    logger.critical("❌ FATAL: MONGO_URI environment variable is not set. REQUIRED for Risk Memory.")
    sys.exit(1)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ismaster')
    db = mongo_client.short_king_gpt_db
    pos_mem = db["position_memory"] # RESTORED
    logger.info("✅ MongoDB connection successful. Memory Engine Online.")
except ConnectionFailure as e:
    logger.critical(f"❌ MongoDB connection failed: {e}")
    sys.exit(1)


# State Management
POTENTIAL_GAPPERS: Dict[str, Any] = {}
PENDING_ORDERS: Dict[str, Dict[str, Any]] = {}
# OPEN_POSITIONS format simplified, no longer holds mem_doc_id
OPEN_POSITIONS: Dict[str, Dict[str, Any]] = {}
TRADED_SYMBOLS_TODAY: set[str] = set()
PROFITABLY_TRADED_SYMBOLS: set[str] = set()
ANALYZED_SYMBOLS_TODAY: set[str] = set()
VETTING_QUEUE = asyncio.Queue()
# ADVANCED_VETTING_QUEUE REMOVED
API_SEMAPHORE = asyncio.Semaphore(SETTINGS.CONCURRENT_TASKS)
trading_enabled: bool = True
REALIZED_PNL_TODAY: float = 0.0

# ==============================================================================
# --- RATCHET ENGINE: CORE MATH & UTILITIES (SIMPLIFIED) ---
# ==============================================================================
def get_current_market_phase() -> str:
    """Determines the current market phase based on NY time."""
    now_ny = datetime.now(NY_TZ)
    time_ny = now_ny.time()

    if not is_trading_day():
        return MarketPhase.CLOSED

    # PM-Only trading bot
    if dtime(4, 0) <= time_ny < dtime(9, 30):
        return MarketPhase.PRE_MARKET
    elif dtime(9, 30) <= time_ny < dtime(9, 36):
        return MarketPhase.OPENING_BELL
    elif dtime(9, 36) <= time_ny < dtime(16, 0):
        return MarketPhase.REGULAR_HOURS
    else:
        return MarketPhase.CLOSED

# Removed get_risk_params_for_phase

def pnl_pct_short(entry, price):
    if entry == 0: return 0.0
    return (entry - price) / entry

# REMOVED: lock_price_for_short
# REMOVED: loss_price_for_short

def _to_aware_utc(dt: datetime) -> datetime:
    """Converts a datetime object to be timezone-aware and in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# REMOVED: stop_breached_for_short
# REMOVED: loss_stop_breached_for_short

async def sync_daily_realized_pnl():
    """Fetches the day's realized PNL from the broker to sync state."""
    global REALIZED_PNL_TODAY
    try:
        today_str = datetime.now(NY_TZ).strftime('%Y-%m-%d')
        # Request portfolio history for just today
        req = GetPortfolioHistoryRequest(
            date_start=today_str,
            date_end=today_str,
            period="1D" # Get data for just this one day
        )
        history = await asyncio.to_thread(trading_client.get_portfolio_history, req)
        
        if history.profit_loss and history.profit_loss[-1] is not None:
            REALIZED_PNL_TODAY = float(history.profit_loss[-1])
            logger.info(f"Successfully synced daily realized PNL from broker: ${REALIZED_PNL_TODAY:+.2f}")
        else:
            logger.warning("Could not sync realized PNL from broker. history.profit_loss is empty or None. Defaulting to 0.0")
            REALIZED_PNL_TODAY = 0.0
    except Exception as e:
        logger.error(f"Failed to sync daily realized PNL: {e}. Defaulting to 0.0")
        REALIZED_PNL_TODAY = 0.0

# ==============================================================================
# --- RATCHET ENGINE: STATEFUL MEMORY & EXECUTION ---
# ==============================================================================
async def cancel_all_symbol_orders(symbol, trading_client):
    """Robustly cancels all open orders for a symbol."""
    if symbol in SETTINGS.BLACKLISTED_TICKERS: return True
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        open_orders = await asyncio.to_thread(trading_client.get_orders, filter=req)
        for o in open_orders:
            try:
                await asyncio.to_thread(trading_client.cancel_order_by_id, order_id=o.id)
            except Exception:
                pass 
        
        for _ in range(CANCEL_RETRIES):
            rem = await asyncio.to_thread(trading_client.get_orders, filter=req)
            if not rem: return True
            await asyncio.sleep(POST_CANCEL_SLEEP)
    except Exception as e:
        logger.error(f"cancel_all_symbol_orders failed for {symbol}: {e}")
    return False

# ==============================================================================
# --- RISK MANAGEMENT: MEMORY & LOGIC (ADDED) ---
# ==============================================================================

def update_position_memory(symbol: str, current_pnl_pct: float) -> dict:
    """
    Updates MongoDB with the latest PnL stats to track Highs, Lows, and Time in Loss/Profit.
    Returns the updated document.
    """
    now_utc = datetime.now(timezone.utc)
    
    # Atomic update to ensure precision
    update_op = {
        "$set": {"last_updated": now_utc},
        "$max": {"highest_pnl_pct": current_pnl_pct}, # Record High Water Mark
        "$min": {"lowest_pnl_pct": current_pnl_pct},  # Record Low Water Mark (Max Loss)
        "$inc": {
            "tick_count": 1, 
            "sum_pnl_pct": current_pnl_pct,
            # Increment counters based on state
            "ticks_in_profit": 1 if current_pnl_pct > 0 else 0,
            "ticks_in_loss": 1 if current_pnl_pct <= 0 else 0
        },
        "$setOnInsert": {"created_at": now_utc, "symbol": symbol}
    }
    
    try:
        doc = pos_mem.find_one_and_update(
            {"symbol": symbol},
            update_op,
            upsert=True,
            return_document=True
        )
        # Calculate derived stats on the fly
        doc['avg_pnl_pct'] = doc['sum_pnl_pct'] / doc['tick_count'] if doc['tick_count'] > 0 else 0.0
        return doc
    except Exception as e:
        logger.error(f"MongoDB Update Error for {symbol}: {e}")
        return {}


async def universal_risk_manager(symbol, pos_obj: Position):
    """
    STRICT RISK MANAGEMENT LAYER.
    1. GLOBAL: 10% Max Loss Cap (Hard Stop).
    2. RATCHET: DEACTIVATED (Logic parked).
    3. OPENING BELL (9:30-9:35): Uses Memory to kill losers on pullback or take winners at highs.
    """
    if symbol in SETTINGS.BLACKLISTED_TICKERS: return

    try:
        # 1. Data Extraction
        entry = float(pos_obj.avg_entry_price)
        last = float(pos_obj.current_price)
        pnl_pct = pnl_pct_short(entry, last) # Returns positive float for profit, negative for loss
        
        # 2. Update Memory (Critical for 9:30 Logic & Trend Meter)
        mem_doc = await asyncio.to_thread(update_position_memory, symbol, pnl_pct)
        if not mem_doc: return # Fail safe

        now_ny = datetime.now(NY_TZ)
        time_ny = now_ny.time()
        
        # --- RULE 1: THE UNHINGED HARD STOP (GLOBAL) ---
        # "10% IS THE MAX CAP OF LOSS... SAME MEASURE DURING RTH"
        if pnl_pct <= -SETTINGS.MAX_LOSS_PCT: # e.g. -0.10
            logger.warning(f"🛑 HARD STOP TRIGGERED for {symbol}. PnL: {pnl_pct:.2%} <= -10%.")
            await liquidate_position_safely(symbol, f"Hard Stop 10% Breached ({pnl_pct:.2%})")
            return

        # --- RULE 2: THE RATCHET (PROFIT PROTECTION) ---
        # [PARKED / DEACTIVATED]
        # To RE-ENABLE: Uncomment the 'logger' and 'liquidate' lines inside the block below.
        
        highest_seen = mem_doc.get('highest_pnl_pct', 0.0)
        
        if highest_seen >= SETTINGS.RATCHET_TRIGGER_PCT: # We are/were in "Ratchet Mode" (>15%)
            stop_trigger = highest_seen - SETTINGS.RATCHET_TRAILING_STEP # e.g. 0.16 - 0.01 = 0.15
            
            if pnl_pct < stop_trigger:
                reason = f"Ratchet Hit: Dropped 1% from High ({highest_seen:.2%} -> {pnl_pct:.2%})"
                # === DEACTIVATED FOR UNLIMITED UPSIDE ===
                # logger.info(f"💰 TAKING PROFIT on {symbol}. {reason}")
                # await liquidate_position_safely(symbol, reason)
                # return
                pass # Proceed without liquidating

        # --- RULE 3: OPENING BELL EXECUTION (9:30 AM - 9:35 AM) ---
        # "CRITICAL PART... FIRST 5 MINUTES... TOUCHES HIGHEST POINT OR... LESS NEGATIVE THAN AVERAGE"
        
        start_open = dtime(9, 30)
        end_open = dtime(9, 35)
        
        if start_open <= time_ny < end_open:
            # A. The Winner Scenario: Touches highest PM point
            # If current PnL is matching or beating the historical high
            if pnl_pct >= (highest_seen - 0.001): # Tolerance of 0.1%
                logger.info(f"🔔 OPENING BELL WINNER: {symbol} re-testing highs at {pnl_pct:.2%}. Closing.")
                await liquidate_position_safely(symbol, "Opening Bell: Re-test of Highs")
                return

            # B. The Loser Scenario: "Negative for almost all the time... liquidate when less negative than average"
            ticks_loss = mem_doc.get('ticks_in_loss', 0)
            ticks_total = mem_doc.get('tick_count', 1)
            loss_ratio = ticks_loss / ticks_total
            avg_pnl = mem_doc.get('avg_pnl_pct', 0.0)

            # If it was red > 80% of the time AND currently doing better than average (pullback)
            if loss_ratio > 0.80 and pnl_pct > avg_pnl:
                 logger.info(f"🔔 OPENING BELL BAILOUT: {symbol} (Red {loss_ratio:.0%} of time). Current {pnl_pct:.2%} > Avg {avg_pnl:.2%}. Exiting on pullback.")
                 await liquidate_position_safely(symbol, "Opening Bell: Bailout on Dead Cat Bounce")
                 return

    except Exception as e:
        logger.error(f"Error in Universal Risk Manager for {symbol}: {e}", exc_info=True)


# ==============================================================================
# --- GENERAL UTILITY & SCANNING LOGIC ---
# ==============================================================================
def check_environment_variables():
    """Checks for all required environment variables at startup."""
    # Mongo URI check restored
    required_vars = {
        "Alpaca Key ID": APCA_API_KEY_ID, "Alpaca Secret Key": APCA_API_SECRET_KEY,
        "Polygon API Key": POLYGON_API_KEY, "OpenAI API Key": OPENAI_API_KEY,
        "Telegram Bot Token": TELEGRAM_BOT_TOKEN, "Telegram Chat ID": TELEGRAM_CHAT_ID,
        "Mongo URI": MONGO_URI
    }
    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        logging.critical(f"FATAL: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    logging.info("All required environment variables are present.")

def get_current_et_time() -> dtime:
    return datetime.now(tz=NY_TZ).time()

def is_premarket() -> bool:
    return get_current_market_phase() == MarketPhase.PRE_MARKET

def is_rth() -> bool:
    phase = get_current_market_phase()
    return phase in [MarketPhase.OPENING_BELL, MarketPhase.REGULAR_HOURS]

def is_trading_day() -> bool:
    now = datetime.now(tz=NY_TZ)
    return now.weekday() < 5

async def send_telegram_alert(message: str):
    if not (telegram_bot and TELEGRAM_CHAT_ID): return
    if any(f"${ticker}" in message for ticker in SETTINGS.BLACKLISTED_TICKERS):
        logger.info(f"Skipping Telegram alert for blacklisted ticker: {message[:100]}...")
        return
    try:
        if len(message) > 4096:
            for x in range(0, len(message), 4096):
                await telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message[x:x+4096], parse_mode='Markdown')
        else:
            await telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")

def fetch_all_shortable_symbols_sync() -> set[str]:
    """Fetches all shortable/tradable assets via a raw HTTP request."""
    headers = {
        "APCA-API-KEY-ID": APCA_API_KEY_ID,
        "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY
    }
    base_url = "https://paper-api.alpaca.markets" if ALPACA_IS_PAPER else "https://api.alpaca.markets"
    url = f"{base_url}/v2/assets"
    params = {"status": "active"}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        all_assets = response.json()
        shortable_symbols = {
            asset['symbol'] for asset in all_assets
            if asset.get('tradable') and asset.get('shortable')
            and asset['symbol'] not in SETTINGS.BLACKLISTED_TICKERS
        }
        logger.info(f"Fetched {len(shortable_symbols)} usable shortable symbols from Alpaca.")
        return shortable_symbols
    except Exception as e:
        logger.error(f"Failed to fetch shortable symbols: {e}")
        return set()

def _mk_snapshot_from_raw(gapper_dict: dict) -> SimpleNamespace:
    """Converts the raw dictionary from a full snapshot fetch into a standard SimpleNamespace."""
    return SimpleNamespace(
        ticker=gapper_dict.get("ticker"),
        pct=gapper_dict.get("todaysChangePerc"),
        volume=(gapper_dict.get("day") or {}).get("v", 0),
        last_price=(gapper_dict.get("lastTrade") or {}).get("p")
    )

def fetch_all_snapshot_tickers_sync(limit: int = 2000) -> list[dict]:
    """Fetch US stock snapshots from Polygon with pagination (cursor). Slower but most compatible."""
    base_url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params: Dict[str, Any] = {"limit": limit, "apiKey": POLYGON_API_KEY}
    all_tickers = []
    url = base_url
    retries = 3
    while retries > 0:
        try:
            resp = requests.get(url, params=params, timeout=45) # Increased timeout
            resp.raise_for_status()
            data = resp.json()
            tickers_batch = data.get("tickers", [])
            all_tickers.extend([t for t in tickers_batch if t.get("ticker") not in SETTINGS.BLACKLISTED_TICKERS])
            
            next_url = data.get("next_url")
            if not next_url:
                break # Finished pagination
            url = next_url
            params = {"apiKey": POLYGON_API_KEY} # Key is needed again for next_url
            retries = 3 # Reset retries on success
        except requests.exceptions.Timeout:
             retries -= 1
             logger.warning(f"Snapshot fetch timed out, {retries} retries left...")
             time.sleep(2) # Wait before retrying
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch a batch of snapshots: {e}")
            return [] 
    if retries == 0: logger.error("Snapshot fetch failed after multiple timeouts.")
    return all_tickers


# MODIFIED: MIN_GAP_PERCENT updated to 15.0%
# MODIFIED: Removed logic for ADVANCED_VETTING_QUEUE
async def premarket_gapper_scanner_task():
    """Scans for gappers using a slow-but-reliable full market snapshot."""
    SLEEP_SEC = 60
    while True:
        if not (is_trading_day() and is_premarket()):
            await asyncio.sleep(60)
            continue
        try:
            logger.info("Scanning for new potential short targets (Compatibility Mode)...")
            shortable_symbols = await asyncio.to_thread(fetch_all_shortable_symbols_sync)
            if not shortable_symbols:
                await asyncio.sleep(SLEEP_SEC); continue

            raw_snapshots = await asyncio.to_thread(fetch_all_snapshot_tickers_sync)
            if not raw_snapshots:
                logger.warning("Scanner fetched no tickers. Check API key or connection."); await asyncio.sleep(SLEEP_SEC); continue

            snaps = [_mk_snapshot_from_raw(r) for r in raw_snapshots]
            shortable_snaps = [s for s in snaps if s.ticker in shortable_symbols]
            gainers = sorted([s for s in shortable_snaps if s.pct is not None and s.pct > 0],
                             key=lambda s: s.pct, reverse=True)

            reasons = defaultdict(int)
            new_candidates_count = 0
            for snap in gainers:
                sym = snap.ticker
                if sym in SETTINGS.BLACKLISTED_TICKERS: continue
                
                if sym in POTENTIAL_GAPPERS or sym in TRADED_SYMBOLS_TODAY:
                    reasons["already_processed"] += 1; continue
                
                # NEW: Explicit check for OPEN_POSITIONS to prevent re-vetting active trades
                if sym in OPEN_POSITIONS: 
                    reasons["already_processed"] += 1; continue 
                
                # === MODIFICATION: Use new 15.0% threshold ===
                if snap.pct < SETTINGS.MIN_GAP_PERCENT: # Now 15.0%
                    reasons["gap_below_threshold"] += 1; continue
                if snap.volume and snap.volume < SETTINGS.MIN_PM_VOLUME:
                    reasons["low_volume"] += 1; continue
                
                new_candidates_count += 1
                logger.info(f"Gapper {sym} at {snap.pct:.1f}% → adding to watchlist.")
                POTENTIAL_GAPPERS[sym] = snap
                price_str = f"${snap.last_price:.2f}" if snap.last_price is not None else "N/A"
                await send_telegram_alert(
                    f"👀 *New Short Candidate:* `${sym}`\n"
                    f"Gap: *{snap.pct:.1f}%* | Price: {price_str}"
                )
                
                # === MODIFICATION: All candidates go to the single VETTING_QUEUE ===
                await VETTING_QUEUE.put(snap)
            
            logger.info(f"Scan complete. Found={new_candidates_count} | Dropped Reasons={dict(reasons)}")

        except Exception as e:
            logger.error(f"Scanner error: {e}", exc_info=True)
        await asyncio.sleep(SLEEP_SEC)

# MODIFIED: Removed logic for ADVANCED_VETTING_QUEUE
async def priority_monitoring_task():
    """High-frequency monitoring of previously profitable stocks for re-entry opportunities."""
    SLEEP_SEC = 10
    while True:
        await asyncio.sleep(SLEEP_SEC)
        try:
            if not (is_trading_day() and is_premarket()):
                continue

            priority_list = list(PROFITABLY_TRADED_SYMBOLS - set(OPEN_POSITIONS.keys()) - SETTINGS.BLACKLISTED_TICKERS)
            if not priority_list: continue

            logger.info(f"Priority Radar checking {len(priority_list)} symbols: {priority_list}")

            for symbol in priority_list:
                if symbol in SETTINGS.BLACKLISTED_TICKERS: continue
                try:
                    snapshot_data = await asyncio.to_thread(polygon_client.get_snapshot_ticker, market_type="stocks", ticker=symbol)
                    current_gap_pct = snapshot_data.todays_change_percent

                    # === MODIFICATION: Use new 15.0% threshold ===
                    if current_gap_pct >= SETTINGS.MIN_GAP_PERCENT: # Now 15.0%
                        logger.info(f"RADAR HIT: Profitable symbol ${symbol} has re-gapped to {current_gap_pct:.2f}%. Queueing for vetting.")
                        await send_telegram_alert(f"🎯 *Radar Hit:* `${symbol}` re-gapped to {current_gap_pct:.2f}%. Vetting for re-entry.")

                        vetting_snapshot = SimpleNamespace(ticker=symbol, pct=current_gap_pct, volume=snapshot_data.day.volume, last_price=snapshot_data.last_trade.price)

                        # === MODIFICATION: All candidates go to the single VETTING_QUEUE ===
                        logger.info(f"Re-queuing {symbol} to vetting queue.")
                        await VETTING_QUEUE.put(vetting_snapshot)

                        PROFITABLY_TRADED_SYMBOLS.discard(symbol) # Remove once queued
                except Exception as e:
                    logger.error(f"Could not fetch snapshot for priority symbol {symbol}: {e}")

        except Exception as e:
            logger.error(f"Priority monitoring task error: {e}", exc_info=True)

# MODIFIED: Renamed from fast_vetting_and_execution_task
async def vetting_and_execution_task():
    while True:
        snapshot = await VETTING_QUEUE.get()
        symbol = snapshot.ticker

        if not is_premarket():
            logger.info(f"Market not in PM. Discarding vetting task for {symbol}.")
            VETTING_QUEUE.task_done(); continue

        if symbol in SETTINGS.BLACKLISTED_TICKERS:
             logger.info(f"Skipping vetting for blacklisted symbol: {symbol}")
             VETTING_QUEUE.task_done(); continue

        if symbol in OPEN_POSITIONS or len(OPEN_POSITIONS) >= SETTINGS.MAX_POSITIONS or not trading_enabled:
            VETTING_QUEUE.task_done(); continue

        async with API_SEMAPHORE:
            try:
                if symbol in SETTINGS.BLACKLISTED_TICKERS: continue
                # TRADED_SYMBOLS_TODAY.add(symbol) # <-- MOVED from here

                logger.info(f"--- VETTING {symbol} FOR SHORT (Gap: {snapshot.pct:.2f}%) ---")
                quote = await asyncio.to_thread(polygon_client.get_last_quote, ticker=symbol)
                ask, bid = getattr(quote, "ask_price", 0), getattr(quote, "bid_price", 0)

                if not (ask > 0 and bid > 0): logger.warning(f"{symbol} invalid quote."); continue

                spread_pct = ((ask - bid) / bid) * 100 if bid > 0 else float('inf')
                if spread_pct > SETTINGS.MAX_SPREAD_PERCENT: logger.warning(f"Spread for {symbol} ({spread_pct:.2f}%) too wide."); continue

                # Use Bid price for LIMIT order target
                entry_price_target = bid
                
                # === MODIFICATION: Use new 1000.0 notional value ===
                qty = math.floor(SETTINGS.TRADE_NOTIONAL_VALUE / entry_price_target) if entry_price_target > 0 else 0
                if qty <= 0: logger.warning(f"Calculated quantity is 0 for {symbol}."); continue

                # --- MOVED TO HERE ---
                # Add to processed list ONLY after all checks have passed
                TRADED_SYMBOLS_TODAY.add(symbol)

                logger.info(f"✅ VETTING PASSED: Executing SHORT trade for {symbol}.")
                # Pass is_high_risk_gapper as False (since adv. vetting is gone)
                await execute_simple_trade(symbol, qty, entry_price_target, snapshot.pct, is_high_risk_gapper=False)
            except Exception as e:
                logger.error(f"Error during fast vetting for {symbol}: {e}", exc_info=True)
            finally: VETTING_QUEUE.task_done()

# ==============================================================================
# --- V9 REVERT: EXECUTION LOGIC BACK TO LIMIT ORDERS ---
# (This function remains largely unchanged, but red_flag is no longer passed from adv. vetting)
# ==============================================================================
async def execute_simple_trade(symbol: str, qty: int, limit_price: float, initial_gap_percent: float, is_high_risk_gapper: bool = False, red_flag: bool = False):
    if symbol in SETTINGS.BLACKLISTED_TICKERS:
        logger.warning(f"Attempted to execute trade for blacklisted symbol {symbol}. Aborting.")
        return

    order_request = LimitOrderRequest(
        symbol=symbol, qty=qty, side=OrderSide.SELL, limit_price=limit_price,
        time_in_force=TimeInForce.DAY, extended_hours=True 
    )
    logger.info(f"ENTRY SHORT {qty} {symbol} | Type: LIMIT @ {limit_price:.2f}")
    try:
        trade_order = await asyncio.to_thread(trading_client.submit_order, order_data=order_request)
        PENDING_ORDERS[trade_order.id] = {
            "symbol": symbol,
            "initial_gap_percent": initial_gap_percent,
            "submitted_at": datetime.now(timezone.utc),
            "is_high_risk_gapper": is_high_risk_gapper, # Kept in case, but adv. vetting gone
            "red_flag": red_flag,
            "order_type": "limit" 
        }
        await send_telegram_alert(f"➡️ *Short LIMIT Order Submitted for ${symbol}*\nQty: {qty} @ ${limit_price:.2f} (Gap: {initial_gap_percent:.1f}%)")
    except Exception as e:
        logger.error(f"Failed to submit short LIMIT entry order for {symbol}: {e}")
        await send_telegram_alert(f"❌ Failed to submit entry order for ${symbol}: {e}") 

# ==============================================================================
# --- CORE ORCHESTRATION & EVENT HANDLING ---
# ==============================================================================
# MODIFIED: Simplified to remove MongoDB/Ratchet logic
async def process_filled_order(order: Order):
    symbol = order.symbol
    if symbol in SETTINGS.BLACKLISTED_TICKERS:
        logger.warning(f"Received fill for blacklisted symbol {symbol}. Attempting immediate liquidation.")
        try: await liquidate_position_safely(symbol, "Fill received for blacklisted ticker.")
        except: pass 
        if order.id in PENDING_ORDERS: PENDING_ORDERS.pop(order.id)
        return

    logger.info(f"Entry fill received: {order.side} {order.qty} of {symbol} @ ${order.filled_avg_price}")
    pending_order_data = PENDING_ORDERS.get(order.id, {})
    
    # Red flag status no longer relevant from adv. vetting
    red_flag = pending_order_data.get("red_flag", False) 

    try:
        position = await asyncio.to_thread(trading_client.get_open_position, symbol)
        
        # === MODIFICATION: Simplified position tracking ===
        # No doc_id, no init_position_memory
        OPEN_POSITIONS[symbol] = {
            "position_obj": position,
            "manual_override_until": None,
            "red_flag": red_flag, # Kept for consistency
            "accumulated_filled_qty": 0.0, 
            "accumulated_pnl_this_trade": 0.0 
        }

        await send_telegram_alert(f"✅ *Short Position Opened:* {order.side.value} {abs(float(order.filled_qty))} ${symbol}\n"
                                  f"Avg Price: ${order.filled_avg_price}\n🧠 _Unhinged Risk Management engaged._")

    except APIError as e:
        if "position not found" in str(e):
             logger.warning(f"Position {symbol} not found after fill, likely already closed.")
             OPEN_POSITIONS.pop(symbol, None) 
        else:
             logger.error(f"API error processing fill for {symbol}: {e}")
             await liquidate_position_safely(symbol, f"Fill processing API failure: {e}")
    except Exception as e:
        logger.error(f"General error processing fill for {symbol}: {e}", exc_info=True)
        await liquidate_position_safely(symbol, f"Fill processing general failure: {e}")
    finally:
        if order.id in PENDING_ORDERS: PENDING_ORDERS.pop(order.id)


# MODIFIED: Calls the new simplified management function
async def manage_open_positions_task():
    """Runs the Universal Risk Manager at high frequency."""
    while True:
        # === MODIFICATION: Sample rate is less critical, 5s is fine ===
        await asyncio.sleep(2) # Polling at 2 seconds for high precision
        await sync_positions_with_broker() # Sync first
        
        active_symbols = list(OPEN_POSITIONS.keys())
        if not active_symbols: continue

        non_blacklisted_symbols = [s for s in active_symbols if s not in SETTINGS.BLACKLISTED_TICKERS]
        if not non_blacklisted_symbols: continue 

        for symbol in non_blacklisted_symbols:
            if symbol not in OPEN_POSITIONS: continue
            if symbol in SETTINGS.BLACKLISTED_TICKERS: continue

            data = OPEN_POSITIONS[symbol]
            override_until = data.get('manual_override_until')
            if override_until and datetime.now(timezone.utc) < override_until:
                # logger.info(f"Skipping management for {symbol} due to manual override.")
                continue

            try:
                position = data.get('position_obj')
                if not position:
                    logger.warning(f"Position object missing for {symbol}. Attempting sync/removal.")
                    try:
                         live_pos = await asyncio.to_thread(trading_client.get_open_position, symbol_or_asset_id=symbol)
                         if live_pos: data['position_obj'] = live_pos
                         else: OPEN_POSITIONS.pop(symbol, None); continue 
                         position = data['position_obj'] 
                    except APIError as sync_e:
                         if "position not found" in str(sync_e): OPEN_POSITIONS.pop(symbol, None); continue
                         else: raise sync_e 

                # === MODIFICATION: Call the new, UNHINGED risk manager ===
                if position:
                    await universal_risk_manager(symbol, position)
                else:
                    logger.error(f"CRITICAL: Missing position object for {symbol}, cannot manage position.")

            except APIError as e:
                if "position not found" in str(e):
                    logger.warning(f"Position {symbol} closed externally or liquidated. Removing.")
                    OPEN_POSITIONS.pop(symbol, None)
                else: logger.error(f"API Error managing {symbol}: {e}")
            except Exception as e:
                logger.error(f"Error managing {symbol}: {e}", exc_info=True)


async def liquidate_position_safely(symbol: str, reason: str):
    """
    Attempts to liquidate position, ensuring blacklisted symbols aren't processed.
    This function is critical and remains unchanged as it supports PM liquidation.
    """
    if symbol in SETTINGS.BLACKLISTED_TICKERS:
        logger.warning(f"Liquidation request ignored for blacklisted symbol: {symbol}")
        return
        
    logger.warning(f"LIQUIDATING {symbol} due to: {reason}")
    await send_telegram_alert(f"🚨 *LIQUIDATING:* Attempting to close ${symbol}. Reason: {reason}.")
    
    OPEN_POSITIONS.pop(symbol, None)
    
    try:
        await cancel_all_symbol_orders(symbol, trading_client)
        
        try:
            pos = await asyncio.to_thread(trading_client.get_open_position, symbol)
            qty_to_close = abs(float(pos.qty))
        except APIError as e:
             if "position not found" in str(e):
                 logger.info(f"Position {symbol} already closed before liquidation order could be placed.")
                 return 
             else:
                 raise 
        
        client_order_id = f"sk_liq_{symbol}_{int(time.time())}"

        if is_rth():
            logger.info(f"RTH detected. Using MARKET order to liquidate {symbol}.")
            close_req = MarketOrderRequest(
                symbol=symbol, qty=qty_to_close, side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY, client_order_id=client_order_id)
            order_type_log = "Market"
        else: # Pre-Market or Closed (handles PM liquidations)
            logger.info(f"Outside RTH. Using aggressive LIMIT order to liquidate {symbol}.")
            try:
                quote = await asyncio.to_thread(polygon_client.get_last_quote, ticker=symbol)
                ask_price = getattr(quote, "ask_price", 0)
                if not ask_price > 0: 
                    # Fallback: get last trade price if quote is bad
                    last_trade = await asyncio.to_thread(polygon_client.get_last_trade, ticker=symbol)
                    ask_price = getattr(last_trade, "price", 0)
                    if not ask_price > 0:
                        raise ValueError("Invalid ask price and last trade price")
                
                limit_price = round(ask_price * 1.05, 2) # 5% slippage allowance
                logger.info(f"Aggressive limit price for {symbol} set to {limit_price} (based on ask/trade {ask_price})")
            except Exception as quote_e:
                logger.critical(f"Could not get valid quote for {symbol} during PM liquidation: {quote_e}. MANUAL INTERVENTION REQUIRED.")
                await send_telegram_alert(f"❌ *CRITICAL LIQUIDATION FAILED* for ${symbol}. Could not fetch quote. PLEASE CLOSE MANUALLY.")
                return

            close_req = LimitOrderRequest(
                symbol=symbol, qty=qty_to_close, side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY, limit_price=limit_price,
                extended_hours=True, client_order_id=client_order_id)
            order_type_log = "Limit"

        await asyncio.to_thread(trading_client.submit_order, order_data=close_req)
        logger.info(f"Liquidation order ({order_type_log}) for {qty_to_close} shares of {symbol} submitted successfully.")
        await send_telegram_alert(f"Liquidation order ({order_type_log}) for {qty_to_close} shares of ${symbol} submitted.")

    except APIError as e:
        if "position not found" not in str(e): 
            logger.error(f"Failed to liquidate {symbol}: {e}")
            await send_telegram_alert(f"❌ *LIQUIDATION FAILED* for ${symbol}. Error: {e}. MANUAL ACTION MAY BE NEEDED.")
    except Exception as e:
        logger.error(f"Unexpected error during liquidation for {symbol}: {e}", exc_info=True)
        await send_telegram_alert(f"❌ *UNEXPECTED LIQUIDATION ERROR* for ${symbol}. Error: {e}. MANUAL ACTION MAY BE NEEDED.")


async def liquidate_all_positions():
    await send_telegram_alert("🚨 *LIQUIDATING ALL POSITIONS...*")
    try:
        positions = await asyncio.to_thread(trading_client.get_all_positions)
        symbols_to_close = [p.symbol for p in positions if p.symbol not in SETTINGS.BLACKLISTED_TICKERS]
        logger.info(f"Found {len(symbols_to_close)} non-blacklisted positions to liquidate: {symbols_to_close}")
        for symbol in symbols_to_close:
            await liquidate_position_safely(symbol, "Manual command /closeall")
    except Exception as e:
         logger.error(f"Failed to get positions for /closeall command: {e}")
         await send_telegram_alert("❌ Failed to fetch positions for /closeall. Please check manually.")


# MODIFIED: Removed memory-based PNL, relies on internal accumulation
async def handle_trade_updates(trade: TradeUpdate):
    global REALIZED_PNL_TODAY
    try:
        order = trade.order
        symbol = order.symbol

        if symbol in SETTINGS.BLACKLISTED_TICKERS: return

        if trade.event in ('new', 'accepted') and symbol in OPEN_POSITIONS and order.side == OrderSide.BUY:
            client_id = order.client_order_id or ""
            if not client_id.startswith("sk_"): 
                pos_data = OPEN_POSITIONS[symbol]
                now = datetime.now(timezone.utc)
                if not pos_data.get('manual_override_until') or now > pos_data['manual_override_until']:
                    override_until = now + timedelta(minutes=15)
                    pos_data['manual_override_until'] = override_until
                    logger.info(f"MANUAL OVERRIDE DETECTED for {symbol}. Pausing auto-management until {override_until.isoformat()}.")
                    await send_telegram_alert(f"🕹️ Manual override for ${symbol} detected. Bot paused for 15 mins.")

        if trade.event in ('fill', 'partial_fill'):
            if order.id in PENDING_ORDERS:
                await process_filled_order(order) 

            elif symbol in OPEN_POSITIONS and order.side == OrderSide.BUY:
                 pos_data = OPEN_POSITIONS[symbol]
                 pos_obj = pos_data.get('position_obj')
                 
                 # === MODIFICATION: Get entry price from synced pos_obj ===
                 if not pos_obj:
                      logger.warning(f"Received exit fill for {symbol} but missing internal pos_obj.")
                      # Try to sync it one last time
                      try:
                          pos_obj = await asyncio.to_thread(trading_client.get_open_position, symbol)
                          OPEN_POSITIONS[symbol]['position_obj'] = pos_obj
                      except Exception as e:
                          logger.error(f"Could not sync pos_obj for {symbol} during exit fill: {e}")
                          return # Cannot calculate PnL

                 entry = float(pos_obj.avg_entry_price)
                 exit_price = float(order.filled_avg_price) if order.filled_avg_price else float(pos_obj.current_price) 
                 qty_filled = abs(float(order.filled_qty))
                 known_qty = abs(float(pos_obj.qty))
                 accumulated_filled = pos_data.get('accumulated_filled_qty', 0.0) + qty_filled
                 pos_data['accumulated_filled_qty'] = accumulated_filled
                 
                 pl_this_fill = qty_filled * (entry - exit_price)
                 
                 accumulated_pnl_this_trade = pos_data.get('accumulated_pnl_this_trade', 0.0) + pl_this_fill
                 pos_data['accumulated_pnl_this_trade'] = accumulated_pnl_this_trade
                 
                 REALIZED_PNL_TODAY_BEFORE = REALIZED_PNL_TODAY
                 REALIZED_PNL_TODAY += pl_this_fill
                 logger.info(f"[{symbol}] Exit Fill Detected: Qty={qty_filled}, AvgPx={exit_price:.2f}, P/L This Fill=${pl_this_fill:+.2f}")
                 logger.info(f"[{symbol}] REALIZED_PNL_TODAY updated from ${REALIZED_PNL_TODAY_BEFORE:+.2f} to ${REALIZED_PNL_TODAY:+.2f} (Daily Total)")

                 is_fully_closed = (order.status == 'filled' and accumulated_filled >= known_qty - 0.01)

                 if is_fully_closed:
                    logger.info(f"[{symbol}] Full closure detected by fill event. Accumulated Qty: {accumulated_filled}/{known_qty}")
                    
                    final_pl_for_display = accumulated_pnl_this_trade
                    logger.info(f"[{symbol}] Final P/L for this trade: ${final_pl_for_display:+.2f}")
                    
                    re_entry_message = ""
                    if final_pl_for_display > 0: 
                        PROFITABLY_TRADED_SYMBOLS.add(symbol)
                        logger.info(f"[{symbol}] Added to PROFITABLY_TRADED_SYMBOLS for re-entry monitoring.")
                        re_entry_message = "\nAdded to priority watchlist for re-entry."

                    await send_telegram_alert(f"💰 *Position Covered:* ${symbol}\n*P/L:* `${final_pl_for_display:+.2f}`{re_entry_message}") 
                    OPEN_POSITIONS.pop(symbol, None) 
                 else:
                     logger.info(f"[{symbol}] Partial exit fill. Qty filled: {qty_filled}. Total Accum: {accumulated_filled}/{known_qty}")

    except Exception as e:
        logger.error(f"Trade update handling failed: {e}", exc_info=True)


async def run_trading_stream():
    logging.info("Trading stream consumer started.")
    try:
        await trading_stream._run_forever()
    except Exception as exc:
        logging.error(f"Trading stream terminated: {exc}")

# ==============================================================================
# --- ADVANCED VETTING (REMOVED) ---
# ==============================================================================
# REMOVED: get_catalyst_classification
# REMOVED: get_advanced_vetting_data
# REMOVED: advanced_vetting_task

# ==============================================================================
# --- TELEGRAM BOT & BACKGROUND TASKS ---
# ==============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("*Short_King_GPT UNHINGED Risk v6 (LIVE)* is running.", parse_mode='Markdown') # Update version

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_positions_with_broker()
    active_positions = {s: d for s, d in OPEN_POSITIONS.items() if s not in SETTINGS.BLACKLISTED_TICKERS}
    pnl = sum(float(p['position_obj'].unrealized_pl) for p in active_positions.values() if p.get('position_obj'))
    phase = get_current_market_phase()
    text = (f"*Short King Unhinged Risk v6 (LIVE)*\n" # Update version
            f"*Market Phase:* {phase}\n"
            f"*Open Positions:* {len(active_positions)}/{SETTINGS.MAX_POSITIONS}\n"
            f"*Trading Enabled:* {'✅' if trading_enabled else '⏸️'}\n"
            f"*Stop-Loss Rule:* 10% Hard / Ratchet PAUSED\n" # Added new rule
            f"*Unrealized P/L:* `${pnl:+.2f}`\n"
            f"*Realized P/L Today:* `${REALIZED_PNL_TODAY:+.2f}`")
    await update.message.reply_text(text, parse_mode='Markdown')

# MODIFIED: Simplified to remove Floor/Stop display
async def positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_positions_with_broker() 
    active_positions = {s: d for s, d in OPEN_POSITIONS.items() if s not in SETTINGS.BLACKLISTED_TICKERS}
    if not active_positions:
        await update.message.reply_text("No active (non-blacklisted) short positions."); return

    message = "*--- Open Positions (10% Stop / Ratchet PAUSED) ---*\n"
    for symbol, data in active_positions.items():
        pos = data.get('position_obj')
        if not pos: continue

        pnl_usd = float(pos.unrealized_pl)
        pnl_pct = pnl_pct_short(float(pos.avg_entry_price), float(pos.current_price))
        
        # === REMOVED: Floor and GTC Stop display ===
        # red_flag_icon = "🚨 " if data.get("red_flag") else "" # No longer relevant
        
        message += (f"`${symbol}` | P/L: `${pnl_usd:+.2f}` ({pnl_pct:+.2%})\n"
                    f"   Entry: ${float(pos.avg_entry_price):.2f} | Qty: {abs(float(pos.qty))}\n")
    await update.message.reply_text(message, parse_mode='Markdown')

async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_positions_with_broker() 
    active_positions = {s: d for s, d in OPEN_POSITIONS.items() if s not in SETTINGS.BLACKLISTED_TICKERS}
    unrealized = sum(float(p['position_obj'].unrealized_pl) for p in active_positions.values() if p.get('position_obj'))
    logger.info(f"/pnl command: Realized=${REALIZED_PNL_TODAY:+.2f}, Unrealized=${unrealized:+.2f}")
    message = (f"💰 *Realized P/L (Today):* `${REALIZED_PNL_TODAY:+.2f}`\n"
               f"📈 *Unrealized P/L (Open):* `${unrealized:+.2f}`")
    await update.message.reply_text(message, parse_mode='Markdown')


async def liquidate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Allow $TSLA or TSLA
        symbol = context.args[0].upper().replace('$', '')
        if symbol in SETTINGS.BLACKLISTED_TICKERS:
             await update.message.reply_text(f"${symbol} is blacklisted. No action taken."); return
        
        # Check internal state first
        if symbol in OPEN_POSITIONS:
            await update.message.reply_text(f"🚨 Liquidating position in ${symbol} now...")
            await liquidate_position_safely(symbol, "Manual liquidation via Telegram command.")
        else:
            # If not in internal state, double-check broker
            try:
                await asyncio.to_thread(trading_client.get_open_position, symbol)
                # If this succeeds, a position exists but isn't in our state
                logger.warning(f"Manual liquidation for {symbol} which was not in OPEN_POSITIONS. Liquidating anyway.")
                await update.message.reply_text(f"🚨 Position ${symbol} found at broker (not in local state). Liquidating now...")
                await liquidate_position_safely(symbol, "Manual liquidation via Telegram command.")
            except APIError as e:
                if "position not found" in str(e):
                    await update.message.reply_text(f"No open position found for ${symbol} (checked broker).")
                else:
                    await update.message.reply_text(f"Error checking broker for ${symbol}: {e}")
            
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /liquidate [SYMBOL]")

async def close_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_positions_with_broker() 
    if not any(s not in SETTINGS.BLACKLISTED_TICKERS for s in OPEN_POSITIONS.keys()):
        await update.message.reply_text("No active (non-blacklisted) positions to close.")
        return
    await update.message.reply_text(f"🚨🚨 Liquidating ALL active position(s) now...")
    await liquidate_all_positions()

async def pause_trading_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global trading_enabled; trading_enabled = False
    await update.message.reply_text("⏸️ Trading has been PAUSED.")

async def resume_trading_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global trading_enabled; trading_enabled = True
    await update.message.reply_text("▶️ Trading has been RESUMED.")

async def show_watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = "*--- Primary Watchlist ---*\n"
    gapper_symbols = [s for s, v in POTENTIAL_GAPPERS.items() if isinstance(v, SimpleNamespace) and s not in SETTINGS.BLACKLISTED_TICKERS]
    if gapper_symbols:
        symbols_str = [f"{s} ({POTENTIAL_GAPPERS[s].pct:.1f}%)" for s in gapper_symbols]
        message += f"`{', '.join(symbols_str)}`"
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
        await update.message.reply_text("Usage: /maxpos [number]")

async def market_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = get_current_market_phase()
    await update.message.reply_text(f"📈 Market Status: *{status}*", parse_mode='Markdown')

async def ask_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("This feature is under development. Usage: /ask [SYMBOL] [Your Question]")

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

async def set_gap_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_gap = float(context.args[0])
        if 5.0 <= new_gap <= 200.0:
            SETTINGS.MIN_GAP_PERCENT = new_gap
            await update.message.reply_text(f"✅ Scanner gap threshold set to *{new_gap:.1f}%*.", parse_mode='Markdown')
        else:
            await update.message.reply_text("Please provide a number between 5 and 200.")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /setgap [percentage]")

async def set_volume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_vol = int(context.args[0])
        if new_vol >= 1000:
            SETTINGS.MIN_PM_VOLUME = new_vol
            await update.message.reply_text(f"✅ Scanner minimum volume set to *{new_vol:,}* shares.", parse_mode='Markdown')
        else:
            await update.message.reply_text("Please provide a number >= 1000.")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /setvolume [number]")

async def manual_scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ *Triggering manual market scan (Compatibility Mode)...*")
    try:
        raw_snapshots = await asyncio.to_thread(fetch_all_snapshot_tickers_sync)
        valid_snapshots = [s for s in raw_snapshots if s.get("ticker") not in SETTINGS.BLACKLISTED_TICKERS]
        gainers = sorted([s for s in valid_snapshots if s.get("todaysChangePerc") is not None and s.get("todaysChangePerc") > 0],
            key=lambda s: s["todaysChangePerc"], reverse=True)

        if not gainers:
            await update.message.reply_text("Manual scan complete. No significant non-blacklisted gappers found."); return

        message = "*--- Top 5 Market Gappers (Manual Scan) ---*\n"
        for snap in gainers[:5]:
            symbol = snap.get("ticker")
            gap_pct = snap.get("todaysChangePerc", 0)
            volume = (snap.get("day") or {}).get("v", 0)
            price = (snap.get("lastTrade") or {}).get("p")
            price_str = f"${price:.2f}" if price else "N/A"
            message += f"`${symbol}` | Gap: *{gap_pct:+.2f}%* | Vol: {volume:,.0f} | Price: {price_str}\n"
        message += f"\n_Note: Unfiltered. Auto-trade threshold >{SETTINGS.MIN_GAP_PERCENT:.1f}%._"
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Manual scan failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Manual scan failed. Check logs.")


async def manage_pending_orders_task():
    while True:
        await asyncio.sleep(30)
        now_utc = datetime.now(timezone.utc)
        if not PENDING_ORDERS: continue
        
        to_cancel = []
        for oid, data in list(PENDING_ORDERS.items()): 
             if data.get("order_type") == "market":
                  if data.get('submitted_at') and (now_utc - data['submitted_at']).total_seconds() > 120:
                       logger.warning(f"Removing potentially stuck pending MARKET order {oid} ({data.get('symbol')}) after 2 mins.")
                       PENDING_ORDERS.pop(oid, None)
                  continue

             submitted_at = data.get('submitted_at')
             symbol = data.get('symbol', 'UNKNOWN')
             if submitted_at and (now_utc - submitted_at).total_seconds() > 300:
                 try:
                      order_status = await asyncio.to_thread(trading_client.get_order_by_id, order_id=oid)
                      if order_status.status not in ['filled', 'partially_filled']:
                          to_cancel.append((oid, symbol))
                      else:
                           PENDING_ORDERS.pop(oid, None) 
                 except APIError as e:
                     if "order not found" in str(e): PENDING_ORDERS.pop(oid, None) 
                     else: logger.error(f"Error checking status for pending order {oid}: {e}")
                 except Exception as e:
                      logger.error(f"Unexpected error checking pending order {oid}: {e}")

        for order_id, symbol in to_cancel:
            try:
                if symbol in SETTINGS.BLACKLISTED_TICKERS:
                    PENDING_ORDERS.pop(order_id, None); continue
                    
                logger.warning(f"Cancelling stale pending order {order_id} for {symbol}.")
                await asyncio.to_thread(trading_client.cancel_order_by_id, order_id=order_id)
                await send_telegram_alert(f"❌ *Order Canceled:* Entry for `${symbol}` unfilled after 5 minutes.")
                PENDING_ORDERS.pop(order_id, None);
            except APIError as e:
                if "order is not cancelable" in str(e) or "order not found" in str(e):
                    PENDING_ORDERS.pop(order_id, None)
                else: logger.error(f"Failed to cancel pending order {order_id}: {e}")
            except Exception as e:
                 logger.error(f"Unexpected error cancelling pending order {order_id}: {e}")

async def run_heartbeat():
    while True:
        await asyncio.sleep(60)
        status = get_current_market_phase()
        active_pos = len([s for s in OPEN_POSITIONS if s not in SETTINGS.BLACKLISTED_TICKERS])
        # Removed AdvancedQ
        logger.info(f"Heartbeat | {status} | VettingQ: {VETTING_QUEUE.qsize()} | Open: {active_pos}")

async def telegram_status_summary_task():
    while True:
        await asyncio.sleep(SETTINGS.TELEGRAM_HEARTBEAT_INTERVAL)
        try:
            active_pos_count = len([s for s in OPEN_POSITIONS if s not in SETTINGS.BLACKLISTED_TICKERS])
            status_mode = "ACTIVE" if trading_enabled else "PAUSED"
            message = f"⏳ *Bot Status Summary* | *{status_mode}*\nOpen Positions: {active_pos_count}"
            await send_telegram_alert(message)
        except Exception as e:
            logger.error(f"Telegram status summary failed: {e}")

# MODIFIED: Simplified PNL update, removed timers/floors
# RE-ADDED: TIMER & TREND METER LOGIC HERE
async def telegram_pnl_update_task():
    while True:
        await asyncio.sleep(SETTINGS.TELEGRAM_PNL_UPDATE_INTERVAL) 
        active_positions = {s: d for s, d in OPEN_POSITIONS.items() if s not in SETTINGS.BLACKLISTED_TICKERS}
        if not active_positions or not is_trading_day() or get_current_market_phase() == MarketPhase.CLOSED: continue

        try:
            message = "📊 *Live P&L & Trend Meter*\n"
            total_unrealized_pl = 0.0
            market_phase = get_current_market_phase()

            for symbol, data in active_positions.items():
                pos = data.get('position_obj')
                if not pos: continue

                try:
                    pnl_usd = float(pos.unrealized_pl)
                    pnl_pct = pnl_pct_short(float(pos.avg_entry_price), float(pos.current_price))
                    total_unrealized_pl += pnl_usd
                    
                    # --- TIMER & TREND FETCH LOGIC ---
                    time_str = "0m 0s"
                    trend_str = ""
                    try:
                        mem = await asyncio.to_thread(pos_mem.find_one, {"symbol": symbol})
                        if mem:
                            ticks_green = mem.get('ticks_in_profit', 0)
                            ticks_red = mem.get('ticks_in_loss', 0)
                            total_ticks = ticks_green + ticks_red
                            
                            # Calculate total duration (Approx 2s per tick)
                            secs = total_ticks * 2
                            time_str = f"{secs // 60}m {secs % 60}s"
                            
                            # Calculate Trend %
                            if total_ticks > 0:
                                pct_green = (ticks_green / total_ticks) * 100
                                pct_red = (ticks_red / total_ticks) * 100
                                trend_str = f" (🟢{int(pct_green)}% 🔴{int(pct_red)}%)"
                            
                    except Exception as mem_e:
                        logger.error(f"Failed to fetch timer for {symbol}: {mem_e}")

                    # === MODIFICATION: Enhanced status line ===
                    status_line = f"`${symbol}`: `{pnl_pct:+.2%}` (`${pnl_usd:+.2f}`) | Time: {time_str}{trend_str}"

                    # Removed all timer/floor logic
                    if market_phase != MarketPhase.PRE_MARKET:
                        status_line += " (Risk Mode)"

                    message += status_line + "\n"

                except Exception as inner_e:
                     logger.error(f"Error processing PNL update for {symbol}: {inner_e}")
                     message += f"`${symbol}`: Error calculating status.\n"


            message += f"*Total Unrealized:* `${total_unrealized_pl:+.2f}`"
            await send_telegram_alert(message)
        except Exception as e:
            logger.error(f"Telegram PNL update task failed: {e}")


async def daily_reset_task():
    while True:
        now_ny = datetime.now(NY_TZ)
        if now_ny.hour == 0 and now_ny.minute == 5:
            logger.info("--- Performing daily reset ---")
            await send_telegram_alert("☀️ *Good Morning!* Performing daily reset.")
            global trading_enabled, REALIZED_PNL_TODAY
            trading_enabled = True
            
            await sync_daily_realized_pnl()
            
            TRADED_SYMBOLS_TODAY.clear()
            PROFITABLY_TRADED_SYMBOLS.clear()
            POTENTIAL_GAPPERS.clear();
            while not VETTING_QUEUE.empty(): await VETTING_QUEUE.get()
            # Removed ADVANCED_VETTING_QUEUE clear
            
            await sync_positions_with_broker() 
            logger.info("Daily flags and lists reset.")
            await asyncio.sleep(3600 * 23) 
        await asyncio.sleep(60) 


async def cancel_stale_pm_orders_task():
    """Runs once daily just before RTH to cancel any lingering PM orders."""
    while True:
        now_ny = datetime.now(NY_TZ)
        cancel_time_ny = now_ny.replace(hour=9, minute=28, second=0, microsecond=0)

        if now_ny >= cancel_time_ny:
            next_run_time = cancel_time_ny + timedelta(days=1)
        else:
            next_run_time = cancel_time_ny

        sleep_seconds = max(0, (next_run_time - now_ny).total_seconds())

        logger.info(f"Pre-market order cleanup scheduled for {next_run_time}. Sleeping for {sleep_seconds:.0f} seconds.")
        await asyncio.sleep(sleep_seconds)

        logger.warning("--- Executing Pre-Market Stale Order Cleanup ---")
        stale_pending_orders = {oid: data for oid, data in PENDING_ORDERS.items() if data.get("order_type") != "market"}
        if stale_pending_orders:
            await send_telegram_alert(f"🧹 *Housekeeping:* Cancelling {len(stale_pending_orders)} potentially stale pre-market orders before RTH open.")
            pending_to_cancel = list(stale_pending_orders.items()) 

            for order_id, data in pending_to_cancel:
                symbol = data.get('symbol', 'UNKNOWN')
                if symbol in SETTINGS.BLACKLISTED_TICKERS:
                    PENDING_ORDERS.pop(order_id, None); continue
                try:
                    await asyncio.to_thread(trading_client.cancel_order_by_id, order_id=order_id)
                    logger.info(f"Cancelled stale pre-market order {order_id} for {symbol}.")
                    await send_telegram_alert(f"❌ *Order Canceled (Stale):* PM entry for `${symbol}` removed before RTH.")
                    PENDING_ORDERS.pop(order_id, None)
                except APIError as e:
                    if "order is not cancelable" in str(e) or "order not found" in str(e):
                         logger.info(f"Stale order {order_id} ({symbol}) already resolved.")
                         PENDING_ORDERS.pop(order_id, None) 
                    else: logger.error(f"Failed to cancel stale order {order_id} ({symbol}): {e}")
                except Exception as e:
                     logger.error(f"Unexpected error cancelling stale order {order_id} ({symbol}): {e}")
        else:
            logger.info("No stale non-market pre-market orders found for cleanup.")
            await send_telegram_alert("🧹 Housekeeping: No stale non-market pre-market orders found.")
        
        await asyncio.sleep(3600 * 23)

# ==============================================================================
# --- STARTUP RECOVERY LOGIC (NEW) ---
# ==============================================================================
async def repopulate_state_on_startup():
    """
    Fetches today's orders to repopulate TRADED_SYMBOLS_TODAY.
    This prevents re-entering trades if the bot is restarted.
    """
    logger.info("--- REPOPULATING TRADING STATE FROM BROKER ---")
    try:
        today = datetime.now(NY_TZ).date()
        today_str = today.strftime('%Y-%m-%d')
        
        # Get all filled orders for today
        req = GetOrdersRequest(status=QueryOrderStatus.FILLED, after=today_str)
        filled_orders = await asyncio.to_thread(trading_client.get_orders, filter=req)
        
        count = 0
        for order in filled_orders:
            # If we sold (entered short) today, mark as traded
            if order.side == OrderSide.SELL:
                TRADED_SYMBOLS_TODAY.add(order.symbol)
                count += 1
                
        logger.info(f"State Recovery: Marked {count} symbols as already traded today: {TRADED_SYMBOLS_TODAY}")
        await send_telegram_alert(f"🔄 *State Restored:* Recognized {count} symbols already traded today.")
        
    except Exception as e:
        logger.error(f"Failed to repopulate state: {e}")

# ==============================================================================
# --- STARTUP & MAIN LOOP ---
# ==============================================================================
async def cancel_all_open_orders_on_startup():
    """Clears all existing open orders on startup to prevent conflicts."""
    logger.info("--- CANCELING ALL OPEN ORDERS ON STARTUP ---")
    try:
        cancel_result = await asyncio.to_thread(trading_client.cancel_orders)
        cancelled_ids = [str(status.id) for status in cancel_result if status.status == 200]
        failed_ids = [str(status.id) for status in cancel_result if status.status != 200]
        
        logger.info(f"Startup cancel results: {len(cancelled_ids)} succeeded, {len(failed_ids)} failed.")
        if failed_ids: logger.error(f"Failed to cancel orders on startup: {failed_ids}")
        
        await send_telegram_alert(f"🧹 *Startup Cleanup:* Attempted cancellation of all open orders ({len(cancelled_ids)} success, {len(failed_ids)} failed).")
    except Exception as e:
        logger.critical(f"FATAL: Failed during cancel_all_orders on startup: {e}")
        await send_telegram_alert(f"🚨 CRITICAL: Error during startup order cancellation. Please check manually!")


# MODIFIED: Simplified to remove memory init and GTC stop logic
async def sync_positions_with_broker():
    """Periodically syncs the bot's internal state with the broker's positions."""
    try:
        live_positions = await asyncio.to_thread(trading_client.get_all_positions)
        live_symbols = {p.symbol for p in live_positions if p.symbol not in SETTINGS.BLACKLISTED_TICKERS} 
        known_symbols = set(OPEN_POSITIONS.keys()) - SETTINGS.BLACKLISTED_TICKERS 

        for symbol in known_symbols - live_symbols:
            logger.warning(f"Position {symbol} is no longer live. Removing from internal state.")
            OPEN_POSITIONS.pop(symbol, None)

        for pos in live_positions:
            symbol = pos.symbol
            if symbol in SETTINGS.BLACKLISTED_TICKERS: continue 

            if symbol in OPEN_POSITIONS:
                OPEN_POSITIONS[symbol]['position_obj'] = pos; continue 

            logger.warning(f"Discovered untracked live position for {symbol}. Initializing now.")
            
            # === MODIFICATION: Simplified untracked position handling ===
            OPEN_POSITIONS[symbol] = {
                "position_obj": pos, "manual_override_until": None, "red_flag": False,
                "accumulated_filled_qty": 0.0, "accumulated_pnl_this_trade": 0.0 
            }

            # === REMOVED: GTC stop logic for discovered positions ===

    except Exception as e:
        logger.error(f"Error during position sync with broker: {e}", exc_info=True)


async def main():
    """Main entry point for the bot."""
    check_environment_variables()
    logger.info(f"--- Initializing Short_King_GPT UNHINGED RISK v6 (LIVE TRADING) ---") # Updated version
    
    await cancel_all_open_orders_on_startup()
    await sync_positions_with_broker() 
    await repopulate_state_on_startup() # NEW: Recover state
    
    await sync_daily_realized_pnl()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(telegram_request).build()

    # Register command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("positions", positions_command))
    app.add_handler(CommandHandler("pnl", pnl_command))
    app.add_handler(CommandHandler("liquidate", liquidate_command))
    app.add_handler(CommandHandler("closeall", close_all_command))
    app.add_handler(CommandHandler("pause", pause_trading_command))
    app.add_handler(CommandHandler("resume", resume_trading_command))
    app.add_handler(CommandHandler("watchlist", show_watchlist_command))
    app.add_handler(CommandHandler("maxpos", set_max_positions_command))
    app.add_handler(CommandHandler("market", market_status_command))
    app.add_handler(CommandHandler("ask", ask_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("scan", manual_scan_command))
    app.add_handler(CommandHandler("setgap", set_gap_command))
    app.add_handler(CommandHandler("setvolume", set_volume_command))

    trading_stream.subscribe_trade_updates(handle_trade_updates)
    
    # === MODIFICATION: Removed advanced_vetting_task ===
    main_tasks = [
        run_trading_stream(),
        premarket_gapper_scanner_task(),
        priority_monitoring_task(),
        vetting_and_execution_task(), # Renamed
        # advanced_vetting_task(), # REMOVED
        manage_open_positions_task(),
        manage_pending_orders_task(),
        run_heartbeat(),
        telegram_status_summary_task(),
        telegram_pnl_update_task(),
        daily_reset_task(),
        cancel_stale_pm_orders_task()
    ]

    try:
        await app.initialize()
        try: await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception as webhook_e: logger.warning(f"Webhook deletion failed (ignore if not set): {webhook_e}")
        
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True) 
        
        active_pos_count = len([s for s in OPEN_POSITIONS if s not in SETTINGS.BLACKLISTED_TICKERS])
        await send_telegram_alert(f"🤖 *Short King UNHINGED Risk v6 LIVE TRADING...*\nNow tracking {active_pos_count} active positions.\nState Restored: {len(TRADED_SYMBOLS_TODAY)} symbols.") # Updated version

        await asyncio.gather(*main_tasks)

    except Exception as e:
        logger.critical(f"Main loop exited with error: {e}", exc_info=True)
    finally:
        logger.info("--- Initiating graceful shutdown ---")
        if app.updater and app.updater.running: await app.updater.stop()
        if app.running: await app.stop()
        logger.info("--- TRADING BOT SHUT DOWN ---")
        try: await send_telegram_alert("⏹️ *Short King bot shutdown complete.*")
        except: pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    finally:
        logging.info("--- Main process exiting ---")
