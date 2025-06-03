<<<<<<< HEAD
=======
# Genie_stocks_Top3_v0.py - Top 20 Strategy (10 Long / 10 Short)
# - Scans NYSE/NASDAQ based on 30-min % change.
# - Filters by price ($1-$50) and 15-min volume (>300k).
# - Uses previous close for initial price reference if needed.
# - Includes extended hours support for data fetching.
# - NOTE: Requires Alpaca SIP data plan for full universe coverage.
# - NOTE: High API usage - adjust JOB_INTERVAL_MINUTES if hitting rate limits.

>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24
import asyncio
import os
import json
import logging
import sys
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import isoparse
from collections import deque
from dotenv import load_dotenv
# from uuid import UUID # Not used in original, can be omitted if order_id is sufficient
import time
import math # For checking NaN
import numpy as np # For ATR calculation
import requests # For fetching gainers

# --- Alpaca Imports (Using alpaca-py V2) ---
from alpaca.trading.client import TradingClient as REST
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange
from alpaca.trading.models import Asset, Position # Added Position for fetching qty
from alpaca.common.exceptions import APIError
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.models import Bar # Original used BarSet, Bar is fine for lists

# --- Telegram Imports ---
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.constants import ParseMode
import telegram.error

# === Configuration === #
load_dotenv()

# Alpaca API Credentials (using APCA_ prefix as requested)
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TOP3_Stock_TELEGRAM_BOT_TOKEN") # From original script

# --- Basic Sanity Checks ---
if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
    print("ERROR: Alpaca API Key ID or Secret Key not found (APCA_API_KEY_ID, APCA_API_SECRET_KEY).", file=sys.stderr)
    sys.exit(1)
if not TELEGRAM_BOT_TOKEN:
    print("ERROR: TOP3_Stock_TELEGRAM_BOT_TOKEN not found.", file=sys.stderr)
    sys.exit(1)

ALPACA_PAPER_TRADING = os.getenv("ALPACA_PAPER_TRADING", "true").lower() == "true"

<<<<<<< HEAD
# --- State and Log Files (using original names for consistency with existing setup) ---
STATE_FILE = "genie_stocks_top3_v0_state.json" # Original state file name
LOG_FILE = "genie_stocks_top3_v0.log"     # Original log file name
=======
# --- Strategy Parameters ---
TRADE_NOTIONAL_PER_STOCK = 5000
JOB_INTERVAL_MINUTES = 2
HOLD_TIME_LIMIT_MINUTES = 30
EXIT_PRICE_FETCH_DELAY_SECONDS = 1.5
MAX_LONG_POSITIONS = 10
MAX_SHORT_POSITIONS = 10
MAX_OPEN_POSITIONS = MAX_LONG_POSITIONS + MAX_SHORT_POSITIONS
EXTENDED_HOURS_TRADING = True
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24

# --- Strategy Parameters (NEW High-Volume Gapper Strategy) ---
TRADE_NOTIONAL_PER_STOCK = float(os.getenv("TRADE_NOTIONAL_PER_STOCK", "5000.0"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3")) # New strategy: Top-3 long positions
JOB_INTERVAL_MINUTES = int(os.getenv("JOB_INTERVAL_MINUTES", "2"))
HOLD_TIME_LIMIT_MINUTES = int(os.getenv("HOLD_TIME_LIMIT_MINUTES", "30"))
EXIT_PRICE_FETCH_DELAY_SECONDS = 1.5 # Kept from original for PnL on exit

# ATR and Trailing Stop Configuration (NEW)
ATR_TIMEFRAME_MINUTES = int(os.getenv("ATR_TIMEFRAME_MINUTES", "5"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_INITIAL_SL_MULTIPLIER = float(os.getenv("ATR_INITIAL_SL_MULTIPLIER", "1.5"))
TRAIL_ACTIVATION_PROFIT_ATR = float(os.getenv("TRAIL_ACTIVATION_PROFIT_ATR", "1.0")) # ATRs in profit
TRAIL_OFFSET_ATR = float(os.getenv("TRAIL_OFFSET_ATR", "1.0")) # ATRs to trail

# Filtering Parameters (NEW Strategy)
GAINERS_ENDPOINT_URL = os.getenv("GAINERS_ENDPOINT_URL", "http://localhost:8000/gainers") # Placeholder
MIN_RELATIVE_VOLUME_FOR_ENTRY = float(os.getenv("MIN_RELATIVE_VOLUME_FOR_ENTRY", "3.0"))
PRICE_FILTER_MIN = float(os.getenv("PRICE_FILTER_MIN", "1.0"))
PRICE_FILTER_MAX = float(os.getenv("PRICE_FILTER_MAX", "100.0")) # Increased max price for gappers

EXTENDED_HOURS_TRADING = os.getenv("EXTENDED_HOURS_TRADING", "true").lower() == "true" # From original

# === Setup Logging === #
logging.basicConfig(
    level=logging.INFO, # Changed from DEBUG
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    encoding='utf-8'
)
<<<<<<< HEAD
logging.info(f"--- BOT START (High-Volume Gapper Strategy MODIFIED from Genie_stocks_Top3_v0) ---")
logging.info(f"PAPER_TRADING: {ALPACA_PAPER_TRADING}")
logging.info(f"Trade Notional: ${TRADE_NOTIONAL_PER_STOCK}, Max Positions (Long Only): {MAX_POSITIONS}")
logging.info(f"ATR Config: {ATR_PERIOD}P on {ATR_TIMEFRAME_MINUTES}min bars. Init SL: {ATR_INITIAL_SL_MULTIPLIER}x ATR.")
logging.info(f"Trailing Stop: Activates at {TRAIL_ACTIVATION_PROFIT_ATR} ATRs gain, trails by {TRAIL_OFFSET_ATR} ATRs.")
logging.info(f"Job Interval: {JOB_INTERVAL_MINUTES} min, Hold Limit: {HOLD_TIME_LIMIT_MINUTES} min")
logging.info(f"Filters: Price ${PRICE_FILTER_MIN}-${PRICE_FILTER_MAX}, Min Rel Vol: {MIN_RELATIVE_VOLUME_FOR_ENTRY}x")
logging.info(f"Extended Hours Data Fetching: {EXTENDED_HOURS_TRADING}")
=======
logging.info(f"-------------------- BOT START (Top20_v0 - 30min Change) --------------------")
logging.info(f"PAPER_TRADING: {PAPER_TRADING}")
logging.info(
    f"Trade Notional: ${TRADE_NOTIONAL_PER_STOCK}, "
    f"Max Long: {MAX_LONG_POSITIONS}, Max Short: {MAX_SHORT_POSITIONS}, "
    f"Hold Limit: {HOLD_TIME_LIMIT_MINUTES} min"
)
logging.info(f"Job Interval: {JOB_INTERVAL_MINUTES} min, Extended Hours Data: {EXTENDED_HOURS_TRADING}")
logging.info(f"Filters: Price ${MIN_PRICE_FILTER}-${MAX_PRICE_FILTER}, Min Volume ({VOLUME_LOOKBACK_MINUTES}min): {MIN_VOLUME_FILTER}, Ranking Lookback: {RANKING_LOOKBACK_MINUTES}min")
logging.warning("Ensure you have an adequate Alpaca data plan (SIP recommended) for full NYSE/NASDAQ scanning.")
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24


# === Global State & Data Structures (as per original script) ===
state = {} # Loaded by load_state()
active_assets_cache = [] # Maintained as original, though less critical for new scan
last_assets_fetch_time = None
state_lock = asyncio.Lock() # For async operations (Telegram handlers) modifying shared state

# === Initialize Alpaca V2 Clients (as per original script) ===
try:
    rest_client = REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=ALPACA_PAPER_TRADING)
    data_client = StockHistoricalDataClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY)
    account_info = rest_client.get_account()
    logging.info(f"Alpaca clients initialized. Account Status: {account_info.status}, Buying Power: {account_info.buying_power}")
except APIError as e:
    logging.critical(f"Failed to initialize Alpaca clients or connect: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logging.critical(f"An unexpected error occurred during Alpaca client initialization: {e}", exc_info=True)
    sys.exit(1)

# === Helper Functions (Adapted/Merged) ===
def now_utc() -> datetime: # Consistent naming
    """Returns the current UTC datetime."""
    return datetime.now(timezone.utc)

def load_state_original(): # Explicitly named to match its origin for clarity
    """Loads state from JSON file, ensuring default structure (original pattern)."""
    global state # Operates on global state
    default_state_struct = {"trades": [], "pnl": [], "strategy_enabled": {}, "goals": {}}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding='utf-8') as f:
                loaded_data = json.load(f)
            for key, default_value in default_state_struct.items():
                loaded_data.setdefault(key, default_value) # Ensure all keys exist
            # Specific new strategy fields within each trade dict will be handled in run_strategy_cycle
            state = loaded_data
            logging.info(f"Successfully loaded state from {STATE_FILE}")
        except json.JSONDecodeError:
            logging.error(f"Error decoding state JSON from {STATE_FILE}. Initializing.", exc_info=True)
            state = default_state_struct
        except Exception as e:
            logging.error(f"Error loading state file {STATE_FILE}: {e}. Initializing.", exc_info=True)
            state = default_state_struct
    else:
        logging.warning(f"State file {STATE_FILE} not found. Initializing.")
        state = default_state_struct
    return state # Return the global state

def save_state_original(state_to_save_param=None): # Explicit name, param for clarity
    """Saves the current global state (or provided dict) to a JSON file (original pattern)."""
    data_to_save = state_to_save_param if state_to_save_param is not None else state
    try:
        # Ensure datetimes are stringified for JSON
        state_copy = json.loads(json.dumps(data_to_save, default=str))
        with open(STATE_FILE, "w", encoding='utf-8') as f:
            json.dump(state_copy, f, indent=4)
        logging.debug(f"State successfully saved to {STATE_FILE}")
    except Exception as e:
        logging.error(f"Error saving state to {STATE_FILE}: {e}", exc_info=True)

state = load_state_original() # Initial load into global state

def execute_trade_original(symbol: str, side: OrderSide, qty: float = None, notional: float = None) -> str | None:
    """Submits a market order to Alpaca (original pattern)."""
    order_data = None; log_action = ""
    try:
        if side == OrderSide.BUY and notional is not None:
            order_data = MarketOrderRequest(symbol=symbol, notional=notional, side=side, time_in_force=TimeInForce.DAY)
            log_action = f"BUY {symbol} (Notional: ${notional})"
        elif side == OrderSide.SELL and qty is not None and qty > 0 : # Qty must be positive
            order_data = MarketOrderRequest(symbol=symbol, qty=abs(float(qty)), side=side, time_in_force=TimeInForce.DAY)
            log_action = f"SELL {symbol} (Qty: {abs(float(qty))})"
<<<<<<< HEAD
        # Original script's SELL with notional implies shorting, new strategy is long-only for entries.
        # This function will now primarily be used for BUY entries and SELL (to close long) exits.
        else:
            raise ValueError(f"Invalid parameters for execute_trade: side={side}, qty={qty}, notional={notional}")
        
=======
        elif side == OrderSide.SELL and notional is not None:
            order_data = MarketOrderRequest(symbol=symbol, notional=notional, side=side, time_in_force=TimeInForce.DAY)
            log_action = f"SELL {symbol} (Notional: ${notional})"
        else:
            raise ValueError("Invalid parameters for execute_trade: Need (notional for BUY) or (qty/notional for SELL).")
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24
        logging.info(f"Attempting order: {log_action}")
        trade_order = rest_client.submit_order(order_data=order_data)
        logging.info(f"Order submitted for {log_action}. Order ID: {trade_order.id}")
        return str(trade_order.id)
    except APIError as e: logging.error(f"Alpaca API error submitting order for {log_action}: {e}"); return None # No raise, return None
    except ValueError as e: logging.error(f"Value error preparing order for {log_action}: {e}"); return None
    except Exception as e: logging.error(f"Generic error submitting order for {log_action}: {e}", exc_info=True); return None


def get_active_assets_original() -> list[str]:
    """ Fetches and caches active, tradable US equities (original pattern). """
    global active_assets_cache, last_assets_fetch_time
    today = date.today()
    if last_assets_fetch_time == today and active_assets_cache:
        logging.debug(f"Using cached active asset list for {today}.")
        return active_assets_cache

    logging.info("Refreshing active NYSE & NASDAQ asset list...")
    all_assets_list: list[Asset] = []
    # Original script fetched per exchange, this can be simplified if not strictly needed
    try:
        asset_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
        # Fetching all active US equities, then filtering locally if specific exchanges are desired.
        # Or, if original per-exchange fetch is mandatory:
        # for exchange_enum in [AssetExchange.NYSE, AssetExchange.NASDAQ]:
        #     asset_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE, exchange=exchange_enum)
        #     all_assets_list.extend(rest_client.get_all_assets(asset_params))
        all_assets_list = rest_client.get_all_assets(asset_params)
        
        tradable_symbols = {
            a.symbol for a in all_assets_list 
            if a.tradable and '.' not in a.symbol and '/' not in a.symbol and
            (a.exchange in [AssetExchange.NYSE, AssetExchange.NASDAQ, AssetExchange.ARCA, AssetExchange.BATS] or str(a.exchange) in ["NYSE", "NASDAQ", "ARCA", "BATS"]) # Cover enums and strings
        }
        active_symbols = sorted(list(tradable_symbols))
<<<<<<< HEAD
        logging.info(f"Fetched {len(all_assets_list)} assets, filtered to {len(active_symbols)} tradable symbols on major exchanges.")
        active_assets_cache = active_symbols
        last_assets_fetch_time = today
=======
        count_after = len(active_symbols)
        logging.info(f"Fetched total {count_before} assets across specified exchanges, filtered to {count_after} unique tradable US Equity symbols.")

        if active_symbols:
            active_assets_cache = active_symbols
            last_assets_fetch_time = today
        else:
            logging.warning("No active tradable symbols found.")
            return active_assets_cache if active_assets_cache else []

    except APIError as e: logging.error(f"API error fetching asset list: {e}. Returning cache."); return active_assets_cache or []
    except AttributeError as e: logging.critical(f"AttributeError fetching asset list: {e}. Method missing?", exc_info=True); return active_assets_cache or []
    except Exception as e: logging.error(f"Error fetching asset list: {e}. Returning cache.", exc_info=True); return active_assets_cache or []

    return active_symbols


def get_latest_trades_batched(symbols: list[str], batch_size: int = 500) -> dict[str, float]:
    """Fetches latest trades in batches."""
    latest_prices = {}; symbols_processed = 0
    logging.info(f"Fetching latest trades for {len(symbols)} symbols in batches of {batch_size}...")
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=batch, feed='sip')
            latest_trades = data_client.get_stock_latest_trade(req)
            for symbol, trade in latest_trades.items():
                if trade.price > 0: latest_prices[symbol] = trade.price
            symbols_processed += len(batch)
            logging.debug(f"Fetched latest trades batch {i//batch_size + 1}, processed {symbols_processed}/{len(symbols)}. Got {len(latest_trades)} prices.")
            time.sleep(0.1)
        except APIError as e:
            if e.status_code == 429: logging.warning(f"Rate limit hit fetching latest trades batch (start index {i}). {e}")
            else: logging.error(f"API error fetching latest trades batch (start index {i}): {e}")
        except Exception as e: logging.error(f"Error fetching latest trades batch (start index {i}): {e}", exc_info=True)
    logging.info(f"Finished fetching latest trades. Total valid prices retrieved: {len(latest_prices)}")
    return latest_prices

def get_bars_batched(symbols: list[str], timeframe: TimeFrame, start: datetime, end: datetime, batch_size: int = 200) -> dict:
    """Fetches historical bars in batches."""
    all_bars_data = {}; symbols_processed = 0
    logging.info(f"Fetching {timeframe.value} bars for {len(symbols)} symbols from {start.isoformat()} to {end.isoformat()} in batches of {batch_size}...")
    start = start.astimezone(timezone.utc); end = end.astimezone(timezone.utc)
    start_iso = start.isoformat(); end_iso = end.isoformat()
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        if not batch: continue
        try:
            request_params = StockBarsRequest(symbol_or_symbols=batch, timeframe=timeframe, start=start_iso, end=end_iso, adjustment='raw', feed='sip')
            barset = data_client.get_stock_bars(request_params)
            all_bars_data.update(barset.data)
            symbols_processed += len(batch)
            logging.debug(f"Fetched bars batch {i//batch_size + 1}, processed {symbols_processed}/{len(symbols)}. Got data for {len(barset.data)}.")
            time.sleep(0.1)
        except APIError as e:
            if e.status_code == 429: logging.warning(f"Rate limit hit fetching bars batch (start index {i}). {e}")
            elif e.status_code == 422: logging.warning(f"Unprocessable entity (422) fetching bars batch (start index {i}). {e}")
            else: logging.error(f"API error fetching bars batch (start index {i}): {e}")
        except Exception as e: logging.error(f"Error fetching bars batch (start index {i}): {e}", exc_info=True)
    logging.info(f"Finished fetching bars. Total symbols with data: {len(all_bars_data)}")
    return all_bars_data


def scan_market_30min_change() -> list[dict]:
    """Scans NYSE/NASDAQ assets, filters, ranks by 30-min % change."""
    logging.info("--- Starting Market Scan (30-Min Change, Price/Volume Filters) ---")
    scan_start_time = time.time()

    all_active_symbols = get_active_assets()
    if not all_active_symbols: logging.warning("Scan aborted: No active symbols found."); return []
    logging.info(f"Starting scan with {len(all_active_symbols)} active symbols.")

    latest_prices = get_latest_trades_batched(all_active_symbols)
    if not latest_prices: logging.warning("Scan aborted: Could not fetch any latest prices."); return []

    price_filtered_symbols = { s: p for s, p in latest_prices.items() if MIN_PRICE_FILTER <= p <= MAX_PRICE_FILTER }
    logging.info(f"Price Filter Applied: {len(price_filtered_symbols)} symbols remaining.")
    if not price_filtered_symbols: logging.warning("Scan aborted: No symbols passed price filter."); return []

    now_utc = datetime.now(timezone.utc)
    volume_start_time = now_utc - timedelta(minutes=VOLUME_LOOKBACK_MINUTES + 5); volume_end_time = now_utc
    volume_bars_data = get_bars_batched(list(price_filtered_symbols.keys()), TimeFrame.Minute, volume_start_time, volume_end_time)

    volume_filtered_symbols_set = set(); volume_data = {}
    volume_calc_start_time = now_utc - timedelta(minutes=VOLUME_LOOKBACK_MINUTES)
    for symbol, bars in volume_bars_data.items():
        recent_bars = [b for b in bars if b.timestamp >= volume_calc_start_time]
        if not recent_bars: continue
        total_volume = sum(b.volume for b in recent_bars)
        volume_data[symbol] = total_volume
        if total_volume >= MIN_VOLUME_FILTER: volume_filtered_symbols_set.add(symbol)

    logging.info(f"Volume Filter Applied: {len(volume_filtered_symbols_set)} symbols remaining.")
    if not volume_filtered_symbols_set: logging.warning("Scan aborted: No symbols passed volume filter."); return []

    ranking_start_time = now_utc - timedelta(minutes=RANKING_LOOKBACK_MINUTES + 5); ranking_end_time = now_utc
    ranking_bars_data = get_bars_batched(list(volume_filtered_symbols_set), TimeFrame.Minute, ranking_start_time, ranking_end_time)

    ranks = []; target_time_30_min_ago = now_utc - timedelta(minutes=RANKING_LOOKBACK_MINUTES)
    for symbol in volume_filtered_symbols_set:
        current_price = price_filtered_symbols.get(symbol)
        if not current_price: continue
        bars = ranking_bars_data.get(symbol, [])
        if not bars: continue
        price_30_min_ago = None; bars.sort(key=lambda b: b.timestamp)
        ref_bar = next((b for b in reversed(bars) if b.timestamp <= target_time_30_min_ago), None)
        if ref_bar: price_30_min_ago = ref_bar.close
        else: continue
        try: price_30_min_ago = float(price_30_min_ago); current_price = float(current_price)
        except: logging.warning(f"Price conversion error for {symbol}. Skipping."); continue
        if price_30_min_ago > 0 and current_price > 0:
            try:
                percent_change = ((current_price - price_30_min_ago) / price_30_min_ago) * 100
                if math.isnan(percent_change) or math.isinf(percent_change): continue
                if -99 < percent_change < 500:
                     ranks.append({'symbol': symbol, 'change': percent_change, 'price': current_price, 'volume_15m': volume_data.get(symbol, 0)})
            except ZeroDivisionError: logging.warning(f"ZeroDivisionError for {symbol}.")
            except Exception as e: logging.error(f"Ranking calc error for {symbol}: {e}")

    ranks.sort(key=lambda x: x['change'], reverse=True)
    scan_duration = time.time() - scan_start_time
    logging.info(f"Ranking complete. Total ranked: {len(ranks)}. Duration: {scan_duration:.2f} sec.")
    top_performers_log = [f"{r['symbol']}({r['change']:.2f}%)" for r in ranks[:10]]
    logging.info(f"Top {min(10, len(ranks))} performers: {', '.join(top_performers_log)}")
    return ranks


# === Core Strategy Logic (Synchronous Function - Returns Actions) === #
def run_strategy_cycle():
    """Runs one cycle of the trading strategy."""
    logging.info("--- Starting Strategy Cycle (Top3_v0) ---")
    global state; actions_by_user = {}

    try: # Check Market Status
        clock = rest_client.get_clock(); now_utc = datetime.now(timezone.utc); allow_run = False
        status_log = f"Clock: open={clock.is_open}, next_open={clock.next_open.isoformat()}, next_close={clock.next_close.isoformat()}"
        if clock.is_open: allow_run = True; logging.info(f"Market open. {status_log}")
        elif EXTENDED_HOURS_TRADING: allow_run = True; logging.info(f"Market closed, EXTENDED_HOURS enabled. {status_log}")
        else: logging.info(f"Market closed, EXTENDED_HOURS disabled. Skipping. {status_log}")
        if not allow_run: return actions_by_user
    except Exception as e: logging.error(f"Market status check failed: {e}", exc_info=True); return actions_by_user

    ranks = scan_market_30min_change()  # Scan Market
    if not ranks:
        logging.warning("Scan returned no ranks. Aborting cycle.")
        return actions_by_user

    long_candidates = [r for r in ranks if r['change'] > 0][:MAX_LONG_POSITIONS]
    short_candidates = sorted(
        [r for r in ranks if r['change'] < 0], key=lambda x: x['change']
    )[:MAX_SHORT_POSITIONS]

    current_top_performers = long_candidates + short_candidates
    current_top_symbols = {r['symbol'] for r in current_top_performers}
    top_log_str = [f"{r['symbol']}({r['change']:.2f}%)" for r in current_top_performers]
    logging.info(
        f"Current Top {len(current_top_symbols)} (30min %): {', '.join(top_log_str)}"
    )

    enabled_users = list(uid for uid, enabled in state.get("strategy_enabled", {}).items() if enabled)
    if not enabled_users: logging.info("No users enabled."); return actions_by_user
    logging.info(f"Processing for {len(enabled_users)} enabled user(s): {enabled_users}")

    try: # Get Positions
        all_positions = rest_client.get_all_positions()
        positions_map = {p.symbol: p for p in all_positions}
        logging.debug(f"Fetched {len(positions_map)} open Alpaca positions.")
    except Exception as e: logging.error(f"Failed fetching positions: {e}", exc_info=True); return actions_by_user

    for user_id in enabled_users: # Process Each User
        user_actions = []
        try:
            logging.debug(f"Processing User ID: {user_id}")
            open_trades_state = {
                t['symbol']: t
                for t in state.get("trades", [])
                if t.get("user") == user_id and t.get("status") == "open"
            }
            held_symbols_in_state = set(open_trades_state.keys())
            held_long = {s for s, t in open_trades_state.items() if t.get("side") != "short"}
            held_short = {s for s, t in open_trades_state.items() if t.get("side") == "short"}
            logging.debug(
                f"User {user_id} Held (long:{len(held_long)} short:{len(held_short)}): {held_symbols_in_state}"
            )

            # Sync State
            mismatched = {s for s in held_symbols_in_state if s not in positions_map}
            if mismatched:
                logging.warning(f"User {user_id} - Sync Discrepancy (State shows open, Alpaca doesn't): {mismatched}")
                indices = [i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and t.get("symbol") in mismatched and t.get("status") == "open"]
                for idx in indices: state["trades"][idx].update({"status": "closed_orphan", "exit_time": datetime.now(timezone.utc).isoformat(), "exit_reason": "Orphan"})
                open_trades_state = {k: v for k, v in open_trades_state.items() if k not in mismatched}
                held_symbols_in_state = set(open_trades_state.keys())

            # Check Exits
            symbols_to_sell = set(); sell_reasons = {}; now_dt = datetime.now(timezone.utc)
            for symbol in held_symbols_in_state:
                trade_info = open_trades_state.get(symbol); exit_reason = None
                if not trade_info: continue
                if symbol not in current_top_symbols: exit_reason = "Displaced (30min)"
                elif trade_info.get('executed_at'):
                    try:
                        if (now_dt - isoparse(trade_info['executed_at'])) >= timedelta(minutes=HOLD_TIME_LIMIT_MINUTES): exit_reason = f">{HOLD_TIME_LIMIT_MINUTES}min Hold"
                    except ValueError: logging.warning(f"Bad entry time format for {symbol}")
                if exit_reason: logging.info(f"User {user_id} - Exit Trigger: {symbol} ({exit_reason})"); symbols_to_sell.add(symbol); sell_reasons[symbol] = exit_reason

            # Execute Sells
            if symbols_to_sell:
                logging.info(f"User {user_id} - Selling: {symbols_to_sell}")
                sell_time = datetime.now(timezone.utc)
                for symbol in symbols_to_sell:
                    trade_info = open_trades_state.get(symbol)
                    position = positions_map.get(symbol)
                    if not trade_info or not position:
                        logging.warning(f"Missing info for sell {symbol}")
                        continue
                    try:
                        qty = abs(float(position.qty))
                        if qty > 0:
                            exit_side = OrderSide.BUY if trade_info.get("side") == "short" else OrderSide.SELL
                            exit_id = execute_trade(symbol=symbol, side=exit_side, qty=qty)
                            time.sleep(EXIT_PRICE_FETCH_DELAY_SECONDS); exit_price = None
                            try: req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed='sip'); snap = data_client.get_stock_latest_trade(req); exit_price = snap[symbol].price if symbol in snap and snap[symbol].price > 0 else trade_info.get('entry_price')
                            except Exception as e: logging.error(f"Exit price fetch failed for {symbol}: {e}"); exit_price = trade_info.get('entry_price')
                            try: exit_price = float(exit_price) if exit_price is not None else float(trade_info.get('entry_price', 0))
                            except: exit_price = float(trade_info.get('entry_price', 0))
                            idx = next((i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and str(t.get("trade_id")) == str(trade_info.get("trade_id")) and t.get("status") == "open"), -1)
                            profit, pct = 0.0, 0.0
                            if idx != -1:
                                entry_px = float(state["trades"][idx].get("entry_price", 0))
                                state["trades"][idx].update({"status": "closed", "exit_price": exit_price, "exit_time": sell_time.isoformat(), "exit_id": exit_id, "exit_reason": sell_reasons.get(symbol)})
                                if entry_px > 0 and exit_price > 0:
                                    if trade_info.get("side") == "short":
                                        profit = (entry_px - exit_price) * qty
                                        pct = ((entry_px - exit_price) / entry_px) * 100
                                    else:
                                        profit = (exit_price - entry_px) * qty
                                        pct = ((exit_price - entry_px) / entry_px) * 100
                                pnl = {"user": user_id, "symbol": symbol, "profit": round(profit, 2), "pct_change": round(pct, 2), "time": sell_time.isoformat(), "entry_price": entry_px, "exit_price": exit_price, "entry_trade_id": trade_info.get("trade_id"), "exit_trade_id": exit_id, "exit_reason": sell_reasons.get(symbol)}
                                state.setdefault("pnl", []).append(pnl)
                                logging.info(
                                    f"User {user_id} - Closed {symbol}: PnL ${profit:.2f} ({pct:.2f}%) Reason: {sell_reasons.get(symbol)}"
                                )
                                user_actions.append({"action": "SELL", "symbol": symbol, "reason": sell_reasons.get(symbol), "price": exit_price, "pnl": round(profit, 2), "pct": round(pct, 2)})
                            else: logging.error(f"State update failed for closed {symbol}")
                        else: logging.warning(f"Alpaca position qty for {symbol} was 0. Marking closed."); idx = next((i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and str(t.get("trade_id")) == str(trade_info.get("trade_id")) and t.get("status") == "open"), -1); state["trades"][idx].update({"status": "closed_zero_qty", "exit_reason": "Zero quantity found"}) if idx != -1 else None
                    except APIError as e: logging.error(f"API error selling {symbol}: {e}"); idx = next((i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and str(t.get("trade_id")) == str(trade_info.get("trade_id")) and t.get("status") == "open"), -1); state["trades"][idx].update({"status": "closed_orphan", "exit_reason": f"Sell failed ({e.status_code})"}) if idx != -1 and e.status_code == 404 else None
                    except Exception as e: logging.error(f"Generic error closing {symbol}: {e}", exc_info=True)

            # Execute Buys (Long & Short)
            open_trades_state = {
                t['symbol']: t
                for t in state.get("trades", [])
                if t.get("user") == user_id and t.get("status") == "open"
            }
            held_long = {s for s, t in open_trades_state.items() if t.get("side") != "short"}
            held_short = {s for s, t in open_trades_state.items() if t.get("side") == "short"}
            num_can_long = MAX_LONG_POSITIONS - len(held_long)
            num_can_short = MAX_SHORT_POSITIONS - len(held_short)

            symbols_long = {r['symbol'] for r in long_candidates} - held_long
            symbols_short = {r['symbol'] for r in short_candidates} - held_short

            if (symbols_long and num_can_long > 0) or (symbols_short and num_can_short > 0):
                logging.info(
                    f"User {user_id} - Evaluating entries L{num_can_long} S{num_can_short}: {symbols_long | symbols_short}"
                )
                buy_time = datetime.now(timezone.utc)

                for rank in long_candidates:
                    symbol = rank['symbol']
                    if symbol in symbols_long and num_can_long > 0:
                        if symbol in held_long:
                            continue
                        try:
                            price = rank['price']
                            if price <= 0:
                                continue
                            logging.info(f"User {user_id} - Executing BUY {symbol} @ ~${price:.2f}")
                            order_id = execute_trade(symbol=symbol, side=OrderSide.BUY, notional=TRADE_NOTIONAL_PER_STOCK)
                            new = {
                                "user": user_id,
                                "symbol": symbol,
                                "notional": TRADE_NOTIONAL_PER_STOCK,
                                "side": "long",
                                "executed_at": buy_time.isoformat(),
                                "entry_price": price,
                                "trade_id": order_id,
                                "status": "open",
                            }
                            state.setdefault("trades", []).append(new)
                            num_can_long -= 1
                            held_long.add(symbol)
                            logging.info(
                                f"User {user_id} - Entered {symbol} Order ID: {order_id}"
                            )
                            user_actions.append({"action": "BUY", "symbol": symbol, "price": price})
                        except APIError as e:
                            logging.error(f"API error buying {symbol}: {e}", exc_info=False)
                            reason = f"API Error {e.status_code}"
                            if e.status_code == 403:
                                reason = "Forbidden/Funds"
                                break
                            elif e.status_code == 422:
                                reason = "Not Tradable"
                            user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": reason})
                        except Exception as e:
                            logging.error(f"Failed buy execution for {symbol}: {e}", exc_info=True)
                            user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": "Exec Error"})

                for rank in short_candidates:
                    symbol = rank['symbol']
                    if symbol in symbols_short and num_can_short > 0:
                        if symbol in held_short:
                            continue
                        try:
                            price = rank['price']
                            if price <= 0:
                                continue
                            logging.info(f"User {user_id} - Executing SHORT {symbol} @ ~${price:.2f}")
                            order_id = execute_trade(symbol=symbol, side=OrderSide.SELL, notional=TRADE_NOTIONAL_PER_STOCK)
                            new = {
                                "user": user_id,
                                "symbol": symbol,
                                "notional": TRADE_NOTIONAL_PER_STOCK,
                                "side": "short",
                                "executed_at": buy_time.isoformat(),
                                "entry_price": price,
                                "trade_id": order_id,
                                "status": "open",
                            }
                            state.setdefault("trades", []).append(new)
                            num_can_short -= 1
                            held_short.add(symbol)
                            logging.info(
                                f"User {user_id} - Entered SHORT {symbol} Order ID: {order_id}"
                            )
                            user_actions.append({"action": "SHORT", "symbol": symbol, "price": price})
                        except APIError as e:
                            logging.error(f"API error shorting {symbol}: {e}", exc_info=False)
                            reason = f"API Error {e.status_code}"
                            if e.status_code == 403:
                                reason = "Forbidden/Funds"
                                break
                            elif e.status_code == 422:
                                reason = "Not Tradable"
                            user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": reason})
                        except Exception as e:
                            logging.error(f"Failed short execution for {symbol}: {e}", exc_info=True)
                            user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": "Exec Error"})
        except Exception as loop_err: logging.error(f"Error in user loop {user_id}: {loop_err}", exc_info=True)
        if user_actions: actions_by_user[user_id] = user_actions

    save_state(state) # Save State
    logging.info("--- Strategy Cycle Finished ---")
    return actions_by_user


# === Async Job Callback === #
async def top_3_strategy_job_callback(context: ContextTypes.DEFAULT_TYPE):
    """Async callback for JobQueue."""
    logging.info("JobQueue triggered: Running strategy cycle in thread.")
    bot = context.bot; actions_by_user = {}
    try:
        actions_by_user = await asyncio.to_thread(run_strategy_cycle)
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24
    except Exception as e:
        logging.error(f"Error fetching/filtering active assets: {e}. Using previous cache if available.", exc_info=True)
    return active_assets_cache


def fetch_current_price_adapted(symbol: str) -> float | None:
    """Fetches latest trade price using data_client and StockLatestTradeRequest."""
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed='sip')
        latest_trades = data_client.get_stock_latest_trade(req)
        trade = latest_trades.get(symbol)
        return float(trade.price) if trade and trade.price > 0 else None
    except Exception as e:
        logging.error(f"Error fetch_current_price for {symbol}: {e}")
        return None

def fetch_bars_for_atr_adapted(symbol: str, limit_bars: int = ATR_PERIOD + 50) -> list[Bar]: # Ensure return type matches usage
    """Fetches bars for ATR using data_client and StockBarsRequest."""
    end_dt = now_utc()
    # Estimate start date; go back more days to ensure enough bars for the given timeframe
    days_to_go_back = max(10, (limit_bars * ATR_TIMEFRAME_MINUTES) // (6.5 * 60) + 5) # Approx trading days needed
    start_dt = end_dt - timedelta(days=days_to_go_back)

    tf_unit = TimeFrameUnit.Minute
    tf_amount = ATR_TIMEFRAME_MINUTES
    if ATR_TIMEFRAME_MINUTES >= 1440: tf_unit=TimeFrameUnit.Day; tf_amount = ATR_TIMEFRAME_MINUTES // 1440
    elif ATR_TIMEFRAME_MINUTES >= 60: tf_unit=TimeFrameUnit.Hour; tf_amount = ATR_TIMEFRAME_MINUTES // 60

    req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame(tf_amount, tf_unit),
                           start=start_dt, end=end_dt, limit=limit_bars, feed='sip', adjustment='raw')
    try:
        barset = data_client.get_stock_bars(req)
        return barset.data.get(symbol, [])
    except Exception as e:
        logging.error(f"Error fetch_bars_for_atr for {symbol}: {e}")
        return []

def calculate_atr_from_bars(bars: list[Bar], period: int = ATR_PERIOD) -> float | None:
    """Calculates ATR from a list of Alpaca Bar objects."""
    if not bars or len(bars) < period : return None
    highs = np.array([b.high for b in bars]); lows = np.array([b.low for b in bars]); closes = np.array([b.close for b in bars])
    tr = np.zeros(len(bars))
    if len(bars) > 0: tr[0] = highs[0] - lows[0] # Max with 0 to avoid negative TR if H < L (bad data)
    for i in range(1, len(bars)):
        hl = highs[i] - lows[i]
        h_cp = abs(highs[i] - closes[i-1]) if closes[i-1] is not None else hl
        l_cp = abs(lows[i] - closes[i-1]) if closes[i-1] is not None else hl
        tr[i] = max(hl, h_cp, l_cp)
    if len(tr) < period: return None
    atr_val = np.mean(tr[-period:]) # SMA of TR
    return float(atr_val) if atr_val > 0 else None


def fetch_top_volume_gainers_simulated_adapted() -> list[dict]:
    """Simulates fetching top gainers with 'symbol', 'price', 'relative_volume'."""
    logging.debug(f"Simulating fetch from GAINERS_ENDPOINT_URL: {GAINERS_ENDPOINT_URL}")
    # This would be an actual HTTP request in a real bot
    # try:
    #     response = requests.get(GAINERS_ENDPOINT_URL, timeout=10)
    #     response.raise_for_status()
    #     gainers_data = response.json() # Expects list of {'symbol': str, 'price': float, 'relative_volume': float}
    #     gainers_data.sort(key=lambda x: x.get('relative_volume', 0), reverse=True) # Sort by rel_vol
    #     return gainers_data
    # except Exception as e:
    #     logging.error(f"Could not fetch actual gainers: {e}")
    #     return []
    simulated_data = [
        {'symbol': 'AAPL', 'price': 170.0, 'relative_volume': 5.2}, {'symbol': 'MSFT', 'price': 330.0, 'relative_volume': 4.1},
        {'symbol': 'NVDA', 'price': 750.0, 'relative_volume': 7.5}, {'symbol': 'AMD',  'price': 110.0, 'relative_volume': 6.0},
        {'symbol': 'GOOG', 'price': 150.0, 'relative_volume': 3.5},
    ]
    simulated_data.sort(key=lambda x: x.get('relative_volume', 0), reverse=True)
    return simulated_data


def scan_and_filter_candidates() -> list[dict]:
    """Scans for high-volume gappers, filters, and calculates ATR."""
    logging.info("Scanning for Top-3 High-Volume Gapper candidates...")
    raw_gainers = fetch_top_volume_gainers_simulated_adapted()
    
    qualified_candidates = []
    for gainer_data in raw_gainers:
        symbol = gainer_data.get('symbol')
        price = gainer_data.get('price')
        relative_volume = gainer_data.get('relative_volume')

        if not (symbol and isinstance(price, (int,float)) and PRICE_FILTER_MIN <= price <= PRICE_FILTER_MAX and 
                isinstance(relative_volume, (int,float)) and relative_volume >= MIN_RELATIVE_VOLUME_FOR_ENTRY):
            logging.debug(f"Skipping {symbol or 'N/A'}: Fails pre-ATR filters (Price: {price}, RelVol: {relative_volume}).")
            continue

        bars = fetch_bars_for_atr_adapted(symbol) # Fetch bars for ATR
        if not bars:
            logging.warning(f"No bars for ATR for candidate {symbol}. Skipping.")
            continue
        
        current_atr = calculate_atr_from_bars(bars)
        if current_atr is None or current_atr <= 1e-6: # Check for positive ATR
            logging.warning(f"Invalid or zero ATR ({current_atr}) for candidate {symbol}. Skipping.")
            continue
        
        logging.info(f"Qualified Candidate: {symbol} (Price: {price:.2f}, RelVol: {relative_volume:.1f}, ATR: {current_atr:.4f})")
        qualified_candidates.append({
            'symbol': symbol, 
            'price': float(price), 
            'atr': float(current_atr),
            'reason': f"RelVol {relative_volume:.1f}x" # Reason for scan
        })
        # Optimization: if we have enough for MAX_POSITIONS plus a buffer, we can stop early.
        # For simplicity, we'll just take the top ones after processing all.
            
    # Already sorted by relative_volume from simulated fetch, now take top N
    final_candidates = qualified_candidates[:MAX_POSITIONS] # Take only up to MAX_POSITIONS
    logging.info(f"Final {len(final_candidates)} candidates after all filters: {[c['symbol'] for c in final_candidates]}")
    return final_candidates


# === Core Strategy Logic (Adapted for New Strategy within Original Framework) ===
def run_strategy_cycle_new_logic():
    """
    Runs one cycle of the new high-volume gapper strategy,
    fitting into the original script's synchronous, stateful structure.
    Returns actions for Telegram.
    """
    logging.info("--- Starting High-Volume Gapper Strategy Cycle ---")
    global state # Operates on the global state dictionary
    current_actions_for_user = [] # Store actions for current cycle for TG notification

    # Market Status Check
    try:
        clock = rest_client.get_clock()
        current_utc_time = now_utc()
        if not clock.is_open:
            logging.info(f"Market closed. Next open: {clock.next_open.isoformat() if clock.next_open else 'N/A'}. Skipping trading actions.")
            return {} # Return empty if market not open for trading
    except Exception as e:
        logging.error(f"Market status check failed: {e}", exc_info=True)
        return {}

    # Fetch current Alpaca positions once
    try:
        alpaca_positions_raw: list[Position] = rest_client.get_all_positions()
        current_alpaca_positions = {p.symbol: p for p in alpaca_positions_raw}
    except Exception as e:
        logging.error(f"Failed fetching current Alpaca positions: {e}. Aborting cycle.", exc_info=True)
        return {}

    # --- 1. Manage Exits ---
    # The state['trades'] is a list of dicts. We need to iterate and potentially modify/remove items.
    # It's safer to build a new list of trades to keep.
    
    trades_to_keep_in_state = []
    currently_open_in_state = [t for t in state.get('trades', []) if t.get('status') == 'open']
    
    # Get symbols for displacement check (scan once for all open positions)
    top_candidates_for_displacement_check = scan_and_filter_candidates()
    top_symbols_for_displacement = {c['symbol'] for c in top_candidates_for_displacement_check}

    for trade_entry in currently_open_in_state:
        # Make a copy to modify if details change but not exited
        current_trade_details = trade_entry.copy()
        symbol = current_trade_details['symbol']
        entry_price = float(current_trade_details['entry_price'])
        # Original script used 'executed_at', new strategy uses 'entry_time_utc'
        entry_time_iso = current_trade_details.get('executed_at', current_trade_details.get('entry_time_utc'))
        entry_time_utc = isoparse(entry_time_iso) if entry_time_iso else current_utc_time # Fallback

        initial_atr = float(current_trade_details['initial_atr_at_entry'])
        # Safely get trailing stop details, defaulting if not present (for backward compatibility)
        high_water_mark = float(current_trade_details.get('high_water_mark', entry_price))
        current_trailing_stop = float(current_trade_details.get('trailing_stop_price', 
                                      entry_price - (initial_atr * ATR_INITIAL_SL_MULTIPLIER))) # Default to initial SL if missing
        is_trailing_active = bool(current_trade_details.get('trailing_stop_active', False))
        
        live_price = fetch_current_price_adapted(symbol)
        if live_price is None:
            logging.warning(f"No live price for {symbol}, cannot manage exit. Keeping trade.")
            trades_to_keep_in_state.append(current_trade_details) # Keep if price unavailable
            continue

        exit_reason = None; trade_details_updated_this_iteration = False

        # Update High Water Mark (for longs)
        if live_price > high_water_mark:
            current_trade_details['high_water_mark'] = live_price
            high_water_mark = live_price # Update local for current logic
            trade_details_updated_this_iteration = True

        # Activate Trailing Stop
        profit_in_atr_terms = (live_price - entry_price) / initial_atr if initial_atr > 0 else 0
        if not is_trailing_active and profit_in_atr_terms >= TRAIL_ACTIVATION_PROFIT_ATR:
            current_trade_details['trailing_stop_active'] = True
            is_trailing_active = True # Update local
            trade_details_updated_this_iteration = True
            logging.info(f"Trailing stop ACTIVATED for {symbol} at price {live_price:.2f}")
        
        # Adjust Trailing Stop Price if active
        if is_trailing_active:
            new_trail_target = high_water_mark - (initial_atr * TRAIL_OFFSET_ATR)
            # Trail only moves up (or stays) for longs
            if new_trail_target > current_trailing_stop:
                current_trade_details['trailing_stop_price'] = new_trail_target
                current_trailing_stop = new_trail_target # Update local
                trade_details_updated_this_iteration = True
                logging.info(f"Trailing stop for {symbol} ADJUSTED to: {current_trailing_stop:.2f}")
        
        # Check Exits (Priority: Trail, Initial SL, Timeout, Displacement)
        if is_trailing_active and live_price <= current_trailing_stop:
            exit_reason = f"Trailing SL Hit ({live_price:.2f} <= {current_trailing_stop:.2f})"
        if not exit_reason:
            initial_sl_target = entry_price - (initial_atr * ATR_INITIAL_SL_MULTIPLIER)
            if live_price <= initial_sl_target:
                exit_reason = f"Initial SL Hit ({live_price:.2f} <= {initial_sl_target:.2f})"
        if not exit_reason and (current_utc_time - entry_time_utc).total_seconds() / 60 >= HOLD_TIME_LIMIT_MINUTES:
            exit_reason = f"Timeout >{HOLD_TIME_LIMIT_MINUTES}min"
        if not exit_reason and symbol not in top_symbols_for_displacement:
            exit_reason = "Displaced from Top-3 Candidates"

        if exit_reason:
            logging.info(f"Exit triggered for {symbol}: {exit_reason}")
            position_to_close = current_alpaca_positions.get(symbol)
            qty_to_close = float(position_to_close.qty) if position_to_close and hasattr(position_to_close, 'qty') else 0
            
            if qty_to_close > 0: # Assuming long position
                try:
<<<<<<< HEAD
                    # Using original script's execute_trade for selling
                    exit_order_id = execute_trade_original(symbol=symbol, side=OrderSide.SELL, qty=qty_to_close)
                    if exit_order_id:
                        logging.info(f"SELL order to close {symbol} (Qty: {qty_to_close}) submitted. Reason: {exit_reason}. Order ID: {exit_order_id}")
                        # Record PnL (approximate, actual fill might vary)
                        pnl_approx = (live_price - entry_price) * qty_to_close
                        current_actions_for_user.append({
                            "action": "SELL_MODIFIED", "symbol": symbol, "reason": exit_reason, 
                            "price": live_price, "pnl": round(pnl_approx, 2)
                        })
                        # Mark as closed in the main state.trades list later
                        current_trade_details['status'] = 'closed'
                        current_trade_details['exit_reason'] = exit_reason
                        current_trade_details['exit_price'] = live_price # Approx
                        current_trade_details['exit_time_utc'] = current_utc_time.isoformat()
                        current_trade_details['exit_order_id'] = exit_order_id
                        state['pnl'].append({ # Add to global PNL list as per original structure
                            'user': current_trade_details.get('user', 'BOT_USER'), # Use original user or default
                            'symbol': symbol, 'profit': round(pnl_approx, 2), 
                            'entry_price': entry_price, 'exit_price': live_price, 'time': current_utc_time.isoformat(),
                            'exit_reason': exit_reason, 'entry_trade_id': current_trade_details.get('trade_id'),
                            'exit_trade_id': exit_order_id
                        })
                        # This trade is now closed, so it won't be added to trades_to_keep_in_state
                    else:
                        logging.error(f"Failed to submit close order for {symbol}. Keeping in state.")
                        trades_to_keep_in_state.append(current_trade_details) # Keep if close failed
                except Exception as e_close:
                    logging.error(f"Error executing close order for {symbol}: {e_close}")
                    trades_to_keep_in_state.append(current_trade_details) # Keep in state
            elif position_to_close is None and symbol in current_alpaca_positions and float(current_alpaca_positions[symbol].qty) == 0 : # Alpaca says 0 qty
                 logging.warning(f"Alpaca shows 0 qty for {symbol} which was in state. Marking closed (Orphan/ZeroQty). Reason: {exit_reason}")
                 current_trade_details['status'] = 'closed_orphan_qty_zero'
                 current_trade_details['exit_reason'] = exit_reason + " (Alpaca Qty Zero)"
            elif position_to_close is None: # Not in Alpaca positions
                 logging.warning(f"{symbol} marked for exit but not found in Alpaca positions. Marking closed (Orphan). Reason: {exit_reason}")
                 current_trade_details['status'] = 'closed_orphan'
                 current_trade_details['exit_reason'] = exit_reason + " (Not in Alpaca)"

            # if trade was closed or marked orphan, it's not added to trades_to_keep_in_state
        else: # No exit reason
            if trade_details_updated_this_iteration:
                 logging.debug(f"Details updated for {symbol} (HWM/Trail). Keeping in open trades.")
            trades_to_keep_in_state.append(current_trade_details) # Keep the trade, possibly with updated details

    # Rebuild state['trades'] with non-open items and the trades_to_keep_in_state
    non_open_trades = [t for t in state.get('trades', []) if t.get('status') != 'open']
    state['trades'] = non_open_trades + trades_to_keep_in_state
=======
                    a_type = action.get("action")
                    if a_type == "BUY":
                        message = f"⬆️ **Entered {symbol}** (Top20)\nApprox Entry: ${action.get('price', 0):.2f}"
                    elif a_type == "SHORT":
                        message = f"⬇️ **Shorted {symbol}** (Top20)\nApprox Entry: ${action.get('price', 0):.2f}"
                    elif a_type == "SELL":
                        message = (
                            f"⬇️ **Exited {symbol}** ({action.get('reason', 'N/A')})\n"
                            f"Approx Exit: ${action.get('price', 0):.2f}\n"
                            f"Approx PnL: ${action.get('pnl', 0):+.2f} ({action.get('pct', 0):+.2f}%)"
                        )
                    elif a_type == "BUY_FAIL":
                        message = f"⚠️ Failed to buy {symbol}: {action.get('reason', 'Unknown')}"
                    await bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.MARKDOWN)
                    logging.info(f"Sent notification to {user_id}: {message.splitlines()[0]}")
                except telegram.error.BadRequest as tg_err:
                    if "chat not found" in str(tg_err).lower():
                        logging.error(f"Chat not found {user_id}. Disabling.")
                        async with state_lock:
                            state.setdefault("strategy_enabled", {})[user_id] = False
                            await asyncio.to_thread(save_state, state)
                    else:
                        logging.error(f"TG BadRequest {user_id}: {tg_err}")
                except telegram.error.Forbidden as tg_err:
                    logging.error(f"TG Forbidden {user_id}. Disabling.")
                    async with state_lock:
                        state.setdefault("strategy_enabled", {})[user_id] = False
                        await asyncio.to_thread(save_state, state)
                except Exception as tg_err: logging.error(f"TG send error {user_id}: {tg_err}", exc_info=True)
    else: logging.debug("No actions generated.")
    logging.debug("JobQueue callback finished.")
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24


    # --- 2. Manage Entries (New Strategy) ---
    num_currently_open = len([t for t in state['trades'] if t.get('status') == 'open'])
    
    if num_currently_open < MAX_POSITIONS:
        # Use the same candidates from displacement check if fresh enough, or re-scan.
        # For simplicity, let's use the candidates from the displacement check if available.
        entry_candidates_list = top_candidates_for_displacement_check # These already have price & ATR

        num_can_open = MAX_POSITIONS - num_currently_open
        logging.info(f"Space for {num_can_open} new LONG positions.")

        for candidate_info in entry_candidates_list:
            if num_can_open <= 0: break
            
            symbol_candidate = candidate_info['symbol']
            # Check if already holding (based on state['trades'] list)
            is_held = any(t['symbol'] == symbol_candidate and t.get('status') == 'open' for t in state['trades'])
            if is_held:
                logging.debug(f"Already holding {symbol_candidate} or entry pending. Skipping.")
                continue

            entry_price_at_scan = candidate_info['price']
            atr_at_scan = candidate_info['atr']
            entry_reason_scan = candidate_info['reason']

            logging.info(f"Attempting new LONG entry for {symbol_candidate} (Reason: {entry_reason_scan}, ScanPrice: {entry_price_at_scan:.2f}, ScanATR: {atr_at_scan:.4f})")
            try:
                # Use original script's execute_trade for BUY with notional
                entry_order_id = execute_trade_original(symbol=symbol_candidate, side=OrderSide.BUY, notional=TRADE_NOTIONAL_PER_STOCK)
                if entry_order_id:
                    logging.info(f"Submitted BUY for {symbol_candidate}, Notional: ${TRADE_NOTIONAL_PER_STOCK}, Order ID: {entry_order_id}")
                    
                    initial_sl_price_calc = entry_price_at_scan - (atr_at_scan * ATR_INITIAL_SL_MULTIPLIER)
                    
                    # Create a new trade entry dictionary (original style: list of dicts)
                    new_trade_entry = {
                        'user': system_user_id, # Bot-level trade
                        'symbol': symbol_candidate,
                        'notional': TRADE_NOTIONAL_PER_STOCK,
                        'side': 'long', # This strategy is long-only
                        'executed_at': current_utc_time.isoformat(), # Entry time
                        'entry_time_utc': current_utc_time.isoformat(), # More explicit name
                        'entry_price': entry_price_at_scan, # Price at scan time
                        'trade_id': entry_order_id, # Alpaca order ID as trade_id
                        'status': 'open',
                        # New fields for ATR trailing stop
                        'initial_atr_at_entry': atr_at_scan,
                        'high_water_mark': entry_price_at_scan, # Init HWM with entry price
                        'trailing_stop_price': initial_sl_price_calc, # Initial trail is the initial SL
                        'trailing_stop_active': False,
                        'entry_reason': entry_reason_scan
                    }
                    state['trades'].append(new_trade_entry)
                    current_actions_for_user.append({
                        "action": "BUY_NEW_MODIFIED", "symbol": symbol_candidate, 
                        "price": entry_price_at_scan, "reason": entry_reason_scan
                    })
                    num_can_open -= 1
                else:
                    logging.error(f"Entry order submission failed for {symbol_candidate} (execute_trade returned None).")
                    current_actions_for_user.append({"action": "BUY_FAIL_MODIFIED", "symbol": symbol_candidate, "reason": "Order Submit Fail"})

            except Exception as e_entry:
                logging.error(f"Failed entry execution for {symbol_candidate}: {e_entry}", exc_info=True)
                current_actions_for_user.append({"action": "BUY_FAIL_MODIFIED", "symbol": symbol_candidate, "reason": str(e_entry)})
    else:
        logging.info(f"Max positions ({MAX_POSITIONS}) reached or no new candidates. No new entries considered.")

    # --- 3. Save State ---
    save_state_original() # Saves global state
    
    # For Telegram notifications, if a user is targeted
    final_actions_map = {}
    if current_actions_for_user and notification_user_id:
        final_actions_map[notification_user_id] = current_actions_for_user
    
    logging.info("--- Modified Strategy Cycle Finished ---")
    return final_actions_map


# === Async Job Callback (from original script, calls new strategy logic) ===
async def top_3_strategy_job_callback(context: ContextTypes.DEFAULT_TYPE): # Renamed from original
    """Async callback for JobQueue, runs the new strategy logic."""
    logging.info("JobQueue triggered: Running MODIFIED (High-Volume Gapper) strategy cycle in thread.")
    bot = context.bot
    actions_map_for_tg = {}
    
    try:
        # run_strategy_cycle_modified is synchronous
        actions_map_for_tg = await asyncio.to_thread(run_strategy_cycle_new_logic)
    except Exception as e:
        logging.error(f"Exception in threaded modified strategy cycle: {e}", exc_info=True)
        # Notify admin (first enabled user, or hardcoded if available)
        admin_id_tg = next(iter(state.get("strategy_enabled", {})), None) # Get first key
        if admin_id_tg and state.get("strategy_enabled", {}).get(admin_id_tg):
            try:
                await bot.send_message(chat_id=admin_id_tg, text=f"⚠️ CRITICAL ERROR in MODIFIED strategy job: {type(e).__name__}. Check logs.")
            except Exception as notify_err_tg:
                logging.error(f"Failed to send critical error TG notification: {notify_err_tg}")

    if actions_map_for_tg:
        num_total_actions = sum(len(acts) for acts in actions_map_for_tg.values())
        logging.info(f"Processing {num_total_actions} actions for TG notification from modified strategy...")
        for user_id_tg, actions_list_tg in actions_map_for_tg.items():
            for action_tg in actions_list_tg:
                message_tg = "❓ Unknown action"
                symbol_tg = action_tg.get("symbol", "N/A")
                reason_tg = action_tg.get("reason", "N/A")
                price_tg = action_tg.get("price", 0)
                pnl_tg = action_tg.get("pnl") # Can be None

                try:
                    a_type_tg = action_tg.get("action")
                    if a_type_tg == "BUY_NEW_MODIFIED": # New action type
                        message_tg = f"⬆️ **Entered LONG {symbol_tg}** (VolGapper)\nReason: {reason_tg}\nApprox Entry: ${price_tg:.2f}"
                    elif a_type_tg == "SELL_CLOSE_MODIFIED": # New action type
                        pnl_str_tg = f"${pnl_tg:+.2f}" if pnl_tg is not None else "N/A"
                        message_tg = (
                            f"⬇️ **Exited {symbol_tg}** (Reason: {reason_tg})\n"
                            f"Approx Exit: ${price_tg:.2f}\n"
                            f"Approx PnL: {pnl_str_tg}"
                        )
                    elif a_type_tg == "BUY_FAIL_MODIFIED": # New action type
                         message_tg = f"⚠️ Failed to enter {symbol_tg}: {reason_tg}"
                    else: # Fallback for original action types if any slip through
                        message_tg = f"Original Action: {a_type_tg} for {symbol_tg}"
                    
                    await bot.send_message(chat_id=user_id_tg, text=message_tg, parse_mode=ParseMode.MARKDOWN)
                    logging.info(f"Sent TG notification to {user_id_tg}: {message_tg.splitlines()[0]}")
                except Exception as tg_err_send:
                    logging.error(f"Error sending TG notification to {user_id_tg}: {tg_err_send}", exc_info=True)
                    if isinstance(tg_err_send, (telegram.error.BadRequest, telegram.error.Forbidden)):
                        if "chat not found" in str(tg_err_send).lower() or "bot was blocked" in str(tg_err_send).lower():
                            logging.error(f"Chat not found or bot blocked for {user_id_tg}. Disabling in state.")
                            async with state_lock:
                                if user_id_tg in state.get("strategy_enabled", {}):
                                    state["strategy_enabled"][user_id_tg] = False
                                    await asyncio.to_thread(save_state_original)
    else:
        logging.debug("No actions generated by modified strategy for TG notification.")
    logging.debug("JobQueue callback for modified strategy finished.")


# === Telegram Handlers (from original script) ===
async def handle_start_top3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/start_top3 from User {user_id}")
    async with state_lock: 
        state.setdefault("strategy_enabled", {})[user_id] = True
        state.setdefault("goals", {}).setdefault(user_id, {"goal": "Trade High-Volume Gappers", "log": []}) # Updated goal
    await asyncio.to_thread(save_state_original); 
    await update.message.reply_text("✅ High-Volume Gapper Strategy Enabled for your notifications."); 
    logging.info(f"Strategy enabled for user: {user_id}")

async def handle_stop_top3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/stop_top3 from User {user_id}")
    async with state_lock: state.setdefault("strategy_enabled", {})[user_id] = False
    await asyncio.to_thread(save_state_original); 
    await update.message.reply_text("⏹️ High-Volume Gapper Strategy Disabled for your notifications."); 
    logging.info(f"Strategy disabled for user: {user_id}")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE): # Adapted for new strategy state
    user = update.effective_user; user_id_str = str(user.id)
    logging.info(f"/status from User {user_id_str} (High-Volume Gapper Bot)")
    async with state_lock:
        is_enabled_for_user = state.get("strategy_enabled", {}).get(user_id_str, False)
        open_bot_trades = [t for t in state.get("trades", []) if t.get('status') == 'open'] # Bot's trades
    
    status_msg_user = "✅ ENABLED (for your notifications)" if is_enabled_for_user else "⏹️ DISABLED (for your notifications)"
    reply_msg = f"📊 **Bot Status (High-Volume Gapper): {status_msg_user}**\n\n"
    total_unrealized_bot_pnl = 0.0

<<<<<<< HEAD
    if not open_bot_trades:
        reply_msg += "ℹ️ No open positions currently managed by the bot."
    else:
        symbols_to_fetch_price = list({t['symbol'] for t in open_bot_trades})
        current_prices_map = {}
        if symbols_to_fetch_price:
            # This is sync, called from async handler. Original script did this.
            current_prices_map = {s: fetch_current_price_adapted(s) for s in symbols_to_fetch_price}
=======
async def handle_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/pnl from User {user_id}")
    async with state_lock: state.setdefault("pnl", []); pnl_recs = [p for p in state["pnl"] if p.get("user") == user_id]
    if not pnl_recs: await update.message.reply_text("📉 No closed trades yet."); return
    profit = sum(t.get("profit", 0) for t in pnl_recs); wins = sum(1 for t in pnl_recs if t.get("profit", 0) > 0); losses = sum(1 for t in pnl_recs if t.get("profit", 0) < 0); trades = len(pnl_recs); win_rate = (wins / trades * 100) if trades > 0 else 0
    limit = 10; summary = f"📈 **Closed PnL (Last {min(limit, trades)}/{trades})**\n\n"
    for t in reversed(pnl_recs[-limit:]):
        sym = t.get('symbol', '?'); p = t.get('profit', 0); pct = t.get('pct_change', 0); r = t.get('exit_reason', 'N/A'); ts = "N/A"
        if t.get("time"):
            try:
                ts = isoparse(t["time"]).strftime('%m-%d %H:%M')
            except Exception:
                pass
        summary += f" - **{sym}**: ${p:+.2f} ({pct:+.2f}%) at {ts} _({r})_\n"
    summary += f"\n💰 **Total Realized: ${profit:.2f}** | Win Rate: {win_rate:.1f}% ({wins}W/{losses}L)"
    await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24

        reply_msg += f"📈 **Bot's Open Positions ({len(open_bot_trades)}):**\n"
        for trade_item in open_bot_trades:
            sym_item = trade_item.get("symbol", "N/A")
            entry_item = float(trade_item.get("entry_price", 0))
            notional_item = float(trade_item.get("notional", trade_item.get("notional_value", 0)))
            current_price_item = current_prices_map.get(sym_item)
            exec_at_item = trade_item.get("executed_at", trade_item.get("entry_time_utc"))
            trail_sp_item = trade_item.get("trailing_stop_price")
            trail_active_item = trade_item.get("trailing_stop_active", False)

            pnl_str_item, pct_str_item, dur_str_item = "N/A", "", "N/A"
            if current_price_item is None: pnl_str_item = "(Price N/A)"
            elif entry_item > 0 and notional_item > 0:
                try:
                    shares_item = notional_item / entry_item
                    pnl_item = (current_price_item - entry_item) * shares_item
                    pct_item = ((current_price_item - entry_item) / entry_item) * 100 if entry_item != 0 else 0
                    total_unrealized_bot_pnl += pnl_item
                    pnl_str_item = f"${pnl_item:+.2f}"; pct_str_item = f"({pct_item:+.2f}%)"
                except Exception: pnl_str_item = "(Calc Err)"
            
            if exec_at_item:
                try:
                    duration_item = now_utc() - isoparse(exec_at_item)
                    secs_item = int(duration_item.total_seconds())
                    d_i,r_i = divmod(secs_item,86400); h_i,r_i=divmod(r_i,3600); m_i,_=divmod(r_i,60)
                    dur_parts_item = [f"{x}{u}" for x,u in zip([d_i,h_i,m_i],['d','h','m']) if x > 0]
                    dur_str_item = " ".join(dur_parts_item) if dur_parts_item else "~0m"
                except Exception: dur_str_item = "(Time Err)"
            
            trail_info_item = f"TS: ${trail_sp_item:.2f}{' (A)' if trail_active_item else ''}" if trail_sp_item is not None else "TS: N/A"
            reply_msg += f" - **{sym_item}** | PnL: {pnl_str_item} {pct_str_item}\n   Entry: ${entry_item:.2f} | {trail_info_item} | Held: {dur_str_item}\n"
        
        reply_msg += f"\n💰 **Total Bot Unrealized: ${total_unrealized_bot_pnl:+.2f}**"
    
    await update.message.reply_text(reply_msg, parse_mode=ParseMode.MARKDOWN)


async def handle_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE): # Adapted
    user = update.effective_user; user_id_str = str(user.id)
    logging.info(f"/pnl from User {user_id_str} (High-Volume Gapper Bot)")
    async with state_lock:
        all_pnl_records = state.get("pnl", [])
        # Assuming PNL records from the new strategy are marked with a specific user or have a common trait
        # For simplicity, show all PNL records for now, or filter by 'BOT_USER' if that's how they are stored
        bot_pnl_records = [p for p in all_pnl_records if p.get('user') == "BOT_STRATEGY_USER" or not p.get('user')] # Or more specific filter

    if not bot_pnl_records:
        await update.message.reply_text("📉 No closed trades recorded by the bot strategy yet."); return

    total_profit_val = sum(t.get("profit", 0) for t in bot_pnl_records)
    wins_count = sum(1 for t in bot_pnl_records if t.get("profit", 0) > 0)
    losses_count = sum(1 for t in bot_pnl_records if t.get("profit", 0) < 0)
    total_trades_count = len(bot_pnl_records)
    win_rate_val = (wins_count / total_trades_count * 100) if total_trades_count > 0 else 0
    
    display_limit = 10
    summary_msg = f"📈 **Bot Realized PnL (Last {min(display_limit, total_trades_count)} of {total_trades_count} Total Strategy Trades)**\n\n"
    for pnl_entry_item in reversed(bot_pnl_records[-display_limit:]):
        sym_pnl = pnl_entry_item.get('symbol', '?'); profit_pnl = pnl_entry_item.get('profit', 0)
        entry_p_pnl = pnl_entry_item.get('entry_price', 0); exit_p_pnl = pnl_entry_item.get('exit_price', 0)
        pct_pnl = ((exit_p_pnl - entry_p_pnl) / entry_p_pnl * 100) if entry_p_pnl else 0
        reason_pnl = pnl_entry_item.get('exit_reason', 'N/A'); time_str_pnl = "N/A"
        if pnl_entry_item.get("time"):
            try:
                time_str_pnl = isoparse(pnl_entry_item["time"]).strftime('%m-%d %H:%M')
            except Exception: # It's good practice to catch specific exceptions, but Exception catches most.
                pass # If isoparse or strftime fails, time_str_pnl will retain its default "N/A"
        summary_msg += f" - **{sym_pnl}**: ${profit_pnl:+.2f} ({pct_pnl:+.2f}%) at {time_str_pnl} _({reason_pnl})_\n"
    summary_msg += f"\n💰 **Total Bot Strategy Realized: ${total_profit_val:.2f}** | Win Rate: {win_rate_val:.1f}% ({wins_count}W/{losses_count}L)"
    await update.message.reply_text(summary_msg, parse_mode=ParseMode.MARKDOWN)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE): # From original
    user = update.effective_user; msg = update.message; user_id = str(user.id); username = user.username or "N/A"
    if not user or not msg: return
    logging.info(f"📨 Msg from User {user_id} ({username}): '{msg.text}'")
    async with state_lock: known_user_check = user_id in state.get("strategy_enabled", {})
    
    # Simplified: Bot is either enabled for notifications or not by user. Core strategy runs independently.
    reply_text_options = "ℹ️ Bot active (High-Volume Gapper Strategy).\nCommands:\n`/start_top3` (Enable Notifications)\n`/stop_top3` (Disable Notifications)\n`/status` (Bot's Open Trades)\n`/pnl` (Bot's Realized PnL)"
    if known_user_check :
        await update.message.reply_text(reply_text_options, parse_mode=ParseMode.MARKDOWN)
    else: # New user interaction
        logging.info(f"New user {user_id} ({username}) interacted via message.")
        async with state_lock:
            state.setdefault("strategy_enabled", {})[user_id] = False # Default new users to notifications disabled
            state.setdefault("goals", {}).setdefault(user_id, {"goal": "Observe VolGapper Bot", "log": []})
        await asyncio.to_thread(save_state_original)
        await update.message.reply_text(f"👋 Welcome, {user.first_name}!\nThis bot trades high-volume gappers.\nUse `/start_top3` to enable notifications about its trades.", parse_mode=ParseMode.MARKDOWN)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None: # From original
    logging.error("Exception during Telegram update handling:", exc_info=context.error)
    if isinstance(context.error, telegram.error.Conflict):
        logging.critical("TELEGRAM CONFLICT ERROR: Another instance of this bot may be running!")
        if isinstance(update, Update) and update.effective_chat:
             try: await context.bot.send_message(chat_id=update.effective_chat.id, text="🚨 BOT CONFLICT DETECTED! Please ensure only one instance is running.")
             except Exception as e_conf: logging.error(f"Failed to send TG conflict warning: {e_conf}")

# === Main Application Setup (from original script) ===
def run_bot_main_combined():
    if not TELEGRAM_BOT_TOKEN:
        logging.warning("TELEGRAM_BOT_TOKEN not set. Telegram features will be disabled. Strategy will attempt to run headless.")
        # Fallback to a non-Telegram loop if desired, or exit
        # For now, let's try to run the strategy loop directly if no TG token
        logging.info("Attempting to run strategy headless due to missing Telegram token.")
        try:
            while True:
                run_strategy_cycle_new_logic() # This is synchronous
                time.sleep(JOB_INTERVAL_MINUTES * 60)
        except KeyboardInterrupt: logging.info("Headless bot stopped manually.")
        except Exception as e_headless: logging.critical(f"Critical error in headless loop: {e_headless}", exc_info=True)
        finally: 
            logging.info("Headless bot attempting final state save.")
            save_state_original()
        return

    try:
        app_tg = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        app_tg.add_handler(CommandHandler("start_top3", handle_start_top3))
        app_tg.add_handler(CommandHandler("stop_top3", handle_stop_top3))
        app_tg.add_handler(CommandHandler("status", handle_status))
        app_tg.add_handler(CommandHandler("pnl", handle_pnl))
        app_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        app_tg.add_error_handler(error_handler)

        jq_tg = app_tg.job_queue
        if jq_tg:
            # Use the new strategy callback
            jq_tg.run_repeating(top_3_strategy_job_callback, 
                                interval=timedelta(minutes=JOB_INTERVAL_MINUTES), 
                                first=timedelta(seconds=20), # Give a bit of time for init
                                name="high_volume_gapper_strategy_job_MOD") # New job name
            logging.info(f"Scheduled MODIFIED strategy job every {JOB_INTERVAL_MINUTES} minutes via Telegram JobQueue.")
        else:
            logging.error("Telegram JobQueue not available. Periodic strategy execution will not occur via JobQueue.")
            # Fallback or exit if JobQueue is essential
            return
        
        logging.info("Starting Telegram polling for High-Volume Gapper Bot...")
        app_tg.run_polling(drop_pending_updates=True)
    except Exception as e_tg_main:
        logging.critical(f"Failed running Telegram bot application: {e_tg_main}", exc_info=True)

# === Entry Point (from original script) ===
if __name__ == "__main__":
    logging.info(f"Initial state loaded. Trades: {len(state.get('trades',[]))}, PnL: {len(state.get('pnl',[]))}, Users Configured: {len(state.get('strategy_enabled',{}))}")
    try:
        run_bot_main_combined()
    except KeyboardInterrupt:
        logging.info("Bot stopped manually (KeyboardInterrupt in __main__).")
    except Exception as e_main_exc:
        logging.critical(f"Unhandled exception in __main__ execution block: {e_main_exc}", exc_info=True)
    finally:
<<<<<<< HEAD
        logging.info("Attempting final state save from __main__ finally block...")
        save_state_original(state) # Ensure global state is saved
        logging.info("-------------------- BOT END (High-Volume Gapper Strategy - MODIFIED) ----------------------")
=======
        logging.info("Attempting final state save...")
        save_state(state)
        logging.info("-------------------- BOT END ----------------------")
>>>>>>> b690d80a4280774987762d719d92e88d5fe6da24
