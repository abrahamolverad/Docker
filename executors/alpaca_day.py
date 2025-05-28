# Genie_stocks_Top3_v0.py - Top 3 Strategy (30-Min Change, Price/Volume Filters)
# - Scans NYSE/NASDAQ based on 30-min % change.
# - Filters by price ($1-$50) and 15-min volume (>300k).
# - Uses previous close for initial price reference if needed.
# - Includes extended hours support for data fetching.
# - NOTE: Requires Alpaca SIP data plan for full universe coverage.
# - NOTE: High API usage - adjust JOB_INTERVAL_MINUTES if hitting rate limits.

import asyncio
import os
import json
import logging
import sys
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import isoparse
from collections import deque
from dotenv import load_dotenv
from uuid import UUID
import statistics
import time
import math # For checking NaN

# --- Alpaca Imports (Using alpaca-py V2) ---
from alpaca.trading.client import TradingClient as REST
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest # Use this request object
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange
from alpaca.trading.models import Asset # For type hinting
from alpaca.common.exceptions import APIError
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.models import BarSet # For type hinting

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
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TOP3_Stock_TELEGRAM_BOT_TOKEN")

# --- Basic Sanity Checks ---
if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("ERROR: Alpaca API Key/Secret not found in environment variables.", file=sys.stderr)
    sys.exit(1)
if not TELEGRAM_BOT_TOKEN:
    print("ERROR: TOP3_Stock_TELEGRAM_BOT_TOKEN not found in environment variables.", file=sys.stderr)
    sys.exit(1)

PAPER_TRADING = True
# --- State and Log Files ---
STATE_FILE = "genie_stocks_top3_v0_state.json"
LOG_FILE = "genie_stocks_top3_v0.log"

# --- Strategy Parameters ---
TRADE_NOTIONAL_PER_STOCK = 5000
JOB_INTERVAL_MINUTES = 2
HOLD_TIME_LIMIT_MINUTES = 30
EXIT_PRICE_FETCH_DELAY_SECONDS = 1.5
MAX_OPEN_POSITIONS = 3
EXTENDED_HOURS_TRADING = True

# --- Filtering Parameters ---
MIN_PRICE_FILTER = 1.0
MAX_PRICE_FILTER = 50.0
MIN_VOLUME_FILTER = 300000
VOLUME_LOOKBACK_MINUTES = 15
RANKING_LOOKBACK_MINUTES = 30

# === Setup Logging === #
logging.basicConfig(
    level=logging.DEBUG, # Keep DEBUG for now
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    encoding='utf-8'
)
logging.info(f"-------------------- BOT START (Top3_v0 - 30min Change) --------------------")
logging.info(f"PAPER_TRADING: {PAPER_TRADING}")
logging.info(f"Trade Notional: ${TRADE_NOTIONAL_PER_STOCK}, Max Positions: {MAX_OPEN_POSITIONS}, Hold Limit: {HOLD_TIME_LIMIT_MINUTES} min")
logging.info(f"Job Interval: {JOB_INTERVAL_MINUTES} min, Extended Hours Data: {EXTENDED_HOURS_TRADING}")
logging.info(f"Filters: Price ${MIN_PRICE_FILTER}-${MAX_PRICE_FILTER}, Min Volume ({VOLUME_LOOKBACK_MINUTES}min): {MIN_VOLUME_FILTER}, Ranking Lookback: {RANKING_LOOKBACK_MINUTES}min")
logging.warning("Ensure you have an adequate Alpaca data plan (SIP recommended) for full NYSE/NASDAQ scanning.")


# === Global State & Data Structures === #
state = {}
active_assets_cache = []
last_assets_fetch_time = None
state_lock = asyncio.Lock()

# === Initialize Alpaca V2 Clients === #
try:
    rest_client = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=PAPER_TRADING)
    data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    account_info = rest_client.get_account()
    logging.info(f"Alpaca REST client initialized. Account Status: {account_info.status}, Buying Power: {account_info.buying_power}")
    logging.debug(f"Type of rest_client after init: {type(rest_client)}")
except APIError as e:
    logging.critical(f"Failed to initialize Alpaca clients or connect: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logging.critical(f"An unexpected error occurred during Alpaca client initialization: {e}", exc_info=True)
    sys.exit(1)

# === Persistent State Load/Save (Synchronous) === #
def load_state():
    """Loads state from JSON file, ensuring default structure."""
    default_state = {"trades": [], "pnl": [], "strategy_enabled": {}, "goals": {}}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding='utf-8') as f:
                state_data = json.load(f)
            for key, default_value in default_state.items():
                state_data.setdefault(key, default_value)
            logging.info(f"Successfully loaded state from {STATE_FILE}")
            return state_data
        except json.JSONDecodeError:
            logging.error(f"Error decoding state JSON from {STATE_FILE}. Starting with default state.", exc_info=True)
        except Exception as e:
             logging.error(f"Error loading state file {STATE_FILE}: {e}. Starting with default state.", exc_info=True)
    else:
        logging.warning(f"State file {STATE_FILE} not found. Starting with default state.")
    return default_state

def save_state(state_to_save):
    """Saves the current state to a JSON file."""
    try:
        state_copy = json.loads(json.dumps(state_to_save, default=str))
        with open(STATE_FILE, "w", encoding='utf-8') as f:
            json.dump(state_copy, f, indent=4)
        logging.debug(f"State successfully saved to {STATE_FILE}")
    except Exception as e:
        logging.error(f"Error saving state to {STATE_FILE}: {e}", exc_info=True)

state = load_state()

# === Alpaca Trade Execution (Synchronous) === #
def execute_trade(symbol: str, side: OrderSide, qty: float = None, notional: float = None):
    """Submits a market order to Alpaca."""
    order_data = None; log_action = ""
    try:
        if side == OrderSide.BUY and notional is not None:
            order_data = MarketOrderRequest(symbol=symbol, notional=notional, side=side, time_in_force=TimeInForce.DAY)
            log_action = f"BUY {symbol} (Notional: ${notional})"
        elif side == OrderSide.SELL and qty is not None:
            order_data = MarketOrderRequest(symbol=symbol, qty=abs(float(qty)), side=side, time_in_force=TimeInForce.DAY)
            log_action = f"SELL {symbol} (Qty: {abs(float(qty))})"
        else: raise ValueError("Invalid parameters for execute_trade: Need (notional for BUY) or (qty for SELL).")
        logging.info(f"Attempting order: {log_action}")
        trade_order = rest_client.submit_order(order_data=order_data)
        logging.info(f"Order submitted for {log_action}. Order ID: {trade_order.id}")
        return str(trade_order.id)
    except APIError as e: logging.error(f"Alpaca API error submitting order for {log_action}: {e} (Status: {e.status_code}, Message: {e.message})"); raise
    except ValueError as e: logging.error(f"Value error preparing order for {log_action}: {e}"); raise
    except Exception as e: logging.error(f"Generic error submitting order for {log_action}: {e}", exc_info=True); raise

# === Market Data Fetching & Processing (Synchronous) === #

def get_active_assets() -> list[str]:
    """
    Fetches and caches the list of active, tradable US equities on NYSE/NASDAQ.
    Refreshes cache daily. Makes separate calls for each exchange.
    Uses get_all_assets based on successful test run.
    """
    global active_assets_cache, last_assets_fetch_time
    today = date.today()

    if last_assets_fetch_time == today and active_assets_cache:
        logging.debug(f"Using cached asset list for {today}.")
        return active_assets_cache

    logging.info("Fetching active NYSE & NASDAQ asset list...")
    all_assets: list[Asset] = []
    exchanges_to_fetch = [AssetExchange.NYSE, AssetExchange.NASDAQ]

    try:
        logging.debug(f"Type of rest_client before get_all_assets loop: {type(rest_client)}")
        for exchange in exchanges_to_fetch:
            logging.debug(f"Fetching assets for exchange: {exchange.value} using get_all_assets")
            # --- Use get_all_assets and pass the request object ---
            asset_params = GetAssetsRequest(
                asset_class=AssetClass.US_EQUITY,
                status=AssetStatus.ACTIVE,
                exchange=exchange
            )
            # Use the method confirmed working in the test script
            assets_on_exchange = rest_client.get_all_assets(asset_params)
            # --- End Update ---
            all_assets.extend(assets_on_exchange)
            logging.debug(f"Fetched {len(assets_on_exchange)} assets from {exchange.value}.")
            # time.sleep(0.1) # Optional delay

        count_before = len(all_assets)
        tradable_symbols = set()
        for a in all_assets:
            if a.tradable and a.asset_class == AssetClass.US_EQUITY and '.' not in a.symbol and '/' not in a.symbol:
                tradable_symbols.add(a.symbol)

        active_symbols = sorted(list(tradable_symbols))
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

    ranks = scan_market_30min_change() # Scan Market
    if not ranks: logging.warning("Scan returned no ranks. Aborting cycle."); return actions_by_user

    current_top_performers = ranks[:MAX_OPEN_POSITIONS]
    current_top_symbols = {r['symbol'] for r in current_top_performers}
    top_log_str = [f"{r['symbol']}({r['change']:.2f}%)" for r in current_top_performers]
    logging.info(f"Current Top {MAX_OPEN_POSITIONS} (30min %): {', '.join(top_log_str)}")

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
            open_trades_state = {t['symbol']: t for t in state.get("trades", []) if t.get("user") == user_id and t.get("status") == "open"}
            held_symbols_in_state = set(open_trades_state.keys())
            logging.debug(f"User {user_id} Held (state): {held_symbols_in_state}")

            # Sync State
            mismatched = {s for s in held_symbols_in_state if s not in positions_map}
            if mismatched:
                logging.warning(f"User {user_id} - Sync Discrepancy (State shows open, Alpaca doesn't): {mismatched}")
                indices = [i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and t.get("symbol") in mismatched and t.get("status") == "open"]
                for idx in indices: state["trades"][idx].update({"status": "closed_orphan", "exit_time": datetime.now(timezone.utc).isoformat(), "exit_reason": "Orphan"})
                open_trades_in_state = {k: v for k, v in open_trades_in_state.items() if k not in mismatched}
                held_symbols_in_state = set(open_trades_in_state.keys())

            # Check Exits
            symbols_to_sell = set(); sell_reasons = {}; now_dt = datetime.now(timezone.utc)
            for symbol in held_symbols_in_state:
                trade_info = open_trades_in_state.get(symbol); exit_reason = None
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
                    trade_info = open_trades_in_state.get(symbol); position = positions_map.get(symbol)
                    if not trade_info or not position: logging.warning(f"Missing info for sell {symbol}"); continue
                    try:
                        qty = abs(float(position.qty))
                        if qty > 0:
                            exit_id = execute_trade(symbol=symbol, side=OrderSide.SELL, qty=qty)
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
                                if entry_px > 0 and exit_price > 0: profit = (exit_price - entry_px) * qty; pct = ((exit_price - entry_px) / entry_px) * 100
                                pnl = {"user": user_id, "symbol": symbol, "profit": round(profit, 2), "pct_change": round(pct, 2), "time": sell_time.isoformat(), "entry_price": entry_px, "exit_price": exit_price, "entry_trade_id": trade_info.get("trade_id"), "exit_trade_id": exit_id, "exit_reason": sell_reasons.get(symbol)}
                                state.setdefault("pnl", []).append(pnl)
                                logging.info(f"User {user_id} - Closed {symbol}: PnL ${profit:.2f} ({pct:.2f}%) Reason: {sell_reasons.get(symbol)}")
                                user_actions.append({"action": "SELL", "symbol": symbol, "reason": sell_reasons.get(symbol), "price": exit_price, "pnl": round(profit, 2), "pct": round(pct, 2)})
                            else: logging.error(f"State update failed for closed {symbol}")
                        else: logging.warning(f"Alpaca position qty for {symbol} was 0. Marking closed."); idx = next((i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and str(t.get("trade_id")) == str(trade_info.get("trade_id")) and t.get("status") == "open"), -1); state["trades"][idx].update({"status": "closed_zero_qty", "exit_reason": "Zero quantity found"}) if idx != -1 else None
                    except APIError as e: logging.error(f"API error selling {symbol}: {e}"); idx = next((i for i, t in enumerate(state["trades"]) if t.get("user") == user_id and str(t.get("trade_id")) == str(trade_info.get("trade_id")) and t.get("status") == "open"), -1); state["trades"][idx].update({"status": "closed_orphan", "exit_reason": f"Sell failed ({e.status_code})"}) if idx != -1 and e.status_code == 404 else None
                    except Exception as e: logging.error(f"Generic error closing {symbol}: {e}", exc_info=True)

            # Execute Buys
            open_trades_state = {t['symbol']: t for t in state.get("trades", []) if t.get("user") == user_id and t.get("status") == "open"}
            held_symbols = set(open_trades_state.keys()); num_held = len(held_symbols); num_can_buy = MAX_OPEN_POSITIONS - num_held
            symbols_to_buy = current_top_symbols - held_symbols
            if symbols_to_buy and num_can_buy > 0:
                logging.info(f"User {user_id} - Evaluating buys ({num_can_buy} slots): {symbols_to_buy}")
                buy_count = 0; buy_time = datetime.now(timezone.utc)
                for rank in current_top_performers:
                    symbol = rank['symbol']
                    if symbol in symbols_to_buy and buy_count < num_can_buy:
                        if symbol in held_symbols: continue # Should not happen
                        try:
                            price = rank['price']
                            if price <= 0: continue
                            logging.info(f"User {user_id} - Executing BUY {symbol} @ ~${price:.2f}")
                            order_id = execute_trade(symbol=symbol, side=OrderSide.BUY, notional=TRADE_NOTIONAL_PER_STOCK)
                            new = {"user": user_id, "symbol": symbol, "notional": TRADE_NOTIONAL_PER_STOCK, "side": "buy", "executed_at": buy_time.isoformat(), "entry_price": price, "trade_id": order_id, "status": "open"}
                            state.setdefault("trades", []).append(new)
                            buy_count += 1; held_symbols.add(symbol) # Update local view
                            logging.info(f"User {user_id} - Entered {symbol} (#{buy_count}/{num_can_buy}) Order ID: {order_id}")
                            user_actions.append({"action": "BUY", "symbol": symbol, "price": price})
                        except APIError as e:
                            logging.error(f"API error buying {symbol}: {e}", exc_info=False); reason = f"API Error {e.status_code}"
                            if e.status_code == 403: reason = "Forbidden/Funds"; break
                            elif e.status_code == 422: reason = "Not Tradable"
                            user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": reason})
                        except Exception as e: logging.error(f"Failed buy execution for {symbol}: {e}", exc_info=True); user_actions.append({"action": "BUY_FAIL", "symbol": symbol, "reason": "Exec Error"})
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
    except Exception as e:
        logging.error(f"Exception in threaded strategy cycle: {e}", exc_info=True)
        try: # Notify admin
            admin_id = next(iter(state.get("strategy_enabled", {})), None)
            if admin_id: await bot.send_message(chat_id=admin_id, text=f"⚠️ CRITICAL ERROR in job: {type(e).__name__}. Check logs.")
        except Exception as notify_err: logging.error(f"Failed to send critical error notification: {notify_err}")

    if actions_by_user: # Send Notifications
        count = sum(len(acts) for acts in actions_by_user.values()); logging.info(f"Processing {count} actions for notification...")
        for user_id, actions in actions_by_user.items():
            for action in actions:
                message = "❓ Unknown action"; symbol = action.get("symbol", "N/A")
                try:
                    a_type = action.get("action")
                    if a_type == "BUY": message = f"⬆️ **Entered {symbol}** (Top3)\nApprox Entry: ${action.get('price', 0):.2f}"
                    elif a_type == "SELL": message = f"⬇️ **Exited {symbol}** ({action.get('reason', 'N/A')})\nApprox Exit: ${action.get('price', 0):.2f}\nApprox PnL: ${action.get('pnl', 0):+.2f} ({action.get('pct', 0):+.2f}%)"
                    elif a_type == "BUY_FAIL": message = f"⚠️ Failed to buy {symbol}: {action.get('reason', 'Unknown')}"
                    await bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.MARKDOWN)
                    logging.info(f"Sent notification to {user_id}: {message.splitlines()[0]}")
                except telegram.error.BadRequest as tg_err:
                     if "chat not found" in str(tg_err).lower(): logging.error(f"Chat not found {user_id}. Disabling."); async with state_lock: state.setdefault("strategy_enabled", {})[user_id] = False; await asyncio.to_thread(save_state, state)
                     else: logging.error(f"TG BadRequest {user_id}: {tg_err}")
                except telegram.error.Forbidden as tg_err: logging.error(f"TG Forbidden {user_id}. Disabling."); async with state_lock: state.setdefault("strategy_enabled", {})[user_id] = False; await asyncio.to_thread(save_state, state)
                except Exception as tg_err: logging.error(f"TG send error {user_id}: {tg_err}", exc_info=True)
    else: logging.debug("No actions generated.")
    logging.debug("JobQueue callback finished.")


# === Telegram Handlers (Async) ===
async def handle_start_top3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/start_top3 from User {user_id}")
    async with state_lock: state.setdefault("strategy_enabled", {})[user_id] = True; state.setdefault("goals", {}).setdefault(user_id, {"goal": "N/A", "log": []})
    await asyncio.to_thread(save_state, state); await update.message.reply_text("✅ Top 3 Strategy Enabled."); logging.info(f"Strategy enabled: {user_id}")

async def handle_stop_top3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/stop_top3 from User {user_id}")
    async with state_lock: state.setdefault("strategy_enabled", {})[user_id] = False
    await asyncio.to_thread(save_state, state); await update.message.reply_text("⏹️ Top 3 Strategy Disabled."); logging.info(f"Strategy disabled: {user_id}")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/status from User {user_id}")
    async with state_lock: open_trades = [t for t in state.get("trades", []) if t.get("user") == user_id and t.get("status") == "open"]; status = "✅ ENABLED" if state.get("strategy_enabled", {}).get(user_id, False) else "⏹️ DISABLED"
    reply = f"📊 **Status: {status}**\n\n"; current_prices = {}; total_pnl = 0.0; fetch_error = False
    if not open_trades: reply += "ℹ️ No open positions."; await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN); return
    symbols = list({t['symbol'] for t in open_trades})
    if symbols and rest_client:
        logging.debug(f"Fetching status prices: {symbols}")
        try: req = StockLatestTradeRequest(symbol_or_symbols=symbols, feed='sip'); snapshot = await asyncio.to_thread(data_client.get_stock_latest_trade, req); current_prices = {s: t.price for s, t in snapshot.items() if t.price > 0}
        except Exception as e: logging.error(f"Status price fetch error: {e}"); fetch_error = True
    reply += f"📈 **Holdings ({len(open_trades)}):**\n"
    for trade in open_trades:
        sym = trade.get("symbol", "N/A"); entry = float(trade.get("entry_price", 0)); notional = float(trade.get("notional", 0)); current = current_prices.get(sym); exec_at = trade.get("executed_at")
        pnl_str, pct_str, dur_str = "N/A", "", "N/A"
        if current is None: pnl_str = "(Price N/A)"
        elif entry > 0 and notional > 0:
            try: shares = notional / entry; pnl = (current - entry) * shares; pct = ((current - entry) / entry) * 100; total_pnl += pnl; pnl_str = f"${pnl:+.2f}"; pct_str = f"({pct:+.2f}%)"
            except: pnl_str = "(Calc Err)"
        if exec_at:
            try: dur = datetime.now(timezone.utc) - isoparse(exec_at); secs = int(dur.total_seconds()); d, r = divmod(secs, 86400); h, r = divmod(r, 3600); m, _ = divmod(r, 60); p = [f"{x}{u}" for x, u in zip([d, h, m], ['d', 'h', 'm']) if x > 0]; dur_str = " ".join(p) if p else "0m"
            except: dur_str = "(Time Err)"
        reply += f" - **{sym}** | PnL: {pnl_str} {pct_str} | Entry: ${entry:.2f} | Held: {dur_str}\n"
    if fetch_error: reply += "\n_(Price fetch error)_"
    else: reply += f"\n💰 **Total Unrealized: ${total_pnl:+.2f}**"
    await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)

async def handle_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; user_id = str(user.id); logging.info(f"/pnl from User {user_id}")
    async with state_lock: state.setdefault("pnl", []); pnl_recs = [p for p in state["pnl"] if p.get("user") == user_id]
    if not pnl_recs: await update.message.reply_text("📉 No closed trades yet."); return
    profit = sum(t.get("profit", 0) for t in pnl_recs); wins = sum(1 for t in pnl_recs if t.get("profit", 0) > 0); losses = sum(1 for t in pnl_recs if t.get("profit", 0) < 0); trades = len(pnl_recs); win_rate = (wins / trades * 100) if trades > 0 else 0
    limit = 10; summary = f"📈 **Closed PnL (Last {min(limit, trades)}/{trades})**\n\n"
    for t in reversed(pnl_recs[-limit:]):
        sym = t.get('symbol', '?'); p = t.get('profit', 0); pct = t.get('pct_change', 0); r = t.get('exit_reason', 'N/A'); ts = "N/A"
        if t.get("time"): try: ts = isoparse(t["time"]).strftime('%m-%d %H:%M'); except: pass
        summary += f" - **{sym}**: ${p:+.2f} ({pct:+.2f}%) at {ts} _({r})_\n"
    summary += f"\n💰 **Total Realized: ${profit:.2f}** | Win Rate: {win_rate:.1f}% ({wins}W/{losses}L)"
    await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user; msg = update.message; user_id = str(user.id); username = user.username or "N/A"
    if not user or not msg: return
    logging.info(f"📨 Msg from User {user_id} ({username}): '{msg.text}'")
    async with state_lock: known = user_id in state.get("goals", {}) or user_id in state.get("strategy_enabled", {})
    if known: await update.message.reply_text("ℹ️ Bot active. Commands:\n`/start_top3` | `/stop_top3` | `/status` | `/pnl`", parse_mode=ParseMode.MARKDOWN)
    else:
        logging.info(f"New user {user_id} ({username}) registered.")
        async with state_lock: state.setdefault("goals", {})[user_id] = {"goal": msg.text, "log": []}; state.setdefault("strategy_enabled", {})[user_id] = False; await asyncio.to_thread(save_state, state)
        await update.message.reply_text(f"👋 Welcome, {user.first_name}!\nUse `/start_top3` to enable trading.")

# === Error Handler ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("Exception during update handling:", exc_info=context.error)
    if isinstance(context.error, telegram.error.Conflict):
        logging.critical("TELEGRAM CONFLICT ERROR: Another instance running!")
        if isinstance(update, Update) and update.effective_chat:
             try: await context.bot.send_message(chat_id=update.effective_chat.id, text="🚨 Bot Conflict Detected! Stop other instances.")
             except Exception as e: logging.error(f"Failed to send conflict warning: {e}")

# === Start Telegram Bot ===
def run_bot():
    if not TELEGRAM_BOT_TOKEN: logging.critical("TG Token not set."); return
    try:
        app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start_top3", handle_start_top3))
        app.add_handler(CommandHandler("stop_top3", handle_stop_top3))
        app.add_handler(CommandHandler("status", handle_status))
        app.add_handler(CommandHandler("pnl", handle_pnl))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        app.add_error_handler(error_handler)
        jq = app.job_queue
        if jq: jq.run_repeating(top_3_strategy_job_callback, interval=timedelta(minutes=JOB_INTERVAL_MINUTES), first=timedelta(seconds=15), name="top_3_30min_strategy_job"); logging.info(f"Scheduled job every {JOB_INTERVAL_MINUTES} min.")
        else: logging.error("JobQueue error."); return
        logging.info("Starting Telegram polling..."); app.run_polling(drop_pending_updates=True)
    except Exception as e: logging.critical(f"Failed running bot: {e}", exc_info=True)

# === Entry Point === #
if __name__ == "__main__":
    # --- FIX: Correct indentation for this block ---
    logging.info(f"Initial state loaded: {len(state.get('trades',[]))} trades, {len(state.get('pnl',[]))} pnl records, {len(state.get('strategy_enabled',{}))} users configured.")
    try:
        run_bot()
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
    except Exception as e:
        logging.critical(f"Unhandled exception in main block: {e}", exc_info=True)
    finally:
        logging.info("Attempting final state save...")
        save_state(state)
        logging.info("-------------------- BOT END ----------------------")
```

**Test Script Update:**
(Updating the `alpaca_test_script` immersive block to use `get_all_assets`)


```python
# test_alpaca.py - Minimal script to test asset fetching

import os
import logging
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetStatus, AssetClass, AssetExchange
from alpaca.trading.requests import GetAssetsRequest # Import request model
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    logging.error("API Keys not found in .env file!")
    exit()
else:
    logging.info(f"API Key loaded: {ALPACA_API_KEY[:5]}...")

try:
    logging.info("Initializing TradingClient...")
    client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
    logging.info(f"Client Type: {type(client)}")

    # Check if the method exists
    has_get_all_assets = hasattr(client, 'get_all_assets')
    logging.info(f"Client has 'get_all_assets' method: {has_get_all_assets}")

    if not has_get_all_assets:
         logging.error("FATAL: client object does not have 'get_all_assets' method!")
         exit()

    # Try calling get_all_assets (which worked based on user logs)
    logging.info("Attempting client.get_all_assets for NASDAQ...")
    search_params = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE,
        exchange=AssetExchange.NASDAQ # Pass Enum directly if get_all_assets accepts it
    )
    assets = client.get_all_assets(search_params)
    logging.info(f"SUCCESS: Fetched {len(assets)} NASDAQ assets using get_all_assets.")

    # Print first 5 assets for verification
    for i, asset in enumerate(assets[:5]):
        logging.info(f"Asset {i+1}: {asset.symbol} - {asset.name}")


except AttributeError as ae:
    logging.critical(f"AttributeError during test: {ae}")
    traceback.print_exc() # Print full traceback for AttributeError
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
    traceback.print_exc() # Print full traceback for other errors

