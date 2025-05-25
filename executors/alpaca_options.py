# Unholy_V2.py - Aggressive Long/Short Strategy Bot (v2.0.18 - Enhanced TG Commands)

import asyncio
import os
import json
import logging
import sys
import pytz
from datetime import datetime, timedelta, date, timezone
from datetime import time as dt_time # Import time specifically as dt_time
from dateutil.parser import isoparse
from collections import deque, defaultdict
from dotenv import load_dotenv
import time # Standard time module
import math
import re
import html # Standard library for HTML escaping
import pandas as pd
import pandas_ta as ta
import numpy as np
import warnings # To suppress specific warnings

# Alpaca Imports
from alpaca.trading.client import TradingClient as REST
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest, ClosePositionRequest
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, OrderStatus
from alpaca.trading.models import Asset, Position, Order
from alpaca.common.exceptions import APIError
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest, StockSnapshotRequest # Import Snapshot Request
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.models import BarSet, Bar, Snapshot # Import Snapshot model

# Telegram Imports
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, Application
from telegram.constants import ParseMode # Use HTML now
import telegram.error

# === V2 Configuration === #
load_dotenv()
ALPACA_API_KEY = os.getenv("ALPACA_UNHOLY_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_UNHOLY_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("Unholy_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_USER_ID")

# --- Basic Sanity Checks ---
if not ALPACA_API_KEY or not ALPACA_SECRET_KEY: print("ERROR: Alpaca Keys missing.", file=sys.stderr); sys.exit(1)
if not TELEGRAM_BOT_TOKEN: print("ERROR: Telegram Token missing.", file=sys.stderr); sys.exit(1)
if not TELEGRAM_CHAT_ID: print("ERROR: TELEGRAM_USER_ID missing.", file=sys.stderr); sys.exit(1)

PAPER_TRADING = True
STATE_FILE = "unholy_v2_state.json"
LOG_FILE = "unholy_v2.log"
TRADE_LOG_JSONL = "Unholy_Trades_log_v2.jsonl"

POSITION_SIZE_USD = 5000.00
JOB_INTERVAL_MINUTES = 2

# --- V2 Filtering Parameters ---
# <<< REMEMBER TO SET THIS BACK TO YOUR DESIRED LEVEL (e.g., 500000) AFTER TESTING! >>>
MIN_TODAY_VOLUME = 50000

# Technical Analysis Parameters
EMA_SHORT=9; EMA_LONG=21; RSI_PERIOD=14; RSI_OVERBOUGHT=70; RSI_OVERSOLD=30
MACD_FAST=12; MACD_SLOW=26; MACD_SIGNAL=9; ATR_PERIOD=14

# --- Risk Management Parameters ---
INITIAL_STOP_ATR_MULT_LONG=1.5; INITIAL_STOP_ATR_MULT_SHORT=1.5
TRAILING_STOP_ATR_MULT_LONG=1.0; TRAILING_STOP_ATR_MULT_SHORT=1.0
PROFIT_TARGET_ATR_MULT=2.5; MIN_RISK_ATR_MULT=0.5

# Turbo Breaks Up Parameters
TURBO_VOLUME_LOOKBACK_MIN=20; TURBO_VOLUME_SPIKE_THRESHOLD=3.0
TURBO_PRICE_SURGE_LOOKBACK_MIN=5; TURBO_PRICE_SURGE_THRESHOLD=0.02
TURBO_RECENT_HIGH_LOOKBACK_MIN=30; TURBO_MIN_CONDITIONS=2

# === Setup Logging === #
LOGGING_LEVEL = logging.INFO
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s', handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler(sys.stdout)], encoding='utf-8')
# Suppress the specific pandas_ta UserWarning about timezone dropping for VWAP
warnings.filterwarnings("ignore", message="Converting to PeriodArray/Index representation will drop timezone information.", category=UserWarning)
logging.info(f"-------------------- BOT START (Unholy_V2.0.18 - Enhanced TG Commands) --------------------") # Version Update
logging.info(f"PAPER_TRADING: {PAPER_TRADING}")
logging.info(f"Position Size: ${POSITION_SIZE_USD}")
logging.info(f"JSON Log: {TRADE_LOG_JSONL}")
logging.info(f"Filters: Min TODAY Volume >= {MIN_TODAY_VOLUME}") # Reflect current setting
logging.info(f"Risk Params: InitSL={INITIAL_STOP_ATR_MULT_LONG}xATR, TrailSL={TRAILING_STOP_ATR_MULT_LONG}xATR, PT={PROFIT_TARGET_ATR_MULT}xATR")
logging.info(f"Turbo Params: VolThresh={TURBO_VOLUME_SPIKE_THRESHOLD}x, PriceSurge={TURBO_PRICE_SURGE_THRESHOLD*100}%, MinCond={TURBO_MIN_CONDITIONS}")
logging.info(f"Running 24/7 scan cycle ({JOB_INTERVAL_MINUTES} min interval). Execution gated by market open status.")

# === Global State & Data Structures === #
state = {}; assets_cache = {}; last_assets_fetch_time = None
state_lock = asyncio.Lock(); recent_notifications = deque(maxlen=30); EST = pytz.timezone('US/Eastern')
shutdown_event = asyncio.Event(); cycle_stats = defaultdict(int); failed_candidates_details = {}

# === Initialize Alpaca Client === #
try:
    rest_client = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=PAPER_TRADING)
    data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    account_info = rest_client.get_account()
    logging.info(f"Alpaca client initialized. Account: {account_info.status}, BP: {account_info.buying_power}")
except APIError as e: logging.critical(f"Alpaca client init failed: {e}", exc_info=True); sys.exit(1)
except Exception as e: logging.critical(f"Unexpected Alpaca client init error: {e}", exc_info=True); sys.exit(1)

# === Telegram Messaging (Using HTML) === #
async def send_telegram_message(bot: Bot, message: str, add_to_recent=True, parse_mode=ParseMode.HTML): # Default to HTML
    """Sends a message using the provided Bot instance, escaping for HTML."""
    if not bot: logging.warning(f"TG Bot missing. Skip: {message[:50]}..."); return
    if not TELEGRAM_CHAT_ID: logging.warning(f"TG Chat ID missing. Skip: {message[:50]}..."); return
    common_messages_to_limit = ["No valid Long/Short setups detected"]
    is_common = any(common in message for common in common_messages_to_limit)
    if is_common and message in recent_notifications and LOGGING_LEVEL > logging.DEBUG:
        logging.debug(f"Skip duplicate TG msg: {message[:50]}..."); return
    try:
        # Basic HTML escaping for message content (but allows tags like <b>, <i> etc.)
        # If parse_mode is None, don't escape (assume plain text)
        if parse_mode == ParseMode.HTML:
            escaped_message = html.escape(message)
            # Re-allow specific tags needed for formatting
            allowed_tags = ['b', 'i', 'code', 'pre']
            for tag in allowed_tags:
                escaped_message = escaped_message.replace(html.escape(f"<{tag}>"), f"<{tag}>")
                escaped_message = escaped_message.replace(html.escape(f"</{tag}>"), f"</{tag}>")
        else:
            escaped_message = message # No escaping if not HTML mode

        max_len = 4096
        if len(escaped_message) > max_len:
             logging.warning(f"Truncating TG message ({len(escaped_message)} > {max_len})")
             escaped_message = escaped_message[:max_len-20] + ('\n... (truncated)' if parse_mode != ParseMode.HTML else html.escape('\n... (truncated)'))

        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=escaped_message, parse_mode=parse_mode)
        logging.info(f"Sent TG message: {message[:50]}...");
        if add_to_recent: recent_notifications.append(message) # Add original msg to recent queue
    except telegram.error.BadRequest as e: logging.error(f"TG BadRequest: {e}. Escaped: '{escaped_message[:100]}...'. Original: '{message[:100]}...'")
    except telegram.error.Forbidden as e: logging.error(f"TG Forbidden: {e}.")
    except telegram.error.NetworkError as e: logging.error(f"TG NetworkError: {e}.", exc_info=True)
    except Exception as e: logging.error(f"Failed send TG message: {e}", exc_info=True)

# === Persistent State Load/Save === #
def load_state():
    default_state={"open_positions":{}}; f=STATE_FILE
    if os.path.exists(f):
        try:
             with open(f, "r", encoding='utf-8') as file: data=json.load(file)
             # Ensure trailing_stop exists for backward compatibility
             for _,d in data.get("open_positions",{}).items(): d.setdefault('trailing_stop',d.get('stop_loss')) # Init TS from SL if missing
             logging.info(f"Loaded state {f}. Open:{len(data.get('open_positions',{}))}"); return data
        except Exception as e: logging.error(f"Err load {f}: {e}. Fresh."); return default_state
    else: logging.warning(f"{f} not found. Fresh."); return default_state
def save_state(state_to_save):
    try:
        minimal={"open_positions": state_to_save.get("open_positions",{})}
        def serializer(obj):
            if isinstance(obj,(datetime,date)): return obj.isoformat()
            if isinstance(obj,np.integer): return int(obj);
            if isinstance(obj,np.floating): return float(obj);
            if isinstance(obj,np.ndarray): return obj.tolist();
            if pd.isna(obj): return None;
            if obj is None: return None;
             # Allow OrderStatus enum to be saved as string
            if isinstance(obj, OrderStatus): return obj.value
            raise TypeError(f"Type {type(obj)} not serializable: {obj}")
        with open(STATE_FILE,"w",encoding='utf-8') as f: json.dump(minimal,f,indent=4,default=serializer)
    except Exception as e: logging.error(f"Err save state: {e}")
state = load_state(); logging.info(f"Initial state - Open Pos: {len(state.get('open_positions',{}))}")

# === JSON Lines Trade Logging === #
def log_trade_to_jsonl(trade_data: dict):
    try:
        copy=trade_data.copy();
        for k,v in copy.items():
            if isinstance(v, datetime): copy[k]=v.isoformat()
            elif isinstance(v,(np.integer)): copy[k]=int(v)
            elif isinstance(v,(np.floating)): copy[k]=float(v)
            elif pd.isna(v): copy[k]=None
        json_s=json.dumps(copy);
        with open(TRADE_LOG_JSONL,'a',encoding='utf-8') as f: f.write(json_s+'\n')
    except Exception as e: logging.error(f"Error write trade JSONL: {e}")

# === Alpaca Trade Execution Wrappers === #
def execute_long_trade(symbol: str, qty: float) -> Order | None:
    if qty <= 0: logging.warning(f"Attempted LONG on {symbol} with qty <= 0 ({qty}). Skipping."); return None
    log_action = f"BUY LONG {symbol}(Qty:{qty})"
    order = None
    try:
        data = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
        logging.info(f"Attempt:{log_action}")
        order = rest_client.submit_order(order_data=data)
        logging.info(f"Submitted:{log_action}. ID:{order.id}, Status:{order.status}")
        time.sleep(1.5) # Consider making this shorter or configurable
    except APIError as e:
        logging.error(f"API err submit {log_action}: {e}")
        return None
    except Exception as e:
        logging.error(f"Generic err submit {log_action}: {e}", exc_info=True)
        return None

    # If order submission was successful, try to get the order details
    if order:
        try:
            filled = rest_client.get_order_by_id(order.id)
            logging.info(f"Order {filled.id} status after check: {filled.status}")
            return filled
        except APIError as get_e:
            logging.error(f"API Err getting order details for {order.id}: {get_e}. Returning originally submitted order object.")
            return order # Return the original order object if fetching updated status fails
        except Exception as get_e:
            logging.error(f"Unexpected error getting order details for {order.id}: {get_e}. Returning originally submitted order object.", exc_info=True)
            return order
    return None # Should not be reached if order was submitted, but included for completeness

def execute_short_trade(symbol: str, qty: float) -> Order | None:
    if qty <= 0: logging.warning(f"Attempted SHORT on {symbol} with qty <= 0 ({qty}). Skipping."); return None
    log_action = f"SHORT SELL {symbol}(Qty:{qty})"
    order = None
    try:
        data = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
        logging.info(f"Attempt:{log_action}")
        order = rest_client.submit_order(order_data=data)
        logging.info(f"Submitted:{log_action}. ID:{order.id}, Status:{order.status}")
        time.sleep(1.5) # Consider making this shorter or configurable
    except APIError as e:
        logging.error(f"API err submit {log_action}: {e}")
        return None
    except Exception as e:
        logging.error(f"Generic err submit {log_action}: {e}", exc_info=True)
        return None

    # If order submission was successful, try to get the order details
    if order:
        try:
            filled = rest_client.get_order_by_id(order.id)
            logging.info(f"Order {filled.id} status after check: {filled.status}")
            return filled
        except APIError as get_e:
            logging.error(f"API Err getting order details for {order.id}: {get_e}. Returning originally submitted order object.")
            return order # Return the original order object if fetching updated status fails
        except Exception as get_e:
             logging.error(f"Unexpected error getting order details for {order.id}: {get_e}. Returning originally submitted order object.", exc_info=True)
             return order
    return None # Should not be reached if order was submitted


def close_position_wrapper(symbol: str) -> Order | None:
    """Closes a position and handles potential errors including unexpected return types."""
    log_a = f"CLOSE POS {symbol}"
    logging.info(f"Attempt:{log_a}")
    orders: list[Order] | tuple | None = None # Explicitly allow tuple for type hinting robustness
    try:
        # Use ClosePositionRequest for potentially better control/predictability
        # close_request = ClosePositionRequest(qty=None, percentage=None) # Close entire position
        # orders = rest_client.close_position(symbol_or_asset_id=symbol, close_options=close_request)
        # Simpler method:
        orders = rest_client.close_position(symbol_or_asset_id=symbol) # Returns list[Order] typically

        if isinstance(orders, list) and all(hasattr(o, 'id') for o in orders):
            order_ids = [o.id for o in orders]
            logging.info(f"Close req sub {symbol}. Orders: {order_ids}")
        elif orders is not None:
             logging.warning(f"Close req sub {symbol}. Unexpected return type/content: {type(orders)} - {orders}")
        else:
             logging.warning(f"Close req sub {symbol}. Returned None.")

    except APIError as e:
        # Check for 404 or "position not found" string
        if (hasattr(e, 'response') and e.response and e.response.status_code == 404) or \
           ("position not found" in str(e).lower()) or \
           (hasattr(e, 'status_code') and e.status_code == 404): # Added check for direct status_code
            logging.warning(f"Close fail: Pos {symbol} not found via API (404). Assuming closed."); return None
        elif "market is closed" in str(e).lower():
            logging.warning(f"Close fail {log_a}: Market closed."); return None
        # Handle 422 (Unprocessable Entity) - often means no position exists
        elif (hasattr(e, 'response') and e.response and e.response.status_code == 422) or \
             (hasattr(e, 'status_code') and e.status_code == 422):
             logging.warning(f"Close fail: Unprocessable Entity (422) for {symbol}. Position likely already closed or none exists."); return None
        else:
            logging.error(f"API error closing {log_a}: {e}"); return None
    except Exception as e:
        logging.error(f"Generic error closing {log_a}: {e}", exc_info=True); return None

    # Proceed only if we received a list of orders as expected and it's not empty
    if isinstance(orders, list) and len(orders) > 0:
        order1 = orders[0] # Usually only one order is returned for market close
        time.sleep(1.5) # Wait after submission before checking status
    else:
        logging.warning(f"close_position did not return a valid list of orders for {symbol}. Result: {orders}. Assuming closed or failed submission.");
        return None # Cannot proceed if we didn't get a valid order list

    # Try to get the updated status of the first order
    try:
        filled = rest_client.get_order_by_id(order1.id)
        logging.info(f"Close Order {filled.id} status after check: {filled.status}")
        return filled
    except APIError as get_e:
        logging.error(f"API Err get close order {order1.id}: {get_e}. Returning original order object.")
        return order1 # Return original order if fetching status fails
    except Exception as get_e:
        logging.error(f"Unexpected Err get close order {order1.id}: {get_e}", exc_info=True)
        return order1


# === Market Data Fetching & Processing === #
def get_active_tradable_assets() -> dict[str, dict]:
    global assets_cache, last_assets_fetch_time, cycle_stats; today=date.today()
    # Cache valid for one day
    if last_assets_fetch_time==today and assets_cache:
        cycle_stats['01_InitialAssetsFetched']=cycle_stats.get('01_InitialAssetsFetched',len(assets_cache))
        cycle_stats['02_AfterBasicFilters']=len(assets_cache)
        logging.debug(f"Using cached assets from {today}. Count: {len(assets_cache)}")
        return assets_cache
    logging.info("V2: Fetching/Refreshing assets list..."); assets_cache={}; shortable=0; initial=0; filtered_c=0
    try:
        params=GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE); all_assets:list[Asset]=rest_client.get_all_assets(params); initial=len(all_assets); filtered={}
        for a in all_assets:
            # Basic filters: tradable, equity, active, simple symbol
            if (a.tradable and a.asset_class==AssetClass.US_EQUITY and a.status==AssetStatus.ACTIVE and '.' not in a.symbol and '/' not in a.symbol):
                 # Additional filters can be added here if needed (e.g., exchange, marginable)
                filtered[a.symbol]={'tradable':a.tradable,'shortable':a.shortable,'easy_to_borrow':a.easy_to_borrow,'exchange':a.exchange.value if a.exchange else None,'marginable': a.marginable}
                if a.shortable: shortable+=1
        filtered_c=len(filtered); logging.info(f"Fetched {initial} assets, filtered to {filtered_c} tradable US equities ({shortable} shortable).")
        if filtered: assets_cache=filtered; last_assets_fetch_time=today
        else: logging.warning("No tradable symbols found after initial filtering.")
    except APIError as e: logging.error(f"API Error fetching assets: {e}")
    except Exception as e: logging.error(f"Unexpected error fetching assets: {e}", exc_info=True)
    cycle_stats['01_InitialAssetsFetched']=initial; cycle_stats['02_AfterBasicFilters']=filtered_c
    return assets_cache

def get_todays_volumes(symbols: list[str], batch_size: int = 400) -> dict[str, float]:
    """Fetches the volume for the current trading day using the Snapshot endpoint."""
    todays_volumes = {}
    if not symbols: return todays_volumes

    today_dt = datetime.now(timezone.utc).date()
    logging.info(f"Fetching Snapshots for {len(symbols)} symbols for volume check (Batch: {batch_size})...")
    symbols_processed = 0
    symbols_with_data = 0 # Count symbols that actually return snapshot data

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        time.sleep(0.25) # Keep delay relatively short for snapshots
        if not batch: continue
        batch_num = i//batch_size + 1
        total_batches = math.ceil(len(symbols)/batch_size)
        logging.debug(f"Fetching snapshot batch {batch_num}/{total_batches}...")
        try:
            params = StockSnapshotRequest(symbol_or_symbols=batch, feed='sip')
            snapshot_data: dict[str, Snapshot] = data_client.get_stock_snapshot(params)

            for symbol, snapshot in snapshot_data.items():
                symbols_processed += 1 # Count every symbol attempted in the batch
                if snapshot and snapshot.daily_bar:
                    symbols_with_data += 1
                    daily_bar = snapshot.daily_bar
                    if daily_bar.timestamp.date() == today_dt:
                        try:
                            vol = daily_bar.volume
                            if vol is not None and vol > 0: todays_volumes[symbol] = float(vol)
                            # else: logging.debug(f"Volume for {symbol} in snapshot daily bar is {vol}. Skipping.")
                        except Exception as vol_err: logging.warning(f"Error accessing volume for {symbol} from snapshot daily bar {daily_bar}: {vol_err}")
                    # else: logging.debug(f"Snapshot daily bar for {symbol} is from {daily_bar.timestamp.date()}, not {today_dt}. Skipping.")
                # else: logging.debug(f"No snapshot or daily_bar data found for {symbol} in batch {batch_num}.")

        except APIError as e:
            if e.status_code == 403: logging.error(f"SIP Data Sub Error (403) fetching snapshots. Check Alpaca subscription. {e}"); break
            elif e.status_code == 429: logging.warning(f"Rate limit hit snapshots batch {batch_num}. Waiting... {e}"); time.sleep(5)
            elif e.status_code == 422: logging.warning(f"Unprocessable entity (422) snapshots batch {batch_num}. Symbols: {batch[:5]}... {e}")
            else: logging.error(f"API error snapshots batch {batch_num}: Status {e.status_code}, Message: {e}")
        except Exception as e: logging.error(f"Unexpected error fetching/processing snapshots batch {batch_num}: {e}", exc_info=True)

    logging.info(f"Finished fetching snapshots. Attempted: {symbols_processed}, Received data for: {symbols_with_data}, Got volume for: {len(todays_volumes)} symbols.")
    return todays_volumes


def get_latest_prices_batched(symbols: list[str], batch_size: int = 500) -> dict[str, float]:
    """Fetches latest trade prices with batching and error handling."""
    prices={};
    if not symbols: return prices;
    logging.debug(f"Fetching latest prices for {len(symbols)} symbols (Batch: {batch_size})...")
    symbols_with_price = 0
    for i in range(0,len(symbols),batch_size):
        batch=symbols[i:i+batch_size]; time.sleep(0.1) # Short delay between price batches
        if not batch: continue
        batch_num = i//batch_size + 1
        total_batches = math.ceil(len(symbols)/batch_size)
        logging.debug(f"Fetching prices batch {batch_num}/{total_batches}...")
        try:
            req=StockLatestTradeRequest(symbol_or_symbols=batch, feed='sip')
            trades=data_client.get_stock_latest_trade(req)
            for sym, t in trades.items():
                if t and t.price is not None and t.price > 0: # Check price is not None explicitly
                    prices[sym]=t.price
                    symbols_with_price += 1
                # else: logging.debug(f"No valid latest trade price for {sym} in batch {batch_num}.")
        except APIError as e:
            if e.status_code==403: logging.error(f"SIP Price Err(403) batch {batch_num}. Check Subscription. {e}"); break
            elif e.status_code==429: logging.warning(f"Rate limit prices batch {batch_num}. Waiting... {e}"); time.sleep(3) # Increased wait
            elif e.status_code==422: logging.warning(f"Unprocessable prices batch {batch_num} ({batch[:5]}...). {e}")
            else: logging.error(f"API err prices batch {batch_num}: Status {e.status_code}, Message: {e}")
            continue
        except Exception as e: logging.error(f"Generic err prices batch {batch_num}: {e}", exc_info=True); continue

    logging.debug(f"Got latest prices for {symbols_with_price} symbols.")
    return prices

def get_bars_data(symbols:list[str], tf:TimeFrame, start:datetime, end:datetime, limit:int=200, batch_size:int=200)->dict[str,pd.DataFrame]:
    """Fetches historical bar data with increased delay, logging, and SIP feed."""
    bars_data={};
    if not symbols: return bars_data
    # Ensure start/end are timezone-aware (UTC)
    start_utc=start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc=end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)
    tf_unit=tf.unit.value if isinstance(tf.unit,TimeFrameUnit) else str(tf.unit);
    logging.info(f"Fetching bars for {len(symbols)} symbols ({tf.amount} {tf_unit}, Lim:{limit}, Batch:{batch_size}, SIP)...")
    processed_symbols = 0
    symbols_with_bars = 0
    total_batches = math.ceil(len(symbols) / batch_size)

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        time.sleep(0.5) # Increased delay between batches
        if not batch: continue
        current_batch_num = i // batch_size + 1
        logging.debug(f"Fetching bars batch {current_batch_num}/{total_batches}...")

        try:
            params=StockBarsRequest(symbol_or_symbols=batch,timeframe=tf,start=start_utc,end=end_utc,limit=limit,adjustment='raw',feed='sip')
            barset: BarSet=data_client.get_stock_bars(params)

            symbols_in_batch_with_bars = 0
            processed_in_batch = 0
            for sym, bars in barset.data.items():
                if bars: # API returned a list for this symbol
                    symbols_with_bars += 1
                    symbols_in_batch_with_bars += 1
                    try:
                        df=pd.DataFrame([b.model_dump() for b in bars])
                        if not df.empty:
                            df['ts']=pd.to_datetime(df['timestamp']); df=df.set_index('ts').sort_index()
                            # Ensure required columns are numeric
                            num_cols = ['open','high','low','close','volume']
                            if 'vwap' in df.columns: num_cols.append('vwap')
                            for col in num_cols:
                                if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')

                            # Drop rows where essential price data is missing AFTER conversion
                            df=df.dropna(subset=['open','high','low','close'])
                            if not df.empty:
                                bars_data[sym]=df;
                                processed_symbols+=1
                                processed_in_batch+=1
                        # else: logging.debug(f"Bars for {sym} resulted in empty DataFrame after initial processing.")
                    except Exception as df_e: logging.error(f"Err processing bars for {sym} in batch {current_batch_num}: {df_e}")
                # else: logging.debug(f"API returned no bar list for symbol {sym} in batch {current_batch_num}.")
            logging.debug(f"Batch {current_batch_num}: Requested {len(batch)}, API bars for {symbols_in_batch_with_bars}, Processed DF for {processed_in_batch} symbols.")

        except APIError as e:
            if e.status_code==403: logging.error(f"SIP Bars Err(403) on batch {current_batch_num}: {e}. Check Subscription."); break
            elif e.status_code==429: logging.warning(f"Rate limit bars batch {current_batch_num}. Waiting... {e}"); time.sleep(5)
            elif e.status_code==422: logging.warning(f"Unprocessable bars batch {current_batch_num}({batch[:5]}...). {e}") # Often means invalid symbol or date range issue
            else: logging.error(f"API err bars batch {current_batch_num}: Status {e.status_code}, Message: {e}")
        except Exception as e: logging.error(f"Generic err bars batch {current_batch_num}: {e}", exc_info=True)

    logging.info(f"Finished fetching bars. Symbols with any bars from API: {symbols_with_bars}. Successfully processed into non-empty DF: {processed_symbols} symbols.")
    return bars_data

# --- Indicator Calculation ---
def calculate_indicators(bars_df: pd.DataFrame, calculate_atr=True, calculate_other_ta=True) -> pd.DataFrame:
    """Calculates technical indicators with added checks and explicit ATR/VWAP assignment."""
    if bars_df.empty:
        logging.debug("calculate_indicators received empty DataFrame. Skipping.")
        return bars_df
    if not isinstance(bars_df.index, pd.DatetimeIndex):
        logging.warning("DataFrame index is not DatetimeIndex. Skipping indicator calculation.")
        return bars_df

    if not bars_df.index.is_monotonic_increasing:
        logging.debug("DataFrame index not monotonic, sorting...")
        bars_df = bars_df.sort_index()

    required_ohlc = ['open', 'high', 'low', 'close']
    required_vol = ['volume']
    required_ohlcv = required_ohlc + required_vol
    if not all(col in bars_df.columns for col in required_ohlc):
        logging.warning(f"Missing OHLC columns. Cols: {bars_df.columns}. Skipping indicators.")
        return bars_df

    atr_col = f'ATR_{ATR_PERIOD}'
    ema_s_col = f'EMA_{EMA_SHORT}'; ema_l_col = f'EMA_{EMA_LONG}'; rsi_col = f'RSI_{RSI_PERIOD}'
    macd_col = f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'; macdh_col = f'MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'; macds_col = f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'
    vwap_col = f'VWAP_{RSI_PERIOD}' # Using RSI period for VWAP length, adjust if needed

    try:
        # Ensure volume is float if present and needed
        if 'volume' in bars_df.columns and any(col in required_ohlcv for col in [vwap_col]): # Only if needed
            bars_df['volume'] = bars_df['volume'].astype(float)

        # --- ATR ---
        if calculate_atr:
            min_rows_atr = ATR_PERIOD + 1
            if len(bars_df) >= min_rows_atr:
                logging.debug(f"Calculating {atr_col} for DF shape {bars_df.shape}")
                try:
                    atr_series = ta.atr(high=bars_df['high'], low=bars_df['low'], close=bars_df['close'], length=ATR_PERIOD)
                    bars_df[atr_col] = atr_series
                    if bars_df[atr_col].isnull().all(): logging.warning(f"{atr_col} calculated but contains only NaNs.")
                    # else: logging.debug(f"Successfully calculated {atr_col}.")
                except Exception as atr_e: logging.error(f"Error calculating ATR: {atr_e}", exc_info=False); bars_df[atr_col] = pd.NA
            else: logging.debug(f"Skipping ATR: Not enough rows ({len(bars_df)} < {min_rows_atr})"); bars_df[atr_col] = pd.NA

        # --- Other TA ---
        if calculate_other_ta:
            min_rows_ema_long = EMA_LONG + 1; min_rows_rsi = RSI_PERIOD + 1; min_rows_macd = MACD_SLOW + MACD_SIGNAL # Approx minimums

            if len(bars_df) >= min_rows_ema_long:
                try: bars_df.ta.ema(length=EMA_SHORT, append=True)
                except Exception as e: logging.error(f"Error calculating {ema_s_col}: {e}", exc_info=False)
                try: bars_df.ta.ema(length=EMA_LONG, append=True)
                except Exception as e: logging.error(f"Error calculating {ema_l_col}: {e}", exc_info=False)
            else: logging.debug(f"Skipping EMAs: Not enough rows ({len(bars_df)})")

            if len(bars_df) >= min_rows_rsi:
                try: bars_df.ta.rsi(length=RSI_PERIOD, append=True)
                except Exception as e: logging.error(f"Error calculating {rsi_col}: {e}", exc_info=False)
            else: logging.debug(f"Skipping RSI: Not enough rows ({len(bars_df)})")

            if len(bars_df) >= min_rows_macd:
                try: bars_df.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL, append=True)
                except Exception as e: logging.error(f"Error calculating MACD: {e}", exc_info=False)
            else: logging.debug(f"Skipping MACD: Not enough rows ({len(bars_df)})")

            # --- VWAP ---
            # VWAP requires volume
            if all(c in bars_df.columns for c in required_ohlcv):
                 # Check if volume exists and is usable
                 if not bars_df['volume'].isnull().all() and bars_df['volume'].sum(skipna=True) > 0:
                    # Using RSI_PERIOD for VWAP length, ensure enough rows
                    if len(bars_df) >= RSI_PERIOD:
                        try:
                            vwap_series = ta.vwap(high=bars_df['high'], low=bars_df['low'], close=bars_df['close'], volume=bars_df['volume'], length=RSI_PERIOD)
                            bars_df[vwap_col] = vwap_series
                            if bars_df[vwap_col].isnull().all(): logging.warning(f"{vwap_col} calculated but contains only NaNs.")
                            # else: logging.debug(f"Successfully calculated {vwap_col}.")
                        except Exception as v_e: logging.warning(f"Error calculating VWAP: {v_e}", exc_info=False); bars_df[vwap_col] = pd.NA
                    else: logging.debug(f"Skipping VWAP: Not enough rows ({len(bars_df)} < {RSI_PERIOD})"); bars_df[vwap_col] = pd.NA
                 else: logging.debug(f"Skipping VWAP: Volume column unusable."); bars_df[vwap_col] = pd.NA
            else: logging.debug(f"Skipping VWAP: Missing required OHLCV columns."); bars_df[vwap_col] = pd.NA

    except Exception as e:
        logging.error(f"Unexpected error during indicator calculation: {e}", exc_info=True)
        # Ensure expected columns exist as NA if calculation failed partway
        cols_to_check = []
        if calculate_atr: cols_to_check.append(atr_col)
        if calculate_other_ta: cols_to_check.extend([ema_s_col, ema_l_col, rsi_col, macd_col, macdh_col, macds_col, vwap_col])
        for col in cols_to_check:
            if col not in bars_df.columns: bars_df[col] = pd.NA

    return bars_df


# === Core Strategy Logic === #

# --- V2: Turbo Breaks Up Strategy ---
def turbo_breaks_up_strategy(symbol: str, bars_1m_df: pd.DataFrame, current_price: float) -> tuple[str | None, str | None, str | None, tuple | None]:
    bias="LONG"; strat="Turbo Breaks Up"; min_bars=max(TURBO_VOLUME_LOOKBACK_MIN,TURBO_PRICE_SURGE_LOOKBACK_MIN,TURBO_RECENT_HIGH_LOOKBACK_MIN)+5
    if bars_1m_df is None or bars_1m_df.empty or len(bars_1m_df)<min_bars: return None,None,None,("Turbo", f"Not enough 1m bars ({len(bars_1m_df) if bars_1m_df is not None else 'None'}<{min_bars})")
    try:
        vol_spike=False; surge=False; break_high=False; reasons=[]; fail=None

        # --- Volume Spike ---
        if len(bars_1m_df) < TURBO_VOLUME_LOOKBACK_MIN + 1: # Need lookback + current bar
             return None, None, None, ("Turbo", f"Not enough bars for Vol Rolling({len(bars_1m_df)}<{TURBO_VOLUME_LOOKBACK_MIN+1})")
        # Calculate rolling average volume on previous bars, ensure iloc[-2] is valid
        rolling_vol = bars_1m_df['volume'].iloc[:-1].rolling(TURBO_VOLUME_LOOKBACK_MIN, min_periods=max(5, TURBO_VOLUME_LOOKBACK_MIN // 2)).mean()
        if len(rolling_vol) < 1 or pd.isna(rolling_vol.iloc[-1]):
            return None, None, None, ("Turbo", f"Not enough data for Avg Vol calc (Rolling len: {len(rolling_vol)})")
        avg_vol = rolling_vol.iloc[-1]
        last_vol=bars_1m_df['volume'].iloc[-1]
        if not pd.isna(avg_vol) and avg_vol > 0 and not pd.isna(last_vol):
            if last_vol > (TURBO_VOLUME_SPIKE_THRESHOLD * avg_vol): vol_spike=True; reasons.append(f"Vol {last_vol:,.0f}>{TURBO_VOLUME_SPIKE_THRESHOLD:.1f}x{avg_vol:,.0f}")
            else: fail=("Turbo", f"Vol {last_vol:,.0f}<={TURBO_VOLUME_SPIKE_THRESHOLD:.1f}xAvg {avg_vol:,.0f}")
        else: return None,None,None,("Turbo", f"Invalid Vol Data (Last:{last_vol}, Avg:{avg_vol})")

        # --- Price Surge ---
        if len(bars_1m_df) > TURBO_PRICE_SURGE_LOOKBACK_MIN: # Needs lookback+1 bars total
            try: price_ago=bars_1m_df['close'].iloc[-(TURBO_PRICE_SURGE_LOOKBACK_MIN+1)];
            except IndexError: price_ago = None
            if price_ago is not None and not pd.isna(price_ago) and price_ago > 0:
                 roc=(current_price-price_ago)/price_ago;
                 if roc > TURBO_PRICE_SURGE_THRESHOLD: surge=True; reasons.append(f"Surge {roc*100:.1f}%>{TURBO_PRICE_SURGE_THRESHOLD*100:.1f}%")
                 elif fail is None: fail=("Turbo", f"Surge {roc*100:.1f}%<={TURBO_PRICE_SURGE_THRESHOLD*100:.1f}%")
            elif fail is None: fail=("Turbo", "Invalid Price_ago for Surge")
        elif fail is None: fail=("Turbo", f"Not enough bars for Surge ({len(bars_1m_df)}<{TURBO_PRICE_SURGE_LOOKBACK_MIN+1})")

        # --- Recent High Break ---
        if len(bars_1m_df) > TURBO_RECENT_HIGH_LOOKBACK_MIN: # Needs lookback+1 bars total
             # Look back from index -2 (previous bar's high) for TURBO_RECENT_HIGH_LOOKBACK_MIN bars
             lookback_slice = bars_1m_df['high'].iloc[-(TURBO_RECENT_HIGH_LOOKBACK_MIN + 1):-1]
             if not lookback_slice.empty:
                 try: r_high=lookback_slice.max();
                 except Exception: r_high=None
                 if r_high is not None and not pd.isna(r_high) and current_price > r_high:
                      break_high=True; reasons.append(f"Break >{r_high:.2f}")
                 elif fail is None: fail=("Turbo", f"No Break>{r_high:.2f}")
             elif fail is None: fail=("Turbo", "Empty slice for Recent High")
        elif fail is None: fail=("Turbo", f"Not enough bars for High ({len(bars_1m_df)}<{TURBO_RECENT_HIGH_LOOKBACK_MIN+1})")

        # --- Check Conditions ---
        cond=sum([vol_spike, surge, break_high])
        if cond>=TURBO_MIN_CONDITIONS:
            logging.info(f"Turbo Trigger: {symbol} - Conditions Met: {cond}/{TURBO_MIN_CONDITIONS}. Reasons: {'. '.join(reasons)}");
            return strat, bias, ". ".join(reasons), None
        else:
            final_fail_reason = fail if fail else ("Turbo", f"MinCond Fail({cond}/{TURBO_MIN_CONDITIONS})")
            # logging.debug(f"{symbol}: Turbo fail: {final_fail_reason}") # Keep minimal unless debugging
            return None,None,None, final_fail_reason
    except Exception as e: logging.error(f"{symbol}: Turbo Err: {e}", exc_info=True); return None,None,None,("Turbo", "Exception")


# --- V2: Strategy Selector ---
def strategy_selector(symbol: str, asset_info: dict, bars_5m_df: pd.DataFrame, current_price: float, bars_1m_df: pd.DataFrame | None) -> tuple[str | None, str | None, str | None, tuple | None]:
    fail_turbo=None; fail_5m=None

    # --- Try Turbo Strategy First (if 1m data available) ---
    if bars_1m_df is not None and not bars_1m_df.empty:
        strat_t, bias_t, reason_t, fail_turbo = turbo_breaks_up_strategy(symbol, bars_1m_df, current_price)
        if strat_t:
            return strat_t, bias_t, reason_t, None # Turbo triggered, return immediately

    # --- If Turbo didn't trigger, proceed with 5m strategies ---
    if bars_5m_df is None or bars_5m_df.empty: return None,None,None,("Data Check", "5m Bars Missing/Empty")
    min_bars=max(EMA_LONG,RSI_PERIOD,MACD_SLOW+MACD_SIGNAL,ATR_PERIOD)+5 # Need enough bars for longest indicator + buffer
    if len(bars_5m_df)<min_bars: return None,None,None,("Data Check",f"Not enough 5m bars({len(bars_5m_df)}<{min_bars})")

    # Define expected column names
    ema_s_col=f'EMA_{EMA_SHORT}'; ema_l_col=f'EMA_{EMA_LONG}'; rsi_col=f'RSI_{RSI_PERIOD}'
    # ATR check happens in risk manager, not strategy selection logic itself
    vwap_col = f'VWAP_{RSI_PERIOD}' # Must match calculate_indicators
    macd_l_col=f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'; macd_s_col=f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'

    try: # Access indicators safely
        latest=bars_5m_df.iloc[-1]; prev=bars_5m_df.iloc[-2] if len(bars_5m_df)>=2 else latest

        # Use .get() to avoid KeyError if columns are missing after failed calculation
        ema_s=latest.get(ema_s_col); ema_l=latest.get(ema_l_col); rsi=latest.get(rsi_col)
        vwap=latest.get(vwap_col); macd_l=latest.get(macd_l_col); macd_s=latest.get(macd_s_col)

        # Check essential indicators for calculation success (handle None or NaN)
        essentials_check = { ema_s_col: ema_s, ema_l_col: ema_l, rsi_col: rsi, macd_l_col: macd_l, macd_s_col: macd_s }
        missing_essentials = [name for name, val in essentials_check.items() if val is None or pd.isna(val)]
        if missing_essentials:
             return None, None, None, ("Indicator Check", f"Missing 5m: {', '.join(missing_essentials)}")

        # Handle potentially missing/NaN VWAP (use current price as fallback for checks)
        if vwap is None or pd.isna(vwap): vwap = current_price

    except IndexError: return None,None,None, ("Data Access", "IndexError accessing 5m bars")
    except Exception as e: return None,None,None,("Data Access", f"Indicator access error: {e}")

    # --- Evaluate SHORT Strategies ---
    if asset_info.get('shortable', False) and asset_info.get('easy_to_borrow', False): # Check ETB too
        # Downward Dog (Mean Reversion Short)
        if rsi > RSI_OVERBOUGHT and current_price < vwap and current_price < ema_s and latest['high'] > vwap:
            return "Downward Dog", "SHORT", f"RSI {rsi:.1f}>{RSI_OVERBOUGHT}, <VWAP/EMA_S", None
        if fail_5m is None and rsi <= RSI_OVERBOUGHT: fail_5m = ("Downward Dog", f"RSI {rsi:.1f}<={RSI_OVERBOUGHT}")

        # Downward Momentum (Trend Following Short - Rejection)
        if current_price < ema_l and ema_s < ema_l and macd_l < macd_s: # Basic downtrend conditions
            # Check for rejection near EMA_S or VWAP
            rej_ema = latest['high'] >= ema_s * 0.995 and current_price < ema_s * 0.99
            rej_vwap = latest['high'] >= vwap * 0.995 and current_price < vwap * 0.99
            if rej_ema or rej_vwap:
                reason = "5m Downtrend Rej EMA" if rej_ema else "5m Downtrend Rej VWAP"
                return "Downward Momentum", "SHORT", reason, None
            elif fail_5m is None: fail_5m = ("Downward Momentum", "Downtrend but no rej")
        elif fail_5m is None: fail_5m = ("Downward Momentum", f"Criteria fail (Px:{current_price:.2f}<EL:{ema_l:.2f}?, ES:{ema_s:.2f}<EL:{ema_l:.2f}?, ML:{macd_l:.3f}<MS:{macd_s:.3f}?)")

    elif fail_5m is None: fail_5m = ("Short Check", "Not shortable/ETB")


    # --- Evaluate LONG Strategies ---
    # Upward Dog (Mean Reversion Long)
    if rsi < RSI_OVERSOLD and current_price > vwap and current_price > ema_s and latest['low'] < vwap:
        return "Upward Dog", "LONG", f"RSI {rsi:.1f}<{RSI_OVERSOLD}, >VWAP/EMA_S", None
    if fail_5m is None and rsi >= RSI_OVERSOLD: fail_5m = ("Upward Dog", f"RSI {rsi:.1f}>={RSI_OVERSOLD}")

    # Upward Momentum (Trend Following Long - Bounce)
    if current_price > ema_l and ema_s > ema_l and macd_l > macd_s: # Basic uptrend conditions
        # Check for bounce near EMA_S or VWAP
        bounce_ema = latest['low'] <= ema_s * 1.005 and current_price > ema_s * 1.01
        bounce_vwap = latest['low'] <= vwap * 1.005 and current_price > vwap * 1.01
        if bounce_ema or bounce_vwap:
             reason = "5m Uptrend Bounce EMA" if bounce_ema else "5m Uptrend Bounce VWAP"
             return "Upward Momentum", "LONG", reason, None
        elif fail_5m is None: fail_5m = ("Upward Momentum", "Uptrend but no bounce")
    elif fail_5m is None: fail_5m = ("Upward Momentum", f"Criteria fail (Px:{current_price:.2f}>EL:{ema_l:.2f}?, ES:{ema_s:.2f}>EL:{ema_l:.2f}?, ML:{macd_l:.3f}>MS:{macd_s:.3f}?)")


    # --- No strategy triggered ---
    # Prioritize 5m failure reason if it exists, otherwise use Turbo failure reason (if any)
    final_fail = fail_5m or fail_turbo or ("Conditions", "No 5m/Turbo criteria met");
    # logging.debug(f"{symbol}: No strategy trigger. Final Reason: {final_fail}") # Keep minimal unless debugging
    return None, None, None, final_fail


# --- Entry Trigger ---
def entry_trigger(symbol: str, strategy: str, bias: str, reason: str, current_price: float, bars_df: pd.DataFrame) -> bool:
    """Confirms entry conditions just before execution."""
    # Turbo strategy bypasses this secondary check (already confirmed by its own logic)
    if strategy=="Turbo Breaks Up":
        logging.info(f"{symbol}: Trigger CONFIRMED (Bypass Check) {bias} '{strategy}'."); return True

    if bars_df is None or bars_df.empty: logging.warning(f"{symbol}: Trigger FAIL {strategy}({bias}). Empty 5m bars for final check."); return False

    logging.debug(f"{symbol}: Final trigger check {bias} '{strategy}' @ ${current_price:.2f}")
    try:
        latest=bars_df.iloc[-1];
        # Define column names needed for checks
        vwap_col=f'VWAP_{RSI_PERIOD}'; ema_s_col=f'EMA_{EMA_SHORT}'; rsi_col=f'RSI_{RSI_PERIOD}'

        # Use .get with default to handle potentially missing indicator columns
        vwap=latest.get(vwap_col); ema_s=latest.get(ema_s_col); rsi=latest.get(rsi_col)

        # Fallback if indicators are missing/NaN (treat as non-confirming usually)
        if vwap is None or pd.isna(vwap): vwap=current_price
        if ema_s is None or pd.isna(ema_s): ema_s=current_price

        # --- Bias-Specific Checks ---
        if bias=="SHORT":
            # Price shouldn't have moved significantly above key levels since selection
            if current_price >= vwap or current_price >= ema_s:
                 # Allow Downward Dog if RSI is still high and price near recent high (not run away)
                 if strategy == "Downward Dog" and rsi is not None and not pd.isna(rsi) and rsi > RSI_OVERBOUGHT - 5 and current_price < latest['high'] * 1.01:
                     pass # Allow trigger
                 else:
                     logging.info(f"{symbol}: Trigger FAIL {strategy}({bias}). Px {current_price:.2f} >= VWAP {vwap:.2f} or EMA_S {ema_s:.2f}.")
                     return False
        elif bias=="LONG":
            # Price shouldn't have moved significantly below key levels since selection
            if current_price <= vwap or current_price <= ema_s:
                 # Allow Upward Dog if RSI is still low and price near recent low (not run away)
                 if strategy == "Upward Dog" and rsi is not None and not pd.isna(rsi) and rsi < RSI_OVERSOLD + 5 and current_price > latest['low'] * 0.99:
                     pass # Allow trigger
                 else:
                     logging.info(f"{symbol}: Trigger FAIL {strategy}({bias}). Px {current_price:.2f} <= VWAP {vwap:.2f} or EMA_S {ema_s:.2f}.")
                     return False

        logging.info(f"{symbol}: Trigger CONFIRMED {bias} '{strategy}'.")
        return True
    except IndexError: logging.error(f"{symbol}: Trigger FAIL {strategy}({bias}). IndexError accessing latest bar."); return False
    except Exception as e: logging.error(f"{symbol}: Error entry_trigger {strategy}({bias}): {e}", exc_info=True); return False


# --- Risk Manager ---
def risk_manager(symbol: str, strategy: str, bias: str, entry_price: float, bars_df: pd.DataFrame) -> tuple[float, float, float] | None:
    """Calculates position size, stop loss, and take profit, ensuring ATR exists and is valid."""
    logging.debug(f"{symbol}: Risk calc {bias} {strategy} @ ${entry_price:.2f}")
    if entry_price <= 0 or bars_df is None or bars_df.empty:
        logging.warning(f"{symbol}: Risk calc failed - Invalid entry price ({entry_price}) or missing/empty bars_df.")
        return None

    # Calculate Quantity first (independent of ATR)
    try:
        qty = math.floor(POSITION_SIZE_USD / entry_price)
    except ZeroDivisionError:
         logging.error(f"{symbol}: Risk calc failed - Entry price is zero.")
         return None
    if qty <= 0:
        logging.warning(f"{symbol}: Risk calc failed - Calculated quantity is zero or negative ({qty}) for price {entry_price:.2f}.")
        return None

    # --- Check for ATR Column and Value ---
    atr_col = f'ATR_{ATR_PERIOD}'
    if atr_col not in bars_df.columns:
        logging.error(f"{symbol}: ATR column '{atr_col}' missing in DataFrame for risk calculation! Columns: {bars_df.columns}")
        return None

    try:
        latest = bars_df.iloc[-1]
        atr = latest.get(atr_col) # Use .get for safety

        # Validate ATR Value
        if atr is None or pd.isna(atr) or not isinstance(atr, (int, float)) or atr <= 0:
            # Attempt to use previous bar's ATR as fallback
            if len(bars_df) >= 2:
                prev_atr = bars_df[atr_col].iloc[-2]
                if prev_atr is not None and not pd.isna(prev_atr) and isinstance(prev_atr, (int, float)) and prev_atr > 0:
                    logging.warning(f"{symbol}: Latest ATR invalid ({atr}). Using previous bar's ATR: {prev_atr:.3f}")
                    atr = prev_atr
                else:
                     logging.error(f"{symbol}: Invalid latest ATR ({atr}) and invalid previous ATR ({prev_atr}). Cannot calculate stops/targets.")
                     return None
            else:
                logging.error(f"{symbol}: Invalid latest ATR ({atr}) and no previous bar available. Cannot calculate stops/targets.")
                return None

        # --- Calculate SL and TP ---
        sl = None; tp = None
        # Minimum distance for SL/TP based on ATR
        min_risk_dist = atr * MIN_RISK_ATR_MULT
        if min_risk_dist <= 0: # Ensure minimum risk distance is positive
             logging.warning(f"{symbol}: Minimum risk distance ({min_risk_dist:.3f}) based on ATR ({atr:.3f}) is not positive. Using fallback.")
             min_risk_dist = max(entry_price * 0.001, 0.01) # Fallback: 0.1% of price or 1 cent

        if bias == "LONG":
            initial_sl = entry_price - (atr * INITIAL_STOP_ATR_MULT_LONG)
            initial_tp = entry_price + (atr * PROFIT_TARGET_ATR_MULT)
            # Ensure SL is below entry and at least min_risk_dist away
            sl = min(initial_sl, entry_price - min_risk_dist)
            # Ensure TP is above entry and at least min_risk_dist away
            tp = max(initial_tp, entry_price + min_risk_dist)
        elif bias == "SHORT":
            initial_sl = entry_price + (atr * INITIAL_STOP_ATR_MULT_SHORT)
            initial_tp = entry_price - (atr * PROFIT_TARGET_ATR_MULT)
            # Ensure SL is above entry and at least min_risk_dist away
            sl = max(initial_sl, entry_price + min_risk_dist)
            # Ensure TP is below entry and at least min_risk_dist away
            tp = min(initial_tp, entry_price - min_risk_dist)
        else:
            logging.error(f"{symbol}: Invalid bias '{bias}' in risk_manager.")
            return None

        # Final checks and rounding (to 2 decimal places for $)
        if sl <= 0 or tp <= 0:
            logging.warning(f"{symbol}: Risk calc resulted in non-positive SL ({sl}) or TP ({tp}). Entry: {entry_price:.2f}, ATR: {atr:.3f}. Skipping.")
            return None
        # Ensure SL and TP make sense relative to entry for the bias
        if (bias == "LONG" and (sl >= entry_price or tp <= entry_price)) or \
           (bias == "SHORT" and (sl <= entry_price or tp >= entry_price)):
            logging.warning(f"{symbol}: Risk calc produced invalid SL/TP relative to entry. E:{entry_price:.2f}, SL:{sl:.2f}, TP:{tp:.2f}, Bias:{bias}, ATR:{atr:.3f}. Skipping.")
            return None

        sl = round(sl, 2); tp = round(tp, 2)

        logging.info(f"{symbol}({bias} {strategy}): Qty={int(qty)}, Entry=${entry_price:.2f}, SL=${sl:.2f}, Tgt=${tp:.2f} (ATR:{atr:.3f})")
        return float(qty), sl, tp

    except IndexError:
         logging.error(f"{symbol}: IndexError accessing latest/previous bar in risk_manager (DF shape: {bars_df.shape}).")
         return None
    except Exception as e:
        logging.error(f"{symbol}: Error in risk_manager: {e}", exc_info=True)
        return None


# === V2 Main Trading Cycle === #
async def run_strategy_cycle(bot: Bot):
    global state, cycle_stats, failed_candidates_details
    cycle_stats.clear(); failed_candidates_details.clear(); start_cycle_time=time.monotonic()
    logging.info("--- Starting V2 Strategy Cycle ---")
    now_utc=datetime.now(timezone.utc); now_est=now_utc.astimezone(EST)
    logging.info(f"Time: {now_utc.isoformat()} UTC / {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    is_market_open=False; cycle_duration = 0.0
    try:
        clock=rest_client.get_clock(); is_market_open=clock.is_open
        logging.info(f"Market: {'OPEN' if is_market_open else 'CLOSED'} (Next Open: {clock.next_open.isoformat() if not is_market_open else 'N/A'}, Next Close: {clock.next_close.isoformat() if is_market_open else 'N/A'})")
        cycle_stats['00_MarketIsOpen']=int(is_market_open)
    except Exception as e: logging.error(f"Clock error: {e}"); await send_telegram_message(bot, f"⚠️ Clock error: {e}", parse_mode=None)

    # --- Position Management (Runs when market is open, or briefly after close) ---
    market_close_dt_est=datetime.combine(now_est.date(), dt_time(16,0,0), tzinfo=EST) # Standard 4 PM EST close
    grace_minutes=15 # Manage positions for 15 mins after official close
    should_manage=is_market_open or (now_est > market_close_dt_est and now_est < market_close_dt_est + timedelta(minutes=grace_minutes))

    if should_manage:
        open_pos_before=state.get("open_positions",{}).copy(); # Copy for safe iteration & logging
        symbols_in_state=list(open_pos_before.keys()); alpaca_pos={}; state_changed=False

        if symbols_in_state:
            logging.info(f"Managing {len(symbols_in_state)} positions in state...");
            # --- Sync with Alpaca Positions ---
            try:
                fetched_positions:list[Position]=rest_client.get_all_positions();
                alpaca_pos={p.symbol:p for p in fetched_positions}
                logging.info(f"Fetched {len(alpaca_pos)} positions from Alpaca API.")
            except APIError as e: logging.error(f"API Error fetching positions: {e}")
            except Exception as e: logging.error(f"Failed fetch positions: {e}")

            symbols_to_remove_from_state=[]
            current_state_keys=list(state.get("open_positions",{}).keys()) # Get current keys inside loop

            for sym in current_state_keys:
                if sym not in alpaca_pos:
                    logging.warning(f"{sym}: Position no longer found in Alpaca account. Removing from state.")
                    symbols_to_remove_from_state.append(sym)
                else:
                    # Optional: Check quantity consistency (handle potential floats)
                    try:
                        state_qty = float(state["open_positions"][sym].get('qty', 0))
                        alpaca_qty = float(alpaca_pos[sym].qty)
                         # Check if sides match (long state = positive alpaca qty, short state = negative alpaca qty)
                        state_bias = state["open_positions"][sym].get('bias')
                        alpaca_side_match = (state_bias == "LONG" and alpaca_qty > 0) or \
                                            (state_bias == "SHORT" and alpaca_qty < 0)
                        # Use absolute values for quantity comparison
                        if not math.isclose(abs(state_qty), abs(alpaca_qty), rel_tol=1e-5) or not alpaca_side_match:
                            logging.warning(f"{sym}: Discrepancy! State Qty: {state_qty} ({state_bias}), Alpaca Qty: {alpaca_qty}. Removing from state for safety.")
                            symbols_to_remove_from_state.append(sym)
                    except (ValueError, TypeError, KeyError) as comp_e:
                         logging.warning(f"Error comparing state/Alpaca qty for {sym}: {comp_e}. Removing from state.")
                         symbols_to_remove_from_state.append(sym)


            if symbols_to_remove_from_state:
                async with state_lock:
                    for sym in symbols_to_remove_from_state: state["open_positions"].pop(sym, None)
                    state_changed = True
                # Update symbols_in_state after removal
                symbols_in_state = list(state.get("open_positions", {}).keys());
                logging.info(f"State sync complete. {len(symbols_in_state)} positions remain in state after sync.")

            # --- Calculate ATR for remaining positions ---
            atrs={};
            if symbols_in_state:
                logging.debug(f"Fetching 5m bars for ATR calc for {len(symbols_in_state)} open positions.")
                try:
                    # Fetch enough bars for ATR calculation + buffer
                    atr_start_time = now_utc - timedelta(minutes=5 * (ATR_PERIOD + 50)) # Look back enough for ATR period + buffer
                    # Increased limit significantly to ensure enough data even with gaps
                    bars_for_atr = get_bars_data(symbols_in_state, TimeFrame(5, TimeFrameUnit.Minute), start=atr_start_time, end=now_utc, limit=ATR_PERIOD + 50)

                    for sym, df in bars_for_atr.items():
                        if df is not None and not df.empty:
                            # Calculate ONLY ATR for efficiency
                            df_with_atr = calculate_indicators(df.copy(), calculate_atr=True, calculate_other_ta=False)
                            atr_col = f'ATR_{ATR_PERIOD}'
                            # Check last value isn't NaN and is positive
                            if atr_col in df_with_atr.columns and not df_with_atr.empty and not pd.isna(df_with_atr[atr_col].iloc[-1]):
                                atr_val = df_with_atr[atr_col].iloc[-1]
                                if isinstance(atr_val, (int, float)) and atr_val > 0:
                                     atrs[sym] = atr_val
                                     logging.debug(f"Calculated ATR for open pos {sym}: {atr_val:.3f}")
                                else:
                                     logging.warning(f"Calculated ATR for open pos {sym} is not positive or invalid type ({atr_val}).")
                            else:
                                last_atr_val = df_with_atr.get(atr_col, pd.Series(dtype=float)).iloc[-1] if atr_col in df_with_atr.columns and not df_with_atr.empty else 'N/A'
                                logging.warning(f"Failed to get valid latest ATR for open pos {sym}. ATR col present: {atr_col in df_with_atr.columns}. Last ATR value: {last_atr_val}")
                        else:
                             logging.warning(f"No 5m bars returned/processed for ATR calc for open pos {sym}.")
                except Exception as e: logging.error(f"Error fetch/calc ATR for open positions: {e}", exc_info=True)

            # --- Check Exits & Update Trailing Stops ---
            if symbols_in_state:
                prices=get_latest_prices_batched(symbols_in_state); to_close={}; updated_ts_count = 0
                async with state_lock:
                    # Iterate over a copy of keys, as we might delete from the dict
                    keys_now=list(state.get("open_positions",{}).keys())
                    for sym in keys_now:
                        # Re-check if symbol still exists in case removed during sync
                        if sym not in state["open_positions"]: continue
                        info=state["open_positions"][sym]; px=prices.get(sym)

                        if px is None: logging.warning(f"No current price for open pos {sym}, cannot manage."); continue

                        bias=info.get('bias'); sl=info.get('stop_loss'); tgt=info.get('target')
                        ts=info.get('trailing_stop'); # This should exist after load_state fix
                        exit_reason=None;

                        # --- Safely convert SL/TGT/TS to float ---
                        try: sl_f=float(sl) if sl is not None else None
                        except (ValueError, TypeError): sl_f = None; logging.warning(f"Invalid Initial SL format for {sym}: {sl}")
                        try: tgt_f=float(tgt) if tgt is not None else None
                        except (ValueError, TypeError): tgt_f = None; logging.warning(f"Invalid Target format for {sym}: {tgt}")
                        try: ts_f=float(ts) if ts is not None else None # Current Trailing Stop
                        except (ValueError, TypeError): ts_f = None; logging.warning(f"Invalid Trailing Stop format for {sym}: {ts}")

                        # Initialize trailing stop if it's still missing (e.g., older state file) or invalid
                        if ts_f is None and sl_f is not None:
                             state["open_positions"][sym]['trailing_stop'] = sl_f # Initialize TS with initial SL
                             ts_f = sl_f # Update ts_f for current logic
                             state_changed = True
                             logging.info(f"{sym}({bias}): Initialized Trailing Stop @ Initial SL: ${ts_f:.2f}")
                        elif ts_f is None and sl_f is None:
                             logging.error(f"{sym}({bias}): Cannot manage position - Initial SL and Trailing Stop are both invalid/missing!")
                             continue # Skip management for this symbol if no valid stop exists

                        # --- Update trailing stop based on ATR ---
                        atr=atrs.get(sym); new_ts = ts_f # Default to current ts
                        if atr is not None and atr > 0 and ts_f is not None: # Ensure current TS is valid before update
                            if bias=="LONG":
                                potential_ts = px - (atr * TRAILING_STOP_ATR_MULT_LONG)
                                # Trailing stop can only move up (or stay) for longs
                                if potential_ts > ts_f: new_ts = round(potential_ts, 2)
                            elif bias=="SHORT":
                                potential_ts = px + (atr * TRAILING_STOP_ATR_MULT_SHORT)
                                # Trailing stop can only move down (or stay) for shorts
                                if potential_ts < ts_f: new_ts = round(potential_ts, 2)

                            # Check if the new TS is significantly different and valid
                            if new_ts != ts_f and not math.isclose(new_ts, ts_f, rel_tol=1e-5):
                                # Sanity check: TS shouldn't cross entry price in the wrong direction
                                entry_f = float(info.get('entry_price', px)) # Get entry price safely
                                if (bias == "LONG" and new_ts < entry_f) or (bias == "SHORT" and new_ts > entry_f):
                                    logging.debug(f"{sym}({bias}): Trail Stop Update ${ts_f:.2f} -> ${new_ts:.2f} (Px: {px:.2f}, ATR: {atr:.3f})")
                                    state["open_positions"][sym]['trailing_stop'] = new_ts
                                    ts_f = new_ts # Update ts_f for exit check below
                                    state_changed = True
                                    updated_ts_count += 1
                                # else: # Optional: Log if TS update skipped due to crossing entry
                                #    logging.debug(f"{sym}({bias}): Proposed TS ${new_ts:.2f} invalid relative to entry ${entry_f:.2f}. Keeping ${ts_f:.2f}.")


                        # --- Check Exit Conditions (using updated ts_f) ---
                        if ts_f is not None: # Check against the potentially updated trailing stop
                             if bias=="LONG" and px <= ts_f: exit_reason=f"Trailing Stop Hit ${px:.2f}<=${ts_f:.2f}"
                             elif bias=="SHORT" and px >= ts_f: exit_reason=f"Trailing Stop Hit ${px:.2f}>=${ts_f:.2f}"

                        if tgt_f is not None and not exit_reason: # Check target only if stop not hit
                             if bias=="LONG" and px >= tgt_f: exit_reason=f"Target Hit ${px:.2f}>=${tgt_f:.2f}"
                             elif bias=="SHORT" and px <= tgt_f: exit_reason=f"Target Hit ${px:.2f}<=${tgt_f:.2f}"

                        # --- Mark for Closure ---
                        if exit_reason:
                            # Use the trailing stop value that triggered the exit for logging
                            final_stop_value = ts_f if "Stop" in exit_reason else None
                            to_close[sym]={'reason':exit_reason, 'final_stop':final_stop_value}
                            if sym in state["open_positions"]:
                                 logging.info(f"Marking {sym} for closure. Reason: {exit_reason}")
                                 del state["open_positions"][sym];
                                 state_changed=True
                            else: logging.warning(f"Tried to mark {sym} for closure, but it was already removed from state.") # Should not happen

                if updated_ts_count > 0: logging.info(f"Updated trailing stops for {updated_ts_count} positions.")

                # --- Execute Closures ---
                if to_close:
                    logging.info(f"Attempting to close {len(to_close)} positions: {list(to_close.keys())}")
                    closed_count = 0; failed_closes = []
                    for sym, data in to_close.items():
                        reason=data['reason']; final_stop = data['final_stop']
                        logging.info(f"Closing {sym} via API. Reason: {reason}")
                        order = close_position_wrapper(sym); # Attempt to close via API
                        # --- Logging Closure ---
                        # Get original trade details from the copy made at the start of management phase
                        orig = open_pos_before.get(sym, {});
                        exit_px=None; pnl=None; oid=None; exit_status = "Submission Failed"

                        if order: # If close_position_wrapper returned an Order object
                            oid=order.id; exit_status = order.status
                            if order.status == OrderStatus.FILLED:
                                exit_px = float(order.filled_avg_price) if order.filled_avg_price else None
                                exit_qty = float(order.filled_qty) if order.filled_qty else None
                                exit_status = OrderStatus.FILLED # Explicitly set for clarity
                                closed_count += 1
                                if exit_px is not None:
                                    entry = orig.get('entry_price'); qty_orig = orig.get('qty'); b = orig.get('bias')
                                    if entry is not None and qty_orig is not None and b is not None:
                                        try:
                                             entry_f=float(entry); qty_f=float(qty_orig); exit_px_f=float(exit_px);
                                             pnl_calc=((exit_px_f-entry_f)*qty_f) if b=="LONG" else ((entry_f-exit_px_f)*qty_f);
                                             pnl = round(pnl_calc, 2)
                                        except (ValueError, TypeError) as calc_e: logging.warning(f"Could not calculate PnL for {sym}: {calc_e}"); pnl = None
                                    else: pnl = None # Missing original data
                            else:
                                logging.warning(f"Close order for {sym} submitted but status is {order.status}. ID: {order.id}")
                                failed_closes.append(sym)
                        elif order is None and reason: # close_position_wrapper returned None (e.g., 404, 422, market closed)
                             # If the reason indicates a stop/target hit, assume it might have closed before/concurrently.
                             # Log based on original data, mark status as 'Assumed Closed'.
                             # If reason was market closed, log status 'Closure Failed (Market Closed)'
                             if "Market closed" in reason:
                                 exit_status = "Closure Failed (Market Closed)"
                                 failed_closes.append(sym)
                             else:
                                 # Assume closed if API reported 404/422 or other non-market reason
                                 exit_status = "Assumed Closed (API Error/Not Found)"
                                 # We can't calculate PNL without a fill confirmation
                                 pnl = None
                                 exit_px = None
                                 closed_count += 1 # Count it as resolved from our state perspective
                        else:
                             # Should not happen if close_position_wrapper logic is correct
                             logging.error(f"Failed to submit close order for {sym} via API. Unknown reason.")
                             failed_closes.append(sym)


                        # --- Log the Trade Closure Attempt ---
                        log_data={
                            'ts_utc': (order.filled_at if (order and order.filled_at) else now_utc).isoformat(), # Use fill time if available
                            'sym': sym, 'bias': orig.get('bias'), 'strat': orig.get('strategy'),
                            'side': 'sell' if orig.get('bias')=='LONG' else 'buy_to_cover',
                            'entry': orig.get('entry_price'), 'qty': orig.get('qty'),
                            'init_sl': orig.get('stop_loss'), 'tgt': orig.get('target'),
                            'final_stop': final_stop, # Log the stop value that triggered the exit attempt
                            'exit_px': exit_px, 'pnl': pnl, 'reason': reason,
                            'status': str(exit_status), # Log the exit order status or assumption
                            'entry_oid': orig.get('order_id'), # Original entry order ID
                            'exit_oid': oid # Exit order ID (if generated)
                        }
                        log_trade_to_jsonl(log_data)

                        # --- Send Telegram Notification ONLY on Confirmed Fill ---
                        if exit_status == OrderStatus.FILLED:
                            pnl_s=f"${pnl:+.2f}" if pnl is not None else "N/A";
                            msg_prefix="✅ V2 Closed (LONG)" if orig.get('bias')=="LONG" else "✅ V2 Covered (SHORT)"
                            alert=(f"{msg_prefix}\n"
                                   f"💰 Symbol: ${html.escape(sym)}\n"
                                   # f"📈 Strategy: {html.escape(orig.get('strategy','Exit'))}\n" # Maybe too verbose
                                   f"👋 Exit @ ${exit_px:.2f}\n"
                                   f"   Reason: {html.escape(reason)}\n"
                                   f"💸 Realized PnL: <b>{pnl_s}</b>")
                            await send_telegram_message(bot, alert, parse_mode=ParseMode.HTML) # Use HTML default
                        elif str(exit_status) not in ["Assumed Closed (API Error/Not Found)"]: # Don't alert for assumed closures without fills or failed submissions
                            alert = f"⚠️ Close order for ${html.escape(sym)} status: {html.escape(str(exit_status))}. Reason: {html.escape(reason)}"
                            await send_telegram_message(bot, alert, parse_mode=ParseMode.HTML)


                    logging.info(f"Finished closing attempts. Confirmed closed: {closed_count}. Failed/Pending: {len(failed_closes)}.")
                    if failed_closes: logging.warning(f"Failed/Pending closure symbols: {failed_closes}")

        # --- Save State if Changed ---
        if state_changed:
            await asyncio.to_thread(save_state, state);
            logging.info("State saved after position management.")
    else:
        logging.info("Market closed and outside grace period, skipping position management.")
        # Optional: Perform a quick sync even when closed to remove positions closed manually/after hours
        try:
            open_pos_in_state = list(state.get("open_positions", {}).keys())
            if open_pos_in_state:
                 fetched_positions:list[Position]=rest_client.get_all_positions()
                 alpaca_pos_symbols = {p.symbol for p in fetched_positions}
                 removed_count = 0
                 async with state_lock:
                     state_changed_closed_market = False
                     for sym in open_pos_in_state:
                         if sym not in alpaca_pos_symbols:
                             logging.info(f"(Market Closed Sync) Position {sym} not found in Alpaca. Removing from state.")
                             state["open_positions"].pop(sym, None)
                             state_changed_closed_market = True
                             removed_count += 1
                     if state_changed_closed_market:
                         await asyncio.to_thread(save_state, state)
                         logging.info(f"(Market Closed Sync) Removed {removed_count} positions from state. Saved.")

        except Exception as sync_err:
             logging.warning(f"Error during closed-market state sync: {sync_err}")


    # --- V2 Scan for New Entries (Only if Market is Open) ---
    potential_trades = []; evaluated_count = 0;
    if is_market_open:
        logging.info("V2: Scanning for new entries...")
        try: # Main Scan Try Block
            assets = get_active_tradable_assets() # Gets filtered, tradable, simple US equities
            basic_filtered_symbols = list(assets.keys())
            if not basic_filtered_symbols: raise Exception("No symbols after basic asset filters")
            logging.info(f"{cycle_stats.get('02_AfterBasicFilters', 0)} symbols passed basic filters.")

            # Filter by Today's Volume
            todays_volumes = get_todays_volumes(basic_filtered_symbols)
            volume_filtered = [s for s in basic_filtered_symbols if todays_volumes.get(s, 0) >= MIN_TODAY_VOLUME]
            cycle_stats['03_AfterVolumeFilter'] = len(volume_filtered);
            logging.info(f"{len(volume_filtered)} symbols passed TODAY volume filter (>={MIN_TODAY_VOLUME}).")

            if not volume_filtered:
                 logging.info("No symbols passed today's volume filter. Skipping candidate evaluation.")
            else:
                # Filter out already held positions
                async with state_lock: held = set(state.get("open_positions", {}).keys())
                candidates = [s for s in volume_filtered if s not in held]
                cycle_stats['04_AfterHeldFilter'] = len(candidates);
                logging.info(f"Screening {len(candidates)} candidates (not currently held).")

                if not candidates: logging.info("No candidates left after held filter.")
                else:
                    # Get Latest Prices
                    prices = get_latest_prices_batched(candidates); cycle_stats['05_GotLatestPrices'] = len(prices)
                    priced_candidates = list(prices.keys()); cycle_stats['05a_CandidatesWithPrice'] = len(priced_candidates)
                    if not priced_candidates: logging.info("No candidates have latest price data.")
                    else:
                        logging.info(f"Fetching 1m & 5m bars for {len(priced_candidates)} candidates with price...")
                        # Fetch Bars Data (Fetch 1m first for Turbo, then 5m for others)
                        bars_1m = get_bars_data(priced_candidates,TimeFrame.Minute,start=now_utc-timedelta(hours=3),end=now_utc,limit=120) # More 1m data
                        bars_5m = get_bars_data(priced_candidates,TimeFrame(5,TimeFrameUnit.Minute),start=now_utc-timedelta(days=5),end=now_utc,limit=300) # Keep 5m lookback reasonable
                        cycle_stats['06_Got5mBars']=len(bars_5m); cycle_stats['07_Got1mBars']=len(bars_1m)

                        logging.info(f"Evaluating {len(priced_candidates)} candidates...")
                        evaluated_count = 0 # Reset count for this stage
                        potential_trades.clear()
                        failed_candidates_details.clear() # Clear failure details for this cycle

                        for sym in priced_candidates:
                            asset_info=assets.get(sym); b5m=bars_5m.get(sym); b1m=bars_1m.get(sym); px=prices.get(sym)

                            # --- Pre-evaluation Checks ---
                            if px is None: failed_candidates_details[sym]=("Data Check","Missing Price"); continue
                            if asset_info is None: failed_candidates_details[sym]=("Data Check","Missing Asset Info"); continue
                            # 5m bars are essential for main strategies and risk management ATR
                            if b5m is None or b5m.empty: failed_candidates_details[sym]=("Data Check","Missing/Empty 5m Bars"); continue
                            # 1m bars are optional (needed only for Turbo), handled inside selector

                            # --- Calculate Indicators (on 5m bars) ---
                            logging.debug(f"Calculating indicators for {sym} (5m bars shape: {b5m.shape if b5m is not None else 'None'})")
                            b5m_indic = calculate_indicators(b5m.copy()) # Use copy to avoid modifying cache/original DF

                            if b5m_indic.empty:
                                 logging.warning(f"Indicator calculation for {sym} resulted in an empty DataFrame.")
                                 failed_candidates_details[sym]=("Indicator Calc","Empty DF Result"); continue
                            # Check if ATR calculation succeeded for risk management later
                            atr_col = f'ATR_{ATR_PERIOD}'
                            if atr_col not in b5m_indic.columns or b5m_indic[atr_col].isnull().all():
                                 logging.warning(f"ATR calculation failed or missing for {sym}. Cannot proceed.")
                                 failed_candidates_details[sym]=("Indicator Calc", "ATR Failed/Missing"); continue


                            # --- Strategy Selection (using 5m indicators and 1m bars if available) ---
                            evaluated_count+=1
                            strat, bias, reason, fail_reason = strategy_selector(sym, asset_info, b5m_indic, px, b1m)

                            if strat:
                                # --- Entry Trigger Check (final confirmation) ---
                                if entry_trigger(sym, strat, bias, reason, px, b5m_indic):
                                    # --- Risk Management (calculate SL/TP/Qty) ---
                                    risk = risk_manager(sym, strat, bias, px, b5m_indic)
                                    if risk:
                                        qty_calc, sl_calc, tp_calc = risk
                                        potential_trades.append({
                                            "symbol":sym,"strategy":strat,"bias":bias,"reason":reason,
                                            "entry_price":px,"qty":qty_calc,"stop_loss":sl_calc,"target":tp_calc
                                        })
                                        logging.info(f"Potential Trade FOUND: {sym} ({bias} {strat} - {reason}) Qty:{int(qty_calc)} SL:{sl_calc} TP:{tp_calc}")
                                    else:
                                        logging.warning(f"{sym}: {strat}({bias}) triggered but Risk Manager failed.")
                                        failed_candidates_details[sym]=(strat,"Risk Fail")
                                else:
                                    # Entry trigger failed final check
                                    # logging.info(f"{sym}: {strat}({bias}) selected but Entry Trigger failed final check.") # Already logged in entry_trigger
                                    failed_candidates_details[sym]=(strat,"Trigger Fail")
                            elif fail_reason:
                                # Strategy selector did not find a match
                                # logging.debug(f"{sym}: No strategy trigger. Reason: {fail_reason}") # Keep minimal
                                failed_candidates_details[sym]=fail_reason # Store first failure reason encountered

                        cycle_stats['08_Evaluated']=evaluated_count; cycle_stats['09_PotentialTrades']=len(potential_trades);
                        logging.info(f"Evaluation complete. Evaluated: {evaluated_count}. Potential Trades: {len(potential_trades)}.")
        except Exception as e:
            logging.error(f"Scan Phase Error: {e}", exc_info=True);
            await send_telegram_message(bot, f"🚨 Scan Error: {html.escape(str(e))}", parse_mode=None) # Send error via TG
    else:
        logging.info("Market closed, skipping scan for new entries.")


    # --- Execute Trades (Only if Market is Open & potential trades found) ---
    executed_count = 0
    if potential_trades and is_market_open:
        logging.info(f"Market OPEN. Attempting to execute {len(potential_trades)} potential trades...")
        trades_executed_symbols = set() # Track symbols executed in this cycle

        for trade in potential_trades:
            sym=trade['symbol']; bias=trade['bias']; qty=trade['qty']; strat=trade['strategy']
            reason = trade['reason']; sl = trade['stop_loss']; target = trade['target']
            entry_px_estimate = trade['entry_price'] # Price at time of signal
            alert_msg=None

            # Prevent duplicate executions within the same cycle if signal repeats
            if sym in trades_executed_symbols:
                logging.warning(f"Skipping duplicate execution attempt for {sym} in this cycle.")
                continue

            # Double-check state lock just before executing trade
            async with state_lock:
                if sym in state.get("open_positions",{}):
                    logging.warning(f"Skipping execution for {sym}, position already exists in state (race condition?).")
                    continue

                # --- Execute Trade via API ---
                order:Order|None = execute_long_trade(sym,qty) if bias=="LONG" else execute_short_trade(sym,qty)

                if order:
                    filled_px = entry_px_estimate # Default to estimate
                    filled_qty = qty
                    entry_t = order.submitted_at # Default to submitted time
                    order_status = order.status

                    # Attempt to get updated order status, handle potential delays/errors
                    try:
                        time.sleep(1.5) # Wait briefly for order propagation/fill
                        filled_order = rest_client.get_order_by_id(order.id)
                        order_status = filled_order.status
                        # Use filled price/qty/time if available and valid
                        if filled_order.filled_avg_price: filled_px = float(filled_order.filled_avg_price)
                        if filled_order.filled_qty: filled_qty = float(filled_order.filled_qty)
                        if filled_order.filled_at: entry_t = filled_order.filled_at
                        logging.info(f"Order {order.id} status update: {order_status}, FilledPx: {filled_px}, FilledQty: {filled_qty}")
                    except (APIError, ValueError, TypeError) as get_e:
                        logging.warning(f"Error getting/processing order {order.id} details after submission: {get_e}. Using submitted data/estimate.")
                    except Exception as get_e:
                        logging.error(f"Unexpected Error getting order {order.id} details: {get_e}. Using submitted data/estimate.", exc_info=True)


                    # --- Update State if Order likely to proceed ---
                    # (Includes states where it might fill shortly, or already filled)
                    accepted_statuses = [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED, OrderStatus.ACCEPTED, OrderStatus.NEW, OrderStatus.PENDING_NEW, OrderStatus.HELD]
                    if order_status in accepted_statuses:
                        executed_count+=1
                        trades_executed_symbols.add(sym) # Mark as executed for this cycle

                        # Use initial SL as the first trailing stop value
                        initial_trailing_stop = sl
                        state["open_positions"][sym]={
                            "entry_price": filled_px, # Use filled price if available, else estimate
                            "qty": filled_qty,    # Use filled qty if available, else original
                            "stop_loss": sl,      # Initial stop loss
                            "trailing_stop": initial_trailing_stop, # Initial trailing stop = initial SL
                            "target": target,
                            "strategy": strat,
                            "bias": bias,
                            "entry_time": entry_t.isoformat() if entry_t else now_utc.isoformat(),
                            "order_id": str(order.id),
                            "status": str(order_status) # Store current known status
                            }
                        logging.info(f"Added {sym} to state. Status: {order_status}. Initial TS: {initial_trailing_stop:.2f}")
                        state_changed = True # Mark state as changed

                        # Log trade details to JSONL
                        log_data={
                            'ts_utc': entry_t.isoformat() if entry_t else now_utc.isoformat(),
                            'sym': sym, 'bias': bias, 'strat': strat,
                            'side': 'buy' if bias=='LONG' else 'sell_short',
                            'entry': filled_px, 'qty': filled_qty,
                            'init_sl': sl, 'tgt': target, 'reason': reason,
                            'oid': str(order.id), 'status': str(order_status)
                            }
                        log_trade_to_jsonl(log_data)

                        # Prepare Telegram alert (using HTML)
                        emoji="📈" if bias=="LONG" else "📉";
                        alert_msg=(f"{emoji} <b>Unholy V2 Trade Submitted ({bias})</b>\n"
                               f"🔍 Strategy: {html.escape(strat)}\n"
                               f"💰 Symbol: <code>${html.escape(sym)}</code>\n"
                               f"   Rationale: <i>{html.escape(reason)}</i>\n"
                               f"🎯 Entry: ~<code>${filled_px:.2f}</code> | Qty: <code>{int(filled_qty)}</code>\n"
                               f"🛡️ SL: <code>${sl:.2f}</code> | Tgt: <code>${target:.2f}</code>\n"
                               f"🚦 Status: <i>{html.escape(str(order_status))}</i>")
                    else:
                        # Handle orders that failed immediately or are in unexpected states
                        logging.error(f"{bias} Trade for {sym} FAILED or unexpected status. ID {order.id}, Status: {order_status}.")
                        alert_msg=f"❌ {bias} trade FAIL <code>${html.escape(sym)}</code>. Status: {html.escape(str(order_status))}"
                else:
                     # Handle case where submit_order itself failed (execute_long/short returned None)
                     logging.error(f"{bias} order submission FAILED for {sym}.")
                     alert_msg=f"❌ {bias} order submission FAILED for <code>${html.escape(sym)}</code>."

            # --- Send Telegram Alert (outside lock) ---
            if alert_msg:
                await send_telegram_message(bot, alert_msg, parse_mode=ParseMode.HTML) # Send trade alerts with HTML
                await asyncio.sleep(0.5) # Small delay between trade alert messages

        # Save state once after all executions in the loop if changed
        if state_changed:
             await asyncio.to_thread(save_state, state)
             logging.info("State saved after trade executions.")

        logging.info(f"Finished trade execution attempts. Executed/Submitted: {executed_count} new trades.")

    elif potential_trades and not is_market_open:
        logging.info(f"Market CLOSED. Holding {len(potential_trades)} potential trades until next open.")
    elif evaluated_count > 0:
         logging.info("Scan complete. No potential trades met all criteria.")
    # Else: No candidates passed filters, already logged earlier.

    cycle_stats['10_ExecutedTrades'] = executed_count


    # --- V2 Cycle Reporting ---
    try:
        end_cycle_time=time.monotonic(); duration=end_cycle_time-start_cycle_time;
        async with state_lock: current_open_pos_count = len(state.get("open_positions",{})) # Get final count

        # Build HTML report
        report=[f"<b>Unholy V2 Cycle Report ({now_est.strftime('%H:%M:%S %Z')})</b>", f"<b>Market Open:</b> {'Yes' if is_market_open else 'No'}"]
        report.append("\n<b>Filter & Scan Stats:</b>")
        report.append(f"- Init Assets: {cycle_stats.get('01_InitialAssetsFetched','N/A')} -> Basic Filters: {cycle_stats.get('02_AfterBasicFilters','N/A')}")
        report.append(f"- Vol Filter: {cycle_stats.get('03_AfterVolumeFilter','N/A')} -> Held Filter: {cycle_stats.get('04_AfterHeldFilter','N/A')}")
        report.append(f"- Candidates w/ Price: {cycle_stats.get('05a_CandidatesWithPrice','N/A')}")
        report.append(f"- Fetched 5m/1m Bars: {cycle_stats.get('06_Got5mBars','N/A')} / {cycle_stats.get('07_Got1mBars','N/A')}")
        report.append(f"- Evaluated Candidates: {cycle_stats.get('08_Evaluated','N/A')}")

        report.append("\n<b>Trade Actions:</b>")
        report.append(f"- Potential New Trades: {cycle_stats.get('09_PotentialTrades',0)}")
        report.append(f"- Executed/Submitted New: {executed_count}")
        report.append(f"- Currently Open Positions: {current_open_pos_count}") # Use final count

        # Report on failed candidates (Close Calls)
        if failed_candidates_details:
            report.append("\n<b>Close Calls / Failures (Top 5):</b>")
            # Sort by symbol for consistency
            sorted_failures = sorted(failed_candidates_details.items())
            for sym, fail_info in sorted_failures[:5]:
                 # Ensure fail_info is a tuple/list with at least two elements
                 if isinstance(fail_info, (list, tuple)) and len(fail_info) >= 2:
                     strat_or_stage, reason = fail_info[0], fail_info[1]
                 elif isinstance(fail_info, (list, tuple)) and len(fail_info) == 1:
                     strat_or_stage, reason = fail_info[0], "Unknown Detail"
                 else: # Handle unexpected format
                     strat_or_stage, reason = "Unknown Stage", str(fail_info)

                 # Escape for HTML
                 safe_sym = html.escape(str(sym)); safe_stage = html.escape(str(strat_or_stage)); safe_reason = html.escape(str(reason))
                 report.append(f"- <code>${safe_sym}</code>: {safe_stage} (<i>{safe_reason}</i>)")
        else:
            if evaluated_count > 0 : report.append("\n<i>No close calls / failures recorded this cycle.</i>")
            # else: report is short, no need for this line if no evaluation happened

        report.append(f"\n<i>Cycle Duration: {duration:.2f}s</i>")
        # Send using HTML parse mode via the helper function
        await send_telegram_message(bot, "\n".join(report), add_to_recent=False, parse_mode=ParseMode.HTML)
    except Exception as report_e: logging.error(f"Report Error: {report_e}", exc_info=True)
    logging.info(f"--- V2 Cycle Finished (Duration: {duration:.2f}s) ---")


# === Async Job Scheduler === #
async def scheduled_job(bot: Bot):
    """Wrapper for the main strategy cycle with top-level error catching."""
    try:
        await run_strategy_cycle(bot)
    except APIError as e:
        logging.critical(f"CRITICAL API ERROR in scheduled_job: {e}", exc_info=True)
        await send_telegram_message(bot, f"🚨 <b>CRITICAL API ERROR:</b> {html.escape(e.__class__.__name__)} - {html.escape(str(e))}", parse_mode=ParseMode.HTML)
        # Consider adding logic to potentially pause or shutdown if API errors persist
    except ConnectionError as e:
        logging.critical(f"CRITICAL Connection ERROR in scheduled_job: {e}", exc_info=True)
        await send_telegram_message(bot, f"🚨 <b>CRITICAL Connection ERROR:</b> {html.escape(e.__class__.__name__)}. Check network/Alpaca status.", parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.critical(f"CRITICAL UNEXPECTED ERROR in scheduled_job: {e}", exc_info=True)
        await send_telegram_message(bot, f"🚨 <b>CRITICAL UNEXPECTED ERROR:</b> {html.escape(e.__class__.__name__)} - {html.escape(str(e))}", parse_mode=ParseMode.HTML)
        # Consider shutdown on critical unknown errors
        # shutdown_event.set()

# === Main Strategy Loop === #
async def main_strategy_loop(bot: Bot):
    """Schedules and runs the strategy cycle repeatedly."""
    logging.info(f"Scheduling V2 cycle to run every {JOB_INTERVAL_MINUTES} minutes.")
    while not shutdown_event.is_set():
        start_time = time.monotonic()
        await scheduled_job(bot)
        end_time = time.monotonic()
        elapsed = end_time - start_time
        sleep_duration = max(0, JOB_INTERVAL_MINUTES * 60 - elapsed)
        logging.info(f"Cycle took {elapsed:.2f}s. Sleeping for {sleep_duration:.2f}s...")
        try:
            # Wait for the sleep duration or until shutdown is triggered
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_duration)
            # If wait() completes without timeout, shutdown was triggered
            logging.info("Shutdown signal received during sleep. Exiting loop.")
            break # Exit the while loop
        except asyncio.TimeoutError:
            pass # Timeout occurred, continue to the next cycle
        except Exception as e:
            logging.error(f"Error during sleep/shutdown check: {e}")
            break # Exit loop on unexpected error during wait
    logging.info("Main strategy loop finished.")


# === Telegram Command Handlers (UPDATED for Enhanced Status & PnL) === #
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logging.info(f"/start command received from user {user.id} ({user.username or user.first_name})")
    await update.message.reply_text(
        f'👋 Hello {user.first_name}!\n\n'
        '<b>Unholy V2 Bot Active.</b>\n\n'
        '<u>Available Commands:</u>\n'
        '🔹 /status - Show open positions & unrealized PnL.\n'
        '🔹 /pnl - Show realized PnL report from closed trades.\n'
        '🔹 /stop_bot - Request bot shutdown gracefully.',
        parse_mode=ParseMode.HTML
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logging.info(f"/status command received from user {user.id} ({user.username or user.first_name})")
    # Send immediate feedback
    processing_message = await update.message.reply_text("<i>Fetching status and current prices... Please wait.</i>", parse_mode=ParseMode.HTML)

    status_lines = ["<b>Unholy V2 Status (Open Positions)</b>"]
    total_unrealized_pnl = 0.0
    position_count = 0
    fetch_error = False

    async with state_lock:
        # Make a copy to work with, preventing modification issues during iteration
        open_positions = state.get("open_positions", {}).copy()
        position_count = len(open_positions)

    if not open_positions:
        status_lines.append("\n<i>No open positions.</i>")
    else:
        symbols = list(open_positions.keys())
        current_prices = {}
        logging.info(f"/status: Fetching prices for {len(symbols)} open positions...")
        try:
            # Fetch latest prices using the existing batched function
            current_prices = get_latest_prices_batched(symbols)
            logging.info(f"/status: Fetched prices for {len(current_prices)}/{len(symbols)} symbols.")
        except Exception as e:
            logging.error(f"Error fetching prices for /status command: {e}", exc_info=True)
            status_lines.append("\n⚠️ <i>Error fetching current prices. PnL calculation may be incomplete.</i>")
            fetch_error = True

        status_lines.append(f"\n<u>Open Positions ({position_count}):</u>")
        # Sort symbols alphabetically for consistent output
        for sym, data in sorted(open_positions.items()):
            pos_status = data.get('status', '?') # Get order status stored in state
            entry_price_str = data.get('entry_price', 'N/A')
            qty_str = data.get('qty', 'N/A')
            bias = data.get('bias', '?')
            strategy = data.get('strategy', 'N/A')
            entry_time_iso = data.get('entry_time')
            ts_price = data.get('trailing_stop', 'N/A') # Get trailing stop price

            # Format entry time nicely if available
            entry_time_str = "N/A"
            if entry_time_iso:
                try: entry_dt = isoparse(entry_time_iso).astimezone(EST); entry_time_str = entry_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                except ValueError: entry_time_str = entry_time_iso # Fallback to ISO string if parsing fails

            sym_safe = html.escape(sym)
            bias_safe = html.escape(bias)
            status_safe = html.escape(str(pos_status)) # Ensure status is string
            strategy_safe = html.escape(strategy)

            unrealized_pnl_str = "N/A"
            if not fetch_error:
                current_px = current_prices.get(sym)
                if current_px is not None:
                    try:
                        entry_f = float(entry_price_str)
                        qty_f = float(qty_str)
                        current_px_f = float(current_px)
                        unrealized = 0.0
                        if bias == "LONG": unrealized = (current_px_f - entry_f) * qty_f
                        elif bias == "SHORT": unrealized = (entry_f - current_px_f) * qty_f

                        total_unrealized_pnl += unrealized
                        unrealized_pnl_str = f"{unrealized:+.2f}"

                    except (ValueError, TypeError, KeyError) as calc_e:
                        logging.warning(f"Could not calculate PnL for {sym} in /status: {calc_e} (Entry: {entry_price_str}, Qty: {qty_str}, Px: {current_px})")
                        unrealized_pnl_str = "Calc Error"
                else:
                    unrealized_pnl_str = "No Price" # Explicitly state if price fetch failed for this sym

            # Format entry price, quantity, and trailing stop safely
            try: entry_formatted = f"{float(entry_price_str):.2f}"
            except (ValueError, TypeError): entry_formatted = str(entry_price_str)
            try: qty_formatted = f"{int(float(qty_str))}"
            except (ValueError, TypeError): qty_formatted = str(qty_str)
            try: ts_formatted = f"{float(ts_price):.2f}" if ts_price not in [None, 'N/A'] else "N/A"
            except (ValueError, TypeError): ts_formatted = str(ts_price)


            status_lines.append(
                f"\n<b>${sym_safe}</b> ({bias_safe}) | Qty: <code>{qty_formatted}</code>\n"
                f"  Strat: <i>{strategy_safe}</i>\n"
                f"  Entry: <code>${entry_formatted}</code> | TS: <code>${ts_formatted}</code>\n"
                f"  Entry Time: <i>{entry_time_str}</i>\n"
                # f"  State Status: <code>{status_safe}</code>\n" # Maybe too technical?
                f"  Unrealized PnL: <code>{unrealized_pnl_str}</code>"
            )

        if not fetch_error:
            status_lines.append(f"\n<b>Total Unrealized PnL:</b> <code>${total_unrealized_pnl:+.2f}</code>")

    # Join lines into the final message
    final_message = "\n".join(status_lines)

    # Try to edit the original "Processing..." message
    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=processing_message.message_id,
            text=final_message,
            parse_mode=ParseMode.HTML
        )
    except telegram.error.BadRequest as e:
        if "Message is not modified" in str(e):
            pass # Ignore if message content is identical
        else:
            logging.error(f"Error editing status message: {e}. Sending new message.")
            # Fallback: Send as a new message if editing fails for other reasons
            await send_telegram_message(bot=context.bot, message=final_message, add_to_recent=False, parse_mode=ParseMode.HTML)
    except Exception as e:
         logging.error(f"Unexpected error editing status message: {e}. Sending new message.")
         await send_telegram_message(bot=context.bot, message=final_message, add_to_recent=False, parse_mode=ParseMode.HTML)


async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logging.info(f"/pnl command received from user {user.id} ({user.username or user.first_name})")
    processing_message = await update.message.reply_text(f"<i>Calculating realized PnL from trade log (<code>{TRADE_LOG_JSONL}</code>)...</i>", parse_mode=ParseMode.HTML)

    total_realized_pnl = 0.0
    trade_count = 0
    win_count = 0
    loss_count = 0
    other_records = 0 # Records skipped or with PnL=0

    log_exists = os.path.exists(TRADE_LOG_JSONL)
    if log_exists:
        try:
            with open(TRADE_LOG_JSONL, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logging.warning(f"Skipping invalid JSON line in trade log: {line.strip()}")
                        other_records += 1
                        continue

                    # Criteria for a CLOSED trade contributing to Realized PnL:
                    is_closing_trade = record.get('side') in ['sell', 'buy_to_cover']
                    # Status should ideally be FILLED or assumed closed for PNL calc
                    is_filled_or_assumed = str(record.get('status')).lower() in ['filled', 'assumed closed (api error/not found)']
                    has_pnl = 'pnl' in record and record['pnl'] is not None

                    if is_closing_trade and is_filled_or_assumed and has_pnl:
                        try:
                            pnl = float(record['pnl'])
                            total_realized_pnl += pnl
                            trade_count += 1 # Count only valid closed trades with PnL
                            if pnl > 0: win_count += 1
                            elif pnl < 0: loss_count += 1
                            # else: pnl is 0, counted in trade_count
                        except (ValueError, TypeError):
                            logging.warning(f"Could not convert PnL '{record['pnl']}' to float in log line: {line.strip()}")
                            other_records += 1
                            continue
                    elif is_closing_trade:
                         other_records += 1 # Count closing trades missing criteria (e.g., no PNL, wrong status)

        except Exception as e:
            logging.error(f"Error processing trade log file {TRADE_LOG_JSONL}: {e}", exc_info=True)
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=processing_message.message_id, text=f"❌ Error reading trade log: {html.escape(str(e))}", parse_mode=ParseMode.HTML)
            return
    else:
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=processing_message.message_id, text=f"⚠️ Trade log file not found: <code>{TRADE_LOG_JSONL}</code>", parse_mode=ParseMode.HTML)
        return

    # Calculate statistics
    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
    avg_pnl = (total_realized_pnl / trade_count) if trade_count > 0 else 0.0

    # Build reply message
    reply_lines = [
        f"<b>V2 Realized PnL Report (Closed Trades)</b>",
        f"<i>Source: <code>{TRADE_LOG_JSONL}</code></i>",
        "="*25,
        f"Total Realized PnL: <code>${total_realized_pnl:,.2f}</code>",
        f"Closed Trades Counted: <code>{trade_count}</code>",
        f"  Wins: <code>{win_count}</code>",
        f"  Losses: <code>{loss_count}</code>",
        f"  Breakeven/Other: <code>{trade_count - win_count - loss_count}</code>", # Explicitly show BE
        f"Win Rate (Wins / Total): <code>{win_rate:.1f}%</code>",
        f"Avg PnL/Trade: <code>${avg_pnl:,.2f}</code>"
    ]
    if other_records > 0:
        reply_lines.append(f"\n<i>(Note: {other_records} log records skipped or irrelevant for PnL calculation)</i>")

    final_message = "\n".join(reply_lines)

    # Edit the original message with the results
    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=processing_message.message_id,
            text=final_message,
            parse_mode=ParseMode.HTML
        )
    except telegram.error.BadRequest as e:
        if "Message is not modified" in str(e): pass
        else: logging.error(f"Error editing PnL message: {e}. Sending new."); await send_telegram_message(bot=context.bot, message=final_message, add_to_recent=False, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Unexpected error editing PnL message: {e}. Sending new."); await send_telegram_message(bot=context.bot, message=final_message, add_to_recent=False, parse_mode=ParseMode.HTML)


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logging.info(f"/stop_bot command received from user {user.id} ({user.username or user.first_name}). Initiating shutdown.")
    await update.message.reply_text("🤖 <i>Shutdown initiated... Bot will stop after the current cycle finishes and save state.</i>", parse_mode=ParseMode.HTML)
    shutdown_event.set()


# === Entry Point === #
if __name__ == "__main__":
    logging.info("Initializing Application...")
    try:
        application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

        # Add command handlers (using updated functions)
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("pnl", pnl_command))
        application.add_handler(CommandHandler("stop_bot", stop_command))
        logging.info("Telegram command handlers added.")

    except Exception as init_e:
        logging.critical(f"Failed to initialize Telegram Application: {init_e}", exc_info=True)
        sys.exit(1) # Exit if Telegram setup fails


    # --- Main Async Execution Function ---
    async def run_all():
        global application # Make application accessible in finally block if needed
        strategy_task = None
        start_message_sent = False
        try:
            # Send startup message using HTML
            startup_message = (
                f"✅ <b>Unholy_V2 Bot Started</b>\n"
                f"<b>Mode:</b> {'Paper' if PAPER_TRADING else '🔴 LIVE'}\n"
                f"<b>Position Size:</b> ${POSITION_SIZE_USD:.2f}\n"
                f"<b>Cycle Interval:</b> {JOB_INTERVAL_MINUTES} min\n"
                f"<b>State File:</b> <code>{STATE_FILE}</code>\n"
                f"<b>Trade Log:</b> <code>{TRADE_LOG_JSONL}</code>"
            )
            await send_telegram_message(application.bot, startup_message, add_to_recent=False, parse_mode=ParseMode.HTML)
            start_message_sent = True

            # Start polling and the main strategy loop concurrently
            async with application: # Manages start_polling and stop_polling
                logging.info("Starting Telegram polling...")
                await application.start() # Start network layer
                await application.updater.start_polling(poll_interval=1.0, drop_pending_updates=True)
                logging.info("Polling started.")

                logging.info("Starting main strategy loop...")
                strategy_task = asyncio.create_task(main_strategy_loop(application.bot))

                # Wait until shutdown is signaled
                await shutdown_event.wait()
                logging.info("Shutdown signaled. Stopping strategy loop and polling...")

        except Exception as e:
             logging.critical(f"Critical error in run_all: {e}", exc_info=True);
             shutdown_event.set() # Ensure shutdown is signaled on error
             if application and application.bot and not start_message_sent: # Send error only if startup msg failed
                 try: await send_telegram_message(application.bot, f"🚨 <b>CRITICAL Error during startup:</b> {html.escape(e.__class__.__name__)} - {html.escape(str(e))}. Shutting down!", parse_mode=ParseMode.HTML)
                 except Exception as send_e: logging.error(f"Failed send critical TG msg during run_all error: {send_e}")
        finally:
            logging.info("run_all: Starting cleanup...")
            # Stop strategy task if running
            if strategy_task and not strategy_task.done():
                logging.info("Cancelling strategy task...")
                strategy_task.cancel();
                try: await strategy_task
                except asyncio.CancelledError: logging.info("Strategy task successfully cancelled.")
                except Exception as e: logging.error(f"Error awaiting cancelled strategy task: {e}")
            else: logging.info("Strategy task already done or not started.")

            # Polling is stopped by the 'async with application' context manager
            logging.info("run_all: Cleanup finished.")


    # --- Run the application ---
    main_run_task = None
    loop = asyncio.get_event_loop()
    try:
        logging.info("Creating and running main task (run_all)...")
        main_run_task = loop.create_task(run_all())
        loop.run_until_complete(main_run_task)
        logging.info("Main task (run_all) completed normally.")
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Signaling shutdown...")
        shutdown_event.set()
        # Allow run_all's finally block to handle cleanup
        if main_run_task and not main_run_task.done():
            logging.info("Waiting for run_all task to finish cleanup after KeyboardInterrupt...")
            try: loop.run_until_complete(main_run_task)
            except asyncio.CancelledError: logging.info("Main task cancelled during KeyboardInterrupt cleanup.")
            except Exception as e: logging.error(f"Error during main task cleanup after KeyboardInterrupt: {e}")
        logging.info("Cleanup after KeyboardInterrupt should be complete.")

    except Exception as e:
        logging.critical(f"Unhandled exception in main execution scope: {e}", exc_info=True);
        shutdown_event.set() # Signal shutdown
        if 'application' in locals() and hasattr(application, 'bot') and application.bot:
             try:
                 # Try sending error message via Telegram
                 error_msg = f"🚨 <b>CRITICAL Unhandled Exception in Main Scope:</b> {html.escape(e.__class__.__name__)} - {html.escape(str(e))}. Bot stopping NOW!"
                 # Use run_coroutine_threadsafe if loop is running, else run directly
                 if loop.is_running(): asyncio.run_coroutine_threadsafe(send_telegram_message(application.bot, error_msg, parse_mode=ParseMode.HTML), loop).result(timeout=10)
                 else: asyncio.run(send_telegram_message(application.bot, error_msg, parse_mode=ParseMode.HTML))
             except Exception as exit_e: logging.warning(f"Could not send critical error message during main scope exit: {exit_e}")
        # Ensure run_all cleanup happens even on main scope error
        if main_run_task and not main_run_task.done():
            logging.info("Awaiting run_all task completion after main scope exception...")
            try: loop.run_until_complete(main_run_task)
            except asyncio.CancelledError: logging.info("Main task cancelled during main scope exception cleanup.")
            except Exception as e_cleanup: logging.error(f"Error during main task cleanup after main scope exception: {e_cleanup}")

    finally:
        logging.info("Main execution: Starting final cleanup...")
        shutdown_event.set() # Ensure it's set

        # Wait for run_all task to finish if it hasn't already (with timeout)
        if main_run_task and not main_run_task.done():
             logging.info("Awaiting run_all task finalization during cleanup...")
             try: loop.run_until_complete(asyncio.wait_for(main_run_task, timeout=15.0))
             except asyncio.TimeoutError: logging.warning("run_all task timed out during final cleanup. Forcing cancellation.") ; main_run_task.cancel()
             except asyncio.CancelledError: logging.info("run_all task was already cancelled during final cleanup.")
             except Exception as e: logging.error(f"Error awaiting run_all task completion in finally block: {e}")
             # Ensure cancellation completes if timeout occurred
             if main_run_task.cancelled():
                  try: loop.run_until_complete(main_run_task) # Wait for cancellation to finish
                  except asyncio.CancelledError: pass # Expected
                  except Exception as e_inner: logging.error(f"Error awaiting forced cancelled run_all task: {e_inner}")

        logging.info("Attempting final state save...")
        # Run save_state synchronously as the loop might be closing/closed
        save_state(state)
        logging.info("Final state save attempted.")

        # Cleanly shutdown asyncio tasks and close the loop
        if loop.is_running():
            logging.info("Shutting down async generators...")
            try: loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception as e: logging.error(f"Error shutting down async gens: {e}")
            finally:
                 logging.info("Closing event loop."); loop.close(); logging.info("Event loop closed.")
        elif not loop.is_closed():
             logging.warning("Event loop not running but not closed. Closing now.")
             loop.close(); logging.info("Event loop closed.")
        else: logging.info("Event loop already closed.")

        logging.info("-------------------- BOT END (Unholy_V2) ----------------------")
        logging.shutdown() # Flush and close logging handlers