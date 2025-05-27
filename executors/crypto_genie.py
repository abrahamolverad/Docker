# Crypto_Genie_V6.3 (Framework v0.9.5) – STRICT momentum-filtered edition
# Changes vs V6.2:
# - Reduced Max Open Positions (5 from 8)
# - Adjusted Vol Spike Multipliers (30m: 1.5x, Entry: 2.0x)
# - Tightened Exit Params (Max SL 5%, TP 1%, Trail 0.5%, Timeout 30m/0.2%)
# - Adjusted RSI Entry Range (50/50)
# - Added Risk Guards (Daily Loss Limit 5%, Consecutive Loss Limit 6)
# - Refined Memecoin List & Constants (PEPE, 1000BONK, WIF only; SL 2.0%, Trail Act 2.5%, TP 3.5%)
# - Confirmed Cooldown logic (15 min)
# - Updated log/state file names

import asyncio
import os
import json
import logging
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import isoparse
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, ApplicationBuilder
from telegram.constants import ParseMode
import time
import math
import pandas as pd
from decimal import Decimal, ROUND_DOWN, ROUND_UP # Use ROUND_UP for TP/Short SL, ROUND_DOWN for Long SL
import html
import platform
import signal
import requests # Needed for debug patch and manual request
import types # Needed for revised patching method
import hmac # Needed for manual request
import hashlib # Needed for manual request
from urllib.parse import urlencode # Needed for manual request


# Technical Analysis library (install pandas_ta)
try:
    import pandas_ta as ta
except ImportError:
    # Logger needs to be defined before this point if used in except block
    # logger.warning("pandas_ta library not found. Please install it: pip install pandas_ta")
    ta = None # Set to None if not available

# === Logging Setup === #
logging.basicConfig(
    level=logging.INFO, # Set to INFO for production, DEBUG for detailed signal checks
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(funcName)s - %(message)s",
    handlers=[
        logging.FileHandler("crypto_genie_v6.3.log", encoding='utf-8'), # <<< UPDATED Log file name
        logging.StreamHandler()
    ]
)
# Set higher level for noisy libraries if needed
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING) # Added for requests library

logger = logging.getLogger(__name__)
# Set root logger level if needed, or specific loggers
logger.setLevel(logging.DEBUG) # Uncomment this if you want DEBUG level logging for entry/exit signals

if ta:
    logger.info("pandas_ta library loaded successfully.")
else:
    logger.warning("pandas_ta library not found. Please install it: pip install pandas_ta")


# === Load Environment === #
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_FUTURES_API_KEY", os.getenv("BINANCE_API_KEY"))
BINANCE_SECRET_KEY = os.getenv("BINANCE_FUTURES_SECRET_KEY", os.getenv("BINANCE_SECRET_KEY"))
Crypto_TELEGRAM_BOT_TOKEN = os.getenv("Crypto_TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = os.getenv("TELEGRAM_USER_ID")


# === V6.3 Configuration === #
# --- General ---
SCAN_INTERVAL = 15 # seconds
STATE_FILE = "crypto_genie_v6.3_state.json" # <<< UPDATED state file name
AUTHORIZED_TELEGRAM_USER_IDS = [int(uid.strip()) for uid in os.getenv("AUTHORIZED_TELEGRAM_USER_IDS", TELEGRAM_USER_ID or "").split(',') if uid.strip()]

# --- Trading ---
TARGET_TRADE_AMOUNT_USDT = 15 # Reference for fixed size fallback (unused)
RISK_PER_TRADE_PERCENT = 1.0 # Risk 1% of available balance per trade
MIN_NOTIONAL_VALUE = 10.0 # Minimum position value in USDT (unchanged)
LEVERAGE = 5 # Keep leverage, could be adjusted
MAX_OPEN_POSITIONS = 5 # <<< REDUCED max concurrent trades
MIN_USDT_VOLUME_24H = 3_000_000 # Primary liquidity filter (unchanged)

# --- Candidate Selection ---
MAX_SYMBOLS_FOR_KLINE_FETCH = 50 # Symbols checked for 30m/1h data after volume filter (unchanged)
CANDIDATE_LIST_SIZE = 10 # How many Long and Short candidates to evaluate further (unchanged)
CANDIDATE_STRATEGY = "MOM_VOL_TREND_30M_1H_V6.3" # <<< UPDATED Strategy Name
VOL_SPIKE_LOOKBACK_30M = 20 # Periods for 30m avg volume calc (unchanged)
VOL_SPIKE_MULTIPLIER_30M = 1.5 # <<< INCREASED multiplier for 30m volume spike check
TREND_FILTER_TIMEFRAME = Client.KLINE_INTERVAL_1HOUR
TREND_FILTER_EMA_PERIOD = 50 # Keep 1H EMA filter (unchanged)
OVEREXTENSION_FILTER_PERCENT_30M = 20.0 # Keep 30m candle change filter (unchanged)

# --- Technical Indicators (Periods & Entry Conditions) ---
KLINE_INTERVAL_ENTRY = Client.KLINE_INTERVAL_5MINUTE
KLINE_LIMIT_ENTRY = 100 # Candles needed for 5m indicators + breakout lookback (unchanged)
EMA_FAST_PERIOD = 20 # Period for 5m EMA (entry alignment) (unchanged)
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70 # Filter, not direct entry
RSI_OVERSOLD = 30  # Filter, not direct entry
RSI_ENTRY_MIN_LONG = 50 # <<< ADJUSTED RSI range for Long entry
RSI_ENTRY_MAX_LONG = 70
RSI_ENTRY_MIN_SHORT = 30
RSI_ENTRY_MAX_SHORT = 50 # <<< ADJUSTED RSI range for Short entry
VOLUME_AVG_PERIOD_ENTRY = 20 # Periods for 5m avg volume calc (unchanged)
VOLUME_SPIKE_MULTIPLIER_ENTRY = 2.0 # <<< INCREASED multiplier for 5m entry volume check
ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 2.5 # <<< KEPT multiplier for ATR-based Stop Loss
BREAKOUT_LOOKBACK_PERIOD = 20 # Periods for 5m high/low breakout check (unchanged)

# --- Exit Strategy ---
EXIT_STOP_LOSS_PERCENT = 5.0 # <<< REDUCED Max stop loss % cap
EXIT_TAKE_PROFIT_PERCENT = 1.0 # <<< REDUCED Target profit % for Trailing Stop activation
TRAILING_STOP_PERCENT = 0.5 # <<< REDUCED Trailing stop distance
TIMEOUT_EXIT_MINUTES = 30 # <<< REDUCED Timeout duration check start
TIMEOUT_EXIT_MAX_DURATION_NEGATIVE = 60 # Max duration if PnL is negative (effectively overridden by TIMEOUT_EXIT_MINUTES if flat)
TIMEOUT_EXIT_FLAT_PERCENT = 0.2 # <<< UPDATED PnL range (+/-) to trigger timeout exit at 30 mins

# Signal Invalidation Exit Conditions (Need 2 of 3) (Unchanged)
INVALIDATION_RSI_THRESHOLD_LONG = 40 # RSI threshold for long invalidation
INVALIDATION_RSI_THRESHOLD_SHORT = 60 # RSI threshold for short invalidation

# --- Risk Management ---
REENTRY_COOLDOWN_MINUTES = 15 # <<< KEPT Cooldown after closing a trade on a symbol
DAILY_LOSS_LIMIT_PERCENT = 5.0 # <<< NEW Max daily loss percentage (of start balance)
CONSECUTIVE_LOSS_LIMIT = 6 # <<< NEW Max consecutive losses

# --- Memecoin Handling ---
# <<< UPDATED LIST: Keep only PEPE, 1000BONK, WIF >>>
MEMECOIN_SYMBOLS = [
    "PEPEUSDT", "1000BONKUSDT", "WIFUSDT",
]
# <<< UPDATED Constants for V6.3 >>>
MEMECOIN_TP_PERCENT = 3.5 # Target TP% for Memecoin Trailing Activation
MEMECOIN_TRAILING_ACTIVATION_PERCENT = 2.5 # Threshold to ACTIVATE trailing for memecoins
MEMECOIN_SL_PERCENT = 2.0 # Fixed SL % for Memecoins (overrides ATR/Max SL%)


# === Global Variables === #
exchange_info_map = {}
state = {
    "open_positions": {},
    "pnl_history": [],
    "daily_stats": {},
    "symbol_cooldowns": {},
    "trading_paused_reason": None, # <<< NEW: Stores reason for pause ("DAILY_LOSS", "CONSECUTIVE_LOSS")
}
shutdown_requested = False
main_trading_task = None
application_instance = None
candidate_cache = {"long": [], "short": [], "timestamp": 0}
account_balance_cache = {"balance": None, "timestamp": 0}
BALANCE_CACHE_DURATION = 10 # seconds

# === Initialize Binance Futures Client === #
# (Same as V6.2)
if not BINANCE_API_KEY or not BINANCE_SECRET_KEY: logger.critical("Binance Keys not found."); exit()
try:
    futures_client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_SECRET_KEY)
    logger.info(f"Binance Futures client initialized. Relying on library default API URL.")
except Exception as e: logger.critical(f"Failed Binance client init: {e}. Exiting."); exit()

# === Apply Debug Patch (AFTER initial client creation) ===
# (Same as V6.2)
try:
    if hasattr(futures_client, '_request'):
        original_request_method = futures_client._request # Get the original bound method
        def debug_request_wrapper(self, method, path, signed=False, force_params=False, **kwargs):
            base_url = getattr(self, 'API_URL', None)
            if not base_url:
                if path.startswith('/fapi/'): base_url = "https://fapi.binance.com"
                elif path.startswith('/dapi/'): base_url = "https://dapi.binance.com"
                else:
                    base_url = "https://api.binance.com"
                    logger.warning(f"[DEBUG] Could not determine base URL for path: {path}. Falling back to {base_url}")

            full_url = base_url + path
            log_msg = f"[DEBUG] Requesting: {method} {full_url} (Signed: {signed}, ForceParams: {force_params})"
            if logger.isEnabledFor(logging.DEBUG):
                if kwargs.get('params'): log_msg += f" Params: {kwargs['params']}"
            logger.debug(log_msg)
            try:
                return original_request_method(method, path, signed=signed, force_params=force_params, **kwargs)
            except BinanceAPIException as api_err_inner:
                response_text_inner = getattr(api_err_inner, 'response', None)
                response_text_inner = getattr(response_text_inner, 'text', '') if response_text_inner else ''
                if response_text_inner: logger.error(f"[DEBUG] API Error Response Text for {full_url}: {response_text_inner[:500]}...")
                raise api_err_inner
            except Exception as e_inner:
                logger.error(f"[DEBUG] Non-API Exception during request to {full_url}: {e_inner}")
                raise e_inner

        futures_client._request = types.MethodType(debug_request_wrapper, futures_client)
        logger.info("Patched Binance client _request method for debug logging using types.MethodType (with force_params).")
    else:
        logger.error("Could not find _request method on futures_client to patch.")
except Exception as e:
    logger.critical(f"Failed to apply debug patch: {e}. Continuing without patch.")


# === Helper Functions === #

async def send_alert(application: Application, text: str):
    """Sends an alert message to the primary TELEGRAM_USER_ID."""
    # (Same as V6.2)
    if not TELEGRAM_USER_ID or not application: return
    try:
        await application.bot.send_message(chat_id=str(TELEGRAM_USER_ID), text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed Telegram alert send (HTML): {e}", exc_info=False)
        try:
            import re
            plain_text = re.sub('<[^<]+?>', '', text) # Basic HTML tag removal
            logger.info("Retrying Telegram alert send without HTML parsing.")
            await application.bot.send_message(chat_id=str(TELEGRAM_USER_ID), text=plain_text, parse_mode=None)
        except Exception as e2:
            logger.error(f"Failed Telegram alert send (Plain Text Fallback): {e2}", exc_info=False)

def load_exchange_info():
    """Fetches and filters exchange info."""
    # (Same as V6.2)
    global exchange_info_map
    logger.info("Fetching futures exchange info...")
    try:
        info = futures_client.futures_exchange_info()
        exchange_info_map = {
            s['symbol']: s for s in info['symbols']
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
        }
        logger.info(f"Successfully loaded and filtered exchange info for {len(exchange_info_map)} TRADING perpetual USDT symbols.")
    except BinanceAPIException as api_err:
        logger.critical(f"API Error fetching exchange info: {api_err}. Check debug logs. Exiting.")
        exit()
    except Exception as e:
        logger.critical(f"Failed load exchange info: {e}. Exiting.", exc_info=True)
        exit()

def adjust_qty_to_filters(symbol, quantity):
    """Adjusts quantity based on LOT_SIZE filter and quantityPrecision."""
    # (Same as V6.2)
    symbol_info = exchange_info_map.get(symbol)
    if not symbol_info: return Decimal("0")
    precision = symbol_info.get('quantityPrecision')
    step_size_str, min_qty_str = None, None
    for f in symbol_info.get('filters', []):
        if f['filterType'] == 'LOT_SIZE':
            step_size_str = f.get('stepSize'); min_qty_str = f.get('minQty')
            if precision is None and step_size_str and '.' in step_size_str:
                precision = len(step_size_str.split('.')[-1].rstrip('0'))
            elif precision is None: precision = 0
            break
    if precision is None: precision = 8
    try:
        step_size = Decimal(step_size_str or "0"); min_qty = Decimal(min_qty_str or "0")
        quantity_dec = Decimal(str(quantity))
    except Exception: return Decimal("0")
    if quantity_dec < min_qty: return Decimal("0")
    if step_size > 0: adjusted_qty = (quantity_dec // step_size) * step_size
    else: adjusted_qty = quantity_dec
    quantizer = Decimal('1e-' + str(precision))
    final_qty = adjusted_qty.quantize(quantizer, rounding=ROUND_DOWN)
    if final_qty < min_qty: return Decimal("0")
    if final_qty <= 0: return Decimal("0")
    return final_qty

def load_state():
    """Loads bot state, ensuring new keys like symbol_cooldowns and risk management exist."""
    global state
    default_state = {
        "open_positions": {},
        "pnl_history": [],
        "daily_stats": {},
        "symbol_cooldowns": {},
        "trading_paused_reason": None # <<< NEW default
    }
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding='utf-8') as f:
                loaded_data = json.load(f)
            if not isinstance(loaded_data, dict): raise ValueError("State file not a dict.")
            state = loaded_data
            # Ensure all top-level keys from default_state exist
            for key, default_value in default_state.items():
                state.setdefault(key, default_value)
            # Validate sub-structures
            if not isinstance(state.get("open_positions"), dict): state["open_positions"] = {}
            if not isinstance(state.get("pnl_history"), list): state["pnl_history"] = []
            if not isinstance(state.get("daily_stats"), dict): state["daily_stats"] = {}
            if not isinstance(state.get("symbol_cooldowns"), dict): state["symbol_cooldowns"] = {}

            logger.info(f"Loaded state from {STATE_FILE}: {len(state.get('open_positions', {}))} open, {len(state.get('pnl_history', []))} PnL, {len(state.get('symbol_cooldowns', {}))} cooldowns. Trading Paused: {state.get('trading_paused_reason')}")

            # Convert numeric fields in open_positions (same as V6.2)
            for symbol, pos_data in state.get("open_positions", {}).items():
                for field in ['entry_price', 'qty', 'high_watermark', 'low_watermark', 'notional_value', 'initial_sl_price', 'current_sl_price']:
                    if field in pos_data and pos_data[field] is not None:
                        try: pos_data[field] = float(pos_data[field])
                        except (ValueError, TypeError): pos_data[field] = None
                for field in ['partial_tp_hit', 'trailing_stop_active']:
                    if field in pos_data and not isinstance(pos_data[field], bool):
                        pos_data.setdefault('partial_tp_hit', False)
                        pos_data.setdefault('trailing_stop_active', False)
                        pos_data[field] = bool(pos_data[field])

            # Validate cooldown timestamps (same as V6.2)
            invalid_cooldowns = []
            for symbol, timestamp_str in state.get("symbol_cooldowns", {}).items():
                try:
                    # Try parsing to ensure it's a valid ISO format string
                    isoparse(timestamp_str)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid cooldown timestamp format for {symbol}: '{timestamp_str}'. Removing.")
                    invalid_cooldowns.append(symbol)
            for symbol in invalid_cooldowns:
                del state["symbol_cooldowns"][symbol]

            # <<< NEW: Validate daily stats structure >>>
            invalid_days = []
            for day_str, stats in state.get("daily_stats", {}).items():
                if not isinstance(stats, dict):
                    logger.warning(f"Invalid daily_stats entry for {day_str}. Removing.")
                    invalid_days.append(day_str)
                    continue
                stats.setdefault("trades", 0)
                stats.setdefault("wins", 0)
                stats.setdefault("losses", 0)
                stats.setdefault("pnl", 0.0)
                stats.setdefault("consecutive_losses", 0)
                stats.setdefault("start_balance", None) # Initialize if missing
                stats.setdefault("daily_loss_limit_hit", False)
                stats.setdefault("consecutive_loss_limit_hit", False)
                # Ensure types
                for key in ["trades", "wins", "losses", "consecutive_losses"]:
                    if not isinstance(stats[key], int): stats[key] = 0
                if not isinstance(stats["pnl"], (int, float)): stats["pnl"] = 0.0
                if stats["start_balance"] is not None and not isinstance(stats["start_balance"], (int, float)): stats["start_balance"] = None
                if not isinstance(stats["daily_loss_limit_hit"], bool): stats["daily_loss_limit_hit"] = False
                if not isinstance(stats["consecutive_loss_limit_hit"], bool): stats["consecutive_loss_limit_hit"] = False
            for day in invalid_days:
                del state["daily_stats"][day]

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"Error loading/validating state from {STATE_FILE}: {e}. Starting fresh.")
            state = default_state
        except Exception as e:
            logger.error(f"Unexpected error loading state: {e}. Starting fresh.", exc_info=True)
            state = default_state
    else:
        logger.info(f"State file '{STATE_FILE}' not found. Starting fresh.")
        state = default_state

    # Initialize today's stats if needed (same as V6.2 but adds new fields)
    today_str = date.today().isoformat()
    if today_str not in state.get("daily_stats", {}):
        logger.info(f"Initializing daily stats for {today_str}.")
        state.setdefault("daily_stats", {})[today_str] = {
            "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            "consecutive_losses": 0, "start_balance": None,
            "daily_loss_limit_hit": False, "consecutive_loss_limit_hit": False
        }
        state["trading_paused_reason"] = None # Reset pause reason on new day


def save_state():
    """Saves the current bot state."""
    # (Same as V6.2)
    global state
    try:
        # Ensure Decimal is converted to string for JSON compatibility
        serializable_state = json.loads(json.dumps(state, default=lambda x: str(x) if isinstance(x, Decimal) else str(x)))
        with open(STATE_FILE, "w", encoding='utf-8') as f:
            json.dump(serializable_state, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving state: {e}", exc_info=True)

# === V6.3 Candidate Selection Function ===
def select_candidates():
    """
    Performs V6.3 filtering: Volume, 30m Spike (1.5x), Overextension, 1H Trend.
    Returns tuple: (list_of_long_candidate_symbols, list_of_short_candidate_symbols)
    """
    logger.info("Starting V6.3 candidate selection...")
    candidate_longs = []
    candidate_shorts = []

    try:
        # 1. Get all tickers (Unchanged)
        all_tickers = futures_client.futures_ticker()
        if not all_tickers:
            logger.warning("Received empty ticker list from API for volume filtering.")
            return [], []

        # 2. Pre-filter by 24h Volume (Unchanged)
        volume_filtered_symbols_data = []
        for t in all_tickers:
            symbol = t.get('symbol')
            if symbol in exchange_info_map:
                try:
                    volume = float(t.get('quoteVolume', 0))
                    if volume >= MIN_USDT_VOLUME_24H:
                        volume_filtered_symbols_data.append({'symbol': symbol, 'volume': volume})
                except (ValueError, TypeError): continue

        logger.info(f"Found {len(volume_filtered_symbols_data)} symbols meeting 24h volume >= ${MIN_USDT_VOLUME_24H:,.0f}.")
        if not volume_filtered_symbols_data: return [], []

        # Sort by volume and limit for K-line fetching (Unchanged)
        volume_filtered_symbols_data.sort(key=lambda x: x['volume'], reverse=True)
        symbols_for_kline_check = [d['symbol'] for d in volume_filtered_symbols_data[:MAX_SYMBOLS_FOR_KLINE_FETCH]]
        logger.info(f"Selected Top {len(symbols_for_kline_check)} by volume (max {MAX_SYMBOLS_FOR_KLINE_FETCH}) for further checks.")
        if not symbols_for_kline_check: return [], []

        # 3. Fetch 30m Klines & Calculate 30m Change and Relative Volume Spike (NEW multiplier)
        symbols_passing_30m_check = []
        logger.info(f"Fetching 30m klines for {len(symbols_for_kline_check)} symbols...")
        kline_limit_30m = VOL_SPIKE_LOOKBACK_30M + 2
        for symbol in symbols_for_kline_check:
            try:
                klines_30m = futures_client.futures_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_30MINUTE, limit=kline_limit_30m)
                if klines_30m and len(klines_30m) >= VOL_SPIKE_LOOKBACK_30M + 1:
                    df = pd.DataFrame(klines_30m, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base', 'Taker buy quote', 'Ignore'])
                    df['Open'] = pd.to_numeric(df['Open'])
                    df['Close'] = pd.to_numeric(df['Close'])
                    df['Volume'] = pd.to_numeric(df['Volume'])

                    avg_vol = df['Volume'].iloc[-(VOL_SPIKE_LOOKBACK_30M + 1):-1].mean()
                    last_closed_candle = df.iloc[-2]
                    k_open = Decimal(str(last_closed_candle['Open']))
                    k_close = Decimal(str(last_closed_candle['Close']))
                    kline_vol = Decimal(str(last_closed_candle['Volume']))

                    if k_open > 0 and avg_vol > 0:
                        percent_change = float(((k_close - k_open) / k_open) * 100)
                        relative_volume = float(kline_vol / Decimal(str(avg_vol)))

                        # <<< Use NEW Volume Spike Multiplier >>>
                        if relative_volume >= VOL_SPIKE_MULTIPLIER_30M:
                            # Filter by Overextension (Unchanged)
                            if abs(percent_change) <= OVEREXTENSION_FILTER_PERCENT_30M:
                                symbols_passing_30m_check.append({
                                    'symbol': symbol,
                                    'change_30m': percent_change,
                                })
                            else:
                                logger.debug(f"Skipping {symbol}: 30m change {percent_change:.2f}% exceeds limit {OVEREXTENSION_FILTER_PERCENT_30M}%.")
                        # else: logger.debug(f"Skipping {symbol}: 30m Rel Vol {relative_volume:.2f} < {VOL_SPIKE_MULTIPLIER_30M}")
            except BinanceAPIException as api_err: logger.warning(f"API error fetching 30m klines for {symbol}: {api_err}")
            except Exception as e: logger.warning(f"Error processing 30m klines for {symbol}: {e}")
            # time.sleep(0.01) # Be cautious with sleeps in async loops

        logger.info(f"{len(symbols_passing_30m_check)} symbols passed 30m volume spike (>{VOL_SPIKE_MULTIPLIER_30M}x) & overextension checks.")
        if not symbols_passing_30m_check: return [], []

        # 4. Fetch 1H Klines & Calculate 1H EMA for Trend Alignment (Unchanged Logic)
        symbols_passing_trend_check = []
        logger.info(f"Fetching 1H klines for {len(symbols_passing_30m_check)} symbols for trend check...")
        kline_limit_1h = TREND_FILTER_EMA_PERIOD + 5
        symbols_to_check_trend = [d['symbol'] for d in symbols_passing_30m_check]

        for symbol in symbols_to_check_trend:
            try:
                klines_1h = futures_client.futures_klines(symbol=symbol, interval=TREND_FILTER_TIMEFRAME, limit=kline_limit_1h)
                if klines_1h and len(klines_1h) >= TREND_FILTER_EMA_PERIOD:
                    df_1h = pd.DataFrame(klines_1h, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base', 'Taker buy quote', 'Ignore'])
                    df_1h['Close'] = pd.to_numeric(df_1h['Close'])

                    if ta:
                        df_1h.ta.ema(length=TREND_FILTER_EMA_PERIOD, append=True)
                        ema_col = f'EMA_{TREND_FILTER_EMA_PERIOD}'
                        if ema_col in df_1h.columns and not df_1h[ema_col].isnull().iloc[-1]:
                            last_close = df_1h['Close'].iloc[-1]
                            last_ema = df_1h[ema_col].iloc[-1]
                            trend_aligned_long = last_close > last_ema
                            trend_aligned_short = last_close < last_ema

                            data_30m = next((d for d in symbols_passing_30m_check if d['symbol'] == symbol), None)
                            if data_30m:
                                symbols_passing_trend_check.append({
                                    'symbol': symbol,
                                    'change_30m': data_30m['change_30m'],
                                    'trend_aligned_long': trend_aligned_long,
                                    'trend_aligned_short': trend_aligned_short
                                })
            except BinanceAPIException as api_err: logger.warning(f"API error fetching 1H klines for {symbol}: {api_err}")
            except Exception as e: logger.warning(f"Error processing 1H klines for {symbol}: {e}")
            # time.sleep(0.01)

        logger.info(f"{len(symbols_passing_trend_check)} symbols passed 1H EMA trend alignment check.")

        # 5. Separate and Rank Long/Short Candidates based on 30m change (Unchanged Logic)
        long_candidates_data = [d for d in symbols_passing_trend_check if d['trend_aligned_long'] and d['change_30m'] > 0]
        short_candidates_data = [d for d in symbols_passing_trend_check if d['trend_aligned_short'] and d['change_30m'] < 0]

        long_candidates_data.sort(key=lambda x: x['change_30m'], reverse=True)
        short_candidates_data.sort(key=lambda x: x['change_30m'], reverse=False)

        candidate_longs = [d['symbol'] for d in long_candidates_data[:CANDIDATE_LIST_SIZE]]
        candidate_shorts = [d['symbol'] for d in short_candidates_data[:CANDIDATE_LIST_SIZE]]

        logger.info(f"Final Candidates - Longs ({len(candidate_longs)}): {', '.join(candidate_longs)}")
        logger.info(f"Final Candidates - Shorts ({len(candidate_shorts)}): {', '.join(candidate_shorts)}")

        return candidate_longs, candidate_shorts

    except Exception as e:
        logger.error(f"Unexpected error during V6.3 candidate selection: {e}", exc_info=True)
        return [], []


# === Indicator Calculation Function (V6.3 - No logic changes from V6.2) ===
async def calculate_indicators(symbol):
    """
    Fetches 5m klines and calculates V6.3 indicators including breakout levels.
    Returns a dictionary of the latest indicator values or None on error.
    (Logic identical to V6.2)
    """
    logger.debug(f"Calculating 5min indicators for {symbol} (V6.3)...")
    if not ta:
        logger.error("pandas_ta library not available. Cannot calculate indicators.")
        return None

    try:
        # Fetch enough klines for the longest period indicator + breakout lookback + buffer
        kline_limit = max(EMA_FAST_PERIOD, MACD_SLOW, RSI_PERIOD, VOLUME_AVG_PERIOD_ENTRY, ATR_PERIOD, BREAKOUT_LOOKBACK_PERIOD) + 50
        klines_5m = await asyncio.to_thread(
            futures_client.futures_klines,
            symbol=symbol, interval=KLINE_INTERVAL_ENTRY, limit=kline_limit
        )

        if not klines_5m or len(klines_5m) < max(EMA_FAST_PERIOD, MACD_SLOW, RSI_PERIOD, VOLUME_AVG_PERIOD_ENTRY, ATR_PERIOD, BREAKOUT_LOOKBACK_PERIOD):
            logger.warning(f"Insufficient 5m kline data received for {symbol} ({len(klines_5m)} candles). Cannot calculate all indicators for V6.3.")
            return None

        # Create DataFrame
        df = pd.DataFrame(klines_5m, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base', 'Taker buy quote', 'Ignore'])
        df['Open'] = pd.to_numeric(df['Open'])
        df['High'] = pd.to_numeric(df['High'])
        df['Low'] = pd.to_numeric(df['Low'])
        df['Close'] = pd.to_numeric(df['Close'])
        df['Volume'] = pd.to_numeric(df['Volume'])
        df['IsRedCandle'] = df['Close'] < df['Open']

        # Calculate Standard Indicators using pandas_ta
        df.ta.ema(length=EMA_FAST_PERIOD, append=True)
        df.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL, append=True)
        df.ta.rsi(length=RSI_PERIOD, append=True)
        vol_ma_col = f'VOL_MA_{VOLUME_AVG_PERIOD_ENTRY}'
        df[vol_ma_col] = df['Volume'].rolling(window=VOLUME_AVG_PERIOD_ENTRY).mean()
        df.ta.atr(length=ATR_PERIOD, append=True)

        # Calculate VWAP Manually
        if df['Volume'].sum() > 0:
            typical_price = (df['High'] + df['Low'] + df['Close']) / 3
            cum_tp_vol = (typical_price * df['Volume']).cumsum()
            cum_vol = df['Volume'].cumsum()
            df['VWAP'] = cum_tp_vol / cum_vol.replace(0, float('nan'))
        else:
            logger.warning(f"Total volume for {symbol} is zero. Cannot calculate VWAP.")
            df['VWAP'] = None

        # Calculate Recent High/Low for Breakout Check
        df['RecentHigh'] = df['High'].rolling(window=BREAKOUT_LOOKBACK_PERIOD).max().shift(1)
        df['RecentLow'] = df['Low'].rolling(window=BREAKOUT_LOOKBACK_PERIOD).min().shift(1)

        # Get the latest values (last row)
        latest = df.iloc[-1]
        prev_candle = df.iloc[-2] # Needed for invalidation check (candle close)

        # Get ATR value
        atr_col_name = f'ATRr_{ATR_PERIOD}'
        if atr_col_name not in df.columns: atr_col_name = f'ATR_{ATR_PERIOD}'
        atr_value = latest.get(atr_col_name) if atr_col_name in df.columns else None
        if atr_value is None: logger.warning(f"ATR column not found for {symbol}. Using None.")


        indicators = {
            "EMA_20": latest.get(f'EMA_{EMA_FAST_PERIOD}'),
            "VWAP": latest.get('VWAP'),
            "MACD_hist": latest.get(f'MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'),
            "MACD_line": latest.get(f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'),
            "MACD_signal": latest.get(f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'),
            "RSI_14": latest.get(f'RSI_{RSI_PERIOD}'),
            "Vol_MA_20": latest.get(vol_ma_col),
            "ATR_14": atr_value,
            "Close": latest.get('Close'),
            "Volume": latest.get('Volume'),
            "RecentHigh": latest.get('RecentHigh'),
            "RecentLow": latest.get('RecentLow'),
            "PrevClose": prev_candle.get('Close'),
            "PrevEMA_20": prev_candle.get(f'EMA_{EMA_FAST_PERIOD}'),
            "PrevVWAP": prev_candle.get('VWAP'),
            "PrevIsRedCandle": prev_candle.get('IsRedCandle')
        }

        # Check for NaN/None values
        required_for_signal = ["EMA_20", "VWAP", "MACD_line", "MACD_signal", "RSI_14", "Vol_MA_20", "Close", "Volume", "RecentHigh", "RecentLow", "PrevClose", "PrevEMA_20", "PrevVWAP"]

        if any(indicators.get(k) is None or (isinstance(indicators.get(k), float) and math.isnan(indicators.get(k))) for k in required_for_signal):
            missing = [k for k in required_for_signal if indicators.get(k) is None or (isinstance(indicators.get(k), float) and math.isnan(indicators.get(k)))]
            logger.warning(f"NaN/None found in required 5m indicators for {symbol} (V6.3): {missing}. Check data/periods.")
            return None

        logger.debug(f"Calculated 5m indicators for {symbol} (V6.3): { {k: round(v, 4) if isinstance(v, (int, float)) else v for k, v in indicators.items()} }")
        return indicators

    except BinanceAPIException as api_err:
        if api_err.code == -1120: logger.error(f"API Error: Invalid interval '{KLINE_INTERVAL_ENTRY}' used for {symbol}. Code: {api_err.code}, Msg: {api_err.message}")
        else: logger.warning(f"API error fetching 5m klines for {symbol} indicators: {api_err}")
        return None
    except Exception as e:
        logger.error(f"Error calculating 5m indicators for {symbol} (V6.3): {e}", exc_info=True)
        return None


# === Entry Signal Check Function (V6.3 Implementation with Updated Params) ===
async def check_entry_signal(symbol, side, indicators):
    """
    Checks if V6.3 entry conditions (5min timeframe + breakout) are met.
    Uses updated RSI ranges and Volume multiplier.
    Returns True if entry conditions met, False otherwise.
    """
    if not indicators:
        logger.debug(f"Skipping entry check for {symbol}: No indicators provided.")
        return False

    # Extract latest values
    price = indicators.get("Close")
    ema20 = indicators.get("EMA_20")
    macd_line = indicators.get("MACD_line")
    macd_signal = indicators.get("MACD_signal")
    rsi = indicators.get("RSI_14")
    volume = indicators.get("Volume")
    vol_ma20 = indicators.get("Vol_MA_20")
    recent_high = indicators.get("RecentHigh")
    recent_low = indicators.get("RecentLow")

    # Check if any required value is None
    required_values = [price, ema20, macd_line, macd_signal, rsi, volume, vol_ma20, recent_high, recent_low]
    if any(v is None for v in required_values):
        logger.warning(f"Cannot check entry signal for {symbol} (V6.3): Missing required indicator values.")
        return False

    logger.debug(f"Checking {side} 5m entry signal for {symbol} (V6.3) - Price:{price:.4f} EMA:{ema20:.4f} MACD_L:{macd_line:.4f} MACD_S:{macd_signal:.4f} RSI:{rsi:.2f} Vol:{volume} VolMA:{vol_ma20:.2f} RecHigh:{recent_high:.4f} RecLow:{recent_low:.4f}")

    results = {} # Store individual results for detailed logging

    if side == "LONG":
        # Trend & Momentum
        results['Price > EMA20'] = price > ema20
        results['MACD Line > Signal'] = macd_line > macd_signal
        # <<< Use UPDATED RSI Range >>>
        results[f'RSI Range ({RSI_ENTRY_MIN_LONG}-{RSI_ENTRY_MAX_LONG})'] = RSI_ENTRY_MIN_LONG <= rsi <= RSI_ENTRY_MAX_LONG
        # <<< Use UPDATED Volume Spike Multiplier >>>
        results[f'Volume Spike (>{VOLUME_SPIKE_MULTIPLIER_ENTRY}x)'] = volume >= (vol_ma20 * VOLUME_SPIKE_MULTIPLIER_ENTRY) if vol_ma20 > 0 else False
        # Breakout Confirmation (Unchanged)
        results['Breakout Above Recent High'] = price > recent_high

        all_conditions_met = all(results.values())

        if all_conditions_met:
            logger.info(f"LONG Entry Signal Confirmed for {symbol} (V6.3 - 5m + Breakout)")
            return True
        else:
            if logger.isEnabledFor(logging.DEBUG):
                failed_conditions = {k: v for k, v in results.items() if not v}
                logger.debug(f"LONG Entry Signal Conditions Not Met for {symbol} (V6.3). Failed: {failed_conditions}")
            else:
                logger.info(f"LONG Entry signal conditions not met for {symbol} (V6.3).")
            return False

    elif side == "SHORT":
        # Trend & Momentum
        results['Price < EMA20'] = price < ema20
        results['MACD Line < Signal'] = macd_line < macd_signal
        # <<< Use UPDATED RSI Range >>>
        results[f'RSI Range ({RSI_ENTRY_MIN_SHORT}-{RSI_ENTRY_MAX_SHORT})'] = RSI_ENTRY_MIN_SHORT <= rsi <= RSI_ENTRY_MAX_SHORT
        # <<< Use UPDATED Volume Spike Multiplier >>>
        results[f'Volume Spike (>{VOLUME_SPIKE_MULTIPLIER_ENTRY}x)'] = volume >= (vol_ma20 * VOLUME_SPIKE_MULTIPLIER_ENTRY) if vol_ma20 > 0 else False
        # Breakout Confirmation (Unchanged)
        results['Breakdown Below Recent Low'] = price < recent_low

        all_conditions_met = all(results.values())

        if all_conditions_met:
            logger.info(f"SHORT Entry Signal Confirmed for {symbol} (V6.3 - 5m + Breakout)")
            return True
        else:
            if logger.isEnabledFor(logging.DEBUG):
                failed_conditions = {k: v for k, v in results.items() if not v}
                logger.debug(f"SHORT Entry Signal Conditions Not Met for {symbol} (V6.3). Failed: {failed_conditions}")
            else:
                logger.info(f"SHORT Entry signal conditions not met for {symbol} (V6.3).")
            return False

    else:
        logger.warning(f"Invalid side '{side}' passed to check_entry_signal for {symbol}.")
        return False


# === Account Balance Fetching (Manual Request Version) ===
# (Same as V6.2 - No Changes Needed)
async def get_account_balance(force_refresh=False):
    """ Fetches available USDT balance from futures account using a manual signed request. Uses caching. """
    global account_balance_cache
    now = time.time()
    if not force_refresh and account_balance_cache['balance'] is not None and (now - account_balance_cache['timestamp'] < BALANCE_CACHE_DURATION):
        logger.debug(f"Using cached balance: {account_balance_cache['balance']:.2f} USDT")
        if isinstance(account_balance_cache['balance'], Decimal): return account_balance_cache['balance']
        else:
            try: return Decimal(str(account_balance_cache['balance']))
            except Exception: logger.error("Cached balance is not a valid Decimal. Forcing refetch."); account_balance_cache['balance'] = None

    logger.info(f"Fetching account balance via manual signed request (Force Refresh: {force_refresh})...")
    usdt_balance = None
    try:
        timestamp = int(time.time() * 1000)
        params = {"timestamp": timestamp}
        query_string = urlencode(params)
        signature = hmac.new( BINANCE_SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256 ).hexdigest()
        params["signature"] = signature
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        account_url = "https://fapi.binance.com/fapi/v2/account"

        logger.debug(f"Manually requesting: GET {account_url} Headers: {headers} Params: {params}")
        response = await asyncio.to_thread(requests.get, account_url, headers=headers, params=params, timeout=15)

        logger.info(f"Manual Account Balance Check - HTTP Status: {response.status_code}")
        response_text_preview = response.text[:500] + ('...' if len(response.text) > 500 else '')
        logger.debug(f"Manual Account Balance Check - Response Preview: {response_text_preview}")

        if response.status_code == 200:
            try:
                account_info = response.json()
                for asset_data in account_info.get('assets', []):
                    if asset_data.get('asset') == 'USDT':
                        usdt_balance = Decimal(asset_data.get('availableBalance', '0'))
                        logger.info(f"Manual fetch successful: Found available balance: {usdt_balance:.2f} USDT")
                        break
                if usdt_balance is None: logger.error("Manual fetch succeeded (HTTP 200) but could not find USDT 'availableBalance' in assets.")
            except json.JSONDecodeError: logger.error(f"Manual fetch succeeded (HTTP 200) but failed to decode JSON response: {response_text_preview}")
            except Exception as parse_err: logger.error(f"Manual fetch succeeded (HTTP 200) but error parsing response: {parse_err}")
        else:
            try:
                error_data = response.json()
                binance_code = error_data.get('code', 'N/A'); binance_msg = error_data.get('msg', 'N/A')
                logger.error(f"Manual fetch failed! HTTP Status: {response.status_code}, Binance Code: {binance_code}, Msg: {binance_msg}. Response: {response_text_preview}")
            except json.JSONDecodeError: logger.error(f"Manual fetch failed! HTTP Status: {response.status_code}. Response was not JSON: {response_text_preview}")
    except requests.exceptions.Timeout: logger.error(f"Manual fetch timed out after 15 seconds.")
    except requests.exceptions.RequestException as req_err: logger.error(f"Manual fetch failed due to requests error: {req_err}")
    except Exception as e: logger.error(f"Unexpected error during manual account balance fetch: {e}", exc_info=True)

    if usdt_balance is not None:
        account_balance_cache['balance'] = usdt_balance
        account_balance_cache['timestamp'] = now
    else:
        logger.warning("Returning None for account balance after manual fetch attempt.")
        account_balance_cache['balance'] = None # Ensure cache reflects failure
        account_balance_cache['timestamp'] = now # Update timestamp even on failure

    return usdt_balance


# === Dynamic Position Size Calculation (V6.3 - No changes from V6.2 logic) ===
async def calculate_position_size(symbol, side, entry_price, stop_loss_price):
    """
    Calculates V6.3 position size based on risk %, SL distance, and min notional.
    Returns Decimal quantity or None.
    (Logic identical to V6.2)
    """
    logger.debug(f"Calculating dynamic position size for {symbol} {side} (V6.3)...")
    try:
        entry_price_dec = Decimal(str(entry_price))
        stop_loss_price_dec = Decimal(str(stop_loss_price))

        if side == "LONG" and stop_loss_price_dec >= entry_price_dec:
            logger.error(f"Invalid SL for {symbol} LONG: SL price {stop_loss_price_dec} >= Entry price {entry_price_dec}")
            return None
        if side == "SHORT" and stop_loss_price_dec <= entry_price_dec:
            logger.error(f"Invalid SL for {symbol} SHORT: SL price {stop_loss_price_dec} <= Entry price {entry_price_dec}")
            return None

        stop_distance_abs = abs(entry_price_dec - stop_loss_price_dec)
        if stop_distance_abs <= 0:
            logger.error(f"Stop distance is zero or negative for {symbol}. Cannot calculate size.")
            return None

        available_balance = await get_account_balance()
        if available_balance is None:
            logger.error("Failed to fetch account balance. Cannot calculate position size.")
            return None
        if available_balance <= 0:
            logger.error(f"Available balance is zero or negative (${available_balance:.2f}). Cannot calculate position size.")
            return None

        risk_amount_usdt = available_balance * (Decimal(str(RISK_PER_TRADE_PERCENT)) / Decimal(100))
        logger.debug(f"Risk Amount for {symbol}: {RISK_PER_TRADE_PERCENT}% of ${available_balance:.2f} = ${risk_amount_usdt:.4f}")
        if risk_amount_usdt <= 0:
            logger.error(f"Calculated risk amount ${risk_amount_usdt:.4f} is zero or negative. Cannot size position.")
            return None

        quantity = risk_amount_usdt / stop_distance_abs
        logger.debug(f"Calculated Raw Quantity for {symbol}: ${risk_amount_usdt:.4f} / ${stop_distance_abs:.8f} = {quantity:.8f}")

        adjusted_quantity = await asyncio.to_thread(adjust_qty_to_filters, symbol, quantity)

        if adjusted_quantity is None or adjusted_quantity <= 0:
            logger.warning(f"Position size calculation resulted in zero or invalid quantity for {symbol} after adjustments. Raw Qty: {quantity:.8f}")
            return None

        # Final check: Min Notional Value
        symbol_info = exchange_info_map.get(symbol)
        min_qty_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'), None)
        min_qty = Decimal(min_qty_filter.get('minQty', "0")) if min_qty_filter else Decimal("0")

        calculated_notional = adjusted_quantity * entry_price_dec

        if calculated_notional < Decimal(str(MIN_NOTIONAL_VALUE)) or adjusted_quantity < min_qty:
            logger.warning(f"Calculated position size for {symbol} (Qty: {adjusted_quantity}) with notional ${calculated_notional:.2f} does not meet minimums (MinNotional: ${MIN_NOTIONAL_VALUE:.2f}, MinQty: {min_qty}). Skipping trade.")
            return None

        logger.info(f"Calculated Position Size for {symbol} {side} (V6.3): Risk ${risk_amount_usdt:.2f}, SL Dist ${stop_distance_abs:.4f} -> Qty {adjusted_quantity}")
        return adjusted_quantity

    except Exception as e:
        logger.error(f"Error calculating position size for {symbol} (V6.3): {e}", exc_info=True)
        return None


# === Stop Loss Calculation Function (V6.3 Update: ATR x 2.5, 5% cap, Updated Memecoins) ===
async def calculate_stop_loss_price(symbol, side, entry_price, indicators):
    """
    Calculates V6.3 initial SL price (ATR*2.5 or Max 5%, Memecoin specific).
    Uses updated Max SL % and Memecoin list/constants.
    Returns the calculated SL price (float) or None on error.
    """
    logger.debug(f"Calculating SL for {symbol} {side} (V6.3)...")
    try:
        entry_price_float = float(entry_price)
        entry_price_dec = Decimal(str(entry_price))

        # <<< Check for Memecoin specific SL using V6.3 list and constants >>>
        if symbol in MEMECOIN_SYMBOLS:
            logger.info(f"Applying Memecoin SL rule ({MEMECOIN_SL_PERCENT}%) for {symbol}")
            stop_distance = entry_price_float * (MEMECOIN_SL_PERCENT / 100.0)
            if side == "LONG":
                sl_price = entry_price_float - stop_distance
                sl_price_dec = Decimal(str(sl_price)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)
            else: # SHORT
                sl_price = entry_price_float + stop_distance
                sl_price_dec = Decimal(str(sl_price)).quantize(Decimal('1e-8'), rounding=ROUND_UP)

            if sl_price_dec <= 0:
                logger.error(f"Calculated Memecoin SL price for {symbol} is zero or negative ({sl_price_dec}). Cannot set SL.")
                return None
            logger.info(f"Calculated Memecoin SL Price for {symbol} {side}: Entry=${entry_price_float:.4f}, Fixed % Dist=${stop_distance:.4f} -> SL=${float(sl_price_dec):.4f}")
            return float(sl_price_dec)

        # --- Standard SL Calculation ---
        atr_value = indicators.get('ATR_14') if indicators else None

        # <<< Calculate Max Percentage Stop Distance (5%) >>>
        max_percent_stop_distance = entry_price_float * (EXIT_STOP_LOSS_PERCENT / 100.0)

        # Calculate ATR Stop Distance (ATR x 2.5) (Multiplier unchanged)
        atr_stop_distance = None
        if atr_value is not None and not math.isnan(atr_value) and atr_value > 0:
            atr_stop_distance = atr_value * ATR_MULTIPLIER_SL
            logger.debug(f"ATR Stop Calc for {symbol} (5m): ATR={atr_value:.4f}, Multiplier={ATR_MULTIPLIER_SL}, ATR Dist={atr_stop_distance:.4f}")
        else:
            if indicators: logger.warning(f"ATR value invalid ({atr_value}) for {symbol} (5m). Cannot use ATR for SL calc.")

        # Determine the stop distance: Tighter of ATR*2.5 or Max 5%
        if atr_stop_distance is not None and atr_stop_distance > 0:
            stop_distance = min(max_percent_stop_distance, atr_stop_distance)
            logger.debug(f"Using Tighter SL Distance for {symbol}: Min(Max% ${max_percent_stop_distance:.4f}, ATR ${atr_stop_distance:.4f}) -> ${stop_distance:.4f}")
        else:
            # Fallback to max percentage if ATR is invalid
            stop_distance = max_percent_stop_distance
            logger.debug(f"Using Max Percent SL Distance for {symbol} (ATR invalid): ${stop_distance:.4f}")


        # Calculate final SL price
        if side == "LONG":
            sl_price = entry_price_float - stop_distance
            sl_price_dec = Decimal(str(sl_price)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)
        elif side == "SHORT":
            sl_price = entry_price_float + stop_distance
            sl_price_dec = Decimal(str(sl_price)).quantize(Decimal('1e-8'), rounding=ROUND_UP)
        else:
            return None

        # Final validation
        if sl_price_dec <= 0:
            logger.error(f"Calculated SL price for {symbol} is zero or negative ({sl_price_dec}). Cannot set SL.")
            return None

        logger.info(f"Calculated SL Price for {symbol} {side}: Entry=${entry_price_float:.4f}, Dist=${stop_distance:.4f} (ATR/Max%) -> SL=${float(sl_price_dec):.4f}")
        return float(sl_price_dec)

    except Exception as e:
        logger.error(f"Error calculating stop loss for {symbol} (V6.3): {e}", exc_info=True)
        return None


# === Execute Trade Function (Base - Synchronous) - V6.3 === #
def _execute_trade_sync_v6_3(symbol, side, quantity_to_trade, is_reduce_only=False):
    """Synchronous base function for V6.3. Assumes quantity is calculated/adjusted."""
    # (Logic remains the same as V6.2, only logging/naming updated)
    try:
        symbol_info = exchange_info_map.get(symbol)
        if not symbol_info or symbol_info.get('status') != 'TRADING':
            logger.error(f"Trade failed (sync V6.3): Pre-check failed for {symbol}.")
            return None

        order_side = side.upper()
        if order_side not in ["BUY", "SELL"]: raise ValueError(f"Invalid side: {side}")
        quantity_dec = Decimal(str(quantity_to_trade))
        if quantity_dec <= 0: raise ValueError(f"Invalid quantity: {quantity_dec}")

        # Set margin type and leverage (ignore errors if already set)
        try: futures_client.futures_change_margin_type(symbol=symbol, marginType='CROSSED')
        except BinanceAPIException as mt_err:
            if mt_err.code != -4046: logger.warning(f"Could not set margin type for {symbol}: {mt_err}")
        try: futures_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
        except BinanceAPIException as lev_err:
            if lev_err.code != -4049: logger.warning(f"Could not set leverage for {symbol}: {lev_err}")

        # Format quantity
        qty_precision = symbol_info.get('quantityPrecision', 0)
        quantity_str = str(quantity_dec.quantize(Decimal('1e-' + str(qty_precision)), rounding=ROUND_DOWN))
        if Decimal(quantity_str) <= 0: raise ValueError(f"Formatted quantity is zero: {quantity_str}")

        order_params = {'symbol': symbol, 'side': order_side, 'type': ORDER_TYPE_MARKET, 'quantity': quantity_str}
        if is_reduce_only: order_params['reduceOnly'] = 'true'

        logger.info(f"Prepared Trade (sync V6.3) - Symbol: {symbol}, Side: {order_side}, Qty String: '{quantity_str}', ReduceOnly: {is_reduce_only}")

        # Place the market order
        order = futures_client.futures_create_order(**order_params)
        logger.info(f"Submitted (sync V6.3) {order_side} order for {symbol}. ID: {order.get('orderId')}, Status: {order.get('status')}")
        order['intended_quantity'] = float(quantity_dec)
        return order

    except BinanceAPIException as api_err:
        logger.error(f"Trade failed (sync V6.3) for {symbol}: Binance API error. Code={api_err.code}, Msg={api_err.message}. Check debug logs.")
        return None
    except Exception as e:
        logger.error(f"Trade failed (sync V6.3) for {symbol}: Unexpected error: {e}", exc_info=True)
        return None

# === Execute Trade Function (Async Wrapper - V6.3) ===
async def execute_trade_v6_3(application: Application, symbol, side, quantity_to_trade=None, is_reduce_only=False, position_data=None):
    """
    Async wrapper V6.3: Places trade, verifies fill, updates state.
    Requires quantity. `position_data` used for exits.
    """
    # (Logic remains the same as V6.2, only logging/naming updated, calls new sync func)
    global state
    order = None
    if quantity_to_trade is None or quantity_to_trade <= 0:
        logger.error(f"execute_trade_v6_3 called for {symbol} {side} with invalid quantity: {quantity_to_trade}")
        return None

    try:
        # Run the synchronous trade execution in a separate thread
        order = await asyncio.to_thread(
            _execute_trade_sync_v6_3, symbol, side, quantity_to_trade=quantity_to_trade, is_reduce_only=is_reduce_only
        )
        if order is None: return None
        if not order.get('orderId'):
            logger.warning(f"_execute_trade_sync_v6_3 returned invalid order object for {symbol} {side}: {order}")
            return None

        order_id = order['orderId']
        qty_filled = 0.0; avg_price = 0.0; order_status = "UNKNOWN"
        commission = 0.0; commission_asset = None
        verification_attempts = 3; verified = False

        # Verify order fill status
        for attempt in range(verification_attempts):
            await asyncio.sleep(0.6 + attempt * 0.4)
            try:
                order_details = await asyncio.to_thread(futures_client.futures_get_order, symbol=symbol, orderId=order_id)
                qty_filled = float(order_details.get('executedQty', 0))
                avg_price = float(order_details.get('avgPrice', 0))
                order_status = order_details.get('status')
                commission = float(order_details.get('commission', 0.0))
                commission_asset = order_details.get('commissionAsset')

                logger.info(f"Verify Attempt {attempt+1}: Order {order_id} ({symbol} {side}) - Status: {order_status}, Filled Qty: {qty_filled}, Avg Price: {avg_price}, Comm: {commission} {commission_asset}")

                if order_status in ['FILLED'] and qty_filled > 0:
                    verified = True; break
                elif order_status in ['CANCELED', 'EXPIRED', 'REJECTED', 'PENDING_CANCEL']:
                    logger.warning(f"Order {order_id} ({symbol} {side}) has final/failed status {order_status}. Trade failed.")
                    return None
            except BinanceAPIException as api_err:
                if api_err.code == -2013: # Order does not exist
                    logger.warning(f"Verify Attempt {attempt+1}: Order {order_id} ({symbol} {side}) does not exist (API code {api_err.code}). Assuming failure.")
                    return None
                logger.warning(f"Verify Attempt {attempt+1}: API Error checking order {order_id} ({symbol} {side}): {api_err}. Check debug logs.")
            except Exception as e: logger.error(f"Verify Attempt {attempt+1}: Error checking order {order_id} ({symbol} {side}): {e}")

            if attempt == verification_attempts - 1 and not verified:
                logger.error(f"Failed to verify order {order_id} ({symbol} {side}) fill status after {verification_attempts} attempts. Assuming failure.")
                return None

        # --- Update state based on verified order ---
        if not verified: return None

        order['executedQty'] = qty_filled
        order['avgPrice'] = avg_price
        order['status'] = order_status
        order['commission'] = commission
        order['commissionAsset'] = commission_asset

        trade_side_long_short = "LONG" if side.upper() == "BUY" else "SHORT"

        if not is_reduce_only: # --- Handle Entry ---
            if avg_price > 0:
                entry_time_dt = datetime.now(timezone.utc)
                # Calculate initial SL using the actual filled entry price and current indicators
                indicators_for_sl = await calculate_indicators(symbol)
                if indicators_for_sl is None:
                    logger.error(f"Could not calculate indicators for {symbol} entry post-fill. Cannot determine SL. Aborting entry state update.")
                    # Consider if we should try to close the position immediately here? Risky.
                    return None
                sl_price = await calculate_stop_loss_price(symbol, trade_side_long_short, avg_price, indicators_for_sl)
                if sl_price is None:
                    logger.error(f"Could not calculate SL price for {symbol} {trade_side_long_short} entry post-fill. Aborting state update.")
                    return None

                notional_value = qty_filled * avg_price

                new_position = {
                    "symbol": symbol, "order_id": order_id, "client_order_id": order.get('clientOrderId'),
                    "entry_price": avg_price, "entry_time": entry_time_dt.isoformat(),
                    "side": trade_side_long_short, "qty": qty_filled,
                    "notional_value": notional_value,
                    "initial_sl_price": sl_price, "current_sl_price": sl_price,
                    "status": "open", "user": "DEFAULT",
                    "high_watermark": avg_price, "low_watermark": avg_price,
                    "partial_tp_hit": False, # Used for Trailing Stop activation
                    "trailing_stop_active": False
                }
                state.setdefault("open_positions", {})[symbol] = new_position
                logger.info(f"{side.upper()} success: Added {symbol} {trade_side_long_short} to state (Qty: {qty_filled}, Entry: ${avg_price:.4f}, Notional: ${notional_value:.2f}, Init SL: ${sl_price:.4f})")
                save_state()

                # Send Telegram Alert
                alert_side = "BUY ENTRY" if trade_side_long_short == "LONG" else "SELL ENTRY (SHORT)"
                alert_emoji = "🚀" if trade_side_long_short == "LONG" else "🔻"
                await send_alert(
                    application,
                    f"{alert_emoji} {alert_side} <code>{html.escape(symbol)}</code>\n"
                    f"Entry ≈ ${avg_price:.4f}, Qty: {qty_filled}, Value ≈ ${notional_value:.2f}\n"
                    f"Initial SL ≈ ${sl_price:.4f}"
                )
                return order
            else:
                logger.warning(f"{side.upper()} entry order {order_id} ({symbol}) verified but avg price is invalid.")
                return None

        elif is_reduce_only: # --- Handle Exit (ReduceOnly=True) ---
            logger.info(f"Exit order {order_id} ({symbol} {side}) confirmed as {order_status}.")
            # PnL calc and state removal happen in handle_exit
            return order

    except Exception as e:
        logger.error(f"Unexpected error in execute_trade_v6_3 for {symbol} {side}: {e}", exc_info=True)
        return None

# === Cooldown Management Functions ===
# (Unchanged from V6.2)
def set_cooldown(symbol):
    """Sets the cooldown end time for a symbol."""
    global state
    cooldown_end_time = datetime.now(timezone.utc) + timedelta(minutes=REENTRY_COOLDOWN_MINUTES)
    state.setdefault("symbol_cooldowns", {})[symbol] = cooldown_end_time.isoformat()
    logger.info(f"Cooldown set for {symbol} until {cooldown_end_time.isoformat()}.")
    save_state() # Save state after setting cooldown

def is_on_cooldown(symbol):
    """Checks if a symbol is currently on cooldown."""
    global state
    cooldown_end_str = state.get("symbol_cooldowns", {}).get(symbol)
    if not cooldown_end_str:
        return False # No cooldown record

    try:
        cooldown_end_time = isoparse(cooldown_end_str)
        if datetime.now(timezone.utc) < cooldown_end_time:
            logger.debug(f"{symbol} is on cooldown until {cooldown_end_time.isoformat()}.")
            return True
        else:
            # Cooldown expired, remove the entry
            logger.debug(f"Cooldown for {symbol} expired. Removing entry.")
            del state["symbol_cooldowns"][symbol]
            # No need to save state here, it will be saved on next relevant action
            return False
    except (ValueError, TypeError):
        logger.warning(f"Invalid cooldown timestamp '{cooldown_end_str}' for {symbol} in state. Removing.")
        if symbol in state.get("symbol_cooldowns", {}):
             del state["symbol_cooldowns"][symbol]
        return False


# === Risk Guard Check Function ===
async def check_risk_limits(application: Application):
    """Checks daily and consecutive loss limits. Returns True if trading should pause, False otherwise."""
    global state
    today_str = date.today().isoformat()
    daily_stats = state.get("daily_stats", {}).get(today_str)

    if not daily_stats:
        logger.warning(f"Daily stats for {today_str} not found. Cannot check risk limits.")
        return False # Cannot determine pause status

    # Check if already paused today
    if state.get("trading_paused_reason"):
        logger.info(f"Trading remains paused due to: {state['trading_paused_reason']}")
        return True

    pause_trading = False
    pause_reason = None

    # Check Consecutive Loss Limit
    consecutive_losses = daily_stats.get("consecutive_losses", 0)
    if consecutive_losses >= CONSECUTIVE_LOSS_LIMIT:
        if not daily_stats.get("consecutive_loss_limit_hit", False): # Check if alert already sent today
             logger.warning(f"RISK GUARD HIT: Consecutive loss limit ({CONSECUTIVE_LOSS_LIMIT}) reached ({consecutive_losses} losses). Pausing new entries for today.")
             await send_alert(application, f"🚨 RISK GUARD HIT 🚨\nConsecutive loss limit ({CONSECUTIVE_LOSS_LIMIT}) reached. New entries paused for today.")
             daily_stats["consecutive_loss_limit_hit"] = True # Mark as hit for today
        pause_trading = True
        pause_reason = "CONSECUTIVE_LOSS"


    # Check Daily Loss Limit
    start_balance = daily_stats.get("start_balance")
    current_daily_pnl = daily_stats.get("pnl", 0.0)
    if start_balance is not None and start_balance > 0 and current_daily_pnl < 0:
        daily_loss_percent = (abs(current_daily_pnl) / start_balance) * 100
        logger.debug(f"Daily Loss Check: PNL ${current_daily_pnl:.2f}, Start Bal ${start_balance:.2f}, Loss % {daily_loss_percent:.2f}")
        if daily_loss_percent >= DAILY_LOSS_LIMIT_PERCENT:
            if not daily_stats.get("daily_loss_limit_hit", False): # Check if alert already sent today
                logger.warning(f"RISK GUARD HIT: Daily loss limit ({DAILY_LOSS_LIMIT_PERCENT}%) reached ({daily_loss_percent:.2f}% loss). Pausing new entries for today.")
                await send_alert(application, f"🚨 RISK GUARD HIT 🚨\nDaily loss limit ({DAILY_LOSS_LIMIT_PERCENT}%) reached ({daily_loss_percent:.2f}% loss). New entries paused for today.")
                daily_stats["daily_loss_limit_hit"] = True # Mark as hit for today
            pause_trading = True
            # Prioritize showing daily loss if both limits hit
            pause_reason = "DAILY_LOSS"

    if pause_trading:
        state["trading_paused_reason"] = pause_reason
        save_state() # Save the paused state
        return True
    else:
        # Ensure pause reason is cleared if limits are no longer hit (e.g., state edited manually)
        if state.get("trading_paused_reason") is not None:
             logger.info("Risk limits clear. Resuming trading.")
             state["trading_paused_reason"] = None
             save_state()
        return False


# === Update Daily Stats Function ===
async def update_daily_stats(profit, start_balance_today):
    """Updates daily trade stats, PnL, consecutive losses."""
    global state
    today_str = date.today().isoformat()
    stats = state.setdefault("daily_stats", {}).setdefault(today_str, {
            "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            "consecutive_losses": 0, "start_balance": start_balance_today, # Set start balance if provided
            "daily_loss_limit_hit": False, "consecutive_loss_limit_hit": False
        })

    # Ensure start balance is recorded if not already set
    if stats.get("start_balance") is None and start_balance_today is not None:
        stats["start_balance"] = float(start_balance_today) # Ensure float

    stats["trades"] += 1
    stats["pnl"] = float(stats.get("pnl", 0.0)) + float(profit)

    if profit > 0:
        stats["wins"] += 1
        stats["consecutive_losses"] = 0 # Reset on win
    elif profit < 0:
        stats["losses"] += 1
        stats["consecutive_losses"] = stats.get("consecutive_losses", 0) + 1
    # else: # profit == 0 (breakeven), don't change consecutive loss count

    logger.info(f"Updated Daily Stats ({today_str}): Trades={stats['trades']}, Wins={stats['wins']}, Losses={stats['losses']}, PnL=${stats['pnl']:.4f}, Consec Losses={stats['consecutive_losses']}")
    # Risk limit check will happen in the main loop before entry


# === Exit Condition Check Function (V6.3 Implementation with Updated Params) ===
async def check_exit_conditions(symbol, position_info, current_price):
    """
    Checks V6.3 exit conditions: SL, Trailing Stop, Timeout, Signal Invalidated.
    Uses updated TP %, Trailing %, Timeout, Memecoin rules.
    Returns exit_reason string or None.
    """
    global state
    exit_reason = None
    try:
        side = position_info['side']
        entry_price = float(position_info['entry_price'])
        current_sl_price = float(position_info.get('current_sl_price', 0))
        entry_time_str = position_info.get('entry_time')
        partial_tp_hit = position_info.get('partial_tp_hit', False) # Flag for trailing activation
        trailing_active = position_info.get('trailing_stop_active', False)
        high_watermark = float(position_info.get('high_watermark', entry_price))
        low_watermark = float(position_info.get('low_watermark', entry_price))

        # Update watermarks (Unchanged)
        new_high_watermark = max(high_watermark, current_price)
        new_low_watermark = min(low_watermark, current_price)
        if new_high_watermark != high_watermark: position_info['high_watermark'] = new_high_watermark
        if new_low_watermark != low_watermark: position_info['low_watermark'] = new_low_watermark

        # Calculate PnL Percentage (Unchanged)
        pnl_percent = 0.0
        if entry_price > 0 and current_price > 0:
             pnl_percent = ((current_price / entry_price) - 1) * 100 if side == "LONG" else ((entry_price / current_price) - 1) * 100

        # --- Determine TP Activation Threshold (Normal vs Memecoin - Using V6.3 params) ---
        # <<< Use UPDATED Normal TP % and Memecoin Activation % >>>
        tp_activation_percent = MEMECOIN_TRAILING_ACTIVATION_PERCENT if symbol in MEMECOIN_SYMBOLS else EXIT_TAKE_PROFIT_PERCENT
        tp_activation_price = entry_price * (1 + tp_activation_percent / 100.0) if side == "LONG" else entry_price * (1 - tp_activation_percent / 100.0)

        # --- Check Initial Take Profit (and activate trailing stop) ---
        if not partial_tp_hit: # Only check if trailing isn't already active
            if (side == "LONG" and current_price >= tp_activation_price) or \
               (side == "SHORT" and current_price <= tp_activation_price):
                logger.info(f"Trailing Stop activation threshold ({tp_activation_percent}%) hit for {symbol}. Activating.")
                position_info['partial_tp_hit'] = True # Mark activation condition met
                position_info['trailing_stop_active'] = True
                trailing_active = True # Update local variable
                partial_tp_hit = True # Update local variable
                # save_state() # Optional: Save state immediately on activation

        # --- Check Trailing Stop (if active) ---
        if trailing_active:
            new_sl_price = current_sl_price
            # <<< Use UPDATED Trailing Stop Percentage >>>
            trail_percent = TRAILING_STOP_PERCENT
            if side == "LONG":
                trailing_sl = new_high_watermark * (1 - trail_percent / 100.0)
                new_sl_price = max(current_sl_price, trailing_sl)
            elif side == "SHORT":
                trailing_sl = new_low_watermark * (1 + trail_percent / 100.0)
                new_sl_price = min(current_sl_price, trailing_sl)

            # Update SL in state if it moved
            if new_sl_price != current_sl_price:
                sl_price_dec = Decimal(str(new_sl_price)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN if side=="LONG" else ROUND_UP)
                if sl_price_dec > 0:
                    new_sl_price_float = float(sl_price_dec)
                    if new_sl_price_float != current_sl_price:
                        logger.info(f"Trailing Stop for {symbol} ({side}) moved from ${current_sl_price:.4f} to ${new_sl_price_float:.4f}")
                        position_info['current_sl_price'] = new_sl_price_float
                        current_sl_price = new_sl_price_float # Update local variable for SL check
                        # save_state() # Optional save
                else:
                    logger.warning(f"Trailing stop calculation for {symbol} resulted in invalid price: {sl_price_dec}. SL not updated.")


        # --- Check Stop Loss (using potentially updated current_sl_price) ---
        if current_sl_price > 0:
            if side == "LONG" and current_price <= current_sl_price:
                exit_reason = f"Trailing Stop Hit (${current_sl_price:.4f})" if trailing_active else f"Stop Loss Hit (${current_sl_price:.4f})"
            elif side == "SHORT" and current_price >= current_sl_price:
                exit_reason = f"Trailing Stop Hit (${current_sl_price:.4f})" if trailing_active else f"Stop Loss Hit (${current_sl_price:.4f})"


        # --- Check Timeout Exit (Revised V6.3 Logic) ---
        # <<< Use NEW Timeout and Flat % >>>
        if not exit_reason and entry_time_str:
            entry_time = isoparse(entry_time_str)
            elapsed_time = datetime.now(timezone.utc) - entry_time
            elapsed_minutes = elapsed_time.total_seconds() / 60

            if elapsed_minutes > TIMEOUT_EXIT_MINUTES:
                if abs(pnl_percent) <= TIMEOUT_EXIT_FLAT_PERCENT: # Check if PnL is within the flat range
                     exit_reason = f"Timeout Exit ({TIMEOUT_EXIT_MINUTES} min flat ±{TIMEOUT_EXIT_FLAT_PERCENT}%)"
                # else: # PnL is positive or negative beyond the flat threshold, let it run or hit SL/Trailing
                #     logger.debug(f"{symbol} PnL ({pnl_percent:.2f}%) outside flat range at {elapsed_minutes:.1f} min. Letting run.")


        # --- Check Signal Invalidated Exit (Logic Unchanged from V6.2) ---
        if not exit_reason:
            logger.debug(f"Checking signal invalidation for {symbol} (V6.3)...")
            # Fetch fresh 5m indicators needed for this check
            indicators = await calculate_indicators(symbol)
            if indicators:
                ema20 = indicators.get("EMA_20")
                vwap = indicators.get("VWAP")
                rsi = indicators.get("RSI_14")
                prev_close = indicators.get("PrevClose")
                prev_ema = indicators.get("PrevEMA_20")
                prev_vwap = indicators.get("PrevVWAP")
                prev_is_red = indicators.get("PrevIsRedCandle") # Boolean

                # Check if critical values are available
                if all(v is not None for v in [current_price, ema20, vwap, rsi, prev_close, prev_ema, prev_vwap, prev_is_red]):
                    invalidation_conditions_met = 0

                    # Condition 1: Price below BOTH EMA and VWAP (Long) / above BOTH (Short)
                    cond1_long = current_price < ema20 and current_price < vwap
                    cond1_short = current_price > ema20 and current_price > vwap
                    if (side == "LONG" and cond1_long) or (side == "SHORT" and cond1_short):
                        invalidation_conditions_met += 1
                        logger.debug(f"{symbol} Invalidation Cond 1 MET: Price vs EMA/VWAP.")

                    # Condition 2: Previous candle was Red and closed below BOTH EMA and VWAP (Long) / Green and above BOTH (Short)
                    cond2_long = prev_is_red and prev_close < prev_ema and prev_close < prev_vwap
                    cond2_short = (not prev_is_red) and prev_close > prev_ema and prev_close > prev_vwap
                    if (side == "LONG" and cond2_long) or (side == "SHORT" and cond2_short):
                        invalidation_conditions_met += 1
                        logger.debug(f"{symbol} Invalidation Cond 2 MET: Prev Candle Close vs Prev EMA/VWAP (Red:{prev_is_red}).")

                    # Condition 3: RSI drops below threshold (Long) / rises above threshold (Short)
                    # (Thresholds unchanged from V6.2)
                    cond3_long = rsi < INVALIDATION_RSI_THRESHOLD_LONG
                    cond3_short = rsi > INVALIDATION_RSI_THRESHOLD_SHORT
                    if (side == "LONG" and cond3_long) or (side == "SHORT" and cond3_short):
                        invalidation_conditions_met += 1
                        logger.debug(f"{symbol} Invalidation Cond 3 MET: RSI ({rsi:.2f}) vs Threshold.")

                    # Check if at least 2 conditions are met
                    if invalidation_conditions_met >= 2:
                        exit_reason = f"Signal Invalidated ({invalidation_conditions_met}/3 conditions)"
                        logger.info(f"{symbol} {side} invalidated: Met {invalidation_conditions_met} conditions. Price ${current_price:.4f}, EMA ${ema20:.4f}, VWAP ${vwap:.4f}, RSI {rsi:.2f}")
                else:
                    logger.warning(f"Could not check signal invalidation for {symbol}: Missing required indicator values for check.")
            else:
                logger.warning(f"Could not check signal invalidation for {symbol}: Failed to fetch fresh 5m indicators.")

    except Exception as e:
        logger.error(f"Error checking exit conditions for {symbol} (V6.3): {e}", exc_info=True)
    return exit_reason


# === Entry Evaluation and Execution Function (V6.3) ===
async def evaluate_and_enter(symbol, side, application: Application):
    """Fetches V6.3 indicators, checks entry signal, cooldown, size, and enters."""
    global state
    logger.info(f"Evaluating {side} entry for {symbol} (V6.3)...")

    # Check Cooldown (Unchanged)
    if is_on_cooldown(symbol):
        logger.info(f"Skipping entry evaluation for {symbol}: Symbol is on re-entry cooldown.")
        return

    # Check Max Positions (Uses V6.3 constant)
    # This is checked again in the main loop, but double-check here
    if len(state.get("open_positions", {})) >= MAX_OPEN_POSITIONS:
         logger.info(f"Skipping entry evaluation for {symbol}: Max open positions ({MAX_OPEN_POSITIONS}) reached.")
         return

    # <<< NEW: Check if trading is paused by risk limits >>>
    if state.get("trading_paused_reason"):
        logger.info(f"Skipping entry evaluation for {symbol}: Trading paused due to {state['trading_paused_reason']}.")
        return


    # 1. Calculate 5m Indicators (V6.3 version - no logic change)
    indicators = await calculate_indicators(symbol)
    if not indicators:
        logger.warning(f"Failed to calculate 5m indicators for {symbol}. Skipping entry.")
        return

    # 2. Check 5m Entry Signal (V6.3 Implementation with updated params)
    signal_valid = await check_entry_signal(symbol, side, indicators)
    if not signal_valid:
        # Reasons logged inside check_entry_signal
        return

    # 3. Calculate SL and Position Size
    try:
        # Use mark price for entry approximation (unchanged)
        ticker_info = await asyncio.to_thread(futures_client.futures_mark_price, symbol=symbol)
        entry_price_approx = float(ticker_info['markPrice'])
        if entry_price_approx <= 0: raise ValueError("Invalid mark price")
    except Exception as e:
        logger.warning(f"Could not get mark price for {symbol} ({e}). Skipping entry.")
        return

    # Calculate SL using V6.3 logic (ATR*2.5/5% cap/Memecoin)
    stop_loss_price = await calculate_stop_loss_price(symbol, side, entry_price_approx, indicators)
    if stop_loss_price is None:
         logger.warning(f"Failed to calculate V6.3 stop loss for {symbol}. Skipping entry.")
         return

    # Calculate dynamic position size using V6.3 logic (Min Notional $10)
    quantity_to_trade_dec = await calculate_position_size(symbol, side, entry_price_approx, stop_loss_price)

    if quantity_to_trade_dec is None or quantity_to_trade_dec <= 0:
        logger.warning(f"Calculated quantity is zero or invalid for {symbol} (V6.3). Skipping entry.")
        return

    # 4. Execute Entry Trade
    logger.info(f"Entry signal valid for {symbol} {side} (V6.3). Attempting to execute trade...")
    entry_order = await execute_trade_v6_3( # Use V6.3 function name
        application,
        symbol,
        "BUY" if side=="LONG" else "SELL",
        quantity_to_trade=float(quantity_to_trade_dec),
        is_reduce_only=False
    )

    if entry_order:
        logger.info(f"Successfully submitted entry order for {symbol} {side}.")
        # State update happens within execute_trade_v6_3
        # Daily stats update happens on exit
    else:
        logger.error(f"Failed to execute entry order for {symbol} {side}.")


# === V6.3 Exit Handler (Handles Daily Stats Update) ===
async def handle_exit(symbol, exit_reason, position_info, application: Application):
    """Handles V6.3 full position exit, updates state, updates daily stats, sets cooldown."""
    global state
    logger.info(f"Executing exit for {symbol} ({position_info.get('side')}): Reason: {exit_reason}")

    try:
        side = position_info['side']
        entry_price = float(position_info['entry_price'])
        qty_open = float(position_info['qty'])
        exit_side = "SELL" if side == "LONG" else "BUY"

        # --- V6.3: Always close full amount ---
        qty_to_close = qty_open
        logger.info(f"Performing full exit for {symbol}. Qty: {qty_to_close}")
        # ---

        # Adjust quantity to filters
        qty_to_close_adjusted_dec = await asyncio.to_thread(adjust_qty_to_filters, symbol, qty_to_close)
        if qty_to_close_adjusted_dec <= 0:
            logger.error(f"Cannot exit {symbol}: Adjusted quantity to close is zero ({qty_to_close_adjusted_dec}). Original qty: {qty_to_close}")
            if symbol in state.get("open_positions", {}):
                logger.warning(f"Removing position {symbol} from state due to zero adjusted exit quantity.")
                del state["open_positions"][symbol]
                set_cooldown(symbol) # Still set cooldown even if exit failed due to qty adjustment
                # Don't record PnL or stats for this failed exit
                save_state()
            return

        # Execute the exit order using V6.3 function
        exit_order = await execute_trade_v6_3(
            application,
            symbol,
            exit_side,
            quantity_to_trade=float(qty_to_close_adjusted_dec),
            is_reduce_only=True,
            position_data=position_info
        )

        if exit_order:
            exit_price = exit_order.get('avgPrice', entry_price) # Fallback to entry for PnL if avgPrice missing
            filled_exit_qty = exit_order.get('executedQty', float(qty_to_close_adjusted_dec))
            commission = exit_order.get('commission', 0.0)
            commission_asset = exit_order.get('commissionAsset')

            if exit_price <= 0: # If avgPrice was invalid/missing and entry_price was 0 (unlikely)
                logger.error(f"Cannot calculate PnL for {symbol} exit: Invalid exit price ({exit_price})")
                profit = 0.0
                pct_change = 0.0
            else:
                # Calculate PnL
                if side == "LONG": profit = (exit_price - entry_price) * filled_exit_qty
                else: profit = (entry_price - exit_price) * filled_exit_qty

                # Adjust PnL for USDT commission if applicable
                if commission_asset and commission_asset.upper() == 'USDT':
                    profit -= commission
                elif commission > 0:
                     logger.warning(f"Commission paid in non-USDT asset ({commission_asset}) for {symbol}. PnL calculation might be slightly off.")

                pct_change = 0.0
                if entry_price > 0:
                     pct_change = ((exit_price / entry_price) - 1) * 100 if side == "LONG" else ((entry_price / exit_price) - 1) * 100


            # Create PnL record (Unchanged)
            pnl_record = {
                "symbol": symbol, "profit": round(profit, 4), "pct_change": round(pct_change, 2),
                "time": datetime.now(timezone.utc).isoformat(), "entry_price": round(entry_price, 6),
                "exit_price": round(exit_price, 6), "side": side, "qty_closed": filled_exit_qty,
                "entry_trade_id": position_info.get("order_id"), "exit_trade_id": exit_order.get('orderId'),
                "user": "DEFAULT", "exit_reason": exit_reason,
                "commission": round(commission, 6), "commission_asset": commission_asset
            }
            state.setdefault("pnl_history", []).append(pnl_record)
            logger.info(f"Closed {filled_exit_qty:.4f} {symbol} {side}: PnL ${profit:+.4f} ({pct_change:+.2f}%) (Comm: {commission} {commission_asset})")

            # Send Telegram alert (Unchanged)
            safe_exit_reason = html.escape(str(exit_reason))
            side_emoji = "⬇️" if side == "LONG" else "⬆️"
            close_type = "EXIT"
            telegram_msg = (
                f"{side_emoji} {close_type} <code>{html.escape(symbol)}</code> ({side}, {safe_exit_reason})\n"
                f"Entry: ${entry_price:.4f}, Exit ≈ ${exit_price:.4f}\n"
                f"Qty Closed: {filled_exit_qty}\n"
                f"PnL ≈ <code>${profit:+.4f} ({pct_change:+.2f}%)</code>"
            )
            await send_alert(application, telegram_msg)

            # Full close, remove position from state
            if symbol in state.get("open_positions", {}):
                del state["open_positions"][symbol]
                logger.info(f"Removed {symbol} from open positions state after full exit.")

            # <<< Update Daily Stats >>>
            today_str = date.today().isoformat()
            start_balance_today = state.get("daily_stats", {}).get(today_str, {}).get("start_balance")
            if start_balance_today is None:
                logger.warning("Start balance for today not found when updating stats. Fetching now.")
                # Attempt to fetch current balance as a fallback start balance for PnL % calc later
                # Note: This isn't ideal, start balance should ideally be captured at day start.
                current_bal = await get_account_balance(force_refresh=True)
                if current_bal is not None:
                     start_balance_today = float(current_bal)
                     state["daily_stats"][today_str]["start_balance"] = start_balance_today
                else:
                     logger.error("Failed to fetch balance to set start_balance for today. Daily loss % check might fail.")

            await update_daily_stats(profit, start_balance_today)

            # <<< Set Cooldown >>>
            set_cooldown(symbol) # Cooldown set AFTER state removal and stats update

            save_state() # Save after successful exit, stats update, and cooldown set

        else:
            # If the exit order failed
            logger.error(f"{exit_side} order failed for {symbol} exit in handle_exit. Position remains open in state.")
            await send_alert(application, f"⚠️ FAILED EXIT ORDER for <code>{html.escape(symbol)}</code> ({side}). Position may still be open!")

    except Exception as e:
        logger.error(f"Unexpected error in handle_exit for {symbol} (V6.3): {e}", exc_info=True)


# === Main Strategy Loop (V6.3 Framework with Risk Checks) === #
async def high_frequency_loop(application: Application):
    """Main loop for V6.3 strategy, includes daily setup and risk checks."""
    global state, shutdown_requested, candidate_cache
    logger.info("Starting V6.3 Trading Loop...")
    last_checked_day = None # Track the day for daily setup

    while not shutdown_requested:
        start_time = time.time()
        current_day = date.today()
        current_day_str = current_day.isoformat()

        # --- Daily Setup (Run once per day) ---
        if current_day != last_checked_day:
            logger.info(f"--- Performing Daily Setup for {current_day_str} ---")
            state["trading_paused_reason"] = None # Reset pause reason
            # Ensure today's stats entry exists
            if current_day_str not in state.get("daily_stats", {}):
                 state.setdefault("daily_stats", {})[current_day_str] = {
                     "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
                     "consecutive_losses": 0, "start_balance": None,
                     "daily_loss_limit_hit": False, "consecutive_loss_limit_hit": False
                 }

            # Get and store starting balance for the day
            logger.info("Fetching starting balance for the day...")
            start_bal = await get_account_balance(force_refresh=True)
            if start_bal is not None:
                state["daily_stats"][current_day_str]["start_balance"] = float(start_bal)
                logger.info(f"Stored start balance for {current_day_str}: ${start_bal:.2f}")
                save_state() # Save state after updating start balance
            else:
                logger.error("FAILED to fetch starting balance for the day. Daily loss limit check may fail.")
                # Consider if bot should pause if balance fetch fails?

            last_checked_day = current_day # Update the day tracker
            logger.info(f"--- Daily Setup Complete for {current_day_str} ---")

        logger.info(f"--- Starting V6.3 Scan Cycle (Time: {datetime.now(timezone.utc).isoformat()}) ---")

        # --- Phase 1: Candidate Selection (Using V6.3 logic) ---
        logger.info("Phase 1: Selecting Candidates (V6.3)...")
        candidate_longs, candidate_shorts = await asyncio.to_thread(select_candidates)
        candidate_cache['long'] = candidate_longs
        candidate_cache['short'] = candidate_shorts
        candidate_cache['timestamp'] = time.time()
        logger.info(f"Phase 1 Complete. Long Candidates: {len(candidate_longs)}, Short Candidates: {len(candidate_shorts)}")

        # --- Fetch Current Prices ---
        open_positions_dict = state.get("open_positions", {})
        symbols_to_fetch_price = list(open_positions_dict.keys()) + candidate_cache['long'] + candidate_cache['short']
        symbols_to_fetch_price = list(set(s for s in symbols_to_fetch_price if s))
        latest_prices = {}
        if symbols_to_fetch_price:
            try:
                # Use futures_mark_price for potentially smoother PnL/exit checks
                tickers_info = await asyncio.to_thread(futures_client.futures_mark_price)
                latest_prices = {ticker['symbol']: float(ticker['markPrice']) for ticker in tickers_info if ticker['symbol'] in symbols_to_fetch_price}
                logger.debug(f"Fetched latest mark prices for {len(latest_prices)} relevant symbols.")
            except Exception as e:
                logger.error(f"Failed to fetch latest mark prices: {e}")
                await asyncio.sleep(max(1, SCAN_INTERVAL - (time.time() - start_time)))
                continue # Skip cycle if price fetch fails

        # --- Phase 2a: Check Exits for Open Positions (Using V6.3 logic) ---
        exit_tasks = []
        symbols_being_exited = set()
        logger.info("Phase 2a: Checking Exits (V6.3)...")
        # Use list() to create a copy for safe iteration while potentially modifying the dict
        for symbol, position_info in list(open_positions_dict.items()):
            if symbol in symbols_being_exited: continue
            current_price = latest_prices.get(symbol)
            if current_price is None:
                logger.warning(f"No price data for open position {symbol}. Cannot check exit.")
                continue

            # <<< Call V6.3 exit logic check >>>
            exit_reason = await check_exit_conditions(symbol, position_info, current_price)

            if exit_reason:
                logger.info(f"Exit Triggered for {symbol} ({position_info.get('side', '?')}): {exit_reason}. Current Price: ${current_price:.4f}")
                symbols_being_exited.add(symbol)
                # <<< Pass necessary info to V6.3 exit handler >>>
                exit_tasks.append(handle_exit(symbol, exit_reason, position_info, application))

        if exit_tasks:
            logger.info(f"Executing {len(exit_tasks)} exit tasks...")
            await asyncio.gather(*exit_tasks)
            logger.info("Exit tasks complete.")
            # State is saved within handle_exit after successful close, stats update, cooldown set

        # --- <<< NEW: Risk Limit Check >>> ---
        logger.info("Phase 2b Pre-Check: Checking Risk Limits...")
        trading_should_pause = await check_risk_limits(application)
        if trading_should_pause:
             logger.warning(f"RISK LIMITS HIT ({state.get('trading_paused_reason')}). Skipping entry evaluations for this cycle.")
        else:
            # --- Phase 2b: Evaluate Candidates for Entry (Using V6.3 logic) ---
            logger.info("Phase 2b: Evaluating Candidates for Entry (V6.3)...")
            entry_tasks = []
            # Re-check open position count *after* exits have been processed
            open_pos_count = len(state.get("open_positions", {}))
            held_symbols = set(state.get("open_positions", {}).keys())

            # Evaluate Long Candidates
            # <<< Use NEW Max Positions Limit >>>
            if open_pos_count < MAX_OPEN_POSITIONS:
                for symbol in candidate_cache['long']:
                    if open_pos_count + len(entry_tasks) >= MAX_OPEN_POSITIONS: break
                    if symbol not in held_symbols:
                        # <<< Call V6.3 entry evaluation function >>>
                        entry_tasks.append(evaluate_and_enter(symbol, "LONG", application))

            # Evaluate Short Candidates
            if open_pos_count + len(entry_tasks) < MAX_OPEN_POSITIONS:
                for symbol in candidate_cache['short']:
                     if open_pos_count + len(entry_tasks) >= MAX_OPEN_POSITIONS: break
                     if symbol not in held_symbols:
                         entry_tasks.append(evaluate_and_enter(symbol, "SHORT", application))

            if entry_tasks:
                logger.info(f"Executing {len(entry_tasks)} entry evaluation/execution tasks...")
                await asyncio.gather(*entry_tasks)
                logger.info("Entry evaluation/execution tasks complete.")
                # State is saved within execute_trade_v6_3 if entry is successful

        # --- Cycle End ---
        end_time = time.time()
        cycle_duration = end_time - start_time
        sleep_duration = SCAN_INTERVAL - cycle_duration
        if sleep_duration < 0.1:
            logger.warning(f"Cycle took longer ({cycle_duration:.2f}s) than scan interval ({SCAN_INTERVAL}s). Enforcing 0.1s sleep.")
            sleep_duration = 0.1

        logger.info(f"--- V6.3 Scan Cycle Finished ({cycle_duration:.2f}s). Sleeping for {sleep_duration:.2f}s... ---")
        if shutdown_requested: break
        await asyncio.sleep(sleep_duration)

    logger.info("V6.3 Trading loop received shutdown request and is exiting.")


# === Telegram Command Handlers (V6.3) ===
def authorized_user_only(func):
    """Decorator to restrict command access."""
    # (Same as V6.2)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if AUTHORIZED_TELEGRAM_USER_IDS and user_id not in AUTHORIZED_TELEGRAM_USER_IDS:
            logger.warning(f"Unauthorized command attempt from user ID: {user_id}")
            await update.message.reply_text("Sorry, you are not authorized.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

@authorized_user_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    await update.message.reply_text("Crypto Genie V6.3 Active. Use /pnl, /pos, /status, /closeall, /stats.") # <<< UPDATED text

@authorized_user_only
async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports accumulated PnL from state history."""
    # (Same as V6.2, updated title)
    global state
    logger.info(f"Received /pnl command from user ID: {update.effective_user.id}")
    total_pnl = 0.0; trade_count = 0; win_count = 0; loss_count = 0
    pnl_history = state.get("pnl_history", [])
    if not isinstance(pnl_history, list):
        logger.error("pnl_history in state is not a list."); await update.message.reply_text("Error: PnL history data corrupt."); return

    for record in pnl_history:
        try:
            profit = float(record.get("profit", 0.0))
            total_pnl += profit; trade_count += 1
            if profit > 0: win_count += 1
            elif profit < 0: loss_count += 1
        except (ValueError, TypeError): continue

    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
    reply_text = (
        f"<b>Accumulated PnL Report (V6.3)</b>\n" # <<< UPDATED text
        f"---------------------------\n"
        f"Total Realized PnL: <code>${total_pnl:,.4f}</code>\n"
        f"Total Records: <code>{trade_count}</code>\n"
        f"Winning Records: <code>{win_count}</code>\n"
        f"Losing Records: <code>{loss_count}</code>\n"
        f"Win Rate (by record): <code>{win_rate:.2f}%</code>"
    )
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

@authorized_user_only
async def pos_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports current open positions from state (basic info)."""
    # (Same as V6.2, updated title)
    global state
    logger.info(f"Received /pos command from user ID: {update.effective_user.id}")
    open_positions_dict = state.get("open_positions", {})
    if not open_positions_dict:
        await update.message.reply_text("No open positions found.")
        return

    reply_text = "<b>Open Positions (V6.3 - Basic):</b>\n" # <<< UPDATED text
    reply_text += "---------------------------\n"
    for symbol, pos_data in open_positions_dict.items():
        try:
            side = pos_data.get('side', 'UNKNOWN')
            qty = float(pos_data.get('qty', 0))
            entry = float(pos_data.get('entry_price', 0))
            sl = float(pos_data.get('current_sl_price', 0))
            entry_time_str = pos_data.get('entry_time', 'N/A')
            entry_time_dt = isoparse(entry_time_str) if entry_time_str != 'N/A' else None
            duration = datetime.now(timezone.utc) - entry_time_dt if entry_time_dt else None
            duration_str = str(duration).split('.')[0] if duration else "N/A"
            trailing_active = pos_data.get('trailing_stop_active', False)
            tp_hit = pos_data.get('partial_tp_hit', False) # Trailing activation flag

            reply_text += (
                f"<code>{html.escape(symbol)}</code> ({side})\n"
                f"  Qty: {qty}, Entry: ${entry:.4f}\n"
                f"  SL: ${sl:.4f}, Age: {duration_str}\n"
                f"  TP Hit: {tp_hit}, Trail Active: {trailing_active}\n"
                 "---\n" # Add separator
            )
        except Exception as e:
            logger.error(f"Error formatting position {symbol} for /pos: {e}")
            reply_text += f"<code>{html.escape(symbol)}</code> - Error displaying data\n---\n"
    reply_text += "---------------------------\n"
    reply_text += "Use /status for PnL, /stats for daily summary."

    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)


# (Same /status command as V6.2, updated title)
@authorized_user_only
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports open positions with current unrealized PnL."""
    global state
    logger.info(f"Received /status command from user ID: {update.effective_user.id}")
    open_positions_dict = state.get("open_positions", {})
    if not open_positions_dict:
        await update.message.reply_text("No open positions found.")
        return

    symbols_to_fetch = list(open_positions_dict.keys())
    latest_prices = {}
    if symbols_to_fetch:
        try:
            # Fetch current mark prices for PnL calculation
            tickers_info = await asyncio.to_thread(futures_client.futures_mark_price) # Fetch all mark prices
            latest_prices = {ticker['symbol']: float(ticker['markPrice']) for ticker in tickers_info if ticker['symbol'] in symbols_to_fetch}
            logger.debug(f"Fetched mark prices for {len(latest_prices)} open positions for /status.")
        except Exception as e:
            logger.error(f"Failed to fetch mark prices for /status: {e}")
            await update.message.reply_text("Error fetching current prices. Cannot calculate PnL.")
            return

    reply_text = "<b>Open Positions Status (V6.3):</b>\n" # <<< UPDATED title
    reply_text += "---------------------------\n"
    total_unrealized_pnl = 0.0

    for symbol, pos_data in open_positions_dict.items():
        current_price = latest_prices.get(symbol)
        unrealized_pnl = 0.0
        unrealized_pnl_str = "N/A (Price Error)"

        try:
            side = pos_data.get('side', 'UNKNOWN')
            qty = float(pos_data.get('qty', 0))
            entry_price = float(pos_data.get('entry_price', 0))
            sl = float(pos_data.get('current_sl_price', 0))
            trailing_active = pos_data.get('trailing_stop_active', False)

            if current_price is not None and entry_price > 0:
                if side == "LONG":
                    unrealized_pnl = (current_price - entry_price) * qty
                elif side == "SHORT":
                    unrealized_pnl = (entry_price - current_price) * qty
                else:
                    unrealized_pnl = 0.0

                total_unrealized_pnl += unrealized_pnl
                unrealized_pnl_str = f"<code>${unrealized_pnl:+.4f}</code>"
            elif current_price is None:
                 logger.warning(f"/status: Could not get price for {symbol}")


            reply_text += (
                f"<code>{html.escape(symbol)}</code> ({side})\n"
                f"  Qty: {qty}, Entry: ${entry_price:.4f}\n"
                f"  Curr: ${current_price:.4f if current_price else 'N/A'}, SL: ${sl:.4f}\n"
                f"  Unrealized PnL: {unrealized_pnl_str}\n"
                f"  Trailing: {'✅' if trailing_active else '❌'}\n"
                "---\n"
            )
        except Exception as e:
            logger.error(f"Error formatting position {symbol} for /status: {e}")
            reply_text += f"<code>{html.escape(symbol)}</code> - Error displaying data\n---\n"

    reply_text += f"<b>Total Unrealized PnL: <code>${total_unrealized_pnl:+.4f}</code></b>\n"
    reply_text += "---------------------------"

    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

# <<< NEW /stats COMMAND >>>
@authorized_user_only
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports daily statistics."""
    global state
    logger.info(f"Received /stats command from user ID: {update.effective_user.id}")
    today_str = date.today().isoformat()
    daily_stats = state.get("daily_stats", {}).get(today_str)

    if not daily_stats:
        await update.message.reply_text("No statistics found for today yet.")
        return

    trades = daily_stats.get("trades", 0)
    wins = daily_stats.get("wins", 0)
    losses = daily_stats.get("losses", 0)
    pnl = daily_stats.get("pnl", 0.0)
    consecutive_losses = daily_stats.get("consecutive_losses", 0)
    start_balance = daily_stats.get("start_balance")
    paused_reason = state.get("trading_paused_reason")

    win_rate = (wins / trades * 100) if trades > 0 else 0
    daily_loss_percent = 0.0
    if start_balance is not None and start_balance > 0 and pnl < 0:
         daily_loss_percent = (abs(pnl) / start_balance) * 100

    reply_text = f"<b>Daily Stats ({today_str} - V6.3)</b>\n"
    reply_text += "---------------------------\n"
    reply_text += f"Trades: <code>{trades}</code>\n"
    reply_text += f"Wins: <code>{wins}</code>\n"
    reply_text += f"Losses: <code>{losses}</code>\n"
    reply_text += f"Win Rate: <code>{win_rate:.2f}%</code>\n"
    reply_text += f"Realized PnL: <code>${pnl:,.4f}</code>\n"
    if start_balance:
        reply_text += f"Start Balance: <code>${start_balance:,.2f}</code>\n"
        reply_text += f"Current Daily Loss: <code>{daily_loss_percent:.2f}%</code> (Limit: {DAILY_LOSS_LIMIT_PERCENT}%)\n"
    else:
        reply_text += "Start Balance: <code>N/A</code>\n"
        reply_text += f"Current Daily Loss: <code>N/A</code> (Limit: {DAILY_LOSS_LIMIT_PERCENT}%)\n"
    reply_text += f"Consecutive Losses: <code>{consecutive_losses}</code> (Limit: {CONSECUTIVE_LOSS_LIMIT})\n"
    reply_text += "---------------------------\n"
    if paused_reason:
        reply_text += f"Trading Paused: <b>YES</b> (Reason: {paused_reason})\n"
    else:
        reply_text += "Trading Paused: <b>NO</b>\n"
    reply_text += "---------------------------"

    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)


@authorized_user_only
async def close_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Closes all open positions based on V6.3 state."""
    # (Logic mostly same as V6.2, calls V6.3 handle_exit)
    global state
    user_id = update.effective_user.id
    application = context.application
    logger.info(f"Received /closeall command from user ID: {user_id}")
    await update.message.reply_text("Attempting to close all open positions (V6.3)...")

    open_positions_copy = list(state.get("open_positions", {}).items())
    if not open_positions_copy:
        await update.message.reply_text("No open positions found in state to close.")
        return

    close_tasks = []
    symbols_to_close = []
    for symbol, position_info in open_positions_copy:
        try:
            symbols_to_close.append(symbol)
            # <<< Call V6.3 exit handler >>>
            close_tasks.append(handle_exit(symbol, "Manual Close Command", position_info, application))
        except Exception as e:
            logger.error(f"/closeall: Error processing position data for {symbol}: {e}. Skipping.")

    if not close_tasks:
        await update.message.reply_text("Found positions, but failed to queue any for closure.")
        return

    await update.message.reply_text(f"Closing {len(close_tasks)} positions: {', '.join(symbols_to_close)}...")
    results = await asyncio.gather(*close_tasks, return_exceptions=True)

    success_count = 0; fail_count = 0
    for i, result in enumerate(results):
        symbol_closed = symbols_to_close[i]
        if isinstance(result, Exception):
            fail_count += 1; logger.error(f"/closeall: Error closing {symbol_closed}: {result}")
        elif symbol_closed in state.get("open_positions", {}):
             fail_count += 1; logger.error(f"/closeall: Failed to close {symbol_closed} (still present in state).")
        else: success_count += 1

    final_message = f"Close all command finished.\nSuccessfully closed: {success_count}\nFailed to close: {fail_count}"
    logger.info(final_message)
    await update.message.reply_text(final_message)
    # State saving happens within handle_exit or cooldown setting

# === Graceful Shutdown Handler ===
# (Same as V6.2)
async def signal_handler(sig):
    """Sets shutdown flag and cancels main task."""
    global shutdown_requested, main_trading_task
    if shutdown_requested: return
    logger.info(f"Received signal {sig.name}. Initiating shutdown...")
    shutdown_requested = True
    if main_trading_task and not main_trading_task.done():
        main_trading_task.cancel()
        logger.info("Main trading task cancellation requested.")

async def shutdown(application: Application):
    """Performs cleanup actions during shutdown."""
    global shutdown_requested, main_trading_task
    if not shutdown_requested: shutdown_requested = True; logger.info("Shutdown called directly.")
    logger.info("Shutdown process starting...")

    # Stop Telegram Polling FIRST
    if application and application.updater and application.updater.running:
        logger.info("Stopping Telegram bot polling...")
        await application.updater.stop()
        logger.info("Telegram bot polling stopped.")
    else: logger.info("Telegram bot polling not running or updater unavailable.")

    # Cancel and Wait for Trading Loop Task
    if main_trading_task and not main_trading_task.done():
        if not main_trading_task.cancelled(): main_trading_task.cancel()
        try:
            logger.info("Waiting for trading loop task to finish...")
            await main_trading_task
            logger.info("Trading loop task finished after cancellation.")
        except asyncio.CancelledError: logger.info("Trading loop task successfully cancelled.")
        except Exception as e: logger.error(f"Error during trading loop cancellation/awaiting: {e}", exc_info=True)
    else: logger.info("Trading loop task already done or not started.")

    # Final State Save
    logger.info("Attempting final state save during shutdown...")
    save_state() # Save state including cooldowns and pause status

    # Shutdown Telegram Application
    is_app_running = False
    if application:
        if hasattr(application, 'running') and application.running: is_app_running = True
        elif hasattr(application, '_running') and application._running: is_app_running = True
        elif hasattr(application, '_is_running') and application._is_running: is_app_running = True

    if is_app_running:
        logger.info("Shutting down Telegram application...")
        try: await application.shutdown() ; logger.info("Telegram application shut down.")
        except RuntimeError as e: logger.warning(f"Caught RuntimeError during application.shutdown(): {e}. Might be stopped.")
        except Exception as e: logger.error(f"Unexpected error during application.shutdown(): {e}", exc_info=True)
    else: logger.info("Telegram application instance not running or unavailable for shutdown.")

    logger.info("-------------------- BOT SHUTDOWN COMPLETE (V6.3) ----------------------")


# === Script Entry Point === #
async def main():
    """Sets up and runs the V6.3 bot and trading loop."""
    global main_trading_task, application_instance

    # Log configuration details
    logger.info(f"Starting Crypto Genie V6.3 (Version: {datetime.now().strftime('%Y%m%d-%H%M')})...") # <<< UPDATED
    logger.info(f"--- V6.3 Configuration ---")
    logger.info(f"Scan Interval: {SCAN_INTERVAL}s")
    logger.info(f"Position Sizing: Dynamic (Risk: {RISK_PER_TRADE_PERCENT}%), Lev: {LEVERAGE}x, Max Pos: {MAX_OPEN_POSITIONS}, Min Notional: ${MIN_NOTIONAL_VALUE}") # <<< UPDATED
    logger.info(f"Min 24h Vol: ${MIN_USDT_VOLUME_24H:,.0f}") # <<< UPDATED
    logger.info(f"Candidate Selection: {CANDIDATE_STRATEGY} (Top {CANDIDATE_LIST_SIZE} L/S from Top {MAX_SYMBOLS_FOR_KLINE_FETCH} by Vol, 30m Spike: {VOL_SPIKE_MULTIPLIER_30M}x)") # <<< UPDATED
    logger.info(f"Entry Signal: 5m (EMA:{EMA_FAST_PERIOD}, MACD:{MACD_FAST}/{MACD_SLOW}/{MACD_SIGNAL}, RSI:{RSI_ENTRY_MIN_LONG}-{RSI_ENTRY_MAX_LONG} / {RSI_ENTRY_MIN_SHORT}-{RSI_ENTRY_MAX_SHORT}, Vol:{VOLUME_SPIKE_MULTIPLIER_ENTRY}x, Breakout:{BREAKOUT_LOOKBACK_PERIOD}bar)") # <<< UPDATED
    logger.info(f"Exit: TP Activation {EXIT_TAKE_PROFIT_PERCENT}% | SL ATR*{ATR_MULTIPLIER_SL} (Max {EXIT_STOP_LOSS_PERCENT}%) | Trail {TRAILING_STOP_PERCENT}% | Timeout {TIMEOUT_EXIT_MINUTES}m (Flat ±{TIMEOUT_EXIT_FLAT_PERCENT}%) | Invalidation (2/3)") # <<< UPDATED
    logger.info(f"Memecoin Rules: SL {MEMECOIN_SL_PERCENT}%, TP Act {MEMECOIN_TRAILING_ACTIVATION_PERCENT}% (Symbols: {', '.join(MEMECOIN_SYMBOLS)})") # <<< UPDATED
    logger.info(f"Re-entry Cooldown: {REENTRY_COOLDOWN_MINUTES} min") # <<< UPDATED
    logger.info(f"Risk Guards: Daily Loss {DAILY_LOSS_LIMIT_PERCENT}%, Consecutive Losses {CONSECUTIVE_LOSS_LIMIT}") # <<< NEW
    logger.info(f"State File: {STATE_FILE}") # <<< UPDATED
    logger.info(f"Log File: crypto_genie_v6.3.log") # <<< UPDATED
    logger.info(f"Authorized User IDs: {AUTHORIZED_TELEGRAM_USER_IDS}")
    logger.info(f"-------------------------")

    # Initial setup
    if ta is None: logger.critical("pandas_ta required. pip install pandas_ta. Exiting."); exit()
    load_exchange_info()
    load_state() # Loads state including cooldowns and risk status

    # Test Binance connection (same as V6.2)
    try:
        logger.info("Pinging Binance Futures API...")
        futures_client.futures_ping()
        logger.info("Binance Futures API connection successful.")
    except Exception as e: logger.critical(f"Binance API Connection failed: {e}. Exiting."); exit()

    # Initial Account Data Check (same manual checks as V6.2)
    logger.info("Performing initial account data check...")
    try:
        # Manual positionRisk Check
        logger.info("Performing manual signed API check to /fapi/v2/positionRisk...")
        timestamp = int(time.time() * 1000); params = { "timestamp": timestamp }
        query_string = urlencode(params)
        signature = hmac.new( BINANCE_SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256 ).hexdigest()
        params["signature"] = signature; headers = { "X-MBX-APIKEY": BINANCE_API_KEY }
        position_risk_url = "https://fapi.binance.com/fapi/v2/positionRisk"
        logger.debug(f"Manually requesting: GET {position_risk_url} Headers: {headers} Params: {params}")
        response = await asyncio.to_thread(requests.get, position_risk_url, headers=headers, params=params, timeout=15)
        logger.info(f"Manual positionRisk Check - HTTP Status: {response.status_code}")
        response_text_preview = response.text[:500] + ('...' if len(response.text) > 500 else '')
        logger.info(f"Manual positionRisk Check - Binance Raw Response Preview: {response_text_preview}")
        if response.status_code != 200:
            logger.critical(f"Manual request to positionRisk failed ({response.status_code}). Check API Key/IP/Time. Exiting.")
            exit()
        else:
            try: initial_positions_data = response.json(); logger.info(f"Manual positionRisk API check passed ✅.")
            except json.JSONDecodeError: logger.critical("Manual positionRisk OK but invalid JSON response. Exiting."); exit()

        # Balance Check using manual function
        initial_balance = await get_account_balance(force_refresh=True) # Force refresh at startup
        if initial_balance is None:
            logger.critical("CRITICAL: positionRisk check passed, but failed manual balance fetch. Check API/IP. Exiting.")
            exit()
        elif initial_balance <= 0:
            logger.critical(f"CRITICAL: Initial available balance is {initial_balance:.2f} USDT. Cannot start. Exiting.")
            exit()
        else:
            logger.info(f"Initial balance check successful (manual): ${initial_balance:.2f} USDT available.")

    except BinanceAPIException as api_err:
         logger.critical(f"CRITICAL: Initial account check API Error: {api_err}. Check Key Permissions/debug logs. Exiting.")
         exit()
    except Exception as e:
         logger.critical(f"CRITICAL: Unexpected error during initial account data check: {e}. Exiting.", exc_info=True)
         exit()


    # Setup Telegram Bot
    if not Crypto_TELEGRAM_BOT_TOKEN: logger.critical("Crypto_TELEGRAM_BOT_TOKEN not found. Exiting."); exit()
    if not AUTHORIZED_TELEGRAM_USER_IDS: logger.warning("No AUTHORIZED_TELEGRAM_USER_IDS set.")

    application_instance = ApplicationBuilder().token(Crypto_TELEGRAM_BOT_TOKEN).build()
    application_instance.add_handler(CommandHandler("start", start_command))
    application_instance.add_handler(CommandHandler("pnl", pnl_command))
    application_instance.add_handler(CommandHandler("pos", pos_command)) # Basic position info
    application_instance.add_handler(CommandHandler("status", status_command)) # Position PnL
    application_instance.add_handler(CommandHandler("stats", stats_command)) # <<< ADDED stats command
    application_instance.add_handler(CommandHandler("closeall", close_all_command))

    # Start the main trading loop and Telegram bot
    try:
        logger.info("Starting Telegram bot polling and V6.3 trading loop...")
        main_trading_task = asyncio.create_task(high_frequency_loop(application_instance))
        await application_instance.initialize()
        await application_instance.start()
        await application_instance.updater.start_polling(poll_interval=1.0)
        logger.info("Telegram bot started polling.")

        try: await main_trading_task # Wait for the trading loop
        except asyncio.CancelledError: logger.info("Main trading task was cancelled (caught in main).")
        except Exception as loop_err: logger.critical(f"Main trading loop exited unexpectedly: {loop_err}", exc_info=True)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Received stop signal (KeyboardInterrupt/SystemExit).")
    except Exception as e:
        logger.critical(f"Critical error in main execution block: {e}", exc_info=True)
    finally:
        logger.info("Initiating shutdown process from main() (V6.3)...")
        global shutdown_requested
        shutdown_requested = True
        if application_instance: await shutdown(application_instance)
        else: logger.error("Application instance NA for shutdown in main finally.")

if __name__ == "__main__":
    # Setup signal handlers (Same as V6.2)
    loop = asyncio.get_event_loop_policy().get_event_loop()
    signals_to_handle = {signal.SIGINT, signal.SIGTERM}
    if platform.system() != "Windows": signals_to_handle.add(signal.SIGHUP)
    for s in signals_to_handle:
        try: loop.add_signal_handler(s, lambda s=s: asyncio.create_task(signal_handler(s)))
        except NotImplementedError: logger.warning(f"Signal handler for {s.name} not implemented.")
        except ValueError: logger.warning(f"Invalid signal {s.name} for platform?")
    # Run main async function
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Main execution stopped by KeyboardInterrupt.")
    except Exception as e: logger.critical(f"Unhandled exception at top level: {e}", exc_info=True)
    finally: logger.info("Main execution finished or interrupted.")