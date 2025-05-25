# === Alpaca Momentum Scalper Bot v1.1 (Refined Strategy) ===
# Description: Performs fast scalping on US stocks based on momentum indicators,
#              using refined entry filters, risk-based sizing, dynamic exits
#              (Simulated AI), and Telegram for alerts.
# Version: 1.1.3
# Changelog v1.1.3:
#   - Fixed SyntaxError in handle_pnl function indentation.
# Changelog v1.1.2:
#   - Fixed ImportError for Account model (removed from type hint).
# Changelog v1.1.1:
#   - Fixed ImportError for GetAccountRequest (not needed in alpaca-py v2).
# Changelog v1.1:
#   - Implemented Strategy Refinements requested.
#   - Added Entry Filters: Signal Blacklist, Liquidity (Quote Size, Spread), Volatility (ATR).
#   - Added Market Bias filter based on SPY VWAP slope.
#   - Switched Position Sizing to RISK_PERCENT based on account equity.
#   - Implemented Dynamic Exits: Asymmetric R:R, Trailing Stop, Scale-Out, Hard Timeout.
#   - Added data fetching for Quotes and SPY bars.
#   - Refactored state management for new trade parameters.
#   - Added ATR/VWAP Slope calculation using numpy.
#   - Updated Telegram messages and status display.
#   - Requires numpy: pip install numpy

import asyncio
import os
import json
import logging
import sys # Import sys to check platform
import math
import random
import time
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import isoparse
from collections import deque
from dotenv import load_dotenv
from typing import List, Dict, Optional, Tuple, Any, Union
from uuid import UUID, uuid4
import signal # For graceful shutdown
import numpy as np # Required for ATR and VWAP Slope calculations

# --- Third-Party Imports ---
# Install required packages:
# pip install python-dotenv alpaca-py python-telegram-bot asyncio numpy

# --- Alpaca Imports (Using alpaca-py V2) ---
from alpaca.trading.client import TradingClient as REST
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import (
    MarketOrderRequest, GetAssetsRequest, ClosePositionRequest, GetOrdersRequest
)
from alpaca.trading.enums import (
    OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderType,
    OrderStatus, QueryOrderStatus
)
# Removed Account from import as it causes ImportError in some versions
from alpaca.trading.models import Asset, Position, Order # For type hinting
from alpaca.common.exceptions import APIError
from alpaca.data.requests import (
    StockBarsRequest, StockLatestTradeRequest, StockLatestQuoteRequest
)
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.models import Bar, BarSet, Trade, Quote # For type hinting

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

# --- Alpaca API Keys ---
ALPACA_API_KEY = os.getenv("ALPACA_SCALPINGSNIPER_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SCALPINGSNIPER_SECRET_KEY")
PAPER_TRADING = os.getenv("ALPACA_PAPER", "true").lower() == "true"

# --- Telegram Bot Token ---
TELEGRAM_BOT_TOKEN = os.getenv("Slcaper_Stock_TELEGRAM_BOT_TOKEN")
ADMIN_TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") # Optional admin ID for critical errors

# --- Basic Sanity Checks ---
if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("ERROR: Alpaca API Key/Secret not found in environment variables (ALPACA_SCALPINGSNIPER_KEY, ALPACA_SCALPINGSNIPER_SECRET_KEY).", file=sys.stderr)
    sys.exit(1)
if not TELEGRAM_BOT_TOKEN:
    print("ERROR: Telegram Bot Token not found in environment variables (Slcaper_Stock_TELEGRAM_BOT_TOKEN).", file=sys.stderr)
    sys.exit(1)

# --- State and Log Files ---
STATE_FILE = "momentum_scalper_state_v1.json" # Updated state file name
LOG_FILE = "momentum_scalper_v1.log" # Updated log file name

# --- Strategy Parameters ---
SCAN_INTERVAL_SECONDS = 45
MIN_PRICE_FILTER = 0.50
MAX_PRICE_FILTER = 50.00
MAX_OPEN_POSITIONS = 15 # Base limit, adjusted by market bias
EXTENDED_HOURS_TRADING = False

# --- Position Sizing (v1.1 Update) ---
SIZING_MODE = 'RISK_PERCENT' # CHANGED TO RISK_PERCENT
# Risk percentage of *account equity* per trade
CAPITAL_PER_TRADE_RISK_PERCENT = float(os.getenv("SCALPER_RISK_PERCENT", "0.5")) # Default 0.5%

# --- Entry Filters (v1.1 Update) ---
# Signal Blacklist: Reasons from simulated AI to avoid
SIGNAL_REASON_BLACKLIST = ["Vol Spike/Below VWAP", "VWAP Reject"]
# Liquidity Filters
MIN_QUOTE_SIZE = int(os.getenv("SCALPER_MIN_QUOTE_SIZE", "3000")) # Minimum shares on bid/ask
MAX_SPREAD_PERCENT = float(os.getenv("SCALPER_MAX_SPREAD_PERCENT", "0.15")) # Max NBBO spread %
# Volatility Filter
MIN_ATR_PERCENT = float(os.getenv("SCALPER_MIN_ATR_PERCENT", "0.8")) # Min 5-min ATR as % of price
ATR_PERIOD = 14 # Period for ATR calculation
ATR_TIMEFRAME = TimeFrame(5, TimeFrameUnit.Minute) # Use 5-minute bars for ATR
# Market Bias Filter (SPY VWAP Slope)
SPY_SLOPE_TIMEFRAME = TimeFrame(5, TimeFrameUnit.Minute) # Use 5-minute bars for SPY VWAP
SPY_SLOPE_LOOKBACK = 6 # Number of 5-min bars to calculate slope over (30 mins)
SPY_POSITIVE_SLOPE_THRESHOLD = float(os.getenv("SCALPER_SPY_POSITIVE_SLOPE", "0.15")) # Threshold for bullish bias
SPY_NEGATIVE_SLOPE_THRESHOLD = float(os.getenv("SCALPER_SPY_NEGATIVE_SLOPE", "-0.15")) # Threshold for bearish bias (negative value)
MARKET_BIAS_POSITION_LIMIT_PERCENT = float(os.getenv("SCALPER_MARKET_BIAS_LIMIT", "30")) # Max % of positions against bias

# --- Exit Parameters (v1.1 Update) ---
INITIAL_STOP_LOSS_PERCENT = float(os.getenv("SCALPER_INITIAL_SL_PERCENT", "0.30"))
INITIAL_TAKE_PROFIT_PERCENT = float(os.getenv("SCALPER_INITIAL_TP_PERCENT", "0.90")) # 3R target
TRAIL_START_GAIN_PERCENT = float(os.getenv("SCALPER_TRAIL_START_PERCENT", "0.45")) # 1.5R to start trail
TRAIL_OFFSET_PERCENT = float(os.getenv("SCALPER_TRAIL_OFFSET_PERCENT", "0.25")) # Trail distance
SCALE_OUT_GAIN_PERCENT = INITIAL_TAKE_PROFIT_PERCENT # Scale out at initial target
SCALE_OUT_FRACTION = 2/3 # Sell/Cover 2/3rds at scale out target
HARD_TIMEOUT_MINUTES = int(os.getenv("SCALPER_HARD_TIMEOUT_MIN", "15")) # Max time in trade

# --- Indicator Parameters ---
RSI_PERIOD = 14
SHORT_MA_PERIOD = 9
LONG_MA_PERIOD = 21
VOLUME_SPIKE_LOOKBACK = 10 # Bars to average volume over
VOLUME_SPIKE_MULTIPLIER = 1.5 # e.g., 1.5x average volume
BASE_TIMEFRAME = TimeFrame.Minute # Timeframe for primary analysis (RSI, MA, Volume)

# --- AI Simulation ---
SIMULATE_AI = True # Set to False to integrate a real AI model
AI_SIMULATION_SIGNAL_PROBABILITY = 0.02 # Chance AI finds a signal per stock (e.g., 2%)
AI_SIMULATION_EXECUTE_PROBABILITY = 0.70 # Chance AI decides to execute a found signal

# --- Data Fetching ---
BAR_HISTORY_MINUTES = 90 # Fetch enough bars for indicators (increased slightly for ATR lookback)
ASSET_FETCH_INTERVAL_MINUTES = 60 # How often to refresh the full asset list
MAX_SYMBOLS_PER_BAR_REQUEST = 100 # Alpaca limit adjustment might be needed
MAX_SYMBOLS_PER_TRADE_REQUEST = 500 # Alpaca limit
MAX_SYMBOLS_PER_QUOTE_REQUEST = 500 # Alpaca limit

# === Setup Logging === #
LOGGING_LEVEL = logging.INFO
# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)
log_file_path = os.path.join("logs", LOG_FILE)

logging.basicConfig(
    level=LOGGING_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Also print logs to console
    ],
    encoding='utf-8'
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("alpaca").setLevel(logging.WARNING)

logging.info(f"--- Momentum Scalper Bot v1.1.3 ---") # Updated version
logging.info(f"PAPER_TRADING: {PAPER_TRADING}")
logging.info(f"Scan Interval: {SCAN_INTERVAL_SECONDS}s, Base Max Positions: {MAX_OPEN_POSITIONS}")
logging.info(f"Price Filter: ${MIN_PRICE_FILTER} - ${MAX_PRICE_FILTER}")
logging.info(f"Sizing: {SIZING_MODE} ({CAPITAL_PER_TRADE_RISK_PERCENT}% Equity)")
logging.info(f"Entry Filters: Blacklist={SIGNAL_REASON_BLACKLIST}, QuoteSize>={MIN_QUOTE_SIZE}, Spread%<={MAX_SPREAD_PERCENT}, ATR%>={MIN_ATR_PERCENT}, MarketBiasActive={True}")
logging.info(f"Exit Params: InitSL={INITIAL_STOP_LOSS_PERCENT}%, InitTP={INITIAL_TAKE_PROFIT_PERCENT}%, TrailStart={TRAIL_START_GAIN_PERCENT}%, TrailOffset={TRAIL_OFFSET_PERCENT}%, ScaleOut={SCALE_OUT_FRACTION*100:.0f}%@{SCALE_OUT_GAIN_PERCENT}%, Timeout={HARD_TIMEOUT_MINUTES}min")
logging.info(f"Simulate AI: {SIMULATE_AI}")
logging.warning("Ensure Alpaca account has market data subscriptions (SIP recommended for broad coverage).")
logging.warning("Ensure numpy is installed (`pip install numpy`).")


# === Global State & Locks === #
state = {
    "trades": {},
    "pnl": [],
    "users": {},
    "assets_cache": {
        "data": {},
        "last_fetch_time": None
    }
}
state_lock = asyncio.Lock()
telegram_app = None
active_telegram_users = set()
shutdown_event = asyncio.Event()
current_account_equity = 0.0

# === Initialize Alpaca Clients === #
try:
    rest_client = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=PAPER_TRADING)
    data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    account_info = rest_client.get_account() # Returns an Account object implicitly
    current_account_equity = float(account_info.equity)
    logging.info(f"Alpaca clients initialized. Account: {account_info.account_number}, Status: {account_info.status}, Equity: ${current_account_equity:,.2f}")
except APIError as e:
    logging.critical(f"Alpaca API Error during client initialization: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logging.critical(f"Unexpected error during Alpaca client initialization: {e}", exc_info=True)
    sys.exit(1)

# === Persistent State Load/Save === #
def save_state_sync(state_to_save):
    """Synchronously saves the bot's state to a JSON file."""
    try:
        state_copy = json.loads(json.dumps(state_to_save, default=str))
        with open(STATE_FILE, "w", encoding='utf-8') as f:
            json.dump(state_copy, f, indent=4)
        logging.debug(f"State successfully saved to {STATE_FILE}")
    except Exception as e:
        logging.error(f"Error saving state to {STATE_FILE}: {e}", exc_info=True)

def load_state_sync():
    """Synchronously loads the bot's state from a JSON file."""
    global state, active_telegram_users
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding='utf-8') as f:
                loaded_state = json.load(f)
                state.setdefault("trades", {}).update(loaded_state.get("trades", {}))
                state.setdefault("pnl", []).extend(loaded_state.get("pnl", []))
                state.setdefault("users", {}).update(loaded_state.get("users", {}))
                state.setdefault("assets_cache", {"data": {}, "last_fetch_time": None}).update(loaded_state.get("assets_cache", {}))

                for trade_id, trade in state["trades"].items():
                    for key in ['qty', 'entry_price', 'initial_stop_price', 'initial_target_price',
                                'trail_start_price', 'trail_offset_percent', 'current_stop_price',
                                'exit_price', 'profit', 'pct_change', 'remaining_qty']:
                        if key in trade and trade[key] is not None:
                            try: trade[key] = float(trade[key])
                            except (ValueError, TypeError):
                                logging.warning(f"Could not convert trade[{key}]='{trade[key]}' to float for trade {trade_id}. Setting to None.")
                                trade[key] = None
                    for key in ['trail_active', 'scaled_out']:
                         if key in trade and trade[key] is not None:
                            try:
                                if isinstance(trade[key], str): trade[key] = trade[key].lower() == 'true'
                                else: trade[key] = bool(trade[key])
                            except Exception:
                                 logging.warning(f"Could not convert trade[{key}]='{trade[key]}' to bool for trade {trade_id}. Setting to False.")
                                 trade[key] = False

                active_telegram_users = {u['chat_id'] for uid, u in state['users'].items() if u.get('enabled')}
                logging.info(f"Successfully loaded state from {STATE_FILE}. Trades: {len(state['trades'])}, PnL Records: {len(state['pnl'])}, Users: {len(state['users'])}")
        except json.JSONDecodeError:
            logging.error(f"Error decoding state JSON from {STATE_FILE}. Starting with default state.", exc_info=True)
            initialize_default_state()
        except Exception as e:
            logging.error(f"Error loading state file {STATE_FILE}: {e}. Starting with default state.", exc_info=True)
            initialize_default_state()
    else:
        logging.warning(f"State file {STATE_FILE} not found. Starting with default state.")
        initialize_default_state()

def initialize_default_state():
    """Sets the global state to its default empty structure."""
    global state
    state = { "trades": {}, "pnl": [], "users": {}, "assets_cache": {"data": {}, "last_fetch_time": None} }
    logging.info("Initialized with default empty state.")


# === Telegram Bot Functions === #
async def send_telegram_message(bot, text: str, user_chat_id: Optional[int] = None):
    """Sends a message to a specific user or all active users."""
    target_chat_ids = set()
    if user_chat_id: target_chat_ids.add(user_chat_id)
    else: target_chat_ids.update(active_telegram_users)
    if not target_chat_ids:
        logging.warning(f"No active Telegram users to send message: {text[:50]}...")
        return
    for chat_id in target_chat_ids:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
            logging.debug(f"Sent Telegram message to {chat_id}: {text[:50]}...")
            await asyncio.sleep(0.1)
        except (telegram.error.BadRequest, telegram.error.Forbidden) as e:
            logging.error(f"Telegram error sending to {chat_id} ({type(e).__name__}). Removing user.")
            async with state_lock:
                user_id_to_remove = next((uid for uid, u_data in state['users'].items() if u_data.get('chat_id') == chat_id), None)
                if user_id_to_remove:
                    state['users'][user_id_to_remove]['enabled'] = False
                    if chat_id in active_telegram_users: active_telegram_users.remove(chat_id)
        except Exception as e:
            logging.error(f"Unexpected Telegram error sending to {chat_id}: {e}", exc_info=True)

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    user, user_id, chat_id = update.effective_user, str(update.effective_user.id), update.effective_chat.id
    logging.info(f"/start command from User ID: {user_id}, Chat ID: {chat_id}")
    async with state_lock:
        state['users'][user_id] = {'enabled': True, 'chat_id': chat_id}
        active_telegram_users.add(chat_id)
        await asyncio.to_thread(save_state_sync, state)
    await update.message.reply_text("✅ Scalper Bot v1.1 **ENABLED** for your user.\nYou will receive trade alerts.")
    logging.info(f"User {user_id} enabled the bot.")

async def handle_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /stop command."""
    user, user_id, chat_id = update.effective_user, str(update.effective_user.id), update.effective_chat.id
    logging.info(f"/stop command from User ID: {user_id}, Chat ID: {chat_id}")
    async with state_lock:
        if user_id in state['users']:
            state['users'][user_id]['enabled'] = False
            if chat_id in active_telegram_users: active_telegram_users.remove(chat_id)
            await asyncio.to_thread(save_state_sync, state)
    await update.message.reply_text("⏹️ Scalper Bot v1.1 **DISABLED** for your user.\nYou will no longer receive alerts.")
    logging.info(f"User {user_id} disabled the bot.")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /status command."""
    user_id = str(update.effective_user.id)
    logging.info(f"/status command from User ID: {user_id}")
    async with state_lock:
        is_enabled = state.get("users", {}).get(user_id, {}).get("enabled", False)
        open_trades = [t for t in state.get("trades", {}).values() if t.get("status") == "open"]
    status_text = "✅ ENABLED" if is_enabled else "⏹️ DISABLED"
    reply = f"📊 **Scalper Bot v1.1 Status for User `{user_id}`: {status_text}**\n"
    reply += f"⏱️ Scan Interval: {SCAN_INTERVAL_SECONDS}s\n💰 Sizing: {SIZING_MODE} ({CAPITAL_PER_TRADE_RISK_PERCENT}% Equity)\n"
    reply += f"📈 Base Max Positions: {MAX_OPEN_POSITIONS}\n⏱️ Hard Timeout: {HARD_TIMEOUT_MINUTES} min\n🤖 AI Simulation: {SIMULATE_AI}\n\n"
    if not open_trades:
        reply += "ℹ️ No open positions currently."
        await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
        return
    reply += f"💼 **Open Positions ({len(open_trades)}):**\n"
    total_unrealized_pnl = 0.0
    symbols_to_fetch = list({t['symbol'] for t in open_trades})
    current_prices, fetch_error = {}, False
    if symbols_to_fetch:
        try: current_prices = await get_latest_trades_batched(symbols_to_fetch)
        except Exception as e: logging.error(f"Status: Error fetching prices: {e}"); fetch_error = True
    for trade in open_trades:
        symbol, entry_price = trade.get("symbol", "N/A"), float(trade.get("entry_price", 0))
        qty, initial_qty = float(trade.get("remaining_qty", trade.get("qty", 0))), float(trade.get("qty", 0))
        bias, entry_time_str, trade_id = trade.get("bias", "LONG"), trade.get("entry_time"), trade.get("trade_id", "N/A")[-6:]
        current_price = current_prices.get(symbol)
        pnl_str, pct_str, dur_str = "N/A", "", "N/A"
        if current_price is not None and entry_price > 0 and qty > 0:
            try:
                pnl = (current_price - entry_price) * qty if bias == "LONG" else (entry_price - current_price) * qty
                pct = ((current_price - entry_price) / entry_price) * 100 if entry_price != 0 else 0
                realized_profit = float(trade.get("profit", 0.0)) if trade.get("scaled_out") else 0.0
                total_unrealized_pnl += pnl
                current_total_pnl = pnl + realized_profit
                pnl_str, pct_str = f"${current_total_pnl:+.2f}", f"({pct:+.1f}%)"
            except Exception as e: pnl_str = f"(Calc Err: {e})"
        elif fetch_error: pnl_str = "(Price Err)"
        if entry_time_str:
            try:
                duration = datetime.now(timezone.utc) - isoparse(entry_time_str)
                minutes, seconds = divmod(int(duration.total_seconds()), 60)
                dur_str = f"{minutes}m{seconds}s"
            except Exception: dur_str = "(Time Err)"
        current_sl, init_tp = trade.get('current_stop_price'), trade.get('initial_target_price')
        trail_active, scaled_out = trade.get('trail_active', False), trade.get('scaled_out', False)
        sl_str, tp_str = f"SL: {current_sl:.2f}" if current_sl else "SL: N/A", f"TP: {init_tp:.2f}" if init_tp else "TP: N/A"
        trail_str, scale_str = " (T)" if trail_active else "", " (S)" if scaled_out else ""
        qty_str = f"{qty:.4f}/{initial_qty:.4f}" if scaled_out else f"{initial_qty:.4f}"
        reply += f" - `{symbol}` ({bias} {qty_str} units) `ID:{trade_id}`{scale_str}\n"
        reply += f"   Entry: ${entry_price:.2f} | PnL: {pnl_str} {pct_str}\n"
        reply += f"   {sl_str}{trail_str} | {tp_str} | Held: {dur_str}\n"
    if fetch_error: reply += "\n_(Error fetching current prices)_"
    else: reply += f"\n💰 **Total Unrealized (Open Parts): ${total_unrealized_pnl:+.2f}**"
    await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)

async def handle_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /pnl command."""
    user_id = str(update.effective_user.id)
    logging.info(f"/pnl command from User ID: {user_id}")
    async with state_lock: pnl_records = state.get("pnl", [])
    if not pnl_records: await update.message.reply_text("📉 No closed trades recorded yet."); return
    total_profit = sum(p.get("profit", 0) for p in pnl_records)
    total_trades = len(pnl_records)
    wins = sum(1 for p in pnl_records if p.get("profit", 0) > 0)
    losses = total_trades - wins
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    reply = f"📈 **Realized PnL Summary ({total_trades} Trades)**\n\n"
    reply += f"💰 **Total Profit:** ${total_profit:+.2f}\n📊 **Win Rate:** {win_rate:.1f}% ({wins} W / {losses} L)\n\n"
    reply += f"📜 **Last 10 Closed Trades:**\n"
    for pnl_entry in reversed(pnl_records[-10:]):
        symbol, profit = pnl_entry.get("symbol", "N/A"), pnl_entry.get("profit", 0)
        pct, exit_reason = pnl_entry.get("pct_change", 0), pnl_entry.get("exit_reason", "N/A")
        exit_time_str, bias = pnl_entry.get("exit_time"), pnl_entry.get("bias", "")
        scaled_info = " (S)" if pnl_entry.get('scaled_out') else ""
        ts = "N/A"
        # Correct indentation for try-except block
        if exit_time_str:
            try:
                ts = isoparse(exit_time_str).strftime('%m-%d %H:%M')
            except Exception:
                pass # Keep ts as "N/A" if parsing fails

        emoji = "✅" if profit >= 0 else "❌"
        reply += f"{emoji} `{symbol}` ({bias}){scaled_info}: ${profit:+.2f} ({pct:+.1f}%) at {ts}\n"
        reply += f"   Reason: _{exit_reason}_\n"
    await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles non-command messages."""
    logging.info(f"Received message from {update.effective_user.id}: {update.message.text}")
    await update.message.reply_text( "ℹ️ I am Scalper Bot v1.1. Use commands:\n"
                                     "`/start` | `/stop` | `/status` | `/pnl`", parse_mode=ParseMode.MARKDOWN)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors and notifies admin."""
    logging.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    if ADMIN_TELEGRAM_CHAT_ID:
        try: await context.bot.send_message(chat_id=ADMIN_TELEGRAM_CHAT_ID, text=f"🚨 Bot Error: {type(context.error).__name__}: {context.error}"[:4000])
        except Exception as e: logging.error(f"Failed to send error notification to admin ({ADMIN_TELEGRAM_CHAT_ID}): {e}")


# === Alpaca Data Fetching === #
async def get_active_assets() -> Dict[str, Dict]:
    """Fetches and caches active, tradable, shortable US equities within the price range."""
    global state
    now = datetime.now(timezone.utc)
    cache_data = state['assets_cache']['data']
    last_fetch = state['assets_cache']['last_fetch_time']
    if last_fetch:
        try:
            if now - isoparse(last_fetch) < timedelta(minutes=ASSET_FETCH_INTERVAL_MINUTES) and cache_data:
                logging.debug(f"Using cached asset list ({len(cache_data)} symbols).")
                return cache_data
        except Exception: logging.warning("Could not parse last asset fetch time. Refetching.")
    logging.info(f"Fetching active US equity assets (Price: ${MIN_PRICE_FILTER}-${MAX_PRICE_FILTER})...")
    assets_dict = {}
    try:
        search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
        all_assets = await asyncio.to_thread(rest_client.get_all_assets, search_params)
        candidate_assets = [a for a in all_assets if a.tradable and a.shortable and '.' not in a.symbol and '/' not in a.symbol]
        symbols_to_price_check = [a.symbol for a in candidate_assets]
        latest_prices = await get_latest_trades_batched(symbols_to_price_check)
        for asset in candidate_assets:
            symbol, price = asset.symbol, latest_prices.get(asset.symbol)
            if price and MIN_PRICE_FILTER <= price <= MAX_PRICE_FILTER:
                assets_dict[symbol] = {k: getattr(asset, k, None) for k in ['tradable', 'shortable', 'easy_to_borrow', 'exchange', 'fractionable']}
                assets_dict[symbol]['exchange'] = str(assets_dict[symbol]['exchange']) # Ensure string
        logging.info(f"Found {len(assets_dict)} assets within price range ${MIN_PRICE_FILTER}-${MAX_PRICE_FILTER}.")
        async with state_lock:
            state['assets_cache']['data'] = assets_dict
            state['assets_cache']['last_fetch_time'] = now.isoformat()
    except Exception as e:
        logging.error(f"Error fetching assets: {e}. Returning previous cache.", exc_info=True)
        return cache_data
    return assets_dict

async def get_latest_trades_batched(symbols: List[str], batch_size: int = MAX_SYMBOLS_PER_TRADE_REQUEST) -> Dict[str, float]:
    """Fetches latest trade prices."""
    latest_prices = {}
    if not symbols: return latest_prices
    logging.debug(f"Fetching latest trades for {len(symbols)} symbols...")
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        if not batch: continue
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=batch, feed='sip')
            latest_trades_batch = await asyncio.to_thread(data_client.get_stock_latest_trade, req)
            for symbol, trade in latest_trades_batch.items():
                if trade and trade.price > 0: latest_prices[symbol] = trade.price
            await asyncio.sleep(0.05)
        except APIError as e:
            if e.status_code == 429: logging.warning(f"Rate limit hit fetching trades batch {i}. Waiting..."); await asyncio.sleep(5)
            else: logging.error(f"API error fetching trades batch {i}: {e}")
        except Exception as e: logging.error(f"Unexpected error fetching trades batch {i}: {e}", exc_info=True)
    logging.debug(f"Finished latest trades fetch. Got prices for {len(latest_prices)} symbols.")
    return latest_prices

async def get_latest_quotes_batched(symbols: List[str], batch_size: int = MAX_SYMBOLS_PER_QUOTE_REQUEST) -> Dict[str, Quote]:
    """Fetches latest quotes."""
    latest_quotes = {}
    if not symbols: return latest_quotes
    logging.debug(f"Fetching latest quotes for {len(symbols)} symbols...")
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        if not batch: continue
        try:
            req = StockLatestQuoteRequest(symbol_or_symbols=batch, feed='sip')
            latest_quotes_batch = await asyncio.to_thread(data_client.get_stock_latest_quote, req)
            latest_quotes.update(latest_quotes_batch)
            await asyncio.sleep(0.05)
        except APIError as e:
            if e.status_code == 429: logging.warning(f"Rate limit hit fetching quotes batch {i}. Waiting..."); await asyncio.sleep(5)
            else: logging.error(f"API error fetching quotes batch {i}: {e}")
        except Exception as e: logging.error(f"Unexpected error fetching quotes batch {i}: {e}", exc_info=True)
    logging.debug(f"Finished latest quotes fetch. Got quotes for {len(latest_quotes)} symbols.")
    return latest_quotes

async def get_bars_batched(symbols: List[str], timeframe: TimeFrame, start: datetime, end: datetime, batch_size: int = MAX_SYMBOLS_PER_BAR_REQUEST) -> Dict[str, List[Bar]]:
    """Fetches historical bars."""
    all_bars_data: Dict[str, List[Bar]] = {symbol: [] for symbol in symbols}
    if not symbols: return all_bars_data
    start_iso, end_iso = start.isoformat(), end.isoformat()
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        if not batch: continue
        try:
            params = StockBarsRequest(symbol_or_symbols=batch, timeframe=timeframe, start=start_iso, end=end_iso, adjustment='raw', feed='sip')
            barset = await asyncio.to_thread(data_client.get_stock_bars, params)
            for symbol, bars in barset.data.items():
                if symbol in all_bars_data: all_bars_data[symbol].extend(bars)
            await asyncio.sleep(0.05)
        except APIError as e:
            if e.status_code == 429: logging.warning(f"Rate limit hit fetching bars batch {i}. Waiting..."); await asyncio.sleep(5)
            elif e.status_code == 422 and "not queryable" in str(e): logging.warning(f"Bars batch {i} contained unqueryable symbols.")
            else: logging.error(f"API error fetching bars batch {i}: {e}")
        except Exception as e: logging.error(f"Unexpected error fetching bars batch {i}: {e}", exc_info=True)
    for symbol in all_bars_data: all_bars_data[symbol].sort(key=lambda b: b.timestamp)
    return all_bars_data


# === Technical Indicator Calculations === #
def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """Calculates RSI."""
    if len(prices) < period + 1: return None
    deltas = np.diff(prices)
    seed = deltas[:period]
    if len(seed) == 0: return 50.0
    gains, losses = np.where(seed > 0, seed, 0), np.where(seed < 0, -seed, 0)
    avg_gain, avg_loss = np.mean(gains), np.mean(losses)
    if avg_loss == 0: return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_sma(prices: List[float], period: int) -> Optional[float]:
    """Calculates SMA."""
    if len(prices) < period or period <= 0: return None
    return np.mean(prices[-period:])

def calculate_average_volume(volumes: List[Union[int, float]], period: int) -> Optional[float]:
    """Calculates Average Volume."""
    if len(volumes) < period or period <= 0: return None
    valid_volumes = [v for v in volumes[-period:] if isinstance(v, (int, float))]
    if len(valid_volumes) < period: return None
    return np.mean(valid_volumes)

def calculate_atr(bars: List[Bar], period: int = 14) -> Optional[float]:
    """Calculates ATR."""
    if len(bars) < period + 1 or period <= 0: return None
    highs = np.array([b.high for b in bars if b.high is not None])
    lows = np.array([b.low for b in bars if b.low is not None])
    closes = np.array([b.close for b in bars if b.close is not None])
    min_len = min(len(highs), len(lows), len(closes))
    if min_len < period + 1: return None
    highs, lows, closes = highs[-min_len:], lows[-min_len:], closes[-min_len:]
    high_low = highs - lows
    high_close_prev = np.abs(highs[1:] - closes[:-1])
    low_close_prev = np.abs(lows[1:] - closes[:-1])
    tr = np.maximum(high_low[1:], high_close_prev)
    tr = np.maximum(tr, low_close_prev)
    if len(tr) < period: return None
    return np.mean(tr[-period:])

def calculate_vwap_slope(bars: List[Bar], lookback: int) -> Optional[float]:
    """Calculates VWAP slope."""
    if len(bars) < lookback or lookback < 2: return None
    vwaps = [b.vwap for b in bars[-lookback:] if b.vwap is not None]
    if len(vwaps) < 2: return None
    x, y = np.arange(len(vwaps)), np.array(vwaps)
    valid = np.isfinite(y)
    if np.sum(valid) < 2: return None
    x, y = x[valid], y[valid]
    try:
        slope = np.polyfit(x, y, 1)[0]
        return slope if np.isfinite(slope) else None
    except (np.linalg.LinAlgError, ValueError) as e:
        logging.warning(f"VWAP slope calc error: {e}")
        return None


# === Simulated AI Analysis === #
def analyze_setup_simulated(symbol: str, latest_price: float, bars: List[Bar], asset_info: Dict) -> Optional[Dict]:
    """Simulated AI analysis."""
    if not bars or len(bars) < max(LONG_MA_PERIOD, RSI_PERIOD + 1, VOLUME_SPIKE_LOOKBACK + 1): return None
    closes = [b.close for b in bars if b.close is not None]
    volumes = [b.volume for b in bars if isinstance(b.volume, (int, float))]
    vwaps = [b.vwap for b in bars if b.vwap is not None]
    if len(closes) < max(LONG_MA_PERIOD, RSI_PERIOD + 1) or len(volumes) < VOLUME_SPIKE_LOOKBACK + 1 or not vwaps: return None
    current_volume, current_vwap = volumes[-1], vwaps[-1]
    try:
        rsi = calculate_rsi(closes, RSI_PERIOD)
        short_ma = calculate_sma(closes, SHORT_MA_PERIOD)
        long_ma = calculate_sma(closes, LONG_MA_PERIOD)
        avg_volume = calculate_average_volume(volumes[-VOLUME_SPIKE_LOOKBACK-1:-1], VOLUME_SPIKE_LOOKBACK)
    except Exception as e: logging.error(f"[{symbol}] Error calculating indicators: {e}"); return None
    if rsi is None or short_ma is None or long_ma is None or avg_volume is None or current_vwap is None: return None
    bias, setup_type, potential_signal = None, None, False
    is_volume_spike = avg_volume > 0 and current_volume > (avg_volume * VOLUME_SPIKE_MULTIPLIER)
    prev_short_ma, prev_long_ma = calculate_sma(closes[:-1], SHORT_MA_PERIOD), calculate_sma(closes[:-1], LONG_MA_PERIOD)
    ma_crossed_up = prev_short_ma is not None and prev_long_ma is not None and prev_short_ma <= prev_long_ma and short_ma > long_ma
    ma_crossed_down = prev_short_ma is not None and prev_long_ma is not None and prev_short_ma >= prev_long_ma and short_ma < long_ma
    prev_close = closes[-2] if len(closes) >= 2 else None
    vwap_reclaimed = prev_close is not None and prev_close < current_vwap and latest_price > current_vwap
    vwap_rejected = prev_close is not None and prev_close > current_vwap and latest_price < current_vwap
    if is_volume_spike and latest_price > current_vwap and latest_price > long_ma: potential_signal, bias, setup_type = True, "LONG", "Vol Spike/Above VWAP"
    elif ma_crossed_up and rsi > 50: potential_signal, bias, setup_type = True, "LONG", "MA Cross Up"
    elif vwap_reclaimed and short_ma > long_ma: potential_signal, bias, setup_type = True, "LONG", "VWAP Reclaim/Trend"
    elif is_volume_spike and latest_price < current_vwap and latest_price < long_ma: potential_signal, bias, setup_type = True, "SHORT", "Vol Spike/Below VWAP"
    elif ma_crossed_down and rsi < 50: potential_signal, bias, setup_type = True, "SHORT", "MA Cross Down"
    elif vwap_rejected and short_ma < long_ma: potential_signal, bias, setup_type = True, "SHORT", "VWAP Reject/Trend"
    if potential_signal and random.random() < AI_SIMULATION_SIGNAL_PROBABILITY:
        if setup_type in SIGNAL_REASON_BLACKLIST: return {"decision": "REJECT", "symbol": symbol, "reason": f"Blacklisted Signal: {setup_type}"}
        if random.random() < AI_SIMULATION_EXECUTE_PROBABILITY:
            entry_price = latest_price
            if bias == "LONG":
                sl = entry_price * (1 - INITIAL_STOP_LOSS_PERCENT / 100.0)
                tp = entry_price * (1 + INITIAL_TAKE_PROFIT_PERCENT / 100.0)
                ts = entry_price * (1 + TRAIL_START_GAIN_PERCENT / 100.0)
            else:
                if not asset_info.get('shortable'): return None
                sl = entry_price * (1 + INITIAL_STOP_LOSS_PERCENT / 100.0)
                tp = entry_price * (1 - INITIAL_TAKE_PROFIT_PERCENT / 100.0)
                ts = entry_price * (1 - TRAIL_START_GAIN_PERCENT / 100.0)
            decision = {"decision": "EXECUTE", "symbol": symbol, "bias": bias, "entry_price": round(entry_price, 4),
                        "initial_stop_price": round(sl, 4), "initial_target_price": round(tp, 4), "trail_start_price": round(ts, 4),
                        "reason": f"SimAI: {setup_type} (RSI:{rsi:.1f}, Vol:{current_volume:.0f}/{avg_volume or 0:.0f})"}
            logging.info(f"[{symbol}] Simulated AI generated EXECUTE signal: {bias} ({setup_type})")
            return decision
        else: return {"decision": "REJECT", "symbol": symbol, "reason": "Simulated AI rejection"}
    return None


# === Alpaca Trade Execution === #
async def execute_trade(symbol: str, side: OrderSide, qty: float, tag: str = "") -> Optional[str]:
    """Submits a market order."""
    if qty <= 0: logging.error(f"[{symbol}] Invalid qty ({qty}) for {tag} trade."); return None
    try:
        order_data = MarketOrderRequest(symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.DAY)
        trade_order = await asyncio.to_thread(rest_client.submit_order, order_data=order_data)
        logging.info(f"[{symbol}] Market order ({tag}) submitted: {side.value} {qty:.4f} shares. ID: {trade_order.id}")
        return str(trade_order.id)
    except APIError as e: logging.error(f"[{symbol}] API error submitting {side.value} ({tag}) order: {e}")
    except Exception as e: logging.error(f"[{symbol}] Unexpected error submitting {side.value} ({tag}) order: {e}", exc_info=True)
    if telegram_app: await send_telegram_message(telegram_app.bot, f"🚨 **Trade Failed! ({tag})**\n`{symbol}` {side.value} {qty:.4f}\nError")
    return None

async def close_trade_position(symbol: str) -> Optional[str]:
    """Closes the entire position."""
    logging.info(f"[{symbol}] Attempting to close ENTIRE position.")
    try:
        close_order = await asyncio.to_thread(rest_client.close_position, symbol_or_asset_id=symbol)
        logging.info(f"[{symbol}] Close ENTIRE position order submitted. ID: {close_order.id}")
        return str(close_order.id)
    except APIError as e:
        if e.status_code == 404 or (e.status_code == 422 and "position not found" in str(e.message).lower()):
            logging.warning(f"[{symbol}] Close failed: No position found (Code: {e.status_code}).")
            return None
        logging.error(f"[{symbol}] API error closing position: {e}")
        if telegram_app: await send_telegram_message(telegram_app.bot, f"🚨 **Close Failed!** `{symbol}`\nError: `{e}`")
        return "ERROR"
    except Exception as e:
        logging.error(f"[{symbol}] Unexpected error closing position: {e}", exc_info=True)
        if telegram_app: await send_telegram_message(telegram_app.bot, f"🚨 **Close Failed!** `{symbol}`\nUnexpected Error")
        return "ERROR"


# === Position Sizing === #
def calculate_qty(symbol: str, entry_price: float, initial_stop_price: float, bias: str, asset_info: Dict) -> Optional[float]:
    """Calculates quantity based on risk % of equity."""
    global current_account_equity
    if entry_price <= 0 or initial_stop_price <= 0: return None
    if current_account_equity <= 0: logging.warning(f"[{symbol}] Equity <= 0. Cannot size."); return 0.0
    risk_per_share = abs(entry_price - initial_stop_price)
    if risk_per_share <= 1e-9: logging.warning(f"[{symbol}] Risk per share near zero."); return 0.0
    capital_to_risk = current_account_equity * (CAPITAL_PER_TRADE_RISK_PERCENT / 100.0)
    qty = capital_to_risk / risk_per_share
    if qty <= 0: return 0.0
    if qty * entry_price < 1.00: logging.warning(f"[{symbol}] Notional value < $1."); return 0.0
    is_short, is_fractionable = (bias == "SHORT"), asset_info.get('fractionable', False)
    if is_short: qty = math.floor(qty)
    elif is_fractionable: qty = math.floor(qty * 10000) / 10000
    else: qty = math.floor(qty)
    if qty <= 0: logging.warning(f"[{symbol}] Qty became <= 0 after rounding."); return 0.0
    if qty * entry_price < 1.00: logging.warning(f"[{symbol}] Notional < $1 after rounding."); return 0.0
    logging.info(f"[{symbol}] Calculated trade quantity: {qty} shares.")
    return qty


# === Core Strategy Loop === #
async def run_strategy_cycle(app: Optional[ApplicationBuilder] = None):
    """Main strategy cycle."""
    global state, current_account_equity
    logging.info(f"--- Starting Strategy Cycle v{SCRIPT_VERSION} ---")
    cycle_start_time = time.monotonic()

    # 1. Market Status Check
    try:
        clock = await asyncio.to_thread(rest_client.get_clock)
        if not clock.is_open and not EXTENDED_HOURS_TRADING:
            logging.info(f"Market closed. Next open: {clock.next_open.isoformat()}. Skipping.")
            return
    except Exception as e: logging.error(f"Error checking market status: {e}"); return

    # 2. Update Equity
    try:
        account_info = await asyncio.to_thread(rest_client.get_account)
        current_account_equity = float(account_info.equity)
        logging.info(f"Current Account Equity: ${current_account_equity:,.2f}")
    except Exception as e: logging.error(f"Error updating equity: {e}. Using previous: ${current_account_equity:,.2f}")

    # 3. Get Assets
    active_assets = await get_active_assets()
    if not active_assets: logging.warning("No active assets found."); return
    symbols_to_scan = list(active_assets.keys())
    logging.info(f"Scanning {len(symbols_to_scan)} potential symbols.")

    # 4. Fetch Data
    fetch_start_time = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    base_bars_start = now_utc - timedelta(minutes=90) # Adjusted lookback
    atr_bars_start = now_utc - timedelta(minutes=(ATR_PERIOD + 5) * ATR_TIMEFRAME.amount)
    spy_bars_start = now_utc - timedelta(minutes=(SPY_SLOPE_LOOKBACK + 5) * SPY_SLOPE_TIMEFRAME.amount)
    bars_end = now_utc
    tasks = {
        "prices": asyncio.create_task(get_latest_trades_batched(symbols_to_scan)),
        "base_bars": asyncio.create_task(get_bars_batched(symbols_to_scan, BASE_TIMEFRAME, base_bars_start, bars_end)),
        "atr_bars": asyncio.create_task(get_bars_batched(symbols_to_scan, ATR_TIMEFRAME, atr_bars_start, bars_end)),
        "spy_bars": asyncio.create_task(get_bars_batched(["SPY"], SPY_SLOPE_TIMEFRAME, spy_bars_start, bars_end))
    }
    try:
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        fetched_data = {k: r for k, r in zip(tasks.keys(), results) if not isinstance(r, Exception)}
        if "prices" not in fetched_data or "base_bars" not in fetched_data:
             logging.error("Critical data fetch failed. Ending cycle."); return
        latest_prices = fetched_data.get("prices", {})
        base_bars_data = fetched_data.get("base_bars", {})
        atr_bars_data = fetched_data.get("atr_bars", {})
        spy_bars = fetched_data.get("spy_bars", {}).get("SPY", [])
    except Exception as e: logging.error(f"Data fetch gather error: {e}"); return
    logging.info(f"Data fetch complete ({time.monotonic() - fetch_start_time:.2f}s).")

    # 4.5 Market Bias
    market_bias = "NEUTRAL"
    spy_slope = calculate_vwap_slope(spy_bars, SPY_SLOPE_LOOKBACK)
    if spy_slope is not None:
        if spy_slope > SPY_POSITIVE_SLOPE_THRESHOLD: market_bias = "BULLISH"
        elif spy_slope < SPY_NEGATIVE_SLOPE_THRESHOLD: market_bias = "BEARISH"
        logging.info(f"SPY Slope: {spy_slope:.4f}. Market Bias: {market_bias}")
    else: logging.warning("Could not calculate SPY slope.")

    # 5. Manage Exits
    exit_check_start_time = time.monotonic()
    async with state_lock: open_trades_copy = {tid: t.copy() for tid, t in state.get("trades", {}).items() if t.get("status") == "open"}
    open_pos_symbols = list({t['symbol'] for t in open_trades_copy.values()})
    open_pos_prices = await get_latest_trades_batched(open_pos_symbols) if open_pos_symbols else {}
    to_close_fully, to_scale_out, state_updates = {}, {}, {}

    for trade_id, trade in open_trades_copy.items():
        symbol, current_price = trade["symbol"], open_pos_prices.get(trade["symbol"])
        if current_price is None: continue
        bias, entry_price = trade.get("bias", "LONG"), float(trade.get("entry_price", 0))
        current_sl, initial_tp = trade.get("current_stop_price"), trade.get("initial_target_price")
        trail_start, trail_offset = trade.get("trail_start_price"), trade.get("trail_offset_percent")
        trail_active, scaled_out = trade.get("trail_active", False), trade.get("scaled_out", False)
        timeout_str = trade.get("hard_timeout_time")
        close_fully, scale_out, update_needed, exit_reason = False, False, False, None

        # Check Timeout
        if timeout_str:
             try:
                 if now_utc >= isoparse(timeout_str): exit_reason, close_fully = f"Timeout", True
             except Exception as e: logging.error(f"[{symbol}] Error parsing timeout str '{timeout_str}': {e}")
        # Check Stop
        if not close_fully and current_sl and ((bias == "LONG" and current_price <= current_sl) or (bias == "SHORT" and current_price >= current_sl)): exit_reason, close_fully = f"Stop Hit ({current_sl:.2f})", True
        # Check Scale Out
        elif not close_fully and not scaled_out and initial_tp and ((bias == "LONG" and current_price >= initial_tp) or (bias == "SHORT" and current_price <= initial_tp)): exit_reason, scale_out = f"Scale Target Hit ({initial_tp:.2f})", True
        # Check Trail
        elif not close_fully and not scale_out and trail_start and trail_offset:
            new_trail_active = trail_active
            new_stop = current_sl
            if not trail_active and ((bias == "LONG" and current_price >= trail_start) or (bias == "SHORT" and current_price <= trail_start)):
                new_trail_active = True; logging.info(f"[{symbol}] Trail activated: {trade_id[-6:]}")
            if new_trail_active:
                offset_amt = current_price * (trail_offset / 100.0)
                potential_stop = current_price - offset_amt if bias == "LONG" else current_price + offset_amt
                if bias == "LONG" and (new_stop is None or potential_stop > new_stop): new_stop = potential_stop
                elif bias == "SHORT" and (new_stop is None or potential_stop < new_stop): new_stop = potential_stop
            rounded_new_stop = round(new_stop, 4) if new_stop is not None else None
            if new_trail_active != trail_active or rounded_new_stop != current_sl:
                state_updates[trade_id] = {"trail_active": new_trail_active, "current_stop_price": rounded_new_stop}
                logging.info(f"[{symbol}] Trail updated: Active={new_trail_active}, Stop={rounded_new_stop}. ID: {trade_id[-6:]}")
                update_needed = True

        if close_fully: to_close_fully[trade_id] = exit_reason
        elif scale_out: to_scale_out[trade_id] = current_price # Use current price as trigger price

    # Execute Exits
    closed_trade_details = []
    exit_time = datetime.now(timezone.utc)

    if to_close_fully:
        logging.info(f"Closing fully {len(to_close_fully)} positions...")
        close_tasks = [close_trade_position(open_trades_copy[tid]['symbol']) for tid in to_close_fully]
        close_results = await asyncio.gather(*close_tasks, return_exceptions=True)
        async with state_lock:
            for i, trade_id in enumerate(to_close_fully.keys()):
                result = close_results[i]
                trade_info = state["trades"].get(trade_id)
                if not trade_info or trade_info.get("status") != "open": continue
                if isinstance(result, Exception) or result == "ERROR": logging.error(f"Error closing fully {trade_id[-6:]}: {result}"); continue
                symbol, exit_reason = trade_info["symbol"], to_close_fully[trade_id]
                trade_info.update({"status": "closed", "exit_time": exit_time.isoformat(), "exit_reason": exit_reason, "exit_order_id": result})
                entry_price, qty = float(trade_info.get("entry_price", 0)), float(trade_info.get("remaining_qty", trade_info.get("qty", 0)))
                bias = trade_info.get("bias", "LONG")
                exit_price = open_pos_prices.get(symbol, entry_price)
                profit = (exit_price - entry_price) * qty if bias == "LONG" else (entry_price - exit_price) * qty
                pct = (profit / (entry_price * qty) * 100) if entry_price * qty != 0 else 0
                total_profit = profit + float(trade_info.get("profit", 0.0))
                trade_info.update({"exit_price": round(exit_price, 4), "profit": round(total_profit, 2), "pct_change": round(pct, 2)})
                pnl_record = {k: trade_info.get(k) for k in ["trade_id", "symbol", "bias", "qty", "entry_price", "exit_price", "initial_stop_price", "initial_target_price", "entry_time", "exit_time", "exit_reason", "profit", "pct_change", "entry_order_id", "exit_order_id", "scaled_out"]}
                state.setdefault("pnl", []).append(pnl_record)
                closed_trade_details.append(pnl_record)
                logging.info(f"[{symbol}] Closed fully {trade_id[-6:]}. Reason: {exit_reason}. PnL: ${total_profit:.2f}")
                del state["trades"][trade_id] # Remove from active trades

    if to_scale_out:
        logging.info(f"Scaling out of {len(to_scale_out)} positions...")
        scale_tasks_data = []
        async with state_lock:
            for trade_id, scale_price in to_scale_out.items():
                trade_info = state["trades"].get(trade_id)
                if not trade_info or trade_info.get("status") != "open" or trade_info.get("scaled_out"): continue
                symbol, bias, initial_qty = trade_info['symbol'], trade_info['bias'], float(trade_info['qty'])
                scale_qty_raw = initial_qty * SCALE_OUT_FRACTION
                scale_qty = math.floor(scale_qty_raw) if bias == "SHORT" else math.floor(scale_qty_raw * 10000) / 10000
                if scale_qty <= 0: continue
                side = OrderSide.SELL if bias == "LONG" else OrderSide.BUY
                scale_tasks_data.append((symbol, side, scale_qty, trade_id, scale_price))
        scale_tasks = [execute_trade(s, sd, q, tag=f"ScaleOut-{tid[-6:]}") for s, sd, q, tid, sp in scale_tasks_data]
        scale_results = await asyncio.gather(*scale_tasks, return_exceptions=True)
        async with state_lock:
            for i, (symbol, side, scale_qty, trade_id, scale_price) in enumerate(scale_tasks_data):
                result = scale_results[i]
                trade_info = state["trades"].get(trade_id)
                if not trade_info or trade_info.get("status") != "open" or trade_info.get("scaled_out"): continue
                if isinstance(result, Exception) or result is None: logging.error(f"Error scaling out {trade_id[-6:]}: {result}"); continue
                initial_qty, entry_price = float(trade_info['qty']), float(trade_info['entry_price'])
                remaining = round(initial_qty - scale_qty, 4)
                bias = trade_info['bias']
                profit = (scale_price - entry_price) * scale_qty if bias == "LONG" else (entry_price - scale_price) * scale_qty
                trade_info.update({"scaled_out": True, "remaining_qty": remaining, "profit": round(profit, 2),
                                   "scale_out_order_id": result, "scale_out_time": exit_time.isoformat(), "scale_out_price": round(scale_price, 4)})
                logging.info(f"[{symbol}] Scaled out {scale_qty:.4f} shares {trade_id[-6:]}. Remain: {remaining:.4f}. PnL(part): ${profit:.2f}")
                if app: await send_telegram_message(app.bot, f"⚖️ **Scaled Out (2/3)**\n`{symbol}` ({bias})\nQty: {scale_qty:.4f} @ ~${scale_price:.2f}\nRemain: {remaining:.4f}\nPnL(part): ${profit:+.2f}")
                state_updates[trade_id] = trade_info.copy() # Ensure full update recorded

    # Apply state updates (trail adjustments or full updates from scale-out)
    if state_updates:
        async with state_lock:
            for trade_id, updates in state_updates.items():
                if trade_id in state["trades"]: state["trades"][trade_id].update(updates)

    # Final state save if anything changed
    if to_close_fully or to_scale_out or state_updates:
        async with state_lock: await asyncio.to_thread(save_state_sync, state)

    # Send notifications for fully closed trades
    if closed_trade_details and app:
        for pnl in closed_trade_details:
            profit, emoji = pnl['profit'], "✅" if pnl['profit'] >= 0 else "❌"
            scale_info = " (Scaled)" if pnl.get('scaled_out') else ""
            msg = (f"{emoji} **Closed (Final)**{scale_info}\n`{pnl['symbol']}` ({pnl['bias']})\n"
                   f"Reason: {pnl['exit_reason']}\nExit: ~${pnl['exit_price']:.2f}\nPnL: ${profit:+.2f}")
            await send_telegram_message(app.bot, msg)

    logging.info(f"Exit checks complete ({time.monotonic() - exit_check_start_time:.2f}s).")

    # 6. Evaluate Entries
    entry_eval_start_time = time.monotonic()
    async with state_lock:
        open_trade_count = len(state.get("trades", {}))
        held_symbols = {t['symbol'] for t in state.get("trades", {}).values()}
        recently_closed_symbols = set()
        cutoff = now_utc - timedelta(minutes=5)
        for pnl in state.get("pnl", []):
            try:
                if isoparse(pnl.get("exit_time", "")) > cutoff: recently_closed_symbols.add(pnl.get("symbol"))
            except Exception: pass

    num_can_open = MAX_OPEN_POSITIONS - open_trade_count
    logging.info(f"Evaluating entries. Slots: {num_can_open}. Held: {len(held_symbols)}. Recent: {len(recently_closed_symbols)}.")
    potential_entries = []

    if num_can_open > 0:
        symbols_to_analyze = [s for s in symbols_to_scan if s in latest_prices and s in base_bars_data and s not in held_symbols and s not in recently_closed_symbols]
        logging.info(f"Analyzing {len(symbols_to_analyze)} symbols.")
        quotes_data = await get_latest_quotes_batched(symbols_to_analyze) if symbols_to_analyze else {}
        analysis_count = 0
        for symbol in symbols_to_analyze:
            latest_price, base_bars = latest_prices.get(symbol), base_bars_data.get(symbol)
            atr_bars, asset_info = atr_bars_data.get(symbol), active_assets.get(symbol)
            quote_info = quotes_data.get(symbol)
            if not latest_price or not base_bars or not asset_info: continue
            analysis_count += 1
            ai_decision = analyze_setup_simulated(symbol, latest_price, base_bars, asset_info) if SIMULATE_AI else None
            if not ai_decision or ai_decision.get("decision") != "EXECUTE": continue

            # Filters
            passes, reason = True, ""
            # Liquidity
            if quote_info and quote_info.bid_price > 0 and quote_info.ask_price > 0:
                spread = quote_info.ask_price - quote_info.bid_price
                mid = (quote_info.ask_price + quote_info.bid_price) / 2
                spread_pct = (spread / mid * 100) if mid > 0 else float('inf')
                if quote_info.bid_size < MIN_QUOTE_SIZE or quote_info.ask_size < MIN_QUOTE_SIZE: passes, reason = False, f"LiqSize"
                elif spread_pct > MAX_SPREAD_PERCENT: passes, reason = False, f"LiqSpread"
            else: passes, reason = False, "NoQuote"
            # Volatility
            if passes and atr_bars:
                atr = calculate_atr(atr_bars, ATR_PERIOD)
                if atr and latest_price > 0:
                    atr_pct = (atr / latest_price) * 100
                    if atr_pct < MIN_ATR_PERCENT: passes, reason = False, f"ATR%"
                else: passes, reason = False, "ATR Calc"
            elif passes: passes, reason = False, "No ATR Bars"
            # Market Bias
            if passes:
                 bias = ai_decision["bias"]
                 async with state_lock:
                     longs = sum(1 for t in state.get("trades", {}).values() if t.get("bias") == "LONG")
                     shorts = sum(1 for t in state.get("trades", {}).values() if t.get("bias") == "SHORT")
                 max_against = math.floor(MAX_OPEN_POSITIONS * (MARKET_BIAS_POSITION_LIMIT_PERCENT / 100.0))
                 if bias == "SHORT" and market_bias == "BULLISH" and shorts >= max_against: passes, reason = False, f"BiasShort"
                 elif bias == "LONG" and market_bias == "BEARISH" and longs >= max_against: passes, reason = False, f"BiasLong"

            if passes: potential_entries.append(ai_decision)
            elif reason: logging.debug(f"[{symbol}] EXECUTE rejected by filter: {reason}")
        logging.info(f"Analyzed {analysis_count}. Found {len(potential_entries)} potential entries.")

    # Execute Entries
    executed_count = 0
    if potential_entries and num_can_open > 0:
        for entry in potential_entries:
            async with state_lock: current_open = len(state.get("trades", {}))
            if current_open >= MAX_OPEN_POSITIONS: logging.info("Max positions reached."); break
            symbol, bias = entry["symbol"], entry["bias"]
            # Re-check bias limit
            passes_bias = True
            async with state_lock:
                longs = sum(1 for t in state.get("trades", {}).values() if t.get("bias") == "LONG")
                shorts = sum(1 for t in state.get("trades", {}).values() if t.get("bias") == "SHORT")
                max_against = math.floor(MAX_OPEN_POSITIONS * (MARKET_BIAS_POSITION_LIMIT_PERCENT / 100.0))
                if bias == "SHORT" and market_bias == "BULLISH" and shorts >= max_against: passes_bias = False
                elif bias == "LONG" and market_bias == "BEARISH" and longs >= max_against: passes_bias = False
            if not passes_bias: logging.debug(f"[{symbol}] Entry skipped pre-exec: Bias limit."); continue

            entry_price, sl, tp, ts = entry["entry_price"], entry["initial_stop_price"], entry["initial_target_price"], entry["trail_start_price"]
            asset_info = active_assets.get(symbol)
            qty = calculate_qty(symbol, entry_price, sl, bias, asset_info)
            if qty is None or qty <= 0: logging.warning(f"[{symbol}] Invalid qty ({qty}) for entry."); continue

            trade_id = str(uuid4())
            order_side = OrderSide.BUY if bias == "LONG" else OrderSide.SELL
            entry_order_id = await execute_trade(symbol, order_side, qty, tag=f"Entry-{trade_id[-6:]}")

            if entry_order_id:
                entry_time = datetime.now(timezone.utc)
                timeout_dt = entry_time + timedelta(minutes=HARD_TIMEOUT_MINUTES)
                new_trade = {
                    "trade_id": trade_id, "user": "SYSTEM_AI", "symbol": symbol, "bias": bias, "qty": qty, "remaining_qty": qty,
                    "entry_price": entry_price, "initial_stop_price": sl, "initial_target_price": tp, "current_stop_price": sl,
                    "trail_start_price": ts, "trail_offset_percent": TRAIL_OFFSET_PERCENT, "trail_active": False, "scaled_out": False,
                    "entry_time": entry_time.isoformat(), "hard_timeout_time": timeout_dt.isoformat(), "entry_order_id": entry_order_id,
                    "status": "open", "exit_time": None, "exit_price": None, "exit_reason": None, "exit_order_id": None,
                    "scale_out_order_id": None, "scale_out_time": None, "scale_out_price": None, "profit": 0.0, "pct_change": None,
                    "ai_reason": entry.get("reason", "N/A")
                }
                async with state_lock:
                    state.setdefault("trades", {})[trade_id] = new_trade
                    await asyncio.to_thread(save_state_sync, state)
                executed_count += 1
                logging.info(f"[{symbol}] Executed {bias} entry. ID: {trade_id[-6:]}, Order: {entry_order_id}")
                if app:
                    msg = (f"{'⬆️' if bias == 'LONG' else '⬇️'} **Entered**\n`{symbol}` ({bias})\n"
                           f"Qty: {qty:.4f} @ ~${entry_price:.2f}\nSL: {sl:.2f}, TP: {tp:.2f}\n"
                           f"Reason: _{entry.get('reason', 'N/A')}_")
                    await send_telegram_message(app.bot, msg)
            else: logging.error(f"[{symbol}] Failed to execute {bias} entry order.")

    logging.info(f"Entry evaluation complete ({time.monotonic() - entry_eval_start_time:.2f}s). Executed: {executed_count}.")
    logging.info(f"--- Strategy Cycle v{SCRIPT_VERSION} Finished ({time.monotonic() - cycle_start_time:.2f}s) ---")


# === Main Application Logic === #
SCRIPT_VERSION = "1.1.3" # Define script version

async def main():
    """Initializes and runs the bot's main loop."""
    global telegram_app
    load_state_sync()
    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            # Add handlers (start, stop, status, pnl, message, error)
            handlers = [CommandHandler("start", handle_start), CommandHandler("stop", handle_stop),
                        CommandHandler("status", handle_status), CommandHandler("pnl", handle_pnl),
                        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)]
            telegram_app.add_handlers(handlers)
            telegram_app.add_error_handler(error_handler)
            await telegram_app.initialize()
            await telegram_app.start()
            await telegram_app.updater.start_polling(drop_pending_updates=True)
            logging.info("Telegram bot started.")
            await send_telegram_message(telegram_app.bot, f"🚀 Scalper Bot v{SCRIPT_VERSION} Started!")
        except Exception as e: logging.critical(f"Failed to start Telegram bot: {e}", exc_info=True); telegram_app = None
    else: logging.warning("No Telegram token. Running without Telegram.")

    while not shutdown_event.is_set():
        loop_start = time.monotonic()
        try: await run_strategy_cycle(telegram_app)
        except APIError as e:
             logging.error(f"API Error in main loop: {e}", exc_info=True)
             if e.status_code == 401: logging.critical("CRITICAL: Alpaca API Unauthorized (401)."); shutdown_event.set(); break
             await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Unhandled exception in main loop: {e}", exc_info=True)
            if telegram_app: await send_telegram_message(telegram_app.bot, f"🚨 Unhandled Error: {type(e).__name__}. Check logs.")
            await asyncio.sleep(15)
        if shutdown_event.is_set(): break
        elapsed = time.monotonic() - loop_start
        wait = max(0, SCAN_INTERVAL_SECONDS - elapsed)
        logging.debug(f"Cycle took {elapsed:.2f}s. Waiting {wait:.2f}s.")
        try: await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
        except asyncio.TimeoutError: pass
        except Exception as e: logging.error(f"Error during wait: {e}"); break

async def perform_shutdown():
    """Handles graceful shutdown."""
    logging.info("Starting shutdown...")
    if telegram_app and telegram_app.updater and telegram_app.updater.is_running:
        logging.info("Stopping Telegram...")
        try: await telegram_app.updater.stop(); await telegram_app.stop(); await telegram_app.shutdown()
        except Exception as e: logging.error(f"Error stopping Telegram: {e}")
    logging.info("Saving final state...")
    try: await asyncio.to_thread(save_state_sync, state)
    except Exception as e: logging.error(f"Error saving final state: {e}")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if tasks:
        logging.info(f"Cancelling {len(tasks)} tasks...")
        [task.cancel() for task in tasks]
        try: await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError: pass
        logging.info("Tasks cancelled.")
    logging.info(f"--- Momentum Scalper Bot v{SCRIPT_VERSION} Shutdown Complete ---")

def signal_handler_func(signum, frame):
    """Signal handler."""
    logging.info(f"Received signal {signum}. Initiating shutdown...")
    shutdown_event.set()

if __name__ == "__main__":
    try: import numpy
    except ImportError: print("ERROR: numpy required.", file=sys.stderr); sys.exit(1)
    try: loop = asyncio.get_running_loop()
    except RuntimeError: loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, signal_handler_func, sig, None)
    else: logging.warning("Use Ctrl+C on Windows.")
    main_task, shutdown_task = None, None
    try:
        logging.info("Starting main task...")
        main_task = loop.create_task(main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt: logging.info("KeyboardInterrupt. Shutting down..."); shutdown_event.set();
    except asyncio.CancelledError: logging.info("Main task cancelled.")
    finally:
        logging.info("Entering final shutdown.")
        if not shutdown_event.is_set(): shutdown_event.set()
        if main_task and not main_task.done(): main_task.cancel() # Ensure main task is cancelled
        try:
            shutdown_task = loop.create_task(perform_shutdown())
            loop.run_until_complete(shutdown_task)
        except Exception as e: logging.error(f"Error during shutdown task: {e}", exc_info=True)
        try: loop.run_until_complete(loop.shutdown_asyncgens())
        finally: logging.info("Closing event loop."); loop.close(); asyncio.set_event_loop(None)
