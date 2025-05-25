# Genie_stocks_Top3_v3.2.py (Patched for Daily Bar Request)
import os
import logging
import time
from datetime import datetime, timedelta, timezone as dt_timezone, date as dt_date, time as dt_time
import pytz
import pandas as pd
import numpy as np
from scipy.stats import zscore

from dotenv import load_dotenv
load_dotenv()

from alpaca.trading.client import TradingClient
from alpaca.trading.models import Asset, Calendar
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import (
    LimitOrderRequest, GetAssetsRequest, QueryOrderStatus, ReplaceOrderRequest,
    GetCalendarRequest, StopOrderRequest
)
from alpaca.trading.enums import (
    OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus as AlpacaOrderStatus
)
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest, StockSnapshotRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import finnhub

# --- Configuration Defaults ---
DEFAULT_CONFIG = {
    "ALPACA_API_KEY": os.getenv("ALPACA_API_KEY"),
    "ALPACA_SECRET_KEY": os.getenv("ALPACA_SECRET_KEY"),
    "ALPACA_PAPER_TRADING": True,
    "FINNHUB_API_KEY": os.getenv("FINNHUB_API_KEY"),
    "RISK_PER_TRADE": 100.0, "ATR_PERIOD": 14, "ATR_BARS_LOOKBACK_MINUTES": 75,
    "TOP_N_PERCENT_CHANGE_CANDIDATES": 150, "STAGE_A_SCAN_INTERVAL_MINUTES": 2,
    "STAGE_D_RECONFIRM_INTERVAL_SECONDS": 20, "MIN_PRE_MARKET_VOLUME": 50000,
    "MIN_PRE_MARKET_NOTIONAL": 500000, "MAX_SPREAD_PERCENTAGE": 0.02,
    "MAX_SPREAD_ABSOLUTE_LOW_PRICE": 0.05, "LOW_PRICE_THRESHOLD_FOR_SPREAD": 2.0,
    "OTC_MAX_SPREAD_PERCENTAGE_FOR_SHORT": 0.01, "SENTIMENT_WEIGHT_VADER": 0.3,
    "SENTIMENT_WEIGHT_FINBERT": 0.0, "SENTIMENT_WEIGHT_EARNINGS": 0.2,
    "SENTIMENT_WEIGHT_SOCIAL": 0.0, "SP500_SYMBOLS_FILE": "sp500_symbols.txt",
    "MARKET_BREADTH_THRESHOLD_PCT": 1.0, "RETRACEMENT_THRESHOLD_FRACTION": 1/3,
    "ADAPTIVE_LIMIT_PRICE_PCT_COMPONENT": 0.005, "ADAPTIVE_LIMIT_PRICE_SPREAD_COMPONENT_FRACTION": 0.5,
    "MARKETABLE_LIMIT_CONVERSION_SPREAD_THRESHOLD_PCT": 0.003,
    "MARKETABLE_LIMIT_TICK_ADJUSTMENT": 0.01, "ORDER_UNFILLED_CONVERSION_SECONDS": 60,
    "ORDER_CANCELLATION_TIME_ET_STR": "10:00",
    "ALLOWED_EXCHANGES": [AssetExchange.NASDAQ, AssetExchange.NYSE, AssetExchange.ARCA, AssetExchange.BATS],
    "INCLUDE_OTC": True, "LOG_JSON_PER_TRADE_FILE": "genie_top3_v3_tradelog.jsonl",
    "EXCLUDE_INVERSE_ETF": True,
    "INVERSE_KEYWORDS": ["Bear", "Inverse", "Short", "-1X", "-2X", "-3X", "UltraShort", "S&P500 VIX Short"],
    "STOP_LOSS_ATR_MULT": 2.0, "TRAIL_TRIGGER_ATR": 1.0, "TRAIL_ATR_MULT": 1.5,
    "ATR_FOR_EXIT_PERIODS": 14,
}

# --- Logger Setup ---
logger = logging.getLogger("GenieStocksV3.2")
if not logger.handlers:
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)

# --- Timezone ---
ET = pytz.timezone("America/New_York")

# --- State Object ---
class PremarketScannerState:
    def __init__(self, config_dict):
        self.config = config_dict
        self.last_stage_a_scan_time_et = None
        self.daily_data_initialized_date_et = None
        self.previous_day_closes = {}
        self.tradable_assets_map = {}
        self.sp500_symbols = self._load_sp500_symbols()
        self.current_top_200_raw = pd.DataFrame()
        self.current_filtered_candidates = pd.DataFrame()
        self.current_shortlist = pd.DataFrame()
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.finbert_pipeline = self._initialize_finbert()
        self.social_fetcher = self._initialize_social_fetcher()
        self.positions_to_trail = {}
        self.cached_stop_prices = {}

    def _load_sp500_symbols(self):
        try:
            with open(self.config["SP500_SYMBOLS_FILE"], 'r') as f:
                symbols = [line.strip().upper() for line in f if line.strip()]
                logger.info(f"Loaded {len(symbols)} S&P 500 symbols from {self.config['SP500_SYMBOLS_FILE']}.")
                return symbols
        except FileNotFoundError:
            logger.warning(f"S&P 500 symbols file '{self.config['SP500_SYMBOLS_FILE']}' not found. Market breadth calculation will be limited or use a small default list.")
            return ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
        except Exception as e:
            logger.error(f"Error loading S&P500 symbols: {e}")
            return ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

    def _initialize_finbert(self):
        if self.config.get("SENTIMENT_WEIGHT_FINBERT", 0) > 0:
            logger.warning("FinBERT initialization not implemented by user. FinBERT sentiment will be zero.")
        return None

    def _initialize_social_fetcher(self):
        if self.config.get("SENTIMENT_WEIGHT_SOCIAL", 0) > 0:
            logger.warning("Social media fetcher not implemented by user. Social sentiment will be zero.")
        return None

    def reset_daily_state_values(self, current_date_et_val: dt_date):
        self.previous_day_closes = {}
        self.tradable_assets_map = {}
        self.current_top_200_raw = pd.DataFrame()
        self.current_filtered_candidates = pd.DataFrame()
        self.current_shortlist = pd.DataFrame()
        self.daily_data_initialized_date_et = current_date_et_val
        self.positions_to_trail = {}
        self.cached_stop_prices = {}
        logger.info(f"Daily scanner state values (including trailing stop info) reset for {current_date_et_val}.")

# --- Helper Functions ---
# (get_atr_for_exits, get_last_trade_price, _get_asset_map functions remain the same as last version)
def get_atr_for_exits(
    symbol: str,
    alpaca_data_client: StockHistoricalDataClient,
    config: dict,
    now_dt_et: datetime
) -> float | None:
    periods = config.get("ATR_FOR_EXIT_PERIODS", 14)
    lookback_minutes = max(periods * 5, config.get("ATR_BARS_LOOKBACK_MINUTES", 75))
    end_dt_utc = now_dt_et.astimezone(dt_timezone.utc)
    start_dt_utc = end_dt_utc - timedelta(minutes=lookback_minutes)
    try:
        bars_req = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Minute,
            start=start_dt_utc,
            end=end_dt_utc,
            limit=periods * 3,
            feed="sip"
        )
        bars_data = alpaca_data_client.get_stock_bars(bars_req)
        if symbol not in bars_data.data or not bars_data.data[symbol]:
            logger.warning(f"ATR_EXITS: No 1-min bars for {symbol} in lookback window.")
            return None
        df = pd.DataFrame([b.__dict__ for b in bars_data.data[symbol]])
        if len(df) < 2:
            logger.warning(f"ATR_EXITS: Not enough bars for {symbol} ({len(df)}) for ATR calc.")
            return None
        df['high_low'] = df['high'] - df['low']
        df['high_prev_close'] = np.abs(df['high'] - df['close'].shift(1))
        df['low_prev_close'] = np.abs(df['low'] - df['close'].shift(1))
        df['true_range'] = df[['high_low', 'high_prev_close', 'low_prev_close']].max(axis=1)
        df.dropna(subset=['true_range'], inplace=True)
        if len(df) < periods:
            logger.warning(f"ATR_EXITS: Not enough True Range bars for {symbol} ({len(df)}) for {periods}-period ATR.")
            return None
        atr_val = df['true_range'].ewm(alpha=1/periods, adjust=False).mean().iloc[-1]
        logger.debug(f"ATR_EXITS: Calculated ATR for {symbol} over {periods} periods: {atr_val:.4f} using {len(df)} TRs.")
        return float(atr_val) if pd.notna(atr_val) and atr_val > 0.00001 else None
    except Exception as e:
        logger.error(f"ATR_EXITS: Error calculating ATR for {symbol}: {e}", exc_info=True)
        return None

def get_last_trade_price(symbol: str, alpaca_data_client: StockHistoricalDataClient) -> float | None:
    try:
        latest_trade_req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed="sip")
        latest_trade = alpaca_data_client.get_stock_latest_trade(latest_trade_req)
        if symbol in latest_trade and latest_trade[symbol] and latest_trade[symbol].price > 0:
            return latest_trade[symbol].price
        else:
            latest_quote_req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed="sip")
            latest_quote = alpaca_data_client.get_stock_latest_quote(latest_quote_req)
            if symbol in latest_quote and latest_quote[symbol] and latest_quote[symbol].ask_price > 0:
                return latest_quote[symbol].ask_price
            logger.warning(f"Could not get valid last trade or quote price for {symbol}")
            return None
    except APIError as e:
        logger.error(f"APIError fetching last trade price for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching last trade price for {symbol}: {e}", exc_info=True)
        return None

def _get_asset_map(alpaca_trading_client: TradingClient, config: dict) -> dict[str, Asset]:
    search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
    all_assets_list = alpaca_trading_client.get_all_assets(search_params)
    asset_map = {}
    for asset_obj in all_assets_list:
        if asset_obj.tradable and '.' not in asset_obj.symbol and '/' not in asset_obj.symbol:
            if asset_obj.exchange in config["ALLOWED_EXCHANGES"]:
                asset_map[asset_obj.symbol] = asset_obj
            elif config["INCLUDE_OTC"] and asset_obj.exchange == AssetExchange.OTC:
                asset_map[asset_obj.symbol] = asset_obj
    logger.info(f"Fetched {len(asset_map)} tradable assets.")
    return asset_map

def _fetch_previous_day_closes_from_alpaca(
    alpaca_data_client: StockHistoricalDataClient,
    alpaca_trading_client: TradingClient,
    symbols: list,
    current_date_et: dt_date,
    now_dt_et: datetime
):
    closes = {}
    actual_prev_close_date_et = None
    try:
        start_calendar_check = current_date_et - timedelta(days=7)
        end_calendar_check = current_date_et - timedelta(days=1)

        logger.debug(f"Fetching calendar using GetCalendarRequest from {start_calendar_check} to {end_calendar_check}")
        calendar_request = GetCalendarRequest(start=start_calendar_check, end=end_calendar_check)
        calendars: list[Calendar] = alpaca_trading_client.get_calendar(calendar_request)

        if not calendars:
            logger.error(f"Could not get calendar data for the range {start_calendar_check} to {end_calendar_check}.")
            return {}, None
        # Corrected line: c.date is already a date object
        valid_past_calendars = [c for c in calendars if c.date < current_date_et]
        if not valid_past_calendars:
            logger.error(f"No valid past trading days found in calendar data up to {end_calendar_check}.")
            return {}, None

        valid_past_calendars.sort(key=lambda c: c.date, reverse=True)
        last_trading_day_calendar = valid_past_calendars[0]
        actual_prev_close_date_et = last_trading_day_calendar.date

        logger.info(f"Identified last trading day as {actual_prev_close_date_et}. Fetching closes.")
        fetched_closes_for_day = {}

        # --- PATCH: Define start_dt and end_dt for daily bar requests ---
        # These are UTC datetimes bracketing the entire previous trading day
        start_dt = datetime.combine(actual_prev_close_date_et, dt_time(0, 0), tzinfo=dt_timezone.utc)
        end_dt = start_dt + timedelta(days=1) # Exclusive end, so this covers the full start_dt day
        logger.debug(f"Fetching EOD bars for {actual_prev_close_date_et} using UTC window: {start_dt.isoformat()} to {end_dt.isoformat()}")
        # --- END PATCH ---

        batch_size = 500
        for j in range(0, len(symbols), batch_size):
            batch = symbols[j:j+batch_size]
            if not batch: continue

            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                # --- PATCH: Use start_dt and end_dt ---
                start=start_dt,
                end=end_dt,
                # --- END PATCH ---
                feed="sip"
            )
            logger.debug(f"Requesting EOD for batch {j//batch_size + 1} ({len(batch)} symbols). Symbols: {batch[:3]}...")

            bars_resp = alpaca_data_client.get_stock_bars(request_params)
            bars_df = bars_resp.df

            if (bars_df is None or bars_df.empty) and now_dt_et.time() < dt_time(7, 30):
                logger.info(f"Batch {j//batch_size + 1}: SIP feed returned empty for EOD bars before 07:30 ET. Trying IEX feed.")
                request_params.feed = "iex" # type: ignore
                bars_resp_iex = alpaca_data_client.get_stock_bars(request_params)
                bars_df = bars_resp_iex.df
                if bars_df is not None and not bars_df.empty:
                    logger.info(f"Batch {j//batch_size + 1}: IEX feed provided EOD data.")
                else:
                    logger.warning(f"Batch {j//batch_size + 1}: IEX feed also returned empty for EOD data.")
            
            if bars_df is not None and not bars_df.empty:
                # The DataFrame should have a multi-index (symbol, timestamp)
                # We want the 'close' for each symbol. Since we requested for a specific day,
                # there should ideally be only one bar per symbol.
                # If multiple bars are returned for a symbol for TimeFrame.Day (unusual for a single day request span),
                # groupby().last() will pick the last one.
                last_closes = bars_df['close'].groupby(level=0).last()
                
                update_dict = {}
                for sym, px in last_closes.items():
                    if pd.notna(px):
                        # Verify the bar's date matches actual_prev_close_date_et
                        # The timestamp in bars_df.index is UTC.
                        # Get the specific timestamp for this symbol's bar
                        bar_timestamp_utc = bars_df.loc[sym].index[-1] # Assuming last bar if multiple, or only bar
                        bar_date_et = bar_timestamp_utc.astimezone(ET).date()
                        if bar_date_et == actual_prev_close_date_et:
                            update_dict[sym] = {"close_price": float(px), "close_date_et": actual_prev_close_date_et}
                        else:
                            logger.warning(f"Symbol {sym}: EOD bar date {bar_date_et} mismatch with expected {actual_prev_close_date_et}. Timestamp: {bar_timestamp_utc.isoformat()}. Price: {px}. Skipping.")
                
                fetched_closes_for_day.update(update_dict)
                logger.debug(f"Batch {j//batch_size + 1}: Processed {len(last_closes)} symbols, added {len(update_dict)} valid closes from bars_df.")
            else:
                logger.warning(f"Batch {j//batch_size + 1}: No bar data DataFrame returned or DataFrame empty for symbols: {batch[:3]}")
            time.sleep(0.12)

        # --- PATCH: Optional Guard ---
        if not fetched_closes_for_day:
            logger.critical(f"Previous-day closes ({actual_prev_close_date_et}) still empty after processing all batches - aborting today’s EOD fetch.")
            return {}, None # Return empty and None as before, but after a critical log
        # --- END PATCH ---

        logger.info(f"Fetched {len(fetched_closes_for_day)} previous day closes for {actual_prev_close_date_et}.")
        return fetched_closes_for_day, actual_prev_close_date_et

    except APIError as e:
        logger.error(f"API error fetching calendar or EOD bars: {e}")
    except Exception as e:
        logger.error(f"Unexpected error determining previous day closes: {e}", exc_info=True)
    return {}, None


# ... (_fetch_live_market_data_batch, _fetch_catalysts_and_calc_sentiment (with 502 fix),
#      _calculate_market_breadth, _get_1min_atr, _log_trade_decision_json,
#      run_premarket_scan_stages_A_B_C (caller of _fetch_previous_day_closes_from_alpaca),
#      place_entry_orders_stages_D_E, and __main__ block remain structurally the same as the prior version
#      where these patches were applied, ensuring `now_dt_et` is passed to _fetch_previous_day_closes_from_alpaca)
# For brevity, I'm not re-pasting all of them but the changes are localized to _fetch_previous_day_closes_from_alpaca
# and the call to it in run_premarket_scan_stages_A_B_C.
# The Finnhub 502 fix is in _fetch_catalysts_and_calc_sentiment.

def _fetch_live_market_data_batch(alpaca_data_client: StockHistoricalDataClient, symbols: list, now_dt_et: datetime):
    # (Identical to previous version, ensure it's robust)
    data = {sym: {} for sym in symbols}
    if not symbols: return data
    start_pre_market_et = now_dt_et.replace(hour=4, minute=0, second=0, microsecond=0)
    if now_dt_et < start_pre_market_et: 
        end_pre_market_et = start_pre_market_et
    else: 
        end_pre_market_et = min(now_dt_et, now_dt_et.replace(hour=9, minute=38, second=0, microsecond=0))

    if start_pre_market_et <= end_pre_market_et : 
        try:
            batch_size = 200
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i+batch_size]
                if not batch: continue
                bar_request = StockBarsRequest(
                    symbol_or_symbols=batch, timeframe=TimeFrame.Minute,
                    start=start_pre_market_et.astimezone(dt_timezone.utc), 
                    end=end_pre_market_et.astimezone(dt_timezone.utc),     
                    feed="sip"
                )
                bars_data = alpaca_data_client.get_stock_bars(bar_request)
                for symbol, bars in bars_data.data.items(): 
                    if bars:
                        df_bars = pd.DataFrame([bar.__dict__ for bar in bars])
                        total_volume = df_bars['volume'].sum()
                        vwap_val = 0
                        if total_volume > 0:
                            if 'vwap' in df_bars.columns and pd.notna(df_bars['vwap']).any() and (df_bars['vwap'] > 0).any():
                                valid_vwap_bars = df_bars[df_bars['vwap'].notna() & (df_bars['vwap'] > 0)]
                                if not valid_vwap_bars.empty:
                                     vwap_val = (valid_vwap_bars['vwap'] * valid_vwap_bars['volume']).sum() / valid_vwap_bars['volume'].sum()
                                else:
                                     vwap_val = ((df_bars['high'] + df_bars['low'] + df_bars['close']) / 3 * df_bars['volume']).sum() / total_volume
                            else:
                                vwap_val = ((df_bars['high'] + df_bars['low'] + df_bars['close']) / 3 * df_bars['volume']).sum() / total_volume
                        elif bars:
                            vwap_val = bars[-1].close
                        data[symbol].update({
                            "vwap": vwap_val, "high": df_bars['high'].max(), "low": df_bars['low'].min(),
                            "volume": total_volume,
                            "avg_vol_1m_last_30m": df_bars['volume'].tail(30).mean() if len(df_bars) >= 1 else 0,
                            "pre_mkt_open": bars[0].open,
                            "pre_mkt_high": df_bars['high'].max(),
                            "pre_mkt_low": df_bars['low'].min()
                        })
                time.sleep(0.1)
        except Exception as e: logger.error(f"Error fetching/processing pre-market bars: {e}", exc_info=True)
    else:
        logger.debug(f"Skipping pre-market bar fetch: start_pre_market_et ({start_pre_market_et}) > end_pre_market_et ({end_pre_market_et})")

    try:
        batch_size = 500
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            if not batch: continue
            snapshot_request = StockSnapshotRequest(symbol_or_symbols=batch, feed="sip")
            snapshots = alpaca_data_client.get_stock_snapshot(snapshot_request)
            for symbol, snap_item in snapshots.items():
                if not snap_item: continue
                current_price_val = None
                if snap_item.latest_trade and snap_item.latest_trade.price > 0:
                    current_price_val = snap_item.latest_trade.price
                elif snap_item.latest_quote and snap_item.latest_quote.ask_price > 0:
                    current_price_val = snap_item.latest_quote.ask_price
                elif snap_item.minute_bar and snap_item.minute_bar.close > 0:
                    current_price_val = snap_item.minute_bar.close
                elif data.get(symbol, {}).get('vwap', 0) > 0 :
                    current_price_val = data[symbol]['vwap']

                if snap_item.latest_quote and snap_item.latest_quote.bid_price > 0 and snap_item.latest_quote.ask_price > 0:
                    data[symbol].update({
                        "bid": snap_item.latest_quote.bid_price,
                        "ask": snap_item.latest_quote.ask_price,
                        "current_price": current_price_val if current_price_val else snap_item.latest_quote.ask_price,
                        "quote_time_et": snap_item.latest_quote.timestamp.astimezone(ET)
                    })
                elif snap_item.minute_bar:
                    data[symbol].setdefault("bid", snap_item.minute_bar.close)
                    data[symbol].setdefault("ask", snap_item.minute_bar.close)
                    data[symbol].setdefault("current_price", current_price_val if current_price_val else snap_item.minute_bar.close)
                    data[symbol].setdefault("quote_time_et", snap_item.minute_bar.timestamp.astimezone(ET))
            time.sleep(0.1)
    except Exception as e: logger.error(f"Error fetching/processing snapshots: {e}", exc_info=True)
    return data

def _fetch_catalysts_and_calc_sentiment(
    scanner_state: PremarketScannerState,
    finnhub_client: finnhub.Client,
    symbols: list,
    now_dt_et: datetime
):
    sentiment_data = {sym: {"news_headlines": [], "earnings_data": None, "earnings_surprise_score": 0.0,
                            "vader_score":0.0, "finbert_score": 0.0, "social_score": 0.0,
                            "final_sentiment": 0.0} for sym in symbols}
    config = scanner_state.config
    use_finnhub = finnhub_client and (config["SENTIMENT_WEIGHT_VADER"] > 0 or config["SENTIMENT_WEIGHT_EARNINGS"] > 0 or config["SENTIMENT_WEIGHT_FINBERT"] > 0)
    use_finbert = scanner_state.finbert_pipeline and config["SENTIMENT_WEIGHT_FINBERT"] > 0
    use_social = scanner_state.social_fetcher and config["SENTIMENT_WEIGHT_SOCIAL"] > 0

    if not (use_finnhub or use_finbert or use_social):
        logger.debug("No sentiment sources active or weighted. Skipping catalyst/sentiment fetch.")
        return sentiment_data

    start_date_str = (now_dt_et - timedelta(days=2)).strftime('%Y-%m-%d')
    end_date_str = now_dt_et.strftime('%Y-%m-%d')

    for symbol_val in symbols:
        vader_s, finbert_s, earnings_s, social_s = 0.0, 0.0, 0.0, 0.0
        headlines = []
        if use_finnhub or use_finbert:
            try:
                if finnhub_client:
                    news = finnhub_client.company_news(symbol_val, _from=start_date_str, to=end_date_str)
                    headlines = [n['headline'] for n in news[:5] if n.get('headline')]
                    sentiment_data[symbol_val]["news_headlines"] = headlines
                    if config["SENTIMENT_WEIGHT_EARNINGS"] > 0:
                        calendar = finnhub_client.earnings_calendar(_from=start_date_str, to=end_date_str, symbol=symbol_val, international=False)
                        if calendar and calendar.get('earningsCalendar'):
                            for earning_event in calendar['earningsCalendar']:
                                if earning_event.get('epsActual') is not None and earning_event.get('epsEstimate') is not None:
                                    sentiment_data[symbol_val]["earnings_data"] = earning_event
                                    if earning_event['epsActual'] > earning_event['epsEstimate']: earnings_s = 1.0
                                    elif earning_event['epsActual'] < earning_event['epsEstimate']: earnings_s = -1.0
                                    break
                if headlines:
                    if config["SENTIMENT_WEIGHT_VADER"] > 0:
                        vader_scores_list = [scanner_state.vader_analyzer.polarity_scores(str(h))['compound'] for h in headlines]
                        vader_s = sum(vader_scores_list) / len(vader_scores_list) if vader_scores_list else 0.0
                    if use_finbert and scanner_state.finbert_pipeline:
                        logger.debug(f"FinBERT processing for {symbol_val} headlines (stubbed).")
                        pass
                if finnhub_client: time.sleep(0.6) 
            except finnhub.FinnhubAPIException as e:
                if e.status_code == 502: # PATCHED
                    logger.warning(f"Finnhub 502 Bad Gateway for {symbol_val}; skipping news/earnings for this symbol.")
                    continue 
                if e.status_code == 429:
                    logger.warning("Finnhub rate limit hit. Breaking sentiment fetch loop.")
                    break
                logger.error(f"Finnhub API error for {symbol_val}: {e}")
            except Exception as e:
                logger.error(f"Error fetching Finnhub/news data for {symbol_val}: {e}")
        if use_social and scanner_state.social_fetcher:
            logger.debug(f"Social sentiment fetch for {symbol_val} (stubbed).")
            pass
        sentiment_data[symbol_val].update({
            "vader_score": vader_s, "finbert_score": finbert_s,
            "earnings_surprise_score": earnings_s, "social_score": social_s
        })
        final_sent = (
            vader_s * config["SENTIMENT_WEIGHT_VADER"] + finbert_s * config["SENTIMENT_WEIGHT_FINBERT"] +
            earnings_s * config["SENTIMENT_WEIGHT_EARNINGS"] + social_s * config["SENTIMENT_WEIGHT_SOCIAL"]
        )
        sentiment_data[symbol_val]["final_sentiment"] = np.clip(final_sent, -1.0, 1.0)
    return sentiment_data

def _calculate_market_breadth(
    scanner_state: PremarketScannerState,
    alpaca_data_client: StockHistoricalDataClient,
    now_dt_et: datetime
):
    if not scanner_state.sp500_symbols: return 0.5
    sp500_prev_closes = { sym: scanner_state.previous_day_closes.get(sym) for sym in scanner_state.sp500_symbols if scanner_state.previous_day_closes.get(sym) }
    symbols_for_breadth_live_fetch = list(sp500_prev_closes.keys())
    if not symbols_for_breadth_live_fetch:
        logger.warning("No S&P500 symbols with previous close data for breadth calculation.")
        return 0.5
    sp500_live_data = _fetch_live_market_data_batch(alpaca_data_client, symbols_for_breadth_live_fetch, now_dt_et)
    up_count = 0
    considered_count = 0
    for symbol_val, prev_close_info in sp500_prev_closes.items():
        live_info = sp500_live_data.get(symbol_val)
        if live_info and live_info.get('current_price') and prev_close_info.get('close_price') and prev_close_info['close_price'] > 0:
            current_price_val = live_info['current_price']
            prev_close_val = prev_close_info['close_price']
            pct_change_val = ((current_price_val - prev_close_val) / prev_close_val) * 100
            if pct_change_val > scanner_state.config["MARKET_BREADTH_THRESHOLD_PCT"]:
                up_count += 1
            considered_count += 1
    breadth = (up_count / considered_count) if considered_count > 0 else 0.5
    logger.debug(f"Market Breadth: {breadth:.3f} ({up_count}/{considered_count} S&P500 symbols up > {scanner_state.config['MARKET_BREADTH_THRESHOLD_PCT']}%)")
    return breadth

def _get_1min_atr(alpaca_data_client: StockHistoricalDataClient, symbol_val: str, now_dt_et: datetime, config: dict) -> float | None:
    atr_period = config.get("ATR_PERIOD", 14)
    lookback_minutes = config.get("ATR_BARS_LOOKBACK_MINUTES", 75)
    end_dt_utc = now_dt_et.astimezone(dt_timezone.utc)
    start_dt_utc = end_dt_utc - timedelta(minutes=lookback_minutes)
    try:
        request_limit = atr_period * 3 if lookback_minutes >= atr_period * 3 else lookback_minutes
        bars_req = StockBarsRequest(
            symbol_or_symbols=[symbol_val], timeframe=TimeFrame.Minute,
            start=start_dt_utc, end=end_dt_utc, limit=request_limit, feed="sip"
        )
        bars_data = alpaca_data_client.get_stock_bars(bars_req)
        if symbol_val not in bars_data.data or not bars_data.data[symbol_val]:
            logger.warning(f"ATR_ENTRY: No 1-min bars for {symbol_val} in ATR lookback window.")
            return None
        df = pd.DataFrame([b.__dict__ for b in bars_data.data[symbol_val]])
        if len(df) < 2:
            logger.warning(f"ATR_ENTRY: Not enough bars for {symbol_val} ({len(df)}) for ATR, need 2.")
            return None
        df['high_low'] = df['high'] - df['low']
        df['high_prev_close'] = np.abs(df['high'] - df['close'].shift(1))
        df['low_prev_close'] = np.abs(df['low'] - df['close'].shift(1))
        df['true_range'] = df[['high_low', 'high_prev_close', 'low_prev_close']].max(axis=1)
        df.dropna(subset=['true_range'], inplace=True)
        if len(df) < atr_period:
            logger.warning(f"ATR_ENTRY: Not enough True Range bars for {symbol_val} ({len(df)}) for {atr_period}-period ATR.")
            return None
        atr_val = df['true_range'].rolling(window=atr_period).mean().iloc[-1]
        logger.debug(f"ATR_ENTRY: Calculated ATR for {symbol_val} over {atr_period} periods: {atr_val:.4f} using {len(df)} TRs.")
        return float(atr_val) if pd.notna(atr_val) and atr_val > 0.0001 else None
    except Exception as e:
        logger.error(f"ATR_ENTRY: Error calculating ATR for {symbol_val}: {e}", exc_info=True)
        return None

def _log_trade_decision_json(scan_row_data, config, decision_ts_utc, status, order_detail=None, submit_ts_utc=None, error_message=None):
    log_entry = {
        "symbol": scan_row_data.get('symbol'), "decision_ts_utc": decision_ts_utc.isoformat(),
        "intended_side": scan_row_data.get('intended_side'), "status": status,
        "risk_per_trade": config.get("RISK_PER_TRADE"),
        "atr_1min_at_decision": order_detail.get("atr_at_order") if order_detail else scan_row_data.get("atr_1min_calc", scan_row_data.get("atr_at_entry_sizing")),
        "planned_shares": order_detail.get("qty") if order_detail else None,
        "planned_px": order_detail.get("entry_price", order_detail.get("entry_limit_price")) if order_detail else None,
        "submit_ts_utc": submit_ts_utc.isoformat() if submit_ts_utc else None,
        "order_id": order_detail.get("order_id") if order_detail else None,
        "client_order_id": order_detail.get("client_order_id") if order_detail else None,
        "stop_loss_details": order_detail.get("stop_loss_details") if order_detail else None,
        "pct_change_at_decision": scan_row_data.get('pct_change'), "sentiment_at_decision": scan_row_data.get('final_sentiment'),
        "composite_score_at_decision": scan_row_data.get('composite_score'), "vwap_at_decision": scan_row_data.get('vwap'),
        "pre_mkt_low_at_decision": scan_row_data.get('pre_mkt_low'), "pre_mkt_high_at_decision": scan_row_data.get('pre_mkt_high'),
        "error_message": error_message if error_message else None,
    }
    try:
        with open(config["LOG_JSON_PER_TRADE_FILE"], 'a') as f:
            import json
            f.write(json.dumps(log_entry) + "\n")
    except Exception as e: logger.error(f"Failed to write JSON trade log for {scan_row_data.get('symbol')}: {e}")

# --- Main Scanner Functions ---
def run_premarket_scan_stages_A_B_C(
    scanner_state: PremarketScannerState,
    now_dt_et: datetime,
    alpaca_trading_client: TradingClient,
    alpaca_data_client: StockHistoricalDataClient,
    finnhub_client: finnhub.Client = None
) -> pd.DataFrame:
    config = scanner_state.config
    current_date_et_val = now_dt_et.date()
    if scanner_state.daily_data_initialized_date_et != current_date_et_val:
        logger.info(f"Scanner performing daily initialization for {current_date_et_val}...")
        scanner_state.reset_daily_state_values(current_date_et_val)
        scanner_state.tradable_assets_map = _get_asset_map(alpaca_trading_client, config)
        if not scanner_state.tradable_assets_map:
            logger.error("No tradable assets found. Scan cannot proceed today.")
            return pd.DataFrame()
        all_symbols_for_prev_close = list(set(list(scanner_state.tradable_assets_map.keys()) + scanner_state.sp500_symbols))
        closes, _ = _fetch_previous_day_closes_from_alpaca( alpaca_data_client, alpaca_trading_client, all_symbols_for_prev_close, current_date_et_val, now_dt_et ) # Pass now_dt_et
        
        scanner_state.previous_day_closes = closes 
        if not scanner_state.previous_day_closes: 
            logger.critical(f"CRITICAL: Previous day's closes ({current_date_et_val}) could not be fetched. Scan cannot proceed effectively without this essential data.")
            scanner_state.current_shortlist = pd.DataFrame() 
            return pd.DataFrame() 

    logger.info(f"Running Stages A-C scan cycle at {now_dt_et.strftime('%H:%M:%S ET')}")
    # ... (rest of run_premarket_scan_stages_A_B_C is identical to the previous version with this fix incorporated)
    all_tradable_symbols = list(scanner_state.tradable_assets_map.keys())
    if not all_tradable_symbols:
        logger.warning("No tradable symbols in map for current scan cycle.")
        return scanner_state.current_shortlist

    current_market_snapshot = _fetch_live_market_data_batch(alpaca_data_client, all_tradable_symbols, now_dt_et)
    processed_data_for_ranking = []
    for sym, live_data in current_market_snapshot.items():
        entry = {"symbol": sym, **live_data}
        prev_close_info = scanner_state.previous_day_closes.get(sym)
        if prev_close_info and live_data.get('current_price') and prev_close_info.get('close_price', 0) > 0:
            entry["pct_change"] = ((live_data['current_price'] - prev_close_info['close_price']) / prev_close_info['close_price']) * 100
            entry["prev_close_px"] = prev_close_info['close_price']
        else:
            entry["pct_change"] = np.nan # Will be filled with 0 later if needed for calculations
            entry["prev_close_px"] = prev_close_info.get('close_price') if prev_close_info else np.nan
            if not prev_close_info: logger.debug(f"No prev_close_info for {sym} during %change calc in Stage A.")
            elif not live_data.get('current_price'): logger.debug(f"No current_price for {sym} during %change calc in Stage A.")

        processed_data_for_ranking.append(entry)

    if not processed_data_for_ranking:
        logger.warning("No symbols with processable live data. Skipping cycle.")
        return scanner_state.current_shortlist
    raw_df = pd.DataFrame(processed_data_for_ranking)
    
    # Ensure current_price exists before attempting to use it for pct_change or other filters
    raw_df.dropna(subset=['current_price'], inplace=True)
    if raw_df.empty:
        logger.warning("No symbols remaining after dropping those without current_price.")
        return scanner_state.current_shortlist

    # Fill NaN pct_change only after ensuring rows with prev_close data had a chance to calculate it
    # If prev_close was missing, pct_change remains NaN, then filled to 0.
    raw_df['pct_change'] = raw_df['pct_change'].fillna(0.0)

    top_n = config["TOP_N_PERCENT_CHANGE_CANDIDATES"]
    # Ensure pct_change is not NaN for nlargest/nsmallest to avoid errors
    raw_df_for_ranking = raw_df.dropna(subset=['pct_change']) # Should be redundant if filled with 0, but safe
    
    gainers = raw_df_for_ranking.nlargest(top_n, 'pct_change')
    losers = raw_df_for_ranking.nsmallest(top_n, 'pct_change')
    scanner_state.current_top_200_raw = pd.concat([gainers, losers]).drop_duplicates(subset=['symbol']).reset_index(drop=True)
    
    if scanner_state.current_top_200_raw.empty:
        logger.info("No candidates after initial % change ranking (top N).")
        scanner_state.current_shortlist = pd.DataFrame(); return scanner_state.current_shortlist
    logger.info(f"Identified {len(scanner_state.current_top_200_raw)} unique top/bottom candidates based on pct_change.")

    symbols_for_deep_dive = scanner_state.current_top_200_raw['symbol'].tolist()
    catalyst_sentiment_map = _fetch_catalysts_and_calc_sentiment(scanner_state, finnhub_client, symbols_for_deep_dive, now_dt_et)
    sentiment_df = pd.DataFrame.from_dict(catalyst_sentiment_map, orient='index').reset_index().rename(columns={'index': 'symbol'})
    df_enriched = pd.merge(scanner_state.current_top_200_raw, sentiment_df, on='symbol', how='left')
    for col in ['vader_score', 'finbert_score', 'earnings_surprise_score', 'social_score', 'final_sentiment']:
        if col in df_enriched.columns: df_enriched[col] = df_enriched[col].fillna(0.0)
        else: df_enriched[col] = 0.0

    df_filtered = df_enriched.copy()
    cols_to_check_for_nan_stage_b = ['bid', 'ask', 'volume', 'current_price', 'pct_change', 'final_sentiment']
    for col in cols_to_check_for_nan_stage_b:
        if col not in df_filtered.columns: df_filtered[col] = np.nan
    df_filtered.dropna(subset=cols_to_check_for_nan_stage_b, inplace=True)
    if df_filtered.empty:
        logger.info("No symbols after initial NaN drop in Stage B.")
        scanner_state.current_shortlist = pd.DataFrame(); return scanner_state.current_shortlist

    df_filtered['notional_volume'] = df_filtered['volume'] * df_filtered['current_price']
    liquidity_ok = (
        (df_filtered['volume'] >= config["MIN_PRE_MARKET_VOLUME"]) |
        (df_filtered['notional_volume'] >= config["MIN_PRE_MARKET_NOTIONAL"])
    )
    df_filtered = df_filtered[liquidity_ok].copy()
    if df_filtered.empty: logger.info("No symbols after liquidity filter."); scanner_state.current_shortlist = pd.DataFrame(); return scanner_state.current_shortlist
    logger.info(f"{len(df_filtered)} symbols remaining after liquidity filter.")

    df_filtered['spread_val'] = df_filtered['ask'] - df_filtered['bid']
    df_filtered['spread_pct'] = df_filtered['spread_val'] / df_filtered['ask'].replace(0, np.nan)
    spread_cond = (
        ((df_filtered['current_price'] >= config["LOW_PRICE_THRESHOLD_FOR_SPREAD"]) & (df_filtered['spread_pct'].fillna(float('inf')) <= config["MAX_SPREAD_PERCENTAGE"])) |
        ((df_filtered['current_price'] < config["LOW_PRICE_THRESHOLD_FOR_SPREAD"]) & (df_filtered['spread_val'].fillna(float('inf')) <= config["MAX_SPREAD_ABSOLUTE_LOW_PRICE"]))
    )
    df_filtered = df_filtered[spread_cond & (df_filtered['spread_val'] > 0.00001)].copy()
    if df_filtered.empty: logger.info("No symbols after spread filter."); scanner_state.current_shortlist = pd.DataFrame(); return scanner_state.current_shortlist
    logger.info(f"{len(df_filtered)} symbols remaining after spread filter.")

    if config.get("EXCLUDE_INVERSE_ETF", False):
        symbols_to_drop_inverse = []
        inverse_keywords_lower = [kw.lower() for kw in config.get("INVERSE_KEYWORDS", [])]
        temp_df_for_iter = df_filtered.copy() 
        for index, row in temp_df_for_iter.iterrows(): 
            asset_obj = scanner_state.tradable_assets_map.get(row['symbol'])
            if asset_obj and asset_obj.name:
                asset_name_lower = asset_obj.name.lower()
                if any(kw in asset_name_lower for kw in inverse_keywords_lower):
                    logger.info(f"Stage B: Skipping potential inverse ETF {row['symbol']} (Name: {asset_obj.name})")
                    symbols_to_drop_inverse.append(row['symbol']) 
        if symbols_to_drop_inverse:
            df_filtered = df_filtered[~df_filtered['symbol'].isin(symbols_to_drop_inverse)].copy() 
            if df_filtered.empty: logger.info("No symbols after inverse ETF filter."); scanner_state.current_shortlist = pd.DataFrame(); return scanner_state.current_shortlist
            logger.info(f"{len(df_filtered)} symbols remaining after inverse ETF filter.")

    scanner_state.current_filtered_candidates = df_filtered
    logger.info(f"{len(scanner_state.current_filtered_candidates)} symbols remaining after all Stage B filters.")

    market_breadth = _calculate_market_breadth(scanner_state, alpaca_data_client, now_dt_et)
    N_long = round(3 + 5 * market_breadth); N_short = round(3 + 5 * (1 - market_breadth))
    logger.info(f"Calculated N_long: {N_long}, N_short: {N_short} (Breadth: {market_breadth:.2f})")
    df_to_score = scanner_state.current_filtered_candidates.copy()
    if len(df_to_score) > 1:
        for col_name in ['pct_change', 'volume']:
            mean_val = df_to_score[col_name].mean(); std_val = df_to_score[col_name].std()
            if pd.notna(std_val) and std_val != 0: df_to_score[f'z_{col_name}'] = (df_to_score[col_name] - mean_val) / std_val
            else: df_to_score[f'z_{col_name}'] = 0.0
    else: df_to_score['z_pct_change'] = 0.0; df_to_score['z_volume'] = 0.0
    df_to_score['composite_score'] = ( df_to_score.get('z_pct_change', 0.0) + 0.5 * df_to_score.get('z_volume', 0.0) + df_to_score.get('final_sentiment', 0.0) )

    selected_longs_list = []
    df_sorted_for_longs = df_to_score.sort_values(by='composite_score', ascending=False)
    for _, row in df_sorted_for_longs.iterrows():
        if len(selected_longs_list) < N_long:
            if row['composite_score'] > 0 and row['pct_change'] > 0: 
                row_dict = row.to_dict(); row_dict['intended_side'] = 'long'; selected_longs_list.append(row_dict)
        else: break

    selected_shorts_list = []
    df_potential_shorts = df_to_score[df_to_score['pct_change'] < 0].sort_values(by='composite_score', ascending=True)
    for _, row in df_potential_shorts.iterrows():
        if len(selected_shorts_list) < N_short:
            asset_info = scanner_state.tradable_assets_map.get(row['symbol']);
            is_otc = asset_info and asset_info.exchange == AssetExchange.OTC
            shortable_check_passed = True
            if not (asset_info and asset_info.shortable and asset_info.easy_to_borrow):
                logger.debug(f"Skipping short {row['symbol']}: Not marked as shortable/easy_to_borrow by Alpaca.")
                shortable_check_passed = False
            if is_otc and shortable_check_passed: 
                if row.get('spread_pct', float('inf')) > config["OTC_MAX_SPREAD_PERCENTAGE_FOR_SHORT"]:
                    logger.debug(f"Skipping OTC short {row['symbol']}: Spread {row.get('spread_pct', 0):.2%} > threshold.")
                    shortable_check_passed = False
            if shortable_check_passed and row['final_sentiment'] < -0.05 and pd.notna(row.get('vwap')) and pd.notna(row.get('pre_mkt_low')) and row['vwap'] < row['pre_mkt_low']:
                row_dict = row.to_dict(); row_dict['intended_side'] = 'short'; selected_shorts_list.append(row_dict)
        else: break

    final_shortlist_dfs = []
    if selected_longs_list: final_shortlist_dfs.append(pd.DataFrame(selected_longs_list))
    if selected_shorts_list: final_shortlist_dfs.append(pd.DataFrame(selected_shorts_list))
    if final_shortlist_dfs: scanner_state.current_shortlist = pd.concat(final_shortlist_dfs).drop_duplicates(subset=['symbol']).reset_index(drop=True)
    else: scanner_state.current_shortlist = pd.DataFrame()
    logger.info(f"Updated Shortlist (Stages A-C): {len(scanner_state.current_shortlist)} symbols. Longs: {len(selected_longs_list)}, Shorts: {len(selected_shorts_list)}")
    if not scanner_state.current_shortlist.empty: logger.debug(f"Shortlist sample:\n{scanner_state.current_shortlist[['symbol', 'intended_side', 'composite_score', 'pct_change']].head(3)}")
    scanner_state.last_stage_a_scan_time_et = now_dt_et
    return scanner_state.current_shortlist


def place_entry_orders_stages_D_E(
    shortlist_from_scan: pd.DataFrame,
    scanner_state: PremarketScannerState,
    stage_d_start_time_et: datetime,
    alpaca_trading_client: TradingClient,
    alpaca_data_client: StockHistoricalDataClient,
    finnhub_client: finnhub.Client = None
) -> list:
    # (Identical to previous version)
    config = scanner_state.config; pending_orders_for_sltp_setup = []
    if shortlist_from_scan.empty:
        logger.info("Initial shortlist for Stage D/E is empty.")
        return pending_orders_for_sltp_setup

    final_candidates_df = shortlist_from_scan.copy()
    target_order_placement_time_et = stage_d_start_time_et.replace(hour=9, minute=40, second=0, microsecond=0)
    if stage_d_start_time_et >= target_order_placement_time_et:
         target_order_placement_time_et = stage_d_start_time_et + timedelta(seconds=config["STAGE_D_RECONFIRM_INTERVAL_SECONDS"] * 2)


    logger.info(f"--- Starting Stage D Reconfirmation (triggered at {stage_d_start_time_et.strftime('%H:%M:%S ET')}, orders target {target_order_placement_time_et.strftime('%H:%M:%S ET')}) ---")

    while datetime.now(ET) < target_order_placement_time_et:
        loop_start_time_monotonic = time.monotonic(); current_reconfirm_time_et = datetime.now(ET)
        logger.debug(f"Stage D reconfirm cycle at {current_reconfirm_time_et.strftime('%H:%M:%S')}")

        if final_candidates_df.empty: logger.info("Stage D: Candidate list empty during reconfirmation."); break
        symbols_to_reconfirm = final_candidates_df['symbol'].tolist()
        live_reconfirm_data = _fetch_live_market_data_batch(alpaca_data_client, symbols_to_reconfirm, current_reconfirm_time_et)
        indices_to_drop = []

        for index, row in final_candidates_df.iterrows():
            sym = row['symbol']; live_data = live_reconfirm_data.get(sym, {})
            current_price = live_data.get('current_price', row.get('current_price')); bid = live_data.get('bid', row.get('bid'))
            ask = live_data.get('ask', row.get('ask')); volume = live_data.get('volume', row.get('volume'))
            if not all(pd.notna(val) and (val > 0 if isinstance(val, (int, float)) else True) for val in [current_price, bid, ask, volume]):
                logger.warning(f"[D:{sym}] Missing/invalid live data (price/bid/ask/vol). Drop."); indices_to_drop.append(index); continue
            final_candidates_df.loc[index, 'current_price'] = current_price; final_candidates_df.loc[index, 'bid'] = bid
            final_candidates_df.loc[index, 'ask'] = ask; final_candidates_df.loc[index, 'volume'] = volume
            notional_vol = volume * current_price
            if not ((volume >= config["MIN_PRE_MARKET_VOLUME"]) or (notional_vol >= config["MIN_PRE_MARKET_NOTIONAL"])):
                logger.info(f"[D:{sym}] Failed liquidity (Vol: {volume}, Notional: {notional_vol:.0f}). Drop."); indices_to_drop.append(index); continue
            spread_val = ask - bid; spread_pct = (spread_val / ask) if ask > 0 else float('inf')
            spread_ok = ((current_price >= config["LOW_PRICE_THRESHOLD_FOR_SPREAD"] and spread_pct <= config["MAX_SPREAD_PERCENTAGE"]) or \
                         (current_price < config["LOW_PRICE_THRESHOLD_FOR_SPREAD"] and spread_val <= config["MAX_SPREAD_ABSOLUTE_LOW_PRICE"])) and \
                        spread_val > 0.00001
            if not spread_ok: logger.info(f"[D:{sym}] Failed spread (Spread: {spread_val:.2f}, Pct: {spread_pct:.3%}, Price: {current_price:.2f}). Drop."); indices_to_drop.append(index); continue
            prev_close_px = row.get('prev_close_px'); pre_mkt_high = row.get('pre_mkt_high'); pre_mkt_low = row.get('pre_mkt_low')
            if pd.notna(prev_close_px) and pd.notna(pre_mkt_high) and pd.notna(pre_mkt_low) and prev_close_px > 0:
                retracement_frac = config["RETRACEMENT_THRESHOLD_FRACTION"]
                if row['intended_side'] == 'long':
                    original_pm_move = pre_mkt_high - prev_close_px
                    if original_pm_move > 0.001:
                        current_retracement_from_high = pre_mkt_high - current_price
                        if (current_retracement_from_high / original_pm_move) > retracement_frac:
                            logger.info(f"[D:{sym}] Long retraced >{retracement_frac:.0%} from PM high. Retrace: {current_retracement_from_high:.2f}, Orig Move: {original_pm_move:.2f}. Drop."); indices_to_drop.append(index); continue
                elif row['intended_side'] == 'short':
                    original_pm_move = prev_close_px - pre_mkt_low
                    if original_pm_move > 0.001:
                        current_retracement_from_low = current_price - pre_mkt_low
                        if (current_retracement_from_low / original_pm_move) > retracement_frac:
                            logger.info(f"[D:{sym}] Short retraced >{retracement_frac:.0%} from PM low. Retrace: {current_retracement_from_low:.2f}, Orig Move: {original_pm_move:.2f}. Drop."); indices_to_drop.append(index); continue
        if indices_to_drop:
            final_candidates_df.drop(list(set(indices_to_drop)), inplace=True)
            final_candidates_df.reset_index(drop=True, inplace=True)
            logger.info(f"[D] Dropped {len(list(set(indices_to_drop)))} symbols. Remaining: {len(final_candidates_df)}");
        if final_candidates_df.empty: break
        elapsed_seconds = time.monotonic() - loop_start_time_monotonic
        sleep_duration = config["STAGE_D_RECONFIRM_INTERVAL_SECONDS"] - elapsed_seconds
        if sleep_duration > 0: time.sleep(sleep_duration)
        if datetime.now(ET) >= target_order_placement_time_et: break

    current_time_for_placement = datetime.now(ET)
    if current_time_for_placement < target_order_placement_time_et:
        wait_seconds = (target_order_placement_time_et - current_time_for_placement).total_seconds()
        if wait_seconds > 0 :
            logger.info(f"Stage D finished reconfirm loop. Waiting {wait_seconds:.1f}s for exact order placement time {target_order_placement_time_et.strftime('%H:%M:%S ET')}.")
            time.sleep(max(0, wait_seconds))

    logger.info(f"--- Stage E: Order Placement at {datetime.now(ET).strftime('%H:%M:%S ET')} ---")
    if final_candidates_df.empty: logger.info("Final candidate list empty after Stage D. No orders placed."); return pending_orders_for_sltp_setup
    try:
        market_clock = alpaca_trading_client.get_clock()
        if not market_clock.is_open:
            logger.warning(f"Market NOT OPEN at targeted order placement time ({datetime.now(ET).strftime('%H:%M:%S ET')}). Market status: {market_clock.is_open}, Next Open: {market_clock.next_open}")
        else:
            logger.info("Market is OPEN. Proceeding with order placement.")
    except APIError as e:
        logger.error(f"APIError getting market clock status: {e}. Assuming market state is uncertain.")

    for _, row in final_candidates_df.iterrows():
        decision_ts = datetime.now(dt_timezone.utc); sym = row['symbol']
        intended_side_str = row['intended_side']
        side = OrderSide.BUY if intended_side_str == 'long' else OrderSide.SELL
        atr_for_sizing = _get_1min_atr(alpaca_data_client, sym, datetime.now(ET), config)
        if atr_for_sizing is None or atr_for_sizing <= 0.0001:
            logger.warning(f"[E:{sym}] Invalid ATR for sizing ({atr_for_sizing}). Skip order.");
            _log_trade_decision_json(row, config, decision_ts, "skipped_invalid_atr_sizing_stage_e", None, None); continue
        shares = np.floor(config["RISK_PER_TRADE"] / atr_for_sizing)
        if shares <= 0:
            logger.warning(f"[E:{sym}] Zero shares calculated (ATR: {atr_for_sizing}, Risk: {config['RISK_PER_TRADE']}). Skip order.");
            _log_trade_decision_json(row, config, decision_ts, "skipped_zero_shares_stage_e", {"atr_at_order": atr_for_sizing}, None); continue
        fresh_market_data = _fetch_live_market_data_batch(alpaca_data_client, [sym], datetime.now(ET))
        last_ask = fresh_market_data.get(sym, {}).get('ask', row['ask'])
        last_bid = fresh_market_data.get(sym, {}).get('bid', row['bid'])
        if not (last_ask and last_bid and last_ask > 0 and last_bid > 0):
            logger.warning(f"[E:{sym}] Missing fresh bid/ask for limit price. Skip order.");
            _log_trade_decision_json(row, config, decision_ts, "skipped_missing_fresh_quote_stage_e", {"atr_at_order": atr_for_sizing}, None); continue
        current_spread = last_ask - last_bid; limit_price = 0.0
        pct_comp = config["ADAPTIVE_LIMIT_PRICE_PCT_COMPONENT"]; spread_comp_frac = config["ADAPTIVE_LIMIT_PRICE_SPREAD_COMPONENT_FRACTION"]
        if side == OrderSide.BUY:
            price_adj = max(pct_comp * last_ask, spread_comp_frac * current_spread)
            limit_price = round(last_ask + price_adj, 2)
        else:
            price_adj = max(pct_comp * last_bid, spread_comp_frac * current_spread)
            limit_price = round(last_bid - price_adj, 2)
            limit_price = max(0.01, limit_price)
        if limit_price <= 0:
            logger.warning(f"[E:{sym}] Invalid limit price {limit_price:.2f}. Skip order.");
            _log_trade_decision_json(row, config, decision_ts, "skipped_invalid_limit_price_e", {"atr_at_order": atr_for_sizing, "last_bid": last_bid, "last_ask": last_ask}, None); continue
        order_data = LimitOrderRequest(symbol=sym, qty=shares, side=side, time_in_force=TimeInForce.DAY, limit_price=limit_price, extended_hours=False)
        submit_ts_utc, order_id_str, client_order_id_str = None, None, None
        order_detail_for_log = {"atr_at_order": atr_for_sizing, "qty": shares, "entry_price": limit_price}
        try:
            submit_ts_utc = datetime.now(dt_timezone.utc)
            trade_order = alpaca_trading_client.submit_order(order_data=order_data)
            order_id_str = str(trade_order.id); client_order_id_str = str(trade_order.client_order_id)
            logger.info(f"[E:{sym}] SUBMITTED ENTRY {side.value} {shares} @ LMT ${limit_price:.2f}. ID:{order_id_str}")
            pending_orders_for_sltp_setup.append({
                "symbol": sym, "side": side.value.lower(), "qty": float(shares),
                "entry_limit_price": float(limit_price), "order_id": order_id_str,
                "client_order_id": client_order_id_str, "submission_time_utc": submit_ts_utc.isoformat(),
                "atr_at_entry_sizing": float(atr_for_sizing),
                "initial_order_status": trade_order.status.value if trade_order.status else "unknown"
            })
            order_detail_for_log.update({"order_id": order_id_str, "client_order_id": client_order_id_str})
            _log_trade_decision_json(row, config, decision_ts, "submitted_entry", order_detail_for_log, submit_ts_utc)
        except APIError as e:
            logger.error(f"[E:{sym}] APIError submitting entry: {e}");
            _log_trade_decision_json(row, config, decision_ts, f"submit_entry_api_error_{e.status_code}", order_detail_for_log, submit_ts_utc, str(e.message if hasattr(e, 'message') else e))
        except Exception as e:
            logger.error(f"[E:{sym}] Generic error submitting entry: {e}", exc_info=True);
            _log_trade_decision_json(row, config, decision_ts, "submit_entry_generic_error", order_detail_for_log, submit_ts_utc, str(e))
    return pending_orders_for_sltp_setup


# --- Main Execution Block ---
if __name__ == "__main__":
    print(f"--- Starting Genie_stocks_Top3_v3.2.py (PID: {os.getpid()}) ---")
    if not logger.handlers:
        log_formatter_main = logging.Formatter('%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s')
        console_handler_main = logging.StreamHandler(); console_handler_main.setFormatter(log_formatter_main)
        logger.addHandler(console_handler_main); logger.setLevel(logging.INFO)

    logger.info("Script execution started in __main__.")
    config = DEFAULT_CONFIG.copy()
    trading_client = None; data_client = None

    if not config["ALPACA_API_KEY"] or not config["ALPACA_SECRET_KEY"]:
        logger.critical("Alpaca API Key/Secret not found. Exiting."); exit(1)
    try:
        trading_client = TradingClient(config["ALPACA_API_KEY"], config["ALPACA_SECRET_KEY"], paper=config["ALPACA_PAPER_TRADING"])
        data_client = StockHistoricalDataClient(config["ALPACA_API_KEY"], config["ALPACA_SECRET_KEY"])
        account = trading_client.get_account()
        logger.info(f"Alpaca connection successful. Account: {account.account_number}, Paper: {config['ALPACA_PAPER_TRADING']}, Buying Power: {account.buying_power}")
    except Exception as e: logger.critical(f"Failed to initialize Alpaca clients: {e}", exc_info=True); exit(1)

    finnhub_client_instance = None
    if config["FINNHUB_API_KEY"]:
        try: finnhub_client_instance = finnhub.Client(api_key=config["FINNHUB_API_KEY"]); logger.info("Finnhub client initialized.")
        except Exception as e: logger.warning(f"Failed to initialize Finnhub client: {e}")
    else: logger.info("Finnhub API key not provided. Proceeding without Finnhub data.")

    scanner_state_obj = PremarketScannerState(config_dict=config)
    orders_placed_for_the_day = False
    active_entry_orders_being_managed = []

    SCAN_CYCLE_START_ET_TIME = datetime.strptime("04:00", "%H:%M").time()
    SCAN_CYCLE_END_ET_TIME = datetime.strptime("09:38", "%H:%M").time()
    STAGE_D_TRIGGER_ET_TIME = datetime.strptime("09:35", "%H:%M").time()
    ORDER_CANCELLATION_ET_TIME_OBJ = datetime.strptime(config["ORDER_CANCELLATION_TIME_ET_STR"], "%H:%M").time()
    END_OF_TRADING_DAY_ET_TIME = datetime.strptime("16:00", "%H:%M").time()

    logger.info(f"Operational Windows ET: Scan Cycle [{SCAN_CYCLE_START_ET_TIME}-{SCAN_CYCLE_END_ET_TIME}], Stage D Trigger [{STAGE_D_TRIGGER_ET_TIME}], Cancel Unfilled Entry [{ORDER_CANCELLATION_ET_TIME_OBJ}]")

    try:
        while True:
            now_utc_main = datetime.now(dt_timezone.utc); now_et_main = now_utc_main.astimezone(ET)
            current_et_time_obj_main = now_et_main.time()
            current_et_date_obj_main = now_et_main.date()

            logger.debug(f"Main loop check. ET: {now_et_main.strftime('%Y-%m-%d %H:%M:%S')}. Orders placed: {orders_placed_for_the_day}. Active entries: {len(active_entry_orders_being_managed)}. Positions to trail: {len(scanner_state_obj.positions_to_trail)}")

            if scanner_state_obj.daily_data_initialized_date_et != current_et_date_obj_main:
                if current_et_time_obj_main < SCAN_CYCLE_START_ET_TIME or current_et_time_obj_main > END_OF_TRADING_DAY_ET_TIME :
                    logger.info(f"Performing daily reset for {current_et_date_obj_main}...")
                    orders_placed_for_the_day = False; active_entry_orders_being_managed = []
                    scanner_state_obj.reset_daily_state_values(current_et_date_obj_main)
                    logger.info("Daily reset complete.")

            if SCAN_CYCLE_START_ET_TIME <= current_et_time_obj_main < SCAN_CYCLE_END_ET_TIME :
                if not orders_placed_for_the_day:
                    if scanner_state_obj.last_stage_a_scan_time_et is None or \
                       (now_et_main - scanner_state_obj.last_stage_a_scan_time_et) >= timedelta(minutes=config["STAGE_A_SCAN_INTERVAL_MINUTES"]):
                        logger.info(f"Main loop: Triggering run_premarket_scan_stages_A_B_C at {now_et_main.strftime('%H:%M:%S ET')}")
                        try:
                            run_premarket_scan_stages_A_B_C( scanner_state_obj, now_et_main, trading_client, data_client, finnhub_client_instance )
                        except Exception as e_scan: logger.error(f"Error in run_premarket_scan_stages_A_B_C: {e_scan}", exc_info=True)
                    else: logger.debug(f"Main loop: Too soon for next A-C scan.")
                else: logger.debug("Orders already attempted/placed for the day, skipping A-C scan.")

            if not orders_placed_for_the_day and \
               current_et_time_obj_main >= STAGE_D_TRIGGER_ET_TIME and \
               current_et_time_obj_main < ORDER_CANCELLATION_ET_TIME_OBJ:
                if scanner_state_obj.current_shortlist.empty:
                    logger.warning(f"Main loop: Shortlist empty at Stage D trigger time. No entry orders will be placed today.")
                    orders_placed_for_the_day = True
                else:
                    logger.info(f"Main loop: Triggering place_entry_orders_stages_D_E at {now_et_main.strftime('%H:%M:%S ET')}")
                    try:
                        submitted_orders_details = place_entry_orders_stages_D_E(
                            scanner_state_obj.current_shortlist, scanner_state_obj,
                            now_et_main, trading_client, data_client, finnhub_client_instance
                        )
                        if submitted_orders_details:
                            logger.info(f"Main loop: {len(submitted_orders_details)} entry order(s) submitted details captured.");
                            active_entry_orders_being_managed.extend(submitted_orders_details)
                        else: logger.info("Main loop: Stage E completed, no entry orders were submitted.")
                        orders_placed_for_the_day = True
                    except Exception as e_place:
                        logger.error(f"Error in place_entry_orders_stages_D_E: {e_place}", exc_info=True)
                        orders_placed_for_the_day = True

            if active_entry_orders_being_managed:
                logger.debug(f"Main loop: Managing {len(active_entry_orders_being_managed)} active entry orders.")
                orders_still_active_after_entry_management = []
                for order_info in active_entry_orders_being_managed:
                    is_terminal_entry_order = False
                    try:
                        live_alpaca_order = trading_client.get_order_by_id(order_info["order_id"])
                        order_info["current_api_status"] = live_alpaca_order.status.value
                        if live_alpaca_order.status == AlpacaOrderStatus.FILLED:
                            logger.info(f"Entry Order {order_info['order_id']} ({order_info['symbol']}) FILLED.")
                            is_terminal_entry_order = True
                            qty = float(live_alpaca_order.filled_qty)
                            entry_px = float(live_alpaca_order.filled_avg_price)
                            symbol = order_info['symbol']
                            entry_side = order_info['side']
                            current_atr_for_sl = get_atr_for_exits(symbol, data_client, config, now_et_main)
                            if current_atr_for_sl is None or current_atr_for_sl <= 0:
                                fallback_atr = max(0.01, 0.005 * entry_px)
                                logger.warning(f"Could not calculate ATR for SL on {symbol} at fill time. Using fallback ATR: {fallback_atr:.4f}")
                                current_atr_for_sl = fallback_atr
                            sl_atr_mult = config["STOP_LOSS_ATR_MULT"]
                            sl_price = 0.0
                            stop_order_side = OrderSide.SELL
                            if entry_side == 'buy':
                                sl_price = round(entry_px - sl_atr_mult * current_atr_for_sl, 2)
                                stop_order_side = OrderSide.SELL
                            elif entry_side == 'sell':
                                sl_price = round(entry_px + sl_atr_mult * current_atr_for_sl, 2)
                                stop_order_side = OrderSide.BUY
                            sl_price = max(0.01, sl_price)
                            try:
                                stop_loss_order_req = StopOrderRequest(
                                    symbol=symbol, qty=qty, side=stop_order_side,
                                    time_in_force=TimeInForce.GTC, stop_price=sl_price
                                )
                                sl_order_submission = trading_client.submit_order(order_data=stop_loss_order_req)
                                logger.info(f"{symbol}: Initial HARD STOP order submitted: {stop_order_side.value} {qty} @ STP ${sl_price:.2f} (ATR: {current_atr_for_sl:.4f}). SL Order ID: {sl_order_submission.id}")
                                scanner_state_obj.positions_to_trail[symbol] = {
                                    "qty": qty, "entry_px": entry_px,
                                    "high_water_mark": entry_px if entry_side == 'buy' else -float('inf'),
                                    "low_water_mark": entry_px if entry_side == 'sell' else float('inf'),
                                    "atr_at_sl_setup": current_atr_for_sl,
                                    "sl_order_id": sl_order_submission.id,
                                    "sl_price": sl_price, "side": entry_side
                                }
                                scanner_state_obj.cached_stop_prices[symbol] = sl_price
                                _log_trade_decision_json(
                                    {"symbol": symbol, "intended_side": entry_side}, config, datetime.now(dt_timezone.utc),
                                    "stop_loss_set",
                                    {"order_id": order_info["order_id"], "stop_loss_details": {"sl_order_id": sl_order_submission.id, "sl_price": sl_price, "atr": current_atr_for_sl}}
                                )
                            except APIError as e_sl: logger.error(f"APIError submitting initial SL for {symbol}: {e_sl}")
                            except Exception as e_sl_gen: logger.error(f"Generic error submitting initial SL for {symbol}: {e_sl_gen}", exc_info=True)
                        elif live_alpaca_order.status in [AlpacaOrderStatus.CANCELED, AlpacaOrderStatus.EXPIRED, AlpacaOrderStatus.REJECTED, AlpacaOrderStatus.DONE_FOR_DAY]:
                            logger.info(f"Entry Order {order_info['order_id']} ({order_info['symbol']}) is terminal: {live_alpaca_order.status.value}")
                            is_terminal_entry_order = True
                        else:
                            if current_et_time_obj_main >= ORDER_CANCELLATION_ET_TIME_OBJ:
                                logger.info(f"Entry order {order_info['order_id']} ({order_info['symbol']}) past {config['ORDER_CANCELLATION_TIME_ET_STR']} ET. Attempting to cancel.")
                                try:
                                    trading_client.cancel_order_by_id(order_info["order_id"])
                                    logger.info(f"Cancel request sent for entry order {order_info['order_id']}.")
                                except APIError as e_cancel:
                                    if e_cancel.status_code == 404: logger.info(f"Entry order {order_info['order_id']} not found for cancel.")
                                    elif e_cancel.status_code == 422: logger.info(f"Entry order {order_info['order_id']} not cancellable.")
                                    else: logger.error(f"APIError cancelling entry order {order_info['order_id']}: {e_cancel}")
                        if not is_terminal_entry_order:
                            orders_still_active_after_entry_management.append(order_info)
                    except APIError as e_ord_manage:
                        if e_ord_manage.status_code == 404: logger.warning(f"Entry order {order_info.get('order_id')} not found (terminal?).")
                        else: logger.error(f"APIError managing entry order {order_info.get('order_id')}: {e_ord_manage}")
                        orders_still_active_after_entry_management.append(order_info)
                    except Exception as e_ord_manage_gen:
                        logger.error(f"Generic error managing entry order {order_info.get('order_id')}: {e_ord_manage_gen}", exc_info=True)
                        orders_still_active_after_entry_management.append(order_info)
                active_entry_orders_being_managed = orders_still_active_after_entry_management

            if scanner_state_obj.positions_to_trail:
                logger.debug(f"Main loop: Managing trailing stops for {len(scanner_state_obj.positions_to_trail)} positions.")
                symbols_to_remove_from_trailing = []
                for sym, meta in list(scanner_state_obj.positions_to_trail.items()):
                    try:
                        sl_order_status_obj = trading_client.get_order_by_id(meta["sl_order_id"]) 
                        if sl_order_status_obj.status in [AlpacaOrderStatus.FILLED, AlpacaOrderStatus.CANCELED, AlpacaOrderStatus.EXPIRED, AlpacaOrderStatus.REJECTED, AlpacaOrderStatus.DONE_FOR_DAY]:
                            logger.info(f"Position for {sym} (Entry side: {meta['side']}) seems closed or SL order {meta['sl_order_id']} is terminal: {sl_order_status_obj.status.value}. Removing from trailing.")
                            symbols_to_remove_from_trailing.append(sym)
                            _log_trade_decision_json(
                                {"symbol": sym, "intended_side": meta['side']}, config, datetime.now(dt_timezone.utc),
                                f"stop_loss_terminal_{sl_order_status_obj.status.value}",
                                {"stop_loss_details": {"sl_order_id": meta["sl_order_id"], "sl_price": meta["sl_price"], "atr_at_sl_setup": meta["atr_at_sl_setup"], "filled_avg_price": sl_order_status_obj.filled_avg_price}}
                            )
                            continue
                        last_px = get_last_trade_price(sym, data_client)
                        if last_px is None:
                            logger.warning(f"Could not get last trade price for {sym} during trailing. Skipping trail adjustment.")
                            continue
                        current_pos_atr = meta["atr_at_sl_setup"]
                        trail_trigger_profit = config["TRAIL_TRIGGER_ATR"] * current_pos_atr
                        trail_gap = config["TRAIL_ATR_MULT"] * current_pos_atr
                        new_sl_candidate = None
                        if meta['side'] == 'buy':
                            meta["high_water_mark"] = max(meta["high_water_mark"], last_px)
                            unrealized_profit = meta["high_water_mark"] - meta["entry_px"]
                            if unrealized_profit >= trail_trigger_profit:
                                new_sl_candidate = round(meta["high_water_mark"] - trail_gap, 2)
                                if new_sl_candidate > scanner_state_obj.cached_stop_prices.get(sym, -float('inf')) and new_sl_candidate > meta["entry_px"]: # Check if sym exists in cache
                                    try:
                                        trading_client.replace_order(order_id=meta["sl_order_id"], stop_price=new_sl_candidate, time_in_force=TimeInForce.GTC)
                                        scanner_state_obj.cached_stop_prices[sym] = new_sl_candidate
                                        meta["sl_price"] = new_sl_candidate
                                        logger.info(f"{sym} (Long): Trailing SL upgraded to ${new_sl_candidate:.2f} (High: {meta['high_water_mark']:.2f}, ATR: {current_pos_atr:.4f})")
                                    except APIError as e_replace:
                                        logger.error(f"APIError replacing SL for {sym} (long) to {new_sl_candidate:.2f}: {e_replace}")
                                        if e_replace.status_code in [404, 422]: symbols_to_remove_from_trailing.append(sym)
                                    except Exception as e_replace_gen: logger.error(f"Generic error replacing SL for {sym} (long): {e_replace_gen}", exc_info=True)
                        elif meta['side'] == 'sell':
                            meta["low_water_mark"] = min(meta["low_water_mark"], last_px)
                            unrealized_profit = meta["entry_px"] - meta["low_water_mark"]
                            if unrealized_profit >= trail_trigger_profit:
                                new_sl_candidate = round(meta["low_water_mark"] + trail_gap, 2)
                                if new_sl_candidate < scanner_state_obj.cached_stop_prices.get(sym, float('inf')) and new_sl_candidate < meta["entry_px"]: # Check if sym exists
                                    try:
                                        trading_client.replace_order(order_id=meta["sl_order_id"], stop_price=new_sl_candidate, time_in_force=TimeInForce.GTC)
                                        scanner_state_obj.cached_stop_prices[sym] = new_sl_candidate
                                        meta["sl_price"] = new_sl_candidate
                                        logger.info(f"{sym} (Short): Trailing SL upgraded to ${new_sl_candidate:.2f} (Low: {meta['low_water_mark']:.2f}, ATR: {current_pos_atr:.4f})")
                                    except APIError as e_replace:
                                        logger.error(f"APIError replacing SL for {sym} (short) to {new_sl_candidate:.2f}: {e_replace}")
                                        if e_replace.status_code in [404, 422]: symbols_to_remove_from_trailing.append(sym)
                                    except Exception as e_replace_gen: logger.error(f"Generic error replacing SL for {sym} (short): {e_replace_gen}", exc_info=True)
                    except APIError as e_trail_manage:
                        if e_trail_manage.status_code == 404:
                            logger.warning(f"SL order for {sym} (ID: {meta.get('sl_order_id')}) not found. Removing from trailing.")
                            symbols_to_remove_from_trailing.append(sym)
                        else: logger.error(f"APIError managing trailing stop for {sym}: {e_trail_manage}")
                    except Exception as e_trail_gen: logger.error(f"Generic error managing trailing stop for {sym}: {e_trail_gen}", exc_info=True)
                for sym_to_remove in set(symbols_to_remove_from_trailing):
                    if sym_to_remove in scanner_state_obj.positions_to_trail: del scanner_state_obj.positions_to_trail[sym_to_remove]
                    if sym_to_remove in scanner_state_obj.cached_stop_prices: del scanner_state_obj.cached_stop_prices[sym_to_remove]

            sleep_seconds = 30
            logger.debug("Evaluating sleep conditions...")
            if current_et_time_obj_main > END_OF_TRADING_DAY_ET_TIME or current_et_time_obj_main < datetime.strptime("03:55", "%H:%M").time():
                logger.info(f"Condition MET: Outside core trading/scan window. Sleeping longer.")
                next_day_et = current_et_date_obj_main + timedelta(days=1)
                target_wakeup_dt_et = datetime.combine(next_day_et, SCAN_CYCLE_START_ET_TIME, tzinfo=ET) - timedelta(minutes=5)
                if target_wakeup_dt_et < now_et_main: target_wakeup_dt_et = now_et_main + timedelta(hours=1)
                sleep_seconds = (target_wakeup_dt_et - now_et_main).total_seconds()
                if sleep_seconds < 0 : sleep_seconds = 3600
                if now_et_main + timedelta(seconds=sleep_seconds) >= datetime.combine(next_day_et, SCAN_CYCLE_START_ET_TIME, tzinfo=ET) - timedelta(minutes=10) :
                    orders_placed_for_the_day = False
            elif current_et_time_obj_main < SCAN_CYCLE_START_ET_TIME:
                logger.info("Condition MET: Before scan cycle start. Sleeping until scan start.")
                next_event_dt_et = datetime.combine(current_et_date_obj_main, SCAN_CYCLE_START_ET_TIME, tzinfo=ET)
                sleep_seconds = (next_event_dt_et - now_et_main).total_seconds()
            elif active_entry_orders_being_managed or scanner_state_obj.positions_to_trail:
                logger.info(f"Condition MET: Active orders ({len(active_entry_orders_being_managed)}) or positions ({len(scanner_state_obj.positions_to_trail)}). Short sleep.")
                sleep_seconds = 10
            else:
                logger.info("Condition MET: General case. Calculating sleep until next event.")
                seconds_to_next_scan = float('inf')
                if SCAN_CYCLE_START_ET_TIME <= current_et_time_obj_main < SCAN_CYCLE_END_ET_TIME and not orders_placed_for_the_day:
                    next_scan_trigger_dt = (scanner_state_obj.last_stage_a_scan_time_et or (now_et_main - timedelta(minutes=config["STAGE_A_SCAN_INTERVAL_MINUTES"]))) + \
                                          timedelta(minutes=config["STAGE_A_SCAN_INTERVAL_MINUTES"])
                    if next_scan_trigger_dt > now_et_main: seconds_to_next_scan = (next_scan_trigger_dt - now_et_main).total_seconds()
                    else: seconds_to_next_scan = 0
                seconds_to_stage_d = float('inf')
                if not orders_placed_for_the_day and current_et_time_obj_main < STAGE_D_TRIGGER_ET_TIME:
                    next_stage_d_trigger_dt = now_et_main.replace(hour=STAGE_D_TRIGGER_ET_TIME.hour, minute=STAGE_D_TRIGGER_ET_TIME.minute, second=0, microsecond=0)
                    if next_stage_d_trigger_dt > now_et_main: seconds_to_stage_d = (next_stage_d_trigger_dt - now_et_main).total_seconds()
                sleep_seconds = min(seconds_to_next_scan, seconds_to_stage_d, 60)
                logger.debug(f"Calculated sleep: next_scan~{seconds_to_next_scan:.0f}, stage_d~{seconds_to_stage_d:.0f}, result={sleep_seconds:.0f}")
            sleep_seconds = max(5, int(sleep_seconds))
            logger.info(f"Main loop sleeping for {sleep_seconds} seconds.")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt: logger.info("KeyboardInterrupt received. Shutting down...")
    except Exception as e_main: logger.critical(f"Unhandled exception in main loop: {e_main}", exc_info=True)
    finally:
        logger.info("Script cleanup and exit process starting...")
        if trading_client:
            logger.info("Attempting to cancel all open orders on exit...")
            try:
                # Consolidate order cancellation logic
                open_orders_to_cancel = []
                for entry_order_info in active_entry_orders_being_managed:
                    open_orders_to_cancel.append(entry_order_info["order_id"])
                for sym, pos_meta in list(scanner_state_obj.positions_to_trail.items()):
                    open_orders_to_cancel.append(pos_meta["sl_order_id"])
                
                # Remove duplicates just in case, though unlikely here
                open_orders_to_cancel = list(set(open_orders_to_cancel))

                for order_id_to_cancel in open_orders_to_cancel:
                    try:
                        order_status = trading_client.get_order_by_id(order_id_to_cancel)
                        # Check for cancellable states more broadly
                        if order_status.status not in [AlpacaOrderStatus.FILLED, AlpacaOrderStatus.CANCELED, AlpacaOrderStatus.EXPIRED, AlpacaOrderStatus.REJECTED, AlpacaOrderStatus.DONE_FOR_DAY]:
                           trading_client.cancel_order_by_id(order_id_to_cancel)
                           logger.info(f"Cancelled open order {order_id_to_cancel} on exit.")
                    except APIError as e_cancel_order:
                        if e_cancel_order.status_code != 404: # Ignore if not found (already closed/cancelled)
                            logger.error(f"Error cancelling order {order_id_to_cancel} on exit: {e_cancel_order}")
                logger.info("Finished attempting to cancel open orders.")
            except Exception as e_cancel_all: 
                logger.error(f"Error during final order cancellation attempt: {e_cancel_all}", exc_info=True)
        logger.info(f"--- Genie_stocks_Top3_v3.2.py Finished ---")