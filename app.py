import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import random
import warnings
import logging
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
import nest_asyncio
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from tvDatafeed import TvDatafeed, Interval

# CRITICAL: Patch Streamlit's event loop
nest_asyncio.apply()

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# CONFIGURATION
# ================================================================================
@dataclass
class ScaleConfig:
    TV_POOL_SIZE: int = 12
    ASYNC_TIMEOUT: int = 12
    MAX_RETRIES: int = 2          # 2 retries per stock = 3 total attempts
    BATCH_SIZE: int = 12          # Process 12 at a time (1:1 with TV pool)
    PER_STOCK_TIMEOUT: int = 15   # Hard kill after 15s

CONFIG = ScaleConfig()

# ================================================================================
# TV POOL — WITH HEALTH TRACKING
# ================================================================================
class TVConnectionPool:
    def __init__(self, size):
        self.pool = []
        self.health = {}  # index -> bool
        for i in range(size):
            try:
                self.pool.append(TvDatafeed())
                self.health[i] = True
                time.sleep(0.35)
            except Exception:
                break
        self.size = len(self.pool)
        if self.size == 0:
            st.error("❌ Failed to initialize any TV connections")
            st.stop()
    
    def get(self, idx):
        """Rotate to find a healthy connection."""
        for offset in range(self.size):
            i = (idx + offset) % self.size
            if self.health.get(i, False):
                return self.pool[i], i
        # All marked unhealthy — return first anyway (last resort)
        return self.pool[0], 0
    
    def mark_bad(self, idx):
        """Mark a connection as potentially flaky."""
        self.health[idx] = False

# ================================================================================
# ASYNC TV CLIENT — WITH PROPER TIMEOUT
# ================================================================================
class AsyncTVClient:
    """Async HTTP client with timeout that actually works."""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=CONFIG.ASYNC_TIMEOUT, connect=5)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            
    async def get_hist(self, symbol: str, exchange: str = 'NSE', 
                       interval: str = '15', n_bars: int = 500) -> Optional[pd.DataFrame]:
        try:
            url = "https://prodata.tradingview.com/history"
            params = {
                'symbol': f"{exchange}:{symbol}",
                'resolution': interval,
                'from': int((datetime.now() - timedelta(days=10)).timestamp()),
                'to': int(datetime.now().timestamp()),
                'countback': n_bars,
            }
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                
            if not data or 't' not in data or not data['t']:
                return None
                
            df = pd.DataFrame({
                'open': data['o'], 'high': data['h'], 'low': data['l'],
                'close': data['c'], 'volume': data.get('v', [0]*len(data['t'])),
            }, index=pd.to_datetime(data['t'], unit='s'))
            
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            return df
            
        except Exception:
            return None

# ================================================================================
# HYBRID FETCHER — RETRY LOGIC
# ================================================================================
class HybridDataFetcher:
    def __init__(self, tv_pool: TVConnectionPool):
        self.tv_pool = tv_pool
        self.tv_index = 0
        
    async def fetch_with_retry(self, symbol: str, resolution: int, days_back: int = 5) -> pd.DataFrame:
        """
        Try multiple sources with rotation:
        1. Async HTTP
        2. TV instance 1
        3. TV instance 2 (different one)
        """
        n_bars = days_back * 75 if resolution == 5 else days_back * 25
        
        # Attempt 1: Async HTTP
        try:
            async with AsyncTVClient() as client:
                df = await client.get_hist(
                    symbol.replace('.NS', ''), 'NSE', 
                    str(resolution), n_bars
                )
                if df is not None and not df.empty:
                    return df
        except Exception:
            pass
        
        # Attempt 2 & 3: Try 2 different TV instances
        for attempt in range(2):
            tv_inst, tv_idx = self.tv_pool.get(self.tv_index + attempt)
            self.tv_index += 1
            
            try:
                if resolution == 5:
                    interval = Interval.in_5_minute
                else:
                    interval = Interval.in_15_minute
                
                df = tv_inst.get_hist(
                    symbol=symbol.replace('.NS', ''),
                    exchange='NSE',
                    interval=interval,
                    n_bars=n_bars
                )
                
                if df is not None and not df.empty:
                    # Success — format and return
                    df = df.rename(columns={
                        'open': 'open', 'high': 'high', 'low': 'low',
                        'close': 'close', 'volume': 'volume'
                    })
                    df.index = pd.to_datetime(df.index)
                    if df.index.tz is None:
                        df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
                    else:
                        df.index = df.index.tz_convert('Asia/Kolkata')
                    return df
                    
            except Exception:
                self.tv_pool.mark_bad(tv_idx)
                time.sleep(0.3)
                continue
        
        # All attempts failed
        return pd.DataFrame()

# ================================================================================
# STRATEGY LOGIC
# ================================================================================
class Config:
    EMA_FAST = 20
    EMA_SLOW = 50
    FIB_LEVEL_1 = 0.50
    FIB_LEVEL_2 = 0.618
    MIN_IMPULSE_PCT = 0.5
    DEFAULT_TARGET_RR = 2.0

class OpenDriveFibStrategy:
    def __init__(self):
        self.config = Config()

    def calculate_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def check_trend(self, df_slice):
        if len(df_slice) < 2:
            return False, False
        ema20_now = df_slice['ema20'].iloc[-1]
        ema50_now = df_slice['ema50'].iloc[-1]
        ema20_prev = df_slice['ema20'].iloc[-2]
        ema50_prev = df_slice['ema50'].iloc[-2]
        is_up = (ema20_now > ema50_now) and (ema20_prev > ema50_prev)
        is_down = (ema20_now < ema50_now) and (ema20_prev < ema50_prev)
        return is_up, is_down

    def scan_stock(self, df_5min: pd.DataFrame, df_15min: pd.DataFrame, 
                   target_date, tolerance_pct: float, sym_clean: str) -> Dict:
        """
        Returns:
            - signal dict if found
            - None if no setup (valid)
            - {"_error": True, ...} if data issue
        """
        try:
            market_open = pd.Timestamp('09:15').time()
            market_close = pd.Timestamp('15:30').time()

            df_5min_today = df_5min[
                (df_5min.index.date == target_date) & 
                (df_5min.index.time >= market_open) &
                (df_5min.index.time <= market_close)
            ].copy()

            # EMA on full history, then slice
            df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
            df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

            df_15min_today = df_15min[
                (df_15min.index.date == target_date) & 
                (df_15min.index.time >= market_open) &
                (df_15min.index.time <= market_close)
            ].copy()

            if df_5min_today.empty or len(df_15min_today) < 3:
                return None

            first_5m = df_5min_today.iloc[0]
            first5_open = float(first_5m['open'])
            first5_high = float(first_5m['high'])
            first5_low = float(first_5m['low'])

            price = first5_open if first5_open > 0 else 1.0
            tolerance = price * (tolerance_pct / 100)

            is_buy_setup = abs(first5_open - first5_low) <= tolerance
            is_sell_setup = abs(first5_open - first5_high) <= tolerance

            if not is_buy_setup and not is_sell_setup:
                return None  # Valid: no setup

            # State variables
            buy_setup_invalid = False; sell_setup_invalid = False
            buy_impulse_done = False; buy_impulse_bar = -1
            buy_retraced = False; buy_retrace_bar = -1; buy_signal_fired = False
            buy_swing_high = None; buy_fib_50 = None; buy_fib_618 = None

            sell_impulse_done = False; sell_impulse_bar = -1
            sell_retraced = False; sell_retrace_bar = -1; sell_signal_fired = False
            sell_swing_low = None; sell_fib_50 = None; sell_fib_618 = None

            signal = None

            for i, (idx, row) in enumerate(df_15min_today.iterrows()):
                if i == 0:
                    if is_buy_setup and row['low'] < first5_low:
                        buy_setup_invalid = True
                    if is_sell_setup and row['high'] > first5_high:
                        sell_setup_invalid = True
                    continue

                if idx.time() > market_close:
                    break

                # Invalidation
                if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if row['low'] < first5_low:
                        buy_setup_invalid = True
                if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if row['high'] > first5_high:
                        sell_setup_invalid = True

                # Buy impulse
                if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    buy_swing_high = float(row['high']) if buy_swing_high is None else max(buy_swing_high, float(row['high']))
                    threshold = first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100)
                    if buy_swing_high >= threshold:
                        buy_impulse_done = True
                        buy_impulse_bar = i
                        buy_fib_50 = buy_swing_high - self.config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                        buy_fib_618 = buy_swing_high - self.config.FIB_LEVEL_2 * (buy_swing_high - first5_low)

                # Sell impulse
                if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    sell_swing_low = float(row['low']) if sell_swing_low is None else min(sell_swing_low, float(row['low']))
                    threshold = first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100)
                    if sell_swing_low <= threshold:
                        sell_impulse_done = True
                        sell_impulse_bar = i
                        sell_fib_50 = sell_swing_low + self.config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                        sell_fib_618 = sell_swing_low + self.config.FIB_LEVEL_2 * (first5_high - sell_swing_low)

                # Retrace
                if buy_impulse_done and not buy_retraced and i > buy_impulse_bar:
                    if row['low'] <= buy_fib_50:
                        buy_retraced = True; buy_retrace_bar = i
                if sell_impulse_done and not sell_retraced and i > sell_impulse_bar:
                    if row['high'] >= sell_fib_50:
                        sell_retraced = True; sell_retrace_bar = i

                # Buy signal
                if buy_retraced and not buy_signal_fired and i > buy_retrace_bar:
                    if row['close'] > row['open'] and row['close'] > buy_fib_50:
                        is_uptrend, _ = self.check_trend(df_15min_today.iloc[:i+1])
                        if (row['close'] > row['ema20']) and (row['close'] > row['ema50']) and is_uptrend:
                            entry = float(row['close']); sl = float(row['low'])
                            target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR
                            return {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(buy_fib_50, 2),
                                'fib_618': round(buy_fib_618, 2), 'swing_high': round(buy_swing_high, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'UP'
                            }

                # Sell signal
                if sell_retraced and not sell_signal_fired and i > sell_retrace_bar:
                    if row['close'] < row['open'] and row['close'] < sell_fib_50:
                        _, is_downtrend = self.check_trend(df_15min_today.iloc[:i+1])
                        if (row['close'] < row['ema20']) and (row['close'] < row['ema50']) and is_downtrend:
                            entry = float(row['close']); sl = float(row['high'])
                            target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR
                            return {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(sell_fib_50, 2),
                                'fib_618': round(sell_fib_618, 2), 'swing_low': round(sell_swing_low, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN'
                            }

            return None  # Valid: setup but no signal formed

        except Exception as e:
            return {"_error": True, "_reason": str(e), "symbol": sym_clean}

# ================================================================================
# ASYNC SCAN WITH RETRY AND SECOND PASS
# ================================================================================
async def scan_stock_safe(fetcher, strategy, symbol, target_date, tolerance_pct, timeout):
    """
    Wrap scan in asyncio.wait_for to prevent hangs.
    Returns (result, symbol, is_error).
    """
    sym_clean = symbol.replace('.NS', '')
    
    try:
        # Fetch both timeframes
        df_5, df_15 = await asyncio.wait_for(
            asyncio.gather(
                fetcher.fetch_with_retry(symbol, 5),
                fetcher.fetch_with_retry(symbol, 15)
            ),
            timeout=timeout
        )
        
        if df_5.empty or df_15.empty:
            return {"_error": True, "_reason": "empty_after_retry", "symbol": sym_clean}, symbol, True
        
        result = strategy.scan_stock(df_5, df_15, target_date, tolerance_pct, sym_clean)
        if result is None:
            return None, symbol, False  # Valid no-setup
        if isinstance(result, dict) and result.get("_error"):
            return result, symbol, True
        return result, symbol, False  # Signal found
        
    except asyncio.TimeoutError:
        return {"_error": True, "_reason": "timeout", "symbol": sym_clean}, symbol, True
    except Exception as e:
        return {"_error": True, "_reason": str(e), "symbol": sym_clean}, symbol, True

async def async_scan_all(stock_list, scan_date, tolerance_pct, strategy, tv_pool, progress_bar, status_text):
    """
    Two-pass scan:
    Pass 1: Scan all stocks.
    Pass 2: Retry only the ones that failed in Pass 1.
    """
    fetcher = HybridDataFetcher(tv_pool)
    target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date
    
    all_results = []
    failed_stocks = []
    completed = 0
    total = len(stock_list)
    
    # =====================================================================
    # PASS 1: Initial scan of all stocks
    # =====================================================================
    sem = asyncio.Semaphore(CONFIG.BATCH_SIZE)
    
    async def scan_one_pass1(symbol):
        async with sem:
            return await scan_stock_safe(fetcher, strategy, symbol, target_date, tolerance_pct, CONFIG.PER_STOCK_TIMEOUT)
    
    tasks = [scan_one_pass1(sym) for sym in stock_list]
    
    for coro in asyncio.as_completed(tasks):
        result, symbol, is_error = await coro
        completed += 1
        
        if completed % 5 == 0 or completed == total:
            progress_bar.progress(completed / total)
            status_text.text(f"⚡ Pass 1... ({completed}/{total})")
        
        if is_error:
            failed_stocks.append(symbol)
            all_results.append(result)  # Keep error for reporting
        elif result is not None:
            all_results.append(result)
    
    # =====================================================================
    # PASS 2: Retry failed stocks with fresh connections
    # =====================================================================
    if failed_stocks:
        status_text.text(f"🔄 Retrying {len(failed_stocks)} failed stocks...")
        
        # Small delay to let TV connections recover
        await asyncio.sleep(2)
        
        retry_tasks = [scan_one_pass1(sym) for sym in failed_stocks]
        retry_results = []
        
        for coro in asyncio.as_completed(retry_tasks):
            result, symbol, is_error = await coro
            retry_results.append((result, is_error))
            status_text.text(f"🔄 Retrying... ({len(retry_results)}/{len(failed_stocks)})")
        
        # Replace errors with retry results where possible
        final_results = []
        retry_idx = 0
        
        for r in all_results:
            if isinstance(r, dict) and r.get("_error"):
                if retry_idx < len(retry_results):
                    retry_res, retry_err = retry_results[retry_idx]
                    retry_idx += 1
                    if not retry_err and retry_res is not None:
                        final_results.append(retry_res)  # Success on retry!
                    elif not retry_err and retry_res is None:
                        pass  # Valid no-setup, don't add error
                    else:
                        final_results.append(retry_res)  # Still failed
            else:
                final_results.append(r)
        
        all_results = final_results
    
    return all_results

# ================================================================================
# UI SETUP
# ================================================================================
st.set_page_config(page_title="Open Drive Fib Scanner", page_icon="📈", layout="wide")
st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; }
    .sub-header { font-size: 1.1rem; color: #666; text-align: center; margin-bottom: 2rem; }
    .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 10px; color: white; text-align: center; }
    .signal-card { padding: 1rem; border-radius: 8px; margin: 0.5rem 0; border-left: 4px solid; }
    .buy-card { background: linear-gradient(135deg, #1a5f5f 0%, #2d8a8a 100%) !important; color: white; border-left-color: #4CAF50; }
    .sell-card { background: linear-gradient(135deg, #7a1f1f 0%, #a03030 100%) !important; color: white; border-left-color: #f44336; }
    .perf-card { background: #1e1e2e; padding: 0.8rem; border-radius: 6px; color: #a0a0b0; font-size: 0.85rem; }
    .error-card { background: #2d1f1f; padding: 0.6rem; border-radius: 4px; color: #ff6b6b; font-size: 0.8rem; margin: 0.3rem 0; }
</style>
""", unsafe_allow_html=True)

def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown('<div style="font-size:2rem;text-align:center;">🔒 Private Access</div>', unsafe_allow_html=True)
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown('<div style="font-size:2rem;text-align:center;">🔒 Private Access</div>', unsafe_allow_html=True)
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("❌ Incorrect password.")
        return False
    return True

# ================================================================================
# DISPLAY
# ================================================================================
def display_results(all_results, scan_date, perf_stats=None):
    valid_signals = [r for r in all_results if r is not None and not (isinstance(r, dict) and r.get("_error"))]
    errors = [r for r in all_results if isinstance(r, dict) and r.get("_error")]

    if not valid_signals:
        st.warning("⚠️ No signals found.")
        if errors:
            with st.expander(f"📊 Data Errors ({len(errors)} stocks)"):
                for e in errors[:10]:
                    st.markdown(f'<div class="error-card">{e["symbol"]}: {e.get("_reason", "unknown")}</div>', unsafe_allow_html=True)
        return

    df = pd.DataFrame(valid_signals)

    if perf_stats:
        st.markdown(f"""
        <div class="perf-card">
            ⚡ <b>Performance:</b> {perf_stats['stocks_scanned']} stocks in {perf_stats['duration']:.1f}s 
            | {len(valid_signals)} signals | {perf_stats['stocks_per_sec']:.1f} stocks/sec
            | Pass 1 + {perf_stats.get('retried', 0)} retried
        </div>
        """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="metric-card"><h3>{len(df)}</h3><p>Total Signals</p></div>', unsafe_allow_html=True)
    with c2:
        buys = len(df[df['direction'] == 'BUY'])
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);"><h3>{buys}</h3><p>BUY</p></div>', unsafe_allow_html=True)
    with c3:
        sells = len(df[df['direction'] == 'SELL'])
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);"><h3>{sells}</h3><p>SELL</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📋 Fib Retracement Signals")

    for _, row in df.iterrows():
        card = "buy-card" if row['direction'] == 'BUY' else "sell-card"
        swing = f"Swing High: {row.get('swing_high', 'N/A')}" if row['direction'] == 'BUY' else f"Swing Low: {row.get('swing_low', 'N/A')}"

        st.markdown(f"""
        <div class="signal-card {card}">
            <h4>{row['symbol']} — {row['direction']} @ {row['entry_price']}</h4>
            <p><b>Setup:</b> {row['setup_time']} | <b>Signal:</b> {row['signal_time']} | <b>Trend:</b> {row['trend']}</p>
            <p><b>Entry:</b> {row['entry_price']} | <b>SL:</b> {row['stop_loss']} | <b>TGT:</b> {row['target']} | <b>R:R:</b> {row['risk_reward']}</p>
            <p><b>Fib 0.5:</b> {row['fib_50']} | <b>Fib 0.618:</b> {row['fib_618']} | {swing}</p>
            <p><b>EMA20:</b> {row['ema20']} | <b>EMA50:</b> {row['ema50']}</p>
        </div>
        """, unsafe_allow_html=True)

    with st.expander("📊 Export Data"):
        st.dataframe(df, hide_index=True, use_container_width=True)

    if errors:
        with st.expander(f"⚠️ Errors ({len(errors)} stocks failed after all retries)"):
            for e in errors[:20]:
                st.markdown(f'<div class="error-card">{e["symbol"]}: {e.get("_reason", "unknown")}</div>', unsafe_allow_html=True)

# ================================================================================
# MAIN APP
# ================================================================================
def main():
    if not check_password():
        st.stop()

    st.markdown('<div class="main-header">📈 Open Drive Fib Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">NSE Open=Low/High + Fib Retracement + EMA Alignment</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Settings")
        scan_mode = st.radio("Mode:", ["Historical Scan", "Real-Time Scan"])
        scan_date = st.date_input("Scan Date", value=datetime.now(IST) - timedelta(days=1), max_value=datetime.now(IST))

        st.markdown("---")
        st.subheader("📋 Stock List")
        input_method = st.radio("Input:", ["Paste Symbols", "Use Default List"])

        default_symbols = (
            "HDFCBANK.NS\nAXISBANK.NS\nICICIBANK.NS\nKOTAKBANK.NS\nRBLBANK.NS\nFEDERALBANK.NS\nBANDHANBANK.NS\nAUBANK.NS\nINDUSINDBANK.NS\nIDFCFIRSTBANK.NS\n"
            "SBIN.NS\nBANKBARODA.NS\nCANBANK.NS\nPNB.NS\nABCAPITAL.NS\nANGELONE.NS\nBAJAJFINSERV.NS\nBAJAJFINANCE.NS\nBSE.NS\nCDSL.NS\n"
            "HDFCAMC.NS\nJIOFIN.NS\nLICHOUSING.NS\nLICI.NS\nMANAPURAM.NS\nMCX.NS\nPFC.NS\nREC.NS\nSHRIRAMFINANCE.NS\nHCLTECH.NS\n"
            "INFY.NS\nLTM.NS\nTCS.NS\nTECHM.NS\nWIPRO.NS\nHINDALCO.NS\nHINDZINC.NS\nNATIONALALUMINUM.NS\nNMDC.NS\nSAIL.NS\n"
            "TATASTEEL.NS\nVEDL.NS\nDLF.NS\nOBEROIREALITY.NS\nBRITANNIA.NS\nCOLPAL.NS\nDABUR.NS\nHINDUNILVR.NS\nMARICO.NS\nTATACONSUMER.NS\n"
            "BPCL.NS\nCOALINDIA.NS\nGAIL.NS\nHINDPETRO.NS\nIOC.NS\nOIL.NS\nONGC.NS\nRELIANCE.NS\nASHOKLEY.NS\nBAJAJAUTO.NS\n"
            "BHARATFORG.NS\nEICHER.NS\nEXIDE.NS\nHEROMOTO.NS\nM&M.NS\nMARUTI.NS\nTMPV.NS\nTVSMOTOR.NS\nASIANPAINT.NS\nCROMPTON.NS\n"
            "HAVELLS.NS\nTITAN.NS\nVOLTAS.NS\nAPOLLOHOSPITAL.NS\nAUROPHARMA.NS\nBIOCON.NS\nDRREDDY.NS\nLAURUSLAB.NS\nLUPIN.NS\nSUNPHARMA.NS\n"
            "SRF.NS\nSOLARINDUSTRY.NS\nAMBUJACEMENT.NS\nGRASIM.NS\nLT.NS\nNBCC.NS\nULTRATECH.NS\nABB.NS\nASTRAL.NS\nBEL.NS\n"
            "BHEL.NS\nCGPOWER.NS\nCUMMINS.NS\nHAL.NS\nKEI.NS\nPOLYCAB.NS\nPOWERINDIA.NS\nETERNAL.NS\nINDHOTEL.NS\nNYKAA.NS\n"
            "TRENT.NS\nNTPC.NS\nTATAPOWER.NS\nPOWERGRID.NS\nADANIPORTS.NS\nDELHIVERY.NS\nCONCOR.NS\nGMR.NS\nINDIGO.NS\nBHARTIAIRTEL.NS"
        )

        if input_method == "Paste Symbols":
            symbols_text = st.text_area("Symbols (one per line):", height=150, value=default_symbols)
            stock_list = [line.strip() for line in symbols_text.split('\n') if line.strip()]
        else:
            stock_list = [line.strip() for line in default_symbols.split('\n') if line.strip()]

        st.markdown(f"**{len(stock_list)} stocks loaded**")
        st.markdown("---")

        st.subheader("🔧 Parameters")
        ema_fast = st.number_input("EMA Fast", value=20, min_value=5, max_value=100)
        ema_slow = st.number_input("EMA Slow", value=50, min_value=10, max_value=200)
        fib_50 = st.number_input("Fib Level 1", value=0.50, min_value=0.10, max_value=0.90, step=0.01)
        fib_618 = st.number_input("Fib Level 2", value=0.618, min_value=0.10, max_value=0.90, step=0.001)
        impulse_pct = st.number_input("Min Impulse %", value=0.5, min_value=0.1, max_value=5.0, step=0.1)
        rr = st.number_input("Risk:Reward", value=2.0, min_value=1.0, max_value=5.0, step=0.5)
        tolerance_pct = st.number_input("Tolerance %", value=0.01, min_value=0.001, max_value=1.0, step=0.001, format="%.3f")
        max_sig_time = st.text_input("Max Signal Time (HH:MM)", value="15:30",
                                     help="Signals after this time are ignored.")

        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        start_time = time.time()

        tv_pool = TVConnectionPool(CONFIG.TV_POOL_SIZE)
        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.FIB_LEVEL_1 = fib_50
        strategy.config.FIB_LEVEL_2 = fib_618
        strategy.config.MIN_IMPULSE_PCT = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        try:
            mst = max_sig_time if max_sig_time != "15:30" else None
            if mst:
                pd.Timestamp(mst).time()
        except Exception:
            mst = None

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning...")
            bar = st.progress(0)
            status = st.empty()

        # Run async scan with two-pass retry
        loop = asyncio.get_event_loop()
        all_results = loop.run_until_complete(async_scan_all(
            stock_list, scan_date, tolerance_pct, strategy, tv_pool, bar, status
        ))

        progress_container.empty()

        duration = time.time() - start_time
        valid_signals = [r for r in all_results if r is not None and not (isinstance(r, dict) and r.get("_error"))]
        errors = [r for r in all_results if isinstance(r, dict) and r.get("_error")]
        retried_count = len([r for r in all_results if isinstance(r, dict) and r.get("_reason") == "timeout"])

        perf_stats = {
            'stocks_scanned': len(stock_list),
            'duration': duration,
            'signals_found': len(valid_signals),
            'stocks_per_sec': len(stock_list) / duration if duration > 0 else 0,
            'retried': retried_count
        }

        display_results(all_results, scan_date, perf_stats)

    # ---------------------------------------------------------
    # REAL-TIME SCAN
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span style="background:#28a745;color:white;padding:4px 12px;border-radius:10px;">🟢 LIVE</span></div>', unsafe_allow_html=True)

        tv_pool = TVConnectionPool(CONFIG.TV_POOL_SIZE)
        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.FIB_LEVEL_1 = fib_50
        strategy.config.FIB_LEVEL_2 = fib_618
        strategy.config.MIN_IMPULSE_PCT = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        try:
            mst = max_sig_time if max_sig_time != "15:30" else None
            if mst:
                pd.Timestamp(mst).time()
        except Exception:
            mst = None

        live_container = st.empty()

        while True:
            all_results = []
            live_dt = datetime.now(IST)
            ct = live_dt.time()
            is_open = (live_dt.weekday() < 5 and
                       ct >= datetime.strptime("09:15", "%H:%M").time() and
                       ct <= datetime.strptime("15:30", "%H:%M").time())

            progress_container = st.empty()
            with progress_container.container():
                live_bar = st.progress(0)
                live_status = st.empty()

            total = len(stock_list)
            batch_size = CONFIG.BATCH_SIZE

            # Live scan: batch async with timeout
            for batch_start in range(0, total, batch_size):
                batch = stock_list[batch_start:batch_start + batch_size]
                sem = asyncio.Semaphore(batch_size)

                async def live_scan_one(sym):
                    async with sem:
                        fetcher = HybridDataFetcher(tv_pool)
                        return await scan_stock_safe(fetcher, strategy, sym, live_dt, tolerance_pct, CONFIG.PER_STOCK_TIMEOUT)

                tasks = [live_scan_one(s) for s in batch]
                loop = asyncio.get_event_loop()
                batch_results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

                for r in batch_results:
                    if isinstance(r, tuple) and len(r) == 3:
                        res, sym, is_err = r
                        if not is_err and res is not None:
                            all_results.append(res)

                completed = min(batch_start + batch_size, total)
                live_bar.progress(completed / total)
                live_status.text(f"⚡ Live Scanning... ({completed}/{total})")

            progress_container.empty()

            with live_container.container():
                mkt = "🟢 Market Open" if is_open else "🔴 Market Closed"
                st.write(f"⏱️ Last Updated: {datetime.now(IST).strftime('%H:%M:%S IST')} | {mkt}")
                display_results(all_results, live_dt)

            time.sleep(60 if not is_open else 10)

    elif not stock_list:
        st.info("Please add stocks to scan.")

if __name__ == "__main__":
    main()
