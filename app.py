import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import warnings
import logging
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tvDatafeed import TvDatafeed, Interval

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# OPTIMIZED CONFIGURATION — THE BOTTLENECK FIX
# ================================================================================
class ScaleConfig:
    TV_POOL_SIZE = 8           # 8 stable connections (30 caused flakiness)
    MAX_THREAD_WORKERS = 8     # 1:1 mapping — no contention
    RETRY_ATTEMPTS = 2
    BASE_DELAY = 0.05          # 50ms between requests
    
    # CRITICAL FIX: Was pulling 7,500 bars. Now pulls only what's needed.
    BARS_5MIN = 250            # ~2 trading days of 5m data
    BARS_15MIN = 350           # ~3.5 days of 15m data (plenty for EMA50 warmup)

# ================================================================================
# TV POOL — SIMPLIFIED AND STABLE
# ================================================================================
class TVConnectionPool:
    def __init__(self, size):
        self.pool = []
        for i in range(size):
            try:
                tv = TvDatafeed()
                self.pool.append(tv)
                time.sleep(0.4)
            except Exception:
                break
        self.size = len(self.pool)
        if self.size == 0:
            st.error("❌ Failed to initialize any TradingView connections.")
            st.stop()
    
    def get_instance(self, idx):
        return self.pool[idx % self.size]

@st.cache_resource
def get_tv_pool():
    return TVConnectionPool(ScaleConfig.TV_POOL_SIZE)

# ================================================================================
# SECURITY GATEWAY
# ================================================================================
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
# UI SETUP
# ================================================================================
st.set_page_config(page_title="Open Drive Fib Scanner", page_icon="📈", layout="wide")
st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 0.5rem; }
    .sub-header { font-size: 1.1rem; color: #666; text-align: center; margin-bottom: 2rem; }
    .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 10px; color: white; text-align: center; }
    .signal-card { padding: 1rem; border-radius: 8px; margin: 0.5rem 0; border-left: 4px solid; }
    .buy-card { background: linear-gradient(135deg, #1a5f5f 0%, #2d8a8a 100%) !important; color: white; border-left-color: #4CAF50; }
    .sell-card { background: linear-gradient(135deg, #7a1f1f 0%, #a03030 100%) !important; color: white; border-left-color: #f44336; }
    .perf-card { background: #1e1e2e; padding: 0.8rem; border-radius: 6px; color: #a0a0b0; font-size: 0.85rem; }
    .error-card { background: #2d1f1f; padding: 0.6rem; border-radius: 4px; color: #ff6b6b; font-size: 0.8rem; margin: 0.3rem 0; }
</style>
""", unsafe_allow_html=True)

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
    def __init__(self, tv_pool):
        self.config = Config()
        self.tv_pool = tv_pool

    def get_data(self, tv_inst, symbol, interval, n_bars):
        """Fetch with retry. Optimized bar count makes this ~20× faster."""
        for attempt in range(ScaleConfig.RETRY_ATTEMPTS + 1):
            try:
                df = tv_inst.get_hist(
                    symbol=symbol.replace('.NS', ''),
                    exchange='NSE',
                    interval=interval,
                    n_bars=n_bars
                )
                if df is not None and not df.empty:
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
                if attempt < ScaleConfig.RETRY_ATTEMPTS:
                    time.sleep(0.2 * (attempt + 1))
            except Exception:
                if attempt < ScaleConfig.RETRY_ATTEMPTS:
                    time.sleep(0.2 * (attempt + 1))
                else:
                    break
        return pd.DataFrame()

    def calculate_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def check_trend(self, df_slice):
        """Lenient trend: EMA20 vs EMA50 alignment for 2 consecutive bars."""
        if len(df_slice) < 2:
            return False, False
        ema20_now = df_slice['ema20'].iloc[-1]
        ema50_now = df_slice['ema50'].iloc[-1]
        ema20_prev = df_slice['ema20'].iloc[-2]
        ema50_prev = df_slice['ema50'].iloc[-2]
        
        is_up = (ema20_now > ema50_now) and (ema20_prev > ema50_prev)
        is_down = (ema20_now < ema50_now) and (ema20_prev < ema50_prev)
        return is_up, is_down

    def scan_stock(self, tv_inst, symbol, scan_date, tolerance_pct=0.01, max_signal_time=None):
        try:
            # OPTIMIZED: Minimal bars. 350 bars of 15m is enough for EMA50 to converge.
            df_5min = self.get_data(tv_inst, symbol, Interval.in_5_minute, ScaleConfig.BARS_5MIN)
            df_15min = self.get_data(tv_inst, symbol, Interval.in_15_minute, ScaleConfig.BARS_15MIN)

            if df_5min.empty or df_15min.empty:
                return {"_error": True, "_reason": "empty_data", "symbol": symbol.replace('.NS', '')}

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date
            sym_clean = symbol.replace('.NS', '')

            # STRICT market hours: 09:15 to 15:30 only
            market_open = pd.Timestamp('09:15').time()
            market_close = pd.Timestamp('15:30').time()

            df_5min_today = df_5min[
                (df_5min.index.date == target_date) &
                (df_5min.index.time >= market_open) &
                (df_5min.index.time <= market_close)
            ].copy()

            # EMA on FULL available history (sufficiently warmed with 350 bars)
            df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
            df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

            df_15min_today = df_15min[
                (df_15min.index.date == target_date) &
                (df_15min.index.time >= market_open) &
                (df_15min.index.time <= market_close)
            ].copy()

            if df_5min_today.empty or len(df_15min_today) < 3:
                return None

            # First 5m candle = 9:15–9:20
            first_5m = df_5min_today.iloc[0]
            first5_open = float(first_5m['open'])
            first5_high = float(first_5m['high'])
            first5_low = float(first_5m['low'])

            price = first5_open if first5_open > 0 else 1.0
            tolerance = price * (tolerance_pct / 100)

            is_buy_setup = abs(first5_open - first5_low) <= tolerance
            is_sell_setup = abs(first5_open - first5_high) <= tolerance

            if not is_buy_setup and not is_sell_setup:
                return None

            # Optional signal cutoff (e.g., "14:30" to avoid end-of-day noise)
            cutoff = pd.Timestamp(max_signal_time).time() if max_signal_time else market_close

            # State variables
            buy_setup_invalid = False; sell_setup_invalid = False
            buy_impulse_done = False; buy_impulse_bar = -1
            buy_retraced = False; buy_retrace_bar = -1; buy_signal_fired = False
            buy_swing_high = None; buy_fib_50 = None; buy_fib_618 = None
            
            sell_impulse_done = False; sell_impulse_bar = -1
            sell_retraced = False; sell_retrace_bar = -1; sell_signal_fired = False
            sell_swing_low = None; sell_fib_50 = None; sell_fib_618 = None
            
            signal = None

            # =====================================================================
            # MAIN LOOP — CRITICAL FIX: First 15m bar (i=0, 9:15-9:30) is ONLY for 
            # invalidation. Impulse, retrace, and signal start from i>=1 (9:30+).
            # =====================================================================
            for i, (idx, row) in enumerate(df_15min_today.iterrows()):
                
                # --- i=0: Invalidation only ---
                if i == 0:
                    if is_buy_setup and row['low'] < first5_low:
                        buy_setup_invalid = True
                    if is_sell_setup and row['high'] > first5_high:
                        sell_setup_invalid = True
                    continue

                # Hard cutoff to prevent late-day noise signals
                if idx.time() > cutoff:
                    break

                # --- Invalidation (before impulse completes) ---
                if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if row['low'] < first5_low:
                        buy_setup_invalid = True

                if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if row['high'] > first5_high:
                        sell_setup_invalid = True

                # --- Buy Impulse ---
                if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    buy_swing_high = float(row['high']) if buy_swing_high is None else max(buy_swing_high, float(row['high']))
                    threshold = first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100)
                    if buy_swing_high >= threshold:
                        buy_impulse_done = True
                        buy_impulse_bar = i
                        buy_fib_50 = buy_swing_high - self.config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                        buy_fib_618 = buy_swing_high - self.config.FIB_LEVEL_2 * (buy_swing_high - first5_low)

                # --- Sell Impulse ---
                if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    sell_swing_low = float(row['low']) if sell_swing_low is None else min(sell_swing_low, float(row['low']))
                    threshold = first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100)
                    if sell_swing_low <= threshold:
                        sell_impulse_done = True
                        sell_impulse_bar = i
                        sell_fib_50 = sell_swing_low + self.config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                        sell_fib_618 = sell_swing_low + self.config.FIB_LEVEL_2 * (first5_high - sell_swing_low)

                # --- Buy Retrace ---
                if buy_impulse_done and not buy_retraced and i > buy_impulse_bar:
                    if row['low'] <= buy_fib_50:
                        buy_retraced = True
                        buy_retrace_bar = i

                # --- Sell Retrace ---
                if sell_impulse_done and not sell_retraced and i > sell_impulse_bar:
                    if row['high'] >= sell_fib_50:
                        sell_retraced = True
                        sell_retrace_bar = i

                # --- Buy Signal ---
                if buy_retraced and not buy_signal_fired and i > buy_retrace_bar:
                    if row['close'] > row['open'] and row['close'] > buy_fib_50:
                        is_uptrend, _ = self.check_trend(df_15min_today.iloc[:i+1])
                        if (row['close'] > row['ema20']) and (row['close'] > row['ema50']) and is_uptrend:
                            entry = float(row['close'])
                            sl = float(row['low'])
                            target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR
                            signal = {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(buy_fib_50, 2),
                                'fib_618': round(buy_fib_618, 2), 'swing_high': round(buy_swing_high, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'UP'
                            }
                            buy_signal_fired = True
                            break

                # --- Sell Signal ---
                if sell_retraced and not sell_signal_fired and i > sell_retrace_bar:
                    if row['close'] < row['open'] and row['close'] < sell_fib_50:
                        _, is_downtrend = self.check_trend(df_15min_today.iloc[:i+1])
                        if (row['close'] < row['ema20']) and (row['close'] < row['ema50']) and is_downtrend:
                            entry = float(row['close'])
                            sl = float(row['high'])
                            target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR
                            signal = {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(sell_fib_50, 2),
                                'fib_618': round(sell_fib_618, 2), 'swing_low': round(sell_swing_low, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN'
                            }
                            sell_signal_fired = True
                            break

            return signal

        except Exception as e:
            return {"_error": True, "_reason": str(e), "symbol": symbol.replace('.NS', '')}

# ================================================================================
# WORKER — RETRY WITH CONNECTION ROTATION
# ================================================================================
def process_stock(task_data):
    idx, sym, target_date, tv_pool, strategy, tolerance_pct, max_signal_time = task_data
    tv_inst = tv_pool.get_instance(idx)
    
    for attempt in range(ScaleConfig.RETRY_ATTEMPTS):
        try:
            time.sleep(ScaleConfig.BASE_DELAY * attempt)
            res = strategy.scan_stock(tv_inst, sym, target_date, tolerance_pct, max_signal_time)
            
            # If error dict, rotate TV instance and retry
            if isinstance(res, dict) and res.get("_error"):
                tv_inst = tv_pool.get_instance(idx + attempt + 1)
                continue
            
            return res  # Signal dict or None (valid no-setup)
            
        except Exception:
            if attempt < ScaleConfig.RETRY_ATTEMPTS - 1:
                time.sleep(0.2 * (attempt + 1))
                tv_inst = tv_pool.get_instance(idx + attempt + 1)
    
    return None

# ================================================================================
# DISPLAY
# ================================================================================
def display_results(all_results, scan_date, perf_stats=None):
    valid_signals = [s for s in all_results if s is not None and not (isinstance(s, dict) and s.get("_error"))]
    errors = [s for s in all_results if isinstance(s, dict) and s.get("_error")]
    
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
            | {ScaleConfig.MAX_THREAD_WORKERS} threads × {ScaleConfig.TV_POOL_SIZE} TV connections
            | {len(errors)} errors (auto-retried)
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
        with st.expander(f"⚠️ Errors ({len(errors)} stocks failed)"):
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
                                     help="Signals after this time are ignored. Set to 14:30 to avoid end-of-day noise.")
        
        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        start_time = time.time()
        tv_pool = get_tv_pool()
        
        strategy = OpenDriveFibStrategy(tv_pool)
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.FIB_LEVEL_1 = fib_50
        strategy.config.FIB_LEVEL_2 = fib_618
        strategy.config.MIN_IMPULSE_PCT = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr
        
        # Parse cutoff time
        try:
            mst = max_sig_time if max_sig_time != "15:30" else None
            if mst:
                pd.Timestamp(mst).time()  # validate
        except Exception:
            st.warning("Invalid Max Signal Time format. Using 15:30.")
            mst = None
        
        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning...")
            bar = st.progress(0)
            status = st.empty()
        
        all_results = []
        total = len(stock_list)
        completed = 0
        
        with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_THREAD_WORKERS) as executor:
            tasks = [(i, sym, scan_date, tv_pool, strategy, tolerance_pct, mst) for i, sym in enumerate(stock_list)]
            futures = {executor.submit(process_stock, task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                if completed % 5 == 0 or completed == total:
                    bar.progress(completed / total)
                    status.text(f"⚡ Scanning... ({completed}/{total})")
                
                try:
                    res = future.result(timeout=15)
                    all_results.append(res)
                except Exception:
                    all_results.append(None)
        
        progress_container.empty()
        
        duration = time.time() - start_time
        valid_signals = [r for r in all_results if r is not None and not (isinstance(r, dict) and r.get("_error"))]
        errors = [r for r in all_results if isinstance(r, dict) and r.get("_error")]
        
        perf_stats = {
            'stocks_scanned': total,
            'duration': duration,
            'signals_found': len(valid_signals),
            'stocks_per_sec': total / duration if duration > 0 else 0
        }
        
        display_results(all_results, scan_date, perf_stats)
        
        if len(valid_signals) <= 2 and total > 50:
            st.info("ℹ️ Only 1-2 signals found. This strategy is strict. If you expect more, try slightly increasing 'Min Impulse %' or 'Tolerance %'.")

    # ---------------------------------------------------------
    # REAL-TIME SCAN
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span style="background:#28a745;color:white;padding:4px 12px;border-radius:10px;">🟢 LIVE</span></div>', unsafe_allow_html=True)
        tv_pool = get_tv_pool()
        
        strategy = OpenDriveFibStrategy(tv_pool)
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
            completed = 0
            
            with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_THREAD_WORKERS) as executor:
                tasks = [(i, sym, live_dt, tv_pool, strategy, tolerance_pct, mst) for i, sym in enumerate(stock_list)]
                futures = {executor.submit(process_stock, task): task for task in tasks}
                
                for future in concurrent.futures.as_completed(futures):
                    completed += 1
                    if completed % 5 == 0 or completed == total:
                        live_bar.progress(completed / total)
                        live_status.text(f"⚡ Live Scanning... ({completed}/{total})")
                    
                    try:
                        res = future.result(timeout=15)
                        all_results.append(res)
                    except Exception:
                        all_results.append(None)
            
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
