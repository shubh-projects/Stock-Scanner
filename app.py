import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import random
import warnings
import logging
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import cpu_count
from tvDatafeed import TvDatafeed, Interval

# Mute warnings
warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# SCALING CONFIGURATION — TUNE THESE
# ================================================================================
class ScaleConfig:
    # Method 1: TV Pool Size (more instances = more connections)
    TV_POOL_SIZE = 20           # Was 5. Try 15-25. Each uses ~50MB RAM.
    
    # Method 2: Thread Workers per process
    MAX_THREAD_WORKERS = 10     # Threads per process
    
    # Method 3: Process Pool (true CPU parallelism)
    # Set to None to use all CPU cores, or 1-4 for Streamlit Cloud
    PROCESS_COUNT = min(4, cpu_count())  # 4 processes max
    
    # Method 6: Adaptive throttling
    BATCH_SIZE = 25             # Stocks per batch
    BATCH_DELAY = 0.3         # Seconds between batches
    RETRY_ATTEMPTS = 2
    BASE_DELAY = 0.1          # Min delay between requests

# ================================================================================
# PAGE UI & CSS
# ================================================================================
st.set_page_config(
    page_title="Open Drive Fib Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 0.5rem; }
    .sub-header { font-size: 1.1rem; color: #666; text-align: center; margin-bottom: 2rem; }
    .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 10px; color: white; text-align: center; }
    .live-badge { background-color: #ff4b4b; color: white; padding: 2px 8px; border-radius: 10px; font-size: 0.8rem; font-weight: bold; animation: blink 2s infinite; }
    @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
    .signal-card { padding: 1rem; border-radius: 8px; margin: 0.5rem 0; border-left: 4px solid; }
    .buy-card { background: linear-gradient(135deg, #1a5f5f 0%, #2d8a8a 100%) !important; color: white; border-left-color: #4CAF50; }
    .sell-card { background: linear-gradient(135deg, #7a1f1f 0%, #a03030 100%) !important; color: white; border-left-color: #f44336; }
    .perf-card { background: #1e1e2e; padding: 0.8rem; border-radius: 6px; color: #a0a0b0; font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

# ================================================================================
# TV POOL FACTORY — Can be called per-process
# ================================================================================
def create_tv_pool(size):
    """Create a pool of TvDatafeed instances. Call once per process."""
    pool = []
    for i in range(size):
        try:
            pool.append(TvDatafeed())
            time.sleep(0.8)  # Stagger connections
        except Exception:
            break  # Stop if we can't create more
    return pool

@st.cache_resource
def get_tv_pool():
    """Cached TV pool for main process."""
    return create_tv_pool(ScaleConfig.TV_POOL_SIZE)

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
        st.markdown('<div class="main-header">🔒 Private Access Only</div>', unsafe_allow_html=True)
        st.text_input("Enter Scanner Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown('<div class="main-header">🔒 Private Access Only</div>', unsafe_allow_html=True)
        st.text_input("Enter Scanner Password", type="password", on_change=password_entered, key="password")
        st.error("❌ Access Denied. Incorrect password.")
        return False
    else:
        return True

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

    def get_historical_candles(self, tv_instance, symbol, resolution_minutes, is_live=False, days_back=100):
        try:
            if resolution_minutes == 5:
                tv_interval = Interval.in_5_minute
                bars_to_pull = 500 if is_live else (days_back * 75)
            elif resolution_minutes == 10:
                tv_interval = Interval.in_10_minute
                bars_to_pull = 500 if is_live else (days_back * 50)
            else:
                tv_interval = Interval.in_15_minute
                bars_to_pull = 500 if is_live else (days_back * 25)

            formatted_symbol = symbol.replace('.NS', '')
            df = tv_instance.get_hist(symbol=formatted_symbol, exchange='NSE', interval=tv_interval, n_bars=bars_to_pull)

            if df is None or df.empty:
                return pd.DataFrame()

            df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
            df.index = pd.to_datetime(df.index)

            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')

            return df
        except Exception as e:
            return pd.DataFrame()

    def calculate_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def check_trend(self, df):
        if len(df) < 2:
            return False, False

        ema20_now, ema20_prev = df['ema20'].iloc[-1], df['ema20'].iloc[-2]
        ema50_now, ema50_prev = df['ema50'].iloc[-1], df['ema50'].iloc[-2]

        is_uptrend = (ema20_now > ema50_now) and (ema20_now > ema20_prev) and (ema50_now > ema50_prev)
        is_downtrend = (ema20_now < ema50_now) and (ema20_now < ema20_prev) and (ema50_now < ema50_prev)

        return is_uptrend, is_downtrend

    def scan_stock(self, tv_instance, symbol, scan_date, tolerance_pct=0.01):
        try:
            df_5min = self.get_historical_candles(tv_instance, symbol, 5, is_live=False, days_back=100)
            df_15min = self.get_historical_candles(tv_instance, symbol, 15, is_live=False, days_back=100)

            if df_5min.empty or df_15min.empty:
                return None

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

            # Calculate EMAs on FULL history
            df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
            df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

            # Filter to target date
            df_5min_today = df_5min[
                (df_5min.index.date == target_date) & 
                (df_5min.index.time >= pd.Timestamp('09:15').time())
            ].copy()
            df_15min_today = df_15min[
                (df_15min.index.date == target_date) & 
                (df_15min.index.time >= pd.Timestamp('09:15').time())
            ].copy()

            if df_5min_today.empty or df_15min_today.empty:
                return None

            first_5m = df_5min_today.iloc[0]
            first5_open = float(first_5m['open'])
            first5_high = float(first_5m['high'])
            first5_low = float(first_5m['low'])

            price = first5_open if first5_open > 0 else 1.0
            tolerance = price * (tolerance_pct / 100)

            first5_is_buy_setup = abs(first5_open - first5_low) <= tolerance
            first5_is_sell_setup = abs(first5_open - first5_high) <= tolerance

            sym_clean = symbol.replace('.NS', '')

            if not first5_is_buy_setup and not first5_is_sell_setup:
                return None

            # State variables
            buy_swing_high = None; buy_fib_50 = None; buy_fib_618 = None
            buy_impulse_done = False; buy_impulse_bar = -1
            buy_retraced = False; buy_retrace_bar = -1; buy_signal_fired = False

            sell_swing_low = None; sell_fib_50 = None; sell_fib_618 = None
            sell_impulse_done = False; sell_impulse_bar = -1
            sell_retraced = False; sell_retrace_bar = -1; sell_signal_fired = False

            buy_setup_invalid = False; sell_setup_invalid = False
            signal = None

            for i, (idx, row) in enumerate(df_15min_today.iterrows()):
                # Invalidation before impulse
                if first5_is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if row['low'] < first5_low:
                        buy_setup_invalid = True

                if first5_is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if row['high'] > first5_high:
                        sell_setup_invalid = True

                # Buy impulse
                if first5_is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if buy_swing_high is None:
                        buy_swing_high = float(row['high'])
                    else:
                        buy_swing_high = max(buy_swing_high, float(row['high']))
                    threshold = first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100)
                    if buy_swing_high >= threshold:
                        buy_impulse_done = True; buy_impulse_bar = i
                        buy_fib_50 = buy_swing_high - self.config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                        buy_fib_618 = buy_swing_high - self.config.FIB_LEVEL_2 * (buy_swing_high - first5_low)

                # Sell impulse
                if first5_is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if sell_swing_low is None:
                        sell_swing_low = float(row['low'])
                    else:
                        sell_swing_low = min(sell_swing_low, float(row['low']))
                    threshold = first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100)
                    if sell_swing_low <= threshold:
                        sell_impulse_done = True; sell_impulse_bar = i
                        sell_fib_50 = sell_swing_low + self.config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                        sell_fib_618 = sell_swing_low + self.config.FIB_LEVEL_2 * (first5_high - sell_swing_low)

                # Retracements
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
                            signal = {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(buy_fib_50, 2),
                                'fib_618': round(buy_fib_618, 2), 'swing_high': round(buy_swing_high, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'UP'
                            }
                            buy_signal_fired = True; break

                # Sell signal
                if sell_retraced and not sell_signal_fired and i > sell_retrace_bar:
                    if row['close'] < row['open'] and row['close'] < sell_fib_50:
                        _, is_downtrend = self.check_trend(df_15min_today.iloc[:i+1])
                        if (row['close'] < row['ema20']) and (row['close'] < row['ema50']) and is_downtrend:
                            entry = float(row['close']); sl = float(row['high'])
                            target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR
                            signal = {
                                'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}", 'fib_50': round(sell_fib_50, 2),
                                'fib_618': round(sell_fib_618, 2), 'swing_low': round(sell_swing_low, 2),
                                'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN'
                            }
                            sell_signal_fired = True; break

            return signal

        except Exception as e:
            return None

# ================================================================================
# WORKER FUNCTION FOR PROCESS POOL
# ================================================================================
def process_batch(batch_data):
    """
    Process a batch of stocks in a separate process.
    Each process gets its own TV pool and strategy instance.
    """
    stock_batch, scan_date, tolerance_pct, config_values = batch_data
    
    # Each process creates its own TV pool (isolated connections)
    tv_pool = create_tv_pool(ScaleConfig.TV_POOL_SIZE // ScaleConfig.PROCESS_COUNT + 1)
    strategy = OpenDriveFibStrategy()
    strategy.config.EMA_FAST = config_values['ema_fast']
    strategy.config.EMA_SLOW = config_values['ema_slow']
    strategy.config.FIB_LEVEL_1 = config_values['fib_50']
    strategy.config.FIB_LEVEL_2 = config_values['fib_618']
    strategy.config.MIN_IMPULSE_PCT = config_values['impulse_pct']
    strategy.config.DEFAULT_TARGET_RR = config_values['rr']
    
    results = []
    for idx, sym in enumerate(stock_batch):
        tv_inst = tv_pool[idx % len(tv_pool)]
        
        # Adaptive retry
        for attempt in range(ScaleConfig.RETRY_ATTEMPTS):
            try:
                time.sleep(ScaleConfig.BASE_DELAY + random.uniform(0, 0.1))
                res = strategy.scan_stock(tv_inst, sym, scan_date, tolerance_pct)
                if res:
                    results.append(res)
                break
            except Exception:
                if attempt == ScaleConfig.RETRY_ATTEMPTS - 1:
                    break
                time.sleep(0.5 * (attempt + 1))
    
    return results

# ================================================================================
# DISPLAY
# ================================================================================
def display_results(signals, scan_date, perf_stats=None):
    if not signals:
        st.warning("⚠️ No signals found.")
        return

    df = pd.DataFrame([s for s in signals if s is not None])
    if df.empty:
        st.warning("⚠️ No valid signals.")
        return

    # Performance stats
    if perf_stats:
        st.markdown(f"""
        <div class="perf-card">
            ⚡ <b>Scan Performance:</b> {perf_stats['stocks_scanned']} stocks in {perf_stats['duration']:.1f}s 
            | {perf_stats['signals_found']} signals | {perf_stats['stocks_per_sec']:.1f} stocks/sec
            | Using {ScaleConfig.PROCESS_COUNT} processes × {ScaleConfig.MAX_THREAD_WORKERS} threads
        </div>
        """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f'<div class="metric-card"><h3>{len(df)}</h3><p>Total Signals</p></div>', unsafe_allow_html=True)
    with col2:
        buy_count = len(df[df['direction'] == 'BUY'])
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);"><h3>{buy_count}</h3><p>BUY Signals</p></div>', unsafe_allow_html=True)
    with col3:
        sell_count = len(df[df['direction'] == 'SELL'])
        st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);"><h3>{sell_count}</h3><p>SELL Signals</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📋 Filtered Stocks — Fib Retracement Signals")

    for _, row in df.iterrows():
        card_class = "buy-card" if row['direction'] == 'BUY' else "sell-card"
        swing_text = f"Swing High: {row.get('swing_high', 'N/A')}" if row['direction'] == 'BUY' else f"Swing Low: {row.get('swing_low', 'N/A')}"

        st.markdown(f"""
        <div class="signal-card {card_class}">
            <h4>{row['symbol']} — {row['direction']} @ {row['entry_price']}</h4>
            <p><b>Setup:</b> {row['setup_time']} | <b>Signal:</b> {row['signal_time']} | <b>Trend:</b> {row['trend']}</p>
            <p><b>Entry:</b> {row['entry_price']} | <b>SL:</b> {row['stop_loss']} | <b>TGT:</b> {row['target']} | <b>R:R:</b> {row['risk_reward']}</p>
            <p><b>Fib 0.5:</b> {row['fib_50']} | <b>Fib 0.618:</b> {row['fib_618']} | {swing_text}</p>
            <p><b>EMA20:</b> {row['ema20']} | <b>EMA50:</b> {row['ema50']}</p>
        </div>
        """, unsafe_allow_html=True)

    with st.expander("📊 Export Data"):
        st.dataframe(df, hide_index=True, use_container_width=True)

# ================================================================================
# MAIN APP
# ================================================================================
def main():
    if not check_password():
        st.stop()

    st.markdown('<div class="main-header">📈 Open Drive Fib Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Scans NSE stocks for Open=Low/High + Fib Retracement + EMA alignment</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Settings")

        scan_mode = st.radio("Select Mode:", ["Historical Scan", "Real-Time Scan"])
        scan_date = st.date_input("Scan Date", value=datetime.now(IST) - timedelta(days=1), max_value=datetime.now(IST))

        st.markdown("---")
        st.subheader("📋 Stock List")
        input_method = st.radio("Input method:", ["Paste Symbols", "Use Default List"])

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
            symbols_text = st.text_area("Enter symbols (one per line):", height=150, value=default_symbols)
            stock_list = [line.strip() for line in symbols_text.split('\n') if line.strip()]
        else:
            stock_list = [line.strip() for line in default_symbols.split('\n') if line.strip()]

        st.markdown(f"**{len(stock_list)} stocks loaded**")
        st.markdown("---")

        st.subheader("🔧 Strategy Parameters")
        ema_fast = st.number_input("EMA Fast", value=20, min_value=5, max_value=100)
        ema_slow = st.number_input("EMA Slow", value=50, min_value=10, max_value=200)
        fib_50 = st.number_input("Fib Level 1 (0.5)", value=0.50, min_value=0.10, max_value=0.90, step=0.01)
        fib_618 = st.number_input("Fib Level 2 (0.618)", value=0.618, min_value=0.10, max_value=0.90, step=0.001)
        impulse_pct = st.number_input("Min Impulse %", value=0.5, min_value=0.1, max_value=5.0, step=0.1)
        rr = st.number_input("Risk:Reward Ratio", value=2.0, min_value=1.0, max_value=5.0, step=0.5)
        tolerance_pct = st.number_input("Open=High/Low Tolerance (%)", value=0.01, min_value=0.001, max_value=1.0, step=0.001, format="%.3f")

        # Scaling controls
        st.markdown("---")
        st.subheader("⚡ Performance Scaling")
        use_processes = st.checkbox("Use Multi-Process Mode (faster, uses more RAM)", value=False)
        st.caption(f"Default: {ScaleConfig.MAX_THREAD_WORKERS} threads. Multi-process: {ScaleConfig.PROCESS_COUNT} processes × {ScaleConfig.MAX_THREAD_WORKERS} threads = {ScaleConfig.PROCESS_COUNT * ScaleConfig.MAX_THREAD_WORKERS} concurrent")

        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN — WITH MULTI-PROCESS OPTION
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        start_time = time.time()
        st.info(f"🔌 Initializing {'Multi-Process' if use_processes else 'Thread'} Pool...")

        config_values = {
            'ema_fast': ema_fast, 'ema_slow': ema_slow,
            'fib_50': fib_50, 'fib_618': fib_618,
            'impulse_pct': impulse_pct, 'rr': rr
        }

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning...")
            progress_bar = st.progress(0)
            status_text = st.empty()

        all_signals = []
        total = len(stock_list)

        if use_processes and ScaleConfig.PROCESS_COUNT > 1:
            # === MULTI-PROCESS MODE ===
            # Split stocks into batches for each process
            batch_size = max(1, len(stock_list) // ScaleConfig.PROCESS_COUNT)
            batches = []
            for i in range(0, len(stock_list), batch_size):
                batch = stock_list[i:i + batch_size]
                batches.append((batch, scan_date, tolerance_pct, config_values))

            completed_batches = 0
            with ProcessPoolExecutor(max_workers=ScaleConfig.PROCESS_COUNT) as executor:
                futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
                
                for future in concurrent.futures.as_completed(futures):
                    completed_batches += 1
                    progress_bar.progress(min(1.0, completed_batches / len(batches)))
                    status_text.text(f"⚡ Process {completed_batches}/{len(batches)} complete...")
                    
                    try:
                        batch_results = future.result()
                        all_signals.extend(batch_results)
                    except Exception as e:
                        st.error(f"Process error: {e}")

        else:
            # === THREAD-ONLY MODE (original, more stable) ===
            tv_pool = get_tv_pool()
            strategy = OpenDriveFibStrategy()
            strategy.config.EMA_FAST = ema_fast
            strategy.config.EMA_SLOW = ema_slow
            strategy.config.FIB_LEVEL_1 = fib_50
            strategy.config.FIB_LEVEL_2 = fib_618
            strategy.config.MIN_IMPULSE_PCT = impulse_pct
            strategy.config.DEFAULT_TARGET_RR = rr

            completed = 0

            def process_stock(task_data):
                idx, sym, target_date = task_data
                tv_inst = tv_pool[idx % len(tv_pool)]
                time.sleep(random.uniform(0.05, 0.2))
                return strategy.scan_stock(tv_inst, sym, target_date, tolerance_pct)

            with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_THREAD_WORKERS) as executor:
                tasks = [(i, sym, scan_date) for i, sym in enumerate(stock_list)]
                
                # Batch submission to avoid overwhelming
                for batch_start in range(0, len(tasks), ScaleConfig.BATCH_SIZE):
                    batch = tasks[batch_start:batch_start + ScaleConfig.BATCH_SIZE]
                    futures = {executor.submit(process_stock, task): task for task in batch}

                    for future in concurrent.futures.as_completed(futures):
                        completed += 1
                        if completed % 5 == 0 or completed == total:
                            progress_bar.progress(completed / total)
                            status_text.text(f"⚡ Scanning... ({completed}/{total})")

                        try:
                            res = future.result()
                            if res:
                                all_signals.append(res)
                        except Exception:
                            pass
                    
                    time.sleep(ScaleConfig.BATCH_DELAY)

        progress_container.empty()

        # Performance stats
        duration = time.time() - start_time
        perf_stats = {
            'stocks_scanned': total,
            'duration': duration,
            'signals_found': len(all_signals),
            'stocks_per_sec': total / duration if duration > 0 else 0
        }

        display_results(all_signals, scan_date, perf_stats)

    # ---------------------------------------------------------
    # REAL-TIME SCAN — Thread-only (safer for continuous)
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span class="live-badge">🔴 INITIALIZING...</span></div>', unsafe_allow_html=True)
        tv_pool = get_tv_pool()
        st.markdown('<div style="text-align:center;"><span class="live-badge" style="background-color: #28a745;">🟢 LIVE</span></div>', unsafe_allow_html=True)

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.FIB_LEVEL_1 = fib_50
        strategy.config.FIB_LEVEL_2 = fib_618
        strategy.config.MIN_IMPULSE_PCT = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        live_container = st.empty()

        def process_live(task_data):
            idx, sym, target_time = task_data
            tv_inst = tv_pool[idx % len(tv_pool)]
            time.sleep(random.uniform(0.05, 0.15))
            return strategy.scan_stock(tv_inst, sym, target_time, tolerance_pct)

        while True:
            all_signals = []
            live_datetime = datetime.now(IST)
            current_time = live_datetime.time()

            is_market_open = (live_datetime.weekday() < 5 and 
                current_time >= datetime.strptime("09:15", "%H:%M").time() and 
                current_time <= datetime.strptime("15:30", "%H:%M").time())

            progress_container = st.empty()
            with progress_container.container():
                live_progress = st.progress(0)
                live_status = st.empty()

            total = len(stock_list)
            completed = 0

            with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_THREAD_WORKERS) as executor:
                tasks = [(i, sym, live_datetime) for i, sym in enumerate(stock_list)]
                
                for batch_start in range(0, len(tasks), ScaleConfig.BATCH_SIZE):
                    batch = tasks[batch_start:batch_start + ScaleConfig.BATCH_SIZE]
                    futures = {executor.submit(process_live, task): task for task in batch}

                    for future in concurrent.futures.as_completed(futures):
                        completed += 1
                        if completed % 5 == 0 or completed == total:
                            live_progress.progress(completed / total)
                            live_status.text(f"⚡ Live Scanning... ({completed}/{total})")

                        try:
                            res = future.result()
                            if res:
                                all_signals.append(res)
                        except Exception:
                            pass
                    
                    time.sleep(ScaleConfig.BATCH_DELAY)

            progress_container.empty()

            with live_container.container():
                market_status = "🟢 Market Open" if is_market_open else "🔴 Market Closed"
                st.write(f"⏱️ Last Updated: {datetime.now(IST).strftime('%H:%M:%S IST')} | {market_status}")
                display_results(all_signals, live_datetime)

            time.sleep(60 if not is_market_open else 5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
