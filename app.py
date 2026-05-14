import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import random
import warnings
import logging
import concurrent.futures
from tvDatafeed import TvDatafeed, Interval

# Mute warnings
warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

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
    .signal-card { padding: 1rem; border-radius: 8px; margin: 0.5rem 0; }
    .buy-card { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; }
    .sell-card { background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); color: white; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_tv_pool():
    pool = []
    for i in range(5):
        pool.append(TvDatafeed())
        time.sleep(1.5)
    return pool

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
# STRATEGY LOGIC — FIBONACCI RETRACEMENT EDITION
# ================================================================================
class Config:
    EMA_FAST = 20
    EMA_SLOW = 50
    FIB_LEVEL_1 = 0.50      # Must cross this level
    FIB_LEVEL_2 = 0.618     # Near this level is acceptable
    MIN_IMPULSE_PCT = 0.5   # Minimum impulse move %
    FIXED_IMPULSE_AMT = 5.0 # Fixed ₹ amount (if used instead of %)
    USE_FIXED_IMPULSE = False
    USE_10M_CONFIRM = True
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

    def check_first_5min_candle(self, df_5min, tolerance=0.05):
        """Check if first 5m candle is Open=Low (buy setup) or Open=High (sell setup)"""
        if df_5min.empty:
            return None, None
        
        first = df_5min.iloc[0]
        o, h, l = first['open'], first['high'], first['low']
        
        if abs(o - l) <= tolerance:
            return 'buy', {'open': o, 'high': h, 'low': l, 'time': df_5min.index[0]}
        elif abs(o - h) <= tolerance:
            return 'sell', {'open': o, 'high': h, 'low': l, 'time': df_5min.index[0]}
        return None, None

    def scan_stock(self, tv_instance, symbol, scan_date, tolerance=0.05):
        """
        Full Fibonacci retracement scan for one stock.
        Returns signal dict if valid, else None.
        """
        try:
            # Fetch data
            df_5min = self.get_historical_candles(tv_instance, symbol, 5, is_live=False, days_back=100)
            df_15min = self.get_historical_candles(tv_instance, symbol, 15, is_live=False, days_back=100)
            
            if df_5min.empty or df_15min.empty:
                return None

            # Filter to scan date
            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date
            df_5min_today = df_5min[df_5min.index.date == target_date]
            df_15min_today = df_15min[df_15min.index.date == target_date]
            
            if df_5min_today.empty or df_15min_today.empty:
                return None

            # Check first 5m candle setup
            setup_type, first_candle = self.check_first_5min_candle(df_5min_today, tolerance)
            if setup_type is None:
                return None

            first_open = first_candle['open']
            first_high = first_candle['high']
            first_low = first_candle['low']

            # === SETUP INVALIDATION ===
            # Buy setup invalid if ANY candle breaks below first 5m low
            # Sell setup invalid if ANY candle breaks above first 5m high
            if setup_type == 'buy':
                if (df_15min_today['low'] < first_low).any():
                    return None  # Setup invalidated
            else:
                if (df_15min_today['high'] > first_high).any():
                    return None  # Setup invalidated

            # === CALCULATE EMAS ON 15M ===
            df_15min_today = df_15min_today.copy()
            df_15min_today['ema20'] = self.calculate_ema(df_15min_today, self.config.EMA_FAST)
            df_15min_today['ema50'] = self.calculate_ema(df_15min_today, self.config.EMA_SLOW)

            # === IMPULSE PHASE ===
            # Buy: Price must go up above first_high by min threshold
            # Sell: Price must go down below first_low by min threshold
            impulse_threshold = self.config.FIXED_IMPULSE_AMT if self.config.USE_FIXED_IMPULSE else (first_high * self.config.MIN_IMPULSE_PCT / 100)
            
            if setup_type == 'buy':
                swing_high = df_15min_today['high'].max()
                min_required = first_high + impulse_threshold
                if swing_high < min_required:
                    return None  # No impulse
            else:
                swing_low = df_15min_today['low'].min()
                min_required = first_low - impulse_threshold
                if swing_low > min_required:
                    return None  # No impulse

            # === FIBONACCI RETRACEMENT LEVELS ===
            if setup_type == 'buy':
                fib_50 = swing_high - self.config.FIB_LEVEL_1 * (swing_high - first_low)
                fib_618 = swing_high - self.config.FIB_LEVEL_2 * (swing_high - first_low)
            else:
                fib_50 = swing_low + self.config.FIB_LEVEL_1 * (first_high - swing_low)
                fib_618 = swing_low + self.config.FIB_LEVEL_2 * (first_high - swing_low)

            # === RETRACEMENT PHASE ===
            # Buy: Price must drop to or below fib_50
            # Sell: Price must bounce to or above fib_50
            retraced = False
            retrace_idx = None
            
            for i, (idx, row) in enumerate(df_15min_today.iterrows()):
                if i == 0:
                    continue  # Skip first candle (impulse might be forming)
                
                if setup_type == 'buy':
                    if row['low'] <= fib_50:
                        retraced = True
                        retrace_idx = i
                        break
                else:
                    if row['high'] >= fib_50:
                        retraced = True
                        retrace_idx = i
                        break
            
            if not retraced:
                return None

            # === RECOVERY / RESUMPTION PHASE ===
            # Buy: Green candle closing above fib_50
            # Sell: Red candle closing below fib_50
            # Optional 10m confirmation
            signal_candle = None
            signal_idx = None
            
            for i, (idx, row) in enumerate(df_15min_today.iterrows()):
                if i <= retrace_idx:
                    continue  # Must be after retrace candle
                
                is_green = row['close'] > row['open']
                is_red = row['close'] < row['open']
                
                if setup_type == 'buy':
                    if is_green and row['close'] > fib_50:
                        signal_candle = row
                        signal_idx = i
                        break
                else:
                    if is_red and row['close'] < fib_50:
                        signal_candle = row
                        signal_idx = i
                        break

            if signal_candle is None:
                return None

            # === EMA & TREND CONDITIONS ===
            df_up_to_signal = df_15min_today.iloc[:signal_idx+1]
            is_uptrend, is_downtrend = self.check_trend(df_up_to_signal)
            
            if setup_type == 'buy':
                if not (signal_candle['close'] > signal_candle['ema20'] and 
                        signal_candle['close'] > signal_candle['ema50'] and 
                        is_uptrend):
                    return None
            else:
                if not (signal_candle['close'] < signal_candle['ema20'] and 
                        signal_candle['close'] < signal_candle['ema50'] and 
                        is_downtrend):
                    return None

            # === BUILD SIGNAL ===
            entry = signal_candle['close']
            if setup_type == 'buy':
                sl = signal_candle['low']
                target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR
            else:
                sl = signal_candle['high']
                target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR

            return {
                'symbol': symbol.replace('.NS', ''),
                'date': target_date.strftime('%Y-%m-%d'),
                'direction': 'BUY' if setup_type == 'buy' else 'SELL',
                'setup_time': first_candle['time'].strftime('%H:%M'),
                'signal_time': df_15min_today.index[signal_idx].strftime('%H:%M'),
                'entry_price': round(entry, 2),
                'stop_loss': round(sl, 2),
                'target': round(target, 2),
                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                'fib_50': round(fib_50, 2),
                'fib_618': round(fib_618, 2),
                'swing_high': round(swing_high, 2) if setup_type == 'buy' else None,
                'swing_low': round(swing_low, 2) if setup_type == 'sell' else None,
                'ema20': round(signal_candle['ema20'], 2),
                'ema50': round(signal_candle['ema50'], 2),
                'trend': 'UP' if is_uptrend else 'DOWN'
            }

        except Exception as e:
            return None

# ================================================================================
# DISPLAY RESULTS
# ================================================================================
def display_results(signals, scan_date):
    if not signals:
        st.warning("⚠️ No signals found. No stocks met all Fibonacci retracement conditions.")
        return

    df = pd.DataFrame(signals)
    
    # Metrics
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

    # Display each signal as a card
    for _, row in df.iterrows():
        card_class = "buy-card" if row['direction'] == 'BUY' else "sell-card"
        st.markdown(f"""
        <div class="signal-card {card_class}">
            <h4>{row['symbol']} — {row['direction']} @ {row['entry_price']}</h4>
            <p><b>Setup:</b> {row['setup_time']} | <b>Signal:</b> {row['signal_time']} | <b>Trend:</b> {row['trend']}</p>
            <p><b>Entry:</b> {row['entry_price']} | <b>SL:</b> {row['stop_loss']} | <b>TGT:</b> {row['target']} | <b>R:R:</b> {row['risk_reward']}</p>
            <p><b>Fib 0.5:</b> {row['fib_50']} | <b>Fib 0.618:</b> {row['fib_618']} | <b>EMA20:</b> {row['ema20']} | <b>EMA50:</b> {row['ema50']}</p>
        </div>
        """, unsafe_allow_html=True)

    # Also show as dataframe for export
    with st.expander("📊 Export Data"):
        st.dataframe(df, hide_index=True, use_container_width=True)

# ================================================================================
# MAIN APP
# ================================================================================
def main():
    if not check_password():
        st.stop()

    st.markdown('<div class="main-header">📈 Open Drive Fib Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Scans 110 NSE stocks for Open=Low/High + Fib Retracement + EMA alignment</div>', unsafe_allow_html=True)

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
        tolerance = st.number_input("Open=High/Low Tolerance (₹)", value=0.05, min_value=0.01, max_value=1.0, step=0.01)
        
        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        st.info("🔌 Initializing 5-Pipe TradingView Connection...")
        tv_pool = get_tv_pool()

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.FIB_LEVEL_1 = fib_50
        strategy.config.FIB_LEVEL_2 = fib_618
        strategy.config.MIN_IMPULSE_PCT = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning for Fib Retracement Signals...")
            progress_bar = st.progress(0)
            status_text = st.empty()

        all_signals = []
        total = len(stock_list)
        completed = 0

        def process_stock(task_data):
            idx, sym, target_date = task_data
            tv_inst = tv_pool[idx % 5]
            time.sleep(random.uniform(0.1, 0.4))
            return strategy.scan_stock(tv_inst, sym, target_date, tolerance)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            tasks = [(i, sym, scan_date) for i, sym in enumerate(stock_list)]
            futures = {executor.submit(process_stock, task): task for task in tasks}
            
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

        progress_container.empty()
        display_results(all_signals, scan_date)

    # ---------------------------------------------------------
    # REAL-TIME SCAN
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
            tv_inst = tv_pool[idx % 5]
            time.sleep(random.uniform(0.1, 0.4))
            return strategy.scan_stock(tv_inst, sym, target_time, tolerance)

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

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                tasks = [(i, sym, live_datetime) for i, sym in enumerate(stock_list)]
                futures = {executor.submit(process_live, task): task for task in tasks}
                
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
