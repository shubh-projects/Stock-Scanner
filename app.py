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

# 🚨 Mute Streamlit and tvDatafeed warnings/connection drops
warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

# 🚨 Global IST Timezone
IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# PAGE UI & CSS
# ================================================================================
st.set_page_config(
    page_title="Open Drive Strategy Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .live-badge {
        background-color: #ff4b4b;
        color: white;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.8rem;
        font-weight: bold;
        animation: blink 2s infinite;
    }
    @keyframes blink {
        0% { opacity: 1; }
        50% { opacity: 0.4; }
        100% { opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# 🚨 THE FIX: STAGGERED CONNECTION POOL
@st.cache_resource
def get_tv_pool():
    """Spins up 5 WebSocket connections slowly to prevent Cloudflare blocks."""
    pool = []
    for i in range(5):
        pool.append(TvDatafeed())
        time.sleep(1.5) # The magic stagger that bypasses the firewall
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
# STRATEGY LOGIC
# ================================================================================
class Config:
    EMA_FAST = 20
    EMA_SLOW = 50
    HAMMER_BODY_PCT = 0.30
    HAMMER_SHADOW_RATIO = 2.0
    DEFAULT_SL_PCT = 0.30
    DEFAULT_TARGET_RR = 2.0

class OpenDriveStrategy:
    def __init__(self):
        self.config = Config()

    def get_stealth_historical_candles(self, tv_instance, symbol, resolution_minutes, is_live=False, days_back=100):
        try:
            if resolution_minutes == 5:
                tv_interval = Interval.in_5_minute
                bars_to_pull = 500 if is_live else (days_back * 75)
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
                # 🚨 THE FIX: Acknowledge the data is UTC first, THEN convert to IST
                df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
                
            return df
        except Exception as e:
            return pd.DataFrame()
    
    def calculate_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def check_trend(self, df):
        df = df.copy()
        df['ema20'] = self.calculate_ema(df, self.config.EMA_FAST)
        df['ema50'] = self.calculate_ema(df, self.config.EMA_SLOW)
        
        if len(df) < 2: return False, False
        
        ema20_now, ema20_prev = df['ema20'].iloc[-1], df['ema20'].iloc[-2]
        ema50_now, ema50_prev = df['ema50'].iloc[-1], df['ema50'].iloc[-2]

        is_uptrend = (ema20_now > ema50_now) and (ema20_now > ema20_prev) and (ema50_now > ema50_prev)
        is_downtrend = (ema20_now < ema50_now) and (ema20_now < ema20_prev) and (ema50_now < ema50_prev)

        return is_uptrend, is_downtrend
    
    def is_hammer(self, candle):
        body = abs(candle['open'] - candle['close'])
        candle_range = candle['high'] - candle['low']
        if candle_range == 0 or candle_range < (candle['close'] * 0.001): return False
        if (body / candle_range) >= self.config.HAMMER_BODY_PCT: return False

        if candle['close'] >= candle['open']:
            max_shadow = max(candle['high'] - candle['close'], candle['open'] - candle['low'])
        else:
            max_shadow = max(candle['high'] - candle['open'], candle['close'] - candle['low'])
            
        return max_shadow >= (body * self.config.HAMMER_SHADOW_RATIO)

    def is_engulfing(self, prev, curr, direction):
        prev_body, curr_body = abs(prev['open'] - prev['close']), abs(curr['open'] - curr['close'])
        if direction == 'bullish':
            if not (curr['close'] > curr['open'] and prev['open'] > prev['close']): return False
            return (curr_body > prev_body) and (curr['open'] <= prev['close']) and (curr['close'] >= prev['open'])
        else:
            if not (curr['open'] > curr['close'] and prev['close'] > prev['open']): return False
            return (curr_body > prev_body) and (curr['close'] <= prev['open']) and (curr['open'] >= prev['close'])

    def is_harami(self, prev, curr, direction):
        prev_body, curr_body = abs(prev['open'] - prev['close']), abs(curr['open'] - curr['close'])
        if direction == 'bullish':
            if not (curr['close'] > curr['open'] and prev['open'] > prev['close']): return False
            return (curr_body < prev_body) and (curr['high'] <= prev['high']) and (curr['low'] >= prev['low'])
        else:
            if not (curr['open'] > curr['close'] and prev['close'] > prev['open']): return False
            return (curr_body < prev_body) and (curr['high'] <= prev['high']) and (curr['low'] >= prev['low'])

    def detect_pattern(self, df, idx, direction):
        if idx < 1: return None
        prev, curr = df.iloc[idx - 1], df.iloc[idx]

        if self.is_engulfing(prev, curr, direction): return 'engulfing'
        elif self.is_harami(prev, curr, direction): return 'harami'
        elif self.is_hammer(curr): return 'hammer'
        return None

    def check_first_5min_candle(self, df_5min):
        if df_5min.empty: return None, {}
        first_candle = df_5min.iloc[0]
        o, h, l, c = first_candle['open'], first_candle['high'], first_candle['low'], first_candle['close']

        if abs(o - l) <= 0.05:
            return 'buy', {'open': o, 'high': h, 'low': l, 'close': c, 'time': df_5min.index[0].strftime('%H:%M')}
        elif abs(o - h) <= 0.05:
            return 'sell', {'open': o, 'high': h, 'low': l, 'close': c, 'time': df_5min.index[0].strftime('%H:%M')}
        return None, {}

    # 🚨 LAZY LOADING APPLIED HERE
    def scan_stock(self, tv_instance, symbol, scan_date, progress_bar=None, status_text=None, current_idx=0, total=1):
        try:
            df_5min = self.get_stealth_historical_candles(tv_instance, symbol, 5, is_live=False, days_back=100)
            df_15min = self.get_stealth_historical_candles(tv_instance, symbol, 15, is_live=False, days_back=100)
            if df_5min.empty or df_15min.empty: return []
            return self._evaluate_signals(symbol, scan_date, df_5min, df_15min)
        except Exception:
            return []

    def _evaluate_signals(self, symbol, date, df_5min, df_15min):
        signals = []
        df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
        df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

        target_date = date.date() if hasattr(date, 'date') else date
        df_5min_today = df_5min[df_5min.index.date == target_date]
        
        setup_type, first_candle_info = self.check_first_5min_candle(df_5min_today)
        if setup_type is None: return signals

        buy_signal_fired, sell_signal_fired = False, False
        today_indices = [i for i, dt in enumerate(df_15min.index) if dt.date() == target_date]

        for i in today_indices:
            if i < 1: continue 

            candle = df_15min.iloc[i]
            if pd.isna(candle['ema20']) or pd.isna(candle['ema50']): continue

            is_uptrend, is_downtrend = self.check_trend(df_15min.iloc[:i+1])

            if setup_type == 'buy' and not buy_signal_fired:
                if not ((candle['close'] > candle['ema20'] and candle['close'] > candle['ema50']) and is_uptrend): continue
                pattern = self.detect_pattern(df_15min, i, 'bullish')
                if pattern:
                    entry = candle['close']
                    sl = entry * (1 - self.config.DEFAULT_SL_PCT / 100)
                    target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR
                    signals.append({'symbol': symbol.replace('.NS', ''), 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY', 'pattern': f'Bullish {pattern.title()}', 'entry_time': df_15min.index[i].strftime('%H:%M'), 'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2), 'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}"})
                    buy_signal_fired = True

            elif setup_type == 'sell' and not sell_signal_fired:
                if not ((candle['close'] < candle['ema20'] and candle['close'] < candle['ema50']) and is_downtrend): continue
                pattern = self.detect_pattern(df_15min, i, 'bearish')
                if pattern:
                    entry = candle['close']
                    sl = entry * (1 + self.config.DEFAULT_SL_PCT / 100)
                    target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR
                    signals.append({'symbol': symbol.replace('.NS', ''), 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL', 'pattern': f'Bearish {pattern.title()}', 'entry_time': df_15min.index[i].strftime('%H:%M'), 'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2), 'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}"})
                    sell_signal_fired = True
        return signals

def display_results(all_signals, scan_date):
    if all_signals:
        df_signals = pd.DataFrame(all_signals).sort_values(['direction', 'entry_time'])
        col1, col2, col3 = st.columns(3)
        with col1: st.markdown(f'<div class="metric-card"><h3>{len(df_signals)}</h3><p>Total Signals</p></div>', unsafe_allow_html=True)
        with col2: st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);"><h3>{len(df_signals[df_signals["direction"] == "BUY"])}</h3><p>BUY Signals</p></div>', unsafe_allow_html=True)
        with col3: st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);"><h3>{len(df_signals[df_signals["direction"] == "SELL"])}</h3><p>SELL Signals</p></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("📋 Signal Details")
        display_df = df_signals[['symbol', 'direction', 'pattern', 'entry_time', 'entry_price', 'stop_loss', 'target']].copy()
        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.warning("⚠️ No signals found for the current data criteria.")

# ================================================================================
# MAIN STREAMLIT APP
# ================================================================================
def main():
    if not check_password():
        st.stop()

    st.markdown('<div class="main-header">📈 Open Drive Strategy Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Filter 110 NSE stocks using your Open=Low/High + 15min Pattern + EMA Strategy</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Data Source")
        scan_mode = st.radio("Select Engine Mode:", ["Historical Scan", "Real-Time Scan (TV Polling)"])
        st.markdown("---")
        st.header("⚙️ Settings")
        
        scan_date = st.date_input("Select Date (For Historical Only)", value=datetime.now(IST) - timedelta(days=1), max_value=datetime.now(IST))
        st.markdown("---")
        
        st.subheader("📋 Stock List")
        input_method = st.radio("Choose input method:", ["Paste Symbols", "Use Default List"])

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
        sl_pct = st.number_input("Stop Loss %", value=0.30, min_value=0.05, max_value=5.0, step=0.05)
        target_rr = st.number_input("Risk:Reward Ratio", value=2.0, min_value=1.0, max_value=5.0, step=0.5)
        st.markdown("---")
        
        button_label = "🚀 Start Scan" if scan_mode == "Historical Scan" else "⚡ Launch Live Dashboard"
        scan_button = st.button(button_label, type="primary", width="stretch")

    # ---------------------------------------------------------
    # ROUTE 1: HISTORICAL SCAN (MULTI-PIPE THREADING)
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        st.info("🔌 Initializing Secure 5-Pipe TradingView Connection... (Takes ~7 seconds)")
        tv_pool = get_tv_pool()

        strategy = OpenDriveStrategy()
        strategy.config.EMA_FAST, strategy.config.EMA_SLOW = ema_fast, ema_slow
        strategy.config.DEFAULT_SL_PCT, strategy.config.DEFAULT_TARGET_RR = sl_pct, target_rr

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scraping Deep Historical Data...")
            progress_bar = st.progress(0)
            status_text = st.empty()

        all_signals = []
        total = len(stock_list)
        completed = 0

        # 🚨 THE HISTORICAL WORKER FUNCTION
        def process_historical_stock(task_data):
            idx, sym, target_date = task_data
            tv_inst = tv_pool[idx % 5] # Distribute across the 5 pipes
            
            # Anti-ban staggering
            time.sleep(random.uniform(0.1, 0.4)) 
            
            # is_live=False triggers the massive 100-day payload (7,500 candles per stock)
            d5 = strategy.get_stealth_historical_candles(tv_inst, sym, 5, is_live=False, days_back=100)
            d15 = strategy.get_stealth_historical_candles(tv_inst, sym, 15, is_live=False, days_back=100)
            
            if not d5.empty and not d15.empty:
                return strategy._evaluate_signals(sym, target_date, d5, d15)
            return []

        # 🚨 5 WORKERS, 5 PIPES FOR MASSIVE DATA EXTRACTION
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            tasks = [(i, sym, scan_date) for i, sym in enumerate(stock_list)]
            futures = {executor.submit(process_historical_stock, task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                
                # UI Throttle: Update screen every 5 stocks to prevent rendering lag
                if completed % 5 == 0 or completed == total:
                    progress_bar.progress(completed / total)
                    status_text.text(f"⚡ Multi-Pipe Historical Scraping... ({completed}/{total})")
                
                try:
                    res = future.result()
                    if res: all_signals.extend(res)
                except Exception:
                    pass

        progress_container.empty()
        display_results(all_signals, scan_date)

    # ---------------------------------------------------------
    # ROUTE 2: REAL-TIME ENGINE (MULTI-PIPE THREADING)
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan (TV Polling)":
        st.markdown('<div style="text-align:center;"><span class="live-badge">🔴 INITIALIZING 5-PIPE CONNECTION POOL... (Takes ~7 Sec)</span></div>', unsafe_allow_html=True)
        
        # 🚨 LAZY LOAD: Connects only after you click Launch
        tv_pool = get_tv_pool()
        st.markdown('<div style="text-align:center;"><span class="live-badge" style="background-color: #28a745;">🟢 LIVE POLLING ACTIVE</span></div>', unsafe_allow_html=True)

        strategy = OpenDriveStrategy()
        strategy.config.EMA_FAST, strategy.config.EMA_SLOW = ema_fast, ema_slow
        strategy.config.DEFAULT_SL_PCT, strategy.config.DEFAULT_TARGET_RR = sl_pct, target_rr
        
        live_container = st.empty()
        
        def process_live_stock(task_data):
            idx, sym, target_time = task_data
            tv_inst = tv_pool[idx % 5] 
            
            time.sleep(random.uniform(0.1, 0.4)) 
            
            d5 = strategy.get_stealth_historical_candles(tv_inst, sym, 5, is_live=True)
            d15 = strategy.get_stealth_historical_candles(tv_inst, sym, 15, is_live=True)
            
            if not d5.empty and not d15.empty:
                return strategy._evaluate_signals(sym, target_time, d5, d15)
            return []

        while True:
            all_signals = []
            live_datetime = datetime.now(IST)
            current_time = live_datetime.time()
            
            is_market_open = (live_datetime.weekday() < 5 and 
                (current_time >= datetime.strptime("09:15", "%H:%M").time() and 
                 current_time <= datetime.strptime("15:30", "%H:%M").time()))

            progress_container = st.empty()
            with progress_container.container():
                live_progress = st.progress(0)
                live_status = st.empty()

            total = len(stock_list)
            completed = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                tasks = [(i, sym, live_datetime) for i, sym in enumerate(stock_list)]
                futures = {executor.submit(process_live_stock, task): task for task in tasks}
                
                for future in concurrent.futures.as_completed(futures):
                    completed += 1
                    
                    if completed % 5 == 0 or completed == total:
                        live_progress.progress(completed / total)
                        live_status.text(f"⚡ Multi-Pipe Engine Processing... ({completed}/{total})")
                    
                    try:
                        res = future.result()
                        if res: all_signals.extend(res)
                    except Exception:
                        pass
                
            progress_container.empty()

            with live_container.container():
                market_status = "🟢 Market Open" if is_market_open else "🔴 Market Closed"
                st.write(f"⏱️ Last Updated: {datetime.now(IST).strftime('%H:%M:%S IST')} | Status: {market_status} (Auto-refreshing...)")
                display_results(all_signals, live_datetime)
                
            if not is_market_open:
                time.sleep(60) 
            else:
                time.sleep(2)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
