import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import random
import warnings
import logging
from tvDatafeed import TvDatafeed, Interval

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)
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

# Initialize TradingView Guest Connection
@st.cache_resource
def get_tv_connection():
    return TvDatafeed()

tv = get_tv_connection()


# ================================================================================
# SECURITY GATEWAY
# ================================================================================
def check_password():
    """Returns `True` if the user has entered the correct password."""
    
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Clear the password from memory securely
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show the password input box
        st.markdown('<div class="main-header">🔒 Private Access Only</div>', unsafe_allow_html=True)
        st.text_input("Enter Scanner Password", type="password", on_change=password_entered, key="password")
        return False
    
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input box again with an error
        st.markdown('<div class="main-header">🔒 Private Access Only</div>', unsafe_allow_html=True)
        st.text_input("Enter Scanner Password", type="password", on_change=password_entered, key="password")
        st.error("❌ Access Denied. Incorrect password.")
        return False
    
    else:
        # Password correct
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

    def get_stealth_historical_candles(self, symbol, resolution_minutes, scan_date, days_back=100):
        """Pulls exact chart data directly from TradingView."""
        try:
            if resolution_minutes == 5:
                tv_interval = Interval.in_5_minute
                bars_to_pull = days_back * 75  
            else:
                tv_interval = Interval.in_15_minute
                bars_to_pull = days_back * 25  

            formatted_symbol = symbol.replace('.NS', '')
            
            df = tv.get_hist(symbol=formatted_symbol, exchange='NSE', interval=tv_interval, n_bars=bars_to_pull)
            
            if df is None or df.empty:
                return pd.DataFrame()

            df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
            df.index = pd.to_datetime(df.index)
            
            if df.index.tz is None:
                df.index = df.index.tz_localize('Asia/Kolkata')
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
        
        if len(df) < 2:
            return False, False
        
        ema20_now = df['ema20'].iloc[-1]
        ema20_prev = df['ema20'].iloc[-2]
        ema50_now = df['ema50'].iloc[-1]
        ema50_prev = df['ema50'].iloc[-2]

        is_uptrend = (ema20_now > ema50_now) and (ema20_now > ema20_prev) and (ema50_now > ema50_prev)
        is_downtrend = (ema20_now < ema50_now) and (ema20_now < ema20_prev) and (ema50_now < ema50_prev)

        return is_uptrend, is_downtrend
    
    def is_hammer(self, candle):
        body = abs(candle['open'] - candle['close'])
        candle_range = candle['high'] - candle['low']

        if candle_range == 0 or candle_range < (candle['close'] * 0.001):
            return False

        if (body / candle_range) >= self.config.HAMMER_BODY_PCT:
            return False

        if candle['close'] >= candle['open']:
            upper_shadow = candle['high'] - candle['close']
            lower_shadow = candle['open'] - candle['low']
        else:
            upper_shadow = candle['high'] - candle['open']
            lower_shadow = candle['close'] - candle['low']

        max_shadow = max(upper_shadow, lower_shadow)
        return max_shadow >= (body * self.config.HAMMER_SHADOW_RATIO)

    def is_engulfing(self, prev, curr, direction):
        prev_body = abs(prev['open'] - prev['close'])
        curr_body = abs(curr['open'] - curr['close'])

        if direction == 'bullish':
            if not (curr['close'] > curr['open'] and prev['open'] > prev['close']): return False
            if curr_body <= prev_body: return False
            return (curr['open'] <= prev['close']) and (curr['close'] >= prev['open'])
        else:
            if not (curr['open'] > curr['close'] and prev['close'] > prev['open']): return False
            if curr_body <= prev_body: return False
            return (curr['close'] <= prev['open']) and (curr['open'] >= prev['close'])

    def is_harami(self, prev, curr, direction):
        prev_body = abs(prev['open'] - prev['close'])
        curr_body = abs(curr['open'] - curr['close'])

        if direction == 'bullish':
            if not (curr['close'] > curr['open'] and prev['open'] > prev['close']): return False
            if curr_body >= prev_body: return False
            return (curr['high'] <= prev['high']) and (curr['low'] >= prev['low'])
        else:
            if not (curr['open'] > curr['close'] and prev['close'] > prev['open']): return False
            if curr_body >= prev_body: return False
            return (curr['high'] <= prev['high']) and (curr['low'] >= prev['low'])

    def detect_pattern(self, df, idx, direction):
        if idx < 1: return None
        prev = df.iloc[idx - 1]
        curr = df.iloc[idx]

        if self.is_engulfing(prev, curr, direction): return 'engulfing'
        elif self.is_harami(prev, curr, direction): return 'harami'
        elif self.is_hammer(curr): return 'hammer'
        return None

    def check_first_5min_candle(self, df_5min):
        if df_5min.empty: return None, {}
        first_candle = df_5min.iloc[0]
        open_price = first_candle['open']
        high_price = first_candle['high']
        low_price = first_candle['low']
        close_price = first_candle['close']

        if abs(open_price - low_price) <= 0.05:
            return 'buy', {
                'open': open_price, 'high': high_price, 'low': low_price,
                'close': close_price, 'time': df_5min.index[0].strftime('%H:%M')
            }
        elif abs(open_price - high_price) <= 0.05:
            return 'sell', {
                'open': open_price, 'high': high_price, 'low': low_price,
                'close': close_price, 'time': df_5min.index[0].strftime('%H:%M')
            }
        return None, {}

    def scan_stock(self, symbol, scan_date, progress_bar=None, status_text=None, current_idx=0, total=1):
        signals = []
        try:
            # 100 days for perfect EMA stabilization
            df_5min = self.get_stealth_historical_candles(symbol, 5, scan_date, days_back=100)
            df_15min = self.get_stealth_historical_candles(symbol, 15, scan_date, days_back=100)

            if df_5min.empty or df_15min.empty: return signals

            return self._evaluate_signals(symbol, scan_date, df_5min, df_15min)
        except Exception as e:
            return signals

    def _evaluate_signals(self, symbol, date, df_5min, df_15min):
        signals = []
        
        df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
        df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

        target_date = date.date() if hasattr(date, 'date') else date
        df_5min_today = df_5min[df_5min.index.date == target_date]
        
        setup_type, first_candle_info = self.check_first_5min_candle(df_5min_today)

        if setup_type is None: return signals

        buy_signal_fired = False
        sell_signal_fired = False

        today_indices = [i for i, dt in enumerate(df_15min.index) if dt.date() == target_date]

        for i in today_indices:
            if i < 1: continue 

            candle = df_15min.iloc[i]
            if pd.isna(candle['ema20']) or pd.isna(candle['ema50']): continue

            df_upto_now = df_15min.iloc[:i+1]
            is_uptrend, is_downtrend = self.check_trend(df_upto_now)

            if setup_type == 'buy' and not buy_signal_fired:
                price_above_emas = (candle['close'] > candle['ema20']) and (candle['close'] > candle['ema50'])
                if not (price_above_emas and is_uptrend): continue

                pattern = self.detect_pattern(df_15min, i, 'bullish')
                if pattern:
                    entry = candle['close']
                    sl = entry * (1 - self.config.DEFAULT_SL_PCT / 100)
                    target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR

                    signals.append({
                        'symbol': symbol.replace('.NS', ''),
                        'date': target_date.strftime('%Y-%m-%d'),
                        'direction': 'BUY',
                        'pattern': f'Bullish {pattern.title()}',
                        'entry_time': df_15min.index[i].strftime('%H:%M'),
                        'entry_price': round(entry, 2),
                        'stop_loss': round(sl, 2),
                        'target': round(target, 2),
                        'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}"
                    })
                    buy_signal_fired = True

            elif setup_type == 'sell' and not sell_signal_fired:
                price_below_emas = (candle['close'] < candle['ema20']) and (candle['close'] < candle['ema50'])
                if not (price_below_emas and is_downtrend): continue

                pattern = self.detect_pattern(df_15min, i, 'bearish')
                if pattern:
                    entry = candle['close']
                    sl = entry * (1 + self.config.DEFAULT_SL_PCT / 100)
                    target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR

                    signals.append({
                        'symbol': symbol.replace('.NS', ''),
                        'date': target_date.strftime('%Y-%m-%d'),
                        'direction': 'SELL',
                        'pattern': f'Bearish {pattern.title()}',
                        'entry_time': df_15min.index[i].strftime('%H:%M'),
                        'entry_price': round(entry, 2),
                        'stop_loss': round(sl, 2),
                        'target': round(target, 2),
                        'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}"
                    })
                    sell_signal_fired = True

        return signals

def display_results(all_signals, scan_date):
    if all_signals:
        df_signals = pd.DataFrame(all_signals)
        df_signals = df_signals.sort_values(['direction', 'entry_time'])

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f'<div class="metric-card"><h3>{len(df_signals)}</h3><p>Total Signals</p></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);"><h3>{len(df_signals[df_signals["direction"] == "BUY"])}</h3><p>BUY Signals</p></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="metric-card" style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);"><h3>{len(df_signals[df_signals["direction"] == "SELL"])}</h3><p>SELL Signals</p></div>', unsafe_allow_html=True)

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
    # 🚨 ACTIVATE SECURITY GATEWAY 🚨
    if not check_password():
        st.stop()  # Completely halts the app until the password is correct

    st.markdown('<div class="main-header">📈 Open Drive Strategy Scanner</div>', unsafe_allow_html=True)
    # ... the rest of your main() code continues normally below this ...
    st.markdown('<div class="main-header">📈 Open Drive Strategy Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Filter 200+ NSE stocks using your Open=Low/High + 15min Pattern + EMA Strategy</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Data Source")
        scan_mode = st.radio("Select Engine Mode:", ["Historical Scan", "Real-Time Scan (TV Polling)"])
        
        st.markdown("---")
        st.header("⚙️ Settings")

        scan_date = st.date_input(
            "Select Date (For Historical Only)",
            value=datetime.now() - timedelta(days=1),
            max_value=datetime.now()
        )

        st.markdown("---")
        st.subheader("📋 Stock List")
        input_method = st.radio("Choose input method:", ["Paste Symbols", "Use Default List"])

        # Formatted the massive list with \n for the text box
        default_symbols = (
            "HDFCBANK.NS\nAXISBANK.NS\nICICIBANK.NS\nKOTAKBANK.NS\nRBLBANK.NS\n"
            "FEDERALBANK.NS\nBANDHANBANK.NS\nAUBANK.NS\nINDUSINDBANK.NS\nIDFCFIRSTBANK.NS\n"
            "SBIN.NS\nBANKBARODA.NS\nCANBANK.NS\nPNB.NS\nABCAPITAL.NS\n"
            "ANGELONE.NS\nBAJAJFINSERV.NS\nBAJAJFINANCE.NS\nBSE.NS\nCDSL.NS\n"
            "HDFCAMC.NS\nJIOFIN.NS\nLICHOUSING.NS\nLICI.NS\nMANAPURAM.NS\n"
            "MCX.NS\nPFC.NS\nREC.NS\nSHRIRAMFINANCE.NS\nHCLTECH.NS\n"
            "INFY.NS\nLTM.NS\nTCS.NS\nTECHM.NS\nWIPRO.NS\n"
            "HINDALCO.NS\nHINDZINC.NS\nNATIONALALUMINUM.NS\nNMDC.NS\nSAIL.NS\n"
            "TATASTEEL.NS\nVEDL.NS\nDLF.NS\nOBEROIREALITY.NS\nBRITANNIA.NS\n"
            "COLPAL.NS\nDABUR.NS\nHINDUNILVR.NS\nMARICO.NS\nTATACONSUMER.NS\n"
            "BPCL.NS\nCOALINDIA.NS\nGAIL.NS\nHINDPETRO.NS\nIOC.NS\n"
            "OIL.NS\nONGC.NS\nRELIANCE.NS\nASHOKLEY.NS\nBAJAJAUTO.NS\n"
            "BHARATFORG.NS\nEICHER.NS\nEXIDE.NS\nHEROMOTO.NS\nM&M.NS\n"
            "MARUTI.NS\nTMPV.NS\nTVSMOTOR.NS\nASIANPAINT.NS\nCROMPTON.NS\n"
            "HAVELLS.NS\nTITAN.NS\nVOLTAS.NS\nAPOLLOHOSPITAL.NS\nAUROPHARMA.NS\n"
            "BIOCON.NS\nDRREDDY.NS\nLAURUSLAB.NS\nLUPIN.NS\nSUNPHARMA.NS\n"
            "SRF.NS\nSOLARINDUSTRY.NS\nAMBUJACEMENT.NS\nGRASIM.NS\nLT.NS\n"
            "NBCC.NS\nULTRATECH.NS\nABB.NS\nASTRAL.NS\nBEL.NS\n"
            "BHEL.NS\nCGPOWER.NS\nCUMMINS.NS\nHAL.NS\nKEI.NS\n"
            "POLYCAB.NS\nPOWERINDIA.NS\nETERNAL.NS\nINDHOTEL.NS\nNYKAA.NS\n"
            "TRENT.NS\nNTPC.NS\nTATAPOWER.NS\nPOWERGRID.NS\nADANIPORTS.NS\n"
            "DELHIVERY.NS\nCONCOR.NS\nGMR.NS\nINDIGO.NS\nBHARTIAIRTEL.NS"
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
    # ROUTE 1: HISTORICAL SCAN
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        strategy = OpenDriveStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.DEFAULT_SL_PCT = sl_pct
        strategy.config.DEFAULT_TARGET_RR = target_rr

        progress_container = st.container()
        
        with progress_container:
            st.subheader("⏳ Scraping Historical Data...")
            progress_bar = st.progress(0)
            status_text = st.empty()

        all_signals = []
        total = len(stock_list)

        for i, symbol in enumerate(stock_list):
            progress_bar.progress((i + 1) / total)
            status_text.text(f"Scanning: {symbol} ({i+1}/{total})")
            
            signals = strategy.scan_stock(symbol, scan_date, progress_bar, status_text, i, total)
            all_signals.extend(signals)
            
            # Anti-ban sleep
            time.sleep(random.uniform(0.5, 1.0))

        progress_container.empty()
        display_results(all_signals, scan_date)

    # ---------------------------------------------------------
    # ROUTE 2: REAL-TIME TRADINGVIEW POLLING ENGINE
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan (TV Polling)":
        st.markdown('<div style="text-align:center;"><span class="live-badge">🔴 LIVE TRADINGVIEW CONNECTION ACTIVE</span></div>', unsafe_allow_html=True)
        
        strategy = OpenDriveStrategy()
        strategy.config.EMA_FAST = ema_fast
        strategy.config.EMA_SLOW = ema_slow
        strategy.config.DEFAULT_SL_PCT = sl_pct
        strategy.config.DEFAULT_TARGET_RR = target_rr
        
        live_container = st.empty()
        
        # Infinite Live Polling Loop
        while True:
            all_signals = []
            live_datetime = datetime.now()
            #live_datetime = datetime(2026, 5, 4, 10, 20)
            
            # Smart Market Hours Detector (09:15 to 15:30 IST)
            current_time = live_datetime.time()
            is_market_open = (
                live_datetime.weekday() < 5 and 
                (current_time >= datetime.strptime("09:15", "%H:%M").time() and 
                 current_time <= datetime.strptime("15:30", "%H:%M").time())
            )

            for symbol in stock_list:
                df_5min = strategy.get_stealth_historical_candles(symbol, 5, live_datetime, days_back=100)
                df_15min = strategy.get_stealth_historical_candles(symbol, 15, live_datetime, days_back=100)
                
                if not df_5min.empty and not df_15min.empty:
                    signals = strategy._evaluate_signals(symbol, live_datetime, df_5min, df_15min)
                    all_signals.extend(signals)
                
                # 🚨 THE ANTI-BAN SHIELD 🚨
                # Micro-sleeps between 0.8 and 1.8 seconds per stock
                time.sleep(random.uniform(0.8, 1.8))
                
            with live_container.container():
                market_status = "🟢 Market Open" if is_market_open else "🔴 Market Closed"
                st.write(f"⏱️ Last Updated: {live_datetime.strftime('%H:%M:%S IST')} | Status: {market_status} (Auto-refreshing...)")
                display_results(all_signals, live_datetime)
                
            # IP Risk Mitigation: If the market is closed, sleep for 60 seconds to avoid wasting server hits.
            if not is_market_open:
                time.sleep(60) 
            else:
                time.sleep(5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
