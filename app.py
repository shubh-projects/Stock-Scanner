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
    .signal-card { padding: 1rem; border-radius: 8px; margin: 0.5rem 0; border-left: 4px solid; }
    .buy-card { background: linear-gradient(135deg, #1a5f5f 0%, #2d8a8a 100%) !important; color: white; border-left-color: #4CAF50; }
    .sell-card { background: linear-gradient(135deg, #7a1f1f 0%, #a03030 100%) !important; color: white; border-left-color: #f44336; }
    .debug-log { font-family: 'Courier New', monospace; font-size: 0.82rem; line-height: 1.35; white-space: pre-wrap; background: #1e1e1e; color: #d4d4d4; padding: 1rem; border-radius: 6px; }
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
# STRATEGY LOGIC — EXACT PINE SCRIPT MIRROR WITH [1] BAR GAP
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
        debug_lines = []
        def log(msg):
            debug_lines.append(msg)

        try:
            df_5min = self.get_historical_candles(tv_instance, symbol, 5, is_live=False, days_back=100)
            df_15min = self.get_historical_candles(tv_instance, symbol, 15, is_live=False, days_back=100)

            if df_5min.empty or df_15min.empty:
                return None, "\n".join(debug_lines)

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

            # =================================================================
            # CRITICAL: Calculate EMAs on FULL history first for proper warmup
            # =================================================================
            df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
            df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

            # Now filter to target date AND market hours (9:15 onwards)
            df_5min_today = df_5min[
                (df_5min.index.date == target_date) & 
                (df_5min.index.time >= pd.Timestamp('09:15').time())
            ].copy()
            df_15min_today = df_15min[
                (df_15min.index.date == target_date) & 
                (df_15min.index.time >= pd.Timestamp('09:15').time())
            ].copy()

            if df_5min_today.empty or df_15min_today.empty:
                log(f"[{symbol}] No market-hours data for {target_date}")
                return None, "\n".join(debug_lines)

            # Get first 5m candle (strictly 9:15-9:20)
            first_5m = df_5min_today.iloc[0]
            first5_open = float(first_5m['open'])
            first5_high = float(first_5m['high'])
            first5_low = float(first_5m['low'])

            # ADAPTIVE TOLERANCE based on price
            price = first5_open if first5_open > 0 else 1.0
            tolerance = price * (tolerance_pct / 100)

            # Check setup
            first5_is_buy_setup = abs(first5_open - first5_low) <= tolerance
            first5_is_sell_setup = abs(first5_open - first5_high) <= tolerance

            sym_clean = symbol.replace('.NS', '')
            debug_mode = sym_clean in ['SAIL', 'DABUR', 'TCS']

            if debug_mode:
                log(f"{'='*60}")
                log(f"DEBUG {sym_clean} on {target_date}")
                log(f"  First 5m: O={first5_open:.2f} H={first5_high:.2f} L={first5_low:.2f}")
                log(f"  abs(O-L)={abs(first5_open-first5_low):.4f} | abs(O-H)={abs(first5_open-first5_high):.4f}")
                log(f"  Tolerance%: {tolerance_pct}% | Tolerance₹: {tolerance:.4f}")
                log(f"  Buy setup: {first5_is_buy_setup} | Sell setup: {first5_is_sell_setup}")

            if not first5_is_buy_setup and not first5_is_sell_setup:
                if debug_mode:
                    log(f"  ❌ NO SETUP")
                    log(f"{'='*60}")
                return None, "\n".join(debug_lines)

            # === STATE VARIABLES ===
            buy_swing_high = None
            buy_fib_50 = None
            buy_fib_618 = None
            buy_impulse_done = False
            buy_impulse_bar = -1
            buy_retraced = False
            buy_retrace_bar = -1
            buy_signal_fired = False

            sell_swing_low = None
            sell_fib_50 = None
            sell_fib_618 = None
            sell_impulse_done = False
            sell_impulse_bar = -1
            sell_retraced = False
            sell_retrace_bar = -1
            sell_signal_fired = False

            buy_setup_invalid = False
            sell_setup_invalid = False

            signal = None

            # =================================================================
            # MAIN LOOP — DO NOT SKIP i=0
            # =================================================================
            for i, (idx, row) in enumerate(df_15min_today.iterrows()):

                # === SETUP INVALIDATION — ONLY BEFORE IMPULSE COMPLETES ===
                if first5_is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if row['low'] < first5_low:
                        buy_setup_invalid = True
                        if debug_mode:
                            log(f"  ❌ BUY INVALIDATED at bar {i} {idx.strftime('%H:%M')} (low={row['low']:.2f} < first5_low={first5_low:.2f})")

                if first5_is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if row['high'] > first5_high:
                        sell_setup_invalid = True
                        if debug_mode:
                            log(f"  ❌ SELL INVALIDATED at bar {i} {idx.strftime('%H:%M')} (high={row['high']:.2f} > first5_high={first5_high:.2f})")

                # === BUY IMPULSE ===
                if first5_is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                    if buy_swing_high is None:
                        buy_swing_high = float(row['high'])
                    else:
                        buy_swing_high = max(buy_swing_high, float(row['high']))

                    impulse_threshold = first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100)
                    if buy_swing_high >= impulse_threshold:
                        buy_impulse_done = True
                        buy_impulse_bar = i
                        buy_fib_50 = buy_swing_high - self.config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                        buy_fib_618 = buy_swing_high - self.config.FIB_LEVEL_2 * (buy_swing_high - first5_low)
                        if debug_mode:
                            log(f"  📈 BUY IMPULSE DONE at bar {i} {idx.strftime('%H:%M')} | swing_high={buy_swing_high:.2f} | fib50={buy_fib_50:.2f}")

                # === SELL IMPULSE ===
                if first5_is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                    if sell_swing_low is None:
                        sell_swing_low = float(row['low'])
                    else:
                        sell_swing_low = min(sell_swing_low, float(row['low']))

                    impulse_threshold = first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100)
                    if sell_swing_low <= impulse_threshold:
                        sell_impulse_done = True
                        sell_impulse_bar = i
                        sell_fib_50 = sell_swing_low + self.config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                        sell_fib_618 = sell_swing_low + self.config.FIB_LEVEL_2 * (first5_high - sell_swing_low)
                        if debug_mode:
                            log(f"  📉 SELL IMPULSE DONE at bar {i} {idx.strftime('%H:%M')} | swing_low={sell_swing_low:.2f} | fib50={sell_fib_50:.2f}")

                # === BUY RETRACEMENT (bar AFTER impulse) ===
                if buy_impulse_done and not buy_retraced and i > buy_impulse_bar:
                    if row['low'] <= buy_fib_50:
                        buy_retraced = True
                        buy_retrace_bar = i
                        if debug_mode:
                            log(f"  🔄 BUY RETRACED at bar {i} {idx.strftime('%H:%M')} | low={row['low']:.2f} <= fib50={buy_fib_50:.2f}")

                # === SELL RETRACEMENT (bar AFTER impulse) ===
                if sell_impulse_done and not sell_retraced and i > sell_impulse_bar:
                    if row['high'] >= sell_fib_50:
                        sell_retraced = True
                        sell_retrace_bar = i
                        if debug_mode:
                            log(f"  🔄 SELL RETRACED at bar {i} {idx.strftime('%H:%M')} | high={row['high']:.2f} >= fib50={sell_fib_50:.2f}")

                # === BUY RECOVERY (bar AFTER retrace) ===
                if buy_retraced and not buy_signal_fired and i > buy_retrace_bar:
                    is_green = row['close'] > row['open']
                    
                    if debug_mode:
                        is_uptrend_dbg, _ = self.check_trend(df_15min_today.iloc[:i+1])
                        price_above_dbg = (row['close'] > row['ema20']) and (row['close'] > row['ema50'])
                        log(f"  [BUY SIG CHECK bar {i} {idx.strftime('%H:%M')}] green={is_green} close>{buy_fib_50:.2f}={row['close'] > buy_fib_50} aboveEMAs={price_above_dbg} uptrend={is_uptrend_dbg}")

                    if is_green and row['close'] > buy_fib_50:
                        is_uptrend, _ = self.check_trend(df_15min_today.iloc[:i+1])
                        price_above_emas = (row['close'] > row['ema20']) and (row['close'] > row['ema50'])

                        if price_above_emas and is_uptrend:
                            entry = float(row['close'])
                            sl = float(row['low'])
                            target = entry + (entry - sl) * self.config.DEFAULT_TARGET_RR

                            signal = {
                                'symbol': sym_clean,
                                'date': target_date.strftime('%Y-%m-%d'),
                                'direction': 'BUY',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2),
                                'stop_loss': round(sl, 2),
                                'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50': round(buy_fib_50, 2),
                                'fib_618': round(buy_fib_618, 2),
                                'swing_high': round(buy_swing_high, 2),
                                'ema20': round(float(row['ema20']), 2),
                                'ema50': round(float(row['ema50']), 2),
                                'trend': 'UP'
                            }
                            buy_signal_fired = True
                            if debug_mode:
                                log(f"  ✅ BUY SIGNAL FIRED at bar {i} {idx.strftime('%H:%M')} | entry={entry:.2f}")
                            break

                # === SELL RESUMPTION (bar AFTER retrace) ===
                if sell_retraced and not sell_signal_fired and i > sell_retrace_bar:
                    is_red = row['close'] < row['open']
                    
                    if debug_mode:
                        _, is_downtrend_dbg = self.check_trend(df_15min_today.iloc[:i+1])
                        price_below_dbg = (row['close'] < row['ema20']) and (row['close'] < row['ema50'])
                        log(f"  [SELL SIG CHECK bar {i} {idx.strftime('%H:%M')}] red={is_red} close<{sell_fib_50:.2f}={row['close'] < sell_fib_50} belowEMAs={price_below_dbg} downtrend={is_downtrend_dbg}")

                    if is_red and row['close'] < sell_fib_50:
                        _, is_downtrend = self.check_trend(df_15min_today.iloc[:i+1])
                        price_below_emas = (row['close'] < row['ema20']) and (row['close'] < row['ema50'])

                        if price_below_emas and is_downtrend:
                            entry = float(row['close'])
                            sl = float(row['high'])
                            target = entry - (sl - entry) * self.config.DEFAULT_TARGET_RR

                            signal = {
                                'symbol': sym_clean,
                                'date': target_date.strftime('%Y-%m-%d'),
                                'direction': 'SELL',
                                'setup_time': df_5min_today.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(entry, 2),
                                'stop_loss': round(sl, 2),
                                'target': round(target, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50': round(sell_fib_50, 2),
                                'fib_618': round(sell_fib_618, 2),
                                'swing_low': round(sell_swing_low, 2),
                                'ema20': round(float(row['ema20']), 2),
                                'ema50': round(float(row['ema50']), 2),
                                'trend': 'DOWN'
                            }
                            sell_signal_fired = True
                            if debug_mode:
                                log(f"  ✅ SELL SIGNAL FIRED at bar {i} {idx.strftime('%H:%M')} | entry={entry:.2f}")
                            break

            # =================================================================
            # FINAL SUMMARY
            # =================================================================
            if debug_mode:
                if signal:
                    log(f"  🎯 FINAL: {signal['direction']} signal @ {signal['entry_price']}")
                else:
                    log(f"  ❌ FINAL: No signal generated")
                    reasons = []
                    if first5_is_buy_setup and buy_setup_invalid:
                        reasons.append("Buy setup invalidated early")
                    if first5_is_sell_setup and sell_setup_invalid:
                        reasons.append("Sell setup invalidated early")
                    if first5_is_buy_setup and not buy_impulse_done:
                        reasons.append(f"No buy impulse (swing_high={buy_swing_high}, need>={first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100):.2f})")
                    if first5_is_sell_setup and not sell_impulse_done:
                        reasons.append(f"No sell impulse (swing_low={sell_swing_low}, need<<={first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100):.2f})")
                    if buy_impulse_done and not buy_retraced:
                        reasons.append(f"No buy retracement (fib50={buy_fib_50:.2f})")
                    if sell_impulse_done and not sell_retraced:
                        reasons.append(f"No sell retracement (fib50={sell_fib_50:.2f})")
                    if buy_retraced and not buy_signal_fired:
                        reasons.append("No buy recovery (green+close>fib50+EMAs+trend)")
                    if sell_retraced and not sell_signal_fired:
                        reasons.append("No sell resumption (red+close<<fib50+EMAs+trend)")
                    if not reasons:
                        reasons.append("Unknown")
                    log(f"     Reasons: {' | '.join(reasons)}")
                log(f"{'='*60}")

            return signal, "\n".join(debug_lines)

        except Exception as e:
            log(f"ERROR in {symbol}: {str(e)}")
            import traceback
            log(traceback.format_exc())
            return None, "\n".join(debug_lines)

# ================================================================================
# DISPLAY RESULTS
# ================================================================================
def display_results(signals, scan_date):
    if not signals:
        st.warning("⚠️ No signals found. No stocks met all Fibonacci retracement conditions.")
        return

    df = pd.DataFrame([s for s in signals if s is not None])
    if df.empty:
        st.warning("⚠️ No valid signals found.")
        return

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
        all_debug_logs = []
        total = len(stock_list)
        completed = 0

        def process_stock(task_data):
            idx, sym, target_date = task_data
            tv_inst = tv_pool[idx % 5]
            time.sleep(random.uniform(0.1, 0.4))
            signal, debug_log = strategy.scan_stock(tv_inst, sym, target_date, tolerance_pct)
            return signal, debug_log

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            tasks = [(i, sym, scan_date) for i, sym in enumerate(stock_list)]
            futures = {executor.submit(process_stock, task): task for task in tasks}

            for future in concurrent.futures.as_completed(futures):
                completed += 1
                if completed % 5 == 0 or completed == total:
                    progress_bar.progress(completed / total)
                    status_text.text(f"⚡ Scanning... ({completed}/{total})")

                try:
                    res, debug_log = future.result()
                    if debug_log:
                        all_debug_logs.append(debug_log)
                    if res:
                        all_signals.append(res)
                except Exception:
                    pass

        progress_container.empty()
        display_results(all_signals, scan_date)

        # Show debug console
        if all_debug_logs:
            with st.expander("🐛 Debug Console (SAIL / DABUR / TCS)", expanded=True):
                st.markdown('<div class="debug-log">' + "\n\n".join(all_debug_logs) + '</div>', unsafe_allow_html=True)

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
        debug_container = st.empty()

        def process_live(task_data):
            idx, sym, target_time = task_data
            tv_inst = tv_pool[idx % 5]
            time.sleep(random.uniform(0.1, 0.4))
            return strategy.scan_stock(tv_inst, sym, target_time, tolerance_pct)

        while True:
            all_signals = []
            all_debug_logs = []
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
                        res, debug_log = future.result()
                        if debug_log:
                            all_debug_logs.append(debug_log)
                        if res:
                            all_signals.append(res)
                    except Exception:
                        pass

            progress_container.empty()

            with live_container.container():
                market_status = "🟢 Market Open" if is_market_open else "🔴 Market Closed"
                st.write(f"⏱️ Last Updated: {datetime.now(IST).strftime('%H:%M:%S IST')} | {market_status}")
                display_results(all_signals, live_datetime)

            with debug_container.container():
                if all_debug_logs:
                    with st.expander("🐛 Debug Console (SAIL / DABUR / TCS)"):
                        st.markdown('<div class="debug-log">' + "\n\n".join(all_debug_logs) + '</div>', unsafe_allow_html=True)

            time.sleep(60 if not is_market_open else 5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
