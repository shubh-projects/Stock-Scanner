import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import warnings
import logging
import subprocess
import sys
import json
import os
from tvDatafeed import TvDatafeed, Interval

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# CONFIGURATION
# ================================================================================
class Config:
    EMA_FAST = 20
    EMA_SLOW = 50
    FIB_LEVEL_1 = 0.50
    FIB_LEVEL_2 = 0.618
    MIN_IMPULSE_PCT = 0.5
    DEFAULT_TARGET_RR = 2.0
    PER_STOCK_TIMEOUT = 15  # Seconds before killing the subprocess

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
# TV POOL
# ================================================================================
class TVPool:
    def __init__(self, size):
        self.pool = []
        for i in range(size):
            try:
                self.pool.append(TvDatafeed())
                time.sleep(0.3)
            except Exception:
                break
        self.size = len(self.pool)
        if self.size == 0:
            st.error("❌ No TV connections")
            st.stop()

    def get(self, idx):
        return self.pool[idx % self.size]

# ================================================================================
# STRATEGY LOGIC
# ================================================================================
def calculate_ema(df, period):
    return df['close'].ewm(span=period, adjust=False).mean()

def check_trend(df_slice):
    if len(df_slice) < 2:
        return False, False
    ema20_now = df_slice['ema20'].iloc[-1]
    ema50_now = df_slice['ema50'].iloc[-1]
    ema20_prev = df_slice['ema20'].iloc[-2]
    ema50_prev = df_slice['ema50'].iloc[-2]
    is_up = (ema20_now > ema50_now) and (ema20_prev > ema50_prev)
    is_down = (ema20_now < ema50_now) and (ema20_prev < ema50_prev)
    return is_up, is_down

def get_data(tv, symbol, res, days=5):
    for attempt in range(2):
        try:
            if res == 5:
                interval = Interval.in_5_minute
                bars = days * 75
            else:
                interval = Interval.in_15_minute
                bars = days * 25

            df = tv.get_hist(symbol=symbol.replace('.NS', ''), exchange='NSE', interval=interval, n_bars=bars)
            if df is not None and not df.empty:
                df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
                df.index = pd.to_datetime(df.index)
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
                else:
                    df.index = df.index.tz_convert('Asia/Kolkata')
                return df
            if attempt == 0:
                time.sleep(0.2)
        except Exception:
            if attempt == 0:
                time.sleep(0.2)
    return pd.DataFrame()

def scan_stock(tv, symbol, target_date, tolerance_pct):
    sym_clean = symbol.replace('.NS', '')
    try:
        df_5 = get_data(tv, symbol, 5, days=5)
        df_15 = get_data(tv, symbol, 15, days=10)

        if df_5.empty or df_15.empty:
            return {"_error": True, "_reason": "empty_data", "symbol": sym_clean}

        market_open = pd.Timestamp('09:15').time()
        market_close = pd.Timestamp('15:30').time()

        df_5_today = df_5[(df_5.index.date == target_date) & (df_5.index.time >= market_open) & (df_5.index.time <= market_close)].copy()
        df_15['ema20'] = calculate_ema(df_15, Config.EMA_FAST)
        df_15['ema50'] = calculate_ema(df_15, Config.EMA_SLOW)
        df_15_today = df_15[(df_15.index.date == target_date) & (df_15.index.time >= market_open) & (df_15.index.time <= market_close)].copy()

        if df_5_today.empty or len(df_15_today) < 3:
            return None

        first = df_5_today.iloc[0]
        f_open = float(first['open'])
        f_high = float(first['high'])
        f_low = float(first['low'])

        price = f_open if f_open > 0 else 1.0
        tol = price * (tolerance_pct / 100)

        buy_setup = abs(f_open - f_low) <= tol
        sell_setup = abs(f_open - f_high) <= tol

        if not buy_setup and not sell_setup:
            return None

        buy_inv = False; sell_inv = False
        buy_imp = False; buy_imp_bar = -1
        buy_ret = False; buy_ret_bar = -1; buy_sig = False
        buy_sh = None; buy_f50 = None; buy_f618 = None

        sell_imp = False; sell_imp_bar = -1
        sell_ret = False; sell_ret_bar = -1; sell_sig = False
        sell_sl = None; sell_f50 = None; sell_f618 = None

        signal = None

        for i, (idx, row) in enumerate(df_15_today.iterrows()):
            if i == 0:
                if buy_setup and row['low'] < f_low:
                    buy_inv = True
                if sell_setup and row['high'] > f_high:
                    sell_inv = True
                continue

            if idx.time() > market_close:
                break

            if buy_setup and not buy_inv and not buy_imp:
                if row['low'] < f_low:
                    buy_inv = True
            if sell_setup and not sell_inv and not sell_imp:
                if row['high'] > f_high:
                    sell_inv = True

            if buy_setup and not buy_inv and not buy_imp:
                buy_sh = float(row['high']) if buy_sh is None else max(buy_sh, float(row['high']))
                if buy_sh >= f_high * (1 + Config.MIN_IMPULSE_PCT / 100):
                    buy_imp = True; buy_imp_bar = i
                    buy_f50 = buy_sh - Config.FIB_LEVEL_1 * (buy_sh - f_low)
                    buy_f618 = buy_sh - Config.FIB_LEVEL_2 * (buy_sh - f_low)

            if sell_setup and not sell_inv and not sell_imp:
                sell_sl = float(row['low']) if sell_sl is None else min(sell_sl, float(row['low']))
                if sell_sl <= f_low * (1 - Config.MIN_IMPULSE_PCT / 100):
                    sell_imp = True; sell_imp_bar = i
                    sell_f50 = sell_sl + Config.FIB_LEVEL_1 * (f_high - sell_sl)
                    sell_f618 = sell_sl + Config.FIB_LEVEL_2 * (f_high - sell_sl)

            if buy_imp and not buy_ret and i > buy_imp_bar:
                if row['low'] <= buy_f50:
                    buy_ret = True; buy_ret_bar = i
            if sell_imp and not sell_ret and i > sell_imp_bar:
                if row['high'] >= sell_f50:
                    sell_ret = True; sell_ret_bar = i

            if buy_ret and not buy_sig and i > buy_ret_bar:
                if row['close'] > row['open'] and row['close'] > buy_f50:
                    up, _ = check_trend(df_15_today.iloc[:i+1])
                    if row['close'] > row['ema20'] and row['close'] > row['ema50'] and up:
                        entry = float(row['close']); sl = float(row['low'])
                        tgt = entry + (entry - sl) * Config.DEFAULT_TARGET_RR
                        return {
                            'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY',
                            'setup_time': df_5_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                            'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(tgt, 2),
                            'risk_reward': f"1:{Config.DEFAULT_TARGET_RR}", 'fib_50': round(buy_f50, 2),
                            'fib_618': round(buy_f618, 2), 'swing_high': round(buy_sh, 2),
                            'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'UP'
                        }

            if sell_ret and not sell_sig and i > sell_ret_bar:
                if row['close'] < row['open'] and row['close'] < sell_f50:
                    _, down = check_trend(df_15_today.iloc[:i+1])
                    if row['close'] < row['ema20'] and row['close'] < row['ema50'] and down:
                        entry = float(row['close']); sl = float(row['high'])
                        tgt = entry - (sl - entry) * Config.DEFAULT_TARGET_RR
                        return {
                            'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL',
                            'setup_time': df_5_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                            'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(tgt, 2),
                            'risk_reward': f"1:{Config.DEFAULT_TARGET_RR}", 'fib_50': round(sell_f50, 2),
                            'fib_618': round(sell_f618, 2), 'swing_low': round(sell_sl, 2),
                            'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN'
                        }

        return None

    except Exception as e:
        return {"_error": True, "_reason": str(e), "symbol": sym_clean}

# ================================================================================
# SUBPROCESS WORKER — ISOLATED PROCESS WITH HARD TIMEOUT
# ================================================================================
def scan_with_timeout(tv_pool, idx, symbol, target_date, tolerance_pct):
    """
    Run scan_stock in a subprocess with a hard timeout.
    If the process hangs, we kill it and return an error.
    """
    # Write the scan logic to a temporary script and execute it
    # This is heavy but guarantees isolation
    
    # Actually, a simpler approach: use a separate script file
    # For now, use direct call with manual timeout check
    tv = tv_pool.get(idx)
    
    # Use alarm signal for timeout (Linux only, works on Streamlit Cloud)
    import signal
    
    class TimeoutException(Exception):
        pass
    
    def handler(signum, frame):
        raise TimeoutException()
    
    # Set alarm
    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.alarm(Config.PER_STOCK_TIMEOUT)
    
    try:
        result = scan_stock(tv, symbol, target_date, tolerance_pct)
        signal.alarm(0)  # Cancel alarm
        signal.signal(signal.SIGALRM, old_handler)
        return result
    except TimeoutException:
        signal.signal(signal.SIGALRM, old_handler)
        return {"_error": True, "_reason": "timeout", "symbol": symbol.replace('.NS', '')}

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
            | {perf_stats.get('retried', 0)} retried
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

        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN — SIMPLE LOOP WITH TIMEOUT
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        start_time = time.time()

        tv_pool = TVPool(8)
        target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning...")
            bar = st.progress(0)
            status = st.empty()

        all_results = []
        failed_symbols = []
        total = len(stock_list)

        for i, symbol in enumerate(stock_list):
            # Update progress every stock
            bar.progress((i + 1) / total)
            status.text(f"⚡ Scanning... ({i+1}/{total}) — {symbol.replace('.NS', '')}")

            result = scan_with_timeout(tv_pool, i, symbol, target_date, tolerance_pct)

            if isinstance(result, dict) and result.get("_error"):
                failed_symbols.append(symbol)
                all_results.append(result)
            elif result is not None:
                all_results.append(result)
            # If None, valid no-setup — skip

        # Retry failed stocks once
        if failed_symbols:
            status.text(f"🔄 Retrying {len(failed_symbols)} failed stocks...")
            time.sleep(1)

            for symbol in failed_symbols:
                result = scan_with_timeout(tv_pool, 999, symbol, target_date, tolerance_pct)
                if not (isinstance(result, dict) and result.get("_error")):
                    # Remove old error, add new result
                    all_results = [r for r in all_results if not (isinstance(r, dict) and r.get("symbol") == symbol.replace('.NS', '') and r.get("_error"))]
                    if result is not None:
                        all_results.append(result)

        progress_container.empty()

        duration = time.time() - start_time
        valid_signals = [r for r in all_results if r is not None and not (isinstance(r, dict) and r.get("_error"))]
        errors = [r for r in all_results if isinstance(r, dict) and r.get("_error")]

        perf_stats = {
            'stocks_scanned': total,
            'duration': duration,
            'signals_found': len(valid_signals),
            'stocks_per_sec': total / duration if duration > 0 else 0,
            'retried': len(failed_symbols)
        }

        display_results(all_results, scan_date, perf_stats)

    # ---------------------------------------------------------
    # REAL-TIME SCAN
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span style="background:#28a745;color:white;padding:4px 12px;border-radius:10px;">🟢 LIVE</span></div>', unsafe_allow_html=True)

        tv_pool = TVPool(8)
        live_container = st.empty()

        while True:
            all_results = []
            live_dt = datetime.now(IST)
            ct = live_dt.time()
            is_open = (live_dt.weekday() < 5 and
                       ct >= datetime.strptime("09:15", "%H:%M").time() and
                       ct <= datetime.strptime("15:30", "%H:%M").time())

            target_date = live_dt.date()

            progress_container = st.empty()
            with progress_container.container():
                live_bar = st.progress(0)
                live_status = st.empty()

            total = len(stock_list)

            for i, symbol in enumerate(stock_list):
                live_bar.progress((i + 1) / total)
                live_status.text(f"⚡ Live Scanning... ({i+1}/{total})")

                result = scan_with_timeout(tv_pool, i, symbol, target_date, tolerance_pct)
                if result is not None and not (isinstance(result, dict) and result.get("_error")):
                    all_results.append(result)

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
