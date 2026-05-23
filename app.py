import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import warnings
import logging
import multiprocessing
from multiprocessing import Pool
from tvDatafeed import TvDatafeed, Interval

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# CONFIGURATION
# ================================================================================
class ScaleConfig:
    TV_POOL_SIZE = 8           # 8 TV instances (created in each worker process)
    WORKER_PROCESSES = 4       # 4 parallel processes (safe for Streamlit Cloud)
    PER_STOCK_TIMEOUT = 20     # Kill worker if stuck longer than 20s
    RETRY_ATTEMPTS = 2         # Retry failed stocks once more

CONFIG = ScaleConfig()

# ================================================================================
# STRATEGY LOGIC — PURE FUNCTIONS (must be picklable for multiprocessing)
# ================================================================================
class Config:
    EMA_FAST = 20
    EMA_SLOW = 50
    FIB_LEVEL_1 = 0.50
    FIB_LEVEL_2 = 0.618
    MIN_IMPULSE_PCT = 0.5
    DEFAULT_TARGET_RR = 2.0

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

def get_historical_candles(tv_instance, symbol, resolution_minutes, days_back=5):
    """Fetch with retry. Reduced bar count for speed."""
    for attempt in range(2):
        try:
            if resolution_minutes == 5:
                interval = Interval.in_5_minute
                bars = days_back * 75
            else:
                interval = Interval.in_15_minute
                bars = days_back * 25

            df = tv_instance.get_hist(
                symbol=symbol.replace('.NS', ''),
                exchange='NSE',
                interval=interval,
                n_bars=bars
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

            if attempt == 0:
                time.sleep(0.2)

        except Exception:
            if attempt == 0:
                time.sleep(0.2)

    return pd.DataFrame()

def scan_stock_worker(args):
    """
    Worker function for multiprocessing.
    Each worker creates its own TvDatafeed instance.
    Returns: (symbol, result_dict_or_none, is_error)
    """
    symbol, target_date_str, tolerance_pct = args
    sym_clean = symbol.replace('.NS', '')
    
    try:
        # Each worker creates its own TV connection
        tv = TvDatafeed()
        
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        
        df_5min = get_historical_candles(tv, symbol, 5, days_back=5)
        df_15min = get_historical_candles(tv, symbol, 15, days_back=10)

        if df_5min.empty or df_15min.empty:
            return (sym_clean, {"_error": True, "_reason": "empty_data"}, True)

        market_open = pd.Timestamp('09:15').time()
        market_close = pd.Timestamp('15:30').time()

        df_5min_today = df_5min[
            (df_5min.index.date == target_date) &
            (df_5min.index.time >= market_open) &
            (df_5min.index.time <= market_close)
        ].copy()

        # EMA on full history first
        df_15min['ema20'] = calculate_ema(df_15min, Config.EMA_FAST)
        df_15min['ema50'] = calculate_ema(df_15min, Config.EMA_SLOW)

        df_15min_today = df_15min[
            (df_15min.index.date == target_date) &
            (df_15min.index.time >= market_open) &
            (df_15min.index.time <= market_close)
        ].copy()

        if df_5min_today.empty or len(df_15min_today) < 3:
            return (sym_clean, None, False)  # Valid: no data for this date

        first_5m = df_5min_today.iloc[0]
        first5_open = float(first_5m['open'])
        first5_high = float(first_5m['high'])
        first5_low = float(first_5m['low'])

        price = first5_open if first5_open > 0 else 1.0
        tolerance = price * (tolerance_pct / 100)

        is_buy_setup = abs(first5_open - first5_low) <= tolerance
        is_sell_setup = abs(first5_open - first5_high) <= tolerance

        if not is_buy_setup and not is_sell_setup:
            return (sym_clean, None, False)  # Valid: no setup

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

            if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                if row['low'] < first5_low:
                    buy_setup_invalid = True
            if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                if row['high'] > first5_high:
                    sell_setup_invalid = True

            if is_buy_setup and not buy_setup_invalid and not buy_impulse_done:
                buy_swing_high = float(row['high']) if buy_swing_high is None else max(buy_swing_high, float(row['high']))
                threshold = first5_high * (1 + Config.MIN_IMPULSE_PCT / 100)
                if buy_swing_high >= threshold:
                    buy_impulse_done = True
                    buy_impulse_bar = i
                    buy_fib_50 = buy_swing_high - Config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                    buy_fib_618 = buy_swing_high - Config.FIB_LEVEL_2 * (buy_swing_high - first5_low)

            if is_sell_setup and not sell_setup_invalid and not sell_impulse_done:
                sell_swing_low = float(row['low']) if sell_swing_low is None else min(sell_swing_low, float(row['low']))
                threshold = first5_low * (1 - Config.MIN_IMPULSE_PCT / 100)
                if sell_swing_low <= threshold:
                    sell_impulse_done = True
                    sell_impulse_bar = i
                    sell_fib_50 = sell_swing_low + Config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                    sell_fib_618 = sell_swing_low + Config.FIB_LEVEL_2 * (first5_high - sell_swing_low)

            if buy_impulse_done and not buy_retraced and i > buy_impulse_bar:
                if row['low'] <= buy_fib_50:
                    buy_retraced = True; buy_retrace_bar = i
            if sell_impulse_done and not sell_retraced and i > sell_impulse_bar:
                if row['high'] >= sell_fib_50:
                    sell_retraced = True; sell_retrace_bar = i

            if buy_retraced and not buy_signal_fired and i > buy_retrace_bar:
                if row['close'] > row['open'] and row['close'] > buy_fib_50:
                    is_uptrend, _ = check_trend(df_15min_today.iloc[:i+1])
                    if (row['close'] > row['ema20']) and (row['close'] > row['ema50']) and is_uptrend:
                        entry = float(row['close']); sl = float(row['low'])
                        target = entry + (entry - sl) * Config.DEFAULT_TARGET_RR
                        signal = {
                            'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'BUY',
                            'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                            'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                            'risk_reward': f"1:{Config.DEFAULT_TARGET_RR}", 'fib_50': round(buy_fib_50, 2),
                            'fib_618': round(buy_fib_618, 2), 'swing_high': round(buy_swing_high, 2),
                            'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'UP'
                        }
                        buy_signal_fired = True
                        break

            if sell_retraced and not sell_signal_fired and i > sell_retrace_bar:
                if row['close'] < row['open'] and row['close'] < sell_fib_50:
                    _, is_downtrend = check_trend(df_15min_today.iloc[:i+1])
                    if (row['close'] < row['ema20']) and (row['close'] < row['ema50']) and is_downtrend:
                        entry = float(row['close']); sl = float(row['high'])
                        target = entry - (sl - entry) * Config.DEFAULT_TARGET_RR
                        signal = {
                            'symbol': sym_clean, 'date': target_date.strftime('%Y-%m-%d'), 'direction': 'SELL',
                            'setup_time': df_5min_today.index[0].strftime('%H:%M'), 'signal_time': idx.strftime('%H:%M'),
                            'entry_price': round(entry, 2), 'stop_loss': round(sl, 2), 'target': round(target, 2),
                            'risk_reward': f"1:{Config.DEFAULT_TARGET_RR}", 'fib_50': round(sell_fib_50, 2),
                            'fib_618': round(sell_fib_618, 2), 'swing_low': round(sell_swing_low, 2),
                            'ema20': round(float(row['ema20']), 2), 'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN'
                        }
                        sell_signal_fired = True
                        break

        return (sym_clean, signal, False)

    except Exception as e:
        return (sym_clean, {"_error": True, "_reason": str(e)}, True)

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
            | {perf_stats.get('retried', 0)} retried | {len(errors)} final errors
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

        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ---------------------------------------------------------
    # HISTORICAL SCAN — MULTIPROCESSING WITH TIMEOUT
    # ---------------------------------------------------------
    if scan_button and stock_list and scan_mode == "Historical Scan":
        start_time = time.time()

        # Build task list: (symbol, target_date_str, tolerance_pct)
        target_date_str = scan_date.strftime('%Y-%m-%d')
        tasks = [(sym, target_date_str, tolerance_pct) for sym in stock_list]

        progress_container = st.empty()
        with progress_container.container():
            st.subheader("⏳ Scanning...")
            bar = st.progress(0)
            status = st.empty()

        all_results = []
        failed_symbols = []
        completed = 0
        total = len(stock_list)

        # Use multiprocessing with timeout
        # Pool creates separate processes — each gets its own TV connection
        with Pool(processes=CONFIG.WORKER_PROCESSES) as pool:
            # imap_unordered yields results as they complete
            for result in pool.imap_unordered(scan_stock_worker, tasks):
                symbol, data, is_error = result
                completed += 1

                if completed % 5 == 0 or completed == total:
                    bar.progress(completed / total)
                    status.text(f"⚡ Scanning... ({completed}/{total})")

                if is_error:
                    failed_symbols.append(symbol)
                    all_results.append(data)
                elif data is not None:
                    all_results.append(data)
                # If data is None, it's valid no-setup — skip

        # =====================================================================
        # PASS 2: Retry failed stocks with fresh processes
        # =====================================================================
        if failed_symbols:
            status.text(f"🔄 Retrying {len(failed_symbols)} failed stocks...")
            time.sleep(1)  # Brief pause for TV servers

            retry_tasks = [(sym + ".NS", target_date_str, tolerance_pct) for sym in failed_symbols]
            # Note: symbol already has .NS stripped in worker, but we need to pass full symbol
            # Actually worker strips .NS, so we need original symbols
            retry_tasks = []
            for sym in stock_list:
                if sym.replace('.NS', '') in failed_symbols:
                    retry_tasks.append((sym, target_date_str, tolerance_pct))

            with Pool(processes=CONFIG.WORKER_PROCESSES) as pool:
                for result in pool.imap_unordered(scan_stock_worker, retry_tasks):
                    symbol, data, is_error = result

                    if not is_error and data is not None:
                        # Replace the error with the signal
                        # Remove old error, add signal
                        all_results = [r for r in all_results if not (isinstance(r, dict) and r.get('symbol') == symbol and r.get('_error'))]
                        all_results.append(data)
                    elif not is_error and data is None:
                        # Valid no-setup on retry — remove error
                        all_results = [r for r in all_results if not (isinstance(r, dict) and r.get('symbol') == symbol and r.get('_error'))]

        progress_container.empty()

        duration = time.time() - start_time
        valid_signals = [r for r in all_results if r is not None and not (isinstance(r, dict) and r.get("_error"))]
        errors = [r for r in all_results if isinstance(r, dict) and r.get("_error")]

        perf_stats = {
            'stocks_scanned': len(stock_list),
            'duration': duration,
            'signals_found': len(valid_signals),
            'stocks_per_sec': len(stock_list) / duration if duration > 0 else 0,
            'retried': len(failed_symbols)
        }

        display_results(all_results, scan_date, perf_stats)

    # ---------------------------------------------------------
    # REAL-TIME SCAN
    # ---------------------------------------------------------
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span style="background:#28a745;color:white;padding:4px 12px;border-radius:10px;">🟢 LIVE</span></div>', unsafe_allow_html=True)

        live_container = st.empty()

        while True:
            all_results = []
            live_dt = datetime.now(IST)
            ct = live_dt.time()
            is_open = (live_dt.weekday() < 5 and
                       ct >= datetime.strptime("09:15", "%H:%M").time() and
                       ct <= datetime.strptime("15:30", "%H:%M").time())

            target_date_str = live_dt.strftime('%Y-%m-%d')
            tasks = [(sym, target_date_str, tolerance_pct) for sym in stock_list]

            progress_container = st.empty()
            with progress_container.container():
                live_bar = st.progress(0)
                live_status = st.empty()

            total = len(stock_list)
            completed = 0

            with Pool(processes=CONFIG.WORKER_PROCESSES) as pool:
                for result in pool.imap_unordered(scan_stock_worker, tasks):
                    symbol, data, is_error = result
                    completed += 1

                    if completed % 5 == 0 or completed == total:
                        live_bar.progress(completed / total)
                        live_status.text(f"⚡ Live Scanning... ({completed}/{total})")

                    if not is_error and data is not None:
                        all_results.append(data)

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
