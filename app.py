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
import threading

warnings.filterwarnings('ignore')
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# ================================================================================
# CONFIGURATION
# ================================================================================
class ScaleConfig:
    """
    SPEED FIX: Previous version had sleeps INSIDE the TV lock → each lock held
    for 0.4-1.2s per retry × 2 timeframes = 0.8-2.4s of dead wait per stock.
    With 30 instances and 22 workers queueing, that compounded to 244 seconds.

    New design — PRE-FETCH PIPELINE:
      Phase 1: Fetch all 5min data for ALL stocks in parallel  (pure I/O)
      Phase 2: Fetch all 15min data for ALL stocks in parallel (pure I/O)
      Phase 3: Process/scan all stocks                         (pure CPU, instant)

    Each TV instance is locked for ONE single get_hist() call (~0.3-1s), then
    immediately released. No sleeping inside the lock. Ever.
    """
    TV_POOL_SIZE        = 40    # 40 persistent WebSocket connections
    MAX_FETCH_WORKERS   = 38    # Near pool size → almost no queueing per instance
    BASE_DELAY          = 0.01  # 10ms between requests (minimal pacing)
    CONNECTION_STAGGER  = 0.25  # 250ms per TV instance during pool creation
    FETCH_RETRIES       = 2     # Retries on empty/error — sleep is OUTSIDE the lock

# ================================================================================
# PAGE CONFIG & CSS
# ================================================================================
st.set_page_config(
    page_title="Open Drive Fib Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header  { font-size:2.5rem; font-weight:bold; color:#1f77b4; text-align:center; margin-bottom:0.5rem; }
    .sub-header   { font-size:1.1rem; color:#666; text-align:center; margin-bottom:2rem; }
    .metric-card  { background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); padding:1rem; border-radius:10px; color:white; text-align:center; }
    .live-badge   { background-color:#ff4b4b; color:white; padding:2px 8px; border-radius:10px; font-size:0.8rem; font-weight:bold; animation:blink 2s infinite; }
    @keyframes blink { 0%{opacity:1;} 50%{opacity:0.4;} 100%{opacity:1;} }
    .signal-card  { padding:1rem; border-radius:8px; margin:0.5rem 0; border-left:4px solid; }
    .buy-card     { background:linear-gradient(135deg,#1a5f5f 0%,#2d8a8a 100%)!important; color:white; border-left-color:#4CAF50; }
    .sell-card    { background:linear-gradient(135deg,#7a1f1f 0%,#a03030 100%)!important; color:white; border-left-color:#f44336; }
    .speed-badge  { background:linear-gradient(135deg,#00b09b 0%,#96c93d 100%); color:white; padding:4px 14px; border-radius:20px; font-weight:bold; }
    .phase-box    { background:#1e1e2e; border-radius:8px; padding:0.6rem 1rem; color:#a0f0a0; font-family:monospace; font-size:0.85rem; margin-bottom:0.5rem; }
</style>
""", unsafe_allow_html=True)

# ================================================================================
# TV POOL — THREAD-SAFE, PER-INSTANCE LOCK
# ================================================================================
class TVPool:
    def __init__(self, size):
        self.pool  = []
        self.locks = []
        for _ in range(size):
            try:
                self.pool.append(TvDatafeed())
                self.locks.append(threading.Lock())
                time.sleep(ScaleConfig.CONNECTION_STAGGER)
            except Exception:
                break
        self.size          = len(self.pool)
        self._counter      = 0
        self._counter_lock = threading.Lock()

    def acquire(self):
        with self._counter_lock:
            idx = self._counter % self.size
            self._counter += 1
        return idx, self.pool[idx], self.locks[idx]

@st.cache_resource(show_spinner=False)
def get_tv_pool():
    return TVPool(ScaleConfig.TV_POOL_SIZE)

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
    return True

# ================================================================================
# STRATEGY CONFIG
# ================================================================================
class Config:
    EMA_FAST          = 20
    EMA_SLOW          = 50
    FIB_LEVEL_1       = 0.50
    FIB_LEVEL_2       = 0.618
    MIN_IMPULSE_PCT   = 0.5
    DEFAULT_TARGET_RR = 2.0

# ================================================================================
# PHASE 1 & 2 — PARALLEL DATA FETCHER
# ================================================================================
def _fetch_one(task, tv_pool):
    """
    Fetch a single (symbol, resolution) pair.
    Lock is held ONLY during the get_hist() call — NO sleeping inside.
    Retries acquire a FRESH TV instance so the lock is re-acquired clean.
    """
    sym, resolution, days_back = task

    if resolution == 5:
        tv_interval  = Interval.in_5_minute
        n_bars       = days_back * 75
    else:
        tv_interval  = Interval.in_15_minute
        n_bars       = days_back * 25

    formatted = sym.replace('.NS', '')

    for attempt in range(ScaleConfig.FETCH_RETRIES):
        _, tv_inst, tv_lock = tv_pool.acquire()
        try:
            with tv_lock:
                time.sleep(ScaleConfig.BASE_DELAY)
                df = tv_inst.get_hist(
                    symbol=formatted,
                    exchange='NSE',
                    interval=tv_interval,
                    n_bars=n_bars
                )
            # Lock released — now safe to inspect and sleep

            if df is None or df.empty:
                if attempt < ScaleConfig.FETCH_RETRIES - 1:
                    time.sleep(0.3)   # Sleep OUTSIDE the lock
                continue

            # Normalise timestamps
            df.index = pd.to_datetime(df.index)
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
            return df

        except Exception:
            if attempt < ScaleConfig.FETCH_RETRIES - 1:
                time.sleep(0.3)

    return pd.DataFrame()   # All attempts failed


def prefetch_all(stock_list, tv_pool, days_back, progress_bar, status_text):
    """
    Pre-fetch ALL 5min and ALL 15min data in parallel.
    Returns: { symbol: {'5': df_5min, '15': df_15min} }
    """
    tasks = (
        [(sym, 5,  days_back) for sym in stock_list] +
        [(sym, 15, days_back) for sym in stock_list]
    )
    total     = len(tasks)
    completed = 0
    cache     = {sym: {} for sym in stock_list}
    lock      = threading.Lock()

    def fetch_and_store(task):
        nonlocal completed
        df = _fetch_one(task, tv_pool)
        sym, resolution, _ = task
        with lock:
            cache[sym][resolution] = df
            completed += 1
            if completed % 10 == 0 or completed == total:
                pct = completed / total
                progress_bar.progress(pct)
                status_text.text(
                    f"📡 Fetching data… {completed}/{total} calls "
                    f"({'5min' if resolution == 5 else '15min'}: {sym.replace('.NS','')})"
                )

    with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_FETCH_WORKERS) as ex:
        futures = [ex.submit(fetch_and_store, t) for t in tasks]
        concurrent.futures.wait(futures)

    return cache

# ================================================================================
# PHASE 3 — PURE-CPU SCANNER (no network, no locks, instant)
# ================================================================================
class OpenDriveFibStrategy:
    def __init__(self):
        self.config = Config()

    def calculate_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def check_trend(self, df):
        if len(df) < 2:
            return False, False
        e20n, e20p = df['ema20'].iloc[-1], df['ema20'].iloc[-2]
        e50n, e50p = df['ema50'].iloc[-1], df['ema50'].iloc[-2]
        up   = (e20n > e50n) and (e20n > e20p) and (e50n > e50p)
        down = (e20n < e50n) and (e20n < e20p) and (e50n < e50p)
        return up, down

    def scan_from_cache(self, symbol, df_5min, df_15min, scan_date, tolerance_pct):
        """
        Pure in-memory scan — uses pre-fetched DataFrames, zero network calls.
        Mirrors Pine Script bar-by-bar logic exactly.
        """
        try:
            if df_5min is None or df_5min.empty or df_15min is None or df_15min.empty:
                return None

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

            # EMA on FULL history before filtering (critical for accuracy)
            df_15min = df_15min.copy()
            df_15min['ema20'] = self.calculate_ema(df_15min, self.config.EMA_FAST)
            df_15min['ema50'] = self.calculate_ema(df_15min, self.config.EMA_SLOW)

            # Filter to target date + market hours
            mkt_start = pd.Timestamp('09:15').time()
            df_5d  = df_5min[ (df_5min.index.date  == target_date) & (df_5min.index.time  >= mkt_start)].copy()
            df_15d = df_15min[(df_15min.index.date == target_date) & (df_15min.index.time >= mkt_start)].copy()

            if df_5d.empty or df_15d.empty:
                return None

            first_5m    = df_5d.iloc[0]
            first5_open = float(first_5m['open'])
            first5_high = float(first_5m['high'])
            first5_low  = float(first_5m['low'])

            price     = first5_open if first5_open > 0 else 1.0
            tolerance = price * (tolerance_pct / 100)

            is_buy_setup  = abs(first5_open - first5_low)  <= tolerance
            is_sell_setup = abs(first5_open - first5_high) <= tolerance

            sym_clean = symbol.replace('.NS', '')
            if not is_buy_setup and not is_sell_setup:
                return None

            # ── State variables ──
            buy_swing_high = None;  buy_fib_50 = None;  buy_fib_618 = None
            buy_imp_done   = False; buy_imp_bar = -1
            buy_ret_done   = False; buy_ret_bar = -1;   buy_fired   = False

            sell_swing_low = None;  sell_fib_50 = None; sell_fib_618 = None
            sell_imp_done  = False; sell_imp_bar = -1
            sell_ret_done  = False; sell_ret_bar = -1;  sell_fired  = False

            buy_invalid  = False
            sell_invalid = False
            signal       = None

            for i, (idx, row) in enumerate(df_15d.iterrows()):

                # Invalidation (before impulse only)
                if is_buy_setup  and not buy_invalid  and not buy_imp_done:
                    if row['low'] < first5_low:
                        buy_invalid = True
                if is_sell_setup and not sell_invalid and not sell_imp_done:
                    if row['high'] > first5_high:
                        sell_invalid = True

                # BUY impulse
                if is_buy_setup and not buy_invalid and not buy_imp_done:
                    buy_swing_high = float(row['high']) if buy_swing_high is None \
                                     else max(buy_swing_high, float(row['high']))
                    if buy_swing_high >= first5_high * (1 + self.config.MIN_IMPULSE_PCT / 100):
                        buy_imp_done = True; buy_imp_bar = i
                        buy_fib_50  = buy_swing_high - self.config.FIB_LEVEL_1 * (buy_swing_high - first5_low)
                        buy_fib_618 = buy_swing_high - self.config.FIB_LEVEL_2 * (buy_swing_high - first5_low)

                # SELL impulse
                if is_sell_setup and not sell_invalid and not sell_imp_done:
                    sell_swing_low = float(row['low']) if sell_swing_low is None \
                                     else min(sell_swing_low, float(row['low']))
                    if sell_swing_low <= first5_low * (1 - self.config.MIN_IMPULSE_PCT / 100):
                        sell_imp_done = True; sell_imp_bar = i
                        sell_fib_50  = sell_swing_low + self.config.FIB_LEVEL_1 * (first5_high - sell_swing_low)
                        sell_fib_618 = sell_swing_low + self.config.FIB_LEVEL_2 * (first5_high - sell_swing_low)

                # BUY retracement
                if buy_imp_done and not buy_ret_done and i > buy_imp_bar:
                    if row['low'] <= buy_fib_50:
                        buy_ret_done = True; buy_ret_bar = i

                # SELL retracement
                if sell_imp_done and not sell_ret_done and i > sell_imp_bar:
                    if row['high'] >= sell_fib_50:
                        sell_ret_done = True; sell_ret_bar = i

                # BUY recovery
                if buy_ret_done and not buy_fired and i > buy_ret_bar:
                    if row['close'] > row['open'] and row['close'] > buy_fib_50:
                        up, _ = self.check_trend(df_15d.iloc[:i+1])
                        if up and row['close'] > row['ema20'] and row['close'] > row['ema50']:
                            e = float(row['close']); sl = float(row['low'])
                            signal = {
                                'symbol':      sym_clean,
                                'date':        target_date.strftime('%Y-%m-%d'),
                                'direction':   'BUY',
                                'setup_time':  df_5d.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(e, 2),
                                'stop_loss':   round(sl, 2),
                                'target':      round(e + (e - sl) * self.config.DEFAULT_TARGET_RR, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50':      round(buy_fib_50, 2),
                                'fib_618':     round(buy_fib_618, 2),
                                'swing_high':  round(buy_swing_high, 2),
                                'ema20':       round(float(row['ema20']), 2),
                                'ema50':       round(float(row['ema50']), 2),
                                'trend':       'UP',
                            }
                            buy_fired = True; break

                # SELL resumption
                if sell_ret_done and not sell_fired and i > sell_ret_bar:
                    if row['close'] < row['open'] and row['close'] < sell_fib_50:
                        _, down = self.check_trend(df_15d.iloc[:i+1])
                        if down and row['close'] < row['ema20'] and row['close'] < row['ema50']:
                            e = float(row['close']); sl = float(row['high'])
                            signal = {
                                'symbol':      sym_clean,
                                'date':        target_date.strftime('%Y-%m-%d'),
                                'direction':   'SELL',
                                'setup_time':  df_5d.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(e, 2),
                                'stop_loss':   round(sl, 2),
                                'target':      round(e - (sl - e) * self.config.DEFAULT_TARGET_RR, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50':      round(sell_fib_50, 2),
                                'fib_618':     round(sell_fib_618, 2),
                                'swing_low':   round(sell_swing_low, 2),
                                'ema20':       round(float(row['ema20']), 2),
                                'ema50':       round(float(row['ema50']), 2),
                                'trend':       'DOWN',
                            }
                            sell_fired = True; break

            return signal

        except Exception:
            return None

# ================================================================================
# DISPLAY RESULTS
# ================================================================================
def display_results(signals, scan_date, perf_stats=None):
    valid = [s for s in signals if s]
    if not valid:
        st.warning("⚠️ No signals found. No stocks met all Fibonacci retracement conditions.")
        return

    df = pd.DataFrame(valid)

    if perf_stats:
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:1rem;">
        <span class="speed-badge">
          ⚡ {perf_stats['total']} stocks | fetch {perf_stats['fetch_t']:.1f}s
          | scan {perf_stats['scan_t']:.2f}s | total {perf_stats['total_t']:.1f}s
          | {perf_stats['signals']} signals
          | {perf_stats['missed']} stocks missing data
        </span>
        </div>
        """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="metric-card"><h3>{len(df)}</h3><p>Total Signals</p></div>', unsafe_allow_html=True)
    with c2:
        bc = len(df[df['direction']=='BUY'])
        st.markdown(f'<div class="metric-card" style="background:linear-gradient(135deg,#11998e,#38ef7d);"><h3>{bc}</h3><p>BUY Signals</p></div>', unsafe_allow_html=True)
    with c3:
        sc = len(df[df['direction']=='SELL'])
        st.markdown(f'<div class="metric-card" style="background:linear-gradient(135deg,#eb3349,#f45c43);"><h3>{sc}</h3><p>SELL Signals</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📋 Filtered Stocks — Fib Retracement Signals")
    for _, row in df.iterrows():
        cc = "buy-card" if row['direction']=='BUY' else "sell-card"
        sw = f"Swing High: {row.get('swing_high','N/A')}" if row['direction']=='BUY' else f"Swing Low: {row.get('swing_low','N/A')}"
        st.markdown(f"""
        <div class="signal-card {cc}">
          <h4>{row['symbol']} — {row['direction']} @ {row['entry_price']}</h4>
          <p><b>Setup:</b> {row['setup_time']} | <b>Signal:</b> {row['signal_time']} | <b>Trend:</b> {row['trend']}</p>
          <p><b>Entry:</b> {row['entry_price']} | <b>SL:</b> {row['stop_loss']} | <b>TGT:</b> {row['target']} | <b>R:R:</b> {row['risk_reward']}</p>
          <p><b>Fib 0.5:</b> {row['fib_50']} | <b>Fib 0.618:</b> {row['fib_618']} | {sw}</p>
          <p><b>EMA20:</b> {row['ema20']} | <b>EMA50:</b> {row['ema50']}</p>
        </div>
        """, unsafe_allow_html=True)

    with st.expander("📊 Export Data"):
        st.dataframe(df, hide_index=True, use_container_width=True)

# ================================================================================
# MAIN
# ================================================================================
def main():
    if not check_password():
        st.stop()

    st.markdown('<div class="main-header">📈 Open Drive Fib Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Scans NSE stocks for Open=Low/High + Fib Retracement + EMA alignment</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Settings")
        scan_mode = st.radio("Select Mode:", ["Historical Scan", "Real-Time Scan"])
        scan_date = st.date_input("Scan Date",
                                   value=datetime.now(IST) - timedelta(days=1),
                                   max_value=datetime.now(IST))

        st.markdown("---")
        st.subheader("📋 Stock List")
        input_method = st.radio("Input method:", ["Paste Symbols", "Use Default List"])

        default_symbols = """
HDFCBANK.NS
AXISBANK.NS
ICICIBANK.NS
KOTAKBANK.NS
RBLBANK.NS
FEDERALBANK.NS
BANDHANBANK.NS
AUBANK.NS
INDUSINDBANK.NS
IDFCFIRSTBANK.NS
SBIN.NS
BANKBARODA.NS
CANBANK.NS
PNB.NS
ABCAPITAL.NS
ANGELONE.NS
BAJAJFINSERV.NS
BAJAJFINANCE.NS
BSE.NS
CDSL.NS
HDFCAMC.NS
JIOFIN.NS
LICHOUSING.NS
LICI.NS
MANAPURAM.NS
MCX.NS
PFC.NS
REC.NS
SHRIRAMFINANCE.NS
HCLTECH.NS
INFY.NS
LTM.NS
TCS.NS
TECHM.NS
WIPRO.NS
HINDALCO.NS
HINDZINC.NS
NATIONALALUMINUM.NS
NMDC.NS
SAIL.NS
TATASTEEL.NS
VEDL.NS
DLF.NS
OBEROIREALITY.NS
BRITANNIA.NS
COLPAL.NS
DABUR.NS
HINDUNILVR.NS
MARICO.NS
TATACONSUMER.NS
BPCL.NS
COALINDIA.NS
GAIL.NS
HINDPETRO.NS
IOC.NS
OIL.NS
ONGC.NS
RELIANCE.NS
ASHOKLEY.NS
BAJAJAUTO.NS
BHARATFORG.NS
EICHER.NS
EXIDE.NS
HEROMOTO.NS
M&M.NS
MARUTI.NS
TMPV.NS
TVSMOTOR.NS
ASIANPAINT.NS
CROMPTON.NS
HAVELLS.NS
TITAN.NS
VOLTAS.NS
APOLLOHOSPITAL.NS
AUROPHARMA.NS
BIOCON.NS
DRREDDY.NS
LAURUSLAB.NS
LUPIN.NS
SUNPHARMA.NS
SRF.NS
SOLARINDUSTRY.NS
AMBUJACEMENT.NS
GRASIM.NS
LT.NS
NBCC.NS
ULTRATECH.NS
ABB.NS
ASTRAL.NS
BEL.NS
BHEL.NS
CGPOWER.NS
CUMMINS.NS
HAL.NS
KEI.NS
POLYCAB.NS
POWERINDIA.NS
ETERNAL.NS
INDHOTEL.NS
NYKAA.NS
TRENT.NS
NTPC.NS
TATAPOWER.NS
POWERGRID.NS
ADANIPORTS.NS
DELHIVERY.NS
CONCOR.NS
GMR.NS
INDIGO.NS
BHARTIAIRTEL.NS
"""

        if input_method == "Paste Symbols":
            symbols_text = st.text_area("Enter symbols (one per line):", height=150,
                                         value=default_symbols.strip())
            stock_list = [l.strip() for l in symbols_text.strip().splitlines() if l.strip()]
        else:
            stock_list = [l.strip() for l in default_symbols.strip().splitlines() if l.strip()]

        st.markdown(f"**{len(stock_list)} stocks loaded**")
        st.markdown("---")

        st.subheader("🔧 Strategy Parameters")
        ema_fast      = st.number_input("EMA Fast",              value=20,    min_value=5,    max_value=100)
        ema_slow      = st.number_input("EMA Slow",              value=50,    min_value=10,   max_value=200)
        fib_50        = st.number_input("Fib Level 1 (0.5)",    value=0.50,  min_value=0.10, max_value=0.90, step=0.01)
        fib_618       = st.number_input("Fib Level 2 (0.618)",  value=0.618, min_value=0.10, max_value=0.90, step=0.001)
        impulse_pct   = st.number_input("Min Impulse %",         value=0.5,   min_value=0.1,  max_value=5.0,  step=0.1)
        rr            = st.number_input("Risk:Reward Ratio",     value=2.0,   min_value=1.0,  max_value=5.0,  step=0.5)
        tolerance_pct = st.number_input("Open=High/Low Tolerance (%)",
                                         value=0.01, min_value=0.001, max_value=1.0,
                                         step=0.001, format="%.3f")
        days_back     = st.slider("Days of history to pull", min_value=30, max_value=200, value=100, step=10)

        st.markdown("---")
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ─────────────────────────────────────────────────────────────────────
    # HISTORICAL SCAN — PRE-FETCH PIPELINE
    # ─────────────────────────────────────────────────────────────────────
    if scan_button and stock_list and scan_mode == "Historical Scan":
        t0     = time.time()
        tv_pool = get_tv_pool()

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST          = ema_fast
        strategy.config.EMA_SLOW          = ema_slow
        strategy.config.FIB_LEVEL_1       = fib_50
        strategy.config.FIB_LEVEL_2       = fib_618
        strategy.config.MIN_IMPULSE_PCT   = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        # ── Phase 1 + 2: Parallel fetch ───────────────────────────────
        st.markdown('<div class="phase-box">📡 Phase 1 / 2 — Fetching all market data in parallel…</div>',
                    unsafe_allow_html=True)
        prog  = st.progress(0)
        sts   = st.empty()

        data_cache = prefetch_all(stock_list, tv_pool, days_back, prog, sts)
        t_fetch    = time.time() - t0

        prog.progress(1.0)
        sts.text(f"✅ Data fetched in {t_fetch:.1f}s  |  Now scanning…")

        # ── Phase 3: Pure-CPU scan ────────────────────────────────────
        st.markdown('<div class="phase-box">🧠 Phase 3 — Scanning logic (pure CPU, no network)…</div>',
                    unsafe_allow_html=True)
        t1          = time.time()
        all_signals = []
        missed      = 0

        for sym in stock_list:
            df_5  = data_cache.get(sym, {}).get(5,  pd.DataFrame())
            df_15 = data_cache.get(sym, {}).get(15, pd.DataFrame())
            if df_5.empty or df_15.empty:
                missed += 1
                continue
            sig = strategy.scan_from_cache(sym, df_5, df_15, scan_date, tolerance_pct)
            if sig:
                all_signals.append(sig)

        t_scan  = time.time() - t1
        t_total = time.time() - t0

        perf = {
            'total':   len(stock_list),
            'fetch_t': t_fetch,
            'scan_t':  t_scan,
            'total_t': t_total,
            'signals': len(all_signals),
            'missed':  missed,
        }
        display_results(all_signals, scan_date, perf)

    # ─────────────────────────────────────────────────────────────────────
    # REAL-TIME SCAN — SAME PIPELINE, LOOPS EVERY 5s / 60s
    # ─────────────────────────────────────────────────────────────────────
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span class="live-badge">🔴 INITIALIZING…</span></div>',
                    unsafe_allow_html=True)
        tv_pool = get_tv_pool()
        st.markdown('<div style="text-align:center;"><span class="live-badge" style="background-color:#28a745;">🟢 LIVE</span></div>',
                    unsafe_allow_html=True)

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST          = ema_fast
        strategy.config.EMA_SLOW          = ema_slow
        strategy.config.FIB_LEVEL_1       = fib_50
        strategy.config.FIB_LEVEL_2       = fib_618
        strategy.config.MIN_IMPULSE_PCT   = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        live_container = st.empty()

        while True:
            t0            = time.time()
            live_datetime = datetime.now(IST)
            ct            = live_datetime.time()
            is_open       = (
                live_datetime.weekday() < 5 and
                ct >= datetime.strptime("09:15", "%H:%M").time() and
                ct <= datetime.strptime("15:30", "%H:%M").time()
            )

            prog = st.progress(0)
            sts  = st.empty()

            cache   = prefetch_all(stock_list, tv_pool, days_back, prog, sts)
            t_fetch = time.time() - t0

            t1 = time.time(); all_signals = []; missed = 0
            for sym in stock_list:
                df5  = cache.get(sym, {}).get(5,  pd.DataFrame())
                df15 = cache.get(sym, {}).get(15, pd.DataFrame())
                if df5.empty or df15.empty:
                    missed += 1; continue
                sig = strategy.scan_from_cache(sym, df5, df15, live_datetime, tolerance_pct)
                if sig: all_signals.append(sig)
            t_scan = time.time() - t1

            with live_container.container():
                mkt = "🟢 Market Open" if is_open else "🔴 Market Closed"
                st.write(f"⏱️ {datetime.now(IST).strftime('%H:%M:%S IST')} | {mkt}")
                display_results(all_signals, live_datetime, {
                    'total': len(stock_list), 'fetch_t': t_fetch,
                    'scan_t': t_scan, 'total_t': time.time()-t0,
                    'signals': len(all_signals), 'missed': missed,
                })

            time.sleep(60 if not is_open else 5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
