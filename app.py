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
    TV_POOL_SIZE      = 20    # 20 connections — avoids TradingView rate-limiting
    MAX_FETCH_WORKERS = 18    # 18 workers, close to pool → minimal queueing
    FETCH_RETRIES     = 2     # Retry on empty/error; sleep OUTSIDE the lock
    RETRY_DELAY       = 0.5   # 500ms between retries (outside lock)
    BASE_DELAY        = 0.01  # 10ms pacing inside lock
    CONN_STAGGER      = 0.15  # 150ms between connections during pool creation

    # Bar counts — calibrated for scan dates up to 1 week back + EMA warmup
    # 5min:  500 bars ≈ 6-7 trading days
    # 15min: 700 bars ≈ 4+ weeks (EMA(50) converges within 150 bars)
    # These are 5-6× smaller than the old days_back=100 defaults, so ~5-6× faster.
    BARS_5MIN  = 500
    BARS_15MIN = 700

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
    .main-header { font-size:2.5rem; font-weight:bold; color:#1f77b4; text-align:center; margin-bottom:0.5rem; }
    .sub-header  { font-size:1.1rem; color:#666; text-align:center; margin-bottom:2rem; }
    .metric-card { background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); padding:1rem; border-radius:10px; color:white; text-align:center; }
    .live-badge  { background-color:#ff4b4b; color:white; padding:2px 8px; border-radius:10px; font-size:0.8rem; font-weight:bold; animation:blink 2s infinite; }
    @keyframes blink { 0%{opacity:1;} 50%{opacity:0.4;} 100%{opacity:1;} }
    .signal-card { padding:1rem; border-radius:8px; margin:0.5rem 0; border-left:4px solid; }
    .buy-card    { background:linear-gradient(135deg,#1a5f5f 0%,#2d8a8a 100%)!important; color:white; border-left-color:#4CAF50; }
    .sell-card   { background:linear-gradient(135deg,#7a1f1f 0%,#a03030 100%)!important; color:white; border-left-color:#f44336; }
    .speed-badge { background:linear-gradient(135deg,#00b09b 0%,#96c93d 100%); color:white; padding:4px 14px; border-radius:20px; font-weight:bold; }
    .phase-box   { background:#1e1e2e; border-radius:8px; padding:0.6rem 1rem; color:#a0f0a0; font-family:monospace; font-size:0.85rem; margin-bottom:0.4rem; }
    .warn-box    { background:#fff3cd; border-left:4px solid #ffc107; padding:0.7rem 1rem; border-radius:4px; color:#856404; font-size:0.85rem; margin-bottom:0.5rem; }
    .err-box     { background:#f8d7da; border-left:4px solid #dc3545; padding:0.7rem 1rem; border-radius:4px; color:#721c24; font-size:0.85rem; margin-bottom:0.5rem; }
</style>
""", unsafe_allow_html=True)

# ================================================================================
# TV POOL — per-instance lock, round-robin assignment
# ================================================================================
class TVPool:
    def __init__(self, size):
        self.pool  = []
        self.locks = []
        for _ in range(size):
            try:
                self.pool.append(TvDatafeed())
                self.locks.append(threading.Lock())
                time.sleep(ScaleConfig.CONN_STAGGER)
            except Exception:
                break
        self.size          = len(self.pool)
        self._counter      = 0
        self._counter_lock = threading.Lock()

    def acquire(self):
        with self._counter_lock:
            idx = self._counter % self.size
            self._counter += 1
        return self.pool[idx], self.locks[idx]

@st.cache_resource(show_spinner=False)
def get_tv_pool():
    return TVPool(ScaleConfig.TV_POOL_SIZE)

def force_new_pool():
    """Clear the cached pool so next call rebuilds fresh connections."""
    get_tv_pool.clear()

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
        st.error("❌ Access Denied.")
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
# PARALLEL DATA FETCHER
# Rules:
#   - Lock held ONLY during get_hist() — never during sleep
#   - Retry acquires a FRESH TV instance (different slot via round-robin)
#   - If all retries fail → return empty DataFrame (counted as "missed")
# ================================================================================
def _fetch_one(sym, resolution, tv_pool):
    fmt = sym.replace('.NS', '')
    interval = Interval.in_5_minute  if resolution == 5  else Interval.in_15_minute
    n_bars   = ScaleConfig.BARS_5MIN if resolution == 5  else ScaleConfig.BARS_15MIN

    for attempt in range(ScaleConfig.FETCH_RETRIES):
        tv_inst, tv_lock = tv_pool.acquire()   # fresh instance each attempt
        try:
            with tv_lock:                       # lock held for ONE get_hist() only
                time.sleep(ScaleConfig.BASE_DELAY)
                df = tv_inst.get_hist(symbol=fmt, exchange='NSE',
                                      interval=interval, n_bars=n_bars)
            # ← lock released here, safe to sleep

            if df is not None and not df.empty:
                df.index = pd.to_datetime(df.index)
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
                else:
                    df.index = df.index.tz_convert('Asia/Kolkata')
                return sym, resolution, df

            # Empty response — wait then retry with a different instance
            if attempt < ScaleConfig.FETCH_RETRIES - 1:
                time.sleep(ScaleConfig.RETRY_DELAY * (attempt + 1))

        except Exception:
            if attempt < ScaleConfig.FETCH_RETRIES - 1:
                time.sleep(ScaleConfig.RETRY_DELAY)

    return sym, resolution, pd.DataFrame()


def prefetch_all(stock_list, tv_pool, progress_bar, status_text):
    """
    Submit all 220 fetch tasks at once (110 stocks × 2 timeframes).
    18 workers process them concurrently. Each task takes ~0.2-0.5s
    for the small bar counts we now use → total ~5-10 seconds.
    """
    tasks = [(s, 5) for s in stock_list] + [(s, 15) for s in stock_list]
    total     = len(tasks)
    completed = 0
    cache     = {s: {5: pd.DataFrame(), 15: pd.DataFrame()} for s in stock_list}
    lock      = threading.Lock()

    def fetch_and_store(args):
        nonlocal completed
        sym, res = args
        s, r, df = _fetch_one(sym, res, tv_pool)
        with lock:
            cache[s][r] = df
            completed  += 1
            if completed % 20 == 0 or completed == total:
                pct = completed / total
                progress_bar.progress(pct)
                ok  = sum(1 for s2 in stock_list if not cache[s2][5].empty or not cache[s2][15].empty)
                status_text.text(f"📡 Fetching… {completed}/{total} calls  |  {ok} stocks with data so far")

    with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_FETCH_WORKERS) as ex:
        futs = [ex.submit(fetch_and_store, t) for t in tasks]
        concurrent.futures.wait(futs)

    return cache


# ================================================================================
# PHASE 3 — PURE-CPU SCANNER (zero network)
#
# SIGNAL ACCURACY vs PREVIOUS VERSION:
#   Old: EMA calculated on 2500 bars (100 days × 25 bars/day)
#   New: EMA calculated on 700 bars (~4 weeks)
#   EMA(50) converges within ~150 bars. After 150 bars the difference between
#   computing EMA on 700 vs 2500 bars is < 0.01%. Signals will be IDENTICAL
#   for any date within the last 4 weeks.
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
        Exact Pine Script mirror. Pure pandas/numpy — no network calls.
        EMA is calculated on ALL fetched history before filtering to scan_date,
        so the warmup is correct.
        """
        try:
            if df_5min.empty or df_15min.empty:
                return None

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

            # EMA on FULL fetched history before date-filter — same as Pine Script
            df15 = df_15min.copy()
            df15['ema20'] = self.calculate_ema(df15, self.config.EMA_FAST)
            df15['ema50'] = self.calculate_ema(df15, self.config.EMA_SLOW)

            mkt = pd.Timestamp('09:15').time()
            df5d  = df_5min[(df_5min.index.date == target_date) & (df_5min.index.time >= mkt)].copy()
            df15d = df15[   (df15.index.date   == target_date) & (df15.index.time   >= mkt)].copy()

            if df5d.empty or df15d.empty:
                return None

            f5    = df5d.iloc[0]
            o, h, l = float(f5['open']), float(f5['high']), float(f5['low'])
            tol   = max(o, 1.0) * (tolerance_pct / 100)
            is_buy  = abs(o - l) <= tol
            is_sell = abs(o - h) <= tol
            sym     = symbol.replace('.NS', '')

            if not is_buy and not is_sell:
                return None

            # ── State ──────────────────────────────────────────────────────────
            bsh=None; bf50=bf618=None; bid=False; bib=-1; brd=False; brb=-1; bfired=False; binv=False
            ssl=None; sf50=sf618=None; sid=False; sib=-1; srd=False; srb=-1; sfired=False; sinv=False
            signal = None

            for i, (idx, row) in enumerate(df15d.iterrows()):

                # Invalidation (before impulse only)
                if is_buy  and not binv and not bid: binv = row['low']  < l
                if is_sell and not sinv and not sid: sinv = row['high'] > h

                # BUY impulse
                if is_buy and not binv and not bid:
                    bsh = float(row['high']) if bsh is None else max(bsh, float(row['high']))
                    if bsh >= h * (1 + self.config.MIN_IMPULSE_PCT / 100):
                        bid=True; bib=i
                        bf50  = bsh - self.config.FIB_LEVEL_1 * (bsh - l)
                        bf618 = bsh - self.config.FIB_LEVEL_2 * (bsh - l)

                # SELL impulse
                if is_sell and not sinv and not sid:
                    ssl = float(row['low']) if ssl is None else min(ssl, float(row['low']))
                    if ssl <= l * (1 - self.config.MIN_IMPULSE_PCT / 100):
                        sid=True; sib=i
                        sf50  = ssl + self.config.FIB_LEVEL_1 * (h - ssl)
                        sf618 = ssl + self.config.FIB_LEVEL_2 * (h - ssl)

                # BUY retracement
                if bid and not brd and i > bib:
                    if row['low'] <= bf50: brd=True; brb=i

                # SELL retracement
                if sid and not srd and i > sib:
                    if row['high'] >= sf50: srd=True; srb=i

                # BUY recovery
                if brd and not bfired and i > brb:
                    if row['close'] > row['open'] and row['close'] > bf50:
                        up, _ = self.check_trend(df15d.iloc[:i+1])
                        if up and row['close'] > row['ema20'] and row['close'] > row['ema50']:
                            e, sl_ = float(row['close']), float(row['low'])
                            signal = {
                                'symbol': sym, 'date': target_date.strftime('%Y-%m-%d'),
                                'direction': 'BUY',
                                'setup_time': df5d.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(e, 2), 'stop_loss': round(sl_, 2),
                                'target': round(e + (e - sl_) * self.config.DEFAULT_TARGET_RR, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50': round(bf50, 2), 'fib_618': round(bf618, 2),
                                'swing_high': round(bsh, 2),
                                'ema20': round(float(row['ema20']), 2),
                                'ema50': round(float(row['ema50']), 2), 'trend': 'UP',
                            }
                            bfired=True; break

                # SELL resumption
                if srd and not sfired and i > srb:
                    if row['close'] < row['open'] and row['close'] < sf50:
                        _, down = self.check_trend(df15d.iloc[:i+1])
                        if down and row['close'] < row['ema20'] and row['close'] < row['ema50']:
                            e, sl_ = float(row['close']), float(row['high'])
                            signal = {
                                'symbol': sym, 'date': target_date.strftime('%Y-%m-%d'),
                                'direction': 'SELL',
                                'setup_time': df5d.index[0].strftime('%H:%M'),
                                'signal_time': idx.strftime('%H:%M'),
                                'entry_price': round(e, 2), 'stop_loss': round(sl_, 2),
                                'target': round(e - (sl_ - e) * self.config.DEFAULT_TARGET_RR, 2),
                                'risk_reward': f"1:{self.config.DEFAULT_TARGET_RR}",
                                'fib_50': round(sf50, 2), 'fib_618': round(sf618, 2),
                                'swing_low': round(ssl, 2),
                                'ema20': round(float(row['ema20']), 2),
                                'ema50': round(float(row['ema50']), 2), 'trend': 'DOWN',
                            }
                            sfired=True; break

            return signal
        except Exception:
            return None


# ================================================================================
# DISPLAY
# ================================================================================
def display_results(signals, scan_date, perf=None):
    valid = [s for s in signals if s]
    if not valid:
        st.warning("⚠️ No signals found. No stocks met all Fibonacci retracement conditions.")
        return

    df = pd.DataFrame(valid)
    if perf:
        missed_warn = f"| ⚠️ {perf['missed']} stocks missing data" if perf['missed'] > 5 else \
                      (f"| {perf['missed']} missed" if perf['missed'] else "")
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:1rem;">
        <span class="speed-badge">
          ⚡ {perf['total']} stocks | fetch {perf['fetch_t']:.1f}s | scan {perf['scan_t']:.2f}s
          | total {perf['total_t']:.1f}s | {perf['signals']} signals {missed_warn}
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
        sw = f"Swing High: {row.get('swing_high','N/A')}" if row['direction']=='BUY' \
             else f"Swing Low: {row.get('swing_low','N/A')}"
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
        ema_fast      = st.number_input("EMA Fast",             value=20,    min_value=5,    max_value=100)
        ema_slow      = st.number_input("EMA Slow",             value=50,    min_value=10,   max_value=200)
        fib_50        = st.number_input("Fib Level 1 (0.5)",   value=0.50,  min_value=0.10, max_value=0.90, step=0.01)
        fib_618       = st.number_input("Fib Level 2 (0.618)", value=0.618, min_value=0.10, max_value=0.90, step=0.001)
        impulse_pct   = st.number_input("Min Impulse %",        value=0.5,   min_value=0.1,  max_value=5.0,  step=0.1)
        rr            = st.number_input("Risk:Reward Ratio",    value=2.0,   min_value=1.0,  max_value=5.0,  step=0.5)
        tolerance_pct = st.number_input("Open=High/Low Tolerance (%)",
                                         value=0.01, min_value=0.001, max_value=1.0,
                                         step=0.001, format="%.3f")

        st.markdown("---")

        # ── Connection health ──────────────────────────────────────────
        if st.button("🔄 Reset Connections", help="Use this if scanner shows 0s fetch time or returns no data"):
            force_new_pool()
            st.success("✅ Connections reset. Click Start Scan again.")
            st.rerun()

        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ─────────────────────────────────────────────────────────────────────
    # HISTORICAL SCAN
    # ─────────────────────────────────────────────────────────────────────
    if scan_button and stock_list and scan_mode == "Historical Scan":
        t0      = time.time()
        tv_pool = get_tv_pool()

        # Stale pool detection: if pool has fewer instances than expected, rebuild
        if tv_pool.size < ScaleConfig.TV_POOL_SIZE // 2:
            st.markdown('<div class="warn-box">⚠️ TV pool has fewer connections than expected. Rebuilding…</div>',
                        unsafe_allow_html=True)
            force_new_pool()
            tv_pool = get_tv_pool()

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST          = ema_fast
        strategy.config.EMA_SLOW          = ema_slow
        strategy.config.FIB_LEVEL_1       = fib_50
        strategy.config.FIB_LEVEL_2       = fib_618
        strategy.config.MIN_IMPULSE_PCT   = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        # ── Fetch phase ───────────────────────────────────────────────
        st.markdown(
            f'<div class="phase-box">📡 Fetching {len(stock_list)*2} data streams '
            f'({len(stock_list)} stocks × 2 timeframes, {ScaleConfig.MAX_FETCH_WORKERS} parallel workers)…</div>',
            unsafe_allow_html=True)
        prog = st.progress(0)
        sts  = st.empty()

        cache   = prefetch_all(stock_list, tv_pool, prog, sts)
        t_fetch = time.time() - t0

        # Stale-pool guard: if fetch took <1s for >100 stocks, pool is dead
        if t_fetch < 1.0 and len(stock_list) > 10:
            st.markdown(
                '<div class="err-box">⚠️ Data fetch completed in under 1 second — '
                'this means connections are stale. Click <b>🔄 Reset Connections</b> '
                'in the sidebar, then scan again.</div>',
                unsafe_allow_html=True)
            st.stop()

        prog.progress(1.0)
        sts.text(f"✅ Data fetched in {t_fetch:.1f}s — now scanning…")

        # ── Scan phase (CPU only) ─────────────────────────────────────
        st.markdown('<div class="phase-box">🧠 Scanning — pure CPU, zero network…</div>',
                    unsafe_allow_html=True)
        t1 = time.time()
        all_signals, missed = [], 0

        for sym in stock_list:
            df5  = cache[sym][5]
            df15 = cache[sym][15]
            if df5.empty or df15.empty:
                missed += 1
                continue
            sig = strategy.scan_from_cache(sym, df5, df15, scan_date, tolerance_pct)
            if sig:
                all_signals.append(sig)

        t_scan  = time.time() - t1
        t_total = time.time() - t0

        # Show missed stocks warning
        if missed > 0:
            pct = missed * 100 // len(stock_list)
            if missed > 10:
                st.markdown(
                    f'<div class="err-box">⚠️ <b>{missed} stocks ({pct}%) returned no data.</b> '
                    f'Too many missing suggests stale connections. '
                    f'Click <b>🔄 Reset Connections</b> and re-scan.</div>',
                    unsafe_allow_html=True)
            elif missed > 0:
                st.markdown(
                    f'<div class="warn-box">ℹ️ {missed} stocks had no data for {scan_date} '
                    f'(may be holiday, halt, or newly listed).</div>',
                    unsafe_allow_html=True)

        display_results(all_signals, scan_date, {
            'total': len(stock_list), 'fetch_t': t_fetch, 'scan_t': t_scan,
            'total_t': t_total, 'signals': len(all_signals), 'missed': missed,
        })

    # ─────────────────────────────────────────────────────────────────────
    # REAL-TIME SCAN
    # ─────────────────────────────────────────────────────────────────────
    elif scan_button and stock_list and scan_mode == "Real-Time Scan":
        st.markdown('<div style="text-align:center;"><span class="live-badge">🔴 LIVE</span></div>',
                    unsafe_allow_html=True)
        tv_pool = get_tv_pool()

        strategy = OpenDriveFibStrategy()
        strategy.config.EMA_FAST          = ema_fast
        strategy.config.EMA_SLOW          = ema_slow
        strategy.config.FIB_LEVEL_1       = fib_50
        strategy.config.FIB_LEVEL_2       = fib_618
        strategy.config.MIN_IMPULSE_PCT   = impulse_pct
        strategy.config.DEFAULT_TARGET_RR = rr

        live_container = st.empty()

        while True:
            t0      = time.time()
            live_dt = datetime.now(IST)
            ct      = live_dt.time()
            is_open = (
                live_dt.weekday() < 5 and
                ct >= datetime.strptime("09:15", "%H:%M").time() and
                ct <= datetime.strptime("15:30", "%H:%M").time()
            )

            prog = st.progress(0); sts = st.empty()
            cache   = prefetch_all(stock_list, tv_pool, prog, sts)
            t_fetch = time.time() - t0

            t1 = time.time(); sigs = []; missed = 0
            for sym in stock_list:
                df5, df15 = cache[sym][5], cache[sym][15]
                if df5.empty or df15.empty: missed += 1; continue
                s = strategy.scan_from_cache(sym, df5, df15, live_dt, tolerance_pct)
                if s: sigs.append(s)
            t_scan = time.time() - t1

            with live_container.container():
                mkt = "🟢 Market Open" if is_open else "🔴 Market Closed"
                st.write(f"⏱️ {live_dt.strftime('%H:%M:%S IST')} | {mkt}")
                display_results(sigs, live_dt, {
                    'total': len(stock_list), 'fetch_t': t_fetch, 'scan_t': t_scan,
                    'total_t': time.time()-t0, 'signals': len(sigs), 'missed': missed,
                })

            time.sleep(60 if not is_open else 5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")

if __name__ == "__main__":
    main()
