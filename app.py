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
    WHY THE PREVIOUS VERSION WAS SLOW (46s):
    ─────────────────────────────────────────
    1. days_back=200  →  5min: 15,000 bars/stock, 15min: 5,000 bars/stock
       Each fetch call was downloading a huge payload → 1-8s per call.
    2. 40 TV connections at once → TradingView rate-limited us → 45 stocks failed.
    3. Failed fetches triggered retries → extra wait time.

    THE FIX — two key insights:
    ─────────────────────────────
    A) We only need ~180 bars of 15min data (7 days) for EMA(50) to be accurate.
       We only need ~100 bars of 5min data (2 days) to get today's first candle.
       These tiny payloads fetch in 150-300ms each instead of 1-8s.

    B) Fewer connections (20) = TradingView doesn't rate-limit us
       → near-zero missing stocks.

    EXPECTED RESULT: ~5-10 seconds for 110 stocks.
    """
    TV_POOL_SIZE      = 20    # 20 connections — sweet spot before TradingView throttles
    MAX_FETCH_WORKERS = 18    # Close to pool size — minimal queueing
    BASE_DELAY        = 0.005 # 5ms pacing (minimal)
    CONN_STAGGER      = 0.15  # 150ms per TV instance during pool creation (20×0.15=3s)

    # EXACT bar counts — no more days_back multiplication
    # EMA(50) needs 50 warmup bars + today's bars (~26 for a full day) + buffer
    BARS_15MIN = 200   # ~8 trading days of 15min data — plenty for EMA accuracy
    BARS_5MIN  = 120   # ~2 trading days of 5min data — just need today's first candle

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
    .phase-box    { background:#1e1e2e; border-radius:8px; padding:0.6rem 1rem; color:#a0f0a0; font-family:monospace; font-size:0.85rem; margin-bottom:0.4rem; }
    .warn-box     { background:#fff3cd; border-left:4px solid #ffc107; padding:0.6rem 1rem; border-radius:4px; color:#856404; font-size:0.85rem; }
</style>
""", unsafe_allow_html=True)

# ================================================================================
# TV POOL — PER-INSTANCE LOCK, ROUND-ROBIN
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
# PARALLEL DATA FETCHER — lock held only during get_hist(), never during sleep
# ================================================================================
def _fetch_one(sym, resolution, tv_pool):
    """
    Single get_hist() call. Lock is held ONLY during the API call.
    If it returns empty → return empty DataFrame immediately (no sleep, no retry).
    The 'missing data' counter is shown in the UI so the user can see it.
    With small BARS counts (200/120), empty responses are rare (TradingView rarely
    times out on small payloads).
    """
    tv_inst, tv_lock = tv_pool.acquire()
    fmt = sym.replace('.NS', '')

    if resolution == 5:
        interval = Interval.in_5_minute
        n_bars   = ScaleConfig.BARS_5MIN
    else:
        interval = Interval.in_15_minute
        n_bars   = ScaleConfig.BARS_15MIN

    try:
        with tv_lock:
            time.sleep(ScaleConfig.BASE_DELAY)
            df = tv_inst.get_hist(symbol=fmt, exchange='NSE', interval=interval, n_bars=n_bars)

        # Lock released — safe to process
        if df is None or df.empty:
            return sym, resolution, pd.DataFrame()

        df.index = pd.to_datetime(df.index)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
        else:
            df.index = df.index.tz_convert('Asia/Kolkata')
        return sym, resolution, df

    except Exception:
        return sym, resolution, pd.DataFrame()


def prefetch_all(stock_list, tv_pool, progress_bar, status_text):
    """
    Submit ALL fetch tasks at once (220 for 110 stocks × 2 timeframes).
    ThreadPoolExecutor queues them, 18 run at a time.
    Each task holds the lock for ~150-300ms (small payload).
    Total: 220 tasks / 18 workers × 0.25s avg ≈ 3 seconds.
    """
    tasks     = [(s, 5) for s in stock_list] + [(s, 15) for s in stock_list]
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
            if completed % 15 == 0 or completed == total:
                progress_bar.progress(completed / total)
                status_text.text(
                    f"📡 Fetching… {completed}/{total} calls complete  "
                    f"({completed*100//total}%)"
                )

    with ThreadPoolExecutor(max_workers=ScaleConfig.MAX_FETCH_WORKERS) as ex:
        futs = [ex.submit(fetch_and_store, t) for t in tasks]
        concurrent.futures.wait(futs)

    return cache


# ================================================================================
# PHASE 3 — PURE-CPU SCANNER (zero network, deterministic, instant)
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
        try:
            if df_5min.empty or df_15min.empty:
                return None

            target_date = scan_date.date() if hasattr(scan_date, 'date') else scan_date

            # EMA on FULL history before date filtering — critical for accuracy
            df15 = df_15min.copy()
            df15['ema20'] = self.calculate_ema(df15, self.config.EMA_FAST)
            df15['ema50'] = self.calculate_ema(df15, self.config.EMA_SLOW)

            mkt_start = pd.Timestamp('09:15').time()
            df5d  = df_5min[(df_5min.index.date == target_date) & (df_5min.index.time >= mkt_start)].copy()
            df15d = df15[   (df15.index.date   == target_date) & (df15.index.time   >= mkt_start)].copy()

            if df5d.empty or df15d.empty:
                return None

            f5m = df5d.iloc[0]
            o, h, l = float(f5m['open']), float(f5m['high']), float(f5m['low'])
            tol = max(o, 1.0) * (tolerance_pct / 100)

            is_buy  = abs(o - l) <= tol
            is_sell = abs(o - h) <= tol
            sym     = symbol.replace('.NS', '')

            if not is_buy and not is_sell:
                return None

            # ── State ──
            bsh = None; bf50 = bf618 = None
            bid = False; bib = -1; brd = False; brb = -1; bfired = False; binv = False

            ssl = None; sf50 = sf618 = None
            sid = False; sib = -1; srd = False; srb = -1; sfired = False; sinv = False

            signal = None

            for i, (idx, row) in enumerate(df15d.iterrows()):

                if is_buy  and not binv and not bid: binv = row['low']  < l
                if is_sell and not sinv and not sid: sinv = row['high'] > h

                if is_buy and not binv and not bid:
                    bsh = float(row['high']) if bsh is None else max(bsh, float(row['high']))
                    if bsh >= h * (1 + self.config.MIN_IMPULSE_PCT / 100):
                        bid = True; bib = i
                        bf50  = bsh - self.config.FIB_LEVEL_1 * (bsh - l)
                        bf618 = bsh - self.config.FIB_LEVEL_2 * (bsh - l)

                if is_sell and not sinv and not sid:
                    ssl = float(row['low']) if ssl is None else min(ssl, float(row['low']))
                    if ssl <= l * (1 - self.config.MIN_IMPULSE_PCT / 100):
                        sid = True; sib = i
                        sf50  = ssl + self.config.FIB_LEVEL_1 * (h - ssl)
                        sf618 = ssl + self.config.FIB_LEVEL_2 * (h - ssl)

                if bid and not brd and i > bib:
                    if row['low'] <= bf50: brd = True; brb = i

                if sid and not srd and i > sib:
                    if row['high'] >= sf50: srd = True; srb = i

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
                            bfired = True; break

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
                            sfired = True; break

            return signal
        except Exception:
            return None


# ================================================================================
# DISPLAY
# ================================================================================
def display_results(signals, scan_date, perf=None):
    valid = [s for s in signals if s]
    if not valid:
        st.warning("⚠️ No signals found.")
        return

    df = pd.DataFrame(valid)

    if perf:
        missed_pct = perf['missed'] * 100 // perf['total'] if perf['total'] else 0
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:1rem;">
        <span class="speed-badge">
          ⚡ {perf['total']} stocks | fetch {perf['fetch_t']:.1f}s | scan {perf['scan_t']:.2f}s
          | total {perf['total_t']:.1f}s | {perf['signals']} signals
        </span>
        </div>
        """, unsafe_allow_html=True)
        if perf['missed'] > 0:
            st.markdown(f"""
            <div class="warn-box">
              ⚠️ <b>{perf['missed']} stocks ({missed_pct}%) had no data</b> for scan date
              {scan_date} — they may be holidays, halted, or newly listed.
              Check the symbol names in your list.
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
        scan_button = st.button("🚀 Start Fib Scan", type="primary")

    # ─────────────────────────────────────────────────────────────────────
    # HISTORICAL SCAN
    # ─────────────────────────────────────────────────────────────────────
    if scan_button and stock_list and scan_mode == "Historical Scan":
        t0      = time.time()
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
            f'({len(stock_list)} stocks × 2 timeframes) in parallel…</div>',
            unsafe_allow_html=True)
        prog = st.progress(0)
        sts  = st.empty()

        cache   = prefetch_all(stock_list, tv_pool, prog, sts)
        t_fetch = time.time() - t0

        prog.progress(1.0)
        sts.text(f"✅ All data fetched in {t_fetch:.1f}s — now scanning…")

        # ── Scan phase (CPU only) ─────────────────────────────────────
        st.markdown('<div class="phase-box">🧠 Scanning logic — pure CPU, zero network calls…</div>',
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

        display_results(all_signals, scan_date, {
            'total': len(stock_list), 'fetch_t': t_fetch,
            'scan_t': t_scan, 'total_t': t_total,
            'signals': len(all_signals), 'missed': missed,
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
            t0            = time.time()
            live_dt       = datetime.now(IST)
            ct            = live_dt.time()
            is_open       = (
                live_dt.weekday() < 5 and
                ct >= datetime.strptime("09:15", "%H:%M").time() and
                ct <= datetime.strptime("15:30", "%H:%M").time()
            )

            prog = st.progress(0)
            sts  = st.empty()
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
                    'total': len(stock_list), 'fetch_t': t_fetch,
                    'scan_t': t_scan, 'total_t': time.time()-t0,
                    'signals': len(sigs), 'missed': missed,
                })

            time.sleep(60 if not is_open else 5)

    elif not stock_list:
        st.info("Please add stocks to scan from the sidebar.")


if __name__ == "__main__":
    main()
