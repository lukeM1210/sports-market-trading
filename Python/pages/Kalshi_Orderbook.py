"""
Kalshi Order Book — Sharp Liquidity Tracker
============================================
Live Kalshi order book depth for MLB prediction markets with sharp money signals.

API format (2026, api.elections.kalshi.com):
  GET /markets/{ticker}/orderbook → {"orderbook_fp": {"yes_dollars": [...], "no_dollars": [...]}}
  Each level: ["price_decimal", "dollar_amount"]  e.g. ["0.5700", "130371.44"]
  Prices are decimal fractions (0.57 = 57¢).  Levels sorted ascending in response.
  Trades endpoint returns 404 for sports markets — omitted.

Sharp signals:
  - Spread tightness: 1¢ spread on a game = very liquid, sharp-driven market
  - Order Flow Imbalance (OFI): dollar-weighted YES vs NO depth
  - Large-level detection: single price levels with ≥ $X at stake
  - Sharp Score: composite 0–100 index
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from dotenv import load_dotenv

st.set_page_config(page_title="Kalshi Order Book", layout="wide", page_icon="📊")
load_dotenv(override=True)

BASE = Path(__file__).parent.parent
SNAPSHOT_DIR = BASE / "MLB" / "kalshi_snapshots"
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(BASE))
from kalshi_client import KalshiClient  # noqa: E402

# ── sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Kalshi Settings")

    env_key = os.getenv("KALSHI_API_KEY", "").strip()
    if env_key:
        st.success("API key loaded from .env")
        api_key = env_key
    else:
        api_key = st.text_input(
            "Kalshi API Key",
            type="password",
            placeholder="Paste your API key here",
            help="kalshi.com → Account → API Settings",
        )

    st.markdown("---")
    st.markdown("**Manual market entry**")
    manual_ticker = st.text_input(
        "Market ticker (optional)",
        placeholder="e.g. KXMLBGAME-26JUN061410KCMIN-MIN",
        help=(
            "Paste any Kalshi market ticker from the game's URL on kalshi.com. "
            "Bypasses the event browser and goes straight to the order book."
        ),
    ).strip()

    st.markdown("---")
    refresh_mode = st.select_slider(
        "Auto-refresh", options=["Off", "30s", "1 min", "5 min"], value="1 min"
    )
    depth = st.slider("Order book depth (levels per side)", 5, 25, 10, 5)
    sharp_threshold_k = st.slider(
        "Sharp $ threshold (K)", min_value=10, max_value=500, value=100, step=10,
        help="Highlight price levels with at least this many thousand dollars",
    )
    sharp_threshold = sharp_threshold_k * 1000

    st.markdown("---")
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()

# ── title ──────────────────────────────────────────────────────────────────────
st.title("Kalshi Order Book — Sharp Liquidity Tracker")
st.caption(
    "Live order book depth and sharp money signals from Kalshi MLB prediction markets. "
    "Snapshots saved locally for price history."
)

if not api_key:
    st.warning(
        "**No Kalshi API key found.**  \n"
        "Add `KALSHI_API_KEY=your_key` to `.env`, or paste it in the sidebar."
    )
    st.stop()

# ── client ─────────────────────────────────────────────────────────────────────
client = KalshiClient(api_key=api_key)
_key_hash = str(hash(api_key))


@st.cache_data(ttl=120, show_spinner="Fetching MLB events from Kalshi…")
def _get_events(kh: str) -> list[dict]:
    return client.get_mlb_events()


@st.cache_data(ttl=60, show_spinner="Loading markets…")
def _get_markets(event_ticker: str, kh: str) -> list[dict]:
    return client.get_markets_for_event(event_ticker)


@st.cache_data(ttl=20, show_spinner="Loading order book…")
def _get_orderbook(ticker: str, d: int, kh: str) -> dict:
    return client.get_orderbook(ticker, d)


@st.cache_data(ttl=30, show_spinner="Loading market details…")
def _get_market(ticker: str, kh: str) -> dict:
    return client.get_market(ticker)


# ── market selection ───────────────────────────────────────────────────────────
try:
    events = _get_events(_key_hash)
except Exception as e:
    events = []
    if not manual_ticker:
        st.error(f"Failed to fetch MLB events: {e}")
        with st.expander("Diagnostics"):
            st.write("Base URL:", getattr(client, "base_url", "—"))
            st.write("URLs tried:", getattr(client, "_tried_urls", []))
        st.stop()


def _event_label(e: dict) -> str:
    title = e.get("title") or e.get("event_ticker", "")
    sub = e.get("sub_title") or ""
    return f"{title} — {sub}".rstrip(" —") if sub else title


if manual_ticker:
    market_ticker: str = manual_ticker
    chosen_market_label = manual_ticker

elif events:
    event_map = {_event_label(e): e for e in events}
    chosen_event_label = st.selectbox("Select MLB Game", list(event_map.keys()))
    chosen_event = event_map[chosen_event_label]
    event_ticker = chosen_event.get("event_ticker", "")

    try:
        markets = _get_markets(event_ticker, _key_hash)
    except Exception as e:
        st.error(f"Failed to load markets: {e}")
        st.stop()

    if not markets:
        st.info("No open markets for this event.")
        st.stop()

    market_map = {m.get("title") or m.get("ticker", ""): m for m in markets}
    chosen_market_label = st.selectbox("Select Market", list(market_map.keys()))
    market_ticker = market_map[chosen_market_label].get("ticker", "")

else:
    st.warning(
        "No open MLB game markets found via auto-discovery.  \n"
        "Paste a market ticker in the **Manual market entry** box in the sidebar  \n"
        "(find it on kalshi.com → any MLB game page, the ticker is in the URL)."
    )
    with st.expander("Diagnostics"):
        st.write("Base URL:", getattr(client, "base_url", "—"))
        st.write("URLs tried:", getattr(client, "_tried_urls", []))
    st.stop()

st.markdown("---")

# ── fetch data ─────────────────────────────────────────────────────────────────
try:
    raw_ob = _get_orderbook(market_ticker, depth, _key_hash)
    market_meta = _get_market(market_ticker, _key_hash)
except Exception as e:
    st.error(f"Failed to fetch order book for `{market_ticker}`: {e}")
    with st.expander("Diagnostics"):
        st.write("Base URL:", getattr(client, "base_url", "—"))
    st.stop()

# ── parse levels ───────────────────────────────────────────────────────────────

def _parse_levels(levels: list) -> list[tuple[int, float]]:
    """
    Convert API levels to (price_cents, dollar_amount) sorted descending (best first).
    Input format: [["0.5700", "130371.44"], ...] sorted ascending.
    """
    if not levels:
        return []
    result = []
    for lvl in levels:
        if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            price_raw, dollar_raw = lvl[0], lvl[1]
        elif isinstance(lvl, dict):
            price_raw = lvl.get("price", 0)
            dollar_raw = lvl.get("quantity", lvl.get("dollar_amount", 0))
        else:
            continue
        p = float(price_raw)
        price_cents = int(round(p * 100 if p <= 1.0 else p))
        result.append((price_cents, float(dollar_raw)))
    return sorted(result, key=lambda x: x[0], reverse=True)  # best bid first


yes_levels = _parse_levels(raw_ob.get("yes_dollars", []))
no_levels = _parse_levels(raw_ob.get("no_dollars", []))

yes_df = (pd.DataFrame(yes_levels, columns=["price", "dollars"])
          if yes_levels else pd.DataFrame(columns=["price", "dollars"]))
no_df = (pd.DataFrame(no_levels, columns=["price", "dollars"])
         if no_levels else pd.DataFrame(columns=["price", "dollars"]))
if not no_df.empty:
    no_df["yes_equiv"] = 100 - no_df["price"]

# ── key metrics from market meta + order book ──────────────────────────────────
# Market meta gives us best bid/ask directly (more reliable than deriving from book)
def _cents(field: str) -> int | None:
    v = market_meta.get(field)
    if v is None:
        return None
    f = float(v)
    return int(round(f * 100 if f <= 1.0 else f))


yes_bid_meta = _cents("yes_bid_dollars")    # highest price someone pays for YES
yes_ask_meta = _cents("yes_ask_dollars")    # lowest price someone sells YES
no_bid_meta  = _cents("no_bid_dollars")     # highest price someone pays for NO
last_price   = _cents("last_price_dollars") # last traded price

# Derive best YES bid/ask: YES bid from meta or top of yes_levels;
# YES ask = 100 - best NO bid
best_yes_bid = yes_bid_meta or (yes_levels[0][0] if yes_levels else None)
best_no_bid  = no_bid_meta  or (no_levels[0][0]  if no_levels  else None)
best_yes_ask = (100 - best_no_bid) if best_no_bid is not None else yes_ask_meta

mid_price = (
    round((best_yes_bid + best_yes_ask) / 2, 1)
    if best_yes_bid is not None and best_yes_ask is not None
    else (last_price or None)
)
spread = (
    best_yes_ask - best_yes_bid
    if best_yes_bid is not None and best_yes_ask is not None
    else None
)

# Order Flow Imbalance — dollar-weighted depth each side
yes_depth = sum(d for _, d in yes_levels) if yes_levels else 0.0
no_depth  = sum(d for _, d in no_levels)  if no_levels  else 0.0
total_depth = yes_depth + no_depth
ofi = ((yes_depth - no_depth) / total_depth * 100) if total_depth > 0 else 0.0

max_yes_dollars = max((d for _, d in yes_levels), default=0.0)
max_no_dollars  = max((d for _, d in no_levels),  default=0.0)
max_any_dollars = max(max_yes_dollars, max_no_dollars)

# ── sharp score (0–100) ────────────────────────────────────────────────────────
spread_score = max(0.0, 40 - (spread or 20) * 3) if spread is not None else 0.0
ofi_score    = min(30.0, abs(ofi) * 0.6)
if max_any_dollars >= sharp_threshold:
    size_score = 30.0
elif max_any_dollars >= sharp_threshold * 0.5:
    size_score = 15.0
else:
    size_score = 5.0
sharp_score = int(min(100, max(0, spread_score + ofi_score + size_score)))

# ── persist snapshot ───────────────────────────────────────────────────────────
_snap_path = SNAPSHOT_DIR / f"{market_ticker.replace('/', '_')}.csv"
pd.DataFrame([{
    "timestamp":    datetime.now(timezone.utc).isoformat(),
    "mid_price":    mid_price,
    "best_bid":     best_yes_bid,
    "best_ask":     best_yes_ask,
    "spread":       spread,
    "yes_depth":    round(yes_depth, 2),
    "no_depth":     round(no_depth, 2),
    "ofi":          round(ofi, 2),
    "sharp_score":  sharp_score,
}]).to_csv(_snap_path, mode="a", header=not _snap_path.exists(), index=False)

# ── live signal metrics ────────────────────────────────────────────────────────
yes_label = market_meta.get("yes_sub_title") or "YES"
no_label  = market_meta.get("no_sub_title")  or "NO"
game_title = market_meta.get("title") or chosen_market_label

st.subheader(f"Live Signals — {game_title}")
st.caption(
    f"{yes_label} to win · Last fetched {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(f"{yes_label} Best Bid",  f"{best_yes_bid}¢"  if best_yes_bid  is not None else "—")
c2.metric(f"{yes_label} Best Ask",  f"{best_yes_ask}¢"  if best_yes_ask  is not None else "—")
c3.metric("Mid Price",              f"{mid_price}¢"     if mid_price     is not None else "—",
          help="(Best bid + best ask) / 2")
c4.metric(
    "Spread",
    f"{spread}¢" if spread is not None else "—",
    delta="tight ✓" if (spread is not None and spread <= 2) else "wide",
    delta_color="normal" if (spread is not None and spread <= 2) else "inverse",
)
_ofi_side = f"{yes_label}-heavy" if ofi > 10 else (f"{no_label}-heavy" if ofi < -10 else "balanced")
c5.metric(
    "Order Flow Imbalance",
    f"{ofi:+.1f}%",
    delta=_ofi_side,
    delta_color="normal" if abs(ofi) < 10 else "off",
    help="Positive = more dollar depth on the YES side",
)

# ── sharp score + depth chart ─────────────────────────────────────────────────
st.markdown("---")
gauge_col, book_col = st.columns([1, 2])

with gauge_col:
    _gc = "#00c853" if sharp_score >= 65 else "#ffd600" if sharp_score >= 35 else "#d50000"
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=sharp_score,
        title={"text": "Sharp Liquidity Score", "font": {"size": 14}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar": {"color": _gc},
            "steps": [
                {"range": [0, 35], "color": "#1c0a0a"},
                {"range": [35, 65], "color": "#1c1c0a"},
                {"range": [65, 100], "color": "#0a1c0a"},
            ],
            "threshold": {"line": {"color": "white", "width": 2}, "thickness": 0.75, "value": 65},
        },
    ))
    fig_gauge.update_layout(template="plotly_dark", height=240,
                            margin=dict(t=30, b=10, l=20, r=20))
    st.plotly_chart(fig_gauge, use_container_width=True, key="gauge")

    st.markdown("**Signal Breakdown**")
    if spread is None:
        st.markdown("⚪ Spread: no data")
    elif spread <= 1:
        st.markdown(f"🟢 **{spread}¢ spread** — razor-thin, very sharp market")
    elif spread <= 3:
        st.markdown(f"🟢 Tight spread ({spread}¢) — active, liquid market")
    elif spread <= 6:
        st.markdown(f"🟡 Moderate spread ({spread}¢)")
    else:
        st.markdown(f"🔴 Wide spread ({spread}¢) — thin liquidity")

    if abs(ofi) >= 30:
        st.markdown(f"🟢 Strong lean → **{_ofi_side}** ({abs(ofi):.0f}%)")
    elif abs(ofi) >= 15:
        st.markdown(f"🟡 Mild lean → {_ofi_side} ({abs(ofi):.0f}%)")
    else:
        st.markdown("⚪ Balanced order flow")

    _thresh_k = sharp_threshold // 1000
    if max_any_dollars >= sharp_threshold:
        st.markdown(f"🟢 Large level ≥ ${_thresh_k}K at one price ⚡")
    elif max_any_dollars >= sharp_threshold * 0.5:
        st.markdown(f"🟡 Mid-sized level (~${max_any_dollars/1000:.0f}K at one price)")
    else:
        st.markdown(f"⚪ No oversized levels (threshold: ${_thresh_k}K)")

    st.markdown(
        f"**{yes_label} depth:** ${yes_depth:,.0f}  \n"
        f"**{no_label} depth:** ${no_depth:,.0f}"
    )

with book_col:
    fig_book = go.Figure()

    if not yes_df.empty:
        fig_book.add_trace(go.Bar(
            x=yes_df["dollars"],
            y=yes_df["price"],
            orientation="h",
            name=f"{yes_label} Bids (YES)",
            marker_color=[
                "#00e676" if d >= sharp_threshold else "#00c853"
                for d in yes_df["dollars"]
            ],
            hovertemplate=(
                f"{yes_label} bid: %{{y}}¢  |  $%{{x:,.0f}}<extra></extra>"
            ),
        ))

    if not no_df.empty:
        fig_book.add_trace(go.Bar(
            x=-no_df["dollars"],
            y=no_df["yes_equiv"],
            orientation="h",
            name=f"{no_label} Bids → YES asks",
            marker_color=[
                "#ff1744" if d >= sharp_threshold else "#d50000"
                for d in no_df["dollars"]
            ],
            hovertemplate=(
                f"YES ask equiv: %{{y}}¢  |  $%{{x:,.0f}}<extra></extra>"
            ),
        ))

    if mid_price is not None:
        fig_book.add_hline(
            y=mid_price, line_dash="dash", line_color="white", opacity=0.6,
            annotation_text=f"Mid {mid_price}¢", annotation_position="right",
        )

    fig_book.update_layout(
        template="plotly_dark",
        title=f"Order Book Depth  (⚡ = ≥ ${_thresh_k}K at level)",
        xaxis_title=f"← {no_label} side (asks)       {yes_label} side (bids) →",
        yaxis=dict(title="Price (¢)", range=[0, 100]),
        height=380,
        margin=dict(t=45, b=20, l=10, r=80),
        legend=dict(orientation="h", y=1.08),
    )
    st.plotly_chart(fig_book, use_container_width=True, key="depth_chart")

# ── order book ladder ──────────────────────────────────────────────────────────
st.markdown("---")
with st.expander("Order Book Ladder", expanded=True):
    lad_l, lad_r = st.columns(2)

    with lad_l:
        st.markdown(f"**{yes_label} Bids** — buying YES at X¢")
        if yes_df.empty:
            st.info("No YES bids.")
        else:
            _yd = yes_df.rename(columns={"price": "Price (¢)", "dollars": "$ Depth"}).copy()
            _yd["⚡"] = _yd["$ Depth"].apply(lambda d: "⚡" if d >= sharp_threshold else "")
            _yd["$ Depth"] = _yd["$ Depth"].round(0).astype(int)
            st.dataframe(
                _yd.style.background_gradient(subset=["$ Depth"], cmap="Greens"),
                hide_index=True, use_container_width=True,
            )

    with lad_r:
        st.markdown(f"**{no_label} Bids** — buying NO at X¢ (= YES asks at 100−X¢)")
        if no_df.empty:
            st.info("No NO bids.")
        else:
            _nd = no_df[["price", "yes_equiv", "dollars"]].rename(columns={
                "price": "NO Price (¢)",
                "yes_equiv": "YES Ask (¢)",
                "dollars": "$ Depth",
            }).copy()
            _nd["⚡"] = _nd["$ Depth"].apply(lambda d: "⚡" if d >= sharp_threshold else "")
            _nd["$ Depth"] = _nd["$ Depth"].round(0).astype(int)
            st.dataframe(
                _nd.style.background_gradient(subset=["$ Depth"], cmap="Reds"),
                hide_index=True, use_container_width=True,
            )

# ── price history ──────────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("📈 Price History")

if not _snap_path.exists():
    st.info("No snapshot history yet — refreshes will accumulate data here.")
else:
    hist = pd.read_csv(_snap_path)
    hist["timestamp"] = pd.to_datetime(hist["timestamp"])
    hist = hist.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    if len(hist) < 2:
        st.info("Collecting data… need at least 2 snapshots to chart.")
    else:
        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(
            x=hist["timestamp"], y=hist["mid_price"],
            mode="lines+markers", name="Mid Price",
            line=dict(color="#00c853", width=2),
            fill="tozeroy", fillcolor="rgba(0,200,83,0.08)",
        ))
        if "best_bid" in hist.columns:
            fig_price.add_trace(go.Scatter(
                x=hist["timestamp"], y=hist["best_bid"], mode="lines",
                name="Best Bid", line=dict(color="#76ff03", width=1, dash="dot"),
            ))
        if "best_ask" in hist.columns:
            fig_price.add_trace(go.Scatter(
                x=hist["timestamp"], y=hist["best_ask"], mode="lines",
                name="Best Ask", line=dict(color="#ff6d00", width=1, dash="dot"),
            ))
        fig_price.update_layout(
            template="plotly_dark", title="YES Price Over Time",
            xaxis_title="Time",
            yaxis=dict(title="Price (¢)", range=[0, 100]),
            height=300, margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig_price, use_container_width=True, key="price_hist")

        fig_ofi = go.Figure(go.Bar(
            x=hist["timestamp"], y=hist["ofi"],
            marker_color=["#00c853" if v > 0 else "#d50000" for v in hist["ofi"]],
        ))
        fig_ofi.add_hline(y=0, line_color="white", opacity=0.3)
        fig_ofi.update_layout(
            template="plotly_dark",
            title=f"Order Flow Imbalance (+ = {yes_label}-heavy)",
            yaxis_title="OFI %", height=220, margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig_ofi, use_container_width=True, key="ofi_hist")

        fig_sharp = go.Figure(go.Scatter(
            x=hist["timestamp"], y=hist["sharp_score"],
            mode="lines+markers", name="Sharp Score",
            line=dict(color="#ffd600", width=2),
            fill="tozeroy", fillcolor="rgba(255,214,0,0.08)",
        ))
        fig_sharp.add_hline(y=65, line_dash="dash", line_color="#00c853", opacity=0.5,
                            annotation_text="Sharp (65)", annotation_position="right")
        fig_sharp.update_layout(
            template="plotly_dark", title="Sharp Liquidity Score Over Time",
            yaxis=dict(title="Score (0–100)", range=[0, 100]),
            height=220, margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig_sharp, use_container_width=True, key="sharp_hist")

        st.download_button(
            "⬇ Download Snapshot History CSV",
            data=hist.to_csv(index=False),
            file_name=f"kalshi_{market_ticker}_snapshots.csv",
            mime="text/csv",
        )

# ── auto-refresh ───────────────────────────────────────────────────────────────
_interval_map = {"30s": 30, "1 min": 60, "5 min": 300}
if refresh_mode in _interval_map:
    with st.spinner(f"Auto-refreshing in {_interval_map[refresh_mode]}s…"):
        time.sleep(_interval_map[refresh_mode])
    st.cache_data.clear()
    st.rerun()
