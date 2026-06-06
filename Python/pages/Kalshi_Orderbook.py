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
MLB_ODDS_CSV = BASE / "MLB" / "output" / "odds.csv"
SNAPSHOT_DIR = BASE / "MLB" / "kalshi_snapshots"
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(BASE))
from kalshi_client import KalshiClient  # noqa: E402

# ── sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Kalshi Order Book")

    env_key = os.getenv("KALSHI_API_KEY", "").strip()
    api_key = env_key or st.text_input(
        "API Key", type="password", placeholder="Paste Kalshi API key",
        help="kalshi.com → Account → API Settings",
    )
    if env_key:
        st.caption("Key loaded from .env")

    st.markdown("---")
    manual_ticker = st.text_input(
        "Market ticker (optional)",
        placeholder="KXMLBGAME-26JUN061410KCMIN-MIN",
        help="Paste from the Kalshi game page URL to skip the browser.",
    ).strip()

    depth = st.slider("Book depth (levels)", 5, 25, 10, 5)
    sharp_k = st.slider("Highlight threshold ($K)", 50, 1000, 200, 50)
    sharp_threshold = sharp_k * 1_000

    refresh_mode = st.select_slider(
        "Auto-refresh", ["Off", "30s", "1 min", "5 min"], value="1 min"
    )
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

st.title("Kalshi Order Book")

if not api_key:
    st.warning("Add `KALSHI_API_KEY=your_key` to `.env` or paste it in the sidebar.")
    st.stop()

# ── client + cached fetches ────────────────────────────────────────────────────
client = KalshiClient(api_key=api_key)
_kh = str(hash(api_key))


@st.cache_data(ttl=120, show_spinner=False)
def _events(kh):  return client.get_mlb_events()

@st.cache_data(ttl=60, show_spinner=False)
def _markets(ev, kh):  return client.get_markets_for_event(ev)

@st.cache_data(ttl=20, show_spinner=False)
def _book(ticker, d, kh):  return client.get_orderbook(ticker, d)

@st.cache_data(ttl=30, show_spinner=False)
def _meta(ticker, kh):  return client.get_market(ticker)


# ── market selection ───────────────────────────────────────────────────────────
if manual_ticker:
    market_ticker = manual_ticker
    label = manual_ticker
else:
    try:
        events = _events(_kh)
    except Exception as e:
        st.error(f"Could not fetch events: {e}")
        st.stop()
    if not events:
        st.info(
            "No open MLB game markets found right now.  \n"
            "Paste a market ticker in the sidebar (from the Kalshi game URL)."
        )
        st.stop()

    def _ev_label(e):
        title = e.get("title") or e.get("event_ticker", "")
        sub   = e.get("sub_title") or ""
        return f"{title}  —  {sub}" if sub else title

    ev_map  = {_ev_label(e): e for e in events}
    ev_pick = st.selectbox("Game", list(ev_map.keys()))
    ev_obj  = ev_map[ev_pick]

    try:
        mkts = _markets(ev_obj.get("event_ticker", ""), _kh)
    except Exception as e:
        st.error(f"Could not load markets: {e}")
        st.stop()
    if not mkts:
        st.info("No open markets for this event.")
        st.stop()

    mkt_map  = {m.get("title") or m.get("ticker", ""): m for m in mkts}
    mkt_pick = st.selectbox("Market", list(mkt_map.keys()))
    market_ticker = mkt_map[mkt_pick].get("ticker", "")
    label = mkt_pick

# ── fetch order book + meta ────────────────────────────────────────────────────
try:
    raw_ob = _book(market_ticker, depth, _kh)
    meta   = _meta(market_ticker, _kh)
except Exception as e:
    st.error(f"Order book fetch failed for `{market_ticker}`: {e}")
    st.stop()

# ── parse levels → (price_cents, dollars) sorted desc ─────────────────────────
def _parse(levels):
    out = []
    for lvl in (levels or []):
        p, d = (lvl[0], lvl[1]) if isinstance(lvl, (list, tuple)) else (lvl.get("price", 0), lvl.get("quantity", 0))
        pc = int(round(float(p) * 100 if float(p) <= 1.0 else float(p)))
        out.append((pc, float(d)))
    return sorted(out, key=lambda x: x[0], reverse=True)

yes_lvls = _parse(raw_ob.get("yes_dollars", []))
no_lvls  = _parse(raw_ob.get("no_dollars",  []))

# ── key prices ─────────────────────────────────────────────────────────────────
def _cents(key):
    v = meta.get(key)
    if v is None: return None
    f = float(v)
    return int(round(f * 100 if f <= 1.0 else f))

best_yes_bid = _cents("yes_bid_dollars") or (yes_lvls[0][0] if yes_lvls else None)
best_no_bid  = _cents("no_bid_dollars")  or (no_lvls[0][0]  if no_lvls  else None)
best_yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
mid          = round((best_yes_bid + best_yes_ask) / 2, 1) if (best_yes_bid and best_yes_ask) else None
spread       = (best_yes_ask - best_yes_bid) if (best_yes_bid is not None and best_yes_ask is not None) else None

yes_depth = sum(d for _, d in yes_lvls)
no_depth  = sum(d for _, d in no_lvls)

# ── sharp book price from local MLB odds CSV ───────────────────────────────────
sharp_cents = None
sharp_book_name = None
yes_team = meta.get("yes_sub_title") or ""
try:
    if MLB_ODDS_CSV.exists() and yes_team:
        odds_df = pd.read_csv(MLB_ODDS_CSV)
        odds_df = odds_df[
            (odds_df["market_key"] == "h2h") &
            (odds_df["bookmaker_key"] == "pinnacle")
        ]
        # Match by team name substring (case-insensitive)
        mask = odds_df["outcome_name"].str.contains(yes_team, case=False, na=False)
        if not mask.any():
            # try first word of yes_team (e.g. "Minnesota" from "Minnesota Twins")
            first_word = yes_team.split()[0]
            mask = odds_df["outcome_name"].str.contains(first_word, case=False, na=False)
        match = odds_df[mask]
        if not match.empty:
            raw_odds = pd.to_numeric(match["price_american"], errors="coerce").dropna()
            if not raw_odds.empty:
                o = raw_odds.iloc[-1]
                p = (-o / (-o + 100) * 100) if o < 0 else (100 / (o + 100) * 100)
                sharp_cents = round(p)
                sharp_book_name = "Pinnacle"
except Exception:
    pass  # best-effort only

# ── snapshot ───────────────────────────────────────────────────────────────────
snap = SNAPSHOT_DIR / f"{market_ticker.replace('/', '_')}.csv"
pd.DataFrame([{
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "mid": mid, "bid": best_yes_bid, "ask": best_yes_ask,
    "spread": spread, "yes_depth": round(yes_depth), "no_depth": round(no_depth),
    "sharp": sharp_cents,
}]).to_csv(snap, mode="a", header=not snap.exists(), index=False)

# ── header metrics ─────────────────────────────────────────────────────────────
yes_name = meta.get("yes_sub_title") or "YES"
no_name  = meta.get("no_sub_title")  or "NO"
game_name = meta.get("title") or label

st.subheader(game_name)
st.caption(datetime.now(timezone.utc).strftime("Updated %H:%M:%S UTC"))

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(f"{yes_name} Bid",  f"{best_yes_bid}¢"  if best_yes_bid  else "—")
c2.metric(f"{yes_name} Ask",  f"{best_yes_ask}¢"  if best_yes_ask  else "—")
c3.metric("Mid Price",        f"{mid}¢"            if mid           else "—")
c4.metric("Spread",           f"{spread}¢"         if spread is not None else "—",
          delta="tight" if spread is not None and spread <= 2 else "wide",
          delta_color="normal" if spread is not None and spread <= 2 else "inverse")
c5.metric(
    f"Sharp ({sharp_book_name or 'Pinnacle'})",
    f"{sharp_cents}¢" if sharp_cents else "—",
    delta=(f"{round(mid - sharp_cents):+d}¢ vs mid" if (mid and sharp_cents) else None),
    delta_color="normal" if (mid and sharp_cents and abs(mid - sharp_cents) <= 3) else "off",
)

# ── main depth chart ───────────────────────────────────────────────────────────
fig = go.Figure()

if yes_lvls:
    yes_prices  = [p for p, _ in yes_lvls]
    yes_dollars = [d for _, d in yes_lvls]
    yes_colors  = ["#00e676" if d >= sharp_threshold else "#00c853" for d in yes_dollars]
    fig.add_trace(go.Bar(
        x=yes_dollars, y=yes_prices, orientation="h",
        name=f"{yes_name} bids (buying YES)",
        marker_color=yes_colors,
        hovertemplate=f"{yes_name} bid: %{{y}}¢  —  $%{{x:,.0f}}<extra></extra>",
    ))

if no_lvls:
    no_prices    = [100 - p for p, _ in no_lvls]   # convert to YES-ask equivalent
    no_dollars   = [d for _, d in no_lvls]
    no_neg       = [-d for d in no_dollars]
    no_colors    = ["#ff1744" if d >= sharp_threshold else "#d50000" for d in no_dollars]
    fig.add_trace(go.Bar(
        x=no_neg, y=no_prices, orientation="h",
        name=f"{no_name} bids → YES offers",
        marker_color=no_colors,
        hovertemplate=f"YES offer at %{{y}}¢  —  $%{{x:,.0f}}<extra></extra>",
    ))

# Kalshi mid price line
if mid:
    fig.add_hline(y=mid, line_color="white", line_width=1.5, opacity=0.7,
                  annotation_text=f"Kalshi mid {mid}¢", annotation_position="right",
                  annotation_font_color="white")

# Sharp book price line
if sharp_cents:
    diff_text = f"{sharp_cents - mid:+d}¢ from mid" if mid else ""
    fig.add_hline(
        y=sharp_cents, line_color="#ffd600", line_dash="dash", line_width=2, opacity=0.9,
        annotation_text=f"{sharp_book_name} {sharp_cents}¢  {diff_text}",
        annotation_position="left",
        annotation_font_color="#ffd600",
    )

# Shade the agreement zone (±3¢ of sharp price)
if sharp_cents:
    fig.add_hrect(
        y0=sharp_cents - 3, y1=sharp_cents + 3,
        fillcolor="rgba(255,214,0,0.06)", line_width=0,
    )

# Depth totals annotation
fig.add_annotation(
    x=0.01, y=0.97, xref="paper", yref="paper",
    text=f"YES depth: ${yes_depth:,.0f}  |  NO depth: ${no_depth:,.0f}",
    showarrow=False, font=dict(size=12, color="white"),
    align="left", bgcolor="rgba(0,0,0,0.4)", borderpad=6,
)

fig.update_layout(
    template="plotly_dark",
    xaxis=dict(
        title=f"← {no_name} offers (selling YES)       {yes_name} bids (buying YES) →",
        tickformat="$,.0f",
    ),
    yaxis=dict(title="Price (¢)", range=[
        max(0,  min((yes_lvls[-1][0] if yes_lvls else 0),
                    (100 - no_lvls[-1][0] if no_lvls else 100)) - 5),
        min(100, max((yes_lvls[0][0]  if yes_lvls else 100),
                     (100 - no_lvls[0][0]  if no_lvls else 0))  + 5),
    ]),
    height=480,
    margin=dict(t=20, b=40, l=10, r=160),
    showlegend=True,
    legend=dict(orientation="h", y=1.04, x=0),
    barmode="overlay",
)

st.plotly_chart(fig, use_container_width=True, key="depth")

# ── price history ──────────────────────────────────────────────────────────────
if snap.exists():
    hist = pd.read_csv(snap)
    hist["timestamp"] = pd.to_datetime(hist["timestamp"])
    hist = hist.drop_duplicates("timestamp").sort_values("timestamp")

    if len(hist) >= 2:
        st.markdown("---")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=hist["timestamp"], y=hist["mid"],
            mode="lines", name="Kalshi mid",
            line=dict(color="#00c853", width=2),
        ))
        if "sharp" in hist.columns:
            fig2.add_trace(go.Scatter(
                x=hist["timestamp"], y=hist["sharp"],
                mode="lines", name=f"{sharp_book_name or 'Pinnacle'} price",
                line=dict(color="#ffd600", width=2, dash="dash"),
            ))
        fig2.update_layout(
            template="plotly_dark",
            title="YES Price Over Time",
            yaxis=dict(title="Price (¢)"),
            height=240,
            margin=dict(t=40, b=20, l=10, r=10),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig2, use_container_width=True, key="hist")

# ── auto-refresh ───────────────────────────────────────────────────────────────
_secs = {"30s": 30, "1 min": 60, "5 min": 300}
if refresh_mode in _secs:
    with st.spinner(f"Refreshing in {_secs[refresh_mode]}s…"):
        time.sleep(_secs[refresh_mode])
    st.cache_data.clear()
    st.rerun()
