from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="NFL Futures", layout="wide")
st_autorefresh(interval=60 * 1000, key="futures_refresh")

FUTURES_PATH = Path(__file__).parent.parent / "NFL" / "futures_output" / "futures.csv"

DISPLAY_BOOKS = {
    "pinnacle", "fanduel", "betonlineag", "prophetx",
    "novig", "draftkings", "betmgm", "caesars",
}

BOOK_LABELS = {
    "pinnacle": "Pinnacle",
    "fanduel": "FanDuel",
    "betonlineag": "BetOnline",
    "prophetx": "ProphetX",
    "novig": "NoVig",
    "draftkings": "DraftKings",
    "betmgm": "BetMGM",
    "caesars": "Caesars",
}

st.title("NFL Futures — Super Bowl Winner")
st.caption("Odds to win Super Bowl · Sharp consensus across major books")

if not FUTURES_PATH.exists():
    st.warning("No futures data yet. Start the ingestor:")
    st.code("python NFL/ingest_nfl_futures.py", language="bash")
    st.stop()


def american_to_implied(odds: float) -> float:
    if odds < 0:
        return -odds / (-odds + 100)
    return 100 / (odds + 100)


def implied_to_american(p: float) -> int:
    if p <= 0 or p >= 1:
        return 0
    if p >= 0.5:
        return int(round(-(p / (1 - p)) * 100))
    return int(round(((1 - p) / p) * 100))


@st.cache_data(ttl=60)
def load_futures(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
    df["snapshot_utc"] = pd.to_datetime(df["snapshot_utc"], utc=True, errors="coerce")
    df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce")
    df = df.dropna(subset=["price_american", "market_last_update", "outcome_name"])
    df = df[df["bookmaker_key"].isin(DISPLAY_BOOKS)]
    return df


df = load_futures(str(FUTURES_PATH))

if df.empty:
    st.info("No futures data available yet.")
    st.stop()

# Latest price per team per book (most recent market_last_update)
latest = (
    df.sort_values("market_last_update")
    .drop_duplicates(subset=["bookmaker_key", "outcome_name"], keep="last")
)

# ---------------------------------------------------------------------------
# Consensus odds board — one row per team, sorted by implied prob
# ---------------------------------------------------------------------------
st.subheader("Current Odds Board")
st.caption("Latest line per book. Consensus = average implied probability across available books → converted back to American odds.")

teams = latest["outcome_name"].unique()

board_rows = []
for team in teams:
    team_rows = latest[latest["outcome_name"] == team]
    book_odds = dict(zip(team_rows["bookmaker_key"], team_rows["price_american"]))
    probs = [american_to_implied(o) for o in book_odds.values()]
    consensus_prob = sum(probs) / len(probs) if probs else None
    consensus_american = implied_to_american(consensus_prob) if consensus_prob else None
    row = {"Team": team, "Consensus": consensus_american, "Impl. Prob": round(consensus_prob * 100, 1) if consensus_prob else None}
    for book in sorted(DISPLAY_BOOKS):
        row[BOOK_LABELS.get(book, book)] = int(book_odds[book]) if book in book_odds else None
    board_rows.append(row)

board_df = pd.DataFrame(board_rows).sort_values("Impl. Prob", ascending=False).reset_index(drop=True)

def fmt_american(v):
    if pd.isna(v) or v == 0:
        return "—"
    return f"+{int(v)}" if v > 0 else str(int(v))

book_display_cols = [BOOK_LABELS.get(b, b) for b in sorted(DISPLAY_BOOKS)]
format_dict = {col: fmt_american for col in ["Consensus"] + book_display_cols}
format_dict["Impl. Prob"] = "{:.1f}%"

st.dataframe(
    board_df.style
        .format(format_dict, na_rep="—")
        .background_gradient(subset=["Impl. Prob"], cmap="RdYlGn", vmin=0, vmax=30),
    hide_index=True,
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Consensus implied probability bar chart
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Implied Win Probability")

top_n = st.slider("Show top N teams", min_value=5, max_value=32, value=16, step=1)
chart_df = board_df.head(top_n).copy()

fig_bar = go.Figure(go.Bar(
    x=chart_df["Team"],
    y=chart_df["Impl. Prob"],
    text=[f"{p:.1f}%" for p in chart_df["Impl. Prob"]],
    textposition="outside",
    marker_color="#4fc3f7",
    cliponaxis=False,
))
fig_bar.update_layout(
    template="plotly_dark",
    yaxis=dict(title="Implied Win Probability (%)"),
    xaxis=dict(tickangle=-35),
    height=420,
    margin=dict(t=20, b=10),
)
st.plotly_chart(fig_bar, use_container_width=True, key="bar_chart")

# ---------------------------------------------------------------------------
# Odds movement over time — pick teams to track
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Odds Movement Over Time")
st.caption("Tracks consensus implied probability across snapshots. Select teams to compare.")

all_teams = sorted(board_df["Team"].tolist())
default_teams = board_df.head(6)["Team"].tolist()
selected_teams = st.multiselect("Teams", all_teams, default=default_teams, key="team_select")

if selected_teams:
    movement_df = df[df["outcome_name"].isin(selected_teams)].copy()
    movement_df["snapshot_local"] = movement_df["snapshot_utc"].dt.tz_convert("America/Chicago")

    # Per snapshot per team: average implied prob across books
    movement_df["impl_prob"] = movement_df["price_american"].apply(american_to_implied)
    consensus_ts = (
        movement_df.groupby(["outcome_name", "snapshot_local"])["impl_prob"]
        .mean()
        .reset_index()
        .sort_values("snapshot_local")
    )

    fig_line = go.Figure()
    for team in selected_teams:
        g = consensus_ts[consensus_ts["outcome_name"] == team]
        if g.empty:
            continue
        fig_line.add_trace(go.Scatter(
            x=g["snapshot_local"],
            y=(g["impl_prob"] * 100).round(2),
            mode="lines+markers",
            name=team,
            line=dict(width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{team}</b><br>%{{x|%m/%d %I:%M%p}}<br>Impl. Prob: %{{y:.1f}}%<extra></extra>",
        ))

    fig_line.update_layout(
        template="plotly_dark",
        yaxis=dict(title="Implied Win Probability (%)"),
        xaxis=dict(title="Date (CT)", tickformat="%m/%d", dtick=3600000 * 24 * 7, tickangle=-30),
        height=420,
        legend_title_text="Team",
        margin=dict(t=20, b=10),
    )
    st.plotly_chart(fig_line, use_container_width=True, key="movement_chart")
else:
    st.info("Select at least one team above.")

# ---------------------------------------------------------------------------
# Book-by-book movement for a single team
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Per-Book Movement — Single Team")
st.caption("See how each book's line has moved for a specific team.")

focus_team = st.selectbox("Team", all_teams, index=0, key="focus_team")
team_df = df[df["outcome_name"] == focus_team].copy()
team_df["snapshot_local"] = team_df["snapshot_utc"].dt.tz_convert("America/Chicago")
team_df = team_df.sort_values("snapshot_local")

fig_book = go.Figure()
for book, g in team_df.groupby("bookmaker_key"):
    fig_book.add_trace(go.Scatter(
        x=g["snapshot_local"],
        y=g["price_american"],
        mode="lines+markers",
        name=BOOK_LABELS.get(book, book),
        line=dict(width=2),
        marker=dict(size=5),
        hovertemplate=(
            "%{x|%m/%d %I:%M%p}<br>"
            "Odds: %{y:+d}"
            "<extra>%{fullData.name}</extra>"
        ),
    ))

fig_book.update_layout(
    template="plotly_dark",
    title=f"{focus_team} — Futures Odds by Book",
    yaxis=dict(title="American Odds", tickformat="+d"),
    xaxis=dict(title="Date (CT)", tickformat="%m/%d", dtick=3600000 * 24 * 7, tickangle=-30),
    height=380,
    legend_title_text="Sportsbook",
    margin=dict(t=50, b=10),
)
st.plotly_chart(fig_book, use_container_width=True, key="book_chart")
