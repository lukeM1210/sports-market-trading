from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Line Shopping", layout="wide")

SPORT_PATHS = {
    "NFL":   Path(__file__).parent.parent / "NFL"   / "output" / "odds.csv",
    "NBA":   Path(__file__).parent.parent / "NBA"   / "output" / "odds.csv",
    "MLB":   Path(__file__).parent.parent / "MLB"   / "output" / "odds.csv",
    "NHL":   Path(__file__).parent.parent / "NHL"   / "output" / "odds.csv",
    "NCAAF": Path(__file__).parent.parent / "NCAAF" / "output" / "odds.csv",
}

BOOK_ORDER = ["pinnacle", "fanduel", "draftkings", "betonlineag", "prophetx", "novig", "betmgm", "caesars", "kalshi", "polymarket"]
BOOK_LABELS = {
    "pinnacle":   "Pinnacle",
    "fanduel":    "FanDuel",
    "draftkings": "DraftKings",
    "betonlineag":"BetOnline",
    "prophetx":   "ProphetX",
    "novig":      "NoVig",
    "betmgm":     "BetMGM",
    "caesars":    "Caesars",
    "kalshi":     "Kalshi",
    "polymarket": "Polymarket",
}

st.title("Line Shopping")
st.caption("Best available moneyline across all books for upcoming games.")

available_sports = [s for s, p in SPORT_PATHS.items() if p.exists()]
if not available_sports:
    st.warning("No odds data found. Start the ingestors first.")
    st.stop()

sport = st.sidebar.selectbox("Sport", available_sports)
lookahead = st.sidebar.slider("Days ahead", 1, 14, 7)


@st.cache_data(ttl=30)
def load(path: str, days: int) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
    df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_id", "market_last_update", "event_commence_utc"])
    now = datetime.now(timezone.utc)
    df = df[(df["event_commence_utc"] >= now) & (df["event_commence_utc"] <= now + timedelta(days=days))]
    df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce")
    df = df.dropna(subset=["price_american"])
    df["price_american"] = df["price_american"].astype(int)
    # Latest line per unique key
    UNIQUE = ["event_id", "bookmaker_key", "market_key", "outcome_name", "line_point"]
    df = df.sort_values("market_last_update").drop_duplicates(subset=UNIQUE, keep="last")
    return df


df = load(str(SPORT_PATHS[sport]), lookahead)

if df.empty:
    st.info("No upcoming games found.")
    st.stop()

h2h = df[df["market_key"] == "h2h"].copy()
if h2h.empty:
    st.info("No moneyline data available.")
    st.stop()

# Latest price per (event, book, team)
latest_h2h = (
    h2h.sort_values("market_last_update")
    .drop_duplicates(subset=["event_id", "bookmaker_key", "outcome_name"], keep="last")
)

# Event metadata
events = (
    df[["event_id", "event_commence_utc"]]
    .drop_duplicates("event_id")
    .sort_values("event_commence_utc")
)

# Books present in this data
present_books = [b for b in BOOK_ORDER if b in latest_h2h["bookmaker_key"].unique()]

def fmt(v):
    if pd.isna(v):
        return "—"
    v = int(v)
    return f"+{v}" if v > 0 else str(v)


for _, event_row in events.iterrows():
    eid = event_row["event_id"]
    kickoff = event_row["event_commence_utc"].tz_convert("America/Chicago")

    ev = latest_h2h[latest_h2h["event_id"] == eid]
    teams = ev["outcome_name"].dropna().unique().tolist()
    if len(teams) < 2:
        continue

    # Determine home/away labels if available
    if "is_home_team" in ev.columns:
        home_rows = ev[ev["is_home_team"] == True]["outcome_name"].dropna().unique()
        away_rows = ev[ev["is_home_team"] == False]["outcome_name"].dropna().unique()
        home_team = home_rows[0] if len(home_rows) else teams[0]
        away_team = away_rows[0] if len(away_rows) else teams[1]
        title = f"{away_team} @ {home_team}"
    else:
        title = " vs ".join(teams)
        home_team, away_team = teams[0], teams[1]

    st.markdown(f"### {title}")
    st.caption(kickoff.strftime("%B %d, %Y  |  %I:%M %p CT"))

    # Build table: rows = teams, cols = books
    rows = []
    for team in [away_team, home_team]:
        team_ev = ev[ev["outcome_name"] == team]
        book_odds = {r["bookmaker_key"]: r["price_american"] for _, r in team_ev.iterrows()}
        row = {"Team": team}
        for book in present_books:
            row[BOOK_LABELS.get(book, book)] = book_odds.get(book)
        # Best line = highest number (most favorable for bettor)
        valid_odds = [v for v in book_odds.values() if not pd.isna(v)]
        row["Best Line"] = int(max(valid_odds)) if valid_odds else None
        best_book_key = max(book_odds, key=lambda b: book_odds[b]) if book_odds else None
        row["Best Book"] = BOOK_LABELS.get(best_book_key, best_book_key) if best_book_key else "—"
        rows.append(row)

    table_df = pd.DataFrame(rows)
    book_cols = [BOOK_LABELS.get(b, b) for b in present_books]

    def highlight_best(row):
        styles = [""] * len(row)
        book_values = {col: row[col] for col in book_cols if col in row.index and not pd.isna(row[col])}
        if not book_values:
            return styles
        best_val = max(book_values.values())
        for i, col in enumerate(row.index):
            if col in book_values and book_values[col] == best_val:
                styles[i] = "background-color: #1b5e20; color: white; font-weight: bold"
        return styles

    format_dict = {col: fmt for col in book_cols + ["Best Line"]}
    st.dataframe(
        table_df.style
            .apply(highlight_best, axis=1)
            .format(format_dict, na_rep="—"),
        hide_index=True,
        use_container_width=True,
    )

    st.divider()
