import time
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(layout="wide")
REFRESH_SECONDS = 30  # dashboard refresh cadence

csv_path = Path("./output/odds.csv")

st.title("Line Movement Dashboard")

if not csv_path.exists():
    st.warning("Waiting for output/odds.csv ...")
    st.stop()

df = pd.read_csv(csv_path)

df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")

# Pick next upcoming event
df2 = df.dropna(subset=["event_commence_utc"]).sort_values("event_commence_utc")
event_id = df2["event_id"].iloc[0]

market = "h2h"
true_market_name = "Moneyline"
# Sort and de-duplicate
d = df[(df["event_id"] == event_id) & (df["market_key"] == market)].copy()
d = d.sort_values("market_last_update").drop_duplicates(
    subset=["event_id","bookmaker_key","market_key","outcome_name","line_point","market_last_update"],
    keep="last"
)

# Clean Rows
d = d.dropna(subset=["market_last_update", "price_american"])
d = d[pd.to_numeric(d["price_american"], errors="coerce").notna()].copy()
d["price_american"] = d["price_american"].astype(int)

# Get Each Team
teams = sorted(d["outcome_name"].dropna().unique())

# Create the line plots (for each team, not together)
cols = st.columns(len(teams)) if teams else []
for i, team in enumerate(teams):
    dt = d[d["outcome_name"] == team]
    fig = px.line(
        dt,
        x="market_last_update",
        y="price_american",
        color="bookmaker_key",
        markers=True
    )
    fig.update_traces(line=dict(width=3, shape="hv"), marker=dict(size=6))
    fig.update_yaxes(tickformat="+d", title="American Odds")
    fig.update_xaxes(tickformat="%m/%d %I:%M%p", dtick=3600000 * 6, tickangle=-30, title="Time")
    fig.update_layout(template="plotly_dark", title=f"{team} {true_market_name}", legend_title_text="Sportsbook")
    cols[i].plotly_chart(fig, use_container_width=True)

# Auto refresh
time.sleep(REFRESH_SECONDS)
st.rerun()
