import pandas as pd
import numpy as np
import plotly.express as px

df = pd.read_csv(r"./output/odds.csv")

# Parse timestamps
df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")

# Pick one event
event_id = df["event_id"].iloc[0]
market = "h2h"
true_market_name = "Moneyline"

d = df[(df["event_id"] == event_id) & (df["market_key"] == market)].copy()

# Sort and de-duplicate
d = d.sort_values("market_last_update")
d = d.drop_duplicates(subset=["event_id","bookmaker_key","market_key","outcome_name","market_last_update"], keep="last")

# Clean rows (need odds + timestamp)
d = d.dropna(subset=["market_last_update", "price_american"])
d = d[pd.to_numeric(d["price_american"], errors="coerce").notna()].copy()
d["price_american"] = d["price_american"].astype(int)

# Get each team
teams = sorted(d["outcome_name"].dropna().unique())

for team in teams:
    dt = d[d["outcome_name"] == team].copy()
    if dt.empty:
        continue

    fig = px.line(
        dt,
        x="market_last_update",
        y="price_american",
        color="bookmaker_key",
        markers=True,
        hover_data=["line_point", "event_commence_utc"]
    )

    # Thicker lines, step-style
    fig.update_traces(line=dict(width=3, shape="hv"), marker=dict(size=7))

    # X axis formatting
    fig.update_xaxes(
        showticklabels=True,
        automargin=True,
        tickformat="%m/%d %I:%M%p",
        dtick=3600000 * 6,
        tickangle=-30,
        tickfont=dict(color="white"),
        ticks="outside",
        title="Time"
    )

    # Y axis formatting for American odds
    fig.update_yaxes(
        autorange=True,
        tickformat="+d",
        zeroline=False,
        title="American Odds"
    )

    # Layout
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111111",
        plot_bgcolor="#111111",
        legend_title_text="Sportsbook",
        margin=dict(b=90),
        title=f"{team} {true_market_name}"
    )

    fig.show()