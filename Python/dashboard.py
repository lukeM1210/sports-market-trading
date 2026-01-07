import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")
REFRESH_SECONDS = 30  # dashboard refresh cadence

# Autorefresh instance to trigger a refresh of the application
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="odds_refresh")

csv_path = Path("./output/odds.csv")

st.title("NFL Line Movement Dashboard")

if not csv_path.exists():
    st.warning("Waiting for output/odds.csv ...")
    st.stop()

def read_csv_retry(path: Path, tries=5, delay=0.2):
    for _ in range(tries):
        try:
            return pd.read_csv(path)
        except Exception:
            time.sleep(delay)
    return pd.read_csv(path)

df = read_csv_retry(csv_path)

#df = pd.read_csv(csv_path)

df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")

# Clean main fields
df = df.dropna(subset=["event_id", "market_key", "market_last_update", "event_commence_utc"])

# Keep only upcoming games
now = datetime.now(timezone.utc)
df = df[df["event_commence_utc"] >= now].copy()

# No upcoming games -> break off
if df.empty:
    st.info("No upcoming events found in odds.csv yet.")
    st.stop()

# Sort games by kickoff time
event_order = (
    df[["event_id", "event_commence_utc"]]
    .drop_duplicates()
    .sort_values("event_commence_utc")
)

event_ids = event_order["event_id"].tolist()

# Markets displayed for each game
MARKETS = [("h2h", "Moneyline"), ("totals", "Total")]

# Keep only numeric odds
df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce")
df = df.dropna(subset=["price_american"])
df["price_american"] = df["price_american"].astype(int)

# Treat -100 as +100
df.loc[df["price_american"] == -100, "price_american"] = 100

# Help issue with non-linearity American odds. For example +105 and -105
def odds_tickvals_from_data(series: pd.Series, max_ticks: int = 12):
    s = pd.to_numeric(series, errors="coerce").dropna().astype(int)

    # treat -100 as +100 for display
    s = s.replace(-100, 100)

    uniq = sorted(set(s.tolist()))
    if not uniq:
        return [100], ["+100"]

    # always include +100 (even)
    if 100 not in uniq:
        uniq.append(100)
        uniq = sorted(uniq)

    # too many unique odds? downsample evenly to keep chart readable
    if len(uniq) > max_ticks:
        idx = [round(i * (len(uniq) - 1) / (max_ticks - 1)) for i in range(max_ticks)]
        uniq = [uniq[i] for i in idx]

    ticktext = [f"+{v}" if v > 0 else str(v) for v in uniq]
    return uniq, ticktext


# Dedup: keep the last snapshot per timestamp
df = df.sort_values("market_last_update").drop_duplicates(
    subset=["event_id", "bookmaker_key", "market_key", "outcome_name", "line_point", "market_last_update"],
    keep="last",
)

# Render each event section
for event_id in event_ids:
    ev = df[df["event_id"] == event_id].copy()
    if ev.empty:
        continue

    kickoff = ev["event_commence_utc"].iloc[0]

    # Set home/away team names from h2h outcomes
    h2h_all = ev[ev["market_key"] == "h2h"].copy()
    home_team = away_team = None
    if not h2h_all.empty and "is_home_team" in h2h_all.columns:
        ht = h2h_all[h2h_all["is_home_team"] == True]["outcome_name"].dropna().unique()
        at = h2h_all[h2h_all["is_home_team"] == False]["outcome_name"].dropna().unique()
        home_team = ht[0] if len(ht) else None
        away_team = at[0] if len(at) else None

    title = f"{away_team} @ {home_team}" if home_team and away_team else f"Event {event_id}"

    st.markdown(f"## {title}")

    # Add game date and time under game title
    kickoff_local = kickoff.tz_convert("America/Chicago")
    time_str = kickoff_local.strftime("%I:%M%p").lstrip("0").lower()
    date_str = kickoff_local.strftime("%B %d, %Y")

    st.caption(f"{date_str} | {time_str}")

    # 3 charts per game (2 for each teams moneyline, 1 for the total)
    # c1, c2, c3 = st.columns(3)
    # Row 1: moneylines (wide)
    with st.container():
        ml1, ml2 = st.columns(2)
        totals_col = st.container()
    

        #ml1, ml2 = st.columns(2)

        # Row 2: totals (full width)
        #totals_col = st.container()

        if not h2h_all.empty:
            teams = sorted(h2h_all["outcome_name"].dropna().unique())
        else:
            teams = []

        # If we have exactly 2 teams, keep them in order (away, home) when possible
        if away_team and home_team:
            teams = [away_team, home_team]

        # Function called to render the charts
        def plot_moneyline(col, team_name):
            dt = h2h_all[h2h_all["outcome_name"] == team_name].copy()
            if dt.empty:
                col.info(f"No moneyline data for {team_name}")
                return

            dt = dt.copy()
            dt["market_last_update_local"] = dt["market_last_update"].dt.tz_convert("America/Chicago")

            fig = px.line(
                dt.sort_values("market_last_update_local"),
                x="market_last_update_local",
                y="price_american",
                color="bookmaker_key",
                markers=True,
                title=f"{team_name} Moneyline",
            )
            #fig.update_traces(line=dict(width=3, shape="hv"), marker=dict(size=6))
            fig.update_traces(line=dict(width=3), marker=dict(size=6))
            #fig.update_yaxes(tickformat="+d", title="American Odds")

            # tickvals and ticktext will be used to help with -105 and +105 linearity problems
            tickvals, ticktext = odds_tickvals_from_data(dt["price_american"])

            fig.update_yaxes(
                title="American Odds",
                tickmode="array",
                tickvals=tickvals,
                ticktext=ticktext,
            )

            fig.update_xaxes(tickformat="%m/%d %I:%M%p", dtick=3600000 * 6, tickangle=-30, title="Time (CT)")
            fig.update_layout(legend_title_text="Sportsbook", uirevision="keep", template="plotly_dark")
            #col.plotly_chart(fig, use_container_width=True)
            col.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{event_id}_{team_name}_moneyline",
            )


        if len(teams) >= 1:
            plot_moneyline(ml1, teams[0])
        else:
            ml1.info("No moneyline data yet.")

        if len(teams) >= 2:
            plot_moneyline(ml2, teams[1])
        else:
            ml2.info("No moneyline data yet.")


    # =========================
    # Totals: visualize the point total, not the odds (45.5 vs -110)
    # =========================
        totals = ev[ev["market_key"] == "totals"].copy()
        if totals.empty:
            totals_col.info("No totals data yet.")
        else:
            # Keep only rows where we actually have a total number
            totals["line_point"] = pd.to_numeric(totals["line_point"], errors="coerce")
            totals = totals.dropna(subset=["line_point", "market_last_update"])

            # Only need one of Over/Under because line_point is the same for both.
            # Keep Over to dedup.
            totals = totals[totals["outcome_name"] == "Over"].copy()

            # To dedup identical timestamps per book:
            totals = totals.sort_values("market_last_update").drop_duplicates(
                subset=["event_id", "bookmaker_key", "market_last_update", "line_point"],
                keep="last",
            )

            fig = px.line(
                totals.sort_values("market_last_update"),
                x="market_last_update",
                y="line_point",
                color="bookmaker_key",     # one line per book
                markers=True,
                title="Total Points Line",
            )
            #fig.update_traces(line=dict(width=3, shape="hv"), marker=dict(size=6))
            fig.update_traces(line=dict(width=3), marker=dict(size=6))
            fig.update_yaxes(title="Total (Points)")
            fig.update_xaxes(tickformat="%m/%d %I:%M%p", dtick=3600000 * 6, tickangle=-30, title="Time (UTC)")
            fig.update_layout(legend_title_text="Sportsbook", uirevision="keep", template="plotly_dark")
            #totals_col.plotly_chart(fig, use_container_width=True)
            totals_col.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{event_id}_totals_line",
            )


    st.divider()

# Auto refresh
#time.sleep(REFRESH_SECONDS)
#st.rerun()

