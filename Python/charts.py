from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

UNIQUE_COLS = [
    "event_id",
    "bookmaker_key",
    "market_key",
    "outcome_name",
    "line_point",
    "market_last_update",
]


def load_odds(csv_path: Path) -> pd.DataFrame | None:
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
    df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_id", "market_key", "market_last_update", "event_commence_utc"])

    now = datetime.now(timezone.utc)
    df = df[df["event_commence_utc"] >= now].copy()

    df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce")
    df = df.dropna(subset=["price_american"])
    df["price_american"] = df["price_american"].astype(int)

    df = df.sort_values("market_last_update").drop_duplicates(subset=UNIQUE_COLS, keep="last")
    return df


def render_odds_page(page_title: str, csv_path: Path) -> None:
    st_autorefresh(interval=30 * 1000, key=f"refresh_{page_title}")
    st.title(page_title)

    df = load_odds(csv_path)

    if df is None:
        st.warning(f"Waiting for data at {csv_path} ...")
        st.stop()

    if df.empty:
        st.info("No upcoming events found.")
        st.stop()

    event_order = (
        df[["event_id", "event_commence_utc"]]
        .drop_duplicates()
        .sort_values("event_commence_utc")
    )

    for event_id in event_order["event_id"].tolist():
        ev = df[df["event_id"] == event_id].copy()
        if ev.empty:
            continue

        kickoff = ev["event_commence_utc"].iloc[0]
        h2h_all = ev[ev["market_key"] == "h2h"].copy()

        home_team = away_team = None
        if not h2h_all.empty and "is_home_team" in h2h_all.columns:
            ht = h2h_all[h2h_all["is_home_team"] == True]["outcome_name"].dropna().unique()
            at = h2h_all[h2h_all["is_home_team"] == False]["outcome_name"].dropna().unique()
            home_team = ht[0] if len(ht) else None
            away_team = at[0] if len(at) else None

        game_title = f"{away_team} @ {home_team}" if home_team and away_team else f"Event {event_id}"
        st.markdown(f"## {game_title}")

        kickoff_local = kickoff.tz_convert("America/Chicago")
        st.caption(f"{kickoff_local.strftime('%B %d, %Y')} | {kickoff_local.strftime('%I:%M%p').lstrip('0').lower()}")

        ml1, ml2 = st.columns(2)
        totals_col = st.container()

        teams = [away_team, home_team] if away_team and home_team else sorted(
            h2h_all["outcome_name"].dropna().unique().tolist() if not h2h_all.empty else []
        )

        def plot_moneyline(col, team_name, h2h=h2h_all):
            dt = h2h[h2h["outcome_name"] == team_name].copy()
            if dt.empty:
                col.info(f"No moneyline data for {team_name}")
                return
            dt["market_last_update_local"] = dt["market_last_update"].dt.tz_convert("America/Chicago")
            fig = go.Figure()
            for book, g in dt.groupby("bookmaker_key"):
                fig.add_trace(go.Scatter(
                    x=g["market_last_update_local"],
                    y=g["price_american"],
                    mode="lines+markers",
                    name=str(book),
                    line=dict(width=3),
                    marker=dict(size=6),
                ))
            fig.update_yaxes(title="American Odds")
            fig.update_xaxes(tickformat="%m/%d %I:%M%p", dtick=3600000 * 6, tickangle=-30, title="Time (CT)")
            fig.update_layout(title=f"{team_name} Moneyline", template="plotly_dark", legend_title_text="Sportsbook")
            col.plotly_chart(fig, use_container_width=True)

        plot_moneyline(ml1, teams[0]) if len(teams) >= 1 else ml1.info("No moneyline data yet.")
        plot_moneyline(ml2, teams[1]) if len(teams) >= 2 else ml2.info("No moneyline data yet.")

        totals = ev[ev["market_key"] == "totals"].copy()
        if totals.empty:
            totals_col.info("No totals data yet.")
        else:
            totals["line_point"] = pd.to_numeric(totals["line_point"], errors="coerce")
            totals = totals.dropna(subset=["line_point", "market_last_update"])
            totals = totals[totals["outcome_name"] == "Over"].copy()
            totals = totals.sort_values("market_last_update").drop_duplicates(
                subset=["event_id", "bookmaker_key", "market_last_update", "line_point"],
                keep="last",
            )
            totals["market_last_update_local"] = totals["market_last_update"].dt.tz_convert("America/Chicago")

            fig = go.Figure()
            for book, g in totals.groupby("bookmaker_key"):
                fig.add_trace(go.Scatter(
                    x=g["market_last_update_local"],
                    y=g["line_point"],
                    mode="lines+markers",
                    name=str(book),
                    line=dict(width=3),
                    marker=dict(size=6),
                ))
            fig.update_yaxes(title="Total (Points)")
            fig.update_xaxes(tickformat="%m/%d %I:%M%p", dtick=3600000 * 6, tickangle=-30, title="Time (CT)")
            fig.update_layout(
                title=dict(text="Total Points", x=0.5, xanchor="center"),
                template="plotly_dark",
                legend_title_text="Sportsbook",
            )
            totals_col.plotly_chart(fig, use_container_width=True)

        st.divider()
