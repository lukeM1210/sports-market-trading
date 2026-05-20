from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import analytics

st.set_page_config(page_title="Sports Line Movement", layout="wide")

st.markdown("""
<style>
[data-testid="stSidebar"] { min-width: 215px; max-width: 215px; }
</style>
""", unsafe_allow_html=True)

st.title("Sports Line Movement")
st.caption("Select a sport to view live line movement charts.")

BASE = Path(__file__).parent

SPORTS = [
    ("NBA", "pages/NBA.py", BASE / "NBA" / "output" / "odds.csv"),
    ("NFL", "pages/NFL.py", BASE / "NFL" / "output" / "odds.csv"),
    ("NHL", "pages/NHL.py", BASE / "NHL" / "output" / "odds.csv"),
    ("MLB", "pages/MLB.py", BASE / "MLB" / "output" / "odds.csv"),
    ("NCAAF", "pages/NCAAF.py", BASE / "NCAAF" / "output" / "odds.csv"),
]

now = datetime.now(timezone.utc)

cols = st.columns(len(SPORTS))

for col, (name, page, csv_path) in zip(cols, SPORTS):
    with col:
        st.subheader(name)
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, usecols=["event_commence_utc"])
                df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")
                upcoming = df[df["event_commence_utc"] >= now]
                game_count = upcoming["event_commence_utc"].nunique()
                st.success(f"{game_count} upcoming game{'s' if game_count != 1 else ''}")
            except Exception:
                st.warning("Data error")
        else:
            st.info("No data yet")
        st.page_link(page, label=f"View {name} Lines")

st.markdown("---")
st.subheader("Top Movers")

ODDS_PATHS = [csv_path for _, _, csv_path in SPORTS]

fav_col, dog_col = st.columns(2)

def mover_chart(df: pd.DataFrame, color: str) -> go.Figure:
    labels = df.apply(lambda r: f"{r['team']} ({r['bookmaker']})", axis=1)
    values = df["prob_shift"].abs()
    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color=color,
        text=[f"{v:+.1f}%" for v in df["prob_shift"]],
        textposition="outside",
        cliponaxis=False,
    ))
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=10, r=80, t=10, b=10),
        xaxis=dict(title="Implied Prob Shift (%)", showgrid=False),
        yaxis=dict(autorange="reversed"),
        height=250,
        showlegend=False,
    )
    return fig

favs = analytics.top_5_favorite_movers(ODDS_PATHS)
dogs = analytics.top_5_underdog_movers(ODDS_PATHS)

def movers_table(df: pd.DataFrame, color: str) -> None:
    st.dataframe(
        df[["team", "opponent", "bookmaker", "open_odds", "current_odds", "prob_shift"]]
        .rename(columns={"prob_shift": "shift (%)", "open_odds": "open", "current_odds": "current"})
        .style.format({"shift (%)": "{:.2f}", "open": "{:+d}", "current": "{:+d}"})
        .applymap(lambda _: f"color: {color}", subset=["shift (%)"]),
        hide_index=True,
        use_container_width=True,
    )

with fav_col:
    st.markdown("#### 📈 Favorite Movers")
    if favs.empty:
        st.info("No movement yet.")
    else:
        st.plotly_chart(mover_chart(favs, "#00c853"), use_container_width=True, key="fav_chart")
        movers_table(favs, "#00c853")

with dog_col:
    st.markdown("#### 📉 Underdog Movers")
    if dogs.empty:
        st.info("No movement yet.")
    else:
        st.plotly_chart(mover_chart(dogs, "#d50000"), use_container_width=True, key="dog_chart")
        movers_table(dogs, "#d50000")
