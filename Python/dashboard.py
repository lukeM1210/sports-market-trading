from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Sports Line Movement", layout="wide")

st.title("Sports Line Movement")
st.caption("Select a sport to view live line movement charts.")

BASE = Path(__file__).parent

SPORTS = [
    ("NBA", "pages/nba.py", BASE / "NBA" / "output" / "odds.csv"),
    ("NFL", "pages/nfl.py", BASE / "NFL" / "output" / "odds.csv"),
    ("NHL", "pages/nhl.py", BASE / "NHL" / "output" / "odds.csv"),
    ("MLB", "pages/mlb.py", BASE / "MLB" / "output" / "odds.csv"),
    ("NCAAF", "pages/ncaaf.py", BASE / "NCAAF" / "output" / "odds.csv"),
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
