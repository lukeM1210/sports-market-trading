from pathlib import Path
import streamlit as st
from charts import render_odds_page

st.set_page_config(page_title="NCAAF Line Movement", layout="wide")
render_odds_page("NCAAF Line Movement", Path(__file__).parent.parent / "NCAAF" / "output" / "odds.csv")
