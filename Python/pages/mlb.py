from pathlib import Path
import streamlit as st
from charts import render_odds_page

st.set_page_config(page_title="MLB Line Movement", layout="wide")
render_odds_page("MLB Line Movement", Path(__file__).parent.parent / "MLB" / "output" / "odds.csv")
