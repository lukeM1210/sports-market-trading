from pathlib import Path
import streamlit as st
from charts import render_odds_page

st.set_page_config(page_title="NHL Line Movement", layout="wide")
render_odds_page("NHL Line Movement", Path(__file__).parent.parent / "NHL" / "output" / "odds.csv")
