import os
import time
from pathlib import Path
from datetime import datetime, UTC
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests

# ---- CONFIG ----
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing API_KEY environment variable. Set it before running.")

API_URL = (
    "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/"
    f"?apiKey={API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american"
)

OUT_DIR = Path("output")
ODDS_OUT_PATH = OUT_DIR / "odds.csv"