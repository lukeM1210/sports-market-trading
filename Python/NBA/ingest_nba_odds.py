import os
import time
from pathlib import Path
from datetime import datetime, UTC
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests

# Config

API_KEY = os.environ.get("API_KEY")

if not API_KEY:
  raise RuntimeError("Missing API_KEY environment variable. Set it before running.")

API_URL = (f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?apiKey={API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american")

OUT_DIR = Path("NBA")
ODDS_OUT_PATH = OUT_DIR / "odds.csv"

# Columns used to uniquely identify a snapshot row
UNIQUE_COLS = [
    "event_id",
    "bookmaker_key",
    "market_key",
    "outcome_name",
    "line_point",
    "market_last_update",
]

def remove_expired_events(odds_path: Path) -> None:
    """
    Remove rows from odds.csv where the event has already commenced.
    """
    if not odds_path.exists():
       return
    
    df = pd.read_csv(odds_path)

    df["event_commence_utc"] = pd.to_datetime(
       df["event_commence_utc"], utc=True, errors="coerce"
    )

    now = datetime.now(UTC)

    before = len(df)
    df = df[df["event_commence_utc"] >= now]
    after = len(df)

    if after < before:
       df.to_csv(odds_path, index=False)
    else:
       print("No expired events to delete")

# Call the API
def fetch_odds_from_api() -> List[Dict[str, Any]]:
   """Call the odds API and return the JSON list."""
   print(f"[{datetime.now(UTC)}] Fetching odds from API...")
   resp = requests.get(API_URL, timeout=30)
   resp.raise_for_status()
   data = resp.json()
   print(f"Got {len(data)} events from API.")
   return data

# Flatten the raw json data into 3 flat dataframes.
def flatten_odds(json_data: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Turn the nested odds JSON into three flat DataFrames:
      - events_df
      - bookmakers_df
      - odds_df
    """

    odds_rows: list[dict[str, Any]] = []
    events_rows: dict[str, dict[str, Any]] = {}
    bookmakers_rows: dict[str, dict[str, Any]] = {}