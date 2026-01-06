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
        print(f"Deleted {before - after} expired rows (past event start time)")
    else:
        print("No expired events to delete")


def fetch_odds_from_api() -> List[Dict[str, Any]]:
    """Call the odds API and return the JSON list."""
    print(f"[{datetime.now(UTC)}] Fetching odds from API...")
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    print(f"Got {len(data)} events from API.")
    return data


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

    for event in json_data:
        event_id = event["id"]
        sport_key = event["sport_key"]
        sport_title = event.get("sport_title")
        commence_time = event.get("commence_time")
        home_team = event.get("home_team")
        away_team = event.get("away_team")

        events_rows[event_id] = {
            "event_id": event_id,
            "sport_key": sport_key,
            "sport_title": sport_title,
            "commence_time_utc": commence_time,
            "home_team": home_team,
            "away_team": away_team,
        }

        for book in event.get("bookmakers", []):
            bookmaker_key = book["key"]
            bookmaker_title = book.get("title")
            bookmaker_last_update = book.get("last_update")

            # NOTE: This is keyed by bookmaker_key; it’s fine for a dimension table.
            bookmakers_rows[bookmaker_key] = {
                "bookmaker_key": bookmaker_key,
                "bookmaker_title": bookmaker_title,
            }

            for market in book.get("markets", []):
                market_key = market["key"]  # h2h, spreads, totals
                market_last_update = market.get("last_update", bookmaker_last_update)

                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name")
                    price = outcome.get("price")
                    point = outcome.get("point")  # may be None for h2h

                    is_home_team = None
                    if outcome_name and home_team:
                        if outcome_name == home_team:
                            is_home_team = True
                        elif outcome_name == away_team:
                            is_home_team = False

                    odds_rows.append(
                        {
                            "event_id": event_id,
                            "bookmaker_key": bookmaker_key,
                            "market_key": market_key,
                            "outcome_name": outcome_name,
                            "is_home_team": is_home_team,
                            "price_american": price,
                            "line_point": point,
                            "event_commence_utc": commence_time,
                            "market_last_update": market_last_update,
                            "snapshot_utc": datetime.now(UTC),
                        }
                    )

    events_df = pd.DataFrame.from_dict(events_rows, orient="index")
    bookmakers_df = pd.DataFrame.from_dict(bookmakers_rows, orient="index")
    odds_df = pd.DataFrame(odds_rows)

    # Convert time strings to datetime if present
    # (events_df uses commence_time_utc; odds_df uses event_commence_utc + market_last_update + snapshot_utc)
    for col in ["commence_time_utc"]:
        if col in events_df.columns:
            events_df[col] = pd.to_datetime(events_df[col], errors="coerce", utc=True)

    for col in ["event_commence_utc", "market_last_update", "snapshot_utc"]:
        if col in odds_df.columns:
            odds_df[col] = pd.to_datetime(odds_df[col], errors="coerce", utc=True)

    return events_df, bookmakers_df, odds_df


def append_odds_snapshot(odds_df: pd.DataFrame, out_path: Path = ODDS_OUT_PATH) -> None:
    """
    Append only NEW rows to odds.csv, using a composite unique key.
    This prevents duplicates when the API returns unchanged odds.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cols_to_write = [
        "event_id",
        "bookmaker_key",
        "market_key",
        "outcome_name",
        "is_home_team",
        "price_american",
        "line_point",
        "event_commence_utc",
        "market_last_update",
        "snapshot_utc",
    ]
    odds_df = odds_df[cols_to_write].copy()

    # First-time write
    if not out_path.exists():
        odds_df.to_csv(out_path, index=False)
        print(f"Wrote new odds file with {len(odds_df)} rows -> {out_path}")
        return

    # Load only columns needed to compute existing keys
    existing = pd.read_csv(out_path, usecols=[c for c in UNIQUE_COLS if c != "market_last_update"] + ["market_last_update"])
    existing["market_last_update"] = pd.to_datetime(existing["market_last_update"], utc=True, errors="coerce")

    incoming = odds_df.copy()
    incoming["market_last_update"] = pd.to_datetime(incoming["market_last_update"], utc=True, errors="coerce")

    # Build composite keys (strings) for fast membership tests
    existing_keys = set(
    existing
        .loc[:, UNIQUE_COLS]
        .astype(str)
        .apply("|".join, axis=1)
        .values
    )

    incoming_keys = (
        incoming
            .loc[:, UNIQUE_COLS]
            .astype(str)
            .apply("|".join, axis=1)
            .values
    )


    mask_new = ~pd.Series(incoming_keys).isin(existing_keys)
    new_rows = incoming.loc[mask_new]

    if new_rows.empty:
        print("No new odds rows (all duplicates). Nothing appended.")
        return

    new_rows.to_csv(out_path, mode="a", header=False, index=False)
    print(f"Appended {len(new_rows)} new odds rows -> {out_path}")


def main() -> None:
    # Call the real API; fall back to local sample if needed
    try:
        data = fetch_odds_from_api()
    except Exception as e:
        print(f"API call failed: {e}")
        print("Falling back to local sample_odds.json (if present)...")
        import json

        sample_path = Path(__file__).parent.parent / "data" / "sample_odds.json"
        if sample_path.exists():
            with open(sample_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            print("No local sample_odds.json found. Exiting.")
            return

    events_df, bookmakers_df, odds_df = flatten_odds(data)

    print("\n=== events_df (first 5 rows) ===")
    print(events_df.head())

    print("\n=== bookmakers_df (first 5 rows) ===")
    print(bookmakers_df.head())

    print("\n=== odds_df (first 5 rows) ===")
    print(odds_df.head())

    OUT_DIR.mkdir(exist_ok=True)

    # Dimension-ish tables can be overwritten
    events_df.to_csv(OUT_DIR / "events.csv", index=False)
    bookmakers_df.to_csv(OUT_DIR / "bookmakers.csv", index=False)

    # Clean out old games BEFORE appending new data
    remove_expired_events(ODDS_OUT_PATH)

    # Odds is the time series (append new snapshots only)
    append_odds_snapshot(odds_df, ODDS_OUT_PATH)

    print(f"\nSaved files to ./{OUT_DIR}/")


def run_forever(interval_seconds: int = 600) -> None:
    while True:
        try:
            main()
        except Exception as e:
            print(f"Run failed: {e}")
        print(f"Sleeping {interval_seconds} seconds...\n")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_forever(3600)  # Rerun every hour
