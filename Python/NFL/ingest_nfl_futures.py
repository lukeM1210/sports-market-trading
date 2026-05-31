import os
import time
import json
from pathlib import Path
from datetime import datetime, UTC

from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv(override=True)
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing API_KEY. Check your .env file.")

# The Odds API sport key for NFL Super Bowl winner futures
SPORT_KEY = "americanfootball_nfl_super_bowl_winner"
BOOKS = "pinnacle,fanduel,betonlineag,prophetx,novig,draftkings,betmgm,caesars"

API_URL = (
    f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds/"
    f"?apiKey={API_KEY}&markets=outrights&oddsFormat=american&bookmakers={BOOKS}"
)

OUT_DIR = Path(__file__).parent / "futures_output"
OUT_PATH = OUT_DIR / "futures.csv"
CACHE_TTL_MINUTES = 60

UNIQUE_COLS = ["bookmaker_key", "outcome_name", "price_american", "market_last_update"]


def is_cache_fresh() -> bool:
    if not OUT_PATH.exists():
        return False
    return (time.time() - OUT_PATH.stat().st_mtime) < CACHE_TTL_MINUTES * 60


def fetch_futures() -> list[dict]:
    print(f"[{datetime.now(UTC)}] Fetching NFL futures from API...")
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"Got {len(data)} events. Credits remaining: {remaining}")
    return data


def flatten_futures(data: list[dict]) -> pd.DataFrame:
    rows = []
    for event in data:
        for book in event.get("bookmakers", []):
            book_key = book["key"]
            for market in book.get("markets", []):
                if market["key"] != "outrights":
                    continue
                last_update = market.get("last_update", book.get("last_update"))
                for outcome in market.get("outcomes", []):
                    rows.append({
                        "snapshot_utc": datetime.now(UTC).isoformat(),
                        "bookmaker_key": book_key,
                        "outcome_name": outcome["name"],
                        "price_american": outcome["price"],
                        "market_last_update": last_update,
                    })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
        df["snapshot_utc"] = pd.to_datetime(df["snapshot_utc"], utc=True, errors="coerce")
    return df


def append_snapshot(df: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not OUT_PATH.exists():
        df.to_csv(OUT_PATH, index=False)
        print(f"Created futures file with {len(df)} rows → {OUT_PATH}")
        return

    existing = pd.read_csv(OUT_PATH)
    existing["market_last_update"] = pd.to_datetime(existing["market_last_update"], utc=True, errors="coerce")

    incoming = df.copy()
    incoming["market_last_update"] = pd.to_datetime(incoming["market_last_update"], utc=True, errors="coerce")

    existing_keys = set(
        existing[UNIQUE_COLS].astype(str).apply("|".join, axis=1)
    )
    incoming_keys = incoming[UNIQUE_COLS].astype(str).apply("|".join, axis=1)

    new_rows = incoming[~incoming_keys.isin(existing_keys)]
    if new_rows.empty:
        print("No new futures rows. Nothing appended.")
        return

    new_rows.to_csv(OUT_PATH, mode="a", header=False, index=False)
    print(f"Appended {len(new_rows)} new rows → {OUT_PATH}")


def main() -> None:
    if is_cache_fresh():
        age = int((time.time() - OUT_PATH.stat().st_mtime) / 60)
        print(f"Cache fresh ({age}m old). Skipping API call.")
        return

    try:
        data = fetch_futures()
    except Exception as e:
        print(f"API call failed: {e}")
        return

    df = flatten_futures(data)
    if df.empty:
        print("No futures data returned.")
        return

    print(f"Flattened {len(df)} outcome rows.")
    append_snapshot(df)


def run_forever(interval_seconds: int = 3600) -> None:
    while True:
        try:
            main()
        except Exception as e:
            print(f"Run failed: {e}")
        print(f"Sleeping {interval_seconds} seconds...\n")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_forever(3600)
