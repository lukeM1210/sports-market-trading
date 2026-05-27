import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

load_dotenv(override=True)
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing API_KEY. Check your .env file.")

BASE = Path(__file__).parent
RAW_DIR = BASE / "NFL" / "historical" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Sharp books used for consensus line (Circa not available in API)
SHARP_BOOKS = ["pinnacle", "fanduel", "draftkings", "prophetx", "betonlineag", "novig"]
# Tracked separately — prediction markets
PREDICTION_MARKETS = ["kalshi", "polymarket"]
ALL_BOOKS = ",".join(SHARP_BOOKS + PREDICTION_MARKETS)

# NFL 2025-26 season snapshots.
# Each tuple: (week_label, open_utc, thu_close_utc_or_None, sun_close_utc)
#
# Opening: Tuesday ~noon ET after previous week's games
#   EDT (Sep–Nov 1):  noon ET = 16:00 UTC
#   EST (Nov 2+):     noon ET = 17:00 UTC
#
# Thursday close: ~8 PM ET kickoff window
#   EDT: 00:00 UTC next day  |  EST: 01:00 UTC next day
#
# Sunday close: ~11:30 AM ET (1 hr before early games)
#   EDT: 15:30 UTC  |  EST: 16:30 UTC
#
# Monday Night close: ~8 PM ET Monday
#   EDT: 00:00 UTC Tue  |  EST: 01:00 UTC Tue
NFL_WEEKS = [
    # --- Regular Season ---
    ("W01",  "2025-09-02T16:00:00Z", "2025-09-04T23:30:00Z", "2025-09-07T15:30:00Z"),
    ("W02",  "2025-09-09T16:00:00Z", "2025-09-11T23:30:00Z", "2025-09-14T15:30:00Z"),
    ("W03",  "2025-09-16T16:00:00Z", "2025-09-18T23:30:00Z", "2025-09-21T15:30:00Z"),
    ("W04",  "2025-09-23T16:00:00Z", "2025-09-25T23:30:00Z", "2025-09-28T15:30:00Z"),
    ("W05",  "2025-09-30T16:00:00Z", "2025-10-02T23:30:00Z", "2025-10-05T15:30:00Z"),
    ("W06",  "2025-10-07T16:00:00Z", "2025-10-09T23:30:00Z", "2025-10-12T15:30:00Z"),
    ("W07",  "2025-10-14T16:00:00Z", "2025-10-16T23:30:00Z", "2025-10-19T15:30:00Z"),
    ("W08",  "2025-10-21T16:00:00Z", "2025-10-23T23:30:00Z", "2025-10-26T15:30:00Z"),
    ("W09",  "2025-10-28T16:00:00Z", "2025-10-30T23:30:00Z", "2025-11-02T15:30:00Z"),
    ("W10",  "2025-11-04T17:00:00Z", "2025-11-06T00:30:00Z", "2025-11-09T16:30:00Z"),
    ("W11",  "2025-11-11T17:00:00Z", "2025-11-13T00:30:00Z", "2025-11-16T16:30:00Z"),
    ("W12",  "2025-11-18T17:00:00Z", "2025-11-20T00:30:00Z", "2025-11-23T16:30:00Z"),
    ("W13",  "2025-11-25T17:00:00Z", "2025-11-27T00:30:00Z", "2025-11-30T16:30:00Z"),
    ("W14",  "2025-12-02T17:00:00Z", "2025-12-04T00:30:00Z", "2025-12-07T16:30:00Z"),
    ("W15",  "2025-12-09T17:00:00Z", "2025-12-11T00:30:00Z", "2025-12-14T16:30:00Z"),
    ("W16",  "2025-12-16T17:00:00Z", "2025-12-18T00:30:00Z", "2025-12-21T16:30:00Z"),
    ("W17",  "2025-12-23T17:00:00Z", "2025-12-25T00:30:00Z", "2025-12-28T16:30:00Z"),
    ("W18",  "2025-12-30T17:00:00Z", None,                    "2026-01-04T16:30:00Z"),
    # --- Playoffs ---
    ("WC",   "2026-01-06T17:00:00Z", None,                    "2026-01-10T16:30:00Z"),
    ("DIV",  "2026-01-13T17:00:00Z", None,                    "2026-01-17T16:30:00Z"),
    ("CONF", "2026-01-20T17:00:00Z", None,                    "2026-01-25T16:30:00Z"),
    ("SB",   "2026-01-27T17:00:00Z", None,                    "2026-02-08T21:30:00Z"),
]


def fetch_snapshot(timestamp: str) -> dict:
    """Fetch historical odds at the given UTC timestamp. Results cached to disk."""
    safe_name = timestamp.replace(":", "-")
    cache_file = RAW_DIR / f"{safe_name}.json"

    if cache_file.exists():
        print(f"  [cache] {timestamp}")
        with open(cache_file) as f:
            return json.load(f)

    print(f"  [fetch] {timestamp}")
    # Use bookmakers param directly — regions is ignored when bookmakers is set
    url = (
        "https://api.the-odds-api.com/v4/historical/sports/americanfootball_nfl/odds/"
        f"?apiKey={API_KEY}"
        f"&date={timestamp}"
        f"&markets=h2h,spreads"
        f"&oddsFormat=american"
        f"&bookmakers={ALL_BOOKS}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)

    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    print(f"    credits remaining={remaining}  used={used}")

    time.sleep(1)
    return data


def fetch_all() -> None:
    print("=== NFL 2025-26 Historical Odds Backfill ===\n")
    total = sum(2 + (1 if thu else 0) for _, _, thu, _ in NFL_WEEKS)
    fetched = 0

    for week_label, open_ts, thu_ts, sun_ts in NFL_WEEKS:
        print(f"\n[{week_label}]")
        fetch_snapshot(open_ts);   fetched += 1
        if thu_ts:
            fetch_snapshot(thu_ts); fetched += 1
        fetch_snapshot(sun_ts);    fetched += 1

    print(f"\nDone. {fetched} snapshots in {RAW_DIR}")


if __name__ == "__main__":
    fetch_all()
