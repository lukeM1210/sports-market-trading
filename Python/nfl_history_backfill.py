"""
NFL Historical Odds Backfill
Usage:
    python nfl_history_backfill.py          # fetch all seasons
    python nfl_history_backfill.py --year 2023
    python nfl_history_backfill.py --year 2023 2024 2025

Raw snapshots cached to: Python/NFL/historical/{year}/raw/
Re-runs cost zero credits — existing cache files are never re-fetched.
"""

import os
import sys
import json
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta

from dotenv import load_dotenv
import requests

load_dotenv(override=True)
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing API_KEY. Check your .env file.")

BASE = Path(__file__).parent

SHARP_BOOKS = ["pinnacle", "fanduel", "draftkings", "prophetx", "betonlineag", "novig"]
PREDICTION_MARKETS = ["kalshi", "polymarket"]
ALL_BOOKS = ",".join(SHARP_BOOKS + PREDICTION_MARKETS)

# Season kickoff = date of the Thursday Night Kickoff game (Week 1)
# Label is the season year (year the regular season starts)
NFL_SEASONS = {
    "2020": "2020-09-10",
    "2021": "2021-09-09",
    "2022": "2022-09-08",
    "2023": "2023-09-07",
    "2024": "2024-09-05",
    "2025": "2025-09-04",
}


def is_edt(dt: datetime) -> bool:
    """True if date falls in Eastern Daylight Time (2nd Sun March – 1st Sun Nov)."""
    year = dt.year
    # 2nd Sunday of March
    march1 = datetime(year, 3, 1)
    edt_start = march1 + timedelta(days=(6 - march1.weekday()) % 7 + 7)
    # 1st Sunday of November
    nov1 = datetime(year, 11, 1)
    edt_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    return edt_start.date() <= dt.date() < edt_end.date()


def to_utc_str(dt: datetime, hour_et: float) -> str:
    """Convert a date + Eastern hour to a UTC ISO string."""
    utc_offset = 4 if is_edt(dt) else 5
    utc_dt = dt + timedelta(hours=hour_et + utc_offset)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_season_snapshots(kickoff_date_str: str) -> list[tuple]:
    """
    Generate (label, open_utc, thu_close_utc_or_None, sun_close_utc) tuples
    for all 18 regular season weeks + playoffs, given the Week 1 kickoff (Thursday).

    Opening:       Tuesday noon ET
    Thu close:     Thursday 8:30 PM ET (after kickoff)
    Sunday close:  Sunday 11:30 AM ET (1 hr before early games)
    """
    kickoff = datetime.strptime(kickoff_date_str, "%Y-%m-%d")  # Thursday

    week1_tue = kickoff - timedelta(days=2)
    week1_thu = kickoff
    week1_sun = kickoff + timedelta(days=3)

    snapshots = []

    # 18 regular season weeks
    for w in range(18):
        offset = timedelta(days=w * 7)
        tue = week1_tue + offset
        thu = week1_thu + offset
        sun = week1_sun + offset
        snapshots.append((
            f"W{w+1:02d}",
            to_utc_str(tue, 12.0),    # noon ET open
            to_utc_str(thu, 20.5),    # 8:30 PM ET Thursday close
            to_utc_str(sun, 11.5),    # 11:30 AM ET Sunday close
        ))

    # Playoffs: Wild Card ~2 weeks after Week 18, then every week after
    week18_sun = week1_sun + timedelta(days=17 * 7)
    playoff_rounds = [
        ("WC",   week18_sun + timedelta(days=7)),
        ("DIV",  week18_sun + timedelta(days=14)),
        ("CONF", week18_sun + timedelta(days=21)),
        ("SB",   week18_sun + timedelta(days=35)),   # ~5 weeks after W18
    ]
    for label, game_sun in playoff_rounds:
        tue = game_sun - timedelta(days=5)
        snapshots.append((
            label,
            to_utc_str(tue, 12.0),
            None,
            to_utc_str(game_sun, 11.5),
        ))

    return snapshots


def raw_dir(year: str) -> Path:
    d = BASE / "NFL" / "historical" / year / "raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def migrate_legacy_raw() -> None:
    """Move old flat raw/ folder into 2025/ if migration hasn't happened yet."""
    legacy = BASE / "NFL" / "historical" / "raw"
    target = BASE / "NFL" / "historical" / "2025" / "raw"
    if legacy.exists() and not target.exists():
        print("Migrating existing 2025 raw data to NFL/historical/2025/raw/ ...")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(legacy), str(target))
        print("Migration done.")


def fetch_snapshot(timestamp: str, year: str) -> dict:
    """Fetch historical odds at timestamp. Cached per season year."""
    safe_name = timestamp.replace(":", "-")
    cache_file = raw_dir(year) / f"{safe_name}.json"

    if cache_file.exists():
        print(f"  [cache] {timestamp}")
        with open(cache_file) as f:
            return json.load(f)

    print(f"  [fetch] {timestamp}")
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


def fetch_season(year: str) -> None:
    kickoff = NFL_SEASONS.get(year)
    if not kickoff:
        print(f"Unknown season year: {year}. Available: {list(NFL_SEASONS)}")
        return

    snapshots = generate_season_snapshots(kickoff)
    total = sum(2 + (1 if thu else 0) for _, _, thu, _ in snapshots)
    print(f"\n=== NFL {year} Season ({total} snapshots) ===")

    fetched = 0
    for label, open_ts, thu_ts, sun_ts in snapshots:
        print(f"\n[{label}]")
        fetch_snapshot(open_ts, year);  fetched += 1
        if thu_ts:
            fetch_snapshot(thu_ts, year); fetched += 1
        fetch_snapshot(sun_ts, year);   fetched += 1

    print(f"\nDone. {fetched} snapshots → NFL/historical/{year}/raw/")


def fetch_all(years: list[str] | None = None) -> None:
    migrate_legacy_raw()
    targets = years or list(NFL_SEASONS)
    for year in targets:
        fetch_season(year)


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--year" in args:
        idx = args.index("--year")
        selected = args[idx + 1:]
        fetch_all(selected if selected else None)
    else:
        fetch_all()
