"""
Fetches NFL final scores from ESPN's public scoreboard API (no API key needed).
Covers any historical date, which the Odds API scores endpoint cannot do.

Usage:
    from nfl_scores import get_scores_for_dates
    scores = get_scores_for_dates(["2025-09-07", "2025-09-08"])
    # Returns dict keyed by (home_team, away_team) → {"home_score": int, "away_score": int, "completed": bool}
"""

import json
import time
from pathlib import Path
from datetime import datetime, timedelta
import requests

BASE = Path(__file__).parent
SCORES_CACHE_DIR = BASE / "NFL" / "historical" / "scores_cache"
SCORES_CACHE_DIR.mkdir(parents=True, exist_ok=True)

ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"


def fetch_espn_scoreboard(date_str: str) -> dict:
    """Fetch ESPN scoreboard for YYYY-MM-DD. Cached to disk."""
    cache_file = SCORES_CACHE_DIR / f"{date_str}.json"
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)

    print(f"  [scores] fetching ESPN {date_str}")
    resp = requests.get(ESPN_URL, params={"dates": date_str.replace("-", "")}, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)

    time.sleep(0.3)
    return data


def parse_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN scoreboard JSON into flat game records."""
    games = []
    for event in data.get("events", []):
        competition = event.get("competitions", [{}])[0]
        competitors = competition.get("competitors", [])
        if len(competitors) != 2:
            continue

        completed = competition.get("status", {}).get("type", {}).get("completed", False)
        game_id = event.get("id")
        commence = event.get("date")

        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home or not away:
            continue

        home_team = home.get("team", {}).get("displayName", "")
        away_team = away.get("team", {}).get("displayName", "")
        home_score = int(home.get("score", 0)) if completed else None
        away_score = int(away.get("score", 0)) if completed else None

        games.append({
            "espn_id": game_id,
            "commence_time": commence,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "completed": completed,
        })
    return games


def get_scores_for_dates(date_strs: list[str]) -> dict[tuple[str, str], dict]:
    """
    Returns a dict keyed by (home_team, away_team) with score info.
    date_strs: list of "YYYY-MM-DD" strings covering all game days to fetch.
    """
    result = {}
    for d in date_strs:
        data = fetch_espn_scoreboard(d)
        for game in parse_scoreboard(data):
            key = (game["home_team"], game["away_team"])
            result[key] = game
    return result


def get_nfl_season_scores() -> dict[tuple[str, str], dict]:
    """Fetch all scores for the 2025-26 NFL season (Sep 4 2025 – Feb 8 2026)."""
    start = datetime(2025, 9, 4)
    end = datetime(2026, 2, 9)
    dates = []
    current = start
    while current <= end:
        # Only fetch Thu, Sat, Sun, Mon — NFL game days
        if current.weekday() in (0, 3, 5, 6):  # Mon=0, Thu=3, Sat=5, Sun=6
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    print(f"Fetching scores for {len(dates)} NFL game days...")
    return get_scores_for_dates(dates)


if __name__ == "__main__":
    scores = get_nfl_season_scores()
    completed = sum(1 for v in scores.values() if v["completed"])
    print(f"\nFetched {len(scores)} games, {completed} completed.")
