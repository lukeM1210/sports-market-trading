"""
Processes raw historical odds snapshots into a per-team-game dataset with:
  - De-vigged consensus implied probability (open & close)
  - ML movement % and movement bucket
  - Consensus spread (open & close) and spread movement
  - Prediction market implied prob (Kalshi + Polymarket, tracked separately)
  - ML result (W/L) and ATS result (W/L/P) from ESPN scores

Run after nfl_history_backfill.py and nfl_scores.py.
Output: Python/NFL/historical/nfl_2025_dataset.csv
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from nfl_scores import get_nfl_season_scores

BASE = Path(__file__).parent
RAW_DIR = BASE / "NFL" / "historical" / "raw"
OUT_PATH = BASE / "NFL" / "historical" / "nfl_2025_dataset.csv"

SHARP_BOOKS = ["pinnacle", "fanduel", "draftkings", "prophetx", "betonlineag", "novig"]
PREDICTION_MARKETS = ["kalshi", "polymarket"]


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def american_to_implied(odds: float) -> float:
    if odds < 0:
        return -odds / (-odds + 100)
    return 100 / (odds + 100)


def devig(p1: float, p2: float) -> tuple[float, float]:
    s = p1 + p2
    return p1 / s, p2 / s


def movement_bucket(pct: float) -> str:
    a = abs(pct)
    if a < 2:
        return "0-2%"
    if a < 5:
        return "2-5%"
    if a < 8:
        return "5-8%"
    return "8%+"


# ---------------------------------------------------------------------------
# Odds extraction helpers
# ---------------------------------------------------------------------------

def _h2h_odds(event: dict, team: str) -> dict[str, float]:
    """Returns {bookmaker_key: american_odds} for team across all available books."""
    result = {}
    for bm in event.get("bookmakers", []):
        mkt = next((m for m in bm["markets"] if m["key"] == "h2h"), None)
        if not mkt:
            continue
        for o in mkt["outcomes"]:
            if o["name"] == team:
                result[bm["key"]] = float(o["price"])
    return result


def _spread_point(event: dict, team: str) -> dict[str, float]:
    """Returns {bookmaker_key: spread_point} for team."""
    result = {}
    for bm in event.get("bookmakers", []):
        mkt = next((m for m in bm["markets"] if m["key"] == "spreads"), None)
        if not mkt:
            continue
        for o in mkt["outcomes"]:
            if o["name"] == team and o.get("point") is not None:
                result[bm["key"]] = float(o["point"])
    return result


def consensus_devigged_prob(event: dict, team: str, opponent: str, books: list[str]) -> float | None:
    """Average de-vigged implied prob for team across the given books."""
    probs = []
    for bm in event.get("bookmakers", []):
        if bm["key"] not in books:
            continue
        mkt = next((m for m in bm["markets"] if m["key"] == "h2h"), None)
        if not mkt:
            continue
        odds_map = {o["name"]: float(o["price"]) for o in mkt["outcomes"]}
        if team not in odds_map or opponent not in odds_map:
            continue
        p_team = american_to_implied(odds_map[team])
        p_opp = american_to_implied(odds_map[opponent])
        p_true, _ = devig(p_team, p_opp)
        probs.append(p_true)
    return round(sum(probs) / len(probs), 4) if probs else None


def consensus_spread(event: dict, team: str, books: list[str]) -> float | None:
    """Average spread point for team across the given books."""
    points = []
    for bm in event.get("bookmakers", []):
        if bm["key"] not in books:
            continue
        mkt = next((m for m in bm["markets"] if m["key"] == "spreads"), None)
        if not mkt:
            continue
        for o in mkt["outcomes"]:
            if o["name"] == team and o.get("point") is not None:
                points.append(float(o["point"]))
    return round(sum(points) / len(points), 2) if points else None


# ---------------------------------------------------------------------------
# Snapshot loading
# ---------------------------------------------------------------------------

def load_all_snapshots() -> dict[datetime, list[dict]]:
    """Load all cached JSON snapshots. Returns {utc_datetime: [event, ...]}."""
    snapshots = {}
    for f in sorted(RAW_DIR.glob("*.json")):
        # Filename: 2025-09-02T16-00-00Z.json  →  2025-09-02T16:00:00Z
        stem = f.stem  # e.g. "2025-09-02T16-00-00Z"
        date_part = stem[:10]
        time_part = stem[11:].rstrip("Z").replace("-", ":")
        iso = f"{date_part}T{time_part}+00:00"
        try:
            ts = datetime.fromisoformat(iso)
        except ValueError:
            continue
        with open(f) as fh:
            raw = json.load(fh)
        data = raw.get("data", raw) if isinstance(raw, dict) else raw
        if isinstance(data, list):
            snapshots[ts] = data
    return snapshots


# ---------------------------------------------------------------------------
# Team name normalization (ESPN ↔ Odds API)
# ---------------------------------------------------------------------------

ESPN_TO_ODDS_API = {
    "Arizona Cardinals": "Arizona Cardinals",
    "Atlanta Falcons": "Atlanta Falcons",
    "Baltimore Ravens": "Baltimore Ravens",
    "Buffalo Bills": "Buffalo Bills",
    "Carolina Panthers": "Carolina Panthers",
    "Chicago Bears": "Chicago Bears",
    "Cincinnati Bengals": "Cincinnati Bengals",
    "Cleveland Browns": "Cleveland Browns",
    "Dallas Cowboys": "Dallas Cowboys",
    "Denver Broncos": "Denver Broncos",
    "Detroit Lions": "Detroit Lions",
    "Green Bay Packers": "Green Bay Packers",
    "Houston Texans": "Houston Texans",
    "Indianapolis Colts": "Indianapolis Colts",
    "Jacksonville Jaguars": "Jacksonville Jaguars",
    "Kansas City Chiefs": "Kansas City Chiefs",
    "Las Vegas Raiders": "Las Vegas Raiders",
    "Los Angeles Chargers": "Los Angeles Chargers",
    "Los Angeles Rams": "Los Angeles Rams",
    "Miami Dolphins": "Miami Dolphins",
    "Minnesota Vikings": "Minnesota Vikings",
    "New England Patriots": "New England Patriots",
    "New Orleans Saints": "New Orleans Saints",
    "New York Giants": "New York Giants",
    "New York Jets": "New York Jets",
    "Philadelphia Eagles": "Philadelphia Eagles",
    "Pittsburgh Steelers": "Pittsburgh Steelers",
    "San Francisco 49ers": "San Francisco 49ers",
    "Seattle Seahawks": "Seattle Seahawks",
    "Tampa Bay Buccaneers": "Tampa Bay Buccaneers",
    "Tennessee Titans": "Tennessee Titans",
    "Washington Commanders": "Washington Commanders",
}


def normalize_team(name: str) -> str:
    return ESPN_TO_ODDS_API.get(name, name)


# ---------------------------------------------------------------------------
# Score matching
# ---------------------------------------------------------------------------

def match_score(home: str, away: str, scores: dict) -> dict | None:
    """Try to find the ESPN score record matching this game."""
    # Direct match
    key = (home, away)
    if key in scores:
        return scores[key]
    # Fuzzy: check if home/away appear as substrings
    for (h, a), v in scores.items():
        if home in h and away in a:
            return v
        if home in a and away in h:
            # flip — ESPN may have home/away swapped for neutral sites
            return {**v, "home_team": v["away_team"], "away_team": v["home_team"],
                    "home_score": v["away_score"], "away_score": v["home_score"]}
    return None


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def build_dataset() -> pd.DataFrame:
    print("Loading snapshots...")
    snapshots = load_all_snapshots()
    if not snapshots:
        print("No snapshots found. Run nfl_history_backfill.py first.")
        return pd.DataFrame()

    print(f"Loaded {len(snapshots)} snapshots.")

    # Index all events by id across all snapshots
    # For each event, find opening snapshot (earliest) and closing snapshot
    # (latest snapshot strictly before commence_time)
    event_open: dict[str, tuple[datetime, dict]] = {}
    event_snapshots: dict[str, list[tuple[datetime, dict]]] = {}

    for ts in sorted(snapshots):
        for ev in snapshots[ts]:
            eid = ev["id"]
            if eid not in event_open:
                event_open[eid] = (ts, ev)
            event_snapshots.setdefault(eid, []).append((ts, ev))

    print(f"Found {len(event_open)} unique events.")

    print("Fetching NFL scores from ESPN...")
    scores = get_nfl_season_scores()
    print(f"Got {len(scores)} game scores.")

    rows = []
    for event_id, (open_ts, open_ev) in event_open.items():
        commence_str = open_ev.get("commence_time", "")
        try:
            commence = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
        except ValueError:
            continue

        home = open_ev.get("home_team", "")
        away = open_ev.get("away_team", "")

        # Find closing snapshot: latest before kickoff
        close_ev = None
        close_ts = None
        for ts, ev in sorted(event_snapshots[event_id], reverse=True):
            if ts < commence:
                close_ev = ev
                close_ts = ts
                break

        if close_ev is None:
            continue

        # Look up score
        score = match_score(normalize_team(home), normalize_team(away), scores)

        for team, opp, is_home in [(home, away, True), (away, home, False)]:
            open_prob = consensus_devigged_prob(open_ev, team, opp, SHARP_BOOKS)
            close_prob = consensus_devigged_prob(close_ev, team, opp, SHARP_BOOKS)
            if open_prob is None or close_prob is None:
                continue

            open_spread = consensus_spread(open_ev, team, SHARP_BOOKS)
            close_spread_val = consensus_spread(close_ev, team, SHARP_BOOKS)
            spread_mov = (
                round(close_spread_val - open_spread, 2)
                if open_spread is not None and close_spread_val is not None
                else None
            )

            open_prob_pm = consensus_devigged_prob(open_ev, team, opp, PREDICTION_MARKETS)
            close_prob_pm = consensus_devigged_prob(close_ev, team, opp, PREDICTION_MARKETS)

            ml_movement = round((close_prob - open_prob) * 100, 2)

            # Results
            ml_result = ""
            ats_result = ""
            if score and score.get("completed"):
                if is_home:
                    team_score = score["home_score"]
                    opp_score = score["away_score"]
                else:
                    team_score = score["away_score"]
                    opp_score = score["home_score"]

                if team_score is not None and opp_score is not None:
                    if team_score > opp_score:
                        ml_result = "W"
                    elif team_score < opp_score:
                        ml_result = "L"
                    else:
                        ml_result = "T"

                    if close_spread_val is not None:
                        margin = team_score - opp_score
                        if margin > -close_spread_val:
                            ats_result = "W"
                        elif margin < -close_spread_val:
                            ats_result = "L"
                        else:
                            ats_result = "P"

            rows.append({
                "event_id": event_id,
                "game_date": commence.date().isoformat(),
                "home_team": home,
                "away_team": away,
                "team": team,
                "opponent": opp,
                "is_home": is_home,
                "open_prob_sharp": open_prob,
                "close_prob_sharp": close_prob,
                "ml_movement_pct": ml_movement,
                "movement_bucket": movement_bucket(ml_movement),
                "direction": "shortening" if ml_movement > 0 else "lengthening",
                "open_spread": open_spread,
                "close_spread": close_spread_val,
                "spread_movement": spread_mov,
                "open_prob_pm": open_prob_pm,
                "close_prob_pm": close_prob_pm,
                "ml_result": ml_result,
                "ats_result": ats_result,
            })

    df = pd.DataFrame(rows)

    # Mark the primary mover per game: the team with the larger absolute movement.
    # Bucket analysis should only use primary movers — otherwise every game
    # contributes one W and one L, forcing all buckets to exactly 50%.
    df["abs_movement"] = df["ml_movement_pct"].abs()
    df["is_primary_mover"] = (
        df.groupby("event_id")["abs_movement"]
        .transform("max") == df["abs_movement"]
    )
    df = df.drop(columns=["abs_movement"])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    resolved = df[df["ml_result"].isin(["W", "L", "T"])]
    print(f"Saved {len(df)} team-game rows ({len(resolved)} with results) → {OUT_PATH}")
    return df


if __name__ == "__main__":
    df = build_dataset()
    if not df.empty:
        print("\nSample:")
        print(df[["game_date", "team", "ml_movement_pct", "movement_bucket", "ml_result", "ats_result"]].head(10).to_string())
