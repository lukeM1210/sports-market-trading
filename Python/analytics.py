from pathlib import Path
import pandas as pd

BASE = Path(__file__).parent


def to_implied_prob(odds: float) -> float:
    return (-odds / (-odds + 100) * 100) if odds < 0 else (100 / (odds + 100) * 100)


def _build_movers(csv_paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_csv(p) for p in csv_paths if p.exists()]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["market_last_update"] = pd.to_datetime(df["market_last_update"], utc=True, errors="coerce")
    df["event_commence_utc"] = pd.to_datetime(df["event_commence_utc"], utc=True, errors="coerce")
    df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce")
    df = df.dropna(subset=["price_american", "market_last_update"])
    df = df[df["market_key"] == "h2h"]

    rows = []
    for (event_id, bookmaker, outcome), group in df.groupby(
        ["event_id", "bookmaker_key", "outcome_name"]
    ):
        group = group.sort_values("market_last_update")
        if len(group) < 2:
            continue

        open_odds = group["price_american"].iloc[0]
        current_odds = group["price_american"].iloc[-1]

        if open_odds == current_odds:
            continue

        open_prob = to_implied_prob(open_odds)
        current_prob = to_implied_prob(current_odds)
        prob_shift = round(current_prob - open_prob, 2)

        opponent_rows = df[
            (df["event_id"] == event_id) &
            (df["outcome_name"] != outcome)
        ]["outcome_name"]
        opponent = opponent_rows.iloc[0] if not opponent_rows.empty else "Unknown"

        rows.append({
            "team": outcome,
            "opponent": opponent,
            "bookmaker": bookmaker,
            "open_odds": int(open_odds),
            "current_odds": int(current_odds),
            "prob_shift": prob_shift,
            "game_start_utc": group["event_commence_utc"].iloc[0],
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def top_5_favorite_movers(csv_paths: list[Path]) -> pd.DataFrame:
    df = _build_movers(csv_paths)
    if df.empty:
        return df
    return (
        df[df["prob_shift"] > 0]
        .sort_values("prob_shift", ascending=False)
        .drop_duplicates(subset=["team", "opponent"])
        .head(5)
        .reset_index(drop=True)
    )


def top_5_underdog_movers(csv_paths: list[Path]) -> pd.DataFrame:
    df = _build_movers(csv_paths)
    if df.empty:
        return df
    return (
        df[df["prob_shift"] < 0]
        .sort_values("prob_shift", ascending=True)
        .drop_duplicates(subset=["team", "opponent"])
        .head(5)
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    path = BASE / "test_odds.csv"

    print("=== Top 5 Favorite Movers ===")
    print(top_5_favorite_movers(path).to_string())

    print("\n=== Top 5 Underdog Movers ===")
    print(top_5_underdog_movers(path).to_string())
