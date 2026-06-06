from pathlib import Path
import pandas as pd

BASE = Path(__file__).parent

# Prediction markets are low-liquidity: a single small trade can move their
# quoted odds by several percentage points with no real sharp signal behind it.
# Keep them OUT of the movers calculation; they still appear in the line charts.
_PREDICTION_MARKETS = {"kalshi", "polymarket"}

# Minimum number of distinct sportsbooks that must agree on direction for a
# team to appear in the top-movers list.  Filters out single-book noise.
_MIN_BOOKS_CONSENSUS = 2


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

    # Exclude prediction markets — their thin liquidity creates false signals
    df = df[~df["bookmaker_key"].isin(_PREDICTION_MARKETS)]

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

    if not rows:
        return pd.DataFrame()

    df_rows = pd.DataFrame(rows)

    # Cross-book consensus filter: a team must show movement in the same
    # direction at _MIN_BOOKS_CONSENSUS or more distinct books.
    # This eliminates single-book noise (one book re-pricing, data error, etc.)
    def _consensus(group_df: pd.DataFrame, direction: str) -> pd.DataFrame:
        if direction == "up":
            movers = group_df[group_df["prob_shift"] > 0]
        else:
            movers = group_df[group_df["prob_shift"] < 0]
        n_books = movers["bookmaker"].nunique()
        if n_books < _MIN_BOOKS_CONSENSUS:
            return pd.DataFrame()
        # Return the single row with the median shift for a stable representative
        median_shift = movers["prob_shift"].median()
        rep = movers.iloc[(movers["prob_shift"] - median_shift).abs().argsort()[:1]].copy()
        rep["confirming_books"] = n_books
        return rep

    up_parts, down_parts = [], []
    for (_, opponent), grp in df_rows.groupby(["team", "opponent"]):
        up_parts.append(_consensus(grp, "up"))
        down_parts.append(_consensus(grp, "down"))

    return df_rows, up_parts, down_parts


def top_5_favorite_movers(csv_paths: list[Path]) -> pd.DataFrame:
    result = _build_movers(csv_paths)
    if isinstance(result, pd.DataFrame):   # empty
        return result
    _, up_parts, _ = result
    up_parts = [p for p in up_parts if not p.empty]
    if not up_parts:
        return pd.DataFrame()
    return (
        pd.concat(up_parts, ignore_index=True)
        .sort_values("prob_shift", ascending=False)
        .head(5)
        .reset_index(drop=True)
    )


def top_5_underdog_movers(csv_paths: list[Path]) -> pd.DataFrame:
    result = _build_movers(csv_paths)
    if isinstance(result, pd.DataFrame):   # empty
        return result
    _, _, down_parts = result
    down_parts = [p for p in down_parts if not p.empty]
    if not down_parts:
        return pd.DataFrame()
    return (
        pd.concat(down_parts, ignore_index=True)
        .sort_values("prob_shift", ascending=True)
        .head(5)
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    path = BASE / "test_odds.csv"

    print("=== Top 5 Favorite Movers ===")
    print(top_5_favorite_movers(path).to_string())

    print("\n=== Top 5 Underdog Movers ===")
    print(top_5_underdog_movers(path).to_string())
