from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(page_title="NFL Model", layout="wide")

BASE = Path(__file__).parent.parent
HIST_DIR = BASE / "NFL" / "historical"

# Discover which season datasets exist on disk
available_years = sorted(
    [p.stem.replace("nfl_", "").replace("_dataset", "")
     for p in HIST_DIR.glob("nfl_*_dataset.csv")],
    reverse=True,
)

if not available_years:
    st.title("NFL Line Movement Model")
    st.warning("No dataset found. Build one first:")
    st.code("python nfl_history_backfill.py --year 2024\npython nfl_model.py --year 2024", language="bash")
    st.stop()

ALL_SEASONS = "All Seasons"
selected_year = st.sidebar.selectbox("Season", [ALL_SEASONS] + available_years, index=0)

def load_df(year: str) -> pd.DataFrame:
    path = HIST_DIR / f"nfl_{year}_dataset.csv"
    d = pd.read_csv(path)
    d["season"] = year
    return d

if selected_year == ALL_SEASONS:
    df = pd.concat([load_df(y) for y in available_years], ignore_index=True)
    season_label = f"{available_years[-1]}–{available_years[0]}"
else:
    df = load_df(selected_year)
    season_label = f"{selected_year}-{str(int(selected_year)+1)[-2:]}"

st.title("NFL Line Movement Model")
st.caption(f"{season_label} season · Sharp consensus (Pinnacle, FanDuel, DraftKings, ProphetX, BetOnline, NoVig)")

df["ml_movement_pct"] = pd.to_numeric(df["ml_movement_pct"], errors="coerce")
df["open_prob_sharp"] = pd.to_numeric(df["open_prob_sharp"], errors="coerce")
df["close_prob_sharp"] = pd.to_numeric(df["close_prob_sharp"], errors="coerce")

BUCKET_ORDER = ["0-2%", "2-5%", "5-8%", "8%+"]
SPREAD_BUCKET_ORDER = ["0-1pt", "1-2pts", "2-3pts", "3pts+"]

resolved = df[df["ml_result"].isin(["W", "L", "T"])].copy()

# Classify spread movement into buckets and direction.
# Positive spread_movement = favorable for the team (easier to cover) for both favs and dogs.
#   Favorite: -6.5 → -3.5 = movement +3 = now needs to win by less = easier = favorable
#   Underdog: +3.5 → +6.5 = movement +3 = now gets more points = easier = favorable
def spread_bucket(mov):
    if pd.isna(mov):
        return ""
    a = abs(mov)
    if a < 1:   return "0-1pt"
    if a < 2:   return "1-2pts"
    if a < 3:   return "2-3pts"
    return "3pts+"

resolved["spread_movement"] = pd.to_numeric(resolved["spread_movement"], errors="coerce")
resolved["spread_bucket"] = resolved["spread_movement"].apply(spread_bucket)
resolved["spread_favorable"] = resolved["spread_movement"] > 0

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    home_away = st.multiselect(
        "Home / Away",
        [True, False],
        default=[True, False],
        format_func=lambda x: "Home" if x else "Away",
    )
    playoffs_only = st.checkbox("Playoffs only", value=False)

filtered = resolved.copy()
if home_away:
    filtered = filtered[filtered["is_home"].isin(home_away)]
if playoffs_only:
    filtered = filtered[pd.to_datetime(filtered["game_date"]) >= pd.Timestamp("2026-01-10")]

# ---------------------------------------------------------------------------
# Core insight: only analyze SHORTENING teams.
#
# After de-vigging, probabilities sum to 1 — so for each game one team's
# implied prob goes UP (shortening) and the other goes DOWN (lengthening).
# If we include both sides, shortening wins cancel lengthening losses → always 50%.
# Filtering to shortening only gives one row per game and a real signal.
# ---------------------------------------------------------------------------
shortening = filtered[filtered["direction"] == "shortening"]
lengthening = filtered[filtered["direction"] == "lengthening"]

# ---------------------------------------------------------------------------
# Summary metrics (shortening teams = one per game)
# ---------------------------------------------------------------------------
total = len(shortening)
ml_wins = (shortening["ml_result"] == "W").sum()
ml_losses = (shortening["ml_result"] == "L").sum()
ats_s = shortening[shortening["ats_result"].isin(["W", "L", "P"])]
ats_wins = (ats_s["ats_result"] == "W").sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Games", total)
c2.metric("ML Record (shortening)", f"{ml_wins}W-{ml_losses}L")
c3.metric("ML Win Rate", f"{ml_wins / total * 100:.1f}%" if total else "—")
c4.metric("ATS Win Rate", f"{ats_wins / len(ats_s) * 100:.1f}%" if len(ats_s) else "—")

st.markdown("---")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bucket_table(data: pd.DataFrame, result_col: str, bucket_col: str = "movement_bucket", order: list = BUCKET_ORDER) -> pd.DataFrame:
    rows = []
    for bucket in order:
        grp = data[data[bucket_col] == bucket]
        n = len(grp)
        w = (grp[result_col] == "W").sum()
        l = (grp[result_col] == "L").sum()
        rows.append({
            "Bucket": bucket,
            "Games": n,
            "W": int(w),
            "L": int(l),
            "Win %": round(w / n * 100, 1) if n else 0,
        })
    return pd.DataFrame(rows)


def bucket_chart(tbl: pd.DataFrame, title: str) -> go.Figure:
    colors = [
        "#00c853" if w >= 55 else "#d50000" if w <= 45 else "#ffd600"
        for w in tbl["Win %"]
    ]
    fig = go.Figure(go.Bar(
        x=tbl["Bucket"],
        y=tbl["Win %"],
        text=[f"{w}%<br>({g} games)" for w, g in zip(tbl["Win %"], tbl["Games"])],
        textposition="outside",
        marker_color=colors,
        cliponaxis=False,
    ))
    fig.add_hline(y=50, line_dash="dash", line_color="white", opacity=0.4)
    fig.update_layout(
        title=title,
        template="plotly_dark",
        yaxis=dict(title="Win %", range=[0, 100]),
        xaxis=dict(title="Movement Bucket (magnitude)"),
        height=350,
        margin=dict(t=50, b=10),
    )
    return fig


# ---------------------------------------------------------------------------
# ML buckets — shortening teams (implied prob movement)
# ---------------------------------------------------------------------------
st.subheader("Moneyline — Shortening Teams by Implied Prob Movement")
st.caption("Teams whose de-vigged implied probability increased from open to close. One team per game.")

ml_tbl = bucket_table(shortening, "ml_result")
st.plotly_chart(bucket_chart(ml_tbl, "ML Win Rate"), use_container_width=True, key="ml_chart")
st.dataframe(
    ml_tbl.style.format({"Win %": "{:.1f}%"})
          .background_gradient(subset=["Win %"], cmap="RdYlGn", vmin=40, vmax=65),
    hide_index=True,
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# ROI chart — $100/game flat bet on shortening teams
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Profitability — $100 Flat Bet per Game (Shortening Teams)")
st.caption("Net profit if you bet $100 on every shortening team at their sharp consensus closing odds.")

def bet_profit(row) -> float | None:
    odds = row.get("close_odds_american")
    if pd.isna(odds):
        return None
    if row["ml_result"] == "W":
        return (odds / 100 * 100) if odds >= 100 else (10000 / abs(odds))
    if row["ml_result"] == "L":
        return -100.0
    return None

if "close_odds_american" not in shortening.columns:
    st.info("Re-run nfl_model.py to enable profitability analysis.")
else:
    roi_data = shortening[shortening["ml_result"].isin(["W", "L"])].copy()
    roi_data["close_odds_american"] = pd.to_numeric(roi_data["close_odds_american"], errors="coerce")
    roi_data["profit"] = roi_data.apply(bet_profit, axis=1)
    roi_data = roi_data.dropna(subset=["profit"])

    roi_rows = []
    for bucket in BUCKET_ORDER:
        grp = roi_data[roi_data["movement_bucket"] == bucket]
        n = len(grp)
        net = grp["profit"].sum()
        wagered = n * 100
        roi_pct = net / wagered * 100 if wagered else 0
        roi_rows.append({
            "Bucket": bucket,
            "Games": n,
            "Net Profit": round(net, 2),
            "Total Wagered": wagered,
            "ROI %": round(roi_pct, 1),
        })
    roi_tbl = pd.DataFrame(roi_rows)

    roi_colors = ["#00c853" if r > 0 else "#d50000" for r in roi_tbl["ROI %"]]
    fig_roi = go.Figure(go.Bar(
        x=roi_tbl["Bucket"],
        y=roi_tbl["ROI %"],
        marker_color=roi_colors,
        text=[f"{r:+.1f}%<br>(${n:+,.0f} on {g} games)"
              for r, n, g in zip(roi_tbl["ROI %"], roi_tbl["Net Profit"], roi_tbl["Games"])],
        textposition="outside",
        cliponaxis=False,
    ))
    fig_roi.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
    fig_roi.update_layout(
        template="plotly_dark",
        yaxis=dict(title="ROI %"),
        xaxis=dict(title="Movement Bucket"),
        height=350,
        margin=dict(t=20, b=10),
    )
    st.plotly_chart(fig_roi, use_container_width=True, key="roi_chart")
    st.dataframe(
        roi_tbl.style
               .format({"Net Profit": "${:+,.2f}", "Total Wagered": "${:,.0f}", "ROI %": "{:+.1f}%"})
               .background_gradient(subset=["ROI %"], cmap="RdYlGn", vmin=-15, vmax=15),
        hide_index=True,
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# ATS buckets — spread-favorable teams (point movement)
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("ATS — Spread-Favorable Teams by Point Movement")
st.caption(
    "Teams whose spread moved in their favor (easier to cover). "
    "Bucketed by how many points the spread moved. "
    "Favorite: -6.5 → -3.5 = +3pts favorable. Underdog: +3.5 → +6.5 = +3pts favorable."
)

ats_favorable = filtered[
    filtered["spread_favorable"] == True &
    filtered["ats_result"].isin(["W", "L", "P"])
].copy()

ats_tbl = bucket_table(ats_favorable, "ats_result", bucket_col="spread_bucket", order=SPREAD_BUCKET_ORDER)

ats_chart_col, ats_table_col = st.columns([3, 2])
with ats_chart_col:
    st.plotly_chart(bucket_chart(ats_tbl, "ATS Win Rate"), use_container_width=True, key="ats_chart")
with ats_table_col:
    st.dataframe(
        ats_tbl.style.format({"Win %": "{:.1f}%"})
               .background_gradient(subset=["Win %"], cmap="RdYlGn", vmin=40, vmax=65),
        hide_index=True,
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Lengthening teams for comparison
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("ML — Lengthening Teams (comparison)")
st.caption("Teams whose implied probability decreased. Should be the inverse of the shortening chart above.")

ml_long_tbl = bucket_table(lengthening, "ml_result")
st.plotly_chart(bucket_chart(ml_long_tbl, "ML Win Rate (lengthening)"), use_container_width=True, key="ml_long_chart")

st.markdown("---")
st.subheader("ATS — Spread-Unfavorable Teams (comparison)")
st.caption("Teams whose spread moved against them (harder to cover).")

ats_unfavorable = filtered[
    (filtered["spread_favorable"] == False) &
    filtered["ats_result"].isin(["W", "L", "P"])
].copy()
ats_unf_tbl = bucket_table(ats_unfavorable, "ats_result", bucket_col="spread_bucket", order=SPREAD_BUCKET_ORDER)
st.plotly_chart(bucket_chart(ats_unf_tbl, "ATS Win Rate (unfavorable spread move)"), use_container_width=True, key="ats_long_chart")

# ---------------------------------------------------------------------------
# Home vs Away split (shortening only)
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Home vs Away — ML Win % by Bucket (Shortening)")

split_rows = []
for bucket in BUCKET_ORDER:
    for label, is_home in [("Home", True), ("Away", False)]:
        grp = shortening[(shortening["movement_bucket"] == bucket) & (shortening["is_home"] == is_home)]
        n = len(grp)
        w = (grp["ml_result"] == "W").sum()
        split_rows.append({"Bucket": bucket, "Side": label, "Games": n,
                           "Win %": round(w / n * 100, 1) if n else None})

split_df = pd.DataFrame(split_rows)
home_df = split_df[split_df["Side"] == "Home"][["Bucket", "Games", "Win %"]].rename(
    columns={"Games": "Home Games", "Win %": "Home Win %"})
away_df = split_df[split_df["Side"] == "Away"][["Bucket", "Games", "Win %"]].rename(
    columns={"Games": "Away Games", "Win %": "Away Win %"})
st.dataframe(
    home_df.merge(away_df, on="Bucket")
           .style.format({"Home Win %": "{:.1f}%", "Away Win %": "{:.1f}%"}, na_rep="—")
           .background_gradient(subset=["Home Win %", "Away Win %"], cmap="RdYlGn", vmin=40, vmax=65),
    hide_index=True,
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Prediction markets comparison
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Prediction Markets vs Sharp Books")
st.caption("Kalshi + Polymarket closing prob vs sharp consensus closing prob (shortening teams)")

pm_df = shortening.dropna(subset=["close_prob_pm", "close_prob_sharp"]).copy()
if pm_df.empty:
    st.info("No prediction market data available.")
else:
    pm_df["pm_vs_sharp"] = (pm_df["close_prob_pm"] - pm_df["close_prob_sharp"]) * 100
    fig_pm = go.Figure()
    for result, color in [("W", "#00c853"), ("L", "#d50000")]:
        grp = pm_df[pm_df["ml_result"] == result]
        fig_pm.add_trace(go.Scatter(
            x=grp["pm_vs_sharp"],
            y=grp["ml_movement_pct"],
            mode="markers",
            name=result,
            marker=dict(color=color, size=6, opacity=0.7),
            text=grp.apply(lambda r: f"{r['team']} vs {r['opponent']}<br>{r['game_date']}", axis=1),
            hovertemplate="%{text}<br>PM vs Sharp: %{x:.1f}pp<br>ML Movement: %{y:.1f}%<extra></extra>",
        ))
    fig_pm.update_layout(
        template="plotly_dark",
        xaxis=dict(title="Prediction Market − Sharp Prob (pp)", zeroline=True),
        yaxis=dict(title="ML Movement % (Sharp)"),
        height=400,
        legend_title_text="ML Result",
    )
    st.plotly_chart(fig_pm, use_container_width=True, key="pm_scatter")

# ---------------------------------------------------------------------------
# Raw game log
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Game Log")
display_cols = [
    "game_date", "team", "opponent", "is_home", "direction",
    "open_prob_sharp", "close_prob_sharp", "ml_movement_pct",
    "movement_bucket", "open_spread", "close_spread", "spread_movement",
    "ml_result", "ats_result",
]
available = [c for c in display_cols if c in df.columns]
st.dataframe(
    df[available].sort_values("game_date", ascending=False)
    .style.format({
        "open_prob_sharp": "{:.1%}",
        "close_prob_sharp": "{:.1%}",
        "ml_movement_pct": "{:+.2f}",
    }),
    hide_index=True,
    use_container_width=True,
)
