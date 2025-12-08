import os
import json
from datetime import datetime

import duckdb
import pandas as pd
from google import genai
import functions_framework

# ==========================
# Gemini / Vertex AI config
# ==========================

PROJECT_ID = os.environ.get("PROJECT_ID", "baratz00-ba882-fall25")
LOCATION = os.environ.get("LOCATION", "us-central1")

client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location=LOCATION,
)

# ==========================
# MotherDuck config
# ==========================

MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")


def get_md_connection():
    """
    Opens a DuckDB connection to MotherDuck.
    """
    if not MD_TOKEN:
        raise RuntimeError("MOTHERDUCK_TOKEN is not set")
    return duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")

# ==========================
# Existing BT Ranking Helpers
# ==========================

def get_bt_rankings(con) -> pd.DataFrame:
    """
    Fetch top-25 Bradley-Terry snapshot with stats.
    """
    query = """
        WITH stats AS (
            SELECT
                team_id,
                games_played,
                wins,
                losses,
                win_pct,
                avg_points_scored,
                avg_points_allowed,
                point_differential,
                turnover_margin
            FROM ncaa.bt.team_stats
        )
        SELECT
          r.rank,
          t.display_name AS team_name,
          r.strength,
          r.prob_vs_avg,
          s.games_played,
          s.wins,
          s.losses,
          s.win_pct,
          s.avg_points_scored,
          s.avg_points_allowed,
          s.point_differential,
          s.turnover_margin
        FROM ncaa.bt.rankings AS r
        LEFT JOIN ncaa.real_deal.dim_teams AS t
          ON r.team_id = t.id
        LEFT JOIN stats AS s
          ON r.team_id = s.team_id
        ORDER BY r.rank ASC
        LIMIT 25
    """
    return con.execute(query).df()


def build_ranking_markdown(df: pd.DataFrame) -> str:
    cols = [
        "rank",
        "team_name",
        "strength",
        "prob_vs_avg",
        "games_played",
        "wins",
        "losses",
        "win_pct",
        "avg_points_scored",
        "avg_points_allowed",
        "point_differential",
        "turnover_margin",
    ]
    return df[cols].to_markdown(index=False)


def generate_bt_summary_text(table_md: str) -> str:
    prompt = f"""
You are a college football analytics reporter.

You are given a Bradley-Terry ranking table.  
Explain the ordering in 3–4 sentences using only normal football stats (wins/losses, scoring, point differential, turnover margin).  
Do NOT use terms like win probability or strength coefficients.

Here is the data:

{table_md}
"""

    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return resp.text.strip()

# =======================================================
# NEW SECTION: Per‑team LLM summaries using last 3 games
# =======================================================

def get_team_ranking_and_recent_games(con) -> pd.DataFrame:
    """
    Pull each team's rank + its last 3 games (scores, opponent, result).
    """
    query = """
        WITH ranked AS (
            SELECT
                r.rank,
                r.team_id,
                t.display_name AS team_name
            FROM ncaa.bt.rankings r
            JOIN ncaa.real_deal.dim_teams t ON r.team_id = t.id
        ),
        games AS (
            SELECT
                f.team_id,
                g.id AS game_id,
                g.week AS week,
                opp.display_name AS opponent_name,
                f.score AS team_score,
                f.opponent_score,
                CASE
                    WHEN f.score > f.opponent_score THEN 'Win'
                    WHEN f.score < f.opponent_score THEN 'Loss'
                    ELSE 'Tie'
                END AS result
            FROM ncaa.bt.game_stats_flat f
            JOIN ncaa.real_deal.dim_games g ON f.game_id = g.id
            JOIN ncaa.real_deal.dim_teams opp ON f.opponent_id = opp.id
        ),
        joined AS (
            SELECT
                r.rank,
                r.team_id,
                r.team_name,
                g.week,
                g.opponent_name,
                g.team_score,
                g.opponent_score,
                g.result,
                ROW_NUMBER() OVER (PARTITION BY r.team_id ORDER BY g.week DESC) AS rn
            FROM ranked r
            JOIN games g ON r.team_id = g.team_id
        )
        SELECT *
        FROM joined
        WHERE rn <= 3
        ORDER BY team_id, week DESC;
    """
    return con.execute(query).df()


def generate_team_summary(team_name: str, rank: int, recent: pd.DataFrame) -> str:
    """
    Generate a short 2–4 sentence summary based on past 3 games.
    """
    games_list = []
    for _, row in recent.iterrows():
        games_list.append(
            f"- Week {row['week']}: {team_name} {row['team_score']} — "
            f"{row['opponent_name']} {row['opponent_score']} ({row['result']})"
        )
    games_md = "\n".join(games_list)

    prompt = f"""
You are a college football analyst.

Write a summary for **{team_name}**, currently ranked #{rank}.
Base your summary ONLY on its last three games listed below.

Do NOT mention win probability, coefficients, or advanced modeling terms.
Use simple, intuitive football ideas like scoring trends, close wins, defensive performance, etc.

### Last 3 Games:
{games_md}

Write 2–4 concise sentences.
"""

    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return resp.text.strip()


def create_team_summary_table(con):
    """
    Generate LLM summaries for every team and write them into a new MotherDuck table.
    """
    df = get_team_ranking_and_recent_games(con)

    summaries = []
    for team_id in df["team_id"].unique():
        team_df = df[df.team_id == team_id]
        team_name = team_df.iloc[0]["team_name"]
        rank = int(team_df.iloc[0]["rank"])

        summary = generate_team_summary(team_name, rank, team_df)

        summaries.append({
            "team_id": team_id,
            "rank": rank,
            "summary": summary
        })

    out = pd.DataFrame(summaries)

    con.execute("""
        CREATE OR REPLACE TABLE ncaa.bt.team_summaries (
            team_id INTEGER,
            rank INTEGER,
            summary VARCHAR
        );
    """)

    con.register("tmp_df", out)
    con.execute("INSERT INTO ncaa.bt.team_summaries SELECT * FROM tmp_df;")

    return out

# =======================================================
# Cloud Function Entrypoints
# =======================================================

@functions_framework.http
def bt_llm_summary(request):
    """
    Existing endpoint: summary of top-25 BT rankings.
    """
    try:
        con = get_md_connection()

        df = get_bt_rankings(con)
        if df.empty:
            return json.dumps({"status": "error", "message": "No rankings found"}), 404

        table_md = build_ranking_markdown(df)
        summary = generate_bt_summary_text(table_md)

        return json.dumps({
            "status": "success",
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500


@functions_framework.http
def team_llm_summaries(request):
    """
    NEW endpoint: Generates per-team summaries using their last 3 games.
    """
    try:
        con = get_md_connection()
        out_df = create_team_summary_table(con)

        return json.dumps({
            "status": "success",
            "rows_written": len(out_df),
            "timestamp": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
