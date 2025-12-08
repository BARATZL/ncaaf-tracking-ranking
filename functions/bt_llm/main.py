import os
import json
from datetime import datetime

import duckdb
import pandas as pd
from google import genai
import functions_framework

# ==========================
# Vertex AI / Gemini config
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
    if not MD_TOKEN:
        raise RuntimeError("MOTHERDUCK_TOKEN is not set")
    return duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")

# ==========================
# Fetch rankings + 3 recent games per team
# ==========================

def get_recent_games_with_rankings(con) -> pd.DataFrame:
    query = """
        WITH exploded_games AS (
            SELECT
                game_id,
                home_team_id AS team_id,
                away_team_id AS opponent_id,
                home_score AS team_score,
                away_score AS opponent_score,
                CASE
                    WHEN home_score > away_score THEN 'W'
                    WHEN home_score < away_score THEN 'L'
                    ELSE 'T'
                END AS result
            FROM ncaa.bt.pairwise_comparisons

            UNION ALL

            SELECT
                game_id,
                away_team_id AS team_id,
                home_team_id AS opponent_id,
                away_score AS team_score,
                home_score AS opponent_score,
                CASE
                    WHEN away_score > home_score THEN 'W'
                    WHEN away_score < home_score THEN 'L'
                    ELSE 'T'
                END AS result
            FROM ncaa.bt.pairwise_comparisons
        ),
        ranked AS (
            SELECT
                r.rank,
                r.team_id,
                t.display_name AS team_name
            FROM ncaa.bt.rankings r
            JOIN ncaa.real_deal.dim_teams t ON r.team_id = t.id
        ),
        with_recent AS (
            SELECT
                r.rank,
                r.team_id,
                r.team_name,
                e.game_id,
                e.opponent_id,
                e.team_score,
                e.opponent_score,
                e.result,
                ROW_NUMBER() OVER (PARTITION BY e.team_id ORDER BY e.game_id DESC) AS rn
            FROM exploded_games e
            JOIN ranked r ON e.team_id = r.team_id
        )
        SELECT *
        FROM with_recent
        WHERE rn <= 3
        ORDER BY team_id, game_id DESC;
    """
    return con.execute(query).df()

# ==========================
# Gemini per-team prompt
# ==========================

def summarize_team(team_name: str, rank: int, recent_games: pd.DataFrame) -> str:
    game_rows = []
    for _, row in recent_games.iterrows():
        game_rows.append(
            f"- Game {row['game_id']}: Team {row['team_id']} {row['team_score']} vs Team {row['opponent_id']} {row['opponent_score']} ({row['result']})"
        )
    games_md = "\n".join(game_rows)

    prompt = f"""
You are a college football analyst.

Write a short summary for the team **{team_name}**, which is currently ranked #{rank}.
Use only the last 3 games shown below.

Avoid technical terms like win probability or strength coefficients.
Focus instead on clear, human-understandable insights like wins, losses, scoring trends, and margins.

### Last 3 Games:
{games_md}

Write 2–4 concise sentences.
"""
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text.strip()

# ==========================
# Generate all summaries + write to MotherDuck
# ==========================

def generate_all_team_summaries(con):
    df = get_recent_games_with_rankings(con)
    summaries = []

    for team_id in df['team_id'].unique():
        team_df = df[df['team_id'] == team_id]
        if team_df.empty:
            continue

        team_name = team_df.iloc[0]["team_name"]
        rank = team_df.iloc[0]["rank"]
        summary = summarize_team(team_name, rank, team_df)

        summaries.append({
            "team_id": int(team_id),
            "rank": int(rank),
            "summary": summary
        })

    result_df = pd.DataFrame(summaries)
    con.execute("CREATE OR REPLACE TABLE ncaa.bt.team_summaries AS SELECT * FROM result_df")
    print("✅ Wrote summaries to ncaa.bt.team_summaries")

# ==========================
# Cloud Function Entrypoint
# ==========================

@functions_framework.http
def bt_llm_summary(request):
    try:
        con = get_md_connection()
        generate_all_team_summaries(con)
        return json.dumps({
            "status": "success",
            "message": "Team summaries generated and saved.",
            "timestamp": datetime.utcnow().isoformat(),
        }), 200, {"Content-Type": "application/json"}

    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": str(e),
        }), 500, {"Content-Type": "application/json"}
