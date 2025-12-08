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
# Data access helpers
# ==========================

def get_top25_rankings(con) -> pd.DataFrame:
    query = """
        SELECT
            r.rank,
            r.team_id,
            t.display_name AS team_name
        FROM ncaa.bt.rankings AS r
        LEFT JOIN ncaa.real_deal.dim_teams AS t
          ON r.team_id = t.id
        ORDER BY r.rank ASC
        LIMIT 25
    """
    return con.execute(query).df()

def get_recent_game_results(con, team_id: int) -> pd.DataFrame:
    query = f"""
        SELECT
            CASE WHEN home_won = 1 THEN home_team_id ELSE away_team_id END AS winner_team_id,
            CASE WHEN home_won = 1 THEN away_team_id ELSE home_team_id END AS loser_team_id,
            home_team_id,
            away_team_id,
            home_score,
            away_score
        FROM ncaa.bt.pairwise_comparisons
        WHERE home_team_id = {team_id} OR away_team_id = {team_id}
        ORDER BY RANDOM()
        LIMIT 3
    """
    return con.execute(query).df()

def build_game_markdown(df: pd.DataFrame, team_id: int) -> str:
    rows = []
    for _, row in df.iterrows():
        if row["home_team_id"] == team_id:
            opponent = row["away_team_id"]
            result = "W" if row["home_score"] > row["away_score"] else "L"
            score_str = f"{row['home_score']}–{row['away_score']}"
        else:
            opponent = row["home_team_id"]
            result = "W" if row["away_score"] > row["home_score"] else "L"
            score_str = f"{row['away_score']}–{row['home_score']}"
        rows.append(f"- vs Team {opponent}: {result} ({score_str})")
    return "\n".join(rows)

def summarize_team(rank: int, team_name: str, team_id: int, recent_games_md: str) -> str:
    prompt = f"""
You are a sportswriter summarizing the recent performance of a college football team.

Team: {team_name} (Rank #{rank})

Below are the results of the team's past three games:
{recent_games_md}

Write a short 2–4 sentence summary of the team’s recent performance based on these results and their top 25 ranking.
Use plain language. Do not mention win probabilities or strength coefficients.

Be clear about how the team's rank is reflected in their wins or losses. Do not make up any information.
"""
    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return resp.text.strip()

# ==========================
# HTTP Function Entry Point
# ==========================

@functions_framework.http
def bt_llm_summary(request):
    try:
        con = get_md_connection()

        top25_df = get_top25_rankings(con)
        summaries = []

        for _, row in top25_df.iterrows():
            team_id = row["team_id"]
            rank = row["rank"]
            team_name = row["team_name"]
            recent_games = get_recent_game_results(con, team_id)
            if recent_games.empty:
                continue
            recent_md = build_game_markdown(recent_games, team_id)
            summary_text = summarize_team(rank, team_name, team_id, recent_md)
            summaries.append((team_id, rank, summary_text))

        result_df = pd.DataFrame(summaries, columns=["team_id", "rank", "summary"])
        con.execute("CREATE TABLE IF NOT EXISTS ncaa.bt.team_summaries (team_id INT, rank INT, summary STRING)")
        con.execute("DELETE FROM ncaa.bt.team_summaries")
        con.execute("INSERT INTO ncaa.bt.team_summaries SELECT * FROM result_df")

        return json.dumps({
            "status": "success",
            "message": "LLM summaries generated and written to ncaa.bt.team_summaries",
            "timestamp": datetime.utcnow().isoformat()
        }), 200, {"Content-Type": "application/json"}

    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": str(e),
        }), 500, {"Content-Type": "application/json"}
