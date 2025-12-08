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
    """
    Create a DuckDB connection to MotherDuck using the MotherDuck token.
    """
    if not MD_TOKEN:
        raise RuntimeError("MOTHERDUCK_TOKEN is not set")
    return duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")


# ==========================
# Data access helpers
# ==========================

def get_top25_rankings(con) -> pd.DataFrame:
    """
    Get the current Bradley-Terry rankings (top 25) with team names.
    """
    query = """
        SELECT
            r.rank,
            r.team_id,
            t.display_name AS team_name
        FROM ncaa.bt.rankings AS r
        LEFT JOIN ncaa.real_deal.dim_teams AS t
          ON r.team_id = t.id
        WHERE r.rank <= 25
        ORDER BY r.rank ASC
    """
    return con.execute(query).df()


def get_recent_game_results(con, team_id: int) -> pd.DataFrame:
    """
    Pull a random sample of up to 3 recent games involving this team
    from the pairwise_comparisons table, with opponent names.
    NOTE: we avoid using alias name 'at' because it causes a parser issue.
    """
    query = f"""
        SELECT
            pc.home_team_id,
            pc.away_team_id,
            pc.home_score,
            pc.away_score,
            pc.home_won,
            ht.display_name   AS home_team_name,
            awt.display_name  AS away_team_name
        FROM ncaa.bt.pairwise_comparisons AS pc
        LEFT JOIN ncaa.real_deal.dim_teams AS ht
          ON pc.home_team_id = ht.id
        LEFT JOIN ncaa.real_deal.dim_teams AS awt
          ON pc.away_team_id = awt.id
        WHERE pc.home_team_id = {team_id} OR pc.away_team_id = {team_id}
        ORDER BY RANDOM()
        LIMIT 3
    """
    return con.execute(query).df()


def build_game_markdown(df: pd.DataFrame, team_id: int) -> str:
    """
    Turn recent game rows into a simple bullet list for the LLM.
    """
    rows = []
    for _, row in df.iterrows():
        if row["home_team_id"] == team_id:
            opponent_name = row["away_team_name"]
            result = "W" if row["home_score"] > row["away_score"] else "L"
            score_str = f"{row['home_score']}–{row['away_score']}"
        else:
            opponent_name = row["home_team_name"]
            result = "W" if row["away_score"] > row["home_score"] else "L"
            score_str = f"{row['away_score']}–{row['home_score']}"
        rows.append(f"- vs {opponent_name}: {result} ({score_str})")
    return "\n".join(rows)


def summarize_team(rank: int, team_name: str, team_id: int, recent_games_md: str) -> str:
    """
    Call Gemini to summarize this team's recent performance and rank.
    """
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
    """
    HTTP entrypoint for generating per-team LLM summaries for the
    top 25 Bradley-Terry ranked teams and writing them to
    ncaa.bt.team_summaries in MotherDuck.

    We always (re)create ncaa.bt.team_summaries with schema:
      team_id INT,
      rank   INT,
      summary STRING
    """
    try:
        con = get_md_connection()

        top25_df = get_top25_rankings(con)
        summaries = []

        for _, row in top25_df.iterrows():
            team_id = int(row["team_id"])
            rank = int(row["rank"])
            team_name = row["team_name"]

            recent_games = get_recent_game_results(con, team_id)
            if recent_games.empty:
                continue

            recent_md = build_game_markdown(recent_games, team_id)
            summary_text = summarize_team(rank, team_name, team_id, recent_md)

            # Store in logical order: team_id, rank, summary
            summaries.append((team_id, rank, summary_text))

        if not summaries:
            body = {
                "status": "error",
                "message": "No summaries generated (no games found for top 25 teams).",
            }
            return json.dumps(body), 404, {"Content-Type": "application/json"}

        # DataFrame columns must match desired table schema
        result_df = pd.DataFrame(summaries, columns=["team_id", "rank", "summary"])

        # Register the DataFrame as a DuckDB table and replace the target table
        con.register("result_df", result_df)

        # This both clears old summaries AND fixes any old wrong schema/order
        con.execute("""
            CREATE OR REPLACE TABLE ncaa.bt.team_summaries AS
            SELECT team_id, rank, summary
            FROM result_df
        """)

        body = {
            "status": "success",
            "message": "LLM summaries generated and written to ncaa.bt.team_summaries",
            "timestamp": datetime.utcnow().isoformat(),
        }
        return json.dumps(body), 200, {"Content-Type": "application/json"}

    except Exception as e:
        body = {
            "status": "error",
            "message": str(e),
        }
        return json.dumps(body), 500, {"Content-Type": "application/json"}
