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
    # Align with existing Bradley-Terry function: use motherduck_token param
    return duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")


# ==========================
# Data access helpers
# ==========================

def get_bt_rankings(con) -> pd.DataFrame:
    """
    Fetch the current Bradley-Terry snapshot from ncaa.bt.rankings,
    joined to the team dimension (for names) and a season stats table
    (for record and efficiency context).
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
    """
    Convert the top-25 rankings plus key season stats into
    a compact Markdown table for the LLM.
    """
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
    small = df[cols]
    return small.to_markdown(index=False)



# ==========================
# LLM helper
# ==========================

def generate_bt_summary_text(table_md: str) -> str:
    """
    Call Gemini to generate a short justification of the Bradley-Terry rankings
    using both model output and season statistics.
    """
    prompt = f"""
You are a college football analytics reporter.

You are given a Bradley-Terry style ranking of FBS teams.
Each row has the team's rank, name, model strength, probability versus an average team,
and key season statistics (wins, win percentage, points scored / allowed, point differential, turnover margin).

Write a short justification of these rankings in 3–4 concise sentences.

Focus on:
- what makes the top teams stand out based on strength, win_pct, and point_differential,
- any clear tiers or gaps in the top 10–15 teams,
- notable overperformers (strong BT rating + good stats) or underperformers (high or low ranking that does not fully match their stats).

Do NOT invent any stats that are not present in the table.
Refer to teams by their names when possible.

Here is the data (Markdown table):

{table_md}
"""

    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return resp.text.strip()

# ==========================
# Cloud Function entrypoint
# ==========================

@functions_framework.http
def bt_llm_summary(request):
    """
    HTTP entrypoint for the LLM-powered Bradley-Terry summary.

    Behavior:
      - Connects to MotherDuck using MOTHERDUCK_TOKEN.
      - Reads the current Bradley-Terry rankings from ncaa.bt.rankings.
      - Builds a compact table of the top 25 teams.
      - Calls Gemini to generate a short natural-language justification.
      - Returns the summary as JSON.
    """
    try:
        con = get_md_connection()

        df = get_bt_rankings(con)
        if df.empty:
            body = {
                "status": "error",
                "message": "No data found in ncaa.bt.rankings"
            }
            return json.dumps(body), 404, {"Content-Type": "application/json"}

        table_md = build_ranking_markdown(df)
        summary = generate_bt_summary_text(table_md)

        body = {
            "status": "success",
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat(),
        }
        return json.dumps(body), 200, {"Content-Type": "application/json"}

    except Exception as e:
        body = {
            "status": "error",
            "message": str(e),
        }
        return json.dumps(body), 500, {"Content-Type": "application/json"}
