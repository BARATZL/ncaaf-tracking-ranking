import pandas as pd
from datetime import datetime

def parse_rankings_data(data):
    """Parse the ESPN rankings JSON into structured DataFrames for each poll."""
    latest_season = data.get("latestSeason", {})
    latest_week = data.get("latestWeek", {})

    polls_to_extract = {
        "AP": "AP Top 25",
        "Coaches": "Coaches"
    }

    poll_dfs = {}

    for key, name in polls_to_extract.items():
        poll_data = next((r for r in data["rankings"] if name.lower() in r["name"].lower()), None)
        
        if poll_data:
            teams = []
            poll_date = datetime.strptime(poll_data["date"], "%Y-%m-%dT%H:%MZ").strftime("%Y-%m-%d")

            for t in poll_data["ranks"]:
                teams.append({
                    "season_year": latest_season.get("year", "N/A"),
                    "week_number": latest_week.get("number", "N/A"),
                    "poll_name": poll_data["shortName"],
                    "poll_date": poll_date,
                    "team_id": t["team"].get("id", None),
                    "team": t["team"].get("displayName", t["team"].get("location", t["team"].get("name"))),
                    "current_rank": t.get("current", None),
                    "previous_rank": t.get("previous", None),
                    "record": t.get("recordSummary", ""),
                    "points": t.get("points", ""),
                    "firstPlaceVotes": t.get("firstPlaceVotes", 0)
                })
            
            df = pd.DataFrame(teams)
            poll_dfs[key] = df
            print(f"✅ {key} poll parsed ({len(df)} teams).")
        else:
            print(f"⚠️ {name} not found in API response.")
    
    return poll_dfs
