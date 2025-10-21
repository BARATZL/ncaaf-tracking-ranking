import functions_framework
import json
import uuid
from datetime import datetime
import duckdb
import pandas as pd
import requests
import io
from google.cloud import secretmanager
from google.cloud import storage

# ===== Global Config =====
project_id = 'baratz00-ba882-fall25'
secret_id = 'MotherDuck'
version_id = 'latest'
bucket_name = 'ba882-ncaa-project'

db = 'ncaa'
schema = 'raw'
db_schema = f'{db}.{schema}'


@functions_framework.http
def task(request):
    """Cloud Function entry point to fetch and store NCAA football ranking data."""
    print("🚀 Starting ESPN Ranking Pipeline")

    # --- 1️⃣ Connect to MotherDuck and GCP services ---
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": secret_name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f"md:?motherduck_token={md_token}") 

    # --- 2️⃣ Generate unique run ID ---
    run_id = request.args.get("run_id") or uuid.uuid4().hex[:12]
    print(f"Run ID: {run_id}")

    # --- 3️⃣ Fetch ESPN Rankings API ---
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings"
    response = requests.get(url)
    if not response.ok:
        raise Exception(f"❌ API error: {response.status_code}")
    print("✅ ESPN API connection successful.")

    data = response.json()

    # --- 4️⃣ Parse ranking data ---
    latest_season = data.get("latestSeason", {})
    latest_week = data.get("latestWeek", {})

    polls_to_extract = {
        "AP": "AP Top 25",
        "Coaches": "Coaches"
    }

    ingest_ts_str = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    poll_dfs = {}

    for key, name in polls_to_extract.items():
        poll_data = next((r for r in data["rankings"] if name.lower() in r["name"].lower()), None)
        if poll_data:
            teams = []
            try:
                poll_date = datetime.strptime(poll_data["date"], "%Y-%m-%dT%H:%MZ").strftime("%Y-%m-%d")
            except Exception:
                poll_date = datetime.utcnow().strftime("%Y-%m-%d")  # fallback for missing date

            for t in poll_data.get("ranks", []):
                teams.append({
                    "season_year": latest_season.get("year", "N/A"),
                    "week_number": latest_week.get("number", "N/A"),
                    "poll_name": poll_data.get("shortName", name),
                    "poll_date": poll_date,
                    "team_id": t.get("team", {}).get("id"),
                    "team": t.get("team", {}).get("displayName") or t.get("team", {}).get("location") or t.get("team", {}).get("name"),
                    "current_rank": t.get("current"),
                    "previous_rank": t.get("previous"),
                    "record": t.get("recordSummary", ""),
                    "points": t.get("points", ""),
                    "firstPlaceVotes": t.get("firstPlaceVotes", 0),
                    "ingest_timestamp": ingest_ts_str,
                    "run_id": run_id
                })

            df = pd.DataFrame(teams)
            poll_dfs[key] = df
            print(f"✅ Parsed {key} poll ({len(df)} teams).")
        else:
            print(f"⚠️ Poll '{name}' not found in API response.")

    if not poll_dfs:
        print("⚠️ No polls found — exiting.")
        return {"status": "no_polls_found", "run_id": run_id}, 200

    # --- 5️⃣ Upload results to GCS ---
    today = datetime.utcnow().strftime("%Y-%m-%d")
    bucket = storage_client.bucket(bucket_name)

    for poll_name, df in poll_dfs.items():
        season = str(df["season_year"].iloc[0]) if "season_year" in df.columns else "unknown"
        week = str(df["week_number"].iloc[0]) if "week_number" in df.columns else "unknown"
        file_name = f"{poll_name.lower()}_poll_{today}.parquet"
        gcs_path = f"raw/rankings/season={season}/week={week}/{file_name}"

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        blob = bucket.blob(gcs_path)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
        print(f"📤 Uploaded {poll_name} ranking to gs://{bucket_name}/{gcs_path}")

    # --- 6️⃣ Insert into MotherDuck (Fixed Binder Error) ---
    expected_cols = [
        "season_year",
        "week_number",
        "poll_name",
        "poll_date",
        "team_id",
        "team",
        "current_rank",
        "previous_rank",
        "record",
        "points",
        "firstPlaceVotes",
        "ingest_timestamp",
    ]

    table_name = f"{db_schema}.rankings"

    for poll_name, df in poll_dfs.items():
        df2 = df.copy()

        # Ensure all expected columns exist
        for c in expected_cols:
            if c not in df2.columns:
                df2[c] = pd.NA

        # Drop unexpected columns (e.g., run_id)
        df2 = df2[expected_cols]

        # Type conversion to avoid binding issues
        df2["team_id"] = pd.to_numeric(df2["team_id"], errors="coerce").astype("Int64")
        df2["current_rank"] = pd.to_numeric(df2["current_rank"], errors="coerce").astype("Int64")
        df2["previous_rank"] = pd.to_numeric(df2["previous_rank"], errors="coerce").astype("Int64")
        df2["points"] = pd.to_numeric(df2["points"], errors="coerce").astype("Int64")
        df2["firstPlaceVotes"] = pd.to_numeric(df2["firstPlaceVotes"], errors="coerce").astype("Int64")

        # Register and insert into MotherDuck
        md.register("temp_df", df2)
        md.sql(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        print(f"🦆 Inserted {poll_name} rankings into MotherDuck table {table_name} (rows={len(df2)})")

    # --- 7️⃣ Success response ---
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    result = {
        "status": "success",
        "run_id": run_id,
        "timestamp": timestamp,
        "polls_processed": list(poll_dfs.keys())
    }

    print(f"✅ Ranking ingestion completed at {timestamp}")
    return (json.dumps(result), 200, {"Content-Type": "application/json"})
