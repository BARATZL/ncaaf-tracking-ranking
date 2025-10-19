import functions_framework
import json
import uuid
from datetime import datetime
import duckdb
from google.cloud import secretmanager
from google.cloud import storage
# 匯入你的自定義模組
from functions.fetch_rankings import fetch_rankings_from_espn
from functions.parse_rankings import parse_rankings_data
from functions.export_rankings import export_rankings_to_file

project_id = 'baratz00-ba882-fall25'
secret_id = 'MotherDuck'
version_id = 'latest'
db = 'ncaa'
schema = 'raw'
db_schema = f'{db}.{schema}'

@functions_framework.http
def task(request):
    """
    Cloud Function entry point.
    Fetches, parses, and exports ESPN ranking data automatically.
    """
    print("🚀 Starting ESPN Ranking Pipeline")
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": secret_name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 
    
    # --- 1️⃣ 產生唯一 run ID ---
    run_id = request.args.get("run_id") or uuid.uuid4().hex[:12]
    print(f"Run ID: {run_id}")

    # --- 2️⃣ 抓取資料 ---
    try:
        data = fetch_rankings_from_espn()
        print("✅ Successfully fetched data from ESPN API")
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return (json.dumps({"status": "failed", "error": str(e)}), 500)

    # --- 3️⃣ 處理資料 ---
    try:
        poll_dfs = parse_rankings_data(data)
        print("✅ Rankings parsed successfully")
    except Exception as e:
        print(f"❌ Error parsing data: {e}")
        return (json.dumps({"status": "failed", "error": str(e)}), 500)

    # --- 4️⃣ 匯出到 GCS 或本地 ---
    try:
        export_rankings_to_file(poll_dfs)
        print("✅ Rankings exported successfully")
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        return (json.dumps({"status": "failed", "error": str(e)}), 500)

    # --- 4️⃣.5️⃣ 上傳MD ---
    
    print(f"appending rows to raw/rankings")
    tbl = db_schema + ".rankings"
    md.execute(f"INSERT INTO {tbl} SELECT * FROM polls_df")
    
    # --- 5️⃣ 成功回傳 ---
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return (
        json.dumps({
            "status": "success",
            "run_id": run_id,
            "timestamp": timestamp,
            "polls_processed": list(poll_dfs.keys())
        }),
        200,
        {"Content-Type": "application/json"}
    )
