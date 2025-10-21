import json
import requests
import functions_framework
from google.cloud import storage
import uuid
import datetime

project_id = 'baratz00-ba882-fall25'
bucket_name = 'ba882-ncaa-project'

def upload_to_gcs(bucket_name, path, run_id, data):
    """Uploads data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{path}/{run_id}/data.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)
    print(f"✅ File {blob_name} uploaded to {bucket_name}.")
    return {'bucket_name': bucket_name, 'blob_name': blob_name}

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"

@functions_framework.http
def task(request):
    # Default date: UTC yesterday; log for traceability
    yyyymmdd = request.args.get("date")
    if not yyyymmdd:
        yyyymmdd = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    print(f"📅 date for the job: {yyyymmdd}")

    # Ensure a run_id is present; log it
    run_id = request.args.get("run_id") or uuid.uuid4().hex[:12]
    print(f"🆔 run_id: {run_id}")

    # Call ESPN scoreboard API
    url = f"{SCOREBOARD_URL}?dates={yyyymmdd}"
    response = requests.get(url)
    if not response.ok:
        raise ValueError(f"Non-200 response: {response.status_code}")

    # Safely load events; handle 'no games' to avoid IndexError/KeyError
    data = response.json()
    events = data.get("events", [])
    num_events = len(events)

    if num_events == 0:
        print(f"⚠️ No games for date {yyyymmdd}.")
        return {
            "num_entries": 0,
            "run_id": run_id
        }, 200

    # Only compute season/week when games exist
    season = data["leagues"][0]["season"]["year"]
    week = events[0]["week"]["number"]
    print(f"✅ Successful. {num_events} games found (season={season}, week={week}).")

    # Serialize and upload raw JSON to GCS
    j_string = json.dumps(data)
    _path = f"raw/scoreboard/season={season}/week={week}"
    gcs_path = upload_to_gcs(bucket_name, path=_path, run_id=run_id, data=j_string)

    # Return concise payload for downstream tasks
    return {
        "num_entries": num_events,
        "run_id": run_id,
        "bucket_name": gcs_path.get("bucket_name"),
        "blob_name": gcs_path.get("blob_name")
    }, 200

