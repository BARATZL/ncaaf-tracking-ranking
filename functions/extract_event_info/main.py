import pandas as pd
import json
import requests
import functions-framework
from google.cloud import storage
import uuid
import datetime

project_id = 'baratz00-ba882-fall25'
bucket_name = 'ba882-ncaa-project'

# Taking from class lab
def upload_to_gcs(bucket_name, path, run_id, data):
    """Uploads data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{path}/{run_id}/data.json"
    blob = bucket.blob(blob_name)

    # Upload the data (here it's a serialized string)
    blob.upload_from_string(data)
    print(f"File {blob_name} uploaded to {bucket_name}.")

    return {'bucket_name':bucket_name, 'blob_name': blob_name}

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"

#==================================================================
#The below should source the information we want properly.

@functions_framework.http
def task(request):
    yyyymmdd = request.args.get("date")
    if not yyyymmdd:
        yyyymmdd = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    print(f"date for the job: {yyyymmdd}")  # implementing date logic for initial script from lab

    run_id = request.args.get("run_id")
    if not run_id:
        run_id = uuid.uuid4().hex[:12]
    print(f"run_id: {run_id}")  # adding run id to initial scripts.
    
    
    url = f"{SCOREBOARD_URL}?dates={yyyymmdd}"
    reponse = requests.get(url)
    data_pull = {}
    if response.ok:
        data = response.json()
        season = data['leagues'][0]['season']['year']
        week = data['events'][0]['week']['number']
        num_events = len(data.get('events')
        print("Successful. {len(event_ids)} games found.")
    else:
        raise ValueError("Non 200 response.")

    if num_events == 0:
        print("No games for date selected.")
        return {
            "num_entries": 0,
            "run_id": run_id
        }, 200
    # data_pull['event_id'] = event_ids
    # data_pull['week'] = week
    # data_pull['season'] = season   # originally returned a dictionary, instead since the function pushes data to bucket, returning info about run.
    
    j_string = json.dumps(data)
    season = data['leagues'][0]['season']['year']
    week = data['events'][0]['week']['number']
    _path = f'raw/scoreboard/season={season}/week={week}'
    gcs_path = upload_to_gcs(bucket_name, path = _path, run_id=run_id, data=j_string


    return {
        "num_entries": num_events,
        "run_id": run_id,
        "bucket_name": gcs_path.get('bucket_name'),
        "blob_name": gcs_path.get('blob_name')
    }, 200  # This return statement only shows the results of the function. The important piece is above, where the data is uploaded to the gcs bucket.
