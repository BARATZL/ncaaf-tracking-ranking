import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import duckdb
import pandas as pd
import json

project_id = 'baratz00-ba882-fall25'
secret_id = 'MotherDuck'
version_id = 'latest'

db = 'ncaa'
schema = 'raw'
db_schema = f'{db}.{schema}'


@functions_framework.http
def task(request):
  sm = secretmanager.SecretManagerServiceClient()
  storage_client = storage.Client()
  secret_name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'

  response = sm.access_secret_version(request={"name": secret_name})
  md_token = response.payload.data.decode("UTF-8")

  md = duckdb.connect(f'md:?motherduck_token={md_token}')

  # if there aren't any records to process, exit
  num_entries = request.args.get("num_entries")
  print(f"num_entries = {num_entries}")
  if int(num_entries) == 0:
      print("no entries found for the date evaluated downsream - EXITING")
      return {}, 200

  bucket_name = request.args.get("bucket_name", "ba882-ncaa-project")
    bucket = storage_client.bucket(bucket_name)
    blob_name = request.args.get("blob_name")
    blob = bucket.blob(blob_name)
    data_str = blob.download_as_text()
    j = json.loads(data_str)
    print(f"number of entries parsed: {len(j.get('events'))} ==========")

  events = j.get('events')
  event_ids = []
  for e in events:     # Need to consider how to iterate here. TBC
    game_id = e.get('id')
    event_ids.append(game_id)  
  games = []
  venues = []
  teams = []
  game_teams_stats = []
  ingested_ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
 
    url = f"{GAME_URL}{e}"
    response = requests.get(url)
    if response.ok:
      print("Successful Response!")
      data = response.json()
    else:
      print("Issue with accessing API, non-valid response.")
    #### Team logic here #####
    
    #### Venue logic here? ####

    #### game team logic ###
    game_teams_stats.append({
        'event_id':e,
        'team':data['boxscore']['teams'][0]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
        #now for stats
        'total_yards':int(data['boxscore']['teams'][0]['statistics'][3]['displayValue']),
        'third_eff':float(data['boxscore']['teams'][0]['statistics'][1]['value']),
        'fourth_eff':float(data['boxscore']['teams'][0]['statistics'][2]['value']),
        'yards_per_pass':float(data['boxscore']['teams'][0]['statistics'][6]['value']),
        'yards_per_rush':float(data['boxscore']['teams'][0]['statistics'][9]['value']),
        'turnovers':int(data['boxscore']['teams'][0]['statistics'][11]['displayValue']),
        'fumbles_lost':int(data['boxscore']['teams'][0]['statistics'][12]['value']),
        'ints_thrown':int(data['boxscore']['teams'][0]['statistics'][13]['value']),
        'top':int(data['boxscore']['teams'][0]['statistics'][14]['value'])  # how long did the team hold onto the ball?
        })

    # repeating for second team.
     game_teams_stats.append({
        'event_id':e 
        'team':data['boxscore']['teams'][1]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
        #now for stats
        'total_yards':int(data['boxscore']['teams'][1]['statistics'][3]['displayValue']),
        'third_eff':float(data['boxscore']['teams'][1]['statistics'][1]['value']),
        'fourth_eff':float(data['boxscore']['teams'][1]['statistics'][2]['value']),
        'yards_per_pass':float(data['boxscore']['teams'][1]['statistics'][6]['value']),
        'yards_per_rush':float(data['boxscore']['teams'][1]['statistics'][9]['value']),
        'turnovers':int(data['boxscore']['teams'][1]['statistics'][11]['displayValue']),
        'fumbles_lost':int(data['boxscore']['teams'][1]['statistics'][12]['value']),
        'ints_thrown':int(data['boxscore']['teams'][1]['statistics'][13]['value']),
        'top':int(data['boxscore']['teams'][1]['statistics'][14]['value'])  # how long did the team hold onto the ball?
        })

  gts = pd.DataFrame(game_teams_stats)

  #when transitioning this to service function, need full path to export to gcs bucket.
  
  gts.to_parquet('path_needed', index=False)
