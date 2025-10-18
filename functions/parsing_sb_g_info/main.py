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
      print("no entries found for the date evaluated downstream - EXITING")
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
  games = []
  venues = []
  team = []
  game_teams_stats = []
  ingested_ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

  for e in events:     # Need to consider how to iterate here. TBC
    print(e)
    game_id = e.get('id')
    event_ids.append(game_id)  
    start_date = pd.to_datetime(e.get("date"), urc=True).tz_localize(None)
    season = e['season']['year']
    week = e['week']['number']
    venue = e['competitions'][0]['venue']
    teams = e['competitions'][0]['competitors']
    attendance = e['competitions'][0]['attendance']
    source_path = bucket_name + blob_name
    run_id = request.args.get('run_id')

  # games themselves
    games.append(
            {
                'game_id': game_id,
                'start_date': start_date,
                'season': season,
                'week': week,
                'venue_id': venue['id'],
                'attendance': attendance,
                'ingest_timestamp': ingest_ts_str,
                'source_path': source_path,
                'run_id': run_id
            }
        )
    
  # venues
    venues.append(
        {
            'id': venue['id'],
            'fullname': venue['fullName'],
            'city': venue['address']['city'],
            'country': venue['address']['country'],
            'indoor': venue['indoor'],
            'ingest_timestamp': ingest_ts_str,
            'source_path': source_path,
            'run_id': run_id
        }
    )
    
    # teams
    teams_df = teams_parsed.copy()
    team_cols = ['id', 
                    'team.name', 
                    'team.abbreviation',
                    'team.displayName',
                    'team.shortDisplayName',
                    'team.color',
                    'team.alternateColor',
                    'team.venue.id',
                    'team.logo']
    teams_df = teams_df[team_cols]
    rename_mapper = {
        'team.name':'name',
        'team.displayName': 'display_name',
        'team.shortDisplayName': 'short_name',
        'team.color': 'color',
        'team.alternateColor': 'alternate_color',
        'team.venue.id': 'venue_id',
        'team.logo':'logo'
    }
    teams_df = teams_df.rename(columns=rename_mapper)
    teams_df['ingest_timestamp'] = ingest_ts_str
    teams_df['source_path'] = source_path
    teams_df['run_id'] = run_id
    team.append(teams_df)
    
    # for individual game stats, pulling second api call.
    url = f"{GAME_URL}{game_id}"
    response = requests.get(url)
    if response.ok:
      print("Successful Response!")
      data = response.json()
    else:
      print("Issue with accessing API, non-valid response.")
    
    #### team-level stats per game logic ###
    game_teams_stats.append({
        'event_id':game_id,
        'team':data['boxscore']['teams'][0]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
        'home_away':'Away'
        #now for stats
        'total_yards':int(data['boxscore']['teams'][0]['statistics'][3]['displayValue']),
        'third_eff':float(data['boxscore']['teams'][0]['statistics'][1]['value']),
        'fourth_eff':float(data['boxscore']['teams'][0]['statistics'][2]['value']),
        'yards_per_pass':float(data['boxscore']['teams'][0]['statistics'][6]['value']),
        'yards_per_rush':float(data['boxscore']['teams'][0]['statistics'][9]['value']),
        'turnovers':int(data['boxscore']['teams'][0]['statistics'][11]['displayValue']),
        'fumbles_lost':int(data['boxscore']['teams'][0]['statistics'][12]['value']),
        'ints_thrown':int(data['boxscore']['teams'][0]['statistics'][13]['value']),
        'top':int(data['boxscore']['teams'][0]['statistics'][14]['value']),  # how long did the team hold onto the ball?
        'ingest_timestamp':ingest_ts_str,
        'source_path':source_path,
        'run_id':run_id
        })

    # repeating for second team.
     game_teams_stats.append({
        'event_id':e 
        'team':data['boxscore']['teams'][1]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
        'home_away':'Home'   # hard coding the second team to be home team, following T1 @ T2 format of most sports promotions.
        'total_yards':int(data['boxscore']['teams'][1]['statistics'][3]['displayValue']),
        'third_eff':float(data['boxscore']['teams'][1]['statistics'][1]['value']),
        'fourth_eff':float(data['boxscore']['teams'][1]['statistics'][2]['value']),
        'yards_per_pass':float(data['boxscore']['teams'][1]['statistics'][6]['value']),
        'yards_per_rush':float(data['boxscore']['teams'][1]['statistics'][9]['value']),
        'turnovers':int(data['boxscore']['teams'][1]['statistics'][11]['displayValue']),
        'fumbles_lost':int(data['boxscore']['teams'][1]['statistics'][12]['value']),
        'ints_thrown':int(data['boxscore']['teams'][1]['statistics'][13]['value']),
        'top':int(data['boxscore']['teams'][1]['statistics'][14]['value']),  # how long did the team hold onto the ball?
        'ingest_timestamp':ingest_ts_str,
        'source_path':source_path,
        'run_id':run_id})

  #dataframing and shipping to the bucket. following practice in lab of processing the timestamps generated above.
  games_df = pd.DataFrame(games)
  games_df['ingest_timestamp'] = pd.to_datetime(games_df['ingest_timestamp'], errors='coerce')
  venues_df = pd.DataFrame(venues)
  venues_df['ingest_timestamp'] = pd.to_datetime(venues_df['ingest_timestamp'], errors='coerce')
  teams_df = pd.concat(team)
  teams_df = teams_df.dropna(subset='id')
  teams_df['ingest_timestamp'] = pd.to_datetime(teams_df['ingest_timestamp'], errors='coerce')
  gts_df = pd.DataFrame(game_teams_stats)
  gts_df['ingest_timestamp'] = pd.to_datetime(gts_df['ingest_timestamp'], errors='coerce')

  #need full path to export to gcs bucket. putting into two places, one for use, others for observability's sake/debugging
  gcs_path = 'gs://ba882-ncaa-project/raw'
  full_path = gcs_path + f'/games/season={season}/week={week}/data.parquet'
  games_df.to_parquet(full_path, index = False)
  full_path_run = gcs_path + f"/games/season={season}/week={week}/run_id={run_id}/data.parquet"
  games_df.to_parquet(full_path_run, index=False)
  
  full_path = gcs_path + f"/venues/season={season}/week={week}/data.parquet"
  venues_df.to_parquet(full_path, index=False)
  full_path_run = gcs_path + f"/venues/season={season}/week={week}/run_id={run_id}/data.parquet"
  venues_df.to_parquet(full_path_run, index=False)

  full_path = gcs_path + f"/teams/season={season}/week={week}/data.parquet"
  teams_df.to_parquet(full_path, index=False)
  full_path_run = gcs_path + f"/teams/season={season}/week={week}/run_id={run_id}/data.parquet"
  teams_df.to_parquet(full_path_run, index=False)

  full_path = gcs_path + f'gt_stats/season={season}/week={week}/data.parquet'
  gts_df.to_parquet(full_path, index = False)
  full_path_run = gcs_path + f'gt_stats/season={season}/week={week}/run_id={run_id}/data.parquet'
  gts_df.to_parquet(full_path_run, index = False)

  print(f"appending rows to raw/games")
  tbl = db_schema + ".games"
  md.execute(f"INSERT INTO {tbl} SELECT * FROM games_df")

  print(f"appending rows to raw/venues")
  tbl = db_schema + ".venues"
  md.execute(f"INSERT INTO {tbl} SELECT * FROM venues_df")

  print(f"appending rows to raw/teams")
  tbl = db_schema + ".teams"
  md.execute(f"INSERT INTO {tbl} SELECT * FROM teams_df")

  print(f"appending rows to raw/game teams")
  tbl = db_schema + ".game_team"
  md.execute(f"INSERT INTO {tbl} SELECT * FROM gt_df")
  

  return {}, 200  # since no function for parsing games, this can be an empty dictionary returned.
