import pandas as pd
import json
import requests
from datetime import datetime

GAME_URL = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event="

def parse_event_info(data_pull:dict):  #should come from previous function's output as currently intended.
  venues = []
  teams = []
  game_teams_stats = []
  for e in data_pull['event_ids']:  
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
