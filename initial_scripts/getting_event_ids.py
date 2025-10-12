import requests
import pandas as pd
import json
import datetime

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"

#==================================================================
#The below should source the information we want properly.

def get_event_ids(yyyymmdd:str):
    url = f"{SCOREBOARD_URL}?dates={yyyymmdd}"
    reponse = requests.get(url)
    if response.ok:
        data = response.json()
        event_ids = [event['id'] for event in data['events']]
        if len(event_ids) < 1:
            return []
        print("Successful. {len(event_ids)} games found.")
        print(event_ids)
    else:
        raise ValueError("Non 200 response.")
    return event_ids
