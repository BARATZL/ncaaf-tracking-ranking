##initial foundation for eventual service function.

import requests
import pandas as pd
import json
import datetime
## import functions_framework
## from google.cloud import storage
## import uuid

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"

#==================================================================
#The below should source the information we want properly.

# This returns a list of event_ids for a given date with games.
def get_event_ids(yyyymmdd:str):
    url = f"{SCOREBOARD_URL}?dates={yyyymmdd}"
    reponse = requests.get(url)
    if response.ok:
        data = response.json()
        event_ids = [event['id'] for event in data['events']]
        if len(event_ids) < 1:
            print("No games found on this day. Or something else went wrong. Who's to say?")
            return []
        print("Successful. {len(event_ids)} games found.")
        print(event_ids)
    else:
        raise ValueError("Non 200 response.")
    return event_ids

# What's missing from this function is code to help it run more automated.
# For example, more code can put the list of event_ids in a GCS folder for the corresponding season/week.
# We can refer to class labs to incorporate the above notes here.
