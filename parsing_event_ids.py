import pandas as pd
import json
import requests
from datetime import datetime

GAME_URL = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event="

def parse_event_info(event_id:str):
  url = f"{GAME_URL}{event_id}"
  response = requests.get(url)
  if response.ok:
    data = response.json()


##### TO BE CONTINUED #####
