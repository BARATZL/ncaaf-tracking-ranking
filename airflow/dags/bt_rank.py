from datetime import datetime
from airflow.decorators import dag, task
import requests

CF_URL = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/bradley-terry-rankings"
CF_LLM_URL = "https://bt-llm-summary-756433949230.us-central1.run.app"

@dag(
    dag_id="bt_rankings_weekly",
    schedule="0 22 * * 2", # every tuesday 10pm
    start_date=datetime(2025, 1, 1),
    catchup=False, # no back fill
    tags=["bt", "ranking", "motherduck"],
)
def bt_rankings_weekly():

    @task
    def call_bt_function():
        resp = requests.get(CF_URL, timeout=60)
        resp.raise_for_status()        # know the status
        data = resp.json()
        print(data)
        return data

    @task
    def call_bt_llm():
        resp = requests.get(CF_LLM_URL, timeout=180)
        resp.raise_for_status()        # know the status
        return {}

    bt_task = call_bt_function()
    llm_task = call_bt_llm()
    bt_task >> llm_task
        
bt_rankings_weekly()
