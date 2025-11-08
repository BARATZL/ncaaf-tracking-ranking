from airflow.decorators import dag, task
from datetime import timedelta
from datetime import datetime
from airflow.operators.python import get_current_context
import requests
import pendulum

# ---------- Helper ----------
def invoke_function(url, params=None) -> dict:
    resp = requests.get(url, params=params or {})
    resp.raise_for_status()
    return resp.json()

# ---------- Shared Tasks ----------
def build_ncaa_real_pipeline_tasks():

    @task
    def ranking_recheck():
        """taking cfp inputs which pop up on tuesday"""
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/ranking"
        return invoke_function(url)
    
    def real_schema():
        """create real_deal schema"""
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/real_schema"
        return invoke_function(url)

    @task
    def load_real_table(payload: dict) -> dict:
        """from raw layer load latest real_deal"""
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/load_real_tables"
        ctx = get_current_context()
        payload["run_id"] = ctx["dag_run"].run_id
        payload["date"] = ctx["ds_nodash"]
        return invoke_function(url, params=payload)

    ranking_recheck()
    s = real_schema()
    l = load_real_table(s)
    return l

# ---------- Global Config ----------
LOCAL_TZ = pendulum.timezone("America/New_York")
START = datetime(2024, 1, 1)  # NCAA starting week

# ---------- DAG #1：Tuesday 8:30 PM ----------
@dag(
    schedule="30 20 * * 2",   # Sun 8:30 p.m.
    start_date=START,
    catchup=False,             #  backfill
    max_active_runs=1,
    tags=["ncaa", "real", "transform"],
)
def ncaa_real_pipeline():
    build_ncaa_real_pipeline_tasks()

# ---------- Instantiate DAGs ----------
sun_dag = ncaa_real_pipeline()
