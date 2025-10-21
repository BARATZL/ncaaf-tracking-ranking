from airflow.decorators import dag, task
from datetime import timedelta
from airflow.operators.python import get_current_context
import requests
import pendulum

def invoke_function(url, params=None) -> dict:
    resp = requests.get(url, params=params or {})
    resp.raise_for_status()
    return resp.json()

def build_ncaa_raw_pipeline_tasks():
    @task
    def schema():
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/schema-setup"
        return invoke_function(url)

    @task
    def extract_event_info(payload: dict) -> dict:
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/extract_event_info"
        ctx = get_current_context()
        process_date = (ctx["data_interval_end"] - timedelta(days=1)).strftime("%Y%m%d")
        payload["run_id"] = ctx["dag_run"].run_id
        payload["date"] = process_date
        return invoke_function(url, params=payload)

    @task
    def parsing_sb_g_info(payload: dict) -> dict:
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/parsing_sb_g_info"
        ctx = get_current_context()
        payload["run_id"] = ctx["dag_run"].run_id
        payload["date"] = ctx["ds_nodash"]
        return invoke_function(url, params=payload)

    @task
    def ranking(payload: dict) -> dict:
        url = "https://us-central1-baratz00-ba882-fall25.cloudfunctions.net/ranking"
        ctx = get_current_context()
        payload["run_id"] = ctx["dag_run"].run_id
        payload["date"] = ctx["ds_nodash"]
        return invoke_function(url, params=payload)

    s = schema()
    e = extract_event_info(s)
    p = parsing_sb_g_info(e)
    r = ranking(p)
    return r

# setting time zone and backfill start time
LOCAL_TZ = pendulum.timezone("America/New_York")
START = pendulum.datetime(2025, 8, 21, 0, 0, tz=LOCAL_TZ)  # Backfill since first week of season

# ---------- dag1：Sat. 10:30 a.m. ----------
@dag(
    schedule="30 10 * * 6",   # 10:30
    start_date=START,
    catchup=True,             # start backfill
    max_active_runs=1,
    tags=["ncaa", "raw", "ingest", "sat-1030am"],
)
def ncaa_raw_pipeline_sat_1030am():
    build_ncaa_raw_pipeline_tasks()

# ---------- dag 2：Sum 6:00 p.m. ----------
@dag(
    schedule="0 18 * * 0",    # 18:00
    start_date=START,
    catchup=True,             # start backfill
    max_active_runs=1,
    tags=["ncaa", "raw", "ingest", "sun-6pm"],
)
def ncaa_raw_pipeline_sun_6pm():
    build_ncaa_raw_pipeline_tasks()

sat_dag = ncaa_raw_pipeline_sat_1030am()
sun_dag = ncaa_raw_pipeline_sun_6pm()
