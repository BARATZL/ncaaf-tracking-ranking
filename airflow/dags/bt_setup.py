from datetime import datetime
from airflow.sdk import dag, task    
from pathlib import Path
import duckdb
import os
from ncaaf import utils

# paths, as the airflow project is a project we deploy to astronomer
BASE_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
SQL_DIR = BASE_DIR / "include" / "sql"

@dag(
    schedule="30 9 * * 2",
    start_date=datetime(2025, 11, 6),
    catchup=False,
    tags=["bt", "setup"]
)
def bt_setup_and_agg():

    @task
    def setup_schema():
        s = utils.read_sql(SQL_DIR / "bt_schema.sql")
        utils.run_execute(s)

    @task
    def run_the_deal():
        s = utils.read_sql(SQL_DIR / "update_bt.sql")
        utils.run_execute(s)
    
    @task
    def pairwise():
        t = utils.read_sql(SQL_DIR / "pairwise_history.sql")
        utils.run_execute(t)

    setup_schema() >> run_the_deal() >> pairwise()

bt_setup_and_agg()
