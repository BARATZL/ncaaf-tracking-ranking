import functions_framework
from google.cloud import secretmanager
import duckdb

project_id = 'baratz00-ba882-fall25
secret_id = 'MotherDuck'
version_id = 'latest'


db = 'ncaa'
schema = 'real_deal'
db_schema=f'{db}.{schema}'

@functions_framework.http
def task(request):
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}')


    # now to get the real tables set up.
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")

   # venue
    raw_tbl_name = f"{db_schema}.dim_venues"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id INT
        ,fullname VARCHAR
        ,city VARCHAR
        ,country VARCHAR
        ,indoor BOOLEAN
        ,ingest_timestamp TIMESTAMP
        ,source_path VARCHAR
        ,run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # game
    raw_tbl_name = f"{db_schema}.dim_games"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id INT 
        ,start_date TIMESTAMP
        ,season INT
        ,week INT
        ,venue_id INT
        ,ingest_timestamp TIMESTAMP
        ,source_path VARCHAR
        ,run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # teams
    raw_tbl_name = f"{db_schema}.dim_teams"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id INT 
        ,name VARCHAR
        ,abbrev VARCHAR
        ,display_name VARCHAR
        ,short_name VARCHAR
        ,color VARCHAR
        ,alternate_color VARCHAR
        ,venue_id INT
        ,logo VARCHAR
        ,ingest_timestamp TIMESTAMP
        ,source_path VARCHAR
        ,run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)  

    # game_team
    raw_tbl_name = f"{db_schema}.fact_game_team"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        game_id INT 
        ,team_id INT
        ,home_away VARCHAR
        ,score INT
        ,total_yards INT
        ,third_eff FLOAT
        ,fourth_eff FLOAT
        ,yards_per_pass FLOAT
        ,yards_per_rush FLOAT
        ,turnovers INT
        ,fumbles_lost INT
        ,ints_thrown INT
        ,top INT
        ,ingest_timestamp TIMESTAMP
        ,source_path VARCHAR
        ,run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)  

    # rankings
    raw_tbl_name = f"{db_schema}.fact_rankings"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        season_year INT 
        ,week_number INT
        ,poll_name VARCHAR
        ,poll_date VARCHAR
        ,team_id INT
        ,team VARCHAR
        ,current_rank INT
        ,previous_rank INT
        ,record VARCHAR
        ,points INT
        ,firstPlaceVotes INT
        ,source_path VARCHAR
        ,run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql) 

    return {}, 200
