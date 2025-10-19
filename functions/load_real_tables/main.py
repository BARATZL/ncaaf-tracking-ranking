import functions_framework
from google.cloud import secretmanager
import duckdb

project_id = 'baratz00-ba882-fall25
secret_id = 'MotherDuck'
version_id = 'latest'

db = 'ncaa'
schema = 'real_deal'
db_schema = f'{db}.{schema}'

@functions_framework.http
def task(request):
    sm = secretmanager.SecretManagerServiceClient()
    name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # pulling from raw teams
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, name, abbrev, display_name, short_name,
        color, alternate_color, venue_id, logo,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM ncaa.raw.teams AS t
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO ncaa.real_deal.dim_teams
    SELECT
    id, name, abbrev, display_name, short_name,
    color, alternate_color, venue_id, logo,
    ingest_timestamp, source_path, run_id
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    #venue time
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, fullname, city, country, indoor,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        v.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM ncaa.raw.venues AS v
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO ncaa.real_deal.dim_venues
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # games
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, start_date, season, week, venue_id, attendance,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        g.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM ncaa.raw.games AS g
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO ncaa.real_deal.dim_games
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # game_team_stats
    """
    WITH latest AS (
    SELECT
        game_id, team_id, home_away, score, total_yards,
        third_eff, fourth_eff, yards_per_pass, yards_per_rush,
        turnovers, fumbles_lost, ints_thrown, top,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        gt.*,
        ROW_NUMBER() OVER (
            PARTITION BY game_id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM ncaa.raw.game_team AS gt
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO ncaa.real_deal.fact_game_team
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # rankings
    """
    WITH latest AS (
    SELECT
        season_year, week_number, poll_name, poll_date, team_id,
        team, current_rank, previous_rank, record, points, firstPlaceVotes,
        ingest_timestamp
    FROM (
        SELECT
        ra.*,
        ROW_NUMBER() OVER (
            PARTITION BY poll_name
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM ncaa.raw.rankings AS ra
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO ncaa.real_deal.fact_rankings
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    return {}, 200
