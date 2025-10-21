import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import duckdb
import pandas as pd
import json
import requests  

# ===== 基本設定 =====
project_id = 'baratz00-ba882-fall25'
secret_id = 'MotherDuck'
version_id = 'latest'
bucket_name = "ba882-ncaa-project"

db = 'ncaa'
schema = 'raw'
db_schema = f'{db}.{schema}'

GAME_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=" 

def safe_cast(value, to_type=float, default=0):   # important for scoreboard piece.

    if value in ('-', None, ''):
        return default
    try:
        return to_type(value)
    except (ValueError, TypeError):
        return default


# ======================================================
@functions_framework.http
def task(request):
    # --- init ---
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()
    secret_name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'

    # from Secret Manager extract MotherDuck token
    response = sm.access_secret_version(request={"name": secret_name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # --- validate params ---
    num_entries = request.args.get("num_entries")
    print(f"num_entries = {num_entries}")
    if not num_entries or int(num_entries) == 0:
        print("no records, stop")
        return {}, 200

    bucket_name = request.args.get("bucket_name", "ba882-ncaa-project")
    blob_name = request.args.get("blob_name")
    run_id = request.args.get("run_id")
    if not blob_name:
        raise ValueError("need blon_name params")

    # --- from GCS load JSON ---
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_str = blob.download_as_text()
    j = json.loads(data_str)
    events = j.get("events", [])
    print(f"📦 extract {len(events)} games")

    # ---container init ---
    games, venues, teams_all, game_team_stats = [], [], [], []
    ingest_ts_str = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # --- each game ---
    for e in events:
        game_id = e.get("id")
        start_date = pd.to_datetime(e.get("date"), utc=True).tz_convert(None)
        season = e["season"]["year"]
        week = e["week"]["number"]
        venue = e["competitions"][0]["venue"]
        competitors = e["competitions"][0]["competitors"]
        attendance = e["competitions"][0].get("attendance", 0)
        source_path = f"{bucket_name}/{blob_name}"

        # -----game-----
        games.append({
            "id": int(game_id),
            "start_date": start_date,
            "season": season,
            "week": week,
            "venue_id": int(venue["id"]),
            "ingest_timestamp": ingest_ts_str,
            "source_path": source_path,
            "run_id": run_id
        })

        # ----- venue -----
        venues.append({
            "id": int(venue["id"]),
            "fullname": venue.get("fullName"),
            "city": venue.get("address", {}).get("city"),
            "country": venue.get("address", {}).get("country"),
            "indoor": venue.get("indoor"),
            "ingest_timestamp": ingest_ts_str,
            "source_path": source_path,
            "run_id": run_id
        })

        # ----- teams -----
        teams_df = pd.json_normalize(competitors)

        # delete id to avoid conflict with team_id
        if "id" in teams_df.columns:
            teams_df = teams_df.drop(columns=["id"])

        rename_map = {
            "team.id": "id",
            "team.name": "name",
            "team.abbreviation": "abbrev",
            "team.displayName": "display_name",
            "team.shortDisplayName": "short_name",
            "team.color": "color",
            "team.alternateColor": "alternate_color",
            "team.venue.id": "venue_id",
            "team.logo": "logo",
        }

        # keep real columns
        valid_cols = [c for c in rename_map.keys() if c in teams_df.columns]
        teams_df = teams_df[valid_cols].rename(columns=rename_map)

        # remove duplicates
        teams_df = teams_df.loc[:, ~teams_df.columns.duplicated()]

        # add metadata
        teams_df["ingest_timestamp"] = ingest_ts_str
        teams_df["source_path"] = source_path
        teams_df["run_id"] = run_id

        print(f"✅ 解析球隊欄位: {list(teams_df.columns)}")

        teams_all.append(teams_df)

        # ----- second  -----
        try:
            summary_url = f"{GAME_URL}{game_id}"
            resp = requests.get(summary_url)
            if resp.ok:
                data = resp.json()
                # box = resp.json().get("boxscore", {})
                # team_stats = box.get("teams", [])
                # if len(team_stats) == 2:
                #     for i, side in enumerate(["Away", "Home"]):
                #         stats = team_stats[i]
                        # stat_map = {s["name"]: s.get("displayValue") or s.get("value") for s in stats.get("statistics", [])}
                        # game_team_stats.append({
                        #     "game_id": int(game_id),
                        #     "team_id": stats["team"]["id"],
                        #     "home_away": side,
                        #     "score": stats.get("score", 0),
                        #     "total_yards": int(stat_map.get("Total Yards", 0) or 0),
                        #     "third_eff": float(stat_map.get("3rd Down %", 0) or 0),
                        #     "fourth_eff": float(stat_map.get("4th Down %", 0) or 0),
                        #     "yards_per_pass": float(stat_map.get("Yards Per Pass", 0) or 0),
                        #     "yards_per_rush": float(stat_map.get("Yards Per Rush", 0) or 0),
                        #     "turnovers": int(stat_map.get("Turnovers", 0) or 0),
                        #     "fumbles_lost": int(stat_map.get("Fumbles Lost", 0) or 0),
                        #     "ints_thrown": int(stat_map.get("Interceptions Thrown", 0) or 0),
                        #     "top": int(stat_map.get("Time of Possession", 0) or 0),
                        #     "ingest_timestamp": ingest_ts_str,
                        #     "source_path": source_path,
                        #     "run_id": run_id
                        # })
                game_team_stats.append({
                    'event_id':game_id,
                    'team':data['boxscore']['teams'][0]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
                    'home_away':'Away',
                    'score' : competitors[0]['score'],
                        #now for stats
                    'total_yards':safe_cast(data['boxscore']['teams'][0]['statistics'][3]['displayValue'], int),
                    'third_eff':safe_cast(data['boxscore']['teams'][0]['statistics'][1]['value'], float),
                    'fourth_eff':safe_cast(data['boxscore']['teams'][0]['statistics'][2]['value'], float),
                    'yards_per_pass':safe_cast(data['boxscore']['teams'][0]['statistics'][6]['displayValue'], float),
                    'yards_per_rush':safe_cast(data['boxscore']['teams'][0]['statistics'][9]['displayValue'], float),
                    'turnovers':safe_cast(data['boxscore']['teams'][0]['statistics'][11]['displayValue'], int),
                    'fumbles_lost':safe_cast(data['boxscore']['teams'][0]['statistics'][12]['value'], int),
                    'ints_thrown':safe_cast(data['boxscore']['teams'][0]['statistics'][13]['value'], int),
                    'top':safe_cast(data['boxscore']['teams'][0]['statistics'][14]['value'], int),  # how long did the team hold onto the ball?
                    'ingest_timestamp':ingest_ts_str,
                    'source_path':source_path,
                    'run_id':run_id
                                        })
                                        
                game_team_stats.append({
                        'event_id':game_id,
                        'team':data['boxscore']['teams'][1]['team']['id'],  # Can be adjusted if id is not sufficient for joins.
                        'home_away':'Home',   # hard coding the second team to be home team, following T1 @ T2 format of most sports promotions.
                        'score': competitors[1]['score'],
                        'total_yards':safe_cast(data['boxscore']['teams'][1]['statistics'][3]['displayValue'], int),
                        'third_eff':safe_cast(data['boxscore']['teams'][1]['statistics'][1]['value'], float),
                        'fourth_eff':safe_cast(data['boxscore']['teams'][1]['statistics'][2]['value'], float),
                        'yards_per_pass':safe_cast(data['boxscore']['teams'][1]['statistics'][6]['displayValue'], float),
                        'yards_per_rush':safe_cast(data['boxscore']['teams'][1]['statistics'][9]['displayValue'], float),
                        'turnovers':safe_cast(data['boxscore']['teams'][1]['statistics'][11]['displayValue'], int),
                        'fumbles_lost':safe_cast(data['boxscore']['teams'][1]['statistics'][12]['value'], int),
                        'ints_thrown':safe_cast(data['boxscore']['teams'][1]['statistics'][13]['value'], int),
                        'top':safe_cast(data['boxscore']['teams'][1]['statistics'][14]['value'], int),  # how long did the team hold onto the ball?
                        'ingest_timestamp':ingest_ts_str,
                        'source_path':source_path,
                        'run_id':run_id})
            else:
                print(f"cannot extract boxscore: {game_id}")
        except Exception as err:
            print(f"extract {game_id} how many errors: {err}")

    # ---  DataFrame ---
    games_df = pd.DataFrame(games)
    venues_df = pd.DataFrame(venues)
    teams_df = pd.concat(teams_all, ignore_index=True)
    gts_df = pd.DataFrame(game_team_stats)

    # --- GCS ---
    gcs_prefix = f"gs://{bucket_name}/raw"
    for name, df in {
        "games": games_df,
        "venues": venues_df,
        "teams": teams_df,
        "game_team": gts_df
    }.items():
        base = f"{gcs_prefix}/{name}/season={season}/week={week}"
        df = df.loc[:, ~df.columns.duplicated()]  
        df.to_parquet(f"{base}/data.parquet", index=False)
        df.to_parquet(f"{base}/run_id={run_id}/data.parquet", index=False)
        print(f"📤 已上傳 {name} parquet 至 {base}")

    # --- load MotherDuck ---
    md.register("games_df", games_df)
    md.register("venues_df", venues_df)
    md.register("teams_df", teams_df)
    md.register("gts_df", gts_df)

    print("🚀 寫入 MotherDuck raw schema ...")
    md.sql(f"INSERT INTO {db_schema}.games SELECT * FROM games_df")
    md.sql(f"INSERT INTO {db_schema}.venues SELECT * FROM venues_df")
    md.sql(f"INSERT INTO {db_schema}.teams SELECT * FROM teams_df")
    md.sql(f"INSERT INTO {db_schema}.game_team SELECT * FROM gts_df")


    for tbl in ["games", "venues", "teams", "game_team"]:
        count = md.sql(f"SELECT COUNT(*) FROM {db_schema}.{tbl}").fetchone()[0]
        print(f"✅ {tbl} load in {count} rows")

    print("🎉 parsing-sb-g-info success！")
    return {"status": "success", "num_games": len(games)}, 200
