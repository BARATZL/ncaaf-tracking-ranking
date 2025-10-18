import io
from google.cloud import storage
from datetime import datetime
import pandas as pd

# === 你的 GCS bucket 名稱 ===
BUCKET_NAME = "ba882-ncaa-project"

def export_rankings_to_file(poll_dfs):
    """
    將 ESPN 排名資料上傳到 GCS，並依 season / week / poll_name 結構化儲存
    """
    print("🚀 Exporting ranking files to GCS with season/week folder structure...")

    # 初始化 GCS 連線
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 取得今天日期（方便追蹤）
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # 對每個排行榜進行上傳
    for poll_name, df in poll_dfs.items():
        # 取出季節與週數（假設在 parse_rankings_data 已加入這兩欄）
        season = str(df["season_year"].iloc[0]) if "season_year" in df.columns else "unknown"
        week = str(df["week_number"].iloc[0]) if "week_number" in df.columns else "unknown"

        # 設定檔案名稱與路徑
        file_name = f"{poll_name.lower()}_poll_{today}.parquet"
        gcs_path = f"rankings/season={season}/week={week}/{file_name}"

        # DataFrame 轉 parquet（不落地）
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # 上傳到 GCS
        blob = bucket.blob(gcs_path)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"✅ Uploaded {poll_name} ranking to gs://{BUCKET_NAME}/{gcs_path}")

    print("🎉 All rankings successfully uploaded to GCS!")
