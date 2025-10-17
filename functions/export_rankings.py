import os
from google.cloud import storage
import pandas as pd
from datetime import datetime
import io

# === 設定你的 bucket 名稱 ===
BUCKET_NAME = "ba882-ncaa-project"

def export_rankings_to_file(poll_dfs):
    """
    將排行榜 DataFrame 上傳到 GCS
    """
    print("🚀 Exporting ranking files to GCS...")

    # 初始化 GCS client
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 建立今天的日期資料夾
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for poll_name, df in poll_dfs.items():
        # 產生檔案名稱
        file_name = f"{poll_name.lower()}_poll_{today}.parquet"
        gcs_path = f"rankings/{today}/{file_name}"

        # 將 DataFrame 轉成 bytes 並上傳
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        blob = bucket.blob(gcs_path)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"✅ Uploaded {poll_name} ranking to gs://{BUCKET_NAME}/{gcs_path}")

    print("🎉 All rankings successfully uploaded to GCS!")
