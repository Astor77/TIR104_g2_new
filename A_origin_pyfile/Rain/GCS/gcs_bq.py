import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\project_2\tir10402611-a43374c6285d.json"
from google.cloud import storage
from google.cloud import bigquery


# 設定 GCP 專案與 BigQuery 資料集
project_id = "tir10402611"
dataset_id = "tir10402_dataset"
table_id = "release_dates"

# 設定 GCS 資料夾位置
gcs_uri = "gs://tir104-bucket-02/GCS/*.csv"

# 建立 BigQuery 客戶端
client = bigquery.Client()

# 設定載入作業
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  # 如果是 JSON 改成 JSON
    autodetect=True,  # 自動偵測欄位格式
    skip_leading_rows=1  # 跳過 CSV 第一行標頭
)

# 執行載入作業
table_ref = f"{project_id}.{dataset_id}.{table_id}"
load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

# 等待完成
load_job.result()
print(f"資料夾已成功載入至 {table_ref}")
