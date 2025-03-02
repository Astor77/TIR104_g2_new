from google.cloud import bigquery
import pandas as pd
import os

# 設定 Google Cloud 服務帳戶金鑰
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r
def download_bigquery_dataset(project_id, dataset_id, output_dir):
    """
    下載 BigQuery Dataset 中的所有表格，並儲存為 CSV 檔案。

    :param project_id: Google Cloud Project ID
    :param dataset_id: BigQuery Dataset ID
    :param output_dir: 輸出的 CSV 檔案資料夾
    """
    client = bigquery.Client(project=project_id)

    # 建立輸出資料夾
    os.makedirs(output_dir, exist_ok=True)

    # 取得 Dataset 中的所有表格
    tables = client.list_tables(f"{project_id}.{dataset_id}")

    for table in tables:
        table_id = table.table_id
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        print(f"正在下載表格: {table_ref}")

        # 執行查詢
        query = f"SELECT * FROM `{table_ref}`"
        query_job = client.query(query)
        df = query_job.result().to_dataframe()

        # 儲存為 CSV
        output_csv = os.path.join(output_dir, f"{table_id}.csv")
        df.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"已儲存 CSV 檔案: {output_csv}")

# 設定 Google Cloud Project ID
PROJECT_ID = "tir104g02"

# 設定 BigQuery Dataset ID
DATASET_ID = "final_data"

# 設定輸出的 CSV 檔案資料夾
OUTPUT_DIR =  r"A2_final_data\tw"

# 執行下載
download_bigquery_dataset(PROJECT_ID, DATASET_ID, OUTPUT_DIR)
