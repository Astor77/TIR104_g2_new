"""
語法來自github googleapis
git clone gh repo clone googleapis/python-bigquery-dataframes
file : pandas_methods_test.py
官方文件 : https://cloud.google.com/python/docs/reference
"""
"""
下載套件
pip install --upgrade bigframes
"""
"""
Google Cloud 的身份驗證機制（例如 gcloud auth 或 Application Default Credentials)
in cmd or power shell(mac in terminal)
gcloud auth application-default login
^^^^如果是在雲端操作有其他做法^^^^
"""
"""
設配額/環境變數
unxi >> export GOOGLE_CLOUD_PROJECT= "PROJECT_ID"
win >> set GOOGLE_CLOUD_PROJECT= "your-project-id"
"""
"""
如果仍無法讀取可確認GOOGLE CLOUD認證 & INSTALL下面這段
pip install --upgrade bigframes pandas
"""

import bigframes.pandas as bpd
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.auth import credentials
from google.auth.transport.requests import Request
from google.api_core.exceptions import Conflict
from google.oauth2 import service_account


#-------------------------將DataFrame從bigquery上抓下來
#抓取csv&json語法差異不大，其餘的調整去找chatgpt or 上面的官方文件
#-----------------------------你的project name
bpd.options.bigquery.project = "my-project-7393-451114"
def test_query(creds, project_id, dataset_name, table_name):
    #------------- 可以去BigQuery複製sql語法from後面那段
    first_query = f"{project_id}, {dataset_name}, {table_name}" 

    try:
        # 嘗試讀取 BigQuery 資料
        movie_query = bpd.read_gbq(first_query, credentials= creds)
        # 檢查是否成功
        if movie_query is not None:
            print(f"✅{table_name}成功下載")
            print(movie_query.head(5))  # 前 5 筆
            print(type(movie_query))
        else:
            print(f"❌{table_name}載入失敗")
            
    except Exception as e:
        print(f"❌ Error occurred: {e}")

# 呼叫測試函式
#test_query()

#-------------------------將DataFrame��存於Google Cloud Storage

#解析prefect導入的gcp credentials-------(cloud_storage)
def get_credentials_gcs(gcp_credentials_block):
        #解析字典
        service_account_info = gcp_credentials_block.service_account_info.get_secret_value()
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        storage_client = storage.Client(credentials=credentials, project=gcp_credentials_block.project)
        return storage_client


#解析prefect導入的gcp credentials-------(big_query)
def get_credentials_bigquery(gcp_credentials_block):
        #解析字典
        service_account_info = gcp_credentials_block.service_account_info.get_secret_value()
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        bigquery_client = bigquery.Client(credentials=credentials, project=gcp_credentials_block.project)
        return bigquery_client





# 創建新的存儲桶
def create_bucket(bucket_name, storage_client, location):
    try:
        bucket = storage_client.create_bucket(bucket_name, location= location)
        print(f"bucket {bucket.name} 已成功創建。")
    except Conflict:
        print(f"bucket {bucket.name} 已存在。")
    except Exception as e:
        print(f"創建bucket{bucket_name}發生錯誤{e}")


#create_bucket('002_test')

#-------------------------上傳檔案
from google.cloud import storage

def upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name):
    """將本地文件上傳到指定的 GCS 存儲桶"""
    #可以想像是一個google api讓我們取的連結可以操作gcs
    bucket = storage_client.bucket(bucket_name)
    #blob 代表 GCS 中的一個檔案物件。在這裡，destination_blob_name 是檔案在 GCS 中的儲存路徑和名稱。這個物件就像是你要上傳的檔案在 GCS 上的代號或位置。
    #destination_blob_name 是檔案在 GCS 儲存桶中的 "目標檔案名"，這個名稱可以包含資料夾結構（例如 folder/in/bucket/file.csv）。
    blob = bucket.blob(destination_blob_name)
    
    # 上傳檔案到 GCS ，source_file_name 是檔案在你本地設備上的路徑
    blob.upload_from_filename(source_file_name)
    print(f"文件 {source_file_name} 已成功上傳到 {bucket_name}/{destination_blob_name}。")


#upload_to_gcs('002_test', r"C:\Users\User\Desktop\Python_note\01-news_folder\2025-02-21_news.csv", 'news/in/bucket/001.csv')


#---設定exteranl table將gcs資料連動至bigquery
from google.cloud import bigquery

def create_external_table_from_gcs(dataset_id, table_id, bucket_name, source_file_name):
    """將 GCS 中的資料設為 BigQuery 的外部表格"""
    #建立與bigquery的連線 
    client = bigquery.Client()

    # 告訴bigquery要取指向gcs的哪一個檔案
    uri = f"gs://{bucket_name}/{source_file_name}"

    # BigQuery 的資料集和表格名稱
    table_ref = client.dataset(dataset_id).table(table_id)

    # 設定外部表格的配置
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # 可以根據檔案格式調整
        skip_leading_rows=1,  # 如果是 CSV 且有標題行，則跳過
        autodetect=True,  # BigQuery 自動推斷欄位類型
    )

    # 創建外部表格
    job = bigquery.Client().load_table_from_uri(
        uri, table_ref, job_config=job_config
    )    
    job.result()  # 等待作業完成

    print(f"外部表格 {table_id} 已成功創建，指向 GCS 中的 {uri}。")


#create_external_table_from_gcs('News', 'TVBS_NEWS', '002_test', 'news/in/bucket/001.csv')



#------- 創建一個dataset
from google.cloud import bigquery

def create_dataset(dataset_id):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)

    # 創建資料集
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)  # 創建資料集

    print(f"資料集 {dataset_id} 已成功創建。")

# 呼叫範例
#create_dataset('News')  # 創建名為 'News' 的資料集


#---檢查目前的project是哪一個---

from google.cloud import storage

def check_gcp_project():
    storage_client = storage.Client()
    # 取得當前的 GCP Project ID
    project_id = storage_client.project
    print(f"當前 GCP Project ID：{project_id}")

#check_gcp_project()


#-------------------------------------------------------------

def upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH):
    # 設定 GCP 資訊
    #PROJECT_ID = "your-gcp-project-id"  # GCP 專案 ID
    #DATASET_ID = "your_dataset_id"  # BigQuery 資料集名稱
    #TABLE_ID = "your_table_id"  # BigQuery 表格名稱
    #CSV_FILE_PATH = "your_file.csv"  # CSV 檔案路徑

    # 讀取 CSV 檔案
    df = pd.read_csv(CSV_FILE_PATH)

    # 設定 BigQuery 資料表的完整 ID
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    # 設定上傳配置
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # "WRITE_TRUNCATE" 覆蓋 | "WRITE_APPEND" 追加
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,  # 跳過 CSV 標題列
    )
    # 上傳 DataFrame 到 BigQuery
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # 等待上傳完成
    print(f"成功上傳 {len(df)} 筆資料到 BigQuery：{table_ref}")
