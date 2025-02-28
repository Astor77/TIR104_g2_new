# 運用gcs_task.py
# 上傳所有raw資料的task
from datetime import datetime
from google.cloud import storage
from prefect_gcp import GcpCredentials
from tasks.Storage_Task import gcs_module as gm
from utils import path_config as pc
from google.oauth2 import service_account


# 載入 Prefect 設定的 GCP Credential
gcp_credentials_block = GcpCredentials.load("tir104-02")
#print(gcp_credentials_block)
location="asia-east1"
#設定資料上傳時間
timestamp = datetime.now().strftime("%Y-%m-%d") 
#將憑證建立連接
storage_client = gm.get_credentials_gcs(gcp_credentials_block)

#創建bucket
bucket_list = [ "omdb_info",
                "tmdb_credits", 
                "tmdb_details", 
                "tmdb_details_en",
                "tmdb_genres", 
                "tmdb_keywords", 
                "tmdb_release_date", 
                "tw_mapping_tmdb",
                "tw_movie_2022_2025",
                "tw_movie_sales",
                "tw_movie_weekly",
                "tw_movie_year_sales",
                "tw_release_dates",
                "tw-search",
                "tw_selenium_download"
                ]

#storage_client = gm.get_credentials(gcp_credentials_block)
#for i in bucket_list:
#    gm.create_bucket(i, storage_client, location)

# task1
def upload_omdb():
    bucket_name = "omdb_info"
    source_file_name = "/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_raw_data_2025-02-23.json"
    destination_blob_name = f"raw_data/{timestamp}/raw_tw_omdb_info"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task2
def upload_tmdb_credits():
    bucket_name = "tmdb_credits"
    source_file_name = pc.raw_tw_credits
    destination_blob_name = f"raw_data/{timestamp}/{pc.omdb_info_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task3
def upload_tmdb_details():
    bucket_name = "tmdb_details"
    source_file_name = pc.raw_tw_details
    destination_blob_name = f"raw_data/{timestamp}/{pc.details_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task4


# task5
