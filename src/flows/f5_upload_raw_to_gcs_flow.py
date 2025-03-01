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
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_info.json"
    destination_blob_name = f"raw_data/{timestamp}/raw_tw_omdb_info"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task2
def upload_tmdb_credits():
    bucket_name = "tmdb_credits"
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tmdb_credits/tmdb_credits.json"
    destination_blob_name = f"raw_data/{timestamp}/{pc.credits_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task3
def upload_tmdb_details():
    bucket_name = "tmdb_details"
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tmdb_details/tmdb_details.json"
    destination_blob_name = f"raw_data/{timestamp}/{pc.details_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task4   還沒上傳，無檔名
def upload_tmdb_details_en():
    bucket_name = "tmdb_details_en"
    source_file_name = pc.raw_tw_details_en
    destination_blob_name = f"raw_data/{timestamp}/{pc.details_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task5
def upload_tmdb_keywords():
    bucket_name = "tmdb_keywords"
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tmdb_keywords/tmdb_keywords.json"
    destination_blob_name = f"raw_data/{timestamp}/{pc.keywords_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task6
def upload_tmdb_release_date():
    bucket_name = "tmdb_release_date"
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tmdb_release_date/tmdb_release_dates.json"
    destination_blob_name = f"raw_data/{timestamp}/{pc.release_date_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task8
def upload_tw_mapping_tmdb():
    bucket_name = "tw_mapping_tmdb"
    source_file_name = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_mapping_tmdb/tw_tmdb_mapping.csv"
    destination_blob_name = f"raw_data/{timestamp}/{pc.mapping_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task9    #要確認使用哪一個檔名(我先用copy path)
def upload_tw_movie_2022_2025_dup():
    bucket_name = "tw_movie_2022_2025"
    source_file_name = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_movie_2022-2025/TWMovie2022-2025_raw.csv"
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_annual_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task9-1   #要確認使用哪一個檔名(我先用copy path)
def upload_tw_movie_2022_2025_not_dup():
    bucket_name = "tw_movie_2022_2025"
    source_file_name = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_movie_2022-2025/TWMovie2022-2025_raw2.csv"
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_annual_not_dup_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task10   #這式2000多個json不用上傳
def upload_tw_movie_sales():
    bucket_name = "tw_movie_sales"
    source_file_name = pc.raw_tw_sales
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_annual_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task11
def upload_tw_movie_weekly():
    bucket_name = "tw_movie_weekly"
    source_file_name = pc.raw_tw_weekly
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_weekly_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task11-1
def upload_tw_movie_weekly2():
    bucket_name = "tw_movie_weekly"
    source_file_name = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_movie_weekly/TWMovie_weekly_data2.csv"
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_weekly_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task12  #這個不需要執行
def upload_tw_movie_year_sales():
    bucket_name = "tw_movie_year_sales"
    source_file_name = pc.raw_tw_year_sales
    destination_blob_name = f"raw_data/{timestamp}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task13
def upload_tw_release_dates():
    bucket_name = "tw_release_dates"
    source_file_name = pc.raw_tw_tw_release_date
    destination_blob_name = f"raw_data/{timestamp}/{pc.tw_release_date_csv}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task14
def upload_tw_search():
    bucket_name = "tw-search"
    source_file_name = pc.raw_tw_search
    destination_blob_name = f"raw_data/{timestamp}/{pc.search_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)

# task15   沒有selenium的路徑跟資料夾名
def upload_tw_selenium_download():
    bucket_name = "tw_selenium_download"
    source_file_name = pc.raw_tw_search
    destination_blob_name = f"raw_data/{timestamp}/{pc.search_json}"
    gm.upload_to_gcs(storage_client, bucket_name, source_file_name, destination_blob_name)



#upload_omdb()
#upload_tmdb_credits()
#upload_tmdb_details()
#upload_tmdb_details_en()
#upload_tmdb_keywords()
#upload_tmdb_release_date()
upload_tw_mapping_tmdb()
#upload_tw_movie_2022_2025_dup()
#upload_tw_movie_2022_2025_not_dup()
#upload_tw_movie_sales()
#upload_tw_movie_weekly()
#upload_tw_movie_weekly2()
#upload_tw_movie_year_sales()
#upload_tw_release_dates()
#upload_tw_search()
#upload_tw_selenium_download()