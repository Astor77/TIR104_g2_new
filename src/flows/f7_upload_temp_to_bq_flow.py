# 運用gcs_task.py
# 看怎麼處理成temp_data的方式，決定是否安排上傳temp資料的task
from prefect_gcp import GcpCredentials
from prefect import task, flow
from google.cloud import bigquery
import pandas as pd
from tasks.Storage_Task import gcs_module as gm
from utils import path_config as pc

import importlib  # Python 內建的重新載入模組工具
importlib.reload(gm)  # 強制重新載入

gcp_credentials_block = GcpCredentials.load("tir104-02")
bigquery_client = gm.get_credentials_bigquery(gcp_credentials_block)
#print(type(bigquery_client))

def create_dataset(dataset_id, client, location):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)  

    dataset.location = location
    
    # 創建資料集
    dataset = client.create_dataset(dataset, exists_ok=True)  # 避免重複
    print(f"資料集 {dataset_id} 已成功創建於專案 {client.project}。")

# 創建 dataset，並指定 Data location 為 asia-east1
#create_dataset("final_data", bigquery_client, location="asia-east1")

@task
def upload_to_bq_omdb():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "omdb_info_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/omdb_info.csv" 
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_tmdb_details():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "tmdb_details_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_details.csv" 
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_release_date():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "release_date_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_release_dates.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_cast_top5_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "cast_top5_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_casts_top5.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_director_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "director_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_directors.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_person_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "person_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_person.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_keywords_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "keywords_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_keywords.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_genres_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "genres_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_movie_genres.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_genres_list_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "genres_list_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/tmdb_genres_list.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_tw_annual_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "tw_annual_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/TWMovie_annual_df3.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_tw_details_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "tw_details_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/TWMovie_details.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_tw_weekly_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "tw_weekly_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A1_temp_data/tw/TWMovie_weekly_df2.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_country_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "country_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A2_final_data/tw/country.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_details_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "details_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A2_final_data/tw/details.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_gender_list_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "gender_list_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A2_final_data/tw/gender_list.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)


@task
def upload_to_bq_release_types_csv():
    PROJECT_ID = "tir104g02"  # GCP 專案 ID
    DATASET_ID = "temp_data"  # BigQuery 資料集名稱
    TABLE_ID = "release_types_csv"  # BigQuery 表格名稱
    CSV_FILE_PATH = r"/workspaces/TIR104_g2_new/A2_final_data/tw/release_types.csv"
    gm.upload_tmp_to_bq(bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)



@flow
def f7_upload_tmp_to_bq_flow():

    a1 = upload_to_bq_omdb().submit()
    a2 = upload_to_bq_tmdb_details().submit()
    a3 = upload_to_bq_release_date().submit()
    a4 = upload_to_bq_cast_top5_csv().submit()
    a5 = upload_to_bq_director_csv().submit()
    a6 = upload_to_bq_person_csv().submit()
    a7 = upload_to_bq_keywords_csv().submit()
    a8 = upload_to_bq_genres_csv().submit()
    a9 = upload_to_bq_genres_list_csv().submit()
    a10 = upload_to_bq_tw_annual_csv().submit()
    a11 = upload_to_bq_tw_details_csv().submit()
    a12 = upload_to_bq_tw_weekly_csv().submit()
    a13 = upload_to_bq_country_csv().submit()
    a14 = upload_to_bq_details_csv().submit()
    a15 = upload_to_bq_gender_list_csv().submit()
    a16 = upload_to_bq_release_types_csv().submit()

    a1.result()
    a2.result()
    a3.result() 
    a4.result() 
    a5.result() 
    a6.result() 
    a7.result() 
    a8.result() 
    a9.result() 
    a10.result()
    a11.result()
    a12.result()
    a13.result()
    a14.result()
    a15.result()
    a16.result()


if __name__ == "__main__":
    f7_upload_tmp_to_bq_flow()

