import bigframes.pandas as bpd
from google.cloud import bigquery
from prefect_gcp import GcpCredentials
from google.oauth2 import service_account
from tasks.Storage_Task import gcs_module as gm
from google.auth import credentials
from google.auth.transport.requests import Request
from pandas_gbq import read_gbq

#起手式取得prefect的認證
gcp_credentials_block = GcpCredentials.load("tir104-02")
creds = gm.get_credentials_download(gcp_credentials_block)

#task1
def download_final_country():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "country"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task2
def download_final_gender_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "gender_list_csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task3
def download_final_genres_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "genres_list"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task4
def download_final_movie_actor_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_actor_list"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task5
def download_final_movie_detail():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_detil"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task6
def download_final_movie_director_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_director_list"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task7
def download_final_movie_genres():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_genres"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task8
def download_final_movie_imdb_rating():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_imdb_rating"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task9
def download_final_movie_release_global():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_release_global"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task10
def download_final_movie_week():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_tw_week_amount"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task11
def download_final_movie_year():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_tw_year_amount"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task12
def download_final_person_detail():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "person_detail"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task13
def download_final_release_type():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "release_types_csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name)

#task14
def download_final_keywords():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "tmdb_keywords"
    gm.download_dataset(creds, project_id, dataset_name, table_name)










































































