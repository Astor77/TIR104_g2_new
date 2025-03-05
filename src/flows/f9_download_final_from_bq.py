import bigframes.pandas as bpd
from google.cloud import bigquery
from prefect_gcp import GcpCredentials
from prefect import task, flow
from google.oauth2 import service_account
from tasks.Storage_Task import gcs_module as gm
from google.auth import credentials
from google.auth.transport.requests import Request
from pandas_gbq import read_gbq

#起手式取得prefect的認證
gcp_credentials_block = GcpCredentials.load("tir104-02")
creds = gm.get_credentials_download(gcp_credentials_block)

@task
def download_final_country():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "country"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/country.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name,file_path)

@task
def download_final_gender_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "gender_list_csv"
    file_path = "/workspaces/TIR104_g2_new/A2_final_data/tw/gender_list_csv.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_genres_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "genres_list"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/genres_list.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_actor_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_actor_list"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_actor_list.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_detail():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_detil"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_detail.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_director_list():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_director_list"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_director_list.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_genres():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_genres"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_genres.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_imdb_rating():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_imdb_rating"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_imdb_rating.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_release_global():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_release_global"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_release_global.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_week():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_tw_week_amount"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_tw_week_amount.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_movie_year():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "movie_tw_year_amount"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/movie_tw_year_amount.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_person_detail():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "person_detail"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/person_detail.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_release_type():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "release_types_csv"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/release_types.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@task
def download_final_keywords():
    project_id = "tir104g02"
    dataset_name = "final_data"
    table_name = "tmdb_keywords"
    file_path = r"/workspaces/TIR104_g2_new/A2_final_data/tw/tmdb_keywords.csv"
    gm.download_dataset(creds, project_id, dataset_name, table_name, file_path)

@flow
def f9_download_finaldata_flow():

    download_final_country()
    download_final_gender_list()
    download_final_genres_list()
    download_final_movie_actor_list()
    download_final_movie_detail()
    download_final_movie_director_list()
    download_final_movie_genres()
    download_final_movie_imdb_rating()
    download_final_movie_release_global()
    download_final_movie_week()
    download_final_movie_year()
    download_final_person_detail()
    download_final_release_type()
    download_final_keywords()
   
   

if __name__ == "__main__":
    f9_download_finaldata_flow()
    









































































