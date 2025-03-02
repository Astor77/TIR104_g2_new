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
credentials = gm.get_credentials_bigquery(gcp_credentials_block)

project_id = "tir104g02"
dataset_name = "final_data"
table_name = "country"

gm.download_dataset(credentials, project_id, dataset_name, table_name)
