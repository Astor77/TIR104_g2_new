# 運用gcs_task.py
# 看怎麼處理成temp_data的方式，決定是否安排上傳temp資料的task
from prefect_gcp import GcpCredentials
from google.cloud import bigquery
import pandas as pd
from tasks.Storage_Task import gcs_module as gm

gcp_credentials_block = GcpCredentials.load("tir104-02")
bigquery_client = gm.get_credentials_bigquery(gcp_credentials_block)
#print(type(bigquery_client))  

def create_dataset(dataset_id, client):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)  
    
    # 創建資料集
    dataset = client.create_dataset(dataset, exists_ok=True)  # 避免重複
    print(f"資料集 {dataset_id} 已成功創建於專案 {client.project}。")

create_dataset("final_data", bigquery_client)

# task1


# task2


# task3


# task4


# task5
