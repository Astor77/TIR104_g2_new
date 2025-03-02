# 運用final_data_task.py
# 將資料整合為最終版本的函式(叫BigQuery處理的SQL邏輯)
# 裡面的函式暫時考慮以最終table的表名命名

from google.cloud import bigquery
from prefect import task, flow
from prefect_gcp import GcpCredentials

# ✅ 載入 GCP 認證
gcp_credentials_block = GcpCredentials.load("tir104-02")
bigquery_client = bigquery.Client(credentials=gcp_credentials_block.get_credentials_from_service_account())

PROJECT_ID = "tir104g02"

def check_scheduled_query_status(scheduled_query_name):
    """檢查指定的 BigQuery Scheduled Query 是否成功執行"""
    query = f"""
    SELECT job_id, state, error_result, creation_time 
    FROM `{PROJECT_ID}.region-asia-east1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE job_type = 'QUERY' 
    AND user_email LIKE '%bigquery-scheduled-query%'
    AND query LIKE '%{scheduled_query_name}%'
    AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
    ORDER BY creation_time DESC
    LIMIT 1
    """
    
    query_job = bigquery_client.query(query)
    results = query_job.result()

    for row in results:
        job_id = row.job_id
        state = row.state
        error_result = row.error_result
        creation_time = row.creation_time

        if state == "DONE" and error_result is None:
            print(f"✅ Scheduled Query `{scheduled_query_name}` 成功執行 (Job ID: {job_id})")
        else:
            print(f"❌ Scheduled Query `{scheduled_query_name}` 失敗！錯誤訊息: {error_result}")

# ✅ 定義要監控的 Scheduled Query 任務
SCHEDULED_QUERIES = [
    "person_detail",
    "movie_genres",
    "tmdb_keywords",
    "movie_director_list",
    "movie_release_global",
    "movie_detail",
    "movie_tw_week_amount",
    "movie_imdb_rating",
    "movie_tw_year_amount",
    "movie_actor_list",
    "genres_list",
]

@task
def monitor_scheduled_query(scheduled_query_name):
    """動態監控指定的 BigQuery Scheduled Query"""
    check_scheduled_query_status(scheduled_query_name)

@flow  # ✅ Prefect 2.0 使用 `@flow`
def monitor_scheduled_queries():
    """動態監控所有 Scheduled Queries"""
    for query_name in SCHEDULED_QUERIES:
        monitor_scheduled_query(query_name)

# ✅ 直接執行 Flow
if __name__ == "__main__":
    monitor_scheduled_queries()


# task1


# task2


# task3


# task4


# task5
