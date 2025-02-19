import pandas as pd
import requests
import time
from google.cloud import storage



# 1.==== 取得imdb_id ==== # 會回傳imdb_id >> list
def fetch_imdb_id():  
    #路徑會可能來自gcs
    movie_id_csv = pd.read_csv("路徑")
    movie_id_list = movie_id_csv["movie_id"].tolist()   
    #去除nan值
    movie_id = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
    #計算id個數
    movie_id_len = len(movie_id)
    return movie_id, movie_id_len


# 2 ==== 進行1000筆的爬蟲 ==== # 會回傳首次請求的1000筆
def crawl_movie_info(movie_id):

    api_token = "YOUR_API_TOKEN"
    max_requests = 1000
    count_requests = 0
    #建立一個list存放，後續回傳出來存檔
    results = []
    try:

        for movie in movie_id:
            if count_requests >= max_requests:
                print(f"已請求{max_requests}次請求，今日結束")
                break
            url = f"http://www.omdbapi.com/?i={movie}&apikey={api_token}"
            headers = {
                "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                movie_info = response.json()
                results.append(movie_info)
                print(f"已將{movie}加入列表")

            count_requests += 1
            time.sleep(2)

    except Exception as e:
        print(f"發生錯誤:{e}")
    
    return results


# 3 ==== 存檔(json)/但這個應該有人寫了....我不寫>< ==== #


# 4 ==== 進行第2次1000筆的請求 ==== #
def crawl_movie_info_second_time(movie_id, results=None):

    api_token = "YOUR_API_TOKEN"
    max_requests = 1000
    count_requests = 0

    if represults is None:
        represults = []
    #先取出已請求過的imdb_id的imdb_id
    existing_id = [movie["imdbID"] for movie in results]

    #將沒請求過的取出做判斷
    new_movie_ids = [movie for movie in movie_ids if movie not in existing_ids]
    if not new_movie_ids:
        print("已爬取所有，結束任務。")
        return results

    try:
        for movie in new_movie_ids:
            if count_requests >= max_requests:

                break

            if movie in existing_id:
                print(f"{movie}:已請求過，此筆跳過")
                continue

            url = f"http://www.omdbapi.com/?i={movie}&apikey={api_token}"
            headers = {
                "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                movie_info = response.json()
                results.append(movie_info)
                print(f"已將{movie}加入列表")

                existing_id.append(movie)
            else:
                print(f"{movie}:請求失敗，狀態碼{response.status_code}")

            count_requests += 1
            time.sleep(2)
        
    except Exception as e:
        print(f"發生錯誤:{e}")
    
    return results
        


# 5 ==== 將檔案寫入gcs ==== # >>要建bucket
def create_bucket(bucket_name):
    #初始化
    storage_client = storage.Client()

    # 創建新的存儲桶
    try:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"{bucket_name}--已建立")

    except Exception as e:
        print(f"{bucket_name}--創建失敗")
        print(f"失敗原因 :{e}")


# 6 ==== 將結果存入gcs ==== # 
# 至少兩天才能爬完，所以地端一定會有檔案
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """將本地文件上傳到指定的 GCS 存儲桶"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    #blob 代表 GCS 中的一個檔案物件。destination_blob_name 是檔案在 GCS 中的儲存路徑和名稱。這個物件就像是你要上傳的檔案在 GCS 上的代號或位置。
    #destination_blob_name 是檔案在 GCS 儲存桶中的 "目標檔案名"，這個名稱可以包含資料夾結構（例如 folder/in/bucket/file.csv）。
    blob = bucket.blob(destination_blob_name)
    
    # 上傳檔案到 GCS ，source_file_name 是檔案在你本地設備上的路徑
    blob.upload_from_filename(source_file_name)
    print(f"文件 {source_file_name} 已成功上傳到 {bucket_name}/{destination_blob_name}。")



####第二段應該可定義為omdb_requests的function，第四段用import的方式