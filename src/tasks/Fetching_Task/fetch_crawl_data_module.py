import pandas as pd
import requests
import time

# fetch_crawl_data_task.py -> 整合用爬蟲獲取的函式
api_token = "de467a5d"
url = f"http://www.omdbapi.com/?i={movie}&apikey={api_token}"
headers = {
        "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        }


def fetch_imdb_id():  
    #路徑會可能來自gcs
    movie_id_csv = pd.read_csv("路徑")
    movie_id_list = movie_id_csv["movie_id"].tolist()   
    #去除nan值
    movie_id = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
    #計算id個數
    movie_id_len = len(movie_id)
    return movie_id, movie_id_len

def crawl_movie_info(movie_id, url, api_token, headers):

    max_requests = 1000
    count_requests = 0

    url = url
    headers = headers

    response = requests.get(url, headers)
    if response.status_code == 200:
        print("開始爬取")
























# get_numbers_movie_box(全球資料)
# 來源為the numbers