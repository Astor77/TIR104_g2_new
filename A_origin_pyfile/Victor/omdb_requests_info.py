from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
import requests
import math
import pandas as pd


def omdb_get():

    
    movie_detail_pd = pd.read_csv(r'C:\Users\User\Desktop\my-repo\read\tmdb_detail_raw.csv')

    #轉成list
    movie_id_list = movie_detail_pd['imdb_id'].tolist()
    #去除list內的nan值(型態為float)
    movie_id_list = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
    #將第一次求取的id list取出來
    try:
        with open('omdb_1000.json', 'r', encoding='utf-8') as file:
            omdb_data_first = json.load(file)
            omdb_ids1 = [movie['imdbID'] for movie in omdb_data_first]
    except (FileNotFoundError, json.JSONDecodeError):
        omdb_data_first = []
        print('讀取失敗')


    output_file = "omdb_1000.json"
    #第一求取
    first_1000_movies = []
    #第二次求取
    second_1000_movies = []

    #api token
    api_token = 'de467a5d'

    #設定最大請求次數
    max_requests = 1000
    requests_count = 0

    for omdb_list in movie_id_list:        
        if omdb_list in omdb_ids1:
            print(f'{omdb_list}已請求過')
            continue

        if requests_count >= max_requests:
            break  
    
        url = f'http://www.omdbapi.com/?i={omdb_list}&apikey={api_token}'
        headers = {
            'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
    
        response = requests.get(url, headers=headers)
        #需要api key才能請求成功

        print(f"正在請求第 {requests_count + 1} 筆：{omdb_list}")

        if response.status_code == 200:
            data = response.json()
            
            # 確保回應是成功的
            if data.get("Response") == "True":
                second_1000_movies.append(data)
                print(f"取得 {omdb_list}，已存入 JSON 清單。")
            else:
                print(f"取得 {omdb_list} 失敗: {data.get('Error')}")

        else:
            print(f"失敗，狀態碼: {response.status_code}")


        requests_count += 1
        time.sleep(1)  # 避免請求過快
    #有成功存取的movie_id
    all_movies = omdb_data_first + second_1000_movies

    # 儲存所有電影資訊到 JSON 檔案
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_movies, f, indent=4, ensure_ascii=False)

    print(f"二次請求{len(second_1000_movies)}，共存入{len(all_movies)}所有資料已存入 {output_file}")
        
        
omdb_get()    