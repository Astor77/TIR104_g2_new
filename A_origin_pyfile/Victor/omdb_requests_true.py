import requests
import json
import pandas as pd
import math
import time

def search_id_info(a):
    requests_list = a

    api_token = 'de467a5d'
    max_requests = 5
    count_requests = 0
    result = []
    try:
        for omdb_list in requests_list:
            if count_requests >= max_requests:
                break

            url = f'http://www.omdbapi.com/?i={omdb_list}&apikey={api_token}'
            headers = {
                'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                }
            response = requests.get(url, headers=headers)
            print(f'正在請求{count_requests+1}筆')

            if response.status_code == 200:
                data = response.json()
                result.append(data)
                count_requests += 1
                time.sleep(1)

                print(f'成功請求{omdb_list}')
            else:
                print(f'請求{omdb_list}失敗')

    except requests.exceptions.RequestException as e:
        print(f'發生錯誤: {e}')

    return result


movie_dt_pd = pd.read_csv(r'/workspaces/TIR104_g2/Ａ_raw_data/tmdb_detail_raw.csv')
#可成功取出list
movie_id_list = movie_dt_pd['imdb_id'].tolist()
#去除nan值()
movie_id = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
#list id數1834
movie_len = (len(movie_id))
#前1000個，留意包前不包後
first_list = movie_id[:1000]
#剩餘834個
second_list = movie_id[1000:]

#呼叫函式
#first_info = search_id_info(first_list)

