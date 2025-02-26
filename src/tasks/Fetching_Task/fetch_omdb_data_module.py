from datetime import datetime
import math
import pandas as pd
import requests
import time
import json
from prefect import task

#API_TOKEN = "de467a5d"
API_TOKEN = "5271bd7c"

timestamp = datetime.now().strftime("%Y-%m-%d")
#第二次存檔function用
filepath = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_raw_data_2025-02-23.json"

#將details的id抓取出來
@task
def fetch_imdb_id():  
    #路徑會可能來自gcs
    details_data = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/tmdb_details/tmdb_detail_raw_20250219.json"
    with open(details_data, "r", encoding="utf-8") as f:
        movie_id_json= json.load(f)
    #movie_id_csv = pd.read_json(r"tmdb_detail_raw_20250219.json")
    movie_id_list = [movie.get("imdb_id") for movie in movie_id_json if "imdb_id" in movie]   
    #去除nan值
    movie_id = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
    print(f"已成功取得電影id")
    #print(movie_id)
    #儲存成csv供兩天查詢
    
    save_path = fr"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/tmdb_imdb_ids.csv"
    df = pd.DataFrame({"imdb_id": movie_id})
    df.to_csv(save_path, index=False, encoding="utf-8-sig")

    print(f"已成功儲存 IMDb ID 至 {save_path}")

    return movie_id

#-------------------------------------------------------------#
#第一次打API(純爬不存)
@task
def crawl_omdb_movies_data(movie_id, API_TOKEN):
    max_request = 1000
    count_requests = 0
    #儲存已求取的id的id
    results = []
    id_list = []

    for id in movie_id:
        if count_requests >= max_request:
            print(f"已請求{max_request}次請求，今日結束")
            break
        url = f"http://www.omdbapi.com/?i={id}&apikey={API_TOKEN}"
        headers = {
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            movie_info = response.json()
            #存電影資料
            results.append(movie_info)
            print(f"已將{id}資訊加入列表")
            #存這次取的id存這次取的id
            id_list.append(id)
            print(f"已將{id}加入求取紀錄")
        else:
            print("爬取失敗")

        count_requests += 1
        time.sleep(2)
        print(f"本日求取資料共{count_requests}筆")

    return results, id_list

#-------------------------------------------------------------#
#存raw_data的function存
@task
def save_data(results):
    file = fr"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_raw_data_{timestamp}.json"
    with open(file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

#-------------------------------------------------------------#
#存id的function
@task
def id_list_save(id_list):
    first_requests = fr"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/first_requests_{timestamp}.csv"
    df = pd.DataFrame({"imdb_id": id_list})
    df.to_csv(first_requests, index=False, encoding="utf-8-sig")

#-------------------------------------------------------------#



#第二次打API
@task
def crawl_omdb_movies_data_second():
    #讀全部id
    df_1 = pd.read_csv(r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/tmdb_imdb_ids.csv")
    #讀已求取過id  
    df_2 = pd.read_csv(r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/first_requests_2025-02-23.csv")
    #print(df_1, df_2)
    #取出df2不再df1內的id
    second_requests_id = df_1[~df_1["imdb_id"].isin(df_2["imdb_id"])]
    requests_id = second_requests_id["imdb_id"].tolist()
    print(requests_id)

    #呼叫function爬蟲
    result, _ = crawl_omdb_movies_data(requests_id, API_TOKEN)
    #呼叫存檔函式
    save_data_second(result, filepath)


#------------------------------------------------------------------------
#二次存檔
@task
def save_data_second(result, path):

    file = path
    # 先讀檔
    with open(file, "r") as f:
        existing_data = json.load(f)
    # 加入新的資料
    existing_data.extend(result)
    # 再存檔
    with open(file, "w") as f:
        json.dump(existing_data, f, indent=4)

#---------------------------------------------------------------------
#呼叫流程
#imdb_ids = fetch_imdb_id()
#results, id_list = crawl_omdb_movies_data(imdb_ids, API_TOKEN)
#save_data(results)
#id_list_save(id_list)


crawl_omdb_movies_data_second()


