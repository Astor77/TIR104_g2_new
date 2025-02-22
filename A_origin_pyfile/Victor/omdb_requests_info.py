import math
import pandas as pd
import requests
import time

API_TOKEN = "de467a5d"

#抓取detail的id抓取detail的id
def fetch_imdb_id():  
    #路徑會可能來自gcs
    movie_id_csv = pd.read_csv(r"tmdb_detail_raw.csv")
    movie_id_list = movie_id_csv["imdb_id"].tolist()   
    #去除nan值
    movie_id = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
    print(f"已成功取得電影id")
    
    return movie_id


#omdb爬蟲函式
def crawl_omdb_movies_data(movie_id, API_TOKEN):

    max_request = 1000
    count_requests = 0
    results = []
    exsiting_id = []

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
            results.append(movie_info)
            print(f"已將{id}資訊加入列表")
        
            exsiting_id.append(id)
            print(f"已將{id}加入求取紀錄")

            
        else:
            print("爬取失敗")
        
        count_requests += 1
        time.sleep(2)
        print(f"本日求取資料共{count_requests}筆")
    #時間戳記
    timestamp = datetime.now().strftime("%Y-%m-%d")
    file = f"omdb_raw_data_{timestamp}.json"

     #儲存檔案，儲存成raw_data  
    with open(file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    


    return results, existing_id

#二次抓取，設定好條件call crawl_omdb_movies_data_second()
def crawl_omdb_movies_data_second(movie_id, exsiting_id):

    second_results = []

    for i in movie_id:

        if i in exsiting_id:
            print(f"{i}已求取過")
            continue
        #儲存尚未求取過的id_list
        second_results.append(i)
        
    #叫函式呼叫第二次
    second_requests_data, second_existing_id = crawl_omdb_movies_data(second_results, API_TOKEN)
    print({second_requests_data}, {second_existing_id})
