# fetch_api_data_task.py -> 整合大家用API方式獲取data的函式
# 此模組收納用API取得原始資料的function
# 建議函式結果為原始資料

import os
import time
import requests

############ 全域變數
TMDB_MAIN_URL = f"https://api.themoviedb.org/3/movie/"

# 取得環境變數中的API_KEY，並去除空白及"符號
# 如果getenv失敗會返回空字串
ASTOR_TMDB_KEY = os.getenv("ASTOR_TMDB_KEY", "").strip().replace("\"", "")
RAIN_TMDB_KEY = os.getenv("RAIN_TMDB_KEY", "").strip().replace("\"", "")
ALLEN_TMDB_KEY = os.getenv("ALLEN_TMDB_KEY", "").strip().replace("\"", "")
JOY_TMDB_KEY = os.getenv("JOY_TMDB_KEY", "").strip().replace("\"", "")
VICTOR_OMDB_KEY = os.getenv("VICTOR_OMDB_KEY", "").strip().replace("\"", "")

# 檢查API_KEY帶入的實際內容
# print(repr(os.getenv("ASTOR_TMDB_KEY")))
############

# tmdb_get_one_movie_detail()
# 抓取單部電影 detail，結果存回一個dict
def tmdb_get_one_movie_detail(tmdb_id: int, language: str="zh-TW", API_KEY: str = ASTOR_TMDB_KEY) -> dict:
    """
    抓取單一 tmdb_id 的 detail，返回 json
    Args:
        tmdb_id (int): one tmdb movie id
        languages (str): 查詢的語言，預設為 zh-TW
        API_KEY (str): API KEY 資訊，預設為 ASTOR 的API
        return: 單筆 movie detail json 資料
    """
    try:
        url = f"{TMDB_MAIN_URL}{tmdb_id}?language={language}"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {API_KEY}"
        }
        response = requests.get(url, headers=headers)
        print(response.request.headers)
        if response.status_code == 200:
            movie_detail = response.json()
            return movie_detail
        else:
            print(f"requests fail: {response.status_code}, tmdb_id: {tmdb_id}")
    except Exception as err:
        print(f"error: {err}, tmdb_id: {tmdb_id}")


# tmdb_get_list_movie_detail()
# 針對 movie list 抓取每一部電影 detail，結果存回一個list
def tmdb_get_list_movie_detail(tmdb_id_list: list, language: str="zh-TW", API_KEY: str = ASTOR_TMDB_KEY) -> list:
    """
    針對 movie list 抓取每一部電影 detail，返回 list
    Args:
        tmdb_id_list (list): one tmdb movie id
        languages (str): 查詢的語言，預設為 zh-TW
        API_KEY (str): API KEY 資訊，預設為 ASTOR 的API
        return: 含多筆 movie detail 資料的 list
    """
    try:
        movie_details = []
        for tmdb_id in tmdb_id_list:
            tmdb_id = int(tmdb_id)
            movie_details.append(tmdb_get_one_movie_detail(tmdb_id, language, API_KEY))
            time.sleep(0.5)
        return movie_details
    except Exception as err:
        print(f"tmdb_id: {tmdb_id}, error: {err}")


# tmdb_get_movie_release_date(台灣資料跟全球資料適用)

def tmdb_get_movie_release_date(id):

    API_KEY = os.getenv("RAIN_TMDB_KEY")

    RAIN_TMDB_KEY = os.getenv("RAIN_TMDB_KEY", "").strip().replace("\"", "")

    if not API_KEY:
        raise ValueError("放置API_KEY放置API_KEY")

    headers = {"Authorization": f"Bearer {RAIN_TMDB_KEY}", "accept": "application/json"}

    url = f"https://api.themoviedb.org/3/movie/{id}/release_dates"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(
            f"查詢失敗: {id}, 狀態碼: {response.status_code}, 錯誤訊息: {response.text}"
        )
        return None

# tmdb_get_movie_credit(台灣資料跟全球資料適用)


# tmdb_get_movie_keyword(台灣資料跟全球資料適用)


# omdb_get_movie_info(台灣資料跟全球資料適用)




if __name__ == "__main__":
    print(tmdb_get_one_movie_detail(550))
    print(tmdb_get_movie_release_date(550))
