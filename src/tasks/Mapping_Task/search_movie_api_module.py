# search_movie_api_task.py 是依據（台灣電影名稱）query tmdb的搜尋結果
import os
import time
import urllib
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
############

# tmdb_search_results(台灣資料)
# 取得單個關鍵字的第一頁搜尋結果，用於mapping function
def tmdb_search_results(query: str, language: str="zh-TW", API_KEY: str = ASTOR_TMDB_KEY) -> list:
    """
    取得tmdb搜尋“單個關鍵字”，返回“單個關鍵字”第一頁所有結果的list
    Args:
        query (str): 搜尋的關鍵字
        languages (str): 查詢的語言，預設為 zh-TW
        API_KEY (str): API KEY 資訊，預設為 ASTOR 的API
        return: 單個關鍵字搜尋的，多筆結果資料 list
    """
    query_to_unicode = urllib.parse.quote(query)
    try:
        url = f"https://api.themoviedb.org/3/search/movie?query={query_to_unicode}&language={language}&region=TW&page=1"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {API_KEY}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            search = response.json()
            if search["results"]:
                search_results = search["results"]
                return search_results
            else:
                print(f"關鍵字：{query}，查無相關結果")
        else:
            print(f"requests fail: {response.status_code}, query: {query}")
    except Exception as err:
        print(f"function: tmdb_search_results -> error: {err}")


# tmdb_list_search_results(台灣資料)
# 取得多個關鍵字的第一頁搜尋結果，用於mapping function
def tmdb_list_search_results(query_list: list, language: str="zh-TW", API_KEY: str = ASTOR_TMDB_KEY) -> list:
    """
    取得tmdb搜尋“多個關鍵字”，返回“多個關鍵字”第一頁所有結果的list
    Args:
        query_list (list): 搜尋的多個關鍵字
        languages (str): 查詢的語言，預設為 zh-TW
        API_KEY (str): API KEY 資訊，預設為 ASTOR 的API
        return: 多個關鍵字搜尋的，多筆結果資料 list
    """
    try:
        total_search_results = []
        for query in query_list:
            search_results = tmdb_search_results(query, language, API_KEY)
            if search_results:
                total_search_results.extend(search_results)
            time.sleep(0.5)
        return total_search_results
    except Exception as err:
        print(f"function: tmdb_list_search_results -> error: {err}")


if __name__ == "__main__":
    print(tmdb_search_results("星際效應"))