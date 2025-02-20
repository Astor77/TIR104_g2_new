# fetch_api_data_task.py -> 整合大家用API方式獲取data的函式
# 此模組收納用API取得原始資料的function
# 建議函式結果為原始資料

import os
import time
import requests

############ 全域變數
#api_name#
TMDB_MAIN_URL = f"https://api.themoviedb.org/3/movie/"
DETAILS_API = "details"
RELEASE_DATES_API = "release_dates"
CREDITS_API = "credits"
KEYWORDS_API = "keywords"

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
    針對 movie_detail 抓取每一部電影 detail，返回 list
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


#tmdb_get_movie_release_date(台灣資料跟全球資料適用)

# 設置 API 請求的標頭
HEADERS = {
    "Authorization": f"Bearer {RAIN_TMDB_KEY}",
    "Accept": "application/json"
}


def get_release_dates(movie_id):
    """
    根據電影 ID 取得該電影的上映日期資訊。
    :param movie_id: int - 電影的 TMDB ID
    :return: list - 包含上映資訊的列表，若請求失敗則返回 None
    """
    url = f"{TMDB_MAIN_URL}{movie_id}/release_dates"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # 自動拋出 HTTP 錯誤
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[錯誤] 無法取得電影 ID {movie_id} 的上映日期: {e}")
        return None


def tmdb_get_one_movie_data(movie_id):
    """
    解析電影的上映日期資訊，過濾掉 'certification' 和 'descriptors' 欄位。
    :param movie_id: int - 電影的 TMDB ID
    :return: list - 經過過濾處理的上映日期列表
    """
    results = get_release_dates(movie_id)
    if results is None:
        return []

    for country_data in results:
        for release in country_data.get("release_dates", []):
            release.pop("certification", None)
            release.pop("descriptors", None)

    return results

print(tmdb_get_one_movie_data(550))
# print(tmdb_get_one_movie_data(550, RELEASE_DATES_API, RAIN_TMDB_KEY))



# tmdb_get_movie_credit(台灣資料跟全球資料適用)


# tmdb_get_movie_keyword(台灣資料跟全球資料適用)


# omdb_get_movie_info(台灣資料跟全球資料適用)








# # 以下是 Astor_統整tmdb版本

# def tmdb_get_one_movie_data(tmdb_id: int, api_name: str, api_key: str ,language: str="zh-TW") -> dict:
#     """
#     抓取單一 tmdb_id 的 data，返回 json
#     Args:
#         tmdb_id (int): one tmdb movie id
#         api_name (str): 需要使用的api端點名稱，可使用全域變數帶入
#         api_key (str): API_KEY 資訊，可使用全域變數帶入
#         languages (str): 查詢的語言，預設為 zh-TW
#         return: 單筆 movie detail json 資料
#     """
#     match api_name:
#         case "details":
#             url = f"{TMDB_MAIN_URL}{tmdb_id}?language={language}"
#         case "release_dates":
#             url = f"{TMDB_MAIN_URL}{tmdb_id}/{api_name}"
#         case "credits":
#             url = f"{TMDB_MAIN_URL}{tmdb_id}/{api_name}"
#         case "keywords":
#             url = f"{TMDB_MAIN_URL}{tmdb_id}/{api_name}"
#         case _:
#             print(f"{api_name}不存在，請帶入正確的api名稱")
#     try:
#         url = url
#         headers = {
#             "accept": "application/json",
#             "Authorization": f"Bearer {api_key}"
#         }
#         response = requests.get(url, headers=headers)
#         if response.status_code == 200:
#             movie_data = response.json()
#             return movie_data
#         else:
#             print(f"requests fail: {response.status_code}, tmdb_id: {tmdb_id}")
#     except Exception as err:
#         print(f"error: {err}, tmdb_id: {tmdb_id}")


# def tmdb_get_list_movies_data(tmdb_id_list: list, api_name: str, api_key: str ,language: str="zh-TW") -> list:
#     """
#     針對 movie list 抓取每一個 tmdb_id 的data，返回 list
#     Args:
#         tmdb_id_list (list): one tmdb movie id
#         api_name (str): 需要使用的api端點名稱，可使用全域變數帶入
#         api_key (str): API_KEY 資訊，可使用全域變數帶入
#         languages (str): 查詢的語言，預設為 zh-TW
#         return: 含多筆 movie data 資料的 list
#     """
#     try:
#         movies_data = []
#         for tmdb_id in tmdb_id_list:
#             tmdb_id = int(tmdb_id)
#             movies_data.append(tmdb_get_one_movie_data(tmdb_id, api_name, api_key, language))
#             time.sleep(0.5)
#         return movies_data
#     except Exception as err:
#         print(f"tmdb_id: {tmdb_id}, error: {err}")


# if __name__ == "__main__":
#     print(tmdb_get_one_movie_data(550, DETAILS_API, ASTOR_TMDB_KEY))
#     print("---------------------------------------------------------")
#     print(tmdb_get_one_movie_data(550, RELEASE_DATES_API, RAIN_TMDB_KEY))
#     print("---------------------------------------------------------")
#     print(tmdb_get_one_movie_data(550, CREDITS_API, ALLEN_TMDB_KEY))
#     print("---------------------------------------------------------")
#     print(tmdb_get_one_movie_data(550, KEYWORDS_API, JOY_TMDB_KEY))
#     print("---------------------------------------------------------")
