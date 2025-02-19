# import os

# api_key = os.getenv("RAIN_TMDB_KEY")
# print(api_key)  # 確保變數是字串格式

import os
import requests

API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MGRjN2NlNjBjNjBkODhkMDdhMGI3OWYzY2RlNjM4OCIsIm5iZiI6MTczNjkzOTg1NC4yODEsInN1YiI6IjY3ODc5OTRlYmQ3OTNjMDM1NDRmMjE3NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.-pibOcIPWjgSH1lNk_rhCRVstS21H1hV5nEPLpAdHfE"
url = "https://api.themoviedb.org/3/movie/550/release_dates"

headers = {"Authorization": f"Bearer {API_KEY}", "accept": "application/json"}

response = requests.get(url, headers=headers)
# print(f"狀態碼: {response.status_code}")
# print(f"回應內容: {response.json()}")


def fetch_release_dates(id):

    API_KEY = os.getenv("RAIN_TMDB_KEY")
    API_KEY2 = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MGRjN2NlNjBjNjBkODhkMDdhMGI3OWYzY2RlNjM4OCIsIm5iZiI6MTczNjkzOTg1NC4yODEsInN1YiI6IjY3ODc5OTRlYmQ3OTNjMDM1NDRmMjE3NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.-pibOcIPWjgSH1lNk_rhCRVstS21H1hV5nEPLpAdHfE"

    RAIN_TMDB_KEY = os.getenv("RAIN_TMDB_KEY", "").strip().replace("\"", "")

    print(f"pi: {API_KEY == API_KEY2}")
    print(f"pi: {API_KEY}")
    print(f"pi: {API_KEY2}")

    if not API_KEY:
        raise ValueError("放置API_KEY放置API_KEY")

    headers = {"Authorization": f"Bearer {RAIN_TMDB_KEY}", "accept": "application/json"}

    url = f"https://api.themoviedb.org/3/movie/{id}/release_dates"

    print(f"url: {url}")

    response = requests.get(url, headers=headers)

    print(f"response: {response}")


    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(
            f"查詢失敗: {id}, 狀態碼: {response.status_code}, 錯誤訊息: {response.text}"
        )
        return None


print(f"回應內容: {fetch_release_dates(550)}")
