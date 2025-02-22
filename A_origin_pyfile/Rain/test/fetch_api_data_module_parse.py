import os
import requests

# ---------------------------
# 全域變數
# ---------------------------
TMDB_BASE_URL = "https://api.themoviedb.org/3/movie/"

# 從環境變數獲取 TMDB API 金鑰，並去除可能的空白與雙引號
TMDB_API_KEY = os.getenv("RAIN_TMDB_KEY", "").strip().replace("\"", "")

if not TMDB_API_KEY:
    raise ValueError("未找到 TMDB API_KEY，請確保已設置環境變數 RAIN_TMDB_KEY")

# 設置 API 請求的標頭
HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "Accept": "application/json"
}


def get_release_dates(movie_id):
    """
    根據電影 ID 取得該電影的上映日期資訊。
    :param movie_id: int - 電影的 TMDB ID
    :return: list - 包含上映資訊的列表，若請求失敗則返回 None
    """
    url = f"{TMDB_BASE_URL}{movie_id}/release_dates"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # 自動拋出 HTTP 錯誤
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[錯誤] 無法取得電影 ID {movie_id} 的上映日期: {e}")
        return None


def parse_release_dates(movie_id):
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


if __name__ == "__main__":
    # 測試函式，預設電影 ID 為 550 (《搏擊俱樂部》)
    test_movie_id = 550
    parsed_data = parse_release_dates(test_movie_id)
    print(parsed_data)
