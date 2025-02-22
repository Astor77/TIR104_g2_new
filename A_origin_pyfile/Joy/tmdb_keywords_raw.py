# 讀取v2_mapping 資料，並取出tmdb_id
import os
import requests
import pandas as pd
import json
import time




# Task 1: Load
# 讀取V2 mapping 資料
def read_csv(file_path: str) -> pd.DataFrame:
    """
    讀取指定路徑的 CSV 檔案並轉換成 DataFrame。
    Args:
        file_path (str): 檔案路徑（含檔案名稱、副檔名）
    Returns:
        轉換後的 Pandas DataFrame
    """
    try:
        df = pd.read_csv(file_path, engine="python")
    except Exception as err:
        print(f"讀取 {file_path} 時發生錯誤: {err}")
        return None
    return df

#Task 2 Extract
# 抓取單一 tmdb movie detail
def get_movie_keywords(tmdb_id: list) -> json:
    """
    tmdb_id: tmdb movie id
    return json for movie detail info
    """
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/keywords"
        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxOTE5MmIwZWIzNjgwNjIwYWE3NDcwMWE4YTViZDg5MiIsIm5iZiI6MTczNjQzMzI2MS40Miwic3ViIjoiNjc3ZmRlNmRhNjc3OGFhNWIzN2IzODA3Iiwic2NvcGVzIjpbImFwaV9yZWFkIl0sInZlcnNpb24iOjF9.rfRQ88t548dSCPuFACgjwM2sp0WlblKQlkup0E7gGSE"
        }
        response = requests.get(url, headers=headers).json()
        return response
    except Exception as err:
        print(f"{tmdb_id} encounter {err}")

def  get_keywords_list(tmdb_id: list) -> list:
    keywords_list = []
    for id in tmdb_id:
        keywords = get_movie_keywords(id)
        keywords_list.append(keywords)
        time.sleep(0.5)
    return keywords_list


#Task 3 Transform
def save_as_json(raw_detail):
    file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tmdb_keywords/"
    file_name = "tmdb_keywords_raw2.json"
    json_file_path = file_path + file_name
    directory = os.path.dirname(json_file_path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(raw_detail, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
# 抓取資料並存入路徑
    file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_mapping_tmdb/v2_mapping_close_true.csv"
    df_tmdb_id = read_csv(file_path)
    tmdb_id = df_tmdb_id["id"]
    print(tmdb_id)

    save_as_json(get_keywords_list(tmdb_id))