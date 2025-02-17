# 讀取v2_mapping 資料，並取出tmdb_id
import os
import requests
import pandas as pd
import json
import time

file_path = "/workspaces/TIR104_g2/A1_temp_data/tw/v2_mapping_close_true.csv"
tmdb_id = pd.read_csv(file_path)
tmdb_id = tmdb_id["tmdb_id"]


# 抓取單一 tmdb movie detail
def get_movie_keywords(tmdb_id) -> json:
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



def save_as_json(raw_detail):
    file_path = "/workspaces/TIR104_g2/A1_temp_data/tw/"
    file_name = "tmdb_keywords_raw.json"
    json_file_path = file_path + file_name
    directory = os.path.dirname(json_file_path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(raw_detail, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
# 抓取資料並存入路徑
    raw_detail = []
    for id in tmdb_id:
        raw_detail.append(get_movie_keywords(id))
        time.sleep(0.5)
    save_as_json(raw_detail)