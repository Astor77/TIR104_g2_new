import pandas as pd
import json
from datetime import datetime

def omdb_raw_to_tmp(filename, columns):
    
    omdb_data = pd.read_json(filename)

    #取出需要轉成tmp的欄位
    omdb_tmp_data = (omdb_data[columns])
    #建立時間
    current_time = datetime.now().strftime("%Y-%m-%d")
    omdb_tmp_data["data_created_time"] = current_time
    omdb_tmp_data["data_updateded_time"] = current_time

    #存成csv
    #omdb_tmp_data.to_csv("omdb_tmp.csv", index=False)
    #print("已成功儲存檔案")


#需要導入的檔案
filename = r"C:\Users\User\Desktop\Python_note\omdb_info.json"
#需要留著的欄位
columns = ["imdbID", "imdbRating"]
#呼叫 
omdb_raw_to_tmp(filename, columns)



# tmdb_get_movie_release_date(台灣資料跟全球資料適用)
def tmdb_get_movie_release_date(tmdb_id_list: list , API_KEY: str = RAIN_TMDB_KEY) -> list:

    dir_path = p.raw_tw_tmdb_release_date
    file_name = "tmdb_release_dates_raw_20250219.json"
    file_path = dir_path / file_name
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data

    release_dates = []

    for movie in data:
        movie_id = movie.get("id")
        for result in movie.get("results"):
            movie_iso = result.get("iso_3166_1")
            for release in result.get("release_dates",[]):
                release["id"] = movie_id
                release["iso_3166_1"] = movie_iso
                release_dates.append({
                        "tmdb_id": movie_id,
                        "release_country_code": movie_iso,
                        "release_type_note": release.get("note"),
                        "type_release_date": release.get("release_date"),
                        "release_type_id": release.get("type")
                })
    release_df = pd.DataFrame(release_dates)