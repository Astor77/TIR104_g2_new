import pandas as pd
import opencc

import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
import utils.path_config as p
# import importlib  # Python 內建的重新載入工具
# importlib.reload(rm)  # 強制重新載入
# importlib.reload(p)  # 強制重新載入
# importlib.reload(om)  # 強制重新載入


# tmdb_details function1
def tmdb_details_merge_mapping():
    # 讀取 details_json
    details_df = rm.read_file_to_df(p.raw_tw_details, p.details_json)
    # 讀取tw_mapping_csv
    tw_tmdb_mapping_df = rm.read_file_to_df(p.raw_tw_mapping, p.mapping_csv)
    # 因為f1 的函式還沒跑更新，所以這裡需要先把mapping 換成string
    tw_tmdb_mapping_df = tw_tmdb_mapping_df.astype("string")
    tw_tmdb_mapping_df["id"] = tw_tmdb_mapping_df["id"].replace(".0", "", regex=False)
    # 把details_df的id欄位轉為string
    details_df["id"] = details_df["id"].astype("string")
    # merge 兩張表，產出帶有 tw_id 的 details
    details_merge_df = om.data_merge_left_df(tw_tmdb_mapping_df, details_df, id1="id", id2="id")
    return details_merge_df

# tmdb_details function2
def tmdb_details_trans(details_merge_df) -> pd.DataFrame:
    # 去除重複的 id
    details_merge_df = details_merge_df.drop_duplicates(subset="id")
    # id, imdb_id欄位轉換字串
    convert_dict = {"id": "string", "imdb_id": "string"}
    details_trans_df = details_merge_df.astype(convert_dict)
    return details_trans_df

# tmdb_release_dates function1
def tmdb_release_date_trans() -> pd.DataFrame:
    """
    讀取 tmdb_release_date 的 dataframe，針對特定欄位型態轉換
    """
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_tmdb_release_date, p.release_date_json)
    # 處理嵌套json結構
    release_dates = []
    for movie in json_data:
        movie_id = movie.get("id")
        for result in movie.get("results"):
            movie_iso = result.get("iso_3166_1")
            for release in result.get("release_dates",[]):
                release["id"] = movie_id
                release["iso_3166_1"] = movie_iso
                release_dates.append(release)
    # 轉為dataframe
    release_df = pd.DataFrame(release_dates)
    # type_release_date 轉換爲"%Y-%m-%d" 台灣時間
    # 確保 release_date 轉換成 datetime，並設為台灣時間
    release_df["release_date"] = pd.to_datetime(release_df["release_date"], utc=True)
    release_df["release_date"] = release_df["release_date"].dt.tz_convert("Asia/Taipei")
    release_df["release_date"] = release_df["release_date"].dt.strftime("%Y-%m-%d")  
    # 轉換為字串（YYYY-MM-DD）
    # id, type欄位轉換字串
    convert_dict = {"id": "string", "type": "string"}
    release_trans_df = release_df.astype(convert_dict)
    # 日期格式因存入csv會被轉為字串就先不轉換
    # 再次轉換 release_date 為 datetime
    release_trans_df["release_date"] = pd.to_datetime(release_trans_df["release_date"]) 
    return release_trans_df


# tmdb_movies_genres function1
def tmdb_genres_trans() -> pd.DataFrame:
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_details, p.details_json)
    # 處理嵌套json結構
    movie_genre = []
    for movie in json_data:
        tmdb_id = movie.get("id")
        for genre in movie.get("genres"):
            genre["tmdb_id"] = tmdb_id
            movie_genre.append(genre)
    # 轉為dataframe
    genres_df = pd.DataFrame(movie_genre)
    # id, tmdb_id欄位轉換字串
    convert_dict = {"tmdb_id": "string", "id": "string"}
    genres_trans_df = genres_df.astype(convert_dict)
    return genres_trans_df

# tmdb_genres_list function1
def tmdb_genres_list_trans() -> pd.DataFrame:
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_genres_list, p.genres_list_json)
    # 取出genres資料轉爲dataframe
    genres_list_df = pd.DataFrame(json_data["genres"])
    # 將genres_name的簡體中文轉繁體
    converter = opencc.OpenCC('s2t')
    genres_list_df["name"] = list(map(converter.convert, genres_list_df["name"]))
    # 依照id數字由小至大排序
    genres_list_df = genres_list_df.sort_values(by="id", ascending=True).reset_index(drop=True)
    # 將分類id轉為字串
    convert_dict = {"id": "string"}
    genres_list_trans_df = genres_list_df.astype(convert_dict)
    return genres_list_trans_df


# tmdb_keywords function1
def tmdb_keywords_trans() -> pd.DataFrame:
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_keywords, p.keywords_json)
    # 處理嵌套json結構
    movie_keywords = []
    for movie in json_data:
        movie_id = movie["id"]
        for keyword in movie.get("keywords"):
            keyword["tmdb_id"] = movie_id
            movie_keywords.append(keyword)
    # 轉為dataframe
    keyword_df = pd.DataFrame(movie_keywords)
    # 將tmdb_id欄位轉為字串
    convert_dict = {"tmdb_id": "string"}
    keyword_trans_df = keyword_df.astype(convert_dict)
    return keyword_trans_df

# tmdb_casts_top5 function1
def tmdb_casts_top5_trans() -> pd.DataFrame:
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_credits, p.credits_json)
    # 處理嵌套json結構
    casts_top5 = []
    for movie in json_data:
        movie_id = movie.get("id")
        for cast in movie.get("cast", [])[:5]:
            cast["tmdb_id"] = movie_id
            casts_top5.append(cast)
    # 轉為dataframe
    casts_top5_df = pd.DataFrame(casts_top5)
    # 將tmdb_id欄位轉為字串
    convert_dict = {"tmdb_id": "string", "id": "string"}
    casts_top5_trans_df = casts_top5_df.astype(convert_dict)
    return casts_top5_trans_df

# tmdb_directors function1
def tmdb_directors_trans() -> pd.DataFrame:
    # 讀取檔案
    json_data = rm.read_json_file_to_json(p.raw_tw_credits, p.credits_json)
    # 處理嵌套json結構
    directors = []
    for movie in json_data:
        movie_id = movie.get("id")
        for crew in movie.get("crew"):
            if crew.get("job") == "Director":
                crew["tmdb_id"] = movie_id
                directors.append(crew)
    # 轉為dataframe
    directors_df = pd.DataFrame(directors)
    # 將tmdb_id、id欄位轉為字串
    convert_dict = {"tmdb_id": "string", "id": "string"}
    directors_trans_df = directors_df.astype(convert_dict)
    return directors_trans_df


# tmdb_person function1
# 合併cast 跟 director兩張表，drop_duplicate，成爲person表
def tmdb_person_trans(casts_top5_trans_df, directors_trans_df):
    # concat兩張dataframe
    person_df = pd.concat([casts_top5_trans_df, directors_trans_df])
    # 去除重複的person_id
    person_df = person_df.drop_duplicates(subset="id")
    # 將id, gender欄位轉為字串
    convert_dict = {"id": "string", "gender": "string"}
    person_trans_df = person_df.astype(convert_dict)
    return person_trans_df

