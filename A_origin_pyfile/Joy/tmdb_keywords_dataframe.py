import pandas as pd
import json
import module_save_file as ms

# Task 1: Load
# 讀取 tmdb_keywords_raw.json 
file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tmdb_keywords/tmdb_keywords_raw.json"
with open(file_path, "r", encoding="utf-8") as f:
    keywords = json.load(f)

# Task 2: Extract
# 逐一取出keyword
# df_movie_keywords 含 tmdb_id, keyword分類id, keyword 名稱

def get_keyword_list(keywords: json) -> list:
    movie_keywords_list = []
    for movie in keywords:
        id = movie["id"]
        for dict in movie["keywords"]:
            dict["tmdb_id"] = id
            movie_keywords_list.append(dict)
    return movie_keywords_list


#Task 3 Transform
#轉換成dataframe 並變更欄位名稱
def data_frame(keywords: list) -> pd.DataFrame:
    df_movie_keywords = pd.DataFrame(keywords)
    df_movie_keywords = df_movie_keywords.rename(columns={"id": "keyword_id", "name": "keyword_name"})
    df_movie_keywords = df_movie_keywords.loc[:, ["tmdb_id", "keyword_id", "keyword_name"]]
    return df_movie_keywords

if __name__ == "__main__":

#轉存為csv檔
    ms.save_as_csv(data_frame(get_keyword_list(keywords)), "tmdb_keywords.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")