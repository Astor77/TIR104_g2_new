import pandas as pd
import json
import module_save_file as ms

file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tmdb_keywords/tmdb_keywords_raw.json"
with open(file_path, "r", encoding="utf-8") as f:
    keywords = json.load(f)

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


if __name__ == "__main__":
    df_movie_keywords = pd.DataFrame(get_keyword_list(keywords))
    df_movie_keywords = df_movie_keywords.rename(columns={"id": "keyword_id", "name": "keyword_name"})
    df_movie_keywords = df_movie_keywords.loc[:, ["tmdb_id", "keyword_id", "keyword_name"]]

    ms.save_as_csv(df_movie_keywords, "tmdb_keywords.csv", "/workspaces/TIR104_g2/A1_temp_data/tw")