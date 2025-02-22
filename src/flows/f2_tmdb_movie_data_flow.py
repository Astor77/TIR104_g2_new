# 運用Fetching Task內的py模組處理資料抓取
# 各個API端點抓取資料，並存成json或csv
from tasks.Fetching_Task import fetch_api_data_module as fa
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_json
import utils.path_config as p

# task1 e_read_mapping_result() -> 共同上游
# 讀取mapping 結果並取出tmdb_id_list
dir_path = p.raw_tw_mapping
file_name = "v2_mapping_close_true.csv"
df = read_file_to_df(dir_path / file_name)
tmdb_id_list = df["id"]


# task2 e_raw_details -> 平行處理
# 抓detail
# 針對tmdb_id_list，抓取detail資訊並存入list
movie_details_raw = fa.tmdb_get_list_movies_data(tmdb_id_list, fa.DETAILS_API, fa.ASTOR_TMDB_KEY)
file_name = "tmdb_detail_raw_20250219"
json = f"{file_name}.json"
save_as_json(movie_details_raw, json, p.raw_tw_details)

# task3 e_raw_release_dates -> 平行處理
# 抓release_dates
movie_release_raw = fa.tmdb_get_list_movies_data(tmdb_id_list, fa.RELEASE_DATES_API, fa.ASTOR_TMDB_KEY)
file_name = "tmdb_release_dates_raw_20250219"
json = f"{file_name}.json"
save_as_json(movie_release_raw, json, p.raw_tw_tmdb_release_date)


# task4 e_raw_credits -> 平行處理
# 抓credits
movie_credits_raw = fa.tmdb_get_list_movies_data(tmdb_id_list, fa.CREDITS_API, fa.ASTOR_TMDB_KEY)
file_name = "tmdb_credits_raw_20250219"
json = f"{file_name}.json"
save_as_json(movie_credits_raw, json, p.raw_tw_credits)

# task4 e_raw_keywords -> 平行處理
# 抓keywords
movie_keywords_raw = fa.tmdb_get_list_movies_data(tmdb_id_list, fa.KEYWORDS_API, fa.ASTOR_TMDB_KEY)
file_name = "tmdb_keywords_raw_20250219"
json = f"{file_name}.json"
save_as_json(movie_keywords_raw, json, p.raw_tw_keywords)


