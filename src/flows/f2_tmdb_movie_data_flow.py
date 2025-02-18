# 運用Fetching Task內的py模組處理資料抓取
# 各個API端點抓取資料，並存成json或csv

from tasks.Fetching_Task import fetch_api_data_module as fa
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_csv, save_as_json
import utils.path_config as p

# task1 e_read_mapping_result() -> 共同上游
# 讀取mapping 結果並取出tmdb_id_list
df = read_file_to_df(p.raw_tw_mapping)
tmdb_id_list = df["id"]


# task2 e_raw_detail -> 平行處理
# 抓detail
# 針對tmdb_id_list，抓取detail資訊並存入list
movie_details_raw = fa.tmdb_get_list_movie_detail(tmdb_id_list)

# task3 e_raw_release_date -> 平行處理
# 抓release_date


# task4 e_raw_credit -> 平行處理
# 抓credit
# 往下類推



# taskn l_raw_data()
# 將detail的list結果轉為json, csv存入路徑
csv_file_name = "tmdb_detail_raw.csv"
json_file_name = "tmdb_detail_raw.json"
save_as_json(movie_details_raw, csv_file_name, p.raw_tw_details)
save_as_csv(movie_details_raw, json_file_name, p.raw_tw_details)