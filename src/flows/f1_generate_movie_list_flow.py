from prefect import task, flow
from tasks.Mapping_Task import map_movie_data_module as mmap, search_movie_api_module as msearch, selenium_data_module as mselenium
from tasks.Storage_Task.save_file_module import save_as_csv,save_as_json
import utils.path_config as p

# task X 
# 運用selenium_data_task.py（Selenium 抓年度票房資料）
# 2022-2025年資料
year_list = [2022, 2023]
# 2025年更新日期
date = "02-10"

# 清洗原始json檔案
dir_path = p.raw_tw_year_sales
file_name = "2022年票房資料_raw.json"
file_name_new = "2022年票房資料.json"
file_path = dir_path / file_name

# task 1
#下載全國2022-2025的年票房資料json檔案
@task
def get_tw_annual_sales(year_list: list, date: str) -> None:
    mselenium.download_annual_rename(year_list, date)
    cleaned_data = mselenium.clean_json_file(file_path)



    if cleaned_data:
        save_as_json(mselenium.extract_json(cleaned_data), file_name_new, p.raw_tw_year_sales)
        print(f"清理後的 JSON 已儲存到: {file_path}")

# task X e_tmdb_query_result()
# 運用search_movie_api_task.py（用 API 進行搜尋
# 並將結果存於指定路徑
query_list = mmap.get_tw_movie_clean_name_list()
total_search_results = msearch.tmdb_list_search_results(query_list)




# task X t_mapping_result()
# 運用map_movie_data_task.py（把tw_annual跟tmdb的搜尋結果整合成完整名單）
tw = mmap.get_og_tw_annual_df()
tmdb = mmap.pd.DataFrame(total_search_results)
mmap.clean_tw_tmdb_map_column(tw, tmdb)
df_mapping_result = mmap.merge_two_df(df1=tw, df2=tmdb, join="left", df1_col="Name_map", df2_col="title_map")


# task X l_save_raw_data()
# 把剛剛搜尋的結果存成json
file_name = "tw_search_results_raw.json"
save_as_json(total_search_results, file_name, p.raw_tw_search)

# mapping的["Year", "MovieId", "Name", "id"]結果存為單一值欄位的csv
df_mapping_select = mmap.drop_not_necessary(df_mapping_result)
file_name = "v2_mapping_close_true.csv"
save_as_csv(df_mapping_select, file_name, p.raw_tw_mapping)




# task X l_raw_data_upload_gcs
# 運用gcs_task.py
# 把 selenium 年度、單片查詢、search的json結果放上GCS -> raw_data
# 將mapping的csv放上GCS -> temp_data



