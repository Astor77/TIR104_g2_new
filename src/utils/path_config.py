# 存放資料分層的路徑
from pathlib import Path

#raw_data資料夾路徑
##tw資料夾路徑
raw_tw = Path("/workspaces/TIR104_g2_new/A0_raw_data/tw/")
raw_tw_numbers = raw_tw / "numbers_box"
raw_tw_omdb_info = raw_tw / "omdb_info"
raw_tw_details = raw_tw / "tmdb_details"
raw_tw_details_en = raw_tw / "tmdb_details_en"
raw_tw_keywords = raw_tw / "tmdb_keywords"
raw_tw_tmdb_release_date = raw_tw / "tmdb_release_date"
raw_tw_credits = raw_tw / "tmdb_credits"
raw_tw_mapping = raw_tw / "tw_mapping_tmdb"
raw_tw_2022_2025 = raw_tw / "tw_movie_2022-2025"
raw_tw_sales = raw_tw / "tw_movie_sales"
raw_tw_weekly = raw_tw / "tw_movie_weekly"
raw_tw_year_sales = raw_tw / "tw_movie_year_sales"
raw_tw_publisher = raw_tw / "tw_publisher"
raw_tw_tw_release_date = raw_tw / "tw_release_dates"
raw_tw_search = raw_tw / "tw_search"
raw_tw_genres_list = raw_tw / "tmdb_genres_list"


## file檔案名稱:統一
mapping_csv = "tw_tmdb_mapping.csv"
details_json = "tmdb_details.json"
omdb_info_json = "omdb_info.json"
credits_json = "tmdb_credits.json"
keywords_json = "tmdb_keywords.json"
release_date_json = "tmdb_release_dates.json"
credits_json = "tmdb_credits.json"
search_json = "tmdb_search_results.json"
genres_list_json = "tmdb_genres_list.json"

tw_annual_dup_csv = "TWMovie2022-2025_raw.csv"
tw_annual_not_dup_csv = "TWMovie2022-2025.csv"
tw_release_date_csv = "tw_release_dates.csv"
tw_weekly_csv = "TWMovie_weekly_data.csv"
tw_weekly2_csv = "TWMovie_weekly_data2.csv"



#temp_data 資料夾路徑
temp = Path("/workspaces/TIR104_g2_new/A1_temp_data/")
temp_tw = temp / "tw"
temp_global = temp / "global"

## file檔案名稱:統一
details_csv = "tmdb_details.csv"
release_date_csv = "tmdb_release_dates.csv"
cast_top5_csv = "tmdb_cast_top5.csv"
director_csv = "tmdb_directors.csv"
person_csv = "tmdb_person.csv"
keywords_csv = "tmdb_keywords.csv"
genres_csv = "tmdb_movie_genres.csv"
genres_list_csv = "tmdb_genres_list.csv"
omdb_info_csv = "omdb_info.csv"

tw_annual_csv = "TWMovie_annual_df3.csv"
tw_details_csv = "TWMovie_details.csv"
tw_weekly_csv = "TWMovie_weekly_df2.csv"



#final_data 資料夾路徑
final = Path("/workspaces/TIR104_g2_new/A2_final_data/")
final_tw = final / "tw"
final_global = final / "global"


#GCS_path
##
##
##


# log資料夾路徑
logs = Path("/workspaces/TIR104_g2_new/project_logs/")

# log檔案路徑
## save_log
save_file_log = logs / "save_file.log"