import pandas as pd
from prefect import flow, task
from tasks.Mapping_Task import selenium_data_module as mselenium
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_csv,save_as_json
import utils.path_config as p

# task 1
#讀取全國年度合併資料
@task
def e_tw_read_csv() -> pd.DataFrame:
    dir_path = p.raw_tw_2022_2025
    file_name = "TWMovie2022-2025.csv"
    file_path = dir_path / file_name
    dfTWMovie = read_file_to_df(file_path)
    return dfTWMovie


# task 2
# get_tw_one_movie_sale(台灣資料)
# 單片查詢票房json檔案
@task
def e_get_tw_one_movie_sale(MovieIds: list) -> None:
    mselenium.download_rename(MovieIds)

# task 3
# 增加id欄位
@task
def e_tw_one_movie_sale_add_id(MovieIds: list) -> None:
    mselenium.add_id_column(MovieIds)

#task 4
#合併所有單片查詢json檔案
@task
def t_concat_tw_one_movie_json(folder_path: str) -> pd.DataFrame:
    merged_tw_one = mselenium.concat_tw_one_jsonfile(folder_path)
    return merged_tw_one

#task 5
#儲存成csv
@task
def save_tw_one_movie_sale(merged_tw_one: object) -> None:
    save_as_csv(merged_tw_one, "TWMovie_weekly_data.csv", p.raw_tw_weekly)
    print(f"單片查詢已儲存到: {p.raw_tw_weekly}")

# task 6
# 台灣上映日期
# 抓取全國單片查詢上顯示的release date
@task
def e_get_tw_one_movie_release_date(MovieIds: list) -> list:
    release_date = mselenium.get_release_date(MovieIds)
    return release_date

# task 7
#儲存成csv
@task
def save_tw_one_movie_release_date(release_date) -> None:
    save_as_csv(release_date, "release_date.csv", p.raw_tw_tmdb_release_date)
    print(f"台灣上映日期已儲存到: {p.raw_tw_tmdb_release_date}")

@flow(name="f4_sele_movie_data_flow")
def sele_movie_data_flow() -> None:
    dfTWMovie = e_tw_read_csv()
    MovieIds = dfTWMovie["MovieId"].loc[0:1]
    e_get_tw_one_movie_sale(MovieIds)
    e_tw_one_movie_sale_add_id(MovieIds)
    merged_tw_one = t_concat_tw_one_movie_json(p.raw_tw_sales)
    save_tw_one_movie_sale(merged_tw_one)
    release_date_list = e_get_tw_one_movie_release_date(MovieIds)
    save_tw_one_movie_release_date(release_date_list)

if __name__ == "__main__":
    sele_movie_data_flow()


