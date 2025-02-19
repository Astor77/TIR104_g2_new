import pandas as pd
from prefect import flow, task
from tasks.Mapping_Task import selenium_data_module as mselenium
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_csv,save_as_json
import utils.path_config as p

# task 1
#讀取
@task
def e_tw_read_csv2() -> pd.DataFrame:
    dir_path = p.raw_tw_2022_2025
    file_name = "TWMovie2022-2025.csv"
    file_path = dir_path / file_name
    dfTWMovie = read_file_to_df(file_path)
    return dfTWMovie


# task 2
# get_tw_one_movie_sale(台灣資料)
# 單片查詢票房json檔案
@task
def get_tw_one_movie_sale(MovieIds: list) -> None:
    mselenium.download_rename(MovieIds)
    mselenium.add_id_column(MovieIds)




# task X
# get_tw_one_movie_release_date(台灣資料)
# 台灣上映日期
# 抓取全國單片查詢上顯示的release date
@task
def get_tw_one_movie_release_date(MovieIds: list):
    release_date = mselenium.get_release_date(MovieIds)
    save_as_csv(release_date, "release_date.csv", p.raw_tw_tmdb_release_date)
    print(f"台灣上映日期已儲存到: {p.raw_tw_tmdb_release_date}")

@flow(name="f4_sele_movie_data_flow")
def sele_movie_data_flow() -> None:
    dfTWMovie = e_tw_read_csv2()
    MovieIds = dfTWMovie["MovieId"].loc[0:1]
    get_tw_one_movie_sale(MovieIds)
    get_tw_one_movie_release_date(MovieIds)

if __name__ == "__main__":
    sele_movie_data_flow.run()


