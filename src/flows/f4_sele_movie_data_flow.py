import pandas as pd
from prefect import flow, task, get_run_logger
from tasks.Mapping_Task import selenium_data_module as mselenium
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_csv
import utils.path_config as p

# task 1
# 讀取全國年度合併資料
@task
def e_tw_read_csv() -> pd.DataFrame:
    logger = get_run_logger()
    try:
        dir_path = p.raw_tw_2022_2025
        file_name = "TWMovie2022-2025.csv"
        dfTWMovie = read_file_to_df(dir_path, file_name)
        logger.info(f"成功讀取 CSV: {file_name}")
        return dfTWMovie
    except Exception as e:
        logger.error(f"讀取 CSV 失敗: {e}")
        raise

# task 2
# 單片查詢票房 json 檔案
@task
def e_get_tw_one_movie_sale(MovieIds: list) -> None:
    logger = get_run_logger()
    try:
        mselenium.download_rename(MovieIds)
        logger.info(f"成功下載單片票房資料: {MovieIds}")
    except Exception as e:
        logger.error(f"下載單片票房資料失敗: {e}")
        raise

# task 3
# 增加 id 欄位
@task
def e_tw_one_movie_sale_add_id(MovieIds: list) -> None:
    logger = get_run_logger()
    try:
        mselenium.add_id_column(MovieIds)
        logger.info(f"成功為單片票房資料新增 ID 欄位: {MovieIds}")
    except Exception as e:
        logger.error(f"新增 ID 欄位失敗: {e}")
        raise

# task 4
# 合併所有單片查詢 json 檔案
@task
def t_concat_tw_one_movie_json(folder_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        merged_tw_one = mselenium.concat_tw_one_jsonfile(folder_path)
        logger.info(f"成功合併單片票房 JSON 檔案: {folder_path}")
        return merged_tw_one
    except Exception as e:
        logger.error(f"合併單片票房 JSON 失敗: {e}")
        raise

# task 5
# 儲存成 csv
@task
def save_tw_one_movie_sale(merged_tw_one: object) -> None:
    logger = get_run_logger()
    try:
        save_as_csv(merged_tw_one, p.raw_tw_weekly, "TWMovie_weekly_data.csv")
        logger.info(f"單片查詢已儲存到: {p.raw_tw_weekly}/TWMovie_weekly_data.csv")
    except Exception as e:
        logger.error(f"儲存單片票房 CSV 失敗: {e}")
        raise

# task 6
# 抓取全國單片查詢的 release date
@task
def e_get_tw_one_movie_release_date(MovieIds: list) -> list:
    logger = get_run_logger()
    try:
        release_date = mselenium.get_release_date(MovieIds)
        logger.info(f"成功獲取上映日期: {MovieIds}")
        return release_date
    except Exception as e:
        logger.error(f"獲取上映日期失敗: {e}")
        raise

# task 7
# 儲存上映日期成 CSV
@task
def save_tw_one_movie_release_date(release_date) -> None:
    logger = get_run_logger()
    try:
        save_as_csv(release_date, p.raw_tw_tmdb_release_date, "release_date.csv")
        logger.info(f"台灣上映日期已儲存到: {p.raw_tw_tmdb_release_date}/release_date.csv")
    except Exception as e:
        logger.error(f"儲存上映日期 CSV 失敗: {e}")
        raise

@flow(name="f4_sele_movie_data_flow")
def sele_movie_data_flow() -> None:
    logger = get_run_logger()
    try:
        dfTWMovie = e_tw_read_csv()
        MovieIds = dfTWMovie["MovieId"].loc[0:1].tolist()
        e_get_tw_one_movie_sale(MovieIds)
        e_tw_one_movie_sale_add_id(MovieIds)

        merged_tw_one = t_concat_tw_one_movie_json(p.raw_tw_sales)
        save_tw_one_movie_sale(merged_tw_one)

        release_date_list = e_get_tw_one_movie_release_date(MovieIds)
        save_tw_one_movie_release_date(release_date_list)

        logger.info("所有任務成功執行，單片票房數據處理完成")
    except Exception as e:
        logger.error(f"Flow 執行失敗: {e}")
        raise

if __name__ == "__main__":
    sele_movie_data_flow()
