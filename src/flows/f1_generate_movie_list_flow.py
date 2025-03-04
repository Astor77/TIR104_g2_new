import pandas as pd
import json
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import importlib  # Python 內建的重新載入模組工具

import tasks.Mapping_Task.map_movie_data_module as map
import tasks.Mapping_Task.search_movie_api_module as search
import tasks.Storage_Task.read_file_module as rm
import tasks.Storage_Task.save_file_module as sm
import tasks.Transform_Task.other_module as om
import utils.path_config as p
import utils.notifier as no

importlib.reload(rm)  # 強制重新載入
importlib.reload(sm)  # 強制重新載入

@task
def e_tw_annual_df() -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"正在讀取台灣年度電影不重複資料...")
        tw_annual_df = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
        # 清理台灣電影名稱，並存入新建立"Name_search"欄位
        tw_annual_df = map.clean_tw_movie_name(tw_annual_df)

        logger.info(f"✅ 清理台灣電影片名資料並返回")
        return tw_annual_df

    except Exception as err:
        logger.error(f"❌ e_tw_annual_df() 執行失敗: {err}")
        no.send_line_notification(f"e_tw_annual_df", str(err))


@task
def e_tmdb_query_result(tw_annual_df) -> json:
    logger = get_run_logger()
    try:
        logger.info(f"正在請求 API: search...")
        query_list = tw_annual_df["Name_search"]
        tmdb_search_results = search.tmdb_list_search_results(query_list)

        if tmdb_search_results:
            logger.info(f"✅ 成功取得 search 的資料！")
            return tmdb_search_results
        else:
            logger.error(f"❌ 取得 search 失敗！")

    except Exception as err:
        logger.error(f"❌ e_tmdb_query_result() 執行失敗: {err}")
        no.send_line_notification(f"e_tmdb_query_result", str(err))

@task
def e_mapping_tw_tmdb_result(tw_annual_df, tmdb_search_results) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"正在比對台灣電影及TMDB搜尋結果...")
        # 將search結果轉換成dataframe
        tmdb_search_df = pd.DataFrame(tmdb_search_results)

        # 針對兩張df欲比較的欄位取出去除空白、轉換一致大小寫
        tw_annual_df["Name_map"] = tw_annual_df["Name_search"].apply(map.normalize_text)
        tmdb_search_df["title_map"] = tmdb_search_df["title"].apply(map.normalize_text)

        # 因為搜尋結果，會有重複電影名稱，僅保留第一筆
        tmdb_search_df.drop_duplicates(subset=["title_map"])

        # 將兩張表df merge
        mapping_result_df = om.data_merge_left_df(
            df1=tw_annual_df,
            df2=tmdb_search_df,
            id1="Name_map",
            id2="title_map"
            )

        logger.info(f"✅ 比對成功，返回比對結果")
        return mapping_result_df

    except Exception as err:
        logger.error(f"❌ e_mapping_tw_tmdb_result() 執行失敗: {err}")
        no.send_line_notification(f"e_mapping_tw_tmdb_result", str(err))

@task
def t_mapping_df(mapping_result_df) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"正在轉換比對結果...")

        columns = ["Year", "MovieId", "Name", "id"]
        mapping_df = om.get_spec_cloumn_df(mapping_result_df, columns)
        mapping_df["id"] = (
            mapping_df["id"]
            .astype(str)
            .str.replace(".0", "", regex=False)  # 移除 .0
            .replace("nan", pd.NA)  # 把 "nan" 轉 Pandas 的 NA
        )
        logger.info(f"✅ 轉換成功，返回轉換結果")
        return mapping_df

    except Exception as err:
        logger.error(f"❌ t_mapping_df() 執行失敗: {err}")
        no.send_line_notification(f"t_mapping_df", str(err))



@task
def l_save_raw_data(data, dir_path, file_name) -> None:
    logger = get_run_logger()
    try:
        if file_name.endswith(".csv"):
            logger.info(f"正在存為csv檔案...")
            sm.save_as_csv(data, dir_path, file_name)

        elif file_name.endswith(".json"):
            logger.info(f"正在存為json檔案...")
            sm.save_as_json(data, dir_path, file_name)

    except Exception as err:
        logger.error(f"❌ l_save_raw_data() 執行失敗，錯誤：\n{err}")
        no.send_line_notification(f"l_save_raw_data:{file_name}", str(err))



@flow(task_runner=ConcurrentTaskRunner())
def f1_generate_movie_list_flow():
    logger = get_run_logger()
    try:
        logger.info(f"f1_generate_movie_list_flow() 開始運行...")

        tw_annual_df = e_tw_annual_df()

        tmdb_search_results = e_tmdb_query_result(tw_annual_df)
        l_save_raw_data(tmdb_search_results, p.raw_tw_search, p.search_json)

        mapping_result_df = e_mapping_tw_tmdb_result(tw_annual_df, tmdb_search_results)
        mapping_df = t_mapping_df(mapping_result_df)
        l_save_raw_data(mapping_df, p.raw_tw_mapping, p.mapping_csv)

        logger.info(f"✅ f1_generate_movie_list_flow() 執行完畢")

    except Exception as err:
        logger.error(f"❌ f1_generate_movie_list_flow() 執行失敗，錯誤：\n{err}")

#本機運行測試
if __name__ == "__main__":
        f1_generate_movie_list_flow()