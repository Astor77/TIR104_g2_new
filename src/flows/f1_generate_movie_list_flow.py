import pandas as pd
import json
from prefect.runtime import deployment
from prefect import get_run_logger, task, flow
from prefect.task_runners import ConcurrentTaskRunner
import importlib  # Python 內建的重新載入模組工具

import tasks.Mapping_Task.map_movie_data_module as map
import tasks.Mapping_Task.search_movie_api_module as search
import tasks.Storage_Task.read_file_module as rm
import tasks.Storage_Task.save_file_module as sm
import tasks.Transform_Task.other_module as om
import utils.path_config as p

importlib.reload(rm)  # 強制重新載入
importlib.reload(sm)  # 強制重新載入

@task
def e_tw_annual_df() -> pd.DataFrame:
    tw_annual_df = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
    # 清理台灣電影名稱，並存入新建立"Name_search"欄位
    tw_annual_df = map.clean_tw_movie_name(tw_annual_df)
    return tw_annual_df

@task
def e_tmdb_query_result(tw_annual_df) -> json:
    logger = get_run_logger()
    logger.info(f"正在請求 API: search...")
    query_list = tw_annual_df["Name_search"]
    tmdb_search_results = search.tmdb_list_search_results(query_list)
    if not tmdb_search_results:
        logger.error(f"❌ 取得 search 失敗！")
    else:
        logger.info(f"✅ 成功取得 search 的資料！")
    return tmdb_search_results

@task
def e_mapping_tw_tmdb_result(tw_annual_df, tmdb_search_results) -> pd.DataFrame:
    # 將search結果轉換成dataframe
    tmdb_search_df = pd.DataFrame(tmdb_search_results)

    # 針對兩張df欲比較的欄位取出去除空白、轉換一致大小寫
    tw_annual_df["Name_map"] = tw_annual_df["Name_search"].apply(map.normalize_text)
    tmdb_search_df["title_map"] = tmdb_search_df["title"].apply(map.normalize_text)
    mapping_result_df = map.merge_two_df(
        df1=tw_annual_df,
        df2=tmdb_search_df,
        how="left",
        df1_col="Name_map",
        df2_col="title_map"
        )
    return mapping_result_df

@task
def t_mapping_df(mapping_result_df) -> pd.DataFrame:
    columns = ["Year", "MovieId", "Name", "id"]
    mapping_df = om.get_spec_cloumn_df(mapping_result_df, columns)
    mapping_df = mapping_df.astype(object).astype("string")
    mapping_df["id"] = mapping_df["id"].replace(".0", "", regex=False)
    return mapping_df

@task
def l_save_raw_data_csv(data, dir_path, file_name):
    sm.save_as_csv(data, dir_path, file_name)


@task
def l_save_raw_data_json(data, dir_path, file_name):
    sm.save_as_json(data, dir_path, file_name)


@flow
def f1_generate_movie_list_flow():
    tw_annual_df = e_tw_annual_df()
    tmdb_search_results = e_tmdb_query_result(tw_annual_df)
    l_save_raw_data_json(tmdb_search_results, p.raw_tw_search, p.search_json)
    mapping_result_df = e_mapping_tw_tmdb_result(tw_annual_df, tmdb_search_results)
    mapping_df = t_mapping_df(mapping_result_df)
    l_save_raw_data_csv(mapping_df, p.raw_tw_mapping, p.mapping_csv)


#本機運行測試
if __name__ == "__main__":
    print("f1_generate_movie_list_flow() 開始運行")
    try:
        f1_generate_movie_list_flow()
        print("✅ f1_generate_movie_list_flow() 執行完畢")
    except Exception as err:
        print(f"❌ f1_generate_movie_list_flow() 執行失敗，錯誤：\n{err}")