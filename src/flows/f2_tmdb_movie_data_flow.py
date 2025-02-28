# tmdb 各個API端點抓取資料，並存成json
# 運用 Fetching Task內的py模組處理資料抓取
from prefect.runtime import deployment
from prefect import get_run_logger, task, flow
from prefect.task_runners import ConcurrentTaskRunner
import json
import importlib  # Python 內建的重新載入模組工具

import tasks.Fetching_Task.fetch_api_data_module as fa
import tasks.Storage_Task.read_file_module as rm
import tasks.Storage_Task.save_file_module as sm
import utils.path_config as p

importlib.reload(fa)  # 強制重新載入
importlib.reload(rm)  # 強制重新載入
importlib.reload(sm)  # 強制重新載入

# e_get_tmdb_id_list 讀取mapping取id欄位去重 -> 返回共同上游的list
@task
def e_get_tmdb_id_list() -> list:
    df = rm.read_file_to_df(p.raw_tw_mapping, p.mapping_csv)
    tmdb_id_list = df["id"].drop_duplicates()
    return tmdb_id_list


# e_tmdb_raw_data -> 平行處理4隻api
@task
def e_tmdb_raw_data(tmdb_id_list, api_name, api_key) -> json:
    logger = get_run_logger()
    logger.info(f"正在請求 API: {api_name}...")
    raw_data = fa.tmdb_get_list_movies_data(tmdb_id_list, api_name, api_key)

    if not raw_data:
        logger.error(f"❌ 取得 {api_name} 失敗！")
    else:
        logger.info(f"✅ 成功取得 {api_name} 的資料！")
    return raw_data

# e_tmdb_genres_list，因api結構不同，獨立取得
@task
def e_tmdb_genres_list(api_key) -> json:
    logger = get_run_logger()
    logger.info(f"正在請求 API: gernes_list...")
    raw_data = fa.tmdb_get_genres_list(api_key)
    if not raw_data:
        logger.error(f"❌ 取得 gernes_list 失敗！")
    else:
        logger.info(f"✅ 成功取得 gernes_list 的資料！")
    return raw_data

# l_save_raw_dat，將原始資料存為json檔案
@task
def l_save_raw_data(data, dir_path, file_name) -> None:
    logger = get_run_logger()
    logger.info(f"💾 正在儲存 {file_name} 到 {dir_path}...")
    try:
        save_result = sm.save_as_json(data, dir_path, file_name)
        if "成功" in save_result:
            logger.info(f"✅ {save_result}")
        else:
            logger.error(f"❌ {save_result}")
    except Exception as e:
        logger.error(f"🚨 儲存過程發生錯誤: {e}")



@flow(task_runner=ConcurrentTaskRunner())
def f2_tmdb_movie_data_flow():
    tmdb_id_list = e_get_tmdb_id_list()
    futures = []  # 存放所有 Future 物件
    apis = [
        {"name": fa.DETAILS_API, "api_key": fa.ASTOR_TMDB_KEY, "save_path": p.raw_tw_details, "file_name": p.details_json},
        {"name": fa.RELEASE_DATES_API, "api_key": fa.RAIN_TMDB_KEY, "save_path": p.raw_tw_tmdb_release_date, "file_name": p.release_date_json},
        {"name": fa.CREDITS_API, "api_key": fa.ALLEN_TMDB_KEY, "save_path": p.raw_tw_credits, "file_name": p.credits_json},
        {"name": fa.KEYWORDS_API, "api_key": fa.JOY_TMDB_KEY, "save_path": p.raw_tw_keywords, "file_name": p.keywords_json}
    ]

    for api in apis:
        raw_data_future = e_tmdb_raw_data.submit(tmdb_id_list, api["name"], api["api_key"])
        save_future = l_save_raw_data.submit(raw_data_future, api["save_path"], api["file_name"])
        futures.append(save_future)

    # genres_list 也放進 future 確保等待它完成
    genres_list_future = e_tmdb_genres_list.submit(fa.ASTOR_TMDB_KEY)
    save_genres_future = l_save_raw_data.submit(genres_list_future, p.raw_tw_genres_list, p.genres_list_json)
    futures.append(save_genres_future)

    # **如果是本機運行，確保所有 Task 都完成**
    if deployment.name is None:
        print("在本機環境執行")
        for future in futures:
            future.result()
    else:
        print("在雲端或其他環境執行")

# 本機運行測試
if __name__ == "__main__":
    print("f2_tmdb_movie_data_flow() 開始運行")
    try:
        f2_tmdb_movie_data_flow()
        print("✅ f2_tmdb_movie_data_flow() 執行完畢")
    except Exception as err:
        print(f"❌ f2_tmdb_movie_data_flow() 執行失敗，錯誤：\n{err}")


