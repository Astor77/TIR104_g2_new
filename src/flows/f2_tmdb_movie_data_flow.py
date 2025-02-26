# 運用Fetching Task內的py模組處理資料抓取
# 各個API端點抓取資料，並存成json或csv
from prefect import get_run_logger, task, flow
import json
from tasks.Fetching_Task import fetch_api_data_module as fa
from tasks.Storage_Task.read_file_module import read_file_to_df
from tasks.Storage_Task.save_file_module import save_as_json
import utils.path_config as p

# e_get_tmdb_id_list 讀取mapping取id欄位去重 -> 返回共同上游的list
@task
def e_get_tmdb_id_list() -> list:
    df = read_file_to_df(p.raw_tw_mapping, p.mapping_csv)
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
    save_as_json(data, dir_path, file_name)
    logger.info(f"✅ {file_name} 儲存完成！")



@flow
def f2_tmdb_movie_data_flow():
    tmdb_id_list = e_get_tmdb_id_list()
    apis = [
        {"name": fa.DETAILS_API, "api_key": fa.ASTOR_TMDB_KEY, "save_path": p.raw_tw_details},
        {"name": fa.RELEASE_DATES_API, "api_key": fa.RAIN_TMDB_KEY, "save_path": p.raw_tw_tmdb_release_date},
        {"name": fa.CREDITS_API, "api_key": fa.ALLEN_TMDB_KEY, "save_path": p.raw_tw_credits},
        {"name": fa.KEYWORDS_API, "api_key": fa.JOY_TMDB_KEY, "save_path": p.raw_tw_keywords}
    ]

    for api in apis:
        raw_data = e_tmdb_raw_data.submit(tmdb_id_list, api["name"], api["api_key"])
        l_save_raw_data.submit(raw_data, f"tmdb_{api['name']}.json", api["save_path"])

    genres_list = e_tmdb_genres_list(fa.ASTOR_TMDB_KEY)
    l_save_raw_data(genres_list, p.raw_tw_genres_list, p.genres_list_json)

f2_tmdb_movie_data_flow()


