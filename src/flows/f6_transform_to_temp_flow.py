import pandas as pd
from prefect.runtime import deployment
from prefect import get_run_logger, task, flow
from prefect.task_runners import ConcurrentTaskRunner
import importlib  # Python 內建的重新載入模組工具

import tasks.Transform_Task.tmdb_transform_temp_module as tmdb
import tasks.Transform_Task.omdb_transform_temp_module as omdb
import tasks.Transform_Task.sele_transform_temp_module as sele
import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
import tasks.Storage_Task.save_file_module as sm
import utils.path_config as p

import importlib  # Python 內建的重新載入工具
importlib.reload(rm)  # 強制重新載入
importlib.reload(om)  # 強制重新載入
importlib.reload(p)  # 強制重新載入


# 最終 omdb_temp_task
@task
def t_omdb_info_temp_df():
    omdb_trans_df = omdb.omdb_trans()
    omdb_columns = ["imdbID", "imdbRating"]
    omdb_temp_df = om.get_spec_cloumn_df(omdb_trans_df, omdb_columns)
    return omdb_temp_df


# 最終 tmdb_details_temp_task
@task
def t_tmdb_details_temp_df():
    details_merge_df = tmdb.tmdb_details_merge_mapping()
    details_trans_df = tmdb.tmdb_details_trans(details_merge_df)
    tmdb_details_columns = ["Year", "MovieId", "Name", "id", "imdb_id", "runtime", "budget", "revenue"]
    details_temp_df = om.get_spec_cloumn_df(details_trans_df, tmdb_details_columns)
    return details_temp_df


# 最終 tmdb_release_date_temp_task
@task
def t_tmdb_release_temp_df():
    release_trans_df = tmdb.tmdb_release_date_trans()
    tmdb_release_columns = ["id", "iso_3166_1", "note", "release_date", "type"]
    release_temp_df = om.get_spec_cloumn_df(release_trans_df, tmdb_release_columns)
    return release_temp_df


# 最終 tmdb_genres_temp_task
@task
def t_tmdb_genres_temp_df():
    genres_trans_df = tmdb.tmdb_genres_trans()
    tmdb_genres_columns = ["tmdb_id", "id"]
    genres_temp_df = om.get_spec_cloumn_df(genres_trans_df, tmdb_genres_columns)
    return genres_temp_df


# 最終 tmdb_genres_list_temp_task
@task
def t_tmdb_genres_list_temp_df():
    genres_list_temp_df = tmdb.tmdb_genres_list_trans()
    return genres_list_temp_df


# 最終 tmdb_keywords_temp_task
@task
def t_tmdb_keywords_temp_df():
    keyword_trans_df = tmdb.tmdb_keywords_trans()
    tmdb_keywords_columns = ["tmdb_id", "name"]
    keyword_temp_df = om.get_spec_cloumn_df(keyword_trans_df, tmdb_keywords_columns)
    return keyword_temp_df


# 最終 tmdb_casts_top5_temp_task
@task
def t_tmdb_casts_top5_temp_df():
    casts_top5_trans_df = tmdb.tmdb_casts_top5_trans()
    tmdb_casts_columns = ["tmdb_id", "id"]
    casts_top5_temp_df = om.get_spec_cloumn_df(casts_top5_trans_df, tmdb_casts_columns)
    return casts_top5_temp_df


# 最終 tmdb_directors_temp_task
@task
def t_tmdb_directors_temp_df():
    directors_trans_df = tmdb.tmdb_directors_trans()
    tmdb_directors_columns = ["tmdb_id", "id"]
    directors_temp_df = om.get_spec_cloumn_df(directors_trans_df, tmdb_directors_columns)
    return directors_temp_df


# 最終 tmdb_person_temp_task
@task
def t_tmdb_person_temp_df():
    casts_top5_trans_df = tmdb.tmdb_casts_top5_trans()
    directors_trans_df = tmdb.tmdb_directors_trans()
    person_trans_df = tmdb.tmdb_person_trans(casts_top5_trans_df, directors_trans_df)
    tmdb_person_columns = ["id", "gender", "name", "original_name", "known_for_department"]
    person_temp_df = om.get_spec_cloumn_df(person_trans_df, tmdb_person_columns)
    return person_temp_df

# sele_tw_annual
# 最終 sele_tw_annual_temp_task
@task
def t_tw_annual_temp_df():
    tw_annual_dup_trans_df = sele.tw_annual_trans()
    tw_annual_columns = ["MovieId", "reference_year", "DayCount", "Amount", "Tickets"]
    tw_annual_temp_df = om.get_spec_cloumn_df(tw_annual_dup_trans_df, tw_annual_columns)
    return tw_annual_temp_df


# sele_tw_release_date
# 最終 sele_tw_release_date_temp_task
@task
def t_sele_tw_details_temp_df() -> pd.DataFrame:
    tw_details_trans_df = sele.tw_release_date_trans()
    tw_details_columns = ["MovieId", "Name", "production_country" ,"tw_first_release_date"]
    tw_details_temp_df = om.get_spec_cloumn_df(tw_details_trans_df, tw_details_columns)
    return tw_details_temp_df


# sele_tw_weekly_data_raw
# 最終 sele_tw_weekly_data_raw_task
# 這邊後面要先存一次檔到raw
@task
def t_sele_tw_weekly_amount_raw() -> pd.DataFrame:
    merge_df = sele.tw_annual_weekly_merge_df()
    tw_weekly_data2_df = sele.tw_split_date_column(merge_df)
    return tw_weekly_data2_df


# sele_tw_weekly_data_temp
# 最終 sele_tw_weekly_data_temp_task
@task
def t_sele_tw_weekly_amount_temp_df() -> pd.DataFrame:
    tw_weekly_trans_df = sele.sele_tw_weekly_amount_trans()
    tw_weekly_columns = ["MovieId", "start_date", "end_date", "Amount", "Tickets", "TheaterCount"]
    tw_weekly_df2 = om.get_spec_cloumn_df(tw_weekly_trans_df, tw_weekly_columns)
    return tw_weekly_df2

@task
def l_save_data(data, dir_path, file_name) -> None:
    logger = get_run_logger()
    logger.info(f"💾 正在儲存 {file_name} 到 {dir_path}...")
    try:
        save_result = sm.save_as_csv(data, dir_path, file_name)
        if "成功" in save_result:
            logger.info(f"✅ {save_result}")
        else:
            logger.error(f"❌ {save_result}")
    except Exception as e:
        logger.error(f"🚨 儲存過程發生錯誤: {e}")


@flow(task_runner=ConcurrentTaskRunner())
def f6_transform_to_temp_flow():
    omdb_temp_df = t_omdb_info_temp_df.submit()
    l_save_data(omdb_temp_df, p.temp_tw, p.omdb_info_csv)
    details_temp_df = t_tmdb_details_temp_df.submit()
    l_save_data(details_temp_df, p.temp_tw, p.details_csv)
    release_temp_df = t_tmdb_release_temp_df.submit()
    l_save_data(release_temp_df, p.temp_tw, p.release_date_csv)
    genres_temp_df = t_tmdb_genres_temp_df.submit()
    l_save_data(genres_temp_df, p.temp_tw, p.genres_csv)
    genres_list_temp_df = t_tmdb_keywords_temp_df.submit()
    l_save_data(genres_list_temp_df, p.temp_tw, p.genres_list_csv)
    keywords_temp_df = t_tmdb_keywords_temp_df.submit()
    l_save_data(keywords_temp_df, p.temp_tw, p.keywords_csv)
    casts_top5_temp_df = t_tmdb_casts_top5_temp_df.submit()
    l_save_data(casts_top5_temp_df, p.temp_tw, p.cast_top5_csv)
    directors_temp_df = t_tmdb_directors_temp_df.submit()
    l_save_data(directors_temp_df, p.temp_tw, p.director_csv)
    person_temp_df = t_tmdb_person_temp_df.submit()
    l_save_data(person_temp_df, p.temp_tw, p.person_csv)

    tw_annual_temp_df = t_tw_annual_temp_df.submit()
    l_save_data(tw_annual_temp_df, p.temp_tw, p.tw_annual_csv)
    tw_details_temp_df = t_sele_tw_details_temp_df.submit()
    l_save_data(tw_details_temp_df, p.temp_tw, p.tw_details_csv)
    tw_weekly_data2_df = t_sele_tw_weekly_amount_raw.submit()
    l_save_data(tw_weekly_data2_df, p.raw_tw_weekly, p.tw_weekly_data2_csv)
    tw_weekly_df2 = t_sele_tw_weekly_amount_temp_df()
    l_save_data(tw_weekly_df2, p.temp_tw, p.tw_weekly_df2_csv)












