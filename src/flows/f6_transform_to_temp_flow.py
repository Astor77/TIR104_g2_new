import pandas as pd

import tasks.Transform_Task.tmdb_transform_temp_module as tmdb
import tasks.Transform_Task.omdb_transform_temp_module as omdb
import tasks.Transform_Task.sele_transform_temp_module as sele
import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
import utils.path_config as p

import importlib  # Python 內建的重新載入工具
importlib.reload(rm)  # 強制重新載入
importlib.reload(om)  # 強制重新載入
importlib.reload(p)  # 強制重新載入


# 最終 omdb_temp_task
def t_omdb_info_temp_df():
    omdb_trans_df = omdb.omdb_trans()
    omdb_columns = ["imdbID", "imdbRating"]
    omdb_temp_df = om.get_spec_cloumn_df(omdb_trans_df, omdb_columns)
    return omdb_temp_df


# 最終 tmdb_details_temp_task
def t_tmdb_details_temp_df():
    details_merge_df = tmdb.tmdb_details_merge_mapping()
    details_trans_df = tmdb.tmdb_details_trans(details_merge_df)
    tmdb_details_columns = ["Year", "MovieId", "Name", "id", "imdb_id", "runtime", "budget", "revenue"]
    details_temp_df = om.get_spec_cloumn_df(details_trans_df, tmdb_details_columns)
    return details_temp_df


# 最終 tmdb_release_date_temp_task
def t_tmdb_release_temp_df():
    release_trans_df = tmdb.tmdb_release_date_trans()
    tmdb_release_columns = ["id", "iso_3166_1", "note", "release_date", "type"]
    release_temp_df = om.get_spec_cloumn_df(release_trans_df, tmdb_release_columns)
    return release_temp_df


# 最終 tmdb_genres_temp_task
def t_tmdb_genres_temp_df():
    genres_trans_df = tmdb.tmdb_genres_trans()
    tmdb_genres_columns = ["tmdb_id", "id"]
    genres_temp_df = om.get_spec_cloumn_df(genres_trans_df, tmdb_genres_columns)
    return genres_temp_df


# 最終 tmdb_genres_list_temp_task
def t_tmdb_genres_list_temp_df():
    genres_list_temp_df = tmdb.tmdb_genres_list_trans()
    return genres_list_temp_df


# 最終 tmdb_keywords_temp_task
def t_tmdb_keywords_temp_df():
    keyword_trans_df = tmdb.tmdb_keywords_trans()
    tmdb_keywords_columns = ["tmdb_id", "name"]
    keyword_temp_df = om.get_spec_cloumn_df(keyword_trans_df, tmdb_keywords_columns)
    return keyword_temp_df


# 最終 tmdb_casts_top5_temp_task
def t_tmdb_casts_top5_temp_df():
    casts_top5_trans_df = tmdb.tmdb_casts_top5_trans()
    tmdb_casts_columns = ["tmdb_id", "id"]
    casts_top5_temp_df = om.get_spec_cloumn_df(casts_top5_trans_df, tmdb_casts_columns)
    return casts_top5_temp_df


# 最終 tmdb_directors_temp_task
def t_tmdb_directors_temp_df():
    directors_trans_df = tmdb.tmdb_directors_trans()
    tmdb_directors_columns = ["tmdb_id", "id"]
    directors_temp_df = om.get_spec_cloumn_df(directors_trans_df, tmdb_directors_columns)
    return directors_temp_df


# 最終 tmdb_person_temp_task
def t_tmdb_person_temp_df():
    casts_top5_trans_df = tmdb.tmdb_casts_top5_trans()
    directors_trans_df = tmdb.tmdb_directors_trans()
    person_trans_df = tmdb.tmdb_person_trans(casts_top5_trans_df, directors_trans_df)
    tmdb_person_columns = ["id", "gender", "name", "original_name", "known_for_department"]
    person_temp_df = om.get_spec_cloumn_df(person_trans_df, tmdb_person_columns)
    return person_temp_df

# sele_tw_annual
# 最終 sele_tw_annual_temp_task
def t_tw_annual_temp_df():
    tw_annual_dup_trans_df = sele.tw_annual_trans()
    tw_annual_columns = ["MovieId", "reference_year", "DayCount", "Amount", "Tickets"]
    tw_annual_temp_df = om.get_spec_cloumn_df(tw_annual_dup_trans_df, tw_annual_columns)
    return tw_annual_temp_df


# sele_tw_release_date
# 最終 sele_tw_release_date_temp_task
def t_sele_tw_release_date_temp_df() -> pd.DataFrame:
    tw_release_trans_df = sele.tw_release_date_trans()
    tw_release_columns = ["MovieId", "Name", "production_country" ,"tw_first_release_date"]
    tw_release_temp_df = om.get_spec_cloumn_df(tw_release_trans_df, tw_release_columns)
    return tw_release_temp_df


# sele_tw_weekly_data_raw
# 最終 sele_tw_weekly_data_raw_task
# 這邊後面要先存一次檔到raw
def t_sele_tw_weekly_amount_raw() -> pd.DataFrame:
    merge_df = sele.tw_annual_weekly_merge_df()
    tw_weekly2_data_df = sele.tw_split_date_column(merge_df)
    return tw_weekly2_data_df


# sele_tw_weekly_data_temp
# 最終 sele_tw_weekly_data_temp_task
def t_sele_tw_weekly_amount_temp_df() -> pd.DataFrame:
    tw_weekly_trans_df = sele.sele_tw_weekly_amount_trans()
    tw_weekly_columns = ["MovieId", "start_date", "end_date", "Amount", "Tickets", "TheaterCount"]
    tw_weekly_temp_df = om.get_spec_cloumn_df(tw_weekly_trans_df, tw_weekly_columns)
    return tw_weekly_temp_df



