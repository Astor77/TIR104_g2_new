# map_movie_data_task.py 這隻task 專門用來 mapping tw_annual 跟 tmdb_search api
import pandas as pd

import tasks.Storage_Task.read_file_task as rf
import tasks.Mapping_Task.search_movie_api_task as ms
import utils.path_config as p

dir_path = p.raw_tw_2022_2025
file_name = "TWMovie2022-2025.csv"
file_path = dir_path / file_name
df_tw_annual = rf.read_file_to_df(file_path)

# get_tw_df()
# 擷取全國年度的合併csv資料並轉為df
def get_og_tw_annual_df() -> pd.DataFrame:
    return df_tw_annual


# get_tw_movie_clean_name_list()
# 清理 Name欄位名稱
# 去除()以及全形的（）
# 取修復版前面的電影名稱
def get_tw_movie_clean_name_list() -> list:
    df_tw_annual["Name_search"] = df_tw_annual["Name"].str.split("(").str[0]
    df_tw_annual["Name_search"] = df_tw_annual["Name_search"].str.split("（").str[0]
    df_tw_annual["Name_search"] = df_tw_annual["Name_search"].apply(lambda name: name.split(" ")[0] if "修復" in name else name)
    query_list = df_tw_annual["Name_search"]
    return query_list


# 簡易清理
def clean_tw_tmdb_map_column(tw ,tmdb):
    tw["Name_map"] = tw["Name_search"].str.replace(" ", "").str.upper()
    tmdb["title_map"] = tmdb["title"].str.replace(" ", "").str.upper()



def merge_two_df(df1: pd.DataFrame, df2: pd.DataFrame, join: str="left", df1_col: str="Name_map", df2_col: str="title_map"):
    df_mapping = df1.merge(
        # 因為比對結果，會有重複電影名稱，僅保留第一筆
        df2.drop_duplicates(subset=[df2_col]),

        how="left",
        #df_tw_annual
        left_on=df1_col,
        #df_search_results
        right_on=df2_col
    )
    df_mapping_result = df_mapping[df_mapping["id"].notna()]
    return df_mapping_result

def drop_not_necessary(df: pd.DataFrame):
    df_select = df.loc[:, ["Year", "MovieId", "Name", "id"]]
    return df_select


if __name__ == "__main__":
    #僅測試search 10筆
    tw = get_og_tw_annual_df()
    query_list = get_tw_movie_clean_name_list()
    total_search_results = ms.tmdb_list_search_results(query_list[:10])
    tmdb = pd.DataFrame(total_search_results)


    clean_tw_tmdb_map_column(tw, tmdb)
    df_mapping_result = merge_two_df(df1=tw, df2=tmdb, join="left", df1_col="Name_map", df2_col="title_map")
    df_mapping_select = drop_not_necessary(df_mapping_result)
    # 這邊就不寫save_file了

    nan_count = df_mapping_result["id"].isna().sum()
    success_count = df_mapping_result["id"].notna().sum()
    print(f"比對判定失敗: {nan_count} 筆資料")
    print(f"比對判定成功: {success_count} 筆資料")
