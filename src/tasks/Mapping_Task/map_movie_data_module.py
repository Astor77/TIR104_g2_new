# 這隻module 專門用來處理 mapping tw_annual 跟 tmdb_search api
import pandas as pd

import tasks.Storage_Task.read_file_module as rm
import tasks.Mapping_Task.search_movie_api_module as search
import utils.path_config as p



def clean_tw_movie_name(tw_annual_df) -> pd.DataFrame:
    """
    取得想在tmdb搜尋的台灣電影名稱，返回list
    """
    # 讀取檔案
    # 清理台灣資料 Name 欄位名稱，存入Name_search
    tw_annual_df["Name_search"] = tw_annual_df["Name"].str.split("(").str[0]
    tw_annual_df["Name_search"] = tw_annual_df["Name_search"].str.split("（").str[0]
    tw_annual_df["Name_search"] = tw_annual_df["Name_search"].apply(lambda name: name.split(" ")[0] if "修復" in name else name)
    return tw_annual_df


def clean_tw_tmdb_map_column(tw_annual_df, tmdb_search_df):
    """
    清理跟tmdb搜尋結果，兩張表要比對的欄位
    """
    # 將搜尋的關鍵字去除中間空白，並且轉換成大寫
    tw_annual_df["Name_map"] = tw_annual_df["Name_search"].str.replace(" ", "").str.upper()
    # 將搜尋的結果去除中間空白，並且轉換成大寫
    tmdb_search_df["title_map"] = tmdb_search_df["title"].str.replace(" ", "").str.upper()



def merge_two_df(df1: pd.DataFrame, df2: pd.DataFrame, how: str="left", df1_col: str="Name_map", df2_col: str="title_map"):
    df_mapping = df1.merge(
        # 因為比對結果，會有重複電影名稱，僅保留第一筆
        df2.drop_duplicates(subset=[df2_col]),

        how=how,
        #df_tw_annual
        left_on=df1_col,
        #df_search_results
        right_on=df2_col
    )
    df_mapping_result = df_mapping[df_mapping["id"].notna()]
    return df_mapping_result


if __name__ == "__main__":
    #僅測試search 10筆
    tw = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
    query_list = get_tw_movie_clean_name_list()
    total_search_results = search.tmdb_list_search_results(query_list[:10])
    tmdb = pd.DataFrame(total_search_results)


    clean_tw_tmdb_map_column(tw, tmdb)
    df_mapping_result = merge_two_df(df1=tw, df2=tmdb, join="left", df1_col="Name_map", df2_col="title_map")
    df_mapping_select = drop_not_necessary(df_mapping_result)
    # 這邊就不寫save_file了

    nan_count = df_mapping_result["id"].isna().sum()
    success_count = df_mapping_result["id"].notna().sum()
    print(f"比對判定失敗: {nan_count} 筆資料")
    print(f"比對判定成功: {success_count} 筆資料")
