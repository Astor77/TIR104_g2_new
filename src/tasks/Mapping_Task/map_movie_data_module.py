# 這隻module 專門用來處理 mapping tw_annual 跟 tmdb_search api
import pandas as pd
import unicodedata
import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
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


def normalize_text(text):
    """
    將全形字轉換為半形，並移除空白與轉大寫
    """
    if isinstance(text, str):
        text = unicodedata.normalize("NFKC", text)  # 轉換全形為半形
        text = text.replace(" ", "").upper()  # 移除空白並轉大寫
    return text


if __name__ == "__main__":
    #僅測試search 10筆
    tw_annual_df = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
    query_list = clean_tw_movie_name(tw_annual_df)
    total_search_results = search.tmdb_list_search_results(query_list[:10])
    tmdb_search_df = pd.DataFrame(total_search_results)


    tw_annual_df["Name_map"] = tw_annual_df["Name_search"].apply(normalize_text)
    tmdb_search_df["title_map"] = tmdb_search_df["title"].apply(normalize_text)

    mapping_result_df = merge_two_df(df1=tw_annual_df, df2=tmdb_search_df, join="left", df1_col="Name_map", df2_col="title_map")
    columns = ["Year", "MovieId", "Name", "id"]
    mapping_df = om.get_spec_cloumn_df(mapping_result_df, columns)
    # 這邊就不寫save_file了

    nan_count = mapping_result_df,["id"].isna().sum()
    success_count = mapping_result_df,["id"].notna().sum()
    print(f"比對判定失敗: {nan_count} 筆資料")
    print(f"比對判定成功: {success_count} 筆資料")
