import pandas as pd
import numpy as np

import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
import utils.path_config as p
# import importlib  # Python 內建的重新載入工具
# importlib.reload(rm)  # 強制重新載入
# importlib.reload(p)  # 強制重新載入
# importlib.reload(om)  # 強制重新載入


# sele_tw_annual founction1
#清洗production_country 欄位
def tw_clean_region(df: pd.DataFrame)-> pd.Series:
    # 用正則表達式一次處理多種分隔符，n=1 最多拆分1次
    # 因為欄位內容是list，需要再補一個.str[0]
    # 或是.apply(lambda x: x[0]) -> 效能較差
    tw_clean_region_series = df["Region"].str.split(r'/|##|、', n=1).str[0]
    # 利用字典一次替換多個值
    replace_dict = {
        "法": "法國",
        "大陸": "中國",
        "中國大陸": "中國",
        "台灣": "中華民國"
    }
    tw_clean_region_series = df["Region"].replace(replace_dict)
    return tw_clean_region_series

# sele_tw_annual founction2
# 整理為movie_tw_year_amount dataframe 更名欄位名稱並轉換型別
def tw_annual_trans() -> pd.DataFrame:
    # 讀取檔案: tw_annual 原始資料
    tw_annual_dup_df = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_dup_csv)
    # 新增reference_year欄位，僅取原始欄位Year的"年份資訊"
    tw_annual_dup_df["reference_year"] = pd.to_datetime(tw_annual_dup_df["Year"], format = '%Y')
    # 將 DayCount 欄位轉為int
    tw_annual_dup_df["DayCount"] = pd.to_numeric(tw_annual_dup_df["DayCount"], errors="coerce").round().astype("Int64")
    # 將 MovieId 欄位轉為字串
    convert_dict = {"MovieId": "string"}
    tw_annual_dup_trans_df = tw_annual_dup_df.astype(convert_dict)
    return tw_annual_dup_trans_df

# sele_tw_release_date function1
# 比較年度資料和單片查詢的上映日期欄位，並根據兩邊的不同新增正確的tw_first_release_date欄位
# 這裡年度資料是去重複版本的
def tw_check_release_date() -> pd.DataFrame:
    df1 = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
    df2 = rm.read_file_to_df(p.raw_tw_tw_release_date, p.tw_release_date_csv)
    # 將df1, df2 往橫向concat
    tw_release_df = pd.concat([df1, df2], axis= 1)
    # 年度跟單片查詢的上映日期欄位名稱
    annual = "ReleaseDate"
    search = "release_dates"
    # 新建一個欄位tw_first_release_date
    new_column = "tw_first_release_date"
    # 比對不相等，或是年度資料的日期缺失時，以單片查詢的release_dates欄位資料為主
    # 將比對結果存入tw_first_release_date欄位
    tw_release_df[new_column] = np.where(
        (tw_release_df[annual] != tw_release_df[search]) & pd.notna(tw_release_df[annual]),
        tw_release_df[search],
        tw_release_df[annual]
        )
    return tw_release_df

# sele_tw_release_date founction2
# 將比對日期結果的dataframe轉換型態
def tw_release_date_trans() -> pd.DataFrame:
    # 取得tw movie日期比對結果
    tw_release_df = tw_check_release_date()
    # 新增 production_country 欄位，來源為清理過的 Region 欄位
    tw_release_df["production_country"] = tw_clean_region(tw_release_df)
    # 將 MovieId, Name 欄位轉為字串型態
    convert_dict = {"MovieId": "string", "Name": "string"}
    tw_release_trans_df = tw_release_df.astype(convert_dict)
    # 將tw_first_release_date欄位轉為特定日期格式
    column = "tw_first_release_date"
    tw_release_trans_df[column] = pd.to_datetime(tw_release_df[column], format = '%Y-%m-%d')
    return tw_release_trans_df

# sele_tw_weekly_data_raw function1
def tw_annual_weekly_merge_df() -> pd.DataFrame:
    # 讀取tw年度資料，篩選 MovieId, Year欄位
    # 這邊統計周票房想用的是重複還是不重複？ -> 不重複
    tw_annual_not_dup = rm.read_file_to_df(p.raw_tw_2022_2025, p.tw_annual_not_dup_csv)
    tw_annual = tw_annual_not_dup[["MovieId", "Year"]]
    # 讀取tw週資料
    tw_weekly_data = rm.read_file_to_df(p.raw_tw_weekly, p.tw_weekly_csv)
    # 將篩選的年度資料跟周資料merge
    tw_annual_weekly_merge_df = om.data_merge_left_df(tw_annual, tw_weekly_data, "MovieId", "MovieId")
    return tw_annual_weekly_merge_df

# sele_tw_weekly_data_raw founction2
# 分割欄位Date為兩欄Start Date, End Date
def tw_split_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df_split = df["Date"].str.split('~', expand= True)
    df["start_date"] = df_split[0]
    df["end_date"] = df_split[1]
    return df

# sele_tw_weekly_data_temp founction1
#遮罩出start_date在2022-01-01之後且'Amount'欄位非空值的資料
def tw_filter_start_date_notna(df: pd.DataFrame) -> pd.DataFrame:
    mask = (df["start_date"] >= pd.Timestamp("2022-01-01")) & (df["Amount"].notna())
    return df[mask]

# sele_tw_weekly_data_temp fonction2
# 處理 tw_weekly_data2內的日期轉型、篩選
def sele_tw_weekly_amount_trans() -> pd.DataFrame:
    # 讀取tw_weekly_data2檔案
    tw_weekly_df = rm.read_file_to_df(p.raw_tw_weekly, p.tw_weekly2_csv)
    # 將開始與結束日期轉換datetime型態
    columns = ["start_date", "end_date"]
    tw_weekly_trans_df = om.column_to_datetime(tw_weekly_df, columns)
    # 篩選開始日期 >= 2022-01-01，並且 Amount 欄位非空值的資料
    tw_weekly_trans_df = tw_filter_start_date_notna(tw_weekly_trans_df)
    # 將 MovieId 欄位轉為字串
    convert_dict = {"MovieId": "string"}
    tw_weekly_trans_df = tw_weekly_trans_df.astype(convert_dict)
    # 將 "Amount", "Tickets", "TheaterCount" 欄位轉為int
    columns = ["Amount", "Tickets", "TheaterCount"]
    tw_weekly_trans_df[columns] = tw_weekly_trans_df[columns].apply(pd.to_numeric, errors="coerce").round().astype("Int64")
    return tw_weekly_trans_df

