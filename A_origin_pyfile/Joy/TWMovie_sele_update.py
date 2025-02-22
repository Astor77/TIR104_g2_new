import pandas as pd
import tasks.Storage_Task.read_file_module as rfm
import tasks.Storage_Task.save_file_module as sfm
import utils.path_config as p

#task 1
#讀取TWMovie_weekly_data.csv
dir_path = p.raw_tw_weekly
file_name = "TWMovie_weekly_data.csv"
file_path = dir_path / file_name
dfTWMovie_sales = rfm.read_file_to_df(file_path)

#task 2
#讀取TWMovie2022-2025.csv
dir_path = p.raw_tw_2022_2025
file_name = "TWMovie2022-2025.csv"
file_path = dir_path / file_name
dfTWMovie = rfm.read_file_to_df(file_path)

dfTWMovie_m = dfTWMovie[['MovieId', 'Year']]

#task 3
#left join 兩張表
#多出 Year 欄位
def merge_dataframe(df1: object, df2: object) -> pd.DataFrame:
    """
    Args:
        df1(Dataframe):要儲存的 DataFrame
        df2(Dataframe):要儲存的 DataFrame
    """

    df_merge = df1.merge(
        df2,
        how= "left",
        left_on= "MovieId",
        right_on= "MovieId",
    )

    return df_merge

#task 4
#分割欄位Date為兩欄Start Date, End Date
def split_date_column(df: object) -> pd.DataFrame:

    df_split = df["Date"].str.split('~', expand= True)
    df['start_date'] = df_split[0]
    df['end_date'] = df_split[1]
    df.drop(columns=['Date'], inplace= True)

    return df

new_df = merge_dataframe(dfTWMovie_m, dfTWMovie_sales)
dfTWMovie_weekly = split_date_column(new_df)

#task 5
#存成TWMovie_weekly_data2.csv
sfm.save_as_csv(dfTWMovie_weekly, "TWMovie_weekly_data2.csv", "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_movie_weekly/")

#task 6
#讀取TWMovie_weekly_data2.csv

dir_path = p.raw_tw_weekly
file_name = "TWMovie_weekly_data2.csv"
file_path = dir_path / file_name

dfTWMovie_weekly_raw = rfm.read_file_to_df(file_path)

print(dfTWMovie_weekly_raw.dtypes)


#task 7
#更改資料型態為日期格式
def column_to_datetime(df : object) -> pd.DataFrame:
    df['start_date'] = pd.to_datetime(df['start_date'], format = '%Y-%m-%d')
    df['end_date'] = pd.to_datetime(df['end_date'], format = '%Y-%m-%d')
    print(df.dtypes)
    return df

#task 8
#遮罩出start_date在2022-01-01之後且'Amount'欄位非空值的資料
def filter_start_day_notna(df: object) -> pd.DataFrame:
    mask = (df['start_date'] >= pd.Timestamp('2022-01-01')) & (df['Amount'].notna())
    return df[mask]

dfTWMovie_weekly_raw2 = filter_start_day_notna(column_to_datetime(dfTWMovie_weekly_raw))

#task 9
#輸出為最後ER model dataframe，並更改欄位名稱與資料型態
def dfTWMovie_weekly_df(df: object) -> pd.DataFrame:
    df = df[['MovieId', "start_date", "end_date", "Amount", "Tickets", "TheaterCount"]]
    df.rename(columns = {"MovieId": "tw_id", "start_date": "week_start_date", "end_date": "week_end_date", "Amount": "current_week_amount", "Tickets": "current_week_tickets", "TheaterCount": "current_week_theater_count"}, inplace = True)
    convert_dict = {'tw_id': str}
    df = df.astype(convert_dict)
    return df


dfTWMovie_weekly_dataframe = dfTWMovie_weekly_df(dfTWMovie_weekly_raw2)
print(dfTWMovie_weekly_dataframe.dtypes)
#task 10
#存成TWMovie_weekly_df.csv
sfm.save_as_csv(dfTWMovie_weekly_dataframe, "TWMovie_weekly_df2.csv", "/workspaces/TIR104_g2_new/A1_temp_data/tw/")

if __name__ == "__main__":
#     new_df = merge_dataframe(dfTWMovie_m, dfTWMovie_sales)
#     dfTWMovie_weekly = split_date_column(new_df)
#     # 存成csv
#     ms.save_as_csv(dfTWMovie_weekly, "TWMovie_weekly_data2.csv", "TW")
    # dfTWMovie_weekly_raw2 = filter_start_day_notna(column_to_datetime(dfTWMovie_weekly_raw))
    #存成csv
    # ms.save_as_csv(filter_start_day_notna(dfTWMovie_weekly_raw2), "TWMovie_weekly_data3.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")
    # dfTWMovie_weekly_dataframe = dfTWMovie_weekly_df(dfTWMovie_weekly_raw2)
    # #存成csv
    # ms.save_as_csv(dfTWMovie_weekly_dataframe, "TWMovie_weekly_df.csv", "/workspaces/TIR104_g2/A1_temp_data")
    pass
