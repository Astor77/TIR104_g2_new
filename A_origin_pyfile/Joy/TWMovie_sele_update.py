import pandas as pd
import module_save_file as ms

#讀取兩個csv檔並存成兩個dataframe
try:
    dfTWMovie_sales = pd.read_csv("/workspaces/TIR104_g2/A0_raw_data/tw/TWMovie_weekly_data.csv")

    # print(dfTWMovie_sales.head())
except Exception as e:
    print(f"Error reading file: {e}")


try:
    file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/TWMovie2022-2025.csv"
    dfTWMovie = pd.read_csv(file_path)
    dfTWMovie_m = dfTWMovie[['MovieId', 'Year']]
    # print(dfTWMovie_m.head())
except Exception as e:
    print(f"Error reading file: {e}")



try:
    file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/TWMovie_weekly_data2.csv"
    dfTWMovie_weekly_raw = pd.read_csv(file_path, index_col= 0)
    print(dfTWMovie_weekly_raw.head())
except Exception as e:
    print(f"Error reading file: {e}")

# print(dfTWMovie_weekly_raw.dtypes)

try:
    file_path = "/workspaces/TIR104_g2/A1_temp_data/tw/TWMovie_weekly_data3.csv"
    dfTWMovie_weekly_raw3 = pd.read_csv(file_path)
    print(dfTWMovie_weekly_raw3.head())
except Exception as e:
    print(f"Error reading file: {e}")


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


#分割欄位Date為兩欄Start Date, End Date
def split_date_column(df: object) -> pd.DataFrame:

    df_split = df["Date"].str.split('~', expand= True)
    df['start_date'] = df_split[0]
    df['end_date'] = df_split[1]
    df.drop(columns=['Date'], inplace= True)

    return df

#更改資料型態為日期格式
def column_to_datetime(df : object) -> pd.DataFrame:
    df['start_date'] = pd.to_datetime(df['start_date'], format = '%Y-%m-%d')
    df['end_date'] = pd.to_datetime(df['end_date'], format = '%Y-%m-%d')
    print(df.dtypes)
    return df

#遮罩出start_date在2022-01-01之後且'Amount'欄位非空值的資料
def filter_start_day_notna(df: object) -> pd.DataFrame:
    mask = (df['start_date'] >= pd.Timestamp('2022-01-01')) & (df['Amount'].notna())
    return df[mask]

#輸出為最後dataframe
def dfTWMovie_weekly_df(df: object) -> pd.DataFrame:
    df = df[['MovieId', "start_date", "end_date", "Amount", "Tickets", "TheaterCount"]]
    df.rename(columns = {"MovieId": "tw_id", "start_date": "week_start_date", "end_date": "week_end_date", "Amount": "current_week_amount", "Tickets": "current_week_tickets", "TheaterCount": "current_week_theater_count"}, inplace = True)
    return df

if __name__ == "__main__":
#     new_df = merge_dataframe(dfTWMovie_m, dfTWMovie_sales)
#     dfTWMovie_weekly = split_date_column(new_df)
#     # 存成csv
#     ms.save_as_csv(dfTWMovie_weekly, "TWMovie_weekly_data2.csv", "TW")
    dfTWMovie_weekly_raw2 = column_to_datetime(dfTWMovie_weekly_raw)
    #存成csv
    ms.save_as_csv(filter_start_day_notna(dfTWMovie_weekly_raw2), "TWMovie_weekly_data3.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")
    # dfTWMovie_weekly_dataframe = dfTWMovie_weekly_df(dfTWMovie_weekly_raw3)
    # #存成csv
    # ms.save_as_csv(dfTWMovie_weekly_dataframe, "TWMovie_weekly_df.csv", "/workspaces/TIR104_g2/A1_temp_data")

