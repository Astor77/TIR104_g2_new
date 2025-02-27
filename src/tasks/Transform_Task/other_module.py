from datetime import datetime
import pandas as pd


#更新時間
def updated_time_column(df: pd.DataFrame) -> pd.DataFrame:
    current_time = datetime.now().strftime("%Y-%m-%d")
    df["data_updated_time"] = current_time

#dataframe篩選欄位
def get_spec_cloumn_df(df: pd.DataFrame, cloumn_name: list):
    df_temp = df[cloumn_name].copy()
    return df_temp

#兩個dataframe left join
def data_merge_left_df(df1: pd.DataFrame, df2: pd.DataFrame, id1: str, id2: str, how: str="left") -> pd.DataFrame:
    # 將兩個 DataFrame 合併
    merge_data = pd.merge(df1, df2, left_on=id1, right_on=id2, how=how)
    return merge_data

# 指定欄位轉為特定datetime格式：'%Y-%m-%d'
def column_to_datetime(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
    return df