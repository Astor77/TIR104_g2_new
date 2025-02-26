import pandas as pd
import numpy as np
import tasks.Storage_Task.read_file_module as rfm
import tasks.Storage_Task.save_file_module as sfm
import utils.path_config as p

#task 1
#讀取原始檔案csv

dfTWMovie = rfm.read_file_to_df(p.raw_tw_2022_2025 / "TWMovie2022-2025_raw.csv")


#task 2
#讀取刪除有重複id的csv

dfTWMovie_distinct = rfm.read_file_to_df(p.raw_tw_2022_2025 / "TWMovie2022-2025.csv")


#task 3
#全國單片抓取的release_date

release_dates = rfm.read_file_to_df(p.raw_tw_tw_release_date / "tw_release_dates.csv")



#task4 Transform
#TWMovie 和 release_date合併
def merge_TWMovie_release_date(df1: object, df2: object) -> pd.DataFrame:
    df_new = pd.concat([df1, df2], axis= 1)
    return df_new

dfTWMovie_new = merge_TWMovie_release_date(dfTWMovie_distinct, release_dates)
print(dfTWMovie_new.head())


#task5 Transform
#比較release_date和 release_dates欄位，並根據兩邊的不同新增正確的tw_first_release_date欄位
def compare_release_date(df: object) -> pd.DataFrame:
    df['tw_first_release_date'] = np.where((df['ReleaseDate'] != df['release_dates']) & pd.notna(df['release_dates']), df['release_dates'], df['ReleaseDate'])
    df['tw_first_release_date'] = pd.to_datetime(df['tw_first_release_date'], format = '%Y-%m-%d')
    df.drop(columns=['ReleaseDate', 'release_dates'], inplace = True)
    # print(df.dtypes)
    return df

#task 5
#清洗production_country 欄位
def clean_region(df:object)-> pd.DataFrame:
    df['Region2'] = df['Region'].str.split('/', n=1).str[0]
    df['Region2'] = df['Region2'].str.split('##', n=1).str[0]
    df['Region2'] = df['Region2'].str.split('、', n=1).str[0]
    df['Region2'].replace("法", "法國", inplace= True)

    df['Region2'].replace("大陸", "中國", inplace= True)
    df['Region2'].replace("中國大陸", "中國", inplace= True)
    df['Region2'].replace('台灣', "中華民國", inplace= True)

    return df

#task6 Transform
#整理為movie_detail dataframe 更名欄位名稱，並轉換資料型態
def to_TWMovie_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', 'Name', 'Region2', "tw_first_release_date"]]
    df.rename(columns= {"MovieId": "tw_id", "Name": "tw_title", "Region2": "production_country"}, inplace= True)

    convert_dict = {'tw_id': str, 'tw_title': str}
    df = df.astype(convert_dict)

    return df

dfTWMovie_fin = to_TWMovie_dataframe(clean_region(compare_release_date(dfTWMovie_new)))
# print(dfTWMovie_fin.dtypes)
#task7
#存成TWMovie_df2.csv
sfm.save_as_csv(dfTWMovie_fin, "TWMovie_df.csv", "/workspaces/TIR104_g2_new/A1_temp_data/tw/")

#task8 Transform
# 整理為movie_tw_year_amount dataframe 更名欄位名稱並轉換型別
def to_TWMovie_annual_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', "Year", "DayCount", "Amount", "Tickets"]]

    # dfTW_count = df.groupby(['MovieId']).count()
    # print(dfTW_count)
    df.rename(columns= {"MovieId": "tw_id", "Year": "reference_year", "DayCount": "tw_release_days", "Amount": "reference_year_amount", "Tickets": "reference_year_tickets"}, inplace= True)
    df['reference_year'] = pd.to_datetime(df['reference_year'], format = '%Y')
    convert_dict = {'tw_id': str}
    df = df.astype(convert_dict)
    

    return df

dfTWMovie_annual = to_TWMovie_annual_dataframe(dfTWMovie)
# print(dfTWMovie_annual.dtypes)

#task9
#存成TWMovie_annual_df.csv
# sfm.save_as_csv(dfTWMovie_annual, "TWMovie_annual_df3.csv", "/workspaces/TIR104_g2_new/A1_temp_data/tw/")




