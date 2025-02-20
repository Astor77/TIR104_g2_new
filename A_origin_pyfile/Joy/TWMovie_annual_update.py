import pandas as pd
import numpy as np
import module_save_file as ms
import tasks.Storage_Task.read_file_module as rfm
import tasks.Storage_Task.save_file_module as sfm
import utils.path_config as p

#task 1
#讀取原始檔案csv

dfTWMovie = rfm.read_file_to_df(p.raw_tw_2022_2025 / "TWMovie2022-2025_raw.csv")


#task 2
#讀取刪除有重複的csv

dfTWMovie_distinct = rfm.read_file_to_df(p.raw_tw_2022_2025 / "TWMovie2022-2025.csv")


#task 3
#全國單片抓取的release_date

release_dates = rfm.read_file_to_df(p.raw_tw_release_dates / "tw_release_dates.csv")



#task4 Transform
#TWMovie 和 release_date合併
def merge_TWMovie_release_date(df1: object, df2: object) -> pd.DataFrame:
    df_new = pd.concat([df1, df2], axis= 1)
    return df_new

#task5 Transform
#比較release_date和 release_dates欄位，並根據兩邊的不同新增正確的tw_first_release_date欄位
def compare_release_date(df: object) -> pd.DataFrame:
    df['tw_release_date'] = np.where((df['ReleaseDate'] != df['release_dates']) & pd.notna(df['release_dates']), df['release_dates'], df['ReleaseDate'])
    df.drop(columns=['ReleaseDate', 'release_dates'], inplace = True)
    return df

#task6 Transform
#整理為movie_detail dataframe 並更名欄位名稱
def to_TWMovie_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', 'Region', 'Name', "tw_release_date"]]
    df.rename(columns= {"MovieId": "tw_id", "Region": "production_country", "Name": "tw_title"}, inplace= True)

    return df

#task7
#存成TWMovie_df2.csv
sfm.save_as_csv (to_TWMovie_dataframe(compare_release_date(merge_TWMovie_release_date(dfTWMovie_distinct, release_dates))), "TWMovie_df2.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")

#task8 Transform
# 整理為movie_tw_year_amount dataframe 並更名欄位名稱
def to_TWMovie_annual_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', "Year", "DayCount", "Amount", "Tickets"]]

    dfTW_count = df.groupby(['MovieId']).count()
    print(dfTW_count)
    df.rename(columns= {"MovieId": "tw_id", "Year": "reference_year", "DayCount": "tw_release_days", "Amount": "reference_year_amount", "Tickets": "reference_year_tickets"}, inplace= True)
    
    return df

#task9
#存成TWMovie_annual_df.csv
sfm.save_as_csv(to_TWMovie_annual_dataframe(dfTWMovie_distinct), "TWMovie_annual_df.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")

if __name__ == "__main__":

    dfTWMovie_new = merge_TWMovie_release_date(dfTWMovie_distinct, release_dates)
    print(dfTWMovie_new.head())
    # ms.save_as_csv(to_TWMovie_dataframe(compare_release_date(dfTWMovie_new)), "TWMovie_df2.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")
    # ms.save_as_csv(to_TWMovie_annual_dataframe(dfTWMovie_distinct), "TWMovie_annual_df.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")