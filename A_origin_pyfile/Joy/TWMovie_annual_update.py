from pathlib import Path
import pandas as pd
import numpy as np
import webbrowser
import module_save_file as ms


#task1 Transform
#TWMovie 和 release_date合併
def merge_TWMovie_release_date(df1: object, df2: object) -> pd.DataFrame:
    df_new = pd.concat([df1, df2], axis= 1)
    return df_new

#task2 Transform
#比較release_date和 release_dates欄位，並根據兩邊的不同新增正確的tw_first_release_date欄位
def compare_release_date(df: object) -> pd.DataFrame:
    df['tw_release_date'] = np.where((df['ReleaseDate'] != df['release_dates']) & pd.notna(df['release_dates']), df['release_dates'], df['ReleaseDate'])
    df.drop(columns=['ReleaseDate', 'release_dates'], inplace = True)
    return df

#task3 Transform
#整理為movie_detail dataframe 並更名欄位名稱
def to_TWMovie_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', 'Region', 'Name', "tw_release_date"]]
    df.rename(columns= {"MovieId": "tw_id", "Region": "production_country", "Name": "tw_title"}, inplace= True)

    return df

#task4 Transform
# 整理為movie_tw_year_amount dataframe 並更名欄位名稱
def to_TWMovie_annual_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', "Year", "DayCount", "Amount", "Tickets"]]

    dfTW_count = df.groupby(['MovieId']).count()
    print(dfTW_count)
    df.rename(columns= {"MovieId": "tw_id", "Year": "reference_year", "DayCount": "tw_release_days", "Amount": "reference_year_amount", "Tickets": "reference_year_tickets"}, inplace= True)
    
    return df


if __name__ == "__main__":
    #原始檔案
    try:
        file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_2022-2025/TWMovie2022-2025_raw.csv"
        dfTWMovie = pd.read_csv(file_path)
        print(dfTWMovie.head())
    except Exception as e:
        print(f"Error reading file: {e}")

    #刪除有重複的movieid
    try:
        file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_2022-2025/TWMovie2022-2025.csv"
        dfTWMovie_distinct = pd.read_csv(file_path)
        print(dfTWMovie_distinct.head())
    except Exception as e:
        print(f"Error reading file: {e}")
    
    #全國單片抓取的release_date
    try:
        file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_release_dates/tw_release_dates.csv"
        release_dates = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading file: {e}")

    dfTWMovie_new = merge_TWMovie_release_date(dfTWMovie_distinct, release_dates)
    print(dfTWMovie_new.head())
    # ms.save_as_csv(to_TWMovie_dataframe(compare_release_date(dfTWMovie_new)), "TWMovie_df2.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")
    ms.save_as_csv(to_TWMovie_annual_dataframe(dfTWMovie_distinct), "TWMovie_annual_df.csv", "/workspaces/TIR104_g2/A1_temp_data/tw/")