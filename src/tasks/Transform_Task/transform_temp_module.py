import pandas as pd
import json
from datetime import datetime

def omdb_raw_to_tmp(filename, columns):
    
    omdb_data = pd.read_json(filename)

    #取出需要轉成tmp的欄位
    omdb_tmp_data = (omdb_data[columns])
    #建立時間
    current_time = datetime.now().strftime("%Y-%m-%d")
    omdb_tmp_data["data_created_time"] = current_time
    omdb_tmp_data["data_updateded_time"] = current_time

    #存成csv
    #omdb_tmp_data.to_csv("omdb_tmp.csv", index=False)
    #print("已成功儲存檔案")


#需要導入的檔案
filename = r"C:\Users\User\Desktop\Python_note\omdb_info.json"
#需要留著的欄位
columns = ["imdbID", "imdbRating"]
#呼叫 
omdb_raw_to_tmp(filename, columns)



# tmdb_get_movie_release_date(台灣資料跟全球資料適用)
def tmdb_get_movie_release_date(tmdb_id_list: list , API_KEY: str = RAIN_TMDB_KEY) -> list:

    dir_path = p.raw_tw_tmdb_release_date
    file_name = "tmdb_release_dates_raw_20250219.json"
    file_path = dir_path / file_name
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data

    release_dates = []

    for movie in data:
        movie_id = movie.get("id")
        for result in movie.get("results"):
            movie_iso = result.get("iso_3166_1")
            for release in result.get("release_dates",[]):
                release["id"] = movie_id
                release["iso_3166_1"] = movie_iso
                release_dates.append({
                        "tmdb_id": movie_id,
                        "release_country_code": movie_iso,
                        "release_type_note": release.get("note"),
                        "type_release_date": release.get("release_date"),
                        "release_type_id": release.get("type")
                })
    release_df = pd.DataFrame(release_dates)



"""
Joy

TWMovie_annual_update
"""

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

#task6 Transform
#整理為movie_detail dataframe 更名欄位名稱，並轉換資料型態
def to_TWMovie_dataframe(df: object) -> pd.DataFrame:

    df = df[['MovieId', 'Name', "tw_first_release_date"]]
    df.rename(columns= {"MovieId": "tw_id", "Name": "tw_title"}, inplace= True)

    convert_dict = {'tw_id': str, 'tw_title': str}
    df = df.astype(convert_dict)

    return df

dfTWMovie_fin = to_TWMovie_dataframe(compare_release_date(dfTWMovie_new))
# print(dfTWMovie_fin.dtypes)
#task7
#存成TWMovie_df2.csv
sfm.save_as_csv(dfTWMovie_fin, "TWMovie_df3.csv", "/workspaces/TIR104_g2_new/A1_temp_data/tw/")

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
print(dfTWMovie_annual.dtypes)

#task9
#存成TWMovie_annual_df.csv
sfm.save_as_csv(dfTWMovie_annual, "TWMovie_annual_df3.csv", "/workspaces/TIR104_g2_new/A1_temp_data/tw/")


"""
Joy
weely資料
TWMovie_sele_update
"""


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


