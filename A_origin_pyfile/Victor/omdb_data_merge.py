import pandas as pd
import json

#merge資料後存檔
def data_merge_csv(f1,f2, key, outout_file):
    #開啟第一個檔案轉換成dataframe
    df1 = pd.read_csv(f1, encoding="utf-8")
    #開啟第二個檔案轉換成dataframe
    df2 = pd.read_csv(f2, encoding="utf-8")
    #將兩個df merge
    merge_data = pd.merge(df1, df2, on=key, how="outer")
    merge_data.to_csv(outout_file, index=False, encoding="utf-8")
    merge_data.to_json(outout_file.replace(".csv", ".json"), orient="records", force_ascii=False)
    print(merge_data)


def data_merge_json(f1, f2, key, output_file):
    # 開啟第一個 JSON 檔案轉換成 DataFrame
    df1 = pd.read_json(f1, encoding="utf-8")
    # 開啟第二個 JSON 檔案轉換成 DataFrame
    df2 = pd.read_json(f2, encoding="utf-8")
    # 將兩個 DataFrame 合併
    merge_data = pd.merge(df1, df2, on=key, how="outer")
    # 儲存成 CSV
    merge_data.to_csv(output_file, index=False, encoding="utf-8")
    # 儲存成 JSON
    merge_data.to_json(output_file.replace(".csv", ".json"), orient="records", force_ascii=False)
    # 印出合併後的 DataFrame
    print(merge_data)

#data_merge_csv()
#資料整理
#merge資料後去除不需要的欄位
def cd_column():
    df = df.drop(["column"], axis=1)



