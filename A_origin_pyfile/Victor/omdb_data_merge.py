import pandas as pd
import json
from datetime import datetime

def omdb_raw_to_tmp(filename, columns, save_path):
    #先讀檔案
    with open(filename, "r", encoding="utf-8") as file:
        omdb_data = json.load(file)

    if not isinstance(omdb_data, list):
        raise ValueError("JSON 格式錯誤。")
    
    combined_data = []
    #攤開json格式重新加入一個list內才能轉df
    for item in omdb_data:
        if isinstance(item, dict):
            combined_data.append(item)
        elif isinstance(item, list):
            combined_data.extend(item)
    
    #轉成df
    omdb_raw_data = pd.DataFrame(combined_data)
    #取出需要轉成tmp的欄位
    omdb_tmp_data = (omdb_raw_data[columns])
    #建立時間
    current_time = datetime.now().strftime("%Y-%m-%d")
    omdb_tmp_data["data_created_time"] = current_time
    omdb_tmp_data["data_updateded_time"] = current_time
    #儲存檔名
    file_info = f"{save_path}/omdb_info_temp_{current_time}.csv"
    #存成csv
    omdb_tmp_data.to_csv(file_info, index=False)
    print("已成功儲存檔案")

#需要導入的檔案
filename = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_raw_data_2025-02-23.json"
#需要留著的欄位
columns = ["imdbID", "imdbRating"]
#指定儲存路徑
save_path = r"/workspaces/TIR104_g2_new/A1_temp_data/tw"
omdb_raw_to_tmp(filename, columns, save_path)




