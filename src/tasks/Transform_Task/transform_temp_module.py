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