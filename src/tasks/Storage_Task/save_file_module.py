# save_file_task.py -> 地端轉存檔成csv, json的函式

import json
import pandas as pd
from pathlib import Path
from utils.write_log_task import write_save_log


# save_as_csv()
# 儲存 DataFrame 為 CSV 檔案
def save_as_csv(dataframe: pd.DataFrame, dir_path: str | Path, file_name: str) -> None:
    """
    將 DataFrame 儲存為 CSV 檔案
    Args:
        dataframe (pd.DataFrame): 要儲存的 DataFrame
        file_name (str): 儲存的檔案名稱字串
        dir_path (str): 儲存的資料夾完整路徑
    """
    try:
        dir_path = Path(dir_path)  # 確保轉成 Path 物件
        csv_file_path = dir_path / file_name
        # 確保目錄存在
        csv_file_path.parent.mkdir(parents=True, exist_ok=True)

        # 轉換數據型態，確保 BigQuery 兼容
        for col in dataframe.columns:
            if pd.api.types.is_integer_dtype(dataframe[col]):
                dataframe[col] = dataframe[col].astype("Int64")  # 保持 NaN
            elif pd.api.types.is_float_dtype(dataframe[col]):
                dataframe[col] = dataframe[col].astype("float64")  # 保持 NaN
            elif pd.api.types.is_object_dtype(dataframe[col]):
                dataframe[col] = dataframe[col].astype("string")  # 確保是 string
            elif pd.api.types.is_datetime64_any_dtype(dataframe[col]):
                dataframe[col] = dataframe[col].dt.strftime('%Y-%m-%d %H:%M:%S')  # 轉成 BigQuery 日期格式
        dataframe.to_csv(csv_file_path, encoding="utf-8-sig", index=False)

        write_save_log("save_as_csv", file_name, "success", "檔案儲存成功", csv_file_path)
        print(f"{file_name} 儲存成功, 存放路徑: {csv_file_path}")

    except Exception as err:
        write_save_log("save_as_csv", file_name, "fail", f"{err}", csv_file_path)
        print(f"{file_name} 儲存失敗。error: {err}")

# save_as_json()
# 將 list 儲存為 JSON 檔案
# 留意此處的 data 引數需要用list type
def save_as_json(data: list, file_name: str, dir_path: str | Path) -> None:
    """
    將 list 儲存為 JSON 檔案
    Args:
        data (list): 要儲存的 DataFrame
        file_name (str): 儲存的檔案名稱字串
        dir_path (str): 儲存的資料夾完整路徑
    """
    try:
        dir_path = Path(dir_path)  # 確保轉成 Path 物件
        json_file_path = dir_path / file_name

        # 確保目錄存在
        json_file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

            write_save_log("save_as_json", file_name, "success", "檔案儲存成功", json_file_path)
            print(f"{file_name} 儲存成功, 存放路徑: {json_file_path}")

    except Exception as err:
        write_save_log("save_as_json", file_name, "fail", f"{err}", json_file_path)
        print(f"{file_name} 儲存失敗。error: {err}")