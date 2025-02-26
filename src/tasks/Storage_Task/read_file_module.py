# read_file_task.py -> 地端讀取csv, json回傳資料（df）的函式

import pandas as pd
import json
from pathlib import Path

# 讀取 csv 或是 json 檔案並返回 dataframe
def read_file_to_df(dir_path: str | Path, file_name: str) -> pd.DataFrame:
    """
    將指定路徑的檔案轉換成 DataFrame，支援 .csv 和 .json 格式。
    Args:
        file_path (str | Path): 檔案路徑（含檔案名稱、副檔名）
        return: 轉換後的 Pandas DataFrame
    """
    file_path = Path(dir_path) / file_name  # 確保轉成 Path 物件
    if not Path(file_path).exists(): # 檢查檔案是否存在
        raise FileNotFoundError(f"檔案未找到: {file_path}")

    try:
        # 會按照檔案路徑的結尾部分進行分析，尋找最後一個「小數點」（.）。拆分主檔名跟副檔名
        file_extension = Path(file_path).suffix.lower()

        if file_extension == ".csv":
            df = pd.read_csv(file_path)
        elif file_extension == ".json":
            df = pd.read_json(file_path)
        else:
            raise ValueError(f"不支援的檔案格式: {file_extension}")

    except Exception as err:
        print(f"error: {err}")
        return None

    return df


# 讀取 json 檔案並返回 json
def read_json_file_to_json(dir_path: str | Path, file_name: str) -> list:
    """
    讀取指定路徑的json檔案返回json，支援 .json 格式。
    Args:
        file_path (str | Path): 檔案路徑（含檔案名稱、副檔名）
        return: json
    """
    file_path = Path(dir_path) / file_name  # 確保轉成 Path 物件
    if not Path(file_path).exists(): # 檢查檔案是否存在
        raise FileNotFoundError(f"檔案未找到: {file_path}")

    try:
        # 會按照檔案路徑的結尾部分進行分析，尋找最後一個「小數點」（.）。拆分主檔名跟副檔名
        file_extension = Path(file_path).suffix.lower()

        if file_extension == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        else:
            raise ValueError(f"不支援的檔案格式: {file_extension}")

    except Exception as err:
        print(f"error: {err}")
        return None

    return json_data