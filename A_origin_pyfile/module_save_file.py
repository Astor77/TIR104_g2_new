import json
import os
import requests
from datetime import datetime
import pandas as pd
from pathlib import Path

# 設定全域的 log 文件路徑
LOG_FILE_PATH = "/workspaces/TIR104_g2/save_as_file_log.txt"

# write_log()
# 寫入訊息至 log file
def write_log(operation: str, file_name: str, status: str, message: str, file_path: str ,log_file_path: str = LOG_FILE_PATH) -> None:
    """
    記錄儲存操作到 log 文件
    Args:
        operation (str): 操作類型（如 save_csv 或 save_json）
        file_name (str): 操作的檔案名稱
        status (str): 成功或失敗（success 或 error）
        message (str): 詳細的訊息
        file_path (str): 檔案儲存的完整路徑
        log_file (str): 要寫入的 log 文件
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_message = f"[{timestamp}] {status.upper()}: {operation} - File '{file_name}' - File Path'{file_path}' - {message}\n"

    with open(log_file_path, "a", encoding="utf-8") as file:
        file.write(log_message)

# save_as_csv()
# 儲存 DataFrame 為 CSV 檔案
def save_as_csv(dataframe: pd.DataFrame, file_name: str, dir_path: str) -> None:
    """
    將 DataFrame 儲存為 CSV 檔案
    Args:
        dataframe (pd.DataFrame): 要儲存的 DataFrame
        file_name (str): 儲存的檔案名稱字串
        dir_path (str): 儲存的資料夾完整路徑
    """
    try:
        csv_file_path = Path(dir_path) / file_name
        dataframe.to_csv(csv_file_path, encoding="utf-8-sig", index=False)

        write_log("save_as_csv", file_name, "success", "檔案儲存成功", csv_file_path)
        print(f"{file_name} 儲存成功, 存放路徑: {csv_file_path}")

    except Exception as err:
        write_log("save_as_csv", file_name, "fail", f"{err}", csv_file_path)
        print(f"{file_name} 儲存失敗。error: {err}")

# save_as_json()
# 將 list 儲存為 JSON 檔案
# 留意此處的 data 引數需要用list type
def save_as_json(data: list, file_name: str, dir_path: str) -> None:
    """
    將 list 儲存為 JSON 檔案
    Args:
        data (list): 要儲存的 DataFrame
        file_name (str): 儲存的檔案名稱字串
        dir_path (str): 儲存的資料夾完整路徑
    """
    try:
        json_file_path = Path(dir_path) / file_name

        directory = os.path.dirname(json_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

            write_log("save_as_json", file_name, "success", "檔案儲存成功", json_file_path)
            print(f"{file_name} 儲存成功, 存放路徑: {json_file_path}")

    except Exception as err:
        write_log("save_as_json", file_name, "fail", f"{err}", json_file_path)
        print(f"{file_name} 儲存失敗。error: {err}")