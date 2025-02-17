#write_log -> 可以針對一些存取內容設計寫入某個log file的函式們
from pathlib import Path
from datetime import datetime
import utils.path_config as p

# write_log()
# 寫入訊息至 log file
def write_save_log(operation: str, file_name: str, status: str, message: str, save_file_path: str | Path ,log_file_path: str | Path = p.save_file_log) -> None:
    """
    記錄儲存操作到指定log文件
    Args:
        operation (str): 操作類型（如 save_csv 或 save_json）
        file_name (str): 操作的檔案名稱
        status (str): 成功或失敗（success 或 error）
        message (str): 詳細的訊息
        save_file_path (str/Path): 檔案儲存的完整路徑
        log_file_path (str/Path): 要寫入的 log 文件
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {status.upper()}: {operation} - File '{file_name}' - {message} - Save File Path'{save_file_path}'\n"

    log_file_path.parent.mkdir(parents=True, exist_ok=True) # 確保目錄存在
    with open(log_file_path, "a", encoding="utf-8") as file:
        file.write(log_message)