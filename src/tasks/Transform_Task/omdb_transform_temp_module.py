import pandas as pd

import tasks.Storage_Task.read_file_module as rm
import tasks.Transform_Task.other_module as om
import utils.path_config as p
# import importlib  # Python 內建的重新載入工具
# importlib.reload(rm)  # 強制重新載入
# importlib.reload(p)  # 強制重新載入


# omdb_info function1
def omdb_trans() -> pd.DataFrame:
    """
    讀取 omdb 的 dataframe，針對特定欄位型態轉換
    return: pd.DataFrame
    """
    # 因json無嵌套結構，直接讀取檔案成dataframe
    omdb_df = rm.read_file_to_df(p.raw_tw_omdb_info, p.omdb_info_json)
    # imdbRating欄位處理NA,轉換成float
    omdb_df["imdbRating"] = omdb_df["imdbRating"].replace("N/A", pd.NA)
    omdb_df["imdbRating"] = pd.to_numeric(omdb_df["imdbRating"], errors='coerce')
    # imdbID欄位轉換
    convert_dict = {"imdbID": "string"}
    omdb_trans_df = omdb_df.astype(convert_dict)
    return omdb_trans_df