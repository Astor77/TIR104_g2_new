
file_path = "/workspaces/TIR104_g2/Ａ_raw_data/v2_mapping_close_true.csv"
def pandas_read_csv(file_path):

    """
    讀取file_path csv檔，返回dataframe
    參數(str):file_path
    返回:pd.dataframe讀取到dataframe，如果檔案不存在或錯誤，返回None
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except FileExistsError:
        print(f"檔案{file_path}錯誤")
        return None
    except FileNotFoundError:
        print(f"檔案{file_path}找不到")
        return None
    except Exception as e: # e為取得有關錯誤資訊的變數
        print(f"執行發生錯誤:{e}")
        return None