# 讀取將抓到的json資料轉成DataFrame

def json_to_dataframe(json_file_path: str) -> pd.DataFrame:
        """
        將JSON讀取成DataFrame
        參數:json_file_path: str
        return:pd.DataFram:從json讀取到的DataFrame
               如果輸入值非字串，回傳None
        """
        if not isinstance(json_file_path, str): #類型檢查
               print("執行錯誤:輸入不是字串")
               return None
        try:
            df = pd.read_json(json_file_path, orient="records")
            return df
        except FileExistsError:
              print(f"檔案出錯{json_file_path}")
              return None
        except ValueError:
              print(f"檔案值錯誤{json_file_path}")
              return None
        except Exception as e:
              print(f"執行錯誤狀態{e}")
              return None
