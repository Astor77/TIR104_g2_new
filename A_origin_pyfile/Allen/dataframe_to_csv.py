

def dataframe_to_csv(df: pd.DataFrame,  output_path: str) -> str:
        """
        將DataFrame轉成CSV存入資料夾
        參數:df: pd.DataFrame,  outpute_path: str
        return:順利將DataFrame轉成csv存入資料夾
               如果輸入值非DataFrame，回傳None
        """
        if not isinstance(df, pd.DataFrame, str): #類型檢查
               print("執行錯誤:輸入不是DataFrame")
               return None
        try:
            df.to_csv(output_path, index=0, encoding="utf-8-sig")
            print(f"csv檔寫入成功{output_path}")
            return output_path
        except Exception as e:
              print(f"執行錯誤狀態{e}")
              return None
