
#將list轉json寫入指定資料夾(A0_raw_data>tw>tmdb_credits)

def list_to_json(input_tgmcm: list) -> str: #資料寫入檔案，返回一個字串，表示 JSON 檔案的路徑
        """
        all_tmdb_id_cast_data轉成JSON格式寫入檔案
        參數:input_tgmcm: list
        return:str: JSON 檔案的路徑
               如果輸入不是list，則返回None
        """
        if not isinstance(input_tgmcm, list): #類型檢查
               print("執行錯誤:輸入不是清單")
               return None
        try:
            output_filename = "/workspaces/TIR104_g2/A0_raw_data/tw/tmdb_credits/all_tmdb_id_cast_data.json" #路徑檔名命名
            with open(output_filename, "w", encoding="utf-8") as f: #寫入(路徑檔名命名),解碼
                   #轉成json格式,寫入檔案,4隔空格縮排,取消ASCII;顯示中文字或其他非ASCII字
                json.dump(input_tgmcm, f, indent=4, ensure_ascii=False)
                print(f"所有演員資料存放在 {output_filename}")
                return output_filename # 返回檔案路徑
        except Exception as e:
               print(f"執行錯誤:{e}")
               return None
