

def dataframe_to_dict(df: pd.DataFrame) -> dict:
    """
    將pd的dataFrame轉成字典
    參數:df: pd.DataFrame
    return:轉換後的字典dirct
           如果輸入不是dataframe，則返回None

    """
    if not isinstance(df, pd.DataFrame): #類型檢查:檢查變數df 是否為 Pandas DataFrame 類型
        print("執行錯誤:輸入的不是Pandas DataFrame")
        return None
    try:
        dict_data  = df.to_dict(orient="records")
        return dict_data
    except Exception as e:
        print(f"執行錯誤:{e}")
        return None
    
