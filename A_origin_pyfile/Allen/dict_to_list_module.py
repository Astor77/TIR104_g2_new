

def dict_to_list(input_dict: dict) -> list: #類型提示:input_dict是參數的名稱，dict 是期望的類型
    """
    將輸入的字典轉成清單
    參數:input_dict: dict
    return:轉換後是包含字典的清單
           如果輸入不是dict，則返回None
    """
    if not isinstance(input_dict, dict): #類型檢查
        print("執行錯誤:輸入的不是字典")
        return None
    try:
        list_data  = list(input_dict.values()) #將字典的值values()轉換成列表list
        return list_data
    except Exception as e:
        print(f"執行錯誤:{e}")
        return None
