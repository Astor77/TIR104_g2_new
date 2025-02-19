

def tbdb_ids(input_list: list) -> list:
    """
    從包含字典的列表中提取 tmdb_id
    參數:input_list: list
    return:list: 有提取到的 tmdb_id 的列表
                 輸入不是列表，或列表中的元素不是字典，則返回 None。
    """
    if not isinstance(input_list, list):
        print("執行錯誤，輸入不是list")
        return None
    
    tmdb_ids = []
    for i in input_list:
        if not isinstance(i, dict):
            print("執行錯誤，list中元素非字典")
            return None
        try:
            tmdb_ids.append(i["tmdb_id"])
        except KeyError:
            print("執行錯誤，字典中缺'tmdb_id'建")
            return None
        except Exception as e:
            print(f"執行出錯狀況{e}")
            return None
    return tmdb_ids
