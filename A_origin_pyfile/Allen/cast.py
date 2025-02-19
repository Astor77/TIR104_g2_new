#所有演員資料的程式碼

#防呆機制(避免跑太久才發現被伺服器檔)
if response.status_code == 200: #如果狀態回應碼200執行以下
    try:
        data = response.json() #將回應的JSON字串轉換為的字典存在變數data
        #print(data) #試印觀察

        cast_data = data.get("cast") #取出字典data裡面的卡司的值

        # 進行tmdb_id放在卡司清單中
        for cast in cast_data: #迴圈取所有卡司資料
            cast["movie_id"] = tmdb_id #將tmdb_id("move_id")放進卡司清單中
            #print(f"{cast}") #試印觀察
            all_tmdb_id_cast_data.append(cast) #將卡司清單加進一開始的空list
            #print(all_tmdb_id_cast_data) #試印觀察

    #若json格式解碼error現示
    except json.JSONDecodeError:
        print(f"解碼{tmdb_ids[i]}的json格式出錯")
    #否則請求狀碼失敗顯示
    else:
        print(f"{tmdb_ids[i]}請求失敗:回應狀態碼為{response.status_code}")