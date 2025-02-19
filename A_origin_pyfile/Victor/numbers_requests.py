import requests
from lxml import html
import pandas as pd
import time

def numbers_movie_info(year):

    offset = 1
    data = []

    columns = [
        "Rank", "Movie", "Worldwide Box Office", "Domestic Box Office", "International Box Office", "DomesticShare"
    ]
    #迴圈去跑資料出來並存檔
    while True:
        
        url = f"https://www.the-numbers.com/box-office-records/worldwide/all-movies/cumulative/released-in-{year}/{offset}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        }

        response = requests.get(url, headers=headers)
        tree = html.fromstring(response.text)

        if response.status_code != 200:
            print("請求失敗，停止爬取")
            break

        print(f"開始抓取 {url}")

        # 取出表格
        movie_element = tree.xpath('//*[@id="page_filling_chart"]/center/table/tbody')

        # 如果找不到表格可以抓就break就break
        if not movie_element:
            print("找不到表格，停止爬取")
            break

        # 取出所有 tr 標籤，包含每一部電影的資訊
        movie_title = movie_element[0].xpath('.//tr')

        # 檢查是否有電影資料
        if not movie_title:
            print("{year}沒有更多電影資料，停止爬取")
            break

        # 顯示調試訊息，檢查每一行的內容
        print(f"已抓取 {len(movie_title)} 部電影資料")

        # 處理每一行數據
        
        for row in movie_title:
            columns_data = row.xpath('./td')  # 取得 <td> 欄位
            if len(columns_data) == 6:  # 確保每一行有 6 個欄位
                row_data = [col.text_content().strip() for col in columns_data]
                data.append(row_data)

        # 收集數據
        data.extend(data)
        offset += 100  # 下一頁

        time.sleep(2)  # 加入延遲，避免封鎖

    # 將數據轉換為 DataFrame
    df = pd.DataFrame(data, columns=columns)
    df.to_csv("numbers_{year}.csv", index=False, encoding="utf-8")

    print(f"完成，共 {len(df)} 筆電影資料，已儲存至 numbers_{year}.csv")



#將每個年份以參數的方式帶進function
yearlist = [2022, 2023, 2024, 2025]
for year in yearlist:
    numbers_movie_info(year)
